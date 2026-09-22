"""git 호출 계층 — 주입 가능(injectable)하게 만든 얇은 subprocess 래퍼.

설계 의도
---------
* 네트워크·git 접근을 전부 이 모듈 한 곳으로 좁힌다. 테스트는 GitRunner 를
  갈아끼우기만 하면 네트워크 없이 돌아간다.
* 자격증명 값은 **stdout/stderr/예외 메시지 어디에도 실리지 않는다**.
  redact() 가 모든 출력 경로에서 강제된다.
* 대화형 프롬프트를 원천 차단한다 (GIT_TERMINAL_PROMPT=0). 헤드리스에서
  git 이 자격증명을 물으며 멈추는 것이 최악의 실패 모드다.
* **자격증명을 왕복마다 다시 조회하지 않는다** — 원격 호스트별로 **한 번**
  `git credential fill` 로 받아 이 프로세스 메모리에 들고, 이후의 네트워크
  호출에는 그 값을 **환경변수로** 먹인다 (`credential_env()`. 실측 −607ms/왕복,
  헬퍼 기동 2회 → 0회). 그 조회가 안 되면 OS 저장소 헬퍼를 얹고
  (`credential_config()`), 그것도 안 되면 **사용자 설정 그대로** 쓴다 — 세 단계
  전부 로그를 남긴다 (조용한 열화 금지). 사용자의 전역 설정은 읽기만 한다.
* **Windows 에서 콘솔 창을 띄우지 않는다** (`creation_flags`). 아래 참조.

Windows — 왜 창 억제 플래그가 필요한가
--------------------------------------
``git.exe`` 는 **콘솔 앱**이다. Windows 의 ``CreateProcess`` 는 콘솔 앱을 띄울 때
부모의 콘솔을 물려주는데, **부모에게 콘솔이 없으면 자식에게 콘솔을 새로 할당하고
그 콘솔은 창으로 그려진다.** 그래서 콘솔 없는 프로세스(GUI 앱·서비스·
``pythonw.exe`` 로 띄운 백그라운드 앱)가 이 러너를 쓰면 **git 호출마다 빈 창이
깜빡인다.** 출력은 ``capture_output`` 으로 파이프에 받으니 그 창은 **비어 있다** —
사용자는 닫아도 되는지 판단할 수 없다.

실측(2026-09-08, 이 라이브러리를 쓰는 채팅 앱): 앱을 ``pythonw.exe`` 로 띄우고
15초마다 폴링하니 ``git.exe`` → ``conhost.exe`` → 터미널 창이 그 주기로 떴다
사라졌다. ``python.exe`` 로 띄웠을 때는 git 이 그 콘솔을 조용히 물려받아 **안
보였을 뿐**, 같은 일이 계속 일어나고 있었다.

그래서 **``DETACHED_PROCESS``** 를 건다 — 자식은 콘솔을 **갖지 않는다.** 창이
없으므로 (1) 깜빡이지 않고 (2) 사용자가 그 창을 닫아 git 을 죽일 수도 없다. git 이
다시 부르는 손자(자격증명 헬퍼 등)도 콘솔 없이 뜬다 — 한 곳만 고쳐도 트리 전체가
조용해진다. (처음에는 ``CREATE_NO_WINDOW`` 였다 — 창만 숨긴 **새 콘솔**을 주는
플래그라 호출마다 ``conhost.exe`` 가 함께 떠 약 40ms 를 더 냈다. `creation_flags`
도크에 실측이 있다.)

⚠️ ``DETACHED_PROCESS`` 는 이 문제의 답이 아니다. 그건 "콘솔을 아예 주지 않는다"는
뜻이라 **손자가 다시 자기 콘솔을 창과 함께 할당한다** — 문제를 한 세대 미룬다.

캡처·타임아웃·프롬프트 억제는 영향받지 않는다. ``capture_output`` 은 파이프이고
파이프는 콘솔과 무관하다. 프롬프트는 원래 ``GIT_TERMINAL_PROMPT=0`` 이 막고, 창
없는 콘솔에서는 사람이 볼 수 있는 터미널 대화가 애초에 불가능하다 — 억제가 **더**
확실해진다. Windows 밖(macOS·Linux)에는 콘솔이라는 개념 자체가 없어 플래그가
``0`` 이다: 동작이 한 글자도 바뀌지 않는다.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Mapping, Protocol, Sequence

from .errors import AuthError, GitError, PushRejected

log = logging.getLogger("gitwire")

_URL_CRED_RE = re.compile(r"(?<=://)[^/\s@]+:[^/\s@]+@")

#: 커밋·재작성 시 항상 붙이는 설정. 전역 git config 에 의존하지 않는다.
BASE_CONFIG: tuple[str, ...] = (
    "-c", "core.autocrlf=false",
    "-c", "core.safecrlf=false",
    "-c", "commit.gpgsign=false",
    "-c", "gc.auto=0",
    # ⭐ `commit`·`fetch` 뒤에 git 이 `git maintenance run --auto` 를 **자식
    # 프로세스로** 띄운다 — `gc.auto=0` 은 그 자식이 *할 일이 없다*고 판정하게
    # 할 뿐, 자식이 뜨는 것 자체는 막지 못한다 (GIT_TRACE 실측: 커밋마다
    # `run_command: git maintenance run --auto --no-quiet --detach` 1개, 이 머신
    # 에서 ~50ms). 이 키가 그 스폰을 막는다 (`commit --allow-empty` 124→77ms).
    "-c", "maintenance.auto=false",
    "-c", "advice.detachedHead=false",
)


#: 자격증명이 필요할 수 있는(= 네트워크를 타는) 서브커맨드.
#: 여기 없는 호출에는 자격증명 설정을 **얹지 않는다** — 로컬 git 호출의 동작을
#: 한 글자도 바꾸지 않기 위해서다.
NETWORK_CMDS = frozenset({"push", "fetch", "clone", "pull", "ls-remote"})

#: 자격증명 헬퍼 이름 강제 지정(진단·특수 환경용). `off`/`0`/`none`/`""` 이면
#: 우리 쪽 지정을 아예 끄고 **사용자 설정 그대로** 쓴다.
HELPER_ENV = "GITWIRE_CREDENTIAL_HELPER"

_OFF = ("off", "0", "no", "none", "false", "")

#: `sys.platform` 접두 → 그 OS 가 기본으로 제공하는 자격증명 저장소 헬퍼.
#: Windows 는 `os.name` 으로 먼저 가른다 (`sys.platform` 이 `win32`·`cygwin` 로
#: 갈리기 때문이다).
_OS_HELPER: tuple[tuple[str, str], ...] = (
    ("darwin", "osxkeychain"),   # macOS 키체인
    ("linux", "libsecret"),      # freedesktop Secret Service
    ("freebsd", "libsecret"),
    ("openbsd", "libsecret"),
    ("netbsd", "libsecret"),
)

_cred_lock = threading.Lock()
_cred_cache: dict[str, tuple[str, ...]] = {}
#: 인증 실패로 "빠른 헬퍼"를 접은 경우. 이 프로세스에서는 다시 시도하지 않는다.
_cred_disabled: set[str] = set()


def _os_helper() -> str | None:
    """이 OS 의 기본 자격증명 저장소 헬퍼 이름 (모르면 None)."""
    if os.name == "nt":
        return "wincred"                 # Windows 자격 증명 관리자
    for prefix, helper in _OS_HELPER:
        if sys.platform.startswith(prefix):
            return helper
    return None


def _helper_installed(name: str, git_path: str) -> bool:
    """`git credential-<name>` 을 이 머신에서 **실제로 실행할 수 있나.**

    ⚠️ 이 판정이 없으면 없는 헬퍼를 강제해 자격증명을 못 얻고 push 가 깨진다.
    (실측: 이 머신에는 `git-credential-wincred.exe` 만 있고
    `git-credential-cache` 는 없다.)

    이름이 아니라 **명령**(`!`로 시작하거나 경로 구분자를 품은 값)이면 git 이
    그걸 그대로 실행하므로 우리가 판정할 것이 없다 — 준 사람을 믿는다.
    """
    if name.startswith("!") or "/" in name or "\\" in name:
        return True
    exe = "git-credential-" + name.split()[0]
    if shutil.which(exe):
        return True
    # 보통 PATH 에는 없고 git 의 exec-path 안에 있다.
    try:
        res = subprocess.run(
            [git_path, "--exec-path"],
            capture_output=True,
            timeout=30,
            creationflags=creation_flags(),
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if res.returncode != 0:
        return False
    d = Path(res.stdout.decode("utf-8", "replace").strip() or ".")
    return any((d / (exe + suffix)).exists() for suffix in ("", ".exe", ".cmd"))


def credential_config(git_path: str = "git") -> tuple[str, ...]:
    """네트워크 git 호출에 얹을 `-c credential.helper=…` 인자 (없으면 빈 튜플).

    ⭐ **이것은 이제 2단이다.** 1단은 아래 `credential_env()` — 자격증명을 기동 시
    한 번 읽어 메모리에 들고 쓰는 쪽이고, 그게 되면 헬퍼가 아예 뜨지 않는다
    (−607ms/왕복). 이 함수가 받는 것은 그 조회가 **안 되는** 환경이다:

    * `credential.useHttpPath=true` 처럼 호스트만으로는 조회가 안 되는 설정 —
      `git credential fill` 은 빈손으로 오지만 git 자신의 push 중 조회(경로까지
      포함)는 성공한다. 이 단계가 그 사람의 유일한 경로다.
    * Basic 헤더를 받지 않는 서버 (사내 게이트웨이·Negotiate 강제).

    그 환경에서는 아래 표가 그대로 유효하다 — 실측 (Windows 11 · git 2.51 ·
    GitHub private repo · `push --dry-run` 5회, 중앙값)::

        사용자 설정 그대로 (system: manager = GCM)      1552 ms
        wincred 만                                      1220 ms
        wincred → manager (사슬)                        1519 ms
        헬퍼 없음                                        540 ms  ← **인증 실패**

    `GIT_TRACE` 로 본 헬퍼 기동 횟수: GCM 은 한 호출에 **2번**(`get`·`store`),
    그리고 한 번이 `sh.exe` → `git.exe` → `.NET` 3프로세스다. OS 저장소 헬퍼로
    바꾸면 그 둘이 싸진다(−330ms). 헬퍼를 **아예 비우면** 제일 빠르지만
    자격증명을 못 얻어 rc 128 로 깨진다 — 그래서 비우지 않는다.

    ⚠️ **`credential.helper` 는 다중값이다.** `-c credential.helper=X` 만 주면
    기존 사슬(system 의 `manager`)에 *추가*되고, git 은 성공 후 `store` 를
    **사슬 전원**에게 보내므로 GCM 비용이 그대로 돌아온다(위 표 3행 = 1519ms,
    트레이스에 `manager store` 가 남는다). 그래서 **빈 값으로 목록을 초기화한
    뒤** 하나만 지정한다.

    ⚠️ 그 헬퍼가 그 머신에 없을 수 있다 — 없으면 **아무것도 얹지 않고 사용자
    설정 그대로** 쓴다(느린 것이 깨지는 것보다 낫다). 그 사실은 로그에 남긴다.
    사슬을 뒤에 덧붙이지 않는 대신, 이 헬퍼로 인증이 **실패하면** 그 호출을
    사용자 설정으로 한 번 더 시도한다 (`Git.run`) — 자격증명이 GCM 전용
    저장소에만 있는 사람도 깨지지 않는다.
    """
    forced = os.environ.get(HELPER_ENV)
    if forced is not None and forced.strip().lower() in _OFF:
        return ()
    name = (forced or "").strip() or _os_helper()
    key = f"{git_path}\x00{name or ''}"
    if not name:
        # 모르는 OS — 사용자 설정을 쓴다. 로그는 **한 번만** 남긴다 (호출마다
        # 남기면 왕복마다 같은 줄이 쌓인다).
        with _cred_lock:
            first = key not in _cred_cache
            _cred_cache[key] = ()
        if first:
            log.info(
                "gitwire: 이 OS 의 기본 자격증명 저장소를 모른다 — 사용자 설정을 쓴다"
            )
        return ()
    with _cred_lock:
        if key in _cred_disabled:
            return ()
        hit = _cred_cache.get(key)
        if hit is not None:
            return hit
    if _helper_installed(name, git_path):
        args = ("-c", "credential.helper=", "-c", f"credential.helper={name}")
    else:
        args = ()
        log.warning(
            "gitwire: 자격증명 헬퍼 %r 가 이 머신에 없다 — "
            "사용자 git 설정을 그대로 쓴다 (느려질 수 있다)",
            name,
        )
    with _cred_lock:
        _cred_cache[key] = args
    return args


def reset_credential_state() -> None:
    """탐지·접힘 기록을 비운다. **프로세스 안의 캐시를 지우는 것뿐이다.**

    한 프로세스 안에서 환경을 바꿔 가며 확인하는 쪽(테스트·진단)이 쓴다.
    """
    with _cred_lock:
        _cred_cache.clear()
        _cred_disabled.clear()
    with _held_lock:
        _held_cache.clear()
        _held_disabled.clear()


def disable_credential_config(git_path: str, reason: str) -> None:
    """지정한 헬퍼로 인증이 안 됐다 — 이 프로세스에서는 접고 사용자 설정을 쓴다.

    조용히 열화하지 않는다: 접는 이유를 남긴다.
    """
    forced = os.environ.get(HELPER_ENV)
    name = (forced or "").strip() or _os_helper() or ""
    key = f"{git_path}\x00{name}"
    with _cred_lock:
        if key in _cred_disabled:
            return
        _cred_disabled.add(key)
        _cred_cache.pop(key, None)
    log.warning(
        "gitwire: 자격증명 헬퍼 %r 로 인증하지 못했다 (%s) — "
        "사용자 git 설정으로 되돌려 다시 시도한다",
        name,
        reason,
    )


# ------------------------------------------------ 기동 시 1회 조회 (메모리 보유)
#
# ⭐ **전송 비용의 가장 큰 항목이 여기였다.**
#
# 위 `credential_config()` 는 *어느 헬퍼를 쓸까*를 고르는 일이다. 그런데 어느
# 헬퍼를 고르든 **왕복마다 헬퍼 프로세스가 두 번 뜬다** (`get` · `store`). 실측
# (Windows 11 · git 2.51 · GitHub private repo · `push --dry-run` 3회 중앙값,
# `GIT_TRACE` 로 `git-credential-*` exec 계수)::
#
#     사용자 설정 그대로 (system: manager)   1532 ms   헬퍼 exec 2회
#     wincred 만 (credential_config 의 값)   1258 ms   헬퍼 exec 2회
#     아래 방식 (메모리 보유 + env)            651 ms   헬퍼 exec 0회
#
# 즉 헬퍼를 바꿔 깎을 수 있는 것은 이미 다 깎았고, **남은 ~607ms 는 헬퍼 기동
# 그 자체**다. 그것을 0 으로 만드는 길은 하나뿐이다 — 자격증명을 **한 번** 받아
# 들고 있다가 직접 먹이는 것. `git credential fill` 은 이 머신에서 422~716ms
# 이고, 그 비용을 **기동 시 딱 한 번** 낸다.
#
# ⚠️ **새 토큰을 만들지 않는다.** 사용자가 이미 저장해 둔 자격증명을 그 사람의
# 헬퍼 사슬에게 정상적으로 물어보는 것뿐이다. 저장소에 쓰지도 않는다.
#
# ⚠️ **어떻게 먹이나 — argv 는 절대 아니다.** 이 OS 에서 남의 프로세스 명령줄은
# 그대로 읽힌다(`Win32_Process.CommandLine`). URL 에 박으면 `.git/config` 와
# 에러 메시지에까지 남는다. 그래서 git 이 제공하는 **설정을 환경변수로 주는
# 규약**(`GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_n`/`GIT_CONFIG_VALUE_n`)을 쓴다:
#
#     http.<원격 URL>.extraHeader = Authorization: Basic <base64>
#     credential.helper           = (빈 값 — 목록을 비운다)
#
# 헤더가 붙으면 서버가 401 을 주지 않으므로 git 은 자격증명을 **물어볼 이유가
# 없다** (그래서 헬퍼 exec 0회이고, 401→재시도 왕복도 사라져 −607ms 가 된다).
# 스코프를 `http.<그 원격 URL>.` 로 좁히므로 리다이렉트로 다른 호스트에 갔을 때
# 그 헤더가 따라가지 않는다.
#
# ⚠️ **안 되면 떨어진다.** 조회가 실패하거나(저장된 것이 없다·
# `credential.useHttpPath` 처럼 호스트만으로는 못 찾는 설정), http(s) 가 아닌
# 원격이거나(ssh·로컬 경로 — 애초에 이 얘기가 아니다), 그 헤더로 인증이 거부되면
# `credential_config()` → 사용자 설정 순으로 내려간다. 느린 것이 깨지는 것보다
# 낫고, 어느 단계로 내려갔는지 **로그에 남는다.**

#: git 설정을 argv 가 아니라 환경변수로 주는 규약의 개수 변수.
CONFIG_COUNT_ENV = "GIT_CONFIG_COUNT"

#: 기동 시 1회 조회를 끈다 (`off`/`0`/`no`/`none`/`false`/`""`). 진단용.
HELD_ENV = "GITWIRE_HELD_CREDENTIAL"

#: http(s) 원격에서 (프로토콜, 호스트, 경로) 를 뽑는다. `user@` 는 버린다 —
#: 스코프·조회 키에 자격증명 조각을 섞지 않는다.
_HTTP_RE = re.compile(r"\A(https?)://(?:[^/@]*@)?([^/?#]+)([^?#]*)", re.I)

_held_lock = threading.Lock()
_held_cache: dict[str, "HeldCredential | None"] = {}
#: 이 헤더로 인증이 거부된 원격. 이 프로세스에서는 다시 시도하지 않는다.
_held_disabled: set[str] = set()


@dataclass(frozen=True)
class HeldCredential:
    """원격 하나의 자격증명 — **이 프로세스 메모리에만** 있다. 디스크에 안 쓴다."""

    scope: str
    """`http.<여기>.extraHeader` 의 스코프 (그 원격 URL)."""
    header: str
    """`Authorization: …` 값. **비밀을 품는다** — 로그·repr 에 싣지 않는다."""
    secrets: tuple[str, ...] = field(default=())
    """`redact()` 에 넘길 값들 (비밀번호 원문 + base64 블롭 둘 다)."""

    def env(self) -> dict[str, str]:
        """git 에 먹일 환경변수. 호출자는 이것을 `env` 로만 넘긴다."""
        return _config_env((
            ("credential.helper", ""),
            (f"http.{self.scope}.extraHeader", self.header),
        ))

    def __repr__(self) -> str:
        return f"HeldCredential(scope={self.scope!r}, header=***)"

    __str__ = __repr__


def _config_env(pairs: Sequence[tuple[str, str]]) -> dict[str, str]:
    """`(키, 값)` 들을 git 의 `GIT_CONFIG_*` 환경변수로 만든다.

    ⚠️ 이미 환경에 `GIT_CONFIG_COUNT` 가 있으면 그 **뒤에 이어 붙인다.** 0번부터
    덮어쓰면 호출자가 그 규약으로 넘긴 설정을 조용히 지운다.
    """
    try:
        base = int(os.environ.get(CONFIG_COUNT_ENV, "") or "0")
    except ValueError:
        base = 0
    base = max(0, base)
    env = {CONFIG_COUNT_ENV: str(base + len(pairs))}
    for i, (key, value) in enumerate(pairs, start=base):
        env[f"GIT_CONFIG_KEY_{i}"] = key
        env[f"GIT_CONFIG_VALUE_{i}"] = value
    return env


def _remote_scope(url: str) -> tuple[str, str, str] | None:
    """http(s) 원격의 (프로토콜, 호스트, 스코프 URL). 아니면 None."""
    m = _HTTP_RE.match((url or "").strip())
    if not m:
        return None
    protocol, host, path = m.group(1).lower(), m.group(2), m.group(3) or ""
    return protocol, host, f"{protocol}://{host}{path}"


def _fill(git_path: str, cwd: Path | None, protocol: str, host: str) -> dict[str, str]:
    """`git credential fill` 한 번. 못 얻으면 빈 dict.

    ⚠️ `GitRunner` 를 타지 않는다 — stdin 을 줘야 하고, 이 조회는 러너를 갈아끼운
    테스트에서 **일어나서는 안 되는** 일이다 (네트워크·머신 설정 의존).
    호출 자체가 http(s) 원격에서만 일어나므로 테스트의 로컬 경로 원격은 여기에
    도달하지 않는다.
    """
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GCM_INTERACTIVE"] = "never"
    try:
        res = subprocess.run(
            [git_path, "credential", "fill"],
            input=f"protocol={protocol}\nhost={host}\n\n".encode(),
            cwd=str(cwd) if cwd else None,
            env=env,
            capture_output=True,
            timeout=60,
            creationflags=creation_flags(),
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if res.returncode != 0:
        return {}
    out: dict[str, str] = {}
    for line in res.stdout.decode("utf-8", "replace").splitlines():
        key, sep, value = line.partition("=")
        if sep:
            out[key.strip()] = value
    return out


def credential_env(
    git_path: str = "git", url: str = "", cwd: Path | None = None
) -> HeldCredential | None:
    """이 원격에 쓸 **메모리 보유 자격증명** (없으면 None).

    원격 **호스트별로 한 번만** 조회한다 — 같은 호스트의 다른 채널·다른 왕복은
    그 결과를 그대로 쓴다. 조회 결과가 "없음"인 것도 캐시한다(호출마다 768ms 를
    다시 내지 않는다).
    """
    forced = os.environ.get(HELD_ENV)
    if forced is not None and forced.strip().lower() in _OFF:
        return None
    scoped = _remote_scope(url)
    if scoped is None:
        return None                       # ssh·로컬 경로 — 이 얘기가 아니다
    protocol, host, scope = scoped
    key = f"{git_path}\x00{protocol}://{host}"
    with _held_lock:
        if key in _held_disabled:
            return None
        if key in _held_cache:
            hit = _held_cache[key]
            # 같은 호스트의 다른 경로면 스코프만 갈아 쓴다 (조회는 재사용).
            if hit is None or hit.scope == scope:
                return hit
            return HeldCredential(scope, hit.header, hit.secrets)
    filled = _fill(git_path, cwd, protocol, host)
    user, password = filled.get("username", ""), filled.get("password", "")
    held: HeldCredential | None = None
    if password:
        blob = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
        held = HeldCredential(scope, f"Authorization: Basic {blob}", (password, blob))
    with _held_lock:
        first = key not in _held_cache
        _held_cache[key] = held
    if first:
        if held is None:
            log.info(
                "gitwire: %s://%s 의 저장된 자격증명을 얻지 못했다 — "
                "git 의 자격증명 헬퍼를 그대로 쓴다 (왕복마다 헬퍼가 뜬다)",
                protocol,
                host,
            )
        else:
            log.info(
                "gitwire: %s://%s 자격증명을 한 번 조회해 메모리에 들었다 — "
                "이후 왕복에서 헬퍼를 띄우지 않는다",
                protocol,
                host,
            )
    return held


def disable_held_credential(git_path: str, url: str, reason: str) -> None:
    """들고 있던 자격증명으로 인증이 거부됐다 — 접고 헬퍼 경로로 내려간다."""
    scoped = _remote_scope(url)
    if scoped is None:
        return
    protocol, host, _ = scoped
    key = f"{git_path}\x00{protocol}://{host}"
    with _held_lock:
        if key in _held_disabled:
            return
        _held_disabled.add(key)
        _held_cache.pop(key, None)
    log.warning(
        "gitwire: 메모리에 든 %s://%s 자격증명으로 인증하지 못했다 (%s) — "
        "git 의 자격증명 헬퍼로 되돌려 다시 시도한다",
        protocol,
        host,
        reason,
    )


def creation_flags() -> int:
    """git 을 띄울 때 줄 ``creationflags``. Windows 밖에서는 ``0``.

    모듈 도크의 그 판정 하나가 여기 전부다. 상수가 아니라 함수인 이유는 (1)
    ``os.name`` 을 **부를 때** 보므로 테스트가 다른 OS 를 흉내 낼 수 있고,
    (2) 이 값이 어디서 오는지 한 곳으로 좁혀지기 때문이다.

    ⭐ 값은 ``DETACHED_PROCESS`` 다 (``CREATE_NO_WINDOW`` 가 아니다). 둘 다 창을
    없애지만 값이 다르다 — ``CREATE_NO_WINDOW`` 는 자식에게 **새 콘솔을 만들어
    주되 창만 숨기는** 것이라 호출마다 ``conhost.exe`` 가 하나 더 뜬다.
    ``DETACHED_PROCESS`` 는 콘솔을 **아예 주지 않는다** — 출력은 어차피 파이프로
    받으므로 잃는 것이 없고 conhost 가 뜨지 않는다. 이 머신 실측
    (subprocess.run ``cmd /c exit``, n=12 중앙값): 플래그 0 = 36ms ·
    CREATE_NO_WINDOW = **73ms** · DETACHED_PROCESS = **33ms**. git 한 호출당
    약 −40ms, 전송 한 번(호출 3~4개)에 −120~160ms.

    자격증명 헬퍼 등 git 의 자식도 콘솔 없이 뜬다 — 우리는 ``GIT_TERMINAL_PROMPT=0``
    ``GCM_INTERACTIVE=never`` 로 어떤 프롬프트도 막고 있으므로 콘솔이 필요한
    경로가 없다.

    ⚠️ "콘솔 없는 프로세스가 콘솔 앱을 부르면 손자가 창을 띄운다"는 일반론이
    있다 — git 에는 해당하지 않는다는 것을 **실측으로** 확인했다 (2026-09-21):
    콘솔 없는 ``pythonw.exe`` 에서 이 플래그로 ``git push``(로컬 receive-pack) ·
    ``fetch``(upload-pack) · ``ls-remote https://``(remote-https) 를 돌리며 5ms
    마다 최상위 콘솔 창을 열거했다 — Git for Windows 는 자기 자식을 스스로 숨긴
    콘솔로 띄우므로(창 제목 ``invisible cygwin console``, 전부 비가시) **보이는 창이
    0개**였고, 플래그를 아예 안 준 대조군에서만 터미널 창이 생겼다.

    ``getattr`` 로 읽는다 — 이 이름은 Windows 의 ``subprocess`` 에만 있다. 폴백은
    **Win32 상수 그대로**(0x00000008) 다: 0 으로 두면 POSIX 에서 돌린 테스트가 이
    플래그를 단언할 수 없어 회귀를 놓친다. 이 값은 위 ``os.name`` 분기 안에서만
    쓰이므로 POSIX 실행에는 새지 않는다.
    """
    if os.name != "nt":
        return 0
    return getattr(subprocess, "DETACHED_PROCESS", 0x00000008)


def redact(text: str, secrets: Iterable[str]) -> str:
    """문자열에서 비밀값을 지운다. 빈 값·짧은 값은 무시(과도한 치환 방지)."""
    if not text:
        return text
    for s in secrets:
        if s and len(s) >= 4:
            text = text.replace(s, "***")
    # URL 에 박힌 자격증명(https://user:token@host)도 방어적으로 지운다.
    return _URL_CRED_RE.sub("***:***@", text)


@dataclass(frozen=True)
class GitResult:
    returncode: int
    stdout: str
    stderr: str


class GitRunner(Protocol):
    """git 실행기 인터페이스. 테스트에서 통째로 대체할 수 있다."""

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> GitResult:  # pragma: no cover - 프로토콜 선언
        ...


_resolve_lock = threading.Lock()
_resolved: dict[str, str] = {}

#: Git for Windows 설치 루트 아래에서 **진짜** git 바이너리가 있는 자리 (래퍼가 아닌 것).
_GFW_REAL_GIT = ("mingw64/bin/git.exe", "mingw32/bin/git.exe")
#: 래퍼는 수십 KB, 진짜 git.exe 는 수 MB 다. 이 아래면 래퍼로 본다.
_REAL_GIT_MIN_BYTES = 1_000_000


def resolve_git_path(git_path: str = "git") -> str:
    r"""Windows 에서 PATH 의 `git` 이 **래퍼**면 진짜 바이너리 경로로 바꾼다. 다른 OS 는 그대로.

    ⭐ 왜 — Git for Windows 의 PATH 항목은 `C:\Program Files\Git\cmd` 이고 거기
    있는 `git.exe` 는 환경을 맞춘 뒤 `mingw64\bin\git.exe` 를 **자식 프로세스로**
    띄우는 래퍼다. 그래서 우리가 `git` 이라고 부르는 모든 호출이 프로세스 **2개**다.
    이 머신(EDR 이 도는 Windows 11) 실측(subprocess.run 기준, n=12 중앙값)::

        cmd\git.exe rev-parse HEAD            91~132 ms
        mingw64\bin\git.exe rev-parse HEAD    56~ 61 ms      ← 호출당 약 −40~70 ms

    전송 한 번이 git 을 3~4번 부르므로 이것만으로 −150~250 ms 다.

    판정은 **파일시스템만** 본다(프로세스 0개): `shutil.which` 로 찾은 파일이
    `cmd\` 또는 `bin\` 아래의 작은 파일이면 설치 루트의 `mingw64\bin\git.exe`
    (또는 `mingw32`)를 쓴다. 그 자리에 **수 MB 짜리 파일이 실제로 있을 때만**
    바꾼다 — 레이아웃을 모르면 손대지 않고 원래 값을 그대로 쓴다 (느릴 뿐 틀리지
    않는다). 결과는 프로세스 안에서 한 번만 계산한다.
    """
    if os.name != "nt":
        return git_path
    with _resolve_lock:
        hit = _resolved.get(git_path)
    if hit is not None:
        return hit
    result = git_path
    found = git_path if os.path.isabs(git_path) else shutil.which(git_path)
    if found:
        result = found
        try:
            p = Path(found)
            small = p.stat().st_size < _REAL_GIT_MIN_BYTES
            if p.name.lower() == "git.exe" and p.parent.name.lower() in ("cmd", "bin") and small:
                root = p.parent.parent
                for rel in _GFW_REAL_GIT:
                    cand = root / rel
                    if cand.is_file() and cand.stat().st_size >= _REAL_GIT_MIN_BYTES:
                        result = str(cand)
                        break
        except OSError:
            result = found
    with _resolve_lock:
        first = git_path not in _resolved
        _resolved[git_path] = result
    if first and result != git_path:
        log.info("gitwire: git 바이너리를 직접 부른다 — %s (PATH 의 %r 는 래퍼다)", result, git_path)
    return result


class SubprocessGitRunner:
    """실제 git 바이너리를 subprocess 로 부르는 기본 구현.

    `git_path` 는 **첫 호출 때** `resolve_git_path` 로 확정한다 (생성만으로
    파일시스템을 보지 않는다 — 러너는 테스트·CLI 에서 자주 만들어진다).
    """

    def __init__(self, git_path: str = "git") -> None:
        self._git_path = git_path

    @property
    def git_path(self) -> str:
        return resolve_git_path(self._git_path)

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> GitResult:
        full_env = dict(os.environ)
        if env:
            full_env.update(env)
        # 어떤 경우에도 대화형 프롬프트로 빠지지 않는다.
        full_env.setdefault("GIT_TERMINAL_PROMPT", "0")
        full_env.setdefault("GCM_INTERACTIVE", "never")
        proc = subprocess.run(
            [self.git_path, *args],
            cwd=str(cwd) if cwd else None,
            env=full_env,
            capture_output=True,
            timeout=timeout,
            # Windows 에서 콘솔 창이 뜨지 않게 (모듈 도크). 다른 OS 에서는 0 이라
            # 아무 효과가 없다 — subprocess 가 그 값을 무시한다.
            creationflags=creation_flags(),
        )
        return GitResult(
            returncode=proc.returncode,
            stdout=proc.stdout.decode("utf-8", "replace"),
            stderr=proc.stderr.decode("utf-8", "replace"),
        )


_AUTH_MARKERS = (
    "authentication failed",
    "could not read username",
    "could not read password",
    "invalid username or password",
    "terminal prompts disabled",
    "permission denied",
    "403 forbidden",
)
_REJECT_MARKERS = (
    "non-fast-forward",
    "fetch first",
    "rejected",
    "cannot lock ref",
)


def _is_auth_failure(res: GitResult) -> bool:
    low = (res.stderr + res.stdout).lower()
    return any(m in low for m in _AUTH_MARKERS)


def runner_git_path(runner: object) -> str:
    """러너 사슬(데코레이터 포함)에서 git 바이너리 경로를 찾아낸다."""
    for _ in range(8):
        path = getattr(runner, "git_path", None)
        if isinstance(path, str) and path:
            return path
        runner = getattr(runner, "inner", None)
        if runner is None:
            break
    return "git"


def subcommand_of(args: Sequence[str]) -> str | None:
    """`-c a=b` 같은 앞선 옵션을 건너뛰고 서브커맨드 이름을 돌려준다."""
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        if arg == "-c" or arg == "--exec-path":
            skip = True
            continue
        if arg.startswith("-"):
            continue
        return arg
    return None


@dataclass(frozen=True)
class _Tier:
    """자격증명 경로 한 단계 — 무엇을 얹고, 실패하면 무엇을 접나.

    세 단계가 이 한 모양으로 표현된다 (`Git._credential_tiers`): 메모리 보유
    자격증명(env) → OS 저장소 헬퍼(`-c`) → 사용자 설정(아무것도 안 얹음).
    """

    prefix: tuple[str, ...]
    """git 인자 앞에 붙일 `-c …` 들."""
    env: Mapping[str, str]
    """이 호출에만 덧씌울 환경변수 (자격증명은 **여기로만** 간다)."""
    secrets: tuple[str, ...]
    """`redact()` 대상으로 등록할 값들."""
    disable: Callable[[str], None] | None
    """인증이 거부됐을 때 이 단계를 접는 함수 (마지막 단계는 None)."""


class Git:
    """특정 작업 디렉토리에 묶인 git 실행 헬퍼.

    `secrets` 로 준 값은 반환값·예외 어디에도 나타나지 않는다.
    """

    def __init__(
        self,
        runner: GitRunner,
        cwd: Path,
        *,
        env: Mapping[str, str] | None = None,
        secrets: Sequence[str] = (),
        timeout: float | None = 120.0,
        cheap_credentials: bool = True,
        remote_url: str | None = None,
    ) -> None:
        self.runner = runner
        self.cwd = Path(cwd)
        self.env = dict(env or {})
        self.secrets = tuple(secrets)
        self.timeout = timeout
        # 네트워크 호출의 원격 URL. 있으면 그 호스트의 **저장된** 자격증명을
        # 기동 시 1회 조회해 메모리로 들고 쓴다 (`credential_env`). None 이면
        # 그 경로를 쓰지 않는다 — 호출자가 자기 자격증명을 직접 주는 경우다.
        self.remote_url = remote_url
        # False = 자격증명 설정을 우리가 얹지 않는다 (호출자가 스스로 관리한다 —
        # 예: `Channel(credential_helpers=…)` 로 사슬을 직접 짠 경우. 그때 우리가
        # 목록을 초기화해 버리면 그 설정이 무효가 된다).
        self.cheap_credentials = cheap_credentials

    def with_cwd(self, cwd: Path) -> "Git":
        return Git(
            self.runner,
            cwd,
            env=self.env,
            secrets=self.secrets,
            timeout=self.timeout,
            cheap_credentials=self.cheap_credentials,
            remote_url=self.remote_url,
        )

    def _credential_tiers(self, args: Sequence[str]) -> list["_Tier"]:
        """이 호출에 시도할 자격증명 경로들 — **싼 쪽부터, 마지막은 사용자 설정.**

        네트워크 호출이 아니면 한 칸(아무것도 얹지 않음)이다. 로컬 git 호출의
        동작을 한 글자도 바꾸지 않기 위해서다.

        앞 단계가 **인증 실패**로 끝나면 다음 단계로 내려간다(`run`). 그것이
        "못 얻으면 사용자 설정 그대로 떨어진다"의 구현이고, 내려갈 때마다 로그가
        남는다 — 조용한 열화 금지.
        """
        plain = _Tier((), {}, (), None)
        if not self.cheap_credentials or subcommand_of(args) not in NETWORK_CMDS:
            return [plain]
        git_path = runner_git_path(self.runner)
        tiers: list[_Tier] = []
        if self.remote_url:
            held = credential_env(git_path, self.remote_url, self.cwd)
            if held is not None:
                url = self.remote_url
                tiers.append(_Tier(
                    (), held.env(), held.secrets,
                    lambda why: disable_held_credential(git_path, url, why),
                ))
        cfg = credential_config(git_path)
        if cfg:
            tiers.append(_Tier(
                cfg, {}, (),
                lambda why: disable_credential_config(git_path, why),
            ))
        tiers.append(plain)
        return tiers

    def _invoke(
        self,
        prefix: Sequence[str],
        args: Sequence[str],
        timeout: float | None,
        env: Mapping[str, str] | None = None,
    ) -> GitResult:
        res = self.runner.run(
            [*prefix, *BASE_CONFIG, *args],
            cwd=self.cwd,
            env={**self.env, **(env or {})},
            timeout=timeout,
        )
        return GitResult(
            res.returncode,
            redact(res.stdout, self.secrets),
            redact(res.stderr, self.secrets),
        )

    def run(
        self, *args: str, check: bool = True, timeout: float | None = None
    ) -> GitResult:
        timeout = timeout or self.timeout
        tiers = self._credential_tiers(args)
        # ⭐ 들고 있는 비밀은 **레닥션 대상에 등록한다.** `_invoke` 와 아래 예외
        # 경로가 전부 `self.secrets` 로 가리므로, 만약 git 이 그 값을 되뱉어도
        # stdout·stderr·예외 메시지에 나타나지 않는다.
        extra = tuple(s for tier in tiers for s in tier.secrets)
        if extra:
            self.secrets = tuple(dict.fromkeys((*self.secrets, *extra)))
        res = self._invoke(tiers[0].prefix, args, timeout, tiers[0].env)
        for i in range(1, len(tiers)):
            if res.returncode == 0 or not _is_auth_failure(res):
                break
            # ⭐ 이 단계로는 인증이 안 된다. 조용히 깨뜨리지 않는다 — 접고(이
            # 프로세스에서는 다음 호출부터 바로 다음 단계로 간다) 한 단계 내려가
            # 다시 시도한다.
            disable = tiers[i - 1].disable
            if disable is not None:
                disable(res.stderr.strip()[:200] or "인증 실패")
            res = self._invoke(tiers[i].prefix, args, timeout, tiers[i].env)
        if check and res.returncode != 0:
            low = (res.stderr + res.stdout).lower()
            safe_args = [redact(a, self.secrets) for a in args]
            if any(m in low for m in _AUTH_MARKERS):
                raise AuthError(
                    "git 인증 실패 또는 자격증명 없음 "
                    f"(git {' '.join(safe_args[:2])}): {res.stderr.strip()}"
                )
            if args and args[0] == "push" and any(m in low for m in _REJECT_MARKERS):
                raise PushRejected(safe_args, res.returncode, res.stderr)
            raise GitError(safe_args, res.returncode, res.stderr)
        return res

    def out(self, *args: str, check: bool = True) -> str:
        """stdout 을 strip 해서 돌려준다."""
        return self.run(*args, check=check).stdout.strip()

    def ok(self, *args: str) -> bool:
        """종료코드만 보는 호출 (조건 판정용)."""
        return self.run(*args, check=False).returncode == 0
