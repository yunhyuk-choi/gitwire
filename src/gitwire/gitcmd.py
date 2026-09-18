"""git 호출 계층 — 주입 가능(injectable)하게 만든 얇은 subprocess 래퍼.

설계 의도
---------
* 네트워크·git 접근을 전부 이 모듈 한 곳으로 좁힌다. 테스트는 GitRunner 를
  갈아끼우기만 하면 네트워크 없이 돌아간다.
* 자격증명 값은 **stdout/stderr/예외 메시지 어디에도 실리지 않는다**.
  redact() 가 모든 출력 경로에서 강제된다.
* 대화형 프롬프트를 원천 차단한다 (GIT_TERMINAL_PROMPT=0). 헤드리스에서
  git 이 자격증명을 물으며 멈추는 것이 최악의 실패 모드다.
* **자격증명 조회를 싸게 만든다** — 네트워크 호출에만 OS 기본 저장소 헬퍼를
  얹는다 (`credential_config()`. 실측 −330ms/왕복). 사용자의 전역 설정은
  읽지도 고치지도 않고, 그 헬퍼가 없거나 인증이 안 되면 **사용자 설정으로
  되돌린다**(로그를 남긴다 — 조용한 열화 금지).
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

그래서 **``CREATE_NO_WINDOW``** 를 건다 — 자식은 콘솔을 갖되 **창이 없는** 콘솔을
갖는다. 창이 없으므로 (1) 깜빡이지 않고 (2) 사용자가 그 창을 닫아 git 을 죽일 수도
없다. 콘솔 자체는 있으므로 git 이 다시 부르는 손자(자격증명 헬퍼 등)도 그 창 없는
콘솔을 물려받는다 — 한 곳만 고쳐도 트리 전체가 조용해진다.

⚠️ ``DETACHED_PROCESS`` 는 이 문제의 답이 아니다. 그건 "콘솔을 아예 주지 않는다"는
뜻이라 **손자가 다시 자기 콘솔을 창과 함께 할당한다** — 문제를 한 세대 미룬다.

캡처·타임아웃·프롬프트 억제는 영향받지 않는다. ``capture_output`` 은 파이프이고
파이프는 콘솔과 무관하다. 프롬프트는 원래 ``GIT_TERMINAL_PROMPT=0`` 이 막고, 창
없는 콘솔에서는 사람이 볼 수 있는 터미널 대화가 애초에 불가능하다 — 억제가 **더**
확실해진다. Windows 밖(macOS·Linux)에는 콘솔이라는 개념 자체가 없어 플래그가
``0`` 이다: 동작이 한 글자도 바뀌지 않는다.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Protocol, Sequence

from .errors import AuthError, GitError, PushRejected

log = logging.getLogger("gitwire")

_URL_CRED_RE = re.compile(r"(?<=://)[^/\s@]+:[^/\s@]+@")

#: 커밋·재작성 시 항상 붙이는 설정. 전역 git config 에 의존하지 않는다.
BASE_CONFIG: tuple[str, ...] = (
    "-c", "core.autocrlf=false",
    "-c", "core.safecrlf=false",
    "-c", "commit.gpgsign=false",
    "-c", "gc.auto=0",
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

    왜 — 실측 (Windows 11 · git 2.51 · GitHub private repo · `push --dry-run`
    5회, 중앙값)::

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


def creation_flags() -> int:
    """git 을 띄울 때 줄 ``creationflags``. Windows 밖에서는 ``0``.

    모듈 도크의 그 판정 하나가 여기 전부다. 상수가 아니라 함수인 이유는 (1)
    ``os.name`` 을 **부를 때** 보므로 테스트가 다른 OS 를 흉내 낼 수 있고,
    (2) 이 값이 어디서 오는지 한 곳으로 좁혀지기 때문이다.

    ``getattr`` 로 읽는다 — ``CREATE_NO_WINDOW`` 는 Windows 의 ``subprocess``
    에만 있는 이름이다. 폴백은 **Win32 상수 그대로**(0x08000000) 다: 0 으로
    두면 POSIX 에서 돌린 테스트가 이 플래그를 단언할 수 없어 회귀를 놓친다.
    이 값은 위 ``os.name`` 분기 안에서만 쓰이므로 POSIX 실행에는 새지 않는다.
    """
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)


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


class SubprocessGitRunner:
    """실제 git 바이너리를 subprocess 로 부르는 기본 구현."""

    def __init__(self, git_path: str = "git") -> None:
        self.git_path = git_path

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
    ) -> None:
        self.runner = runner
        self.cwd = Path(cwd)
        self.env = dict(env or {})
        self.secrets = tuple(secrets)
        self.timeout = timeout
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
        )

    def _credential_args(self, args: Sequence[str]) -> tuple[str, ...]:
        """이 호출에 얹을 자격증명 설정 (네트워크 호출에만 · 실패하면 빈 튜플)."""
        if not self.cheap_credentials:
            return ()
        if subcommand_of(args) not in NETWORK_CMDS:
            return ()
        return credential_config(runner_git_path(self.runner))

    def _invoke(
        self, prefix: Sequence[str], args: Sequence[str], timeout: float | None
    ) -> GitResult:
        res = self.runner.run(
            [*prefix, *BASE_CONFIG, *args],
            cwd=self.cwd,
            env=self.env,
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
        cred = self._credential_args(args)
        res = self._invoke(cred, args, timeout)
        if cred and res.returncode != 0 and _is_auth_failure(res):
            # ⭐ 우리가 지정한 저장소에 자격증명이 없었다 (예: GCM 전용 저장소만
            # 쓰는 사람). 조용히 깨뜨리지 않는다 — 접고, 사용자 설정 그대로 한 번
            # 더 시도한다. 이 프로세스에서는 다음 호출부터 바로 사용자 설정을 쓴다.
            disable_credential_config(
                runner_git_path(self.runner), res.stderr.strip()[:200] or "인증 실패"
            )
            res = self._invoke((), args, timeout)
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
