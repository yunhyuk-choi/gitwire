"""git 호출 계층 — 주입 가능(injectable)하게 만든 얇은 subprocess 래퍼.

설계 의도
---------
* 네트워크·git 접근을 전부 이 모듈 한 곳으로 좁힌다. 테스트는 GitRunner 를
  갈아끼우기만 하면 네트워크 없이 돌아간다.
* 자격증명 값은 **stdout/stderr/예외 메시지 어디에도 실리지 않는다**.
  redact() 가 모든 출력 경로에서 강제된다.
* 대화형 프롬프트를 원천 차단한다 (GIT_TERMINAL_PROMPT=0). 헤드리스에서
  git 이 자격증명을 물으며 멈추는 것이 최악의 실패 모드다.
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

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Protocol, Sequence

from .errors import AuthError, GitError, PushRejected

_URL_CRED_RE = re.compile(r"(?<=://)[^/\s@]+:[^/\s@]+@")

#: 커밋·재작성 시 항상 붙이는 설정. 전역 git config 에 의존하지 않는다.
BASE_CONFIG: tuple[str, ...] = (
    "-c", "core.autocrlf=false",
    "-c", "core.safecrlf=false",
    "-c", "commit.gpgsign=false",
    "-c", "gc.auto=0",
    "-c", "advice.detachedHead=false",
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
    ) -> None:
        self.runner = runner
        self.cwd = Path(cwd)
        self.env = dict(env or {})
        self.secrets = tuple(secrets)
        self.timeout = timeout

    def with_cwd(self, cwd: Path) -> "Git":
        return Git(
            self.runner, cwd, env=self.env, secrets=self.secrets, timeout=self.timeout
        )

    def run(
        self, *args: str, check: bool = True, timeout: float | None = None
    ) -> GitResult:
        res = self.runner.run(
            [*BASE_CONFIG, *args],
            cwd=self.cwd,
            env=self.env,
            timeout=timeout or self.timeout,
        )
        res = GitResult(
            res.returncode,
            redact(res.stdout, self.secrets),
            redact(res.stderr, self.secrets),
        )
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
