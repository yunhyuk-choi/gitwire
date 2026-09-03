"""git 호출 계층 — 주입 가능(injectable)하게 만든 얇은 subprocess 래퍼.

설계 의도
---------
* 네트워크·git 접근을 전부 이 모듈 한 곳으로 좁힌다. 테스트는 GitRunner 를
  갈아끼우기만 하면 네트워크 없이 돌아간다.
* 자격증명 값은 **stdout/stderr/예외 메시지 어디에도 실리지 않는다**.
  redact() 가 모든 출력 경로에서 강제된다.
* 대화형 프롬프트를 원천 차단한다 (GIT_TERMINAL_PROMPT=0). 헤드리스에서
  git 이 자격증명을 물으며 멈추는 것이 최악의 실패 모드다.
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
