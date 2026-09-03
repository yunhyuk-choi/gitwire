"""채널별 자격증명.

원칙
----
* **채널마다 다른 토큰**을 쓸 수 있다. 전역 하나로 두지 않는다 (다른 조직
  레포에 붙는 순간 막힌다).
* 값은 **로컬 프로세스 안에만** 있는다. gitwire 는 토큰을 디스크에 저장하지
  않는다. 저장 책임은 호출자(환경변수·OS 키체인·시크릿 매니저)에게 있다.
* URL 에 토큰을 박지 않는다 (`.git/config` 에 평문으로 남는다). 대신
  GIT_ASKPASS 헬퍼 + 환경변수로 전달한다 — 프로세스 인자(ps 노출)에도
  git config 에도 남지 않는다.
* 로그·예외·반환값에 절대 싣지 않는다 (gitcmd.redact 가 강제).
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from typing import Protocol, Sequence

from .errors import AuthError

_ASKPASS_PY = '''import os, sys
prompt = sys.argv[1].lower() if len(sys.argv) > 1 else ""
key = "GITWIRE_USERNAME" if "username" in prompt else "GITWIRE_TOKEN"
sys.stdout.write(os.environ.get(key, ""))
'''


class Credential(Protocol):
    """git 호출에 자격증명을 주입하는 인터페이스."""

    def env(self, workdir: Path) -> dict[str, str]:
        """git subprocess 에 덧씌울 환경변수. workdir 는 헬퍼 스크립트 보관처."""
        ...

    def secrets(self) -> Sequence[str]:
        """레닥션 대상 값들."""
        ...


class NoCredential:
    """인증이 필요 없는 채널 (로컬 경로 원격, SSH 에이전트, 이미 설정된 helper)."""

    def env(self, workdir: Path) -> dict[str, str]:
        return {}

    def secrets(self) -> Sequence[str]:
        return ()

    def __repr__(self) -> str:
        return "NoCredential()"


class TokenCredential:
    """HTTPS 토큰 인증 (GitHub PAT / GitLab token 등).

    토큰은 GIT_ASKPASS 헬퍼가 환경변수에서 읽어 git 에 넘긴다.
    """

    def __init__(self, token: str, username: str = "gitwire") -> None:
        if not token:
            raise AuthError("빈 토큰으로 TokenCredential 을 만들 수 없다")
        self._token = token
        self._username = username

    @classmethod
    def from_env(cls, var: str = "GITWIRE_TOKEN", username: str = "gitwire"):
        """환경변수에서 읽는다. 없으면 대화형으로 묻지 않고 즉시 AuthError."""
        token = os.environ.get(var)
        if not token:
            raise AuthError(
                f"환경변수 {var} 에 토큰이 없다. 헤드리스 환경에서는 되묻지 않고 실패한다."
            )
        return cls(token, username=username)

    @classmethod
    def from_file(cls, path: str | Path, username: str = "gitwire"):
        """파일에서 읽는다 (한 줄). 없으면 즉시 AuthError."""
        p = Path(path)
        if not p.is_file():
            raise AuthError(f"토큰 파일이 없다: {p}")
        token = p.read_text(encoding="utf-8").strip()
        if not token:
            raise AuthError(f"토큰 파일이 비어 있다: {p}")
        return cls(token, username=username)

    def env(self, workdir: Path) -> dict[str, str]:
        helper = _ensure_askpass(workdir)
        return {
            "GIT_ASKPASS": str(helper),
            "GITWIRE_USERNAME": self._username,
            "GITWIRE_TOKEN": self._token,
            "GIT_TERMINAL_PROMPT": "0",
        }

    def secrets(self) -> Sequence[str]:
        return (self._token,)

    def __repr__(self) -> str:
        # 절대 토큰을 노출하지 않는다.
        return f"TokenCredential(username={self._username!r}, token=***)"

    __str__ = __repr__


def _ensure_askpass(workdir: Path) -> Path:
    """GIT_ASKPASS 헬퍼를 workdir 에 만들고 경로를 돌려준다.

    윈도우에서는 git 이 .py 를 직접 실행하지 못하므로 .bat 로 감싼다.
    헬퍼 파일 자체에는 **토큰이 들어가지 않는다** (환경변수에서 읽는다).
    """
    workdir.mkdir(parents=True, exist_ok=True)
    py = workdir / "askpass.py"
    if not py.exists():
        py.write_text(_ASKPASS_PY, encoding="utf-8", newline="\n")
    if os.name == "nt":
        bat = workdir / "askpass.bat"
        if not bat.exists():
            bat.write_text(
                "@echo off\r\n" f'"{sys.executable}" "{py}" %*\r\n',
                encoding="utf-8",
            )
        return bat
    sh = workdir / "askpass.sh"
    if not sh.exists():
        sh.write_text(
            "#!/bin/sh\n" f'exec "{sys.executable}" "{py}" "$@"\n',
            encoding="utf-8",
            newline="\n",
        )
    sh.chmod(sh.stat().st_mode | stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)
    return sh
