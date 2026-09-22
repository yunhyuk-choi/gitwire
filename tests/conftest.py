"""공용 픽스처.

핵심 검증은 **로컬 bare 레포를 원격으로 삼은 2-클론 왕복**이다. 대역(mock)이
아니라 실제 git 으로 돌린다 — push 거부·rebase·ls-remote 같은 것들은 대역으로
흉내내면 아무것도 증명하지 못하기 때문이다.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import gitwire  # noqa: E402
from gitwire import gitcmd  # noqa: E402
from gitwire.clock import FixedOffsetClock  # noqa: E402


# ---------------------------------------------------------- 창 억제 (Windows)

#: 테스트가 띄우는 자식에게 **콘솔을 주지 않는다.**
#:
#: ⚠️ 이건 제품이 아니라 *테스트 하네스* 의 결함이었다. 콘솔이 없는 부모
#: (에이전트 셸·pythonw GUI 런처)에서 pytest 를 돌리면, 플래그 없이 스폰한
#: 콘솔 프로그램은 자기 콘솔을 **새로 할당**하고 Windows 11 기본 콘솔 호스트가
#: 그것을 **창으로 띄운다** — 실측: `tests/test_cli.py` 하나에서 보이는 창
#: 92개(Windows Terminal + PseudoConsoleWindow 쌍). 터미널에서 직접 돌리면
#: 자식이 부모 콘솔을 물려받아 창이 없으므로 여태 보이지 않았다.
#:
#: 규율은 제품(`gitcmd.creation_flags`)과 같다 — `DETACHED_PROCESS`.
NO_WINDOW: dict = (
    {"creationflags": getattr(subprocess, "DETACHED_PROCESS", 0x00000008)}
    if os.name == "nt" else {}
)


def git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=str(cwd), capture_output=True, text=True,
        encoding="utf-8", errors="replace", check=True, **NO_WINDOW)
    return proc.stdout


@pytest.fixture(autouse=True)
def _reset_credential_detection():
    """`gitcmd` 의 자격증명 헬퍼 탐지·접힘 기록은 **프로세스 전역**이다.

    한 테스트에서 인증 실패를 흉내내면 그 뒤 테스트는 "접힌" 상태로 돌아
    조용히 다른 것을 재게 된다. 그래서 매 테스트 앞뒤로 비운다.
    """
    gitcmd.reset_credential_state()
    yield
    gitcmd.reset_credential_state()


@pytest.fixture(autouse=True)
def _isolated_git_env(monkeypatch, tmp_path_factory):
    """전역 git 설정·자격증명 헬퍼가 테스트에 새지 않게 한다."""
    home = tmp_path_factory.mktemp("githome")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(home / "gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")
    monkeypatch.setenv("GIT_AUTHOR_NAME", "test")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "test@localhost")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "test")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "test@localhost")
    monkeypatch.delenv("GITWIRE_SENDER", raising=False)
    monkeypatch.delenv("GITWIRE_TOKEN", raising=False)


@pytest.fixture
def bare_repo(tmp_path: Path) -> Path:
    """빈 원격 레포 (사용자가 방금 만든 private repo 를 흉내낸다)."""
    repo = tmp_path / "origin.git"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(repo)],
        check=True,
        capture_output=True, **NO_WINDOW)
    return repo


@pytest.fixture
def homes(tmp_path: Path):
    """참가자별 gitwire 상태 루트 (= 서로 다른 머신을 흉내낸다)."""
    def make(name: str) -> Path:
        d = tmp_path / "homes" / name
        d.mkdir(parents=True, exist_ok=True)
        return d

    return make


@pytest.fixture
def participant(bare_repo, homes):
    """이름을 주면 그 참가자용 Channel 을 열어준다.

    시계는 고정 오프셋(=로컬 시계)으로 주입한다 — 네트워크 없이 돌아야 한다.
    """
    opened = []

    def make(name: str, **kwargs) -> gitwire.Channel:
        kwargs.setdefault("home", homes(name))
        kwargs.setdefault("sender", name)
        kwargs.setdefault("clock", FixedOffsetClock(0.0))
        ch = gitwire.Channel(str(bare_repo), **kwargs).open()
        opened.append(ch)
        return ch

    yield make
    for ch in opened:
        try:
            ch.close()
        except Exception:
            pass


@pytest.fixture
def cli_env(monkeypatch):
    """서브프로세스로 CLI 를 부를 때 쓸 환경 (새 프로세스 = 새 메모리)."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC)
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def run_cli(env, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "gitwire", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env, **NO_WINDOW)
