"""gitwire 예외 계층.

모든 예외는 GitwireError 를 상속한다. 자격증명 값은 절대 예외 메시지에
실리지 않는다 (git.py 의 redact() 가 방어한다).
"""

from __future__ import annotations


class GitwireError(Exception):
    """gitwire 의 모든 예외의 루트."""

    #: CLI 종료코드 (cli.py 의 계약과 짝을 이룬다)
    exit_code = 1


class GitError(GitwireError):
    """git 서브프로세스가 0 이 아닌 코드로 끝났다."""

    exit_code = 4

    def __init__(self, args, returncode, stderr):
        self.args_ = list(args)
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"git {' '.join(self.args_)} 실패 (exit={returncode}): {stderr.strip()}"
        )


class AuthError(GitwireError):
    """자격증명이 없거나 거부됐다. 대화형으로 되묻지 않고 즉시 실패한다."""

    exit_code = 3


class PushRejected(GitError):
    """원격이 non-fast-forward 로 push 를 거부했다 (선점)."""

    exit_code = 4


class ChannelInitError(GitwireError):
    """채널 레포 초기화·레이아웃 확정에 실패했다."""

    exit_code = 5


class HistoryRewritten(GitwireError):
    """원격 히스토리가 재작성됐고(압축 등) 로컬에 미푸시 커밋이 남아 있다."""

    exit_code = 6


class ClockError(GitwireError):
    """공통 시계 보정에 실패했다 (치명적이지 않음 — 보통 경고로 흡수)."""

    exit_code = 7
