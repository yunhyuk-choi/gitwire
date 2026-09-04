"""읽기의 **신선도 정책** — 원격을 볼 때와 보지 않을 때 (실제 git 으로 검증).

여기서 못 박는 것 넷:

* **L-1** `fresh=False` 읽기는 `ls-remote` 를 **한 번도** 하지 않는다.
  (`fresh=True` 는 한다 — 대조군이 있어야 "0회"가 의미를 갖는다.)
* **L-2** 계약이 그대로다 — 같은 레코드, 같은 페이지 경계, 중복·누락 없음.
* **L-3** 바뀌는 의미는 **딱 하나**다: 남이 방금 push 한 것은 안 보인다.
  그리고 그 신선도는 **구독(폴러)** 이 가져온다 — 읽기가 아니라.
* **L-4** `credential_helpers=` 는 **이 클론의 local 설정에만** 쓰고, 상속된
  helper 를 사슬 뒤에 남기며, 사용자의 global 을 고치지 않는다.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import gitwire
from gitwire.clock import FixedOffsetClock
from gitwire.gitcmd import SubprocessGitRunner


class StepClock:
    offset = 0.0

    def __init__(self, start: datetime, step: timedelta) -> None:
        self.at = start
        self.step = step

    def now(self) -> datetime:
        value = self.at
        self.at = self.at + self.step
        return value


class CountingRunner(SubprocessGitRunner):
    """git 호출을 하위명령별로 센다 (BASE_CONFIG 의 `-c` 쌍은 건너뛴다)."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    def run(self, args, **kwargs):
        rest, skip = [], False
        for a in args:
            if skip:
                skip = False
            elif a == "-c":
                skip = True
            elif not a.startswith("-"):
                rest.append(a)
        self.calls.append(rest[0] if rest else "?")
        return super().run(args, **kwargs)

    def count(self, name: str) -> int:
        return sum(1 for c in self.calls if c == name)

    def reset(self) -> None:
        self.calls.clear()


def _open(bare_repo, home, sender, runner=None):
    kwargs = dict(
        home=home, sender=sender, clock=FixedOffsetClock(0.0), batch_window=0.0
    )
    if runner is not None:
        kwargs["runner"] = runner
    return gitwire.Channel(str(bare_repo), **kwargs).open()


def _fill(channel, count, start=None, step=None):
    channel.clock = StepClock(
        start or datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc),
        step or timedelta(seconds=1),
    )
    return [channel.append({"i": i}, flush=True) for i in range(count)]


# ------------------------------------------------- L-1. 원격 왕복 0회


def test_local_reads_never_touch_the_remote(bare_repo, homes):
    """⭐ 요점 — 읽기 경로에서 `ls-remote` 가 사라진다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 12)
    runner = CountingRunner()
    reader = _open(bare_repo, homes("r"), "bob", runner)
    try:
        runner.reset()
        page = reader.history_page(limit=5, fresh=False)
        older = reader.history_page(before=page.oldest, limit=5, fresh=False)
        reader.history(limit=3, fresh=False)
        reader.record_ids(limit=3, fresh=False)

        assert runner.count("ls-remote") == 0, "로컬 읽기가 원격을 봤다"
        assert runner.count("fetch") == 0, "로컬 읽기가 fetch 했다"
        assert len(page.records) == 5 and len(older.records) == 5

        # 대조군 — 기본값(fresh=True)은 여전히 원격을 본다.
        runner.reset()
        reader.history_page(limit=5)
        assert runner.count("ls-remote") == 1
    finally:
        reader.close()
        writer.close()


# ------------------------------------------------------- L-2. 계약 동일


def test_local_paging_keeps_the_same_contract(bare_repo, homes):
    """같은 레코드·같은 경계·중복 없음·누락 없음 — 원격을 뗀 것만 다르다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 12, step=timedelta(hours=8))       # 하루 3건 x 4일
    reader = _open(bare_repo, homes("r"), "bob")
    try:
        reader.sync()                                # 폴러가 했을 일을 한 번

        fresh_ids: list[str] = []
        cursor = None
        while True:
            page = reader.history_page(before=cursor, limit=5)
            fresh_ids = [r.id for r in page.records] + fresh_ids
            if not page.has_more:
                break
            cursor = page.oldest

        local_ids: list[str] = []
        cursor = None
        while True:
            page = reader.history_page(before=cursor, limit=5, fresh=False)
            local_ids = [r.id for r in page.records] + local_ids
            if not page.has_more:
                break
            cursor = page.oldest

        assert local_ids == fresh_ids
        assert len(local_ids) == 12
        assert len(set(local_ids)) == 12, "중복이 생겼다"
        assert local_ids == sorted(local_ids), "시간순이 깨졌다"
        assert [r.id for r in reader.history(fresh=False)] == fresh_ids
        assert reader.record_ids(fresh=False) == fresh_ids
    finally:
        reader.close()
        writer.close()


# ----------------------------------- L-3. 바뀌는 의미 + 그걸 누가 메우나


def test_local_read_is_stale_until_the_poller_runs(bare_repo, homes):
    """⭐ 정직하게 — 로컬 읽기는 **마지막 폴 시점** 기준이다.

    그리고 그 지연을 메우는 것은 읽기가 아니라 **구독 경로**(poll_once)다.
    """
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 3)
    reader = _open(bare_repo, homes("r"), "bob")
    try:
        assert len(reader.history(fresh=False)) == 3

        writer.clock = StepClock(
            datetime(2026, 9, 2, 0, 0, 0, tzinfo=timezone.utc), timedelta(seconds=1)
        )
        writer.append({"i": "새 메시지"}, flush=True)

        # 읽기는 원격을 보지 않으므로 아직 모른다 — 이것이 우리가 산 것의 값이다.
        assert len(reader.history(fresh=False)) == 3

        # 그리고 폴러가 그것을 가져온다.
        delivered: list = []
        assert reader.poll_once(delivered.append) >= 1
        assert delivered[-1].payload["i"] == "새 메시지"
        assert len(reader.history(fresh=False)) == 4, "폴 이후에는 로컬 읽기가 본다"
    finally:
        reader.close()
        writer.close()


# --------------------------------------------- L-4. 자격증명 helper 사슬


def _local_helpers(clone: Path) -> list[str]:
    out = subprocess.run(
        ["git", "config", "--local", "--get-all", "credential.helper"],
        cwd=str(clone), capture_output=True, text=True, encoding="utf-8",
    )
    return out.stdout.splitlines()


def test_credential_helpers_are_opt_in_and_local_only(bare_repo, homes):
    """기본은 **아무것도 쓰지 않는다.** 켰을 때만, 그것도 클론 local 에만."""
    off = _open(bare_repo, homes("off"), "alice")
    try:
        assert _local_helpers(off.clone_dir) == [], "기본값이 설정을 건드렸다"
    finally:
        off.close()

    # 사용자의 global 에 helper 가 이미 있다고 하자.
    # (conftest 가 GIT_CONFIG_GLOBAL 을 tmp 파일로 격리해 둔다 — 진짜 설정이 아니다.)
    subprocess.run(
        ["git", "config", "--global", "credential.helper", "manager"], check=True
    )
    on = gitwire.Channel(
        str(bare_repo), home=homes("on"), sender="bob",
        clock=FixedOffsetClock(0.0), batch_window=0.0,
        credential_helpers=gitwire.credential_cache(60),
    ).open()
    try:
        chain = _local_helpers(on.clone_dir)
        # 빈 값 = "목록을 여기서부터 새로 센다" (git 규약).
        assert chain[0] == ""
        assert chain[1] == "cache --timeout=60"
        # 상속된 helper 는 **뒤에 남는다** — 캐시가 비면 원래대로 채워진다.
        assert "manager" in chain[2:]
        # 여러 번 열어도 사슬이 자라지 않는다 (재진입 안전).
        on._configure_credential_helpers()
        assert _local_helpers(on.clone_dir) == chain

        # 사용자의 global 은 그대로다 — 우리가 고치는 것은 이 클론뿐이다.
        got = subprocess.run(
            ["git", "config", "--global", "--get-all", "credential.helper"],
            capture_output=True, text=True, encoding="utf-8",
        )
        assert got.stdout.splitlines() == ["manager"]
    finally:
        on.close()
