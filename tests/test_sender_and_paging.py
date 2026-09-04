"""설치본 식별자(sender) · append 반환값 · 역방향 페이징 — 실제 git 으로 검증.

세 가지를 여기서 못 박는다:

* **A** `sender` 기본값은 *설치본*을 가른다. 같은 머신의 두 설치본이 다른 값을
  갖고, 재시작해도 각자 유지된다. (예전 `<유저>.<호스트>` 는 둘이 같아서
  "이건 내가 낸 것인가" 판정이 무너졌다.)
* **B** `append()` 는 방금 만든 `Record` 를 돌려준다 — ID 문자열에서 시각을
  되파싱하지 않아도 되고, 그 과정에서 깎이던 마이크로초가 살아 있다.
* **C-1** 역방향 페이징은 keyset 커서(`before=<record_id>`)이고, **요청한 만큼의
  blob 만** 연다. "전량을 읽고 잘라낸다"의 반대라는 것을 *세어서* 확인한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import gitwire
from gitwire import records
from gitwire.channel import Channel
from gitwire.clock import FixedOffsetClock
from gitwire.gitcmd import SubprocessGitRunner


class StepClock:
    """부를 때마다 정해진 간격으로 나아가는 시계 (날짜 경계를 만들기 위해)."""

    offset = 0.0

    def __init__(self, start: datetime, step: timedelta) -> None:
        self.at = start
        self.step = step

    def now(self) -> datetime:
        value = self.at
        self.at = self.at + self.step
        return value


class FrozenClock:
    offset = 0.0

    def __init__(self, at: datetime) -> None:
        self.at = at

    def now(self) -> datetime:
        return self.at


# --------------------------------------------------- A. 설치본 식별자


def test_two_installations_on_one_machine_get_different_senders(bare_repo, homes):
    """⭐ A 의 핵심 — 같은 머신·같은 유저·같은 호스트라도 갈린다."""
    one = Channel(str(bare_repo), home=homes("one"), clock=FixedOffsetClock(0.0),
                  batch_window=0.0).open()
    two = Channel(str(bare_repo), home=homes("two"), clock=FixedOffsetClock(0.0),
                  batch_window=0.0).open()
    try:
        assert one.sender != two.sender, "두 설치본이 같은 전송 식별자를 갖는다"

        # 그리고 그 값이 실제 레코드 봉투에 실린다.
        rec_one = one.append({"who": 1}, flush=True)
        rec_two = two.append({"who": 2}, flush=True)
        assert rec_one.sender == one.sender
        assert rec_two.sender == two.sender

        # 상대 레코드를 자기 것으로 오인하지 않는다 (소비자의 에코 판정 근거).
        seen = {r.sender for r in one.history()}
        assert seen == {one.sender, two.sender}
    finally:
        one.close()
        two.close()


def test_sender_survives_restart(bare_repo, homes):
    """재시작 = 같은 home 으로 새 객체. 신원은 파일에서 되살아난다."""
    home = homes("restart")
    first = Channel(str(bare_repo), home=home, clock=FixedOffsetClock(0.0),
                    batch_window=0.0).open()
    original = first.sender
    first.close()

    again = Channel(str(bare_repo), home=home, clock=FixedOffsetClock(0.0),
                    batch_window=0.0).open()
    try:
        assert again.sender == original
        assert (home / "installation.txt").is_file()
    finally:
        again.close()


def test_explicit_sender_is_respected(bare_repo, homes):
    """하위호환 — 명시적으로 준 sender 는 그대로 쓰고 신원 파일도 만들지 않는다."""
    home = homes("explicit")
    ch = Channel(str(bare_repo), home=home, sender="alice", batch_window=0.0,
                 clock=FixedOffsetClock(0.0)).open()
    try:
        rec = ch.append({"n": 1}, flush=True)
        assert ch.sender == "alice"
        assert rec.sender == "alice"
        assert not (home / "installation.txt").exists()
    finally:
        ch.close()


# ------------------------------------------------- B. append 반환값


def test_append_returns_record_with_microsecond_precision(participant):
    """ID 에서 되파싱하면 밀리초로 깎인다 — 그래서 Record 를 돌려준다."""
    ts = datetime(2026, 9, 3, 10, 11, 12, 345678, tzinfo=timezone.utc)
    a = participant("alice", clock=FrozenClock(ts))

    rec = a.append({"n": 1}, flush=True)

    assert isinstance(rec, gitwire.Record)
    assert rec.sender == "alice"
    assert rec.timestamp == ts
    assert rec.payload == {"n": 1}
    # 파일명(=ID)이 담을 수 있는 정밀도는 밀리초까지다.
    stamp = rec.id.rsplit("/", 1)[-1].split("-", 1)[0]
    assert records.parse_ts(stamp).microsecond == 345000
    assert rec.timestamp.microsecond == 345678, "ID 되파싱 수준으로 정밀도가 깎였다"


def test_append_record_matches_what_others_read(participant):
    a = participant("alice")
    b = participant("bob")
    sent = [a.append({"i": i}, flush=True) for i in range(3)]
    assert [r.id for r in b.fetch_new()] == [r.id for r in sent]


# --------------------------------------------- C-1. 역방향 페이징


def participant_channel(bare_repo, home, sender="alice"):
    """픽스처를 거치지 않고 채널 하나를 연다 (runner 를 갈아끼울 때 쓴다)."""
    return gitwire.Channel(
        str(bare_repo), home=home, sender=sender,
        clock=FixedOffsetClock(0.0), batch_window=0.0,
    ).open()


def _fill(channel, count, start=None, step=None):
    clock = StepClock(
        start or datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc),
        step or timedelta(seconds=1),
    )
    channel.clock = clock
    return [channel.append({"i": i}, flush=True) for i in range(count)]


def test_history_pages_backwards_with_keyset_cursor(participant):
    a = participant("alice")
    _fill(a, 12)
    b = participant("bob")

    first = b.history_page(limit=5)
    assert [r.payload["i"] for r in first.records] == [7, 8, 9, 10, 11]
    assert first.has_more is True

    second = b.history_page(before=first.oldest, limit=5)
    assert [r.payload["i"] for r in second.records] == [2, 3, 4, 5, 6]
    assert second.has_more is True

    third = b.history_page(before=second.oldest, limit=5)
    assert [r.payload["i"] for r in third.records] == [0, 1]
    assert third.has_more is False, "맨 위인데 '더 있다'고 말한다"

    # 끝에 닿은 뒤 한 번 더 물어도 조용히 빈 쪽이다 (무한 스크롤의 종료 조건).
    end = b.history_page(before=third.oldest, limit=5)
    assert end.records == [] and end.has_more is False and end.oldest is None


def test_paging_is_stable_when_new_records_arrive(participant):
    """⭐ keyset 을 쓰는 이유 — offset 이면 여기서 중복·누락이 난다."""
    a = participant("alice")
    _fill(a, 10)
    b = participant("bob")

    first = b.history_page(limit=4)          # [6..9]
    a.clock = StepClock(datetime(2026, 9, 2, tzinfo=timezone.utc), timedelta(seconds=1))
    a.append({"i": 100}, flush=True)         # 위로 읽는 도중 새 레코드가 도착
    a.append({"i": 101}, flush=True)

    second = b.history_page(before=first.oldest, limit=4)
    assert [r.payload["i"] for r in second.records] == [2, 3, 4, 5]

    everything = [r.payload["i"] for r in b.history()]
    got = [r.payload["i"] for r in second.records] + [r.payload["i"] for r in first.records]
    assert got == everything[2:10], "경계가 밀려 중복·누락이 났다"


def test_paging_opens_only_the_requested_blobs(participant, monkeypatch):
    """⭐ C-1 의 성능 요점을 **세어서** 못 박는다."""
    a = participant("alice")
    _fill(a, 60)
    b = participant("bob")

    reads: list[str] = []
    original = Channel._read_record

    def counting(self, rid, ref=None):
        reads.append(rid)
        return original(self, rid, ref)

    monkeypatch.setattr(Channel, "_read_record", counting)

    page = b.history_page(limit=10)
    assert len(page.records) == 10
    assert len(reads) == 10, f"blob 을 {len(reads)}개 열었다 (10개여야 한다)"

    reads.clear()
    older = b.history_page(before=page.oldest, limit=10)
    assert len(older.records) == 10
    assert len(reads) == 10

    # 전량을 읽는 경로는 여전히 전량이다 (비교군 — 옛 '이전 불러오기'가 이것이었다).
    reads.clear()
    assert len(b.history()) == 60
    assert len(reads) == 60


def test_paging_skips_day_directories_it_does_not_need(participant, monkeypatch):
    """날짜 디렉토리를 역순으로 훑고, 커서보다 뒤인 날짜는 열지 않는다."""
    a = participant("alice")
    _fill(a, 12, step=timedelta(hours=8))     # 하루 3건 x 4일
    b = participant("bob")

    everything = a.history()
    days = sorted({r.id.split("/")[1] for r in everything})
    assert len(days) == 4, f"날짜 디렉토리가 4개여야 한다: {days}"

    visited: list[str] = []
    original = Channel._day_records

    def spy(self, day, tree):
        visited.append(day)
        return original(self, day, tree)

    monkeypatch.setattr(Channel, "_day_records", spy)

    b.history_page(limit=3)
    # 최신 날짜부터 필요한 만큼만 — 하루 3건이므로 "3건 + 더 있나 1건"에
    # 마지막 두 날짜면 충분하다. 오래된 날짜는 **쳐다보지도 않는다.**
    assert visited == [days[-1], days[-2]], f"열어본 날짜: {visited}"
    assert days[0] not in visited and days[1] not in visited

    # 커서가 셋째 날 안에 있으면, 그보다 뒤인 넷째 날은 **디렉토리째** 건너뛴다.
    visited.clear()
    cursor = everything[7].id
    assert cursor.split("/")[1] == days[-2]
    b.history_page(before=cursor, limit=3)
    assert visited == [days[-2], days[-3]], f"열어본 날짜: {visited}"
    assert days[-1] not in visited


def test_record_ids_lists_without_opening_blobs(participant, monkeypatch):
    a = participant("alice")
    _fill(a, 20)
    b = participant("bob")

    reads: list[str] = []
    original = Channel._read_record
    monkeypatch.setattr(
        Channel, "_read_record",
        lambda self, rid, ref=None: (reads.append(rid), original(self, rid, ref))[1],
    )

    ids = b.record_ids(limit=5)
    assert len(ids) == 5
    assert ids == sorted(ids)
    assert reads == [], "ID 만 물었는데 blob 을 열었다"

    # 그 ID 는 그대로 커서로 쓸 수 있다.
    page = b.history_page(before=ids[0], limit=3)
    assert all(r.id < ids[0] for r in page.records)


def test_history_with_before_is_ascending_and_excludes_cursor(participant):
    a = participant("alice")
    made = _fill(a, 6)
    b = participant("bob")

    got = b.history(before=made[3].id)
    assert [r.payload["i"] for r in got] == [0, 1, 2]
    assert made[3].id not in [r.id for r in got]

    with pytest.raises(TypeError):
        b.history(made[3].id, 3)     # 커서는 반드시 키워드 인자다


# ------------------------------------------ C-1(F). 나열 캐시 — stale 불가능


class CountingRunner(SubprocessGitRunner):
    """git 호출을 종류별로 센다 (BASE_CONFIG 의 -c 쌍은 건너뛴다)."""

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


def test_second_page_costs_no_listing_calls(bare_repo, homes):
    """⭐ F 의 요점 — 같은 상태를 다시 나열할 때 git 을 부르지 않는다.

    (실측 근거: git subprocess 는 하는 일과 무관하게 이 머신에서 42~48ms 다.
    나열을 0회로 만드는 것이 유일하게 그 바닥을 없애는 방법이다.)
    """
    writer = participant_channel(bare_repo, homes("w"))
    _fill(writer, 12)                              # 같은 날짜 12건
    runner = CountingRunner()
    reader = gitwire.Channel(
        str(bare_repo), home=homes("r"), sender="reader",
        clock=FixedOffsetClock(0.0), batch_window=0.0, runner=runner,
    ).open()
    try:
        first = reader.history_page(limit=3)
        after_first = runner.count("ls-tree")
        assert after_first == 2, "첫 쪽은 날짜 목록 + 그 날짜 나열, 2회면 된다"

        second = reader.history_page(before=first.oldest, limit=3)
        third = reader.history_page(before=second.oldest, limit=3)

        assert runner.count("ls-tree") == after_first, "캐시가 안 먹었다 (나열이 또 나갔다)"
        assert [r.payload["i"] for r in first.records] == [9, 10, 11]
        assert [r.payload["i"] for r in second.records] == [6, 7, 8]
        assert [r.payload["i"] for r in third.records] == [3, 4, 5]
        info = reader.cache_info()
        assert info["hits"] > 0 and info["evictions"] == 0
    finally:
        reader.close()
        writer.close()


def test_new_day_directory_costs_one_call_then_is_cached(bare_repo, homes):
    """날짜가 여러 개면 **처음 보는 날짜에만** 나열이 한 번 나간다."""
    writer = participant_channel(bare_repo, homes("w"))
    _fill(writer, 12, step=timedelta(hours=8))     # 하루 3건 x 4일
    runner = CountingRunner()
    reader = gitwire.Channel(
        str(bare_repo), home=homes("r"), sender="reader",
        clock=FixedOffsetClock(0.0), batch_window=0.0, runner=runner,
    ).open()
    try:
        page = reader.history_page(limit=3)
        cursors = []
        while page.has_more:
            cursors.append(page.oldest)
            page = reader.history_page(before=page.oldest, limit=3)
        # 날짜 목록 1회 + 날짜 4개 = 최대 5회.
        assert runner.count("ls-tree") <= 5, runner.calls
        walked = runner.count("ls-tree")
        for cursor in cursors:                      # 같은 자리를 다시 훑는다
            reader.history_page(before=cursor, limit=3)
        assert runner.count("ls-tree") == walked, "이미 본 상태를 또 나열했다"
    finally:
        reader.close()
        writer.close()


def test_cache_cannot_go_stale_when_new_records_arrive(bare_repo, homes):
    """⭐ sha 키의 요점 — 남이 push 한 것이 캐시 때문에 안 보이는 일이 없다."""
    writer = participant_channel(bare_repo, homes("w"))
    _fill(writer, 6)
    runner = CountingRunner()
    reader = gitwire.Channel(
        str(bare_repo), home=homes("r"), sender="reader",
        clock=FixedOffsetClock(0.0), batch_window=0.0, runner=runner,
    ).open()
    try:
        assert [r.payload["i"] for r in reader.history()] == [0, 1, 2, 3, 4, 5]
        assert reader.cache_info()["entries"] > 0      # 캐시가 찼다

        writer.append({"i": 6}, flush=True)            # 커밋 sha 가 바뀐다
        assert [r.payload["i"] for r in reader.history()] == [0, 1, 2, 3, 4, 5, 6]

        page = reader.history_page(limit=2)
        assert [r.payload["i"] for r in page.records] == [5, 6]
    finally:
        reader.close()
        writer.close()


def test_cache_cannot_go_stale_when_a_past_day_grows(bare_repo, homes):
    """⭐ "지난 날짜는 안 바뀐다"는 가정을 쓰지 않았음을 못 박는다.

    오프라인에서 쓰고 나중에 push 하거나 시계가 어긋난 참가자는 **과거 날짜에**
    레코드를 추가한다. 규칙 기반 무효화였다면 여기서 조용히 틀린다.
    """
    writer = participant_channel(bare_repo, homes("w"))
    _fill(writer, 6, step=timedelta(hours=8))          # 9/1 3건, 9/2 3건
    reader = participant_channel(bare_repo, homes("r"), sender="reader")
    try:
        before_ids = reader.record_ids()
        days = sorted({i.split("/")[1] for i in before_ids})
        assert len(days) == 2
        assert reader.cache_info()["entries"] > 0      # 두 날짜 모두 캐시에 있다

        # 뒤늦게 도착한 **첫째 날짜**의 레코드 (오프라인 참가자를 흉내낸다)
        writer.clock = FrozenClock(
            datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        late = writer.append({"i": 999}, flush=True)
        assert late.id.split("/")[1] == days[0], "첫째 날짜에 들어가야 하는 레코드다"

        after = reader.record_ids()
        assert late.id in after, "과거 날짜에 추가된 레코드가 캐시에 가려졌다"
        assert after == sorted(after)
        assert len(after) == len(before_ids) + 1

        # 그 날짜를 페이징해도 보인다 (경계 계산이 새 목록 위에서 돈다).
        page = reader.history_page(before=days[1] and f"records/{days[1]}/", limit=10)
        assert late.id in [r.id for r in page.records]
    finally:
        reader.close()
        writer.close()


def test_cache_budget_evicts_and_stays_correct(bare_repo, homes):
    """상한을 넘으면 LRU 로 버리되 **정확성은 그대로**다 (git 을 다시 부를 뿐)."""
    writer = participant_channel(bare_repo, homes("w"))
    _fill(writer, 9, step=timedelta(hours=8))
    reader = participant_channel(bare_repo, homes("r"), sender="reader")
    try:
        reader._trees.max_items = 1        # 사실상 못 담게 만든다
        reader._trees.clear()
        assert [r.payload["i"] for r in reader.history()] == list(range(9))
        page = reader.history_page(limit=4)
        assert [r.payload["i"] for r in page.records] == [5, 6, 7, 8]
        older = reader.history_page(before=page.oldest, limit=4)
        assert [r.payload["i"] for r in older.records] == [1, 2, 3, 4]
        assert reader.cache_info()["evictions"] > 0
    finally:
        reader.close()
        writer.close()
