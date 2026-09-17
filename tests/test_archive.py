"""지난 날짜 아카이빙(로컬) + 레코드 삭제(합의 뒤) — 실제 git 2-클론 왕복으로 증명.

대역(mock)이 아니라 로컬 bare 레포를 원격으로 쓴다. 여기서 증명해야 하는 것들은
전부 *진짜 git 의 동작*(push 거부·fast-forward·rebase·트리 내용·히스토리)이기
때문이다.

과거 날짜 레코드는 **시계를 뒤로 돌려** 만든다 (`FixedOffsetClock(-N일)`).
파일을 손으로 심지 않고 `append()` 를 그대로 타므로, 실제 발행 경로가 만드는
id·봉투·디렉토리 구조 그대로 검증된다.

⭐ 이 파일의 중심 주장 하나: **아카이브는 절대 커밋되지 않고, 레코드 삭제는
내가 실제로 담은 것만 지운다.**
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import gitwire
from gitwire import rollup as R
from gitwire.clock import FixedOffsetClock

DAY = 86400.0


def past(days: float) -> FixedOffsetClock:
    """days 일 전으로 고정된 시계."""
    return FixedOffsetClock(-days * DAY)


def write_past(ch: gitwire.Channel, days: float, payloads):
    """days 일 전 시각으로 레코드를 발행한다. 발행한 Record 목록."""
    real = ch.clock
    ch.clock = past(days)
    try:
        return [ch.append(p, flush=True) for p in payloads]
    finally:
        ch.clock = real


def day_of(rec) -> str:
    return R.day_of(rec.id)


def tree_paths(ch: gitwire.Channel, ref: str = "HEAD") -> list[str]:
    out = ch.git.out("ls-tree", "-r", "--name-only", ref)
    return sorted(p for p in out.splitlines() if p)


def fold(ch: gitwire.Channel, **kw) -> tuple[dict, dict]:
    """아카이빙 + 그 날짜 삭제 (소비자가 하는 두 동작을 테스트에서 묶은 것)."""
    arch = ch.archive_days(**kw)
    drop = ch.drop_days(arch["archived"], **kw) if arch["archived"] else {
        "dropped": False, "reason": "지울 날짜가 없다", "days": [],
    }
    return arch, drop


# --------------------------------------------------------------- 순수 로직


def test_archive_bytes_are_deterministic():
    """같은 레코드 집합 → **바이트 단위로 같은** 아카이브 (멱등·진단의 근거)."""
    lines = {
        f"records/20260901/x{i}.json": json.dumps(
            {"gitwire": 1, "id": f"records/20260901/x{i}.json", "payload": {"i": i}},
            ensure_ascii=False, separators=(",", ":"),
        )
        for i in range(20)
    }
    a = R.build_archive(lines)
    b = R.build_archive(dict(reversed(list(lines.items()))))  # 삽입 순서만 다르게
    assert a == b
    assert a.endswith(b"\n") and b"\r" not in a
    # 정렬 = id 오름차순
    ids = [R.line_id(ln) or "" for ln in R.split_lines(a)]
    assert ids == sorted(lines)


def test_is_closed_uses_utc_day_plus_grace():
    now = datetime(2026, 9, 2, 1, 0, tzinfo=timezone.utc)
    assert not R.is_closed("20260901", now, grace_hours=2)   # 아직 유예 안
    assert R.is_closed("20260901", now + timedelta(hours=2), grace_hours=2)
    assert not R.is_closed("20260902", now + timedelta(hours=2), grace_hours=2)
    assert R.is_closed("20260831", now, grace_hours=2)


def test_last_closed_day_agrees_with_is_closed():
    """워터마크 상한(`last_closed_day`)이 `is_closed` 와 어긋나지 않는다.

    ⭐ 배치 실행 **시각**과 "어제"의 **정의**는 다른 것이다 — 정의는 언제나 UTC
    날짜다. 로컬 시각으로 날짜를 세면 폴더명과 어긋난다.
    """
    for hour in range(0, 24):
        now = datetime(2026, 9, 2, hour, 30, tzinfo=timezone.utc)
        last = R.last_closed_day(now, 2.0)
        assert R.is_closed(last, now, 2.0), (hour, last)
        nxt = R.previous_day(last)
        assert R.previous_day(R.previous_day(last)) < nxt
        # 그 다음 날짜는 아직 지난 날이 아니다
        after = f"{datetime.strptime(last, '%Y%m%d') + timedelta(days=1):%Y%m%d}"
        assert not R.is_closed(after, now, 2.0), (hour, after)


def test_previous_day():
    assert R.previous_day("20260301") == "20260228"
    assert R.previous_day("20260101") == "20251231"
    assert R.previous_day("nope") == "nope"


def test_canonical_line_refuses_garbage():
    with pytest.raises(R.ArchiveFormatError):
        R.canonical_line(b"not json")
    with pytest.raises(R.ArchiveFormatError):
        R.canonical_line(b"")
    # 여러 줄로 쓰인 봉투는 한 줄로 접힌다
    line = R.canonical_line(b'{\n  "id": "records/20260901/a.json",\n  "payload": 1\n}')
    assert "\n" not in line and json.loads(line)["payload"] == 1


def test_blob_sha1_matches_git(participant):
    """레코드·참가자 상태 blob 읽기에 쓰는 sha1 계산이 git 과 정말 같은가."""
    ch = participant("a")
    data = "안녕 gitwire\n".encode("utf-8")
    (ch.clone_dir / "probe.bin").write_bytes(data)
    assert ch.git.out("hash-object", "--no-filters", "--", "probe.bin") == R.blob_sha1(data)


def test_write_archive_is_atomic_and_leaves_no_temp(tmp_path):
    stamp = R.write_archive(tmp_path, "20260901", b"a\n")
    assert (tmp_path / "archive" / "20260901.jsonl").read_bytes() == b"a\n"
    assert stamp and R.list_archives(tmp_path) == {"20260901": stamp}
    assert not [p for p in (tmp_path / "archive").iterdir() if p.name.startswith(".")]


# -------------------------------------------- ⭐ 아카이브는 커밋되지 않는다


def test_archive_is_local_only_and_never_committed(participant):
    """⭐ 검증 1 — 아카이빙은 **로컬 파일**을 쓰고, 커밋에는 삭제만 담긴다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(5)])
    now = a.append({"n": "today"}, flush=True)
    day = day_of(old[0])
    head_before = a.git.out("rev-parse", "HEAD")

    arch = a.archive_days()
    assert arch["archived"] == [day] and arch["records"] == 5
    assert arch["written"] == [day]
    # (1) 아카이빙은 커밋을 만들지 않는다
    assert a.git.out("rev-parse", "HEAD") == head_before
    # (2) 로컬 파일은 생겼다
    local = a.clone_dir / R.archive_path(day)
    assert local.exists() and local.read_bytes().endswith(b"\n")
    # (3) 그런데 git 은 그 파일을 **모른다** (gitignore)
    assert a.git.out("status", "--porcelain") == ""
    assert R.archive_path(day) not in tree_paths(a)
    assert a.git.ok("check-ignore", "-q", R.archive_path(day))

    drop = a.drop_days(arch["archived"])
    assert drop["dropped"] is True and drop["days"] == [day]
    paths = tree_paths(a)
    # (4) 삭제 커밋에도 아카이브가 들어가지 않는다
    assert not [p for p in paths if p.startswith("archive/")]
    assert not [p for p in paths if p.startswith(f"records/{day}/")]
    assert now.id in paths                      # 오늘은 그대로 살아 있다
    assert not (a.clone_dir / old[0].id).exists()
    assert local.exists()                       # 로컬 아카이브는 남아 있다


def test_new_channel_commits_the_gitignore(participant):
    """⭐ 검증 1(b) — `.gitignore` 는 **추적된다** (전원이 무시해야 하므로)."""
    a = participant("a")
    paths = tree_paths(a)
    assert ".gitignore" in paths
    body = a.git.out("show", "HEAD:.gitignore")
    assert R.ARCHIVE_IGNORE_LINE in [ln.strip() for ln in body.splitlines()]


def test_existing_channel_gets_the_gitignore_without_touching_tracked_files(
    bare_repo, homes
):
    """`.gitignore` 를 모르는 옛 방에도 규칙이 **한 번** 심긴다 (추적분은 그대로).

    옛 방을 흉내내려고 `.gitignore` 를 지운 커밋을 만들고, 그 상태에서 새 채널을
    연다. 규칙이 심기되 **이미 추적 중인 아카이브 파일은 우리가 지우지 않는다**
    (gitignore 는 추적 중인 파일에 영향이 없고, 정리는 사람의 결정이다).
    """
    first = gitwire.Channel(
        str(bare_repo), home=homes("a"), sender="a",
        clock=FixedOffsetClock(0.0), batch_window=0.0,
    ).open()
    try:
        first.git.run("rm", "-q", "--", ".gitignore")
        # 옛 세계의 잔재: 추적되는 아카이브 파일
        legacy = first.clone_dir / R.archive_path("20250101")
        legacy.parent.mkdir(parents=True, exist_ok=True)
        legacy.write_bytes(b'{"gitwire":1,"id":"records/20250101/a.json","payload":1}\n')
        first.git.run("add", "-f", "--", R.archive_path("20250101"))
        first.git.run("commit", "-m", "legacy")
        first.git.run("push", "origin", f"HEAD:refs/heads/{first.branch}")
    finally:
        first.close()

    second = gitwire.Channel(
        str(bare_repo), home=homes("b"), sender="b",
        clock=FixedOffsetClock(0.0), batch_window=0.0,
    ).open()
    try:
        assert ".gitignore" in tree_paths(second)
        body = second.git.out("show", "HEAD:.gitignore")
        assert R.ARCHIVE_IGNORE_LINE in [ln.strip() for ln in body.splitlines()]
        # 추적 중이던 옛 아카이브는 그대로 (우리가 git rm 하지 않는다)
        assert R.archive_path("20250101") in tree_paths(second)
        # 그리고 **새로** 만드는 아카이브는 처음부터 추적되지 않는다
        (second.clone_dir / R.archive_path("20260901")).write_bytes(b"x\n")
        assert second.git.ok("check-ignore", "-q", R.archive_path("20260901"))
    finally:
        second.close()


def test_archiving_twice_writes_nothing_the_second_time(participant):
    """아카이빙은 **멱등**하다 — 같은 바이트면 파일을 만지지 않는다 (mtime 유지)."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    day = day_of(old[0])
    assert a.archive_days()["written"] == [day]
    before = R.stamp_of(a.clone_dir / R.archive_path(day))
    again = a.archive_days()
    assert again["archived"] == [day] and again["written"] == []
    assert R.stamp_of(a.clone_dir / R.archive_path(day)) == before


# ------------------------------------------- ⭐ 합의 전에는 지우지 않는다 (기반 쪽)


def test_drop_archives_first_so_it_never_deletes_what_it_lacks(participant):
    """⭐ 검증 2(기반) — 지우기 직전에 **한 번 더 담는다** (멱등 합집합).

    소비자가 합의를 판정해 "이 날짜를 지워라"고 시킬 때, 그 사이에 늦게 도착한
    레코드가 있을 수 있다. 그것까지 내 로컬 아카이브에 담은 **뒤에만** 지운다 —
    그래서 "내가 담지 않은 것을 지우는" 일이 구조적으로 생기지 않는다.
    """
    a = participant("a")
    b = participant("b")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    day = day_of(old[0])
    b.sync()
    assert day not in b.archive_state()          # b 는 아직 아무것도 안 담았다

    res = b.drop_days([day])
    assert res["dropped"] is True
    assert b.archived_ids(day) == sorted(r.id for r in old)   # 담고 나서 지웠다
    assert {r.id: r.payload for r in b.history(fresh=False)} == {
        r.id: r.payload for r in old
    }


def test_drop_refuses_when_it_cannot_archive(participant, monkeypatch):
    """⭐ 마지막 방어선 — 로컬 아카이브에 없는 레코드는 **끝까지 지우지 않는다**.

    담는 단계가 어떤 이유로든(디스크 오류·훼손) 아무것도 남기지 못한 상황을
    대역으로 만든다. 그때도 삭제 커밋이 만들어지지 않아야 한다.
    """
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    day = day_of(old[0])
    head_before = a.git.out("rev-parse", "HEAD")
    monkeypatch.setattr(
        type(a), "_archive_one", lambda self, d, t: {"day": d, "records": 0, "written": False}
    )

    res = a.drop_days([day])
    assert res["dropped"] is False
    assert day in res["skipped"] and "로컬 아카이브에 없는" in res["skipped"][day]
    assert [p for p in tree_paths(a) if p.startswith(f"records/{day}/")]
    assert a.git.out("rev-parse", "HEAD") == head_before
    for r in old:
        assert (a.clone_dir / r.id).exists()


def test_drop_never_touches_today(participant):
    """오늘 날짜는 `force` 없이는 절대 지우지 않는다 (아직 자라는 중이다)."""
    a = participant("a")
    rec = a.append({"n": 1}, flush=True)
    today = R.day_of(rec.id)
    a.archive_days(days=[today], force=True)       # 부분 실행 (워터마크 없음)
    res = a.drop_days([today])
    assert res["dropped"] is False
    assert res["skipped"][today] == "아직 지난 날이 아니다"
    assert (a.clone_dir / rec.id).exists()


def test_partial_archive_run_has_no_watermark(participant):
    """`days=`·`force=` 로 부분 실행하면 확인응답 워터마크를 만들지 않는다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(2)])
    day = day_of(old[0])
    assert a.archive_days(days=[day])["through"] is None
    assert a.archive_days(force=True)["through"] is None
    assert a.archive_days()["through"] == R.last_closed_day(
        a.clock.now(), a.archive_grace_hours
    )


def test_unfoldable_day_stops_the_watermark(participant):
    """접을 수 없는 날짜가 있으면 워터마크가 **그 앞에서 멈춘다** (건너뛰지 않는다).

    건너뛰고 올리면 그 날의 레코드가 내 아카이브에 없는데도 남들이 지울 수 있다.
    """
    a = participant("a")
    older = write_past(a, 3, [{"n": i} for i in range(2)])
    newer = write_past(a, 2, [{"n": i} for i in range(2)])
    bad_day = day_of(older[0])
    bad = a.clone_dir / f"records/{bad_day}/20260101T000000000Z-x-zzzzzz.json"
    bad.write_bytes(b"NOT JSON AT ALL")
    a.git.run("add", "-A", "--", "records")
    a.git.run("commit", "-m", "bad")
    a.flush()

    res = a.archive_days()
    assert bad_day in res["skipped"]
    assert res["through"] == R.previous_day(bad_day)
    assert day_of(newer[0]) in res["archived"]     # 뒤 날짜는 옮겨졌지만
    # 워터마크가 앞에서 멈췄으므로 소비자는 그 뒤를 지우라고 시키지 않는다
    assert res["through"] < day_of(newer[0])
    assert bad.exists()                            # 아무것도 지우지 않았다


# --------------------------------------------------------- ⭐ id 안정성 / 읽기


def test_ids_survive_archiving_and_drop(participant):
    """삭제 전에 받아둔 id 로 조회·before 커서·기존 소비자 커서가 그대로 동작한다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(10)])
    a.append({"n": "today"}, flush=True)

    before_ids = [r.id for r in a.history(fresh=False)]
    mid = old[5].id
    page_before = a.history(before=mid, limit=3, fresh=False)

    fold(a)

    got = {r.id: r for r in a.history(fresh=False)}
    assert [r.id for r in a.history(fresh=False)] == before_ids
    for r in old:
        assert got[r.id].payload == r.payload
        assert got[r.id].sender == r.sender
        assert got[r.id].timestamp == r.timestamp
    assert [r.id for r in a.history(before=mid, limit=3, fresh=False)] == [
        r.id for r in page_before
    ]
    page = a.history_page(limit=4, fresh=False)
    walked = list(page.records)
    while page.has_more:
        page = a.history_page(before=page.oldest, limit=4, fresh=False)
        walked = list(page.records) + walked
    assert [r.id for r in walked] == before_ids


def test_record_ids_and_paging_span_local_archives(participant):
    """⭐ 검증 7 — 과거 메시지 페이징이 **로컬 아카이브**로도 동작한다."""
    a = participant("a")
    made = []
    for d in (4, 3, 2):
        made += write_past(a, d, [{"n": f"{d}-{i}"} for i in range(5)])
    made += [a.append({"n": "today"}, flush=True)]
    all_ids = [r.id for r in made]

    fold(a)

    assert a.record_ids(fresh=False) == all_ids
    walked = []
    page = a.history_page(limit=3, fresh=False)
    while True:
        walked = [r.id for r in page.records] + walked
        if not page.has_more:
            break
        page = a.history_page(before=page.oldest, limit=3, fresh=False)
    assert walked == all_ids


def test_persisted_consumer_cursor_still_valid(participant):
    """디스크에 이미 영속된 소비자 커서가 삭제 뒤에도 계속 이어진다."""
    a = participant("a")
    first = write_past(a, 2, [{"n": i} for i in range(4)])
    seen = [r.id for r in a.fetch_new()]
    assert seen == [r.id for r in first]
    cur_before = a.cursors.load()

    fold(a)

    assert a.cursors.load().watermark == cur_before.watermark
    assert a.fetch_new() == []
    fresh = a.append({"n": "after"}, flush=True)
    assert [r.id for r in a.fetch_new()] == [fresh.id]


def test_offline_consumer_does_not_miss_deleted_days(participant, homes):
    """⭐ 검증 7 — 주말 내내 꺼져 있던 소비자가 지워진 날의 대화를 놓치지 않는다.

    트리 diff 는 '추가 후 삭제'를 상쇄해 버린다 — `_diff_records` 가 트리 차이
    대신 **범위 안의 커밋이 추가한 경로**를 묻는 것이 그 구멍을 막는 장치다.
    """
    a = participant("a")
    b = participant("b", consumer="reader")
    # ⚠️ 커서를 **실제로 세워 둔다** (레코드 1건을 받아 기준 커밋을 갖게 한다).
    # 한 번도 소비하지 않은 커서는 전량 나열 모드로 떨어지고, 그건 아래 「갓
    # 합류한 사람」 테스트가 다루는 다른 이야기다.
    #
    # ⚠️ 그 1건은 **다른 채널**이 낸다. 같은 채널에서 현재 시각 레코드를 먼저
    # 내면 스탬프 단조 증가 가드 때문에 뒤이은 "과거" 발행이 현재로 찍힌다
    # (`Channel._stamp` — 그게 옳은 동작이다).
    primer = participant("p")
    first = primer.append({"n": "before"}, flush=True)
    assert [r.id for r in b.fetch_new()] == [first.id]

    weekend = write_past(a, 2, [{"n": i} for i in range(6)])
    fold(a)                                        # b 가 자는 사이에 지워졌다

    got = [r.id for r in b.fetch_new()]
    assert got == [r.id for r in weekend]
    assert b.fetch_new() == []                     # 중복 없음
    # ⭐ **전달된 레코드의 내용까지** 읽힌다. b 는 그 날짜의 로컬 아카이브가 없고
    # 파일도 지워졌는데, 히스토리에서 꺼내기 때문이다 (`_record_from_history`).
    assert [r.payload["n"] for r in b.fetch_new(advance=False)] == []
    for rid in got:
        assert b._read_record(rid, b._head()) is not None, rid

    # ⚠️ 다만 **훑어보기**(history)에는 그 날이 아직 없다 — 라이브도 아니고 로컬
    # 아카이브도 없기 때문이다. 그것을 메우는 것이 복구다.
    day = day_of(weekend[0])
    assert [r.id for r in b.history(fresh=False) if R.day_of(r.id) == day] == []
    assert b.recover_archive(day)["added"] == 6
    assert [
        r.payload["n"] for r in b.history(fresh=False) if R.day_of(r.id) == day
    ] == list(range(6))


def test_first_time_consumer_sees_live_history(participant, homes):
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(5)])
    live = a.append({"n": "today"}, flush=True)

    # 아직 아무것도 지우지 않았다 → 새 소비자는 전부 본다
    c = participant("c", consumer="brandnew")
    assert [r.id for r in c.fetch_new()] == [r.id for r in old] + [live.id]


def test_a_newcomer_after_a_drop_gets_the_past_by_recovering(participant, homes):
    """⭐ 지워진 뒤에 **처음 합류한** 참가자는 무엇을 보는가 — 정직하게 못 박는다.

    아카이브가 공유되지 않으므로, 갓 클론한 사람의 날짜 축에는 *살아 있는 날짜*만
    있다. 그래서 첫 소비는 살아 있는 레코드만 준다. 지워진 과거는 **히스토리에
    그대로 있고**, 빈 날짜를 훑어 복구하면(`archive_gaps` + `recover_archive` —
    소비자의 기동 직후 1회 경로) 그 대화가 돌아온다.

    ⚠️ 이 성질을 문서화하지 않으면 "과거가 조용히 사라졌다"로 읽힌다. 사라지지
    않았고, 꺼내는 자리가 있다.
    """
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(5)])
    live = a.append({"n": "today"}, flush=True)
    day = day_of(old[0])
    fold(a)

    c = participant("c", consumer="brandnew")
    assert [r.id for r in c.fetch_new()] == [live.id]      # 살아 있는 것만

    for missing in c.archive_gaps(R.last_closed_day(c.clock.now(), 2.0), max_days=10):
        c.recover_archive(missing)
    assert sorted(c.archived_ids(day)) == sorted(r.id for r in old)
    assert [r.id for r in c.history(fresh=False)] == [r.id for r in old] + [live.id]


# ------------------------------------------- ⭐ 시계가 어긋난 *다른* 참가자


def test_late_record_after_drop_does_not_hide_the_archive(participant):
    """⭐ 이미 지워진 날짜에 남이 뒤늦게 발행 → 그 날의 대화가 **사라지지 않는다**.

    `_day_ids` 의 합집합(라이브 ∪ 로컬 아카이브)이 지키는 성질이다. 라이브만
    보면 아카이브에 든 나머지가 화면에서 통째로 없어진다.
    """
    a = participant("a")
    b = participant("b")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    day = day_of(old[0])
    b.sync()
    fold(a)

    late = write_past(b, 2, [{"n": "late"}])[0]
    assert R.day_of(late.id) == day
    a.sync()

    ids = [r.id for r in a.history(fresh=False)]
    assert late.id in ids
    assert ids == sorted(ids)
    assert len(ids) == len(set(ids)) == 5
    assert a.history(fresh=False)[ids.index(late.id)].payload == {"n": "late"}

    # 다음 아카이빙이 그것을 합집합으로 담고, 그 뒤에 지운다 (한 건도 잃지 않는다)
    arch, drop = fold(a)
    assert drop["dropped"] is True and drop["days"] == [day]
    assert [r.id for r in a.history(fresh=False)] == ids
    assert not [p for p in tree_paths(a) if p.startswith(f"records/{day}/")]
    data = (a.clone_dir / R.archive_path(day)).read_bytes()
    assert [R.line_id(ln) for ln in R.split_lines(data)] == sorted(ids)


def test_late_record_reaches_a_consumer_after_the_second_drop(participant):
    """두 번째 삭제에 실린 늦은 레코드도 소비자에게 **정확히 한 번** 전달된다."""
    a = participant("a")
    b = participant("b")
    reader = participant("r", consumer="reader")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    assert len(reader.fetch_new()) == 3     # 아직 살아 있을 때 받아 둔다
    fold(a)

    late = write_past(b, 2, [{"n": "late"}])[0]
    a.sync()
    fold(a)

    got = [r.id for r in reader.fetch_new()]
    assert got == [late.id]
    assert reader.fetch_new() == []


# ------------------------------------------------------------- ⭐ 경합


def test_two_participants_dropping_the_same_day_converge(participant):
    """⭐ 검증 6 — 동시 삭제: 한쪽 push 거부 → pull 하면 이미 지워짐 → 오류 없이 수렴."""
    a = participant("a")
    b = participant("b")
    old = write_past(a, 2, [{"n": i} for i in range(6)])
    b.sync()
    day = day_of(old[0])
    a.archive_days()
    b.archive_days()
    # 둘의 로컬 아카이브는 **바이트로 같다** (같은 입력 → 같은 출력)
    assert (a.clone_dir / R.archive_path(day)).read_bytes() == (
        b.clone_dir / R.archive_path(day)
    ).read_bytes()

    results = {}
    barrier = threading.Barrier(2)

    def go(name, ch):
        barrier.wait()
        results[name] = ch.drop_days([day])

    ts = [threading.Thread(target=go, args=(n, c)) for n, c in (("a", a), ("b", b))]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    assert not any(t.is_alive() for t in ts)

    a.sync(); b.sync()
    assert a.git.out("rev-parse", "HEAD^{tree}") == b.git.out("rev-parse", "HEAD^{tree}")
    assert not [p for p in tree_paths(a) if p.startswith(f"records/{day}/")]
    assert [r.id for r in a.history(fresh=False)] == [r.id for r in b.history(fresh=False)]
    assert len(a.history(fresh=False)) == 6
    # 한쪽은 지웠고, 다른 쪽은 "지울 것이 없다"거나 경합 뒤 같은 결론에 닿는다
    assert any(r["dropped"] for r in results.values()), results
    for r in results.values():
        assert "error" not in r


def test_drop_never_lands_on_a_moved_remote(participant):
    """push 는 base 가 그대로일 때만 통과한다 → 못 본 레코드가 사라질 수 없다."""
    a = participant("a")
    b = participant("b")
    write_past(a, 2, [{"n": i} for i in range(3)])
    b.sync()
    assert b.archive_days()["archived"], "b 가 그 날짜를 못 봤다"
    base = b._remote_ref()
    day = sorted(b.archive_state())[0]
    commit = b._drop_commit(base, [(day, [])])

    unseen = write_past(a, 2, [{"n": "unseen"}])[0]

    with pytest.raises(gitwire.PushRejected):
        b.git.run("push", "origin", f"{commit}:refs/heads/{b.branch}")

    # 정상 경로는 다시 계산해서(= 새로 온 레코드까지 담고) 착지한다
    b.archive_days()
    res = b.drop_days([day])
    assert res["dropped"] is True and res["attempts"] >= 1
    # ⭐ 못 봤던 레코드가 b 의 로컬 아카이브에 **들어가 있다** (지우기 직전에 한
    # 번 더 담기 때문이다) — 그래서 b 는 그것을 그대로 읽는다.
    assert unseen.id in b.archived_ids(day)
    assert unseen.id in [r.id for r in b.history(fresh=False)]
    # a 는 그 날짜를 옮기지 않았으므로 복구가 필요하다 (규약대로 히스토리에서).
    a.sync()
    assert a.recover_archive(day)["recovered"] is True
    assert unseen.id in [r.id for r in a.history(fresh=False)]


# --------------------------------------------------------------- ⭐ 자동 복구


def test_recover_archive_rebuilds_from_history(participant):
    """⭐ 검증 5 — 응답 안 한 참가자가 삭제를 pull → 히스토리에서 로컬 아카이브 복구."""
    a = participant("a")
    b = participant("b", consumer="reader")
    old = write_past(a, 2, [{"n": i} for i in range(5)])
    day = day_of(old[0])
    b.sync()
    base = b._head()

    fold(a)                                    # b 는 응답하지 않았는데 지워졌다
    b.sync()
    target = b._head()

    # (1) 소비자는 "그 날짜가 지워졌다"를 **캐시된 나열만으로** 알아챈다
    assert b.deleted_days(base, target) == [day]
    assert day not in b.archive_state()

    got = b.recover_archive(day)
    assert got["recovered"] is True and got["added"] == 5 and got["problems"] == []
    assert b.archived_ids(day) == sorted(r.id for r in old)
    # (2) 그리고 과거 메시지가 다시 읽힌다
    assert {r.id: r.payload for r in b.history(fresh=False)} == {
        r.id: r.payload for r in old
    }
    # (3) 멱등하다
    again = b.recover_archive(day)
    assert again["added"] == 0 and again["recovered"] is False


def test_recover_archive_fills_an_incomplete_archive(participant):
    """응답 **뒤에** 늦은 레코드가 더해졌다 → 내 아카이브가 불완전 → 합집합으로 메운다."""
    a = participant("a")
    b = participant("b")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    day = day_of(old[0])
    b.sync()
    b.archive_days()                           # b 는 여기까지만 담았다
    assert len(b.archived_ids(day)) == 3

    late = write_past(a, 2, [{"n": "late"}])[0]
    a.archive_days()
    a.drop_days([day])                         # a 는 4건을 담고 지웠다
    b.sync()

    assert late.id not in b.archived_ids(day)  # b 의 아카이브는 불완전하다
    got = b.recover_archive(day)
    assert got["added"] == 1 and got["problems"] == []
    assert late.id in b.archived_ids(day)
    assert b.history(fresh=False)[-1].payload == {"n": "late"}


def test_recover_archive_says_so_when_history_is_gone(participant):
    """히스토리에 없으면 **조용히 넘기지 않는다** (사유를 돌려준다)."""
    a = participant("a")
    write_past(a, 2, [{"n": 1}])
    got = a.recover_archive("20200101")
    assert got["recovered"] is False and got["added"] == 0
    assert "히스토리에" in got["reason"]
    with pytest.raises(ValueError):
        a.recover_archive("not-a-day")


def test_archive_gaps_is_empty_when_nothing_was_ever_deleted(participant):
    """삭제가 한 번도 없었으면 빈 날짜를 찾지 않는다 (git 호출 1회로 끝낸다).

    갓 만든 방에서 기동마다 `max_days` 번씩 git 을 띄우지 않는 근거다.
    """
    a = participant("a")
    write_past(a, 2, [{"n": i} for i in range(3)])
    assert a.archive_gaps(R.last_closed_day(a.clock.now(), 2.0), max_days=60) == []


def test_archive_gaps_finds_missing_days(participant):
    """기동 직후 훑기 — 라이브도 아니고 로컬 아카이브도 없는 날짜를 집어낸다."""
    a = participant("a")
    b = participant("b", consumer="reader")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    day = day_of(old[0])
    b.sync()
    fold(a)
    b.sync()

    gaps = b.archive_gaps(R.last_closed_day(b.clock.now(), 2.0), max_days=10)
    assert day in gaps
    for missing in gaps:
        b.recover_archive(missing)             # 없던 날짜는 조용히 False
    assert b.archived_ids(day) == sorted(r.id for r in old)
    assert day not in b.archive_gaps(
        R.last_closed_day(b.clock.now(), 2.0), max_days=10
    )


# ------------------------------------------------------------- ⭐ 비동기 / 락


def test_archiving_does_not_block_reads_and_writes(participant, capsys):
    """⭐ 아카이빙이 도는 동안 읽기·쓰기가 막히지 않는다 (시간으로 증명)."""
    a = participant("a", batch_window=3600.0)
    for d in (5, 4, 3, 2):
        write_past(a, d, [{"n": f"{d}-{i}"} for i in range(40)])
    reader = participant("r", consumer="reader")

    stop = threading.Event()
    reads: list[float] = []
    writes: list[float] = []
    done: dict = {}

    def hammer(store, fn, gap=0.0):
        while not stop.is_set():
            t0 = time.perf_counter()
            fn()
            store.append((time.perf_counter() - t0) * 1000)
            if gap:
                time.sleep(gap)

    threads = [
        threading.Thread(
            target=hammer, args=(reads, lambda: reader.history_page(limit=20, fresh=False))
        ),
        threading.Thread(
            target=hammer,
            args=(writes, lambda: a.append({"n": "during"}, flush=False), 0.02),
        ),
    ]
    for t in threads:
        t.start()
    t0 = time.perf_counter()
    done["arch"] = a.archive_days()
    done["drop"] = a.drop_days(done["arch"]["archived"])
    span_ms = (time.perf_counter() - t0) * 1000
    stop.set()
    for t in threads:
        t.join(30)

    assert len(done["arch"]["archived"]) == 4
    assert reads and writes, "읽기/쓰기가 한 번도 완료되지 않았다"
    reads.sort(); writes.sort()
    print(
        f"\n[async] archive+drop={span_ms:.0f}ms  "
        f"read n={len(reads)} p50={reads[len(reads)//2]:.1f}ms max={reads[-1]:.1f}ms  "
        f"write n={len(writes)} p50={writes[len(writes)//2]:.1f}ms max={writes[-1]:.1f}ms"
    )
    assert len(reads) >= 5 and len(writes) >= 5, (len(reads), len(writes))
    assert reads[-1] < span_ms, (reads[-1], span_ms)
    assert writes[-1] < span_ms, (writes[-1], span_ms)
    assert reads[len(reads) // 2] < 500.0
    assert writes[len(writes) // 2] < 200.0


def test_drop_leaves_worktree_and_index_untouched_until_push(participant):
    """삭제 커밋을 만드는 단계는 작업 사본과 인덱스를 한 바이트도 건드리지 않는다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    a.archive_days()
    a.sync()
    base = a._remote_ref()
    head_before = a.git.out("rev-parse", "HEAD")
    status_before = a.git.out("status", "--porcelain")

    a._drop_commit(base, [(day_of(old[0]), [r.id for r in old])])

    assert a.git.out("rev-parse", "HEAD") == head_before
    assert a.git.out("status", "--porcelain") == status_before
    for r in old:
        assert (a.clone_dir / r.id).exists()
    tmp = a.dir / "tmp"
    assert not tmp.exists() or not list(tmp.iterdir())


def test_maybe_archive_is_rate_limited_and_backgrounded(participant):
    a = participant("a", archive_interval=3600.0)
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    assert a.maybe_archive() is True
    th = a._archive_thread
    assert th is not None
    th.join(60)
    assert not th.is_alive()
    assert a.archive_last_error is None
    assert (a.clone_dir / R.archive_path(day_of(old[0]))).exists()
    # 간격 안에서는 다시 띄우지 않는다
    assert a.maybe_archive() is False
    # 그리고 **아무것도 커밋되지 않았다** (자동 경로는 삭제를 하지 않는다)
    assert [p for p in tree_paths(a) if p.startswith("records/")]


def test_auto_archive_can_be_disabled(participant):
    a = participant("a", auto_archive=False)
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    assert a.maybe_archive() is False
    assert not (a.clone_dir / R.archive_path(day_of(old[0]))).exists()


def test_archive_candidate_disappears_once_folded(participant):
    """옮긴 뒤 트리가 그대로면 후보에서 빠진다 (매시간 다시 옮기지 않는다)."""
    a = participant("a", archive_interval=0.0)
    write_past(a, 2, [{"n": i} for i in range(3)])
    assert a.maybe_archive() is True
    a._archive_thread.join(60)
    assert a.maybe_archive() is False, "이미 옮긴 날짜를 다시 후보로 잡았다"


# --------------------------------------------------------------- 안전장치


def test_archive_reads_the_authoritative_blob_not_a_stale_worktree(participant):
    """작업 사본이 훼손돼 있어도 아카이브에는 **git 오브젝트의 정본**이 들어간다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    day = day_of(old[0])
    for r in old[:2]:
        (a.clone_dir / r.id).write_bytes(b"CORRUPTED LOCAL COPY")

    assert a.archive_days()["archived"] == [day]
    lines = R.index_archive((a.clone_dir / R.archive_path(day)).read_bytes())
    assert sorted(lines) == sorted(r.id for r in old)
    for r in old:
        assert json.loads(lines[r.id])["payload"] == r.payload


def test_corrupt_local_archive_is_loud_and_repairable(participant, caplog):
    """로컬 아카이브가 깨지면 **조용히 없는 것이 되지 않는다** (로그 + 복구 가능).

    삭제 뒤에는 로컬 아카이브가 그 날짜의 유일한 사본이므로, 깨진 파일은 그 날을
    읽을 수 없게 만든다. 그 사실을 크게 남기고(`log.error`), 히스토리에서 다시
    채울 수 있어야 한다 — 그것이 `recover_archive()` 다.
    """
    import logging

    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    day = day_of(old[0])
    fold(a)
    (a.clone_dir / R.archive_path(day)).write_bytes(b"corrupted\n")
    a._trees.clear()
    a._archive_memo = (None, {})

    with caplog.at_level(logging.ERROR, logger="gitwire"):
        assert a.history(fresh=False) == []          # 읽히지 않는다 (사실이다)
    assert any("읽을 수 없는 줄" in r.message for r in caplog.records), caplog.text

    got = a.recover_archive(day)
    assert got["added"] == 4 and got["problems"] == []
    a._trees.clear()
    a._archive_memo = (None, {})
    assert {r.id: r.payload for r in a.history(fresh=False)} == {
        r.id: r.payload for r in old
    }


def test_drop_commit_is_an_ordinary_fast_forward(participant):
    """force-push 도 히스토리 재작성도 없다 — 옛 커밋이 그대로 남아 있다."""
    a = participant("a")
    write_past(a, 2, [{"n": i} for i in range(3)])
    before = a.git.out("rev-parse", "HEAD")
    n_before = int(a.git.out("rev-list", "--count", "HEAD"))
    fold(a)
    after = a.git.out("rev-parse", "HEAD")
    assert after != before
    assert a.git.ok("merge-base", "--is-ancestor", before, after)   # ff 관계
    assert int(a.git.out("rev-list", "--count", "HEAD")) == n_before + 1


def test_queued_records_are_flushed_before_drop(participant):
    """삭제는 대기열을 먼저 밀어낸다 — 뒤에 남겨두고 지우지 않는다."""
    a = participant("a", batch_window=3600.0)
    old = write_past(a, 2, [{"n": f"old-{i}"} for i in range(3)])
    arch = a.archive_days()
    queued = a.append({"n": "대기열"})
    assert a.info()["pending"] == 1

    res = a.drop_days(arch["archived"])
    assert res["dropped"] is True
    assert queued.pushed is True, "삭제 전에 대기열을 밀어내지 않았다"
    ids = [r.id for r in a.history(fresh=False)]
    assert ids == sorted([r.id for r in old] + [queued.id])


def test_info_reports_local_archives(participant):
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(2)])
    assert a.info()["archives"] == 0
    a.archive_days()
    got = a.info()
    assert got["archives"] == 1 and got["auto_archive"] is True
    assert got["archive_error"] is None


# ------------------------------------------------------------------ CLI


def test_cli_archive_and_drop(bare_repo, homes, cli_env):
    """`gitwire archive` 가 로컬로 옮기고, `--drop` 이 지우고, `history` 가 읽는다."""
    from conftest import run_cli

    home = homes("cli")
    ch = gitwire.Channel(
        str(bare_repo), home=home, sender="cli", clock=FixedOffsetClock(0.0),
        batch_window=0.0, auto_archive=False,
    ).open()
    old = write_past(ch, 2, [{"n": i} for i in range(4)])
    ch.close()
    day = day_of(old[0])

    r = run_cli(cli_env, "archive", "--repo", str(bare_repo), "--home", str(home))
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["ok"] is True and out["archived"] == [day] and "drop" not in out

    r2 = run_cli(cli_env, "history", "--repo", str(bare_repo), "--home", str(home),
                 "--local")
    assert r2.returncode == 0, r2.stderr
    assert [rec["id"] for rec in json.loads(r2.stdout)["records"]] == [
        rec.id for rec in old
    ]

    r3 = run_cli(cli_env, "archive", "--drop", "--repo", str(bare_repo),
                 "--home", str(home))
    assert r3.returncode == 0, r3.stderr
    assert json.loads(r3.stdout)["drop"]["dropped"] is True

    r4 = run_cli(cli_env, "history", "--repo", str(bare_repo), "--home", str(home),
                 "--local")
    assert [rec["id"] for rec in json.loads(r4.stdout)["records"]] == [
        rec.id for rec in old
    ]


def test_cli_recover_archive(bare_repo, homes, cli_env):
    """`gitwire recover-archive` 가 빈 날짜를 찾아 히스토리에서 채운다."""
    from conftest import run_cli

    a = gitwire.Channel(
        str(bare_repo), home=homes("a"), sender="a", clock=FixedOffsetClock(0.0),
        batch_window=0.0, auto_archive=False,
    ).open()
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    day = day_of(old[0])
    a.archive_days()
    a.drop_days([day])
    a.close()

    home = homes("b")
    b = gitwire.Channel(
        str(bare_repo), home=home, sender="b", clock=FixedOffsetClock(0.0),
        batch_window=0.0, auto_archive=False,
    ).open()
    b.sync()
    assert day not in b.archive_state()
    b.close()

    r = run_cli(cli_env, "recover-archive", "--repo", str(bare_repo),
                "--home", str(home), "--day", day)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["ok"] is True and out["recovered"] and not out["problems"]

    b2 = gitwire.Channel(
        str(bare_repo), home=home, sender="b", clock=FixedOffsetClock(0.0),
        batch_window=0.0, auto_archive=False,
    ).open()
    try:
        assert b2.archived_ids(day) == sorted(r.id for r in old)
    finally:
        b2.close()
