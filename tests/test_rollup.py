"""지난 날짜 롤업 — 실제 git 2-클론 왕복으로 증명한다.

대역(mock)이 아니라 로컬 bare 레포를 원격으로 쓴다. 여기서 증명해야 하는 것들은
전부 *진짜 git 의 동작*(push 거부·fast-forward·rebase·트리 내용)이기 때문이다.

과거 날짜 레코드는 **시계를 뒤로 돌려** 만든다 (`FixedOffsetClock(-N일)`).
파일을 손으로 심지 않고 `append()` 를 그대로 타므로, 실제 발행 경로가 만드는
id·봉투·디렉토리 구조 그대로 검증된다.
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


# --------------------------------------------------------------- 순수 로직


def test_archive_bytes_are_deterministic():
    """같은 레코드 집합 → **바이트 단위로 같은** 아카이브. 동시 롤업의 근거."""
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


def test_canonical_line_refuses_garbage():
    with pytest.raises(R.ArchiveFormatError):
        R.canonical_line(b"not json")
    with pytest.raises(R.ArchiveFormatError):
        R.canonical_line(b"")
    # 여러 줄로 쓰인 봉투는 한 줄로 접힌다
    line = R.canonical_line(b'{\n  "id": "records/20260901/a.json",\n  "payload": 1\n}')
    assert "\n" not in line and json.loads(line)["payload"] == 1


def test_blob_sha1_matches_git(participant):
    """작업 사본 검증에 쓰는 sha1 계산이 git 과 정말 같은가."""
    ch = participant("a")
    data = "안녕 gitwire\n".encode("utf-8")
    (ch.clone_dir / "probe.bin").write_bytes(data)
    assert ch.git.out("hash-object", "--no-filters", "--", "probe.bin") == R.blob_sha1(data)


# --------------------------------------------------------------- 기본 동작


def test_rollup_folds_past_day_and_keeps_today(participant):
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(5)])
    now = [a.append({"n": "today"}, flush=True)]
    day = day_of(old[0])

    res = a.rollup()
    assert res["rolled"] is True
    assert res["days"] == [day] and res["records"] == 5

    paths = tree_paths(a)
    assert R.archive_path(day) in paths
    assert not [p for p in paths if p.startswith(f"records/{day}/")]
    # 오늘은 그대로 살아 있다
    assert now[0].id in paths
    # 작업 사본에서도 실제로 사라졌다 (커밋이 push 된 뒤에만 지워진다)
    assert not (a.clone_dir / old[0].id).exists()
    assert (a.clone_dir / R.archive_path(day)).exists()


def test_rollup_is_noop_when_nothing_closed(participant):
    a = participant("a")
    a.append({"n": 1}, flush=True)
    res = a.rollup()
    assert res["rolled"] is False and res["days"] == []


def test_rollup_skips_days_below_min_records(participant):
    a = participant("a")
    write_past(a, 2, [{"n": 1}])           # 1건 → 접어도 이득 없음
    res = a.rollup(min_records=2)
    assert res["rolled"] is False
    assert list(res["skipped"].values())[0].startswith("레코드 1건")


# --------------------------------------------------------- ⭐ id 안정성


def test_ids_survive_rollup(participant):
    """롤업 전에 받아둔 id 로 조회·before 커서·기존 소비자 커서가 그대로 동작한다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(10)])
    a.append({"n": "today"}, flush=True)

    before_ids = [r.id for r in a.history(fresh=False)]
    mid = old[5].id
    page_before = a.history(before=mid, limit=3, fresh=False)

    a.rollup()

    # (1) 같은 id 로 레코드를 찾을 수 있다 — payload 까지 동일
    got = {r.id: r for r in a.history(fresh=False)}
    assert [r.id for r in a.history(fresh=False)] == before_ids
    for r in old:
        assert got[r.id].payload == r.payload
        assert got[r.id].sender == r.sender
        assert got[r.id].timestamp == r.timestamp

    # (2) before= 커서가 같은 결과를 준다
    assert [r.id for r in a.history(before=mid, limit=3, fresh=False)] == [
        r.id for r in page_before
    ]

    # (3) 페이징 경계·순서가 보존된다
    page = a.history_page(limit=4, fresh=False)
    walked = list(page.records)
    while page.has_more:
        page = a.history_page(before=page.oldest, limit=4, fresh=False)
        walked = list(page.records) + walked
    assert [r.id for r in walked] == before_ids


def test_persisted_consumer_cursor_still_valid(participant):
    """디스크에 이미 영속된 소비자 커서가 롤업 뒤에도 계속 이어진다."""
    a = participant("a")
    first = write_past(a, 2, [{"n": i} for i in range(4)])
    seen = [r.id for r in a.fetch_new()]
    assert seen == [r.id for r in first]
    cur_before = a.cursors.load()

    a.rollup()

    # 커서 파일은 그대로 (우리가 손대지 않는다)
    assert a.cursors.load().watermark == cur_before.watermark
    # 이미 준 것을 다시 주지 않는다
    assert a.fetch_new() == []
    # 이후 새 레코드는 정확히 한 번 온다
    fresh = a.append({"n": "after"}, flush=True)
    assert [r.id for r in a.fetch_new()] == [fresh.id]


def test_offline_consumer_does_not_miss_rolled_up_days(participant, homes):
    """⭐ 주말 내내 꺼져 있던 소비자가 롤업된 날의 대화를 놓치지 않는다.

    diff 는 '추가 후 삭제'를 상쇄해 버린다 — archive/ 의 집합 차이로 메운다.
    """
    a = participant("a")
    b = participant("b", consumer="reader")
    b.fetch_new()                                  # 커서를 '지금'에 맞춰 둔다

    weekend = write_past(a, 2, [{"n": i} for i in range(6)])
    a.rollup()                                     # b 가 자는 사이에 접혔다

    got = [r.id for r in b.fetch_new()]
    assert got == [r.id for r in weekend]
    assert b.fetch_new() == []                     # 중복 없음


def test_first_time_consumer_sees_archived_history(participant, homes):
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(5)])
    live = a.append({"n": "today"}, flush=True)
    a.rollup()

    c = participant("c", consumer="brandnew")
    got = [r.id for r in c.fetch_new()]
    assert got == [r.id for r in old] + [live.id]


# --------------------------------------------------- ⭐ 뒤늦게 도착한 과거 레코드


def test_late_record_into_rolled_up_day(participant):
    """이미 접힌 날짜에 나중에 레코드가 도착 → 보이고, 순서 맞고, 중복 없다."""
    a = participant("a")
    b = participant("b")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    day = day_of(old[0])
    a.rollup()

    # b 가 (오프라인이었다가) 같은 날짜에 뒤늦게 발행한다
    late = write_past(b, 2, [{"n": "late"}])[0]
    assert R.day_of(late.id) == day
    a.sync()

    ids = [r.id for r in a.history(fresh=False)]
    assert late.id in ids
    assert ids == sorted(ids)                      # 시간순 유지
    assert len(ids) == len(set(ids)) == 5          # 중복 없음
    assert a.history(fresh=False)[ids.index(late.id)].payload == {"n": "late"}

    # 다음 롤업이 그것을 아카이브에 합쳐 넣는다 (한 건도 잃지 않는다)
    res = a.rollup()
    assert res["rolled"] is True and res["days"] == [day]
    ids2 = [r.id for r in a.history(fresh=False)]
    assert ids2 == ids
    assert not [p for p in tree_paths(a) if p.startswith(f"records/{day}/")]

    # 아카이브 한 파일 안에 5건이 id 순으로 들어 있다
    data = (a.clone_dir / R.archive_path(day)).read_bytes()
    got = [R.line_id(ln) for ln in R.split_lines(data)]
    assert got == sorted(ids)


def test_late_record_reaches_a_consumer_after_second_rollup(participant):
    """두 번째 롤업으로 아카이브에 합쳐진 늦은 레코드도 소비자에게 한 번 전달된다."""
    a = participant("a")
    b = participant("b")
    reader = participant("r", consumer="reader")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    a.rollup()
    assert len(reader.fetch_new()) == 3

    late = write_past(b, 2, [{"n": "late"}])[0]
    a.sync()
    a.rollup(min_records=1)                        # 곧바로 아카이브에 합친다

    got = [r.id for r in reader.fetch_new()]
    assert got == [late.id]
    assert reader.fetch_new() == []


# ------------------------------------------------------- ⭐ 동시 롤업 (조정 없음)


def test_concurrent_rollup_same_set_converges(participant):
    """같은 집합을 본 두 클론이 같은 날을 동시에 롤업 → 같은 트리로 수렴."""
    a = participant("a")
    b = participant("b")
    old = write_past(a, 2, [{"n": i} for i in range(6)])
    b.sync()
    day = day_of(old[0])

    base_a = a._remote_ref()
    base_b = b._remote_ref()
    assert base_a == base_b

    # 두 참가자가 같은 base 에서 각자 계산한 아카이브 바이트가 **동일**하다
    plan_a, _ = a._rollup_plan(base_a, grace_hours=2.0, min_records=2)
    plan_b, _ = b._rollup_plan(base_b, grace_hours=2.0, min_records=2)
    assert [(d, c) for d, c, _ in plan_a] == [(d, c) for d, c, _ in plan_b]

    results = {}
    barrier = threading.Barrier(2)

    def go(name, ch):
        barrier.wait()
        results[name] = ch.rollup()

    ts = [threading.Thread(target=go, args=(n, c)) for n, c in (("a", a), ("b", b))]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)

    assert not any(t.is_alive() for t in ts)
    # 둘 다 예외 없이 끝났고, 최종 상태는 하나다
    a.sync(); b.sync()
    tree_a = a.git.out("rev-parse", "HEAD^{tree}")
    tree_b = b.git.out("rev-parse", "HEAD^{tree}")
    assert tree_a == tree_b
    assert [r.id for r in a.history(fresh=False)] == [r.id for r in b.history(fresh=False)]
    assert len(a.history(fresh=False)) == 6
    assert R.archive_path(day) in tree_paths(a)


def test_concurrent_rollup_different_sets_loses_nothing(participant):
    """서로 다른 집합을 본 두 클론이 동시에 롤업 → 합집합으로 수렴, 유실 0."""
    a = participant("a")
    b = participant("b")
    shared = write_past(a, 2, [{"n": i} for i in range(4)])
    b.sync()
    day = day_of(shared[0])

    # b 만 알고 있는 레코드를 같은 날짜에 하나 더 만든다 (아직 a 는 모른다)
    extra = write_past(b, 2, [{"n": "b-only"}])[0]
    assert R.day_of(extra.id) == day
    # a 는 일부러 낡은 상태로 계산하게 둔다 (a 는 extra 를 못 봤다)
    assert extra.id not in [r.id for r in a.history(fresh=False)]

    results = {}
    barrier = threading.Barrier(2)

    def go(name, ch):
        barrier.wait()
        results[name] = ch.rollup()

    ts = [threading.Thread(target=go, args=(n, c)) for n, c in (("a", a), ("b", b))]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    assert not any(t.is_alive() for t in ts)

    a.sync(); b.sync()
    assert a.git.out("rev-parse", "HEAD^{tree}") == b.git.out("rev-parse", "HEAD^{tree}")
    ids = [r.id for r in a.history(fresh=False)]
    assert sorted(ids) == sorted([r.id for r in shared] + [extra.id])
    assert len(ids) == len(set(ids))
    # 접혔든 아직 안 접혔든, 다섯 건 모두 읽힌다
    assert {r.payload["n"] for r in a.history(fresh=False)} == {0, 1, 2, 3, "b-only"}
    # 한 번 더 돌리면 완전히 접힌다
    a.rollup(min_records=1)
    assert not [p for p in tree_paths(a) if p.startswith(f"records/{day}/")]
    assert len([r for r in a.history(fresh=False)]) == 5


def test_rollup_never_lands_on_a_moved_remote(participant):
    """push 는 base 가 그대로일 때만 통과한다 → 못 본 레코드가 사라질 수 없다."""
    a = participant("a")
    b = participant("b")
    write_past(a, 2, [{"n": i} for i in range(3)])
    b.sync()
    base = b._remote_ref()
    plan, _ = b._rollup_plan(base, grace_hours=2.0, min_records=2)
    commit = b._rollup_commit(base, plan)

    # b 가 커밋을 만든 뒤, a 가 원격을 움직인다
    unseen = write_past(a, 2, [{"n": "unseen"}])[0]

    with pytest.raises(gitwire.PushRejected):
        b.git.run("push", "origin", f"{commit}:refs/heads/{b.branch}")

    # 정상 경로(rollup)는 다시 계산해서 합집합으로 착지한다
    res = b.rollup(min_records=1)
    assert res["rolled"] is True and res["attempts"] >= 1
    a.sync()
    assert unseen.id in [r.id for r in a.history(fresh=False)]


# ------------------------------------------------------------- ⭐ 비동기 / 락


def test_rollup_does_not_block_reads_and_writes(participant, capsys):
    """⭐ 롤업이 도는 동안 읽기·쓰기가 막히지 않는다 (시간으로 증명).

    * 읽기: 다른 클론이 계속 `history_page()` 를 친다.
    * 쓰기: **같은 클론**이 `append()` 를 계속 한다 (채널 락을 공유하는 쪽이라
      막힌다면 여기서 막힌다). batch_window 를 크게 잡아 push 비용을 배제하고
      **락 대기 시간만** 남긴다.
    """
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
        # ⚠️ 쓰기에는 간격을 준다. 간격 없이 때리면 미커밋 레코드가 `max_batch`(200)를
        # 넘겨 `append()` 자신이 flush·push 를 트리거하고, 그 push 가 롤업 push 와
        # 경합해 이번 롤업이 양보한다(유실은 없지만 이 테스트의 관심사가 아니다).
        threading.Thread(
            target=hammer,
            args=(writes, lambda: a.append({"n": "during"}, flush=False), 0.02),
        ),
    ]
    for t in threads:
        t.start()
    t0 = time.perf_counter()
    done["res"] = a.rollup()
    rollup_ms = (time.perf_counter() - t0) * 1000
    stop.set()
    for t in threads:
        t.join(30)

    assert done["res"]["rolled"] is True and len(done["res"]["days"]) == 4
    assert reads and writes, "읽기/쓰기가 한 번도 완료되지 않았다"
    reads.sort(); writes.sort()
    print(
        f"\n[async] rollup={rollup_ms:.0f}ms  "
        f"read n={len(reads)} p50={reads[len(reads)//2]:.1f}ms max={reads[-1]:.1f}ms  "
        f"write n={len(writes)} p50={writes[len(writes)//2]:.1f}ms max={writes[-1]:.1f}ms"
    )
    # (1) 롤업이 도는 **내내** 읽기·쓰기가 계속 완료됐다 (통째로 멈추지 않았다)
    assert len(reads) >= 5 and len(writes) >= 5, (len(reads), len(writes))
    # (2) 어느 한 호출도 롤업 전체를 기다리지 않았다 (= 락 뒤에 줄 서 있지 않았다)
    assert reads[-1] < rollup_ms, (reads[-1], rollup_ms)
    assert writes[-1] < rollup_ms, (writes[-1], rollup_ms)
    # (3) 대부분의 호출은 락 경합 없이 지나간다 (중앙값)
    assert reads[len(reads) // 2] < 500.0
    assert writes[len(writes) // 2] < 200.0


def test_rollup_leaves_worktree_and_index_untouched_until_push(participant):
    """계획·커밋 단계는 작업 사본과 인덱스를 한 바이트도 건드리지 않는다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    a.sync()
    base = a._remote_ref()
    head_before = a.git.out("rev-parse", "HEAD")
    status_before = a.git.out("status", "--porcelain")

    plan, _ = a._rollup_plan(base, grace_hours=2.0, min_records=2)
    a._rollup_commit(base, plan)                   # push 하지 않는다

    assert a.git.out("rev-parse", "HEAD") == head_before
    assert a.git.out("status", "--porcelain") == status_before
    for r in old:                                  # 원본은 그대로 있다
        assert (a.clone_dir / r.id).exists()
    # 임시 인덱스·스크래치 파일이 남지 않는다
    tmp = a.dir / "tmp"
    assert not tmp.exists() or not list(tmp.iterdir())


def test_maybe_rollup_is_rate_limited_and_backgrounded(participant):
    a = participant("a", rollup_interval=3600.0)
    write_past(a, 2, [{"n": i} for i in range(3)])
    assert a.maybe_rollup() is True
    th = a._rollup_thread
    assert th is not None
    th.join(60)
    assert not th.is_alive()
    assert a.rollup_last_error is None
    assert [p for p in tree_paths(a) if p.startswith("archive/")]
    # 간격 안에서는 다시 띄우지 않는다
    assert a.maybe_rollup() is False


def test_auto_rollup_can_be_disabled(participant):
    a = participant("a", auto_rollup=False)
    write_past(a, 2, [{"n": i} for i in range(3)])
    assert a.maybe_rollup() is False
    assert not [p for p in tree_paths(a) if p.startswith("archive/")]


# --------------------------------------------------------------- 안전장치


def test_rollup_refuses_a_day_it_cannot_fold(participant):
    """접을 수 없는 바이트가 있으면 그 날짜는 통째로 건너뛴다 (지우지 않는다)."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(3)])
    day = day_of(old[0])
    # 그 날짜에 JSON 이 아닌 파일을 심는다 (다른 도구가 남긴 쓰레기를 흉내)
    bad = a.clone_dir / f"records/{day}/20260101T000000000Z-x-zzzzzz.json"
    bad.write_bytes(b"NOT JSON AT ALL")
    a.git.run("add", "-A", "--", "records")
    a.git.run("commit", "-m", "bad")
    a.flush()

    res = a.rollup()
    assert res["rolled"] is False
    assert day in res["skipped"]
    assert bad.exists()                                  # 아무것도 지우지 않았다
    for r in old:
        assert (a.clone_dir / r.id).exists()


def test_rollup_reads_the_authoritative_blob_not_a_stale_worktree(participant):
    """작업 사본이 훼손돼 있어도 아카이브에는 **git 오브젝트의 정본**이 들어간다.

    (파일을 지우는 시나리오는 쓰지 않는다 — gitwire 는 작업 사본의 삭제를 진짜
    삭제로 흡수하므로 그건 롤업이 아니라 발행 경로의 이야기다.)
    """
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    a.sync()
    base = a._remote_ref()
    for r in old[:2]:
        (a.clone_dir / r.id).write_bytes(b"CORRUPTED LOCAL COPY")

    plan, skipped = a._rollup_plan(base, grace_hours=2.0, min_records=2)
    assert not skipped and len(plan) == 1
    lines = R.index_archive(plan[0][1])
    assert sorted(lines) == sorted(r.id for r in old)
    for r in old:
        assert json.loads(lines[r.id])["payload"] == r.payload


def test_archive_read_falls_back_when_worktree_is_stale(participant):
    """작업 사본의 아카이브가 내용이 다르면 정본(git)으로 되돌아간다."""
    a = participant("a")
    old = write_past(a, 2, [{"n": i} for i in range(4)])
    a.rollup()
    day = day_of(old[0])
    (a.clone_dir / R.archive_path(day)).write_bytes(b"corrupted\n")
    a._trees.clear()
    a._archive_memo = (None, {})
    got = {r.id: r.payload for r in a.history(fresh=False)}
    assert got == {r.id: r.payload for r in old}


def test_record_ids_and_paging_span_archives(participant):
    a = participant("a")
    made = []
    for d in (4, 3, 2):
        made += write_past(a, d, [{"n": f"{d}-{i}"} for i in range(5)])
    made += [a.append({"n": "today"}, flush=True)]
    all_ids = [r.id for r in made]

    a.rollup()

    assert a.record_ids(fresh=False) == all_ids
    # 페이지 크기 3으로 끝까지 걸어도 정확히 같은 순서·같은 집합
    walked = []
    page = a.history_page(limit=3, fresh=False)
    while True:
        walked = [r.id for r in page.records] + walked
        if not page.has_more:
            break
        page = a.history_page(before=page.oldest, limit=3, fresh=False)
    assert walked == all_ids


def test_rollup_commit_is_an_ordinary_fast_forward(participant):
    """force-push 도 히스토리 재작성도 없다 — 옛 커밋이 그대로 남아 있다."""
    a = participant("a")
    write_past(a, 2, [{"n": i} for i in range(3)])
    before = a.git.out("rev-parse", "HEAD")
    n_before = int(a.git.out("rev-list", "--count", "HEAD"))
    a.rollup()
    after = a.git.out("rev-parse", "HEAD")
    assert after != before
    assert a.git.ok("merge-base", "--is-ancestor", before, after)   # ff 관계
    assert int(a.git.out("rev-list", "--count", "HEAD")) == n_before + 1


def test_unpushed_records_are_flushed_before_rollup(participant):
    """롤업은 내 미푸시 레코드를 먼저 올린다 — 뒤에 남겨두고 접지 않는다."""
    a = participant("a", batch_window=3600.0)       # 자동 flush 를 사실상 끔
    a.clock = past(2)
    pending = [a.append({"n": i}) for i in range(3)]
    a.clock = FixedOffsetClock(0.0)
    assert a._pending
    res = a.rollup(min_records=1)
    assert res["rolled"] is True
    ids = [r.id for r in a.history(fresh=False)]
    assert ids == [r.id for r in pending]


# ------------------------------------------------------------------ CLI


def test_cli_rollup(bare_repo, homes, cli_env):
    """`gitwire rollup` 이 실제로 접고, 그 뒤 `history` 가 그대로 읽는다."""
    from conftest import run_cli

    home = homes("cli")
    ch = gitwire.Channel(
        str(bare_repo), home=home, sender="cli", clock=FixedOffsetClock(0.0),
        batch_window=0.0, auto_rollup=False,
    ).open()
    old = write_past(ch, 2, [{"n": i} for i in range(4)])
    ch.close()
    day = day_of(old[0])

    r = run_cli(cli_env, "rollup", "--repo", str(bare_repo), "--home", str(home))
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["ok"] is True and out["rolled"] is True and out["days"] == [day]

    r2 = run_cli(cli_env, "history", "--repo", str(bare_repo), "--home", str(home),
                 "--local")
    assert r2.returncode == 0, r2.stderr
    got = json.loads(r2.stdout)
    assert [rec["id"] for rec in got["records"]] == [rec.id for rec in old]

    # 두 번째 호출은 접을 것이 없다 (멱등)
    r3 = run_cli(cli_env, "rollup", "--repo", str(bare_repo), "--home", str(home))
    assert json.loads(r3.stdout)["rolled"] is False
