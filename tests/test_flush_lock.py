"""push(네트워크)를 채널 락 밖에서 한다 — 그 성질을 고정하는 회귀 테스트.

느린 원격은 `GitRunner` 를 감싸 `push` 에만 지연을 넣어 흉내낸다. 나머지는 전부
진짜 git·진짜 bare 레포다 (락 문제는 대역으로 흉내내면 아무것도 증명하지 못한다).

⚠️ 여기서 지켜야 하는 것은 두 가지이고 **둘 다** 증명해야 한다:
  (1) 빠르다 — push 가 도는 동안 `append()`·읽기가 막히지 않는다
  (2) 맞다   — 그동안 들어온 레코드가 유실·중복 없이 전부 원격에 착지한다
빠르기만 하고 유실되면 그건 고친 게 아니다.
"""

from __future__ import annotations

import threading
import time

import gitwire
from gitwire.clock import FixedOffsetClock
from gitwire.gitcmd import SubprocessGitRunner

SLOW = 1.0


class SlowPushRunner(SubprocessGitRunner):
    """`push` 에만 네트워크 지연을 넣는다 (실측 GitHub private repo 는 2~3초)."""

    def __init__(self, delay: float = SLOW) -> None:
        super().__init__()
        self.delay = delay
        self.pushes = 0
        self.started = threading.Event()

    def run(self, args, **kw):
        if any(a == "push" for a in args):
            self.pushes += 1
            self.started.set()
            time.sleep(self.delay)
        return super().run(args, **kw)


def landed(bare_repo, homes, name="verify") -> list[str]:
    """원격에 **정말** 올라간 레코드 id (별도 클론으로 확인한다)."""
    ch = gitwire.Channel(
        str(bare_repo), home=homes(name), sender=name, consumer=name,
        clock=FixedOffsetClock(0.0), auto_rollup=False,
    ).open()
    try:
        return [r.id for r in ch.history(fresh=True)]
    finally:
        ch.close()


# ------------------------------------------------------------------ (1) 빠르다


def test_push_does_not_block_append_or_reads(participant, capsys):
    """push 가 도는 동안 `append()` 와 읽기가 계속 돌아간다 (시간으로 증명)."""
    runner = SlowPushRunner()
    a = participant("a", runner=runner, batch_window=3600.0)
    a.append({"n": "seed"})

    pushing = threading.Thread(target=a.flush, daemon=True)
    pushing.start()
    assert runner.started.wait(10), "push 가 시작되지 않았다"
    time.sleep(0.05)                       # push 한복판에서 잰다

    sends, reads = [], []
    for i in range(6):
        t0 = time.perf_counter()
        rec = a.append({"n": i})
        sends.append((time.perf_counter() - t0) * 1000)
        assert rec.id                      # 응답만 빠르고 빈 것이면 의미가 없다
        t1 = time.perf_counter()
        a.history_page(limit=10, fresh=False)
        reads.append((time.perf_counter() - t1) * 1000)
    pushing.join(30)

    print(f"\n[flush] push={runner.delay*1000:.0f}ms  "
          f"append max={max(sends):.1f}ms  read max={max(reads):.1f}ms")
    # push 한 번(1000ms)이 통째로 얹히지 않았다
    assert max(sends) < runner.delay * 1000 * 0.5, sends
    assert max(reads) < runner.delay * 1000 * 0.5, reads


def test_flush_still_serializes_pushes(participant):
    """빨라졌다고 push 가 겹치지는 않는다 (`_remote` 가 직렬화한다)."""
    runner = SlowPushRunner(0.4)
    a = participant("a", runner=runner, batch_window=3600.0)
    overlap = []
    live = []
    orig = runner.run

    def watched(args, **kw):
        if any(x == "push" for x in args):
            live.append(1)
            overlap.append(len(live))
            try:
                return orig(args, **kw)
            finally:
                live.pop()
        return orig(args, **kw)

    runner.run = watched
    for i in range(3):
        a.append({"n": i})
    ths = [threading.Thread(target=a.flush) for _ in range(4)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(30)
    assert overlap, "push 가 한 번도 일어나지 않았다"
    assert max(overlap) == 1, f"push 가 겹쳤다: {overlap}"


# -------------------------------------------------------------------- (2) 맞다


def test_records_appended_during_push_are_not_lost(participant, bare_repo, homes):
    """push 도중에 들어온 레코드가 전부, 한 번씩, 순서대로 원격에 착지한다."""
    runner = SlowPushRunner()
    a = participant("a", runner=runner, batch_window=3600.0)
    first = a.append({"n": "first"})

    pushing = threading.Thread(target=a.flush, daemon=True)
    pushing.start()
    assert runner.started.wait(10)
    during = [a.append({"n": i}) for i in range(5)]
    pushing.join(30)

    a.flush()                              # 그 사이 들어온 것들을 마저 올린다
    got = landed(bare_repo, homes)
    expect = [r.id for r in [first, *during]]
    # ⚠️ 발행 순서가 아니라 **id 정렬 순서**와 비교한다. 같은 밀리초에 두 건이
    # 나면 그 둘 사이의 순서는 nonce 로 갈리고(모든 참가자에게 동일하다),
    # 발행 순서와 다를 수 있다 — 그건 규약이지 결함이 아니다.
    assert got == sorted(expect)           # 유실·중복 없음 + 읽기는 시간순
    assert len(got) == len(set(got))


def test_pushed_marker_tracks_what_was_actually_pushed(participant, bare_repo, homes):
    """`_mark_pushed` 가 *올린 sha* 를 기록한다 — 안 올린 것을 올렸다고 하지 않는다.

    락 밖에서 `HEAD:` 로 밀고 나서 그때의 HEAD 를 표시하면, push 중에 늘어난
    커밋까지 "올렸다"가 되어 **조용히 유실**된다. 명시 sha 로 미는 이유다.
    """
    runner = SlowPushRunner()
    a = participant("a", runner=runner, batch_window=3600.0)
    a.append({"n": 0})
    a.flush()
    pushed = a.git.out("rev-parse", "refs/gitwire/pushed")
    assert pushed == a.git.out("rev-parse", "HEAD")

    late = a.append({"n": 1})
    a._absorb_worktree()                   # 커밋만 만들고 push 는 하지 않는다
    assert a._unpushed_count() == 1        # 표시가 정확해야 이게 1이 된다
    a.flush()
    assert late.id in landed(bare_repo, homes)


def test_concurrent_participants_do_not_lose_records(participant, bare_repo, homes):
    """두 참가자가 동시에 느린 push 를 해도(선점 → rebase 재시도) 유실이 없다."""
    a = participant("a", runner=SlowPushRunner(0.3), batch_window=3600.0)
    b = participant("b", runner=SlowPushRunner(0.3), batch_window=3600.0)
    made = []
    barrier = threading.Barrier(2)

    def go(ch, tag):
        recs = [ch.append({"who": tag, "n": i}) for i in range(4)]
        made.extend(recs)
        barrier.wait()
        ch.flush()

    ths = [threading.Thread(target=go, args=(c, t)) for c, t in ((a, "a"), (b, "b"))]
    for t in ths:
        t.start()
    for t in ths:
        t.join(60)
    assert not any(t.is_alive() for t in ths)

    got = landed(bare_repo, homes)
    assert sorted(got) == sorted(r.id for r in made)
    assert got == sorted(got)
    assert len(got) == len(set(got))


def test_sync_does_not_interleave_with_a_running_push(participant, bare_repo, homes):
    """push 중에 `sync()` 가 끼어들어 방금 올린 커밋을 갈아치우지 않는다."""
    runner = SlowPushRunner()
    a = participant("a", runner=runner, batch_window=3600.0)
    b = participant("b", batch_window=0.0)
    mine = [a.append({"n": i}) for i in range(3)]
    theirs = b.append({"n": "b"})          # 원격이 앞서 있다 → a 의 push 는 거부된다

    pushing = threading.Thread(target=a.flush, daemon=True)
    pushing.start()
    assert runner.started.wait(10)
    syncing = [threading.Thread(target=a.sync) for _ in range(3)]
    for t in syncing:
        t.start()
    pushing.join(60)
    for t in syncing:
        t.join(60)
    assert not any(t.is_alive() for t in [pushing, *syncing])

    got = landed(bare_repo, homes)
    assert sorted(got) == sorted([r.id for r in mine] + [theirs.id])
    assert len(got) == len(set(got))


def test_rollup_overlapping_with_flush_loses_nothing(participant, bare_repo, homes):
    """롤업(로컬을 크게 바꾼다)과 발행·flush 가 겹쳐도 유실·중복이 없다."""
    from test_rollup import write_past   # noqa: PLC0415

    a = participant("a", batch_window=3600.0)
    old = write_past(a, 2, [{"n": f"old-{i}"} for i in range(6)])

    made = list(old)
    stop = threading.Event()
    err: list[BaseException] = []

    def writer():
        i = 0
        while not stop.is_set():
            try:
                made.append(a.append({"n": f"live-{i}"}))
                i += 1
                a.flush()
            except BaseException as exc:  # noqa: BLE001
                err.append(exc)
                return
            time.sleep(0.2)

    th = threading.Thread(target=writer, daemon=True)
    th.start()
    res = a.rollup()
    stop.set()
    th.join(30)
    a.flush()

    assert not err, err
    # 롤업이 이번에 졌을 수도 있다(발행이 계속 원격을 움직인다). 그것 자체는
    # 정상이다 — 유실이 없고, 조용해지면 다음 시도가 접는다.
    if not res["rolled"]:
        assert res["reason"].startswith("push 경합"), res
        res = a.rollup()
    assert res["rolled"] is True, res
    got = landed(bare_repo, homes)
    assert sorted(got) == sorted(r.id for r in made), (
        set(r.id for r in made) - set(got)
    )
    assert len(got) == len(set(got))
    # 롤업된 날짜도 그대로 읽힌다
    assert {r.id for r in a.history(fresh=True)} == {r.id for r in made}


def test_compact_does_not_deadlock_with_a_running_flusher(participant, bare_repo, homes):
    """`compact()` 는 `_remote` → `_lock` 순서를 지킨다 (교착이 없다)."""
    a = participant("a", batch_window=0.05)
    recs = [a.append({"n": i}) for i in range(5)]
    a.flush()
    done = {}

    def go():
        done["res"] = a.compact(confirm=True)

    th = threading.Thread(target=go)
    th.start()
    for i in range(5):
        a.append({"n": f"during-{i}"})
        time.sleep(0.01)
    th.join(60)
    assert not th.is_alive(), "compact 가 끝나지 않았다 (교착 의심)"
    assert done["res"]["compacted"] is True
    a.flush()
    assert recs[0].id in [r.id for r in a.history(fresh=True)]


def test_close_flushes_without_deadlock(participant, bare_repo, homes):
    a = participant("a", runner=SlowPushRunner(0.3), batch_window=3600.0)
    recs = [a.append({"n": i}) for i in range(3)]
    a.close()
    assert sorted(landed(bare_repo, homes)) == sorted(r.id for r in recs)
