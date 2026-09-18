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

import pytest

import gitwire
from gitwire.clock import FixedOffsetClock
from gitwire import localrefs
from gitwire.errors import GitError
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


def landed(bare_repo, homes, name="verify", *, recover=()) -> list[str]:
    """원격에 **정말** 올라간 레코드 id (별도 클론으로 확인한다).

    ⚠️ `recover=` 는 **이미 지워진 날짜**를 확인할 때 필요하다. 갓 만든 클론에는
    그 날짜의 로컬 아카이브가 없고(아카이브는 공유되지 않는다) 레코드 파일도
    지워져 있으므로, 날짜 축에 그 날이 아예 없다. 지워진 레코드는 **히스토리에
    그대로** 있으므로 거기서 꺼내 온다 — 그게 규약이고(`recover_archive`),
    그래서 "원격에 다 있다"를 여기서 정직하게 확인할 수 있다.
    """
    ch = gitwire.Channel(
        str(bare_repo), home=homes(name), sender=name, consumer=name,
        clock=FixedOffsetClock(0.0), auto_archive=False,
    ).open()
    try:
        for day in recover:
            got = ch.recover_archive(day)
            assert not got["problems"], got
        return [r.id for r in ch.history(fresh=True)]
    finally:
        ch.close()


# ------------------------------------------------------------------ (1) 빠르다


def test_push_does_not_block_append_or_reads(participant, capsys):
    """push 가 도는 동안 `append()` 와 읽기가 계속 돌아간다 (시간으로 증명)."""
    runner = SlowPushRunner()
    a = participant("a", runner=runner, autopublish=False)
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
        # 응답만 빠르고 빈 것이면 의미가 없다. id 는 아직 없는 것이 정상이므로
        # (push 때 정해진다) 대기열에 **실제로 들어갔는지**를 본다.
        assert rec.pushed is False and rec.payload == {"n": i}
        t1 = time.perf_counter()
        a.history_page(limit=10, fresh=False)
        reads.append((time.perf_counter() - t1) * 1000)
    pushing.join(30)

    print(f"\n[flush] push={runner.delay*1000:.0f}ms  "
          f"append max={max(sends):.1f}ms  read max={max(reads):.1f}ms")
    # push 한 번(1000ms)이 통째로 얹히지 않았다
    assert max(sends) < runner.delay * 1000 * 0.5, sends
    assert max(reads) < runner.delay * 1000 * 0.5, reads


def test_sends_during_a_push_ride_one_commit(participant, bare_repo, homes):
    """⭐ 창이 없어도 묶인다 — push 가 **도는 동안** 들어온 3건이 한 커밋으로 나간다.

    드레인 루프의 요점이 이 하나다. 대기열이 비어 있으면 부르는 쪽이 그 자리에서
    밀고(그래서 조용한 방의 한 건은 기다리지 않는다), 미는 동안 들어온 것들은
    쌓여서 **다음 회차에 한 커밋으로** 나간다. 묶음 크기가 부하에 맞춰 저절로
    정해지므로 조율할 숫자가 없다.

    ⚠️ "미는 동안"을 만들려면 첫 건을 **다른 스레드**에서 보내야 한다 — 한
    스레드에서 연달아 보내면 각 건이 자기 push 를 기다렸다 나가고, 그건 묶임이
    아니라 정상 동작이다(창이 없으니 미룰 이유가 없다).
    """
    runner = SlowPushRunner(1.0)
    a = participant("a", runner=runner)              # autopublish = 기본(켜짐)
    before = int(a.git.out("rev-list", "--count", "HEAD"))
    box: dict = {}

    def send_first():
        box["t"] = a.append({"n": "first"})          # 이 호출이 push 를 돌린다

    runner.started.clear()                           # 방 초기화 push 는 제외
    th = threading.Thread(target=send_first, daemon=True)
    th.start()
    assert runner.started.wait(10), "push 가 시작되지 않았다"

    took = []
    during = []
    for i in range(3):
        t0 = time.perf_counter()
        during.append(a.append({"n": i}))            # 미는 동안 들어온다
        took.append((time.perf_counter() - t0) * 1000)
    th.join(30)
    assert not th.is_alive()
    for t in [box["t"], *during]:
        assert t.wait(30) is not None, "안 나갔다"

    after = int(a.git.out("rev-list", "--count", "HEAD"))
    assert after - before == 2, (
        f"커밋 {after - before}개 — 1건 + 3건(한 커밋) = 2 여야 한다"
    )
    print(f"\n[drain] push={runner.delay*1000:.0f}ms  "
          f"미는 동안 append max={max(took):.1f}ms  커밋={after - before}개")
    # 미는 동안의 발행은 그 push 를 기다리지 않는다
    assert max(took) < runner.delay * 1000 * 0.8, took
    # 유실·중복 없음 + id 는 발행 순서대로
    got = landed(bare_repo, homes)
    expect = [t.id for t in [box["t"], *during]]
    assert got == sorted(expect)
    assert len(got) == len(set(got))


def test_flush_still_serializes_pushes(participant):
    """빨라졌다고 push 가 겹치지는 않는다 (`_remote` 가 직렬화한다)."""
    runner = SlowPushRunner(0.4)
    a = participant("a", runner=runner, autopublish=False)
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
    a = participant("a", runner=runner, autopublish=False)
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


def test_a_failed_publish_keeps_retrying_without_lying(participant, bare_repo, homes, caplog):
    """밀 수 없었던 건은 **조용히 사라지지도, 실패했다고 거짓말하지도 않는다.**

    네 가지를 함께 못 박는다:

    1. `append()` 는 전송 실패로 **예외를 올리지 않는다.** 발행(대기열에 넣기)은
       성공했고 전송만 못 한 것이다 — 여기서 예외를 올리면 소비자가 그것을
       "보낼 수 없었다"로 사용자에게 말하고, 그 뒤 배경 재시도가 성공해서
       **"실패했다더니 나갔다"** 가 된다 (gitwire-chat 의 send 는 append 예외를
       RoomError 로 바꾼다).
    2. 그래도 **조용하지 않다** — 경고 로그가 남고 `info()["pending"]` 에 드러난다.
    3. 대기열에 남아 **배경이 계속 다시 민다** — 아무도 다음 건을 발행하지 않아도
       원격이 돌아오는 순간 나간다. 그 재시도 간격이 이 설계에 남은 유일한
       타이머다(배칭 창이 아니다).
    4. 반대로 **기다리라고 한 쪽**(`flush()`)에는 실패를 그대로 올린다.

    그리고 그 상태에서 `close()` 가 교착 없이 끝난다.
    """
    from test_stamp_on_push import BlockedPushRunner   # noqa: PLC0415

    runner = BlockedPushRunner()
    a = participant("a", runner=runner)                # autopublish = 기본
    runner.blocked = True                              # 방이 만들어진 뒤에 막는다

    with caplog.at_level("WARNING", logger="gitwire"):
        a.append({"n": "지금은 못 나갈 말"})           # 예외 없음
    assert a.info()["pending"] == 1, "대기열에 남아 있지 않다"
    assert any("발행 실패" in r.message for r in caplog.records), caplog.text

    # 기다리라고 한 쪽에는 그대로 올린다
    with pytest.raises(gitwire.GitError):
        a.flush(push_attempts=1)
    assert a.info()["pending"] == 1

    runner.blocked = False                             # 원격이 돌아왔다
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and a.info()["pending"]:
        time.sleep(0.2)                                # 아무도 append 하지 않는다
    assert a.info()["pending"] == 0, "배경 재시도가 밀지 않았다"
    assert len(landed(bare_repo, homes)) == 1

    a.close()                                          # 교착 없이 닫힌다


def test_pushed_marker_tracks_what_was_actually_pushed(participant, bare_repo, homes):
    """`_mark_pushed` 가 *올린 sha* 를 기록한다 — 안 올린 것을 올렸다고 하지 않는다.

    락 밖에서 `HEAD:` 로 밀고 나서 그때의 HEAD 를 표시하면, push 중에 늘어난
    커밋까지 "올렸다"가 되어 **조용히 유실**된다. 명시 sha 로 미는 이유다.

    ⭐ 이제 push 중에 늘어날 수 있는 것은 **참가자 상태 커밋**이다 — 레코드는
    대기열(메모리)에 있다가 `flush()` 안에서만 커밋된다. 그래서 그 경로로 잰다.
    """
    runner = SlowPushRunner()
    a = participant("a", runner=runner, autopublish=False)
    a.append({"n": 0})
    a.flush()
    pushed = a.git.out("rev-parse", "refs/gitwire/pushed")
    assert pushed == a.git.out("rev-parse", "HEAD")

    a.write_state("me@localhost", {"cursor": "c1"})
    a._absorb_worktree()                   # 커밋만 만들고 push 는 하지 않는다
    assert a._unpushed_count() == 1        # 표시가 정확해야 이게 1이 된다
    late = a.append({"n": 1})
    a.flush()
    assert late.id in landed(bare_repo, homes)
    assert a._unpushed_count() == 0


def test_concurrent_participants_do_not_lose_records(participant, bare_repo, homes):
    """두 참가자가 동시에 느린 push 를 해도(선점 → rebase 재시도) 유실이 없다."""
    a = participant("a", runner=SlowPushRunner(0.3), autopublish=False)
    b = participant("b", runner=SlowPushRunner(0.3), autopublish=False)
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
    a = participant("a", runner=runner, autopublish=False)
    b = participant("b")
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


def test_drop_overlapping_with_flush_loses_nothing(participant, bare_repo, homes):
    """아카이빙·삭제(로컬을 크게 바꾼다)와 발행·flush 가 겹쳐도 유실·중복이 없다."""
    from test_archive import day_of, write_past   # noqa: PLC0415

    a = participant("a", autopublish=False)
    old = write_past(a, 2, [{"n": f"old-{i}"} for i in range(6)])
    day = day_of(old[0])

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
    arch = a.archive_days()
    res = a.drop_days(arch["archived"])
    stop.set()
    th.join(30)
    a.flush()

    assert not err, err
    # 삭제가 이번에 졌을 수도 있다(발행이 계속 원격을 움직인다). 그것 자체는
    # 정상이다 — 유실이 없고, 조용해지면 다음 시도가 지운다.
    if not res["dropped"]:
        assert res["reason"].startswith("push 경합"), res
        a.archive_days()
        res = a.drop_days(arch["archived"])
    assert res["dropped"] is True, res
    # 지워진 날짜는 검증용 새 클론에서 히스토리로 꺼내 확인한다 (`landed` 도크).
    got = landed(bare_repo, homes, recover=[day])
    assert sorted(got) == sorted(r.id for r in made), (
        set(r.id for r in made) - set(got)
    )
    assert len(got) == len(set(got))
    # 아카이빙된 날짜도 그대로 읽힌다
    assert {r.id for r in a.history(fresh=True)} == {r.id for r in made}


def test_compact_does_not_deadlock_with_a_running_publisher(participant, bare_repo, homes):
    """`compact()` 는 `_remote` → `_lock` 순서를 지킨다 (교착이 없다).

    ⭐ 발행이 **계속 돌고 있는 동안** compact 를 부른다 (드레인 루프는 append
    마다 곧바로 밀기 때문에, 창을 짧게 잡던 예전 설정이 그대로 기본 동작이다).
    """
    a = participant("a")
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
    a = participant("a", runner=SlowPushRunner(0.3), autopublish=False)
    recs = [a.append({"n": i}) for i in range(3)]
    a.close()
    assert sorted(landed(bare_repo, homes)) == sorted(r.id for r in recs)


# ------------------------------------ 전송 한 번의 git 프로세스 수 · 흡수 무손실


class CountingRunner(SubprocessGitRunner):
    """git 호출을 하위명령으로 적는다 (`BASE_CONFIG` 의 `-c` 쌍은 건너뛴다)."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    def run(self, args, **kwargs):
        self.calls.append(localrefs.subcommand(args) or "?")
        return super().run(args, **kwargs)

    def reset(self) -> None:
        self.calls.clear()


def test_a_transfer_spawns_exactly_three_git(participant):
    """⭐ 전송 한 번 = `add` + `commit` + `push`. 그게 전부다.

    예전에는 **7개**였다 — `rev-parse` 2회(기준 HEAD·커밋 후 HEAD)와
    `diff --cached`, `update-ref` 가 더 있었다. git subprocess 한 번은 하는 일과
    무관하게 이 머신에서 90~250ms 이므로(비용은 작업이 아니라 프로세스 기동이다 —
    `localrefs` 모듈 도크) 그 4개가 전송당 **518ms** 였다.

    무엇이 그 4개를 대신하나:
      * `rev-parse` → `localrefs.ref_sha` 가 `.git` ref 파일을 직접 읽는다.
      * `update-ref` → `localrefs.write_ref` 가 loose ref 를 직접 쓴다.
      * `diff --cached` → 찍은 레코드가 있으면 스테이징될 것이 반드시 있으니
        `git commit` 이 스스로 판정한다 (`_commit_plan`).
    """
    runner = CountingRunner()
    a = participant("a", runner=runner, autopublish=False, auto_archive=False)
    for i in range(2):                      # 레이아웃·캐시를 데운다
        a.append({"n": f"warm{i}"})
        a.flush()
    runner.reset()
    rec = a.append({"n": 1})
    a.flush()
    assert rec.pushed, "안 나갔다"
    assert runner.calls == ["add", "commit", "push"], runner.calls


class GateOnArmedCommit(SubprocessGitRunner):
    """`arm()` 한 뒤의 **첫 커밋**을 붙잡아 둔다 (방 초기화 커밋은 통과시킨다)."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.held = False
        self.gate = threading.Event()
        self.go = threading.Event()

    def arm(self) -> None:
        self.armed = True

    def run(self, args, **kw):
        if self.armed and not self.held and any(a == "commit" for a in args):
            self.held = True
            self.gate.set()
            self.go.wait(60)
        return super().run(args, **kw)


def test_state_written_during_the_unlocked_commit_still_gets_published(
    participant, bare_repo, homes
):
    """⚠️ 커밋을 락 밖에서 하는 대가 — 그 창에 들어온 건을 **아무도 안 밀면** 안 된다.

    `flush()` 는 `add`+`commit` 을 채널 락 **밖에서** 한다(그래야 그 ~290ms 동안
    `append()`·읽기가 막히지 않는다 — 위 `[flush]` 측정). 그 창에서
    `write_state()` 가 들어오면 이런 일이 벌어진다:

    1. `write_state` 는 파일을 쓰고 `_pending_state` 에 넣는다.
    2. 그리고 `_drain()` 을 부르는데, **이미 누가 밀고 있으므로**(`_publishers>0`)
       "그 쪽이 내 것까지 가져간다"고 믿고 그냥 돌아간다 (`_drain` 규칙 2).
    3. 그런데 도는 쪽이 끝나면서 `_pending_state` 를 **통째로 비우면** 그 건은
       아무의 몫도 아니게 된다 — 드레인 루프는 "대기 없음"으로 보고 빠져나가고,
       파일은 커밋되지 않은 채 남는다. **아무도 다시 밀지 않는다.**

    그래서 락 안에서 확정한 `_Absorb` 에 적힌 것만 걷어낸다 (`_absorb_plan`).
    이 테스트는 그 성질을 *결과*로 못 박는다: 커밋 한복판에 상태를 하나 넣고,
    **추가로 아무것도 부르지 않은 채** 그것이 커밋되고 원격까지 가는지 본다.
    """
    runner = GateOnArmedCommit()
    a = participant("a", runner=runner, auto_archive=False)   # autopublish 기본
    runner.arm()
    sending = threading.Thread(target=lambda: a.append({"n": 0}), daemon=True)
    sending.start()
    assert runner.gate.wait(30), "커밋이 시작되지 않았다"
    # ⭐ 커밋이 도는 **그 순간** 새 상태가 들어온다 (락이 비어 있어야 가능하다).
    rel = a.write_state("late@localhost", {"cursor": "c2"})
    runner.go.set()
    sending.join(90)
    assert not sending.is_alive(), "발행이 끝나지 않았다"

    # 여기서부터 우리는 **아무것도 더 부르지 않는다.** 드레인 루프가 그 건을
    # 가져갔어야 한다.
    assert a.info()["pending_state"] == 0, "대기에 남았다"
    committed = a.git.out("ls-tree", "-r", "--name-only", "HEAD").splitlines()
    assert rel in committed, f"커밋되지 않았다 (아무도 밀지 않았다): {committed}"
    assert a._unpushed_count() == 0, "커밋은 됐는데 push 되지 않았다"

    verify = gitwire.Channel(
        str(bare_repo), home=homes("verify"), sender="verify",
        clock=FixedOffsetClock(0.0), autopublish=False,
    ).open()
    try:
        states = verify.read_states(fresh=True)
        assert "late@localhost" in states, f"원격에 없다: {sorted(states)}"
        assert states["late@localhost"].value == {"cursor": "c2"}
    finally:
        verify.close()


def test_a_commit_that_stages_nothing_fails_loudly_and_rewinds(
    participant, bare_repo, homes
):
    """⚠️ 레코드를 찍었는데 커밋에 아무것도 안 실리면 **소리 내어 실패한다.**

    예전에는 `diff --cached --quiet` 로 먼저 묻고 "변경 없음"이면 커밋을 건너뛰었다.
    그 분기가 이 경우를 **조용히 삼켰다**: 커밋이 없으니 HEAD 는 그대로, push 는
    "Everything up-to-date" 로 성공, 그리고 대기열은 *나갔다*고 표시된다 —
    레코드는 작업 사본에 남아 아무도 다시 보지 않는다.

    실제로 여기 걸리는 설정이 있다: 누군가 이 클론의 `.gitignore` 에 `records/`
    를 넣으면 `git add` 가 아무것도 스테이징하지 못한다.

    지금은 찍은 레코드가 있으면 `git commit` 을 그대로 부르고, 실패를 올린다.
    그리고 찍은 것을 **되돌린다** — 파일도 지우고 건은 대기열에 남긴다.
    """
    a = participant("a", autopublish=False, auto_archive=False)
    ignore = a.clone_dir / ".gitignore"
    ignore.write_text(
        ignore.read_text(encoding="utf-8") + "records/\n", encoding="utf-8"
    )
    a.git.run("add", "--", ".gitignore")
    a.git.run("commit", "-m", "테스트: records/ 를 무시하게 만든다")

    ticket = a.append({"n": 0})
    with pytest.raises(GitError):
        a.flush()

    # 되돌렸다 — 찍힌 파일이 남지 않았고, 건은 여전히 대기열에 있다.
    assert list((a.clone_dir / "records").rglob("*.json")) == []
    assert ticket.pushed is False and ticket.dropped is False
    assert a.info()["pending"] == 1, "대기열에서 사라졌다"
    assert landed(bare_repo, homes) == [], "안 나갔는데 원격에 있다"
