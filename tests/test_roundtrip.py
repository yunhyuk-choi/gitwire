"""⭐ 핵심 검증 — 로컬 bare 레포를 원격으로 삼은 2-클론 왕복.

대역이 아니라 **실제 git** 으로 돌린다. 여기서 증명하려는 것:

1. A 가 append 한 레코드를 B 가 구독/조회로 받는다 (전송이 실제로 성립).
2. 두 참가자가 **동시에** 발행해도 충돌 없이 **둘 다 살아남는다**
   (레코드 1건 = 파일 1개 + push 거부 시 rebase 재시도).
3. 소비자를 **재시작**해도 중복·유실이 없다 (커서가 디스크에 있으므로).
4. 배칭이 커밋 수를 실제로 줄인다.
5. 빈 레포에 URL 만 줘도 방이 된다.
"""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path

import pytest

import gitwire
from gitwire.clock import FixedOffsetClock


def commit_count(clone: Path) -> int:
    out = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=str(clone), capture_output=True, text=True,
        encoding="utf-8", errors="replace", check=True,
    )
    return int(out.stdout.strip())


# --------------------------------------------------------------- 1. 왕복


def test_empty_repo_becomes_a_room(bare_repo, participant):
    a = participant("alice")
    assert (a.clone_dir / "gitwire.json").exists()
    assert (a.clone_dir / "records").is_dir()
    # 원격에도 규약이 올라갔다
    ls = subprocess.run(
        ["git", "--git-dir", str(bare_repo), "ls-tree", "-r", "--name-only", "main"],
        capture_output=True, text=True,
        encoding="utf-8", errors="replace", check=True,
    ).stdout
    assert "gitwire.json" in ls
    assert ".gitattributes" in ls


def test_a_appends_b_receives(participant):
    a = participant("alice")
    b = participant("bob")

    rid = a.append({"kind": "hello", "n": 1})
    a.flush()

    got = b.fetch_new()
    assert [r.id for r in got] == [rid]
    assert got[0].payload == {"kind": "hello", "n": 1}
    assert got[0].sender == "alice"

    # 두 번째 호출에는 아무것도 없다 (중복 없음)
    assert b.fetch_new() == []


def test_subscribe_callback_delivers(participant):
    a = participant("alice")
    b = participant("bob")
    b.skip_to_now()

    received = []
    done = threading.Event()

    def on_rec(rec):
        received.append(rec)
        if len(received) >= 3:
            done.set()

    sub = b.subscribe(on_rec, interval=0.05)
    try:
        for i in range(3):
            a.append({"i": i}, flush=True)
        assert done.wait(20), f"3건을 기다렸지만 {len(received)}건만 받았다"
    finally:
        sub.stop()

    assert [r.payload["i"] for r in received] == [0, 1, 2]


def test_payload_is_opaque(participant):
    """기반이 아는 것은 봉투(id/sender/ts)뿐. payload 는 그대로 왕복한다."""
    a = participant("alice")
    b = participant("bob")

    odd = {
        "author": {"nested": True},
        "text": None,
        "숫자": [1, 2.5, -3],
        "빈객체": {},
        "유니코드": "한글 ✅ emoji 🚀",
    }
    a.append(odd, flush=True)
    got = b.fetch_new()
    assert len(got) == 1
    assert got[0].payload == odd

    # 리스트·문자열·숫자도 그대로 통과한다 (dict 를 강요하지 않는다)
    for value in ([1, 2, 3], "raw string", 42, None, True):
        a.append(value, flush=True)
    assert [r.payload for r in b.fetch_new()] == [[1, 2, 3], "raw string", 42, None, True]


# ------------------------------------------------- 2. 동시 발행 (충돌 없음)


def test_concurrent_publish_both_survive(participant):
    """A·B 가 같은 순간에 push 하면 한쪽은 거부된다 → rebase 후 재시도."""
    a = participant("alice")
    b = participant("bob")
    c = participant("carol")

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def publish(ch, tag, n):
        try:
            barrier.wait(timeout=10)
            for i in range(n):
                ch.append({"who": tag, "i": i}, flush=True)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=publish, args=(a, "alice", 5))
    t2 = threading.Thread(target=publish, args=(b, "bob", 5))
    t1.start(); t2.start()
    t1.join(120); t2.join(120)

    assert not errors, f"동시 발행 중 예외: {errors}"

    got = c.fetch_new()
    pairs = sorted((r.payload["who"], r.payload["i"]) for r in got)
    expected = sorted(
        [("alice", i) for i in range(5)] + [("bob", i) for i in range(5)]
    )
    assert pairs == expected, "동시 발행에서 레코드가 유실됐다"

    # 레코드 ID 는 전부 유일하다
    assert len({r.id for r in got}) == 10


def test_push_rejected_then_rebase_retry(participant):
    """B 가 먼저 밀어 A 의 push 가 거부되는 상황을 강제로 만든다."""
    a = participant("alice")
    b = participant("bob")

    a.append({"n": "a1"})          # 로컬 커밋만 (flush 안 함)
    a.flush()
    b.append({"n": "b1"}, flush=True)

    # A 는 아직 b1 을 모른 채 커밋을 쌓는다 → push 가 거부되어야 한다
    a.append({"n": "a2"}, flush=True)

    reader = participant("dave")
    payloads = sorted(r.payload["n"] for r in reader.fetch_new())
    assert payloads == ["a1", "a2", "b1"]


# ------------------------------------------------- 3. 재시작 (중복·유실 없음)


def test_restart_no_duplicate_no_loss(bare_repo, homes, participant):
    a = participant("alice")
    for i in range(6):
        a.append({"i": i}, flush=True)

    bob_home = homes("bob")

    def open_bob():
        return gitwire.Channel(
            str(bare_repo), home=bob_home, sender="bob",
            clock=FixedOffsetClock(0.0), batch_window=0.0,
        ).open()

    # 1회차 프로세스: 3건만 가져가고 죽는다
    b1 = open_bob()
    first = b1.fetch_new(limit=3)
    assert [r.payload["i"] for r in first] == [0, 1, 2]
    b1.close()
    del b1

    # 2회차 프로세스: 완전히 새 객체 (메모리 상태 없음)
    b2 = open_bob()
    second = b2.fetch_new()
    assert [r.payload["i"] for r in second] == [3, 4, 5], "재시작 후 중복 또는 유실"

    # 3회차: 새 레코드가 없으면 빈 리스트
    b3 = open_bob()
    assert b3.fetch_new() == []

    # 그 사이 새 레코드가 오면 그것만
    a.append({"i": 6}, flush=True)
    b4 = open_bob()
    assert [r.payload["i"] for r in b4.fetch_new()] == [6]
    for ch in (b2, b3, b4):
        ch.close()


def test_crash_mid_batch_resumes_exactly(bare_repo, homes, participant):
    """poll_once 는 레코드 한 건마다 커서를 저장한다 → 중간에 죽어도 정확히 이어받는다."""
    a = participant("alice")
    for i in range(5):
        a.append({"i": i}, flush=True)

    bob_home = homes("bob")

    def open_bob():
        return gitwire.Channel(
            str(bare_repo), home=bob_home, sender="bob",
            clock=FixedOffsetClock(0.0), batch_window=0.0,
        ).open()

    seen: list[int] = []

    b1 = open_bob()

    def cb(rec):
        if rec.payload["i"] == 2:
            raise RuntimeError("소비자 처리 실패")
        seen.append(rec.payload["i"])

    # 3번째 레코드에서 실패 → 그 지점에서 멈추고 커서를 전진시키지 않는다
    delivered = b1.poll_once(cb, on_error=lambda r, e: None)
    b1.close()
    assert seen == [0, 1]
    assert delivered == 2

    # 새 프로세스가 **실패한 그 레코드부터** 이어받는다 — 유실도 중복도 없다
    b2 = open_bob()
    rest: list[int] = []
    b2.poll_once(lambda r: rest.append(r.payload["i"]))
    assert rest == [2, 3, 4]
    b2.close()


def test_per_consumer_cursor_isolation(bare_repo, homes, participant):
    a = participant("alice")
    for i in range(3):
        a.append({"i": i}, flush=True)

    home = homes("shared")
    web = gitwire.Channel(str(bare_repo), home=home, consumer="webapp",
                          clock=FixedOffsetClock(0.0), batch_window=0.0).open()
    bot = gitwire.Channel(str(bare_repo), home=home, consumer="agent",
                          clock=FixedOffsetClock(0.0), batch_window=0.0).open()

    assert len(web.fetch_new()) == 3
    assert web.fetch_new() == []
    # 다른 소비자는 자기 커서를 갖는다 → 전부 다시 받는다
    assert len(bot.fetch_new()) == 3
    web.close(); bot.close()


# --------------------------------------------------------------- 4. 배칭


def test_batching_groups_into_one_commit(participant):
    a = participant("alice", batch_window=60.0)  # 창을 길게 → 수동 flush 로 한 커밋
    before = commit_count(a.clone_dir)
    for i in range(10):
        a.append({"i": i})
    a.flush()
    after = commit_count(a.clone_dir)
    assert after - before == 1, "10건이 한 커밋으로 묶이지 않았다"

    b = participant("bob")
    assert len(b.fetch_new()) == 10


def test_no_batching_commit_per_record(participant):
    a = participant("alice", batch_window=0.0)
    before = commit_count(a.clone_dir)
    for i in range(3):
        a.append({"i": i})
    assert commit_count(a.clone_dir) - before == 3


def test_batch_window_autoflush(participant):
    a = participant("alice", batch_window=0.2)
    b = participant("bob")
    a.append({"x": 1})
    a.append({"x": 2})
    deadline = 30
    import time

    got = []
    start = time.monotonic()
    while time.monotonic() - start < deadline:
        got = b.fetch_new()
        if len(got) >= 2:
            break
        time.sleep(0.2)
    assert len(got) == 2, "배칭 플러셔가 자동으로 push 하지 않았다"


# ------------------------------------------------------------- 5. 변경 감지


def test_no_change_no_fetch(participant):
    """has_changes() 가 ls-remote 결과만으로 판정한다."""
    a = participant("alice")
    b = participant("bob")
    b.fetch_new()
    assert b.has_changes() is False
    a.append({"x": 1}, flush=True)
    assert b.has_changes() is True
    b.fetch_new()
    assert b.has_changes() is False


def test_change_detection_uses_ls_remote_only(participant, monkeypatch):
    from gitwire.gitcmd import SubprocessGitRunner

    calls: list[str] = []
    orig = SubprocessGitRunner.run

    def spy(self, args, **kwargs):
        calls.append(tuple(args))
        return orig(self, args, **kwargs)

    b = participant("bob")
    b.fetch_new()
    monkeypatch.setattr(SubprocessGitRunner, "run", spy)
    assert b.has_changes() is False
    flat = [a for call in calls for a in call]
    assert "ls-remote" in flat
    assert "fetch" not in flat, "변경이 없는데 fetch 를 했다"


# ------------------------------------------------------------- 6. 히스토리


def test_history_does_not_move_cursor(participant):
    a = participant("alice")
    b = participant("bob")
    for i in range(4):
        a.append({"i": i}, flush=True)

    assert [r.payload["i"] for r in b.history()] == [0, 1, 2, 3]
    assert [r.payload["i"] for r in b.history(limit=2)] == [2, 3]
    # history 를 여러 번 불러도 fetch_new 는 여전히 전부 준다
    assert len(b.fetch_new()) == 4


def test_peek_does_not_advance_cursor(participant):
    a = participant("alice")
    b = participant("bob")
    a.append({"i": 0}, flush=True)
    a.append({"i": 1}, flush=True)

    peeked = b.peek_new()
    assert len(peeked) == 2
    assert len(b.peek_new()) == 2, "peek 이 커서를 움직였다"

    assert b.ack_through(peeked[0].id) is True
    assert [r.payload["i"] for r in b.peek_new()] == [1]
    assert [r.payload["i"] for r in b.fetch_new()] == [1]


def test_skip_to_now_drops_backlog(participant):
    a = participant("alice")
    for i in range(3):
        a.append({"i": i}, flush=True)
    b = participant("bob")
    b.skip_to_now()
    assert b.fetch_new() == []
    a.append({"i": 99}, flush=True)
    assert [r.payload["i"] for r in b.fetch_new()] == [99]


def test_shallow_clone_works(bare_repo, homes, participant):
    a = participant("alice")
    for i in range(5):
        a.append({"i": i}, flush=True)

    shallow = gitwire.Channel(
        str(bare_repo), home=homes("shallow"), depth=1,
        clock=FixedOffsetClock(0.0), batch_window=0.0,
    ).open()
    try:
        assert len(shallow.fetch_new()) == 5
        a.append({"i": 5}, flush=True)
        assert [r.payload["i"] for r in shallow.fetch_new()] == [5]
        shallow.append({"i": 6}, flush=True)
        b = participant("bob")
        assert 6 in [r.payload["i"] for r in b.fetch_new()]
    finally:
        shallow.close()
