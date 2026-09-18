"""레코드의 **시각·id 는 원격에 push 되는 순간**에 정해진다 — 그 성질을 고정한다.

왜 이 파일이 있나
----------------
예전에는 `append()` 가 *작성 시각*으로 id 를 굳히고 push 는 나중에 했다. 그래서
오프라인에서 쓴 메시지가 며칠 뒤 나가면 **과거 날짜 id 로 도착**했고, 실측된 결과가
두 가지였다 (`Channel.append` 도크):

* 안 읽음 카운트 공식이 `|{p ≠ A, cursor(p) < M}|` 이고 커서는 단조 증가라 과거
  id 는 모두의 커서보다 앞이다 → **아무도 안 봤는데 카운트 0.**
* 화면에서는 며칠 위에 끼워진다 → OS 알림은 오는데 **볼 곳이 없다.**

그래서 여기서 못 박는 것은 네 가지다:

1. 대기열에 넣고 **시계를 옮긴 뒤** push 하면 id·경로·봉투 시각이 전부 *push 시점*이다.
2. **어떤 경로로도 과거 날짜 레코드가 새로 생기지 않는다** (push 실패·시계 역행 포함).
   ⚠️ 뒤따르는 아카이브 설계가 이 성질에 의존한다.
3. 앞 건의 push 가 실패하면 **뒤 건이 먼저 나가지 않는다.** 성공하면 발행 순서대로
   id 가 증가한다.
4. 대기열은 **메모리**다 — 프로세스가 죽으면 안 나간 것은 사라지고, 다음 기동이
   밀어내지 **않는다** (의도된 성질).

대역은 push 실패에만 쓴다. 나머지는 전부 진짜 git·진짜 bare 레포다.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import gitwire
from gitwire.errors import NotPushed
from gitwire.gitcmd import GitResult, SubprocessGitRunner

DAY = 86400.0


class BlockedPushRunner(SubprocessGitRunner):
    """`push` 만 실패시킨다 (끊긴 네트워크 — 선점이 아니다).

    `PushRejected` 가 아니라 평범한 `GitError` 가 되는 문구를 쓴다. 그래야
    "밀 수가 없었다"(오프라인)와 "남이 선점했다"(rebase 후 재시도)가 갈린다.
    """

    def __init__(self, blocked: bool = False) -> None:
        super().__init__()
        # ⚠️ 기본은 **열림**이다. 채널 열기(`open()`)가 레이아웃 초기화 커밋을
        # push 하므로, 처음부터 막으면 방이 만들어지지 않는다.
        self.blocked = blocked
        self.pushes = 0          # 전체 push 시도 (방 초기화 포함)
        self.refused = 0         # 그중 막은 것

    def run(self, args, **kw):
        if any(a == "push" for a in args):
            self.pushes += 1
            if self.blocked:
                self.refused += 1
                return GitResult(
                    128, "", "fatal: unable to access: Could not resolve host"
                )
        return super().run(args, **kw)


def remote_ids(bare_repo: Path) -> list[str]:
    """원격에 **정말** 있는 레코드 id (클론도 캐시도 거치지 않는다)."""
    out = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "main"],
        cwd=str(bare_repo), capture_output=True, text=True, encoding="utf-8",
    )
    if out.returncode != 0:
        return []
    return sorted(
        line.strip() for line in out.stdout.splitlines()
        if gitwire.is_record_id(line.strip())
    )


def record_days(channel: gitwire.Channel, bare_repo: Path) -> set[str]:
    """지금까지 만들어진 레코드의 **날짜 디렉토리 전부** (로컬 + 원격)."""
    days = {rid.split("/")[1] for rid in remote_ids(bare_repo)}
    root = channel.clone_dir / "records"
    if root.exists():
        days |= {p.name for p in root.iterdir() if p.is_dir()}
    return days


def day_of(record_id: str) -> str:
    return record_id.split("/")[1]


# ------------------------------------------------ 1. 시각 = push 되는 순간


def test_stamp_is_taken_at_push_not_at_append(participant, bare_repo):
    """대기열에 넣고 → 시계를 **다음 날로** → push. 세 값이 전부 push 시점이다."""
    ch = participant("a", autopublish=False)
    queued_day = gitwire.records.format_ts(ch.clock.now())[:8]

    ticket = ch.append({"n": "오프라인에서 쓴 말"})
    assert ticket.pushed is False
    # 레코드 파일이 하나도 없다 (`records/` 디렉토리 자체는 레이아웃이 만든다)
    assert not list((ch.clone_dir / "records").rglob("*.json")), "대기열이 디스크를 건드렸다"

    ch.clock.offset += DAY                      # 하루가 지났다 (발행은 어제 했다)
    push_day = gitwire.records.format_ts(ch.clock.now())[:8]
    assert push_day != queued_day

    made = ch.flush()
    assert len(made) == 1
    rec = made[0]
    assert ticket.record is rec and ticket.id == rec.id

    # (1) id 의 날짜 디렉토리 (2) id 안의 스탬프 (3) 봉투의 ts — 전부 push 날짜
    assert day_of(rec.id) == push_day, rec.id
    assert rec.id.split("/")[2].startswith(push_day + "T"), rec.id
    assert rec.timestamp.strftime("%Y%m%d") == push_day
    landed = remote_ids(bare_repo)
    assert landed == [rec.id]

    # 원격에 실제로 올라간 **파일 내용**의 시각도 같다 (봉투를 직접 읽는다)
    body = subprocess.run(
        ["git", "show", f"main:{rec.id}"], cwd=str(bare_repo),
        capture_output=True, text=True, encoding="utf-8", check=True,
    ).stdout
    assert f'"ts":"{push_day[:4]}-{push_day[4:6]}-{push_day[6:]}' in body, body
    assert "composed_at" not in body, "작성 시각을 남기지 않기로 했다"


def test_ids_increase_in_send_order_inside_one_batch(participant):
    """한 커밋으로 나가는 여러 건도 **발행 순서대로** id 가 증가한다.

    시계 해상도가 밀리초라 그대로 찍으면 같은 밀리초가 되고, 그러면 순서가 난수
    접미로 갈린다. `_stamp()` 의 단조 증가 가드가 그것을 막는다.
    """
    ch = participant("a", autopublish=False)
    tickets = [ch.append({"n": i}) for i in range(5)]
    made = ch.flush()

    ids = [r.id for r in made]
    assert ids == sorted(ids), ids
    assert len(set(ids)) == 5
    assert [r.payload["n"] for r in made] == [0, 1, 2, 3, 4]
    assert [t.id for t in tickets] == ids


# --------------------------------------- 2. 과거 날짜 레코드가 생기지 않는다


def test_clock_going_backwards_makes_no_past_record(participant, bare_repo):
    """시계가 거꾸로 가도(보정 재조회) 과거 날짜 레코드가 생기지 않는다."""
    ch = participant("a", autopublish=False)
    first = ch.append({"n": 1})
    ch.flush()
    today = day_of(first.id)

    ch.clock.offset -= DAY * 3                  # 시계가 3일 뒤로 튀었다
    second = ch.append({"n": 2})
    ch.flush()

    assert day_of(second.id) == today, second.id
    assert second.id > first.id, (first.id, second.id)
    assert record_days(ch, bare_repo) == {today}


def test_failed_push_leaves_no_record_behind(participant, bare_repo):
    """push 가 실패하면 찍은 것을 **되돌린다** — 과거 시각으로 남지 않는다.

    남겨 두면 그것이 *언제 나갈지 모르는 과거 시각 레코드*가 되고, 며칠 뒤 다른
    push 경로가 밀어내면 정확히 고치려던 결함이 된다.
    """
    runner = BlockedPushRunner()
    ch = participant("a", runner=runner, autopublish=False)
    runner.blocked = True                        # 방이 만들어진 뒤에 막는다
    ch.append({"n": "못 나갈 말"})

    with pytest.raises(gitwire.GitError):
        ch.flush(push_attempts=1)

    assert runner.refused == 1, "막힌 push 가 한 번만 시도됐어야 한다"
    assert remote_ids(bare_repo) == []
    # 로컬에도 남지 않았다 — 파일도, 커밋도.
    assert not list((ch.clone_dir / "records").rglob("*.json"))
    assert ch._unpushed_count() == 0, "되돌리지 않은 커밋이 남았다"
    assert ch.info()["pending"] == 1, "대기열에 돌아오지 않았다"

    # 하루가 지난 뒤 성공하면 **그때의 날짜**로 찍힌다
    ch.clock.offset += DAY
    runner.blocked = False
    made = ch.flush()
    assert len(made) == 1
    push_day = gitwire.records.format_ts(ch.clock.now())[:8]
    assert day_of(made[0].id) == push_day
    assert record_days(ch, bare_repo) == {push_day}


def test_no_path_creates_a_record_dated_before_today(participant, bare_repo):
    """섞어 돌려도 **오늘보다 과거 날짜** 레코드 디렉토리가 하나도 생기지 않는다."""
    runner = BlockedPushRunner()
    ch = participant("a", runner=runner, autopublish=False)
    today = gitwire.records.format_ts(ch.clock.now())[:8]

    ch.append({"n": 1})
    ch.flush()                                   # 정상 경로
    ch.append({"n": 2})
    runner.blocked = True
    with pytest.raises(gitwire.GitError):        # 실패 → 되돌림
        ch.flush(push_attempts=1)
    runner.blocked = False
    ch.clock.offset -= DAY                       # 시계 역행
    ch.flush()                                   # 되돌아온 건이 지금 나간다
    ch.write_state("me@example.com", {"cursor": "x"}, flush=True)  # 상태 경로
    ch.append({"n": 3}, flush=True)              # 즉시 flush 경로

    assert record_days(ch, bare_repo) == {today}
    ids = remote_ids(bare_repo)
    assert len(ids) == 3
    assert all(day_of(rid) >= today for rid in ids), ids


# ------------------------------------------------------------- 3. 순서


def test_first_failure_does_not_let_later_records_pass(participant, bare_repo):
    """3건 연속 전송 → 1번 push 실패 → 2·3번이 먼저 나가지 않는다."""
    runner = BlockedPushRunner()
    ch = participant("a", autopublish=False, runner=runner)
    runner.blocked = True                        # 방이 만들어진 뒤에 막는다

    one = ch.append({"n": 1})
    with pytest.raises(gitwire.GitError):
        ch.flush(push_attempts=1)                # 1번이 막혔다
    assert remote_ids(bare_repo) == []

    two = ch.append({"n": 2})
    three = ch.append({"n": 3})
    with pytest.raises(gitwire.GitError):
        ch.flush(push_attempts=1)                # 여전히 막혀 있다
    # ⭐ 2·3번만 먼저 나가는 일이 없다 — 원격은 아직 비어 있다.
    assert remote_ids(bare_repo) == []
    assert [t.pushed for t in (one, two, three)] == [False, False, False]

    runner.blocked = False
    made = ch.flush()

    assert [r.payload["n"] for r in made] == [1, 2, 3]
    ids = [r.id for r in made]
    assert ids == sorted(ids), ids                # 1 < 2 < 3
    assert remote_ids(bare_repo) == ids
    assert [t.id for t in (one, two, three)] == ids


# --------------------------------------------- 4. 대기열은 메모리다


def test_queue_is_memory_only_and_dies_with_the_process(
    participant, bare_repo, homes
):
    """죽으면 안 나간 것은 **사라진다** — 다음 기동이 밀어내지 않는다 (의도).

    "죽은 프로세스"는 같은 home·같은 클론을 쓰는 **새 Channel 객체**로 흉내 낸다
    (그 둘이 곧 재기동이 물려받는 전부다 — 메모리는 안 물려받는다).
    """
    ch = participant("a", autopublish=False)
    ticket = ch.append({"n": "보내고 바로 죽는다"})

    # 디스크에 아무것도 없다 = 물려줄 것이 없다
    assert not list(ch.clone_dir.rglob("records/**/*.json"))
    assert ch.git.out("status", "--porcelain") == ""
    assert ticket.wait(0.05) is None

    reborn = gitwire.Channel(
        str(bare_repo), home=homes("a"), sender="a",
        clock=gitwire.FixedOffsetClock(0.0), autopublish=False, auto_archive=False,
    ).open()
    try:
        assert reborn.flush() == []               # 밀어낼 것이 없다
        assert remote_ids(bare_repo) == []
        assert [r.payload for r in reborn.history(fresh=True)] == []
    finally:
        reborn.close()


def test_close_drops_what_it_could_not_push(participant):
    """정상 종료의 마지막 flush 가 실패하면 남은 티켓은 **버려졌다고 말한다.**

    조용히 영원히 기다리게 두지 않는다 — 기다리는 쪽이 사실을 알아야 한다.
    """
    runner = BlockedPushRunner()
    ch = participant("a", autopublish=False, runner=runner)
    runner.blocked = True                        # 방이 만들어진 뒤에 막는다
    ticket = ch.append({"n": "종료 때 못 나갈 말"})

    with pytest.raises(gitwire.GitError):
        ch.close()

    assert ticket.dropped is True
    assert ticket.pushed is False
    assert ticket.wait(0.0) is None
    assert ch.info()["pending"] == 0


# ------------------------------------------------- 티켓 계약 (append 반환값)


def test_pending_record_contract(participant, bare_repo):
    """티켓은 대기 중에 **id·시각을 지어내지 않는다** (`NotPushed`)."""
    ch = participant("a", autopublish=False)
    ticket = ch.append({"n": 1}, sender="bob-x")

    assert ticket.sender == gitwire.records.slug_sender("bob-x") == "bob_x"
    assert ticket.payload == {"n": 1}
    assert ticket.seq == 1
    assert ticket.pushed is False and ticket.dropped is False
    assert ticket.record is None
    with pytest.raises(NotPushed):
        ticket.id
    with pytest.raises(NotPushed):
        ticket.timestamp
    assert "queued" in repr(ticket)

    made = ch.flush()
    assert ticket.pushed is True
    assert ticket.id == made[0].id
    assert ticket.timestamp == made[0].timestamp
    assert ticket.wait(0.0) is made[0]
    assert gitwire.is_record_id(ticket.id)


def test_append_with_flush_returns_a_settled_ticket(participant):
    """`flush=True` 는 호출 안에서 push 까지 끝낸다 — 티켓이 이미 settled 다."""
    ch = participant("a", autopublish=False)
    ticket = ch.append({"n": 1}, flush=True)
    assert ticket.pushed is True
    assert gitwire.is_record_id(ticket.id)


def test_cli_append_always_pushes(cli_env, bare_repo, tmp_path):
    """CLI 는 **프로세스가 끝나는** 소비자다 — 밀지 않으면 사라지므로 항상 민다."""
    from conftest import run_cli  # noqa: PLC0415

    env = dict(cli_env)
    env["GITWIRE_HOME"] = str(tmp_path / "clihome")
    res = run_cli(env, "append", "--repo", str(bare_repo), "--payload", '{"n":1}')
    assert res.returncode == 0, res.stderr
    assert remote_ids(bare_repo), "CLI 발행이 원격에 나가지 않았다"

    # 없앤 옵션이 되살아나지 않게 못 박는다 (되살리면 조용히 유실된다)
    res = run_cli(
        env, "append", "--repo", str(bare_repo), "--payload", '{"n":2}', "--no-push"
    )
    assert res.returncode != 0
    assert "no-push" in (res.stderr + res.stdout)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
