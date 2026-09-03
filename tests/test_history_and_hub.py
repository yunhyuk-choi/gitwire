"""히스토리 관리(압축·shallow)와 다중 채널.

압축은 파괴적이다 — force-push 로 히스토리를 재작성하므로 다른 참가자는
그 시점에 로컬을 맞춰야 한다. 여기서 고정하는 것은 **미푸시 레코드를 말없이
버리지 않는다**는 성질이다.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import gitwire
from gitwire.clock import FixedOffsetClock
from gitwire.errors import HistoryRewritten


def commit_count(clone: Path) -> int:
    out = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=str(clone), capture_output=True, text=True,
        encoding="utf-8", errors="replace", check=True,
    )
    return int(out.stdout.strip())


# ------------------------------------------------------------------ 압축


def test_compact_requires_explicit_confirmation(participant):
    a = participant("alice")
    with pytest.raises(ValueError):
        a.compact()          # 자동·암묵 실행 경로가 없다


def test_compact_collapses_history_and_keeps_records(participant):
    a = participant("alice")
    for i in range(8):
        a.append({"i": i}, flush=True)
    before = commit_count(a.clone_dir)
    assert before >= 9   # 초기화 1 + 레코드 8

    result = a.compact(confirm=True)
    assert result["compacted"] is True
    assert result["commits_after"] == 1
    assert result["records_kept"] == 8
    assert commit_count(a.clone_dir) == 1

    # 레코드 자체는 그대로다 — 기록은 레코드 파일이지 커밋이 아니다
    assert [r.payload["i"] for r in a.history()] == list(range(8))


def test_compact_can_drop_old_records(participant):
    a = participant("alice")
    for i in range(6):
        a.append({"i": i}, flush=True)
    a.compact(keep_records=2, confirm=True)
    assert [r.payload["i"] for r in a.history()] == [4, 5]


def test_other_participant_recovers_from_rewrite(participant):
    """미푸시 레코드가 없는 참가자는 자동으로 재작성된 히스토리를 따라간다."""
    a = participant("alice")
    b = participant("bob")
    for i in range(5):
        a.append({"i": i}, flush=True)
    assert len(b.fetch_new()) == 5

    a.compact(confirm=True)

    # B 는 예외 없이 계속 동작하고, 이미 본 레코드를 다시 받지 않는다
    assert b.fetch_new() == []
    assert [r.payload["i"] for r in b.history()] == list(range(5))

    a.append({"i": 99}, flush=True)
    assert [r.payload["i"] for r in b.fetch_new()] == [99]


def test_unpushed_records_survive_a_rewrite(participant):
    """다른 참가자가 압축하는 사이 내 미푸시 레코드가 있어도 살아남는다."""
    a = participant("alice")
    b = participant("bob", batch_window=60.0)  # push 를 늦춘다
    a.append({"who": "alice"}, flush=True)

    b.append({"who": "bob-unpushed"})          # 커밋도 push 도 아직
    b._absorb_worktree()                       # 커밋만 하고 push 는 안 한 상태
    a.compact(confirm=True)

    b.sync()                                    # 재작성 감지 → 미푸시분 재적용
    b.flush()

    c = participant("carol")
    who = sorted(r.payload["who"] for r in c.fetch_new())
    assert who == ["alice", "bob-unpushed"], "미푸시 레코드가 재작성에 휩쓸렸다"


def test_rewrite_without_recovery_marker_refuses_to_destroy(participant):
    """되살릴 근거가 없으면 조용히 버리지 않고 예외를 올린다."""
    a = participant("alice")
    b = participant("bob", batch_window=60.0)
    a.append({"i": 0}, flush=True)
    b.fetch_new()

    b.append({"i": 1})
    b._absorb_worktree()
    # 복구 마커를 지워 "미푸시분을 어디서부터 옮겨야 할지" 알 수 없게 만든다
    b.git.run("update-ref", "-d", "refs/gitwire/pushed", check=False)

    a.compact(confirm=True)
    with pytest.raises(HistoryRewritten):
        b.sync()

    # 사람이 명시적으로 버리기로 하면 그때 복구된다
    b.recover(discard_local=True)
    assert b.sync() is not None


# ------------------------------------------------------------ 다중 채널


@pytest.fixture
def second_bare(tmp_path):
    repo = tmp_path / "origin2.git"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(repo)],
        check=True, capture_output=True,
    )
    return repo


def test_hub_handles_several_channels(bare_repo, second_bare, tmp_path):
    """채널 = 레포 = 클론. 여러 개를 동시에 다룬다."""
    with gitwire.Hub(
        home=tmp_path / "hubhome", clock=FixedOffsetClock(0.0), batch_window=0.0
    ) as hub:
        room1 = hub.open(str(bare_repo))
        room2 = hub.open(str(second_bare))
        assert room1.dir != room2.dir       # 채널마다 별도 클론
        assert len(hub.channels) == 2

        room1.append({"room": 1}, flush=True)
        room2.append({"room": 2}, flush=True)

        # 같은 URL 을 다시 열면 같은 객체 (클론이 중복 생기지 않는다)
        assert hub.open(str(bare_repo)) is room1

    with gitwire.Hub(
        home=tmp_path / "readerhome", clock=FixedOffsetClock(0.0), batch_window=0.0
    ) as reader:
        reader.open(str(bare_repo))
        reader.open(str(second_bare))
        got = reader.fetch_new_all()
        rooms = sorted(r.payload["room"] for recs in got.values() for r in recs)
        assert rooms == [1, 2]
        # 두 번째 호출에는 아무것도 없다 (채널별 커서가 각각 전진했다)
        assert all(not recs for recs in reader.fetch_new_all().values())


def test_hub_channels_use_independent_credentials(bare_repo, second_bare, tmp_path):
    """채널마다 다른 토큰을 줄 수 있다 (전역 하나가 아니다)."""
    with gitwire.Hub(home=tmp_path / "h", clock=FixedOffsetClock(0.0)) as hub:
        c1 = hub.open(str(bare_repo), credential=gitwire.NoCredential())
        c2 = hub.open(
            str(second_bare),
            credential=gitwire.TokenCredential("tok-for-org-b", username="orgB"),
        )
        assert c1.credential is not c2.credential
        env2 = c2.credential.env(c2.dir)
        assert env2["GITWIRE_USERNAME"] == "orgB"
        # 채널 상태 요약에는 토큰이 들어가지 않는다
        assert "tok-for-org-b" not in repr(c2.info())
