"""참가자별 **가변** 상태 — 예약 경로 `participants/` (`state.py`).

여기서 못 박는 것:

* **P-1** 예약 경로가 **커밋에 실린다.** 예전에는 `git add -A -- records/` 라
  소비자가 클론에 파일을 떨어뜨려도 커밋에 들어가지 않았다 (조용한 유실).
* **P-2** 레코드 규약을 깨지 않는다 — `records/`·`archive/` 와 섞이지 않고,
  읽기(커서·페이징)가 상태 파일을 레코드로 오인하지 않는다.
* **P-3** ⭐ **동시 갱신에 충돌이 없다.** 두 참가자가 같은 순간 자기 값을
  전진시켜 push 해도 둘 다 반영된다 (경로가 달라 내용 충돌이 없다).
* **P-4** 한 사람이 기기를 둘 쓰는 경우(= **같은 경로**)에도 push 가 깨지지
  않는다 — rebase 충돌 규약(`_REBASE_RESOLVE`)이 그 하나를 해소한다.
* **P-5** 롤업(비파괴)·`compact()`(파괴적 재작성)가 예약 경로를 **건드리지
  않는다.** 레코드가 아니므로 접거나 버리지 않는다.
* **P-6** 값은 **덮어쓰는 것**이다 — 파일 하나, 히스토리 스캔 없이 지금 값을
  읽는다. 참가자가 늘어야 파일이 늘고, 사건 수와 무관하다.
* **P-7** 읽기는 **캐시**된다 (같은 커밋 = git 호출 0회). 유휴 폴링 비용을
  늘리지 않는다는 근거다.

⚠️ 실제 git 으로 돈다. 대역으로 흉내내면 push 경합·rebase·커밋 대상 pathspec
같은 것들은 아무것도 증명하지 못한다.
"""

from __future__ import annotations

import json
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import gitwire
from gitwire import state as _state
from gitwire.clock import FixedOffsetClock
from gitwire.gitcmd import SubprocessGitRunner
from gitwire import localrefs


class CountingRunner(SubprocessGitRunner):
    """git 호출을 하위명령별로 센다 (`test_idle_polling.py` 와 같은 도구)."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    def run(self, args, **kwargs):
        self.calls.append(localrefs.subcommand(args) or "?")
        return super().run(args, **kwargs)

    def reset(self) -> None:
        self.calls.clear()


def _git_bare(repo: Path, *args: str) -> str:
    """bare 레포에 직접 git 을 건다 (`--git-dir` 명시 — safe.bareRepository 대비)."""
    proc = subprocess.run(
        ["git", f"--git-dir={repo}", *args], cwd=str(repo), capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        raise AssertionError(f"git {args} 실패: {proc.stderr.strip()}")
    return proc.stdout.strip()


def _remote_files(bare: Path) -> list[str]:
    return _git_bare(bare, "ls-tree", "-r", "--name-only", "HEAD").splitlines()


def _remote_state(bare: Path, key: str) -> dict:
    raw = _git_bare(bare, "show", f"HEAD:{_state.state_path(key)}")
    return json.loads(raw)


# ------------------------------------------------------------ 경로·봉투 규약


def test_경로와_키는_파일명으로_안전하게_깎인다():
    assert _state.state_path("yh.choi@example.com") == (
        "participants/yh.choi@example.com.json"
    )
    # 경로 탈출 금지
    assert ".." not in _state.state_key("../../etc/passwd")
    assert "/" not in _state.state_key("a/b/c")
    assert _state.state_key("") == "anon"
    # 왕복
    assert _state.key_from_path(_state.state_path("bob@x.io")) == "bob@x.io"
    assert _state.key_from_path("records/20260908/x.json") is None
    assert _state.key_from_path("participants/.gitkeep") is None


def test_봉투는_값을_해석하지_않는다():
    data = _state.encode("me@x.io", "me@x.io", {"cursor": "records/x.json", "n": 3})
    assert data.endswith(b"\n") and b"\r" not in data
    env = json.loads(data.decode("utf-8"))
    assert env["gitwire_state"] == _state.STATE_VERSION
    got = _state.decode(data, "me@x.io")
    assert got.value == {"cursor": "records/x.json", "n": 3}
    assert got.identity == "me@x.io"
    assert got.updated_at is not None

    with pytest.raises(_state.StateDecodeError):
        _state.decode(b"{}", "me@x.io")            # value 가 없다 = 봉투가 아니다
    with pytest.raises(_state.StateDecodeError):
        _state.decode(b"not json", "me@x.io")


# ---------------------------------------- P-1. 예약 경로가 커밋에 실린다


def test_상태가_원격까지_간다(bare_repo, participant):
    """⭐ 예전 결함의 정면 판정 — `git add -A -- records/` 는 이걸 놓쳤다."""
    alice = participant("alice")
    alice.write_state("alice@x.io", {"cursor": "records/20260908/a.json"}, flush=True)

    files = _remote_files(bare_repo)
    assert "participants/alice@x.io.json" in files, files
    assert _remote_state(bare_repo, "alice@x.io")["value"] == {
        "cursor": "records/20260908/a.json"
    }


def test_레코드와_상태가_한_커밋으로_나간다(bare_repo, participant):
    """부수 상태의 발행이 메시지 전송보다 앞서 끼어들지 않는다 — 같이 실려 간다."""
    alice = participant("alice", batch_window=5.0)
    rec = alice.append({"i": 1})
    alice.write_state("alice@x.io", {"cursor": rec.id})
    assert alice.info()["pending"] == 1
    assert alice.info()["pending_state"] == 1
    alice.flush()

    files = _remote_files(bare_repo)
    assert rec.id in files and "participants/alice@x.io.json" in files
    log = _git_bare(bare_repo, "log", "--format=%s", "-1")
    assert "record(s)" in log and "참가자 상태" in log, log


def test_상태만_바뀌어도_커밋이_생긴다(bare_repo, participant):
    alice = participant("alice")
    alice.write_state("alice@x.io", {"cursor": None}, flush=True)
    assert "참가자 상태" in _git_bare(bare_repo, "log", "--format=%s", "-1")


def test_있음_판정은_git_을_부르지_않는다(bare_repo, homes):
    """방을 열 때마다 하는 판정이라 비용이 0 이어야 한다 (로컬 stat 한 번)."""
    runner = CountingRunner()
    ch = gitwire.Channel(
        str(bare_repo), home=homes("a"), sender="alice", runner=runner,
        clock=FixedOffsetClock(0.0), batch_window=0.0, auto_rollup=False,
    ).open()
    try:
        runner.reset()
        assert ch.state_exists("alice@x.io") is False
        assert ch.state_exists("bob@x.io") is False
        assert runner.calls == [], runner.calls

        ch.write_state("alice@x.io", {"cursor": None}, flush=True)
        runner.reset()
        assert ch.state_exists("alice@x.io") is True
        assert runner.calls == [], runner.calls
    finally:
        ch.close()


# ------------------------------------- P-2. 레코드 규약을 깨지 않는다


def test_상태_파일이_레코드로_보이지_않는다(bare_repo, participant):
    alice = participant("alice")
    rec = alice.append({"i": 1}, flush=True)
    alice.write_state("alice@x.io", {"cursor": rec.id}, flush=True)

    bob = participant("bob")
    assert bob.record_ids() == [rec.id]
    assert [r.id for r in bob.history()] == [rec.id]
    got: list = []
    assert bob.poll_once(got.append) == 1
    assert [r.id for r in got] == [rec.id]
    # 상태를 또 갱신해도 레코드가 늘지 않는다 (사건이 아니다).
    alice.write_state("alice@x.io", {"cursor": rec.id, "n": 2}, flush=True)
    assert bob.poll_once(got.append) == 0
    assert bob.record_ids() == [rec.id]


def test_남이_쓴_상태를_읽는다(bare_repo, participant):
    alice = participant("alice")
    bob = participant("bob")
    alice.write_state("alice@x.io", {"cursor": "records/a.json"}, flush=True)
    bob.write_state("bob@x.io", {"cursor": "records/b.json"}, flush=True)
    alice.sync()

    states = alice.read_states()
    assert set(states) == {"alice@x.io", "bob@x.io"}
    assert states["bob@x.io"].value == {"cursor": "records/b.json"}
    assert alice.read_state("bob@x.io").identity == "bob@x.io"
    assert alice.read_state("없는사람@x.io") is None


def test_깨진_상태_한_개가_나머지를_막지_않는다(bare_repo, participant):
    """append-only 가 아니어도 **읽기는 관대하게** — 남이 쓴 파일이다."""
    alice = participant("alice")
    alice.write_state("alice@x.io", {"cursor": "records/a.json"}, flush=True)
    broken = alice.clone_dir / "participants" / "zz@x.io.json"
    broken.write_bytes(b"{ this is not json")
    alice.flush()

    states = alice.read_states()
    assert set(states) == {"alice@x.io"}       # 깨진 것만 빠진다


# --------------------------------- P-3. ⭐ 동시 갱신 — 충돌 0


def test_두_사람이_같은_순간_전진시켜도_충돌이_없다(bare_repo, participant):
    """⭐ 요점 — **경로마다 쓰는 사람이 하나**라 내용 충돌이 구조적으로 없다."""
    alice = participant("alice")
    bob = participant("bob")
    # 둘 다 같은 base 를 본 상태에서 각자 값을 쓴다.
    alice.sync()
    bob.sync()

    errors: list[BaseException] = []
    gate = threading.Barrier(2)

    def advance(channel, key, value):
        try:
            channel.write_state(key, {"cursor": value})
            gate.wait(timeout=30)
            channel.flush()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=advance, args=(alice, "alice@x.io", "records/a9.json")),
        threading.Thread(target=advance, args=(bob, "bob@x.io", "records/b9.json")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, errors
    # 원격에 **둘 다** 있다 (한쪽이 다른 쪽을 밀어내지 않았다).
    assert _remote_state(bare_repo, "alice@x.io")["value"]["cursor"] == "records/a9.json"
    assert _remote_state(bare_repo, "bob@x.io")["value"]["cursor"] == "records/b9.json"
    # 그리고 각자 읽어도 상대 값이 보인다.
    alice.sync()
    got = alice.read_states()
    assert got["bob@x.io"].value["cursor"] == "records/b9.json"
    assert len(got) == 2


def test_메시지와_상태가_같이_경합해도_메시지가_살아남는다(bare_repo, participant):
    """읽음 표시 발행이 메시지를 잃게 만들면 최악이다 — 둘 다 원격에 남는다."""
    alice = participant("alice")
    bob = participant("bob")
    alice.sync()
    bob.sync()

    a_rec = alice.append({"who": "alice"})
    alice.write_state("alice@x.io", {"cursor": "records/a.json"})
    b_rec = bob.append({"who": "bob"})
    bob.write_state("bob@x.io", {"cursor": "records/b.json"})

    errors: list[BaseException] = []
    gate = threading.Barrier(2)

    def flush(channel):
        try:
            gate.wait(timeout=30)
            channel.flush()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=flush, args=(c,)) for c in (alice, bob)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, errors
    files = _remote_files(bare_repo)
    for want in (a_rec.id, b_rec.id,
                 "participants/alice@x.io.json", "participants/bob@x.io.json"):
        assert want in files, (want, files)


# ------------------- P-4. 한 사람 두 기기 = 같은 경로 (예외적 경합)


def test_한_사람_두_기기가_같은_경로를_써도_push_가_깨지지_않는다(bare_repo, homes):
    """같은 사람이 노트북·데스크탑을 쓰면 **같은 파일**이다.

    이 경합이 rebase 충돌 → `HistoryRewritten` 이 되면 **메시지 전송까지**
    막힌다. 그래서 충돌 해소 규칙을 기반이 미리 정해 둔다 (`_REBASE_RESOLVE`).
    값의 단조성은 소비자가 `max` 로 지킨다 — 여기서는 *깨지지 않는다*만 본다.
    """
    def open_device(name: str):
        return gitwire.Channel(
            str(bare_repo), home=homes(name), sender=name,
            clock=FixedOffsetClock(0.0), batch_window=0.0, auto_rollup=False,
        ).open()

    laptop = open_device("laptop")
    desktop = open_device("desktop")
    try:
        laptop.sync()
        desktop.sync()
        # 같은 키(= 같은 사람)에 각 기기가 자기 값을 쓴다.
        laptop.write_state("me@x.io", {"cursor": "records/m5.json"}, flush=True)
        desktop.write_state("me@x.io", {"cursor": "records/m9.json"}, flush=True)

        # 둘 다 예외 없이 통과했고, 원격에는 그 파일이 하나만 있다.
        files = [
            f for f in _remote_files(bare_repo)
            if f.startswith("participants/") and f.endswith(".json")
        ]
        assert files == ["participants/me@x.io.json"], files
        value = _remote_state(bare_repo, "me@x.io")["value"]["cursor"]
        assert value in ("records/m5.json", "records/m9.json")

        # 그리고 채널이 계속 쓸 수 있는 상태로 남아 있다 (레코드 발행이 산다).
        rec = desktop.append({"i": 1}, flush=True)
        assert rec.id in _remote_files(bare_repo)
    finally:
        desktop.close()
        laptop.close()


# ------------------------- P-5. 롤업·compact 가 건드리지 않는다


def test_롤업이_상태를_건드리지_않는다(bare_repo, participant):
    alice = participant("alice")
    # 지난 날짜 레코드 (접을 수 있게)
    alice.clock = FixedOffsetClock(0.0)
    old = datetime(2026, 8, 1, tzinfo=timezone.utc)

    class Step:
        offset = 0.0

        def __init__(self):
            self.at = old

        def now(self):
            value = self.at
            self.at = self.at + timedelta(hours=1)
            return value

    alice.clock = Step()
    ids = [alice.append({"i": i}, flush=True).id for i in range(3)]
    alice.clock = FixedOffsetClock(0.0)
    alice.write_state("alice@x.io", {"cursor": ids[-1]}, flush=True)

    res = alice.rollup(force=True, min_records=1)
    assert res["rolled"] is True, res

    files = _remote_files(bare_repo)
    assert "participants/alice@x.io.json" in files, files
    assert any(f.startswith("archive/") for f in files), files
    # 아카이브 안에 상태가 섞이지 않았다.
    arch = _git_bare(bare_repo, "show", f"HEAD:{[f for f in files if f.startswith('archive/')][0]}")
    assert "gitwire_state" not in arch
    # 그리고 값은 그대로 읽힌다.
    alice.sync()
    assert alice.read_state("alice@x.io").value == {"cursor": ids[-1]}


def test_compact_이후에도_상태가_남는다(bare_repo, participant):
    alice = participant("alice")
    for i in range(5):
        alice.append({"i": i}, flush=True)
    alice.write_state("alice@x.io", {"cursor": "records/keep.json"}, flush=True)

    res = alice.compact(keep_records=2, confirm=True)
    assert res["compacted"] is True, res
    files = _remote_files(bare_repo)
    assert "participants/alice@x.io.json" in files, files
    assert alice.read_state("alice@x.io").value == {"cursor": "records/keep.json"}


# --------------------------------------- P-6. 덮어쓴다 (파일 하나)


def test_값은_덮어쓴다_히스토리를_스캔하지_않는다(bare_repo, participant):
    alice = participant("alice")
    for n in range(5):
        alice.write_state("alice@x.io", {"cursor": f"records/{n}.json"}, flush=True)

    # 참가자 파일은 **하나**다 (갱신 횟수와 무관하다).
    files = [
        f for f in _remote_files(bare_repo)
        if f.startswith("participants/") and f.endswith(".json")
    ]
    assert files == ["participants/alice@x.io.json"], files
    # 지금 값은 마지막 값이고, 그것을 읽는 데 과거를 훑지 않는다 (파일 하나).
    assert alice.read_state("alice@x.io").value == {"cursor": "records/4.json"}
    blob = _remote_state(bare_repo, "alice@x.io")
    assert blob["value"] == {"cursor": "records/4.json"}
    assert list(blob) == ["gitwire_state", "key", "identity", "updated_at", "value"]


# ------------------------------------------- P-7. 읽기는 캐시된다


def test_같은_커밋을_되풀이해_읽으면_git_을_부르지_않는다(bare_repo, homes, participant):
    """유휴 폴링(git 1개)을 되돌리지 않는다는 근거."""
    writer = participant("alice")
    writer.write_state("alice@x.io", {"cursor": "records/a.json"}, flush=True)

    runner = CountingRunner()
    reader = gitwire.Channel(
        str(bare_repo), home=homes("r"), sender="bob", runner=runner,
        clock=FixedOffsetClock(0.0), batch_window=0.0, auto_rollup=False,
    ).open()
    try:
        reader.sync()
        assert reader.read_states()["alice@x.io"].value["cursor"] == "records/a.json"
        runner.reset()
        for _ in range(5):
            assert set(reader.read_states()) == {"alice@x.io"}
        assert runner.calls == [], runner.calls

        # 값이 바뀌면(= 새 커밋) 당연히 다시 읽는다.
        writer.write_state("alice@x.io", {"cursor": "records/z.json"}, flush=True)
        reader.sync()
        assert reader.read_states()["alice@x.io"].value["cursor"] == "records/z.json"
    finally:
        reader.close()
