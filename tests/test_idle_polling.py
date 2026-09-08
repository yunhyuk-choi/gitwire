"""유휴 폴링 비용 — **폴링 1회 = git 1개**, 그리고 그 대가로 아무것도 팔지 않았음.

실측 배경 (2026-09-08 · Windows 11 · git 2.51 · 방 1개·새 메시지 없음 · 90초)::

    폴링 1회당  git 호출 7개 · 프로세스 16개 · 논리 읽기 8.96 MB
    그 중 원격을 봐야 하는 것은 `ls-remote` **1개**뿐이었다.

git subprocess 는 하는 일과 무관하게 약 494 KB 를 읽는다(프로세스 기동 비용).
그래서 아무 일도 없는 방이 분당 수 MB 를 읽었다.

여기서 못 박는 것:

* **I-1** 유휴 폴링은 git 을 **딱 하나**(`ls-remote`) 부른다.
* **I-2** 새 메시지는 그대로 도착한다 (2-클론 · 실제 git).
* **I-3** 발행·push 가 어긋나지 않는다 — 캐시된 HEAD 로 push 하지 않는다.
* **I-4** **밖에서** 사람이 클론을 건드려도(직접 `git` 명령) 다음 조회가 즉시
  새 값을 본다 — 캐시가 조용히 낡지 않는다.
* **I-5** 롤업·`compact`(히스토리 재작성) 뒤에도 어긋나지 않는다.
* **I-6** 무효화 판정은 **화이트리스트 fail-safe** 다 — 모르는 서브커맨드는
  로컬을 바꾼 것으로 취급한다 (호출 지점을 사람이 챙기지 않아도 된다).
* **I-7** 스탬프를 만들 수 없는 클론(reftable 등)에서는 캐시를 **켜지 않는다**.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import gitwire
from gitwire import localrefs
from gitwire.clock import FixedOffsetClock
from gitwire.gitcmd import GitResult, SubprocessGitRunner


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
        self.calls.append(localrefs.subcommand(args) or "?")
        return super().run(args, **kwargs)

    def count(self, name: str) -> int:
        return sum(1 for c in self.calls if c == name)

    def reset(self) -> None:
        self.calls.clear()


def _open(bare_repo, home, sender, runner=None, **extra):
    kwargs = dict(
        home=home, sender=sender, clock=FixedOffsetClock(0.0), batch_window=0.0,
        auto_rollup=False,
    )
    kwargs.update(extra)
    if runner is not None:
        kwargs["runner"] = runner
    return gitwire.Channel(str(bare_repo), **kwargs).open()


def _fill(channel, count, start=None, step=None):
    channel.clock = StepClock(
        start or datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc),
        step or timedelta(seconds=1),
    )
    return [channel.append({"i": i}, flush=True) for i in range(count)]


def _git(cwd: Path, *args: str) -> str:
    """**우리 러너를 거치지 않고** 클론에 직접 git 을 돌린다 (= 사람이 한 짓).

    실패하면 stderr 를 그대로 올린다 — 조용히 넘어가면 테스트가 무엇을 보고
    실패했는지 알 수 없다.
    """
    proc = subprocess.run(
        ["git", *args], cwd=str(cwd), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"git {' '.join(args)} (cwd={cwd}) 실패 {proc.returncode}: "
            f"{proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def _git_bare(repo: Path, *args: str) -> str:
    """bare 레포에 직접 git 을 돌린다.

    `--git-dir` 를 **명시**한다 — `safe.bareRepository=explicit` 로 굳혀 둔
    환경(우리 CI 를 포함해 흔하다)에서는 bare 레포 안에서 그냥 부르면 거부된다.
    """
    return _git(repo, f"--git-dir={repo}", *args)


# ------------------------------------------- I-1. 유휴 폴링 = git 1개


def test_idle_poll_spawns_exactly_one_git(bare_repo, homes):
    """⭐ 요점 — 아무 일도 없으면 `ls-remote` 하나뿐이다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 5)
    runner = CountingRunner()
    reader = _open(bare_repo, homes("r"), "bob", runner)
    try:
        reader.skip_to_now()
        reader.poll_once(lambda r: None)          # 캐시를 데운다 (상시 앱의 상태)

        for _ in range(4):                        # 유휴 폴 4회
            runner.reset()
            assert reader.poll_once(lambda r: None) == 0
            assert runner.calls == ["ls-remote"], (
                f"유휴 폴링에서 git 을 {len(runner.calls)}개 불렀다: {runner.calls}"
            )

        info = reader.local_ref_cache_info()
        assert info["enabled"] is True
        assert info["hits"] >= 8, info            # 폴마다 HEAD + 원격추적 ref
    finally:
        reader.close()
        writer.close()


def test_idle_reads_and_info_do_not_spawn_git(bare_repo, homes):
    """로컬 읽기(`fresh=False`)도 `rev-parse` 를 더 이상 띄우지 않는다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 6)
    runner = CountingRunner()
    reader = _open(bare_repo, homes("r"), "bob", runner)
    try:
        reader.sync()
        reader.history(limit=3, fresh=False)      # 캐시를 데운다
        runner.reset()
        page = reader.history_page(limit=3, fresh=False)
        assert len(page.records) == 3
        assert runner.calls == [], runner.calls
    finally:
        reader.close()
        writer.close()


# ------------------------------------------------- I-2. 새 메시지 도착


def test_new_records_still_arrive_after_idle_polls(bare_repo, homes):
    """캐시를 데운 뒤에도 새 레코드가 정확히 한 번 도착한다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 3)
    runner = CountingRunner()
    reader = _open(bare_repo, homes("r"), "bob", runner)
    try:
        reader.skip_to_now()
        for _ in range(3):
            assert reader.poll_once(lambda r: None) == 0

        writer.clock = StepClock(
            datetime(2026, 9, 2, tzinfo=timezone.utc), timedelta(seconds=1)
        )
        writer.append({"i": "새 메시지"}, flush=True)

        got: list = []
        assert reader.poll_once(got.append) == 1
        assert got[0].payload["i"] == "새 메시지"
        # 배달 직후의 첫 폴은 캐시를 다시 채우므로 `rev-parse` 가 한 번 남는다
        # (로컬이 실제로 바뀌었으니 다시 물어보는 것이 맞다). 그 다음부터 유휴다.
        assert reader.poll_once(got.append) == 0
        runner.reset()
        assert reader.poll_once(got.append) == 0
        assert runner.calls == ["ls-remote"], runner.calls
        assert len(got) == 1, "중복 전달이 있었다"
    finally:
        reader.close()
        writer.close()


def test_rollup_check_in_the_idle_path_costs_no_git(bare_repo, homes):
    """롤업 후보 확인(`maybe_rollup`)도 유휴 폴에 git 을 더하지 않는다.

    폴링마다 보게 강제(`rollup_interval=0`)하고, 접을 수 있는 지난 날짜가 없는
    상태(레코드가 오늘 날짜)로 둔다 — 확인 비용이 **캐시된 나열 1회 = git 0개**
    임을 못 박는다. (실제 기본값은 1시간마다 한 번 본다.)
    """
    today = datetime.now(timezone.utc)
    writer = _open(bare_repo, homes("w"), "alice", auto_rollup=False)
    _fill(writer, 3, start=today, step=timedelta(seconds=1))
    runner = CountingRunner()
    reader = _open(
        bare_repo, homes("r"), "bob", runner,
        auto_rollup=True, rollup_interval=0.0,
    )
    try:
        reader.skip_to_now()
        reader.poll_once(lambda r: None)
        for _ in range(3):
            runner.reset()
            assert reader.poll_once(lambda r: None) == 0
            assert runner.calls == ["ls-remote"], runner.calls
        assert reader.rollup_last_error is None
    finally:
        reader.close()
        writer.close()


# --------------------------------------------------- I-3. push 정합성


def test_publishing_after_idle_polls_pushes_the_right_commit(bare_repo, homes):
    """유휴 캐시가 낀 뒤에 발행해도 push 가 어긋나지 않는다."""
    alice = _open(bare_repo, homes("a"), "alice")
    bob = _open(bare_repo, homes("b"), "bob")
    try:
        _fill(alice, 2)
        alice.skip_to_now()      # 자기 레코드는 이미 봤다 (전송 계층은 걸러 주지 않는다)
        bob.skip_to_now()
        for _ in range(3):
            bob.poll_once(lambda r: None)         # 유휴 — 캐시가 찬다

        bob.clock = StepClock(
            datetime(2026, 9, 3, tzinfo=timezone.utc), timedelta(seconds=1)
        )
        rec = bob.append({"i": "밥이 쓴다"}, flush=True)

        # 원격에 실제로 올라갔나 (bare 레포를 직접 본다)
        listing = _git_bare(bare_repo, "ls-tree", "-r", "--name-only", "HEAD")
        assert rec.id in listing.splitlines(), listing
        assert bob._unpushed_count() == 0

        got: list = []
        alice.poll_once(got.append)
        assert [r.payload["i"] for r in got] == ["밥이 쓴다"]
    finally:
        bob.close()
        alice.close()


# ------------------------------ I-4. 밖에서 클론을 건드려도 낡지 않는다


def test_external_git_command_is_noticed(bare_repo, homes):
    """⭐ 사람이 그 클론에서 직접 git 을 돌린 경우 (우리 러너를 거치지 않는다).

    스탬프(= `.git` 안 ref 파일의 상태)가 달라지므로 다음 조회가 새 값을 본다.
    이 방어가 없으면 증상이 *새 메시지가 조용히 안 보이는 것* 이다.
    """
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 4)
    reader = _open(bare_repo, homes("r"), "bob")
    try:
        reader.sync()
        head = reader._head()
        assert head and reader._head() == head    # 캐시 적중

        clone = reader.clone_dir
        assert _git(clone, "rev-parse", "HEAD") == head

        # 사람이 한 걸음 되돌린다 (우리 러너를 통하지 않는다).
        _git(clone, "reset", "--hard", "HEAD~1")
        moved = _git(clone, "rev-parse", "HEAD")
        assert moved != head

        assert reader._head() == moved, "캐시가 낡은 HEAD 를 계속 돌려줬다"

        # 원격추적 ref 도 같은 방어를 받는다.
        remote_before = reader._remote_ref()
        _git(clone, "update-ref", "refs/remotes/origin/main", moved)
        assert reader._remote_ref() == moved != remote_before

        # 그리고 정합성은 그대로 회복된다 — 폴링이 밀린 만큼 다시 가져온다.
        got: list = []
        reader.poll_once(got.append)
        assert len(reader.history(fresh=False)) == 4
    finally:
        reader.close()
        writer.close()


def test_packed_refs_change_is_noticed(bare_repo, homes):
    """느슨한 ref 가 `packed-refs` 로 접혀도(gc) 값을 다시 읽는다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 3)
    reader = _open(bare_repo, homes("r"), "bob")
    try:
        reader.sync()
        first = reader._head()
        _git(reader.clone_dir, "pack-refs", "--all")
        _git(reader.clone_dir, "reset", "--hard", "HEAD~1")
        assert reader._head() == _git(reader.clone_dir, "rev-parse", "HEAD") != first
    finally:
        reader.close()
        writer.close()


# --------------------------------------- I-5. 롤업 · compact 뒤에도 정합


def test_rollup_then_reads_stay_consistent(bare_repo, homes):
    """롤업(비파괴)은 로컬 HEAD 를 움직인다 — 캐시가 따라와야 한다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 6, start=datetime(2026, 8, 1, tzinfo=timezone.utc),
          step=timedelta(hours=1))
    reader = _open(bare_repo, homes("r"), "bob")
    try:
        reader.sync()
        before_head = reader._head()
        ids_before = reader.record_ids(fresh=False)

        res = writer.rollup(force=True, min_records=1)
        assert res["rolled"] is True, res

        reader.sync()
        assert reader._head() != before_head, "롤업 커밋을 못 봤다"
        assert reader.record_ids(fresh=False) == ids_before, "레코드가 사라졌다"
        assert [r.id for r in reader.history(fresh=False)] == ids_before
    finally:
        reader.close()
        writer.close()


def test_compact_then_reads_stay_consistent(bare_repo, homes):
    """`compact()` 는 히스토리를 재작성한다 (force-push) — 그 뒤에도 맞아야 한다."""
    writer = _open(bare_repo, homes("w"), "alice")
    _fill(writer, 8)
    reader = _open(bare_repo, homes("r"), "bob")
    try:
        reader.sync()
        reader.skip_to_now()
        assert len(reader.history(fresh=False)) == 8

        res = writer.compact(keep_records=3, confirm=True)
        assert res["compacted"] is True, res
        assert writer._head() == _git(writer.clone_dir, "rev-parse", "HEAD")

        reader.sync()
        assert reader._head() == _git(reader.clone_dir, "rev-parse", "HEAD")
        assert len(reader.history(fresh=False)) == 3
    finally:
        reader.close()
        writer.close()


# ---------------------------------------- I-6. 무효화 판정 (화이트리스트)


@pytest.mark.parametrize(
    "args, expected",
    [
        (["-c", "gc.auto=0", "rev-parse", "HEAD"], "rev-parse"),
        (["-c", "a=b", "-c", "c=d", "ls-remote", "origin"], "ls-remote"),
        (["fetch", "--force", "--prune", "origin"], "fetch"),
        (["-C", "/tmp/x", "commit", "-m", "x"], "commit"),
        (["--git-dir", "/tmp/x/.git", "reset", "--hard"], "reset"),
        (["-c", "a=b"], ""),
    ],
)
def test_subcommand_parsing(args, expected):
    assert localrefs.subcommand(args) == expected


def test_unknown_subcommand_counts_as_mutating():
    """⭐ fail-safe — 모르는 것은 '로컬을 바꿨다'로 분류한다."""
    assert localrefs.is_read_only(["rev-parse", "HEAD"]) is True
    assert localrefs.is_read_only(["ls-remote", "origin"]) is True
    for mutating in (
        ["fetch"], ["push"], ["commit"], ["reset"], ["rebase"], ["merge"],
        ["checkout"], ["branch"], ["update-ref"], ["symbolic-ref", "HEAD", "x"],
        ["add"], ["rm"], ["read-tree"], ["update-index"], ["gc"], ["pack-refs"],
        ["config", "user.name", "x"], ["명령을-모른다"], [],
    ):
        assert localrefs.is_read_only(mutating) is False, mutating


class _NullRunner:
    def run(self, args, *, cwd=None, env=None, timeout=None):
        return GitResult(0, "", "")


class _BoomRunner:
    def run(self, args, *, cwd=None, env=None, timeout=None):
        raise RuntimeError("git 이 터졌다")


def test_guarded_runner_invalidates(tmp_path):
    cache = localrefs.LocalRefCache(tmp_path / "clone")
    guarded = localrefs.GuardedRunner(_NullRunner(), cache)

    guarded.run(["-c", "a=b", "rev-parse", "HEAD"])
    assert cache.info()["invalidations"] == 0

    guarded.run(["-c", "a=b", "fetch", "origin"])
    assert cache.info()["invalidations"] == 1
    assert cache.info()["last_invalidation"] == "git fetch"


def test_guarded_runner_invalidates_even_when_git_fails(tmp_path):
    """⚠️ 실패한 명령도 로컬을 절반쯤 바꿔 놓을 수 있다 (중단된 rebase 등)."""
    cache = localrefs.LocalRefCache(tmp_path / "clone")
    guarded = localrefs.GuardedRunner(_BoomRunner(), cache)
    with pytest.raises(RuntimeError):
        guarded.run(["rebase", "origin/main"])
    assert cache.info()["invalidations"] == 1


def test_our_own_commit_invalidates(bare_repo, homes):
    """우리가 커밋하면 다음 `_head()` 가 새 커밋을 본다 (겹 2 — 러너 무효화)."""
    ch = _open(bare_repo, homes("a"), "alice")
    try:
        ch.sync()
        before = ch._head()
        ch.append({"i": 1}, flush=True)
        assert ch._head() != before
        assert ch._head() == _git(ch.clone_dir, "rev-parse", "HEAD")
    finally:
        ch.close()


def test_two_processes_sharing_one_clone(bare_repo, homes):
    """⭐ 같은 클론을 보는 **두 채널**(= 상시 앱 + 셸에서 부른 CLI 와 같은 모양).

    한쪽이 로컬을 바꿨을 때 다른 쪽 캐시가 그것을 알아채야 한다 — 다른 쪽의
    `GuardedRunner` 는 그 호출을 **보지 못한다**(다른 객체·다른 프로세스다).
    스탬프 겹만이 이 경우를 막는다.
    """
    home = homes("shared")
    app = _open(bare_repo, home, "shared-app")
    cli = _open(bare_repo, home, "shared-cli")
    try:
        assert app.clone_dir == cli.clone_dir
        app.sync()
        before = app._head()
        assert app._head() == before               # 캐시 적중

        cli.append({"i": "다른 프로세스가 발행"}, flush=True)

        after = _git(app.clone_dir, "rev-parse", "HEAD")
        assert after != before
        assert app._head() == after, "다른 채널이 바꾼 로컬을 못 봤다"
        assert [r.payload["i"] for r in app.history(fresh=False)] == [
            "다른 프로세스가 발행"
        ]
    finally:
        cli.close()
        app.close()


# ------------------------------------- I-7. 신뢰할 수 없으면 캐시하지 않는다


def test_cache_is_off_for_unstampable_clones(tmp_path):
    """`.git` 이 없거나 reftable 백엔드면 **매번 git 에게 묻는다**."""
    clone = tmp_path / "clone"
    clone.mkdir()
    cache = localrefs.LocalRefCache(clone)
    assert cache.stamp() is None                  # `.git` 이 아예 없다
    assert cache.info()["enabled"] is False

    calls = []

    def resolve():
        calls.append(1)
        return "sha"

    assert cache.resolve("HEAD", resolve) == "sha"
    assert cache.resolve("HEAD", resolve) == "sha"
    assert len(calls) == 2, "캐시할 수 없는데 캐시했다"
    assert cache.info()["unstampable"] == 2

    # reftable 백엔드 — ref 가 `refs/*` 파일로 없으므로 스탬프가 무의미하다.
    gitdir = clone / ".git"
    (gitdir / "reftable").mkdir(parents=True)
    (gitdir / "HEAD").write_bytes(b"ref: refs/heads/.invalid\n")
    assert cache.stamp() is None
    assert cache.info()["enabled"] is False

    # `.git` 이 파일(워크트리·서브모듈)인 경우도 마찬가지.
    other = tmp_path / "wt"
    other.mkdir()
    (other / ".git").write_text("gitdir: ../real/.git\n", encoding="utf-8")
    assert localrefs.LocalRefCache(other).stamp() is None


def test_stamp_tracks_head_symref_target(bare_repo, homes):
    """HEAD 가 가리키는 ref 파일까지 스탬프에 든다 (다른 브랜치로 옮겨도 안전)."""
    ch = _open(bare_repo, homes("a"), "alice")
    try:
        ch.append({"i": 1}, flush=True)
        cache = ch._localrefs
        stamp = cache.stamp()
        assert stamp is not None
        names = [part[0] for part in stamp]
        assert "HEAD" in names and "refs/heads/main" in names
        assert "packed-refs" in names and "config" in names
    finally:
        ch.close()
