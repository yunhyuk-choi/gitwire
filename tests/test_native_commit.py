"""git 없는 커밋(`nativecommit`) — 파이썬이 쓴 오브젝트·인덱스·ref 를 **git 이 정상으로 읽어야** 한다.

판정은 전부 진짜 git 이 한다: `fsck` 가 조용하고, `status` 가 깨끗하고, 인덱스에서
만든 `write-tree` 가 커밋의 트리와 같고, `log --stat` 이 그 파일을 보이고, reflog 가
쌓이고, 다른 참가자가 원격에서 그 레코드를 읽는다. 그리고 "모르면 하지 않는다" —
인덱스가 v4 면 예전 그대로 git 으로 커밋한다.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import gitwire
from gitwire import localrefs, nativecommit
from gitwire.clock import FixedOffsetClock
from gitwire.gitcmd import SubprocessGitRunner


def git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=str(cwd), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    assert proc.returncode == 0, f"git {' '.join(args)} 실패 rc={proc.returncode}: {proc.stderr}"
    return proc.stdout


class CountingRunner(SubprocessGitRunner):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    def run(self, args, **kwargs):
        self.calls.append(localrefs.subcommand(args) or "?")
        return super().run(args, **kwargs)


def _assert_git_agrees(clone: Path, head: str) -> None:
    """git 의 눈으로 검산 — 이 네 가지가 곧 '같은 바이트로 썼다'의 정의다."""
    assert git("rev-parse", "HEAD", cwd=clone).strip() == head
    assert git("status", "--porcelain", cwd=clone) == "", "작업 사본·인덱스가 HEAD 와 어긋났다"
    fsck = subprocess.run(["git", "fsck", "--strict", "--no-dangling"], cwd=str(clone),
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    # ⚠️ 경고도 안 된다 — 예컨대 ref 파일에 CRLF 를 쓰면 `trailingRefContent` 경고가 난다
    # (Windows 텍스트 모드 `os.open` 이 실제로 그렇게 했다).
    assert fsck.returncode == 0 and (fsck.stdout + fsck.stderr).strip() == "", fsck.stdout + fsck.stderr
    for name in ("HEAD", git("symbolic-ref", "HEAD", cwd=clone).strip(), "refs/gitwire/pushed"):
        raw = (clone / ".git" / name).read_bytes() if (clone / ".git" / name).exists() else b""
        if raw and not raw.startswith(b"ref:"):
            assert len(raw) == 41 and raw.endswith(b"\n") and b"\r" not in raw, (name, raw)
    # 인덱스에서 만든 트리 = 커밋의 트리 (인덱스가 커밋과 정확히 같은 내용이다)
    assert git("write-tree", cwd=clone).strip() == git("rev-parse", "HEAD^{tree}", cwd=clone).strip()


def test_native_commit_is_what_git_would_have_written(participant, bare_repo, homes):
    runner = CountingRunner()
    a = participant("a", runner=runner, autopublish=False, auto_archive=False)
    clone = a.clone_dir
    before_log = len(git("reflog", "show", "HEAD", cwd=clone).splitlines())
    parent = a.local_head()

    runner.calls.clear()
    rec = a.append({"n": 1})
    made = a.flush()
    assert made and rec.pushed
    assert runner.calls == ["push"], runner.calls           # add·commit 이 없다

    head = a.local_head()
    assert head != parent
    _assert_git_agrees(clone, head)
    # 커밋 내용 — 부모·메시지·그 파일 하나
    assert git("rev-parse", "HEAD^", cwd=clone).strip() == parent
    assert git("log", "-1", "--pretty=%s", cwd=clone).strip() == "gitwire: 1 record(s)"
    stat = git("log", "-1", "--stat", "--pretty=", cwd=clone)
    assert rec.id in stat and "1 file changed" in stat, stat
    # 저자·커미터는 git 이 같은 환경에서 썼을 값 (conftest 가 env 로 준다)
    assert git("log", "-1", "--pretty=%an <%ae>|%cn <%ce>", cwd=clone).strip() == \
        "test <test@localhost>|test <test@localhost>"
    # blob 은 파일 바이트 그대로
    assert git("show", f"HEAD:{rec.id}", cwd=clone).encode() == (clone / rec.id).read_bytes()
    # reflog 도 git 처럼 쌓인다
    log = git("reflog", "show", "HEAD", cwd=clone).splitlines()
    assert len(log) == before_log + 1 and "commit: gitwire: 1 record(s)" in log[0]

    # 두 번째 커밋도 같은 규율 (같은 날짜 디렉토리에 두 번째 항목 — 트리 정렬 검산)
    for i in range(3):
        a.append({"n": i + 2})
    a.flush()
    _assert_git_agrees(clone, a.local_head())
    assert git("log", "-1", "--pretty=%s", cwd=clone).strip() == "gitwire: 3 record(s)"

    # 다른 참가자가 원격에서 전부 읽는다
    b = participant("b", autopublish=False, auto_archive=False)
    got = [r.payload["n"] for r in b.history(limit=10, fresh=True)]
    assert got == [1, 2, 3, 4], got


def test_native_commit_carries_participant_state_and_seeds_the_state_cache(participant):
    runner = CountingRunner()
    a = participant("a", runner=runner, autopublish=False, auto_archive=False)
    clone = a.clone_dir
    rel = a.write_state("me@localhost", {"cursor": "c1"})
    rec = a.append({"n": 1})
    runner.calls.clear()
    a.flush()
    assert runner.calls == ["push"], runner.calls
    head = a.local_head()
    _assert_git_agrees(clone, head)
    names = git("show", "--pretty=", "--name-only", "HEAD", cwd=clone).split()
    assert sorted(names) == sorted([rel, rec.id]), names
    assert git("log", "-1", "--pretty=%s", cwd=clone).strip() == "gitwire: 1 record(s) + 참가자 상태 1건"

    # ⭐ push 직후 read_state — chat 이 `_after_push` 에서 부르는 그 호출 — 가 git 0개다.
    runner.calls.clear()
    got = a.read_state("me@localhost", fresh=False)
    assert got is not None and got.value == {"cursor": "c1"}
    assert runner.calls == [], runner.calls
    # 예열한 나열이 git 의 답과 같다
    listed = git("ls-tree", "HEAD", "--", "participants/", cwd=clone)
    assert rel.split("/", 1)[1] in listed
    blob = [l.split()[2] for l in listed.splitlines() if l.endswith(rel.split("/", 1)[1])][0]
    assert a._state_index(head)["me@localhost"] == blob

    # 상태만 바꾼 flush (레코드 0건) 도 이 경로다
    a.write_state("me@localhost", {"cursor": "c2"})
    runner.calls.clear()
    a.flush()
    assert runner.calls == ["push"], runner.calls
    _assert_git_agrees(clone, a.local_head())
    assert git("log", "-1", "--pretty=%s", cwd=clone).strip() == "gitwire: 참가자 상태 1건"


def test_native_commit_falls_back_to_git_when_the_index_is_not_v2(participant, caplog):
    runner = CountingRunner()
    a = participant("a", runner=runner, autopublish=False, auto_archive=False)
    clone = a.clone_dir
    git("update-index", "--index-version", "4", cwd=clone)
    runner.calls.clear()
    with caplog.at_level(logging.INFO, logger="gitwire"):
        rec = a.append({"n": 1})
        a.flush()
    assert rec.pushed
    assert runner.calls == ["add", "commit", "push"], runner.calls
    assert any("git 으로 커밋한다" in r.getMessage() and "버전 4" in r.getMessage() for r in caplog.records)
    _assert_git_agrees(clone, a.local_head())
    # 같은 이유는 한 번만 적는다
    n = sum("git 으로 커밋한다" in r.getMessage() for r in caplog.records)
    a.append({"n": 2}); a.flush()
    assert sum("git 으로 커밋한다" in r.getMessage() for r in caplog.records) == n


def test_native_commit_falls_back_when_head_is_not_our_branch(participant):
    runner = CountingRunner()
    a = participant("a", runner=runner, autopublish=False, auto_archive=False)
    clone = a.clone_dir
    git("checkout", "-q", "--detach", cwd=clone)
    runner.calls.clear()
    a.append({"n": 1})
    try:
        a.flush()
    except Exception:
        pass                                    # 분리 HEAD 에서의 push 는 이 테스트의 관심이 아니다
    assert runner.calls[:2] == ["add", "commit"], runner.calls


def test_native_commit_ignores_a_stray_gitignore(participant, bare_repo, homes):
    """`.gitignore` 에 `records/` 가 들어가도 레코드는 나간다 — 예전 git 경로에서는
    `add` 가 아무것도 스테이징하지 못해 소리 내어 실패하던 상황이다."""
    a = participant("a", autopublish=False, auto_archive=False)
    ignore = a.clone_dir / ".gitignore"
    ignore.write_text(ignore.read_text(encoding="utf-8") + "records/\n", encoding="utf-8")
    rec = a.append({"n": 1})
    a.flush()
    assert rec.pushed
    assert git("status", "--porcelain", cwd=a.clone_dir) == " M .gitignore\n"
    b = participant("b", autopublish=False, auto_archive=False)
    assert [r.id for r in b.history(limit=5, fresh=True)] == [rec.id]


def test_push_failure_rewinds_a_native_commit_cleanly(participant):
    """push 가 막히면 `_rewind` 가 되돌린다 — 네이티브 커밋도 `reset --mixed` 가 그대로 읽는다."""
    from test_stamp_on_push import BlockedPushRunner

    runner = BlockedPushRunner()
    a = participant("a", runner=runner, autopublish=False, auto_archive=False)
    clone = a.clone_dir
    base = a.local_head()
    runner.blocked = True
    rec = a.append({"n": 1})
    try:
        a.flush(push_attempts=1)
    except Exception:
        pass
    assert not rec.pushed
    assert a.local_head() == base
    assert git("status", "--porcelain", cwd=clone) == ""
    runner.blocked = False
    a.flush()
    assert rec.pushed
    _assert_git_agrees(clone, a.local_head())


def test_ref_cas_refuses_when_the_ref_moved(tmp_path):
    repo = tmp_path / "r"
    git("init", "-q", "-b", "main", str(repo), cwd=tmp_path)
    (repo / "a").write_text("a")
    git("add", "a", cwd=repo)
    git("-c", "user.name=x", "-c", "user.email=x@x", "commit", "-q", "-m", "a", cwd=repo)
    head = git("rev-parse", "HEAD", cwd=repo).strip()
    stale = "0" * 40
    try:
        nativecommit.update_ref_cas(repo, "refs/heads/main", stale, "1" * 40)
    except nativecommit.Unsupported:
        pass
    else:
        raise AssertionError("움직인 ref 를 덮어썼다")
    assert git("rev-parse", "HEAD", cwd=repo).strip() == head
    assert not (repo / ".git/refs/heads/main.lock").exists()


def test_tree_body_sorts_like_git():
    """디렉토리는 이름 뒤에 '/' 가 붙은 것처럼 비교한다 — `a-b`(파일) < `a`(디렉토리) < `a0`."""
    body = nativecommit.tree_body([
        (b"a0", 0o100644, "1" * 40),
        (b"a", 0o040000, "2" * 40),
        (b"a-b", 0o100644, "3" * 40),
    ])
    names = [n for n, _, _ in nativecommit.parse_tree(body)]
    assert names == [b"a-b", b"a", b"a0"]
