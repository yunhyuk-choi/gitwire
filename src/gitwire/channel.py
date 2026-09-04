"""채널 = 하나의 git 레포를 전송 계층으로 쓰는 방.

중앙 서버가 없다. 각 참가자가 로컬에서 이 객체를 띄우고, 공유 git 레포를 통해
append-only 레코드를 주고받는다. egress(pull/push)만 쓰므로 인바운드가 막힌
환경에서도 동작한다.

⚠️ 이 계층은 레코드의 payload 를 **해석하지 않는다**. records.py 상단의 설계
경계 설명을 참조.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from . import clock as _clock
from . import identity, layout, records
from .credentials import Credential, NoCredential
from .cursor import Cursor, CursorStore, DEFAULT_CONSUMER
from .errors import ChannelInitError, GitError, HistoryRewritten, PushRejected
from .gitcmd import Git, GitRunner, SubprocessGitRunner

log = logging.getLogger("gitwire")

DEFAULT_BRANCH = "main"
DEFAULT_POLL_INTERVAL = 30.0
DEFAULT_BATCH_WINDOW = 3.0
DEFAULT_MAX_BATCH = 200
DEFAULT_PUSH_ATTEMPTS = 5
DEFAULT_MAX_DELIVERY_ATTEMPTS = 3

#: 배치 계산 모드
MODE_DIFF = "diff"   # 기준 커밋..목표 커밋 diff (정상 경로)
MODE_SCAN = "scan"   # 전량 나열 + 워터마크 (첫 소비 / 히스토리 재작성 후)

#: "여기까지는 원격에 올라갔다"를 로컬에 남기는 ref.
#: shallow 클론에서는 원격 커밋과의 조상 관계를 로컬에서 판정할 수 없기 때문에
#: (히스토리가 잘려 있다) 미푸시 여부를 이 마커로 판단한다.
PUSHED_REF = "refs/gitwire/pushed"


#: 기본 페이지 크기 (역방향 페이징)
DEFAULT_PAGE = 50


def default_sender(home: Path | str | None = None) -> str:
    """설치본 식별자(= 전송 수준 `sender`). 표시용 신원이 아니다.

    규칙과 근거는 `identity.py` — 요약하면 `<git 이메일>.<난수6>` 을 한 번 만들어
    `<home>/installation.txt` 에 영속시킨다. 같은 머신의 두 설치본이 갈리고,
    재시작해도 유지된다.
    """
    return identity.default_sender(home)


@dataclass(frozen=True)
class HistoryPage:
    """역방향 페이징 한 쪽.

    `has_more` 가 있어야 소비자가 **무한 스크롤의 종료 조건**을 안다 — 빈 페이지를
    한 번 더 받아보는 식으로 알아내게 두면 맨 위에서 헛요청이 한 번씩 더 나간다.
    """

    records: list[records.Record]
    has_more: bool
    """`before` 로 준 커서보다 더 앞선 레코드가 아직 남아 있나."""

    @property
    def oldest(self) -> str | None:
        """다음 페이지의 `before` 로 그대로 쓰는 값."""
        return self.records[0].id if self.records else None

    @property
    def newest(self) -> str | None:
        return self.records[-1].id if self.records else None

    def __len__(self) -> int:
        return len(self.records)

    def __iter__(self):
        return iter(self.records)


class Subscription:
    """백그라운드 구독 핸들."""

    def __init__(self, channel: "Channel", thread: threading.Thread, stop: threading.Event):
        self._channel = channel
        self._thread = thread
        self._stop = stop

    @property
    def running(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float | None = 10.0) -> None:
        self._stop.set()
        self._thread.join(timeout)

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()


class Channel:
    """git 레포 하나에 대응하는 채널.

    소비자 표면은 두 갈래이며 **같은 디스크 커서**를 공유한다:

    * **일회성 조회형** — `fetch_new()` / `peek_new()` + `ack_through()` / `history()`.
      한 번 실행하고 끝나는 프로세스(에이전트가 셸에서 부르는 경우)용.
      브라우저·상시 루프를 전제하지 않는다.
    * **상시 구독형** — `subscribe(callback)` / `poll_once(callback)`.
      루프를 도는 프로세스(웹앱 백엔드)용.

    한 채널을 서로 다른 속도로 읽어야 하면 `consumer` 이름을 다르게 열면 된다.
    """

    def __init__(
        self,
        repo_url: str,
        *,
        credential: Credential | None = None,
        consumer: str = DEFAULT_CONSUMER,
        sender: str | None = None,
        branch: str = DEFAULT_BRANCH,
        home: Path | None = None,
        runner: GitRunner | None = None,
        clock: Any | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        batch_window: float = DEFAULT_BATCH_WINDOW,
        max_batch: int = DEFAULT_MAX_BATCH,
        depth: int | None = None,
        name: str | None = None,
        author_name: str = "gitwire",
        author_email: str = "gitwire@localhost",
        clock_refresh_interval: float = _clock.DEFAULT_REFRESH_INTERVAL,
    ) -> None:
        self.repo_url = repo_url
        self.branch = branch
        self.name = name
        self.credential = credential or NoCredential()
        # 명시적으로 넘긴 sender 는 그대로 존중한다(하위호환). 없으면 설치본
        # 식별자를 **첫 사용 시점에** 만든다 — 객체 생성만으로 디스크를 건드리지
        # 않게 (`where` 처럼 읽기만 하는 경로가 있다).
        self._sender: str | None = records.slug_sender(sender) if sender else None
        self.poll_interval = poll_interval
        self.batch_window = batch_window
        self.max_batch = max_batch
        self.depth = depth
        self.author_name = author_name
        self.author_email = author_email

        self.home = Path(home) if home is not None else layout.gitwire_home()
        self.dir = layout.channel_dir(repo_url, self.home)
        self.clone_dir = self.dir / "clone"
        self.cursors = CursorStore(self.dir, consumer)
        self.consumer = self.cursors.consumer

        self._runner = runner or SubprocessGitRunner()
        self._lock = threading.RLock()
        self._pending: list[str] = []
        self._pending_since: float | None = None
        self._flush_cv = threading.Condition(self._lock)
        self._flusher: threading.Thread | None = None
        self._closing = threading.Event()
        self._opened = False
        self._attempts: dict[str, int] = {}

        if clock is not None:
            self.clock = clock
        else:
            base = _clock.clock_base_url(repo_url)
            self.clock = (
                _clock.HttpDateClock(base, refresh_interval=clock_refresh_interval)
                if base
                else _clock.SystemClock()
            )

    # -------------------------------------------------------------- 신원

    @property
    def sender(self) -> str:
        """이 설치본의 전송 수준 식별자 (표시용 이름이 아니다).

        `sender=` 를 명시하지 않았으면 `<home>/installation.txt` 의 설치본
        식별자를 쓴다(없으면 만든다) — `identity.py` 참조.
        """
        if self._sender is None:
            self._sender = identity.default_sender(self.home, runner=self._runner)
        return self._sender

    @sender.setter
    def sender(self, value: str) -> None:
        self._sender = records.slug_sender(value)

    # ------------------------------------------------------------------ git

    def _git(self, cwd: Path | None = None) -> Git:
        return Git(
            self._runner,
            cwd or self.clone_dir,
            env=self.credential.env(self.dir),
            secrets=self.credential.secrets(),
        )

    @property
    def git(self) -> Git:
        return self._git()

    # ----------------------------------------------------------------- open

    def open(self) -> "Channel":
        """클론을 확보하고 레포 레이아웃을 확정한다. 여러 번 불러도 안전하다."""
        with self._lock:
            if self._opened:
                return self
            self.dir.mkdir(parents=True, exist_ok=True)
            (self.dir / "cursors").mkdir(parents=True, exist_ok=True)
            self._ensure_clone()
            self._opened = True   # _ensure_layout 안의 재진입 방지
            try:
                self._ensure_layout()
            except BaseException:
                self._opened = False
                raise
        return self

    def _ensure_clone(self) -> None:
        if (self.clone_dir / ".git").exists():
            self._configure_clone()
            return
        self.clone_dir.parent.mkdir(parents=True, exist_ok=True)
        args = ["clone", "--origin", "origin"]
        if self.depth:
            args += ["--depth", str(self.depth), "--no-single-branch"]
        args += [self.repo_url, str(self.clone_dir)]
        self._git(self.dir).run(*args)
        self._configure_clone()
        # 빈 레포를 클론하면 HEAD 가 unborn 이다. 브랜치 이름을 규약대로 고정한다.
        if not self._has_head():
            self.git.run("symbolic-ref", "HEAD", f"refs/heads/{self.branch}")

    def _configure_clone(self) -> None:
        g = self.git
        g.run("config", "user.name", self.author_name)
        g.run("config", "user.email", self.author_email)
        g.run("config", "core.autocrlf", "false")

    def _has_head(self) -> bool:
        return self.git.ok("rev-parse", "--verify", "--quiet", "HEAD")

    def _head(self) -> str | None:
        res = self.git.run("rev-parse", "--verify", "--quiet", "HEAD", check=False)
        return res.stdout.strip() or None

    def _remote_ref(self) -> str | None:
        """마지막으로 알고 있는 원격 상태 (remote-tracking ref)."""
        res = self.git.run(
            "rev-parse", "--verify", "--quiet",
            f"refs/remotes/origin/{self.branch}", check=False,
        )
        return res.stdout.strip() or None

    def _ensure_layout(self) -> None:
        """레포에 gitwire 규약(디렉토리 구조 + 첫 커밋)이 없으면 만든다.

        '사용자가 새 repo 를 만들고 URL 만 주면 방이 된다'를 성립시키는 부분.
        """
        self._fetch(quiet=True)
        self._integrate()
        if (self.clone_dir / layout.CHANNEL_META).exists():
            return
        created = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        for rel, data in layout.repo_skeleton(self.name, created).items():
            p = self.clone_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            if not p.exists():
                p.write_bytes(data)
        g = self.git
        g.run("add", "-A", "--", ".")
        if g.run("diff", "--cached", "--quiet", check=False).returncode == 0:
            return
        g.run("commit", "-m", "gitwire: 채널 초기화")
        try:
            self._push()
        except PushRejected:
            # 다른 참가자가 먼저 초기화했다. 우리 초기화 커밋을 버리고 그쪽을 따른다.
            self._fetch()
            remote = self._remote_ref()
            if not remote:
                raise ChannelInitError("채널 초기화 경쟁에서 원격 상태를 읽지 못했다")
            g.run("reset", "--hard", remote)
        if not (self.clone_dir / layout.CHANNEL_META).exists():
            raise ChannelInitError("채널 레이아웃 확정 실패")

    # ------------------------------------------------------- 변경 감지 / 동기

    def remote_head(self) -> str | None:
        """`git ls-remote` 로 **원격 SHA 만** 조회한다 (실측 46바이트 · 613~646ms).

        브랜치와 HEAD 를 한 번의 왕복으로 같이 묻는다 (요청 1회 유지).
        """
        out = self.git.out("ls-remote", "origin", f"refs/heads/{self.branch}", "HEAD")
        head_sha = None
        for line in out.splitlines():
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            sha, ref = parts[0].strip(), parts[1].strip()
            if ref == f"refs/heads/{self.branch}":
                return sha
            if ref == "HEAD":
                head_sha = sha
        return head_sha

    def has_changes(self) -> bool:
        """원격이 우리가 아는 상태와 다른가. `fetch` 를 할지 말지의 판정.

        실측 근거: ls-remote 는 46바이트/약 640ms, fetch 는 변경이 없어도
        718~752ms 를 쓴다. 변경이 드문 채널일수록 이 선판정이 이득이다.
        """
        return self.remote_head() != self._remote_ref()

    def _fetch(self, quiet: bool = False) -> None:
        args = ["fetch", "--force", "--prune", "origin"]
        if self.depth:
            args += ["--depth", str(self.depth)]
        res = self.git.run(*args, check=False)
        if res.returncode != 0 and not quiet:
            raise GitError(args, res.returncode, res.stderr)

    def _absorb_worktree(self) -> None:
        """작업 사본에 남은 미커밋 레코드를 먼저 커밋한다.

        아래 통합 로직은 `reset --hard` 를 쓸 수 있는데, 그건 **미커밋 레코드
        파일을 지운다.** 파괴적 동작 전에 항상 흡수해서 데이터를 잃지 않는다.
        """
        g = self.git
        g.run("add", "-A", "--", records.RECORD_DIR, check=False)
        if g.run("diff", "--cached", "--quiet", check=False).returncode != 0:
            n = len(self._pending) or 1
            g.run("commit", "-m", f"gitwire: {n} record(s)")
        self._pending.clear()
        self._pending_since = None

    def _mark_pushed(self) -> None:
        """현재 HEAD 까지가 원격에 반영됐음을 로컬 ref 로 남긴다."""
        self.git.run("update-ref", PUSHED_REF, "HEAD", check=False)

    def _unpushed_count(self) -> int:
        """아직 원격에 올리지 않은 커밋 수. shallow 에서도 성립한다."""
        g = self.git
        if g.ok("rev-parse", "--verify", "--quiet", PUSHED_REF):
            res = g.run("rev-list", "--count", f"{PUSHED_REF}..HEAD", check=False)
            if res.returncode == 0:
                return int(res.stdout.strip() or "0")
        res = g.run(
            "rev-list", "--count", "HEAD", "--not", "--remotes=origin", check=False
        )
        if res.returncode == 0:
            return int(res.stdout.strip() or "0")
        return len(self._pending)

    def _integrate(self) -> None:
        """fetch 결과를 로컬 브랜치에 반영한다.

        * 미푸시 커밋이 없으면 fast-forward (또는 shallow 에서는 remote 로 맞춤).
        * 있으면 rebase (레코드가 서로 다른 파일이라 내용 충돌이 없다).
        * 원격이 재작성됐고 미푸시 커밋을 되살릴 수 없으면 **파괴하지 않고**
          HistoryRewritten 을 올린다 (미푸시 레코드 유실 방지).
        """
        remote = self._remote_ref()
        if remote is None:
            return
        g = self.git
        head = self._head()
        if head is None:
            g.run("reset", "--hard", remote)
            self._mark_pushed()
            return
        if head == remote:
            self._mark_pushed()
            return
        self._absorb_worktree()
        head = self._head() or head
        if g.ok("merge-base", "--is-ancestor", head, remote):
            g.run("merge", "--ff-only", remote)
            self._mark_pushed()
            return
        if self._unpushed_count() == 0:
            # 우리 커밋은 없다 → 원격 상태를 그대로 따른다.
            # (shallow 클론의 정상 갱신, 또는 남이 한 히스토리 재작성)
            g.run("reset", "--hard", remote)
            self._mark_pushed()
            return
        # 미푸시 커밋이 있다. 그것만 원격 위로 옮겨 심는다.
        if g.ok("rev-parse", "--verify", "--quiet", PUSHED_REF):
            if g.run(
                "rebase", "--onto", remote, PUSHED_REF, "HEAD", check=False
            ).returncode == 0:
                g.run("branch", "-f", self.branch, "HEAD", check=False)
                g.run("checkout", self.branch, check=False)
                return
            g.run("rebase", "--abort", check=False)
        if g.ok("merge-base", head, remote):
            if g.run("rebase", remote, check=False).returncode == 0:
                return
            g.run("rebase", "--abort", check=False)
        raise HistoryRewritten(
            "원격 히스토리가 재작성됐는데 로컬에 아직 push 되지 않은 레코드가 있다. "
            "flush() 로 밀어내거나 recover(discard_local=True) 로 명시적으로 버려라."
        )

    def sync(self) -> str | None:
        """변경이 있을 때만 fetch/통합한다. 반환값은 로컬 HEAD SHA."""
        with self._lock:
            self.open()
            if self.has_changes():
                self._fetch()
                self._integrate()
            return self._head()

    def recover(self, discard_local: bool = False) -> None:
        """히스토리 재작성 후 복구. discard_local=True 면 미푸시 커밋을 버린다."""
        with self._lock:
            self.open()
            self._fetch()
            remote = self._remote_ref()
            if remote and discard_local:
                self.git.run("reset", "--hard", remote)
                self._mark_pushed()
            else:
                self._integrate()

    # --------------------------------------------------------------- append

    def append(
        self,
        payload: Any,
        *,
        sender: str | None = None,
        flush: bool = False,
    ) -> records.Record:
        """레코드 1건을 발행한다. **반환값은 방금 만든 `Record`** 다.

        payload 는 **불투명한 JSON** 이다 — gitwire 는 내용을 해석하지 않는다.

        ⚠️ ID 만 돌려주던 시절에는 소비자가 `sender`·`timestamp` 를 알려면 ID
        문자열을 되파싱해야 했다. 그 과정에서 파일명의 밀리초 절삭 때문에
        마이크로초 정밀도가 깎이고, 봉투 규약이 소비자 쪽으로 새어 나갔다.
        발행한 쪽은 이미 세 값을 다 알고 있으므로 그대로 돌려주는 것이 옳다
        (로컬 에코를 그리는 소비자에게 필수다).

        파일은 즉시 디스크에 쓰이고(내구성), 커밋·push 는 배칭 창(batch_window)
        안의 여러 건을 묶어 한 커밋으로 나간다. `flush=True` 면 즉시 밀어낸다.
        """
        with self._lock:
            self.open()
            who = records.slug_sender(sender or self.sender)
            ts = self.clock.now()
            rid = records.make_record_id(ts, who)
            path = self.clone_dir / rid
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(records.encode(rid, who, ts, payload))
            record = records.Record(
                id=rid, sender=who, timestamp=ts.astimezone(timezone.utc),
                payload=payload,
            )
            self._pending.append(rid)
            if self._pending_since is None:
                self._pending_since = time.monotonic()
            need_now = (
                flush or self.batch_window <= 0 or len(self._pending) >= self.max_batch
            )
            if not need_now:
                self._ensure_flusher()
                self._flush_cv.notify_all()
        if need_now:
            self.flush()
        return record

    def _ensure_flusher(self) -> None:
        if self._flusher and self._flusher.is_alive():
            return
        self._closing.clear()
        self._flusher = threading.Thread(
            target=self._flush_loop, name="gitwire-flush", daemon=True
        )
        self._flusher.start()

    def _flush_loop(self) -> None:
        while not self._closing.is_set():
            with self._flush_cv:
                if not self._pending:
                    self._flush_cv.wait(timeout=self.batch_window)
                    if not self._pending:
                        return
                since = self._pending_since or time.monotonic()
                wait = self.batch_window - (time.monotonic() - since)
                if wait > 0 and len(self._pending) < self.max_batch:
                    self._flush_cv.wait(timeout=wait)
                    continue
            try:
                self.flush()
            except Exception:
                log.exception("gitwire: 배치 flush 실패")
                time.sleep(min(self.batch_window, 5.0))

    def flush(self, push_attempts: int = DEFAULT_PUSH_ATTEMPTS) -> int:
        """대기 중인 레코드를 **한 커밋**으로 묶어 커밋·push 한다. 커밋된 건수."""
        with self._lock:
            self.open()
            count = len(self._pending)
            self._absorb_worktree()
            if self._unpushed_count() == 0:
                return 0
            self._push_with_retry(push_attempts)
            return count

    def _push(self) -> None:
        self.git.run("push", "origin", f"HEAD:refs/heads/{self.branch}")

    def _push_with_retry(self, attempts: int) -> None:
        """push 거부(선점) 시 fetch + rebase 후 재시도.

        레코드가 서로 다른 파일이므로 rebase 는 내용 충돌 없이 항상 성공한다.
        """
        delay = 0.05
        last = max(1, attempts) - 1
        for i in range(max(1, attempts)):
            try:
                self._push()
                self._mark_pushed()
                return
            except PushRejected:
                if i == last:
                    raise
                self._fetch()
                self._integrate()
                time.sleep(delay)
                delay = min(delay * 2, 2.0)

    # ----------------------------------------------------------------- 읽기

    def _list_records(self, ref: str) -> list[str]:
        """레코드 경로 **전량**. 커서 계산(diff 폴백)·압축처럼 전량이 필요한 곳 전용.

        읽기 API 는 이걸 쓰지 않는다 — `_ids_before()` 가 날짜 디렉토리를
        역순으로 훑어 필요한 만큼만 연다.
        """
        res = self.git.run(
            "ls-tree", "-r", "--name-only", "-z", ref,
            "--", records.RECORD_DIR + "/", check=False,
        )
        if res.returncode != 0:
            return []
        return sorted(p for p in res.stdout.split("\x00") if p.endswith(".json"))

    def _list_days(self, ref: str) -> list[str]:
        """`records/` 바로 아래 날짜 디렉토리 이름 (오름차순). **비재귀**다."""
        res = self.git.run(
            "ls-tree", "-z", ref, "--", records.RECORD_DIR + "/", check=False
        )
        if res.returncode != 0:
            return []
        days = []
        for entry in res.stdout.split("\x00"):
            if not entry:
                continue
            meta, _, path = entry.partition("\t")
            fields = meta.split()
            if len(fields) >= 2 and fields[1] == "tree" and path:
                days.append(path.rsplit("/", 1)[-1])
        return sorted(days)

    def _list_day(self, ref: str, day: str) -> list[str]:
        """날짜 디렉토리 하나의 레코드 경로 (오름차순 = 시간순)."""
        res = self.git.run(
            "ls-tree", "--name-only", "-z", ref,
            "--", f"{records.RECORD_DIR}/{day}/", check=False,
        )
        if res.returncode != 0:
            return []
        return sorted(p for p in res.stdout.split("\x00") if p.endswith(".json"))

    def _ids_before(
        self, ref: str, before: str | None = None, limit: int | None = None
    ) -> list[str]:
        """`before` 보다 **앞선** 레코드 ID 를 최대 `limit` 개 (오름차순).

        ⭐ 역방향 페이징의 심장. 왜 이렇게 하나:

        * 레코드 ID 는 고정폭 타임스탬프로 시작한다 → **사전식 정렬 = 시간순**.
          그래서 ID 자체가 keyset 커서가 된다. `skip=N` 같은 offset 방식은 쓰지
          않는다 — 위로 읽는 도중 새 레코드가 도착하면 경계가 밀려 **중복·누락**이
          생긴다. 커서는 절대 밀리지 않는다.
        * 날짜 디렉토리를 **역순으로** 훑고, 커서보다 뒤(신)인 날짜는 디렉토리째
          건너뛴다. 필요한 개수를 채우는 순간 멈춘다 → 방문한 디렉토리 수는
          `O(요청 건수 / 하루 레코드 수)` 이고, 실제로 여는 blob 은 딱 요청한 만큼이다.
          (전량 나열 후 잘라내면 대화가 길수록 선형으로 느려진다.)
        """
        out: list[str] = []
        for day in reversed(self._list_days(ref)):
            prefix = f"{records.RECORD_DIR}/{day}/"
            if before is not None and prefix > before:
                continue                      # 이 날짜 전체가 커서보다 뒤다 — 열지 않는다
            names = self._list_day(ref, day)
            if before is not None:
                names = [n for n in names if n < before]
            if limit is None:
                out = names + out
                continue
            need = limit - len(out)
            if need <= 0:
                break
            out = names[-need:] + out
            if len(out) >= limit:
                break
        return out

    def _diff_records(self, base: str, target: str) -> list[str]:
        res = self.git.run(
            "diff", "--name-only", "--diff-filter=d", "-z", base, target,
            "--", records.RECORD_DIR + "/", check=False,
        )
        if res.returncode != 0:
            return self._list_records(target)
        return sorted(p for p in res.stdout.split("\x00") if p.endswith(".json"))

    def _reachable(self, sha: str | None) -> bool:
        return bool(sha) and self.git.ok("cat-file", "-e", f"{sha}^{{commit}}")

    def _read_record(self, rid: str, ref: str | None = None):
        path = self.clone_dir / rid
        try:
            data = path.read_bytes()
        except OSError:
            if not ref:
                return None
            res = self.git.run("show", f"{ref}:{rid}", check=False)
            if res.returncode != 0:
                return None
            data = res.stdout.encode("utf-8")
        try:
            return records.decode(data, rid)
        except records.RecordDecodeError:
            log.warning("gitwire: 해석할 수 없는 레코드를 건너뛴다: %s", rid)
            return None

    def _plan(self, cur: Cursor, head: str | None) -> tuple[str | None, list[str], str]:
        """(목표 커밋, 아직 전달하지 않은 레코드 경로들, 모드) 를 **결정론적**으로 계산.

        같은 커서 + 같은 레포 상태면 항상 같은 결과가 나온다. 이 성질이
        "프로세스가 중간에 죽어도 정확히 이어받는다"를 보장한다.
        """
        if head is None:
            return None, [], MODE_SCAN
        target = head
        base = cur.commit
        if (
            cur.batch_pos > 0
            and self._reachable(cur.batch_head)
            and self._reachable(base)
            and self.git.ok("merge-base", "--is-ancestor", base, cur.batch_head)
        ):
            # 중간까지 전달된 배치가 있다 → **같은 배치를 그대로 재계산**한다.
            target = cur.batch_head  # type: ignore[assignment]
        if (
            base
            and self._reachable(base)
            and self.git.ok("merge-base", "--is-ancestor", base, target)
        ):
            paths = self._diff_records(base, target)
            return target, (paths[cur.batch_pos:] if cur.batch_pos else paths), MODE_DIFF
        # 첫 소비 또는 히스토리 재작성 → 전량 나열 후 워터마크로 거른다.
        paths = self._list_records(target)
        if cur.watermark:
            paths = [p for p in paths if p > cur.watermark]
        return target, paths, MODE_SCAN

    def _advance(
        self, cur: Cursor, target: str, taken: int, paths: Sequence[str], mode: str
    ) -> Cursor:
        """전달 완료 지점을 디스크에 확정한다."""
        if paths:
            hi = max(paths)
            cur.watermark = hi if not cur.watermark else max(cur.watermark, hi)
        cur.started = True
        if mode == MODE_DIFF:
            cur.batch_pos += taken
            cur.batch_head = target
        else:
            # 워터마크 모드에서는 인덱스가 의미 없다 — 워터마크가 곧 위치다.
            cur.batch_pos = 0
            cur.batch_head = None
        self.cursors.save(cur)
        # 이 배치를 전부 소비했으면 기준 커밋을 target 으로 당긴다 (커서 축약).
        _, remaining, _ = self._plan(cur, target)
        if not remaining:
            cur.commit = target
            cur.batch_head = None
            cur.batch_pos = 0
            self.cursors.save(cur)
        return cur

    def peek_new(self, limit: int | None = None) -> list[records.Record]:
        """커서를 **전진시키지 않고** 새 레코드를 본다 (직접 ack 하려는 소비자용)."""
        with self._lock:
            head = self.sync()
            target, paths, _ = self._plan(self.cursors.load(), head)
            if limit is not None:
                paths = paths[:limit]
            return [r for r in (self._read_record(p, target) for p in paths) if r]

    def fetch_new(
        self, limit: int | None = None, *, advance: bool = True
    ) -> list[records.Record]:
        """⭐ 일회성 조회 — "지난번 이후 새 레코드를 지금 달라".

        한 번의 호출로 리스트를 반환하고, 마지막 처리 지점을 **디스크에** 남긴다.
        매번 새 프로세스로 실행해도 중복·유실이 없다.
        """
        with self._lock:
            head = self.sync()
            cur = self.cursors.load()
            target, paths, mode = self._plan(cur, head)
            take = paths if limit is None else paths[:limit]
            out = [r for r in (self._read_record(p, target) for p in take) if r]
            if advance and target is not None and take:
                self._advance(cur, target, len(take), take, mode)
            return out

    def ack_through(self, record_id: str) -> bool:
        """peek_new() 로 받아 처리한 소비자가 커서를 명시적으로 전진시킨다."""
        with self._lock:
            cur = self.cursors.load()
            target, paths, mode = self._plan(cur, self._head())
            if target is None or record_id not in paths:
                return False
            idx = paths.index(record_id) + 1
            self._advance(cur, target, idx, list(paths[:idx]), mode)
            return True

    def history(
        self, limit: int | None = None, *, before: str | None = None
    ) -> list[records.Record]:
        """커서와 무관하게 레코드를 시간순으로 읽는다.

        * `history(limit=N)` — 가장 최근 N건.
        * `history(before=<record_id>, limit=N)` — 그 ID **직전** N건 (역방향 페이징).

        어느 쪽이든 **요청한 N건의 blob 만** 연다. "더 있는가"까지 알아야 하면
        `history_page()` 를 쓴다.
        """
        with self._lock:
            head = self.sync()
            if head is None:
                return []
            paths = self._ids_before(head, before, limit)
            return [r for r in (self._read_record(p, head) for p in paths) if r]

    def history_page(
        self, *, before: str | None = None, limit: int = DEFAULT_PAGE
    ) -> HistoryPage:
        """⭐ 역방향 페이징 한 쪽 — `HistoryPage(records, has_more)`.

        `before=None` 이면 최신 쪽 한 쪽. 다음 쪽은 `before=page.oldest` 로 잇는다.
        `has_more` 는 **한 건을 더 나열해 보고**(blob 은 열지 않는다) 판정한다 —
        소비자가 빈 페이지를 받아보고서야 끝을 아는 일이 없게.
        """
        n = max(1, int(limit))
        with self._lock:
            head = self.sync()
            if head is None:
                return HistoryPage([], False)
            ids = self._ids_before(head, before, n + 1)
            has_more = len(ids) > n
            if has_more:
                ids = ids[-n:]
            recs = [r for r in (self._read_record(p, head) for p in ids) if r]
            return HistoryPage(recs, has_more)

    def record_ids(
        self, *, before: str | None = None, limit: int | None = None
    ) -> list[str]:
        """레코드 **ID 만** 시간순으로 나열한다 (payload blob 을 열지 않는다).

        `_list_records()` 를 공개하는 대신 이 형태로 연다: 소비자가 실제로 원하는
        것은 "전량 나열"이 아니라 **커서 기준 열거**이고, 전량 나열을 공개하면
        비싼 경로가 기본이 된다. 인덱스·동기화 상태 확인처럼 payload 가 필요 없는
        용도를 위한 API 다.
        """
        with self._lock:
            head = self.sync()
            if head is None:
                return []
            return self._ids_before(head, before, limit)

    def skip_to_now(self) -> None:
        """지금까지의 레코드를 '이미 처리됨'으로 표시한다 (백로그 건너뛰기)."""
        with self._lock:
            head = self.sync()
            cur = self.cursors.load()
            paths = self._list_records(head) if head else []
            cur.commit = head
            cur.batch_head = None
            cur.batch_pos = 0
            cur.started = True
            if paths:
                cur.watermark = max(paths)
            self.cursors.save(cur)

    # ----------------------------------------------------------------- 구독

    def poll_once(
        self,
        callback: Callable[[records.Record], None],
        *,
        on_error: Callable[[Any, BaseException], None] | None = None,
        max_delivery_attempts: int = DEFAULT_MAX_DELIVERY_ATTEMPTS,
    ) -> int:
        """한 틱: 변경 감지 → 새 레코드만 콜백으로 방출. 전달 건수를 돌려준다.

        레코드 **한 건을 전달할 때마다** 커서를 디스크에 저장한다. 중간에
        프로세스가 죽어도 다음 실행이 정확히 이어진다.
        """
        delivered = 0
        with self._lock:
            head = self.sync()
            cur = self.cursors.load()
            target, paths, mode = self._plan(cur, head)
            if target is None:
                return 0
            for rid in list(paths):
                rec = self._read_record(rid, target)
                if rec is None:  # 해석 불가 → 건너뛰되 커서는 전진
                    cur = self._advance(cur, target, 1, [rid], mode)
                    continue
                try:
                    callback(rec)
                except BaseException as exc:  # noqa: BLE001
                    n = self._attempts.get(rid, 0) + 1
                    self._attempts[rid] = n
                    if on_error:
                        try:
                            on_error(rec, exc)
                        except Exception:
                            log.exception("gitwire: on_error 훅 실패")
                    else:
                        log.exception("gitwire: 구독 콜백 실패: %s", rid)
                    if n < max_delivery_attempts:
                        return delivered  # 커서를 전진시키지 않고 다음 틱에 재시도
                    log.error("gitwire: %s 전달 %d회 실패 → 건너뛴다", rid, n)
                self._attempts.pop(rid, None)
                cur = self._advance(cur, target, 1, [rid], mode)
                delivered += 1
        return delivered

    def subscribe(
        self,
        callback: Callable[[records.Record], None],
        *,
        interval: float | None = None,
        on_error: Callable[[Any, BaseException], None] | None = None,
    ) -> Subscription:
        """⭐ 상시 구독 — 백그라운드 루프가 새 레코드만 골라 콜백으로 넘긴다."""
        self.open()
        stop = threading.Event()
        period = interval if interval is not None else self.poll_interval

        def loop() -> None:
            while not stop.is_set():
                try:
                    self.poll_once(callback, on_error=on_error)
                except BaseException as exc:  # noqa: BLE001
                    if on_error:
                        try:
                            on_error(None, exc)
                        except Exception:
                            log.exception("gitwire: on_error 훅 실패")
                    else:
                        log.exception("gitwire: 폴링 실패")
                stop.wait(period)

        th = threading.Thread(target=loop, name="gitwire-poll", daemon=True)
        th.start()
        return Subscription(self, th, stop)

    # ------------------------------------------------------------- 히스토리

    def compact(self, *, keep_records: int | None = None, confirm: bool = False) -> dict:
        """히스토리 압축 — 최근 N건만 남긴 **단일 커밋**으로 재작성 후 force-push.

        ⚠️ 파괴적이다. 다른 참가자는 재작성을 감지해 reset 해야 하고, 그 시점에
        **미푸시 레코드가 있으면 날아갈 위험**이 있다. 그래서:

        * `confirm=True` 없이는 실행하지 않는다 — 자동·주기 실행 경로가 없다.
        * 호출자 자신의 미푸시 레코드를 먼저 flush 한다.
        * 다른 참가자 쪽 `_integrate()` 는 재작성을 감지했을 때 미푸시 커밋이
          있으면 reset 하지 않고 HistoryRewritten 을 올린다. 즉 **데이터를 말없이
          버리지 않는다** — flush 하거나 recover(discard_local=True) 를 부르게 한다.
        """
        if not confirm:
            raise ValueError("compact() 는 파괴적이다. confirm=True 를 명시하라.")
        with self._lock:
            self.open()
            self.flush()
            head = self.sync()
            if head is None:
                return {"compacted": False, "reason": "빈 채널"}
            kept = self._list_records(head)
            if keep_records is not None:
                kept = kept[-keep_records:]
            keep_set = set(kept)
            g = self.git
            before = int(g.out("rev-list", "--count", "HEAD") or "0")
            for p in list(self.clone_dir.rglob("*.json")):
                rel = p.relative_to(self.clone_dir).as_posix()
                if rel.startswith(records.RECORD_DIR + "/") and rel not in keep_set:
                    p.unlink()
            g.run("checkout", "--orphan", "gitwire-compact")
            g.run("add", "-A", "--", ".")
            g.run("commit", "-m", f"gitwire: 히스토리 압축 (레코드 {len(kept)}건 보존)")
            g.run("branch", "-M", self.branch)
            g.run("push", "--force", "origin", f"HEAD:refs/heads/{self.branch}")
            self._mark_pushed()
            self._fetch()
            after = int(g.out("rev-list", "--count", "HEAD") or "0")
            return {
                "compacted": True,
                "commits_before": before,
                "commits_after": after,
                "records_kept": len(kept),
            }

    # ------------------------------------------------------------------ 기타

    def info(self) -> dict:
        """상태 요약. 자격증명 값은 절대 포함하지 않는다."""
        with self._lock:
            self.open()
            cur = self.cursors.load()
            return {
                "repo": layout.normalize_repo_url(self.repo_url),
                "branch": self.branch,
                "channel_dir": str(self.dir),
                "consumer": self.consumer,
                "sender": self.sender,
                "head": self._head(),
                "remote_ref": self._remote_ref(),
                "cursor": {
                    "commit": cur.commit,
                    "batch_head": cur.batch_head,
                    "batch_pos": cur.batch_pos,
                    "watermark": cur.watermark,
                    "started": cur.started,
                },
                "clock_offset": round(float(getattr(self.clock, "offset", 0.0)), 3),
                "pending": len(self._pending),
            }

    def close(self) -> None:
        """대기 레코드를 밀어내고 백그라운드 스레드를 정리한다."""
        self._closing.set()
        with self._flush_cv:
            self._flush_cv.notify_all()
        try:
            if self._pending:
                self.flush()
        finally:
            if self._flusher and self._flusher.is_alive():
                self._flusher.join(timeout=5.0)

    def __enter__(self) -> "Channel":
        return self.open()

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"Channel(repo={layout.normalize_repo_url(self.repo_url)!r}, "
            f"branch={self.branch!r}, consumer={self.consumer!r})"
        )


def open_channel(repo_url: str, **kwargs: Any) -> Channel:
    """채널을 열고(빈 레포면 초기화까지) 돌려준다. 라이브러리 진입점."""
    return Channel(repo_url, **kwargs).open()
