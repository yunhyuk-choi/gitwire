"""채널 = 하나의 git 레포를 전송 계층으로 쓰는 방.

중앙 서버가 없다. 각 참가자가 로컬에서 이 객체를 띄우고, 공유 git 레포를 통해
append-only 레코드를 주고받는다. egress(pull/push)만 쓰므로 인바운드가 막힌
환경에서도 동작한다.

⚠️ 이 계층은 레코드의 payload 를 **해석하지 않는다**. records.py 상단의 설계
경계 설명을 참조.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from . import clock as _clock
from . import identity, layout, records
from . import localrefs as _localrefs
from . import rollup as _rollup
from .credentials import Credential, NoCredential
from .cursor import Cursor, CursorStore, DEFAULT_CONSUMER
from .errors import ChannelInitError, GitError, HistoryRewritten, PushRejected
from .gitcmd import Git, GitRunner, SubprocessGitRunner
from .treecache import TreeCache

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

#: 자격증명 메모리 캐시의 기본 수명(초). `credential_cache()` 참조.
DEFAULT_CREDENTIAL_CACHE_TIMEOUT = 900.0

#: 롤업 후보를 다시 살펴보는 최소 간격(초). 근거는 `maybe_rollup()`.
DEFAULT_ROLLUP_INTERVAL = 3600.0
#: 한 번의 롤업에서 push 경합에 양보하고 다시 계산해 볼 횟수.
DEFAULT_ROLLUP_ATTEMPTS = 4

#: "빈 레포" 로 쳐 주는 파일들. forge 가 새 레포를 만들 때 넣어 주는 것들이라
#: 이게 있다고 해서 "쓰고 있는 레포"는 아니다.
EMPTY_REPO_FILES = frozenset({
    "README.md", "README", "README.rst", "readme.md",
    "LICENSE", "LICENSE.md", "LICENSE.txt", "COPYING",
    ".gitignore", ".gitattributes",
})


def default_sender(home: Path | str | None = None) -> str:
    """설치본 식별자(= 전송 수준 `sender`). 표시용 신원이 아니다.

    규칙과 근거는 `identity.py` — 요약하면 `<git 이메일>.<난수6>` 을 한 번 만들어
    `<home>/installation.txt` 에 영속시킨다. 같은 머신의 두 설치본이 갈리고,
    재시작해도 유지된다.
    """
    return identity.default_sender(home)


def credential_cache(
    timeout: float = DEFAULT_CREDENTIAL_CACHE_TIMEOUT,
) -> list[str]:
    """`Channel(credential_helpers=...)` 에 그대로 넘길 수 있는 **메모리 캐시** 사슬.

    git 이 표준으로 제공하는 `credential-cache` 헬퍼 한 줄이다. 무엇을 사고
    무엇을 파는지는 `Channel._configure_credential_helpers()` 의 주석에 있다 —
    **켜기 전에 읽어라.** 기본값은 끔(옵트인)이다.
    """
    return [f"cache --timeout={max(1, int(timeout))}"]


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
        credential_helpers: Sequence[str] | None = None,
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
        auto_rollup: bool | None = None,
        rollup_grace_hours: float = _rollup.DEFAULT_GRACE_HOURS,
        rollup_min_records: int = _rollup.DEFAULT_MIN_RECORDS,
        rollup_interval: float = DEFAULT_ROLLUP_INTERVAL,
    ) -> None:
        self.repo_url = repo_url
        self.branch = branch
        self.name = name
        self.credential = credential or NoCredential()
        # None = 이 클론의 credential 설정을 **건드리지 않는다** (기본값).
        self.credential_helpers = (
            None if credential_helpers is None else list(credential_helpers)
        )
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
        # 지난 날짜 롤업 (rollup.py). 기본은 **켜짐** — 파일 개수가 아프기 시작하는
        # 규모는 활발한 방이면 며칠이면 닿는데(README 실측표), 손으로 부르게 해
        # 두면 아무도 부르지 않는다. 끄려면 auto_rollup=False 또는 환경변수
        # GITWIRE_AUTO_ROLLUP=0.
        self.auto_rollup = (
            os.environ.get("GITWIRE_AUTO_ROLLUP", "1").strip().lower()
            not in ("0", "false", "no", "off")
            if auto_rollup is None
            else bool(auto_rollup)
        )
        self.rollup_grace_hours = float(rollup_grace_hours)
        self.rollup_min_records = max(1, int(rollup_min_records))
        self.rollup_interval = float(rollup_interval)
        self.rollup_last_error: str | None = None
        self._rollup_thread: threading.Thread | None = None
        self._rollup_checked: float | None = None

        self.home = Path(home) if home is not None else layout.gitwire_home()
        self.dir = layout.channel_dir(repo_url, self.home)
        self.clone_dir = self.dir / "clone"
        self.cursors = CursorStore(self.dir, consumer)
        self.consumer = self.cursors.consumer

        # ⭐ 로컬 ref 캐시 (`localrefs.py`) — 유휴 폴링에서 `rev-parse` 를 없앤다.
        #
        # 러너를 `GuardedRunner` 로 감싼다: 이 채널의 **모든** git 호출이 그 하나를
        # 지나므로(임시 인덱스를 쓰는 롤업 경로와 `identity` 조회까지) 로컬을 바꾸는
        # 호출이 새로 생겨도 무효화를 빠뜨릴 수 없다. 판정은 읽기 전용 화이트리스트이고
        # 모르는 서브커맨드는 위험한 쪽(= 무효화)으로 분류한다 (fail-safe).
        self._localrefs = _localrefs.LocalRefCache(
            self.clone_dir, refs=(f"refs/remotes/origin/{branch}",)
        )
        self._runner = _localrefs.GuardedRunner(
            runner or SubprocessGitRunner(), self._localrefs
        )
        # ⭐ 락이 둘이다. **순서는 언제나 `_remote` → `_lock`** 이며 그 반대는 없다.
        #
        # `_lock`   : 작업 사본·인덱스·`_pending`·커서·캐시를 만지는 **짧은 로컬**
        #             구간. `append()` 와 모든 읽기 API 가 이것만 쓴다 →
        #             네트워크 때문에 막히는 일이 없어야 한다.
        # `_remote` : "원격 상태 전이"(커밋 → push → fetch → 통합) 전체를
        #             직렬화한다. **네트워크를 여기서 기다린다.** 두 스레드가
        #             동시에 push 하거나, push 중에 남이 rebase 해서 우리가 방금
        #             올린 커밋을 로컬에서 갈아치우는 일을 막는다.
        #
        # 재진입 가능(RLock)이라 `compact()` 처럼 안에서 `flush()`·`sync()` 를
        # 다시 부르는 경로가 그대로 성립한다.
        #
        # ⚠️ `open()`/`_ensure_layout()` 은 `_remote` 를 **잡지 않는다**. 그것들은
        # `_lock` 안에서 불릴 수 있으므로(`append()` 등) 잡는 순간 순서가 뒤집혀
        # 교착이 생긴다. 부트스트랩은 `_opened` + `_lock` 으로만 보호한다.
        self._lock = threading.RLock()
        self._remote = threading.RLock()
        self._pending: list[str] = []
        self._pending_since: float | None = None
        self._flush_cv = threading.Condition(self._lock)
        self._flusher: threading.Thread | None = None
        self._closing = threading.Event()
        self._opened = False
        self._attempts: dict[str, int] = {}
        # 나열 결과 캐시. 키가 sha(내용 주소)라 stale 이 정의상 불가능하다 —
        # 근거와 크기 제한은 treecache.py 참조.
        self._trees = TreeCache()
        # 마지막으로 연 아카이브 한 개 (sha, {id: 줄}). 페이징은 보통 같은
        # 날짜를 연달아 읽으므로 이 한 칸이 거의 전부를 흡수한다.
        self._archive_memo: tuple[str | None, dict[str, str]] = (None, {})

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
        self._configure_credential_helpers()

    def _inherited_credential_helpers(self) -> list[str]:
        """system·global 에 설정된 helper 목록 (이 클론의 local 은 제외).

        local 을 함께 읽으면 우리가 방금 쓴 값이 다시 섞여 호출마다 사슬이
        길어진다. 그래서 상위 스코프만 읽는다.
        """
        out: list[str] = []
        for scope in ("--system", "--global"):
            res = self.git.run(
                "config", scope, "--get-all", "credential.helper", check=False
            )
            if res.returncode == 0:
                out += [line.strip() for line in res.stdout.splitlines() if line.strip()]
        return out

    def _configure_credential_helpers(self) -> None:
        """이 클론의 **로컬** `credential.helper` 사슬을 다시 짠다 (옵트인).

        왜 이런 게 필요한가 — 실측(Windows 11 · git 2.51 · GitHub private repo,
        같은 머신에서 5회씩)::

            helper 사슬                       ls-remote 1회 (중앙값)
            manager(=GCM, 시스템 기본)                 1320 ms
            cache --timeout=900 → manager               1167 ms
            cache --timeout=900 (단독, 캐시 적중)         910 ms

        즉 **GCM 조회·저장이 왕복당 약 400ms** 다 (`git credential-manager get`
        은 셸 + .NET 프로세스를 새로 띄운다). 남는 ~900ms 는 GCM 과 무관한
        고정비다 — `ls-remote` 는 private repo 에 HTTPS 왕복을 3번 한다
        (①익명 GET → 401 ②인증 GET → 200 ③protocol-v2 ls-refs POST) + 프로세스
        기동. 그래서 **캐시로 지울 수 있는 것은 1.3초 중 0.4초뿐이다.**

        ⚠️ 무엇을 파는가 (켜기 전에 알아야 할 것):

        * `git-credential-cache` 는 **자격증명을 데몬 프로세스의 메모리에**
          timeout 동안 들고 있는다. 디스크에는 쓰지 않지만, 그 시간 동안
          같은 OS 사용자로 도는 프로세스는 소켓을 통해 꺼내 쓸 수 있다.
          짧게 잡을수록 노출 창이 좁고, 대신 왕복마다 상위 helper 로 되돌아간다.
        * 그래서 **기본값은 끔**이다. 켜는 쪽이 명시해야 한다.

        무엇을 사지 않는가 — 이 설정은 **이 클론의 `.git/config` 에만** 쓴다.
        사용자의 global·system 설정은 읽기만 하고 절대 고치지 않는다.
        상속된 helper 는 사슬 **뒤에** 그대로 남겨서, 캐시가 비었을 때 원래대로
        GCM 이 채워 준다 (첫 왕복의 동작이 바뀌지 않는다).
        """
        helpers = self.credential_helpers
        if helpers is None:
            return                      # 기본 — 아무것도 쓰지 않는다
        inherited = [h for h in self._inherited_credential_helpers() if h not in helpers]
        g = self.git
        # 빈 값 하나가 "여기서부터 목록을 새로 센다"는 git 의 규약이다.
        g.run("config", "--local", "--replace-all", "credential.helper", "", check=False)
        for helper in list(helpers) + inherited:
            g.run("config", "--local", "--add", "credential.helper", helper, check=False)

    def _has_head(self) -> bool:
        return self.git.ok("rev-parse", "--verify", "--quiet", "HEAD")

    def _head(self) -> str | None:
        """로컬 HEAD 의 sha. **유휴에서는 git 을 부르지 않는다** (`localrefs.py`).

        예전에는 폴링마다 `rev-parse` 프로세스가 하나 떴다. 유휴에서 로컬 HEAD 는
        바뀌지 않으므로 매번 물어볼 이유가 없다 — 캐시가 유효한지는 `.git` 안
        ref 파일의 스탬프로 확인한다(프로세스 0개). 밖에서 사람이 이 클론에
        git 명령을 돌려도 그 스탬프가 달라지므로 즉시 다시 묻는다.
        """
        return self._localrefs.resolve("HEAD", self._resolve_head)

    def _resolve_head(self) -> str | None:
        res = self.git.run("rev-parse", "--verify", "--quiet", "HEAD", check=False)
        return res.stdout.strip() or None

    def _remote_ref(self) -> str | None:
        """마지막으로 알고 있는 원격 상태 (remote-tracking ref).

        `_head()` 와 같은 캐시를 쓴다 — 이 ref 는 우리가 `fetch` 할 때만 바뀌고,
        그때 파일이 바뀌므로 스탬프가 알려 준다.
        """
        return self._localrefs.resolve(
            f"refs/remotes/origin/{self.branch}", self._resolve_remote_ref
        )

    def _resolve_remote_ref(self) -> str | None:
        res = self.git.run(
            "rev-parse", "--verify", "--quiet",
            f"refs/remotes/origin/{self.branch}", check=False,
        )
        return res.stdout.strip() or None

    def _repo_contents(self, ref: str) -> list[str]:
        res = self.git.run("ls-tree", "-r", "--name-only", "-z", ref, check=False)
        if res.returncode != 0:
            return []
        return [p for p in res.stdout.split("\x00") if p]

    def _refuse_if_repo_has_content(self) -> None:
        """⚠️ **쓰고 있는 레포를 채널로 만들지 않는다.**

        `open()` 은 채널이 아닌 레포를 만나면 규약을 심고 **push 한다.** 그 동작이
        "주소만 주면 방이 된다"를 성립시키지만, 주소를 잘못 넣으면 *남의(또는 내)
        코드 레포에 커밋이 올라간다.* 실제로 그렇게 만든 적이 있다 — 인증 실패를
        시험하려고 진짜 코드 레포 주소를 넣었더니 `gitwire.json` 이 그 레포
        main 에 push 됐다.

        그래서 규칙을 좁힌다: **빈 레포(또는 README·LICENSE 정도만 있는 새 레포)
        에만 심는다.** 이미 내용이 있으면 아무것도 쓰지 않고 거부한다.
        정말 그 레포를 채널로 쓰고 싶으면 `gitwire.json` 을 직접 커밋해 두면 된다
        (그러면 아래 `CHANNEL_META` 검사에서 이미 채널로 인식된다).
        """
        head = self._head()
        if head is None:
            return                      # 완전히 빈 레포 — 방으로 만든다
        extra = [
            path for path in self._repo_contents(head)
            if path not in EMPTY_REPO_FILES
            and not path.startswith(records.RECORD_DIR + "/")
            and path != layout.CHANNEL_META
        ]
        if not extra:
            return                      # README·LICENSE 뿐 — 갓 만든 레포다
        sample = ", ".join(sorted(extra)[:3]) + (" 등" if len(extra) > 3 else "")
        raise ChannelInitError(
            f"이 레포에는 이미 내용이 있다 ({sample}). 채널 규약은 **빈 레포에만** "
            "심는다 — 쓰고 있는 레포에 실수로 커밋하지 않기 위해서다. "
            "새(빈) 레포를 만들어 그 주소를 쓰거나, 정말 이 레포를 채널로 쓰려면 "
            f"{layout.CHANNEL_META} 을 직접 커밋해 두어라."
        )

    def _ensure_layout(self) -> None:
        """레포에 gitwire 규약(디렉토리 구조 + 첫 커밋)이 없으면 만든다.

        '사용자가 새 repo 를 만들고 URL 만 주면 방이 된다'를 성립시키는 부분.
        단, **빈 레포에만** 심는다 (`_refuse_if_repo_has_content`).
        """
        self._fetch(quiet=True)
        self._integrate()
        if (self.clone_dir / layout.CHANNEL_META).exists():
            return
        self._refuse_if_repo_has_content()
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

    def _mark_pushed(self, sha: str | None = None) -> None:
        """여기까지가 원격에 반영됐음을 로컬 ref 로 남긴다 (기본 현재 HEAD)."""
        self.git.run("update-ref", PUSHED_REF, sha or "HEAD", check=False)

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
        """변경이 있을 때만 fetch/통합한다. 반환값은 로컬 HEAD SHA.

        ⭐ **네트워크 왕복은 락 밖에서 한다.**

        `ls-remote` 는 실측 1.3초다(자격증명 헬퍼 + HTTPS 왕복 3회 + 프로세스
        기동). 예전에는 그 1.3초 내내 `_lock` 을 쥐고 있었고, 채널 락은 읽기
        경로도 함께 쓰므로 **폴러가 도는 동안 로컬 읽기가 통째로 막혔다.**
        읽기에서 원격을 뗀 뒤에도 조회가 1.5초씩 걸린 진짜 이유가 이것이었다 —
        조회 자체는 40ms 인데 폴러 뒤에 줄을 서 있었다.

        그래서 락은 **로컬을 바꾸는 동안만** 쥔다:

        * `has_changes()`(= ls-remote)는 로컬 상태를 건드리지 않는다 → 락 밖.
        * `_fetch()` 는 오브젝트와 원격추적 ref 만 쓴다(작업 사본·인덱스를 만지지
          않는다) → **채널 락 밖**. 대신 `_remote` 로 다른 원격 전이와만 직렬화한다.
        * `_integrate()` 는 작업 사본·브랜치를 바꾼다 → 채널 락 안.
          그 사이에 다른 스레드가 이미 당겼을 수 있으므로 **한 번 더 판정**한다
          (이중 검사). 헛돌아도 정확성에는 영향이 없다.
        """
        self.open()
        if self.has_changes():
            with self._remote:              # 원격 상태 전이는 하나씩
                if self.has_changes():      # 다른 스레드가 먼저 당겼을 수 있다
                    self._fetch()           # 네트워크 — 락 밖
                    with self._lock:
                        self._integrate()   # 로컬 변경 — 락 안
        with self._lock:
            return self._head()

    def _pull(self) -> str | None:
        """원격을 당겨 **로컬 브랜치까지 맞춘 뒤** 원격 ref 를 돌려준다.

        ⚠️ `_fetch()` 만 하고 `_integrate()` 를 빼먹으면 안 된다. `has_changes()` 는
        *원격 SHA* 와 *원격추적 ref* 를 비교하므로, 추적 ref 만 앞서 나가면
        `sync()` 가 영영 "변경 없음"이라고 답하고 로컬 브랜치가 뒤처진 채 고정된다
        (롤업 push 경합에서 실제로 밟았다).

        네트워크(fetch)는 **락 밖**, 로컬을 바꾸는 통합만 락 안이다.
        """
        with self._remote:
            self._fetch(quiet=True)
            with self._lock:
                self._integrate()
                return self._remote_ref()

    def local_head(self) -> str | None:
        """원격을 **보지 않고** 로컬 클론의 HEAD 만 돌려준다 (원격 왕복 0회).

        `sync()` 와 짝이다. 둘의 차이가 곧 읽기 API 의 `fresh=` 가 파는 것이다 —
        아래 「신선도 정책」 참조.
        """
        with self._lock:
            self.open()
            return self._head()

    def _read_head(self, fresh: bool) -> str | None:
        """읽기 API 가 기준으로 삼을 커밋.

        ⭐ **신선도 정책 — 읽기는 원격을 볼 필요가 없다.**

        `fresh=True` (기본, 하위호환) 는 `sync()` 를 탄다: `ls-remote` 로 원격
        SHA 를 물어보고, 다르면 fetch·통합한다. 실측(Windows 11 · git 2.51 ·
        GitHub private repo) `ls-remote` 한 번이 **1.3초**다 — 네트워크 왕복
        자체는 100ms 대이고 나머지는 자격증명 헬퍼 + HTTPS 왕복 3회 + 프로세스
        기동이다. 반면 같은 데이터를 **로컬 클론에서** 읽는 비용은 40~110ms 다.

        `fresh=False` 는 그 왕복을 통째로 없앤다. 정당한 이유는 두 가지다:

        * **레코드는 이미 로컬에 있다.** 채널은 클론이다. 원격에 물어봐야 알 수
          있는 것은 "그 뒤에 새 것이 더 있나" 뿐이고, 그건 이미 **구독(폴러)**
          이 맡고 있다 — `subscribe()` 가 주기마다 `sync()` 를 탄다. 읽기까지
          원격을 보면 같은 일을 두 곳에서 한다.
        * **과거로 거슬러 올라가는 페이징은 원격과 무관하다.** 이미 받은
          커밋 안에서 뒤로 가는 것이므로 원격을 확인할 이유가 아예 없다.

        바뀌는 의미는 하나뿐이다: `fresh=False` 로 읽은 결과는 **마지막 폴 시점**
        기준이다. 그 지연이 곤란한 순간(예: 방을 지금 막 열었다)이 있으면
        소비자가 그때만 `fresh=True` 를 쓰거나, 화면을 막지 않고 별도로
        당기면 된다 (gitwire-chat 이 후자를 택했다).
        """
        return self.sync() if fresh else self.local_head()

    def recover(self, discard_local: bool = False) -> None:
        """히스토리 재작성 후 복구. discard_local=True 면 미푸시 커밋을 버린다."""
        self.open()
        with self._remote:
            self._fetch()                   # 네트워크 — 채널 락 밖
            with self._lock:
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
        """대기 중인 레코드를 **한 커밋**으로 묶어 커밋·push 한다. 커밋된 건수.

        ⭐ **push(네트워크)를 채널 락 안에서 하지 않는다.**

        실측(윈도우 · push 2.7초를 흉내낸 원격 · 6건 연속 전송): 예전에는
        `flush()` 가 push 가 끝날 때까지 `_lock` 을 쥐고 있었고, 그 락은
        `append()` 와 모든 읽기가 함께 쓴다. 그래서 **첫 건만 42ms 였고 2번째부터
        3.3초**였다 — 사용자가 친 다음 메시지가 배경 push 뒤에 줄을 섰다.
        (같은 실수를 `sync()` 에서 한 번 고쳤는데 `flush()` 만 그 규율 밖에
        남아 있었다.)

        지금은 커밋까지만 `_lock` 안에서 하고, push 는 `_remote` 만 쥔 채
        **락 밖**에서 기다린다. 같은 조건에서 전송 응답 중앙값이 3365ms → 49ms 가
        되고, push 중 조회는 3484ms → 46ms 가 된다 (README 「push 도 락 밖으로」).

        push 중에 들어오는 `append()` 는 작업 사본에 **새 파일을 쓸 뿐**
        인덱스·HEAD 를 건드리지 않는다. 그 레코드는 다음 flush 가 가져간다.
        읽기는 커밋 기준이라 영향이 없다. 위험한 것은 *다른 원격 전이*(sync 의
        통합·롤업·compact)가 push 도중 끼어들어 우리가 방금 올린 커밋을 로컬에서
        갈아치우는 경우인데, 그것들이 전부 `_remote` 를 거치므로 겹치지 않는다.
        """
        self.open()                          # ⚠️ 락을 잡기 **전에** (교착 방지)
        with self._remote:
            with self._lock:
                count = len(self._pending)
                self._absorb_worktree()      # 커밋 — 로컬 변경이라 락이 필요하다
                if self._unpushed_count() == 0:
                    return 0
                head = self._head()
            if head is None:
                return 0
            self._push_with_retry(head, push_attempts)
            return count

    def _push(self, sha: str | None = None) -> None:
        """`sha`(기본 HEAD)를 원격 브랜치로 밀어낸다.

        ⭐ **명시적인 sha 를 민다.** 락 밖에서 push 하므로 `HEAD:` 로 밀면 "무엇을
        올렸는지"가 push 시점에야 정해지고, 뒤이어 `_mark_pushed()` 가 *그 사이
        늘어난* 커밋까지 "올렸다"고 표시해 **아직 안 올라간 레코드를 올라간 것으로
        착각**할 수 있다.
        """
        self.git.run("push", "origin", f"{sha or 'HEAD'}:refs/heads/{self.branch}")

    def _push_with_retry(self, head: str, attempts: int) -> None:
        """push 거부(선점) 시 fetch + rebase 후 재시도. **`_remote` 를 쥔 채 부른다.**

        레코드가 서로 다른 파일이므로 rebase 는 내용 충돌 없이 항상 성공한다.
        네트워크(push·fetch)는 채널 락 밖에서, 로컬 변경(통합·ref 기록)만 락 안에서.
        """
        delay = 0.05
        last = max(1, attempts) - 1
        for i in range(max(1, attempts)):
            try:
                self._push(head)             # 네트워크 — 채널 락 밖
            except PushRejected:
                if i == last:
                    raise
                self._fetch()                # 네트워크 — 채널 락 밖
                with self._lock:
                    self._integrate()        # 로컬 변경 — 락 안
                    head = self._head() or head   # rebase 로 sha 가 바뀐다
                time.sleep(delay)
                delay = min(delay * 2, 2.0)
                continue
            with self._lock:
                self._mark_pushed(head)      # 방금 **실제로** 올린 sha 를 기록
            return

    # ----------------------------------------------------------------- 읽기

    def _list_records(self, ref: str) -> list[str]:
        """**살아 있는** 레코드 파일 경로 전량 (아카이브는 포함하지 않는다).

        읽기 API 는 이걸 쓰지 않는다 — `_ids_before()` 가 날짜 디렉토리를
        역순으로 훑어 필요한 만큼만 연다. 아카이브까지 포함한 전량이 필요하면
        `_all_ids()` 를 쓴다 (`compact()` 만 이 라이브 전용 목록을 쓴다 — 아카이브를
        건드리지 않기 위해서다).
        """
        res = self.git.run(
            "ls-tree", "-r", "--name-only", "-z", ref,
            "--", records.RECORD_DIR + "/", check=False,
        )
        if res.returncode != 0:
            return []
        return sorted(p for p in res.stdout.split("\x00") if p.endswith(".json"))

    def _tree_index(self, ref: str) -> tuple[dict[str, str], dict[str, str]]:
        """(날짜 → 라이브 트리 sha, 날짜 → 아카이브 blob sha).

        ⭐ **`ls-tree` 를 한 번만 부른다.** 비재귀 나열에 경로를 두 개 주면
        (`records/` 와 `archive/`) 한 호출로 둘 다 나온다 — 롤업을 붙이면서 쪽당
        git 호출이 늘어나면 keyset 페이징으로 얻은 성질을 그대로 잃는다.
        디렉토리 이름과 그 sha 가 함께 나오므로 sha 를 따로 묻는 왕복도 없다.
        결과는 커밋 sha 로 캐시한다(같은 커밋 = 같은 트리 = 같은 목록).
        """
        key = "days:" + ref
        cached = self._trees.get(key)
        if cached is None:
            res = self.git.run(
                "ls-tree", "-z", ref, "--",
                records.RECORD_DIR + "/", _rollup.ARCHIVE_DIR + "/", check=False,
            )
            rows = []
            if res.returncode == 0:
                for entry in res.stdout.split("\x00"):
                    if not entry:
                        continue
                    meta, _, path = entry.partition("\t")
                    fields = meta.split()
                    if len(fields) < 3 or not path:
                        continue
                    # "<sha> <종류(d=라이브 날짜 트리 / a=아카이브)> <날짜>" 한 줄
                    if fields[1] == "tree" and path.startswith(records.RECORD_DIR + "/"):
                        rows.append(f"{fields[2]} d {path.rsplit('/', 1)[-1]}")
                    elif fields[1] == "blob" and path.startswith(
                        _rollup.ARCHIVE_DIR + "/"
                    ):
                        day = _rollup.day_from_archive(path)
                        if day:
                            rows.append(f"{fields[2]} a {day}")
            cached = self._trees.put(key, sorted(rows, key=lambda r: r.split(" ", 2)[2]))
        live: dict[str, str] = {}
        arch: dict[str, str] = {}
        for row in cached:
            sha, kind, day = row.split(" ", 2)
            (live if kind == "d" else arch)[day] = sha
        return live, arch

    def _day_trees(self, ref: str) -> list[tuple[str, str]]:
        """`records/` 바로 아래 (날짜, 트리 sha) 목록. 오름차순."""
        return sorted(self._tree_index(ref)[0].items())

    def _day_records(self, day: str, tree: str) -> list[str]:
        """날짜 디렉토리 하나의 레코드 경로 (오름차순 = 시간순).

        **트리 sha 로 직접 나열하고 그 sha 로 캐시한다.** 커밋이 아니라 트리를
        키로 쓰는 것이 더 촘촘하다 — 새 레코드가 오늘 날짜에 추가돼도 어제
        날짜의 트리 sha 는 그대로라 캐시가 계속 맞는다. 반대로 그 날짜에 무엇이든
        추가되면 sha 가 달라지므로 **낡은 목록이 나올 수 없다**(무효화 불필요).

        캐시에는 파일명만 담고 `records/<날짜>/` 접두는 쓸 때 붙인다 — 값이
        접두와 무관하므로 키(sha)와 값이 어긋날 여지가 아예 없다.
        """
        key = "tree:" + tree
        names = self._trees.get(key)
        if names is None:
            res = self.git.run("ls-tree", "--name-only", "-z", tree, check=False)
            found = (
                sorted(n for n in res.stdout.split("\x00") if n.endswith(".json"))
                if res.returncode == 0
                else []
            )
            names = self._trees.put(key, found)
        prefix = f"{records.RECORD_DIR}/{day}/"
        return [prefix + n for n in names]

    # ------------------------------------------------- 아카이브 (지난 날 롤업)

    def _archive_index(self, ref: str) -> dict[str, str]:
        """`archive/` 아래 {날짜: blob sha}. `_tree_index` 와 **같은 나열**을 쓴다."""
        return self._tree_index(ref)[1]

    def _blob_bytes(self, sha: str, rel: str) -> bytes:
        """blob 하나의 **검증된** 바이트.

        작업 사본의 `rel` 파일을 먼저 읽고 **sha1 을 직접 계산해** 요청한 blob 과
        같은지 확인한다 (파일 읽기 + sha1 = 0.2ms, git subprocess = 45ms). 다르거나
        없으면 git 오브젝트에서 가져오되 그 바이트도 sha 로 다시 확인한다 —
        낡은 작업 사본을 정본으로 착각하는 조용한 오류를 원천 차단한다.
        (sha 가 40자가 아니면 sha256 레포다 — 검증을 건너뛰고 git 을 정본으로 쓴다.)
        """
        path = self.clone_dir / rel
        try:
            data = path.read_bytes()
        except OSError:
            data = None
        if data is not None and len(sha) == 40 and _rollup.blob_sha1(data) == sha:
            return data
        res = self.git.run("cat-file", "blob", sha, check=False)
        if res.returncode != 0:
            if data is not None:
                return data
            raise GitError(["cat-file", "blob", sha], res.returncode, res.stderr)
        got = res.stdout.encode("utf-8")
        if len(sha) == 40 and _rollup.blob_sha1(got) != sha:
            raise _rollup.ArchiveFormatError(
                f"{rel}: 읽은 바이트가 blob {sha[:8]} 와 일치하지 않는다"
            )
        return got

    def _archive_bytes(self, day: str, sha: str) -> bytes:
        return self._blob_bytes(sha, _rollup.archive_path(day))

    def _day_blobs(self, day: str, tree: str) -> tuple[list[tuple[str, str]], bool]:
        """그 날짜 트리의 (파일명, blob sha) 목록과 **"깨끗한가"** 판정.

        깨끗하다 = 항목이 전부 `.json` **blob** 이다. 하나라도 아니면(하위 디렉토리
        등) 그 날짜는 접지 않는다 — 롤업은 인덱스에서 날짜 디렉토리를 통째로
        제거하므로, 우리가 아카이브에 담지 못한 무언가가 그 안에 있으면 그것이
        사라진다. **접을 수 없는 것은 지우지 않는다.**
        """
        res = self.git.run("ls-tree", "-z", tree, check=False)
        if res.returncode != 0:
            return [], False
        out: list[tuple[str, str]] = []
        clean = True
        for entry in res.stdout.split("\x00"):
            if not entry:
                continue
            meta, _, name = entry.partition("\t")
            fields = meta.split()
            if len(fields) < 3 or fields[1] != "blob" or not name.endswith(".json"):
                clean = False
                continue
            out.append((name, fields[2]))
        return sorted(out), clean

    def _archive_lines(self, day: str, sha: str) -> dict[str, str]:
        return _rollup.index_archive(self._archive_bytes(day, sha))

    def _archive_ids(self, day: str, sha: str) -> list[str]:
        """아카이브에 든 레코드 id 목록 (오름차순). blob sha 로 캐시 → 재조회 0회."""
        key = "aids:" + sha
        ids = self._trees.get(key)
        if ids is None:
            ids = self._trees.put(key, sorted(self._archive_lines(day, sha)))
        return ids

    def _day_index(self, ref: str) -> list[tuple[str, str | None, str | None]]:
        """(날짜, 라이브 트리 sha|None, 아카이브 blob sha|None) 오름차순.

        롤업된 날짜와 아직 살아 있는 날짜를 **하나의 날짜 축**으로 합친다. 소비자는
        어느 쪽인지 알 필요가 없다 (요구: 레거시를 투명하게 가로지른다).
        전환 중에는 한 날짜에 둘 다 있을 수 있다 — 롤업 뒤에 도착한 레코드다.
        """
        live, arch = self._tree_index(ref)
        return [(d, live.get(d), arch.get(d)) for d in sorted(set(live) | set(arch))]

    def _day_ids(self, day: str, tree: str | None, arch: str | None) -> list[str]:
        """그 날짜의 레코드 id 전부 (라이브 + 아카이브 합집합, 오름차순·중복 없음)."""
        if tree and not arch:
            return self._day_records(day, tree)
        if arch and not tree:
            return list(self._archive_ids(day, arch))
        if tree and arch:
            return sorted(set(self._day_records(day, tree)) | set(self._archive_ids(day, arch)))
        return []

    def _all_ids(self, ref: str) -> list[str]:
        """레코드 id **전량** (아카이브 포함). 전량이 정말 필요한 경로 전용."""
        out: list[str] = []
        for day, tree, arch in self._day_index(ref):
            out += self._day_ids(day, tree, arch)
        return sorted(out)

    def cache_info(self) -> dict:
        """나열 캐시 상태 (관측용). 키가 sha 라 무효화 항목은 없다."""
        return self._trees.info()

    def local_ref_cache_info(self) -> dict:
        """로컬 ref 캐시 상태 (관측용) — `localrefs.py`.

        적중·무효화·**포기**(스탬프를 만들 수 없어 매번 git 을 부른 횟수) 횟수를
        드러낸다. 캐시가 조용히 꺼져 있거나 조용히 계속 어긋나는 상태를 밖에서
        볼 수 있어야 한다.
        """
        return self._localrefs.info()

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
        * 나열 결과는 **sha 로 캐시**되므로, 무한 스크롤처럼 같은 상태를 반복
          조회하는 경로에서는 git 호출이 0회가 된다 (treecache.py).
        * **롤업된 날짜도 같은 축 위에 있다** (`_day_index`). 아카이브 파일은 그
          날짜를 실제로 방문할 때만, 그 **하루치 한 파일만** 연다 — 전량 파싱으로
          돌아가지 않는다. id 목록은 blob sha 로 캐시되므로 되풀이 조회는 0회다.
        """
        out: list[str] = []
        for day, tree, arch in reversed(self._day_index(ref)):
            prefix = f"{records.RECORD_DIR}/{day}/"
            if before is not None and prefix > before:
                continue                      # 이 날짜 전체가 커서보다 뒤다 — 열지 않는다
            names = self._day_ids(day, tree, arch)
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
        """base..target 사이에 **새로 생긴** 레코드 id (오름차순).

        ⭐ 롤업이 만드는 함정과 그 해소 — 여기가 유실 금지의 핵심이다.

        롤업 커밋은 하루치 레코드 **파일을 지우고** 아카이브를 하나 추가한다.
        그래서 소비자가 오래 쉬었다가 돌아오면(예: 주말 내내 앱을 꺼 둠) 그 사이에
        *추가됐다가 롤업으로 지워진* 레코드는 `git diff base target` 에 아예
        나타나지 않는다 — 추가와 삭제가 상쇄되기 때문이다. 그대로 두면 **주말
        대화가 조용히 사라진다.**

        그래서 `archive/` 의 변화도 함께 본다: 바뀐 아카이브마다
        *target 의 id 집합* − *base 시점 그 날짜의 id 집합(아카이브 ∪ 라이브)* 을
        더한다. 이것이 정확히 "그 사이에 그 날짜에 새로 생긴 레코드"다.
        워터마크 같은 근사가 아니라 **집합 차이**라, 이미 롤업된 날짜에 늦게
        도착했다가 다음 롤업에 합쳐진 레코드도 정확히 한 번 잡힌다.
        """
        res = self.git.run(
            "diff", "--name-only", "--diff-filter=d", "-z", base, target,
            "--", records.RECORD_DIR + "/", check=False,
        )
        if res.returncode != 0:
            return self._all_ids(target)
        out = {p for p in res.stdout.split("\x00") if p.endswith(".json")}

        adiff = self.git.run(
            "diff", "--name-only", "-z", base, target,
            "--", _rollup.ARCHIVE_DIR + "/", check=False,
        )
        if adiff.returncode != 0:
            return sorted(out)
        days = {
            d
            for d in (
                _rollup.day_from_archive(x)
                for x in adiff.stdout.split("\x00")
                if x
            )
            if d
        }
        if days:
            base_live, base_arch = self._tree_index(base)
            tgt_arch = self._archive_index(target)
            for day in days:
                if day not in tgt_arch:
                    continue
                seen = set(self._day_ids(day, base_live.get(day), base_arch.get(day)))
                out |= set(self._archive_ids(day, tgt_arch[day])) - seen
        return sorted(out)

    def _reachable(self, sha: str | None) -> bool:
        return bool(sha) and self.git.ok("cat-file", "-e", f"{sha}^{{commit}}")

    def _read_record(self, rid: str, ref: str | None = None):
        """레코드 id 하나를 Record 로 해석한다.

        ⭐ **id 는 롤업 전후로 바뀌지 않는다.** 그래서 해결 경로가 세 갈래다:
        살아 있는 파일 → (없으면) 그 날짜의 **아카이브 한 줄** → (없으면) git
        오브젝트. 소비자는 어느 쪽인지 알 필요가 없다.
        """
        path = self.clone_dir / rid
        try:
            data = path.read_bytes()
        except OSError:
            data = None
        if data is None:
            line = self._archived_line(rid, ref)
            if line is not None:
                data = (line + "\n").encode("utf-8")
        if data is None:
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

    def _archived_line(self, rid: str, ref: str | None) -> str | None:
        """아카이브 안에서 이 id 의 줄을 찾는다 (없으면 None).

        하루치 아카이브 **한 파일만** 연다. 연속으로 같은 날짜를 읽는 페이징을
        위해 마지막으로 연 아카이브 하나를 메모해 둔다 (한 페이지 = 보통 하루~이틀).
        """
        day = _rollup.day_of(rid)
        if not day or not ref:
            return None
        try:
            sha = self._archive_index(ref).get(day)
        except GitError:
            return None
        if not sha:
            return None
        if self._archive_memo[0] != sha:
            try:
                self._archive_memo = (sha, self._archive_lines(day, sha))
            except (GitError, _rollup.ArchiveFormatError) as exc:
                log.warning("gitwire: 아카이브 %s 를 읽지 못했다: %s", day, exc)
                return None
        return self._archive_memo[1].get(rid)

    def _plan(self, cur: Cursor, head: str | None) -> tuple[str | None, list[str], str]:
        """(목표 커밋, 아직 전달하지 않은 레코드 경로들, 모드) 를 **결정론적**으로 계산.

        같은 커서 + 같은 레포 상태면 항상 같은 결과가 나온다. 이 성질이
        "프로세스가 중간에 죽어도 정확히 이어받는다"를 보장한다.
        """
        if head is None:
            return None, [], MODE_SCAN
        target = head
        base = cur.commit
        if base == target and cur.batch_pos == 0:
            # ⭐ 유휴 — 기준 커밋이 곧 목표 커밋이다. 그러면 답이 **계산 없이**
            # 정해진다: 도달 가능성(방금 로컬에서 읽은 커밋이다)·조상 관계(자기
            # 자신)·diff(자기와의 차이 = 공집합)가 모두 자명하다. 예전에는 이
            # 자명한 세 가지를 확인하려고 폴링마다 git 을 4개(`cat-file -e`,
            # `merge-base --is-ancestor`, `diff` 2회) 띄웠다.
            #
            # ⚠️ `batch_pos == 0` 조건이 필요하다. 전달 중간에 끊긴 배치가 있으면
            # (batch_pos > 0) 그 배치를 **같은 목표 커밋으로 재계산**해야 하므로
            # 아래 정상 경로를 그대로 타야 한다.
            return target, [], MODE_DIFF
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
        # (아카이브 포함 — 처음 붙는 소비자가 롤업된 과거를 통째로 놓치면 안 된다)
        paths = self._all_ids(target)
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
        head = self.sync()                     # 네트워크는 락 밖에서
        with self._lock:
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
        head = self.sync()                     # 네트워크는 락 밖에서
        with self._lock:
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
        self,
        limit: int | None = None,
        *,
        before: str | None = None,
        fresh: bool = True,
    ) -> list[records.Record]:
        """커서와 무관하게 레코드를 시간순으로 읽는다.

        * `history(limit=N)` — 가장 최근 N건.
        * `history(before=<record_id>, limit=N)` — 그 ID **직전** N건 (역방향 페이징).

        어느 쪽이든 **요청한 N건의 blob 만** 연다. "더 있는가"까지 알아야 하면
        `history_page()` 를 쓴다.

        `fresh=False` 면 원격을 보지 않고 로컬 클론만 읽는다 (`_read_head()` 의
        「신선도 정책」).
        """
        head = self._read_head(fresh)          # 원격 왕복이 있다면 락 밖에서
        with self._lock:
            if head is None:
                return []
            paths = self._ids_before(head, before, limit)
            return [r for r in (self._read_record(p, head) for p in paths) if r]

    def history_page(
        self,
        *,
        before: str | None = None,
        limit: int = DEFAULT_PAGE,
        fresh: bool = True,
    ) -> HistoryPage:
        """⭐ 역방향 페이징 한 쪽 — `HistoryPage(records, has_more)`.

        `before=None` 이면 최신 쪽 한 쪽. 다음 쪽은 `before=page.oldest` 로 잇는다.
        `has_more` 는 **한 건을 더 나열해 보고**(blob 은 열지 않는다) 판정한다 —
        소비자가 빈 페이지를 받아보고서야 끝을 아는 일이 없게.

        `fresh=False` 면 원격을 보지 않고 로컬 클론만 읽는다 (`_read_head()` 의
        「신선도 정책」). 페이지 경계·중복/누락 없음은 그대로다 — keyset 커서는
        기준 커밋이 무엇이든 성립한다.
        """
        n = max(1, int(limit))
        head = self._read_head(fresh)          # 원격 왕복이 있다면 락 밖에서
        with self._lock:
            if head is None:
                return HistoryPage([], False)
            ids = self._ids_before(head, before, n + 1)
            has_more = len(ids) > n
            if has_more:
                ids = ids[-n:]
            recs = [r for r in (self._read_record(p, head) for p in ids) if r]
            return HistoryPage(recs, has_more)

    def record_ids(
        self,
        *,
        before: str | None = None,
        limit: int | None = None,
        fresh: bool = True,
    ) -> list[str]:
        """레코드 **ID 만** 시간순으로 나열한다 (payload blob 을 열지 않는다).

        `_list_records()` 를 공개하는 대신 이 형태로 연다: 소비자가 실제로 원하는
        것은 "전량 나열"이 아니라 **커서 기준 열거**이고, 전량 나열을 공개하면
        비싼 경로가 기본이 된다. 인덱스·동기화 상태 확인처럼 payload 가 필요 없는
        용도를 위한 API 다.

        `fresh=False` 면 원격을 보지 않고 로컬 클론만 읽는다 (`_read_head()` 의
        「신선도 정책」).
        """
        head = self._read_head(fresh)          # 원격 왕복이 있다면 락 밖에서
        with self._lock:
            if head is None:
                return []
            return self._ids_before(head, before, limit)

    def skip_to_now(self) -> None:
        """지금까지의 레코드를 '이미 처리됨'으로 표시한다 (백로그 건너뛰기)."""
        head = self.sync()                     # 네트워크는 락 밖에서
        with self._lock:
            cur = self.cursors.load()
            # 워터마크는 "가장 큰 id" 하나면 된다 → 전량 나열 대신 마지막 한 건만
            # 집는다 (아카이브 여부와 무관하게 성립한다).
            paths = self._ids_before(head, None, 1) if head else []
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
        head = self.sync()                     # 네트워크는 락 밖에서 —
        with self._lock:                       # 폴링이 읽기를 막지 않는다
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
        # 락을 놓은 뒤에 본다 — 후보가 있으면 배경 스레드로 나간다 (여기서 기다리지
        # 않는다). 폴링 경로에 다는 이유: 상시 소비자가 도는 동안에만 접으면 되고,
        # 일회성 CLI 호출이 롤업 때문에 느려지면 안 되기 때문이다.
        self.maybe_rollup()
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

    # --------------------------------------------------- 지난 날짜 롤업 (비파괴)

    def _git_index(self, index_path: Path) -> Git:
        """**임시 인덱스**로 동작하는 git 핸들.

        롤업은 작업 사본과 진짜 인덱스를 **한 바이트도 건드리지 않는다.** 그래야
        롤업이 도는 동안 발행·읽기가 그대로 흐르고, 중간에 죽어도 남는 것이 없다.
        """
        env = dict(self.credential.env(self.dir))
        env["GIT_INDEX_FILE"] = str(index_path)
        return Git(
            self._runner, self.clone_dir, env=env, secrets=self.credential.secrets()
        )

    def _rollup_candidates(self, ref: str, now) -> list[str]:
        """접을 만한 지난 날짜 (이름만 — 캐시된 나열 1회, 레코드는 열지 않는다).

        `now` 를 **인자로 받는다**: `HttpDateClock.now()` 는 주기적으로 HTTP 왕복을
        하므로 채널 락 안에서 부르면 안 된다 (그 순간 읽기·쓰기가 통째로 막힌다).
        """
        try:
            live, _ = self._tree_index(ref)
        except GitError:
            return []
        return _rollup.closed_days(live, now, self.rollup_grace_hours)

    def _rollup_plan(
        self,
        base: str,
        *,
        grace_hours: float,
        min_records: int,
        days: Sequence[str] | None = None,
        force: bool = False,
    ) -> tuple[list[tuple[str, bytes, list[str]]], dict[str, str]]:
        """`base` 커밋에서 접을 날짜와 **그 날짜의 아카이브 바이트**를 계산한다.

        ⭐ 이 함수는 `base` 의 **순수 함수**다. 같은 커밋을 보는 두 참가자는 항상
        같은 바이트를 얻는다 (`rollup.build_archive`). 그것이 조정 없이 동시 롤업을
        성립시키는 유일한 장치다.
        """
        live, arch = self._tree_index(base)
        now = self.clock.now()
        wanted = set(days) if days else None
        plan: list[tuple[str, bytes, list[str]]] = []
        skipped: dict[str, str] = {}
        for day in sorted(live):
            if wanted is not None and day not in wanted:
                continue
            if not force and not _rollup.is_closed(day, now, grace_hours):
                if wanted is not None:
                    skipped[day] = "아직 지난 날이 아니다"
                continue
            entries, clean = self._day_blobs(day, live[day])
            if not entries:
                continue
            if not clean:
                skipped[day] = "레코드가 아닌 항목이 섞여 있다 — 접지 않는다"
                log.warning("gitwire: 롤업 건너뜀 %s — %s", day, skipped[day])
                continue
            if day not in arch and len(entries) < min_records:
                skipped[day] = f"레코드 {len(entries)}건 (최소 {min_records}건)"
                continue
            lines: dict[str, str] = {}
            try:
                if day in arch:
                    # 이미 아카이브가 있다 = 이 날짜에 **뒤늦게 도착한 레코드**다.
                    # 기존 줄과 합집합을 만든다 (한 건도 버리지 않는다).
                    lines.update(self._archive_lines(day, arch[day]))
                for name, sha in entries:
                    rid = f"{records.RECORD_DIR}/{day}/{name}"
                    lines[rid] = _rollup.canonical_line(self._blob_bytes(sha, rid))
            except (GitError, _rollup.ArchiveFormatError) as exc:
                skipped[day] = f"접을 수 없다: {exc}"
                log.warning("gitwire: 롤업 건너뜀 %s — %s", day, exc)
                continue
            paths = [f"{records.RECORD_DIR}/{day}/{name}" for name, _ in entries]
            plan.append((day, _rollup.build_archive(lines), paths))
        return plan, skipped

    def _rollup_commit(self, base: str, plan) -> str:
        """계획을 **커밋 하나**로 만든다 (아직 push 하지 않는다).

        임시 인덱스 + `commit-tree` 만 쓴다 → 작업 사본·진짜 인덱스·현재 브랜치가
        전혀 바뀌지 않는다. 만들어진 커밋은 `base` 를 부모로 갖고, 그 트리는
        `base` 의 순수 함수다.
        """
        tmp = self.dir / "tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        uniq = f"{os.getpid()}-{threading.get_ident()}"
        index = tmp / f"rollup-{uniq}.index"
        scratch: list[Path] = []
        try:
            if index.exists():
                index.unlink()
            g = self._git_index(index)
            g.run("read-tree", base)
            total = 0
            for day, content, paths in plan:
                f = tmp / f"rollup-{uniq}-{day}.jsonl"
                f.write_bytes(content)
                scratch.append(f)
                sha = g.out("hash-object", "-w", "--no-filters", "--", str(f))
                g.run(
                    "rm", "--cached", "-r", "-f", "-q",
                    "--", f"{records.RECORD_DIR}/{day}",
                )
                g.run(
                    "update-index", "--add", "--cacheinfo",
                    f"100644,{sha},{_rollup.archive_path(day)}",
                )
                total += len(paths)
            tree = g.out("write-tree")
            msg = (
                f"gitwire: 지난 날짜 롤업 — {len(plan)}일 / 레코드 {total}건 "
                f"→ {_rollup.ARCHIVE_DIR}/"
            )
            return g.out("commit-tree", tree, "-p", base, "-m", msg)
        finally:
            for f in scratch:
                try:
                    f.unlink()
                except OSError:
                    pass
            try:
                index.unlink()
            except OSError:
                pass

    def rollup(
        self,
        *,
        grace_hours: float | None = None,
        min_records: int | None = None,
        days: Sequence[str] | None = None,
        force: bool = False,
        attempts: int = DEFAULT_ROLLUP_ATTEMPTS,
    ) -> dict:
        """⭐ 지난 날짜 롤업 — 하루치 레코드를 아카이브 파일 1개로 접는다 (비파괴).

        `compact()` 와 **다른 물건이다**: force-push 도 히스토리 재작성도 없다.
        평범한 커밋 하나로 `archive/<날짜>.jsonl` 을 추가하고 그 날짜의 레코드
        파일을 지운다. 레코드는 **한 건도 버리지 않는다** — 옮길 뿐이고, id 는
        그대로다. 형식·근거는 `rollup.py`.

        ⭐ **원본 삭제와 아카이브 추가가 같은 커밋 안에 있다.** 그래서 "지웠는데
        아카이브가 없는" 중간 상태가 존재할 수 없고, push 되기 전에는 로컬에서도
        아무것도 지워지지 않는다 (중간에 죽어도 잃는 것이 없다).

        ⭐ **동시 롤업은 조정하지 않는다.** 커밋의 트리는 부모 커밋의 순수 함수이고
        push 는 그 부모를 여전히 가리킬 때만 통과한다(fast-forward). 즉 *우리가
        보지 못한 레코드가 원격에 있으면 push 자체가 거부된다* — 유실이 구조적으로
        불가능하다. 거부되면 새 원격 상태에서 **처음부터 다시 계산**한다. 같은
        집합을 봤다면 상대의 결과가 이미 내 결과와 같으므로 할 일이 없어지고,
        다른 집합을 봤다면 이번엔 합집합으로 다시 만든다. 락도 리더 선출도 없다.
        """
        self.open()
        self.flush()                 # 내 미푸시 레코드를 먼저 올린다 (이번 롤업에 포함되게)
        grace = self.rollup_grace_hours if grace_hours is None else float(grace_hours)
        minr = (
            self.rollup_min_records if min_records is None else max(1, int(min_records))
        )
        skipped: dict[str, str] = {}
        tries = max(1, int(attempts))
        for attempt in range(1, tries + 1):
            # 네트워크는 락 밖, 통합만 락 안 (`_pull`). fetch 실패는 조용히
            # 넘어간다 — 오래된 base 는 아래 push 거부로 반드시 드러난다.
            base = self._pull()
            if base is None:
                return {"rolled": False, "reason": "원격 브랜치가 없다", "days": []}
            plan, skipped = self._rollup_plan(
                base, grace_hours=grace, min_records=minr, days=days, force=force
            )
            if not plan:
                return {
                    "rolled": False, "reason": "접을 지난 날짜가 없다",
                    "days": [], "skipped": skipped, "attempts": attempt,
                }
            commit = self._rollup_commit(base, plan)
            # push 와 뒤이은 로컬 통합은 하나의 원격 전이다 → `_remote` 안에서
            # (채널 락은 여전히 잡지 않는다 — 발행·읽기가 계속 흐른다).
            with self._remote:
                try:
                    self.git.run("push", "origin", f"{commit}:refs/heads/{self.branch}")
                except PushRejected:
                    log.info(
                        "gitwire: 롤업 push 경합 (%d/%d) — 새 원격 상태에서 다시 계산한다",
                        attempt, tries,
                    )
                    continue
                # 원격이 확정됐다. 로컬을 따라오게 한다 (통합만 채널 락 안).
                self._pull()
            return {
                "rolled": True, "commit": commit, "base": base,
                "days": [d for d, _, _ in plan],
                "records": sum(len(x[2]) for x in plan),
                "skipped": skipped, "attempts": attempt,
            }
        return {
            "rolled": False, "reason": "push 경합이 반복돼 이번엔 접지 않았다",
            "days": [], "skipped": skipped, "attempts": tries,
        }

    def maybe_rollup(self) -> bool:
        """조건이 맞으면 롤업을 **배경 스레드로** 띄운다. 띄웠으면 True.

        ⭐ 사용자가 특히 강조한 지점 — *"이 작업한다고 다른 동작을 못하면 안 된다."*
        그래서 이 함수 자체가 하는 일은 **캐시된 나열 1회**뿐이고(레코드 blob 을
        열지 않는다), 실제 작업은 전부 별도 스레드로 나간다. 그 스레드도 채널 락을
        길게 쥐지 않는다 (`rollup()` 참조 — 네트워크는 전부 락 밖).

        **빈도의 근거**: 접을 수 있는 날짜가 늘어나는 사건은 하루에 한 번(UTC 자정
        + 유예)뿐이다. 그보다 자주 볼 이유가 없고, 그보다 드물게 보면 접히는 시점이
        늦어진다. 기본 1시간은 "하루 한 번 생기는 사건을 1시간 안에 알아챈다"는
        뜻이고, 확인 비용이 사실상 0(캐시 적중 시 git 호출 0회)이라 더 촘촘히 볼
        이유도 없다. 후보가 없으면 스레드조차 만들지 않는다.
        """
        if not self.auto_rollup:
            return False
        now = time.monotonic()
        wall = self.clock.now()          # ⚠️ 락 **밖**에서 (네트워크 왕복일 수 있다)
        with self._lock:
            running = self._rollup_thread
            if running is not None and running.is_alive():
                return False
            if (
                self._rollup_checked is not None
                and now - self._rollup_checked < self.rollup_interval
            ):
                return False
            self._rollup_checked = now
            head = self._head() if self._opened else None
            if head is None or not self._rollup_candidates(head, wall):
                return False
            th = threading.Thread(
                target=self._rollup_bg, name="gitwire-rollup", daemon=True
            )
            self._rollup_thread = th
        th.start()
        return True

    def _rollup_bg(self) -> None:
        """배경 롤업 1회. 실패해도 앱을 죽이지 않되 **조용히 넘어가지도 않는다**."""
        try:
            res = self.rollup()
            self.rollup_last_error = None
            if res.get("rolled"):
                log.info(
                    "gitwire: 지난 날짜 롤업 완료 — %s (레코드 %d건)",
                    ", ".join(res["days"]), res["records"],
                )
            elif res.get("skipped"):
                log.info("gitwire: 롤업 건너뛴 날짜 %s", res["skipped"])
        except BaseException as exc:  # noqa: BLE001
            self.rollup_last_error = f"{type(exc).__name__}: {exc}"
            log.exception("gitwire: 지난 날짜 롤업 실패 — 다음 기회에 다시 시도한다")

    # ------------------------------------------------------------- 히스토리

    def compact(self, *, keep_records: int | None = None, confirm: bool = False) -> dict:
        """히스토리 압축 — 최근 N건만 남긴 **단일 커밋**으로 재작성 후 force-push.

        ⚠️⚠️ **`rollup()` 과 혼동하지 마라.** 이름이 비슷하지만 정반대다:
        `rollup()` 은 비파괴(평범한 커밋·레코드 보존)이고, 이쪽은 히스토리를
        재작성해 오래된 레코드를 **버린다**. 표는 `rollup.py` 상단에 있다.
        본 함수는 `archive/` 를 건드리지 않는다 — 아카이브된 레코드는 그대로
        보존되고, `keep_records` 는 **살아 있는** 레코드에만 적용된다.

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
        self.open()                          # ⚠️ 락을 잡기 **전에** (교착 방지)
        # 히스토리 재작성은 가장 큰 원격 전이다 → `_remote` 를 먼저(순서 규약),
        # 그 안에서 채널 락. 안쪽의 flush()·sync() 는 RLock 이라 재진입한다.
        with self._remote, self._lock:
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
                "archives_kept": len(self._archive_index("HEAD")),
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
                "auto_rollup": self.auto_rollup,
                "archives": len(self._archive_index(self._head()))
                if self._head()
                else 0,
                "rollup_error": self.rollup_last_error,
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
            th = self._rollup_thread
            if th is not None and th.is_alive():
                th.join(timeout=10.0)

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
