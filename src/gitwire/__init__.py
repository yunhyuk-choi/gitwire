"""gitwire — 중앙 서버 없이 git 레포를 전송 계층으로 쓰는 파이썬 라이브러리.

각 참가자가 로컬에서 프로세스를 띄우고, 공유 git 레포(private)를 통해
**append-only 레코드**를 주고받는다. 서버 운영 0, egress(pull/push)만 사용하므로
인바운드가 차단된 환경에서도 동작한다.

⚠️ 이것은 **기반 프레임워크**다. 레코드는 불투명한 JSON 이며, gitwire 는
payload 안을 절대 해석하지 않는다. 스키마는 소비자(채팅·칸반 등)가 정한다.

빠른 사용
--------
    import gitwire

    ch = gitwire.open_channel("https://github.com/me/my-room.git",
                              credential=gitwire.TokenCredential.from_env())

    # 발행
    ch.append({"kind": "msg", "body": "안녕"})

    # 일회성 조회 (한 번 실행하고 끝나는 소비자 — 에이전트 등)
    for rec in ch.fetch_new():
        print(rec.id, rec.payload)

    # 역방향 페이징 (과거로 거슬러 올라가는 소비자 — 무한 스크롤 등)
    # fresh=False = 원격을 보지 않고 로컬 클론만 읽는다 (신선도는 구독이 맡는다)
    page = ch.history_page(limit=50, fresh=False)
    while page.has_more:
        page = ch.history_page(before=page.oldest, limit=50, fresh=False)

    # 상시 구독 (루프를 도는 소비자 — 웹앱 등)
    sub = ch.subscribe(lambda rec: print(rec.payload))

    # 참가자별 **가변** 상태 (레코드가 아니다 — `state.py` 참조)
    ch.write_state("me@example.com", {"cursor": "records/2026.../….json"})
    states = ch.read_states()        # {키: ParticipantState}
"""

from .channel import (
    DEFAULT_BATCH_WINDOW,
    DEFAULT_BRANCH,
    DEFAULT_CREDENTIAL_CACHE_TIMEOUT,
    DEFAULT_PAGE,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_ROLLUP_INTERVAL,
    Channel,
    HistoryPage,
    Subscription,
    credential_cache,
    open_channel,
)
from .clock import FixedOffsetClock, HttpDateClock, SystemClock
from .credentials import Credential, NoCredential, TokenCredential
from .cursor import Cursor, CursorStore
from .errors import (
    AuthError,
    ChannelInitError,
    ClockError,
    GitError,
    GitwireError,
    HistoryRewritten,
    PushRejected,
)
from .gitcmd import GitRunner, SubprocessGitRunner
from .hub import Hub
from .identity import git_email, installation_id
from .layout import channel_dir, gitwire_home, normalize_repo_url
from .records import Record
from .state import (
    STATE_DIR,
    STATE_VERSION,
    ParticipantState,
    StateDecodeError,
    state_key,
    state_path,
)
from .rollup import (
    ARCHIVE_DIR,
    DEFAULT_GRACE_HOURS,
    DEFAULT_MIN_RECORDS,
    ArchiveFormatError,
    archive_path,
)

__version__ = "0.2.0"

__all__ = [
    "__version__",
    # 채널
    "Channel",
    "Subscription",
    "open_channel",
    "Hub",
    "Record",
    "HistoryPage",
    # 자격증명
    "Credential",
    "NoCredential",
    "TokenCredential",
    "credential_cache",
    # 시계
    "SystemClock",
    "HttpDateClock",
    "FixedOffsetClock",
    # 주입점
    "GitRunner",
    "SubprocessGitRunner",
    # 커서
    "Cursor",
    "CursorStore",
    # 설치본 신원 (sender 기본값)
    "installation_id",
    # git 이 아는 사람 신원 (설치본이 아니라 **사람** 단위 키가 필요한 소비자용)
    "git_email",
    # 참가자별 가변 상태 (예약 경로 — 레코드가 아니다)
    "ParticipantState",
    "StateDecodeError",
    "STATE_DIR",
    "STATE_VERSION",
    "state_key",
    "state_path",
    # 위치 규약
    "gitwire_home",
    "channel_dir",
    "normalize_repo_url",
    # 예외
    "GitwireError",
    "GitError",
    "AuthError",
    "PushRejected",
    "ChannelInitError",
    "HistoryRewritten",
    "ClockError",
    "ArchiveFormatError",
    # 지난 날짜 롤업 (비파괴 — compact 와 다르다)
    "ARCHIVE_DIR",
    "archive_path",
    "DEFAULT_GRACE_HOURS",
    "DEFAULT_MIN_RECORDS",
    "DEFAULT_ROLLUP_INTERVAL",
    # 기본값
    "DEFAULT_BRANCH",
    "DEFAULT_POLL_INTERVAL",
    "DEFAULT_BATCH_WINDOW",
    "DEFAULT_PAGE",
    "DEFAULT_CREDENTIAL_CACHE_TIMEOUT",
]
