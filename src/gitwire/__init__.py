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

    # 상시 구독 (루프를 도는 소비자 — 웹앱 등)
    sub = ch.subscribe(lambda rec: print(rec.payload))
"""

from .channel import (
    DEFAULT_BATCH_WINDOW,
    DEFAULT_BRANCH,
    DEFAULT_POLL_INTERVAL,
    Channel,
    Subscription,
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
from .layout import channel_dir, gitwire_home, normalize_repo_url
from .records import Record

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # 채널
    "Channel",
    "Subscription",
    "open_channel",
    "Hub",
    "Record",
    # 자격증명
    "Credential",
    "NoCredential",
    "TokenCredential",
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
    # 기본값
    "DEFAULT_BRANCH",
    "DEFAULT_POLL_INTERVAL",
    "DEFAULT_BATCH_WINDOW",
]
