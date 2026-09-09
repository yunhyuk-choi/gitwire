"""레코드 = 불투명한 JSON 1건 = 파일 1개.

⚠️ 설계 경계 (가장 중요)
------------------------
기반 계층은 **payload 안을 절대 들여다보지 않는다.** `author`·`text` 같은
필드를 기반이 알면 그 순간 전송 계층이 아니라 채팅 라이브러리가 된다.
스키마는 소비자가 정한다.

봉투(envelope)에만 전송 계층 메타데이터가 있다:

    {"gitwire": 1, "id": "...", "sender": "...", "ts": "...", "payload": {...}}

`sender` 는 "누가 말했나"가 아니라 **어느 설치본이 발행했나**라는 전송 수준
식별자다(IP 주소에 가깝다). 파일명 충돌 회피와 순서 안정화, 그리고 "이 레코드가
내가 낸 것인가" 판정에 쓴다. 기본값을 만드는 규칙은 `identity.py` 에 있다.
소비자가 표시용 신원을 쓰고 싶다면 payload 안에 자기 스키마로 담는다.

파일 배치
--------
    records/<YYYYMMDD>/<YYYYMMDD>T<HHMMSS><mmm>Z-<sender>-<rand6>.json

* 레코드마다 **다른 파일** → 동시 발행해도 내용 머지 충돌이 원천적으로 없다.
  (공유 로그 파일 하나를 편집하는 방식은 금지다.)
* 날짜 디렉토리로 샤딩 → 한 디렉토리에 파일이 무한히 쌓이지 않는다.
* 고정폭 타임스탬프가 앞에 오므로 **전체 경로의 사전식 정렬 = 시간순 정렬**.

⭐ **레코드 id = 이 상대 경로**이고, 그것이 정렬 키·커서·답장 대상이다. 지난
날짜를 하루 1파일로 접는 롤업(`rollup.py`)이 있어도 **id 는 절대 바뀌지 않는다** —
저장 위치만 `archive/<날짜>.jsonl` 안의 한 줄로 옮기고, 봉투의 `"id"` 는 원본
그대로 남는다. 그래서 이 파일이 실제로 없을 수도 있다는 점만 기억하면 된다
(해결은 `Channel._read_record()` 가 한다).
"""

from __future__ import annotations

import json
import re
import secrets as _secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

#: 채널 레포 안의 레코드 루트
RECORD_DIR = "records"
#: 봉투 포맷 버전
ENVELOPE_VERSION = 1

#: 발신자 슬러그 상한. 설치본 식별자가 `<git 이메일>.<난수6>` 이므로 예전
#: 상한(24자)으로는 이메일이 잘리며 **난수 접미까지 함께 잘려** 설치본이 구별되지
#: 않을 수 있다. 파일명 길이는 여전히 넉넉하다(경로 총 100자 안팎).
MAX_SENDER_LEN = 40

#: 파일명에 허용하는 문자. '@' 와 '+' 는 이메일에 쓰이고 모든 대상 OS 에서
#: 파일명으로 안전하다. '-' 는 파일명 구분자라 계속 제외한다.
_SENDER_SAFE = re.compile(r"[^A-Za-z0-9_.@+]+")
_TS_RE = re.compile(r"^(\d{8}T\d{9}Z)-")

#: 레코드 id 의 형태. **id 는 곧 파일 경로**이므로(위 「파일 배치」) 이 하나가
#: 형식의 정본이다. 롤업으로 저장 위치가 `archive/` 안의 한 줄로 옮겨져도 id 는
#: 바뀌지 않으므로, 아카이브된 레코드도 이 형태를 만족한다.
#: 난수 접미는 호출자가 넘길 수도 있으므로(`make_record_id(nonce=...)`) 길이를
#: 못 박지 않는다 — 형태 판정의 힘은 앞의 **고정폭 타임스탬프**에서 나온다.
_RECORD_ID_RE = re.compile(
    r"^" + RECORD_DIR + r"/(\d{8})/(\d{8}T\d{9}Z)-[A-Za-z0-9_.@+]+-[A-Za-z0-9]+\.json$"
)


class RecordDecodeError(ValueError):
    """레코드 파일을 봉투로 해석할 수 없다."""


def slug_sender(sender: str, max_len: int = MAX_SENDER_LEN) -> str:
    """발신자 식별자를 파일명에 안전한 형태로 만든다.

    '-' 는 파일명 구분자라 제거한다. 빈 값이면 'anon'.
    """
    s = _SENDER_SAFE.sub("_", (sender or "").strip())
    s = s.strip("_")[:max_len]
    return s or "anon"


def format_ts(dt: datetime) -> str:
    """YYYYMMDDTHHMMSSmmmZ (밀리초, 고정폭 17자)."""
    dt = dt.astimezone(timezone.utc)
    return f"{dt:%Y%m%dT%H%M%S}{dt.microsecond // 1000:03d}Z"


def parse_ts(token: str) -> datetime:
    base = datetime.strptime(token[:15], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    return base.replace(microsecond=int(token[15:18]) * 1000)


def make_record_id(
    ts: datetime, sender: str, nonce: str | None = None
) -> str:
    """레코드 ID = 채널 레포 기준 상대 경로. 이것이 곧 정렬 키다."""
    stamp = format_ts(ts)
    day = stamp[:8]
    rand = nonce or _secrets.token_hex(3)
    return f"{RECORD_DIR}/{day}/{stamp}-{slug_sender(sender)}-{rand}.json"


def is_record_id(value: Any) -> bool:
    """이 문자열이 **이 채널이 실제로 발행할 수 있는** 레코드 id 인가.

    ⭐ 왜 기반이 이것을 제공하는가 — id 형식의 주인이 여기이기 때문이다. 소비자는
    이 id 를 **커서**로 쓴다(`cursor.Cursor.watermark`, 참가자 상태의 읽음 커서).
    커서에 id 가 아닌 값이 들어가면 사전식 비교가 무의미해지고, 그 비교로 만드는
    모든 파생값(안 읽은 개수·구분선·"이미 준 것" 판정)이 조용히 0/전부로 무너진다.
    실제로 그렇게 무너졌다 — 소비자가 화면의 낙관적 임시 ID(`~pending/…`)를 커서로
    저장해 원격까지 올렸고, `~` 가 `records/` 보다 사전식으로 뒤라 **한 번 오염되면
    실제 id 로 되돌아갈 수도 없었다**(단조 증가가 그 값을 최대값으로 굳힌다).

    그래서 판정을 소비자마다 다시 짜지 않게 **형식의 주인이 내준다.** 값을 고치지
    않고 참/거짓만 돌려준다 — 무엇을 할지(거부·무시)는 소비자가 정한다.
    """
    if not isinstance(value, str):
        return False
    m = _RECORD_ID_RE.match(value)
    # 날짜 디렉토리는 타임스탬프에서 **파생된** 값이다 (`make_record_id`). 둘이
    # 어긋난 경로는 이 채널이 만든 것이 아니다.
    return m is not None and m.group(1) == m.group(2)[:8]


@dataclass(frozen=True)
class Record:
    """소비자에게 전달되는 레코드 한 건."""

    id: str
    """채널 안에서 유일하고 정렬 가능한 식별자 (= 레포 상대 경로)."""

    sender: str
    """발행한 참가자 식별자 (전송 수준 메타데이터)."""

    timestamp: datetime
    """발행 시각 (공통 시계 기준, UTC)."""

    payload: Any
    """소비자 스키마의 불투명 JSON. 기반은 내용을 해석하지 않는다."""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "sender": self.sender,
            "ts": self.timestamp.isoformat().replace("+00:00", "Z"),
            "payload": self.payload,
        }


def encode(record_id: str, sender: str, ts: datetime, payload: Any) -> bytes:
    """봉투를 UTF-8(BOM 없음)·LF 바이트로 직렬화한다."""
    envelope = {
        "gitwire": ENVELOPE_VERSION,
        "id": record_id,
        "sender": sender,
        "ts": ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }
    text = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), indent=None)
    return (text + "\n").encode("utf-8")


def decode(data: bytes, record_id: str) -> Record:
    """레코드 파일 바이트를 Record 로 해석한다."""
    try:
        env = json.loads(data.decode("utf-8-sig"))
    except Exception as exc:
        raise RecordDecodeError(f"{record_id}: JSON 파싱 실패: {exc}") from exc
    if not isinstance(env, dict) or "payload" not in env:
        raise RecordDecodeError(f"{record_id}: gitwire 봉투가 아니다")
    ts_raw = env.get("ts")
    ts = None
    if isinstance(ts_raw, str):
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except ValueError:
            ts = None
    if ts is None:
        m = _TS_RE.search(record_id.rsplit("/", 1)[-1])
        ts = parse_ts(m.group(1)) if m else datetime.fromtimestamp(0, timezone.utc)
    return Record(
        id=env.get("id") or record_id,
        sender=str(env.get("sender", "")),
        timestamp=ts.astimezone(timezone.utc),
        payload=env["payload"],
    )
