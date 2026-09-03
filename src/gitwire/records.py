"""레코드 = 불투명한 JSON 1건 = 파일 1개.

⚠️ 설계 경계 (가장 중요)
------------------------
기반 계층은 **payload 안을 절대 들여다보지 않는다.** `author`·`text` 같은
필드를 기반이 알면 그 순간 전송 계층이 아니라 채팅 라이브러리가 된다.
스키마는 소비자가 정한다.

봉투(envelope)에만 전송 계층 메타데이터가 있다:

    {"gitwire": 1, "id": "...", "sender": "...", "ts": "...", "payload": {...}}

`sender` 는 "누가 말했나"가 아니라 **어느 참가자 프로세스가 발행했나**라는
전송 수준 식별자다(IP 주소에 가깝다). 파일명 충돌 회피와 순서 안정화에 쓴다.
소비자가 표시용 신원을 쓰고 싶다면 payload 안에 자기 스키마로 담는다.

파일 배치
--------
    records/<YYYYMMDD>/<YYYYMMDD>T<HHMMSS><mmm>Z-<sender>-<rand6>.json

* 레코드마다 **다른 파일** → 동시 발행해도 내용 머지 충돌이 원천적으로 없다.
  (공유 로그 파일 하나를 편집하는 방식은 금지다.)
* 날짜 디렉토리로 샤딩 → 한 디렉토리에 파일이 무한히 쌓이지 않는다.
* 고정폭 타임스탬프가 앞에 오므로 **전체 경로의 사전식 정렬 = 시간순 정렬**.
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

_SENDER_SAFE = re.compile(r"[^A-Za-z0-9_.]+")
_TS_RE = re.compile(r"^(\d{8}T\d{9}Z)-")


class RecordDecodeError(ValueError):
    """레코드 파일을 봉투로 해석할 수 없다."""


def slug_sender(sender: str, max_len: int = 24) -> str:
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
