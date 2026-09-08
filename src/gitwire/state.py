"""참가자별 **가변** 상태 — 레코드가 아닌 예약 경로.

⚠️ 이것은 레코드가 아니다 (헷갈리면 규약이 무너진다)
----------------------------------------------------
| | `records/` (+ `archive/`) | **`participants/` (본 모듈)** |
|---|---|---|
| 의미 | 일어난 사건 | 참가자의 **지금 값** |
| 수정 | 절대 안 한다 (append-only) | **덮어쓴다** (그게 요점이다) |
| 개수 | 사건 수만큼 무한히 늘어난다 | **참가자 수만큼** (상한이 사람 수다) |
| 읽는 법 | 커서·페이징으로 열거 | 파일 하나 = 값 하나, 그대로 읽는다 |
| 롤업·압축 | 접거나 버린다 | **건드리지 않는다** |

왜 레코드로 하지 않나
---------------------
append-only 매체에 "지금 값"을 담으면, 그 값을 알기 위해 **과거로 스캔**해야
한다. 마지막 갱신이 2주 전이면 2주치를 훑는다 — keyset 페이징으로 없앤 전량
스캔이 그대로 되살아난다. "지금 값"은 사건이 아니므로 사건으로 적지 않는다.

⭐ 안전성의 근거 — **경로마다 쓰는 사람이 한 명**
-----------------------------------------------
파일 하나 = 참가자 한 명이고, **그 파일을 쓰는 것은 그 참가자뿐이다.**

    participants/<키>.json      ← 이 파일의 쓰기자는 <키> 본인 하나

그래서 서로 다른 참가자의 동시 갱신이 **다른 경로**를 만지고, 동시 push 는
기존 fetch+rebase 재시도 경로가 그대로 처리한다 (내용 충돌이 날 파일이 없다).
공유 파일 하나에 전원의 값을 담으면 정반대가 된다 — 두 사람이 같이 읽으면 같은
경로를 같이 써서 매번 충돌한다.

⚠️ **이 성질은 소비자가 지켜야 한다.** 기반은 키를 검사하지 않는다(누구의 키인지
알 방법이 없다 — 신원은 소비자의 개념이다). 남의 키에 쓰면 그 순간 이 API 의
안전성 근거가 사라진다. 한 사람이 기기를 여럿 쓰는 경우는 예외적으로 같은 경로를
공유할 수 있고, 그때는 *같은 사람*이 쓰는 것이므로 값의 병합 규칙(예: 단조 증가
값이면 `max`)을 소비자가 정한다. 그 경합에서 무엇이 이기는지는
`Channel._integrate()` 의 rebase 규약에 적혀 있다.

값은 불투명하다
--------------
봉투에만 전송 계층 메타데이터가 있고 `value` 안은 **소비자 스키마**다 —
레코드와 같은 경계다. 기반은 `value` 를 해석하지 않는다.

    {"gitwire_state": 1, "key": "...", "identity": "...",
     "updated_at": "2026-09-08T…Z", "value": { 소비자 마음대로 }}

`identity` 는 슬러그로 깎이기 전의 원본 식별자다(파일명은 안전한 문자만 쓰므로
정보가 깎인다). 소비자가 사람에게 보여주거나 되짚을 때 쓴다.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

#: 채널 레포 안의 참가자 상태 루트 (**예약 경로**). `records/`·`archive/` 와
#: 섞지 않는다 — 그 디렉토리를 훑는 코드가 "이건 레코드인가"를 매번 판정하게
#: 만들지 않기 위해서다.
STATE_DIR = "participants"
STATE_SUFFIX = ".json"

#: 봉투 포맷 버전
STATE_VERSION = 1

#: 파일명에 허용하는 문자. 레코드의 발신자 슬러그와 같은 집합이다 — 이메일이
#: 들어와도 모든 대상 OS 에서 안전하다 (`-` 는 레코드 파일명 구분자라 계속 제외).
_KEY_SAFE = re.compile(r"[^A-Za-z0-9_.@+]+")

#: 키 길이 상한. 경로 총 길이를 넉넉히 남긴다.
MAX_KEY_LEN = 64


class StateDecodeError(ValueError):
    """참가자 상태 파일을 봉투로 해석할 수 없다."""


@dataclass(frozen=True)
class ParticipantState:
    """참가자 한 명의 지금 값."""

    key: str
    """파일명이 된 슬러그 (경로 = `participants/<키>.json`)."""

    identity: str
    """슬러그로 깎이기 전 원본 식별자 (소비자가 정한다)."""

    value: Any
    """소비자 스키마의 불투명 JSON. 기반은 내용을 해석하지 않는다."""

    updated_at: datetime | None = None
    """이 값을 쓴 시각 (공통 시계 기준 UTC). 참고용 — 판단에 쓰지 않는다."""


def state_key(identity: str) -> str:
    """식별자를 파일명으로 안전하게 만든다 (경로 탈출 방지 포함)."""
    s = _KEY_SAFE.sub("_", (identity or "").strip())
    while ".." in s:
        s = s.replace("..", "_")
    s = s.strip("._")[:MAX_KEY_LEN]
    return s or "anon"


def state_path(key: str) -> str:
    """`participants/<키>.json` (채널 레포 기준 상대 경로)."""
    return f"{STATE_DIR}/{state_key(key)}{STATE_SUFFIX}"


def key_from_path(path: str) -> str | None:
    """`participants/me@x.com.json` → `me@x.com`. 상태 파일이 아니면 None."""
    if not path.startswith(STATE_DIR + "/") or not path.endswith(STATE_SUFFIX):
        return None
    name = path[len(STATE_DIR) + 1: -len(STATE_SUFFIX)]
    return name or None


def encode(
    key: str, identity: str, value: Any, updated_at: datetime | None = None
) -> bytes:
    """봉투를 UTF-8(BOM 없음)·LF 바이트로 직렬화한다.

    `indent=2` 로 쓴다 — 이 파일은 레코드와 달리 **사람이 열어 볼 수 있는 상태**
    이고(레포를 직접 들여다본 사람이 보는 첫 화면이다), 크기가 참가자 수만큼만
    늘어나므로 몇 바이트를 아낄 이유가 없다.
    """
    when = (updated_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    envelope = {
        "gitwire_state": STATE_VERSION,
        "key": state_key(key),
        "identity": identity or "",
        "updated_at": when.isoformat().replace("+00:00", "Z"),
        "value": value,
    }
    text = json.dumps(envelope, ensure_ascii=False, indent=2)
    return (text + "\n").encode("utf-8")


def decode(data: bytes, key: str) -> ParticipantState:
    """상태 파일 바이트 → `ParticipantState`.

    ⚠️ **읽기는 관대하게** 한다 — 남이 쓴 파일이고, 우리보다 새 버전이 쓴 것일 수
    있다. 봉투로 해석되지 않는 바이트만 거부한다.
    """
    try:
        env = json.loads(data.decode("utf-8-sig"))
    except Exception as exc:  # noqa: BLE001
        raise StateDecodeError(f"{key}: JSON 파싱 실패: {exc}") from exc
    if not isinstance(env, dict) or "value" not in env:
        raise StateDecodeError(f"{key}: gitwire 참가자 상태 봉투가 아니다")
    when = None
    raw = env.get("updated_at")
    if isinstance(raw, str):
        try:
            when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            when = None
    if when is not None and when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return ParticipantState(
        key=str(env.get("key") or key),
        identity=str(env.get("identity") or ""),
        value=env["value"],
        updated_at=when.astimezone(timezone.utc) if when else None,
    )
