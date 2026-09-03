"""마지막 처리 지점(커서) 영속화.

왜 디스크인가
------------
소비자는 두 가지 모양 모두일 수 있다:

* **상시 프로세스** (웹앱): 루프를 돌며 구독한다.
* **일회성 프로세스** (에이전트가 셸에서 호출): 한 번 실행하고 끝난다.

후자는 메모리 커서를 가질 수 없다. 매 호출이 새 프로세스이므로 커서가
메모리에만 있으면 **매번 전체를 다시 읽거나(중복) 놓친다(유실)**. 그래서
커서는 채널 디렉토리 안 파일에 저장한다.

소비자별 분리
------------
한 채널을 여러 소비자가 각자의 속도로 읽을 수 있다(웹앱 + 에이전트). 커서는
`cursors/<consumer>.json` 으로 **소비자마다 따로** 둔다.

커서 내용
--------
    commit      : 마지막으로 배치를 계산한 기준 커밋 SHA
    batch_head  : 진행 중인 배치를 계산할 때의 목표 커밋 SHA
    batch_pos   : 그 배치 중 몇 개까지 전달했는지
    watermark   : 지금까지 전달한 레코드 ID 의 최대값 (히스토리 재작성 복구용)

`commit` + `batch_pos` 는 **결정적 재계산**을 가능하게 한다. 프로세스가 중간에
죽어도 같은 기준 커밋에서 같은 배치를 다시 만들어 앞의 batch_pos 개를 건너뛰면
정확히 이어진다 → 중복·유실 없음.

`watermark` 는 기준 커밋이 원격 히스토리에서 사라졌을 때(압축·force-push)의
폴백이다. 이때는 레코드 ID 사전순 비교로 "이미 준 것"을 걸러낸다.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path

CURSOR_VERSION = 1
_CONSUMER_RE = re.compile(r"[^A-Za-z0-9_.-]+")
DEFAULT_CONSUMER = "default"


def safe_consumer(name: str) -> str:
    """소비자 이름을 파일명으로 안전하게 만든다 (경로 탈출 방지 포함)."""
    s = _CONSUMER_RE.sub("_", (name or "").strip())
    while ".." in s:
        s = s.replace("..", "_")
    s = s.strip("._")[:48]
    return s or DEFAULT_CONSUMER


@dataclass
class Cursor:
    version: int = CURSOR_VERSION
    commit: str | None = None
    batch_head: str | None = None
    batch_pos: int = 0
    watermark: str | None = None
    started: bool = False
    """한 번이라도 전달을 시작했는지. False 면 '처음 붙는 소비자'."""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=2) + "\n"


class CursorStore:
    """채널 디렉토리 안의 소비자별 커서 파일."""

    def __init__(self, base_dir: Path, consumer: str = DEFAULT_CONSUMER) -> None:
        self.consumer = safe_consumer(consumer)
        self.path = Path(base_dir) / "cursors" / f"{self.consumer}.json"

    def load(self) -> Cursor:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return Cursor()
        return Cursor(
            version=int(raw.get("version", CURSOR_VERSION)),
            commit=raw.get("commit"),
            batch_head=raw.get("batch_head"),
            batch_pos=int(raw.get("batch_pos", 0)),
            watermark=raw.get("watermark"),
            started=bool(raw.get("started", raw.get("watermark") is not None)),
        )

    def save(self, cursor: Cursor) -> None:
        """원자적 저장 — 부분 기록된 커서가 남지 않게 한다."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(cursor.to_json())
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def reset(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
