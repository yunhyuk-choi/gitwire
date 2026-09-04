"""지난 날짜 롤업 — 하루치 레코드 파일들을 **아카이브 파일 1개**로 접는다.

⚠️ `Channel.compact()` 와 다른 물건이다 (헷갈리지 말 것)
--------------------------------------------------------
| | `compact()` | **롤업 (본 모듈)** |
|---|---|---|
| 방식 | 히스토리 **재작성** + `--force` push | 평범한 커밋 1개 (add/delete) |
| 파괴적인가 | **그렇다** — 남의 미푸시 레코드를 위협한다 | 아니다 — force 없음, 히스토리 보존 |
| 자동 실행 | 없음 (`confirm=True` 필수) | 있다 (배경 스레드, 옵트아웃 가능) |
| 레코드 | 오래된 것을 **버린다** | **한 건도 버리지 않는다** — 옮길 뿐 |

무엇을 푸는가
-------------
레코드 1건 = 파일 1개다. 활발한 방은 하루 수백 건이 쌓인다. **읽기 속도가
문제가 아니다** (keyset 페이징 + sha 캐시로 조회는 규모와 무관하게 평평하다).
아픈 것은 **파일 개수 그 자체**다 — 클론 후 체크아웃, `git add -A`(레코드를
발행할 때마다 돈다), 작업 트리의 파일 수. 윈도우 파일시스템은 작은 파일
수만 개를 특히 싫어한다. 실측표는 README 「지난 날짜 롤업」 참조.

형식 — 왜 JSON Lines 인가 (⭐ 동시 롤업의 근거)
----------------------------------------------
    archive/<YYYYMMDD>.jsonl
    <레코드 봉투 1건>\n
    <레코드 봉투 1건>\n
    ...                      ← 레코드 **id 오름차순**, UTF-8(BOM 없음)·LF

중앙 서버가 없으므로 "누가 롤업할지"를 조정할 방법이 없고, 조정할 필요도 없다.
대신 **같은 입력에서 항상 바이트 단위로 같은 출력**이 나오게 만든다. 그러면 두
참가자가 같은 날을 동시에 롤업해도 같은 blob·같은 삭제가 되어 결과가 수렴한다.

그래서 형식을 이렇게 고른다:

* **한 레코드 = 한 줄.** 줄이 곧 레코드이므로 두 아카이브의 **합집합이 자연스럽게
  정의된다**(줄 집합의 합집합). 통짜 JSON 배열 하나였다면 괄호·쉼표 때문에
  텍스트 병합이 의미를 잃고, 합집합을 정의하려면 전체를 파싱해 다시 써야 한다.
* **id 오름차순 정렬.** id 는 고정폭 타임스탬프로 시작하는 유일 키라 정렬이
  전순서로 결정된다. 순서가 결정적이어야 바이트가 결정적이다.
* **compact separators + `ensure_ascii=False`.** 직렬화 형태를 고정한다.
  실제로는 레코드 파일의 바이트를 **그대로** 한 줄로 옮기므로(재직렬화하지
  않는다) 입력이 같으면 출력이 같다.
* 개행은 LF, 끝줄에도 개행 하나. 인코딩은 UTF-8(BOM 없음).

id 는 바뀌지 않는다
-------------------
레코드 id = `records/<날짜>/<타임스탬프>-<sender>-<nonce>.json` 이고 그것이 곧
정렬 키·커서·답장 대상이다. 롤업은 **저장 위치만 바꾸고 id 는 데이터로 보존한다** —
봉투 안의 `"id"` 필드가 원본 그대로 남는다. 읽기 경로는 id 를 "살아 있는 파일"
또는 "아카이브 안의 한 줄" 중 어디서든 해결한다.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone

from .errors import GitwireError

#: 아카이브 파일이 사는 곳. `records/` 밖에 두는 이유 —
#: `records/` 의 불변식("한 파일 = 한 레코드, 절대 수정하지 않는다")을 깨지 않기
#: 위해서다. 섞어 두면 그 디렉토리를 훑는 모든 코드가 "이건 레코드인가 아카이브인가"를
#: 매번 판정해야 한다.
ARCHIVE_DIR = "archive"
ARCHIVE_SUFFIX = ".jsonl"

#: "지난 날"로 치기까지 UTC 자정 이후 더 기다리는 시간(시간 단위). 근거는
#: `is_closed()` 참조.
DEFAULT_GRACE_HOURS = 2.0

#: 이 미만이면 접어도 이득이 없다 (파일 1개 → 파일 1개).
DEFAULT_MIN_RECORDS = 2

_DAY_RE = re.compile(r"^\d{8}$")
#: 봉투의 첫 `"id"` 필드. `records.encode()` 가 키를 gitwire→id→sender→ts→payload
#: 순으로 쓰므로 첫 매치가 봉투의 id 다. 어긋나면 아래에서 json 파싱으로 떨어진다.
_ID_RE = re.compile(r'"id"\s*:\s*"((?:[^"\\]|\\.)*)"')


class ArchiveFormatError(GitwireError):
    """아카이브로 접을 수 없는 바이트를 만났다 (그 날짜는 접지 않는다)."""

    exit_code = 8


# ------------------------------------------------------------------ 경로 규약


def archive_path(day: str) -> str:
    """`archive/<YYYYMMDD>.jsonl` (채널 레포 기준 상대 경로)."""
    return f"{ARCHIVE_DIR}/{day}{ARCHIVE_SUFFIX}"


def day_of(record_id: str) -> str | None:
    """레코드 id 에서 날짜 디렉토리 이름을 뽑는다."""
    parts = record_id.split("/")
    if len(parts) >= 3 and _DAY_RE.match(parts[1]):
        return parts[1]
    return None


def day_from_archive(path: str) -> str | None:
    """`archive/20260901.jsonl` → `20260901`."""
    name = path.rsplit("/", 1)[-1]
    if not name.endswith(ARCHIVE_SUFFIX):
        return None
    day = name[: -len(ARCHIVE_SUFFIX)]
    return day if _DAY_RE.match(day) else None


# ------------------------------------------------------------------ 날짜 판정


def day_end_utc(day: str) -> datetime:
    """그 날짜 디렉토리가 더 이상 자라지 않게 되는 경계 (= 다음 날 UTC 00:00)."""
    d = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    return d + timedelta(days=1)


def is_closed(day: str, now: datetime, grace_hours: float = DEFAULT_GRACE_HOURS) -> bool:
    """이 날짜를 "지난 날"로 봐도 되는가.

    ⭐ **기준은 UTC 날짜다 — 다른 선택지가 없다.**
    날짜 디렉토리 이름 자체가 `format_ts()` 가 찍은 **UTC** 날짜다. 한국 날짜로
    끊으려면 한 롤업이 UTC 디렉토리 두 개에 걸쳐야 하고, 그러면 "디렉토리 하나 =
    아카이브 하나"라는 단순한 대응이 깨진다. 참가자가 여러 시간대일 수 있는
    이상, 로컬 달력이 아니라 **저장 형식의 달력**을 따르는 것이 유일하게
    자기정합적이다.

    `grace_hours` 는 무엇을 사는가:

    * 참가자 간 **시계 오차**. gitwire 는 git 호스트의 HTTP `Date` 로 시계를
      맞추므로 잔여 오차는 초 단위다(`clock.py`). 다만 시계 보정에 실패한
      참가자는 로컬 시계로 degraded 동작하므로 분~시간 단위로 어긋날 수 있다.
    * 발행 **배칭 창**과 push 지연.

    2시간이면 위 둘을 넉넉히 덮는다. 더 길게 잡을 이유가 없는 이유는, **그보다
    늦게 도착하는 레코드(오프라인 참가자의 나중 push)는 유예로 막을 수 있는
    성질이 아니기 때문**이다 — 지연이 무한할 수 있다. 그래서 늦은 레코드는
    유예가 아니라 **뒤늦은 도착 경로**(다음 롤업이 아카이브에 합쳐 넣는다)로
    처리한다. 유예는 "흔한 경우를 두 번 일하지 않게" 하는 최적화일 뿐이다.

    한국 시간으로 읽으면: UTC 날짜 D 는 KST D+1 11:00 경에 접힌다
    (UTC D+1 00:00 + 2h = KST D+1 11:00). 즉 "어제까지의 대화"가 다음 날
    오전 중에 접힌다 — 사용자가 말한 감각과 맞는다.
    """
    return now.astimezone(timezone.utc) >= day_end_utc(day) + timedelta(
        hours=max(0.0, float(grace_hours))
    )


def closed_days(days, now: datetime, grace_hours: float = DEFAULT_GRACE_HOURS) -> list[str]:
    return sorted(d for d in days if _DAY_RE.match(d) and is_closed(d, now, grace_hours))


# ------------------------------------------------------------------ 직렬화


def canonical_line(data: bytes) -> str:
    """레코드 파일 바이트 1건 → 아카이브 한 줄.

    보통은 **원본 바이트를 그대로** 옮긴다(`records.encode()` 가 이미 개행 없는
    한 줄을 만든다). 어쩌다 여러 줄로 쓰인 봉투를 만나면 compact 형태로 다시
    직렬화한다 — 그래야 "한 레코드 = 한 줄"이 깨지지 않는다.

    해석할 수 없으면 `ArchiveFormatError` 를 올린다. 접을 수 없는 것을 접었다고
    하고 원본을 지우는 일은 절대 없어야 한다 (호출자는 그 날짜를 통째로 건너뛴다).
    """
    try:
        text = data.decode("utf-8-sig").strip()
    except UnicodeDecodeError as exc:
        raise ArchiveFormatError(f"UTF-8 이 아니다: {exc}") from exc
    if not text:
        raise ArchiveFormatError("빈 레코드 파일")
    try:
        obj = json.loads(text)
    except ValueError as exc:
        raise ArchiveFormatError(f"JSON 파싱 실패: {exc}") from exc
    if not isinstance(obj, dict):
        raise ArchiveFormatError("봉투가 JSON 객체가 아니다")
    if "\n" in text or "\r" in text:
        text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return text


def build_archive(lines_by_id: dict[str, str]) -> bytes:
    """id → 줄 매핑을 **결정적 바이트**로 만든다 (id 오름차순 · LF · UTF-8)."""
    return "".join(f"{lines_by_id[k]}\n" for k in sorted(lines_by_id)).encode("utf-8")


def split_lines(data: bytes) -> list[str]:
    """아카이브 바이트 → 줄 목록 (빈 줄 제거)."""
    return [ln for ln in data.decode("utf-8-sig").split("\n") if ln.strip()]


def line_id(line: str) -> str | None:
    """아카이브 한 줄에서 레코드 id 를 뽑는다.

    정규식 우선(전량 json 파싱보다 10배 빠르다), 형태가 어긋나면 json 으로 확인.
    """
    m = _ID_RE.search(line)
    if m:
        rid = m.group(1)
        if rid.startswith("records/") and rid.endswith(".json"):
            return rid
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    rid = obj.get("id") if isinstance(obj, dict) else None
    return rid if isinstance(rid, str) else None


def index_archive(data: bytes) -> dict[str, str]:
    """아카이브 바이트 → {레코드 id: 줄}. 해석 못 한 줄은 버리지 않고 보존한다."""
    out: dict[str, str] = {}
    for i, line in enumerate(split_lines(data)):
        rid = line_id(line)
        # id 를 못 읽은 줄도 **버리지 않는다** — 정렬만 불가하므로 합성 키로 남긴다.
        out[rid or f"￿{i:08d}"] = line
    return out


def blob_sha1(data: bytes) -> str:
    """git 이 이 바이트에 매길 blob 오브젝트 이름 (sha1).

    작업 사본의 아카이브 파일이 정말 그 blob 인지 **git 을 부르지 않고** 검증할
    때 쓴다 (파일 하나 읽고 sha1 = 0.2ms vs git subprocess 45ms).
    """
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
