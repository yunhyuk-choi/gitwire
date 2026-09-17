"""지난 날짜 아카이빙 — 하루치 레코드를 **로컬 파일 1개**로 접는다.

⭐⭐ 아카이브는 **추적하지 않는다 (로컬 전용)** — 왜 바뀌었나
------------------------------------------------------------
예전 롤업은 한 커밋에서 *아카이브 추가 + 레코드 삭제*를 함께 했다. 그러면 같은
데이터가 레포에 **두 벌** 남는다: 삭제된 레코드 blob 은 히스토리에 영구히
살아 있고, 그 옆에 같은 바이트의 아카이브 blob 이 새로 추가된다. 실측(이 저장소
`tests/` 로 만든 실제 레포): 롤업 커밋이 추가한 아카이브 blob **5,515 바이트** =
직전 커밋의 레코드 blob 합계 **5,515 바이트**. 즉 롤업은 용량을 줄이지 않고
**늘렸다**(pack +11%).

그래서 아카이브를 **각자 로컬에만** 둔다:

* `archive/` 는 **gitignore** 된다 (`layout.repo_skeleton` 이 심고 커밋한다 —
  전원이 무시해야 하므로 `.gitignore` 자체는 추적된다).
* 메시지는 `records/` 로만 흐르고, 그것만 추적·push 된다.
* 각 앱이 하루 한 번 전날 `records/` 를 **로컬** `archive/<날짜>.jsonl` 로 옮긴다.
* 레코드 삭제는 **전원이 그 날짜까지 옮겼다고 확인응답한 뒤에만** 일어난다.
  그 판정은 소비자 몫이다 — 기반은 *아카이빙*(`Channel.archive_days`)과
  *삭제*(`Channel.drop_days`)를 **두 동작으로 분리해** 내준다.

⚠️ `Channel.compact()` 와 다른 물건이다 (헷갈리지 말 것)
--------------------------------------------------------
| | `compact()` | **아카이빙 + 삭제 (본 모듈)** |
|---|---|---|
| 방식 | 히스토리 **재작성** + `--force` push | 평범한 커밋 1개 (삭제만) |
| 파괴적인가 | **그렇다** — 남의 미푸시 레코드를 위협한다 | 아니다 — force 없음, 히스토리 보존 |
| 자동 실행 | 없음 (`confirm=True` 필수) | 아카이빙만 자동(로컬 파일). **삭제는 절대 자동이 아니다** |
| 레코드 | 오래된 것을 **버린다** | **한 건도 버리지 않는다** — 옮길 뿐 |

무엇을 푸는가
-------------
레코드 1건 = 파일 1개다. 활발한 방은 하루 수백 건이 쌓인다. **읽기 속도가
문제가 아니다** (keyset 페이징 + 캐시로 조회는 규모와 무관하게 평평하다).
아픈 것은 **파일 개수 그 자체**다 — 클론 후 체크아웃, `git add -A`(레코드를
발행할 때마다 돈다), 작업 트리의 파일 수. 윈도우 파일시스템은 작은 파일
수만 개를 특히 싫어한다. 실측표는 README 「지난 날짜 아카이빙」 참조.

형식 — 왜 JSON Lines 인가
-------------------------
    archive/<YYYYMMDD>.jsonl        ← 추적되지 않는다 (각자 로컬)
    <레코드 봉투 1건>\n
    <레코드 봉투 1건>\n
    ...                      ← 레코드 **id 오름차순**, UTF-8(BOM 없음)·LF

* **한 레코드 = 한 줄.** 줄이 곧 레코드이므로 두 아카이브의 **합집합이 자연스럽게
  정의된다**(줄 집합의 합집합). 아카이빙은 언제나 *기존 로컬 아카이브 ∪ 지금
  살아 있는 레코드*로 다시 쓴다 — 시계가 어긋난 **다른** 참가자가 과거 날짜에
  레코드를 더해도 한 건도 잃지 않는다.
* **id 오름차순 정렬.** id 는 고정폭 타임스탬프로 시작하는 유일 키라 정렬이
  전순서로 결정된다. 순서가 결정적이어야 바이트가 결정적이다.
* **compact separators + `ensure_ascii=False`.** 직렬화 형태를 고정한다.
  실제로는 레코드 파일의 바이트를 **그대로** 한 줄로 옮기므로(재직렬화하지
  않는다) 입력이 같으면 출력이 같다.
* 개행은 LF, 끝줄에도 개행 하나. 인코딩은 UTF-8(BOM 없음).

바이트가 결정적인 성질은 이제 "동시 롤업 수렴"의 근거가 아니다(아카이브는
공유되지 않는다). 그래도 유지한다 — 같은 입력에서 같은 파일이 나오면 두
참가자의 로컬 아카이브를 바이트로 비교해 진단할 수 있고, 재실행이 멱등하다.

id 는 바뀌지 않는다
-------------------
레코드 id = `records/<날짜>/<타임스탬프>-<sender>-<nonce>.json` 이고 그것이 곧
정렬 키·커서·답장 대상이다. 아카이빙은 **저장 위치만 바꾸고 id 는 데이터로
보존한다** — 봉투 안의 `"id"` 필드가 원본 그대로 남는다. 읽기 경로는 id 를
"살아 있는 파일" 또는 "로컬 아카이브 안의 한 줄" 중 어디서든 해결한다.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import records
from .errors import GitwireError

#: 아카이브 파일이 사는 곳 (**채널 레포 기준 상대 경로**, 추적되지 않는다).
#: `records/` 밖에 두는 이유 — `records/` 의 불변식("한 파일 = 한 레코드, 절대
#: 수정하지 않는다")을 깨지 않기 위해서다. 섞어 두면 그 디렉토리를 훑는 모든
#: 코드가 "이건 레코드인가 아카이브인가"를 매번 판정해야 한다.
ARCHIVE_DIR = "archive"
ARCHIVE_SUFFIX = ".jsonl"

#: `.gitignore` 에 반드시 들어가야 하는 줄. 이 한 줄이 "아카이브는 로컬 전용"을
#: 전원에게 강제한다 — 그래서 `.gitignore` **자체는 추적·커밋된다.**
#:
#: ⚠️ 패턴 뒤에 인라인 주석을 붙이지 않는다. gitignore 에서 '#' 는 줄 첫 칸에서만
#: 주석이라 "archive/  # 설명" 은 패턴 전체를 무효화한다(실제 사고 사례).
ARCHIVE_IGNORE_LINE = ARCHIVE_DIR + "/"

#: 채널 레포에 심는 `.gitignore` 전문.
ARCHIVE_GITIGNORE = """# gitwire 채널 레포
#
# ⚠️ 규칙: 패턴 뒤에 인라인 주석을 붙이지 말 것.
# gitignore 에서 '#' 는 **줄 첫 칸에서만** 주석이다.

# 지난 날짜 아카이브는 **로컬 전용**이다 (각자 자기 컴퓨터에만 있다).
# 추적하면 같은 데이터가 두 벌 저장된다 — 삭제된 레코드는 히스토리에 영구히
# 남고, 그 옆에 같은 바이트의 아카이브 blob 이 더해진다 (실측 pack +11%).
archive/
"""

#: "지난 날"로 치기까지 UTC 자정 이후 더 기다리는 시간(시간 단위). 근거는
#: `is_closed()` 참조.
DEFAULT_GRACE_HOURS = 2.0

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


def is_day(value: str) -> bool:
    """`YYYYMMDD` 형태인가 (날짜 디렉토리·아카이브 파일명의 형식 판정)."""
    return bool(value) and _DAY_RE.match(value) is not None


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


def today_utc(now: datetime) -> str:
    """지금의 **UTC 날짜** (`YYYYMMDD`).

    ⭐ 배치 실행 **시각**과 "어제"의 **정의**는 다른 것이다. 실행 시각은 각자
    로컬 04:00 이어도 되지만(참가자마다 시간대가 달라도 상관없다 — 삭제는 합의
    뒤라 시각을 맞출 필요가 없다), *어떤 폴더가 어제인가*는 **UTC 날짜**로
    판정해야 한다. 폴더 이름 자체가 `records.format_ts()` 가 찍은 UTC 날짜이기
    때문이다. 로컬 시각으로 날짜를 계산하면 폴더명과 어긋난다.
    """
    return f"{now.astimezone(timezone.utc):%Y%m%d}"


def is_closed(day: str, now: datetime, grace_hours: float = DEFAULT_GRACE_HOURS) -> bool:
    """이 날짜를 "지난 날"로 봐도 되는가.

    ⭐ **기준은 UTC 날짜다 — 다른 선택지가 없다** (`today_utc` 참조).

    `grace_hours` 는 무엇을 사는가:

    * 참가자 간 **시계 오차**. gitwire 는 git 호스트의 HTTP `Date` 로 시계를
      맞추므로 잔여 오차는 초 단위다(`clock.py`). 다만 시계 보정에 실패한
      참가자는 로컬 시계로 degraded 동작하므로 분~시간 단위로 어긋날 수 있다.
    * 발행 **배칭 창**과 push 지연.

    2시간이면 위 둘을 넉넉히 덮는다. 더 길게 잡을 이유가 없는 이유는, **그보다
    늦게 도착하는 레코드(시계가 어긋난 참가자의 push)는 유예로 막을 수 있는
    성질이 아니기 때문**이다. 그래서 늦은 레코드는 유예가 아니라 **다시
    아카이빙**으로 처리한다 (아카이빙은 언제나 기존 아카이브와의 합집합이고,
    삭제는 내가 실제로 담은 것만 지운다 — `Channel.drop_days`).
    """
    return now.astimezone(timezone.utc) >= day_end_utc(day) + timedelta(
        hours=max(0.0, float(grace_hours))
    )


def closed_days(days, now: datetime, grace_hours: float = DEFAULT_GRACE_HOURS) -> list[str]:
    return sorted(d for d in days if _DAY_RE.match(d) and is_closed(d, now, grace_hours))


def previous_day(day: str) -> str:
    """`20260902` → `20260901`. 형식이 아니면 그대로 돌려준다."""
    if not is_day(day):
        return day
    d = datetime.strptime(day, "%Y%m%d") - timedelta(days=1)
    return f"{d:%Y%m%d}"


def last_closed_day(now: datetime, grace_hours: float = DEFAULT_GRACE_HOURS) -> str:
    """지금 시점에 "지난 날"인 **가장 큰** UTC 날짜.

    확인응답 워터마크("이 날짜까지는 다 옮겼다")의 상한이다. `is_closed` 를 날짜
    후보마다 물어보는 대신 한 번 계산한다: `now - grace` 의 UTC 날짜 **하루 전**이
    그것이다 (그 날짜의 끝 + 유예가 곧 `now - grace` 의 자정 이전이므로).
    """
    edge = now.astimezone(timezone.utc) - timedelta(hours=max(0.0, float(grace_hours)))
    return f"{edge - timedelta(days=1):%Y%m%d}"


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
        # 형식 판정은 id 의 주인이 한다 (`records.is_record_id`) — 여기서 접두·
        # 접미를 손으로 다시 세면 두 곳이 어긋난다.
        if records.is_record_id(rid):
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

    작업 사본의 파일이 정말 그 blob 인지 **git 을 부르지 않고** 검증할 때 쓴다
    (파일 하나 읽고 sha1 = 0.2ms vs git subprocess 45ms).

    ⚠️ **아카이브 경로에는 쓰지 않는다.** 이 대조는 "작업 사본이 *요청한 버전*
    인지" 확인하는 성능 최적화였고, 로컬 아카이브는 버전이 하나뿐이라(git 오브젝트
    가 아니다) 대조할 상대가 없다. 손상 검증이 아니므로 대체물을 만들지도 않는다.
    쓰이는 곳은 여전히 **레코드 blob**과 **참가자 상태 blob** 읽기다.
    """
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


# --------------------------------------------------- 로컬 아카이브 파일 (추적 X)


def archive_dir(clone: Path) -> Path:
    """작업 사본 안의 아카이브 디렉토리 (`<clone>/archive`)."""
    return Path(clone) / ARCHIVE_DIR


def stamp_of(path: Path) -> str:
    """이 파일의 **캐시 열쇠** — `<mtime_ns>:<크기>`. 없으면 빈 문자열.

    ⭐ 예전에는 blob sha 가 캐시 열쇠였다(내용 주소라 stale 이 정의상 불가능).
    로컬 파일에는 sha 가 없으므로 mtime+크기를 쓴다. 그 조합이 바뀌지 않았는데
    내용이 바뀌는 경우는 *같은 나노초에 같은 크기로 다르게 쓰는 것*뿐이고, 이
    파일을 쓰는 것은 본인 프로세스 하나(원자적 교체)라 그런 일이 생기지 않는다.
    **자체 해시를 새로 만들지 않는다** — 그건 손상 검증이고, 우리는 캐시 열쇠가
    필요한 것이다 (파일 전량 읽기 = 캐시가 없애려던 비용 그 자체다).
    """
    try:
        st = path.stat()
    except OSError:
        return ""
    return f"{st.st_mtime_ns}:{st.st_size}"


def list_archives(clone: Path) -> dict[str, str]:
    """{날짜: 스탬프} — 로컬 아카이브 **디렉토리 나열 1회**. git 호출 0개.

    날짜가 곧 파일명이므로 예전의 `ls-tree` 조회가 이 한 번의 `scandir` 로
    대체된다.
    """
    out: dict[str, str] = {}
    root = archive_dir(clone)
    try:
        entries = list(os.scandir(root))
    except OSError:
        return out
    for entry in entries:
        day = day_from_archive(entry.name)
        if not day:
            continue
        try:
            if not entry.is_file():
                continue
            st = entry.stat()
        except OSError:
            continue
        out[day] = f"{st.st_mtime_ns}:{st.st_size}"
    return out


def read_archive(clone: Path, day: str) -> bytes:
    """로컬 아카이브 한 날짜의 바이트. 없으면 `b""`."""
    try:
        return (archive_dir(clone) / f"{day}{ARCHIVE_SUFFIX}").read_bytes()
    except OSError:
        return b""


def write_archive(clone: Path, day: str, data: bytes) -> str:
    """로컬 아카이브 한 날짜를 **원자적으로** 쓴다. 반환값은 새 스탬프.

    ⚠️ **순서가 데이터 안전의 전부다.** 호출자(소비자)는 이 함수가 돌아온 *뒤에*
    확인응답을 발행해야 한다. 반대로 하면 "옮겼다"고 말해 놓고 죽었을 때 남들이
    레코드를 지우고 **그 사람만 잃는다.** 그래서 여기서:

    * 임시 파일에 쓰고 `fsync` 로 **디스크에 내려보낸 뒤** 원자적으로 교체하고,
    * 디렉토리 엔트리까지 `fsync` 한다(가능한 플랫폼에서).

    실패는 예외로 올린다 — 조용히 넘기면 그 순간이 유실 지점이 된다.
    """
    root = archive_dir(clone)
    root.mkdir(parents=True, exist_ok=True)
    final = root / f"{day}{ARCHIVE_SUFFIX}"
    tmp = root / f".{day}{ARCHIVE_SUFFIX}.tmp"
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.replace(tmp, final)
    # 디렉토리 엔트리도 내려보낸다 (POSIX). 윈도우는 디렉토리 fd 를 열 수 없어
    # 건너뛴다 — `os.replace` 가 이미 원자적이고, 잃는 것은 *교체 자체가 디스크에
    # 도달했는지*의 보장뿐이다 (플랫폼이 주지 않는 것을 흉내내지 않는다).
    if hasattr(os, "O_DIRECTORY"):  # pragma: no cover - 플랫폼 분기
        try:
            dfd = os.open(str(root), os.O_DIRECTORY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    return stamp_of(final)
