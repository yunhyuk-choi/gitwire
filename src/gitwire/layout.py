"""로컬 클론 위치 규약 + 채널 레포 디렉토리 구조.

클론 위치 — 판단과 근거
----------------------
사용자는 이 디렉토리를 볼 일이 없다. 앱의 **캐시/상태 저장소**이지 작업
사본이 아니다. 그래서 OS 관례를 따른다:

    Windows : %LOCALAPPDATA%/gitwire/channels/<slug>-<hash12>/
    macOS   : ~/Library/Application Support/gitwire/channels/...
    Linux   : $XDG_DATA_HOME(or ~/.local/share)/gitwire/channels/...

* 사용자 문서·프로젝트 폴더를 오염시키지 않는다.
* **캐시(XDG_CACHE_HOME)가 아니라 데이터 디렉토리**를 쓴다. 커서(마지막 처리
  지점)와 **아직 push 되지 않은 레코드**가 여기 있다 — 캐시 청소 도구가
  지워도 되는 데이터가 아니다.
* 환경변수 `GITWIRE_HOME` 으로 통째로 덮어쓸 수 있다 (테스트·격리 실행).

디렉토리 이름 = `<slug>-<sha256(정규화 URL)[:12]>`
* 해시: 결정적이고 충돌이 사실상 없다. 같은 URL 이면 어느 프로세스에서 열든
  같은 클론·같은 커서를 가리킨다 → **일회성 CLI 호출을 반복해도 상태가 이어진다.**
* slug: 사람이 디버깅할 때 어느 레포인지 알아보게 하는 접두사(기능적 의미 없음).
* 정규화(소문자 스킴/호스트, 끝의 `.git`·`/` 제거, **자격증명 부분 제거**)로
  같은 레포를 다른 문자열로 적어도 같은 채널이 되게 한다.

채널 디렉토리 안
---------------
    <channel-dir>/
      clone/            git 클론 (작업 사본)
      cursors/<consumer>.json   소비자별 마지막 처리 지점
      channel.json      로컬 메타(원격 URL 등, 토큰은 없음)
      askpass.*         자격증명 헬퍼(토큰 값은 들어있지 않음)

채널 레포(공유되는 쪽) 안
------------------------
    gitwire.json        채널 메타 (포맷 버전)
    records/<날짜>/*.json
    .gitattributes      모든 파일 바이트 보존 (CRLF 변환 금지)
    README.md           레포를 직접 열어본 사람을 위한 안내
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

CHANNEL_META = "gitwire.json"
CHANNEL_FORMAT = 1
_SLUG_RE = re.compile(r"[^a-z0-9_.-]+")

_REPO_README = """# gitwire 채널 레포

이 레포는 [gitwire](https://example.invalid/gitwire) 가 **전송 계층**으로 쓰는
저장소다. 사람이 직접 편집하지 않는다.

* `records/<날짜>/*.json` — append-only 레코드. 한 건 = 한 파일. 수정·삭제하지 않는다.
* `gitwire.json` — 채널 메타데이터.

레코드의 `payload` 스키마는 이 레포를 쓰는 **소비자 애플리케이션**이 정한다.
gitwire 자신은 payload 내용을 해석하지 않는다.
"""

_REPO_GITATTRIBUTES = """* -text
*.json -text
"""


def normalize_repo_url(url: str) -> str:
    """채널 동일성 판정을 위한 정규화. 자격증명 부분은 제거한다."""
    u = (url or "").strip()
    if not u:
        raise ValueError("빈 레포 URL")
    parts = urlsplit(u)
    if parts.scheme in ("http", "https", "ssh", "git"):
        netloc = parts.netloc.rsplit("@", 1)[-1].lower()
        path = parts.path.rstrip("/")
        if path.endswith(".git"):
            path = path[:-4]
        return urlunsplit((parts.scheme.lower(), netloc, path, "", ""))
    if u.startswith("git@") or ("@" in u and ":" in u and not parts.scheme):
        # scp 형식: git@host:owner/repo.git
        host, _, path = u.partition(":")
        host = host.rsplit("@", 1)[-1].lower()
        path = path.rstrip("/")
        if path.endswith(".git"):
            path = path[:-4]
        return f"ssh://{host}/{path}"
    # 로컬 경로 (테스트의 bare 레포 포함)
    try:
        return Path(u).resolve().as_posix().rstrip("/")
    except OSError:
        return u.rstrip("/")


def channel_key(url: str) -> str:
    """정규화 URL 의 sha256 앞 12자리."""
    return hashlib.sha256(normalize_repo_url(url).encode("utf-8")).hexdigest()[:12]


def channel_slug(url: str) -> str:
    """디버깅용 사람 친화 접두사."""
    norm = normalize_repo_url(url)
    tail = norm.rstrip("/").rsplit("/", 1)[-1] or "channel"
    slug = _SLUG_RE.sub("-", tail.lower()).strip("-")[:32]
    return slug or "channel"


def gitwire_home() -> Path:
    """gitwire 의 로컬 상태 루트."""
    env = os.environ.get("GITWIRE_HOME")
    if env:
        return Path(env)
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local")
        return Path(base) / "gitwire"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "gitwire"
    base = os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share")
    return Path(base) / "gitwire"


def channel_dir(url: str, home: Path | None = None) -> Path:
    """이 레포 URL 에 대응하는 채널 디렉토리 (결정적)."""
    root = home or gitwire_home()
    return root / "channels" / f"{channel_slug(url)}-{channel_key(url)}"


def repo_skeleton(channel_name: str | None, created_at: str) -> dict[str, bytes]:
    """빈 레포에 심을 초기 파일들. '주소만 넣으면 방이 된다'를 성립시킨다."""
    meta = {
        "gitwire": CHANNEL_FORMAT,
        "kind": "channel",
        "name": channel_name or "",
        "created_at": created_at,
        "records_dir": "records",
    }
    return {
        CHANNEL_META: (
            json.dumps(meta, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8"),
        "README.md": _REPO_README.encode("utf-8"),
        ".gitattributes": _REPO_GITATTRIBUTES.encode("utf-8"),
        "records/.gitkeep": b"",
    }
