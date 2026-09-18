"""로컬 ref 조회 캐시 — **유휴 폴링에서 git 프로세스를 없앤다.**

무엇이 문제였나 (실측, 2026-09-08 · Windows 11 · git 2.51 · 방 1개·새 메시지 없음)
----------------------------------------------------------------------------
아무 일도 없는데 폴링 **한 번마다 git 프로세스 7개**가 떴다::

    ls-remote origin refs/heads/main HEAD          ← 원격을 봐야 하는 유일한 호출
    rev-parse --verify --quiet refs/remotes/origin/main
    rev-parse --verify --quiet HEAD
    cat-file -e <sha>^{commit}                     ┐ `_plan()` — 기준 커밋과 목표
    merge-base --is-ancestor <sha> <sha>           │ 커밋이 **같은데도** 도달성·
    diff --name-only --diff-filter=d -z <sha> <sha>│ 조상 관계·diff 를 매번 물었다
    diff --name-only -z <sha> <sha>                ┘

git subprocess 한 번은 하는 일과 무관하게 **약 494 KB 를 읽고 65~73회의 읽기
연산**을 낸다(같은 조건 실측 — 비용은 작업이 아니라 프로세스 기동이다: 바이너리·
DLL·설정·인덱스를 매번 다시 읽는다). 그래서 "아무 일도 없는 방"이 분당 수 MB 를
읽는다. CPU·메모리는 멀쩡한데 **디스크 읽기만** 아픈 이유가 이것이었다.

이 모듈이 없애는 것
------------------
로컬 HEAD 와 원격추적 ref 는 **우리가 로컬을 바꾸지 않는 한 바뀌지 않는다.**
유휴에서는 아무도 바꾸지 않으므로 매번 물어볼 이유가 없다. 그래서 그 두 값을
메모리에 들고, 아래 **두 겹의 방어**로 낡은 값을 막는다.

⭐ 겹 1 — `.git` 안 ref 파일의 **스탬프**로 유효성을 확인한다
-------------------------------------------------------------
캐시가 채워질 때 `.git/HEAD`·(HEAD 가 가리키는 ref 파일)·`refs/remotes/...`·
`packed-refs`·`config` 의 **stat + (작은 파일은) 내용**을 함께 기록한다. 조회할
때 그 스탬프를 다시 계산해 **한 바이트라도 다르면 캐시를 버리고 git 에게 다시
묻는다.** 스탬프 계산은 stat 4~5회 + 41바이트 파일 3개 읽기 = 프로세스 0개다.

이 겹이 있어야 **밖에서 클론을 건드리는 경우**를 견딘다 — 사람이 그 클론에서
직접 `git fetch`·`git reset` 을 돌리면 ref 파일이 반드시 바뀌므로 우리가 즉시
알아챈다. "우리만 로컬을 바꾼다"는 가정에 기대지 않는다. (그 가정은 언젠가
깨지고, 깨졌을 때의 증상이 *새 메시지가 안 보이는 조용한 오류*다 — 이 프로젝트가
가장 싫어하는 실패 모드.)

시각 해상도에 기대지 않는다: ref 파일은 41바이트라 **내용을 그대로 비교**한다.
mtime 해상도가 1초인 파일시스템(FAT·일부 네트워크 공유)에서도 정확하다.
`packed-refs`·`config` 는 클 수 있어 stat 만 보는데, git 은 이 둘을 **임시파일 +
rename** 으로 통째로 갈아치우므로 stat 이 반드시 달라진다.

⭐ 겹 2 — 우리가 부르는 **모든** git 호출을 한 곳에서 보고 무효화한다
--------------------------------------------------------------------
`GuardedRunner` 가 채널의 유일한 git 실행 경로를 감싼다. **읽기 전용 화이트
리스트에 없는 서브커맨드면 무조건 캐시를 버린다** (fail-safe 기본값 = 무효화).
호출 지점을 사람이 하나하나 챙기는 방식이 아니므로 *새 코드가 로컬을 바꿔도
무효화를 빠뜨릴 수 없다* — 빠뜨리려면 화이트리스트에 일부러 추가해야 한다.

신뢰할 수 없으면 **캐시하지 않는다**
-----------------------------------
`.git` 이 디렉토리가 아니거나(워크트리·서브모듈), `reftable` 백엔드를 쓰거나,
HEAD 를 읽을 수 없으면 스탬프를 만들지 않고 **매번 git 에게 묻는다** (예전 동작).
느려질 뿐 틀리지 않는다. 특히 reftable 은 ref 를 `.git/refs/*` 파일로 두지
않으므로 스탬프가 영영 그대로일 수 있다 — 그 경우가 정확히 "조용히 낡는" 경우라
아예 켜지 않는다.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from pathlib import Path
from typing import Callable, Sequence

log = logging.getLogger("gitwire")

#: 스탬프에 내용까지 담는 파일의 크기 상한. ref 파일은 41바이트(sha + 개행)이고
#: symref 는 그보다 조금 길다. 이보다 크면 stat 만 본다.
MAX_STAMP_BYTES = 4096

#: **로컬 ref 를 절대 바꾸지 않는** 서브커맨드. 여기 없으면 무효화한다.
#: (`commit-tree`·`hash-object -w`·`write-tree` 는 오브젝트만 만들고 ref 를
#: 움직이지 않는다 — 롤업이 임시 인덱스로 쓰는 경로다.)
READ_ONLY_COMMANDS = frozenset({
    "cat-file",
    "commit-tree",
    "count-objects",
    "credential",
    "diff",
    "for-each-ref",
    "hash-object",
    "log",
    "ls-remote",
    "ls-tree",
    "merge-base",
    "rev-list",
    "rev-parse",
    "show",
    "show-ref",
    "status",
    "var",
    "version",
    "write-tree",
})


def subcommand(args: Sequence[str]) -> str:
    """`git` 인자열에서 서브커맨드 이름을 뽑는다 (`-c k=v` 같은 전역 옵션은 건너뛴다).

    `Git.run()` 이 항상 `BASE_CONFIG`(`-c core.autocrlf=false` …)를 앞에 붙이므로
    `args[0]` 은 서브커맨드가 아니다. 못 찾으면 빈 문자열 — 호출자는 그것을
    "읽기 전용이 아니다"로 다룬다 (fail-safe).
    """
    it = iter(args)
    for arg in it:
        if arg in ("-c", "-C", "--git-dir", "--work-tree", "--namespace"):
            next(it, None)                      # 값 하나를 함께 소비한다
            continue
        if arg.startswith("-"):
            continue
        return arg
    return ""


def is_read_only(args: Sequence[str]) -> bool:
    """이 호출이 로컬 ref 를 바꿀 수 **없다**고 단정할 수 있나."""
    return subcommand(args) in READ_ONLY_COMMANDS


#: ref 파일 하나에 담긴 값. sha1(40) 또는 sha256(64) 16진수.
_SHA_RE = re.compile(r"\A[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")

#: symref 사슬을 따라갈 최대 깊이. 실전에서는 1(`HEAD` → `refs/heads/x`)이다.
_MAX_SYMREF = 4


def gitdir(clone_dir: Path) -> Path | None:
    """ref 파일을 **직접 읽어도 되는** `.git` 인가. 아니면 None (= git 에게 물어라).

    거르는 것은 모듈 도크의 「신뢰할 수 없으면 캐시하지 않는다」와 같은 판정이다:
    `.git` 이 파일이면 워크트리·서브모듈이라 ref 가 다른 곳에 있고, `reftable`
    백엔드는 ref 를 `refs/*` 파일로 두지 않는다.
    """
    gd = Path(clone_dir) / ".git"
    if not gd.is_dir():
        return None
    if (gd / "reftable").exists():
        return None
    return gd


def _packed(gd: Path, name: str) -> tuple[bool, str | None]:
    """`packed-refs` 에서 `name` 을 찾는다. 반환 규약은 `ref_sha` 와 같다."""
    try:
        raw = (gd / "packed-refs").read_bytes()
    except FileNotFoundError:
        return (True, None)                     # 파일이 없다 = 묶인 ref 도 없다
    except OSError:
        return (False, None)                    # 읽을 수 없다 — 모른다
    want = name.encode()
    for line in raw.splitlines():
        if not line or line[:1] in (b"#", b"^"):
            continue                            # 헤더 / peel 줄
        sha, _, ref = line.partition(b" ")
        if ref.strip() == want:
            text = sha.decode("ascii", "replace")
            return (True, text) if _SHA_RE.match(text) else (False, None)
    return (True, None)


def ref_sha(clone_dir: Path, name: str = "HEAD") -> tuple[bool, str | None]:
    """ref 의 sha 를 **`.git` 파일에서 직접** 읽는다 — git 프로세스 0개.

    반환은 `(판정했나, sha)` 다. 이 두 값을 갈라 주는 것이 요점이다:

    * `(True, "<sha>")` — 그 ref 는 이 sha 다.
    * `(True, None)`    — 그 ref 는 **없다** (unborn 브랜치).
    * `(False, None)`   — **모른다.** 호출자는 `rev-parse` 로 물어야 한다.

    셋을 합치면 "모른다"가 "없다"로 번역되고, 그 오역이 정확히 *우리가 가장
    싫어하는 조용한 오류*다 (없다고 보면 `_integrate` 가 `reset --hard` 로 간다).

    왜 이것이 필요한가 — `rev-parse` 한 번이 이 머신에서 **87ms**(프로세스 기동
    비용이고 하는 일과 무관하다, `localrefs` 모듈 도크)인데 전송 한 번에 두 번
    돈다. 이 함수는 stat + 41바이트 읽기 몇 번이라 **0.1ms 미만**이다.

    ⚠️ `rev-parse --verify` 와 **한 가지가 다르다** — 오브젝트가 실제로 있는지
    확인하지 않는다(그건 `.git/objects` 를 뒤지는 일이다). ref 가 없는 오브젝트를
    가리키는 손상된 클론에서는 `rev-parse` 가 "없다"로, 이 함수는 "이 sha 다"로
    답한다. 그 차이로 무엇도 조용히 사라지지 않는다: 그 sha 를 쓰는 다음 명령이
    큰 소리로 실패한다 (push·merge-base 전부 오브젝트를 읽는다).
    """
    gd = gitdir(clone_dir)
    if gd is None:
        return (False, None)
    for _ in range(_MAX_SYMREF):
        if name != "HEAD" and not (name.startswith("refs/") and ".." not in name):
            return (False, None)                # 모양을 모른다 — 판단 보류
        try:
            raw = (gd / name).read_bytes()
        except FileNotFoundError:
            return _packed(gd, name)            # loose 가 없다 → 묶인 쪽
        except OSError:
            return (False, None)
        text = raw.decode("utf-8", "replace").strip()
        if text.startswith("ref:"):
            name = text[4:].strip()
            continue
        return (True, text) if _SHA_RE.match(text) else (False, None)
    return (False, None)                        # symref 사슬이 너무 깊다·순환


def write_ref(clone_dir: Path, name: str, sha: str) -> bool:
    """loose ref 파일을 **git 의 락 규약대로** 직접 쓴다. 성공했으면 True.

    False 면 호출자는 `git update-ref` 로 되돌아가야 한다 (실패가 아니라 폴백).

    왜 — `update-ref` 한 번도 프로세스 기동 **91ms** 다. 이 함수는 파일 두 번
    쓰기다. 규약은 git 과 같다: `<ref>.lock` 을 **배타 생성**해 쓰고 제자리로
    rename 한다. 그래서 같은 순간 `git update-ref` 가 돌면 둘 중 하나만 이기고,
    반쯤 쓰인 ref 파일이 보이는 일이 없다.

    ⚠️ **reflog 를 쓰지 않는다.** `core.logAllRefUpdates` 의 기본값은
    `refs/heads`·`refs/remotes`·`refs/notes`·`HEAD` 에만 로그를 남기므로 우리가
    쓰는 `refs/gitwire/*` 에는 원래 reflog 가 생기지 않는다 — 기본 설정에서는
    `update-ref` 와 결과가 **한 바이트도 다르지 않다.** `always` 로 켜 둔 사람은
    이 ref 의 reflog 만 잃고, 그 reflog 는 아무도 읽지 않는다.
    """
    gd = gitdir(clone_dir)
    if gd is None or not (name.startswith("refs/") and ".." not in name):
        return False
    if not _SHA_RE.match(sha):
        return False
    target = gd / name
    lock = gd / (name + ".lock")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    except OSError:
        # ⚠️ 락을 못 얻었다 (남이 쥐고 있거나 쓸 수 없는 곳이다). **그 파일에
        # 손대지 않는다** — 남의 락을 치우면 그쪽 ref 갱신이 깨진다.
        return False
    try:
        try:
            os.write(fd, (sha + '\n').encode("ascii"))
        finally:
            os.close(fd)
        os.replace(str(lock), str(target))
        return True
    except OSError:
        try:
            lock.unlink()            # 우리가 만든 락만 치운다
        except OSError:
            pass
        return False


def _part(path: Path) -> tuple:
    """파일 하나의 스탬프 조각 — (크기, mtime_ns, ctime_ns, 내용|None)."""
    try:
        st = path.stat()
    except OSError:
        return (None,)                          # 없음도 하나의 상태다
    data: bytes | None = None
    if st.st_size <= MAX_STAMP_BYTES:
        try:
            data = path.read_bytes()
        except OSError:
            data = None
    return (st.st_size, st.st_mtime_ns, st.st_ctime_ns, data)


class LocalRefCache:
    """로컬 ref → sha 메모리 캐시. 스탬프가 어긋나면 스스로 버린다.

    스레드 안전하다 — 채널 락 밖에서도 불린다(`sync()` 는 네트워크를 락 밖에서
    한다). 경합이 없을 때 락 비용은 수십 ns 다.
    """

    def __init__(self, clone_dir: Path, refs: Sequence[str] = ()) -> None:
        self.clone_dir = Path(clone_dir)
        self.refs = tuple(refs)
        self._lock = threading.Lock()
        self._values: dict[str, str | None] = {}
        self._stamp_at: tuple | None = None
        self._gen = 0
        self.hits = 0
        self.misses = 0
        self.invalidations = 0
        self.unstampable = 0
        self.races = 0
        self.last_reason: str | None = None

    # --------------------------------------------------------------- 스탬프

    def _gitdir(self) -> Path | None:
        """`.git` 디렉토리. 신뢰할 수 없는 모양이면 None (= 캐시 끔).

        판정은 모듈 함수 `gitdir()` 하나다 — 직접 읽기(`ref_sha`)와 **같은 기준**
        이어야 한다. 둘이 갈리면 한쪽만 신뢰하는 모양이 생긴다.
        """
        return gitdir(self.clone_dir)

    def stamp(self) -> tuple | None:
        """지금의 ref 파일 상태. None 이면 **캐시하지 않는다**."""
        gitdir = self._gitdir()
        if gitdir is None:
            return None
        head = _part(gitdir / "HEAD")
        if head[0] is None:
            return None                         # HEAD 를 못 읽는다 — 판단 보류
        parts: list[tuple] = [("HEAD", head)]
        # HEAD 가 symref 면 그 대상 파일까지 봐야 값이 결정된다.
        raw = head[3]
        if raw is not None:
            text = raw.decode("utf-8", "replace").strip()
            if text.startswith("ref:"):
                name = text[4:].strip()
                if name.startswith("refs/") and ".." not in name:
                    parts.append((name, _part(gitdir / name)))
                else:
                    return None                 # 알 수 없는 HEAD 모양
        else:
            return None                         # HEAD 를 읽지 못했다
        for name in self.refs:
            parts.append((name, _part(gitdir / name)))
        parts.append(("packed-refs", _part(gitdir / "packed-refs")))
        parts.append(("config", _part(gitdir / "config")))
        return tuple(parts)

    # ---------------------------------------------------------------- 조회

    def resolve(
        self, key: str, resolver: Callable[[], str | None]
    ) -> str | None:
        """`key` 의 값을 돌려준다. 캐시가 유효하면 `resolver` 를 부르지 않는다.

        `resolver` 는 git 을 부르는 함수다 — **락 밖에서** 부른다(그 안에서 다시
        채널 락을 잡을 수 있으므로 여기서 락을 쥐고 있으면 교착이 생긴다).
        """
        stamp = self.stamp()
        with self._lock:
            gen = self._gen
            if stamp is not None and stamp == self._stamp_at and key in self._values:
                self.hits += 1
                return self._values[key]
        value = resolver()
        if stamp is None:
            with self._lock:
                self.unstampable += 1
            return value
        after = self.stamp()
        with self._lock:
            if after != stamp or gen != self._gen:
                # resolver 가 도는 동안 무언가 바뀌었다 → 담지 않는다.
                self.races += 1
                self._values.clear()
                self._stamp_at = None
                return value
            if self._stamp_at != stamp:
                self._values.clear()
                self._stamp_at = stamp
            self._values[key] = value
            self.misses += 1
        return value

    def invalidate(self, reason: str = "") -> None:
        """캐시를 버린다. 로컬을 바꿀 수 있는 git 호출마다 불린다."""
        with self._lock:
            self._gen += 1
            self._values.clear()
            self._stamp_at = None
            self.invalidations += 1
            self.last_reason = reason or None

    def info(self) -> dict:
        """관측용 상태. 조용히 동작하지 않게 — 적중·무효화·포기 횟수를 드러낸다."""
        with self._lock:
            return {
                "enabled": self._gitdir() is not None,
                "cached": sorted(self._values),
                "hits": self.hits,
                "misses": self.misses,
                "invalidations": self.invalidations,
                "unstampable": self.unstampable,
                "races": self.races,
                "last_invalidation": self.last_reason,
            }


class GuardedRunner:
    """GitRunner 데코레이터 — **로컬을 바꿀 수 있는 호출이면 캐시를 무효화한다.**

    채널의 모든 git 호출이 이 하나를 지나므로(임시 인덱스를 쓰는 롤업 경로까지)
    "무효화를 빠뜨린 호출 지점"이 구조적으로 존재할 수 없다. 판정은
    `READ_ONLY_COMMANDS` **화이트리스트**이고, 모르는 서브커맨드는 위험한 쪽으로
    (= 무효화) 분류한다.

    무효화는 `finally` 에서 한다 — 실패한 명령도 로컬을 절반쯤 바꿔 놓을 수 있다
    (rebase 중단, 부분 fetch 등).
    """

    def __init__(self, inner, cache: LocalRefCache) -> None:
        self.inner = inner
        self.cache = cache

    def run(self, args, *, cwd=None, env=None, timeout=None):
        if is_read_only(args):
            return self.inner.run(args, cwd=cwd, env=env, timeout=timeout)
        try:
            return self.inner.run(args, cwd=cwd, env=env, timeout=timeout)
        finally:
            self.cache.invalidate("git " + (subcommand(args) or "?"))
