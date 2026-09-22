"""git 없이 커밋한다 — blob·tree·commit 오브젝트와 인덱스·ref·reflog 를 파이썬이 직접 쓴다.

왜
--
전송 한 번의 로컬 단계는 `git add` + `git commit` 두 프로세스였고, 이 머신(EDR 이
도는 Windows 11) 실측으로 **83 + 115 ms** 다 — 그런데 git 자신이 잰 일은 add 14ms ·
commit 93ms 이고 그 대부분도 인덱스를 다시 읽고 상태 요약을 내는 일이다. 우리가
커밋에 담을 것은 *우리가 방금 쓴 파일 몇 개*뿐이고, 그 바이트·경로를 이미 알고
있다. 그래서 git 이 만들 오브젝트를 **바이트 단위로 똑같이** 만들어 쓰면 프로세스
0개다 (이 머신에서 3~6ms).

무엇을 git 과 똑같이 하나 (하나라도 다르면 다음 `git` 호출이 이상하게 본다)
-------------------------------------------------------------------------
* **오브젝트** — `blob`·`tree`·`commit` 을 loose 로 쓴다 (`zlib(header + body)`,
  `objects/xx/yyyy…`). sha 는 git 과 같은 `sha1("<type> <len>\\0" + body)`.
  tree 항목 정렬은 git 규약(디렉토리는 이름 뒤에 `/` 를 붙인 것으로 비교)이다.
* **인덱스** — v2 를 읽어 우리 경로의 항목을 넣거나 갈아 끼우고 다시 쓴다
  (stat 정보 포함 — 그래야 `git status` 가 파일을 다시 해시하지 않는다). 선택
  확장(`TREE`·`UNTR`… 대문자로 시작하는 것)은 **버린다** — git 은 다음에 필요하면
  다시 만든다. 소문자(필수) 확장이나 v3/v4 를 만나면 **하지 않는다**(호출자가 git
  으로 되돌아간다). `git status` 가 깨끗하고 `git fsck` 가 조용한 것이 판정 기준이다.
* **ref** — `refs/heads/<branch>` 를 `<ref>.lock` 배타 생성 → rename 으로 쓰되, 락
  안에서 현재 값이 우리가 본 base 와 같은지 **다시 읽어** 확인한다(compare-and-swap).
  다르면 손대지 않는다.
* **reflog** — `logs/HEAD`·`logs/refs/heads/<branch>` 에 git 과 같은 한 줄을 붙인다
  (`core.logAllRefUpdates` 가 켜진 비-bare 레포의 기본 동작).

무엇을 하지 **않나**
-----------------
* `git add -A -- records participants` 가 하던 **작업 사본 스캔**을 하지 않는다 —
  우리가 쓴 경로만 스테이징한다. 사람이 클론 안에 손으로 만든 파일은 이 경로로
  커밋되지 않는다(그건 예전에도 우연히 실려 나가던 것이고, 통합 경로의
  `_absorb_worktree` 가 여전히 `git add -A` 로 거둔다).
* 훅을 돌리지 않는다. 채널 클론에는 훅이 없다.
* 서명(`commit.gpgsign`)을 하지 않는다 — git 경로도 `-c commit.gpgsign=false` 였다.

⚠️ **모르면 하지 않는다.** HEAD 가 우리 브랜치의 symref 가 아니거나, 인덱스 형식을
모르거나, ref 가 그 사이에 움직였으면 `Unsupported` 를 올리고 호출자는 예전
그대로 git 으로 커밋한다. 느릴 뿐 틀리지 않는다.
"""

from __future__ import annotations

import hashlib
import os
import struct
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from . import localrefs

INDEX_SIGNATURE = b"DIRC"
INDEX_VERSION = 2
MODE_FILE = 0o100644
MODE_TREE = 0o040000
_FLAG_EXTENDED = 0x4000
_FLAG_STAGE_MASK = 0x3000
_NAME_MASK = 0x0FFF
_O_BINARY = getattr(os, "O_BINARY", 0)


class Unsupported(Exception):
    """이 클론은 이 경로로 커밋할 수 없다 — git 으로 되돌아가라 (오류가 아니다)."""


# ------------------------------------------------------------------ 오브젝트

def object_sha(kind: str, body: bytes) -> str:
    h = hashlib.sha1()
    h.update(f"{kind} {len(body)}\0".encode("ascii"))
    h.update(body)
    return h.hexdigest()


def write_object(gitdir: Path, kind: str, body: bytes) -> str:
    """loose 오브젝트를 쓴다 (이미 있으면 건드리지 않는다). sha 를 돌려준다."""
    sha = object_sha(kind, body)
    target = gitdir / "objects" / sha[:2] / sha[2:]
    if target.exists():
        return sha
    target.parent.mkdir(parents=True, exist_ok=True)
    data = zlib.compress(f"{kind} {len(body)}\0".encode("ascii") + body)
    tmp = target.parent / f"tmp_obj_{os.getpid()}_{sha[2:10]}"
    with open(tmp, "wb") as f:
        f.write(data)
    try:
        os.replace(tmp, target)
    except OSError:
        # 남이 같은 오브젝트를 먼저 썼다 — 내용이 같으므로 우리 것을 버린다.
        try:
            tmp.unlink()
        except OSError:
            pass
        if not target.exists():
            raise
    return sha


def read_object(gitdir: Path, sha: str) -> tuple[str, bytes] | None:
    """loose 오브젝트 하나. 없으면(=팩에 있을 수 있다) None."""
    path = gitdir / "objects" / sha[:2] / sha[2:]
    try:
        raw = zlib.decompress(path.read_bytes())
    except (OSError, zlib.error):
        return None
    header, _, body = raw.partition(b"\0")
    kind, _, _ = header.decode("ascii", "replace").partition(" ")
    return kind, body


def _tree_sort_key(name: bytes, mode: int) -> bytes:
    return name + (b"/" if (mode & 0o170000) == MODE_TREE else b"")


def tree_body(entries: Iterable[tuple[bytes, int, str]]) -> bytes:
    """`(이름, 모드, sha)` 들 → tree 오브젝트 본문 (git 정렬 규약)."""
    out = bytearray()
    for name, mode, sha in sorted(entries, key=lambda e: _tree_sort_key(e[0], e[1])):
        out += f"{mode:o} ".encode("ascii") + name + b"\0" + bytes.fromhex(sha)
    return bytes(out)


def parse_tree(body: bytes) -> list[tuple[bytes, int, str]]:
    out = []
    i = 0
    while i < len(body):
        sp = body.index(b" ", i)
        nul = body.index(b"\0", sp)
        mode = int(body[i:sp], 8)
        name = body[sp + 1:nul]
        sha = body[nul + 1:nul + 21].hex()
        out.append((name, mode, sha))
        i = nul + 21
    return out


def _tz_suffix(ts: float) -> str:
    lt = time.localtime(ts)
    off = lt.tm_gmtoff
    sign = "+" if off >= 0 else "-"
    off = abs(off) // 60
    return f"{sign}{off // 60:02d}{off % 60:02d}"


@dataclass(frozen=True)
class Identity:
    name: str
    email: str

    def line(self, ts: int, tz: str) -> str:
        return f"{self.name} <{self.email}> {ts} {tz}"


def identity_from_env(default_name: str, default_email: str, role: str) -> Identity:
    """git 과 같은 우선순위 — `GIT_<ROLE>_NAME/EMAIL` 환경변수가 설정을 이긴다."""
    name = os.environ.get(f"GIT_{role}_NAME") or default_name
    email = os.environ.get(f"GIT_{role}_EMAIL") or default_email
    return Identity(name, email)


def commit_body(tree: str, parents: Sequence[str], author: Identity, committer: Identity,
                message: str, now: float | None = None) -> bytes:
    ts = now if now is not None else time.time()
    tz = _tz_suffix(ts)
    lines = [f"tree {tree}"]
    lines += [f"parent {p}" for p in parents]
    lines.append(f"author {author.line(int(ts), tz)}")
    lines.append(f"committer {committer.line(int(ts), tz)}")
    msg = message.rstrip("\n") + "\n"
    return ("\n".join(lines) + "\n\n" + msg).encode("utf-8")


# -------------------------------------------------------------------- 인덱스

@dataclass
class IndexEntry:
    ctime_ns: int
    mtime_ns: int
    dev: int
    ino: int
    mode: int
    uid: int
    gid: int
    size: int
    sha: str
    flags: int
    name: bytes

    @property
    def stage(self) -> int:
        return (self.flags & _FLAG_STAGE_MASK) >> 12


@dataclass
class Index:
    entries: list[IndexEntry]

    def find(self, name: bytes) -> IndexEntry | None:
        for e in self.entries:
            if e.name == name:
                return e
        return None


def _split_ns(ns: int) -> tuple[int, int]:
    sec, rem = divmod(ns, 1_000_000_000)
    return sec & 0xFFFFFFFF, rem


def read_index(path: Path) -> Index:
    """`.git/index` v2. 모르는 것을 만나면 `Unsupported`."""
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return Index([])
    if len(raw) < 32 or raw[:4] != INDEX_SIGNATURE:
        raise Unsupported("인덱스 서명이 다르다")
    version, count = struct.unpack(">II", raw[4:12])
    if version != INDEX_VERSION:
        raise Unsupported(f"인덱스 버전 {version} 은 다루지 않는다 (v2 만)")
    if hashlib.sha1(raw[:-20]).digest() != raw[-20:]:
        raise Unsupported("인덱스 체크섬이 맞지 않는다")
    off = 12
    entries: list[IndexEntry] = []
    for _ in range(count):
        if off + 62 > len(raw):
            raise Unsupported("인덱스가 잘렸다")
        (cs, cns, ms, mns, dev, ino, mode, uid, gid, size) = struct.unpack(">10I", raw[off:off + 40])
        sha = raw[off + 40:off + 60].hex()
        (flags,) = struct.unpack(">H", raw[off + 60:off + 62])
        if flags & _FLAG_EXTENDED:
            raise Unsupported("확장 플래그(v3) 항목이 있다")
        name_len = flags & _NAME_MASK
        start = off + 62
        if name_len < _NAME_MASK:
            name = raw[start:start + name_len]
        else:
            end = raw.index(b"\0", start)
            name = raw[start:end]
        entry_len = 62 + len(name)
        off += entry_len + (8 - entry_len % 8)      # NUL 패딩 (최소 1)
        entries.append(IndexEntry(cs * 1_000_000_000 + cns, ms * 1_000_000_000 + mns,
                                  dev, ino, mode, uid, gid, size, sha, flags, name))
    # 확장 — 대문자로 시작하면 선택(버려도 된다), 소문자면 필수(모르면 안 된다).
    end = len(raw) - 20
    while off < end:
        if off + 8 > end:
            raise Unsupported("확장 헤더가 잘렸다")
        ext_name = raw[off:off + 4]
        (ext_len,) = struct.unpack(">I", raw[off + 4:off + 8])
        if not (65 <= ext_name[0] <= 90):
            raise Unsupported(f"필수 확장 {ext_name!r} 을 모른다")
        off += 8 + ext_len
    return Index(entries)


def write_index(path: Path, entries: Sequence[IndexEntry]) -> None:
    """v2 인덱스를 **락 규약대로** 쓴다 (`index.lock` 배타 생성 → rename). 확장은 없다."""
    ordered = sorted(entries, key=lambda e: (e.name, e.stage))
    buf = bytearray(struct.pack(">4sII", INDEX_SIGNATURE, INDEX_VERSION, len(ordered)))
    for e in ordered:
        cs, cns = _split_ns(e.ctime_ns)
        ms, mns = _split_ns(e.mtime_ns)
        flags = (e.flags & ~_NAME_MASK) | min(len(e.name), _NAME_MASK)
        buf += struct.pack(">10I", cs, cns, ms, mns, e.dev & 0xFFFFFFFF, e.ino & 0xFFFFFFFF,
                           e.mode, e.uid & 0xFFFFFFFF, e.gid & 0xFFFFFFFF, e.size & 0xFFFFFFFF)
        buf += bytes.fromhex(e.sha)
        buf += struct.pack(">H", flags)
        buf += e.name
        entry_len = 62 + len(e.name)
        buf += b"\0" * (8 - entry_len % 8)
    buf += hashlib.sha1(buf).digest()
    lock = path.with_name(path.name + ".lock")
    try:
        fd = os.open(str(lock), os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_BINARY, 0o666)
    except OSError as exc:
        raise Unsupported(f"인덱스 락을 얻지 못했다: {exc}") from exc
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(buf)
        os.replace(lock, path)
    except BaseException:
        try:
            lock.unlink()
        except OSError:
            pass
        raise


def stat_entry(root: Path, rel: str, sha: str, mode: int = MODE_FILE) -> IndexEntry:
    """작업 사본 파일의 stat 으로 인덱스 항목을 만든다 (git 이 `add` 뒤에 적는 것과 같은 자리)."""
    st = os.lstat(root / rel)
    ctime = getattr(st, "st_birthtime_ns", None) if os.name == "nt" else None
    if ctime is None:
        ctime = st.st_ctime_ns
    # dev/ino/uid/gid: Git for Windows 는 전부 0 을 적는다. 다른 OS 에서는 lstat 값.
    win = os.name == "nt"
    return IndexEntry(
        ctime_ns=ctime, mtime_ns=st.st_mtime_ns,
        dev=0 if win else st.st_dev, ino=0 if win else st.st_ino,
        mode=mode, uid=0 if win else st.st_uid, gid=0 if win else st.st_gid,
        size=st.st_size, sha=sha, flags=min(len(rel.encode()), _NAME_MASK),
        name=rel.encode("utf-8"),
    )


# ---------------------------------------------------------------------- 트리

def build_trees(entries: Sequence[IndexEntry]) -> tuple[str, dict[str, bytes]]:
    """인덱스 항목 전체 → (루트 tree sha, {tree sha: 본문}). `git write-tree` 와 같다.

    stage 가 0 이 아닌 항목(충돌)이 있으면 `Unsupported`.
    """
    root: dict = {}
    for e in entries:
        if e.stage != 0:
            raise Unsupported("병합 충돌 항목이 인덱스에 있다")
        parts = e.name.split(b"/")
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})
            if not isinstance(node, dict):
                raise Unsupported("경로가 파일과 디렉토리로 겹친다")
        node[parts[-1]] = (e.mode, e.sha)
    bodies: dict[str, bytes] = {}

    def emit(node: dict) -> str:
        items = []
        for name, value in node.items():
            if isinstance(value, dict):
                items.append((name, MODE_TREE, emit(value)))
            else:
                items.append((name, value[0], value[1]))
        body = tree_body(items)
        sha = object_sha("tree", body)
        bodies[sha] = body
        return sha

    return emit(root), bodies


def subtree_entries(entries: Sequence[IndexEntry], prefix: str) -> list[tuple[str, str]]:
    """`prefix/` 바로 아래 blob 들 → [(이름, sha)]."""
    p = (prefix.rstrip("/") + "/").encode()
    out = []
    for e in entries:
        if e.name.startswith(p) and b"/" not in e.name[len(p):]:
            out.append((e.name[len(p):].decode("utf-8", "replace"), e.sha))
    return out


# ----------------------------------------------------------------------- ref

def head_branch_ref(gitdir: Path) -> str | None:
    """HEAD 가 `ref: refs/heads/…` 이면 그 이름, 아니면(분리 HEAD 등) None."""
    try:
        text = (gitdir / "HEAD").read_text("utf-8").strip()
    except OSError:
        return None
    if text.startswith("ref:"):
        name = text[4:].strip()
        return name if name.startswith("refs/heads/") else None
    return None


def update_ref_cas(clone_dir: Path, name: str, old: str, new: str) -> bool:
    """`name` 을 `old` → `new` 로 옮긴다. 현재 값이 `old` 가 아니면 **손대지 않고** False.

    `localrefs.write_ref` 와 같은 락 규약이되, 락을 쥔 채 현재 값을 다시 읽는다 —
    push·통합 같은 다른 전이가 그 사이 ref 를 움직였을 가능성을 닫는다.
    """
    gd = localrefs.gitdir(clone_dir)
    if gd is None or not (name.startswith("refs/") and ".." not in name):
        return False
    target = gd / name
    lock = gd / (name + ".lock")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        # O_BINARY: Windows 텍스트 모드가 LF 를 CRLF 로 바꾸는 것을 막는다 (`localrefs.write_ref` 와 같다).
        fd = os.open(str(lock), os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_BINARY, 0o666)
    except OSError:
        return False
    try:
        known, cur = localrefs.ref_sha(clone_dir, name)
        if not known or cur != old:
            raise Unsupported(f"{name} 이 그 사이 움직였다 ({(cur or '없음')[:12]} ≠ {old[:12]})")
        os.write(fd, (new + "\n").encode("ascii"))
        os.close(fd)
        fd = -1
        os.replace(str(lock), str(target))
        return True
    except Unsupported:
        raise
    except OSError:
        return False
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            lock.unlink()
        except OSError:
            pass


def append_reflog(gitdir: Path, name: str, old: str, new: str, who: Identity,
                  ts: int, tz: str, message: str) -> None:
    """`logs/<name>` 에 git 과 같은 한 줄. `logs/` 자체가 없으면(로그 끈 레포) 쓰지 않는다."""
    logs = gitdir / "logs"
    if not logs.is_dir():
        return
    path = logs / name
    path.parent.mkdir(parents=True, exist_ok=True)
    line = f"{old} {new} {who.line(ts, tz)}\t{message}\n"
    with open(path, "ab") as f:
        f.write(line.encode("utf-8"))


# ------------------------------------------------------------------- 한 커밋

@dataclass(frozen=True)
class Committed:
    sha: str
    tree: str
    participants: tuple[tuple[str, str], ...]
    """커밋된 `participants/` 바로 아래 (파일 이름, blob sha) — 상태 캐시 예열용."""


def commit_paths(
    clone_dir: Path,
    branch: str,
    base: str,
    files: Mapping[str, bytes],
    message: str,
    author: Identity,
    committer: Identity,
    *,
    now: float | None = None,
) -> Committed:
    """`files`(상대 경로 → 바이트)를 `base` 위에 한 커밋으로 얹는다. 프로세스 0개.

    순서: 오브젝트 → 인덱스 → ref(+reflog). 중간에 죽어도 인덱스가 앞서 있을 뿐이고
    (스테이징된 상태) 그것은 다음 `git commit` 이 그대로 거둔다.
    """
    gd = localrefs.gitdir(clone_dir)
    if gd is None:
        raise Unsupported(".git 이 디렉토리가 아니거나 reftable 이다")
    want = f"refs/heads/{branch}"
    if head_branch_ref(gd) != want:
        raise Unsupported(f"HEAD 가 {want} 를 가리키지 않는다")
    if not files:
        raise Unsupported("커밋할 파일이 없다")
    index = read_index(gd / "index")
    by_name = {e.name: i for i, e in enumerate(index.entries)}
    entries = list(index.entries)
    for rel, data in files.items():
        sha = write_object(gd, "blob", data)
        entry = stat_entry(clone_dir, rel, sha)
        i = by_name.get(entry.name)
        if i is None:
            entries.append(entry)
        else:
            entries[i] = entry
    root_sha, bodies = build_trees(entries)
    for sha, body in bodies.items():
        write_object(gd, "tree", body)
    ts = now if now is not None else time.time()
    body = commit_body(root_sha, [base], author, committer, message, now=ts)
    commit_sha = write_object(gd, "commit", body)
    write_index(gd / "index", entries)
    if not update_ref_cas(clone_dir, want, base, commit_sha):
        raise Unsupported(f"{want} 를 쓸 수 없다 (락 경합)")
    tz = _tz_suffix(ts)
    subject = message.splitlines()[0] if message.strip() else ""
    append_reflog(gd, "HEAD", base, commit_sha, committer, int(ts), tz, f"commit: {subject}")
    append_reflog(gd, want, base, commit_sha, committer, int(ts), tz, f"commit: {subject}")
    return Committed(commit_sha, root_sha, tuple(subtree_entries(entries, "participants")))
