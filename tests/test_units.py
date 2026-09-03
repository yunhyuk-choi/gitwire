"""네트워크·git 없이 도는 단위 테스트.

모든 외부 의존(git 실행·HTTP)은 주입점으로 대체된다.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from gitwire import clock, credentials, gitcmd, layout, records
from gitwire.cursor import Cursor, CursorStore
from gitwire.errors import AuthError, GitError, PushRejected


# --------------------------------------------------------------- records


def test_record_id_sorts_chronologically():
    """고정폭 타임스탬프 → 경로 사전식 정렬 = 시간순 정렬."""
    t1 = datetime(2026, 9, 3, 10, 0, 0, 1000, tzinfo=timezone.utc)
    t2 = datetime(2026, 9, 3, 10, 0, 0, 2000, tzinfo=timezone.utc)
    t3 = datetime(2026, 9, 4, 0, 0, 0, 0, tzinfo=timezone.utc)
    ids = [
        records.make_record_id(t3, "zeta", "aaaaaa"),
        records.make_record_id(t1, "zeta", "ffffff"),
        records.make_record_id(t2, "alpha", "000000"),
    ]
    assert sorted(ids) == [ids[1], ids[2], ids[0]]


def test_record_id_is_one_file_per_record():
    """레코드마다 다른 파일 → 동시 발행해도 내용 머지 충돌이 없다."""
    t = datetime(2026, 9, 3, 10, 0, 0, tzinfo=timezone.utc)
    made = {records.make_record_id(t, "alice") for _ in range(200)}
    assert len(made) == 200
    assert all(m.startswith("records/20260903/") for m in made)


def test_sender_slug_is_filename_safe():
    assert records.slug_sender("alice/bob") == "alice_bob"
    assert records.slug_sender("a-b") == "a_b"          # '-' 는 구분자라 제거
    assert records.slug_sender("  ") == "anon"
    assert len(records.slug_sender("x" * 100)) <= 24


def test_envelope_roundtrip_keeps_payload_opaque():
    t = datetime(2026, 9, 3, 10, 0, 0, 123000, tzinfo=timezone.utc)
    rid = records.make_record_id(t, "alice", "abc123")
    payload = {"anything": [1, {"nested": "값"}], "null": None}
    raw = records.encode(rid, "alice", t, payload)
    assert raw.endswith(b"\n")
    assert b"\r" not in raw                    # LF 고정
    assert not raw.startswith(b"\xef\xbb\xbf")  # BOM 없음
    rec = records.decode(raw, rid)
    assert rec.payload == payload
    assert rec.sender == "alice"
    assert rec.timestamp == t


def test_decode_rejects_non_envelope():
    with pytest.raises(records.RecordDecodeError):
        records.decode(b'{"no": "payload"}', "records/x.json")
    with pytest.raises(records.RecordDecodeError):
        records.decode(b"not json", "records/x.json")


# ---------------------------------------------------------------- layout


def test_normalize_url_makes_same_repo_one_channel():
    a = layout.normalize_repo_url("https://github.com/me/room.git")
    b = layout.normalize_repo_url("https://GitHub.com/me/room/")
    c = layout.normalize_repo_url("https://user:tok@github.com/me/room.git")
    assert a == b == c
    assert "tok" not in c  # 자격증명은 채널 식별자에 들어가지 않는다


def test_scp_style_url_normalizes():
    assert layout.normalize_repo_url("git@github.com:me/room.git") == (
        "ssh://github.com/me/room"
    )


def test_channel_dir_is_deterministic(tmp_path):
    """같은 URL 이면 어느 프로세스에서 열든 같은 클론·같은 커서를 가리킨다."""
    d1 = layout.channel_dir("https://github.com/me/room.git", tmp_path)
    d2 = layout.channel_dir("https://github.com/me/room/", tmp_path)
    assert d1 == d2
    assert d1.parent == tmp_path / "channels"
    assert d1.name.startswith("room-")

    other = layout.channel_dir("https://github.com/me/other.git", tmp_path)
    assert other != d1


def test_gitwire_home_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("GITWIRE_HOME", str(tmp_path / "custom"))
    assert layout.gitwire_home() == tmp_path / "custom"


def test_skeleton_is_utf8_lf():
    for name, data in layout.repo_skeleton("방", "2026-09-03T00:00:00Z").items():
        assert b"\r\n" not in data, name
        assert not data.startswith(b"\xef\xbb\xbf"), name
        data.decode("utf-8")


# ----------------------------------------------------------------- clock


def test_http_date_offset_compensates_truncation_and_rtt():
    """Date 는 초 단위 절삭 → +0.5초 보정, RTT 는 중점으로 상쇄."""
    # 로컬 t0=1000.0, t1=1000.4 (RTT 0.4초), 서버 Date=1010 (초 절삭)
    def probe(url, timeout):
        return (1000.0, 1010.0, 1000.4)

    c = clock.HttpDateClock("https://example.invalid/", probe=probe)
    off = c.refresh(force=True)
    assert off == pytest.approx(1010.5 - 1000.2)
    assert c.synced is True


def test_clock_refresh_is_periodic():
    calls = []

    def probe(url, timeout):
        calls.append(1)
        return (0.0, 10.0, 0.0)

    fake_time = [0.0]
    c = clock.HttpDateClock(
        "https://example.invalid/", probe=probe, refresh_interval=900.0,
        monotonic=lambda: fake_time[0],
    )
    c.refresh()
    assert len(calls) == 1
    c.refresh()                      # 주기 안 → 재측정 안 함
    assert len(calls) == 1
    fake_time[0] = 901.0             # 주기 지남 → 드리프트 흡수
    c.refresh()
    assert len(calls) == 2


def test_clock_failure_degrades_to_local_time():
    """시계 보정 실패로 전송이 멈추면 안 된다."""
    def probe(url, timeout):
        raise OSError("네트워크 없음")

    c = clock.HttpDateClock("https://example.invalid/", probe=probe)
    assert c.refresh(force=True) is None
    assert c.offset == 0.0
    assert c.last_error is not None
    assert c.now().tzinfo is timezone.utc


def test_clock_base_url_strips_credentials():
    assert clock.clock_base_url("https://u:p@github.com/me/x.git") == (
        "https://github.com/"
    )
    assert clock.clock_base_url("git@github.com:me/x.git") is None
    assert clock.clock_base_url("C:/tmp/bare.git") is None


# ----------------------------------------------------------- credentials


def test_token_never_appears_in_repr_or_str():
    cred = credentials.TokenCredential("super-secret-token-value")
    assert "super-secret" not in repr(cred)
    assert "super-secret" not in str(cred)
    assert "super-secret" not in f"{cred}"


def test_askpass_helper_does_not_contain_the_token(tmp_path):
    cred = credentials.TokenCredential("super-secret-token-value")
    env = cred.env(tmp_path)
    helper = Path(env["GIT_ASKPASS"])
    assert helper.exists()
    for f in tmp_path.rglob("*"):
        if f.is_file():
            assert b"super-secret" not in f.read_bytes(), f
    # 토큰은 환경변수로만 전달된다
    assert env["GITWIRE_TOKEN"] == "super-secret-token-value"
    assert env["GIT_TERMINAL_PROMPT"] == "0"


def test_missing_credential_fails_fast_not_prompt(monkeypatch):
    """헤드리스에서 되묻지 않고 즉시 실패한다."""
    monkeypatch.delenv("NOPE_TOKEN", raising=False)
    with pytest.raises(AuthError):
        credentials.TokenCredential.from_env("NOPE_TOKEN")
    with pytest.raises(AuthError):
        credentials.TokenCredential.from_file("/definitely/not/here")
    with pytest.raises(AuthError):
        credentials.TokenCredential("")


def test_per_channel_credentials_are_independent(tmp_path):
    """채널마다 다른 토큰 — 전역 하나가 아니다."""
    a = credentials.TokenCredential("token-aaaa", username="orgA")
    b = credentials.TokenCredential("token-bbbb", username="orgB")
    ea = a.env(tmp_path / "a")
    eb = b.env(tmp_path / "b")
    assert ea["GITWIRE_TOKEN"] != eb["GITWIRE_TOKEN"]
    assert ea["GITWIRE_USERNAME"] == "orgA"
    assert eb["GITWIRE_USERNAME"] == "orgB"


# ---------------------------------------------------------------- gitcmd


class FakeRunner:
    """네트워크·git 없이 응답을 흉내내는 주입용 실행기."""

    def __init__(self, result):
        self.result = result
        self.calls = []

    def run(self, args, *, cwd=None, env=None, timeout=None):
        self.calls.append(list(args))
        return self.result


def test_secrets_are_redacted_from_output_and_exceptions():
    secret = "ghp_supersecrettoken"
    runner = FakeRunner(gitcmd.GitResult(1, f"out {secret}", f"fatal: {secret} bad"))
    g = gitcmd.Git(runner, Path("."), secrets=(secret,))
    with pytest.raises(GitError) as ei:
        g.run("push", "origin", "main")
    assert secret not in str(ei.value)
    assert "***" in str(ei.value)

    res = g.run("status", check=False)
    assert secret not in res.stdout
    assert secret not in res.stderr


def test_url_embedded_credentials_are_redacted():
    out = gitcmd.redact("https://user:tok123@github.com/x.git 실패", ())
    assert "tok123" not in out
    assert "***:***@github.com" in out


def test_push_rejection_is_a_distinct_exception():
    runner = FakeRunner(
        gitcmd.GitResult(1, "", "! [rejected] main -> main (non-fast-forward)")
    )
    g = gitcmd.Git(runner, Path("."))
    with pytest.raises(PushRejected):
        g.run("push", "origin", "main")


def test_auth_failure_is_a_distinct_exception():
    runner = FakeRunner(gitcmd.GitResult(128, "", "fatal: Authentication failed"))
    g = gitcmd.Git(runner, Path("."))
    with pytest.raises(AuthError):
        g.run("fetch", "origin")


def test_git_calls_never_prompt():
    runner = FakeRunner(gitcmd.GitResult(0, "", ""))
    g = gitcmd.Git(runner, Path("."))
    g.run("fetch")
    assert runner.calls[0][:2] == ["-c", "core.autocrlf=false"]


# ---------------------------------------------------------------- cursor


def test_cursor_persists_across_instances(tmp_path):
    """일회성 호출을 반복해도 이어지려면 커서가 디스크에 있어야 한다."""
    s1 = CursorStore(tmp_path, "agent")
    s1.save(Cursor(commit="abc", batch_head="def", batch_pos=3,
                   watermark="records/x", started=True))

    s2 = CursorStore(tmp_path, "agent")   # 새 인스턴스 = 새 프로세스를 흉내
    cur = s2.load()
    assert (cur.commit, cur.batch_head, cur.batch_pos) == ("abc", "def", 3)
    assert cur.watermark == "records/x"
    assert cur.started is True


def test_cursor_is_per_consumer(tmp_path):
    CursorStore(tmp_path, "webapp").save(Cursor(commit="a"))
    assert CursorStore(tmp_path, "agent").load().commit is None


def test_cursor_missing_file_is_fresh(tmp_path):
    cur = CursorStore(tmp_path, "new").load()
    assert cur.commit is None and cur.batch_pos == 0 and cur.started is False


def test_cursor_write_is_atomic(tmp_path):
    store = CursorStore(tmp_path, "x")
    store.save(Cursor(commit="a"))
    store.save(Cursor(commit="b"))
    leftovers = list(store.path.parent.glob("*.tmp"))
    assert leftovers == []
    assert store.load().commit == "b"


def test_consumer_name_is_sanitized(tmp_path):
    store = CursorStore(tmp_path, "../../evil")
    assert ".." not in store.consumer
    assert store.path.parent == tmp_path / "cursors"
