"""네트워크·git 없이 도는 단위 테스트.

모든 외부 의존(git 실행·HTTP)은 주입점으로 대체된다.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gitwire import (
    clock, credentials, gitcmd, identity, layout, localrefs, records, rollup,
)
from gitwire.treecache import TreeCache
from gitwire.cursor import Cursor, CursorStore
from gitwire.errors import AuthError, GitError, PushRejected

from conftest import NO_WINDOW


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


def test_is_record_id_accepts_what_this_channel_makes():
    """⭐ 형식 판정의 정본이 여기다 — 만든 것을 그대로 받아들여야 한다."""
    t = datetime(2026, 9, 3, 10, 0, 0, 123000, tzinfo=timezone.utc)
    for sender in ("alice", "yh.choi@interxlab.com.c587c2", "alice+tag"):
        assert records.is_record_id(records.make_record_id(t, sender))
    # 롤업으로 저장 위치가 archive/ 로 옮겨져도 id 는 안 바뀐다 → 그대로 참이다.
    assert records.is_record_id(records.make_record_id(t, "alice", "abc123"))


def test_is_record_id_rejects_anything_that_is_not_one():
    """⭐ 커서에 id 아닌 값이 들어가는 문을 여기서 닫는다.

    실측된 사고: 소비자가 화면의 낙관적 임시 ID(`~pending/…`)를 읽음 커서로
    저장해 원격까지 올렸다. `~` 가 `records/` 보다 사전식으로 **뒤**라 그 값이
    항상 최대값이 되고, 커서가 단조 증가라 실제 id 로 되돌아갈 수 없었다.
    """
    for bad in (
        "~pending/000001",                      # ⭐ 실제로 저장돼 있던 값
        "", None, 123, "records/", "?",
        "archive/20260903.jsonl",                # 아카이브 **파일**은 id 가 아니다
        "participants/alice@x.io.json",          # 예약 경로도 아니다
        "records/20260903/notatimestamp-a-abc123.json",
        "records/20260904/20260903T100000123Z-a-abc123.json",  # 날짜 칸이 어긋난다
        "records/20260903/20260903T100000123Z-a-abc123.txt",
        "records/20260903/sub/20260903T100000123Z-a-abc123.json",
    ):
        assert not records.is_record_id(bad), bad


def test_rollup_line_id_uses_the_same_judgement():
    """롤업의 id 추출이 형식 판정을 손으로 다시 세지 않는다 (두 곳이 어긋나지 않게)."""
    t = datetime(2026, 9, 3, 10, 0, 0, 123000, tzinfo=timezone.utc)
    rid = records.make_record_id(t, "alice", "abc123")
    line = records.encode(rid, "alice", t, {"x": 1}).decode("utf-8").strip()
    assert rollup.line_id(line) == rid
    # 빠른 정규식 경로가 형식을 만족하지 않는 값을 **id 로 채택하지 않는다** —
    # 그 판단이 `is_record_id` 하나에서 나온다는 것이 이 검증의 요지다.
    assert not records.is_record_id("~pending/000001")


def test_sender_slug_is_filename_safe():
    assert records.slug_sender("alice/bob") == "alice_bob"
    assert records.slug_sender("a-b") == "a_b"          # '-' 는 구분자라 제거
    assert records.slug_sender("  ") == "anon"
    assert len(records.slug_sender("x" * 100)) <= records.MAX_SENDER_LEN
    # 설치본 식별자는 `<git 이메일>.<난수>` 라 '@' 가 살아남아야 한다. 잘리면
    # **난수 접미까지 함께 잘려** 두 설치본이 같은 슬러그가 될 수 있다.
    assert records.slug_sender("me@example.com.a3f9c1") == "me@example.com.a3f9c1"


# -------------------------------------------------------------- identity


class _StubRunner:
    """git 을 부르지 않는 실행기 (설치본 식별자 씨앗 주입점)."""

    def __init__(self, stdout: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.calls: list[list[str]] = []

    def run(self, args, *, cwd=None, env=None, timeout=None):
        self.calls.append(list(args))
        return gitcmd.GitResult(self.returncode, self.stdout, "")


def test_installation_id_is_seeded_by_git_email(tmp_path):
    runner = _StubRunner("  yh.choi@example.com  ")   # 앞뒤 공백은 다듬어진다
    value = identity.installation_id(tmp_path / "home", runner=runner)
    assert runner.calls == [["config", "--get", "user.email"]]
    assert value.startswith("yh.choi@example.com.")   # 사람이 알아볼 수 있다
    assert len(value) <= records.MAX_SENDER_LEN
    # 파일명 슬러그를 통과해도 값이 그대로다 (= 봉투와 파일명이 어긋나지 않는다)
    assert records.slug_sender(value) == value


def test_installation_id_falls_back_without_git_identity(tmp_path):
    """git 전역 설정이 없는 환경(CI·컨테이너)도 정상 경로다."""
    value = identity.installation_id(tmp_path / "home", runner=_StubRunner("", 1))
    assert value.rsplit(".", 1)[0] == records.slug_sender(identity.local_seed())
    assert len(value.rsplit(".", 1)[1]) == 6


def test_installation_id_persists_and_differs_per_installation(tmp_path):
    """⭐ A 의 핵심: 재시작해도 유지되고, 같은 머신의 두 설치본은 갈린다."""
    runner = _StubRunner("me@example.com")
    first = identity.installation_id(tmp_path / "A", runner=runner)
    again = identity.installation_id(tmp_path / "A", runner=runner)   # 재시작 흉내
    other = identity.installation_id(tmp_path / "B", runner=runner)

    assert first == again, "재시작하면 신원이 바뀐다"
    assert first != other, "같은 머신의 두 설치본이 같은 신원을 갖는다"
    marker = tmp_path / "A" / identity.INSTALLATION_FILE
    raw = marker.read_bytes()
    assert raw.decode("utf-8").strip() == first
    assert bytes([13, 10]) not in raw                       # LF 고정
    assert not raw.startswith(bytes([0xEF, 0xBB, 0xBF]))    # BOM 없음


def test_installation_id_survives_unwritable_home(tmp_path, monkeypatch):
    """저장 못 하는 환경에서도 죽지 않는다 (그 프로세스 한정 값으로 degraded)."""
    def boom(*args, **kwargs):
        raise OSError("read-only")

    monkeypatch.setattr(identity, "open", boom, raising=False)
    value = identity.installation_id(tmp_path / "ro", runner=_StubRunner("", 1))
    assert value and records.slug_sender(value) == value


def test_explicit_sender_env_wins(tmp_path, monkeypatch):
    """하위호환 — 명시적으로 준 값은 그대로 존중한다."""
    monkeypatch.setenv("GITWIRE_SENDER", "고정-이름")
    assert identity.default_sender(tmp_path) == records.slug_sender("고정-이름")
    assert not (tmp_path / identity.INSTALLATION_FILE).exists()


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
    # ⚠️ 앞에 자격증명 설정이 붙을 수 있다(네트워크 호출) — BASE_CONFIG 는
    # 그 뒤에 **언제나** 온다.
    assert "core.autocrlf=false" in runner.calls[0]
    assert runner.calls[0][-1] == "fetch"


# ------------------------------------------ 자격증명 조회를 싸게 (네트워크만)


def test_credential_config_resets_the_multivalued_list(monkeypatch):
    """⭐ `credential.helper` 는 다중값이다 — **빈 값으로 초기화한 뒤** 지정해야 한다.

    추가만 하면 기존 사슬(예: 시스템의 GCM)이 그대로 남고, git 은 인증 성공 후
    `store` 를 사슬 전원에게 보내므로 지우려던 비용이 되돌아온다 (실측:
    wincred 단독 1220ms → wincred→manager 1519ms).
    """
    monkeypatch.setenv(gitcmd.HELPER_ENV, "!echo")   # `!` 형태 = 있다고 믿는다
    gitcmd.reset_credential_state()
    args = gitcmd.credential_config("git")
    assert args[:2] == ("-c", "credential.helper="), args
    assert args[2:] == ("-c", "credential.helper=!echo"), args


def test_credential_config_is_only_for_network_commands(monkeypatch):
    """로컬 git 호출의 동작은 한 글자도 바뀌지 않는다."""
    monkeypatch.setenv(gitcmd.HELPER_ENV, "!echo")
    gitcmd.reset_credential_state()
    runner = FakeRunner(gitcmd.GitResult(0, "", ""))
    g = gitcmd.Git(runner, Path("."))
    g.run("commit", "-m", "x")
    g.run("push", "origin", "main")
    assert "credential.helper=" not in runner.calls[0], runner.calls[0]
    assert "credential.helper=" in runner.calls[1], runner.calls[1]


def test_missing_helper_falls_back_to_user_config(monkeypatch):
    """⚠️ 없는 헬퍼를 강제하면 **아무것도 얹지 않는다** — 느린 것이 깨지는 것보다 낫다."""
    monkeypatch.setenv(gitcmd.HELPER_ENV, "no-such-helper-xyz")
    gitcmd.reset_credential_state()
    assert gitcmd.credential_config("git") == ()


def test_credential_helper_can_be_turned_off(monkeypatch):
    monkeypatch.setenv(gitcmd.HELPER_ENV, "off")
    gitcmd.reset_credential_state()
    assert gitcmd.credential_config("git") == ()


def test_auth_failure_retries_once_with_the_user_config(monkeypatch):
    """지정한 저장소에 자격증명이 없으면 **사용자 설정으로 한 번 더** 시도한다.

    (자격증명이 GCM 전용 저장소에만 있는 사람이 깨지지 않는 유일한 이유다.)
    그리고 그 프로세스에서는 접는다 — 왕복마다 실패를 두 번 내지 않는다.
    """
    monkeypatch.setenv(gitcmd.HELPER_ENV, "!echo")
    gitcmd.reset_credential_state()

    class Flaky(FakeRunner):
        def run(self, args, *, cwd=None, env=None, timeout=None):
            self.calls.append(list(args))
            if "credential.helper=!echo" in args:
                return gitcmd.GitResult(
                    128, "", "fatal: could not read Username: terminal prompts disabled"
                )
            return gitcmd.GitResult(0, "ok", "")

    runner = Flaky(None)
    g = gitcmd.Git(runner, Path("."))
    assert g.run("push", "origin", "main").returncode == 0
    assert len(runner.calls) == 2
    assert "credential.helper=!echo" in runner.calls[0]
    assert "credential.helper=!echo" not in runner.calls[1]

    # 접혔다 — 다음 호출은 처음부터 사용자 설정이다 (실패를 두 번 내지 않는다)
    g.run("push", "origin", "main")
    assert len(runner.calls) == 3
    assert "credential.helper=!echo" not in runner.calls[2]


# ------------------------------------------------- Windows 콘솔 창 억제


def test_windows_git_is_spawned_without_a_console_window(monkeypatch):
    """⭐ Windows 에서 ``DETACHED_PROCESS`` 가 **실제 호출에** 걸려 있어야 한다.

    이 단언이 없으면 플래그가 빠져도 아무 테스트가 깨지지 않는다 — 콘솔 없는
    프로세스(``pythonw.exe`` 로 띄운 앱·서비스)에서 git 호출마다 빈 창이
    깜빡이는 것으로만 드러난다. 그건 남의 데스크탑에서만 보이는 회귀다.

    **창을 실제로 세지 않는다** — 세려면 창을 띄워야 하고, 그러면 이 테스트가
    돌 때마다 사용자 화면에 창이 깜빡인다. 우리가 통제하는 것은 플래그이므로
    플래그를 단언한다.
    """
    monkeypatch.setattr(gitcmd.os, "name", "nt")
    seen: dict = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, b"ok", b"")

    monkeypatch.setattr(gitcmd.subprocess, "run", fake_run)
    result = gitcmd.SubprocessGitRunner().run(["status"], timeout=5.0)

    expected = getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
    assert seen["kwargs"]["creationflags"] & expected == expected, (
        "git 이 DETACHED_PROCESS 없이 떠 있다 — 콘솔 없는 부모에서 빈 창이 깜빡인다"
    )
    no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    assert seen["kwargs"]["creationflags"] & no_window == 0, (
        "CREATE_NO_WINDOW 는 창만 숨긴 새 콘솔(conhost.exe)을 호출마다 만든다 — "
        "DETACHED_PROCESS 로 콘솔 자체를 주지 않는다 (gitcmd.creation_flags 도크)"
    )
    # 창을 없앤 대신 잃은 것이 없어야 한다.
    assert seen["kwargs"]["capture_output"] is True, "출력 캡처가 이 라이브러리의 근간이다"
    assert seen["kwargs"]["timeout"] == 5.0
    assert seen["kwargs"]["env"]["GIT_TERMINAL_PROMPT"] == "0"
    assert seen["kwargs"]["env"]["GCM_INTERACTIVE"] == "never"
    assert result.stdout == "ok"


def test_non_windows_keeps_flags_at_zero(monkeypatch):
    """macOS·리눅스에는 콘솔 개념이 없다 — 플래그가 0 이라 동작이 그대로다.

    (POSIX 에서 ``creationflags`` 가 0 이 아니면 ``subprocess`` 가 ValueError 를
    던진다. 0 을 넘기는 것은 아무 일도 하지 않는다.)
    """
    for name in ("posix", "java"):
        monkeypatch.setattr(gitcmd.os, "name", name)
        assert gitcmd.creation_flags() == 0


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


# ------------------------------------------------------------- treecache


def test_tree_cache_is_keyed_by_content_address():
    """키가 sha 라 값이 섞이지 않는다 — 무효화 로직이 필요 없는 이유."""
    cache = TreeCache()
    cache.put("tree:aaa", ["1.json", "2.json"])
    cache.put("tree:bbb", ["3.json"])
    assert cache.get("tree:aaa") == ["1.json", "2.json"]
    assert cache.get("tree:bbb") == ["3.json"]
    assert cache.get("tree:ccc") is None          # 모르는 sha 는 미스일 뿐
    info = cache.info()
    assert info["hits"] == 2 and info["misses"] == 1 and info["entries"] == 2


def test_tree_cache_evicts_least_recently_used_by_item_budget():
    """상한 단위는 항목 수가 아니라 담긴 경로 수다 (항목 크기가 들쭉날쭉하다)."""
    cache = TreeCache(max_items=5)
    cache.put("a", ["1", "2", "3"])
    cache.put("b", ["4", "5"])
    assert cache.get("a") is not None             # a 를 최근 사용으로 올린다
    cache.put("c", ["6", "7"])                    # 5 초과 → 가장 오래된 b 가 나간다
    assert cache.get("b") is None
    assert cache.get("a") == ["1", "2", "3"]
    assert cache.get("c") == ["6", "7"]
    assert cache.info()["evictions"] == 1
    assert cache.info()["items"] <= 5


def test_tree_cache_can_be_disabled():
    cache = TreeCache(max_items=0)
    assert cache.put("a", ["1"]) == ["1"]         # 값은 그대로 돌려준다
    assert cache.get("a") is None                 # 담지는 않는다
    assert cache.info()["entries"] == 0


# ------------------------- ref 를 `.git` 에서 직접 읽고 쓴다 (git 프로세스 0개)
#
# ⚠️ 여기서 지켜야 하는 것은 속도가 아니라 **"모른다"를 "없다"로 번역하지 않는
# 것**이다. 그 오역이 곧 조용한 유실이다 (`localrefs.ref_sha` 도크).


def _repo(path: Path) -> None:
    for args in (
        ["init", "-q", "-b", "main", "."],
        ["config", "user.email", "t@localhost"],
        ["config", "user.name", "t"],
    ):
        subprocess.run(["git", *args], cwd=str(path), check=True, capture_output=True, **NO_WINDOW)


def _commit_one(path: Path) -> str:
    (path / "f").write_text("x", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=str(path), check=True,
                   capture_output=True, **NO_WINDOW)
    subprocess.run(["git", "commit", "-q", "-m", "one"], cwd=str(path), check=True,
                   capture_output=True, **NO_WINDOW)
    res = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(path), check=True,
                         capture_output=True, text=True, **NO_WINDOW)
    return res.stdout.strip()


def test_ref_sha_says_unborn_when_the_branch_has_no_commit(tmp_path):
    _repo(tmp_path)
    assert localrefs.ref_sha(tmp_path) == (True, None)      # 없다 (모른다가 아니다)


def test_ref_sha_matches_rev_parse_loose_packed_and_detached(tmp_path):
    _repo(tmp_path)
    want = _commit_one(tmp_path)

    assert localrefs.ref_sha(tmp_path) == (True, want)       # loose ref
    subprocess.run(["git", "pack-refs", "--all"], cwd=str(tmp_path), check=True,
                   capture_output=True, **NO_WINDOW)
    assert localrefs.ref_sha(tmp_path) == (True, want)       # packed-refs
    subprocess.run(["git", "checkout", "-q", "--detach", "HEAD"], cwd=str(tmp_path),
                   check=True, capture_output=True, **NO_WINDOW)
    assert localrefs.ref_sha(tmp_path) == (True, want)       # detached HEAD


def test_ref_sha_refuses_to_guess_for_shapes_it_does_not_know(tmp_path):
    """`.git` 이 디렉토리가 아니거나 reftable 이면 **모른다**고 답한다."""
    assert localrefs.ref_sha(tmp_path / "nope") == (False, None)
    _repo(tmp_path)
    (tmp_path / ".git" / "reftable").mkdir()
    assert localrefs.ref_sha(tmp_path) == (False, None)
    assert localrefs.ref_sha(tmp_path, "refs/heads/../../evil") == (False, None)


def test_write_ref_is_visible_to_git(tmp_path):
    _repo(tmp_path)
    sha = _commit_one(tmp_path)
    assert localrefs.write_ref(tmp_path, "refs/gitwire/pushed", sha) is True
    res = subprocess.run(["git", "rev-parse", "refs/gitwire/pushed"],
                         cwd=str(tmp_path), check=True, capture_output=True, text=True, **NO_WINDOW)
    assert res.stdout.strip() == sha
    assert localrefs.write_ref(tmp_path, "refs/gitwire/pushed", sha) is True  # 덮어쓰기
    assert localrefs.write_ref(tmp_path, "refs/gitwire/pushed", "nope") is False
    assert localrefs.write_ref(tmp_path, "../evil", sha) is False


def test_write_ref_does_not_steal_someone_elses_lock(tmp_path):
    """⚠️ 남의 `<ref>.lock` 을 치우면 그쪽 ref 갱신이 깨진다 — 손대지 않고 폴백."""
    _repo(tmp_path)
    lock = tmp_path / ".git" / "refs" / "gitwire" / "pushed.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text("someone else", encoding="utf-8")
    assert localrefs.write_ref(tmp_path, "refs/gitwire/pushed", "0" * 40) is False
    assert lock.read_text(encoding="utf-8") == "someone else"


# ------------------- 자격증명을 기동 시 1회 조회해 메모리로 들고 env 로 먹인다


def test_config_env_appends_after_an_existing_count(monkeypatch):
    """⚠️ 0번부터 덮어쓰면 호출자가 그 규약으로 넘긴 설정을 조용히 지운다."""
    monkeypatch.setenv(gitcmd.CONFIG_COUNT_ENV, "2")
    env = gitcmd._config_env((("a.b", "c"),))
    assert env[gitcmd.CONFIG_COUNT_ENV] == "3"
    assert env["GIT_CONFIG_KEY_2"] == "a.b" and env["GIT_CONFIG_VALUE_2"] == "c"
    assert "GIT_CONFIG_KEY_0" not in env


def test_remote_scope_is_only_for_http_remotes():
    assert gitcmd._remote_scope("https://github.com/me/room.git") == (
        "https", "github.com", "https://github.com/me/room.git"
    )
    # ⚠️ URL 에 박힌 자격증명 조각은 스코프·조회 키에 섞지 않는다.
    assert gitcmd._remote_scope("https://u:p@github.com/me/room.git")[2] == (
        "https://github.com/me/room.git"
    )
    assert gitcmd._remote_scope("git@github.com:me/room.git") is None
    assert gitcmd._remote_scope("C:/tmp/origin.git") is None
    assert gitcmd._remote_scope("") is None


def test_held_credential_goes_into_env_only_and_never_into_repr():
    held = gitcmd.HeldCredential(
        "https://github.com/me/room.git", "Authorization: Basic c2VjcmV0",
        ("tok3n-value", "c2VjcmV0"),
    )
    env = held.env()
    assert env["GIT_CONFIG_KEY_0"] == "credential.helper"
    assert env["GIT_CONFIG_VALUE_0"] == ""          # 목록을 비운다
    assert env["GIT_CONFIG_KEY_1"] == (
        "http.https://github.com/me/room.git.extraHeader"
    )
    assert env["GIT_CONFIG_VALUE_1"] == "Authorization: Basic c2VjcmV0"
    assert "c2VjcmV0" not in repr(held) and "c2VjcmV0" not in str(held)


def test_local_remotes_never_shell_out_to_credential_fill(monkeypatch):
    """로컬 경로·ssh 원격에서는 조회 자체가 일어나지 않는다 (테스트가 그 경로다)."""
    def boom(*a, **kw):                             # pragma: no cover
        raise AssertionError("credential fill 을 불렀다")

    monkeypatch.setattr(gitcmd, "_fill", boom)
    assert gitcmd.credential_env("git", "C:/tmp/origin.git") is None
    assert gitcmd.credential_env("git", "git@github.com:me/room.git") is None


def test_held_credential_is_fetched_once_per_host(monkeypatch):
    calls = []

    def fake_fill(git_path, cwd, protocol, host):
        calls.append((protocol, host))
        return {"username": "u", "password": "tok3n"}

    monkeypatch.setattr(gitcmd, "_fill", fake_fill)
    gitcmd.reset_credential_state()
    a = gitcmd.credential_env("git", "https://github.com/me/one.git")
    b = gitcmd.credential_env("git", "https://github.com/me/two.git")
    assert calls == [("https", "github.com")], "호스트당 한 번이 아니다"
    # 같은 조회를 재사용하되 스코프는 그 원격으로 좁혀 준다.
    assert a.scope == "https://github.com/me/one.git"
    assert b.scope == "https://github.com/me/two.git"
    assert a.header == b.header


def test_held_credential_falls_back_when_nothing_is_stored(monkeypatch):
    """못 얻으면 None — 그 뒤는 헬퍼 단계가 받는다 (느린 게 실패보다 낫다)."""
    monkeypatch.setattr(gitcmd, "_fill", lambda *a: {})
    gitcmd.reset_credential_state()
    assert gitcmd.credential_env("git", "https://github.com/me/room.git") is None


def test_held_credential_can_be_turned_off(monkeypatch):
    monkeypatch.setattr(gitcmd, "_fill", lambda *a: {"password": "tok3n"})
    monkeypatch.setenv(gitcmd.HELD_ENV, "off")
    gitcmd.reset_credential_state()
    assert gitcmd.credential_env("git", "https://github.com/me/room.git") is None


def test_credential_tiers_are_held_then_helper_then_user_config(monkeypatch):
    monkeypatch.setattr(gitcmd, "_fill", lambda *a: {"username": "u",
                                                     "password": "tok3n"})
    monkeypatch.setenv(gitcmd.HELPER_ENV, "!echo")
    gitcmd.reset_credential_state()
    g = gitcmd.Git(FakeRunner(gitcmd.GitResult(0, "", "")), Path("."),
                   remote_url="https://github.com/me/room.git")
    tiers = g._credential_tiers(["push", "origin", "main"])
    assert [bool(t.env) for t in tiers] == [True, False, False]
    assert [bool(t.prefix) for t in tiers] == [False, True, False]
    # 로컬 호출은 한 단계 — 동작이 한 글자도 바뀌지 않는다.
    assert len(g._credential_tiers(["commit", "-m", "x"])) == 1


def test_auth_failure_walks_down_to_the_user_config(monkeypatch):
    """들고 있던 것 → 헬퍼 → 사용자 설정. 각 단계는 인증 실패에만 내려간다."""
    monkeypatch.setattr(gitcmd, "_fill", lambda *a: {"username": "u",
                                                     "password": "tok3n"})
    monkeypatch.setenv(gitcmd.HELPER_ENV, "!echo")
    gitcmd.reset_credential_state()
    seen = []

    class Walker:
        git_path = "git"

        def run(self, args, *, cwd=None, env=None, timeout=None):
            has_header = any(
                key.startswith("GIT_CONFIG_VALUE_")
                and str(value).startswith("Authorization:")
                for key, value in (env or {}).items()
            )
            has_helper = "credential.helper=!echo" in args
            seen.append("held" if has_header else
                        ("helper" if has_helper else "plain"))
            if has_header or has_helper:
                return gitcmd.GitResult(
                    128, "",
                    "fatal: could not read Username: terminal prompts disabled",
                )
            return gitcmd.GitResult(0, "ok", "")

    g = gitcmd.Git(Walker(), Path("."), remote_url="https://github.com/me/room.git")
    assert g.run("push", "origin", "main").stdout == "ok"
    assert seen == ["held", "helper", "plain"], seen
    # 접혔으므로 다음 호출은 바로 사용자 설정이다 — 왕복마다 실패를 되풀이하지 않는다.
    seen.clear()
    g.run("push", "origin", "main")
    assert seen == ["plain"], seen


def test_the_held_secret_is_registered_for_redaction(monkeypatch):
    """⚠️ git 이 그 값을 되뱉어도 stdout·stderr·예외에 나타나지 않아야 한다."""
    monkeypatch.setattr(gitcmd, "_fill", lambda *a: {"username": "u",
                                                     "password": "tok3n-secret"})
    monkeypatch.delenv(gitcmd.HELPER_ENV, raising=False)
    gitcmd.reset_credential_state()
    runner = FakeRunner(gitcmd.GitResult(1, "", "fatal: tok3n-secret is bad"))
    g = gitcmd.Git(runner, Path("."), remote_url="https://github.com/me/room.git")
    res = g.run("push", "origin", "main", check=False)
    assert "tok3n-secret" not in res.stderr and "***" in res.stderr
    with pytest.raises(GitError) as err:
        g.run("push", "origin", "main")
    assert "tok3n-secret" not in str(err.value)
    # 그리고 명령줄에는 애초에 실리지 않는다 (env 로만 간다).
    assert not any("tok3n-secret" in " ".join(call) for call in runner.calls)
