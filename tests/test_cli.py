"""CLI 표면 — 헤드리스 소비자(에이전트)가 셸로 쓰는 경로.

여기서 고정하려는 계약:

* stdout 은 **항상 기계가 읽는 JSON**, 사람용 진단은 stderr.
* 종료코드로 "새 레코드 있었나 / 없었나 / 실패했나"를 셸에서 분기할 수 있다.
* **매번 새 프로세스**로 호출해도 중복·유실이 없다 (커서가 디스크에 있으므로).
* 자격증명이 없으면 되묻지 않고 즉시 실패한다 (무한 대기 금지).
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from conftest import run_cli

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_AUTH = 3
EXIT_NO_RECORDS = 10


@pytest.fixture
def cli(bare_repo, tmp_path, cli_env):
    """이 채널을 향한 CLI 호출기. 매 호출이 **새 프로세스**다."""
    home = tmp_path / "clihome"

    def call(*args: str, consumer: str = "default") -> subprocess.CompletedProcess:
        return run_cli(
            cli_env, args[0], "--repo", str(bare_repo), "--home", str(home),
            "--consumer", consumer, *args[1:],
        )

    return call


def payload_of(proc) -> dict:
    assert proc.stdout, f"stdout 이 비었다. stderr={proc.stderr}"
    return json.loads(proc.stdout.strip().splitlines()[-1])


# ------------------------------------------------------------------ 기본


def test_init_emits_machine_json(cli):
    p = cli("init", "--name", "테스트방")
    assert p.returncode == EXIT_OK, p.stderr
    body = payload_of(p)
    assert body["ok"] is True
    assert body["command"] == "init"
    assert "channel_dir" in body
    # 자격증명 값이 새지 않는다
    assert "token" not in p.stdout.lower()


def test_append_then_fetch(cli):
    cli("init")
    a = cli("append", "--payload", '{"kind":"msg","body":"안녕"}')
    assert a.returncode == EXIT_OK, a.stderr
    rid = payload_of(a)["id"]
    assert rid.startswith("records/")

    f = cli("fetch", consumer="reader")
    assert f.returncode == EXIT_OK
    body = payload_of(f)
    assert body["count"] == 1
    assert body["records"][0]["payload"] == {"kind": "msg", "body": "안녕"}
    assert body["records"][0]["id"] == rid


def test_exit_code_10_when_nothing_new(cli):
    cli("init")
    cli("append", "--payload", '{"n":1}')
    assert cli("fetch", consumer="r").returncode == EXIT_OK
    second = cli("fetch", consumer="r")
    assert second.returncode == EXIT_NO_RECORDS
    assert payload_of(second)["count"] == 0


def test_repeated_oneshot_calls_have_no_dup_no_loss(cli):
    """⭐ 매번 새 프로세스로 호출해도 정확히 한 번씩만 받는다."""
    cli("init")
    for i in range(5):
        assert cli("append", "--payload", json.dumps({"i": i})).returncode == EXIT_OK

    seen: list[int] = []
    # 호출 1: 2건만
    p = cli("fetch", "--limit", "2", consumer="agent")
    assert p.returncode == EXIT_OK
    seen += [r["payload"]["i"] for r in payload_of(p)["records"]]
    # 호출 2 (새 프로세스): 나머지
    p = cli("fetch", consumer="agent")
    assert p.returncode == EXIT_OK
    seen += [r["payload"]["i"] for r in payload_of(p)["records"]]
    # 호출 3: 더 없음
    assert cli("fetch", consumer="agent").returncode == EXIT_NO_RECORDS

    assert seen == [0, 1, 2, 3, 4], "새 프로세스 반복 호출에서 중복 또는 유실"

    # 그 뒤에 온 레코드는 그것만
    cli("append", "--payload", '{"i":99}')
    p = cli("fetch", consumer="agent")
    assert [r["payload"]["i"] for r in payload_of(p)["records"]] == [99]


def test_no_advance_lets_consumer_ack_explicitly(cli):
    cli("init")
    cli("append", "--payload", '{"i":0}')
    cli("append", "--payload", '{"i":1}')

    p = cli("fetch", "--no-advance", consumer="careful")
    recs = payload_of(p)["records"]
    assert len(recs) == 2
    # 커서가 안 움직였다 → 다시 봐도 2건
    assert payload_of(cli("fetch", "--no-advance", consumer="careful"))["count"] == 2

    ack = cli("ack", "--record-id", recs[0]["id"], consumer="careful")
    assert ack.returncode == EXIT_OK
    assert payload_of(cli("fetch", consumer="careful"))["count"] == 1


def test_from_now_skips_backlog(cli):
    cli("init")
    cli("append", "--payload", '{"old":1}')
    p = cli("fetch", "--from-now", consumer="late")
    assert p.returncode == EXIT_NO_RECORDS
    cli("append", "--payload", '{"new":1}')
    p = cli("fetch", consumer="late")
    assert payload_of(p)["count"] == 1


# ------------------------------------------------------------ 출력 계약


def test_stdout_is_pure_json_even_with_verbose(cli):
    """사람용 로그는 stderr 로만 간다 — 플래그와 무관하게 계약이 성립한다."""
    cli("init")
    cli("append", "--payload", '{"n":1}')
    p = cli("fetch", "--verbose", consumer="v")
    for line in p.stdout.strip().splitlines():
        json.loads(line)   # stdout 의 모든 줄이 JSON 이어야 한다


def test_ndjson_streams_one_record_per_line(cli):
    cli("init")
    for i in range(3):
        cli("append", "--payload", json.dumps({"i": i}))
    p = cli("fetch", "--ndjson", consumer="nd")
    lines = [json.loads(l) for l in p.stdout.strip().splitlines()]
    assert [r["payload"]["i"] for r in lines] == [0, 1, 2]


def test_payload_from_stdin(bare_repo, tmp_path, cli_env):
    home = tmp_path / "h"
    run_cli(cli_env, "init", "--repo", str(bare_repo), "--home", str(home))
    proc = subprocess.run(
        [sys.executable, "-m", "gitwire", "append", "--repo", str(bare_repo),
         "--home", str(home), "--payload-file", "-"],
        input='{"from":"stdin"}', capture_output=True, text=True,
        encoding="utf-8", env=cli_env,
    )
    assert proc.returncode == EXIT_OK, proc.stderr
    p = run_cli(cli_env, "fetch", "--repo", str(bare_repo), "--home", str(home),
                "--consumer", "r")
    assert payload_of(p)["records"][0]["payload"] == {"from": "stdin"}


def test_stdout_json_is_utf8_regardless_of_console_codepage(cli):
    """윈도우 콘솔 코드페이지(cp949 등)가 표현 못 하는 문자도 그대로 나와야 한다."""
    cli("init")
    exotic = {"emoji": "🚀✅", "ko": "한글", "ja": "日本語", "math": "∑∫"}
    assert cli("append", "--payload", json.dumps(exotic)).returncode == EXIT_OK
    p = cli("fetch", consumer="utf8")
    assert p.returncode == EXIT_OK, p.stderr
    assert payload_of(p)["records"][0]["payload"] == exotic


def test_status_reports_cursor_and_remote(cli):
    cli("init")
    cli("append", "--payload", '{"n":1}')
    body = payload_of(cli("status"))
    assert body["ok"] is True
    assert body["head"] == body["remote_head"]
    assert body["has_changes"] is False
    assert "cursor" in body


def test_history_ignores_cursor(cli):
    cli("init")
    for i in range(3):
        cli("append", "--payload", json.dumps({"i": i}))
    cli("fetch", consumer="h")
    assert cli("fetch", consumer="h").returncode == EXIT_NO_RECORDS
    body = payload_of(cli("history", consumer="h"))
    assert body["count"] == 3


def test_where_needs_no_network(bare_repo, tmp_path, cli_env):
    """네트워크·클론 없이 위치만 알려준다."""
    p = run_cli(cli_env, "where", "--repo", "https://github.com/me/room.git",
                "--home", str(tmp_path / "nope"))
    assert p.returncode == EXIT_OK
    body = json.loads(p.stdout)
    assert body["clone_dir"].endswith("clone")
    assert body["cursor_file"].endswith("default.json")
    assert not (tmp_path / "nope").exists()  # 아무것도 만들지 않았다


# ------------------------------------------------------------ 실패 경로


def test_missing_credential_exits_3_without_prompting(bare_repo, tmp_path, cli_env):
    """비대화 환경에서 무한 대기하지 않고 명확한 오류로 끝난다."""
    cli_env.pop("ABSENT_TOKEN_VAR", None)
    p = run_cli(
        cli_env, "fetch", "--repo", str(bare_repo), "--home", str(tmp_path / "h"),
        "--token-env", "ABSENT_TOKEN_VAR",
    )
    assert p.returncode == EXIT_AUTH
    body = json.loads(p.stdout)
    assert body["ok"] is False
    assert body["type"] == "AuthError"


def test_bad_payload_exits_usage(cli):
    cli("init")
    p = cli("append", "--payload", "이건 JSON 이 아니다")
    assert p.returncode == EXIT_USAGE
    body = json.loads(p.stdout)
    assert body["ok"] is False
    assert body["type"] == "JSONDecodeError"


def test_unreachable_repo_exits_git_error(tmp_path, cli_env):
    p = run_cli(
        cli_env, "fetch", "--repo", str(tmp_path / "does-not-exist.git"),
        "--home", str(tmp_path / "h"),
    )
    assert p.returncode not in (EXIT_OK, EXIT_NO_RECORDS)
    body = json.loads(p.stdout)
    assert body["ok"] is False


def test_compact_refuses_without_yes(cli):
    cli("init")
    p = cli("compact")
    assert p.returncode == EXIT_USAGE
    assert json.loads(p.stdout)["ok"] is False
