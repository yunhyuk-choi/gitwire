"""gitwire CLI — 헤드리스 소비자(에이전트)를 1급으로 취급하는 셸 표면.

출력 규약 — 판단과 근거
----------------------
**stdout 은 언제나 기계가 읽는 JSON 이다. 사람이 읽는 진단·로그는 전부 stderr.**

"사람용/기계용을 플래그로 가른다"는 선택지도 있었지만 택하지 않았다. 플래그로
가르면 호출자가 플래그를 빠뜨린 순간 사람용 텍스트가 파서로 흘러들어가 조용히
깨진다. 스트림으로 가르면 **플래그와 무관하게 계약이 항상 성립**하고,
`2>/dev/null` 한 번이면 순수 JSON 이 된다. 기본을 JSON 으로 둔 이유도 같다 —
에이전트가 기본값으로 안전한 쪽이 옳다.

성공이든 실패든 stdout 에는 항상 JSON 객체 하나가 나오고 `ok` 필드로 갈린다.
(`--ndjson` 은 예외적으로 레코드를 줄 단위로 흘린다 — 스트리밍 소비용.)

종료코드 계약
------------
    0   성공. `fetch`/`watch` 는 **새 레코드를 1건 이상** 돌려준 경우.
    10  성공했지만 **새 레코드가 없음** (fetch 계열 전용). 셸에서 분기용.
    2   사용법 오류 (argparse)
    3   자격증명 없음 / 인증 실패  — 절대 대화형으로 되묻지 않는다
    4   git 또는 네트워크 실패
    5   채널 초기화 실패
    6   원격 히스토리 재작성 충돌 (미푸시 레코드 보호를 위해 중단)
    1   그 밖의 오류

대화형 입력을 요구하지 않는다. 자격증명이 없으면 즉시 exit 3 으로 실패한다 —
비대화 환경에서 무한 대기하는 것이 최악의 실패 모드이기 때문이다.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from . import __version__
from .channel import Channel
from .credentials import NoCredential, TokenCredential
from .errors import AuthError, GitwireError

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NO_RECORDS = 10


def _emit(obj: Any) -> None:
    """stdout 에 기계용 JSON 한 덩어리."""
    json.dump(obj, sys.stdout, ensure_ascii=False, indent=None)
    sys.stdout.write("\n")
    sys.stdout.flush()


def _note(msg: str) -> None:
    """사람용 진단은 stderr 로만."""
    print(msg, file=sys.stderr)


def _credential(args: argparse.Namespace):
    if args.token_file:
        return TokenCredential.from_file(args.token_file, username=args.token_user)
    if args.token_env:
        return TokenCredential.from_env(args.token_env, username=args.token_user)
    return NoCredential()


def _channel(args: argparse.Namespace) -> Channel:
    return Channel(
        args.repo,
        credential=_credential(args),
        consumer=args.consumer,
        sender=args.sender,
        branch=args.branch,
        home=Path(args.home) if args.home else None,
        depth=args.depth,
        batch_window=0.0,  # CLI 는 일회성 호출이 기본 → 즉시 커밋·push
        name=getattr(args, "name", None),
    )


def _load_payload(args: argparse.Namespace) -> Any:
    if args.payload_file:
        raw = (
            sys.stdin.read()
            if args.payload_file == "-"
            else Path(args.payload_file).read_text(encoding="utf-8")
        )
    elif args.payload is not None:
        raw = args.payload
    else:
        raise SystemExit(EXIT_USAGE)
    return json.loads(raw)


# --------------------------------------------------------------------- 명령


def cmd_init(args: argparse.Namespace) -> int:
    with _channel(args) as ch:
        _emit({"ok": True, "command": "init", **ch.info()})
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    with _channel(args) as ch:
        info = ch.info()
        info["remote_head"] = ch.remote_head()
        info["has_changes"] = info["remote_head"] != info["remote_ref"]
        _emit({"ok": True, "command": "status", **info})
    return EXIT_OK


def cmd_fetch(args: argparse.Namespace) -> int:
    with _channel(args) as ch:
        if args.from_now and not ch.cursors.load().started:
            ch.skip_to_now()
        recs = (
            ch.peek_new(args.limit)
            if args.no_advance
            else ch.fetch_new(args.limit)
        )
        if args.ndjson:
            for r in recs:
                _emit(r.to_dict())
        else:
            _emit(
                {
                    "ok": True,
                    "command": "fetch",
                    "channel": ch.info()["repo"],
                    "consumer": ch.consumer,
                    "advanced": not args.no_advance,
                    "count": len(recs),
                    "records": [r.to_dict() for r in recs],
                }
            )
    return EXIT_OK if recs else EXIT_NO_RECORDS


def cmd_history(args: argparse.Namespace) -> int:
    with _channel(args) as ch:
        recs = ch.history(args.limit)
        if args.ndjson:
            for r in recs:
                _emit(r.to_dict())
        else:
            _emit(
                {
                    "ok": True,
                    "command": "history",
                    "count": len(recs),
                    "records": [r.to_dict() for r in recs],
                }
            )
    return EXIT_OK if recs else EXIT_NO_RECORDS


def cmd_append(args: argparse.Namespace) -> int:
    payload = _load_payload(args)
    with _channel(args) as ch:
        rid = ch.append(payload, flush=not args.no_push)
        _emit(
            {
                "ok": True,
                "command": "append",
                "id": rid,
                "sender": ch.sender,
                "pushed": not args.no_push,
            }
        )
    return EXIT_OK


def cmd_ack(args: argparse.Namespace) -> int:
    with _channel(args) as ch:
        moved = ch.ack_through(args.record_id)
        _emit({"ok": True, "command": "ack", "advanced": moved, "id": args.record_id})
    return EXIT_OK if moved else EXIT_NO_RECORDS


def cmd_watch(args: argparse.Namespace) -> int:
    """상시 구독을 NDJSON 스트림으로 흘린다 (tail 처럼 쓰는 소비자용)."""
    seen = 0
    with _channel(args) as ch:
        ch.batch_window = 0.0

        def on_rec(rec) -> None:
            nonlocal seen
            seen += 1
            _emit(rec.to_dict())

        sub = ch.subscribe(on_rec, interval=args.interval)
        try:
            while sub.running:
                if args.max_records and seen >= args.max_records:
                    break
                sub._stop.wait(0.2)  # noqa: SLF001 - 내부 이벤트 재사용
        except KeyboardInterrupt:
            _note("gitwire: 중단 요청")
        finally:
            sub.stop()
    return EXIT_OK if seen else EXIT_NO_RECORDS


def cmd_compact(args: argparse.Namespace) -> int:
    if not args.yes:
        _emit(
            {
                "ok": False,
                "command": "compact",
                "error": "히스토리 압축은 파괴적이다(force-push). --yes 를 명시하라.",
                "type": "ConfirmationRequired",
            }
        )
        return EXIT_USAGE
    with _channel(args) as ch:
        result = ch.compact(keep_records=args.keep, confirm=True)
        _emit({"ok": True, "command": "compact", **result})
    return EXIT_OK


def cmd_where(args: argparse.Namespace) -> int:
    """클론 위치만 알려준다 (네트워크 접근 없음)."""
    from . import layout

    home = Path(args.home) if args.home else None
    d = layout.channel_dir(args.repo, home)
    _emit(
        {
            "ok": True,
            "command": "where",
            "repo": layout.normalize_repo_url(args.repo),
            "channel_dir": str(d),
            "clone_dir": str(d / "clone"),
            "cursor_file": str(d / "cursors" / f"{args.consumer}.json"),
        }
    )
    return EXIT_OK


# --------------------------------------------------------------------- 파서


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gitwire",
        description="git 레포를 전송 계층으로 쓰는 append-only 레코드 배관. "
        "stdout=JSON, stderr=사람용 진단.",
    )
    p.add_argument("--version", action="version", version=f"gitwire {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--repo", required=True, help="채널 레포 URL 또는 로컬 경로")
        sp.add_argument("--branch", default="main")
        sp.add_argument("--consumer", default="default", help="커서를 분리할 소비자 이름")
        sp.add_argument("--home", default=None, help="gitwire 상태 루트 (기본: OS 관례)")
        sp.add_argument("--sender", default=None, help="참가자 식별자")
        sp.add_argument("--depth", type=int, default=None, help="shallow clone 깊이")
        sp.add_argument(
            "--token-env", default=None, help="토큰을 담은 환경변수 이름"
        )
        sp.add_argument("--token-file", default=None, help="토큰이 든 파일 경로")
        sp.add_argument("--token-user", default="gitwire", help="HTTPS basic 사용자명")
        sp.add_argument("--verbose", action="store_true", help="stderr 로그 상세화")

    sp = sub.add_parser("init", help="채널 초기화 (빈 레포에 규약을 심는다)")
    common(sp)
    sp.add_argument("--name", default=None, help="채널 표시 이름")
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("status", help="채널·커서 상태 (원격 SHA 조회 포함)")
    common(sp)
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("fetch", help="지난번 이후 새 레코드 조회 (커서 전진)")
    common(sp)
    sp.add_argument("--limit", type=int, default=None)
    sp.add_argument("--no-advance", action="store_true", help="커서를 전진시키지 않음")
    sp.add_argument(
        "--from-now", action="store_true", help="첫 호출에서 과거 백로그를 건너뜀"
    )
    sp.add_argument("--ndjson", action="store_true", help="레코드를 줄 단위로 출력")
    sp.set_defaults(func=cmd_fetch)

    sp = sub.add_parser("history", help="커서와 무관하게 전체 읽기")
    common(sp)
    sp.add_argument("--limit", type=int, default=None, help="최근 N건")
    sp.add_argument("--ndjson", action="store_true")
    sp.set_defaults(func=cmd_history)

    sp = sub.add_parser("append", help="레코드 발행")
    common(sp)
    g = sp.add_mutually_exclusive_group(required=True)
    g.add_argument("--payload", default=None, help="JSON 문자열")
    g.add_argument("--payload-file", default=None, help="JSON 파일 경로 ('-' 는 stdin)")
    sp.add_argument("--no-push", action="store_true", help="로컬에만 쓰고 push 안 함")
    sp.set_defaults(func=cmd_append)

    sp = sub.add_parser("ack", help="peek 로 받은 레코드까지 커서를 전진")
    common(sp)
    sp.add_argument("--record-id", required=True)
    sp.set_defaults(func=cmd_ack)

    sp = sub.add_parser("watch", help="상시 구독 → NDJSON 스트림")
    common(sp)
    sp.add_argument("--interval", type=float, default=30.0)
    sp.add_argument("--max-records", type=int, default=0, help="N건 받으면 종료 (0=무한)")
    sp.set_defaults(func=cmd_watch)

    sp = sub.add_parser("compact", help="히스토리 압축 (파괴적, force-push)")
    common(sp)
    sp.add_argument("--keep", type=int, default=None, help="최근 레코드 N건만 보존")
    sp.add_argument("--yes", action="store_true", help="파괴적 동작을 명시 승인")
    sp.set_defaults(func=cmd_compact)

    sp = sub.add_parser("where", help="로컬 클론·커서 경로 출력 (네트워크 없음)")
    sp.add_argument("--repo", required=True)
    sp.add_argument("--home", default=None)
    sp.add_argument("--consumer", default="default")
    sp.set_defaults(func=cmd_where)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if getattr(args, "verbose", False) else logging.WARNING,
        format="gitwire %(levelname)s %(message)s",
    )
    try:
        return int(args.func(args))
    except AuthError as exc:
        _emit({"ok": False, "command": args.command, "type": "AuthError", "error": str(exc)})
        return exc.exit_code
    except GitwireError as exc:
        _emit(
            {
                "ok": False,
                "command": args.command,
                "type": type(exc).__name__,
                "error": str(exc),
            }
        )
        return exc.exit_code
    except json.JSONDecodeError as exc:
        _emit(
            {
                "ok": False,
                "command": args.command,
                "type": "JSONDecodeError",
                "error": f"payload 가 올바른 JSON 이 아니다: {exc}",
            }
        )
        return EXIT_USAGE
    except KeyboardInterrupt:
        _note("gitwire: 중단됨")
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001
        _emit(
            {
                "ok": False,
                "command": getattr(args, "command", None),
                "type": type(exc).__name__,
                "error": str(exc),
            }
        )
        return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
