"""설치본 신원 — `sender` 기본값을 만드는 곳.

무엇을 가르는 식별자인가
------------------------
`sender` 는 **전역 신원이 아니다.** "이 레코드를 발행한 것이 *이 설치본*인가"를
가르는 전송 수준 값이다(IP 주소에 가깝다). 사람이 읽는 이름은 소비자가 자기
payload 스키마로 소유한다(채팅의 ``author``).

왜 `<유저>.<호스트>` 로는 안 되나
--------------------------------
그 값은 **같은 머신의 두 프로세스에서 동일**하다. 그러면 "이 레코드가 내가
발행한 것인가"라는 판정이 무너진다 — 남의 레코드를 자기 에코로 오인하고,
그 위에 얹은 알림·읽음 처리가 조용히 틀린다. 한 사람이 계정·프로필을 둘 쓰거나,
테스트로 두 인스턴스를 띄우거나, 같은 노트북에서 두 사람이 각자 앱을 돌리는
상황이 전부 여기에 걸린다.

그래서: **설치본마다 한 번 만들어 디스크에 영속**시킨다

    <gitwire_home>/installation.txt

* gitwire 는 이미 이 디렉토리 아래에 채널 클론과 커서를 영속시킨다. 신원도
  같은 자리에 둔다 — 새 저장 위치 규약을 만들지 않는다.
* **재시작해도 같은 값**이다(파일이라서).
* 설치본 = 하나의 ``GITWIRE_HOME``. 두 인스턴스를 같은 머신에서 띄우려면 어차피
  home 이 달라야 한다(같은 home = 같은 클론·같은 커서라 서로를 밟는다). 그래서
  home 단위 신원이 곧 "설치본 단위 신원"이다.

씨앗은 **git 이 이미 아는 신원**
-------------------------------
사람이 로그·레코드 파일을 눈으로 볼 때 누구 것인지 알아볼 수 있어야 한다.
그래서 ``git config user.email`` 을 씨앗으로 쓰고 짧은 난수를 붙인다::

    yh.choi@example.com.a3f9c1

* git 은 이미 우리가 부르는 유일한 외부 프로그램이다 — **의존성이 0 그대로**다.
* ⚠️ **forge 계정(GitHub 등)에 묶지 않는다.** gitwire 는 GitLab·사내 git·bare·
  로컬 경로를 다 지원하는 forge 중립 설계이고, 게다가 forge 계정은 *같은 사람의
  두 머신·두 프로세스를 구별하지 못해* 애초 이 버그를 못 고친다.
* 이메일을 못 읽으면(전역 설정이 없는 CI 등) 예전 기본값 ``<유저>.<호스트>`` 로
  폴백한다. 어느 쪽이든 뒤에 붙는 난수가 설치본을 가른다.

하위호환
--------
* 명시적으로 넘긴 ``sender=`` 는 **그대로 존중**한다 (CLI ``--sender``,
  라이브러리 인자, 환경변수 ``GITWIRE_SENDER``).
* 기존 채널의 기존 레코드는 그대로 읽힌다 — 봉투의 ``sender`` 는 원래부터
  불투명한 문자열이고, 이 변경은 *기본값을 만드는 방법*만 바꾼다.
"""

from __future__ import annotations

import getpass
import logging
import os
import secrets
import socket
from pathlib import Path

from . import layout, records
from .gitcmd import GitRunner, SubprocessGitRunner

log = logging.getLogger("gitwire")

#: 설치본 식별자를 담는 파일 (채널 디렉토리들 옆, 같은 상태 루트 안)
INSTALLATION_FILE = "installation.txt"

#: 난수 접미의 바이트 수 (16진 6자리)
_SUFFIX_BYTES = 3

#: 씨앗에 허용할 최대 길이. 뒤에 ``.<16진6>`` 이 붙어도 슬러그 상한을 넘지 않게.
_SEED_MAX = records.MAX_SENDER_LEN - (_SUFFIX_BYTES * 2) - 1


def local_seed() -> str:
    """git 신원을 못 읽을 때의 폴백 씨앗 (예전 기본값과 같은 모양)."""
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001
        user = "anon"
    try:
        host = socket.gethostname().split(".")[0]
    except Exception:  # noqa: BLE001
        host = "local"
    return f"{user}.{host}"


def git_email(runner: GitRunner | None = None, cwd: Path | None = None) -> str:
    """``git config user.email``. 없거나 실패하면 빈 문자열.

    실패는 정상 경로다 — git 전역 설정이 없는 환경(컨테이너·CI)이 흔하다.
    """
    runner = runner or SubprocessGitRunner()
    where = cwd if cwd and Path(cwd).is_dir() else Path.cwd()
    try:
        res = runner.run(["config", "--get", "user.email"], cwd=Path(where))
    except Exception:  # noqa: BLE001 — git 이 없을 수도 있다. 치명적이지 않다.
        return ""
    if res.returncode != 0:
        return ""
    return res.stdout.strip()


def new_installation_id(
    runner: GitRunner | None = None, cwd: Path | None = None
) -> str:
    """새 설치본 식별자를 만든다 (읽기 전용 — 저장하지 않는다)."""
    seed = git_email(runner, cwd) or local_seed()
    seed = records.slug_sender(seed, max_len=_SEED_MAX)
    return f"{seed}.{secrets.token_hex(_SUFFIX_BYTES)}"


def installation_id(
    home: Path | str | None = None, *, runner: GitRunner | None = None
) -> str:
    """이 설치본의 ``sender``. 없으면 만들고, 있으면 그대로 읽는다.

    ``<home>/installation.txt`` 에 영속한다. 파일을 만들 수 없는 환경(읽기전용
    마운트 등)에서도 **죽지 않는다** — 그 프로세스 한정 값으로 degraded 동작한다.
    """
    root = Path(home) if home is not None else layout.gitwire_home()
    path = root / INSTALLATION_FILE
    try:
        existing = path.read_text(encoding="utf-8-sig").strip()
        if existing:
            return records.slug_sender(existing)
    except OSError:
        pass

    value = new_installation_id(runner, root if root.is_dir() else None)
    try:
        root.mkdir(parents=True, exist_ok=True)
        # 배타적 생성 — 두 프로세스가 동시에 만들면 **먼저 쓴 쪽이 이긴다.**
        # (나중 쪽이 덮어쓰면 이미 그 값으로 발행한 레코드와 신원이 어긋난다.)
        with open(path, "x", encoding="utf-8", newline="\n") as fh:
            fh.write(value + "\n")
    except FileExistsError:
        try:
            winner = path.read_text(encoding="utf-8-sig").strip()
            if winner:
                return records.slug_sender(winner)
        except OSError:
            pass
    except OSError as exc:
        # 저장 실패는 치명적이지 않다. 이번 프로세스만 이 값을 쓴다.
        log.debug("gitwire: 설치본 식별자를 저장하지 못했다 (%s): %s", path, exc)
    return records.slug_sender(value)


def default_sender(
    home: Path | str | None = None, *, runner: GitRunner | None = None
) -> str:
    """``sender`` 기본값. ``GITWIRE_SENDER`` 가 있으면 그것이 이긴다."""
    env = os.environ.get("GITWIRE_SENDER")
    if env:
        return records.slug_sender(env)
    return installation_id(home, runner=runner)
