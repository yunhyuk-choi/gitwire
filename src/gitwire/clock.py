"""공통 시계 — 참가자 로컬 시계를 믿지 않는다.

문제
----
레코드 파일명·타임스탬프를 작성자의 로컬 시계로 찍으면, 참가자 간 시계 차이
(실측: 로컬 vs github.com = 2.1초)만큼 순서가 뒤집힌다. 중앙 서버가 없으니
"권위 있는 시계"를 따로 세울 수도 없다.

해법
----
**이미 통신하고 있는 git 호스트의 HTTP `Date:` 응답 헤더**를 기준 시계로 쓴다.
외부 의존이 0 이고(추가 서비스·NTP 불필요), 모든 참가자가 같은 호스트를 보고
있으므로 자동으로 공통 기준이 된다.

정밀도
------
HTTP `Date` 는 **초 단위로 절삭**된다. 따라서 참값은 [D, D+1) 에 균등분포하고
최선의 점추정은 D + 0.5 다. 여기에 RTT/2 를 보정한다(요청 직전/직후 로컬
시각의 중점을 서버 시각과 대응시킨다). 잔여 오차는 대략 ±(0.5 + RTT/2) 초.

갱신 주기
--------
세션 시작 시 1회 + 주기적 재측정(기본 900초). 근거: 소비자 하드웨어 시계
드리프트는 통상 수 ppm~수십 ppm 수준이라 15분이면 수 ms 밖에 안 벌어진다.
반면 NTP 점프·서스펜드 복귀 같은 계단형 변화는 언제든 생기므로 "1회 측정 후
영구 사용"은 위험하다. 15분은 비용(요청 1회)과 안전 사이의 타협이다.
"""

from __future__ import annotations

import threading
import time
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Protocol
from urllib.parse import urlsplit

#: 기본 재측정 주기(초)
DEFAULT_REFRESH_INTERVAL = 900.0
#: 오프셋이 이 값(초)을 넘으면 "시계가 심하게 어긋남"으로 보고 경고 대상
SUSPICIOUS_OFFSET = 60.0


class ClockSource(Protocol):
    def now(self) -> datetime:
        """UTC aware datetime."""
        ...


class SystemClock:
    """로컬 시계 그대로. 보정 없음 (테스트·오프라인 기본값)."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    @property
    def offset(self) -> float:
        return 0.0


def http_date_probe(url: str, timeout: float = 5.0) -> tuple[float, float, float]:
    """(t0, server_epoch, t1) 을 돌려준다. HEAD 우선, 실패 시 GET 폴백."""
    req = urllib.request.Request(url, method="HEAD")
    req.add_header("User-Agent", "gitwire/1")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            date_hdr = resp.headers.get("Date")
    except Exception:
        req = urllib.request.Request(url, method="GET")
        req.add_header("User-Agent", "gitwire/1")
        req.add_header("Range", "bytes=0-0")
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            date_hdr = resp.headers.get("Date")
            resp.read(1)
    t1 = time.time()
    if not date_hdr:
        raise ValueError("응답에 Date 헤더가 없다")
    server_dt = parsedate_to_datetime(date_hdr)
    if server_dt.tzinfo is None:
        server_dt = server_dt.replace(tzinfo=timezone.utc)
    return t0, server_dt.timestamp(), t1


def clock_base_url(repo_url: str) -> str | None:
    """레포 URL 에서 시계 측정용 origin 을 뽑는다. http(s) 가 아니면 None."""
    parts = urlsplit(repo_url)
    if parts.scheme in ("http", "https") and parts.netloc:
        host = parts.netloc.split("@")[-1]  # 자격증명 부분 제거
        return f"{parts.scheme}://{host}/"
    return None


class HttpDateClock:
    """git 호스트의 HTTP Date 헤더로 보정된 시계.

    측정에 실패하면 오프셋 0(=로컬 시계)으로 degraded 동작한다. 시계 보정
    실패로 전송이 멈추면 안 된다.
    """

    def __init__(
        self,
        url: str,
        *,
        probe: Callable[[str, float], tuple[float, float, float]] = http_date_probe,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        timeout: float = 5.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.url = url
        self._probe = probe
        self.refresh_interval = refresh_interval
        self.timeout = timeout
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._offset = 0.0
        self._measured_at: float | None = None
        self.last_error: str | None = None

    @property
    def offset(self) -> float:
        """서버시각 - 로컬시각 (초). 양수면 로컬이 느리다."""
        return self._offset

    @property
    def synced(self) -> bool:
        return self._measured_at is not None

    def refresh(self, force: bool = False) -> float | None:
        """필요하면 재측정한다. 새 오프셋(또는 갱신 안 했으면 None)."""
        with self._lock:
            now_m = self._monotonic()
            if (
                not force
                and self._measured_at is not None
                and now_m - self._measured_at < self.refresh_interval
            ):
                return None
            try:
                t0, server_epoch, t1 = self._probe(self.url, self.timeout)
            except Exception as exc:  # 네트워크·헤더 이상 → degraded
                self.last_error = f"{type(exc).__name__}: {exc}"
                self._measured_at = now_m  # 폭주 방지: 실패도 주기를 소모한다
                return None
            local_mid = (t0 + t1) / 2.0
            # Date 는 초 단위 절삭 → 참값의 기대치는 +0.5초
            self._offset = (server_epoch + 0.5) - local_mid
            self._measured_at = now_m
            self.last_error = None
            return self._offset

    def now(self) -> datetime:
        self.refresh()
        return datetime.fromtimestamp(time.time() + self._offset, tz=timezone.utc)


class FixedOffsetClock:
    """테스트·수동 보정용 고정 오프셋 시계."""

    def __init__(self, offset: float = 0.0, base: Callable[[], float] = time.time):
        self.offset = offset
        self._base = base

    def now(self) -> datetime:
        return datetime.fromtimestamp(self._base() + self.offset, tz=timezone.utc)
