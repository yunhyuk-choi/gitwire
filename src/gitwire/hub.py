"""여러 채널을 한 프로세스에서 다루는 레지스트리.

다중 채널 폴링 — 순차 vs 병렬, 판단과 근거
------------------------------------------
실측: `ls-remote` 한 번 = 613~646ms. 이 비용은 **CPU 가 아니라 네트워크 왕복
대기**다. 채널이 N개면 순차 폴링의 한 라운드는 N x 0.64초가 되고, 채널 20개면
한 라운드에 13초가 걸린다 — 30초 주기의 절반을 대기로 태우는 셈이고, 한 호스트가
느려지면 그 뒤의 모든 채널이 함께 밀린다(head-of-line blocking).

그래서 **채널마다 독립 폴링 스레드**를 쓴다:

* git 은 `subprocess` 로 호출되므로 대기 중 GIL 을 잡지 않는다 → 파이썬
  스레드로도 실제 병렬 왕복이 된다.
* 채널이 서로 다른 호스트·다른 토큰일 수 있다. 한 채널의 장애·지연이 다른
  채널의 주기를 흔들지 않아야 한다.
* 채널 하나 안에서는 같은 작업 사본을 만지므로 `Channel._lock` 으로 직렬화된다.
  병렬성은 **채널 간**에만 준다.

비용은 채널당 스레드 1개(대부분 sleep)와 채널당 하루 130KB 수준의 egress 다.
채널 수가 수백을 넘어가면 스레드 대신 셀렉트 기반 스케줄러가 맞겠지만, 그건
이 라이브러리가 상정하는 규모(사람이 참여하는 방 몇 개~수십 개)가 아니다.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Callable, Iterator

from .channel import Channel, Subscription
from .records import Record


class Hub:
    """여러 채널을 열고 함께 닫는 레지스트리."""

    def __init__(self, *, home: Path | None = None, **defaults: Any) -> None:
        self.home = home
        self.defaults = defaults
        self._channels: dict[str, Channel] = {}
        self._subs: list[Subscription] = []
        self._lock = threading.Lock()

    def open(self, repo_url: str, **kwargs: Any) -> Channel:
        """채널을 열거나 이미 열린 것을 돌려준다 (같은 URL+consumer 는 같은 객체)."""
        opts = {**self.defaults, **kwargs}
        if self.home is not None:
            opts.setdefault("home", self.home)
        ch = Channel(repo_url, **opts)
        key = f"{ch.dir}::{ch.consumer}"
        with self._lock:
            existing = self._channels.get(key)
            if existing is not None:
                return existing
            ch.open()
            self._channels[key] = ch
            return ch

    def subscribe_all(
        self,
        callback: Callable[[Channel, Record], None],
        *,
        interval: float | None = None,
    ) -> list[Subscription]:
        """열린 모든 채널을 **각자의 스레드**로 구독한다."""
        subs = []
        for ch in list(self._channels.values()):
            subs.append(
                ch.subscribe(lambda rec, _c=ch: callback(_c, rec), interval=interval)
            )
        with self._lock:
            self._subs.extend(subs)
        return subs

    def fetch_new_all(self, limit: int | None = None) -> dict[str, list[Record]]:
        """⭐ 일회성 조회 (전 채널). 채널 dir 문자열 → 새 레코드 목록.

        일회성 소비자는 스레드를 쓰지 않는다 — 순차로 돈다. 한 번 실행하고 끝나는
        프로세스에서는 스레드 기동 비용·정리 복잡도가 이득보다 크다.
        """
        out: dict[str, list[Record]] = {}
        for ch in list(self._channels.values()):
            out[str(ch.dir)] = ch.fetch_new(limit)
        return out

    @property
    def channels(self) -> list[Channel]:
        return list(self._channels.values())

    def __iter__(self) -> Iterator[Channel]:
        return iter(self.channels)

    def close(self) -> None:
        for sub in self._subs:
            sub.stop()
        self._subs.clear()
        for ch in self.channels:
            ch.close()

    def __enter__(self) -> "Hub":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
