"""git 트리 나열 캐시 — 키가 **내용 주소(sha)** 라 stale 이 정의상 불가능하다.

왜 캐시가 필요한가
------------------
실측(이 머신): git subprocess 한 번은 **하는 일과 무관하게 42~48ms** 다.
`rev-parse HEAD`(47.7ms)나 5000건 전량 나열(48.6ms)이나 같다 — 비용은 작업이
아니라 **프로세스 기동**이다. 역방향 페이징은 한 쪽마다 나열을 2회(날짜 목록 →
그 날짜의 레코드) 하므로 ~90ms 가 바닥으로 깔리고, 무한 스크롤은 이 경로를
반복 호출한다. 호출을 **0회**로 만들 수 있는 유일한 수단이 캐시다.
(호출을 1회로 줄이는 길 — `ls-tree -r` 한 방 — 은 같은 48ms 를 계속 내고
출력이 전체 레코드 수에 비례해 커진다. 캐시가 더 낫다.)

왜 시간 기반·규칙 기반 무효화를 쓰지 않는가
-------------------------------------------
"지난 날짜의 목록은 안 바뀐다"는 **틀린 가정**이다:

* 오늘 날짜 디렉토리는 계속 자란다.
* 오프라인에서 쓰고 나중에 push 하거나 시계가 어긋난 참가자가 있으면 **과거
  날짜에 레코드가 추가**된다.
* fetch 로 남의 커밋이 들어오면 과거 날짜의 트리가 바뀐다.

그래서 무효화를 사람이 맞추는 대신 **git 이 이미 주는 불변식**을 쓴다:
git 오브젝트는 내용 주소다 — **같은 sha 면 내용이 반드시 같다.** 커밋 sha 나
트리 sha 를 캐시 키로 쓰면 내용이 바뀌는 순간 키가 달라지므로, 낡은 값이
반환되는 일이 **정의상** 생기지 않는다. 무효화 로직 자체가 없어진다.

크기 제한 — 근거
---------------
키가 sha 라 "같은 날짜의 서로 다른 버전"이 각각 다른 항목이 된다. 활발한 채널에서
레코드를 발행하며 스크롤하면 **오늘 날짜의 트리 sha 가 발행마다 바뀌므로** 항목이
계속 쌓인다(그날 레코드가 2880건이면 항목 하나가 그만큼 크다). 즉 상한이 없으면
세션이 길어질수록 자란다 — 그래서 상한이 필요하다.

단위는 항목 수가 아니라 **캐시에 든 경로 문자열 수**다(항목 크기가 하루 레코드
수만큼 들쭉날쭉하기 때문이다). 실측: 경로 하나당 **109 바이트**
(2880개 = 0.30MB / 20000개 = 2.09MB / 100000개 = 10.40MB).
기본값 20000개 ≈ **2MB** — 아주 활발한 날(2880건) 기준 7일치, 쪽 크기 50 기준
400쪽분의 작업집합이다. 넘치면 **LRU** 로 가장 오래 안 쓴 것부터 버린다
(버려도 정확성에는 영향이 없다 — 다음 조회에서 git 을 한 번 더 부를 뿐이다).
"""

from __future__ import annotations

import threading
from collections import OrderedDict

#: 캐시에 담아 두는 경로 문자열 수 상한 (실측 109 B/개 → 약 2MB)
DEFAULT_MAX_ITEMS = 20_000


class TreeCache:
    """sha → 나열 결과.

    **자체 잠금을 쥔다.** 예전에는 채널 락이 상호배제를 대신했지만, 지난 날짜
    롤업이 배경 스레드에서 *채널 락을 쥐지 않은 채* 나열을 읽으므로(락을 오래
    쥐지 않는 것이 그 기능의 요구사항이다) 캐시가 스스로 안전해야 한다.
    경합이 없을 때 락 비용은 수십 ns 라 측정에 잡히지 않는다.
    """

    def __init__(self, max_items: int = DEFAULT_MAX_ITEMS) -> None:
        self.max_items = max(0, int(max_items))
        self._items: "OrderedDict[str, list[str]]" = OrderedDict()
        self._size = 0
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: str) -> list[str] | None:
        with self._lock:
            value = self._items.get(key)
            if value is None:
                self.misses += 1
                return None
            self._items.move_to_end(key)      # LRU
            self.hits += 1
            return value

    def put(self, key: str, value: list[str]) -> list[str]:
        if not self.max_items:
            return value
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self._size -= len(old)
            self._items[key] = value
            self._size += len(value)
            while self._items and self._size > self.max_items:
                _, dropped = self._items.popitem(last=False)
                self._size -= len(dropped)
                self.evictions += 1
            return value

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self._size = 0

    def info(self) -> dict:
        with self._lock:
            return {
                "entries": len(self._items),
                "items": self._size,
                "max_items": self.max_items,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }
