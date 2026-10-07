# Market Data Interface

The unified Python API for stock prices in FinAlly. One abstract interface, two implementations: the Massive REST API when `MASSIVE_API_KEY` is set, the built-in simulator otherwise. Everything downstream — SSE streaming, trade execution, portfolio valuation, the LLM's portfolio context — reads from a shared in-memory price cache and never knows which source is running.

Related documents: `MASSIVE_API.md` (the external API), `MARKET_SIMULATOR.md` (the simulator's math and structure).

The code in this document and in `MARKET_SIMULATOR.md` was prototyped and run: the simulator end to end, and the Massive poller against a fake HTTP client (snapshot mode, free-tier fallback, rate-limit handling). It has not been run against the live Massive API.

---

## 1. Design

```
                      ┌──────────────────────┐
  MASSIVE_API_KEY? ──▶│ create_market_data_  │
                      │ source(cache)        │
                      └──────────┬───────────┘
                 unset / empty   │   set
              ┌──────────────────┴───────────────────┐
              ▼                                      ▼
   ┌─────────────────────┐              ┌─────────────────────┐
   │ SimulatorDataSource │              │ MassiveDataSource   │
   │ GBM tick every 0.5s │              │ REST poll every 15s │
   └──────────┬──────────┘              └──────────┬──────────┘
              │       both are MarketDataSource     │
              └──────────────────┬──────────────────┘
                                 ▼ writes
                        ┌─────────────────┐
                        │   PriceCache    │   latest PriceUpdate per ticker
                        └────────┬────────┘
                                 │ reads
        ┌────────────────┬───────┴────────┬─────────────────┐
        ▼                ▼                ▼                 ▼
   SSE stream      trade execution   portfolio value    chat context
```

Principles:

- **Push into a cache, pull from the cache.** A data source is a background task that writes prices. Consumers call `cache.get_price("AAPL")`. No consumer ever awaits a network call to get a price, so trades fill instantly and the SSE loop never blocks.
- **The interface is about lifecycle and ticker membership, not prices.** `start`, `stop`, `add_ticker`, `remove_ticker`, `get_tickers`. That is all the two implementations have in common, so that is all the interface contains.
- **One process, one event loop, one writer.** All writes happen on the asyncio event loop, so the cache needs no locks.
- **A source never raises out of its background task.** A failed poll or tick is logged and the cache keeps its last values.

---

## 2. Module Layout

```
backend/
  app/
    market/
      __init__.py          # Public exports (Section 9)
      models.py            # PriceUpdate
      cache.py             # PriceCache
      interface.py         # MarketDataSource (ABC)
      factory.py           # create_market_data_source()
      simulator.py         # GBMSimulator, SimulatorDataSource   (see MARKET_SIMULATOR.md)
      seed_prices.py       # Simulator configuration             (see MARKET_SIMULATOR.md)
      massive_client.py    # MassiveDataSource
      stream.py            # SSE router: GET /api/stream/prices
  tests/
    market/
      test_cache.py
      test_simulator.py
      test_massive.py
      test_factory.py
```

Dependencies: `fastapi`, `uvicorn`, `httpx`. The simulator uses only the standard library.

---

## 3. Data Model — `models.py`

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float  # Unix seconds
    prev_close: float | None = None  # reference price for "daily change %"

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change_percent(self) -> float | None:
        if not self.prev_close:
            return None
        return round((self.price - self.prev_close) / self.prev_close * 100, 4)

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "direction": self.direction,
            "prev_close": self.prev_close,
            "day_change_percent": self.day_change_percent,
        }
```

Notes:

- `price` and `previous_price` are rounded to cents by the cache.
- `direction` compares against the previous *cached* price — the last tick or the last poll — and drives the green/red flash in the UI.
- `prev_close` is the reference for the watchlist's "daily change %". Massive supplies the real previous close. The simulator has no previous day, so it uses the price the ticker started the session at.
- `timestamp` is Unix seconds. Massive's nanosecond and millisecond timestamps are converted at the boundary.

---

## 4. Price Cache — `cache.py`

```python
from __future__ import annotations

import time

from .models import PriceUpdate


class PriceCache:
    """Latest price per ticker. One writer (the data source), many readers."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._version = 0

    @property
    def version(self) -> int:
        """Bumped on every write; lets the SSE loop skip sends when nothing changed."""
        return self._version

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        prev_close: float | None = None,
    ) -> PriceUpdate:
        last = self._prices.get(ticker)
        update = PriceUpdate(
            ticker=ticker,
            price=round(price, 2),
            previous_price=last.price if last else round(price, 2),
            timestamp=timestamp if timestamp is not None else time.time(),
            prev_close=prev_close if prev_close is not None else (last.prev_close if last else None),
        )
        self._prices[ticker] = update
        self._version += 1
        return update

    def get(self, ticker: str) -> PriceUpdate | None:
        return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self._prices.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        return dict(self._prices)

    def remove(self, ticker: str) -> None:
        if self._prices.pop(ticker, None) is not None:
            self._version += 1
```

Notes:

- `update()` computes `previous_price` itself, so sources only report the new price.
- `prev_close` is sticky: pass it once (or on every poll) and later updates without it keep the stored value.
- The first update for a ticker has `previous_price == price`, so `direction` is `"flat"`.
- `version` lets the SSE loop detect "nothing changed" cheaply. This matters with Massive, where the cache changes every 15 seconds but the SSE loop wakes every 500 ms.

---

## 5. The Interface — `interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Produces prices for a set of tickers and writes them to a PriceCache.

    Implementations own a background asyncio task. Consumers never read prices
    from the source; they read from the PriceCache it was constructed with.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing prices. Seeds the cache before returning where possible."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task and release resources. Safe to call twice."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and drop it from the cache. No-op if unknown."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Tickers currently tracked."""
```

Contract details:

| Method | Simulator | Massive |
|---|---|---|
| `start(tickers)` | Seeds the cache with starting prices, then ticks every 500 ms | Polls once immediately (cache is populated on return if the API is reachable), then polls on an interval |
| `stop()` | Cancels the task | Cancels the task and closes the HTTP client |
| `add_ticker(t)` | Price is in the cache on return | Fetches that one ticker immediately; price is in the cache on return if Massive knows the symbol |
| `remove_ticker(t)` | Removed from simulation and cache | Removed from the poll set and cache |

Tickers are passed already normalised (upper-case, stripped). Normalisation belongs in the API route that accepts user input, not here.

---

## 6. Factory — `factory.py`

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(cache: PriceCache) -> MarketDataSource:
    """Massive if MASSIVE_API_KEY is set and non-empty, otherwise the simulator."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        from .massive_client import MassiveDataSource

        poll_interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15"))
        logger.info("Market data: Massive API (poll every %.0fs)", poll_interval)
        return MassiveDataSource(api_key=api_key, cache=cache, poll_interval=poll_interval)

    from .simulator import SimulatorDataSource

    logger.info("Market data: simulator")
    return SimulatorDataSource(cache=cache)
```

- A whitespace-only key counts as unset, matching PLAN.md §5 ("absent or empty").
- `MASSIVE_POLL_INTERVAL` (seconds, default `15`) is an optional addition to the environment variables in PLAN.md §5, for users on paid plans who want faster updates. It is not required.
- Imports are local so the simulator path does not import `httpx`.

---

## 7. Massive Implementation — `massive_client.py`

```python
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)

BASE_URL = "https://api.massive.com"
SNAPSHOT_PATH = "/v2/snapshot/locale/us/markets/stocks/tickers"
GROUPED_DAILY_PATH = "/v2/aggs/grouped/locale/us/market/stocks/{date}"

EOD_POLL_INTERVAL = 3600.0  # end-of-day prices change once a day
EOD_MAX_LOOKBACK_DAYS = 5  # covers a long weekend; also stays inside 5 calls/min


class MassiveDataSource(MarketDataSource):
    """Polls the Massive REST API and writes prices to the PriceCache.

    Starts in "snapshot" mode (one call returns live prices for every ticker).
    If the API key's plan is not entitled to snapshots (HTTP 403, i.e. the free
    tier), it switches permanently to "eod" mode and serves the most recent
    daily close instead.
    """

    def __init__(
        self,
        api_key: str,
        cache: PriceCache,
        poll_interval: float = 15.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._cache = cache
        self._poll_interval = poll_interval
        self._client = client or httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )
        self._tickers: set[str] = set()
        self._mode = "snapshot"
        self._task: asyncio.Task | None = None

    @property
    def mode(self) -> str:
        return self._mode

    async def start(self, tickers: list[str]) -> None:
        self._tickers = set(tickers)
        await self._poll_once()  # seed the cache before the first SSE client connects
        self._task = asyncio.create_task(self._run(), name="massive-poller")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._client.aclose()

    async def add_ticker(self, ticker: str) -> None:
        if ticker in self._tickers:
            return
        self._tickers.add(ticker)
        await self._poll_once([ticker])  # price it now rather than at the next poll

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers.discard(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return sorted(self._tickers)

    async def _run(self) -> None:
        while True:
            interval = self._poll_interval if self._mode == "snapshot" else EOD_POLL_INTERVAL
            await asyncio.sleep(interval)
            await self._poll_once()

    async def _poll_once(self, tickers: list[str] | None = None) -> None:
        """Fetch prices for the given tickers (default: all). Never raises."""
        wanted = sorted(tickers if tickers is not None else self._tickers)
        if not wanted:
            return
        try:
            if self._mode == "snapshot" and not await self._poll_snapshot(wanted):
                logger.warning(
                    "Massive plan has no snapshot access; serving end-of-day prices. "
                    "Prices will update once per day."
                )
                self._mode = "eod"
            if self._mode == "eod":
                await self._poll_eod(wanted)
        except httpx.HTTPError as exc:
            logger.warning("Massive poll failed, keeping last prices: %s", exc)
        except Exception:
            logger.exception("Unexpected error polling Massive")

    async def _poll_snapshot(self, tickers: list[str]) -> bool:
        """Returns False if the plan is not entitled to snapshots."""
        resp = await self._client.get(SNAPSHOT_PATH, params={"tickers": ",".join(tickers)})
        if resp.status_code == 403:
            return False
        resp.raise_for_status()
        for item in resp.json().get("tickers", []):
            price = _snapshot_price(item)
            if price is None:
                continue
            self._cache.update(
                ticker=item["ticker"],
                price=price,
                timestamp=_snapshot_timestamp(item),
                prev_close=(item.get("prevDay") or {}).get("c") or None,
            )
        return True

    async def _poll_eod(self, tickers: list[str]) -> None:
        """Walk back from yesterday to the most recent day that has daily bars."""
        day = datetime.now(timezone.utc).date() - timedelta(days=1)
        for _ in range(EOD_MAX_LOOKBACK_DAYS):
            resp = await self._client.get(GROUPED_DAILY_PATH.format(date=day.isoformat()))
            if resp.status_code != 403:  # 403 here: that day's data is not published yet
                resp.raise_for_status()
                bars = {bar["T"]: bar for bar in resp.json().get("results", [])}
                if bars:  # empty on weekends and holidays
                    for ticker in tickers:
                        bar = bars.get(ticker)
                        if bar and bar.get("c"):
                            self._cache.update(ticker, bar["c"], timestamp=bar["t"] / 1000)
                    return
            day -= timedelta(days=1)
        logger.warning("No end-of-day data found in the last %d days", EOD_MAX_LOOKBACK_DAYS)


def _snapshot_price(item: dict) -> float | None:
    """Best available price. Zero means 'not populated yet', not a real price."""
    candidates = (
        (item.get("lastTrade") or {}).get("p"),
        (item.get("min") or {}).get("c"),
        (item.get("day") or {}).get("c"),
        (item.get("prevDay") or {}).get("c"),
    )
    return next((p for p in candidates if p), None)


def _snapshot_timestamp(item: dict) -> float | None:
    """Unix seconds. lastTrade.t and updated are nanoseconds."""
    nanos = (item.get("lastTrade") or {}).get("t") or item.get("updated")
    return nanos / 1e9 if nanos else None
```

### Two modes

Per `MASSIVE_API.md`, the free tier cannot call the snapshot endpoint and only receives end-of-day data. Rather than fail, the source degrades:

| Mode | Used when | Endpoint | Interval | Result |
|---|---|---|---|---|
| `snapshot` | Paid plans | `/v2/snapshot/locale/us/markets/stocks/tickers?tickers=...` | `poll_interval` (15 s) | Live or 15-minute delayed prices |
| `eod` | Free tier (snapshot returned 403) | `/v2/aggs/grouped/locale/us/market/stocks/{date}` | 1 hour | Most recent daily close; static |

The switch happens on the first 403 and is permanent for the life of the process. In `eod` mode prices do not move, so there is no flashing and sparklines are flat, and `prev_close` is not populated so daily change % is blank. That is an honest representation of what the free tier provides. **Users without a paid Massive plan should leave `MASSIVE_API_KEY` unset and use the simulator.**

### Error handling

| Condition | Behaviour |
|---|---|
| 401 (bad key) | Logged each poll; cache stays empty; the app runs but has no prices |
| 403 on snapshot | Switch to `eod` mode |
| 429 (rate limited) | Logged; poll skipped; retried next interval |
| 5xx, timeout, network error | Logged; cache keeps last prices; retried next interval |
| Ticker unknown to Massive | Omitted from the response; never enters the cache; `cache.get_price()` returns `None` |
| Price fields are `0` (pre-market reset) | Skipped down the fallback chain to `prevDay.c`; if all are zero the ticker is not updated |

### Why httpx and not the official `massive` client

The official client is synchronous (urllib3), so every call would need `asyncio.to_thread`. It raises a single `BadResponse` exception for every non-200 with no status code attribute, which makes the 403 → `eod` switch awkward. It also retries 429s automatically, which wastes budget on the free tier. FinAlly uses two endpoints; calling them directly is less code than adapting the client.

---

## 8. SSE Streaming — `stream.py`

```python
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

STREAM_INTERVAL = 0.5  # seconds between cache checks
KEEPALIVE_INTERVAL = 15.0  # seconds of silence before sending a comment line


def create_stream_router(cache: PriceCache) -> APIRouter:
    router = APIRouter(prefix="/api/stream", tags=["stream"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _price_events(cache, request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return router


async def _price_events(cache: PriceCache, request: Request) -> AsyncIterator[str]:
    yield "retry: 1000\n\n"  # EventSource reconnects after 1s
    last_version = -1
    idle = 0.0
    while not await request.is_disconnected():
        if cache.version != last_version:
            last_version = cache.version
            payload = {t: u.to_dict() for t, u in cache.get_all().items()}
            yield f"data: {json.dumps(payload)}\n\n"
            idle = 0.0
        elif idle >= KEEPALIVE_INTERVAL:
            yield ": keepalive\n\n"
            idle = 0.0
        await asyncio.sleep(STREAM_INTERVAL)
        idle += STREAM_INTERVAL
```

### Wire format

Each event is one JSON object keyed by ticker, containing every ticker currently in the cache:

```
data: {"AAPL": {"ticker": "AAPL", "price": 190.56, "previous_price": 190.52, "timestamp": 1791371689.68, "direction": "up", "prev_close": 190.0, "day_change_percent": 0.2947}, "GOOGL": {...}}
```

- A new client's first event is a full snapshot, so the UI can render immediately.
- A ticker removed from the watchlist disappears from the next event; one added appears in the next event.
- With the simulator there is an event every 500 ms. With Massive there is one per poll, with keepalive comments in between so proxies do not close an idle connection.

Frontend consumption:

```typescript
const source = new EventSource("/api/stream/prices");
source.onmessage = (event) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(event.data);
  // flash on direction, append price to the ticker's sparkline buffer
};
source.onerror = () => { /* readyState CONNECTING => yellow dot; CLOSED => red */ };
```

---

## 9. Public API and App Wiring

`app/market/__init__.py`:

```python
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "MarketDataSource",
    "PriceCache",
    "PriceUpdate",
    "create_market_data_source",
    "create_stream_router",
]
```

Application lifecycle (`app/main.py`):

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()
market_source = create_market_data_source(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tickers = get_watchlist_tickers() + get_position_tickers()  # from SQLite
    await market_source.start(sorted(set(tickers)))
    yield
    await market_source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

The source must track the **union of watchlist tickers and held positions**. A user can remove a ticker from the watchlist while still holding shares; the position still needs a live price for valuation and for selling.

### How downstream code uses it

```python
# Trade execution — fill at the current price
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"No price available for {ticker}")

# Portfolio valuation
total = cash + sum(
    pos.quantity * (price_cache.get_price(pos.ticker) or pos.avg_cost) for pos in positions
)

# Watchlist: add
await market_source.add_ticker(ticker)       # price is normally cached on return
if price_cache.get_price(ticker) is None:    # Massive did not recognise the symbol
    await market_source.remove_ticker(ticker)
    raise HTTPException(404, f"Unknown ticker {ticker}")

# Watchlist: remove — keep pricing it if the user still holds it
if not has_position(ticker):
    await market_source.remove_ticker(ticker)

# After selling a position down to zero — stop pricing it if it is not watched
if not on_watchlist(ticker):
    await market_source.remove_ticker(ticker)
```

Unknown-ticker handling differs by source: Massive rejects symbols it does not know, while the simulator accepts any symbol and invents a price for it. That asymmetry is acceptable — the simulator has no notion of a real ticker — but routes should still validate the format (1–5 letters, optionally a dot suffix) before calling `add_ticker`.

---

## 10. Testing

| Test file | Covers |
|---|---|
| `test_cache.py` | First update is flat; direction up/down; rounding to cents; sticky `prev_close`; `version` increments on update and remove; `remove` of an unknown ticker is a no-op |
| `test_simulator.py` | See `MARKET_SIMULATOR.md` §8 |
| `test_massive.py` | Snapshot parsing and price fallback chain; ns → s timestamps; zero prices skipped; unknown tickers absent; 403 → `eod` mode with walk-back over a 403 day and an empty day; 429 and network errors leave the cache intact; `add_ticker` fetches only the new ticker; `stop` closes the client |
| `test_factory.py` | No key → simulator; whitespace key → simulator; key → Massive |

Massive tests inject a client built on `httpx.MockTransport`, so no network or API key is needed:

```python
import httpx

from app.market import PriceCache
from app.market.massive_client import MassiveDataSource


async def test_free_tier_falls_back_to_end_of_day():
    def handler(request: httpx.Request) -> httpx.Response:
        if "snapshot" in request.url.path:
            return httpx.Response(403, json={"status": "NOT_AUTHORIZED"})
        return httpx.Response(200, json={"results": [{"T": "AAPL", "c": 188.2, "t": 1791259200000}]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://test")
    cache = PriceCache()
    source = MassiveDataSource("key", cache, client=client)

    await source.start(["AAPL"])

    assert source.mode == "eod"
    assert cache.get_price("AAPL") == 188.2
    await source.stop()
```

Both implementations should also pass one shared conformance test, parametrised over the two sources: after `start(["AAPL"])` the cache has a price for AAPL; `add_ticker` then `remove_ticker` leaves the cache as it was; `stop()` twice does not raise.

---

## 11. Open Points for PLAN.md

1. **Free-tier wording.** PLAN.md §6 says the free tier polls every 15 seconds. It can, but it only ever receives the previous close. Suggest rewording to say that live Massive prices require a paid plan and the free tier yields static end-of-day prices.
2. **`MASSIVE_POLL_INTERVAL`** is a new optional environment variable; add it to §5 and `.env.example` if accepted.
3. **Daily change %.** PLAN.md §10 shows it in the watchlist but §6 does not list a reference price in the cache. This design adds `prev_close` to carry it.
