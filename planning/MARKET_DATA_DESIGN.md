# Market Data Backend — Design

Implementation-ready design for FinAlly's market data layer: a unified interface, a GBM simulator, a Massive (Polygon.io) REST poller, a shared price cache, and the SSE endpoint. Code lives in `backend/app/market/`.

This document is the consolidated reference. It matches the code already in `backend/app/market/` (see `MARKET_DATA_SUMMARY.md`) and calls out **Refinements** (§12) where the spec in `PLAN.md` needs something the current code does not yet provide. Earlier, longer drafts are in `planning/archive/`.

## Contents

1. Architecture & file layout
2. Data model — `PriceUpdate`
3. Price cache
4. Unified interface — `MarketDataSource`
5. Factory & configuration
6. Simulator (GBM)
7. Massive API client
8. SSE streaming
9. App integration (lifespan, trades, watchlist)
10. Error handling & edge cases
11. Testing
12. Refinements to the current code

---

## 1. Architecture & file layout

```
 MarketDataSource (ABC)
 ├── SimulatorDataSource ── GBMSimulator          (default)
 └── MassiveDataSource   ── massive.RESTClient    (MASSIVE_API_KEY set)
            │ writes every tick/poll
            ▼
        PriceCache  (in-memory, thread-safe, versioned)
            │ reads
            ├── GET /api/stream/prices     (SSE → browser EventSource)
            ├── POST /api/portfolio/trade  (fill price)
            ├── GET  /api/portfolio        (valuation)
            └── GET  /api/watchlist        (latest prices)
```

Key idea: **producers push into the cache; consumers only read the cache.** Nothing downstream knows or cares which source is active, and sources never return prices from method calls.

```
backend/app/market/
├── __init__.py      # public exports
├── models.py        # PriceUpdate (frozen dataclass)
├── cache.py         # PriceCache
├── interface.py     # MarketDataSource ABC
├── seed_prices.py   # seed prices, per-ticker sigma/mu, correlation constants
├── simulator.py     # GBMSimulator + SimulatorDataSource
├── massive_client.py# MassiveDataSource
├── factory.py       # create_market_data_source()
└── stream.py        # create_stream_router() — SSE
```

Public imports for the rest of the backend:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source
```

---

## 2. Data model — `PriceUpdate`

The only value that leaves the market layer. Immutable so it can be shared across threads and SSE clients without copying.

```python
# models.py
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float                       # price before this update (tick-to-tick)
    timestamp: float = field(default_factory=time.time)   # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:                 # drives the green/red flash
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker, "price": self.price,
            "previous_price": self.previous_price, "timestamp": self.timestamp,
            "change": self.change, "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

Example SSE payload entry:

```json
{"ticker":"AAPL","price":190.52,"previous_price":190.48,"timestamp":1760000000.5,
 "change":0.04,"change_percent":0.021,"direction":"up"}
```

> `change`/`change_percent` here are **tick-to-tick** (used for flashes). The watchlist's "daily change %" needs a separate baseline — see Refinement R1.

---

## 3. Price cache

Single source of truth for the latest price per ticker. A `threading.Lock` (not `asyncio.Lock`) is used because the Massive client does its blocking HTTP call in a worker thread and the SSE/trade handlers read from the event loop.

```python
# cache.py
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0                       # bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price      # first tick => "flat"
            upd = PriceUpdate(ticker, round(price, 2), round(previous_price, 2), ts)
            self._prices[ticker] = upd
            self._version += 1
            return upd

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        u = self.get(ticker)
        return u.price if u else None

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)           # shallow copy; values are immutable

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:                   # SSE change detection
        with self._lock:                        # (review item 3.4: read under lock)
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Usage:

```python
cache = PriceCache()
cache.update("AAPL", 190.00)
cache.update("AAPL", 190.25)
u = cache.get("AAPL")            # PriceUpdate(price=190.25, previous_price=190.0, ...)
u.direction                      # "up"
cache.get_price("ZZZZ")          # None  -> callers must handle a miss
```

Memory is O(tickers): only the latest price is stored. History (sparklines, P&L chart) is built client-side from SSE / from `portfolio_snapshots`.

---

## 4. Unified interface — `MarketDataSource`

```python
# interface.py
from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Pushes prices into a PriceCache on its own schedule."""

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Start the background task. Call once. Cache must have data for
        `tickers` (or be about to) when this returns where the source allows."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Idempotent."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Track a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking and evict from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

Lifecycle:

```python
cache = PriceCache()
source = create_market_data_source(cache)
await source.start(["AAPL", "GOOGL"])
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")
await source.stop()
```

Contract notes for implementers of a third source:
- Tickers are uppercase; normalize at the API boundary (route handlers) *and* defensively in the source.
- Never raise out of the background loop; log and continue.
- `remove_ticker` must also call `cache.remove(ticker)`.

---

## 5. Factory & configuration

```python
# factory.py
import logging, os
from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

| Setting | Where | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | env | empty | non-empty ⇒ Massive; else simulator |
| `update_interval` | `SimulatorDataSource` | `0.5` s | simulator tick |
| `poll_interval` | `MassiveDataSource` | `15.0` s | free tier = 5 req/min; paid can use 2–5 s |
| `event_probability` | `GBMSimulator` | `0.001` | shock chance per ticker per tick |
| `dt` | `GBMSimulator` | ≈ 8.48e-8 | 0.5 s as a fraction of a trading year |
| SSE interval / retry | `stream.py` | `0.5` s / `1000` ms | |

Optional (R4): `MASSIVE_POLL_INTERVAL` env var, parsed in the factory, so paid-tier users can tune polling without code changes.

---

## 6. Simulator (GBM)

### 6.1 Math

Each tick, every price evolves by geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

- `μ` annualized drift, `σ` annualized volatility (per ticker, `seed_prices.py`).
- `dt = 0.5 / (252·6.5·3600) ≈ 8.48e-8` — tiny, so per-tick moves are sub-cent but accumulate realistically (AAPL σ=0.22 ⇒ ≈0.0064 % std per tick).
- `Z` are **correlated** standard normals: draw independent `ε ~ N(0, I)`, then `Z = L·ε` where `L = cholesky(C)` and `C` is the correlation matrix. The exponential form keeps prices strictly positive.
- **Events:** with probability `0.001` per ticker per tick, multiply price by `1 ± U(0.02, 0.05)`. With 10 tickers at 2 ticks/s that is roughly one dramatic move every ~50 s.

### 6.2 Seed data

```python
# seed_prices.py
SEED_PRICES = {"AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
               "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
               "V": 280.00, "NFLX": 600.00}

TICKER_PARAMS = {                       # sigma = annual vol, mu = annual drift
    "AAPL": {"sigma": 0.22, "mu": 0.05}, "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05}, "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03}, "NVDA":  {"sigma": 0.40, "mu": 0.08},
    "META": {"sigma": 0.30, "mu": 0.05}, "JPM":   {"sigma": 0.18, "mu": 0.04},
    "V":    {"sigma": 0.17, "mu": 0.04}, "NFLX":  {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}          # tickers added at runtime

CORRELATION_GROUPS = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR, INTRA_FINANCE_CORR = 0.6, 0.5
CROSS_GROUP_CORR, TSLA_CORR = 0.3, 0.3                # TSLA trades on its own
```

Unknown tickers (user adds `PYPL`) get a random seed price in $50–$300 and `DEFAULT_PARAMS`, correlated at `CROSS_GROUP_CORR` with everything. All pairwise correlations are ≤ 0.6 with unit diagonal, so the matrix is positive-definite and Cholesky always succeeds.

### 6.3 `GBMSimulator`

```python
# simulator.py (core)
import math, random
import numpy as np
from .seed_prices import *

class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR

    def __init__(self, tickers, dt=DEFAULT_DT, event_probability=0.001):
        self._dt, self._event_prob = dt, event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance one tick; returns {ticker: new_price}. Hot path (every 500 ms)."""
        n = len(self._tickers)
        if n == 0:
            return {}
        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z
        out = {}
        for i, t in enumerate(self._tickers):
            mu, sigma = self._params[t]["mu"], self._params[t]["sigma"]
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[t] *= math.exp(drift + diffusion)
            if random.random() < self._event_prob:
                self._prices[t] *= 1 + random.uniform(0.02, 0.05) * random.choice([-1, 1])
            out[t] = round(self._prices[t], 2)
        return out

    def add_ticker(self, ticker):
        if ticker in self._prices: return
        self._add_ticker_internal(ticker); self._rebuild_cholesky()

    def remove_ticker(self, ticker):
        if ticker not in self._prices: return
        self._tickers.remove(ticker); del self._prices[ticker]; del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker): return self._prices.get(ticker)
    def get_tickers(self):       return list(self._tickers)

    def _add_ticker_internal(self, ticker):
        if ticker in self._prices: return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self):
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None; return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                corr[i, j] = corr[j, i] = self._pairwise_correlation(self._tickers[i], self._tickers[j])
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1, t2):
        if "TSLA" in (t1, t2): return TSLA_CORR
        tech, fin = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech: return INTRA_TECH_CORR
        if t1 in fin and t2 in fin:   return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Example:

```python
sim = GBMSimulator(["AAPL", "MSFT", "JPM"])
sim.step()      # {'AAPL': 190.01, 'MSFT': 419.98, 'JPM': 195.0}
sim.add_ticker("PYPL")   # random $50-300 seed, default params
```

### 6.4 `SimulatorDataSource`

Wraps the simulator in an asyncio task. Seeds the cache at `start()` and at `add_ticker()` so every tracked ticker has a price immediately (important for trading a just-added ticker and for the first SSE frame).

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache, update_interval=0.5, event_probability=0.001):
        self._cache, self._interval, self._event_prob = price_cache, update_interval, event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers):
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for t in tickers:
            self._cache.update(t, self._sim.get_price(t))
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
        self._task = None

    async def add_ticker(self, ticker):
        if self._sim:
            self._sim.add_ticker(ticker)
            self._cache.update(ticker, self._sim.get_price(ticker))

    async def remove_ticker(self, ticker):
        if self._sim: self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self):
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self):
        while True:
            try:
                if self._sim:
                    for t, p in self._sim.step().items():
                        self._cache.update(t, p)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

`step()` is pure CPU and takes microseconds for ~10 tickers, so it runs directly on the event loop.

---

## 7. Massive API client

### 7.1 Facts (from `archive/MASSIVE_API.md`)

- Package `massive` (`uv add massive`), `from massive import RESTClient`; base URL `https://api.massive.com`; auth handled by the client.
- One call returns **all** requested tickers — essential for the free tier (5 req/min):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key=KEY)
snaps = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)
for s in snaps:
    s.ticker                  # "AAPL"
    s.last_trade.price        # 125.07           <- price we use
    s.last_trade.timestamp    # 1675190399000    <- Unix MILLISECONDS
    s.day.previous_close      # 129.61           <- baseline for daily change (R1)
    s.day.change_percent      # -3.50
```

- REST equivalent: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL`.
- Errors: 401 bad key, 403 plan lacks endpoint, 429 rate limit, 5xx (client retries 3×).
- When markets are closed `last_trade.price` is the last (possibly after-hours) trade, so prices simply stop moving — expected.

### 7.2 `MassiveDataSource`

The `RESTClient` is synchronous, so each call runs in `asyncio.to_thread`; this is why the cache needs a real `threading.Lock`.

```python
# massive_client.py
import asyncio, logging
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key, price_cache, poll_interval=15.0):
        self._api_key, self._cache, self._interval = api_key, price_cache, poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers):
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = [t.upper() for t in tickers]
        await self._poll_once()                                   # cache warm before first SSE frame
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
        self._task = self._client = None

    async def add_ticker(self, ticker):
        t = ticker.upper().strip()
        if t not in self._tickers:
            self._tickers.append(t)          # appears on next poll (<= poll_interval)

    async def remove_ticker(self, ticker):
        t = ticker.upper().strip()
        self._tickers = [x for x in self._tickers if x != t]
        self._cache.remove(t)

    def get_tickers(self): return list(self._tickers)

    async def _poll_loop(self):
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self):
        if not self._tickers or not self._client:
            return
        try:
            snaps = await asyncio.to_thread(self._fetch_snapshots)
            for s in snaps:
                try:
                    self._cache.update(s.ticker, s.last_trade.price,
                                       timestamp=s.last_trade.timestamp / 1000.0)
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping %s: %s", getattr(s, "ticker", "???"), e)
        except Exception as e:                # 401/403/429/network: log, retry next interval
            logger.error("Massive poll failed: %s", e)

    def _fetch_snapshots(self):
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS, tickers=list(self._tickers))
```

Behavioural notes:
- **Latency of adds:** a newly added ticker has no price for up to `poll_interval`. Trades on it get a clear 400 until then (§10). Refinement R3 triggers an immediate poll on `add_ticker`.
- **Thread-safety of `_tickers`:** `_fetch_snapshots` copies the list (`list(self._tickers)`) before use, since `add/remove` run on the loop thread while the poll runs in a worker thread.
- **Invalid tickers:** Massive omits unknown symbols from the response; they simply never get a price. R3 describes surfacing that to the user.
- Prices from a poll land in the cache in a burst, then the SSE shows `flat`/stale data between polls — the frontend keeps working unchanged.

---

## 8. SSE streaming

`GET /api/stream/prices` — long-lived `text/event-stream`. Every ~500 ms the server checks `cache.version`; if it changed it sends **one event holding every ticker**.

```python
# stream.py
import asyncio, json, logging
from collections.abc import AsyncGenerator
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

def create_stream_router(price_cache: PriceCache) -> APIRouter:
    router = APIRouter(prefix="/api/stream", tags=["streaming"])   # R5: router per call, not module-level

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive",
                     "X-Accel-Buffering": "no"},
        )
    return router


async def _generate_events(cache, request, interval=0.5) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"                      # EventSource reconnect delay
    last_version = -1
    try:
        while True:
            if await request.is_disconnected():
                break
            v = cache.version
            if v != last_version:
                last_version = v
                prices = cache.get_all()
                if prices:
                    yield f"data: {json.dumps({t: u.to_dict() for t, u in prices.items()})}\n\n"
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
```

Wire format (one `data:` line per event):

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.52,"previous_price":190.48,"timestamp":1760000000.5,"change":0.04,"change_percent":0.021,"direction":"up"},"GOOGL":{...}}

```

Why this shape: one event per tick keeps the client simple (one `onmessage`, one state merge); version check avoids re-sending identical Massive data every 500 ms; the browser's built-in reconnection satisfies "SSE resilience" in `PLAN.md`.

Frontend consumption sketch (for the Frontend Engineer):

```ts
const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");
es.onerror = () => setStatus(es.readyState === EventSource.CONNECTING ? "reconnecting" : "disconnected");
es.onmessage = (e) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(e.data);
  for (const u of Object.values(prices)) {
    appendSparklinePoint(u.ticker, u.price);
    flash(u.ticker, u.direction);          // add class, remove after ~500 ms
  }
};
```

---

## 9. App integration

### 9.1 Lifespan

```python
# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import PriceCache, create_market_data_source, create_stream_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = PriceCache()
    source = create_market_data_source(cache)
    app.state.price_cache, app.state.market_source = cache, source

    tickers = await db.get_all_tracked_tickers()   # watchlist ∪ tickers with open positions
    await source.start(tickers)
    yield
    await source.stop()

app = FastAPI(title="FinAlly", lifespan=lifespan)
# Router needs the cache instance: build it at import time with a module-level cache,
# or include it inside lifespan via app.include_router(create_stream_router(cache)).
```

Simplest wiring: create `price_cache = PriceCache()` at module level in `main.py`, pass it to `create_stream_router(price_cache)` at import, and have the lifespan use the same instance. Static file mounting (`/*`) must be registered **after** all `/api` routers.

Dependencies:

```python
def get_price_cache(request: Request) -> PriceCache:   return request.app.state.price_cache
def get_market_source(request: Request) -> MarketDataSource: return request.app.state.market_source
```

### 9.2 Trade execution (consumer)

```python
@router.post("/api/portfolio/trade")
async def trade(req: TradeRequest, cache: PriceCache = Depends(get_price_cache)):
    price = cache.get_price(req.ticker.upper())
    if price is None:
        raise HTTPException(400, f"Price not yet available for {req.ticker}. Try again shortly.")
    # validate cash / shares, update positions + cash, insert trade, snapshot portfolio
```

The same cache lookup is used by the LLM chat path so manual and AI trades fill identically.

### 9.3 Watchlist coordination

```
POST /api/watchlist {ticker}
  → validate/normalize (uppercase, regex ^[A-Z.]{1,6}$)
  → INSERT watchlist row
  → await source.add_ticker(ticker)      # simulator seeds cache now; Massive on next poll
  → return {ticker, price: cache.get_price(ticker)}   # price may be null for Massive

DELETE /api/watchlist/{ticker}
  → DELETE watchlist row
  → if no open position: await source.remove_ticker(ticker)
```

**Open positions must stay priced.** If a user removes a ticker they still hold, keep it in the source (so portfolio valuation and the heatmap stay live). On startup, track `watchlist ∪ positions`. The archived draft has the full route sketch.

The LLM's `watchlist_changes` go through these same functions so the source and DB never drift.

---

## 10. Error handling & edge cases

| Situation | Behaviour |
|---|---|
| Empty watchlist at start | Both sources start idle; SSE sends nothing until a ticker is added |
| Trade on ticker with no cached price | HTTP 400 "price not yet available" (Massive just-added ticker) |
| Invalid/expired Massive key (401) | Logged each poll; no prices; app stays up. Surface via `/api/health` (R6) |
| Rate limit 429 | Logged; retried next interval. Keep free-tier interval ≥ 12–15 s |
| Network failure | Logged; last cached prices keep being served (stale, but valid) |
| Simulator step exception | Logged with traceback, loop continues |
| Client disconnects from SSE | Generator exits on `is_disconnected()` / cancellation |
| Duplicate `add_ticker` / unknown `remove_ticker` | No-ops |
| Float noise | Cache rounds to 2 dp; GBM is multiplicative so prices stay > 0 |

---

## 11. Testing

Existing suite (73 tests, `backend/tests/market/`): models, cache, simulator, simulator source, factory, massive (mocked). Run with `cd backend && uv run --extra dev pytest -v`.

Representative tests (and the gaps worth filling):

```python
# GBM sanity — all 10 default tickers factorize and stay positive
def test_default_universe_cholesky_and_positive():
    sim = GBMSimulator(list(SEED_PRICES))
    for _ in range(1000):
        assert all(p > 0 for p in sim.step().values())

# Cache — direction and version
def test_cache_direction_and_version():
    c = PriceCache(); v0 = c.version
    c.update("AAPL", 100.0); u = c.update("AAPL", 101.0)
    assert u.direction == "up" and u.previous_price == 100.0 and c.version == v0 + 2

# Cache — concurrency (gap)
def test_cache_threads():
    c = PriceCache()
    ts = [threading.Thread(target=lambda: [c.update("A", 1.0) for _ in range(1000)]) for _ in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert c.version == 8000

# Massive — timestamp ms→s and malformed snapshots (mock the fetch, not the thread)
async def test_massive_poll(monkeypatch):
    cache = PriceCache(); src = MassiveDataSource("k", cache); src._client = object(); src._tickers = ["AAPL"]
    snap = SimpleNamespace(ticker="AAPL", last_trade=SimpleNamespace(price=191.5, timestamp=1_700_000_000_000))
    bad = SimpleNamespace(ticker="X", last_trade=None)
    monkeypatch.setattr(src, "_fetch_snapshots", lambda: [snap, bad])
    await src._poll_once()
    assert cache.get("AAPL").timestamp == 1_700_000_000.0 and "X" not in cache

# SSE (gap) — httpx ASGITransport + app.include_router(create_stream_router(cache))
async def test_sse_first_frame():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    app = FastAPI(); app.include_router(create_stream_router(cache))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        async with c.stream("GET", "/api/stream/prices") as r:
            assert r.headers["content-type"].startswith("text/event-stream")
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    assert "AAPL" in json.loads(line[5:]); break
```

E2E (Playwright, `test/`) runs against the simulator; assert prices change within a few seconds and that the page reconnects after the SSE connection is dropped.

---

## 12. Refinements to the current code

Small changes that close gaps between `PLAN.md` and the implementation. None alter the interface; all are additive.

**R1 — Daily change % for the watchlist.** `PLAN.md` asks for "daily change %" but `PriceUpdate.change_percent` is tick-to-tick. Add an optional baseline to the cache:

```python
# cache.py
self._baselines: dict[str, float] = {}           # day-open / previous-close per ticker

def set_baseline(self, ticker, price): 
    with self._lock: self._baselines.setdefault(ticker, price)
```

- Simulator: baseline = seed price at first sight (i.e. "session open").
- Massive: baseline = `snap.day.previous_close`, refreshed each poll (it changes at the daily rollover).
- Add `day_change_percent` to `to_dict()` (or have `PriceCache.get_all()` return it) computed as `(price − baseline) / baseline · 100`.

**R2 — Case normalization.** Uppercase and strip tickers in `SimulatorDataSource.add_ticker/remove_ticker` as Massive already does, so `"aapl"` can never create a second simulator entry.

**R3 — Massive add/unknown tickers.** In `add_ticker`, after appending, fire `asyncio.create_task(self._poll_once())` (guarded against overlapping polls and the free-tier rate limit) so new tickers price within ~1 s. After each poll, compare the response against `self._tickers`; tickers missing from the response for N consecutive polls are logged as "no data (invalid symbol?)" and exposed via `source.unpriced_tickers()` so the watchlist route can return a clear error.

**R4 — Poll interval from env.** `MASSIVE_POLL_INTERVAL` (seconds, default 15) read in `create_market_data_source`.

**R5 — Router per call.** `stream.py` currently defines a module-level `router`; create it inside `create_stream_router` (shown in §8) so calling the factory twice (tests) does not double-register `/prices`.

**R6 — Health reporting.** Give sources a lightweight `status()` (`{"source": "simulator"|"massive", "last_update": ts, "last_error": str|None}`) and include it in `GET /api/health`, so a bad Massive key is visible instead of silent.

**R7 — Tests.** Add the cache-concurrency, 10-ticker Cholesky, and SSE first-frame tests from §11, and make Massive tests patch `_fetch_snapshots` (not the thread) so they do not depend on network or the `massive` import path.

Implementation order if tackled: R5 → R2 → R1 (+ frontend wiring) → R4 → R3 → R6 → R7.
