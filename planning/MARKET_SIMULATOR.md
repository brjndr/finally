# Market Simulator

The approach and code structure for simulating stock prices in FinAlly. The simulator is the default market data source, used whenever `MASSIVE_API_KEY` is unset. It implements the `MarketDataSource` interface from `MARKET_INTERFACE.md` and needs no network access and no dependencies beyond the Python standard library.

The code below was run as written. Measured over 20,000 ticks it reproduced the configured correlations (0.60 / 0.50 / 0.30 targets measured as 0.60 / 0.50 / 0.30), the configured volatility (TSLA 0.50 measured as 0.50), and the expected event rate.

---

## 1. Goals

From PLAN.md §6:

- Prices follow geometric Brownian motion with per-ticker drift and volatility
- Updates every ~500 ms
- Correlated moves across tickers (tech stocks move together)
- Occasional sudden 2–5% "events" for drama
- Realistic starting prices
- Runs in-process as a background task

Beyond those, two design goals: the math must be testable without asyncio or a clock, and adding a ticker at runtime must work for any symbol.

---

## 2. Structure

Two classes in `backend/app/market/simulator.py`, plus a configuration module:

| Piece | Responsibility |
|---|---|
| `seed_prices.py` | Constants only: starting prices, per-ticker drift and volatility, sector membership, correlation levels |
| `GBMSimulator` | Pure math. Holds current prices; `step()` advances every ticker one tick and returns the new prices. No asyncio, no cache, no wall clock; accepts a seeded `random.Random` for deterministic tests |
| `SimulatorDataSource` | The `MarketDataSource` implementation. Owns a `GBMSimulator` and an asyncio task that calls `step()` every 500 ms and writes the results to the `PriceCache` |

```
SimulatorDataSource ──owns──▶ GBMSimulator ──reads──▶ seed_prices
        │  every 0.5s: step()
        ▼
    PriceCache ──▶ SSE, trades, portfolio
```

---

## 3. The Math

### Geometric Brownian motion

Each tick, every price is multiplied by a random factor:

```
S(t + dt) = S(t) · exp( (μ − σ²/2) · dt  +  σ · √dt · Z )
```

| Symbol | Meaning |
|---|---|
| `S(t)` | Current price |
| `μ` | Annualised drift (expected return), e.g. `0.05` |
| `σ` | Annualised volatility, e.g. `0.22` |
| `dt` | Tick length as a fraction of a trading year |
| `Z` | Standard normal random draw, correlated across tickers |

The exponential form means prices can never reach zero or go negative, and percentage moves are independent of price level — a $500 stock and a $50 stock with the same σ have the same percentage volatility.

### Time step

`dt` converts a 500 ms tick into trading-year units so that μ and σ can be quoted as familiar annual figures:

```
trading seconds per year = 252 days × 6.5 hours × 3600 = 5,896,800
dt = 0.5 / 5,896,800 ≈ 8.48 × 10⁻⁸
```

What that produces:

| Ticker | σ | Typical move per tick | Typical move per hour |
|---|---|---|---|
| AAPL ($190) | 0.22 | ±0.006% ≈ 1.2¢ | ±0.5% |
| TSLA ($250) | 0.50 | ±0.015% ≈ 3.6¢ | ±1.2% |
| JPM ($195) | 0.18 | ±0.005% ≈ 1.0¢ | ±0.4% |

Moves are realistic for a live market: a cent or two per tick. Because prices are rounded to cents, a fraction of ticks leave a quiet stock unchanged (direction `"flat"`, no flash), which looks like a real tape. The simulator runs continuously and has no notion of market hours.

Drift is negligible at this timescale (5% a year is about 0.0000004% per tick); it is included for correctness and so that long-running sessions trend gently upward.

### Correlation

Real stocks move together. The simulator draws one independent standard normal per ticker, then mixes them with the Cholesky factor of the correlation matrix:

```
Z_correlated = L · Z_independent        where  L · Lᵀ = C
```

`C` is built from sector membership:

| Pair | Correlation |
|---|---|
| Same ticker | 1.0 |
| Both tech (AAPL, GOOGL, MSFT, AMZN, NVDA, META, NFLX) | 0.6 |
| Both finance (JPM, V) | 0.5 |
| Anything else: cross-sector, TSLA, tickers added at runtime | 0.3 |

TSLA is deliberately in its own group: it trades on its own news and should not track the tech block tightly. The baseline of 0.3 gives a visible "whole market is up/down" effect.

This block structure is always positive definite (each within-group value is at least the cross-group value and all are below 1), so the Cholesky factorisation cannot fail for any combination of tickers. The factor is recomputed whenever a ticker is added or removed. That is an O(n³) operation on a matrix with one row per ticker — microseconds at watchlist sizes — and each tick's mixing is O(n²). This is why the simulator needs no numpy.

### Events

After the GBM step, each ticker independently has a 0.1% chance per tick of a shock: the price is multiplied by `1 ± U(2%, 5%)` with a random sign. With 10 tickers at two ticks per second that is roughly one event somewhere on the watchlist every 50 seconds — frequent enough to notice in a demo, rare enough to feel like news. The shock is permanent; GBM continues from the new level.

---

## 4. Configuration — `seed_prices.py`

```python
"""Static configuration for the simulator. No logic here."""

SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 130.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 650.00,
}

# Annualised drift (mu) and volatility (sigma) per ticker.
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"mu": 0.05, "sigma": 0.22},
    "GOOGL": {"mu": 0.05, "sigma": 0.25},
    "MSFT": {"mu": 0.05, "sigma": 0.20},
    "AMZN": {"mu": 0.05, "sigma": 0.28},
    "TSLA": {"mu": 0.03, "sigma": 0.50},
    "NVDA": {"mu": 0.08, "sigma": 0.40},
    "META": {"mu": 0.05, "sigma": 0.30},
    "JPM": {"mu": 0.04, "sigma": 0.18},
    "V": {"mu": 0.04, "sigma": 0.17},
    "NFLX": {"mu": 0.05, "sigma": 0.35},
}
DEFAULT_PARAMS: dict[str, float] = {"mu": 0.05, "sigma": 0.25}

SECTORS: dict[str, str] = {
    "AAPL": "tech",
    "GOOGL": "tech",
    "MSFT": "tech",
    "AMZN": "tech",
    "NVDA": "tech",
    "META": "tech",
    "NFLX": "tech",
    "JPM": "finance",
    "V": "finance",
    "TSLA": "solo",
}

INTRA_SECTOR_CORR: dict[str, float] = {"tech": 0.6, "finance": 0.5}
CROSS_CORR = 0.3  # different sectors, "solo" tickers, and unknown tickers
```

Tickers not listed here — anything a user or the AI adds at runtime — get a random starting price between $50 and $300, the default drift and volatility, and the baseline 0.3 correlation with everything else.

---

## 5. Implementation — `simulator.py`

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CROSS_CORR,
    DEFAULT_PARAMS,
    INTRA_SECTOR_CORR,
    SECTORS,
    SEED_PRICES,
    TICKER_PARAMS,
)

logger = logging.getLogger(__name__)

# 252 trading days x 6.5 hours x 3600 seconds
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600


def correlation(a: str, b: str) -> float:
    if a == b:
        return 1.0
    sector = SECTORS.get(a)
    if sector is not None and sector == SECTORS.get(b):
        return INTRA_SECTOR_CORR.get(sector, CROSS_CORR)
    return CROSS_CORR


def cholesky(matrix: list[list[float]]) -> list[list[float]]:
    """Lower-triangular L with L @ L.T == matrix. Matrix must be positive definite."""
    n = len(matrix)
    lower = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            acc = matrix[i][j] - sum(lower[i][k] * lower[j][k] for k in range(j))
            lower[i][j] = math.sqrt(acc) if i == j else acc / lower[j][j]
    return lower


class GBMSimulator:
    """Pure price math: correlated geometric Brownian motion plus random shocks.

    No asyncio, no cache, no clock. step() advances every ticker by one tick.
    """

    def __init__(
        self,
        tickers: list[str],
        tick_seconds: float = 0.5,
        event_probability: float = 0.001,
        rng: random.Random | None = None,
    ) -> None:
        self._dt = tick_seconds / TRADING_SECONDS_PER_YEAR
        self._event_probability = event_probability
        self._rng = rng or random.Random()
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: list[list[float]] = []
        for ticker in tickers:
            self._add(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance one tick. Returns the new (unrounded) price of every ticker."""
        n = len(self._tickers)
        independent = [self._rng.gauss(0.0, 1.0) for _ in range(n)]
        sqrt_dt = math.sqrt(self._dt)
        for i, ticker in enumerate(self._tickers):
            z = sum(self._cholesky[i][k] * independent[k] for k in range(i + 1))
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
            log_return = (mu - 0.5 * sigma * sigma) * self._dt + sigma * sqrt_dt * z
            price = self._prices[ticker] * math.exp(log_return)
            if self._rng.random() < self._event_probability:
                shock = self._rng.uniform(0.02, 0.05) * self._rng.choice((-1, 1))
                price *= 1 + shock
                logger.debug("Simulator event: %s %+.2f%%", ticker, shock * 100)
            self._prices[ticker] = price
        return dict(self._prices)

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add(self, ticker: str) -> None:
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker) or self._rng.uniform(50.0, 300.0)
        self._params[ticker] = TICKER_PARAMS.get(ticker, DEFAULT_PARAMS)

    def _rebuild_cholesky(self) -> None:
        corr = [[correlation(a, b) for b in self._tickers] for a in self._tickers]
        self._cholesky = cholesky(corr)


class SimulatorDataSource(MarketDataSource):
    """Runs a GBMSimulator on a timer and writes each tick to the PriceCache."""

    def __init__(self, cache: PriceCache, update_interval: float = 0.5) -> None:
        self._cache = cache
        self._interval = update_interval
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers, tick_seconds=self._interval)
        for ticker in tickers:
            self._seed_cache(ticker)
        self._task = asyncio.create_task(self._run(), name="market-simulator")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim is None or ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        self._seed_cache(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim is not None:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    def _seed_cache(self, ticker: str) -> None:
        # The starting price doubles as "previous close" for daily change %.
        price = self._sim.get_price(ticker)
        self._cache.update(ticker, price, prev_close=round(price, 2))

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                for ticker, price in self._sim.step().items():
                    self._cache.update(ticker, price)
            except Exception:
                logger.exception("Simulator tick failed")
```

---

## 6. Behaviour Notes

- **Startup.** `start()` seeds the cache synchronously, so the first SSE event already carries a price for every ticker. The first GBM tick lands 500 ms later.
- **Daily change %.** There is no previous trading day, so each ticker's starting price is recorded as `prev_close`. The watchlist's change % therefore reads as "change since the app started".
- **Restart.** Prices reset to the seed values on every process start; simulator state is not persisted. Positions keep their recorded `avg_cost`, so unrealised P&L can jump across a restart. That is acceptable for a simulated environment and keeps the simulator stateless.
- **Adding a ticker.** Appears in the cache immediately at its seed (or random) price and joins the correlation matrix on the next tick.
- **Removing a ticker.** Dropped from the simulation and the cache; re-adding it later starts again from the seed price.
- **Failure isolation.** An exception inside a tick is logged and the loop continues.
- **Timing.** `asyncio.sleep(0.5)` drifts by the tick's own (sub-millisecond) run time. Exact cadence does not matter here.
- **Cost.** Ten tickers: about 55 multiplications and 10 `exp()` calls per tick. Fifty tickers is still far below a millisecond.

---

## 7. Tuning

| To change | Adjust |
|---|---|
| How fast prices tick | `update_interval` on `SimulatorDataSource` (`dt` follows automatically, so volatility per unit of wall-clock time is unchanged) |
| How lively a ticker is | Its `sigma` in `TICKER_PARAMS` |
| How lively everything is | Scale every `sigma`, or reduce `TRADING_SECONDS_PER_YEAR` to compress time |
| How often events fire | `event_probability` on `GBMSimulator` (per ticker, per tick) |
| How large events are | The `uniform(0.02, 0.05)` range in `step()` |
| How tightly stocks move together | `INTRA_SECTOR_CORR` and `CROSS_CORR`. Keep each sector value ≥ `CROSS_CORR` and < 1 to stay positive definite |

---

## 8. Testing

`GBMSimulator` takes a seeded `random.Random`, so every test is deterministic.

| Test | Assertion |
|---|---|
| Seed prices | A known ticker starts at its `SEED_PRICES` value; an unknown one starts in [50, 300] |
| Positivity | After 10,000 steps every price is > 0 |
| Volatility | With events off, the standard deviation of log returns divided by `√dt` is within 5% of the configured `sigma` |
| Correlation | Over 20,000 steps, AAPL–MSFT ≈ 0.6, JPM–V ≈ 0.5, AAPL–JPM ≈ 0.3 (±0.03) |
| Cholesky | `L · Lᵀ` reproduces the correlation matrix; factorisation succeeds with unknown tickers added |
| Events | With `event_probability=1.0` every step moves each price by 2–5% beyond the GBM move; with `0.0` no step exceeds 1% |
| Add / remove | Adding is idempotent; removing an unknown ticker is a no-op; `step()` returns exactly the current ticker set |
| Data source | After `start()` the cache holds every ticker at its seed price with `prev_close` set; `cache.version` advances after a short sleep; `add_ticker` / `remove_ticker` are reflected in the cache; `stop()` twice does not raise |

Example:

```python
import math
import random
import statistics

from app.market.simulator import GBMSimulator


def test_tech_stocks_are_correlated():
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0.0, rng=random.Random(1))
    previous = {t: sim.get_price(t) for t in sim.get_tickers()}
    returns = {t: [] for t in previous}
    for _ in range(20_000):
        prices = sim.step()
        for ticker, price in prices.items():
            returns[ticker].append(math.log(price / previous[ticker]))
        previous = prices

    assert abs(statistics.correlation(returns["AAPL"], returns["MSFT"]) - 0.6) < 0.03
    assert abs(statistics.correlation(returns["AAPL"], returns["JPM"]) - 0.3) < 0.03
```

Async tests for `SimulatorDataSource` should pass a short `update_interval` (for example `0.01`) so they finish quickly.
