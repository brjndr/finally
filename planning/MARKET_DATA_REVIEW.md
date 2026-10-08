# Market Data Backend — Code Review

**Date:** 2026-10-08
**Code reviewed:** `backend/app/market/` (9 modules, 349 statements) and `backend/tests/market/` (6 test modules), as of `origin/main` @ `5b39f1e`
**Docs read:** `PLAN.md`, `MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`, `REVIEW.md` (this branch), plus `MARKET_DATA_DESIGN.md`, `MARKET_DATA_SUMMARY.md` and `archive/MARKET_DATA_REVIEW.md` on `main`

## Verdict

The simulator path is sound and ready to build on: the GBM math is correct, measured volatility and correlations match their configured values, and the SSE endpoint streams correctly in a live server.

**The Massive path does not work.** With a real API key it would never write a single price, because the code reads an attribute that does not exist on the `massive` library's model. The 13 Massive tests pass only because they use `MagicMock`, which answers to any attribute name. This must be fixed before `MASSIVE_API_KEY` is supported.

There is also one gap against `PLAN.md` that affects the frontend: the stream carries no reference price, so the watchlist's "daily change %" cannot be computed.

## Scope caveat — read this first

**The backend is not on the current branch.** `start` (`ae49f4a`) has no `backend/` directory; `frontend/`, `test/` and `db/` are empty. The only implementation in the repository is on `origin/main`, so that is what was reviewed. It was exported to a temporary directory and tested there; nothing on `start` was changed apart from adding this file.

This matters because the two branches disagree about the design:

- The code on `main` dates from February 2026 and follows the design now in `planning/archive/` on `main`.
- The design docs on `start` (`MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`, written 2026-10-07) describe a different and better implementation that has not been built.

Section 4 lists the differences. A decision is needed on which is the target: bring the code up to the `start` docs, or accept the `main` code and fix its defects.

## 1. Test results

| Check | Result |
|---|---|
| `pytest` on Python 3.14.7 (uv's default pick) | **73 passed**, 0 failed, 9.1 s, 73 deprecation warnings |
| `pytest` on Python 3.12 (the Docker target) | **73 passed**, 0 failed, 8.5 s |
| `ruff check app/ tests/` | Clean |
| `uv sync --extra dev` | Clean install from the lockfile |
| SSE smoke test (minimal FastAPI app under uvicorn, read with `curl`) | Works — see below |

Coverage is 91% overall:

| Module | Coverage | Not covered |
|---|---|---|
| `models.py`, `cache.py`, `interface.py`, `factory.py`, `seed_prices.py` | 100% | |
| `simulator.py` | 98% | Duplicate guard (L149), exception branch in `_run_loop` (L268–269) |
| `massive_client.py` | 94% | Poll loop body (L85–87), the real API call (L125) |
| `stream.py` | **33%** | Everything except imports — the route and the generator have no tests |

The 94% for `massive_client.py` is misleading. The one uncovered line that matters, the real `get_snapshot_all` call, is the boundary where the critical bug sits.

**SSE smoke test.** There is no `main.py` yet, so a 15-line app was written to wire the cache, the simulator and the router together. The endpoint returned `200` with `content-type: text/event-stream`, sent `retry: 1000`, then one event roughly every 500 ms with all tickers and correct `direction` values. Removing a ticker through the source dropped it from the following events.

**Simulator statistics.** Measured over 100,000 steps with events off:

| Quantity | Configured | Measured |
|---|---|---|
| AAPL volatility | 0.22 | 0.2203 |
| TSLA volatility | 0.50 | 0.5017 |
| V volatility | 0.17 | 0.1696 |
| AAPL–MSFT correlation | 0.60 | 0.598 |
| JPM–V correlation | 0.50 | 0.500 |
| AAPL–JPM correlation | 0.30 | 0.303 |
| TSLA–NVDA correlation | 0.30 | 0.303 |
| Event rate per ticker per tick | 0.001 | 0.0011 |

One step for 10 tickers takes about 83 µs. About 20% of ticks leave a ticker's rounded price unchanged, which gives a realistic tape.

## 2. Defects

Every item marked *confirmed* was reproduced by running code against the implementation, not inferred from reading it.

### Critical

**C1. Massive never updates the cache with real data** — `massive_client.py:103` (*confirmed*)

```python
timestamp = snap.last_trade.timestamp / 1000.0
```

The `LastTrade` model in `massive` 2.2.0 (the locked version) has no `timestamp` attribute. Its fields are `sip_timestamp`, `participant_timestamp` and `trf_timestamp`. A real snapshot parsed with `TickerSnapshot.from_dict` and passed through `_poll_once` raises `AttributeError`, which the `except (AttributeError, TypeError)` at L110 catches and logs as "Skipping snapshot". That happens for every ticker on every poll, so the cache stays empty and the app has no prices.

The tests miss it because `_make_snapshot` builds a `MagicMock`, and a `MagicMock` has every attribute.

The unit is also wrong. `sip_timestamp` is Unix **nanoseconds**, so the fix is `snap.last_trade.sip_timestamp / 1e9`. `test_timestamp_conversion` asserts the millisecond conversion and needs correcting with it.

Fix the tests at the same time: build fixtures with `TickerSnapshot.from_dict(...)` from a real response body, as documented in `MASSIVE_API.md` §4.1, so the tests exercise the real model.

### High

**H1. Free-tier keys produce no prices at all** — `massive_client.py:24`, `:123` (by inspection; no live key available)

The docstring says "Free tier: 5 req/min → poll every 15s". Per `MASSIVE_API.md` §2, the free tier has no access to the snapshot endpoint and gets a 403. The code has no fallback, so a free key logs "Massive poll failed" every 15 seconds indefinitely. The `start`-branch design handles this by switching to end-of-day prices; none of that exists here. At minimum, detect the 403 and say so clearly once.

**H2. No price fallback when `lastTrade` is absent** — `massive_client.py:101` (*confirmed*)

`last_trade` is plan-dependent and can be `None`. A snapshot without it is skipped entirely, even though `min.c`, `day.c` and `prevDay.c` are available. `MASSIVE_API.md` §4.1 specifies the chain `lastTrade.p → min.c → day.c → prevDay.c`, treating `0` as missing.

**H3. No reference price for "daily change %"** — `models.py:23–28`

`PLAN.md` §10 requires the watchlist to show daily change %. `PriceUpdate.change_percent` is the change since the previous tick, which for the simulator is around 0.005%. Nothing in the payload lets the frontend compute a daily figure. The `start`-branch design adds a `prev_close` field for this purpose. This needs settling before frontend work begins, since it changes the SSE wire format.

### Medium

**M1. A ticker removed during an in-flight poll comes back permanently** — `massive_client.py:97–108` (*confirmed*)

If `remove_ticker` runs while `_fetch_snapshots` is in its worker thread, the returning snapshot writes the ticker back into the cache. It is no longer in `_tickers`, so nothing removes it again and it stays in the SSE stream until restart. Fix: skip any snapshot whose ticker is not in the current ticker set.

**M2. `PriceCache.remove` does not bump `version`** — `cache.py:59–62` (*confirmed*)

The SSE loop only sends when `version` changes. With the simulator the next tick hides this within 500 ms. With Massive, a removed ticker keeps appearing for up to a full poll interval. Removing the last ticker never produces an update at all.

**M3. `add_ticker` on Massive leaves the ticker unpriced for up to 15 s** — `massive_client.py:66–70`

No immediate fetch is made. A user who adds a ticker and trades it straight away, or an LLM response that adds and buys in one turn, will find `get_price()` returning `None`. There is also no way for the caller to learn that Massive does not recognise a symbol, so invalid tickers sit in the watchlist forever with no price.

**M4. Ticker normalisation is inconsistent** — `simulator.py:242`, `massive_client.py:41–46`, `:67` (*confirmed*)

`MassiveDataSource` upper-cases and strips in `add_ticker` and `remove_ticker` but not in `start`. `SimulatorDataSource` never normalises: adding `"aapl"` and `" AAPL "` after `"AAPL"` yields three separate tickers at three different prices. Pick one place for normalisation — the API route is the natural one — and make both sources behave the same.

**M5. `create_stream_router` registers on a module-level router** — `stream.py:17`, `:26` (*confirmed*)

Calling it twice returns the same object with `/api/stream/prices` registered twice, and the first cache wins. Harmless in production, but it will break the first person to write SSE tests with a fresh cache per test. Create the `APIRouter` inside the function. The previous review raised this as item 3.6; it was not fixed.

**M6. `stream.py` is untested** — 33% coverage

This is the only module the frontend talks to. `_generate_events` can be tested directly with a stub `request` whose `is_disconnected()` returns `False` then `True`; no server is needed.

**M7. The simulator cannot be seeded** — `simulator.py:84`, `:105`, `:151`

It draws from the global `np.random` and `random` modules. As a result there are no tests of the generated output: `test_pairwise_correlation_*` only checks the constants returned by a static method, and nothing checks volatility, realised correlation, or that events fire with the right size. The measurements in Section 1 show the behaviour is right today, but nothing would catch a regression. Accept an injected RNG, as `MARKET_SIMULATOR.md` §8 describes.

### Low

| # | Location | Issue |
|---|---|---|
| L1 | `massive_client.py:42` | `RESTClient` defaults to `retries=3` with 10 s timeouts. `start()` awaits the first poll, so an unreachable API can delay app startup, and automatic 429 retries waste rate-limit budget |
| L2 | `stream.py:51–87` | No keepalive comment. With Massive, and whenever the market is closed, the connection is silent between polls. Nothing is sent at all while the cache is empty (L80) |
| L3 | `simulator.py:219–223` | `update_interval` is not passed through to `dt`, so changing the tick rate changes volatility per second of wall-clock time |
| L4 | `simulator.py:219` | Calling `start()` twice orphans the first background task, which keeps writing to the cache (*confirmed*). The interface documents this as undefined; a guard is cheap |
| L5 | `tests/conftest.py:11` | `asyncio.DefaultEventLoopPolicy` is deprecated in Python 3.14 and removed in 3.16; it causes all 73 warnings. The fixture is unnecessary and can be deleted |
| L6 | `pyproject.toml` | `requires-python = ">=3.12"` with no `.python-version`, so uv selected 3.14 locally while Docker will use 3.12. Pin it |
| L7 | `pyproject.toml` | `rich` is a runtime dependency used only by `market_data_demo.py`; move it to a dev or demo extra |
| L8 | `cache.py:30` | `timestamp or time.time()` treats a timestamp of `0` as missing. Use `is not None` |
| L9 | `simulator.py:188` | Comment says "TSLA is in tech set"; it is not |
| L10 | `stream.py` | The `Connection: keep-alive` header is hop-by-hop and is ignored or rejected under HTTP/2; drop it |
| L11 | `test_simulator_source.py` | `test_exception_resilience` never injects an exception, so the branch it is named for is uncovered. `test_custom_event_probability` asserts nothing |
| L12 | `tests/market/*` | `@pytest.mark.asyncio` is redundant with `asyncio_mode = "auto"` |

## 3. What is done well

- **Architecture.** One writer, a shared cache, many readers. Consumers never await a network call for a price. The interface holds only lifecycle and ticker membership, which is all the two sources have in common.
- **GBM implementation.** Correct log-normal step with the `−σ²/2` correction, sensible `dt` derived from trading seconds per year, and a correlation matrix that is positive definite for any combination of tickers.
- **Failure isolation.** Both background loops catch exceptions and continue. `stop()` is idempotent on both sources.
- **Immediate seeding.** `start()` and the simulator's `add_ticker()` put a price in the cache before returning, so the first SSE event is complete.
- **SSE change detection.** The `version` counter avoids resending identical payloads between Massive polls.
- **`PriceUpdate`** is frozen and slotted, with `direction` derived rather than stored.
- **Factory.** Treats a whitespace-only key as unset, matching `PLAN.md` §5.

## 4. Implementation versus the design docs on `start`

| Area | Docs on `start` | Code on `main` |
|---|---|---|
| Massive HTTP client | `httpx.AsyncClient`, injectable for tests | Synchronous `massive.RESTClient` in `asyncio.to_thread` |
| Massive tests | `httpx.MockTransport` with real response bodies | `MagicMock` objects (the cause of C1 going unnoticed) |
| Free tier | 403 switches to end-of-day mode | Not handled (H1) |
| Price fallback chain | `lastTrade.p → min.c → day.c → prevDay.c` | `lastTrade.p` only (H2) |
| `prev_close` / `day_change_percent` | In the model, cache and wire format | Absent (H3) |
| `add_ticker` on Massive | Fetches the ticker immediately | Waits for the next poll (M3) |
| `cache.remove` | Bumps `version` | Does not (M2) |
| Cache locking | None; single event loop | `threading.Lock` (harmless, unnecessary) |
| Simulator dependencies | Standard library only | `numpy` |
| Simulator RNG | Injected `random.Random` | Global state (M7) |
| `dt` | Derived from `update_interval` | Fixed at 0.5 s (L3) |
| SSE keepalive | Comment line every 15 s | None (L2) |
| Stream router | Created per call | Module-level (M5) |
| Factory imports | Lazy | Eager; the simulator path imports `massive` |
| `MASSIVE_POLL_INTERVAL` | Supported | Not supported |
| Wire-format fields | `prev_close`, `day_change_percent` | `change`, `change_percent` (tick-to-tick) |
| Seed prices | NVDA 130, NFLX 650 | NVDA 800, NFLX 600 |
| Test files | `test_cache`, `test_simulator`, `test_massive`, `test_factory`, shared conformance test | Adds `test_models` and `test_simulator_source`; no conformance test |

`PLAN.md` §6 still says the free tier polls every 15 seconds. That should be reworded as `MARKET_INTERFACE.md` §11 suggests.

## 5. Recommended actions

**Before anything depends on Massive**

1. Fix C1: read `sip_timestamp` and divide by `1e9`.
2. Rebuild the Massive test fixtures from real response bodies via `TickerSnapshot.from_dict`.
3. Add the price fallback chain with zero treated as missing (H2).
4. Handle the free-tier 403 explicitly (H1).

**Before frontend work starts**

5. Decide the SSE wire format, specifically whether `prev_close` and `day_change_percent` are added (H3). This is the contract the frontend builds against.
6. Decide which design is the target (Section 4) and get the code onto the working branch. `start` and `main` have diverged: `start` is 1 commit ahead and 23 behind.

**Before the watchlist and trade routes are written**

7. Fix M1 to M5: the ghost-ticker race, the `version` bump on remove, immediate pricing on add, consistent normalisation, and the per-call router.

**Test quality**

8. Add tests for `stream.py` (M6).
9. Make the simulator seedable and add statistical tests for volatility, correlation and events (M7).
10. Add the shared conformance test over both sources, as `PLAN.md` §12 requires ("both implementations conform to the abstract interface").

**Housekeeping**

11. L1 to L12, as convenient.

## 6. Not verified

- Nothing was run against the live Massive API; no key was available. C1, H2 and M1 were reproduced against the installed library's real model classes. H1 rests on the plan matrix in `MASSIVE_API.md` §2, which that document itself flags as unconfirmed with a real key.
- The Docker build was not attempted; there is no `Dockerfile` yet.
- `market_data_demo.py` was not run.
- SSE behaviour behind a proxy and browser `EventSource` reconnection were not tested.
