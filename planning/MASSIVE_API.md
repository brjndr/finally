# Massive API Reference (formerly Polygon.io)

Research notes on the Massive REST API for retrieving realtime and end-of-day stock prices for multiple tickers. This is the reference for the `MassiveDataSource` described in `MARKET_INTERFACE.md`.

Researched 2026-10-07 from the official docs at `massive.com/docs` and the source of the official Python client (`github.com/massive-com/client-python`). Nothing here was exercised with a live API key; the only live call made was an unauthenticated request to confirm the base URL and error shape. Items that still need confirming with a real key are listed in [Section 9](#9-unverified-items).

---

## 1. Summary — What Matters for FinAlly

| Question | Answer |
|---|---|
| Base URL | `https://api.massive.com` (legacy `https://api.polygon.io` still works with the same keys) |
| Auth | `Authorization: Bearer <key>` header, or `?apiKey=<key>` query parameter |
| Best endpoint for many tickers, live prices | **Full Market Snapshot**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...` — one call, all tickers |
| Best endpoint for many tickers, end of day | **Daily Market Summary** (grouped daily): `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` — one call, whole market |
| Free tier (Stocks Basic) | 5 calls/minute, **end-of-day data only**, **no snapshot access** |
| Paid tiers | Unlimited calls; 15-minute delayed (Starter, Developer) or realtime (Advanced, Business) |
| Python client | `pip install -U massive`, `from massive import RESTClient` (synchronous, urllib3-based) |

> **Important: this contradicts PLAN.md §6.** The plan says "Free tier (5 calls/min): poll every 15 seconds". Polling at that rate fits the rate limit, but the free tier cannot call the snapshot endpoints at all, and every endpoint it *can* call returns end-of-day data. A free key therefore yields one static closing price per ticker that changes once per day. Live-moving prices from Massive require a paid plan (Starter at $29/mo for 15-minute delayed, Advanced at $199/mo for realtime). The design in `MARKET_INTERFACE.md` handles this by falling back to end-of-day prices when the snapshot is refused, but the simulator remains the right default for anyone without a paid key.

---

## 2. Plans and Limits

| Plan | Price | API calls | Recency | History | Snapshots | WebSockets |
|---|---|---|---|---|---|---|
| Stocks Basic | Free | 5 / minute | End of day | 2 years | No | No |
| Stocks Starter | $29/mo | Unlimited | 15-min delayed | 5 years | Yes | Yes |
| Stocks Developer | $79/mo | Unlimited | 15-min delayed | 10 years | Yes | Yes |
| Stocks Advanced | $199/mo | Unlimited | Realtime | 20+ years | Yes | Yes |
| Stocks Business | Custom | Unlimited | Realtime | All | Yes (+ FMV) | Yes |

Endpoint access by plan:

| Endpoint | Basic | Starter / Developer | Advanced / Business |
|---|---|---|---|
| Full Market Snapshot (`/v2/snapshot/.../tickers`) | **Not included** | 15-min delayed | Realtime |
| Single Ticker Snapshot (`/v2/snapshot/.../tickers/{ticker}`) | **Not included** | 15-min delayed | Realtime |
| Unified Snapshot (`/v3/snapshot`) | **Not included** | 15-min delayed | Realtime |
| Daily Market Summary (`/v2/aggs/grouped/...`) | End of day | 15-min delayed | Realtime |
| Previous Day Bar (`/v2/aggs/ticker/{t}/prev`) | End of day | 15-min delayed | Realtime |
| Daily Ticker Summary (`/v1/open-close/{t}/{date}`) | End of day | 15-min delayed | Realtime |
| Market Status (`/v1/marketstatus/now`) | Yes | Yes | Yes |

---

## 3. Authentication and Conventions

```bash
# Header (preferred — keeps the key out of URLs and logs)
curl -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v2/aggs/ticker/AAPL/prev"

# Query parameter
curl "https://api.massive.com/v2/aggs/ticker/AAPL/prev?apiKey=$MASSIVE_API_KEY"
```

Response conventions:

- JSON body with a top-level `status` (`"OK"`, `"DELAYED"`, `"ERROR"`, ...) and `request_id`.
- Results under `results` for most endpoints; the v2 snapshot endpoints use `tickers` (multi) or `ticker` (single) instead.
- Paginated endpoints return a `next_url`; follow it with the same auth. Neither endpoint FinAlly needs is paginated.
- Field names are terse single letters in the v2 APIs (`o`, `h`, `l`, `c`, `v`, `vw`, `t`, `n`).

Errors:

| HTTP | Meaning | Body |
|---|---|---|
| 401 | Missing or invalid key | `{"status":"ERROR","request_id":"...","error":"API Key was not provided"}` (verified) |
| 403 | Plan not entitled to this endpoint or to this data recency | `{"status":"NOT_AUTHORIZED","request_id":"...","message":"You are not entitled to this data. ..."}` |
| 404 | Unknown ticker (single-ticker endpoints) | `{"status":"NOT_FOUND", ...}` |
| 429 | Rate limit exceeded (Basic: more than 5 calls/minute) | `{"status":"ERROR","error":"You've exceeded the maximum requests per minute..."}` |

Timestamps are inconsistent across fields — check units every time:

| Field | Unit |
|---|---|
| Aggregate bar `t` (grouped daily, prev, snapshot `min.t`) | Unix **milliseconds** |
| Snapshot `updated`, `lastTrade.t`, `lastQuote.t` | Unix **nanoseconds** |
| Market status `serverTime` | RFC 3339 string |

---

## 4. Realtime Prices for Multiple Tickers

### 4.1 Full Market Snapshot (recommended)

```
GET /v2/snapshot/locale/us/markets/stocks/tickers
```

| Parameter | Type | Notes |
|---|---|---|
| `tickers` | string | Comma-separated, case-insensitive. Omit for the whole market (10,000+ tickers). |
| `include_otc` | boolean | Default `false`. |

One call returns every requested ticker, so the call count is independent of watchlist size. This is the endpoint FinAlly polls.

```bash
curl -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT"
```

Response (sample from the docs):

```json
{
  "count": 1,
  "status": "OK",
  "tickers": [
    {
      "ticker": "BCAT",
      "todaysChange": -0.124,
      "todaysChangePerc": -0.601,
      "updated": 1605192894630916600,
      "day":     { "o": 20.64, "h": 20.64, "l": 20.506, "c": 20.506, "v": 37216, "vw": 20.616 },
      "min":     { "o": 20.506, "h": 20.506, "l": 20.506, "c": 20.506, "v": 5000, "vw": 20.5105,
                   "av": 37216, "n": 1, "t": 1684428600000 },
      "prevDay": { "o": 20.79, "h": 21, "l": 20.5, "c": 20.63, "v": 292738, "vw": 20.6939 },
      "lastTrade": { "p": 20.506, "s": 2416, "t": 1605192894630916600, "x": 4,
                     "i": "71675577320245", "c": [14, 41] },
      "lastQuote": { "p": 20.5, "s": 13, "P": 20.6, "S": 22, "t": 1605192959994246100 }
    }
  ]
}
```

| Field | Meaning |
|---|---|
| `ticker` | Symbol |
| `day` | Today's bar so far: `o` open, `h` high, `l` low, `c` latest close, `v` volume, `vw` VWAP |
| `min` | Most recent minute bar; also `av` accumulated volume, `n` trade count, `t` start time (ms) |
| `prevDay` | Previous session's bar. `prevDay.c` is the previous close — the base for daily change % |
| `lastTrade` | Most recent trade: `p` price, `s` size, `t` time (ns), `x` exchange id, `c` condition codes. Plan-dependent |
| `lastQuote` | Most recent NBBO: `p` bid, `s` bid size, `P` ask, `S` ask size, `t` time (ns). Plan-dependent |
| `todaysChange`, `todaysChangePerc` | Change versus previous close, absolute and percent |
| `updated` | Last update time (ns) |

Behaviour to design around:

- **Unknown tickers are silently omitted.** Requesting `AAPL,NOTREAL` returns one entry and a 200. Compare the returned set against the requested set to detect bad symbols.
- **`lastTrade` and `lastQuote` may be absent** depending on plan. Use a fallback chain for "current price": `lastTrade.p` → `min.c` → `day.c` → `prevDay.c`.
- **Daily reset.** Snapshot data is cleared at 3:30 AM ET and repopulates as exchanges report, from as early as 4:00 AM ET. Between the reset and the first trade, `day` and `min` are zero-filled — treat `0` as missing, never as a price.
- **Outside market hours** the snapshot keeps returning the last values; prices simply stop changing.
- **Delayed plans** return 15-minute-old data with `status: "DELAYED"`.

### 4.2 Unified Snapshot (v3)

```
GET /v3/snapshot?ticker.any_of=AAPL,GOOGL,MSFT&limit=250
```

A newer cross-asset endpoint. `ticker.any_of` takes up to 250 comma-separated tickers; `limit` is at most 250 (default 10 — set it explicitly). Results come back with readable field names (illustrative values; field names taken from the client's `UniversalSnapshot` models):

```json
{
  "status": "OK",
  "request_id": "...",
  "results": [
    {
      "ticker": "AAPL",
      "type": "stocks",
      "name": "Apple Inc.",
      "market_status": "open",
      "session": { "price": 190.12, "change": 1.02, "change_percent": 0.54,
                   "open": 189.3, "high": 190.5, "low": 188.9, "close": 190.12,
                   "previous_close": 189.1, "volume": 31245678 },
      "last_trade":  { "price": 190.12, "size": 100, "sip_timestamp": 1700000000000000000 },
      "last_quote":  { "bid": 190.11, "ask": 190.13, "bid_size": 2, "ask_size": 3 },
      "last_minute": { "open": 190.0, "high": 190.2, "low": 189.9, "close": 190.12, "volume": 15234 }
    }
  ]
}
```

Unknown tickers return an entry with `error` and `message` fields instead of being dropped, which makes validation easier. It has the same plan requirements as the v2 snapshot. FinAlly uses the v2 endpoint because its shape is the most widely documented and has been stable for years; v3 is a reasonable alternative, with the caveat that it is paginated above 250 tickers.

### 4.3 Single Ticker Snapshot

```
GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}
```

Same object as one element of the full snapshot, under a top-level `ticker` key. Not needed when the multi-ticker call is available.

### 4.4 Last Trade

```
GET /v2/last/trade/{ticker}
```

Returns the single most recent trade (`results.p` price, `results.t` time in ns). One call per ticker and not available on Basic — not useful for a watchlist.

### 4.5 WebSocket (not used)

`wss://socket.massive.com/stocks` (realtime) or `wss://delayed.massive.com/stocks` (15-minute delayed) streams trades (`T.AAPL`), quotes (`Q.AAPL`) and per-second/minute aggregates (`A.AAPL`, `AM.AAPL`). PLAN.md chooses REST polling deliberately; this is noted only for completeness.

---

## 5. End-of-Day Prices for Multiple Tickers

### 5.1 Daily Market Summary / Grouped Daily (recommended)

```
GET /v2/aggs/grouped/locale/us/market/stocks/{date}
```

| Parameter | Type | Notes |
|---|---|---|
| `date` | path, `YYYY-MM-DD` | Trading day to fetch |
| `adjusted` | boolean | Split-adjusted. Default `true` |
| `include_otc` | boolean | Default `false` |

Returns a daily bar for **every** US stock in one call (roughly 10,000 results, around 1 MB). There is no server-side ticker filter — filter client-side. Available on every plan including Basic, which makes it the only practical way to price a whole watchlist inside a 5 calls/minute budget.

```bash
curl -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-10-06"
```

Response (illustrative values):

```json
{
  "adjusted": true,
  "queryCount": 3,
  "resultsCount": 3,
  "request_id": "...",
  "status": "OK",
  "results": [
    { "T": "AAPL", "o": 189.3, "h": 191.05, "l": 188.9, "c": 190.12,
      "v": 52164500, "vw": 190.02, "n": 612345, "t": 1791259200000 }
  ]
}
```

| Field | Meaning |
|---|---|
| `T` | Ticker |
| `o`, `h`, `l`, `c` | Open, high, low, close |
| `v`, `vw`, `n` | Volume, VWAP, trade count |
| `t` | Bar start (Unix ms) |

Behaviour to design around:

- **Weekends and holidays** return `200` with `resultsCount: 0` and no `results` key.
- **The current day on Basic** is refused (403) until the session's end-of-day data has been published. To find the latest available close, start at yesterday and step backwards until a day returns results.
- Each step costs one call, so cap the walk-back (five attempts covers any long weekend) and cache the answer.

### 5.2 Previous Day Bar

```
GET /v2/aggs/ticker/{ticker}/prev
```

The last completed session's bar for one ticker. No date arithmetic needed, but one call per ticker — ten tickers take two minutes on Basic.

```json
{
  "ticker": "AAPL", "status": "OK", "adjusted": true, "queryCount": 1, "resultsCount": 1,
  "request_id": "6a7e466379af0a71039d60cc78e72282",
  "results": [
    { "T": "AAPL", "o": 115.55, "h": 117.59, "l": 114.13, "c": 115.97,
      "v": 131704427, "vw": 116.3058, "t": 1605042000000 }
  ]
}
```

### 5.3 Daily Ticker Summary (open/close)

```
GET /v1/open-close/{ticker}/{date}
```

One ticker, one date, with pre-market and after-hours prices. Flat response (no `results` wrapper):

```json
{
  "status": "OK", "symbol": "AAPL", "from": "2023-01-09",
  "open": 324.66, "high": 326.2, "low": 322.3, "close": 325.12,
  "volume": 26122646, "preMarket": 324.5, "afterHours": 322.1
}
```

### 5.4 Custom Bars (history)

```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

Historical bars for one ticker, e.g. `/range/1/day/2026-09-01/2026-10-06` or `/range/5/minute/...`. Query parameters: `adjusted`, `sort` (`asc`/`desc`), `limit` (max 50,000). Same bar fields as above. Not needed by the current plan (charts accumulate from the SSE stream), but it is the endpoint to use if the main chart ever needs to backfill history.

---

## 6. Market Status

```
GET /v1/marketstatus/now
```

Available on all plans.

```json
{
  "market": "extended-hours",
  "earlyHours": false,
  "afterHours": true,
  "serverTime": "2020-11-10T17:37:37-05:00",
  "exchanges": { "nasdaq": "extended-hours", "nyse": "extended-hours", "otc": "closed" },
  "currencies": { "crypto": "open", "fx": "open" }
}
```

`market` is `"open"`, `"closed"` or `"extended-hours"`. Optional use: slow polling down while the market is closed. Not required for the first build.

---

## 7. Python Code Examples

### 7.1 Direct REST with httpx (the approach FinAlly uses)

FinAlly needs two endpoints. Calling them directly with `httpx.AsyncClient` is async-native (no thread pool needed inside FastAPI), adds no dependency beyond `httpx`, and is trivial to mock in tests with `httpx.MockTransport`.

```python
import asyncio
import os

import httpx

BASE_URL = "https://api.massive.com"


def make_client(api_key: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=10.0,
    )


async def fetch_snapshots(client: httpx.AsyncClient, tickers: list[str]) -> dict[str, dict]:
    """Live (or 15-min delayed) snapshot for many tickers in one call. Paid plans only."""
    resp = await client.get(
        "/v2/snapshot/locale/us/markets/stocks/tickers",
        params={"tickers": ",".join(tickers)},
    )
    resp.raise_for_status()
    return {item["ticker"]: item for item in resp.json().get("tickers", [])}


def current_price(snapshot: dict) -> float | None:
    """Best available price. Zero means 'not populated yet', not a real price."""
    candidates = (
        (snapshot.get("lastTrade") or {}).get("p"),
        (snapshot.get("min") or {}).get("c"),
        (snapshot.get("day") or {}).get("c"),
        (snapshot.get("prevDay") or {}).get("c"),
    )
    return next((p for p in candidates if p), None)


async def fetch_grouped_daily(client: httpx.AsyncClient, date: str) -> dict[str, dict]:
    """End-of-day bars for the whole market on one date (YYYY-MM-DD). All plans."""
    resp = await client.get(f"/v2/aggs/grouped/locale/us/market/stocks/{date}")
    resp.raise_for_status()
    return {bar["T"]: bar for bar in resp.json().get("results", [])}


async def main() -> None:
    async with make_client(os.environ["MASSIVE_API_KEY"]) as client:
        snapshots = await fetch_snapshots(client, ["AAPL", "GOOGL", "MSFT"])
        for ticker, snap in snapshots.items():
            print(ticker, current_price(snap), "prev close", snap["prevDay"]["c"])

        bars = await fetch_grouped_daily(client, "2026-10-06")
        for ticker in ("AAPL", "GOOGL", "MSFT"):
            print(ticker, "closed at", bars[ticker]["c"])


asyncio.run(main())
```

Handling the errors that matter:

```python
try:
    snapshots = await fetch_snapshots(client, tickers)
except httpx.HTTPStatusError as exc:
    status = exc.response.status_code
    if status == 401:
        ...  # bad key: log once, keep serving stale prices
    elif status == 403:
        ...  # plan lacks snapshots: switch to end-of-day mode
    elif status == 429:
        ...  # rate limited: skip this cycle, try again next interval
    else:
        ...  # 5xx: transient, retry next interval
except httpx.TransportError:
    ...      # network failure / timeout: retry next interval
```

### 7.2 Official Python client

```bash
uv add massive        # or: pip install -U massive   (Python 3.9+)
```

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from the environment if api_key is omitted.
client = RESTClient(api_key="YOUR_KEY")

# Realtime: snapshot for several tickers in one call  ->  list[TickerSnapshot]
for snap in client.get_snapshot_all("stocks", tickers=["AAPL", "GOOGL", "MSFT"]):
    price = snap.last_trade.price if snap.last_trade else snap.day.close
    print(snap.ticker, price, snap.prev_day.close, snap.todays_change_percent)

# Realtime: one ticker  ->  TickerSnapshot
snap = client.get_snapshot_ticker("stocks", "AAPL")

# End of day: whole market for a date  ->  list[GroupedDailyAgg]
closes = {bar.ticker: bar.close for bar in client.get_grouped_daily_aggs("2026-10-06")}

# End of day: previous session for one ticker  ->  list[PreviousCloseAgg]
prev = client.get_previous_close_agg("AAPL")[0]
print(prev.close, prev.timestamp)

# End of day: one ticker, one date  ->  DailyOpenCloseAgg
day = client.get_daily_open_close_agg("AAPL", "2026-10-06")
print(day.open, day.close, day.after_hours)

# History: iterator of Agg, pagination handled automatically
bars = list(client.list_aggs("AAPL", 1, "day", "2026-09-01", "2026-10-06", limit=50000))
```

Method → endpoint map:

| Client method | Endpoint | Returns |
|---|---|---|
| `get_snapshot_all(market_type, tickers=None)` | `/v2/snapshot/locale/us/markets/stocks/tickers` | `list[TickerSnapshot]` |
| `get_snapshot_ticker(market_type, ticker)` | `/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}` | `TickerSnapshot` |
| `list_universal_snapshots(ticker_any_of=[...])` | `/v3/snapshot` | iterator of `UniversalSnapshot` |
| `get_grouped_daily_aggs(date)` | `/v2/aggs/grouped/locale/us/market/stocks/{date}` | `list[GroupedDailyAgg]` |
| `get_previous_close_agg(ticker)` | `/v2/aggs/ticker/{ticker}/prev` | `list[PreviousCloseAgg]` |
| `get_daily_open_close_agg(ticker, date)` | `/v1/open-close/{ticker}/{date}` | `DailyOpenCloseAgg` |
| `list_aggs(ticker, multiplier, timespan, from_, to)` | `/v2/aggs/ticker/{ticker}/range/...` | iterator of `Agg` |
| `get_last_trade(ticker)` | `/v2/last/trade/{ticker}` | `LastTrade` |
| `get_market_status()` | `/v1/marketstatus/now` | `MarketStatus` |

`TickerSnapshot` fields: `ticker`, `day`, `min`, `prev_day`, `last_trade`, `last_quote`, `todays_change`, `todays_change_percent`, `updated`. Bar models (`Agg`, `GroupedDailyAgg`, `PreviousCloseAgg`) expose `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`.

Client behaviour worth knowing:

- `RESTClient(api_key, connect_timeout=10.0, read_timeout=10.0, retries=3, pagination=True, trace=False, verbose=False)`.
- **Synchronous.** It uses urllib3. Inside an async app, wrap calls with `await asyncio.to_thread(client.get_snapshot_all, "stocks", tickers)`.
- Retries 413, 429, 499 and 5xx automatically (3 attempts, 0.1 s backoff factor). On Basic, automatic 429 retries burn rate-limit budget — pass `retries=0` there.
- Any non-200 response raises `massive.exceptions.BadResponse` with the raw body as the message; there is no status-code attribute, so telling a 403 from a 429 means parsing that string. This is the main reason FinAlly uses httpx directly.
- `raw=True` on any method returns the unparsed `urllib3.HTTPResponse`.
- `trace=True` logs request URLs and response headers with the key redacted.

---

## 8. Polling Guidance

| Plan | Endpoint | Interval | Calls/min | What the user sees |
|---|---|---|---|---|
| Basic (free) | Grouped daily | 1 hour | far below 5 | Yesterday's close; static |
| Starter / Developer | Snapshot | 15 s (down to ~5 s) | 4–12 | 15-minute delayed prices |
| Advanced / Business | Snapshot | 2–5 s | 12–30 | Realtime prices |

- One snapshot call covers the whole watchlist, so the interval is the only lever on call volume.
- Aggregates on paid plans update about once a second; polling faster than 1–2 s returns duplicates.
- A realtime key outside US market hours (9:30–16:00 ET, plus extended hours 4:00–20:00 ET) returns unchanging prices. That is correct behaviour, not a bug.

---

## 9. Unverified Items

Confirm these with a real key before relying on them:

1. **403 body for a Basic key on the snapshot endpoint.** The `NOT_AUTHORIZED` shape in Section 3 is the long-standing Polygon behaviour; the Massive docs state only that Basic has no access. The design keys off the HTTP status alone, so the body shape is not load-bearing.
2. **Grouped daily for the current date on Basic.** Expected: 403 until end-of-day data is published. The walk-back logic copes with either a 403 or an empty result.
3. **`lastTrade` presence on Starter.** The docs call it "plan-dependent" without a matrix. The price fallback chain makes this harmless.
4. **Details recalled from the pre-rebrand Polygon API rather than read from the current docs:** the exact 404 and 429 response bodies, and the WebSocket hostnames in Section 4.5. None of them affect the design.
5. **Snapshot reset time.** The docs say 3:30 AM ET; a docstring in the Python client says midnight. Treating zero prices as missing covers both.

---

## 10. Sources

- [Full Market Snapshot](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Unified Snapshot](https://massive.com/docs/rest/stocks/snapshots/unified-snapshot)
- [Daily Market Summary](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary)
- [Previous Day Bar](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)
- [Daily Ticker Summary](https://massive.com/docs/rest/stocks/aggregates/daily-ticker-summary)
- [Market Status](https://massive.com/docs/rest/stocks/market-operations/market-status)
- [REST Quickstart](https://massive.com/docs/rest/quickstart)
- [Pricing](https://massive.com/pricing)
- [Docs index for LLMs](https://massive.com/docs/llms.txt)
- [Official Python client](https://github.com/massive-com/client-python)
