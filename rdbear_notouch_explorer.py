#!/usr/bin/env python3
"""
rdbear_notouch_explorer.py

Connects to Deriv using the NEW Options API connection layer (REST OTP
bootstrap - same pattern as deriv_multisymbol_bot.py / mc_expiryrange_notouch_bot.py)
and collects tick data on RDBEAR (Bear Market index) purely to characterize the
instrument BEFORE building a NOTOUCH bot on it with durations from 2 up to 60
minutes.

Why this matters for RDBEAR specifically:
    - RDBEAR (like RDBULL) only accepts MINUTE durations for NOTOUCH on Deriv -
      no tick durations - which is exactly the 2-60 min range you asked about.
    - RDBEAR is a synthetic index built with a structural drift (it trends by
      construction), so a NOTOUCH bot on it lives or dies on how well you
      estimate that drift and its volatility-scaling behaviour across
      different minute-durations - that's what this script measures directly
      from real historical + live ticks, rather than assuming.

What this script does (no trading, no proposals, no buys - pure data/analysis):
    1. Bootstraps deep tick history via `ticks_history`.
    2. Subscribes to live ticks and keeps collecting (Ctrl+C to stop early, or
       let it run for COLLECT_MINUTES).
    3. Appends every tick (epoch, quote) to a CSV so you have raw data to
       re-analyze later.
    4. Computes, from the real tick series:
         - tick rate (ticks/second) -> lets you convert "N minutes" to "N ticks"
         - EWMA volatility (per tick) and per-minute volatility scaling
         - drift per tick / per minute (RDBEAR's structural trend shows up here)
         - Hurst exponent (trending vs mean-reverting)
         - an EMPIRICAL (not simulated) touch-probability table: for each
           candidate duration in {2,3,5,10,15,20,30,45,60} minutes and each
           barrier width in {0.5,1.0,1.5,2.0,2.5,3.0} x sigma, slides a window
           of that length over the ACTUAL historical price path and measures
           what fraction of real historical windows would have touched a
           barrier that wide - both up-side and down-side, since RDBEAR's
           drift means the two sides behave very differently.
    5. Prints/saves a summary report (JSON) you can use to pick sane
       (duration, barrier) defaults for the live NOTOUCH bot, and re-prints
       the report every REPORT_EVERY_MINUTES while it keeps collecting.

Dependencies:
    pip install websockets numpy requests

Environment variables:
    DERIV_APP_ID          your app_id from a NEW developers.deriv.com application
                          (legacy app_ids, e.g. the old demo id 1089, do NOT
                          work with the new Options API)
    DERIV_API_TOKEN       API token for your Deriv account
    DERIV_ACCOUNT_TYPE    "demo" (default) or "real"
    DERIV_ACCOUNT_ID      optional; skips the accounts lookup
    RDBEAR_SYMBOL         symbol to analyze (default: RDBEAR)
    HISTORY_COUNT         ticks to bootstrap via ticks_history (default: 10000)
    COLLECT_MINUTES       how long to keep collecting live ticks (default: 60;
                          set to 0 to run until Ctrl+C)
    REPORT_EVERY_MINUTES  how often to re-print the analysis while collecting
                          (default: 5)

Run:
    python rdbear_notouch_explorer.py
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import random
import time
from collections import defaultdict, deque
from typing import Optional

import numpy as np
import requests
import websockets

# --------------------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------------------

APP_ID = os.getenv("DERIV_APP_ID", "")
API_TOKEN = os.getenv("DERIV_API_TOKEN", "")
ACCOUNT_TYPE = os.getenv("DERIV_ACCOUNT_TYPE", "demo").strip().lower()
ACCOUNT_ID = os.getenv("DERIV_ACCOUNT_ID") or None

SYMBOL = os.getenv("RDBEAR_SYMBOL", "RDBEAR")
HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "10000"))
COLLECT_MINUTES = float(os.getenv("COLLECT_MINUTES", "60"))       # 0 = run until Ctrl+C
REPORT_EVERY_MINUTES = float(os.getenv("REPORT_EVERY_MINUTES", "5"))

# NOTOUCH duration/barrier grid this analysis targets: 2-60 minutes (RDBEAR has
# no tick-duration option), representative candidates across that range.
DURATION_GRID_MIN = (2, 3, 5, 10, 15, 20, 30, 45, 60)
BARRIER_SIGMA_GRID = (0.5, 1.0, 1.5, 2.0, 2.5, 3.0)

TICK_CSV_PATH = f"{SYMBOL.lower()}_ticks.csv"
REPORT_JSON_PATH = f"{SYMBOL.lower()}_notouch_analysis.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rdbear_explorer")

# --------------------------------------------------------------------------------------
# DERIV API CLIENT - new Options API (REST OTP bootstrap, auto-reconnecting)
# Ported from deriv_multisymbol_bot.py's DerivClient.
# --------------------------------------------------------------------------------------

API_BASE = "https://api.derivws.com"
ACCOUNTS_PATH = "/trading/v1/options/accounts"
OTP_PATH = "/trading/v1/options/accounts/{account_id}/otp"


class DerivClient:
    """
    Client for the new Deriv Options API.

    Auth flow: REST GET .../accounts -> resolve account_id -> REST POST
    .../accounts/{id}/otp -> pre-authenticated WS URL. No `authorize` message
    is sent or needed; the OTP URL is already scoped to the account.

    OTP URLs are short-lived and single-use, so a fresh one is fetched on
    every connect AND every reconnect. After the first successful connect,
    this client auto-reconnects in the background with exponential backoff
    and calls `resubscribe_cb` (if set) so the caller can replay its tick
    subscription - a fresh OTP session has no memory of prior subscriptions.
    """

    HEARTBEAT_INTERVAL = 20
    RECONNECT_BASE = 2.0
    RECONNECT_CAP = 60.0

    def __init__(self, app_id, token, account_type="demo", account_id=None):
        self.app_id = app_id
        self.token = token
        self.account_type = account_type
        self.account_id = account_id
        self.ws = None
        self.req_id = 0
        self.pending = {}
        self.subscriptions = defaultdict(list)  # msg_type -> list[asyncio.Queue]
        self.account = None
        self.resubscribe_cb = None  # async callable(client), replayed after reconnect
        self._running = False
        self._reader_task = None
        self._ka_task = None

    # ---- REST bootstrap ----
    def _rest_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Deriv-App-ID": self.app_id,
            "Content-Type": "application/json",
        }

    def _resolve_account_id_sync(self):
        url = f"{API_BASE}{ACCOUNTS_PATH}"
        resp = requests.get(url, headers=self._rest_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        accounts = data.get("data", data) if isinstance(data, dict) else data
        if isinstance(accounts, dict):
            accounts = accounts.get("accounts", accounts.get("data", []))
        for acc in accounts:
            if acc.get("account_type") == self.account_type:
                acc_id = acc.get("account_id") or acc.get("id")
                if acc_id:
                    return acc_id
        raise RuntimeError(
            f"No '{self.account_type}' account found via {ACCOUNTS_PATH}. "
            f"Set DERIV_ACCOUNT_ID explicitly, or create one first via "
            f"POST {ACCOUNTS_PATH}. Accounts returned: {data}"
        )

    def _fetch_otp_url_sync(self):
        if not self.account_id:
            self.account_id = self._resolve_account_id_sync()
            log.info(f"Resolved {self.account_type} account_id = {self.account_id}")
        url = f"{API_BASE}{OTP_PATH.format(account_id=self.account_id)}"
        resp = requests.post(url, headers=self._rest_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        payload = data.get("data", data) if isinstance(data, dict) else data
        ws_url = payload.get("url")
        if not ws_url:
            raise RuntimeError(f"OTP response missing data.url: {data}")
        return ws_url

    async def _get_ws_url(self):
        return await asyncio.to_thread(self._fetch_otp_url_sync)

    # ---- connection lifecycle ----
    async def connect(self):
        """Connects once (raises on failure, so startup misconfiguration fails
        fast) then runs the supervisor loop forever in the background."""
        self._running = True
        await self._connect_once()
        asyncio.create_task(self._supervise())
        return self.account

    async def _connect_once(self):
        ws_url = await self._get_ws_url()
        self.ws = await websockets.connect(ws_url, ping_interval=None, close_timeout=5)
        # IMPORTANT: start the reader (and heartbeat) BEFORE sending anything.
        # `send()` blocks on a future only resolved by `_dispatch()`, which
        # only runs inside `_read_loop()`. If the reader isn't already
        # running, the handshake below times out forever.
        self._reader_task = asyncio.create_task(self._read_loop())
        self._ka_task = asyncio.create_task(self._heartbeat())
        bal = await self.send({"balance": 1})
        self.account = bal.get("balance", {})
        log.info(
            f"Connected ({self.account_type}). "
            f"loginid={self.account.get('loginid')} balance={self.account.get('balance')}"
        )

    async def _read_loop(self):
        try:
            async for message in self.ws:
                self._dispatch(json.loads(message))
        except (websockets.ConnectionClosed, OSError) as e:
            log.warning(f"[DerivClient] WS connection lost: {e}")

    async def _supervise(self):
        """Watches the current reader task; on disconnect, cleans up and
        reconnects with exponential backoff, restarting reader+heartbeat each
        time inside `_connect_once`."""
        while self._running:
            if self._reader_task is not None:
                await self._reader_task

            if self._ka_task is not None:
                self._ka_task.cancel()
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("Deriv WS disconnected"))
            self.pending.clear()
            self.ws = None

            if not self._running:
                break

            attempt = 0
            while self._running and self.ws is None:
                attempt += 1
                delay = min(
                    self.RECONNECT_BASE * (2 ** (attempt - 1)), self.RECONNECT_CAP
                ) + random.uniform(0, 1)
                log.warning(f"[DerivClient] Reconnecting in {delay:.1f}s (attempt {attempt})...")
                await asyncio.sleep(delay)
                try:
                    await self._connect_once()
                    if self.resubscribe_cb:
                        await self.resubscribe_cb(self)
                except Exception as e:
                    log.warning(f"[DerivClient] Reconnect attempt {attempt} failed: {e}")

    async def _heartbeat(self):
        try:
            while True:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                await self.ws.send(json.dumps({"ping": 1}))
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    def _dispatch(self, data):
        req_id = data.get("req_id")
        msg_type = data.get("msg_type")
        if msg_type == "ping":
            return
        if req_id is not None and req_id in self.pending:
            fut = self.pending.pop(req_id)
            if not fut.done():
                fut.set_result(data)
                return
        if msg_type in self.subscriptions:
            for q in self.subscriptions[msg_type]:
                q.put_nowait(data)

    async def send(self, request, timeout=20):
        self.req_id += 1
        rid = self.req_id
        request = dict(request)
        request["req_id"] = rid
        fut = asyncio.get_event_loop().create_future()
        self.pending[rid] = fut
        await self.ws.send(json.dumps(request))
        return await asyncio.wait_for(fut, timeout=timeout)

    def subscribe_channel(self, msg_type):
        q = asyncio.Queue()
        self.subscriptions[msg_type].append(q)
        return q

    async def close(self):
        self._running = False
        if self._ka_task:
            self._ka_task.cancel()
        if self._reader_task:
            self._reader_task.cancel()
        if self.ws:
            await self.ws.close()


async def fetch_history(client: DerivClient, symbol: str, count: int) -> list[tuple[float, float]]:
    """Fetch up to `count` ticks by paging backwards in time. Deriv's
    ticks_history API hard-caps each response at 1000 ticks regardless of the
    count parameter, so we make ceil(count/1000) sequential calls."""
    BATCH = 1000
    all_ticks: list[tuple[float, float]] = []
    end = "latest"

    while len(all_ticks) < count:
        resp = await client.send({
            "ticks_history": symbol,
            "count": BATCH,
            "end": end,
            "style": "ticks",
        })
        if resp.get("error"):
            log.error(f"ticks_history error: {resp['error']}")
            break
        history = resp.get("history", {})
        times = history.get("times", [])
        prices = history.get("prices", [])
        if not times:
            break

        batch = list(zip(times, prices))
        all_ticks = batch + all_ticks

        if len(batch) < BATCH:
            break

        earliest_epoch = int(times[0]) - 1
        end = earliest_epoch

    if len(all_ticks) > count:
        all_ticks = all_ticks[-count:]
    return all_ticks


# --------------------------------------------------------------------------------------
# DATA STORE + CSV LOGGING
# --------------------------------------------------------------------------------------

class TickStore:
    def __init__(self, symbol: str, csv_path: str):
        self.symbol = symbol
        self.csv_path = csv_path
        self.epochs: list[float] = []
        self.prices: list[float] = []
        self._csv_file = None
        self._csv_writer = None
        self._init_csv()

    def _init_csv(self):
        new_file = not os.path.exists(self.csv_path)
        self._csv_file = open(self.csv_path, "a", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        if new_file:
            self._csv_writer.writerow(["epoch", "quote"])

    def add(self, epoch: float, price: float, persist: bool = True):
        self.epochs.append(epoch)
        self.prices.append(price)
        if persist:
            self._csv_writer.writerow([epoch, price])
            self._csv_file.flush()

    def bulk_add_history(self, ticks: list[tuple[float, float]]):
        # historical bootstrap ticks are written once as a batch, not one flush per row
        for epoch, price in ticks:
            self.epochs.append(float(epoch))
            self.prices.append(float(price))
            self._csv_writer.writerow([epoch, price])
        self._csv_file.flush()

    def close(self):
        if self._csv_file:
            self._csv_file.close()

    @property
    def log_returns(self) -> np.ndarray:
        p = np.asarray(self.prices)
        if len(p) < 2:
            return np.asarray([])
        return np.diff(np.log(p))

    @property
    def tick_rate_per_sec(self) -> float:
        if len(self.epochs) < 2:
            return 1.0
        span = self.epochs[-1] - self.epochs[0]
        if span <= 0:
            return 1.0
        return (len(self.epochs) - 1) / span


# --------------------------------------------------------------------------------------
# ANALYSIS: EWMA vol, drift, Hurst, empirical touch-probability table
# --------------------------------------------------------------------------------------

def ewma_vol(returns: np.ndarray, lam: float = 0.94) -> float:
    if len(returns) == 0:
        return 0.0
    var = returns[0] ** 2
    for r in returns[1:]:
        var = lam * var + (1 - lam) * r * r
    return math.sqrt(var)


def hurst_exponent(returns: np.ndarray) -> float:
    """Variance-of-lagged-differences estimator. H ~ 0.5 = random walk,
    H > 0.5 trending, H < 0.5 mean-reverting."""
    if len(returns) < 100:
        return 0.5
    lags = range(2, 20)
    tau = [np.std(returns[lag:] - returns[:-lag]) for lag in lags]
    tau = [t if t > 0 else 1e-8 for t in tau]
    poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
    h = poly[0] * 2.0
    return float(np.clip(h, 0.05, 0.95))


def empirical_touch_table(prices: np.ndarray, tick_rate: float,
                           duration_grid_min=DURATION_GRID_MIN,
                           sigma_grid=BARRIER_SIGMA_GRID) -> list[dict]:
    """For each (duration_minutes, sigma_k) slides a window of that length
    (converted to ticks via the live-measured tick rate) over the REAL
    historical price path and measures what fraction of actual historical
    windows would have touched a barrier that wide, up-side and down-side
    separately (RDBEAR's drift makes these very different).

    This is empirical (real historical windows), not simulated - it tells you
    what actually happened on this instrument, which is the right first gut
    check before trusting a Monte Carlo model of it.
    """
    log_p = np.log(prices)
    n = len(log_p)
    results = []

    for dur_min in duration_grid_min:
        window_ticks = max(int(round(dur_min * 60 * tick_rate)), 2)
        if window_ticks >= n:
            continue

        n_windows = n - window_ticks
        # rolling max/min of (log_p - log_p[start]) within each window
        starts = log_p[:n_windows]
        # build windows via stride tricks for speed
        shape = (n_windows, window_ticks)
        strides = (log_p.strides[0], log_p.strides[0])
        windows = np.lib.stride_tricks.as_strided(log_p[: n_windows + window_ticks], shape=shape, strides=strides)
        rel = windows - starts[:, None]
        win_max = rel.max(axis=1)
        win_min = rel.min(axis=1)

        # local volatility estimate scaled to this window length, from returns
        # inside these same windows (median absolute step * sqrt(window), robust to jumps)
        local_returns = np.diff(log_p)
        sigma_tick = np.std(local_returns) if len(local_returns) > 1 else 1e-6
        sigma_window = sigma_tick * math.sqrt(window_ticks)

        for k in sigma_grid:
            dist = k * sigma_window
            touched_up = np.mean(win_max >= dist)
            touched_down = np.mean(win_min <= -dist)
            results.append({
                "duration_min": dur_min,
                "window_ticks": window_ticks,
                "sigma_k": k,
                "barrier_distance_log": round(float(dist), 6),
                "p_touch_upside": round(float(touched_up), 4),
                "p_touch_downside": round(float(touched_down), 4),
                "p_no_touch_upside": round(float(1 - touched_up), 4),
                "p_no_touch_downside": round(float(1 - touched_down), 4),
                "n_windows_sampled": int(n_windows),
            })
    return results


def build_report(store: TickStore) -> dict:
    prices = np.asarray(store.prices)
    returns = store.log_returns
    tick_rate = store.tick_rate_per_sec

    drift_per_tick = float(np.mean(returns)) if len(returns) else 0.0
    vol_per_tick = ewma_vol(returns)
    h = hurst_exponent(returns)

    report = {
        "symbol": SYMBOL,
        "generated_at": time.time(),
        "n_ticks": len(prices),
        "spot": float(prices[-1]) if len(prices) else None,
        "tick_rate_per_sec": round(tick_rate, 4),
        "avg_seconds_per_tick": round(1 / tick_rate, 3) if tick_rate > 0 else None,
        "drift_per_tick": drift_per_tick,
        "drift_per_minute_est": drift_per_tick * tick_rate * 60,
        "ewma_vol_per_tick": vol_per_tick,
        "hurst_exponent": h,
        "hurst_interpretation": (
            "trending (drift persists)" if h > 0.55 else
            "mean-reverting" if h < 0.45 else
            "close to random walk"
        ),
        "touch_table_2to60min": empirical_touch_table(prices, tick_rate),
    }
    return report


def print_report_summary(report: dict):
    print("\n" + "=" * 88)
    print(f"  {report['symbol']} ANALYSIS  (n_ticks={report['n_ticks']}, "
          f"spot={report['spot']})")
    print("=" * 88)
    print(f"  tick rate        : {report['tick_rate_per_sec']:.4f} ticks/sec "
          f"(~{report['avg_seconds_per_tick']}s/tick)")
    print(f"  drift/tick        : {report['drift_per_tick']:.8f}   "
          f"drift/min (est)   : {report['drift_per_minute_est']:.6f}")
    print(f"  EWMA vol/tick     : {report['ewma_vol_per_tick']:.8f}")
    print(f"  Hurst exponent    : {report['hurst_exponent']:.3f}  "
          f"({report['hurst_interpretation']})")
    print("-" * 88)
    print(f"  {'dur(min)':>8} {'sigma':>6} {'barrier(log)':>13} "
          f"{'P(notouch up)':>14} {'P(notouch down)':>16}")
    for row in report["touch_table_2to60min"]:
        print(f"  {row['duration_min']:>8} {row['sigma_k']:>6.1f} "
              f"{row['barrier_distance_log']:>13.5f} "
              f"{row['p_no_touch_upside']:>14.3f} {row['p_no_touch_downside']:>16.3f}")
    print("=" * 88 + "\n")


def save_report(report: dict, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(report, f, indent=2)
    os.replace(tmp, path)


# --------------------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------------------

async def main():
    if not APP_ID or not API_TOKEN:
        log.warning(
            "DERIV_APP_ID / DERIV_API_TOKEN are not both set. Set them as environment "
            "variables before running (legacy app_ids like 1089 do NOT work with the "
            "new Options API)."
        )

    client = DerivClient(APP_ID, API_TOKEN, ACCOUNT_TYPE, ACCOUNT_ID)
    await client.connect()

    store = TickStore(SYMBOL, TICK_CSV_PATH)

    log.info(f"Bootstrapping tick history for {SYMBOL} (target: {HISTORY_COUNT} ticks)...")
    history = await fetch_history(client, SYMBOL, HISTORY_COUNT)
    store.bulk_add_history(history)
    log.info(f"Loaded {len(store.prices)} historical ticks. "
             f"Tick rate so far: {store.tick_rate_per_sec:.4f}/sec")

    async def resubscribe(c: DerivClient):
        await c.send({"ticks": SYMBOL, "subscribe": 1})

    tick_queue = client.subscribe_channel("tick")
    client.resubscribe_cb = resubscribe
    await resubscribe(client)

    report = build_report(store)
    print_report_summary(report)
    save_report(report, REPORT_JSON_PATH)

    start = time.time()
    last_report = start
    deadline = None if COLLECT_MINUTES <= 0 else start + COLLECT_MINUTES * 60

    log.info(f"Collecting live ticks for {SYMBOL} "
             f"({'until Ctrl+C' if deadline is None else f'{COLLECT_MINUTES} minutes'})... "
             f"re-reporting every {REPORT_EVERY_MINUTES} min.")

    try:
        while deadline is None or time.time() < deadline:
            timeout = None
            if deadline is not None:
                timeout = max(deadline - time.time(), 0.1)
            try:
                data = await asyncio.wait_for(tick_queue.get(), timeout=timeout or 3600)
            except asyncio.TimeoutError:
                break

            tick = data.get("tick", {})
            if tick:
                store.add(float(tick["epoch"]), float(tick["quote"]))

            if time.time() - last_report >= REPORT_EVERY_MINUTES * 60:
                report = build_report(store)
                print_report_summary(report)
                save_report(report, REPORT_JSON_PATH)
                last_report = time.time()

    except KeyboardInterrupt:
        log.info("Stopped by user (Ctrl+C).")

    finally:
        report = build_report(store)
        print_report_summary(report)
        save_report(report, REPORT_JSON_PATH)
        store.close()
        await client.close()
        log.info(f"Final report saved to {REPORT_JSON_PATH}. Raw ticks saved to {TICK_CSV_PATH}.")


if __name__ == "__main__":
    asyncio.run(main())
