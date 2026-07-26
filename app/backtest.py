from __future__ import annotations

import csv
import json
import logging
import math
import random
import sqlite3
import statistics
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from app.alpaca_client import AlpacaClient, is_likely_stock_symbol
from app.config import Settings
from app.research import quote_size_shares
from app.supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
UTC = timezone.utc
ET = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")

# REFERENCE ONLY: inherited from the 50% source package; execution is disabled in this fork
# until 25%-specific rules pass discovery, validation and sealed testing.
PREOPEN_RULE = {
    "rule_id": "PREOPEN_ACTIVITY_LIQUIDITY_VOLATILITY_V1",
    "decision_time_london": "14:00",
    "threshold": -0.007082378439967338,
    "components": {
        "log1p_trade_volume": {"mean": 8.18382400, "sd": 4.74614788, "sign": 1},
        "log1p_median_spread_bps": {"mean": 6.35332542, "sd": 1.52922959, "sign": -1},
        "realized_second_log_vol": {"mean": 0.18287512, "sd": 0.22326549, "sign": 1},
    },
}
MIDDAY_RULE = {
    "rule_id": "MIDDAY_RETURN_FROM_PRIOR_CLOSE_V1",
    "decision_time_london": "17:00",
    "threshold_pct": 13.776879223878035,
}


def _dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    text = str(value).strip().replace("Z", "+00:00")
    # Python accepts up to microseconds; trim excess fractional digits without changing ordering at second level.
    if "." in text:
        head, tail = text.split(".", 1)
        frac, suffix = tail, ""
        if "+" in tail:
            frac, suffix = tail.split("+", 1); suffix = "+" + suffix
        elif tail.count("-") >= 1 and "T" not in tail:
            frac, suffix = tail.split("-", 1); suffix = "-" + suffix
        frac = frac[:6]
        text = f"{head}.{frac}{suffix}"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _calendar_dt(day: date, value: Any) -> datetime:
    text = str(value or "").strip()
    if "T" in text:
        return _dt(text)
    hour, minute = [int(part) for part in text.split(":")[:2]]
    return datetime.combine(day, time(hour, minute), ET).astimezone(UTC)


def _chunks(values: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def frozen_preopen_score(trade_volume: int, median_spread_bps: float, realized_second_log_vol: float) -> float:
    values = {
        "log1p_trade_volume": math.log1p(max(0, trade_volume)),
        "log1p_median_spread_bps": math.log1p(max(0.0, median_spread_bps)),
        "realized_second_log_vol": max(0.0, realized_second_log_vol),
    }
    terms = []
    for name, spec in PREOPEN_RULE["components"].items():
        terms.append(spec["sign"] * (values[name] - spec["mean"]) / spec["sd"])
    return sum(terms) / len(terms)


def _realized_second_log(last_by_second: dict[int, float]) -> float:
    prices = [last_by_second[key] for key in sorted(last_by_second)]
    if len(prices) < 2:
        return 0.0
    returns = [math.log(b / a) for a, b in zip(prices, prices[1:]) if a > 0 and b > 0]
    return math.sqrt(sum(x * x for x in returns)) if returns else 0.0


class FrozenPreopenAccumulator:
    """Exact V3 one-second aggregation used by the frozen pre-open score."""

    def __init__(self, feature_end: datetime):
        self.feature_end = feature_end
        self.trade_volume = 0
        self.last_trade_by_second: dict[int, float] = {}
        self.quote_seconds: dict[int, dict[str, float]] = {}
        self.quote_count = 0

    def add_trade(self, row: dict[str, Any]) -> None:
        ts = _dt(row.get("t") or row.get("timestamp"))
        if ts >= self.feature_end:
            return
        price = float(row.get("p") if row.get("p") is not None else row.get("price") or 0)
        size = int(row.get("s") if row.get("s") is not None else row.get("size_shares") or 0)
        if price <= 0 or size <= 0:
            return
        self.trade_volume += size
        self.last_trade_by_second[int(ts.timestamp())] = price

    def add_quote(self, row: dict[str, Any]) -> None:
        ts = _dt(row.get("t") or row.get("timestamp"))
        if ts >= self.feature_end:
            return
        bid = float(row.get("bp") if row.get("bp") is not None else row.get("bid_price") or 0)
        ask = float(row.get("ap") if row.get("ap") is not None else row.get("ask_price") or 0)
        if bid <= 0 or ask < bid:
            return
        sec = int(ts.timestamp())
        spread = ask - bid
        state = self.quote_seconds.setdefault(sec, {"bid": bid, "ask": ask, "min_spread": spread})
        state["bid"] = bid
        state["ask"] = ask
        state["min_spread"] = min(state["min_spread"], spread)
        self.quote_count += 1

    def summary(self) -> dict[str, Any] | None:
        spreads = []
        for sec in sorted(self.quote_seconds):
            state = self.quote_seconds[sec]
            midpoint = (state["bid"] + state["ask"]) / 2
            if midpoint > 0:
                spreads.append(state["min_spread"] / midpoint * 10000)
        med = _median(spreads)
        if self.trade_volume <= 0 or med is None or not self.last_trade_by_second:
            return None
        rv = _realized_second_log(self.last_trade_by_second)
        return {
            "trade_volume": self.trade_volume, "median_spread_bps": med,
            "realized_second_log_vol": rv, "trade_seconds": len(self.last_trade_by_second),
            "quote_count": self.quote_count, "quote_seconds": len(self.quote_seconds),
            "score": frozen_preopen_score(self.trade_volume, med, rv),
        }


@dataclass
class ExecutionResult:
    filled: bool
    reason: str | None
    entry_timestamp: str | None = None
    entry_ask_raw: float | None = None
    entry_price: float | None = None
    shares: float | None = None
    invested_notional: float | None = None
    target_price: float | None = None
    stop_price: float | None = None
    exit_timestamp: str | None = None
    exit_bid_raw: float | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    return_pct: float | None = None
    pnl_usd: float | None = None
    max_bid_after_entry: float | None = None
    min_bid_after_entry: float | None = None
    max_quote_gap_seconds: float | None = None
    capacity_shortfall: bool = False

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _normalized_quote(row: dict[str, Any], event_date: date) -> tuple[datetime, float, int, float, int] | None:
    ts = _dt(row.get("t") or row.get("timestamp"))
    bid = float(row.get("bp") if row.get("bp") is not None else row.get("bid_price") or 0)
    ask = float(row.get("ap") if row.get("ap") is not None else row.get("ask_price") or 0)
    bs_raw = row.get("bs") if row.get("bs") is not None else row.get("bid_size_shares") or 0
    as_raw = row.get("as") if row.get("as") is not None else row.get("ask_size_shares") or 0
    bs = int(bs_raw) if "bid_size_shares" in row else quote_size_shares(bs_raw, event_date)
    ass = int(as_raw) if "ask_size_shares" in row else quote_size_shares(as_raw, event_date)
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    return ts, bid, bs, ask, ass


def simulate_quote_scenarios(
    quotes: Iterable[dict[str, Any]], *, event_date: date, entry_boundary: datetime,
    close_boundary: datetime, prior_close: float, position_notional: float,
    stop_loss_pct: float, slippage_scenarios_bps: Iterable[float], fractionable: bool,
    max_time_exit_quote_age_seconds: float = 60.0,
) -> dict[float, ExecutionResult]:
    """Stream sorted historical quotes once and evaluate several fixed friction scenarios.

    Alpaca returns historical quote pages in ascending timestamp order. Only compact
    per-scenario state is retained, so multi-million-quote symbols do not exhaust the
    worker's memory.
    """
    target = prior_close * 1.25
    scenarios = sorted({float(x) for x in slippage_scenarios_bps})
    states: dict[float, dict[str, Any]] = {
        slip: {
            "entry": None, "chosen": None, "last_eligible": None,
            "max_bid": None, "min_bid": None, "last_ts": None,
            "max_gap": 0.0, "capacity_shortfall": False,
        } for slip in scenarios
    }

    for raw in quotes:
        normalized = _normalized_quote(raw, event_date)
        if normalized is None:
            continue
        ts, bid, bs, ask, ass = normalized
        if ts > close_boundary:
            break
        for slip_bps, state in states.items():
            if state["chosen"] is not None:
                continue
            if state["entry"] is None:
                if ts < entry_boundary or ask >= target or ask * ass + 1e-9 < position_notional:
                    continue
                slip = slip_bps / 10000.0
                fill_price = ask * (1 + slip)
                shares = position_notional / fill_price if fractionable else math.floor(position_notional / fill_price)
                if shares <= 0 or ask * ass + 1e-9 < shares * ask:
                    continue
                cost = shares * fill_price
                state["entry"] = {
                    "ts": ts, "ask": ask, "price": fill_price, "shares": shares,
                    "cost": cost, "stop": fill_price * (1 - stop_loss_pct / 100.0),
                }

            entry = state["entry"]
            if entry is None or ts < entry["ts"]:
                continue
            if state["last_ts"] is not None:
                state["max_gap"] = max(state["max_gap"], (ts - state["last_ts"]).total_seconds())
            state["last_ts"] = ts
            state["max_bid"] = bid if state["max_bid"] is None else max(state["max_bid"], bid)
            state["min_bid"] = bid if state["min_bid"] is None else min(state["min_bid"], bid)
            enough = bs + 1e-9 >= entry["shares"]
            if enough:
                state["last_eligible"] = (ts, bid, bs)
            if bid >= target:
                if enough:
                    state["chosen"] = (ts, bid, "target")
                else:
                    state["capacity_shortfall"] = True
            elif bid <= entry["stop"]:
                if enough:
                    state["chosen"] = (ts, bid, "stop")
                else:
                    state["capacity_shortfall"] = True

    results: dict[float, ExecutionResult] = {}
    for slip_bps, state in states.items():
        entry = state["entry"]
        if entry is None:
            results[slip_bps] = ExecutionResult(False, "no_executable_subthreshold_ask")
            continue
        chosen = state["chosen"]
        if chosen is None:
            last = state["last_eligible"]
            fresh = last is not None and (close_boundary - last[0]).total_seconds() <= max_time_exit_quote_age_seconds
            if not fresh:
                results[slip_bps] = ExecutionResult(
                    True, "no_executable_close_exit", _iso(entry["ts"]), entry["ask"],
                    entry["price"], entry["shares"], entry["cost"], target, entry["stop"],
                    max_bid_after_entry=state["max_bid"], min_bid_after_entry=state["min_bid"],
                    max_quote_gap_seconds=state["max_gap"],
                    capacity_shortfall=last is None or state["capacity_shortfall"],
                )
                continue
            chosen = (last[0], last[1], "time")
        exit_ts, exit_bid, exit_reason = chosen
        slip = slip_bps / 10000.0
        exit_price = exit_bid * (1 - slip)
        proceeds = entry["shares"] * exit_price
        pnl = proceeds - entry["cost"]
        ret = (exit_price / entry["price"] - 1) * 100
        results[slip_bps] = ExecutionResult(
            True, None, _iso(entry["ts"]), entry["ask"], entry["price"], entry["shares"],
            entry["cost"], target, entry["stop"], _iso(exit_ts), exit_bid, exit_price,
            exit_reason, ret, pnl, state["max_bid"], state["min_bid"], state["max_gap"],
            state["capacity_shortfall"],
        )
    return results


def simulate_quotes(
    quotes: list[dict[str, Any]], *, event_date: date, entry_boundary: datetime,
    close_boundary: datetime, prior_close: float, position_notional: float,
    stop_loss_pct: float, slippage_bps: float, fractionable: bool,
    max_time_exit_quote_age_seconds: float = 60.0,
) -> ExecutionResult:
    ordered = sorted(quotes, key=lambda row: _dt(row.get("t") or row.get("timestamp")))
    return simulate_quote_scenarios(
        ordered, event_date=event_date, entry_boundary=entry_boundary,
        close_boundary=close_boundary, prior_close=prior_close,
        position_notional=position_notional, stop_loss_pct=stop_loss_pct,
        slippage_scenarios_bps=[slippage_bps], fractionable=fractionable,
        max_time_exit_quote_age_seconds=max_time_exit_quote_age_seconds,
    )[float(slippage_bps)]


class BacktestRunner:
    def __init__(self, settings: Settings, store: SupabaseStore, alpaca: AlpacaClient):
        self.settings = settings
        self.store = store
        self.alpaca = alpaca

    def _source_range(self, entry_job_id: str, mode: str) -> tuple[date, date]:
        rows = self.store.select_all(
            "stock25_entry_assessments", select="event_date",
            filters={"entry_job_id": f"eq.{entry_job_id}"}, order="event_date.asc",
        )
        if not rows:
            raise RuntimeError("Source entry job has no assessments")
        dates = [date.fromisoformat(str(row["event_date"])[:10]) for row in rows]
        source_start, source_end = min(dates), max(dates)
        if mode == "prior_90_days":
            return source_start - timedelta(days=90), source_start - timedelta(days=1)
        return source_start, source_end

    def _assets(self) -> tuple[list[str], dict[str, dict[str, Any]]]:
        raw = self.alpaca.get_assets(all_statuses=True)
        assets: dict[str, dict[str, Any]] = {}
        for item in raw:
            symbol = str(item.get("symbol") or "").strip().upper()
            if not is_likely_stock_symbol(symbol):
                continue
            exchange = str(item.get("exchange") or "").upper()
            if not self.settings.include_otc and exchange == "OTC":
                continue
            assets[symbol] = item
        self.store.save_asset_snapshot(datetime.now(UTC).date().isoformat(), list(assets.values()))
        return sorted(assets), assets

    def _daily_cache(self, path: Path, symbols: list[str], start: date, end: date, feed: str, job_id: str) -> sqlite3.Connection:
        db = sqlite3.connect(path)
        db.execute("create table if not exists daily(symbol text, d text, o real, h real, l real, c real, v integer, raw_o real, raw_c real, primary key(symbol,d))")
        db.execute("create index if not exists daily_d_idx on daily(d)")
        count = db.execute("select count(*) from daily").fetchone()[0]
        if count:
            return db
        begin = datetime.combine(start - timedelta(days=14), time(0), UTC)
        finish = datetime.combine(end + timedelta(days=2), time(0), UTC)
        batches = list(_chunks(symbols, self.settings.backtest_symbol_batch_size))
        for idx, batch in enumerate(batches, 1):
            bars = self.alpaca.get_bars(batch, timeframe="1Day", start=begin, end=finish, feed=feed, adjustment="split")
            raw_bars = self.alpaca.get_bars(batch, timeframe="1Day", start=begin, end=finish, feed=feed, adjustment="raw")
            raw_map: dict[tuple[str, str], dict[str, Any]] = {}
            for symbol, rows in raw_bars.items():
                for row in rows:
                    d = _dt(row["t"]).astimezone(ET).date().isoformat()
                    raw_map[(symbol, d)] = row
            payload = []
            for symbol, rows in bars.items():
                for row in rows:
                    d = _dt(row["t"]).astimezone(ET).date().isoformat()
                    raw = raw_map.get((symbol, d), {})
                    payload.append((symbol, d, float(row.get("o") or 0), float(row.get("h") or 0), float(row.get("l") or 0), float(row.get("c") or 0), int(row.get("v") or 0), float(raw.get("o") or 0), float(raw.get("c") or 0)))
            db.executemany("insert or replace into daily values(?,?,?,?,?,?,?,?,?)", payload)
            db.commit()
            self.store.update_backtest_job(job_id, progress_stage="building_daily_cache", progress_current=idx, progress_total=len(batches))
        return db

    @staticmethod
    def _day_rows(db: sqlite3.Connection, d: date) -> dict[str, dict[str, Any]]:
        rows = db.execute("select symbol,o,h,l,c,v,raw_o,raw_c from daily where d=?", (d.isoformat(),)).fetchall()
        result = {}
        for r in rows:
            scale = (r[1] / r[6]) if r[1] and r[6] else ((r[4] / r[7]) if r[4] and r[7] else None)
            result[r[0]] = {"open":r[1],"high":r[2],"low":r[3],"close":r[4],"volume":r[5],"raw_open":r[6],"raw_close":r[7],"adjustment_scale":scale}
        return result

    def _premarket_active(self, symbols: list[str], day: date, decision: datetime, feed: str) -> list[str]:
        start = datetime.combine(day, time(4,0), ET).astimezone(UTC)
        feature_end = decision + timedelta(seconds=1)
        active: list[str] = []
        for batch in _chunks(symbols, self.settings.backtest_symbol_batch_size):
            bars = self.alpaca.get_bars(batch, timeframe="1Min", start=start, end=feature_end, feed=feed, adjustment="raw")
            for symbol, rows in bars.items():
                if any(int(row.get("v") or 0) > 0 for row in rows):
                    active.append(symbol)
        return sorted(set(active))

    def _preopen_features(self, symbol: str, day: date, decision: datetime, feed: str) -> dict[str, Any] | None:
        start = datetime.combine(day, time(4,0), ET).astimezone(UTC)
        feature_end = decision + timedelta(seconds=1)
        acc = FrozenPreopenAccumulator(feature_end)
        for page in self.alpaca.iter_trades(symbol, start=start, end=feature_end, feed=feed, asof=day):
            for row in page:
                acc.add_trade(row)
        for page in self.alpaca.iter_quotes(symbol, start=start, end=feature_end, feed=feed, asof=day):
            for row in page:
                acc.add_quote(row)
        return acc.summary()

    def _midday_candidates(self, symbols: list[str], day: date, prior: dict[str, dict[str, Any]], today: dict[str, dict[str, Any]], decision: datetime, feed: str) -> list[dict[str, Any]]:
        threshold = MIDDAY_RULE["threshold_pct"]
        prefilter = [s for s in symbols if s in prior and s in today and prior[s]["close"] > 0 and today[s]["high"] >= prior[s]["close"] * (1 + threshold/100)]
        start = datetime.combine(day, time(9,30), ET).astimezone(UTC)
        found: list[dict[str, Any]] = []
        for batch in _chunks(prefilter, self.settings.backtest_symbol_batch_size):
            bars = self.alpaca.get_bars(batch, timeframe="1Min", start=start, end=decision, feed=feed, adjustment="split", asof=day)
            for symbol, rows in bars.items():
                valid = [r for r in rows if _dt(r["t"]) < decision]
                if not valid:
                    continue
                price = float(valid[-1].get("c") or 0)
                feature_end = decision + timedelta(seconds=1)
                # V3 snapshots were built from one-second summaries and included the
                # complete second stamped at the cutoff. Reconstruct that exact frozen
                # convention, then wait until the fixed reaction boundary to enter.
                for page in self.alpaca.iter_trades(symbol, start=decision, end=feature_end, feed=feed, asof=day):
                    for trade in page:
                        ts = _dt(trade["t"])
                        if decision <= ts < feature_end and float(trade.get("p") or 0) > 0:
                            price = float(trade["p"])
                pc = prior[symbol]["close"]
                ret = (price / pc - 1) * 100 if pc else None
                if ret is not None and ret > threshold:
                    found.append({"symbol":symbol,"signal_value":ret,"decision_price":price,"prior_close":pc})
        return sorted(found, key=lambda x: (-x["signal_value"], x["symbol"]))

    def _quote_rows(self, symbol: str, day: date, start: datetime, end: datetime, feed: str) -> Iterator[dict[str, Any]]:
        for page in self.alpaca.iter_quotes(symbol, start=start, end=end, feed=feed, asof=day):
            yield from page

    def _insert_trigger_and_trade(self, job_id: str, trigger: dict[str, Any], trade: dict[str, Any] | None) -> None:
        self.store.upsert("stock25_backtest_triggers", trigger, on_conflict="backtest_job_id,strategy,trade_date,symbol")
        if trade:
            self.store.upsert("stock25_backtest_trades", trade, on_conflict="backtest_job_id,strategy,trade_date,symbol")

    def _process_day(self, job_id: str, day: date, session: dict[str, Any], symbols: list[str], assets: dict[str,dict[str,Any]], db: sqlite3.Connection, params: dict[str, Any]) -> dict[str, Any]:
        # A failed date is replayed from a clean slate; completed dates are skipped by run().
        filters = {"backtest_job_id": f"eq.{job_id}", "trade_date": f"eq.{day.isoformat()}"}
        self.store.delete("stock25_backtest_trades", filters)
        self.store.delete("stock25_backtest_triggers", filters)
        feed = params["feed"]
        prior_day = None
        # Calendar rows are processed in sequence; find latest daily row before day from cache.
        cursor = day - timedelta(days=1)
        while cursor >= day - timedelta(days=10):
            if self._day_rows(db, cursor):
                prior_day = cursor; break
            cursor -= timedelta(days=1)
        if prior_day is None:
            return {"status":"skipped","reason":"no_prior_session"}
        prior = self._day_rows(db, prior_day)
        today = self._day_rows(db, day)
        raw_eligible = sorted(set(prior).intersection(today).intersection(symbols))
        eligible = []
        adjustment_excluded = 0
        for symbol in raw_eligible:
            scale = today[symbol].get("adjustment_scale")
            if scale is not None and not (0.9 <= float(scale) <= 1.1):
                adjustment_excluded += 1
                continue
            eligible.append(symbol)
        open_et = _calendar_dt(day, session["open"])
        close_et = _calendar_dt(day, session["close"])
        close_boundary = close_et - timedelta(minutes=int(params["close_exit_minutes_before"]))
        day_counts = {"eligible":len(eligible),"adjustment_excluded":adjustment_excluded,"preopen_triggers":0,"midday_triggers":0,"trades":0,"fills":0}

        strategies: list[tuple[str,list[dict[str,Any]],datetime,datetime]] = []
        if params.get("enable_preopen", True):
            decision = datetime.combine(day, time(14,0), LONDON).astimezone(UTC)
            if decision >= open_et:
                day_counts["preopen_skipped_not_before_open"] = True
                active = []
            else:
                active = self._premarket_active(eligible, day, decision, feed)
            candidates = []
            for idx, symbol in enumerate(active, 1):
                feat = self._preopen_features(symbol, day, decision, feed)
                if feat and feat["score"] >= PREOPEN_RULE["threshold"]:
                    candidates.append({"symbol":symbol,"signal_value":feat["score"],"prior_close":prior[symbol]["close"],"features":feat})
                if idx % 25 == 0:
                    self.store.update_backtest_job(job_id, progress_stage=f"preopen_features_{day.isoformat()}")
            candidates.sort(key=lambda x:(-x["signal_value"],x["symbol"]))
            day_counts["preopen_triggers"] = len(candidates)
            strategies.append(("preopen", candidates, decision, max(open_et, decision) + timedelta(seconds=float(params["reaction_delay_seconds"]))))

        if params.get("enable_midday", True):
            decision = datetime.combine(day, time(17,0), LONDON).astimezone(UTC)
            candidates = self._midday_candidates(eligible, day, prior, today, decision, feed)
            day_counts["midday_triggers"] = len(candidates)
            strategies.append(("midday", candidates, decision, decision + timedelta(seconds=float(params["reaction_delay_seconds"]))))

        for strategy, candidates, decision_timestamp, entry_boundary in strategies:
            selected = candidates[:int(params["max_trades_per_day"])]
            selected_symbols = {x["symbol"] for x in selected}
            for rank, candidate in enumerate(candidates, 1):
                symbol = candidate["symbol"]
                trigger = {
                    "backtest_job_id":job_id,"strategy":strategy,"trade_date":day.isoformat(),"symbol":symbol,
                    "rank":rank,"selected":symbol in selected_symbols,"signal_value":candidate["signal_value"],
                    "prior_close":candidate["prior_close"],"decision_timestamp":_iso(decision_timestamp),
                    "features":candidate.get("features") or {},"quality_flags":[],
                }
                trade = None
                if symbol in selected_symbols:
                    asset = assets.get(symbol,{})
                    quote_rows = self._quote_rows(symbol, day, entry_boundary - timedelta(seconds=2), close_boundary, feed)
                    friction_values = [float(params["slippage_bps"]), 0.0, 10.0]
                    scenario_map = simulate_quote_scenarios(
                        quote_rows, event_date=day, entry_boundary=entry_boundary, close_boundary=close_boundary,
                        prior_close=float(candidate["prior_close"]), position_notional=float(params["position_notional"]),
                        stop_loss_pct=float(params["stop_loss_pct"]), slippage_scenarios_bps=friction_values,
                        fractionable=bool(asset.get("fractionable")), max_time_exit_quote_age_seconds=60.0,
                    )
                    result = scenario_map[float(params["slippage_bps"])]
                    sensitivity_results = {}
                    for friction in (0.0, 10.0):
                        scenario = scenario_map[friction]
                        sensitivity_results[f"{int(friction)}bps_each_side"] = {
                            "filled": scenario.filled, "issue": scenario.reason,
                            "exit_reason": scenario.exit_reason, "return_pct": scenario.return_pct,
                            "pnl_usd": scenario.pnl_usd,
                        }
                    day_counts["trades"] += 1
                    if result.filled: day_counts["fills"] += 1
                    trade = {
                        "backtest_job_id":job_id,"strategy":strategy,"trade_date":day.isoformat(),"symbol":symbol,"rank":rank,
                        "signal_value":candidate["signal_value"],"prior_close":candidate["prior_close"],"position_notional_requested":params["position_notional"],
                        "filled":result.filled,"unfilled_reason":result.reason,"entry_timestamp":result.entry_timestamp,
                        "entry_ask_raw":result.entry_ask_raw,"entry_price":result.entry_price,"shares":result.shares,
                        "invested_notional":result.invested_notional,"target_price":result.target_price,"stop_price":result.stop_price,
                        "exit_timestamp":result.exit_timestamp,"exit_bid_raw":result.exit_bid_raw,"exit_price":result.exit_price,
                        "exit_reason":result.exit_reason,"return_pct":result.return_pct,"pnl_usd":result.pnl_usd,
                        "max_bid_after_entry":result.max_bid_after_entry,"min_bid_after_entry":result.min_bid_after_entry,
                        "max_quote_gap_seconds":result.max_quote_gap_seconds,"capacity_shortfall":result.capacity_shortfall,
                        "execution_sensitivity":sensitivity_results,
                        "quality_flags": (["point_in_time_tradability_unverified"] if not asset.get("tradable") else []) + (["inactive_current_asset"] if asset.get("status") != "active" else []),
                    }
                self._insert_trigger_and_trade(job_id, trigger, trade)
        return day_counts

    @staticmethod
    def _drawdown(values: list[float]) -> float:
        peak = 0.0; equity = 0.0; worst = 0.0
        for value in values:
            equity += value; peak = max(peak,equity); worst = min(worst,equity-peak)
        return worst

    @staticmethod
    def _bootstrap_daily_mean_lower(daily_pnl: list[float], *, seed: int, iterations: int = 10000) -> float | None:
        if len(daily_pnl) < 10:
            return None
        rng = random.Random(seed)
        n = len(daily_pnl)
        means = []
        for _ in range(iterations):
            sample = [daily_pnl[rng.randrange(n)] for _ in range(n)]
            means.append(sum(sample) / n)
        means.sort()
        return means[max(0, int(0.025 * iterations) - 1)]

    def _summaries(self, trades: list[dict[str,Any]], triggers: list[dict[str,Any]]) -> list[dict[str,Any]]:
        summaries=[]
        for strategy in sorted({str(x["strategy"]) for x in triggers}):
            ts=[x for x in trades if x["strategy"]==strategy]
            entered=[x for x in ts if x.get("filled")]
            filled=[x for x in entered if x.get("pnl_usd") is not None]
            pnl=[float(x["pnl_usd"]) for x in sorted(filled,key=lambda r:(r["trade_date"],r["rank"]))]
            returns=[float(x["return_pct"]) for x in filled if x.get("return_pct") is not None]
            wins=[x for x in pnl if x>0]; losses=[x for x in pnl if x<0]
            streak=0; max_streak=0
            for x in pnl:
                if x<0: streak+=1; max_streak=max(max_streak,streak)
                else: streak=0
            daily_map: dict[str, float] = defaultdict(float)
            for row in filled:
                daily_map[str(row["trade_date"])] += float(row["pnl_usd"])
            daily_pnl = [daily_map[key] for key in sorted(daily_map)]
            losing_day_streak = 0
            max_losing_day_streak = 0
            for value in daily_pnl:
                if value < 0:
                    losing_day_streak += 1
                    max_losing_day_streak = max(max_losing_day_streak, losing_day_streak)
                else:
                    losing_day_streak = 0
            pnl_without_top3 = sum(sorted(pnl)[:-3]) if len(pnl) > 3 else None
            bootstrap_lower = self._bootstrap_daily_mean_lower(
                daily_pnl, seed=1701 if strategy == "preopen" else 1702
            )
            sensitivity_totals = {}
            for key in ("0bps_each_side", "10bps_each_side"):
                values = []
                for row in entered:
                    scenario = (row.get("execution_sensitivity") or {}).get(key) or {}
                    if scenario.get("pnl_usd") is not None:
                        values.append(float(scenario["pnl_usd"]))
                sensitivity_totals[f"total_pnl_{key}"] = sum(values) if values else None
                sensitivity_totals[f"resolved_trades_{key}"] = len(values)
            summaries.append({
                "strategy":strategy,"trigger_count":sum(1 for x in triggers if x["strategy"]==strategy),
                "selected_count":sum(1 for x in triggers if x["strategy"]==strategy and x.get("selected")),
                "entry_fill_count":len(entered),"resolved_trade_count":len(filled),
                "fill_rate":len(entered)/len(ts) if ts else None,
                "target_count":sum(1 for x in filled if x.get("exit_reason")=="target"),
                "stop_count":sum(1 for x in filled if x.get("exit_reason")=="stop"),
                "time_exit_count":sum(1 for x in filled if x.get("exit_reason")=="time"),
                "unresolved_exit_count":len(entered)-len(filled),
                "win_rate":len(wins)/len(pnl) if pnl else None,"mean_return_pct":statistics.mean(returns) if returns else None,
                "median_return_pct":statistics.median(returns) if returns else None,"total_pnl_usd":sum(pnl),
                "profit_factor":sum(wins)/abs(sum(losses)) if losses else (None if not wins else 999.0),
                "market_days_with_resolved_trades":len(daily_pnl),
                "max_drawdown_usd":self._drawdown(daily_pnl),
                "max_consecutive_losses":max_streak,
                "max_consecutive_losing_days":max_losing_day_streak,
                "pnl_without_top3_winners_usd":pnl_without_top3,
                "bootstrap_95pct_lower_mean_daily_pnl_usd":bootstrap_lower,
                **sensitivity_totals,
            })
            summary = summaries[-1]
            unresolved_rate = summary["unresolved_exit_count"] / summary["entry_fill_count"] if summary["entry_fill_count"] else 0.0
            checks = {
                "minimum_30_resolved_trades": summary["resolved_trade_count"] >= 30,
                "minimum_15_trading_days": summary["market_days_with_resolved_trades"] >= 15,
                "positive_primary_pnl": summary["total_pnl_usd"] > 0,
                "profit_factor_at_least_1_25": summary["profit_factor"] is not None and summary["profit_factor"] >= 1.25,
                "positive_at_10bps_each_side": summary.get("total_pnl_10bps_each_side") is not None and summary["total_pnl_10bps_each_side"] > 0,
                "positive_without_top3_winners": summary["pnl_without_top3_winners_usd"] is not None and summary["pnl_without_top3_winners_usd"] > 0,
                "bootstrap_lower_daily_mean_positive": summary["bootstrap_95pct_lower_mean_daily_pnl_usd"] is not None and summary["bootstrap_95pct_lower_mean_daily_pnl_usd"] > 0,
                "drawdown_within_5000": summary["max_drawdown_usd"] >= -5000,
                "maximum_10_consecutive_losses": summary["max_consecutive_losses"] <= 10,
                "unresolved_exit_rate_at_most_1pct": unresolved_rate <= 0.01,
            }
            summary["unresolved_exit_rate"] = unresolved_rate
            summary["graduation_checks"] = checks
            summary["window_gate_passed"] = all(checks.values())

        return summaries

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str,Any]]) -> None:
        if not rows:
            path.write_text("") ; return
        keys=[]
        for row in rows:
            for key in row:
                if key not in keys: keys.append(key)
        with path.open("w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f,fieldnames=keys); w.writeheader()
            for row in rows:
                clean={k:(json.dumps(v,sort_keys=True) if isinstance(v,(dict,list)) else v) for k,v in row.items()}
                w.writerow(clean)

    @staticmethod
    def _write_parquet(path: Path, rows: list[dict[str,Any]]) -> None:
        if rows:
            pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")

    def _export(self, job_id: str, params: dict[str,Any], start: date, end: date, temp: Path) -> str:
        triggers=self.store.select_all("stock25_backtest_triggers",filters={"backtest_job_id":f"eq.{job_id}"},order="trade_date.asc,strategy.asc,rank.asc")
        trades=self.store.select_all("stock25_backtest_trades",filters={"backtest_job_id":f"eq.{job_id}"},order="trade_date.asc,strategy.asc,rank.asc")
        days=self.store.select_all("stock25_backtest_days",filters={"backtest_job_id":f"eq.{job_id}"},order="trade_date.asc")
        summaries=self._summaries(trades,triggers)
        out=temp/"export"; out.mkdir(parents=True,exist_ok=True)
        for name,rows in [("stock25_backtest_triggers",triggers),("stock25_backtest_trades",trades),("stock25_backtest_days",days),("strategy_summary",summaries)]:
            self._write_csv(out/f"{name}.csv",rows); self._write_parquet(out/f"{name}.parquet",rows)
        spec={
            "version":"4.0.1-25pct","status":"reference_execution_spec_pending_25pct_signal_freeze","date_range":{"start":start.isoformat(),"end":end.isoformat()},
            "candidate_rules":{"preopen":PREOPEN_RULE,"midday":MIDDAY_RULE},
            "execution":{
                "position_notional":params["position_notional"],"reaction_delay_seconds":params["reaction_delay_seconds"],
                "maximum_trades_per_strategy_per_day":params["max_trades_per_day"],"entry":"first executable NBBO ask with displayed capacity",
                "profit_target":"125% of split-adjusted previous regular-session close","stop_loss_pct_from_fill":params["stop_loss_pct"],
                "time_exit":f"{params['close_exit_minutes_before']} minutes before official close; quote must be no more than 60 seconds old","exit":"executable NBBO bid with displayed capacity",
                "slippage_bps_each_side":params["slippage_bps"],"whole_shares_unless_asset_fractionable":True,
            },
            "prohibited_retuning":["signal features","signal thresholds","score scaling","stop loss","target","reaction delay","daily trade cap","primary slippage"],
        }
        (out/"preregistered_execution_spec.json").write_text(json.dumps(spec,indent=2),encoding="utf-8")
        gate_payload = {
            "version": "4.0.1-25pct",
            "window_mode": params.get("window_mode"),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "strategies": [
                {
                    "strategy": row["strategy"],
                    "window_gate_passed": row.get("window_gate_passed", False),
                    "checks": row.get("graduation_checks", {}),
                } for row in summaries
            ],
            "binding_final_rule": "A strategy must pass every check in both source_study and prior_90_days jobs; otherwise no prospective test.",
        }
        (out/"profitability_gate.json").write_text(json.dumps(gate_payload, indent=2), encoding="utf-8")
        report=["# V4 execution-aware full-universe backtest","",f"Window: {start} to {end}.","", "## Results"]
        for s in summaries:
            report += [f"### {s['strategy']}", f"- Full-universe triggers: {s['trigger_count']}",f"- Selected / entered / resolved: {s['selected_count']} / {s['entry_fill_count']} / {s['resolved_trade_count']}",f"- Target / stop / time / unresolved exits: {s['target_count']} / {s['stop_count']} / {s['time_exit_count']} / {s['unresolved_exit_count']}",f"- Mean return: {s['mean_return_pct']}",f"- Total P&L: US${s['total_pnl_usd']:.2f}",f"- Profit factor: {s['profit_factor']}",f"- Maximum drawdown: US${s['max_drawdown_usd']:.2f}",f"- Maximum consecutive losses: {s['max_consecutive_losses']}",f"- P&L sensitivity (0 / 10 bps each side): {s.get('total_pnl_0bps_each_side')} / {s.get('total_pnl_10bps_each_side')}",f"- P&L excluding top three winners: {s.get('pnl_without_top3_winners_usd')}",f"- Bootstrap 95% lower mean daily P&L: {s.get('bootstrap_95pct_lower_mean_daily_pnl_usd')}",f"- Frozen window gate: {'PASS' if s.get('window_gate_passed') else 'FAIL'}",f"- Gate checks: {json.dumps(s.get('graduation_checks'), sort_keys=True)}",""]
        report += ["## Binding graduation rule","A strategy is not historically graduated unless it passes every frozen gate in both the original study-window job and the untouched preceding-90-day replication job. A pass in only one window means no prospective test.","","## Interpretation guardrails","This is a historical simulation using NBBO displayed capacity, not a guarantee of fills. Exact historical point-in-time tradability is not fully reconstructable for every inactive asset. The original study window reused data involved in signal discovery; run the prior-90-day replication mode before considering any prospective test."]
        (out/"BACKTEST_REPORT.md").write_text("\n".join(report),encoding="utf-8")
        bundle=temp/f"execution_backtest_{job_id}.zip"
        with zipfile.ZipFile(bundle,"w",zipfile.ZIP_DEFLATED) as z:
            for f in sorted(out.iterdir()): z.write(f,arcname=f.name)
        storage=f"backtests/{job_id}/{bundle.name}"
        self.store.upload_file(bundle,storage)
        self.store.upsert("stock25_backtest_files",{
            "backtest_job_id":job_id,"file_kind":"analysis_index","storage_path":storage,"filename":bundle.name,
            "size_bytes":bundle.stat().st_size,"sha256":self.store.sha256(bundle),
        },on_conflict="backtest_job_id,storage_path")
        return storage

    def run(self, job: dict[str,Any]) -> None:
        if not self.settings.enable_backtest_stage:
            raise RuntimeError("Step 5 is locked until a 25%-specific frozen signal rule set is approved")
        job_id=str(job["id"]); params=dict(job.get("parameters") or {})
        defaults={"window_mode":"source_study","feed":"sip","enable_preopen":True,"enable_midday":True,
                  "position_notional":500.0,"reaction_delay_seconds":5.0,"stop_loss_pct":5.0,"slippage_bps":5.0,
                  "max_trades_per_day":5,"close_exit_minutes_before":5,"maximum_dates":3}
        for k,v in defaults.items(): params.setdefault(k,v)
        try:
            start,end=self._source_range(str(job["source_entry_job_id"]),str(params["window_mode"]))
            calendar=self.alpaca.get_calendar(start,end)
            if int(params.get("maximum_dates") or 0)>0: calendar=calendar[:int(params["maximum_dates"])]
            symbols,assets=self._assets()
            with tempfile.TemporaryDirectory(prefix="alpaca-v4-") as tmpdir:
                temp=Path(tmpdir)
                db=self._daily_cache(temp/"daily.sqlite",symbols,start,end,str(params["feed"]),job_id)
                completed={str(x["trade_date"]) for x in self.store.select_all("stock25_backtest_days",select="trade_date",filters={"backtest_job_id":f"eq.{job_id}","status":"eq.completed"})}
                self.store.update_backtest_job(job_id,progress_stage="full_universe_execution",progress_current=len(completed),progress_total=len(calendar),window_start=start.isoformat(),window_end=end.isoformat(),universe_symbol_count=len(symbols))
                for idx,session in enumerate(calendar,1):
                    day=date.fromisoformat(str(session["date"])[:10])
                    if day.isoformat() in completed: continue
                    self.store.update_backtest_job(job_id,progress_stage=f"processing_{day.isoformat()}",progress_current=idx-1,progress_total=len(calendar))
                    counts=self._process_day(job_id,day,session,symbols,assets,db,params)
                    self.store.upsert("stock25_backtest_days",{
                        "backtest_job_id":job_id,"trade_date":day.isoformat(),"status":"completed",
                        "eligible_symbol_count":counts.get("eligible",0),"preopen_trigger_count":counts.get("preopen_triggers",0),
                        "midday_trigger_count":counts.get("midday_triggers",0),"selected_trade_count":counts.get("trades",0),
                        "filled_trade_count":counts.get("fills",0),"diagnostics":counts,
                    },on_conflict="backtest_job_id,trade_date")
                    self.store.update_backtest_job(job_id,progress_current=idx,progress_total=len(calendar))
                storage=self._export(job_id,params,start,end,temp)
                triggers=self.store.select_all("stock25_backtest_triggers",select="selected",filters={"backtest_job_id":f"eq.{job_id}"})
                trades=self.store.select_all("stock25_backtest_trades",select="filled,pnl_usd",filters={"backtest_job_id":f"eq.{job_id}"})
                filled=[x for x in trades if x.get("filled")]
                self.store.update_backtest_job(job_id,status="completed",progress_stage="completed",progress_current=len(calendar),progress_total=len(calendar),completed_at=datetime.now(UTC).isoformat(),trigger_count=len(triggers),selected_trade_count=sum(bool(x.get("selected")) for x in triggers),filled_trade_count=len(filled),total_pnl_usd=sum(float(x.get("pnl_usd") or 0) for x in filled),export_storage_path=storage,error_message=None)
        except Exception as exc:
            logger.exception("Backtest job %s failed",job_id)
            self.store.update_backtest_job(job_id,status="failed",progress_stage="failed",completed_at=datetime.now(UTC).isoformat(),error_message=str(exc)[:4000])
            raise
