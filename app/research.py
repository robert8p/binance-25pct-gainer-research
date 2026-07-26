from __future__ import annotations

import bisect
import csv
import hashlib
import json
import logging
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from app.alpaca_client import AlpacaClient
from app.config import Settings
from app.supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")
UTC = timezone.utc
QUOTE_SIZE_IN_SHARES_FROM = date(2025, 11, 3)
NOTIONAL_LEVELS = (100.0, 500.0, 1000.0, 5000.0)
MAX_FREE_SAFE_OBJECT_BYTES = 45 * 1024 * 1024


def parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def timestamp_ns(value: str | datetime) -> int:
    """Convert RFC-3339 timestamps to epoch nanoseconds without losing Alpaca's 9-digit precision."""
    if isinstance(value, datetime):
        dt = value.astimezone(UTC)
        return int(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1_000
    text = str(value)
    match = re.fullmatch(r"(.+?)(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})", text)
    if not match:
        dt = parse_ts(text)
        return int(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1_000
    base, fraction, zone = match.groups()
    dt = datetime.fromisoformat(base + ("+00:00" if zone == "Z" else zone)).astimezone(UTC)
    nanos = int(((fraction or "") + "000000000")[:9])
    return int(dt.timestamp()) * 1_000_000_000 + nanos


def et_dt(day: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(day, time(hh, mm), ET).astimezone(UTC)


def json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in value)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


TRADE_SCHEMA = pa.schema([
    ("symbol", pa.string()), ("timestamp", pa.string()), ("price", pa.float64()),
    ("size_shares", pa.int64()), ("exchange", pa.string()),
    ("conditions", pa.list_(pa.string())), ("trade_id", pa.string()),
    ("tape", pa.string()), ("session", pa.string()),
    ("source_window", pa.string()), ("raw_json", pa.string()),
])

QUOTE_SCHEMA = pa.schema([
    ("symbol", pa.string()), ("timestamp", pa.string()),
    ("bid_price", pa.float64()), ("bid_size_raw", pa.int64()),
    ("bid_size_shares", pa.int64()), ("bid_exchange", pa.string()),
    ("ask_price", pa.float64()), ("ask_size_raw", pa.int64()),
    ("ask_size_shares", pa.int64()), ("ask_exchange", pa.string()),
    ("spread", pa.float64()), ("conditions", pa.list_(pa.string())),
    ("tape", pa.string()), ("session", pa.string()),
    ("source_window", pa.string()), ("raw_json", pa.string()),
])

BAR_SCHEMA = pa.schema([
    ("symbol", pa.string()), ("timestamp", pa.string()),
    ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
    ("close", pa.float64()), ("volume", pa.int64()),
    ("trade_count", pa.int64()), ("vwap", pa.float64()),
    ("timeframe", pa.string()), ("adjustment", pa.string()),
    ("session", pa.string()), ("source_window", pa.string()),
    ("raw_json", pa.string()),
])

GENERIC_SCHEMA = pa.schema([
    ("symbol", pa.string()), ("timestamp", pa.string()),
    ("kind", pa.string()), ("raw_json", pa.string()),
])

SECOND_SCHEMA = pa.schema([
    ("second", pa.timestamp("us", tz="UTC")),
    ("trade_open", pa.float64()), ("trade_high", pa.float64()),
    ("trade_low", pa.float64()), ("trade_close", pa.float64()),
    ("trade_volume", pa.int64()), ("trade_count", pa.int64()),
    ("bid_price", pa.float64()), ("bid_size_shares", pa.int64()),
    ("ask_price", pa.float64()), ("ask_size_shares", pa.int64()),
    ("min_spread", pa.float64()), ("quote_updates", pa.int64()),
    ("source_window", pa.string()),
])


class SecondAggregator:
    """Streaming one-second summary; memory is bounded by seconds in one collection window."""

    def __init__(self, window_name: str):
        self.window_name = window_name
        self.data: dict[datetime, dict[str, Any]] = {}

    def add_trade(self, row: dict[str, Any]) -> None:
        ts = parse_ts(row["timestamp"])
        sec = ts.replace(microsecond=0)
        d = self.data.setdefault(sec, {"second": sec, "source_window": self.window_name})
        price = float(row["price"])
        ts_ns = timestamp_ns(row["timestamp"])
        if "trade_first_ns" not in d or ts_ns < d["trade_first_ns"]:
            d["trade_first_ns"] = ts_ns
            d["trade_open"] = price
        if "trade_last_ns" not in d or ts_ns >= d["trade_last_ns"]:
            d["trade_last_ns"] = ts_ns
            d["trade_close"] = price
        d["trade_high"] = max(price, d.get("trade_high", price))
        d["trade_low"] = min(price, d.get("trade_low", price))
        d["trade_volume"] = int(d.get("trade_volume", 0)) + int(row["size_shares"])
        d["trade_count"] = int(d.get("trade_count", 0)) + 1

    def add_quote(self, row: dict[str, Any]) -> None:
        ts = parse_ts(row["timestamp"])
        sec = ts.replace(microsecond=0)
        d = self.data.setdefault(sec, {"second": sec, "source_window": self.window_name})
        ts_ns = timestamp_ns(row["timestamp"])
        if "quote_last_ns" not in d or ts_ns >= d["quote_last_ns"]:
            d["quote_last_ns"] = ts_ns
            d["bid_price"] = float(row["bid_price"])
            d["bid_size_shares"] = int(row["bid_size_shares"])
            d["ask_price"] = float(row["ask_price"])
            d["ask_size_shares"] = int(row["ask_size_shares"])
        spread = row.get("spread")
        if spread is not None:
            d["min_spread"] = min(float(spread), d.get("min_spread", float(spread)))
        d["quote_updates"] = int(d.get("quote_updates", 0)) + 1

    def rows(self) -> list[dict[str, Any]]:
        allowed = {name for name in SECOND_SCHEMA.names}
        return [{k: v for k, v in self.data[sec].items() if k in allowed} for sec in sorted(self.data)]


def session_label(ts: datetime) -> str:
    local = ts.astimezone(ET)
    minute = local.hour * 60 + local.minute
    if minute < 4 * 60:
        return "overnight"
    if minute < 9 * 60 + 30:
        return "premarket"
    if minute < 16 * 60:
        return "regular"
    if minute < 20 * 60:
        return "afterhours"
    return "overnight"


def quote_size_shares(raw_size: Any, event_date: date) -> int:
    size = int(raw_size or 0)
    return size if event_date >= QUOTE_SIZE_IN_SHARES_FROM else size * 100


def normalize_trade(raw: dict[str, Any], symbol: str, window_name: str) -> dict[str, Any]:
    ts = parse_ts(raw["t"])
    return {
        "symbol": symbol, "timestamp": raw["t"], "price": float(raw.get("p") or 0),
        "size_shares": int(raw.get("s") or 0), "exchange": str(raw.get("x") or ""),
        "conditions": [str(x) for x in (raw.get("c") or [])],
        "trade_id": str(raw.get("i") or ""), "tape": str(raw.get("z") or ""),
        "session": session_label(ts), "source_window": window_name,
        "raw_json": json_text(raw),
    }


def normalize_quote(raw: dict[str, Any], symbol: str, window_name: str, event_date: date) -> dict[str, Any]:
    ts = parse_ts(raw["t"])
    bid, ask = float(raw.get("bp") or 0), float(raw.get("ap") or 0)
    bid_raw, ask_raw = int(raw.get("bs") or 0), int(raw.get("as") or 0)
    return {
        "symbol": symbol, "timestamp": raw["t"], "bid_price": bid,
        "bid_size_raw": bid_raw, "bid_size_shares": quote_size_shares(bid_raw, event_date),
        "bid_exchange": str(raw.get("bx") or ""), "ask_price": ask,
        "ask_size_raw": ask_raw, "ask_size_shares": quote_size_shares(ask_raw, event_date),
        "ask_exchange": str(raw.get("ax") or ""), "spread": ask - bid if ask and bid else None,
        "conditions": [str(x) for x in (raw.get("c") or [])], "tape": str(raw.get("z") or ""),
        "session": session_label(ts), "source_window": window_name, "raw_json": json_text(raw),
    }


def normalize_bar(raw: dict[str, Any], symbol: str, timeframe: str, adjustment: str, window_name: str) -> dict[str, Any]:
    ts = parse_ts(raw["t"])
    return {
        "symbol": symbol, "timestamp": raw["t"], "open": float(raw.get("o") or 0),
        "high": float(raw.get("h") or 0), "low": float(raw.get("l") or 0),
        "close": float(raw.get("c") or 0), "volume": int(raw.get("v") or 0),
        "trade_count": int(raw.get("n") or 0), "vwap": float(raw.get("vw") or 0),
        "timeframe": timeframe, "adjustment": adjustment, "session": session_label(ts),
        "source_window": window_name, "raw_json": json_text(raw),
    }


@dataclass
class SellabilityResult:
    eligible: bool
    status: str
    exact_cross_timestamp: datetime
    exact_cross_timestamp_raw: str
    adjusted_threshold_price: float
    raw_threshold_price: float
    adjustment_scale: float
    minimum_notional: float
    window_seconds: int
    active_bid_at_cross_price: float | None
    active_bid_at_cross_notional: float | None
    displayed_seconds_at_or_above_threshold: float
    max_contiguous_displayed_seconds: float
    seconds_to_first_confirmed_exit: float | None
    first_confirmed_exit_slippage_bps: float | None
    max_bid_price: float
    max_bid_notional: float
    max_trade_price_after_cross: float
    trade_volume_at_or_above_threshold: int
    trade_count_at_or_above_threshold: int
    first_confirmed_exit_timestamp: datetime | None
    first_confirmed_exit_timestamp_raw: str | None
    first_confirmed_exit_bid: float | None
    first_confirmed_exit_notional: float | None
    horizon_metrics: dict[str, Any]
    flags: list[str]
    raw_trades: list[dict[str, Any]]
    raw_quotes: list[dict[str, Any]]

    def db_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible, "sellability_status": self.status,
            "exact_cross_timestamp": self.exact_cross_timestamp.isoformat(),
            "exact_cross_timestamp_raw": self.exact_cross_timestamp_raw,
            "adjusted_threshold_price": self.adjusted_threshold_price,
            "raw_threshold_price": self.raw_threshold_price, "adjustment_scale": self.adjustment_scale,
            "minimum_notional": self.minimum_notional,
            "sellability_window_seconds": self.window_seconds,
            "active_bid_at_cross_price": self.active_bid_at_cross_price,
            "active_bid_at_cross_notional": self.active_bid_at_cross_notional,
            "displayed_seconds_at_or_above_threshold": self.displayed_seconds_at_or_above_threshold,
            "max_contiguous_displayed_seconds": self.max_contiguous_displayed_seconds,
            "seconds_to_first_confirmed_exit": self.seconds_to_first_confirmed_exit,
            "first_confirmed_exit_slippage_bps": self.first_confirmed_exit_slippage_bps,
            "max_bid_price": self.max_bid_price, "max_bid_notional": self.max_bid_notional,
            "max_trade_price_after_cross": self.max_trade_price_after_cross,
            "trade_volume_at_or_above_threshold": self.trade_volume_at_or_above_threshold,
            "trade_count_at_or_above_threshold": self.trade_count_at_or_above_threshold,
            "first_confirmed_exit_timestamp": self.first_confirmed_exit_timestamp.isoformat() if self.first_confirmed_exit_timestamp else None,
            "first_confirmed_exit_timestamp_raw": self.first_confirmed_exit_timestamp_raw,
            "first_confirmed_exit_bid": self.first_confirmed_exit_bid,
            "first_confirmed_exit_notional": self.first_confirmed_exit_notional,
            "horizon_metrics": self.horizon_metrics, "quality_flags": self.flags,
        }


class SellabilityAnalyzer:
    def __init__(self, alpaca: AlpacaClient):
        self.alpaca = alpaca

    @staticmethod
    def _max_gap_seconds(times_ns: list[int], start_ns: int, end_ns: int) -> float:
        points = [start_ns] + [t for t in times_ns if start_ns <= t <= end_ns] + [end_ns]
        return max(((right - left) / 1_000_000_000 for left, right in zip(points, points[1:])), default=0.0)

    @staticmethod
    def _interval_duration_metrics(intervals: list[tuple[int, int]]) -> tuple[float, float]:
        """Return total and longest contiguous duration in seconds, merging adjacent states."""
        if not intervals:
            return 0.0, 0.0
        ordered = sorted(intervals)
        merged: list[list[int]] = []
        for start_ns, end_ns in ordered:
            if not merged or start_ns > merged[-1][1]:
                merged.append([start_ns, end_ns])
            else:
                merged[-1][1] = max(merged[-1][1], end_ns)
        durations = [(end_ns - start_ns) / 1_000_000_000 for start_ns, end_ns in merged]
        return sum(durations), max(durations, default=0.0)

    def _latest_quote_before(
        self,
        symbol: str,
        *,
        event_date: date,
        exact_cross: datetime,
        exact_cross_ns: int,
        feed: str,
    ) -> dict[str, Any] | None:
        """Find the last quote state at the crossing when no quote update exists in its minute."""
        latest: dict[str, Any] | None = None
        latest_ns = -1
        # For thin securities this query is small. Active securities virtually always have
        # an update in the crossing minute and therefore never need this fallback.
        for page in self.alpaca.iter_quotes(
            symbol,
            start=et_dt(event_date, 4),
            end=exact_cross + timedelta(microseconds=1),
            feed=feed,
            asof=event_date,
        ):
            for raw in page:
                raw_ns = timestamp_ns(raw["t"])
                if raw_ns <= exact_cross_ns and raw_ns > latest_ns:
                    latest, latest_ns = raw, raw_ns
        return latest

    def analyze(self, result: dict[str, Any], adjusted_cross_bar: dict[str, Any] | None, *, minimum_notional: float, window_seconds: int, require_subsequent_trade: bool) -> SellabilityResult:
        symbol = result["symbol"]
        event_date = date.fromisoformat(result["event_date"])
        feed = result.get("feed") or "sip"
        cross_minute = parse_ts(result["threshold_cross_bar_start"])
        raw_bars = self.alpaca.get_single_bars(symbol, timeframe="1Min", start=cross_minute,
            end=cross_minute + timedelta(minutes=1), feed=feed, adjustment="raw", asof=event_date)
        flags: list[str] = []
        adjusted_high = float((adjusted_cross_bar or {}).get("high") or 0)
        raw_high = max((float(b.get("h") or 0) for b in raw_bars), default=0.0)
        if adjusted_high <= 0:
            adjusted_bars = self.alpaca.get_single_bars(
                symbol,
                timeframe="1Min",
                start=cross_minute,
                end=cross_minute + timedelta(minutes=1),
                feed=feed,
                adjustment="split",
                asof=event_date,
            )
            adjusted_high = max((float(b.get("h") or 0) for b in adjusted_bars), default=0.0)
            if adjusted_high > 0:
                flags.append("adjusted_cross_bar_refetched")
        if adjusted_high > 0 and raw_high > 0:
            scale = raw_high / adjusted_high
        else:
            scale = 1.0
            flags.append("raw_adjustment_scale_unverified")
        adjusted_threshold = float(result["threshold_price"])
        raw_threshold = adjusted_threshold * scale

        # The crossing may occur at the very end of the minute. Request one full extra
        # minute, then enforce the configured horizon from the exact nanosecond crossing.
        request_end = cross_minute + timedelta(seconds=window_seconds, minutes=1)
        trade_rows = [r for page in self.alpaca.iter_trades(symbol, start=cross_minute, end=request_end, feed=feed, asof=event_date) for r in page]
        quote_rows = [r for page in self.alpaca.iter_quotes(symbol, start=cross_minute, end=request_end, feed=feed, asof=event_date) for r in page]
        trades = sorted((normalize_trade(r, symbol, "sellability") for r in trade_rows), key=lambda r: timestamp_ns(r["timestamp"]))
        quotes = sorted((normalize_quote(r, symbol, "sellability", event_date) for r in quote_rows), key=lambda r: timestamp_ns(r["timestamp"]))
        threshold_trades = [t for t in trades if t["price"] >= raw_threshold]
        exact_cross_raw = threshold_trades[0]["timestamp"] if threshold_trades else cross_minute.isoformat()
        exact_cross = parse_ts(exact_cross_raw)
        exact_cross_ns = timestamp_ns(exact_cross_raw)
        if not threshold_trades:
            flags.append("no_raw_trade_matched_adjusted_crossing")

        # A historical quote is a state update, not an expiring order book snapshot. The
        # last quote before the crossing remains the active NBBO until the next update.
        pre_cross = [q for q in quotes if timestamp_ns(q["timestamp"]) <= exact_cross_ns]
        if not pre_cross:
            fallback = self._latest_quote_before(
                symbol,
                event_date=event_date,
                exact_cross=exact_cross,
                exact_cross_ns=exact_cross_ns,
                feed=feed,
            )
            if fallback:
                normalized = normalize_quote(fallback, symbol, "sellability_active_at_cross", event_date)
                quotes.append(normalized)
                quotes.sort(key=lambda r: timestamp_ns(r["timestamp"]))
                pre_cross = [normalized]
                flags.append("active_quote_recovered_before_crossing_minute")
        active_at_cross = pre_cross[-1] if pre_cross else None
        if not active_at_cross:
            flags.append("no_active_nbbo_quote_at_crossing")

        sellability_end_ns = exact_cross_ns + window_seconds * 1_000_000_000
        trades_after = [
            t for t in trades
            if exact_cross_ns <= timestamp_ns(t["timestamp"]) <= sellability_end_ns
        ]
        quote_updates_after = [
            q for q in quotes
            if exact_cross_ns < timestamp_ns(q["timestamp"]) <= sellability_end_ns
        ]

        # Build quote-state intervals. A pre-cross quote starts effectively at the crossing;
        # each later quote supersedes the previous state at its exact nanosecond timestamp.
        state_quotes: list[dict[str, Any]] = []
        if active_at_cross:
            state_quotes.append(active_at_cross)
        state_quotes.extend(quote_updates_after)
        state_quotes.sort(key=lambda q: timestamp_ns(q["timestamp"]))
        # If an update occurs exactly at the crossing, it supersedes any older state.
        same_or_before = [q for q in quotes if timestamp_ns(q["timestamp"]) <= exact_cross_ns]
        if same_or_before:
            latest_at_cross = same_or_before[-1]
            state_quotes = [latest_at_cross] + [q for q in state_quotes if timestamp_ns(q["timestamp"]) > exact_cross_ns]
            active_at_cross = latest_at_cross

        intervals: list[tuple[int, int, dict[str, Any]]] = []
        for idx, q in enumerate(state_quotes):
            q_ns = timestamp_ns(q["timestamp"])
            interval_start = max(exact_cross_ns, q_ns)
            next_ns = timestamp_ns(state_quotes[idx + 1]["timestamp"]) if idx + 1 < len(state_quotes) else sellability_end_ns
            interval_end = min(sellability_end_ns, next_ns)
            if interval_end >= interval_start:
                intervals.append((interval_start, interval_end, q))

        threshold_trade_times = [timestamp_ns(t["timestamp"]) for t in trades_after if t["price"] >= raw_threshold]
        horizon_metrics: dict[str, Any] = {}
        for horizon in sorted(set((30, 60, window_seconds))):
            h_end_ns = min(sellability_end_ns, exact_cross_ns + horizon * 1_000_000_000)
            h_intervals = [(max(start_ns, exact_cross_ns), min(end_ns, h_end_ns), q) for start_ns, end_ns, q in intervals if start_ns <= h_end_ns and end_ns >= exact_cross_ns]
            h_intervals = [(start_ns, end_ns, q) for start_ns, end_ns, q in h_intervals if end_ns >= start_ns]
            h_trades = [t for t in trades_after if timestamp_ns(t["timestamp"]) <= h_end_ns and t["price"] >= raw_threshold]
            displayed_intervals = [
                (start_ns, end_ns)
                for start_ns, end_ns, q in h_intervals
                if q["bid_price"] >= raw_threshold
            ]
            displayed_total, max_contiguous = self._interval_duration_metrics(displayed_intervals)
            max_bid = max((q["bid_price"] for _, _, q in h_intervals), default=0.0)
            horizon_metrics[str(horizon)] = {
                "max_bid_price": max_bid,
                "max_bid_slippage_bps_vs_threshold": ((max_bid / raw_threshold) - 1.0) * 10_000 if max_bid and raw_threshold else None,
                "max_displayed_bid_notional": max((q["bid_price"] * q["bid_size_shares"] for _, _, q in h_intervals), default=0.0),
                "displayed_seconds_at_or_above_threshold": displayed_total,
                "max_contiguous_displayed_seconds_at_or_above_threshold": max_contiguous,
                "trade_count_at_or_above_threshold": len(h_trades),
                "trade_volume_at_or_above_threshold": sum(t["size_shares"] for t in h_trades),
                "displayed_bid_levels": {
                    str(int(level)): any(
                        q["bid_price"] >= raw_threshold and q["bid_price"] * q["bid_size_shares"] >= level
                        for _, _, q in h_intervals
                    )
                    for level in NOTIONAL_LEVELS
                },
            }

        confirmed: list[tuple[int, dict[str, Any]]] = []
        for interval_start, interval_end, q in intervals:
            if q["bid_price"] < raw_threshold or q["bid_price"] * q["bid_size_shares"] < minimum_notional:
                continue
            i = bisect.bisect_right(threshold_trade_times, interval_start)
            later_trade_while_bid_active = i < len(threshold_trade_times) and threshold_trade_times[i] <= interval_end
            if later_trade_while_bid_active or not require_subsequent_trade:
                confirmed.append((interval_start, q))
        threshold_after = [t for t in trades_after if t["price"] >= raw_threshold]
        displayed = any(
            q["bid_price"] >= raw_threshold and q["bid_price"] * q["bid_size_shares"] >= minimum_notional
            for _, _, q in intervals
        )
        full_displayed_intervals = [
            (start_ns, end_ns)
            for start_ns, end_ns, q in intervals
            if q["bid_price"] >= raw_threshold
        ]
        displayed_seconds, max_contiguous_displayed = self._interval_duration_metrics(full_displayed_intervals)

        if confirmed:
            first_ns, first_q = confirmed[0]
            # Preserve exact nanosecond precision for the effective first exit opportunity.
            first_ts_raw = exact_cross_raw if first_ns == exact_cross_ns else first_q["timestamp"]
            first_ts = parse_ts(first_ts_raw)
            eligible, status = True, "confirmed_sellable"
            first_bid = first_q["bid_price"]
            first_notional = first_q["bid_price"] * first_q["bid_size_shares"]
            seconds_to_first = (first_ns - exact_cross_ns) / 1_000_000_000
            first_slippage_bps = ((first_bid / raw_threshold) - 1.0) * 10_000 if raw_threshold else None
            if timestamp_ns(first_q["timestamp"]) < exact_cross_ns:
                flags.append("sellable_bid_active_at_crossing")
        else:
            first_ts = first_ts_raw = first_bid = first_notional = None
            seconds_to_first = first_slippage_bps = None
            eligible = False
            status = "displayed_bid_only" if displayed else ("trade_only" if threshold_after else "not_confirmed")

        active_bid_price = active_at_cross["bid_price"] if active_at_cross else None
        active_bid_notional = active_at_cross["bid_price"] * active_at_cross["bid_size_shares"] if active_at_cross else None
        trade_times_all = [timestamp_ns(t["timestamp"]) for t in trades_after if timestamp_ns(t["timestamp"]) > exact_cross_ns]
        quote_update_times = [timestamp_ns(q["timestamp"]) for q in quote_updates_after]
        horizon_metrics["summary"] = {
            "active_quote_source_timestamp": active_at_cross["timestamp"] if active_at_cross else None,
            "active_bid_at_cross_price": active_bid_price,
            "active_bid_at_cross_notional": active_bid_notional,
            "seconds_to_next_trade_after_cross": (trade_times_all[0] - exact_cross_ns) / 1_000_000_000 if trade_times_all else None,
            "seconds_to_next_quote_update_after_cross": (quote_update_times[0] - exact_cross_ns) / 1_000_000_000 if quote_update_times else None,
            "max_trade_gap_seconds_in_window": self._max_gap_seconds(trade_times_all, exact_cross_ns, sellability_end_ns),
            "max_quote_update_gap_seconds_in_window": self._max_gap_seconds(quote_update_times, exact_cross_ns, sellability_end_ns),
            "sellability_window_end_raw_epoch_ns": sellability_end_ns,
        }

        return SellabilityResult(
            eligible=eligible, status=status, exact_cross_timestamp=exact_cross,
            exact_cross_timestamp_raw=exact_cross_raw,
            adjusted_threshold_price=adjusted_threshold, raw_threshold_price=raw_threshold,
            adjustment_scale=scale, minimum_notional=minimum_notional, window_seconds=window_seconds,
            active_bid_at_cross_price=active_bid_price,
            active_bid_at_cross_notional=active_bid_notional,
            displayed_seconds_at_or_above_threshold=displayed_seconds,
            max_contiguous_displayed_seconds=max_contiguous_displayed,
            seconds_to_first_confirmed_exit=seconds_to_first,
            first_confirmed_exit_slippage_bps=first_slippage_bps,
            max_bid_price=max((q["bid_price"] for _, _, q in intervals), default=0.0),
            max_bid_notional=max((q["bid_price"] * q["bid_size_shares"] for _, _, q in intervals), default=0.0),
            max_trade_price_after_cross=max((t["price"] for t in trades_after), default=0.0),
            trade_volume_at_or_above_threshold=sum(t["size_shares"] for t in threshold_after),
            trade_count_at_or_above_threshold=len(threshold_after),
            first_confirmed_exit_timestamp=first_ts, first_confirmed_exit_timestamp_raw=first_ts_raw,
            first_confirmed_exit_bid=first_bid,
            first_confirmed_exit_notional=first_notional, horizon_metrics=horizon_metrics,
            flags=flags, raw_trades=trades, raw_quotes=quotes,
        )


class ResearchRunner:
    def __init__(self, settings: Settings, store: SupabaseStore, alpaca: AlpacaClient):
        self.settings = settings
        self.store = store
        self.alpaca = alpaca
        self.sellability = SellabilityAnalyzer(alpaca)

    def _update(self, job_id: str, stage: str, current: int, total: int, **extra: Any) -> None:
        self.store.update_research_job(job_id, progress_stage=stage, progress_current=current, progress_total=total, **extra)

    def _prior_sessions(self, event_date: date, count: int) -> list[date]:
        calendar = self.alpaca.get_calendar(event_date - timedelta(days=count * 3 + 15), event_date)
        days = sorted(date.fromisoformat(r["date"]) for r in calendar if date.fromisoformat(r["date"]) < event_date)
        return days[-count:]

    def _write_parquet(self, path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> int:
        if not rows:
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd", use_dictionary=True)
        return len(rows)

    def _register_file(self, job_id: str, event_id: str | None, path: Path, storage_path: str, kind: str, content_type: str = "application/octet-stream") -> dict[str, Any]:
        self.store.upload_file(path, storage_path, content_type=content_type)
        return self.store.upsert("stock25_research_files", {
            "research_job_id": job_id, "research_event_id": event_id,
            "file_kind": kind, "storage_path": storage_path, "filename": path.name,
            "size_bytes": path.stat().st_size, "sha256": sha256_file(path),
        }, on_conflict="research_job_id,storage_path", return_representation=True)[0]

    def _write_upload_rows_safely(self, *, job_id: str, event_id: str, event_prefix: str, kind: str, base_name: str, rows: list[dict[str, Any]], schema: pa.Schema, temp_dir: Path, part_no: int) -> tuple[list[dict[str, Any]], int]:
        """Write one API page, recursively split if the compressed object exceeds 45 MB."""
        if not rows:
            return [], part_no
        path = temp_dir / f"{base_name}_part{part_no:05d}.parquet"
        self._write_parquet(path, rows, schema)
        if path.stat().st_size > MAX_FREE_SAFE_OBJECT_BYTES and len(rows) > 1:
            path.unlink()
            midpoint = len(rows) // 2
            first, next_no = self._write_upload_rows_safely(job_id=job_id, event_id=event_id, event_prefix=event_prefix, kind=kind, base_name=base_name, rows=rows[:midpoint], schema=schema, temp_dir=temp_dir, part_no=part_no)
            second, next_no = self._write_upload_rows_safely(job_id=job_id, event_id=event_id, event_prefix=event_prefix, kind=kind, base_name=base_name, rows=rows[midpoint:], schema=schema, temp_dir=temp_dir, part_no=next_no)
            return first + second, next_no
        storage_path = f"{event_prefix}/raw/{kind}/{path.name}"
        file_row = self._register_file(job_id, event_id, path, storage_path, f"raw_{kind}", "application/vnd.apache.parquet")
        path.unlink(missing_ok=True)
        return [file_row], part_no + 1

    def _stream_pages(self, *, job_id: str, event_id: str, event_prefix: str, kind: str, window_name: str, pages: Iterable[list[dict[str, Any]]], normalizer: Callable[[dict[str, Any]], dict[str, Any]], schema: pa.Schema, aggregator: SecondAggregator, temp_dir: Path, exclusive_end_ns: int | None = None) -> tuple[int, list[dict[str, Any]], bool]:
        total, files, truncated, part_no = 0, [], False, 1
        row_limit = self.settings.max_raw_rows_per_file
        for page in pages:
            rows = [normalizer(r) for r in page]
            if exclusive_end_ns is not None:
                # Enforce the boundary ourselves at nanosecond precision even if an API end is inclusive.
                rows = [row for row in rows if timestamp_ns(row["timestamp"]) < exclusive_end_ns]
            if row_limit and total + len(rows) > row_limit:
                rows = rows[: max(0, row_limit - total)]
                truncated = True
            for row in rows:
                aggregator.add_trade(row) if kind == "trades" else aggregator.add_quote(row)
            new_files, part_no = self._write_upload_rows_safely(job_id=job_id, event_id=event_id, event_prefix=event_prefix, kind=kind, base_name=safe_name(window_name), rows=rows, schema=schema, temp_dir=temp_dir, part_no=part_no)
            files.extend(new_files)
            total += len(rows)
            if row_limit and total >= row_limit:
                break
        return total, files, truncated

    def _upload_small_component(self, job_id: str, event_id: str, event_prefix: str, path: Path, kind: str) -> dict[str, Any] | None:
        if not path.exists():
            return None
        storage_path = f"{event_prefix}/derived/{path.name}"
        return self._register_file(job_id, event_id, path, storage_path, kind, "application/vnd.apache.parquet")


    def _store_sellability_evidence(
        self,
        job_id: str,
        result: dict[str, Any],
        sell: SellabilityResult,
        research_event_id: str,
    ) -> list[dict[str, Any]]:
        """Persist the auditable threshold-window evidence for every source event."""
        symbol = result["symbol"]
        event_date = date.fromisoformat(result["event_date"])
        event_key = f"{event_date.isoformat()}_{safe_name(symbol)}"
        event_prefix = f"jobs/{job_id}/events/{event_key}"
        temp_dir = Path(self.settings.temp_root) / job_id / "sellability_evidence" / event_key
        temp_dir.mkdir(parents=True, exist_ok=True)
        files: list[dict[str, Any]] = []
        try:
            for kind, rows, schema in (
                ("sellability_trades", sell.raw_trades, TRADE_SCHEMA),
                ("sellability_quotes", sell.raw_quotes, QUOTE_SCHEMA),
            ):
                chunk_files, _ = self._write_upload_rows_safely(
                    job_id=job_id,
                    event_id=research_event_id,
                    event_prefix=event_prefix,
                    kind=kind,
                    base_name=kind,
                    rows=rows,
                    schema=schema,
                    temp_dir=temp_dir,
                    part_no=1,
                )
                files.extend(chunk_files)
            metadata_path = temp_dir / "sellability_metadata.json"
            metadata_path.write_text(
                json.dumps(
                    {
                        "source_result": result,
                        "sellability": sell.db_dict(),
                        "trade_rows": len(sell.raw_trades),
                        "quote_rows": len(sell.raw_quotes),
                        "definition": "Confirmed sellable requires the configured displayed NBBO bid notional at/above threshold and, when enabled, a strictly later threshold trade while that bid remains active.",
                    },
                    indent=2,
                    default=str,
                ),
                encoding="utf-8",
            )
            files.append(
                self._register_file(
                    job_id,
                    research_event_id,
                    metadata_path,
                    f"{event_prefix}/sellability/{metadata_path.name}",
                    "sellability_metadata",
                    "application/json",
                )
            )
            return files
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _event_package(self, job_id: str, result: dict[str, Any], sell: SellabilityResult, params: dict[str, Any], research_event_id: str, initial_files: list[dict[str, Any]] | None = None) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
        symbol = result["symbol"]
        event_date = date.fromisoformat(result["event_date"])
        prior_sessions = self._prior_sessions(event_date, int(params["prior_sessions"]))
        root = Path(self.settings.temp_root) / job_id
        event_key = f"{event_date.isoformat()}_{safe_name(symbol)}"
        event_dir = root / event_key
        event_dir.mkdir(parents=True, exist_ok=True)
        temp_chunks = event_dir / "chunks"
        temp_chunks.mkdir()
        event_prefix = f"jobs/{job_id}/events/{event_key}"
        files: list[dict[str, Any]] = list(initial_files or [])
        counts = {"minute_bars": 0, "trades": 0, "quotes": 0, "second_rows": 0, "auctions": 0, "news": 0, "corporate_actions": 0, "sellability_trades": len(sell.raw_trades), "sellability_quotes": len(sell.raw_quotes)}
        quality_flags = list(sell.flags)
        feed = result.get("feed") or "sip"
        windows: list[tuple[str, datetime, datetime]] = [(d.isoformat(), et_dt(d, 4), et_dt(d, 20)) for d in prior_sessions]
        # Request one microsecond beyond the raw nanosecond crossing, then enforce the exact
        # boundary locally. This avoids dropping pre-cross ticks in the same microsecond.
        windows.append((f"{event_date.isoformat()}_to_cross", et_dt(event_date, 4), sell.exact_cross_timestamp + timedelta(microseconds=1)))
        minute_rows: list[dict[str, Any]] = []

        for window_name, start, end in windows:
            if end <= start:
                continue
            raw_bars = self.alpaca.get_single_bars(symbol, timeframe="1Min", start=start, end=end, feed=feed, adjustment="raw", asof=event_date)
            for bar in raw_bars:
                # A full one-minute bar whose minute overlaps the crossing contains future trades.
                # Keep only bars fully completed before the exact event boundary.
                bar_start = parse_ts(bar["t"])
                if bar_start >= end:
                    continue
                if window_name.endswith("_to_cross") and bar_start + timedelta(minutes=1) > sell.exact_cross_timestamp:
                    continue
                minute_rows.append(normalize_bar(bar, symbol, "1Min", "raw", window_name))
            agg = SecondAggregator(window_name)
            window_exclusive_end_ns = (
                timestamp_ns(sell.exact_cross_timestamp_raw)
                if window_name.endswith("_to_cross")
                else timestamp_ns(end)
            )
            if params.get("include_raw_trades", True):
                n, f, trunc = self._stream_pages(job_id=job_id, event_id=research_event_id, event_prefix=event_prefix, kind="trades", window_name=window_name,
                    pages=self.alpaca.iter_trades(symbol, start=start, end=end, feed=feed, asof=event_date),
                    normalizer=lambda r, w=window_name: normalize_trade(r, symbol, w), schema=TRADE_SCHEMA, aggregator=agg, temp_dir=temp_chunks,
                    exclusive_end_ns=window_exclusive_end_ns)
                counts["trades"] += n; files.extend(f)
                if trunc: quality_flags.append(f"trades_truncated:{window_name}")
            if params.get("include_raw_quotes", True):
                n, f, trunc = self._stream_pages(job_id=job_id, event_id=research_event_id, event_prefix=event_prefix, kind="quotes", window_name=window_name,
                    pages=self.alpaca.iter_quotes(symbol, start=start, end=end, feed=feed, asof=event_date),
                    normalizer=lambda r, w=window_name: normalize_quote(r, symbol, w, event_date), schema=QUOTE_SCHEMA, aggregator=agg, temp_dir=temp_chunks,
                    exclusive_end_ns=window_exclusive_end_ns)
                counts["quotes"] += n; files.extend(f)
                if trunc: quality_flags.append(f"quotes_truncated:{window_name}")
            if params.get("derive_one_second", True):
                second_path = event_dir / f"second_summary_{safe_name(window_name)}.parquet"
                second_rows = agg.rows()
                counts["second_rows"] += self._write_parquet(second_path, second_rows, SECOND_SCHEMA)
                file_row = self._upload_small_component(job_id, research_event_id, event_prefix, second_path, "second_summary")
                if file_row: files.append(file_row)

        counts["minute_bars"] = self._write_parquet(event_dir / "minute_bars.parquet", minute_rows, BAR_SCHEMA)
        if minute_rows:
            files.append(self._upload_small_component(job_id, research_event_id, event_prefix, event_dir / "minute_bars.parquet", "minute_bars"))
        daily_rows: list[dict[str, Any]] = []
        daily_start = et_dt((prior_sessions[0] if prior_sessions else event_date - timedelta(days=20)) - timedelta(days=5), 0)
        # Exclude the event day's completed daily bar because it contains post-crossing prices.
        daily_end_exclusive = et_dt(event_date, 0)
        for adjustment in ("raw", "split"):
            for b in self.alpaca.get_single_bars(symbol, timeframe="1Day", start=daily_start, end=daily_end_exclusive, feed=feed, adjustment=adjustment, asof=event_date):
                if parse_ts(b["t"]) < daily_end_exclusive:
                    daily_rows.append(normalize_bar(b, symbol, "1Day", adjustment, "prior_context"))
        self._write_parquet(event_dir / "daily_bars.parquet", daily_rows, BAR_SCHEMA)
        if daily_rows: files.append(self._upload_small_component(job_id, research_event_id, event_prefix, event_dir / "daily_bars.parquet", "daily_bars"))

        if params.get("include_auctions", True) and prior_sessions:
            auction_rows: list[dict[str, Any]] = []
            try:
                for page in self.alpaca.iter_auctions(symbol, start=et_dt(prior_sessions[0], 0), end=sell.exact_cross_timestamp + timedelta(microseconds=1), asof=event_date):
                    for row in page:
                        timestamp = str(row.get("t") or row.get("timestamp") or "")
                        if timestamp and timestamp_ns(timestamp) >= timestamp_ns(sell.exact_cross_timestamp_raw):
                            continue
                        auction_rows.append({"symbol": symbol, "timestamp": timestamp, "kind": "auction", "raw_json": json_text(row)})
                counts["auctions"] = self._write_parquet(event_dir / "auctions.parquet", auction_rows, GENERIC_SCHEMA)
                if auction_rows: files.append(self._upload_small_component(job_id, research_event_id, event_prefix, event_dir / "auctions.parquet", "auctions"))
            except Exception as exc:
                quality_flags.append(f"auctions_unavailable:{type(exc).__name__}")
        if params.get("include_news", True) and prior_sessions:
            try:
                news = self.alpaca.get_news(symbol, start=et_dt(prior_sessions[0], 0), end=sell.exact_cross_timestamp + timedelta(microseconds=1), include_content=True)
                news_rows = []
                for item in news:
                    timestamp = str(item.get("created_at") or item.get("updated_at") or "")
                    if timestamp and timestamp_ns(timestamp) >= timestamp_ns(sell.exact_cross_timestamp_raw):
                        continue
                    news_rows.append({"symbol": symbol, "timestamp": timestamp, "kind": "news", "raw_json": json_text(item)})
                counts["news"] = self._write_parquet(event_dir / "news.parquet", news_rows, GENERIC_SCHEMA)
                if news_rows: files.append(self._upload_small_component(job_id, research_event_id, event_prefix, event_dir / "news.parquet", "news"))
            except Exception as exc:
                quality_flags.append(f"news_unavailable:{type(exc).__name__}")
        if params.get("include_corporate_actions", True):
            try:
                actions = self.alpaca.get_corporate_actions(symbol, start=event_date - timedelta(days=45), end=event_date)
                action_rows = [{"symbol": symbol, "timestamp": str(a.get("process_date") or a.get("ex_date") or ""), "kind": "corporate_action", "raw_json": json_text(a)} for a in actions]
                counts["corporate_actions"] = self._write_parquet(event_dir / "corporate_actions.parquet", action_rows, GENERIC_SCHEMA)
                if action_rows:
                    quality_flags.append("corporate_actions_point_in_time_publication_unverified")
                    files.append(self._upload_small_component(job_id, research_event_id, event_prefix, event_dir / "corporate_actions.parquet", "corporate_actions"))
            except Exception as exc:
                quality_flags.append(f"corporate_actions_unavailable:{type(exc).__name__}")

        # Record every separately stored object for this event inside the compact package.
        manifest_fields = ["file_kind", "filename", "storage_path", "size_bytes", "sha256"]
        with (event_dir / "event_file_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=manifest_fields)
            writer.writeheader()
            for file_row in files:
                if file_row:
                    writer.writerow({key: file_row.get(key) for key in manifest_fields})

        metadata = {
            "job_id": job_id, "research_event_id": research_event_id, "source_result": result,
            "sellability": sell.db_dict(), "prior_trading_sessions": [d.isoformat() for d in prior_sessions],
            "collection_windows": [{"name": n, "start": s.isoformat(), "end": e.isoformat()} for n, s, e in windows],
            "row_counts": counts, "quality_flags": sorted(set(quality_flags)),
            "leakage_boundary": sell.exact_cross_timestamp.isoformat(),
            "leakage_boundary_raw_nanosecond": sell.exact_cross_timestamp_raw,
            "leakage_rule": "All research-input trades and quotes have timestamps strictly earlier than the exact first threshold-reaching trade. The overlapping one-minute bar and event-day daily bar are excluded.",
            "storage_layout": "Compact ZIP plus separately downloadable raw trade/quote and derived Parquet objects under this event prefix.",
            "compact_package_exclusions": ["raw trade chunks", "raw quote chunks", "one-second summaries", "sellability raw trades", "sellability raw quotes"],
            "quote_size_normalisation": "Alpaca SIP quote sizes are treated as shares on/after 2025-11-03 and round lots before that date.",
        }
        (event_dir / "event_metadata.json").write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")
        (event_dir / "data_quality_report.json").write_text(json.dumps({"quality_flags": metadata["quality_flags"], "row_counts": counts}, indent=2), encoding="utf-8")
        # Keep the compact ZIP safely below Supabase Free's per-object limit. High-volume
        # second summaries and sellability tick files remain separately downloadable.
        compact_excluded_prefixes = ("second_summary_", "sellability_trades", "sellability_quotes")
        archive = root / f"{event_key}_compact.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for path in sorted(event_dir.iterdir()):
                if path.is_file() and not path.name.startswith(compact_excluded_prefixes):
                    zf.write(path, path.name)
        if archive.stat().st_size > MAX_FREE_SAFE_OBJECT_BYTES:
            raise RuntimeError(
                f"Compact event package exceeded the 45 MB safety limit ({archive.stat().st_size} bytes). "
                "Disable full-content news or use a Supabase plan with a larger object limit."
            )
        shutil.rmtree(event_dir)
        return archive, metadata, [f for f in files if f]

    def _build_index(self, job: dict[str, Any], event_rows: list[dict[str, Any]], file_rows: list[dict[str, Any]]) -> Path:
        root = Path(self.settings.temp_root) / job["id"]
        index_dir = root / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        fields = sorted({k for row in event_rows for k in row}) if event_rows else ["research_job_id"]
        with (index_dir / "event_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields); writer.writeheader()
            for row in event_rows:
                writer.writerow({k: json_text(v) if isinstance(v, (dict, list)) else v for k, v in row.items()})
        sellability_fields = [
            "source_result_id", "symbol", "event_date", "eligible", "sellability_status",
            "exact_cross_timestamp", "exact_cross_timestamp_raw", "raw_threshold_price",
            "minimum_notional", "sellability_window_seconds", "active_bid_at_cross_price",
            "active_bid_at_cross_notional", "displayed_seconds_at_or_above_threshold",
            "max_contiguous_displayed_seconds", "seconds_to_first_confirmed_exit",
            "first_confirmed_exit_timestamp", "first_confirmed_exit_timestamp_raw",
            "first_confirmed_exit_bid", "first_confirmed_exit_notional",
            "first_confirmed_exit_slippage_bps", "max_bid_price", "max_bid_notional",
            "max_trade_price_after_cross", "trade_volume_at_or_above_threshold",
            "trade_count_at_or_above_threshold", "quality_flags", "horizon_metrics",
        ]
        with (index_dir / "sellability_analysis.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=sellability_fields); writer.writeheader()
            for row in event_rows:
                writer.writerow({
                    key: json_text(row.get(key)) if isinstance(row.get(key), (dict, list)) else row.get(key)
                    for key in sellability_fields
                })
        file_fields = ["research_event_id", "file_kind", "filename", "storage_path", "size_bytes", "sha256"]
        with (index_dir / "download_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=file_fields); writer.writeheader()
            for row in file_rows: writer.writerow({k: row.get(k) for k in file_fields})
        (index_dir / "research_job.json").write_text(json.dumps(job, indent=2, default=str), encoding="utf-8")
        (index_dir / "README.txt").write_text(
            "Research inputs end strictly before each event's exact first threshold-reaching trade. Post-crossing data is isolated for sellability validation. Each eligible event has a compact ZIP; complete raw trades and quotes are stored as page-sized Parquet chunks to avoid truncation and large-file failures. Use the app's event file view to generate fresh signed download links.\n",
            encoding="utf-8",
        )
        archive = root / "research_index.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(index_dir.iterdir()):
                if path.is_file(): zf.write(path, path.name)
        return archive

    def run(self, job: dict[str, Any]) -> None:
        job_id, source_scan_id = job["id"], job["source_scan_id"]
        params = {
            "prior_sessions": self.settings.default_prior_sessions,
            "minimum_sellable_notional": self.settings.default_sellability_notional,
            "sellability_window_seconds": self.settings.default_sellability_window_seconds,
            "require_subsequent_trade": True,
            "include_raw_trades": self.settings.default_include_raw_trades,
            "include_raw_quotes": self.settings.default_include_raw_quotes,
            "derive_one_second": True, "include_news": self.settings.default_include_news,
            "include_auctions": self.settings.default_include_auctions,
            "include_corporate_actions": self.settings.default_include_corporate_actions,
            "max_events": 0, **(job.get("parameters") or {}),
        }
        try:
            source_results = self.store.select_all("stock25_scan_results", filters={"scan_id": f"eq.{source_scan_id}"}, order="event_date.asc,symbol.asc")
            if int(params.get("max_events") or 0): source_results = source_results[: int(params["max_events"])]
            total = len(source_results)
            self._update(job_id, "sellability_and_collection", 0, total, source_event_count=total, failed_event_count=0)
            eligible_count = completed_count = failed_count = 0
            for idx, result in enumerate(source_results, start=1):
                existing = self.store.select("stock25_research_events", filters={"research_job_id": f"eq.{job_id}", "source_result_id": f"eq.{result['id']}"}, limit=1)
                if existing and existing[0].get("status") == "completed":
                    completed_count += 1; eligible_count += int(bool(existing[0].get("eligible")))
                    self._update(job_id, "sellability_and_collection", idx, total, eligible_event_count=eligible_count, completed_event_count=completed_count, failed_event_count=failed_count)
                    continue
                if existing:
                    # Remove stale manifest rows from an interrupted/failed attempt. Objects use
                    # deterministic paths and will be overwritten; orphaned extra objects are not exposed.
                    self.store.delete("stock25_research_files", {"research_event_id": f"eq.{existing[0]['id']}"})
                research_event = self.store.upsert("stock25_research_events", {
                    "research_job_id": job_id, "source_result_id": result["id"],
                    "symbol": result["symbol"], "event_date": result["event_date"],
                    "status": "processing", "error_message": None, "completed_at": None,
                }, on_conflict="research_job_id,source_result_id", return_representation=True)[0]
                try:
                    bars = self.store.select("stock25_event_bars", filters={"result_id": f"eq.{result['id']}", "bar_timestamp": f"eq.{result['threshold_cross_bar_start']}"}, limit=1)
                    sell = self.sellability.analyze(result, bars[0] if bars else None,
                        minimum_notional=float(params["minimum_sellable_notional"]),
                        window_seconds=int(params["sellability_window_seconds"]),
                        require_subsequent_trade=bool(params["require_subsequent_trade"]))
                    update = sell.db_dict(); update["status"] = "collecting" if sell.eligible else "completed"
                    self.store.update("stock25_research_events", {"id": f"eq.{research_event['id']}"}, update)
                    evidence_files = self._store_sellability_evidence(job_id, result, sell, research_event["id"])
                    if sell.eligible:
                        archive, metadata, _ = self._event_package(job_id, result, sell, params, research_event["id"], initial_files=evidence_files)
                        storage_path = f"jobs/{job_id}/events/{archive.name}"
                        self._register_file(job_id, research_event["id"], archive, storage_path, "event_compact_package", "application/zip")
                        self.store.update("stock25_research_events", {"id": f"eq.{research_event['id']}"}, {
                            "status": "completed", "event_storage_path": storage_path,
                            "event_package_size_bytes": archive.stat().st_size,
                            "row_counts": metadata["row_counts"], "quality_flags": metadata["quality_flags"],
                            "completed_at": datetime.now(UTC).isoformat(),
                        })
                        eligible_count += 1; archive.unlink(missing_ok=True)
                    else:
                        self.store.update("stock25_research_events", {"id": f"eq.{research_event['id']}"}, {
                            "row_counts": {
                                "sellability_trades": len(sell.raw_trades),
                                "sellability_quotes": len(sell.raw_quotes),
                            },
                            "quality_flags": sell.flags,
                            "completed_at": datetime.now(UTC).isoformat(),
                        })
                    completed_count += 1
                except Exception as exc:
                    logger.exception("Research event failed: %s %s", result.get("symbol"), result.get("event_date"))
                    self.store.update("stock25_research_events", {"id": f"eq.{research_event['id']}"}, {
                        "status": "failed", "error_message": str(exc)[:4000], "completed_at": datetime.now(UTC).isoformat()})
                    failed_count += 1
                self._update(job_id, "sellability_and_collection", idx, total, eligible_event_count=eligible_count, completed_event_count=completed_count, failed_event_count=failed_count)
            event_rows = self.store.select_all("stock25_research_events", filters={"research_job_id": f"eq.{job_id}"}, order="event_date.asc,symbol.asc")
            file_rows = self.store.select_all("stock25_research_files", filters={"research_job_id": f"eq.{job_id}"}, order="created_at.asc")
            final_status = "failed" if failed_count else "completed"
            final_stage = "completed_with_event_errors" if failed_count else "completed"
            final_error = (
                f"{failed_count} source event(s) failed. Select Retry and resume; completed events will be skipped."
                if failed_count else None
            )
            final_completed_at = datetime.now(UTC).isoformat()
            index_job = {
                **job,
                "parameters": params,
                "status": final_status,
                "completed_at": final_completed_at,
                "progress_stage": final_stage,
                "progress_current": total,
                "progress_total": total,
                "source_event_count": total,
                "eligible_event_count": eligible_count,
                "completed_event_count": completed_count,
                "failed_event_count": failed_count,
                "error_message": final_error,
            }
            index = self._build_index(index_job, event_rows, file_rows)
            index_storage_path = f"jobs/{job_id}/research_index.zip"
            index_row = self._register_file(job_id, None, index, index_storage_path, "research_index", "application/zip")
            index.unlink(missing_ok=True)
            self.store.update_research_job(job_id, status=final_status, completed_at=final_completed_at,
                progress_stage=final_stage, progress_current=total, progress_total=total,
                eligible_event_count=eligible_count, completed_event_count=completed_count,
                failed_event_count=failed_count, error_message=final_error,
                index_file_id=index_row["id"], index_storage_path=index_storage_path)
        except Exception as exc:
            logger.exception("Research job %s failed", job_id)
            self.store.update_research_job(job_id, status="failed", completed_at=datetime.now(UTC).isoformat(), progress_stage="failed", error_message=str(exc)[:4000])
        finally:
            shutil.rmtree(Path(self.settings.temp_root) / job_id, ignore_errors=True)
