from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import shutil
import statistics
import zipfile
from array import array
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from app.config import Settings
from app.research import parse_ts, timestamp_ns
from app.supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
UTC = timezone.utc
ET = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
NOTIONAL_LEVELS = (100.0, 500.0, 1000.0, 5000.0)
SNAPSHOT_SPECS = (
    ("preopen_1400_bst", time(14, 0), "14:00 Europe/London; normally 09:00 ET"),
    ("midday_1700_bst", time(17, 0), "17:00 Europe/London; normally 12:00 ET"),
    ("afternoon_1900_bst", time(19, 0), "19:00 Europe/London; normally 14:00 ET"),
)
MAX_FREE_SAFE_OBJECT_BYTES = 45 * 1024 * 1024


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in value)


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _number(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _iso_from_ns(ns: int) -> str:
    seconds, nanos = divmod(ns, 1_000_000_000)
    dt = datetime.fromtimestamp(seconds, UTC)
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{nanos:09d}Z"


def market_open_ns(event_date: date) -> int:
    return timestamp_ns(datetime.combine(event_date, time(9, 30), ET).astimezone(UTC))


def split_name(event_date: date) -> str:
    if event_date <= date(2026, 6, 9):
        return "discovery"
    if event_date <= date(2026, 6, 24):
        return "validation"
    return "sealed_test"


def snapshot_cutoffs(event_date: date) -> dict[str, int]:
    result: dict[str, int] = {}
    for name, local_time, _ in SNAPSHOT_SPECS:
        dt = datetime.combine(event_date, local_time, LONDON).astimezone(UTC)
        result[name] = timestamp_ns(dt)
    return result


class CompactQuoteIntervals:
    """Memory-bounded columnar quote intervals for multi-million-row events."""

    __slots__ = ("start_ns", "end_ns", "ask", "ask_notional")

    def __init__(self) -> None:
        self.start_ns = array("q")
        self.end_ns = array("q")
        self.ask = array("d")
        self.ask_notional = array("d")

    def append(self, start_ns: int, end_ns: int, ask: float, ask_notional: float) -> None:
        self.start_ns.append(start_ns)
        self.end_ns.append(end_ns)
        self.ask.append(ask)
        self.ask_notional.append(ask_notional)

    def __len__(self) -> int:
        return len(self.start_ns)


class EntryFeasibilityAnalyzer:
    """Reconstructs regular-session ask opportunities strictly before the first +25% trade."""

    def __init__(
        self,
        *,
        minimum_notional: float = 500.0,
        reaction_delay_seconds: float = 5.0,
        minimum_opportunity_seconds: float = 30.0,
        minimum_gross_edge_pct: float = 0.0,
        require_subsequent_trade: bool = True,
    ):
        self.minimum_notional = minimum_notional
        self.reaction_delay_seconds = reaction_delay_seconds
        self.minimum_opportunity_seconds = minimum_opportunity_seconds
        self.minimum_gross_edge_pct = minimum_gross_edge_pct
        self.require_subsequent_trade = require_subsequent_trade

    @staticmethod
    def _valid_quote(row: dict[str, Any]) -> bool:
        ask = _number(row.get("ask_price"), 0.0) or 0.0
        bid = _number(row.get("bid_price"), 0.0) or 0.0
        ask_size = _integer(row.get("ask_size_shares"))
        return ask > 0 and ask_size > 0 and (bid <= 0 or ask >= bid)

    def build_intervals(
        self,
        quote_rows: Iterable[dict[str, Any]],
        *,
        open_ns: int,
        cross_ns: int,
        raw_threshold: float,
    ) -> tuple[CompactQuoteIntervals, dict[str, Any]]:
        intervals = CompactQuoteIntervals()
        first_quote_after_open: dict[str, Any] | None = None
        current: dict[str, Any] | None = None
        invalid_quotes = 0
        regular_quote_count = 0

        def close_current(end_ns: int) -> None:
            nonlocal current
            if not current:
                return
            start_ns = max(open_ns, timestamp_ns(current["timestamp"]))
            end = min(cross_ns, end_ns)
            if end <= start_ns or not self._valid_quote(current):
                return
            ask = float(current["ask_price"])
            ask_size = _integer(current.get("ask_size_shares"))
            ask_notional = ask * ask_size
            if ask >= raw_threshold or ask_notional < min(NOTIONAL_LEVELS):
                return
            intervals.append(start_ns, end, ask, ask_notional)

        for row in quote_rows:
            try:
                ts_ns = timestamp_ns(row["timestamp"])
            except Exception:
                invalid_quotes += 1
                continue
            if ts_ns < open_ns:
                continue
            if ts_ns >= cross_ns:
                break
            regular_quote_count += 1
            if first_quote_after_open is None and self._valid_quote(row):
                first_quote_after_open = row
            if current is not None:
                close_current(ts_ns)
            current = row
        if current is not None:
            close_current(cross_ns)

        metadata = {
            "regular_quote_count": regular_quote_count,
            "invalid_quote_count": invalid_quotes,
            "first_quote_after_open_timestamp": first_quote_after_open.get("timestamp") if first_quote_after_open else None,
            "first_quote_after_open_ask": _number(first_quote_after_open.get("ask_price")) if first_quote_after_open else None,
            "first_quote_after_open_ask_notional": (
                (_number(first_quote_after_open.get("ask_price"), 0.0) or 0.0)
                * _integer(first_quote_after_open.get("ask_size_shares"))
                if first_quote_after_open else None
            ),
        }
        return intervals, metadata

    def confirm_window(
        self,
        intervals: CompactQuoteIntervals,
        trade_rows: Iterable[dict[str, Any]],
        *,
        window_start_ns: int,
        cross_ns: int,
    ) -> tuple[array, dict[str, Any]]:
        confirmations = array("q", [0]) * len(intervals)
        if not intervals:
            return confirmations, {"regular_trade_count": 0, "invalid_trade_count": 0}
        idx = 0
        regular_trade_count = 0
        invalid_trade_count = 0
        effective_start = window_start_ns + int(self.reaction_delay_seconds * 1_000_000_000)
        count = len(intervals)
        for row in trade_rows:
            try:
                ts_ns = timestamp_ns(row["timestamp"])
                price = float(row.get("price") or 0)
            except Exception:
                invalid_trade_count += 1
                continue
            if ts_ns >= cross_ns:
                break
            while idx < count and intervals.end_ns[idx] < ts_ns:
                idx += 1
            if idx >= count:
                break
            if not (intervals.start_ns[idx] < ts_ns <= intervals.end_ns[idx]):
                continue
            regular_trade_count += 1
            if ts_ns < effective_start or price + 1e-12 < intervals.ask[idx]:
                continue
            if confirmations[idx] == 0:
                confirmations[idx] = ts_ns
        return confirmations, {
            "regular_trade_count": regular_trade_count,
            "invalid_trade_count": invalid_trade_count,
        }

    def summarize_window(
        self,
        intervals: CompactQuoteIntervals,
        confirmations: array,
        *,
        open_ns: int,
        cross_ns: int,
        raw_threshold: float,
        nominal_start_ns: int,
    ) -> dict[str, Any]:
        effective_start_ns = max(open_ns, nominal_start_ns) + int(self.reaction_delay_seconds * 1_000_000_000)
        tolerance_ns = 2_000_000
        level_metrics: dict[str, Any] = {}

        for level in NOTIONAL_LEVELS:
            total_seconds = 0.0
            max_contiguous = 0.0
            max_notional = 0.0
            minimum_ask: float | None = None
            first_confirmed: tuple[int, float] | None = None
            first_actionable: tuple[int, float, float, float] | None = None
            displayed = False
            mechanically_purchasable = False

            segment_start = 0
            segment_end = 0
            segment_max_notional = 0.0
            segment_min_ask = math.inf
            segment_first_ask = 0.0
            segment_first_confirm_ns = 0
            segment_first_confirm_ask = 0.0
            segment_first_edge_confirm_ns = 0
            segment_first_edge_ask = 0.0
            segment_first_edge_pct = 0.0
            segment_active = False

            def finalize_segment() -> None:
                nonlocal total_seconds, max_contiguous, max_notional, minimum_ask
                nonlocal first_confirmed, first_actionable, displayed, mechanically_purchasable
                nonlocal segment_active
                if not segment_active:
                    return
                duration = (segment_end - segment_start) / 1_000_000_000
                displayed = True
                total_seconds += duration
                max_contiguous = max(max_contiguous, duration)
                max_notional = max(max_notional, segment_max_notional)
                minimum_ask = min(minimum_ask, segment_min_ask) if minimum_ask is not None else segment_min_ask
                if segment_first_confirm_ns or not self.require_subsequent_trade:
                    mechanically_purchasable = True
                    mechanical_ts = segment_first_confirm_ns or segment_start
                    mechanical_ask = segment_first_confirm_ask or segment_first_ask
                    if first_confirmed is None or mechanical_ts < first_confirmed[0]:
                        first_confirmed = (mechanical_ts, mechanical_ask)
                    if duration + 1e-12 >= self.minimum_opportunity_seconds:
                        action_ts = segment_first_edge_confirm_ns
                        action_ask = segment_first_edge_ask
                        action_edge = segment_first_edge_pct
                        if action_ts and action_edge + 1e-12 >= self.minimum_gross_edge_pct:
                            candidate = (action_ts, action_ask, action_edge, duration)
                            if first_actionable is None or candidate[0] < first_actionable[0]:
                                first_actionable = candidate
                segment_active = False

            for idx in range(len(intervals)):
                interval_end = intervals.end_ns[idx]
                interval_start = intervals.start_ns[idx]
                if interval_end <= effective_start_ns:
                    continue
                if interval_start >= cross_ns:
                    break
                if intervals.ask_notional[idx] + 1e-9 < level:
                    finalize_segment()
                    continue
                s = max(interval_start, effective_start_ns)
                e = min(interval_end, cross_ns)
                if e <= s:
                    continue
                ask = intervals.ask[idx]
                ask_notional = intervals.ask_notional[idx]
                confirm_ns = confirmations[idx] if idx < len(confirmations) else 0
                edge_pct = ((raw_threshold / ask) - 1.0) * 100 if ask > 0 else -math.inf
                if not segment_active or s - segment_end > tolerance_ns:
                    finalize_segment()
                    segment_active = True
                    segment_start = s
                    segment_end = e
                    segment_max_notional = ask_notional
                    segment_min_ask = ask
                    segment_first_ask = ask
                    segment_first_confirm_ns = confirm_ns
                    segment_first_confirm_ask = ask if confirm_ns else 0.0
                    edge_entry_ns = confirm_ns if self.require_subsequent_trade else s
                    if edge_entry_ns and edge_pct + 1e-12 >= self.minimum_gross_edge_pct:
                        segment_first_edge_confirm_ns = edge_entry_ns
                        segment_first_edge_ask = ask
                        segment_first_edge_pct = edge_pct
                    else:
                        segment_first_edge_confirm_ns = 0
                        segment_first_edge_ask = 0.0
                        segment_first_edge_pct = 0.0
                else:
                    segment_end = max(segment_end, e)
                    segment_max_notional = max(segment_max_notional, ask_notional)
                    segment_min_ask = min(segment_min_ask, ask)
                    if confirm_ns and (segment_first_confirm_ns == 0 or confirm_ns < segment_first_confirm_ns):
                        segment_first_confirm_ns = confirm_ns
                        segment_first_confirm_ask = ask
                    edge_entry_ns = confirm_ns if self.require_subsequent_trade else s
                    if edge_entry_ns and edge_pct + 1e-12 >= self.minimum_gross_edge_pct:
                        if segment_first_edge_confirm_ns == 0 or edge_entry_ns < segment_first_edge_confirm_ns:
                            segment_first_edge_confirm_ns = edge_entry_ns
                            segment_first_edge_ask = ask
                            segment_first_edge_pct = edge_pct
            finalize_segment()

            first_ts_ns = first_confirmed[0] if first_confirmed else None
            first_price = first_confirmed[1] if first_confirmed else None
            actionable_ts = first_actionable[0] if first_actionable else None
            actionable_ask = first_actionable[1] if first_actionable else None
            actionable_edge = first_actionable[2] if first_actionable else None
            actionable_duration = first_actionable[3] if first_actionable else None
            level_metrics[str(int(level))] = {
                "displayed": displayed,
                "mechanically_purchasable": mechanically_purchasable,
                "manually_actionable": first_actionable is not None,
                "total_displayed_seconds": total_seconds,
                "max_contiguous_displayed_seconds": max_contiguous,
                "max_displayed_ask_notional": max_notional,
                "minimum_displayed_ask": minimum_ask,
                "first_confirmed_entry_timestamp_raw": _iso_from_ns(first_ts_ns) if first_ts_ns else None,
                "first_confirmed_entry_ask": first_price,
                "first_confirmed_entry_gross_edge_to_threshold_pct": ((raw_threshold / first_price) - 1.0) * 100 if first_price else None,
                "first_actionable_entry_timestamp_raw": _iso_from_ns(actionable_ts) if actionable_ts else None,
                "first_actionable_entry_ask": actionable_ask,
                "first_actionable_entry_gross_edge_to_threshold_pct": actionable_edge,
                "first_actionable_segment_seconds": actionable_duration,
                "seconds_from_window_start_to_first_confirmed_entry": (first_ts_ns - effective_start_ns) / 1_000_000_000 if first_ts_ns else None,
                "seconds_from_first_confirmed_entry_to_threshold": (cross_ns - first_ts_ns) / 1_000_000_000 if first_ts_ns else None,
            }

        return {
            "nominal_window_start_raw": _iso_from_ns(nominal_start_ns),
            "effective_window_start_raw": _iso_from_ns(effective_start_ns),
            "seconds_available_until_threshold": max(0.0, (cross_ns - effective_start_ns) / 1_000_000_000),
            "levels": level_metrics,
        }

    def analyze_with_trade_factory(
        self,
        *,
        event_date: date,
        cross_timestamp_raw: str,
        raw_threshold: float,
        quote_rows: Iterable[dict[str, Any]],
        trade_rows_factory: Callable[[], Iterable[dict[str, Any]]],
        additional_cutoffs: dict[str, int],
    ) -> dict[str, Any]:
        open_ns = market_open_ns(event_date)
        cross_ns = timestamp_ns(cross_timestamp_raw)
        intervals, quote_meta = self.build_intervals(
            quote_rows, open_ns=open_ns, cross_ns=cross_ns, raw_threshold=raw_threshold
        )
        window_starts = {"after_open": open_ns, **additional_cutoffs}
        windows: dict[str, Any] = {}
        trade_meta_by_window: dict[str, dict[str, Any]] = {}
        for window_name, start_ns in window_starts.items():
            confirmations, trade_meta = self.confirm_window(
                intervals,
                trade_rows_factory(),
                window_start_ns=max(open_ns, start_ns),
                cross_ns=cross_ns,
            )
            trade_meta_by_window[window_name] = trade_meta
            windows[window_name] = self.summarize_window(
                intervals,
                confirmations,
                open_ns=open_ns,
                cross_ns=cross_ns,
                raw_threshold=raw_threshold,
                nominal_start_ns=start_ns,
            )
            del confirmations

        primary = windows["after_open"]["levels"][str(int(self.minimum_notional))]
        after_open_trade_meta = trade_meta_by_window["after_open"]
        quality_flags: list[str] = []
        if cross_ns <= open_ns:
            quality_flags.append("threshold_reached_at_or_before_regular_open")
        if quote_meta["regular_quote_count"] == 0:
            quality_flags.append("no_regular_session_quotes_before_threshold")
        if after_open_trade_meta["regular_trade_count"] == 0:
            quality_flags.append("no_regular_session_trades_during_candidate_ask_intervals")
        if not primary["displayed"]:
            quality_flags.append("no_subthreshold_ask_at_primary_notional")
        elif not primary["mechanically_purchasable"]:
            quality_flags.append("subthreshold_ask_not_trade_confirmed")
        elif not primary["manually_actionable"]:
            quality_flags.append("entry_opportunity_below_manual_duration_or_edge_gate")
        return {
            "market_open_timestamp_raw": _iso_from_ns(open_ns),
            "cross_timestamp_raw": cross_timestamp_raw,
            "raw_threshold_price": raw_threshold,
            "seconds_open_to_threshold": (cross_ns - open_ns) / 1_000_000_000,
            "interval_count": len(intervals),
            **quote_meta,
            **after_open_trade_meta,
            "trade_metadata_by_window": trade_meta_by_window,
            "window_metrics": windows,
            "purchase_feasible": bool(primary["mechanically_purchasable"]),
            "primary_actionable": bool(primary["manually_actionable"]),
            "primary_first_entry_timestamp_raw": primary["first_actionable_entry_timestamp_raw"],
            "primary_first_entry_ask": primary["first_actionable_entry_ask"],
            "primary_first_entry_notional_level": self.minimum_notional,
            "primary_first_entry_edge_pct": primary["first_actionable_entry_gross_edge_to_threshold_pct"],
            "primary_opportunity_seconds": primary["first_actionable_segment_seconds"],
            "quality_flags": quality_flags,
        }

    def analyze(
        self,
        *,
        event_date: date,
        cross_timestamp_raw: str,
        raw_threshold: float,
        quote_rows: Iterable[dict[str, Any]],
        trade_rows: Iterable[dict[str, Any]],
        additional_cutoffs: dict[str, int],
    ) -> dict[str, Any]:
        # Unit/small-call convenience API. Production uses a factory that rereads the
        # already-downloaded Parquet trade files once per decision window, keeping peak
        # memory bounded even for events with millions of quotes.
        materialized_trades = list(trade_rows)
        return self.analyze_with_trade_factory(
            event_date=event_date,
            cross_timestamp_raw=cross_timestamp_raw,
            raw_threshold=raw_threshold,
            quote_rows=quote_rows,
            trade_rows_factory=lambda: iter(materialized_trades),
            additional_cutoffs=additional_cutoffs,
        )


def _iter_parquet_rows(paths: list[Path], columns: list[str] | None = None) -> Iterator[dict[str, Any]]:
    for path in sorted(paths):
        parquet = pq.ParquetFile(path)
        available = set(parquet.schema_arrow.names)
        selected = [column for column in (columns or parquet.schema_arrow.names) if column in available]
        for batch in parquet.iter_batches(batch_size=65536, columns=selected):
            for row in batch.to_pylist():
                yield row


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def summarize_second_rows(
    rows: Iterable[dict[str, Any]], *, cutoff_ns: int, prior_close: float | None
) -> dict[str, Any]:
    selected: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        value = row.get("second")
        if value is None:
            continue
        ns = timestamp_ns(value if isinstance(value, datetime) else str(value))
        if ns <= cutoff_ns:
            selected.append((ns, row))
    selected.sort(key=lambda item: item[0])
    if not selected:
        return {"observed": False, "active_seconds": 0}

    trade_rows = [(ns, row) for ns, row in selected if _integer(row.get("trade_count")) > 0]
    quote_rows = [(ns, row) for ns, row in selected if (_number(row.get("bid_price"), 0) or 0) > 0 and (_number(row.get("ask_price"), 0) or 0) > 0]
    closes = [(ns, float(row["trade_close"])) for ns, row in trade_rows if _number(row.get("trade_close"))]
    log_returns: list[float] = []
    for (_, previous), (_, current) in zip(closes, closes[1:]):
        if previous > 0 and current > 0:
            log_returns.append(math.log(current / previous))
    spreads_bps: list[float] = []
    bid_notionals: list[float] = []
    ask_notionals: list[float] = []
    for _, row in quote_rows:
        bid = float(row.get("bid_price") or 0)
        ask = float(row.get("ask_price") or 0)
        midpoint = (bid + ask) / 2 if bid > 0 and ask > 0 else 0
        spread = _number(row.get("min_spread"))
        if spread is not None and midpoint > 0:
            spreads_bps.append(spread / midpoint * 10_000)
        bid_notionals.append(bid * _integer(row.get("bid_size_shares")))
        ask_notionals.append(ask * _integer(row.get("ask_size_shares")))

    first_trade = trade_rows[0][1] if trade_rows else None
    last_trade = trade_rows[-1][1] if trade_rows else None
    high = max((_number(row.get("trade_high"), -math.inf) or -math.inf for _, row in trade_rows), default=None)
    low = min((_number(row.get("trade_low"), math.inf) or math.inf for _, row in trade_rows), default=None)
    if high == -math.inf:
        high = None
    if low == math.inf:
        low = None
    last_price = _number(last_trade.get("trade_close")) if last_trade else None
    first_price = _number(first_trade.get("trade_open")) if first_trade else None

    def trailing(seconds: int) -> dict[str, Any]:
        start = cutoff_ns - seconds * 1_000_000_000
        subset = [(ns, row) for ns, row in selected if ns > start]
        trows = [row for _, row in subset if _integer(row.get("trade_count")) > 0]
        qrows = [row for _, row in subset if (_number(row.get("bid_price"), 0) or 0) > 0 and (_number(row.get("ask_price"), 0) or 0) > 0]
        return {
            "active_seconds": len(subset),
            "trade_count": sum(_integer(row.get("trade_count")) for row in trows),
            "trade_volume": sum(_integer(row.get("trade_volume")) for row in trows),
            "quote_updates": sum(_integer(row.get("quote_updates")) for row in qrows),
            "last_trade_price": _number(trows[-1].get("trade_close")) if trows else None,
            "last_bid": _number(qrows[-1].get("bid_price")) if qrows else None,
            "last_ask": _number(qrows[-1].get("ask_price")) if qrows else None,
        }

    last_quote = quote_rows[-1][1] if quote_rows else None
    return {
        "observed": True,
        "first_observed_timestamp_raw": _iso_from_ns(selected[0][0]),
        "last_observed_timestamp_raw": _iso_from_ns(selected[-1][0]),
        "active_seconds": len(selected),
        "trade_seconds": len(trade_rows),
        "quote_seconds": len(quote_rows),
        "trade_count": sum(_integer(row.get("trade_count")) for _, row in trade_rows),
        "trade_volume": sum(_integer(row.get("trade_volume")) for _, row in trade_rows),
        "quote_updates": sum(_integer(row.get("quote_updates")) for _, row in quote_rows),
        "first_trade_price": first_price,
        "last_trade_price": last_price,
        "high": high,
        "low": low,
        "return_from_first_trade_pct": ((last_price / first_price) - 1) * 100 if last_price and first_price else None,
        "return_from_prior_close_pct": ((last_price / prior_close) - 1) * 100 if last_price and prior_close else None,
        "range_pct_of_prior_close": ((high - low) / prior_close) * 100 if high is not None and low is not None and prior_close else None,
        "realized_volatility_second_log": math.sqrt(sum(value * value for value in log_returns)) if log_returns else 0.0,
        "max_abs_second_log_return": max((abs(value) for value in log_returns), default=0.0),
        "median_spread_bps": statistics.median(spreads_bps) if spreads_bps else None,
        "p90_spread_bps": _percentile(spreads_bps, 0.9),
        "median_bid_notional": statistics.median(bid_notionals) if bid_notionals else None,
        "median_ask_notional": statistics.median(ask_notionals) if ask_notionals else None,
        "last_bid": _number(last_quote.get("bid_price")) if last_quote else None,
        "last_ask": _number(last_quote.get("ask_price")) if last_quote else None,
        "last_bid_notional": (_number(last_quote.get("bid_price"), 0) or 0) * _integer(last_quote.get("bid_size_shares")) if last_quote else None,
        "last_ask_notional": (_number(last_quote.get("ask_price"), 0) or 0) * _integer(last_quote.get("ask_size_shares")) if last_quote else None,
        "trailing_60s": trailing(60),
        "trailing_300s": trailing(300),
    }


SAFE_BASE_FEATURE_KEYS = {
    "atr_pct_10", "corporate_action_45d", "exchange", "feature_cutoff_date",
    "listing_sessions_observed", "log_listing_sessions",
    "log_median_dollar_volume_10", "log_prior_close",
    "median_dollar_volume_10", "median_volume_10", "momentum_10",
    "price_band", "prior_close", "prior_day_return", "realized_vol_10",
}


def safe_base_features(value: Any) -> dict[str, Any]:
    source = _json_obj(value)
    return {key: source.get(key) for key in sorted(SAFE_BASE_FEATURE_KEYS) if key in source}


def flatten_snapshot(base: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for key, value in base.items():
        if isinstance(value, (dict, list)):
            row[f"base_{key}_json"] = _json_text(value)
        else:
            row[f"base_{key}"] = value
    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            row[f"snapshot_{key}_json"] = _json_text(value)
        else:
            row[f"snapshot_{key}"] = value
    return row


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                columns.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_text(value) if isinstance(value, (dict, list)) else value for key, value in row.items()})


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        pq.write_table(pa.table({"empty": pa.array([], type=pa.string())}), path, compression="zstd")
        return
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({key: _json_text(value) if isinstance(value, (dict, list)) else value for key, value in row.items()})
    # PyArrow can infer mixed numeric/null columns reliably after nested values are serialized.
    pq.write_table(pa.Table.from_pylist(normalized), path, compression="zstd")


def _zip_directory(root: Path, archive: Path) -> None:
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                zf.write(path, path.relative_to(root).as_posix())


class EntryExporterRunner:
    def __init__(self, settings: Settings, store: SupabaseStore):
        self.settings = settings
        self.store = store

    def _update(self, job_id: str, stage: str, current: int, total: int, **extra: Any) -> None:
        self.store.update_entry_job(
            job_id,
            progress_stage=stage,
            progress_current=current,
            progress_total=total,
            **extra,
        )

    def _download_paths(
        self, file_rows: list[dict[str, Any]], local_root: Path
    ) -> list[Path]:
        local_root.mkdir(parents=True, exist_ok=True)
        local_paths: list[Path] = []
        for index, file_row in enumerate(sorted(file_rows, key=lambda row: row["storage_path"])):
            local = local_root / f"{index:05d}_{Path(file_row['filename']).name}"
            self.store.download_file(file_row["storage_path"], local)
            local_paths.append(local)
        return local_paths

    def _download_rows(
        self,
        file_rows: list[dict[str, Any]],
        local_root: Path,
        *,
        columns: list[str],
    ) -> Iterator[dict[str, Any]]:
        return _iter_parquet_rows(self._download_paths(file_rows, local_root), columns=columns)

    @staticmethod
    def _event_file_rows(files_by_event: dict[str, list[dict[str, Any]]], event: dict[str, Any], kind: str) -> list[dict[str, Any]]:
        marker = f"{event['event_date']}_to_cross"
        return [
            row for row in files_by_event.get(event["id"], [])
            if row.get("file_kind") == kind and marker in str(row.get("storage_path") or "")
        ]

    def _assess_event(
        self,
        job_id: str,
        event: dict[str, Any],
        scan_result: dict[str, Any] | None,
        files_by_event: dict[str, list[dict[str, Any]]],
        params: dict[str, Any],
    ) -> dict[str, Any]:
        event_date = date.fromisoformat(str(event["event_date"]))
        raw_threshold = float(event.get("raw_threshold_price") or 0)
        exact_cross_raw = str(event.get("exact_cross_timestamp_raw") or event.get("exact_cross_timestamp") or "")
        if raw_threshold <= 0 or not exact_cross_raw:
            raise RuntimeError("Research event lacks raw threshold or exact crossing timestamp")
        quote_files = self._event_file_rows(files_by_event, event, "raw_quotes")
        trade_files = self._event_file_rows(files_by_event, event, "raw_trades")
        if not quote_files:
            raise RuntimeError("No event-day pre-cross raw quote files found")
        if params.get("require_subsequent_trade", True) and not trade_files:
            raise RuntimeError("No event-day pre-cross raw trade files found")

        temp = Path(self.settings.temp_root) / job_id / "entry" / event["id"]
        shutil.rmtree(temp, ignore_errors=True)
        temp.mkdir(parents=True, exist_ok=True)
        try:
            quote_paths = self._download_paths(quote_files, temp / "quotes")
            trade_paths = self._download_paths(trade_files, temp / "trades") if trade_files else []
            analyzer = EntryFeasibilityAnalyzer(
                minimum_notional=float(params.get("minimum_entry_notional", 500)),
                reaction_delay_seconds=float(params.get("reaction_delay_seconds", 5)),
                minimum_opportunity_seconds=float(params.get("minimum_opportunity_seconds", 30)),
                minimum_gross_edge_pct=float(params.get("minimum_gross_edge_pct", 0)),
                require_subsequent_trade=bool(params.get("require_subsequent_trade", True)),
            )
            analysis = analyzer.analyze_with_trade_factory(
                event_date=event_date,
                cross_timestamp_raw=exact_cross_raw,
                raw_threshold=raw_threshold,
                quote_rows=_iter_parquet_rows(
                    quote_paths, columns=["timestamp", "bid_price", "bid_size_shares", "ask_price", "ask_size_shares"]
                ),
                trade_rows_factory=(
                    lambda: _iter_parquet_rows(trade_paths, columns=["timestamp", "price", "size_shares"])
                    if trade_paths else iter(())
                ),
                additional_cutoffs=snapshot_cutoffs(event_date),
            )
            open_ns = timestamp_ns(analysis["market_open_timestamp_raw"])
            cross_ns = timestamp_ns(exact_cross_raw)
            quote_meta = {
                "regular_quote_count": analysis["regular_quote_count"],
                "invalid_quote_count": analysis["invalid_quote_count"],
            }
            trade_meta = {
                "regular_trade_count": analysis["regular_trade_count"],
                "invalid_trade_count": analysis["invalid_trade_count"],
            }
            intervals_count = int(analysis["interval_count"])
            windows = analysis["window_metrics"]
            primary = windows["after_open"]["levels"][str(int(float(params.get("minimum_entry_notional", 500))))]
            flags = list(_json_list(event.get("quality_flags")))
            opened_at_or_above_threshold = False
            if scan_result and _number(scan_result.get("session_open")) is not None and _number(scan_result.get("threshold_price")) is not None:
                opened_at_or_above_threshold = float(scan_result["session_open"]) >= float(scan_result["threshold_price"])
                if opened_at_or_above_threshold:
                    flags.append("opened_at_or_above_adjusted_threshold")
            if cross_ns <= open_ns:
                flags.append("threshold_reached_at_or_before_regular_open")
            if quote_meta["regular_quote_count"] == 0:
                flags.append("no_regular_session_quotes_before_threshold")
            if not primary["displayed"]:
                flags.append("no_subthreshold_ask_at_primary_notional")
            elif not primary["mechanically_purchasable"]:
                flags.append("subthreshold_ask_not_trade_confirmed")
            elif not primary["manually_actionable"]:
                flags.append("entry_opportunity_below_manual_duration_or_edge_gate")
            purchase_feasible = bool(primary["mechanically_purchasable"]) and not opened_at_or_above_threshold and cross_ns > open_ns
            primary_actionable = bool(primary["manually_actionable"]) and not opened_at_or_above_threshold and cross_ns > open_ns
            exclusion_reason = None
            if not primary_actionable:
                if opened_at_or_above_threshold:
                    exclusion_reason = "opened_at_or_above_threshold"
                elif cross_ns <= open_ns:
                    exclusion_reason = "threshold_at_or_before_open"
                elif not primary["displayed"]:
                    exclusion_reason = "no_displayed_subthreshold_ask_at_primary_notional"
                elif not primary["mechanically_purchasable"]:
                    exclusion_reason = "no_confirmed_purchase_at_primary_notional"
                else:
                    exclusion_reason = "manual_duration_or_edge_gate_not_met"
            return {
                "entry_job_id": job_id,
                "research_event_id": event["id"],
                "source_result_id": event["source_result_id"],
                "symbol": event["symbol"],
                "event_date": event_date.isoformat(),
                "split_name": split_name(event_date),
                "status": "completed",
                "market_open_timestamp": parse_ts(_iso_from_ns(open_ns)).isoformat(),
                "market_open_timestamp_raw": _iso_from_ns(open_ns),
                "exact_cross_timestamp": event.get("exact_cross_timestamp"),
                "exact_cross_timestamp_raw": exact_cross_raw,
                "raw_threshold_price": raw_threshold,
                "adjusted_threshold_price": _number(event.get("adjusted_threshold_price")),
                "session_open": _number(scan_result.get("session_open")) if scan_result else None,
                "opening_gap_pct": _number(scan_result.get("opening_gap_pct")) if scan_result else None,
                "seconds_open_to_threshold": (cross_ns - open_ns) / 1_000_000_000,
                "minimum_entry_notional": float(params.get("minimum_entry_notional", 500)),
                "reaction_delay_seconds": float(params.get("reaction_delay_seconds", 5)),
                "minimum_opportunity_seconds": float(params.get("minimum_opportunity_seconds", 30)),
                "minimum_gross_edge_pct": float(params.get("minimum_gross_edge_pct", 0)),
                "require_subsequent_trade": bool(params.get("require_subsequent_trade", True)),
                "purchase_feasible": purchase_feasible,
                "primary_actionable": primary_actionable,
                "first_entry_timestamp_raw": primary["first_actionable_entry_timestamp_raw"],
                "first_entry_ask": primary["first_actionable_entry_ask"],
                "first_entry_notional": float(params.get("minimum_entry_notional", 500)) if primary["first_actionable_entry_ask"] else None,
                "first_entry_gross_edge_pct": primary["first_actionable_entry_gross_edge_to_threshold_pct"],
                "opportunity_seconds": primary["first_actionable_segment_seconds"],
                "seconds_entry_to_threshold": primary["seconds_from_first_confirmed_entry_to_threshold"],
                "sellability_confirmed": bool(event.get("eligible")) and event.get("sellability_status") == "confirmed_sellable",
                "first_confirmed_exit_timestamp_raw": event.get("first_confirmed_exit_timestamp_raw"),
                "first_confirmed_exit_bid": _number(event.get("first_confirmed_exit_bid")),
                "first_confirmed_exit_notional": _number(event.get("first_confirmed_exit_notional")),
                "entry_window_metrics": windows,
                "row_counts": {
                    "candidate_quote_intervals": intervals_count,
                    "regular_quote_count": quote_meta["regular_quote_count"],
                    "regular_trade_count_during_candidate_intervals": trade_meta["regular_trade_count"],
                },
                "quality_flags": sorted(set(str(flag) for flag in flags)),
                "exclusion_reason": exclusion_reason,
                "completed_at": datetime.now(UTC).isoformat(),
                "error_message": None,
            }
        finally:
            shutil.rmtree(temp, ignore_errors=True)

    def _register_file(self, job_id: str, path: Path, storage_path: str, file_kind: str) -> dict[str, Any]:
        if path.stat().st_size > MAX_FREE_SAFE_OBJECT_BYTES:
            raise RuntimeError(f"Export exceeded 45 MB safety limit: {path.name} ({path.stat().st_size} bytes)")
        self.store.upload_file(path, storage_path)
        rows = self.store.upsert(
            "stock25_entry_files",
            {
                "entry_job_id": job_id,
                "file_kind": file_kind,
                "storage_path": storage_path,
                "filename": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            },
            on_conflict="entry_job_id,storage_path",
            return_representation=True,
        )
        return rows[0] if rows else self.store.select(
            "stock25_entry_files", filters={"entry_job_id": f"eq.{job_id}", "storage_path": f"eq.{storage_path}"}, limit=1
        )[0]

    def _build_exports(
        self,
        job: dict[str, Any],
        source_control_job: dict[str, Any],
        assessments: list[dict[str, Any]],
        pairs: list[dict[str, Any]],
        control_observations: list[dict[str, Any]],
        control_datasets: list[dict[str, Any]],
        control_files: list[dict[str, Any]],
        research_files: list[dict[str, Any]],
        params: dict[str, Any],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        job_id = job["id"]
        root = Path(self.settings.temp_root) / job_id / "exports"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)

        assessment_by_event = {row["research_event_id"]: row for row in assessments if row.get("status") == "completed"}
        observation_by_id = {row["id"]: row for row in control_observations}
        dataset_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in control_datasets:
            dataset_by_key[(str(row["symbol"]), str(row["session_date"]), str(row["window_type"]))].append(row)
        control_files_by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in control_files:
            if row.get("control_dataset_id"):
                control_files_by_dataset[row["control_dataset_id"]].append(row)
        positive_second_by_event: dict[str, dict[str, Any]] = {}
        for row in research_files:
            if row.get("file_kind") == "second_summary" and str(row.get("storage_path") or "").endswith("_to_cross.parquet"):
                positive_second_by_event[str(row.get("research_event_id"))] = row

        pair_rows: list[dict[str, Any]] = []
        for pair in pairs:
            assessment = assessment_by_event.get(pair["positive_research_event_id"])
            if not assessment or not assessment.get("primary_actionable"):
                continue
            observation = observation_by_id.get(pair.get("control_observation_id"))
            if not observation or observation.get("status") != "completed":
                continue
            control_features = _json_obj(pair.get("control_features"))
            row_counts = _json_obj(observation.get("row_counts"))
            pair_flags = set(str(x) for x in _json_list(pair.get("quality_flags")))
            observation_flags = set(str(x) for x in _json_list(observation.get("quality_flags")))
            usable_ticks = int(row_counts.get("trades") or 0) > 0 and int(row_counts.get("quotes") or 0) > 0
            current_tradable = bool(control_features.get("currently_tradable", True))
            core_usable = usable_ticks and current_tradable
            unverified_ca = any("corporate_actions_point_in_time_publication_unverified" in flag for flag in pair_flags | observation_flags | set(_json_list(assessment.get("quality_flags"))))
            primary_clean = core_usable and pair.get("positive_tier") == "primary_clean" and not unverified_ca
            pair_rows.append({
                **pair,
                "split_name": assessment["split_name"],
                "positive_primary_actionable": True,
                "control_usable_ticks": usable_ticks,
                "control_currently_tradable": current_tradable,
                "cohort_all_matched": True,
                "cohort_core_usable": core_usable,
                "cohort_primary_clean": primary_clean,
            })

        retained_pairs_by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pair in pair_rows:
            retained_pairs_by_event[pair["positive_research_event_id"]].append(pair)

        # Download one-second summaries and create cutoff-aligned features only for retained pairs.
        snapshot_observations: list[dict[str, Any]] = []
        snapshot_pairs: list[dict[str, Any]] = []
        positive_snapshot_ids: dict[tuple[str, str], str] = {}
        temp = root / "second_temp"
        temp.mkdir(parents=True, exist_ok=True)

        def load_second(file_row: dict[str, Any], cache_key: str) -> list[dict[str, Any]]:
            local = temp / f"{_safe_name(cache_key)}.parquet"
            self.store.download_file(file_row["storage_path"], local)
            rows = pq.read_table(local).to_pylist()
            local.unlink(missing_ok=True)
            return rows

        for event_id, event_pairs in retained_pairs_by_event.items():
            assessment = assessment_by_event[event_id]
            event_date = date.fromisoformat(assessment["event_date"])
            cross_ns = timestamp_ns(assessment["exact_cross_timestamp_raw"])
            cutoffs = snapshot_cutoffs(event_date)
            positive_file = positive_second_by_event.get(event_id)
            if not positive_file:
                assessment.setdefault("quality_flags", []).append("positive_second_summary_missing")
                continue
            positive_rows = load_second(positive_file, f"positive_{event_id}")
            positive_base = safe_base_features(event_pairs[0].get("positive_features"))
            valid_cutoffs: list[tuple[str, int, str]] = []
            for cutoff_name, cutoff_ns in cutoffs.items():
                level = _json_obj(_json_obj(_json_obj(assessment.get("entry_window_metrics")).get(cutoff_name)).get("levels")).get(str(int(float(params.get("minimum_entry_notional", 500))))) or {}
                if cutoff_ns >= cross_ns or not level.get("manually_actionable"):
                    continue
                positive_snapshot_id = f"positive:{event_id}:{cutoff_name}"
                positive_snapshot_ids[(event_id, cutoff_name)] = positive_snapshot_id
                positive_summary = summarize_second_rows(
                    positive_rows, cutoff_ns=cutoff_ns, prior_close=_number(positive_base.get("prior_close"))
                )
                snapshot_observations.append({
                    "snapshot_observation_id": positive_snapshot_id,
                    "label": 1,
                    "research_event_id": event_id,
                    "control_observation_id": None,
                    "symbol": assessment["symbol"],
                    "event_date": assessment["event_date"],
                    "split_name": assessment["split_name"],
                    "cutoff_name": cutoff_name,
                    "cutoff_timestamp_raw": _iso_from_ns(cutoff_ns),
                    "outcome_threshold_timestamp_raw": assessment["exact_cross_timestamp_raw"],
                    "outcome_seconds_cutoff_to_threshold": (cross_ns - cutoff_ns) / 1_000_000_000,
                    "cohort_all_matched": True,
                    "cohort_core_usable": any(p["cohort_core_usable"] for p in event_pairs),
                    "cohort_primary_clean": any(p["cohort_primary_clean"] for p in event_pairs),
                    **flatten_snapshot(positive_base, positive_summary),
                })
                valid_cutoffs.append((cutoff_name, cutoff_ns, positive_snapshot_id))

            # Each control second-summary file is downloaded once and reused across all
            # applicable fixed cutoffs, avoiding two or three identical Storage reads.
            for pair in event_pairs:
                observation = observation_by_id[pair["control_observation_id"]]
                candidates = [
                    row for row in dataset_by_key.get((observation["symbol"], observation["event_date"], "prefix"), [])
                    if str(row.get("window_end_raw")) == str(observation.get("pseudo_event_timestamp_raw"))
                ]
                if not candidates or not valid_cutoffs:
                    continue
                dataset = sorted(candidates, key=lambda row: row.get("created_at") or "")[0]
                second_files = [row for row in control_files_by_dataset.get(dataset["id"], []) if row.get("file_kind") == "second_summary"]
                if not second_files:
                    continue
                control_rows = load_second(second_files[0], f"control_{dataset['id']}")
                control_base = safe_base_features(pair.get("control_features"))
                for cutoff_name, cutoff_ns, positive_snapshot_id in valid_cutoffs:
                    control_summary = summarize_second_rows(
                        control_rows, cutoff_ns=cutoff_ns, prior_close=_number(control_base.get("prior_close"))
                    )
                    control_snapshot_id = f"control:{observation['id']}:{cutoff_name}"
                    snapshot_observations.append({
                        "snapshot_observation_id": control_snapshot_id,
                        "label": 0,
                        "research_event_id": event_id,
                        "control_observation_id": observation["id"],
                        "symbol": observation["symbol"],
                        "event_date": observation["event_date"],
                        "split_name": assessment["split_name"],
                        "cutoff_name": cutoff_name,
                        "cutoff_timestamp_raw": _iso_from_ns(cutoff_ns),
                        "outcome_threshold_timestamp_raw": assessment["exact_cross_timestamp_raw"],
                        "outcome_seconds_cutoff_to_threshold": (cross_ns - cutoff_ns) / 1_000_000_000,
                        "cohort_all_matched": pair["cohort_all_matched"],
                        "cohort_core_usable": pair["cohort_core_usable"],
                        "cohort_primary_clean": pair["cohort_primary_clean"],
                        **flatten_snapshot(control_base, control_summary),
                    })
                    snapshot_pairs.append({
                        "pair_id": pair["id"],
                        "positive_research_event_id": event_id,
                        "positive_snapshot_observation_id": positive_snapshot_id,
                        "control_snapshot_observation_id": control_snapshot_id,
                        "event_date": assessment["event_date"],
                        "split_name": assessment["split_name"],
                        "cutoff_name": cutoff_name,
                        "match_quality": pair.get("match_quality"),
                        "match_score": pair.get("match_score"),
                        "control_rank": pair.get("control_rank"),
                        "cohort_all_matched": pair["cohort_all_matched"],
                        "cohort_core_usable": pair["cohort_core_usable"],
                        "cohort_primary_clean": pair["cohort_primary_clean"],
                    })

        shutil.rmtree(temp, ignore_errors=True)
        # Deduplicate observations because the same control cannot appear twice for one cutoff,
        # while a positive is intentionally represented once regardless of control count.
        deduped: dict[str, dict[str, Any]] = {}
        for row in snapshot_observations:
            deduped[row["snapshot_observation_id"]] = row
        snapshot_observations = list(deduped.values())

        unmatched_actionable = [
            row for row in assessments
            if row.get("primary_actionable") and row["research_event_id"] not in retained_pairs_by_event
        ]
        retention_rows = []
        for assessment in assessments:
            event_pairs = retained_pairs_by_event.get(assessment["research_event_id"], [])
            retention_rows.append({
                "research_event_id": assessment["research_event_id"],
                "symbol": assessment["symbol"],
                "event_date": assessment["event_date"],
                "split_name": assessment["split_name"],
                "primary_actionable": assessment.get("primary_actionable"),
                "exclusion_reason": assessment.get("exclusion_reason"),
                "retained_control_count": len(event_pairs),
                "core_usable_control_count": sum(bool(row["cohort_core_usable"]) for row in event_pairs),
                "primary_clean_control_count": sum(bool(row["cohort_primary_clean"]) for row in event_pairs),
            })

        # Main index contains cohort-definition facts, not predictor feature values.
        index_dir = root / "index"
        index_dir.mkdir()
        assessment_export_rows = []
        for row in assessments:
            assessment_export_rows.append({
                **{key: value for key, value in row.items() if key not in {"entry_window_metrics"}},
                "entry_window_metrics_json": _json_text(_json_obj(row.get("entry_window_metrics"))),
            })
        _write_csv(index_dir / "entry_feasibility_assessments.csv", assessment_export_rows)
        _write_parquet(index_dir / "entry_feasibility_assessments.parquet", assessment_export_rows)
        _write_csv(index_dir / "pair_retention.csv", retention_rows)
        _write_csv(index_dir / "unmatched_actionable_positives.csv", unmatched_actionable)

        symbol_split_map: dict[str, Counter] = defaultdict(Counter)
        for row in assessments:
            symbol_split_map[str(row["symbol"])][str(row["split_name"])] += 1
        cross_split_rows = []
        for symbol, split_counts in sorted(symbol_split_map.items()):
            if len(split_counts) <= 1:
                continue
            cross_split_rows.append({
                "symbol": symbol,
                "split_count": len(split_counts),
                "discovery_events": split_counts.get("discovery", 0),
                "validation_events": split_counts.get("validation", 0),
                "sealed_test_events": split_counts.get("sealed_test", 0),
                "warning": "same_symbol_appears_in_multiple_chronological_splits",
            })
        _write_csv(index_dir / "cross_split_symbol_diagnostics.csv", cross_split_rows)

        counts_by_split = defaultdict(Counter)
        for row in assessments:
            counts_by_split[row["split_name"]]["assessed"] += 1
            if row.get("primary_actionable"):
                counts_by_split[row["split_name"]]["primary_actionable"] += 1
            if row.get("purchase_feasible"):
                counts_by_split[row["split_name"]]["mechanically_purchasable"] += 1
        manifest = {
            "version": "3.0.3",
            "entry_job_id": job_id,
            "source_control_job_id": source_control_job["id"],
            "source_research_job_id": source_control_job["source_research_job_id"],
            "parameters": params,
            "fixed_splits": {
                "discovery": "2026-04-20 through 2026-06-09 inclusive",
                "validation": "2026-06-10 through 2026-06-24 inclusive",
                "sealed_test": "2026-06-25 through 2026-07-16 inclusive",
            },
            "snapshot_cutoffs": {name: note for name, _, note in SNAPSHOT_SPECS},
            "assessment_count": len(assessments),
            "primary_actionable_count": sum(bool(row.get("primary_actionable")) for row in assessments),
            "retained_pair_count": len(pair_rows),
            "snapshot_observation_count": len(snapshot_observations),
            "snapshot_pair_count": len(snapshot_pairs),
            "cross_split_symbol_count": len(cross_split_rows),
            "counts_by_split": {key: dict(value) for key, value in counts_by_split.items()},
            "sealed_test_rule": "Do not inspect sealed-test predictor features until fixed rules survive validation. Cohort-definition and row-count metadata may be inspected.",
        }
        (index_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        (index_dir / "README.txt").write_text(
            "V3.0.3 entry-feasibility index. The primary actionable flag requires a confirmed displayed ask below the +25% threshold at the configured notional, after the regular-session decision/reaction boundary, with the configured continuous opportunity duration and gross edge. Predictor features are separated into discovery, validation and sealed-test archives. Do not inspect sealed-test features before fixed rules survive validation.\n",
            encoding="utf-8",
        )
        index_archive = root / "entry_feasibility_index.zip"
        _zip_directory(index_dir, index_archive)

        output_files: list[dict[str, Any]] = []
        output_files.append(self._register_file(job_id, index_archive, f"entry_jobs/{job_id}/{index_archive.name}", "entry_feasibility_index"))

        for split in ("discovery", "validation", "sealed_test"):
            split_dir = root / split
            split_dir.mkdir()
            split_obs = [row for row in snapshot_observations if row["split_name"] == split]
            split_pairs = [row for row in snapshot_pairs if row["split_name"] == split]
            split_assessments = [row for row in assessment_export_rows if row["split_name"] == split]
            _write_csv(split_dir / "snapshot_observations.csv", split_obs)
            _write_parquet(split_dir / "snapshot_observations.parquet", split_obs)
            _write_csv(split_dir / "snapshot_pairs.csv", split_pairs)
            _write_parquet(split_dir / "snapshot_pairs.parquet", split_pairs)
            _write_csv(split_dir / "entry_assessments.csv", split_assessments)
            split_manifest = {
                "version": "3.0.3",
                "split": split,
                "observation_count": len(split_obs),
                "pair_count": len(split_pairs),
                "positive_observation_count": sum(row["label"] == 1 for row in split_obs),
                "control_observation_count": sum(row["label"] == 0 for row in split_obs),
                "cutoff_counts": dict(Counter(row["cutoff_name"] for row in split_obs)),
                "cohort_primary_clean_pair_count": sum(bool(row["cohort_primary_clean"]) for row in split_pairs),
                "analysis_rule": "Use only information in snapshot feature columns available at the named cutoff. Match or weight controls through snapshot_pairs. A positive row is unique per event/cutoff and must not be naively multiplied by its number of controls.",
            }
            (split_dir / "manifest.json").write_text(json.dumps(split_manifest, indent=2), encoding="utf-8")
            (split_dir / "DATA_DICTIONARY.md").write_text(
                "# V3.0.3 snapshot data\n\n"
                "- `snapshot_observations`: one unique positive or control observation at a fixed decision cutoff.\n"
                "- `snapshot_pairs`: matched positive-control relationships and cohort flags.\n"
                "- `base_*`: ten-session matching/context measurements known before the event day.\n"
                "- `snapshot_*`: event-day trades, quotes and microstructure observed no later than the cutoff.\n"
                "- `outcome_*` and `label` are targets/metadata and must never be used as predictors.\n"
                "- `cohort_primary_clean`: strictest recommended primary population.\n"
                "- `cohort_core_usable`: broader usable sensitivity population.\n"
                "- `cohort_all_matched`: all retained strict controls.\n",
                encoding="utf-8",
            )
            archive_name = "SEALED_TEST_DO_NOT_OPEN.zip" if split == "sealed_test" else f"actionable_{split}.zip"
            archive = root / archive_name
            _zip_directory(split_dir, archive)
            output_files.append(self._register_file(job_id, archive, f"entry_jobs/{job_id}/{archive.name}", f"actionable_{split}"))

        return manifest, output_files

    def run(self, job: dict[str, Any]) -> None:
        job_id = job["id"]
        params = _json_obj(job.get("parameters"))
        try:
            source_rows = self.store.select("stock25_control_jobs", filters={"id": f"eq.{job['source_control_job_id']}"}, limit=1)
            if not source_rows or source_rows[0].get("status") != "completed":
                raise RuntimeError("Source matched-control job is missing or not completed")
            source_control_job = source_rows[0]
            research_job_id = source_control_job["source_research_job_id"]
            events = self.store.select_all(
                "stock25_research_events",
                filters={"research_job_id": f"eq.{research_job_id}", "eligible": "eq.true", "status": "eq.completed"},
                order="event_date.asc,symbol.asc",
            )
            max_events = int(params.get("max_positive_events") or 0)
            if max_events:
                events = events[:max_events]
            event_ids = {row["id"] for row in events}
            source_result_ids = {row["source_result_id"] for row in events}
            scan_results_all = self.store.select_all(
                "stock25_scan_results", filters={"scan_id": f"eq.{self.store.select('stock25_research_jobs', filters={'id': f'eq.{research_job_id}'}, limit=1)[0]['source_scan_id']}"},
            )
            scan_by_id = {row["id"]: row for row in scan_results_all if row["id"] in source_result_ids}
            research_files = self.store.select_all(
                "stock25_research_files", filters={"research_job_id": f"eq.{research_job_id}"}, order="research_event_id.asc,storage_path.asc",
            )
            files_by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in research_files:
                if row.get("research_event_id") in event_ids:
                    files_by_event[row["research_event_id"]].append(row)

            existing = self.store.select_all("stock25_entry_assessments", filters={"entry_job_id": f"eq.{job_id}"})
            completed_by_event = {row["research_event_id"]: row for row in existing if row.get("status") == "completed"}
            self._update(job_id, "assessing_purchase_feasibility", len(completed_by_event), len(events), positive_event_count=len(events))
            for index, event in enumerate(events, start=1):
                if event["id"] in completed_by_event:
                    self._update(job_id, "assessing_purchase_feasibility", index, len(events))
                    continue
                try:
                    assessment = self._assess_event(
                        job_id, event, scan_by_id.get(event["source_result_id"]), files_by_event, params,
                    )
                    self.store.upsert("stock25_entry_assessments", assessment, on_conflict="entry_job_id,research_event_id")
                except Exception as exc:
                    logger.exception("Entry assessment failed for %s %s", event.get("symbol"), event.get("event_date"))
                    failure = {
                        "entry_job_id": job_id,
                        "research_event_id": event["id"],
                        "source_result_id": event["source_result_id"],
                        "symbol": event["symbol"],
                        "event_date": event["event_date"],
                        "split_name": split_name(date.fromisoformat(str(event["event_date"]))),
                        "status": "failed",
                        "purchase_feasible": False,
                        "primary_actionable": False,
                        "quality_flags": ["entry_assessment_failed"],
                        "exclusion_reason": "entry_assessment_failed",
                        "error_message": f"{type(exc).__name__}: {exc}"[:4000],
                    }
                    self.store.upsert("stock25_entry_assessments", failure, on_conflict="entry_job_id,research_event_id")
                self._update(job_id, "assessing_purchase_feasibility", index, len(events))

            assessments = self.store.select_all(
                "stock25_entry_assessments", filters={"entry_job_id": f"eq.{job_id}"}, order="event_date.asc,symbol.asc",
            )
            failed = [row for row in assessments if row.get("status") == "failed"]
            failure_rate = len(failed) / max(1, len(events))
            if failed and (bool(params.get("fail_on_assessment_error", False)) or failure_rate > 0.05):
                raise RuntimeError(f"{len(failed)} entry assessments failed ({failure_rate:.1%}); retry the same job after resolving the first error: {failed[0].get('error_message')}")

            self._update(job_id, "building_fixed_time_exports", 0, 1)
            pairs = self.store.select_all(
                "stock25_control_pairs", filters={"control_job_id": f"eq.{source_control_job['id']}", "status": "eq.completed"}, order="event_date.asc,positive_symbol.asc,control_rank.asc",
            )
            pairs = [row for row in pairs if row["positive_research_event_id"] in event_ids]
            observations = self.store.select_all(
                "stock25_control_observations", filters={"control_job_id": f"eq.{source_control_job['id']}"}, order="event_date.asc,symbol.asc",
            )
            datasets = self.store.select_all(
                "stock25_control_datasets", filters={"control_job_id": f"eq.{source_control_job['id']}", "status": "eq.completed"}, order="symbol.asc,session_date.asc",
            )
            control_files = self.store.select_all(
                "stock25_control_files", filters={"control_job_id": f"eq.{source_control_job['id']}"}, order="storage_path.asc",
            )
            manifest, output_files = self._build_exports(
                job, source_control_job, assessments, pairs, observations, datasets, control_files, research_files, params,
            )
            primary_actionable_count = sum(bool(row.get("primary_actionable")) for row in assessments)
            assessment_map = {row["research_event_id"]: row for row in assessments}
            matched_actionable_ids = {
                row["positive_research_event_id"] for row in pairs
                if assessment_map.get(row["positive_research_event_id"], {}).get("primary_actionable")
            }
            index_file = next(row for row in output_files if row["file_kind"] == "entry_feasibility_index")
            self.store.update_entry_job(
                job_id,
                status="completed",
                completed_at=datetime.now(UTC).isoformat(),
                progress_stage="completed",
                progress_current=1,
                progress_total=1,
                entry_feasible_count=sum(bool(row.get("purchase_feasible")) for row in assessments),
                primary_actionable_count=primary_actionable_count,
                excluded_positive_count=len(assessments) - primary_actionable_count,
                failed_assessment_count=len(failed),
                matched_actionable_positive_count=len(matched_actionable_ids),
                retained_control_pair_count=manifest["retained_pair_count"],
                export_storage_path=index_file["storage_path"],
                error_message=None,
            )
        except Exception as exc:
            logger.exception("Entry exporter job %s failed", job_id)
            self.store.update_entry_job(
                job_id,
                status="failed",
                completed_at=datetime.now(UTC).isoformat(),
                progress_stage="failed",
                error_message=f"{type(exc).__name__}: {exc}"[:4000],
            )
            raise
        finally:
            shutil.rmtree(Path(self.settings.temp_root) / job_id, ignore_errors=True)
