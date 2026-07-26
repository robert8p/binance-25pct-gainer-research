from __future__ import annotations

import json
import math
import shutil
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .analysis_contract import write_analysis_contract
from .binance import BinanceClient, sha256_file
from .matched_controls import (
    MinuteArchiveCache,
    SPLITS,
    _json_ready,
    _segment,
    assign_temporal_splits,
    floor_minute,
    parse_datetime,
    safe_pct,
)
from .supabase import SupabaseClient
from .package_utils import build_grouped_zip_parts

CONTEXT_WINDOWS = (15, 30, 60, 120, 180, 360, 480, 720, 1440, 2880, 4320, 7200, 10080, 14400)
REFERENCE_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
REFERENCE_WINDOWS = (60, 180, 480, 1440, 4320, 10080, 14400)


def _zip_directory(source: Path, destination: Path) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(source.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(source))


def _finite(values: pd.Series) -> np.ndarray:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    return arr[np.isfinite(arr)]


def _trend(values: pd.Series, minutes_per_step: float = 1.0) -> tuple[float | None, float | None]:
    arr = _finite(values)
    if arr.size < 6 or np.any(arr <= 0):
        return None, None
    # Bound computation on ten-day minute windows while preserving the shape.
    stride = max(1, int(math.ceil(arr.size / 600)))
    arr = arr[::stride]
    x = np.arange(arr.size, dtype=float) * minutes_per_step * stride
    y = np.log(arr)
    x_centered = x - x.mean()
    denom = float(np.dot(x_centered, x_centered))
    if denom <= 0:
        return None, None
    slope_per_minute = float(np.dot(x_centered, y - y.mean()) / denom)
    fitted = y.mean() + slope_per_minute * x_centered
    ss_res = float(np.sum((y - fitted) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return (math.expm1(slope_per_minute * 1440.0) * 100.0), r2


def _max_drawdown_runup(values: pd.Series) -> tuple[float | None, float | None]:
    arr = _finite(values)
    if arr.size < 2 or np.any(arr <= 0):
        return None, None
    running_max = np.maximum.accumulate(arr)
    running_min = np.minimum.accumulate(arr)
    return float(np.min(arr / running_max - 1.0) * 100.0), float(np.max(arr / running_min - 1.0) * 100.0)


def _realized_vol(values: pd.Series, scale_minutes: int) -> float | None:
    arr = _finite(values)
    if arr.size < max(5, int(scale_minutes * 0.8)) or np.any(arr <= 0):
        return None
    rets = np.diff(np.log(arr))
    if rets.size < 2:
        return None
    return float(np.std(rets, ddof=1) * math.sqrt(scale_minutes) * 100.0)


def _window_metrics(frame: pd.DataFrame, end_open: pd.Timestamp, window: int, entry_price: float) -> dict[str, Any]:
    segment = _segment(frame, end_open, window)
    observed = int(segment["observed"].sum())
    result: dict[str, Any] = {f"observed_fraction_{window}m": observed / window}
    lag_open = end_open - pd.Timedelta(minutes=window)
    lag_close = float(frame.at[lag_open, "close"]) if lag_open in frame.index and pd.notna(frame.at[lag_open, "close"]) else None
    result[f"ret_{window}m_pct"] = safe_pct(entry_price, lag_close)
    if observed == 0:
        return result
    high = segment["high"].max(skipna=True)
    low = segment["low"].min(skipna=True)
    quote = segment["quote_volume"].sum(min_count=1)
    trades = segment["trade_count"].sum(min_count=1)
    taker = segment["taker_buy_quote_volume"].sum(min_count=1)
    result[f"range_{window}m_pct"] = safe_pct(float(high), float(low)) if pd.notna(high) and pd.notna(low) else None
    result[f"quote_volume_{window}m"] = float(quote) if pd.notna(quote) else None
    result[f"trade_count_{window}m"] = float(trades) if pd.notna(trades) else None
    result[f"average_trade_quote_{window}m"] = float(quote / trades) if pd.notna(quote) and pd.notna(trades) and trades > 0 else None
    result[f"taker_buy_ratio_{window}m"] = float(taker / quote) if pd.notna(taker) and pd.notna(quote) and quote > 0 else None
    result[f"realized_vol_{window}m_pct"] = _realized_vol(segment["close"], window)
    closes = _finite(segment["close"])
    if closes.size > 1 and np.all(closes > 0):
        minute_rets = np.diff(np.log(closes))
        result[f"positive_return_fraction_{window}m"] = float(np.mean(minute_rets > 0))
    else:
        result[f"positive_return_fraction_{window}m"] = None
    drawdown, runup = _max_drawdown_runup(segment["close"])
    result[f"max_drawdown_{window}m_pct"] = drawdown
    result[f"max_runup_{window}m_pct"] = runup
    if pd.notna(high) and pd.notna(low) and float(high) > float(low):
        result[f"position_in_{window}m_range"] = (entry_price - float(low)) / (float(high) - float(low))
        result[f"close_vs_{window}m_high_pct"] = safe_pct(entry_price, float(high))
        result[f"close_vs_{window}m_low_pct"] = safe_pct(entry_price, float(low))
        high_times = segment.index[segment["high"] == high]
        low_times = segment.index[segment["low"] == low]
        result[f"minutes_since_{window}m_high"] = int((end_open - high_times[-1]).total_seconds() // 60) if len(high_times) else None
        result[f"minutes_since_{window}m_low"] = int((end_open - low_times[-1]).total_seconds() // 60) if len(low_times) else None
    else:
        result[f"position_in_{window}m_range"] = None
    slope, r2 = _trend(segment["close"])
    result[f"log_price_trend_{window}m_pct_per_day"] = slope
    result[f"log_price_trend_{window}m_r2"] = r2
    return result


def _daily_context(frame: pd.DataFrame, end_open: pd.Timestamp) -> dict[str, Any]:
    segment = _segment(frame, end_open, 14400).copy()
    observed = segment[segment["observed"]].copy()
    if observed.empty:
        return {}
    daily = observed.resample("1D").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        quote_volume=("quote_volume", "sum"),
        trades=("trade_count", "sum"),
        taker_quote=("taker_buy_quote_volume", "sum"),
        minute_count=("observed", "sum"),
    )
    daily = daily[daily["minute_count"] > 0].tail(11)
    result: dict[str, Any] = {
        "complete_daily_bars_in_context": int((daily["minute_count"] >= 1430).sum()),
        "calendar_days_with_data_in_context": int(len(daily)),
    }
    if len(daily) >= 3:
        volume_slope, volume_r2 = _trend(daily["quote_volume"], minutes_per_step=1440)
        price_slope, price_r2 = _trend(daily["close"], minutes_per_step=1440)
        ranges = (daily["high"] / daily["low"] - 1.0) * 100.0
        range_slope, range_r2 = _trend((ranges.clip(lower=1e-9) + 1.0), minutes_per_step=1440)
        result.update(
            {
                "daily_quote_volume_trend_pct_per_day": volume_slope,
                "daily_quote_volume_trend_r2": volume_r2,
                "daily_close_trend_pct_per_day": price_slope,
                "daily_close_trend_r2": price_r2,
                "daily_range_trend_pct_per_day": range_slope,
                "daily_range_trend_r2": range_r2,
                "up_day_fraction_context": float((daily["close"] > daily["open"]).mean()),
                "daily_taker_buy_ratio_median": float((daily["taker_quote"] / daily["quote_volume"].replace(0, np.nan)).median()),
            }
        )
    return result


def _episode_features(frame: pd.DataFrame, end_open: pd.Timestamp) -> dict[str, Any]:
    """Continuous ten-day distribution summaries without candidate-signal cut-offs."""
    segment = _segment(frame, end_open, 14400)
    observed = segment[segment["observed"]].copy()
    if observed.empty:
        return {}

    closes = observed["close"].astype(float)
    minute_returns = closes.pct_change().replace([np.inf, -np.inf], np.nan).dropna() * 100.0
    hourly = observed.resample("1h").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        quote_volume=("quote_volume", "sum"),
        trades=("trade_count", "sum"),
        minutes=("observed", "sum"),
    )
    hourly = hourly[hourly["minutes"] > 0].copy()
    hourly_returns = hourly["close"].pct_change().replace([np.inf, -np.inf], np.nan).dropna() * 100.0

    result: dict[str, Any] = {
        "observed_minutes_10d": int(len(observed)),
        "observed_hours_10d": int(len(hourly)),
    }

    def add_distribution(prefix: str, values: pd.Series) -> None:
        clean = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if clean.empty:
            return
        result[f"{prefix}_mean"] = float(clean.mean())
        result[f"{prefix}_std"] = float(clean.std(ddof=1)) if len(clean) > 1 else 0.0
        result[f"{prefix}_skew"] = float(clean.skew()) if len(clean) > 2 else None
        result[f"{prefix}_min"] = float(clean.min())
        result[f"{prefix}_p01"] = float(clean.quantile(0.01))
        result[f"{prefix}_p05"] = float(clean.quantile(0.05))
        result[f"{prefix}_p25"] = float(clean.quantile(0.25))
        result[f"{prefix}_median"] = float(clean.median())
        result[f"{prefix}_p75"] = float(clean.quantile(0.75))
        result[f"{prefix}_p95"] = float(clean.quantile(0.95))
        result[f"{prefix}_p99"] = float(clean.quantile(0.99))
        result[f"{prefix}_max"] = float(clean.max())
        result[f"{prefix}_positive_fraction"] = float((clean > 0).mean())

    add_distribution("minute_return_pct_10d", minute_returns)
    add_distribution("hourly_return_pct_10d", hourly_returns)
    add_distribution("hourly_quote_volume_10d", hourly["quote_volume"])
    add_distribution("hourly_trade_count_10d", hourly["trades"])

    if not hourly.empty:
        median_volume = float(hourly["quote_volume"].median())
        result["maximum_hourly_volume_vs_median_10d"] = (
            float(hourly["quote_volume"].max() / median_volume) if median_volume > 0 else None
        )
        prior_24h_high = hourly["high"].shift(1).rolling(24, min_periods=18).max()
        result["new_rolling_24h_high_count_10d"] = int((hourly["high"] >= prior_24h_high).fillna(False).sum())
        distance = (hourly["close"] / prior_24h_high - 1.0) * 100.0
        add_distribution("close_vs_prior_24h_high_pct_10d", distance)

    return result

def _same_time_features(frame: pd.DataFrame, end_open: pd.Timestamp) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for length in (15, 60):
        current = _segment(frame, end_open, length)["quote_volume"].sum(min_count=length)
        history: list[float] = []
        for days_back in range(1, 8):
            hist_end = end_open - pd.Timedelta(days=days_back)
            value = _segment(frame, hist_end, length)["quote_volume"].sum(min_count=length)
            if pd.notna(value):
                history.append(float(value))
        median = float(np.median(history)) if history else None
        result[f"quote_volume_{length}m_prior_7d_same_time_median"] = median
        result[f"quote_volume_{length}m_vs_prior_7d_same_time"] = float(current / median) if pd.notna(current) and median and median > 0 else None
    return result


def _reference_context(reference: pd.DataFrame, decision_time: datetime, prefix: str) -> dict[str, Any]:
    end_open = pd.Timestamp(floor_minute(decision_time) - timedelta(minutes=1))
    result: dict[str, Any] = {}
    if end_open not in reference.index or pd.isna(reference.at[end_open, "close"]):
        for window in REFERENCE_WINDOWS:
            result[f"{prefix}_ret_{window}m_pct"] = None
        return result
    entry = float(reference.at[end_open, "close"])
    for window in REFERENCE_WINDOWS:
        lag = end_open - pd.Timedelta(minutes=window)
        old = float(reference.at[lag, "close"]) if lag in reference.index and pd.notna(reference.at[lag, "close"]) else None
        result[f"{prefix}_ret_{window}m_pct"] = safe_pct(entry, old)
    result[f"{prefix}_rv_1440m_pct"] = _realized_vol(_segment(reference, end_open, 1440)["close"], 1440)
    return result


def compute_context_feature_row(
    frame: pd.DataFrame,
    *,
    sample: dict[str, Any],
    horizon_minutes: int,
    prior_days: int,
    min_entry_notional: float,
    reference_frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    anchor = parse_datetime(sample["anchor_time"])
    decision_time = anchor - timedelta(minutes=horizon_minutes)
    end_open = pd.Timestamp(floor_minute(decision_time) - timedelta(minutes=1))
    row: dict[str, Any] = {
        "sample_id": sample["sample_id"],
        "match_group_id": sample["match_group_id"],
        "sample_type": sample["sample_type"],
        "label": int(sample["label"]),
        "split": sample["split"],
        "symbol": sample["symbol"],
        "base_asset": sample.get("base_asset"),
        "quote_asset": sample.get("quote_asset"),
        "event_id": sample.get("event_id"),
        "control_id": sample.get("control_id"),
        "control_rank": sample.get("control_rank"),
        "anchor_time": anchor.isoformat(),
        "decision_horizon_minutes": int(horizon_minutes),
        "decision_time": decision_time.isoformat(),
        "last_complete_bar_open": end_open.isoformat(),
    }
    if end_open not in frame.index or pd.isna(frame.at[end_open, "close"]):
        row["feature_quality_status"] = "missing_decision_bar"
        return row
    entry = float(frame.at[end_open, "close"])
    row["entry_price"] = entry
    for window in CONTEXT_WINDOWS:
        row.update(_window_metrics(frame, end_open, window, entry))
    row.update(_daily_context(frame, end_open))
    row.update(_episode_features(frame, end_open))
    row.update(_same_time_features(frame, end_open))

    def ratio(numerator_key: str, denominator_key: str, out: str) -> None:
        a, b = row.get(numerator_key), row.get(denominator_key)
        row[out] = float(a / b) if a is not None and b not in (None, 0) else None

    ratio("quote_volume_1440m", "quote_volume_4320m", "volume_share_last_1d_of_3d")
    ratio("quote_volume_1440m", "quote_volume_10080m", "volume_share_last_1d_of_7d")
    ratio("quote_volume_4320m", "quote_volume_14400m", "volume_share_last_3d_of_10d")
    ratio("realized_vol_1440m_pct", "realized_vol_10080m_pct", "volatility_1d_to_7d_ratio")
    ratio("realized_vol_4320m_pct", "realized_vol_14400m_pct", "volatility_3d_to_10d_ratio")
    ratio("range_1440m_pct", "range_10080m_pct", "range_1d_to_7d_ratio")
    ratio("average_trade_quote_1440m", "average_trade_quote_10080m", "average_trade_size_1d_to_7d_ratio")

    ret1d, ret3d, ret7d, ret10d = (row.get(f"ret_{w}m_pct") for w in (1440, 4320, 10080, 14400))
    row["return_acceleration_1d_vs_prior_2d_pct_points_per_day"] = (
        float(ret1d) - (float(ret3d) - float(ret1d)) / 2.0
        if ret1d is not None and ret3d is not None else None
    )
    row["return_acceleration_3d_vs_10d_pct_points_per_day"] = (
        float(ret3d) / 3.0 - (float(ret10d) - float(ret3d)) / 7.0
        if ret3d is not None and ret10d is not None else None
    )
    row["relative_strength_3d_minus_10d_rate_pct_points"] = (
        float(ret3d) / 3.0 - float(ret10d) / 10.0 if ret3d is not None and ret10d is not None else None
    )

    history_minutes = prior_days * 1440
    history = _segment(frame, end_open, history_minutes)
    observed_fraction = float(history["observed"].sum() / history_minutes) if history_minutes else 0.0
    row["observed_fraction_prior_window"] = observed_fraction
    row["missing_minutes_prior_window"] = int(history_minutes - history["observed"].sum())
    observed_before = frame.loc[:end_open, "observed"]
    first_observed = observed_before[observed_before].index.min() if bool(observed_before.any()) else None
    row["observed_history_days"] = float((end_open - first_observed).total_seconds() / 86400.0) if first_observed is not None else 0.0
    entry_volume = _segment(frame, end_open, 5)["quote_volume"].sum(min_count=5)
    row["entry_quote_volume_5m"] = float(entry_volume) if pd.notna(entry_volume) else None
    row["entry_liquidity_pass"] = bool(pd.notna(entry_volume) and float(entry_volume) >= min_entry_notional)

    reference_returns: dict[int, list[float]] = defaultdict(list)
    for symbol, reference in reference_frames.items():
        prefix = symbol.replace("USDT", "").lower()
        features = _reference_context(reference, decision_time, prefix)
        row.update(features)
        for window in REFERENCE_WINDOWS:
            value = features.get(f"{prefix}_ret_{window}m_pct")
            if value is not None:
                reference_returns[window].append(float(value))
            coin_ret = row.get(f"ret_{window}m_pct")
            row[f"ret_{window}m_minus_{prefix}_pct_points"] = float(coin_ret) - float(value) if coin_ret is not None and value is not None else None
    for window in REFERENCE_WINDOWS:
        proxy = float(np.mean(reference_returns[window])) if reference_returns[window] else None
        row[f"market_proxy_ret_{window}m_pct"] = proxy
        coin_ret = row.get(f"ret_{window}m_pct")
        row[f"ret_{window}m_minus_market_proxy_pct_points"] = float(coin_ret) - proxy if coin_ret is not None and proxy is not None else None

    outcome_start = pd.Timestamp(floor_minute(decision_time))
    anchor_open = pd.Timestamp(floor_minute(anchor))
    to_anchor = frame.loc[outcome_start:anchor_open]
    next_8h = frame.loc[outcome_start : outcome_start + pd.Timedelta(minutes=479)]
    for prefix, segment in (("to_anchor", to_anchor), ("next_8h", next_8h)):
        high = segment["high"].max(skipna=True)
        low = segment["low"].min(skipna=True)
        row[f"outcome_{prefix}_max_gain_pct"] = safe_pct(float(high), entry) if pd.notna(high) else None
        row[f"outcome_{prefix}_max_drawdown_pct"] = safe_pct(float(low), entry) if pd.notna(low) else None
        row[f"outcome_{prefix}_observed_fraction"] = float(segment["observed"].mean()) if len(segment) else 0.0

    if observed_fraction >= 0.995 and row["entry_liquidity_pass"]:
        row["feature_quality_status"] = "pass"
    elif observed_fraction >= 0.98:
        row["feature_quality_status"] = "warning"
    else:
        row["feature_quality_status"] = "insufficient_history"
    return row


class TenDayContextBuilder:
    def __init__(self, db: SupabaseClient, binance: BinanceClient, temp_root: Path):
        self.db = db
        self.binance = binance
        self.temp_root = temp_root
        self.cache = MinuteArchiveCache(binance, temp_root)

    def _samples(self, matched_job: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        matched_job_id = str(matched_job["id"])
        scan_id = str(matched_job["scan_id"])
        events = self.db.select_all(
            "binance_gainer_events",
            filters={"scan_id": f"eq.{scan_id}", "sellability_pass": "eq.true"},
            order="event_date.asc,symbol.asc",
        )
        matches = self.db.select_all(
            "binance_control_matches",
            filters={"matched_control_job_id": f"eq.{matched_job_id}"},
            order="symbol.asc,control_anchor_time.asc",
        )
        split_map, split_summary = assign_temporal_splits(
            events,
            int(matched_job.get("discovery_pct") or 70),
            int(matched_job.get("validation_pct") or 15),
        )
        samples: list[dict[str, Any]] = []
        event_by_id = {str(event["id"]): event for event in events}
        for event in events:
            split = split_map[date.fromisoformat(str(event["event_date"]))]
            anchor = parse_datetime(event.get("first_cross_trade_time") or event["first_cross_time"])
            samples.append(
                {
                    "sample_id": f"event:{event['id']}",
                    "match_group_id": str(event["id"]),
                    "sample_type": "event",
                    "label": 1,
                    "split": split,
                    "symbol": str(event["symbol"]),
                    "base_asset": event.get("base_asset"),
                    "quote_asset": event.get("quote_asset"),
                    "event_id": str(event["id"]),
                    "control_id": None,
                    "control_rank": None,
                    "anchor_time": anchor.isoformat(),
                }
            )
        for match in matches:
            event = event_by_id.get(str(match["event_id"]))
            if event is None:
                continue
            samples.append(
                {
                    "sample_id": f"control:{match['control_id']}",
                    "match_group_id": str(match["event_id"]),
                    "sample_type": "control",
                    "label": 0,
                    "split": str(match["split"]),
                    "symbol": str(match["symbol"]),
                    "base_asset": event.get("base_asset"),
                    "quote_asset": event.get("quote_asset"),
                    "event_id": str(match["event_id"]),
                    "control_id": str(match["control_id"]),
                    "control_rank": int(match["control_rank"]),
                    "anchor_time": str(match["control_anchor_time"]),
                }
            )
        return samples, split_summary, {"events": events, "matches": matches}

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["id"])
        matched_job_id = str(job["matched_control_job_id"])
        prior_days = int(job.get("prior_days") or 10)
        if prior_days != 10:
            raise ValueError("This release uses exactly 10 days of context")
        horizons = tuple(sorted({int(value) for value in (job.get("horizons_minutes") or [15, 30, 60, 120])}))
        min_entry_notional = float(job.get("min_entry_notional") or 500)
        research_mode = str(job.get("research_mode") or "exploratory_reuse")
        if research_mode not in {"exploratory_reuse", "fresh_staged"}:
            raise ValueError("research_mode must be exploratory_reuse or fresh_staged")
        matched_rows = self.db.select("binance_matched_control_jobs", filters={"id": f"eq.{matched_job_id}"}, limit=1)
        if not matched_rows:
            raise RuntimeError("Matched-control job not found")
        matched_job = matched_rows[0]
        if matched_job.get("status") not in {"completed", "completed_with_warnings"}:
            raise RuntimeError("Matched-control job must be completed")
        samples, split_summary, source = self._samples(matched_job)
        if not samples:
            raise RuntimeError("No event/control samples found")
        anchors = [parse_datetime(row["anchor_time"]) for row in samples]
        load_start = min(anchors).date() - timedelta(days=prior_days + 2)
        load_end = max(anchors).date() + timedelta(days=1)

        self.db.update(
            "binance_context_jobs",
            {"id": f"eq.{job_id}"},
            {
                "samples_total": len(samples),
                "events_total": sum(row["label"] == 1 for row in samples),
                "controls_total": sum(row["label"] == 0 for row in samples),
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        work = Path(tempfile.mkdtemp(prefix=f"context-{job_id}-", dir=self.temp_root))
        feature_rows: list[dict[str, Any]] = []
        quality_rows: list[dict[str, Any]] = []
        source_manifest: list[dict[str, Any]] = []
        failures = 0
        samples_failed = 0
        try:
            symbols = sorted(set(row["symbol"] for row in samples) | set(REFERENCE_SYMBOLS))
            loaded: dict[str, Any] = {}
            for symbol in symbols:
                try:
                    loaded[symbol] = self.cache.load_symbol(symbol, load_start, load_end)
                    source_manifest.extend(loaded[symbol].source_manifest)
                except Exception as exc:
                    failures += 1
                    self.db.insert(
                        "binance_context_issues",
                        {
                            "context_job_id": job_id,
                            "symbol": symbol,
                            "stage": "load_symbol",
                            "message": str(exc)[:4000],
                        },
                    )
            reference_frames = {symbol: loaded[symbol].frame for symbol in REFERENCE_SYMBOLS if symbol in loaded}
            samples_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for sample in samples:
                samples_by_symbol[sample["symbol"]].append(sample)
            processed = 0
            for symbol, symbol_samples in sorted(samples_by_symbol.items()):
                if symbol not in loaded:
                    samples_failed += len(symbol_samples)
                    continue
                frame = loaded[symbol].frame
                for sample in symbol_samples:
                    for horizon in horizons:
                        feature = compute_context_feature_row(
                            frame,
                            sample=sample,
                            horizon_minutes=horizon,
                            prior_days=prior_days,
                            min_entry_notional=min_entry_notional,
                            reference_frames=reference_frames,
                        )
                        feature_rows.append(feature)
                        quality_rows.append(
                            {
                                "sample_id": feature.get("sample_id"),
                                "split": feature.get("split"),
                                "symbol": feature.get("symbol"),
                                "decision_horizon_minutes": horizon,
                                "feature_quality_status": feature.get("feature_quality_status"),
                                "observed_fraction_prior_window": feature.get("observed_fraction_prior_window"),
                                "entry_liquidity_pass": feature.get("entry_liquidity_pass"),
                            }
                        )
                processed += len(symbol_samples)
                self.db.update(
                    "binance_context_jobs",
                    {"id": f"eq.{job_id}"},
                    {
                        "samples_processed": processed,
                        "feature_rows": len(feature_rows),
                        "failures": failures,
                        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

            sample_df = pd.DataFrame(samples)
            feature_df = pd.DataFrame(feature_rows)
            quality_df = pd.DataFrame(quality_rows)
            source_df = pd.DataFrame(source_manifest)
            split_df = pd.DataFrame(split_summary)
            design = {
                "version": "v1_1_chatgpt_led_ten_day_context",
                "source_matched_control_job_id": matched_job_id,
                "source_scan_id": str(matched_job["scan_id"]),
                "research_mode": research_mode,
                "context_days": 10,
                "decision_horizons_minutes": list(horizons),
                "context_windows_minutes": list(CONTEXT_WINDOWS),
                "feature_cutoff": "only fully completed one-minute bars strictly before decision_time",
                "feature_families": [
                    "multi-timescale price trend, range, drawdown, run-up and range position",
                    "volume, trade intensity, average trade size and taker-buy balance",
                    "volatility compression and expansion",
                    "daily trend and acceleration",
                    "continuous ten-day return and activity distributions",
                    "same-clock-time activity shifts",
                    "relative strength versus BTC, ETH, BNB and their equal-weight proxy",
                    "data completeness and executable-entry liquidity",
                ],
                "outcome_columns_rule": "columns beginning outcome_ are labels/diagnostics and must never be predictors",
                "research_integrity": (
                    "All outputs are exploratory because the source splits have already been opened."
                    if research_mode == "exploratory_reuse"
                    else "Discovery first; ChatGPT freezes candidate rules before one validation pass; sealed_test remains closed until the complete rule is frozen."
                ),
                "cluster_warning": "Rows are clustered by symbol and matched event; row count is not independent sample size.",
            }
            quality_report = {
                "samples_total": len(samples),
                "events_total": int(sum(row["label"] == 1 for row in samples)),
                "controls_total": int(sum(row["label"] == 0 for row in samples)),
                "feature_rows": len(feature_rows),
                "quality_counts": quality_df["feature_quality_status"].value_counts(dropna=False).to_dict() if not quality_df.empty else {},
                "symbols": int(sample_df["symbol"].nunique()) if not sample_df.empty else 0,
                "symbol_failures": failures,
                "source_status_counts": source_df["status"].value_counts(dropna=False).to_dict() if not source_df.empty else {},
            }

            uploaded: list[dict[str, Any]] = []
            storage_prefix = f"ten-day-context/{job_id}"

            def upload(path: Path, role: str, split: str | None = None) -> str:
                storage_path = f"{storage_prefix}/{path.name}"
                self.db.upload_file(storage_path, path, "application/zip")
                record = {
                    "context_job_id": job_id,
                    "split": split,
                    "storage_path": storage_path,
                    "filename": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "content_type": "application/zip",
                    "role": role,
                }
                self.db.upsert("binance_context_files", [record], on_conflict="context_job_id,storage_path")
                uploaded.append(record)
                return storage_path

            package_paths: dict[str, list[str]] = {}

            def group_ids_for(samples_part: pd.DataFrame) -> list[str]:
                if samples_part.empty or "match_group_id" not in samples_part:
                    return []
                return samples_part["match_group_id"].dropna().astype(str).drop_duplicates().tolist()

            def build_parts(split_name: str, samples_part: pd.DataFrame,
                            features_part: pd.DataFrame, quality_part: pd.DataFrame,
                            readme: str) -> list[Path]:
                def writer(folder: Path, selected_groups: list[str]) -> None:
                    selected = set(selected_groups)
                    if "__EMPTY_PACKAGE__" in selected:
                        sample_chunk = samples_part.iloc[0:0].copy()
                    else:
                        sample_chunk = samples_part[
                            samples_part["match_group_id"].astype(str).isin(selected)
                        ].copy()
                    sample_ids = set(sample_chunk["sample_id"].astype(str)) if "sample_id" in sample_chunk else set()
                    feature_chunk = features_part[
                        features_part["sample_id"].astype(str).isin(sample_ids)
                    ].copy() if "sample_id" in features_part else features_part.iloc[0:0].copy()
                    quality_chunk = quality_part[
                        quality_part["sample_id"].astype(str).isin(sample_ids)
                    ].copy() if "sample_id" in quality_part else quality_part.iloc[0:0].copy()

                    folder.mkdir(parents=True, exist_ok=True)
                    sample_chunk.to_csv(folder / "sample_anchors.csv", index=False)
                    # Parquet is the canonical feature matrix. Omitting a duplicate
                    # CSV materially reduces package size without losing information.
                    feature_chunk.to_parquet(
                        folder / "ten_day_context_features.parquet",
                        index=False,
                        compression="zstd",
                    )
                    quality_chunk.to_csv(folder / "data_quality.csv", index=False)
                    (folder / "design.json").write_text(
                        json.dumps(design, indent=2, default=_json_ready), encoding="utf-8"
                    )
                    write_analysis_contract(folder, f"ten_day_context_{split_name}")
                    (folder / "README.txt").write_text(
                        readme
                        + "\nThe canonical feature matrix is Parquet. ChatGPT should load every part for this split before analysing it.\n",
                        encoding="utf-8",
                    )

                return build_grouped_zip_parts(
                    work_dir=work,
                    base_name=f"ten_day_context_{split_name}",
                    group_ids=group_ids_for(samples_part),
                    writer=writer,
                )

            if research_mode == "exploratory_reuse":
                paths = build_parts(
                    "exploratory",
                    sample_df,
                    feature_df,
                    quality_df,
                    "EXPLORATORY ONLY. Split-sealing is not enforced in this package. Use it for ChatGPT-led discovery or implementation audit, not final confirmation.\n",
                )
                package_paths["exploratory"] = [
                    upload(path, "ten_day_context_exploratory", None) for path in paths
                ]
            else:
                for split in SPLITS:
                    split_samples = sample_df[sample_df["split"] == split].copy()
                    split_features = feature_df[feature_df["split"] == split].copy()
                    split_quality = quality_df[quality_df["split"] == split].copy()
                    paths = build_parts(
                        split,
                        split_samples,
                        split_features,
                        split_quality,
                        f"Ten-day context {split} package.\n"
                        + ("DO NOT OPEN UNTIL FINAL RULES ARE FROZEN.\n" if split == "sealed_test" else ""),
                    )
                    package_paths[split] = [
                        upload(path, f"ten_day_context_{split}", split) for path in paths
                    ]

            index = work / "index"
            index.mkdir(parents=True, exist_ok=True)
            split_df.to_csv(index / "split_summary.csv", index=False)
            source_df.to_csv(index / "source_archive_manifest.csv", index=False)
            (index / "design.json").write_text(json.dumps(design, indent=2, default=_json_ready), encoding="utf-8")
            (index / "quality_report.json").write_text(json.dumps(quality_report, indent=2, default=_json_ready), encoding="utf-8")
            pd.DataFrame(uploaded).to_csv(index / "package_manifest.csv", index=False)
            write_analysis_contract(index, "ten_day_context_index")
            index_path = work / "ten_day_context_index.zip"
            _zip_directory(index, index_path)
            index_storage = upload(index_path, "ten_day_context_index", None)
            return {
                "samples_total": len(samples),
                "samples_processed": len(samples) - samples_failed,
                "events_total": int(sum(row["label"] == 1 for row in samples)),
                "controls_total": int(sum(row["label"] == 0 for row in samples)),
                "feature_rows": len(feature_rows),
                "failures": failures,
                "research_mode": research_mode,
                "index_storage_path": index_storage,
                "package_paths": package_paths,
                "quality_report": quality_report,
            }
        finally:
            shutil.rmtree(work, ignore_errors=True)
