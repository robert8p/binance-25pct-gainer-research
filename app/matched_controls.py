from __future__ import annotations

import hashlib
import json
import math
import shutil
import tempfile
import uuid
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .analysis_contract import write_analysis_contract
from .binance import BinanceClient, archive_url, download_archive, normalize_archive_timestamp, sha256_file
from .runtime import collect_memory, ensure_disk_headroom, log_resources
from .supabase import SupabaseClient


KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]
NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]
FEATURE_WINDOWS = (1, 5, 15, 30, 60, 120, 180, 360, 480, 720, 1440)
REFERENCE_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
SPLITS = ("discovery", "validation", "sealed_test")


def floor_minute(value: datetime) -> datetime:
    value = ensure_utc(value)
    return value.replace(second=0, microsecond=0)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return ensure_utc(parsed)


def safe_pct(new: float | None, old: float | None) -> float | None:
    if new is None or old is None or not math.isfinite(float(new)) or not math.isfinite(float(old)) or float(old) <= 0:
        return None
    return (float(new) / float(old) - 1.0) * 100.0


def deterministic_tiebreak(*values: Any) -> str:
    payload = "|".join(str(value) for value in values).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def deterministic_uuid(*values: Any) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "|".join(str(value) for value in values)))


def assign_temporal_splits(
    events: list[dict[str, Any]], discovery_pct: int = 70, validation_pct: int = 15
) -> tuple[dict[date, str], list[dict[str, Any]]]:
    """Assign whole UTC event dates to chronological development splits.

    The cut points target event counts but never split one date across datasets.
    At least one date is retained for each split whenever three or more event
    dates exist.
    """
    if discovery_pct <= 0 or validation_pct <= 0 or discovery_pct + validation_pct >= 100:
        raise ValueError("split percentages must be positive and leave room for sealed_test")
    counts: Counter[date] = Counter(date.fromisoformat(str(row["event_date"])) for row in events)
    dates = sorted(counts)
    if not dates:
        return {}, []
    if len(dates) < 3:
        # This is an explicit quality limitation rather than silently creating
        # a random split that would leak dates across datasets.
        mapping = {day: SPLITS[min(index, len(SPLITS) - 1)] for index, day in enumerate(dates)}
    else:
        total = sum(counts.values())
        discovery_target = total * discovery_pct / 100.0
        validation_target = total * (discovery_pct + validation_pct) / 100.0
        mapping: dict[date, str] = {}
        cumulative = 0
        for index, day in enumerate(dates):
            remaining_dates = len(dates) - index
            if cumulative < discovery_target and remaining_dates > 2:
                split = "discovery"
            elif cumulative < validation_target and remaining_dates > 1:
                split = "validation"
            else:
                split = "sealed_test"
            mapping[day] = split
            cumulative += counts[day]

        # Defensive repair for unusual count concentrations.
        present = set(mapping.values())
        if "sealed_test" not in present:
            mapping[dates[-1]] = "sealed_test"
        if "validation" not in present:
            mapping[dates[-2]] = "validation"
        if "discovery" not in present:
            mapping[dates[0]] = "discovery"

    summary: list[dict[str, Any]] = []
    for split in SPLITS:
        split_dates = [day for day in dates if mapping.get(day) == split]
        summary.append(
            {
                "split": split,
                "start_date": min(split_dates).isoformat() if split_dates else None,
                "end_date": max(split_dates).isoformat() if split_dates else None,
                "event_dates": len(split_dates),
                "events": sum(counts[day] for day in split_dates),
            }
        )
    return mapping, summary


def rolling_crossing_mask(
    frame: pd.DataFrame, *, threshold_pct: float, window_minutes: int
) -> pd.Series:
    """Conservative minute crossing mask matching the scanner's ordering rule."""
    if window_minutes < 2:
        raise ValueError("window_minutes must be at least two")
    factor = 1.0 + threshold_pct / 100.0
    prior_low = frame["low"].shift(1).rolling(window_minutes - 1, min_periods=window_minutes - 1).min()
    complete = (
        frame["observed"].shift(1).rolling(window_minutes - 1, min_periods=window_minutes - 1).sum()
        == window_minutes - 1
    )
    result = complete & frame["high"].notna() & (frame["high"] >= prior_low * factor)
    return result.fillna(False).astype(bool)


def _normalise_archive_frame(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    raw = raw.iloc[:, : len(KLINE_COLUMNS)].copy()
    raw.columns = KLINE_COLUMNS[: raw.shape[1]]
    # Recent Binance archives may contain a header row; discard it safely.
    raw["open_time"] = pd.to_numeric(raw["open_time"], errors="coerce")
    raw = raw[raw["open_time"].notna()].copy()
    if raw.empty:
        return pd.DataFrame(columns=["symbol", *KLINE_COLUMNS[:-1]])
    open_values = raw["open_time"].astype("int64").map(normalize_archive_timestamp)
    close_values = pd.to_numeric(raw["close_time"], errors="coerce").fillna(0).astype("int64").map(normalize_archive_timestamp)
    raw["open_time"] = pd.to_datetime(open_values, unit="ms", utc=True)
    raw["close_time"] = pd.to_datetime(close_values, unit="ms", utc=True)
    for column in NUMERIC_COLUMNS:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw["trade_count"] = raw["trade_count"].fillna(0).astype("int64")
    raw["symbol"] = symbol
    return raw[["symbol", *KLINE_COLUMNS[:-1]]]


def read_kline_archive(path: Path, symbol: str) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        if not members:
            raise RuntimeError(f"Archive is empty: {path}")
        with archive.open(members[0]) as handle:
            raw = pd.read_csv(handle, header=None, dtype=str)
    return _normalise_archive_frame(raw, symbol)


def rest_rows_to_frame(rows: list[list[Any]], symbol: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["symbol", *KLINE_COLUMNS[:-1]])
    raw = pd.DataFrame(rows, columns=KLINE_COLUMNS)
    raw["symbol"] = symbol
    raw["open_time"] = pd.to_datetime(pd.to_numeric(raw["open_time"]), unit="ms", utc=True)
    raw["close_time"] = pd.to_datetime(pd.to_numeric(raw["close_time"]), unit="ms", utc=True)
    for column in NUMERIC_COLUMNS:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw["trade_count"] = raw["trade_count"].fillna(0).astype("int64")
    return raw[["symbol", *KLINE_COLUMNS[:-1]]]


@dataclass
class LoadedSymbol:
    frame: pd.DataFrame
    source_manifest: list[dict[str, Any]]


class MinuteArchiveCache:
    def __init__(self, binance: BinanceClient, root: Path):
        self.binance = binance
        self.root = root / "matched-control-cache"
        self.root.mkdir(parents=True, exist_ok=True)

    def _paths(self, symbol: str, day: date) -> tuple[Path, Path, Path]:
        folder = self.root / symbol
        folder.mkdir(parents=True, exist_ok=True)
        stem = f"{symbol}-1m-{day.isoformat()}"
        return folder / f"{stem}.zip", folder / f"{stem}.parquet", folder / f"{stem}.missing"

    def load_symbol(
        self,
        symbol: str,
        start_day: date,
        end_day_exclusive: date,
        *,
        required_columns: tuple[str, ...] | None = None,
    ) -> LoadedSymbol:
        frames: list[pd.DataFrame] = []
        manifest: list[dict[str, Any]] = []
        day = start_day
        while day < end_day_exclusive:
            archive_path, fallback_path, missing_path = self._paths(symbol, day)
            source_url = archive_url("klines", symbol, day, "1m")
            status = ""
            source = ""
            try:
                if archive_path.exists() and zipfile.is_zipfile(archive_path):
                    frame = read_kline_archive(archive_path, symbol)
                    source = "official_daily_archive_cache"
                    status = "available"
                elif fallback_path.exists():
                    frame = pd.read_parquet(fallback_path)
                    frame["open_time"] = pd.to_datetime(frame["open_time"], utc=True)
                    frame["close_time"] = pd.to_datetime(frame["close_time"], utc=True)
                    source = "rest_fallback_cache"
                    status = "available"
                elif missing_path.exists():
                    frame = pd.DataFrame()
                    source = "missing_marker"
                    status = "unavailable"
                else:
                    available = download_archive(source_url, archive_path)
                    if available:
                        frame = read_kline_archive(archive_path, symbol)
                        source = "official_daily_archive"
                        status = "available"
                    else:
                        # The newest completed UTC day can briefly precede archive
                        # publication. The public REST endpoint is an integrity-safe
                        # fallback because the scanner used the same endpoint.
                        start_ms = int(datetime.combine(day, time.min, tzinfo=timezone.utc).timestamp() * 1000)
                        end_ms = start_ms + 86_400_000
                        rows = self.binance.klines(symbol, "1m", start_ms, end_ms)
                        frame = rest_rows_to_frame(rows, symbol)
                        if frame.empty:
                            missing_path.write_text("No archive or REST rows", encoding="utf-8")
                            source = "archive_and_rest_unavailable"
                            status = "unavailable"
                        else:
                            frame.to_parquet(fallback_path, index=False, compression="zstd")
                            source = "public_rest_fallback"
                            status = "available"
                if not frame.empty:
                    # Drop unused columns before retaining each daily frame. This
                    # avoids holding 155 days of full Binance kline schemas plus
                    # the concatenated result at the same time.
                    if required_columns is not None:
                        daily_keep = list(dict.fromkeys(["open_time", *required_columns]))
                        missing_daily = [column for column in daily_keep if column not in frame.columns]
                        if missing_daily:
                            raise RuntimeError(
                                f"Required kline columns missing for {symbol} on {day}: {missing_daily}"
                            )
                        frame = frame[daily_keep].copy()
                    frames.append(frame)
                checksum_path = archive_path if archive_path.exists() else fallback_path
                manifest.append(
                    {
                        "symbol": symbol,
                        "date": day.isoformat(),
                        "status": status,
                        "source": source,
                        "source_url": source_url,
                        "row_count": int(len(frame)),
                        "sha256": sha256_file(checksum_path) if checksum_path.exists() else None,
                        "cache_filename": checksum_path.name if checksum_path.exists() else None,
                    }
                )
            except Exception as exc:
                manifest.append(
                    {
                        "symbol": symbol,
                        "date": day.isoformat(),
                        "status": "error",
                        "source": source or "unknown",
                        "source_url": source_url,
                        "row_count": 0,
                        "sha256": None,
                        "cache_filename": None,
                        "error": str(exc)[:1000],
                    }
                )
            day += timedelta(days=1)

        start_ts = pd.Timestamp(datetime.combine(start_day, time.min, tzinfo=timezone.utc))
        end_ts = pd.Timestamp(datetime.combine(end_day_exclusive, time.min, tzinfo=timezone.utc))
        index = pd.date_range(start_ts, end_ts - pd.Timedelta(minutes=1), freq="1min", tz="UTC")
        if frames:
            combined = pd.concat(frames, ignore_index=True)
            combined = combined.sort_values("open_time").drop_duplicates("open_time", keep="last")
            combined = combined.set_index("open_time").reindex(index)
        else:
            combined = pd.DataFrame(index=index)
        for column in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "taker_buy_base_volume", "taker_buy_quote_volume"]:
            if column not in combined:
                combined[column] = np.nan
        combined["observed"] = combined["close"].notna()
        if required_columns is not None:
            keep = list(dict.fromkeys([*required_columns, "observed"]))
            missing = [column for column in keep if column not in combined.columns]
            if missing:
                raise RuntimeError(f"Required kline columns missing for {symbol}: {missing}")
            combined = combined[keep].copy()
        combined.index.name = "open_time"
        return LoadedSymbol(combined, manifest)


def _segment(frame: pd.DataFrame, end_open: pd.Timestamp, minutes: int) -> pd.DataFrame:
    start = end_open - pd.Timedelta(minutes=minutes - 1)
    return frame.loc[start:end_open]


def _max_drawdown_and_runup(values: np.ndarray) -> tuple[float | None, float | None]:
    values = values[np.isfinite(values)]
    if values.size < 2 or np.any(values <= 0):
        return None, None
    running_max = np.maximum.accumulate(values)
    running_min = np.minimum.accumulate(values)
    drawdown = float(np.min(values / running_max - 1.0) * 100.0)
    runup = float(np.max(values / running_min - 1.0) * 100.0)
    return drawdown, runup


def _reference_features(frame: pd.DataFrame, decision_time: datetime, prefix: str) -> dict[str, Any]:
    end_open = pd.Timestamp(floor_minute(decision_time) - timedelta(minutes=1))
    result: dict[str, Any] = {}
    if end_open not in frame.index or pd.isna(frame.at[end_open, "close"]):
        return {f"{prefix}_ret_{window}m_pct": None for window in (15, 60, 180, 480)} | {f"{prefix}_rv_60m_pct": None}
    end_close = float(frame.at[end_open, "close"])
    for window in (15, 60, 180, 480):
        lag = end_open - pd.Timedelta(minutes=window)
        old = float(frame.at[lag, "close"]) if lag in frame.index and pd.notna(frame.at[lag, "close"]) else None
        result[f"{prefix}_ret_{window}m_pct"] = safe_pct(end_close, old)
    segment = _segment(frame, end_open, 60)["close"].dropna().to_numpy(dtype=float)
    if segment.size >= 30 and np.all(segment > 0):
        returns = np.diff(np.log(segment))
        result[f"{prefix}_rv_60m_pct"] = float(np.std(returns, ddof=1) * math.sqrt(60) * 100.0) if returns.size > 1 else None
    else:
        result[f"{prefix}_rv_60m_pct"] = None
    return result


def compute_feature_row(
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
        "control_rank": sample.get("control_rank"),
        "anchor_time": anchor.isoformat(),
        "decision_horizon_minutes": int(horizon_minutes),
        "decision_time": decision_time.isoformat(),
        "last_complete_bar_open": end_open.isoformat(),
    }
    if end_open not in frame.index or pd.isna(frame.at[end_open, "close"]):
        row["feature_quality_status"] = "missing_decision_bar"
        row["entry_price"] = None
        return row

    entry_price = float(frame.at[end_open, "close"])
    row["entry_price"] = entry_price
    for window in FEATURE_WINDOWS:
        segment = _segment(frame, end_open, window)
        observed = int(segment["observed"].sum())
        row[f"observed_fraction_{window}m"] = observed / window
        lag_open = end_open - pd.Timedelta(minutes=window)
        lag_close = float(frame.at[lag_open, "close"]) if lag_open in frame.index and pd.notna(frame.at[lag_open, "close"]) else None
        row[f"ret_{window}m_pct"] = safe_pct(entry_price, lag_close)
        if observed:
            high = segment["high"].max(skipna=True)
            low = segment["low"].min(skipna=True)
            row[f"range_{window}m_pct"] = safe_pct(float(high), float(low)) if pd.notna(high) and pd.notna(low) else None
            row[f"quote_volume_{window}m"] = float(segment["quote_volume"].sum(min_count=1))
            row[f"trade_count_{window}m"] = float(segment["trade_count"].sum(min_count=1))
            quote_sum = float(segment["quote_volume"].sum(min_count=1))
            taker_sum = float(segment["taker_buy_quote_volume"].sum(min_count=1))
            row[f"taker_buy_ratio_{window}m"] = taker_sum / quote_sum if quote_sum > 0 else None
            closes = segment["close"].dropna().to_numpy(dtype=float)
            drawdown, runup = _max_drawdown_and_runup(closes)
            row[f"max_drawdown_{window}m_pct"] = drawdown
            row[f"max_runup_{window}m_pct"] = runup
            if window >= 5 and closes.size >= max(3, int(window * 0.8)) and np.all(closes > 0):
                minute_returns = np.diff(np.log(closes))
                row[f"realized_vol_{window}m_pct"] = (
                    float(np.std(minute_returns, ddof=1) * math.sqrt(window) * 100.0)
                    if minute_returns.size > 1
                    else None
                )
                row[f"positive_return_fraction_{window}m"] = float(np.mean(minute_returns > 0)) if minute_returns.size else None
            else:
                row[f"realized_vol_{window}m_pct"] = None
                row[f"positive_return_fraction_{window}m"] = None
        else:
            for metric in (
                "range",
                "quote_volume",
                "trade_count",
                "taker_buy_ratio",
                "max_drawdown",
                "max_runup",
                "realized_vol",
                "positive_return_fraction",
            ):
                suffix = "_pct" if metric in {"range", "max_drawdown", "max_runup", "realized_vol"} else ""
                row[f"{metric}_{window}m{suffix}"] = None

    def sum_segment(start_offset: int, length: int, column: str) -> float | None:
        end = end_open - pd.Timedelta(minutes=start_offset)
        segment = _segment(frame, end, length)
        value = segment[column].sum(min_count=length)
        return float(value) if pd.notna(value) else None

    recent5 = sum_segment(0, 5, "quote_volume")
    previous30 = sum_segment(5, 30, "quote_volume")
    recent15 = sum_segment(0, 15, "quote_volume")
    previous60 = sum_segment(15, 60, "quote_volume")
    recent_trades5 = sum_segment(0, 5, "trade_count")
    previous_trades30 = sum_segment(5, 30, "trade_count")
    row["volume_ratio_5m_to_previous_30m"] = recent5 / (previous30 / 6.0) if recent5 is not None and previous30 and previous30 > 0 else None
    row["volume_ratio_15m_to_previous_60m"] = recent15 / (previous60 / 4.0) if recent15 is not None and previous60 and previous60 > 0 else None
    row["trade_intensity_ratio_5m_to_previous_30m"] = recent_trades5 / (previous_trades30 / 6.0) if recent_trades5 is not None and previous_trades30 and previous_trades30 > 0 else None

    ret15 = row.get("ret_15m_pct")
    old_end = end_open - pd.Timedelta(minutes=15)
    old_start = old_end - pd.Timedelta(minutes=15)
    previous15_return = None
    if old_end in frame.index and old_start in frame.index and pd.notna(frame.at[old_end, "close"]) and pd.notna(frame.at[old_start, "close"]):
        previous15_return = safe_pct(float(frame.at[old_end, "close"]), float(frame.at[old_start, "close"]))
    row["return_acceleration_15m_pct_points"] = float(ret15) - float(previous15_return) if ret15 is not None and previous15_return is not None else None

    recent_taker = row.get("taker_buy_ratio_15m")
    previous_taker_segment = _segment(frame, end_open - pd.Timedelta(minutes=15), 15)
    previous_quote = previous_taker_segment["quote_volume"].sum(min_count=15)
    previous_taker_quote = previous_taker_segment["taker_buy_quote_volume"].sum(min_count=15)
    previous_taker = float(previous_taker_quote / previous_quote) if pd.notna(previous_quote) and previous_quote > 0 else None
    row["taker_buy_ratio_change_15m"] = float(recent_taker) - previous_taker if recent_taker is not None and previous_taker is not None else None

    day_segment = _segment(frame, end_open, 1440)
    high24 = day_segment["high"].max(skipna=True)
    low24 = day_segment["low"].min(skipna=True)
    row["close_vs_24h_high_pct"] = safe_pct(entry_price, float(high24)) if pd.notna(high24) else None
    row["close_vs_24h_low_pct"] = safe_pct(entry_price, float(low24)) if pd.notna(low24) else None
    if pd.notna(high24) and pd.notna(low24) and float(high24) > float(low24):
        row["position_in_24h_range"] = (entry_price - float(low24)) / (float(high24) - float(low24))
        high_times = day_segment.index[day_segment["high"] == high24]
        low_times = day_segment.index[day_segment["low"] == low24]
        row["minutes_since_24h_high"] = int((end_open - high_times[-1]).total_seconds() // 60) if len(high_times) else None
        row["minutes_since_24h_low"] = int((end_open - low_times[-1]).total_seconds() // 60) if len(low_times) else None
    else:
        row["position_in_24h_range"] = None
        row["minutes_since_24h_high"] = None
        row["minutes_since_24h_low"] = None

    last = frame.loc[end_open]
    candle_range = float(last["high"] - last["low"]) if pd.notna(last["high"]) and pd.notna(last["low"]) else 0.0
    row["last_candle_body_fraction"] = abs(float(last["close"] - last["open"])) / candle_range if candle_range > 0 else None
    row["last_candle_upper_wick_fraction"] = (float(last["high"]) - max(float(last["open"]), float(last["close"]))) / candle_range if candle_range > 0 else None
    row["last_candle_lower_wick_fraction"] = (min(float(last["open"]), float(last["close"])) - float(last["low"])) / candle_range if candle_range > 0 else None

    historical_same_time: list[float] = []
    for days_back in range(1, 8):
        historical_end = end_open - pd.Timedelta(days=days_back)
        segment = _segment(frame, historical_end, 15)
        value = segment["quote_volume"].sum(min_count=15)
        if pd.notna(value):
            historical_same_time.append(float(value))
    same_time_median = float(np.median(historical_same_time)) if historical_same_time else None
    row["quote_volume_15m_prior_7d_same_time_median"] = same_time_median
    row["quote_volume_15m_vs_prior_7d_same_time"] = recent15 / same_time_median if recent15 is not None and same_time_median and same_time_median > 0 else None

    prior_minutes = prior_days * 1440
    prior_segment = _segment(frame, end_open, prior_minutes)
    row["missing_minutes_prior_24h"] = int(1440 - day_segment["observed"].sum())
    row["missing_minutes_prior_window"] = int(prior_minutes - prior_segment["observed"].sum())
    row["observed_fraction_prior_window"] = float(prior_segment["observed"].mean()) if len(prior_segment) else 0.0
    observed_before = frame.loc[:end_open, "observed"]
    row["observed_history_days"] = int(math.ceil(float(observed_before.sum()) / 1440.0))
    row["entry_quote_volume_5m"] = recent5
    row["entry_liquidity_pass"] = bool(recent5 is not None and recent5 >= min_entry_notional)

    for symbol, reference in reference_frames.items():
        prefix = symbol.replace("USDT", "").lower()
        row.update(_reference_features(reference, decision_time, prefix))
    btc_return = row.get("btc_ret_60m_pct")
    row["ret_60m_minus_btc_pct_points"] = float(row["ret_60m_pct"]) - float(btc_return) if row.get("ret_60m_pct") is not None and btc_return is not None else None
    btc_return_180 = row.get("btc_ret_180m_pct")
    row["ret_180m_minus_btc_pct_points"] = float(row["ret_180m_pct"]) - float(btc_return_180) if row.get("ret_180m_pct") is not None and btc_return_180 is not None else None
    btc_return_480 = row.get("btc_ret_480m_pct")
    row["ret_480m_minus_btc_pct_points"] = float(row["ret_480m_pct"]) - float(btc_return_480) if row.get("ret_480m_pct") is not None and btc_return_480 is not None else None

    outcome_start = pd.Timestamp(floor_minute(decision_time))
    anchor_open = pd.Timestamp(floor_minute(anchor))
    to_anchor = frame.loc[outcome_start:anchor_open]
    next_8h = frame.loc[outcome_start : outcome_start + pd.Timedelta(minutes=479)]
    for prefix, segment in (("to_anchor", to_anchor), ("next_8h", next_8h)):
        high = segment["high"].max(skipna=True)
        low = segment["low"].min(skipna=True)
        row[f"outcome_{prefix}_max_gain_pct"] = safe_pct(float(high), entry_price) if pd.notna(high) else None
        row[f"outcome_{prefix}_max_drawdown_pct"] = safe_pct(float(low), entry_price) if pd.notna(low) else None
        row[f"outcome_{prefix}_observed_fraction"] = float(segment["observed"].mean()) if len(segment) else 0.0
    if anchor_open in frame.index and pd.notna(frame.at[anchor_open, "close"]):
        row["outcome_anchor_minute_close_pct"] = safe_pct(float(frame.at[anchor_open, "close"]), entry_price)
        row["outcome_anchor_minute_high_pct"] = safe_pct(float(frame.at[anchor_open, "high"]), entry_price)
    else:
        row["outcome_anchor_minute_close_pct"] = None
        row["outcome_anchor_minute_high_pct"] = None

    if row["missing_minutes_prior_24h"] == 0 and row["observed_fraction_prior_window"] >= 0.995:
        row["feature_quality_status"] = "pass"
    elif row["observed_fraction_prior_window"] >= 0.98:
        row["feature_quality_status"] = "warning"
    else:
        row["feature_quality_status"] = "insufficient_history"
    return row


def _candidate_quality(
    frame: pd.DataFrame,
    crossing_mask: pd.Series,
    *,
    anchor: datetime,
    horizons: tuple[int, ...],
    prior_days: int,
    contamination_before_minutes: int,
    contamination_after_minutes: int,
    min_entry_notional: float,
) -> tuple[bool, str, dict[str, Any]]:
    anchor_floor = pd.Timestamp(floor_minute(anchor))
    max_horizon = max(horizons)
    local_start = anchor_floor - pd.Timedelta(minutes=max(max_horizon, contamination_before_minutes))
    local_end = anchor_floor + pd.Timedelta(minutes=contamination_after_minutes)
    if local_start not in frame.index or local_end not in frame.index:
        return False, "outside_loaded_range", {}
    local = frame.loc[local_start:local_end]
    if len(local) != int((local_end - local_start).total_seconds() // 60) + 1 or not bool(local["observed"].all()):
        return False, "missing_local_minutes", {}
    if bool(crossing_mask.loc[local_start:local_end].any()):
        return False, "near_25pct_crossing", {}
    earliest_decision = anchor - timedelta(minutes=max_horizon)
    history_end = pd.Timestamp(floor_minute(earliest_decision) - timedelta(minutes=1))
    history_start = history_end - pd.Timedelta(minutes=prior_days * 1440 - 1)
    history = frame.loc[history_start:history_end]
    expected = prior_days * 1440
    observed_fraction = float(history["observed"].sum() / expected) if expected else 0.0
    if len(history) != expected or observed_fraction < 0.98:
        return False, "insufficient_prior_history", {"observed_fraction": observed_fraction}
    minimum_liquidity = math.inf
    for horizon in horizons:
        decision = anchor - timedelta(minutes=horizon)
        end_open = pd.Timestamp(floor_minute(decision) - timedelta(minutes=1))
        segment = _segment(frame, end_open, 5)
        value = segment["quote_volume"].sum(min_count=5)
        if pd.isna(value):
            return False, "missing_entry_liquidity", {}
        minimum_liquidity = min(minimum_liquidity, float(value))
    if minimum_liquidity < min_entry_notional:
        return False, "below_entry_liquidity_floor", {"minimum_5m_quote_volume": minimum_liquidity}
    return True, "pass", {"observed_fraction": observed_fraction, "minimum_5m_quote_volume": minimum_liquidity}


def _match_tier(offset_minutes: int, weekday_match: bool) -> str:
    distance = abs(offset_minutes)
    if distance == 0 and weekday_match:
        return "exact_clock_same_weekday"
    if distance == 0:
        return "exact_clock"
    if distance <= 15 and weekday_match:
        return "within_15m_same_weekday"
    if distance <= 15:
        return "within_15m"
    if distance <= 30:
        return "within_30m"
    return "within_60m"


def select_controls_for_event(
    *,
    event: dict[str, Any],
    split: str,
    split_dates: set[date],
    frame: pd.DataFrame,
    crossing_mask: pd.Series,
    known_event_anchors: list[datetime],
    controls_per_event: int,
    horizons: tuple[int, ...],
    prior_days: int,
    contamination_before_minutes: int,
    contamination_after_minutes: int,
    min_entry_notional: float,
    used_counts: Counter[tuple[str, datetime]],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    event_anchor = parse_datetime(event.get("first_cross_trade_time") or event["first_cross_time"])
    event_floor = floor_minute(event_anchor)
    offsets = (0, -15, 15, -30, 30, -45, 45, -60, 60)
    candidates: list[dict[str, Any]] = []
    rejection_reasons: Counter[str] = Counter()
    for candidate_day in sorted(split_dates):
        base = datetime.combine(candidate_day, event_floor.timetz().replace(tzinfo=None), tzinfo=timezone.utc)
        base = base.replace(second=event_anchor.second, microsecond=event_anchor.microsecond)
        for offset in offsets:
            anchor = base + timedelta(minutes=offset)
            if anchor.date() not in split_dates:
                continue
            if any(abs((anchor - known).total_seconds()) < 24 * 3600 for known in known_event_anchors):
                rejection_reasons["within_24h_of_known_event"] += 1
                continue
            valid, reason, quality = _candidate_quality(
                frame,
                crossing_mask,
                anchor=anchor,
                horizons=horizons,
                prior_days=prior_days,
                contamination_before_minutes=contamination_before_minutes,
                contamination_after_minutes=contamination_after_minutes,
                min_entry_notional=min_entry_notional,
            )
            if not valid:
                rejection_reasons[reason] += 1
                continue
            key = (str(event["symbol"]), floor_minute(anchor))
            weekday_match = anchor.weekday() == event_anchor.weekday()
            tier = _match_tier(offset, weekday_match)
            tier_rank = {
                "exact_clock_same_weekday": 0,
                "exact_clock": 1,
                "within_15m_same_weekday": 2,
                "within_15m": 3,
                "within_30m": 4,
                "within_60m": 5,
            }[tier]
            candidates.append(
                {
                    "anchor": anchor,
                    "key": key,
                    "offset_minutes": offset,
                    "weekday_match": weekday_match,
                    "tier": tier,
                    "tier_rank": tier_rank,
                    "date_distance_days": abs((anchor.date() - event_anchor.date()).days),
                    "quality": quality,
                    "tie": deterministic_tiebreak(event["id"], anchor.isoformat()),
                }
            )

    # Retain one candidate per UTC date first. This prevents one calm day from
    # supplying several highly overlapping controls for the same event.
    best_by_day: dict[date, dict[str, Any]] = {}
    for candidate in sorted(
        candidates,
        key=lambda row: (
            used_counts[row["key"]] > 0,
            used_counts[row["key"]],
            row["tier_rank"],
            0 if row["weekday_match"] else 1,
            row["date_distance_days"],
            row["tie"],
        ),
    ):
        best_by_day.setdefault(candidate["anchor"].date(), candidate)
    ranked = sorted(
        best_by_day.values(),
        key=lambda row: (
            used_counts[row["key"]] > 0,
            used_counts[row["key"]],
            row["tier_rank"],
            0 if row["weekday_match"] else 1,
            row["date_distance_days"],
            row["tie"],
        ),
    )
    selected = ranked[:controls_per_event]
    result: list[dict[str, Any]] = []
    for rank, candidate in enumerate(selected, start=1):
        prior_reuse = used_counts[candidate["key"]]
        used_counts[candidate["key"]] += 1
        control_id = deterministic_uuid("matched-control-v7-8h", event["id"], candidate["anchor"].isoformat())
        result.append(
            {
                "control_id": control_id,
                "event_id": str(event["id"]),
                "match_group_id": str(event["id"]),
                "symbol": str(event["symbol"]),
                "base_asset": event.get("base_asset"),
                "quote_asset": event.get("quote_asset"),
                "split": split,
                "event_anchor_time": event_anchor.isoformat(),
                "control_anchor_time": candidate["anchor"].isoformat(),
                "control_rank": rank,
                "clock_offset_minutes": candidate["offset_minutes"],
                "calendar_distance_days": candidate["date_distance_days"],
                "weekday_match": candidate["weekday_match"],
                "match_tier": candidate["tier"],
                "prior_global_reuse_count": prior_reuse,
                "minimum_5m_quote_volume": candidate["quality"].get("minimum_5m_quote_volume"),
                "prior_history_observed_fraction": candidate["quality"].get("observed_fraction"),
                "quality_status": "pass" if prior_reuse == 0 else "reused_control_anchor",
            }
        )
    return result, rejection_reasons


def _zip_directory(source: Path, destination: Path) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(source.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(source))


def _json_ready(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


class MatchedControlBuilder:
    """Memory-bounded, resumable matched-control builder for V1.3.

    Only one event-symbol frame is resident at a time. Completed event matches
    are written to Supabase immediately, so a worker restart resumes from the
    first unfinished event rather than rebuilding the full period.
    """

    def __init__(
        self,
        db: SupabaseClient,
        binance: BinanceClient,
        temp_root: Path,
        *,
        minimum_disk_free_bytes: int = 750_000_000,
    ):
        self.db = db
        self.binance = binance
        self.temp_root = temp_root
        self.minimum_disk_free_bytes = minimum_disk_free_bytes
        self.cache = MinuteArchiveCache(binance, temp_root)

    @staticmethod
    def _match_payload(job_id: str, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "matched_control_job_id": job_id,
            "event_id": row["event_id"],
            "control_id": row["control_id"],
            "symbol": row["symbol"],
            "split": row["split"],
            "event_anchor_time": row["event_anchor_time"],
            "control_anchor_time": row["control_anchor_time"],
            "control_rank": row["control_rank"],
            "clock_offset_minutes": row["clock_offset_minutes"],
            "calendar_distance_days": row["calendar_distance_days"],
            "weekday_match": row["weekday_match"],
            "match_tier": row["match_tier"],
            "prior_global_reuse_count": row["prior_global_reuse_count"],
            "minimum_5m_quote_volume": row["minimum_5m_quote_volume"],
            "prior_history_observed_fraction": row["prior_history_observed_fraction"],
            "quality_status": row["quality_status"],
        }

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["id"])
        scan_id = str(job["scan_id"])
        controls_per_event = int(job.get("controls_per_event") or 5)
        prior_days = int(job.get("prior_days") or 10)
        horizons = tuple(sorted({int(value) for value in (job.get("horizons_minutes") or [480])}))
        contamination_before = int(job.get("contamination_before_minutes") or max(horizons))
        contamination_after = int(job.get("contamination_after_minutes") or 480)
        min_entry_notional = float(job.get("min_entry_notional") or 500)
        research_purpose = str(job.get("research_purpose") or "")
        if research_purpose != "external_validation_c2_c4":
            raise ValueError("V1.3 processes only the frozen C2/C4 external-validation workflow")
        if controls_per_event != 5 or prior_days != 10 or 480 not in horizons:
            raise ValueError("External validation requires five controls, ten history days and horizon 480")

        events = self.db.select_all(
            "binance_gainer_events",
            filters={"scan_id": f"eq.{scan_id}", "sellability_pass": "eq.true"},
            order="event_date.asc,symbol.asc,id.asc",
        )
        if not events:
            raise RuntimeError("The selected scan has no saleable events")
        scans = self.db.select("binance_scan_jobs", filters={"id": f"eq.{scan_id}"}, limit=1)
        if not scans:
            raise RuntimeError("Source scan not found")
        scan = scans[0]
        if (
            scan.get("event_definition_version") != "v1_25pct_rolling_8h"
            or int(scan.get("window_minutes") or 0) != 480
            or float(scan.get("threshold_pct") or 0) != 25.0
            or str(scan.get("research_purpose") or "") != "external_validation_c2_c4"
        ):
            raise ValueError("Source scan is not the locked 25% external-validation scan")

        result_json = scan.get("result_json") or {}
        if result_json.get("window_start") and result_json.get("window_end_exclusive"):
            scan_start = parse_datetime(result_json["window_start"]).date()
            scan_end = parse_datetime(result_json["window_end_exclusive"]).date()
        else:
            event_dates = [date.fromisoformat(str(row["event_date"])) for row in events]
            scan_start = min(event_dates)
            scan_end = max(event_dates) + timedelta(days=1)
        load_start = scan_start - timedelta(days=prior_days + math.ceil(max(horizons) / 1440) + 1)
        load_end = scan_end
        split_dates = {"external_validation": set()}
        cursor = scan_start
        while cursor < scan_end:
            split_dates["external_validation"].add(cursor)
            cursor += timedelta(days=1)
        for event in events:
            event["split"] = "external_validation"

        existing_progress = self.db.select_all(
            "binance_matched_control_progress",
            filters={"matched_control_job_id": f"eq.{job_id}"},
        )
        completed_event_ids = {
            str(row["event_id"]) for row in existing_progress if row.get("status") == "completed"
        }
        existing_matches = self.db.select_all(
            "binance_control_matches",
            filters={"matched_control_job_id": f"eq.{job_id}"},
            order="event_anchor_time.asc,control_rank.asc",
        )
        # A hard process kill can occur after controls were upserted but before
        # the event checkpoint row was written. Remove only those uncommitted
        # event matches so a resume cannot silently create more than five controls
        # for one event or distort global reuse counts. Completed event matches
        # remain untouched.
        partial_event_ids = {
            str(row["event_id"])
            for row in existing_matches
            if str(row["event_id"]) not in completed_event_ids
        }
        for partial_event_id in sorted(partial_event_ids):
            self.db.delete(
                "binance_control_matches",
                {
                    "matched_control_job_id": f"eq.{job_id}",
                    "event_id": f"eq.{partial_event_id}",
                },
            )
        if partial_event_ids:
            existing_matches = [
                row for row in existing_matches if str(row["event_id"]) not in partial_event_ids
            ]

        used_counts: Counter[tuple[str, datetime]] = Counter()
        for row in existing_matches:
            used_counts[(str(row["symbol"]), floor_minute(parse_datetime(row["control_anchor_time"])))] += 1

        controls_created = len(existing_matches)
        processed = len(completed_event_ids)
        checkpoint = dict(job.get("checkpoint_json") or {})
        checkpoint.update(
            {
                "schema_version": 1,
                "phase": "matched_controls",
                "events_total": len(events),
                "events_completed": processed,
                "controls_created": controls_created,
            }
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        self.db.update(
            "binance_matched_control_jobs",
            {"id": f"eq.{job_id}"},
            {
                "events_total": len(events),
                "controls_target": len(events) * controls_per_event,
                "events_processed": processed,
                "controls_created": controls_created,
                "feature_rows": 0,
                "heartbeat_at": now_iso,
                "checkpoint_json": checkpoint,
                "last_stage": "matched_controls",
                "last_checkpoint_at": now_iso,
            },
        )
        log_resources(
            "matched_controls_resume",
            path=self.temp_root,
            extra={"events_completed": processed, "events_total": len(events)},
        )

        events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in events:
            if str(event["id"]) not in completed_event_ids:
                events_by_symbol[str(event["symbol"])].append(event)

        for symbol, pending_events in sorted(events_by_symbol.items()):
            ensure_disk_headroom(self.temp_root, self.minimum_disk_free_bytes)
            loaded: LoadedSymbol | None = None
            frame: pd.DataFrame | None = None
            crossing_mask: pd.Series | None = None
            try:
                loaded = self.cache.load_symbol(
                    symbol,
                    load_start,
                    load_end,
                    required_columns=("high", "low", "close", "quote_volume"),
                )
                frame = loaded.frame
                crossing_mask = rolling_crossing_mask(
                    frame,
                    threshold_pct=25.0,
                    window_minutes=480,
                )
                all_symbol_events = [row for row in events if str(row["symbol"]) == symbol]
                known_anchors = [
                    parse_datetime(row.get("first_cross_trade_time") or row["first_cross_time"])
                    for row in all_symbol_events
                ]
                for event in pending_events:
                    event_id = str(event["id"])
                    controls, rejected = select_controls_for_event(
                        event=event,
                        split="external_validation",
                        split_dates=split_dates["external_validation"],
                        frame=frame,
                        crossing_mask=crossing_mask,
                        known_event_anchors=known_anchors,
                        controls_per_event=controls_per_event,
                        horizons=horizons,
                        prior_days=prior_days,
                        contamination_before_minutes=contamination_before,
                        contamination_after_minutes=contamination_after,
                        min_entry_notional=min_entry_notional,
                        used_counts=used_counts,
                    )
                    for row in controls:
                        row["matched_control_job_id"] = job_id
                    if controls:
                        self.db.upsert(
                            "binance_control_matches",
                            [self._match_payload(job_id, row) for row in controls],
                            on_conflict="matched_control_job_id,control_id",
                        )
                    self.db.upsert(
                        "binance_matched_control_progress",
                        [
                            {
                                "matched_control_job_id": job_id,
                                "event_id": event_id,
                                "symbol": symbol,
                                "status": "completed",
                                "controls_created": len(controls),
                                "rejection_json": dict(rejected),
                                "error_message": None,
                                "completed_at": datetime.now(timezone.utc).isoformat(),
                            }
                        ],
                        on_conflict="matched_control_job_id,event_id",
                    )
                    if len(controls) < controls_per_event:
                        self.db.insert(
                            "binance_matched_control_issues",
                            {
                                "matched_control_job_id": job_id,
                                "event_id": event_id,
                                "stage": "control_selection",
                                "message": f"Created {len(controls)} of {controls_per_event} requested controls",
                            },
                        )
                    completed_event_ids.add(event_id)
                    processed += 1
                    controls_created += len(controls)
                    checkpoint.update(
                        {
                            "phase": "matched_controls",
                            "last_symbol": symbol,
                            "last_event_id": event_id,
                            "events_completed": processed,
                            "controls_created": controls_created,
                        }
                    )
                    now_iso = datetime.now(timezone.utc).isoformat()
                    self.db.update(
                        "binance_matched_control_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "events_processed": processed,
                            "controls_created": controls_created,
                            "feature_rows": 0,
                            "heartbeat_at": now_iso,
                            "checkpoint_json": checkpoint,
                            "last_stage": "matched_controls",
                            "last_unit": f"{symbol}:{event_id}",
                            "last_checkpoint_at": now_iso,
                        },
                    )
            except Exception as exc:
                self.db.insert(
                    "binance_matched_control_issues",
                    {
                        "matched_control_job_id": job_id,
                        "event_id": None,
                        "stage": f"symbol:{symbol}",
                        "message": str(exc)[:4000],
                    },
                )
                raise
            finally:
                del crossing_mask
                del frame
                del loaded
                collect_memory()
                log_resources(
                    "matched_controls_symbol_checkpoint",
                    path=self.temp_root,
                    extra={"symbol": symbol, "events_completed": processed},
                )

        progress_rows = self.db.select_all(
            "binance_matched_control_progress",
            filters={"matched_control_job_id": f"eq.{job_id}"},
        )
        final_matches = self.db.select_all(
            "binance_control_matches",
            filters={"matched_control_job_id": f"eq.{job_id}"},
        )
        completed_rows = [row for row in progress_rows if row.get("status") == "completed"]
        controls_created = len(final_matches)
        events_without_full = sum(
            1 for row in completed_rows if int(row.get("controls_created") or 0) < controls_per_event
        )
        rejection_totals: Counter[str] = Counter()
        for row in completed_rows:
            rejection_totals.update(row.get("rejection_json") or {})
        checkpoint.update(
            {
                "phase": "complete",
                "events_completed": len(completed_rows),
                "controls_created": controls_created,
            }
        )
        self.db.update(
            "binance_matched_control_jobs",
            {"id": f"eq.{job_id}"},
            {
                "events_processed": len(completed_rows),
                "controls_created": controls_created,
                "feature_rows": 0,
                "checkpoint_json": checkpoint,
                "last_stage": "complete",
                "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return {
            "events_total": len(events),
            "events_processed": len(completed_rows),
            "controls_target": len(events) * controls_per_event,
            "controls_created": controls_created,
            "feature_rows": 0,
            "failures": 0,
            "research_purpose": research_purpose,
            "quality_report": {
                "events_total": len(events),
                "controls_target": len(events) * controls_per_event,
                "controls_created": controls_created,
                "control_completion_pct": (
                    100.0 * controls_created / (len(events) * controls_per_event)
                    if events else 0.0
                ),
                "events_without_full_control_count": events_without_full,
                "control_anchor_reuse_count": int(
                    sum(1 for row in final_matches if int(row.get("prior_global_reuse_count") or 0) > 0)
                ),
                "control_rejection_reasons": dict(rejection_totals),
                "memory_model": "one symbol frame at a time; no external-validation feature matrix built here",
                "resume_model": "event-level Supabase checkpoints",
            },
        }

