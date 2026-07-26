from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable


@dataclass(frozen=True)
class DetectionResult:
    qualifies: bool
    threshold_pct: float
    prior_close: float
    threshold_price: float
    session_open: float
    session_high: float
    session_low: float
    session_close: float
    session_volume: int
    session_trade_count: int
    opening_gap_pct: float
    high_vs_prior_close_pct: float
    open_to_peak_pct: float
    first_minute_close: float
    first_minute_entry_to_peak_pct: float
    threshold_cross_bar_start: datetime | None
    peak_bar_start: datetime | None
    peak_price: float
    minutes_from_open_to_cross: int | None
    minutes_from_open_to_peak: int | None
    first_bar_volume: int
    first_bar_trade_count: int
    peak_bar_volume: int
    peak_bar_trade_count: int
    max_missing_bar_gap_minutes: int
    quality_flags: list[str]


def _f(bar: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = bar.get(key, default)
    return float(value) if value is not None else default


def _i(bar: dict[str, Any], key: str, default: int = 0) -> int:
    value = bar.get(key, default)
    return int(value) if value is not None else default


def detect_regular_session_gainer(
    bars: Iterable[dict[str, Any]],
    *,
    prior_close: float,
    threshold_pct: float,
    market_open: datetime,
    market_close: datetime,
) -> DetectionResult | None:
    """Evaluate a single symbol/session using regular-session one-minute bars.

    Primary criterion: regular-session high >= prior regular-session close * (1 + threshold).
    The first-minute close metric is an executable proxy, not a claim about achievable fills.
    """
    if prior_close <= 0:
        return None

    regular = sorted(
        [
            b
            for b in bars
            if market_open <= b["timestamp"] < market_close
            and _f(b, "open") > 0
            and _f(b, "high") > 0
            and _f(b, "low") > 0
            and _f(b, "close") > 0
        ],
        key=lambda b: b["timestamp"],
    )
    if not regular:
        return None

    first = regular[0]
    peak = max(regular, key=lambda b: _f(b, "high"))
    threshold_price = prior_close * (1.0 + threshold_pct / 100.0)
    cross = next((b for b in regular if _f(b, "high") >= threshold_price), None)

    session_open = _f(first, "open")
    session_high = _f(peak, "high")
    session_low = min(_f(b, "low") for b in regular)
    session_close = _f(regular[-1], "close")
    session_volume = sum(_i(b, "volume") for b in regular)
    session_trade_count = sum(_i(b, "trade_count") for b in regular)
    first_close = _f(first, "close")

    gaps: list[int] = []
    for left, right in zip(regular, regular[1:]):
        delta = int((right["timestamp"] - left["timestamp"]).total_seconds() // 60)
        if delta > 1:
            gaps.append(delta - 1)
    max_gap = max(gaps, default=0)

    flags: list[str] = []
    if first["timestamp"] > market_open:
        flags.append("no_trade_at_official_open")
    if _i(first, "trade_count") <= 1:
        flags.append("thin_opening_bar")
    if _i(peak, "trade_count") <= 1:
        flags.append("single_or_thin_peak_print")
    if max_gap >= 5:
        flags.append("possible_halt_or_illiquidity")
    if session_open < 1:
        flags.append("sub_dollar_at_open")
    if session_volume < 100_000:
        flags.append("low_session_volume")

    def pct(numerator: float, denominator: float) -> float:
        return ((numerator / denominator) - 1.0) * 100.0 if denominator > 0 else 0.0

    cross_minutes = (
        int((cross["timestamp"] - market_open).total_seconds() // 60) if cross else None
    )
    peak_minutes = int((peak["timestamp"] - market_open).total_seconds() // 60)

    return DetectionResult(
        qualifies=cross is not None,
        threshold_pct=threshold_pct,
        prior_close=prior_close,
        threshold_price=threshold_price,
        session_open=session_open,
        session_high=session_high,
        session_low=session_low,
        session_close=session_close,
        session_volume=session_volume,
        session_trade_count=session_trade_count,
        opening_gap_pct=pct(session_open, prior_close),
        high_vs_prior_close_pct=pct(session_high, prior_close),
        open_to_peak_pct=pct(session_high, session_open),
        first_minute_close=first_close,
        first_minute_entry_to_peak_pct=pct(session_high, first_close),
        threshold_cross_bar_start=cross["timestamp"] if cross else None,
        peak_bar_start=peak["timestamp"],
        peak_price=session_high,
        minutes_from_open_to_cross=cross_minutes,
        minutes_from_open_to_peak=peak_minutes,
        first_bar_volume=_i(first, "volume"),
        first_bar_trade_count=_i(first, "trade_count"),
        peak_bar_volume=_i(peak, "volume"),
        peak_bar_trade_count=_i(peak, "trade_count"),
        max_missing_bar_gap_minutes=max_gap,
        quality_flags=flags,
    )
