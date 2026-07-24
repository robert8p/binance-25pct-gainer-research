from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


def pct_change(new: float, old: float) -> float | None:
    if old <= 0:
        return None
    return (new / old - 1.0) * 100.0


def classify_symbol(raw: dict[str, Any]) -> dict[str, Any]:
    symbol = str(raw.get("symbol", ""))
    base = str(raw.get("baseAsset", ""))
    permissions = raw.get("permissions") or []
    permission_sets = raw.get("permissionSets") or []
    has_spot = "SPOT" in permissions or any("SPOT" in group for group in permission_sets)
    stablecoin_like = base in {
        "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "GBP", "TRY", "BRL", "AUD",
    }
    leveraged_like = base.endswith(("UP", "DOWN", "BULL", "BEAR"))
    return {
        "symbol": symbol,
        "base_asset": base,
        "quote_asset": str(raw.get("quoteAsset", "")),
        "status": str(raw.get("status", "")),
        "spot_permission": has_spot,
        "is_spot_trading_allowed": bool(raw.get("isSpotTradingAllowed", has_spot)),
        "order_types": raw.get("orderTypes") or [],
        "base_precision": raw.get("baseAssetPrecision"),
        "quote_precision": raw.get("quoteAssetPrecision"),
        "stablecoin_like": stablecoin_like,
        "leveraged_token_like": leveraged_like,
        "raw_json": raw,
    }


def parse_kline(row: list[Any], symbol: str, interval: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "interval": interval,
        "open_time": datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc).isoformat(),
        "close_time": datetime.fromtimestamp(int(row[6]) / 1000, tz=timezone.utc).isoformat(),
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
        "quote_volume": float(row[7]),
        "trade_count": int(row[8]),
        "taker_buy_base_volume": float(row[9]),
        "taker_buy_quote_volume": float(row[10]),
    }



def first_rolling_surge(
    minute_rows: list[dict[str, Any]],
    *,
    event_day_start: datetime,
    event_day_end: datetime,
    threshold_pct: float,
    window_minutes: int,
) -> dict[str, Any] | None:
    """Return the earliest conservatively ordered rolling-window surge.

    The baseline is the lowest price in a *prior* one-minute bar. The crossing
    is a later minute whose high reaches the configured percentage above that
    baseline. The allowed baseline-minute-open gap is window_minutes - 1, so
    even the worst-case ordering inside the two bars remains within the stated
    wall-clock window. Same-minute low-to-high moves are intentionally excluded
    because their trade ordering cannot be proved from a kline alone.
    """
    if threshold_pct <= 0 or window_minutes < 2:
        raise ValueError("threshold_pct must be positive and window_minutes must be at least 2")
    rows = sorted(minute_rows, key=lambda row: datetime.fromisoformat(row["open_time"]))
    factor = 1.0 + threshold_pct / 100.0
    minima: deque[tuple[datetime, float, dict[str, Any]]] = deque()
    conservative_span = timedelta(minutes=window_minutes - 1)

    for row in rows:
        current_time = datetime.fromisoformat(row["open_time"])
        earliest = current_time - conservative_span
        while minima and minima[0][0] < earliest:
            minima.popleft()

        if event_day_start <= current_time < event_day_end and minima:
            baseline_time, baseline_price, baseline_row = minima[0]
            threshold_price = baseline_price * factor
            if float(row["high"]) + 1e-15 >= threshold_price:
                return {
                    "baseline_row": baseline_row,
                    "baseline_time": baseline_time,
                    "baseline_price": baseline_price,
                    "crossing_row": row,
                    "crossing_time": current_time,
                    "threshold_price": threshold_price,
                    "minutes_baseline_open_to_cross_open": int(
                        (current_time - baseline_time).total_seconds() // 60
                    ),
                }

        low = float(row["low"])
        while minima and minima[-1][1] >= low:
            minima.pop()
        minima.append((current_time, low, row))

    return None

def decision_observations(minute_rows: list[dict[str, Any]], event_day: str) -> list[dict[str, Any]]:
    london = ZoneInfo("Europe/London")
    result: list[dict[str, Any]] = []
    if not minute_rows:
        return result
    for hour in (14, 17, 19):
        local = datetime.fromisoformat(f"{event_day}T{hour:02d}:00:00").replace(tzinfo=london)
        target = local.astimezone(timezone.utc)
        eligible = [r for r in minute_rows if datetime.fromisoformat(r["open_time"]) >= target]
        if not eligible:
            continue
        entry = eligible[0]
        entry_close_time = datetime.fromisoformat(entry["close_time"])
        later = [r for r in minute_rows if datetime.fromisoformat(r["open_time"]) >= entry_close_time]
        if not later:
            continue
        peak = max(later, key=lambda r: r["high"])
        result.append(
            {
                "decision_label": f"{hour:02d}:00 Europe/London",
                "decision_time_utc": target.isoformat(),
                "entry_time_utc": entry["close_time"],
                "entry_close": entry["close"],
                "subsequent_high": peak["high"],
                "subsequent_high_time_utc": peak["open_time"],
                "max_gain_pct": pct_change(peak["high"], entry["close"]),
            }
        )
    return result
