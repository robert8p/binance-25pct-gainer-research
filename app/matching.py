from __future__ import annotations

import hashlib
import math
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

UTC = timezone.utc
MATCH_FEATURES = (
    "log_prior_close",
    "log_median_dollar_volume_10",
    "realized_vol_10",
    "atr_pct_10",
    "prior_day_return",
    "momentum_10",
    "log_listing_sessions",
)
MATCH_WEIGHTS = {
    "log_prior_close": 2.0,
    "log_median_dollar_volume_10": 2.0,
    "realized_vol_10": 2.0,
    "atr_pct_10": 1.25,
    "prior_day_return": 1.0,
    "momentum_10": 1.0,
    "log_listing_sessions": 0.5,
}
PRICE_BANDS = ("lt_0_5", "0_5_1", "1_2", "2_5", "5_10", "10_20", "20_50", "gte_50")
PRICE_BAND_INDEX = {name: idx for idx, name in enumerate(PRICE_BANDS)}


@dataclass(frozen=True)
class MatchConfig:
    controls_per_event: int = 5
    max_control_symbol_uses: int = 20

    # V3.0.2 ranks every same-date candidate globally. Exchange is a soft
    # penalty, never a first-pass silo that can force a poor control. The two
    # legacy fields remain accepted so queued V3.0.x jobs can still be read.
    exact_exchange_first: bool = False
    allow_exchange_fallback: bool = True
    exchange_mismatch_penalty: float = 1.0

    # Broad absolute calipers prevent clearly incomparable stock-days before
    # robust standardisation. The tighter balance gates below decide whether a
    # candidate is strong enough to download.
    price_ratio_min: float = 0.50
    price_ratio_max: float = 2.0
    dollar_volume_ratio_min: float = 0.20
    dollar_volume_ratio_max: float = 5.0
    volatility_ratio_min: float = 0.50
    volatility_ratio_max: float = 2.0
    max_price_band_distance: int = 1
    require_corporate_action_match: bool = True
    corporate_action_mismatch_penalty: float = 4.0

    # No weak controls are packaged. A selected control must satisfy every
    # feature-specific limit and have an overall distance no worse than good.
    max_match_score: float = 4.0
    max_abs_log_prior_close_z: float = 1.5
    max_abs_log_median_dollar_volume_z: float = 1.5
    max_abs_realized_vol_z: float = 2.0
    max_abs_atr_pct_z: float = 2.0
    max_abs_prior_day_return_z: float = 2.0
    max_abs_momentum_z: float = 2.0
    max_abs_log_listing_sessions_z: float = 2.0


def _f(value: Any, default: float = 0.0) -> float:
    try:
        n = float(value)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ts_date(raw: dict[str, Any]) -> date:
    text = str(raw.get("t") or raw.get("timestamp") or "")
    if not text:
        raise ValueError("Bar is missing a timestamp")
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).date()


def _median(values: Iterable[float], default: float = 0.0) -> float:
    rows = [float(v) for v in values if math.isfinite(float(v))]
    return statistics.median(rows) if rows else default


def price_band(price: float) -> str:
    boundaries = (0.5, 1, 2, 5, 10, 20, 50)
    labels = ("lt_0_5", "0_5_1", "1_2", "2_5", "5_10", "10_20", "20_50", "gte_50")
    for idx, boundary in enumerate(boundaries):
        if price < boundary:
            return labels[idx]
    return labels[-1]


def compute_symbol_date_features(
    symbol: str,
    exchange: str | None,
    bars: list[dict[str, Any]],
    event_dates: set[date],
    *,
    threshold_pct: float,
    feature_sessions: int = 10,
    corporate_action_dates: set[date] | None = None,
) -> list[dict[str, Any]]:
    """Create point-in-time daily features using only bars strictly before each event date.

    Bars are expected to be split-adjusted daily bars. The event-day high is used only to
    label an unambiguous negative control (high below the threshold); it is never included
    in the matching features.
    """
    corporate_action_dates = corporate_action_dates or set()
    clean = []
    for raw in bars:
        try:
            day = _ts_date(raw)
        except ValueError:
            continue
        clean.append(
            {
                "date": day,
                "open": _f(raw.get("o") if "o" in raw else raw.get("open")),
                "high": _f(raw.get("h") if "h" in raw else raw.get("high")),
                "low": _f(raw.get("l") if "l" in raw else raw.get("low")),
                "close": _f(raw.get("c") if "c" in raw else raw.get("close")),
                "volume": _i(raw.get("v") if "v" in raw else raw.get("volume")),
            }
        )
    clean.sort(key=lambda row: row["date"])
    by_date = {row["date"]: idx for idx, row in enumerate(clean)}
    output: list[dict[str, Any]] = []
    threshold_multiple = 1.0 + threshold_pct / 100.0

    for event_date in sorted(event_dates):
        idx = by_date.get(event_date)
        if idx is None or idx < max(3, feature_sessions):
            continue
        event_bar = clean[idx]
        prior = clean[:idx]
        window = prior[-feature_sessions:]
        if len(window) < feature_sessions:
            continue
        prior_close = window[-1]["close"]
        if prior_close <= 0 or event_bar["high"] <= 0:
            continue
        closes = [r["close"] for r in window]
        if any(v <= 0 for v in closes):
            continue
        returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
        realized_vol = statistics.stdev(returns) if len(returns) >= 2 else 0.0
        atr_values = []
        preceding = prior[-(feature_sessions + 1):]
        for i in range(1, len(preceding)):
            prev_close = preceding[i - 1]["close"]
            if prev_close > 0:
                row = preceding[i]
                true_range = max(
                    row["high"] - row["low"],
                    abs(row["high"] - prev_close),
                    abs(row["low"] - prev_close),
                )
                atr_values.append(true_range / prev_close)
        dollar_volumes = [max(0.0, r["close"] * r["volume"]) for r in window]
        volumes = [max(0, r["volume"]) for r in window]
        prior_day_return = closes[-1] / closes[-2] - 1.0 if len(closes) >= 2 else 0.0
        momentum_10 = closes[-1] / closes[0] - 1.0 if closes[0] > 0 else 0.0
        action_window_start = event_date.fromordinal(event_date.toordinal() - 45)
        has_action = any(action_window_start <= d <= event_date for d in corporate_action_dates)
        event_high_pct = (event_bar["high"] / prior_close - 1.0) * 100.0
        median_dollar_volume = _median(dollar_volumes)
        listing_sessions = len(prior)
        output.append(
            {
                "symbol": symbol,
                "exchange": exchange or "UNKNOWN",
                "event_date": event_date.isoformat(),
                "prior_close": prior_close,
                "event_high": event_bar["high"],
                "event_high_vs_prior_close_pct": event_high_pct,
                "no_threshold_hit": event_bar["high"] < prior_close * threshold_multiple,
                "median_dollar_volume_10": median_dollar_volume,
                "median_volume_10": _median(volumes),
                "realized_vol_10": realized_vol,
                "atr_pct_10": _median(atr_values),
                "prior_day_return": prior_day_return,
                "momentum_10": momentum_10,
                "listing_sessions_observed": listing_sessions,
                "corporate_action_45d": has_action,
                "price_band": price_band(prior_close),
                "log_prior_close": math.log(max(prior_close, 1e-9)),
                "log_median_dollar_volume_10": math.log(max(median_dollar_volume, 1.0)),
                "log_listing_sessions": math.log(max(listing_sessions, 1)),
                "feature_cutoff_date": window[-1]["date"].isoformat(),
            }
        )
    return output


def classify_positive_tier(event: dict[str, Any]) -> str:
    flags = event.get("quality_flags") or []
    if isinstance(flags, str):
        text = flags.strip()
        if text.startswith("["):
            try:
                import json

                flags = json.loads(text)
            except Exception:
                flags = [text]
        elif text:
            flags = [text]
        else:
            flags = []
    adjustment_scale = _f(event.get("adjustment_scale"), 1.0)
    disqualifying_tokens = (
        "unverified",
        "unavailable",
        "truncated",
        "missing",
        "recovered",
        "error",
    )
    if not 0.9 <= adjustment_scale <= 1.1:
        return "extended"
    if any(any(token in str(flag).lower() for token in disqualifying_tokens) for flag in flags):
        return "extended"
    return "primary_clean"


def _robust_centres(candidates: list[dict[str, Any]]) -> dict[str, tuple[float, float]]:
    output: dict[str, tuple[float, float]] = {}
    for key in MATCH_FEATURES:
        values = [_f(row.get(key)) for row in candidates]
        centre = _median(values)
        deviations = [abs(v - centre) for v in values]
        mad = _median(deviations)
        # 1.4826 scales MAD to a standard-deviation analogue for a normal sample.
        scale = max(mad * 1.4826, 1e-6)
        output[key] = (centre, scale)
    return output


def _price_band_distance(event: dict[str, Any], control: dict[str, Any]) -> int:
    event_band = str(event.get("price_band") or price_band(_f(event.get("prior_close"))))
    control_band = str(control.get("price_band") or price_band(_f(control.get("prior_close"))))
    return abs(PRICE_BAND_INDEX.get(event_band, 999) - PRICE_BAND_INDEX.get(control_band, 999))


def _absolute_caliper_failures(
    event: dict[str, Any], control: dict[str, Any], cfg: MatchConfig
) -> list[str]:
    failures: list[str] = []
    ep = max(_f(event.get("prior_close")), 1e-9)
    cp = max(_f(control.get("prior_close")), 1e-9)
    price_ratio = cp / ep
    if not cfg.price_ratio_min <= price_ratio <= cfg.price_ratio_max:
        failures.append("price_ratio_outside_caliper")

    edv = max(_f(event.get("median_dollar_volume_10")), 1.0)
    cdv = max(_f(control.get("median_dollar_volume_10")), 1.0)
    if not cfg.dollar_volume_ratio_min <= cdv / edv <= cfg.dollar_volume_ratio_max:
        failures.append("dollar_volume_ratio_outside_caliper")

    ev = max(_f(event.get("realized_vol_10")), 1e-5)
    cv = max(_f(control.get("realized_vol_10")), 1e-5)
    if not cfg.volatility_ratio_min <= cv / ev <= cfg.volatility_ratio_max:
        failures.append("volatility_ratio_outside_caliper")

    if _price_band_distance(event, control) > cfg.max_price_band_distance:
        failures.append("price_band_not_same_or_adjacent")

    if (
        cfg.require_corporate_action_match
        and bool(control.get("corporate_action_45d")) != bool(event.get("corporate_action_45d"))
    ):
        failures.append("corporate_action_status_mismatch")
    return failures


def _distance(
    event: dict[str, Any],
    control: dict[str, Any],
    centres: dict[str, tuple[float, float]],
    cfg: MatchConfig,
) -> tuple[float, dict[str, float]]:
    deltas: dict[str, float] = {}
    score = 0.0
    for key in MATCH_FEATURES:
        _, scale = centres[key]
        delta = (_f(control.get(key)) - _f(event.get(key))) / scale
        deltas[key] = delta
        score += MATCH_WEIGHTS[key] * delta * delta
    if str(control.get("exchange")) != str(event.get("exchange")):
        score += cfg.exchange_mismatch_penalty
        deltas["exchange_mismatch"] = 1.0
    else:
        deltas["exchange_mismatch"] = 0.0
    if bool(control.get("corporate_action_45d")) != bool(event.get("corporate_action_45d")):
        score += cfg.corporate_action_mismatch_penalty
        deltas["corporate_action_mismatch"] = 1.0
    else:
        deltas["corporate_action_mismatch"] = 0.0
    return math.sqrt(score), deltas


def _balance_failures(score: float, deltas: dict[str, float], cfg: MatchConfig) -> list[str]:
    limits = {
        "log_prior_close": cfg.max_abs_log_prior_close_z,
        "log_median_dollar_volume_10": cfg.max_abs_log_median_dollar_volume_z,
        "realized_vol_10": cfg.max_abs_realized_vol_z,
        "atr_pct_10": cfg.max_abs_atr_pct_z,
        "prior_day_return": cfg.max_abs_prior_day_return_z,
        "momentum_10": cfg.max_abs_momentum_z,
        "log_listing_sessions": cfg.max_abs_log_listing_sessions_z,
    }
    failures = [f"{key}_standardized_delta_too_large" for key, limit in limits.items() if abs(_f(deltas.get(key))) > limit]
    if score > cfg.max_match_score:
        failures.append("overall_match_score_too_large")
    return failures


def _tie_break(event_id: str, control_symbol: str) -> str:
    return hashlib.sha256(f"{event_id}|{control_symbol}".encode()).hexdigest()


def match_controls_for_date(
    events: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    *,
    cfg: MatchConfig,
    global_symbol_uses: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Deterministic global nearest-neighbour matching with hard balance gates.

    V3.0.2 never forces the requested number of controls. It returns between zero
    and ``controls_per_event`` controls, and every returned pair is excellent or
    good under the configured thresholds. The second return value contains a
    detailed shortfall diagnostic for each event that received fewer controls.
    """
    global_symbol_uses = global_symbol_uses if global_symbol_uses is not None else {}
    eligible_candidates = [row for row in candidates if bool(row.get("no_threshold_hit"))]
    if not eligible_candidates:
        return [], [{
            "event": event,
            "reason": "no_unambiguous_negative_candidates",
            "selected_count": 0,
            "requested_count": cfg.controls_per_event,
            "candidate_count": 0,
            "rejection_counts": {},
            "nearest_rejected": [],
        } for event in events]

    centres = _robust_centres(eligible_candidates)
    positive_symbols = {str(event.get("symbol")) for event in events}
    used_stock_days: set[tuple[str, str]] = set()
    pairs: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    # Match the hardest tail observations first, reducing greedy allocation bias.
    def extremeness(event: dict[str, Any]) -> float:
        return sum(abs((_f(event.get(key)) - centres[key][0]) / centres[key][1]) for key in MATCH_FEATURES)

    for event in sorted(events, key=extremeness, reverse=True):
        event_id = str(event.get("research_event_id") or event.get("id") or event.get("source_result_id"))
        accepted: list[tuple[float, str, dict[str, Any], dict[str, float]]] = []
        rejected: list[tuple[float, str, dict[str, Any], dict[str, float], list[str]]] = []
        rejection_counts: dict[str, int] = {}

        def reject(reason: str) -> None:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

        for control in eligible_candidates:
            symbol = str(control.get("symbol"))
            key = (symbol, str(control.get("event_date")))
            exclusion_reasons: list[str] = []
            if symbol in positive_symbols:
                exclusion_reasons.append("positive_symbol_excluded")
            if key in used_stock_days:
                exclusion_reasons.append("stock_day_already_used")
            if global_symbol_uses.get(symbol, 0) >= cfg.max_control_symbol_uses:
                exclusion_reasons.append("symbol_reuse_limit_reached")
            if exclusion_reasons:
                for reason in exclusion_reasons:
                    reject(reason)
                continue

            absolute_failures = _absolute_caliper_failures(event, control, cfg)
            score, deltas = _distance(event, control, centres, cfg)
            balance_failures = _balance_failures(score, deltas, cfg)
            failures = absolute_failures + balance_failures
            tie = _tie_break(event_id, symbol)
            if failures:
                for reason in failures:
                    reject(reason)
                rejected.append((score, tie, control, deltas, failures))
                continue
            accepted.append((score, tie, control, deltas))

        accepted.sort(key=lambda row: (row[0], row[1]))
        selected = accepted[: cfg.controls_per_event]
        for rank, (score, _, control, deltas) in enumerate(selected, start=1):
            symbol = str(control["symbol"])
            used_stock_days.add((symbol, str(control["event_date"])))
            global_symbol_uses[symbol] = global_symbol_uses.get(symbol, 0) + 1
            quality = "excellent" if score <= 2.0 else "good"
            pairs.append({
                "positive_research_event_id": event.get("research_event_id") or event.get("id"),
                "positive_source_result_id": event.get("source_result_id"),
                "positive_symbol": event.get("symbol"),
                "event_date": event.get("event_date"),
                "positive_tier": event.get("positive_tier") or classify_positive_tier(event),
                "control_symbol": symbol,
                "control_exchange": control.get("exchange"),
                "control_rank": rank,
                "match_score": score,
                "match_quality": quality,
                "pseudo_event_timestamp": event.get("exact_cross_timestamp"),
                "pseudo_event_timestamp_raw": event.get("exact_cross_timestamp_raw") or event.get("exact_cross_timestamp"),
                "positive_features": {key: event.get(key) for key in event if key in set(MATCH_FEATURES) | {
                    "prior_close", "median_dollar_volume_10", "median_volume_10", "price_band",
                    "corporate_action_45d", "exchange", "realized_vol_10", "atr_pct_10",
                    "prior_day_return", "momentum_10", "listing_sessions_observed",
                }},
                "control_features": control,
                "standardized_deltas": deltas,
                "matching_version": "3.0.2",
            })

        if len(selected) < cfg.controls_per_event:
            rejected.sort(key=lambda row: (row[0], row[1]))
            nearest = [{
                "symbol": row[2].get("symbol"),
                "exchange": row[2].get("exchange"),
                "match_score": row[0],
                "failures": row[4],
                "standardized_deltas": row[3],
            } for row in rejected[:5]]
            diagnostics.append({
                "event": event,
                "reason": "no_strong_control" if not selected else "partial_strong_match",
                "selected_count": len(selected),
                "requested_count": cfg.controls_per_event,
                "candidate_count": len(eligible_candidates),
                "accepted_candidate_count": len(accepted),
                "rejection_counts": rejection_counts,
                "nearest_rejected": nearest,
                "matching_version": "3.0.2",
            })
    return pairs, diagnostics

