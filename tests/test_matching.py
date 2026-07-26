from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.control import summarize_minute_rows, summarize_second_rows
from app.matching import (
    MatchConfig,
    classify_positive_tier,
    compute_symbol_date_features,
    match_controls_for_date,
)

UTC = timezone.utc


def bars(start: date, closes: list[float], event_high: float | None = None):
    rows = []
    for idx, close in enumerate(closes):
        day = start + timedelta(days=idx)
        high = close * 1.02
        if idx == len(closes) - 1 and event_high is not None:
            high = event_high
        rows.append({
            "t": datetime(day.year, day.month, day.day, tzinfo=UTC).isoformat(),
            "o": close * 0.99,
            "h": high,
            "l": close * 0.98,
            "c": close,
            "v": 100_000 + idx * 1_000,
        })
    return rows


def feature(symbol: str, exchange: str, price: float, dollar_volume: float, vol: float, *, no_hit: bool = True):
    return {
        "symbol": symbol,
        "exchange": exchange,
        "event_date": "2026-06-01",
        "prior_close": price,
        "median_dollar_volume_10": dollar_volume,
        "median_volume_10": dollar_volume / price,
        "realized_vol_10": vol,
        "atr_pct_10": vol * 1.5,
        "prior_day_return": 0.01,
        "momentum_10": 0.05,
        "listing_sessions_observed": 100,
        "corporate_action_45d": False,
        "price_band": "2_5",
        "log_prior_close": __import__("math").log(price),
        "log_median_dollar_volume_10": __import__("math").log(dollar_volume),
        "log_listing_sessions": __import__("math").log(100),
        "no_threshold_hit": no_hit,
    }


def test_daily_features_use_only_prior_bars_and_label_strict_non_hit():
    start = date(2026, 5, 1)
    closes = [10 + i * 0.1 for i in range(12)]
    event_date = start + timedelta(days=11)
    result = compute_symbol_date_features(
        "ABC", "NASDAQ", bars(start, closes, event_high=13.0), {event_date},
        threshold_pct=25.0, feature_sessions=10,
    )[0]
    assert result["feature_cutoff_date"] == (event_date - timedelta(days=1)).isoformat()
    assert result["prior_close"] == closes[-2]
    assert result["no_threshold_hit"] is True


def test_daily_features_reject_threshold_hit_as_control():
    start = date(2026, 5, 1)
    closes = [10.0] * 12
    event_date = start + timedelta(days=11)
    result = compute_symbol_date_features(
        "ABC", "NASDAQ", bars(start, closes, event_high=12.51), {event_date},
        threshold_pct=25.0, feature_sessions=10,
    )[0]
    assert result["no_threshold_hit"] is False


def test_matcher_prefers_close_same_exchange_candidate():
    event = feature("WIN", "NASDAQ", 3.0, 1_000_000, 0.08)
    event.update({
        "research_event_id": "event-1", "source_result_id": "result-1",
        "exact_cross_timestamp": "2026-06-01T15:00:00+00:00",
        "exact_cross_timestamp_raw": "2026-06-01T15:00:00.123456789Z",
        "positive_tier": "primary_clean",
    })
    close = feature("CLOSE", "NASDAQ", 3.05, 1_100_000, 0.081)
    far = feature("FAR", "NYSE", 8.0, 12_000_000, 0.25)
    pairs, misses = match_controls_for_date(
        [event], [close, far], cfg=MatchConfig(controls_per_event=1), global_symbol_uses={}
    )
    assert not misses
    assert pairs[0]["control_symbol"] == "CLOSE"
    assert pairs[0]["pseudo_event_timestamp_raw"].endswith("123456789Z")


def test_matcher_never_uses_positive_or_threshold_hit_symbol():
    event = feature("WIN", "NASDAQ", 3.0, 1_000_000, 0.08)
    event.update({
        "research_event_id": "event-1", "source_result_id": "result-1",
        "exact_cross_timestamp": "2026-06-01T15:00:00+00:00",
    })
    positive_named = feature("WIN", "NASDAQ", 3.0, 1_000_000, 0.08)
    hit = feature("HIT", "NASDAQ", 3.0, 1_000_000, 0.08, no_hit=False)
    valid = feature("VALID", "NASDAQ", 3.0, 1_000_000, 0.08)
    pairs, _ = match_controls_for_date([event], [positive_named, hit, valid], cfg=MatchConfig(controls_per_event=1))
    assert [row["control_symbol"] for row in pairs] == ["VALID"]


def test_matcher_is_deterministic_and_no_stock_day_reuse():
    events = []
    for idx in range(2):
        event = feature(f"WIN{idx}", "NASDAQ", 3.0, 1_000_000, 0.08)
        event.update({
            "research_event_id": f"event-{idx}", "source_result_id": f"result-{idx}",
            "exact_cross_timestamp": "2026-06-01T15:00:00+00:00",
        })
        events.append(event)
    candidates = [feature(f"C{i}", "NASDAQ", 3.0 + i * 0.01, 1_000_000, 0.08) for i in range(4)]
    first, _ = match_controls_for_date(events, candidates, cfg=MatchConfig(controls_per_event=2))
    second, _ = match_controls_for_date(events, candidates, cfg=MatchConfig(controls_per_event=2))
    assert [(p["positive_symbol"], p["control_symbol"]) for p in first] == [(p["positive_symbol"], p["control_symbol"]) for p in second]
    assert len({p["control_symbol"] for p in first}) == len(first)
    assert all(p["match_quality"] in {"excellent", "good"} for p in first)


def test_quality_tier_is_conservative():
    assert classify_positive_tier({"adjustment_scale": 1.0, "quality_flags": []}) == "primary_clean"
    assert classify_positive_tier({"adjustment_scale": 2.0, "quality_flags": []}) == "extended"
    assert classify_positive_tier({"adjustment_scale": 1.0, "quality_flags": ["active_quote_recovered_before_crossing_minute"]}) == "extended"


def test_minute_summary_preserves_session_segments():
    rows = [
        {"timestamp": "2026-06-01T13:30:00Z", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 100, "trade_count": 5, "session": "regular"},
        {"timestamp": "2026-06-01T13:31:00Z", "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.1, "volume": 200, "trade_count": 7, "session": "regular"},
    ]
    summary = summarize_minute_rows(rows)
    assert summary["bar_count"] == 2
    assert summary["volume"] == 300
    assert summary["session_bar_counts"]["regular"] == 2


def test_second_summary_captures_microstructure():
    rows = [
        {"trade_count": 2, "trade_volume": 100, "quote_updates": 4, "min_spread": 0.02, "bid_size_shares": 500, "ask_size_shares": 600},
        {"trade_count": 5, "trade_volume": 250, "quote_updates": 2, "min_spread": 0.04, "bid_size_shares": 300, "ask_size_shares": 400},
    ]
    summary = summarize_second_rows(rows)
    assert summary["trade_count"] == 7
    assert summary["max_trades_per_second"] == 5
    assert summary["median_spread"] == 0.03


def test_matcher_ranks_other_exchange_globally_instead_of_forcing_same_exchange():
    event = feature("WIN", "NASDAQ", 3.0, 1_000_000, 0.08)
    event.update({
        "research_event_id": "event-global", "source_result_id": "result-global",
        "exact_cross_timestamp": "2026-06-01T15:00:00+00:00",
    })
    poor_same_exchange = feature("POOR", "NASDAQ", 5.9, 4_900_000, 0.159)
    poor_same_exchange["prior_day_return"] = 0.50
    poor_same_exchange["momentum_10"] = 0.80
    close_other_exchange = feature("CLOSE", "NYSE", 3.02, 1_020_000, 0.081)
    pairs, misses = match_controls_for_date(
        [event], [poor_same_exchange, close_other_exchange],
        cfg=MatchConfig(controls_per_event=1),
    )
    assert pairs[0]["control_symbol"] == "CLOSE"
    assert not misses


def test_matcher_never_packages_weak_control_and_records_shortfall():
    event = feature("WIN", "NASDAQ", 3.0, 1_000_000, 0.08)
    event.update({
        "research_event_id": "event-weak", "source_result_id": "result-weak",
        "exact_cross_timestamp": "2026-06-01T15:00:00+00:00",
    })
    weak = feature("WEAK", "NASDAQ", 5.8, 4_900_000, 0.159)
    weak["prior_day_return"] = 1.0
    weak["momentum_10"] = 1.0
    pairs, diagnostics = match_controls_for_date(
        [event], [weak], cfg=MatchConfig(controls_per_event=5),
    )
    assert pairs == []
    assert diagnostics[0]["reason"] == "no_strong_control"
    assert diagnostics[0]["selected_count"] == 0
    assert diagnostics[0]["nearest_rejected"]


def test_matcher_requires_corporate_action_status_for_primary_controls():
    event = feature("WIN", "NASDAQ", 3.0, 1_000_000, 0.08)
    event.update({
        "research_event_id": "event-ca", "source_result_id": "result-ca",
        "exact_cross_timestamp": "2026-06-01T15:00:00+00:00",
        "corporate_action_45d": True,
    })
    mismatch = feature("MISMATCH", "NASDAQ", 3.0, 1_000_000, 0.08)
    pairs, diagnostics = match_controls_for_date(
        [event], [mismatch], cfg=MatchConfig(controls_per_event=1),
    )
    assert pairs == []
    assert diagnostics[0]["rejection_counts"]["corporate_action_status_mismatch"] == 1
