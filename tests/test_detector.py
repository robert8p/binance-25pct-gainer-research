from datetime import datetime, timedelta, timezone

from app.detector import detect_regular_session_gainer

UTC = timezone.utc
OPEN = datetime(2026, 7, 1, 13, 30, tzinfo=UTC)  # 09:30 ET during EDT
CLOSE = datetime(2026, 7, 1, 20, 0, tzinfo=UTC)


def bar(minute, o, h, l, c, volume=10000, trades=100):
    return {
        "timestamp": OPEN + timedelta(minutes=minute),
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": volume,
        "trade_count": trades,
        "vwap": (o + h + l + c) / 4,
    }


def test_threshold_can_be_touched_then_lost():
    bars = [
        bar(0, 12.0, 12.4, 11.8, 12.2),
        bar(1, 12.2, 12.5, 12.1, 12.4),
        bar(2, 12.4, 13.1, 12.3, 12.8),
        bar(3, 12.8, 12.9, 11.5, 11.9),
    ]
    result = detect_regular_session_gainer(
        bars, prior_close=10.0, threshold_pct=25, market_open=OPEN, market_close=CLOSE
    )
    assert result is not None
    assert result.qualifies is True
    assert result.threshold_cross_bar_start == OPEN + timedelta(minutes=1)
    assert round(result.high_vs_prior_close_pct, 2) == 31.0
    assert result.session_close == 11.9


def test_premarket_bar_is_ignored():
    bars = [
        {
            **bar(-10, 10, 20, 10, 18),
            "timestamp": OPEN - timedelta(minutes=10),
        },
        bar(0, 11, 12, 10.8, 11.5),
        bar(1, 11.5, 12.4, 11.4, 12.0),
    ]
    result = detect_regular_session_gainer(
        bars, prior_close=10.0, threshold_pct=25, market_open=OPEN, market_close=CLOSE
    )
    assert result is not None
    assert result.qualifies is False
    assert result.session_high == 12.4


def test_opening_gap_and_post_open_upside_are_separate():
    bars = [
        bar(0, 12.0, 12.2, 11.8, 12.1),
        bar(1, 12.1, 12.7, 12.0, 12.6),
    ]
    result = detect_regular_session_gainer(
        bars, prior_close=10.0, threshold_pct=25, market_open=OPEN, market_close=CLOSE
    )
    assert result is not None and result.qualifies
    assert round(result.opening_gap_pct, 1) == 20.0
    assert round(result.open_to_peak_pct, 1) == 5.8
    assert round(result.first_minute_entry_to_peak_pct, 1) == 5.0


def test_missing_bars_are_flagged():
    bars = [bar(0, 10, 11, 9.8, 10.5), bar(10, 10.5, 16, 10.5, 15)]
    result = detect_regular_session_gainer(
        bars, prior_close=10.0, threshold_pct=25, market_open=OPEN, market_close=CLOSE
    )
    assert result is not None and result.qualifies
    assert result.max_missing_bar_gap_minutes == 9
    assert "possible_halt_or_illiquidity" in result.quality_flags
