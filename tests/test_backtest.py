from datetime import date, datetime, timezone

from app.backtest import FrozenPreopenAccumulator, frozen_preopen_score, simulate_quotes

UTC = timezone.utc


def q(ts, bid, ask, bs=1000, ass=1000):
    return {"t": ts, "bp": bid, "ap": ask, "bs": bs, "as": ass}


def test_frozen_score_is_monotonic_in_volume_and_inverse_spread():
    base = frozen_preopen_score(10_000, 100.0, 0.20)
    assert frozen_preopen_score(100_000, 100.0, 0.20) > base
    assert frozen_preopen_score(10_000, 20.0, 0.20) > base
    assert frozen_preopen_score(10_000, 100.0, 0.50) > base


def test_execution_hits_target():
    rows = [
        q("2026-04-20T13:30:05Z", 11.90, 12.00),
        q("2026-04-20T13:31:00Z", 15.10, 15.20),
    ]
    result = simulate_quotes(
        rows, event_date=date(2026,4,20),
        entry_boundary=datetime(2026,4,20,13,30,5,tzinfo=UTC),
        close_boundary=datetime(2026,4,20,19,55,tzinfo=UTC),
        prior_close=10.0, position_notional=500.0,
        stop_loss_pct=5.0, slippage_bps=0.0, fractionable=True,
    )
    assert result.filled
    assert result.exit_reason == "target"
    assert result.target_price == 12.5
    assert result.return_pct > 20


def test_execution_hits_stop_before_target():
    rows = [
        q("2026-04-20T16:00:05Z", 11.90, 12.00),
        q("2026-04-20T16:01:00Z", 11.30, 11.40),
        q("2026-04-20T16:02:00Z", 15.10, 15.20),
    ]
    result = simulate_quotes(
        rows, event_date=date(2026,4,20),
        entry_boundary=datetime(2026,4,20,16,0,5,tzinfo=UTC),
        close_boundary=datetime(2026,4,20,19,55,tzinfo=UTC),
        prior_close=10.0, position_notional=500.0,
        stop_loss_pct=5.0, slippage_bps=0.0, fractionable=True,
    )
    assert result.exit_reason == "stop"
    assert result.return_pct < 0


def test_rejects_entry_at_or_above_target():
    rows = [q("2026-04-20T13:30:05Z", 12.5, 12.6)]
    result = simulate_quotes(
        rows, event_date=date(2026,4,20),
        entry_boundary=datetime(2026,4,20,13,30,5,tzinfo=UTC),
        close_boundary=datetime(2026,4,20,19,55,tzinfo=UTC),
        prior_close=10.0, position_notional=500.0,
        stop_loss_pct=5.0, slippage_bps=0.0, fractionable=True,
    )
    assert not result.filled
    assert result.reason == "no_executable_subthreshold_ask"


def test_stale_close_quote_does_not_create_fake_time_exit():
    rows = [
        q("2026-04-20T13:30:05Z", 11.90, 12.00),
        q("2026-04-20T14:00:00Z", 12.20, 12.30),
    ]
    result = simulate_quotes(
        rows, event_date=date(2026,4,20),
        entry_boundary=datetime(2026,4,20,13,30,5,tzinfo=UTC),
        close_boundary=datetime(2026,4,20,19,55,tzinfo=UTC),
        prior_close=10.0, position_notional=500.0,
        stop_loss_pct=5.0, slippage_bps=0.0, fractionable=True,
        max_time_exit_quote_age_seconds=60.0,
    )
    assert result.filled
    assert result.reason == "no_executable_close_exit"
    assert result.pnl_usd is None


def test_preopen_accumulator_uses_per_second_minimum_spread_and_last_midpoint():
    end = datetime(2026,4,20,13,0,1,tzinfo=UTC)
    acc = FrozenPreopenAccumulator(end)
    acc.add_trade({"t":"2026-04-20T12:59:59.100Z","p":10.0,"s":100})
    acc.add_trade({"t":"2026-04-20T13:00:00.900Z","p":11.0,"s":50})
    # Same second: minimum absolute spread is 0.10, but final midpoint is 10.15.
    acc.add_quote({"t":"2026-04-20T12:59:59.100Z","bp":9.9,"ap":10.1})
    acc.add_quote({"t":"2026-04-20T12:59:59.900Z","bp":10.1,"ap":10.2})
    summary = acc.summary()
    assert summary is not None
    assert summary["trade_volume"] == 150
    assert summary["trade_seconds"] == 2
    assert abs(summary["median_spread_bps"] - (0.1 / 10.15 * 10000)) < 1e-8


def test_embedded_frozen_constants_match_rule_file():
    import json
    from pathlib import Path
    from app.backtest import MIDDAY_RULE, PREOPEN_RULE
    rules = json.loads((Path(__file__).parents[1] / "frozen_candidate_rules_v1.json").read_text())["rules"]
    pre = next(x for x in rules if x["rule_id"] == PREOPEN_RULE["rule_id"])
    mid = next(x for x in rules if x["rule_id"] == MIDDAY_RULE["rule_id"])
    assert pre["secondary_threshold"] == PREOPEN_RULE["threshold"]
    assert mid["secondary_threshold_pct"] == MIDDAY_RULE["threshold_pct"]
    for source, embedded in zip(pre["features"], PREOPEN_RULE["components"].values()):
        assert source["mean"] == embedded["mean"]
        assert source["standard_deviation"] == embedded["sd"]
        assert source["sign"] == embedded["sign"]
