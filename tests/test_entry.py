from datetime import date, datetime, timezone

from app.entry import EntryFeasibilityAnalyzer, snapshot_cutoffs, split_name, summarize_second_rows
from app.research import timestamp_ns


def test_fixed_chronological_splits():
    assert split_name(date(2026, 6, 9)) == "discovery"
    assert split_name(date(2026, 6, 10)) == "validation"
    assert split_name(date(2026, 6, 24)) == "validation"
    assert split_name(date(2026, 6, 25)) == "sealed_test"


def test_london_decision_cutoffs_are_timezone_aware():
    cutoffs = snapshot_cutoffs(date(2026, 4, 20))
    assert cutoffs["preopen_1400_bst"] == timestamp_ns("2026-04-20T13:00:00Z")
    assert cutoffs["midday_1700_bst"] == timestamp_ns("2026-04-20T16:00:00Z")
    assert cutoffs["afternoon_1900_bst"] == timestamp_ns("2026-04-20T18:00:00Z")


def test_confirmed_continuous_post_open_entry_is_actionable():
    quotes = [
        {"timestamp": "2026-04-20T13:30:05.000000000Z", "bid_price": 9.9, "bid_size_shares": 100, "ask_price": 10.0, "ask_size_shares": 100},
        {"timestamp": "2026-04-20T13:30:40.000000000Z", "bid_price": 10.4, "bid_size_shares": 100, "ask_price": 10.5, "ask_size_shares": 100},
    ]
    trades = [
        {"timestamp": "2026-04-20T13:30:10.000000000Z", "price": 10.0, "size_shares": 50},
    ]
    analyzer = EntryFeasibilityAnalyzer(
        minimum_notional=500,
        reaction_delay_seconds=5,
        minimum_opportunity_seconds=30,
        require_subsequent_trade=True,
    )
    result = analyzer.analyze(
        event_date=date(2026, 4, 20),
        cross_timestamp_raw="2026-04-20T13:31:00.000000000Z",
        raw_threshold=15.0,
        quote_rows=quotes,
        trade_rows=trades,
        additional_cutoffs={},
    )
    assert result["purchase_feasible"] is True
    assert result["primary_actionable"] is True
    assert result["primary_opportunity_seconds"] == 55.0


def test_opening_threshold_without_pre_cross_regular_quote_is_not_actionable():
    analyzer = EntryFeasibilityAnalyzer(
        minimum_notional=500,
        reaction_delay_seconds=0,
        minimum_opportunity_seconds=0,
        require_subsequent_trade=False,
    )
    result = analyzer.analyze(
        event_date=date(2026, 4, 20),
        cross_timestamp_raw="2026-04-20T13:30:00.000000000Z",
        raw_threshold=15.0,
        quote_rows=[],
        trade_rows=[],
        additional_cutoffs={},
    )
    assert result["purchase_feasible"] is False
    assert result["primary_actionable"] is False
    assert "threshold_reached_at_or_before_regular_open" in result["quality_flags"]


def test_snapshot_summary_uses_only_rows_at_or_before_cutoff():
    rows = [
        {"second": datetime(2026, 4, 20, 13, 0, tzinfo=timezone.utc), "trade_open": 10.0, "trade_high": 10.0, "trade_low": 10.0, "trade_close": 10.0, "trade_volume": 100, "trade_count": 1, "bid_price": 9.9, "bid_size_shares": 100, "ask_price": 10.1, "ask_size_shares": 100, "min_spread": 0.2, "quote_updates": 1},
        {"second": datetime(2026, 4, 20, 13, 1, tzinfo=timezone.utc), "trade_open": 11.0, "trade_high": 11.0, "trade_low": 11.0, "trade_close": 11.0, "trade_volume": 100, "trade_count": 1, "bid_price": 10.9, "bid_size_shares": 100, "ask_price": 11.1, "ask_size_shares": 100, "min_spread": 0.2, "quote_updates": 1},
    ]
    summary = summarize_second_rows(
        rows,
        cutoff_ns=timestamp_ns("2026-04-20T13:00:00.999999999Z"),
        prior_close=9.0,
    )
    assert summary["trade_count"] == 1
    assert summary["last_trade_price"] == 10.0


def test_official_open_at_threshold_overrides_later_raw_quote_opportunity(tmp_path, monkeypatch):
    from app.config import Settings
    from app.entry import EntryExporterRunner

    event = {
        "id": "event-1",
        "source_result_id": "result-1",
        "symbol": "TEST",
        "event_date": "2026-04-20",
        "raw_threshold_price": 15.0,
        "adjusted_threshold_price": 15.0,
        "exact_cross_timestamp": "2026-04-20T13:31:00+00:00",
        "exact_cross_timestamp_raw": "2026-04-20T13:31:00.000000000Z",
        "quality_flags": [],
        "eligible": True,
        "sellability_status": "confirmed_sellable",
    }
    files = {
        "event-1": [
            {"file_kind": "raw_quotes", "storage_path": "x/2026-04-20_to_cross_part1.parquet", "filename": "q.parquet"},
            {"file_kind": "raw_trades", "storage_path": "x/2026-04-20_to_cross_part1.parquet", "filename": "t.parquet"},
        ]
    }
    quotes = [
        {"timestamp": "2026-04-20T13:30:05.000000000Z", "bid_price": 9.9, "bid_size_shares": 100, "ask_price": 10.0, "ask_size_shares": 100},
        {"timestamp": "2026-04-20T13:30:40.000000000Z", "bid_price": 10.4, "bid_size_shares": 100, "ask_price": 10.5, "ask_size_shares": 100},
    ]
    trades = [{"timestamp": "2026-04-20T13:30:10.000000000Z", "price": 10.0, "size_shares": 50}]
    runner = EntryExporterRunner(Settings(temp_root=str(tmp_path)), store=None)  # type: ignore[arg-type]
    runner._download_paths = lambda file_rows, local_root: [tmp_path / file_rows[0]["file_kind"]]  # type: ignore[method-assign]
    monkeypatch.setattr(
        "app.entry._iter_parquet_rows",
        lambda paths, columns=None: iter(quotes if paths[0].name == "raw_quotes" else trades),
    )
    result = runner._assess_event(
        "job-1",
        event,
        {"session_open": 15.0, "threshold_price": 15.0},
        files,
        {
            "minimum_entry_notional": 500,
            "reaction_delay_seconds": 5,
            "minimum_opportunity_seconds": 30,
            "minimum_gross_edge_pct": 0,
            "require_subsequent_trade": True,
        },
    )
    assert result["purchase_feasible"] is False
    assert result["primary_actionable"] is False
    assert result["exclusion_reason"] == "opened_at_or_above_threshold"


def test_no_trade_mode_can_use_later_quote_that_meets_edge_gate():
    quotes = [
        {"timestamp": "2026-04-20T13:30:05.000000000Z", "bid_price": 14.8, "bid_size_shares": 100, "ask_price": 14.9, "ask_size_shares": 100},
        {"timestamp": "2026-04-20T13:30:20.000000000Z", "bid_price": 9.9, "bid_size_shares": 100, "ask_price": 10.0, "ask_size_shares": 100},
    ]
    analyzer = EntryFeasibilityAnalyzer(
        minimum_notional=500,
        reaction_delay_seconds=5,
        minimum_opportunity_seconds=30,
        minimum_gross_edge_pct=10,
        require_subsequent_trade=False,
    )
    result = analyzer.analyze(
        event_date=date(2026, 4, 20),
        cross_timestamp_raw="2026-04-20T13:31:00.000000000Z",
        raw_threshold=15.0,
        quote_rows=quotes,
        trade_rows=[],
        additional_cutoffs={},
    )
    assert result["primary_actionable"] is True
    assert result["primary_first_entry_ask"] == 10.0
