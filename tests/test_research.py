from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from app.config import Settings
from app.research import (
    SECOND_SCHEMA,
    TRADE_SCHEMA,
    ResearchRunner,
    SecondAggregator,
    SellabilityAnalyzer,
    normalize_trade,
    quote_size_shares,
    timestamp_ns,
)
from app.supabase_store import SupabaseStore

UTC = timezone.utc
CROSS_MINUTE = datetime(2026, 7, 15, 13, 31, tzinfo=UTC)


def raw_trade(timestamp: str, price: float, size: int = 10) -> dict:
    return {"t": timestamp, "p": price, "s": size, "x": "V", "c": [], "i": "1", "z": "C"}


def raw_quote(timestamp: str, bid: float, bid_size: int, ask: float = 15.02, ask_size: int = 100) -> dict:
    return {"t": timestamp, "bp": bid, "bs": bid_size, "bx": "V", "ap": ask, "as": ask_size, "ax": "Q", "c": [], "z": "C"}


class FakeSellabilityAlpaca:
    def __init__(self, trades: list[dict], quotes: list[dict]):
        self.trades = trades
        self.quotes = quotes

    def get_single_bars(self, *args, **kwargs):
        return [{"t": "2026-07-15T13:31:00Z", "o": 14.9, "h": 15.2, "l": 14.8, "c": 15.1, "v": 1000, "n": 20, "vw": 15.0}]

    def iter_trades(self, *args, **kwargs):
        yield self.trades

    def iter_quotes(self, *args, **kwargs):
        yield self.quotes


def result_row() -> dict:
    return {
        "symbol": "ABC",
        "event_date": "2026-07-15",
        "feed": "sip",
        "threshold_cross_bar_start": "2026-07-15T13:31:00+00:00",
        "threshold_price": 15.0,
    }


def test_quote_size_normalisation_switch_date():
    assert quote_size_shares(5, date(2025, 11, 2)) == 500
    assert quote_size_shares(5, date(2025, 11, 3)) == 5
    assert quote_size_shares(5, date(2026, 7, 15)) == 5


def test_sellability_requires_displayed_bid_and_later_threshold_trade():
    trades = [
        raw_trade("2026-07-15T13:31:05.000000Z", 15.00, 10),
        raw_trade("2026-07-15T13:31:07.000000Z", 15.05, 20),
    ]
    quotes = [raw_quote("2026-07-15T13:31:06.000000Z", 15.00, 100)]
    analyzer = SellabilityAnalyzer(FakeSellabilityAlpaca(trades, quotes))
    sell = analyzer.analyze(
        result_row(),
        {"high": 15.2},
        minimum_notional=500,
        window_seconds=300,
        require_subsequent_trade=True,
    )
    assert sell.eligible is True
    assert sell.status == "confirmed_sellable"
    assert sell.first_confirmed_exit_timestamp == datetime(2026, 7, 15, 13, 31, 6, tzinfo=UTC)
    assert sell.first_confirmed_exit_notional == 1500
    assert sell.trade_count_at_or_above_threshold == 2


def test_displayed_bid_without_later_trade_is_not_confirmed():
    trades = [raw_trade("2026-07-15T13:31:05.000000Z", 15.00, 10)]
    quotes = [raw_quote("2026-07-15T13:31:06.000000Z", 15.00, 100)]
    analyzer = SellabilityAnalyzer(FakeSellabilityAlpaca(trades, quotes))
    sell = analyzer.analyze(
        result_row(),
        {"high": 15.2},
        minimum_notional=500,
        window_seconds=300,
        require_subsequent_trade=True,
    )
    assert sell.eligible is False
    assert sell.status == "displayed_bid_only"


def test_second_aggregator_builds_trade_and_quote_summary():
    agg = SecondAggregator("2026-07-14")
    agg.add_trade(normalize_trade(raw_trade("2026-07-14T13:30:00.100000Z", 10.0, 20), "ABC", "2026-07-14"))
    agg.add_trade(normalize_trade(raw_trade("2026-07-14T13:30:00.900000Z", 10.2, 30), "ABC", "2026-07-14"))
    quote = {
        "symbol": "ABC", "timestamp": "2026-07-14T13:30:00.500000Z",
        "bid_price": 10.0, "bid_size_shares": 100, "ask_price": 10.1,
        "ask_size_shares": 100, "spread": 0.1,
    }
    agg.add_quote(quote)
    rows = agg.rows()
    assert len(rows) == 1
    assert rows[0]["trade_open"] == 10.0
    assert rows[0]["trade_close"] == 10.2
    assert rows[0]["trade_volume"] == 50
    assert rows[0]["bid_price"] == 10.0
    assert set(rows[0]).issubset(set(SECOND_SCHEMA.names))


class FakePagedStore(SupabaseStore):
    def __init__(self):
        self.data = [{"id": i} for i in range(2505)]
        self.calls = []

    def select(self, table, *, select="*", filters=None, order=None, limit=None, offset=None):
        self.calls.append((limit, offset))
        start = offset or 0
        return self.data[start:start + (limit or len(self.data))]


def test_select_all_pages_beyond_supabase_1000_row_default():
    store = FakePagedStore()
    rows = store.select_all("stock25_scan_results", page_size=1000)
    assert len(rows) == 2505
    assert store.calls == [(1000, 0), (1000, 1000), (1000, 2000)]


class NoopStore:
    pass


class NoopAlpaca:
    pass


def test_streaming_enforces_exclusive_event_boundary(tmp_path: Path):
    settings = Settings(max_raw_rows_per_file=0, temp_root=str(tmp_path))
    runner = ResearchRunner(settings, NoopStore(), NoopAlpaca())
    runner._write_upload_rows_safely = lambda **kwargs: ([], kwargs["part_no"] + 1)  # type: ignore[method-assign]
    agg = SecondAggregator("event")
    pages = [[
        raw_trade("2026-07-15T13:31:04.999999Z", 14.99, 10),
        raw_trade("2026-07-15T13:31:05.000000Z", 15.00, 10),
        raw_trade("2026-07-15T13:31:05.000001Z", 15.01, 10),
    ]]
    boundary = datetime(2026, 7, 15, 13, 31, 5, tzinfo=UTC)
    total, files, truncated = runner._stream_pages(
        job_id="job", event_id="event", event_prefix="prefix", kind="trades",
        window_name="event_to_cross", pages=pages,
        normalizer=lambda row: normalize_trade(row, "ABC", "event_to_cross"),
        schema=TRADE_SCHEMA, aggregator=agg, temp_dir=tmp_path,
        exclusive_end_ns=timestamp_ns(boundary),
    )
    assert total == 1
    assert files == []
    assert truncated is False
    assert sum(row.get("trade_count", 0) for row in agg.rows()) == 1


def test_timestamp_ns_preserves_submicrosecond_ordering():
    earlier = "2026-07-15T13:31:05.123456100Z"
    later = "2026-07-15T13:31:05.123456789Z"
    assert timestamp_ns(earlier) < timestamp_ns(later)
    assert timestamp_ns(later) - timestamp_ns(earlier) == 689


class FilteringSellabilityAlpaca(FakeSellabilityAlpaca):
    def __init__(self, trades: list[dict], quotes: list[dict]):
        super().__init__(trades, quotes)
        self.trade_end = None
        self.quote_ends = []

    def iter_trades(self, *args, **kwargs):
        self.trade_end = kwargs["end"]
        yield [r for r in self.trades if datetime.fromisoformat(r["t"].replace("Z", "+00:00")) <= self.trade_end]

    def iter_quotes(self, *args, **kwargs):
        self.quote_ends.append(kwargs["end"])
        quote_end = kwargs["end"]
        yield [r for r in self.quotes if datetime.fromisoformat(r["t"].replace("Z", "+00:00")) <= quote_end]


def test_sellability_window_runs_from_exact_cross_not_minute_start():
    trades = [
        raw_trade("2026-07-15T13:31:59.900000000Z", 15.00, 10),
        raw_trade("2026-07-15T13:36:58.900000000Z", 15.02, 20),
        raw_trade("2026-07-15T13:36:59.900000000Z", 15.03, 20),
    ]
    quotes = [raw_quote("2026-07-15T13:36:58.000000000Z", 15.00, 100)]
    alpaca = FilteringSellabilityAlpaca(trades, quotes)
    sell = SellabilityAnalyzer(alpaca).analyze(
        result_row(), {"high": 15.2}, minimum_notional=500,
        window_seconds=300, require_subsequent_trade=True,
    )
    assert sell.eligible is True
    assert sell.first_confirmed_exit_timestamp_raw == "2026-07-15T13:36:58.000000000Z"
    assert alpaca.trade_end == CROSS_MINUTE + timedelta(minutes=6)
    assert alpaca.quote_ends[0] == CROSS_MINUTE + timedelta(minutes=6)


def test_trade_at_same_nanosecond_as_bid_is_not_subsequent():
    trades = [
        raw_trade("2026-07-15T13:31:05.000000000Z", 15.00, 10),
        raw_trade("2026-07-15T13:31:06.000000000Z", 15.05, 20),
    ]
    quotes = [raw_quote("2026-07-15T13:31:06.000000000Z", 15.00, 100)]
    sell = SellabilityAnalyzer(FakeSellabilityAlpaca(trades, quotes)).analyze(
        result_row(), {"high": 15.2}, minimum_notional=500,
        window_seconds=300, require_subsequent_trade=True,
    )
    assert sell.eligible is False
    assert sell.status == "displayed_bid_only"


def test_active_pre_cross_quote_remains_sellable_until_next_update():
    trades = [
        raw_trade("2026-07-15T13:31:05.000000000Z", 15.00, 10),
        raw_trade("2026-07-15T13:31:07.000000000Z", 15.05, 20),
    ]
    quotes = [raw_quote("2026-07-15T13:31:04.500000000Z", 15.00, 100)]
    sell = SellabilityAnalyzer(FakeSellabilityAlpaca(trades, quotes)).analyze(
        result_row(), {"high": 15.2}, minimum_notional=500,
        window_seconds=300, require_subsequent_trade=True,
    )
    assert sell.eligible is True
    assert sell.seconds_to_first_confirmed_exit == 0
    assert sell.active_bid_at_cross_price == 15.0
    assert "sellable_bid_active_at_crossing" in sell.flags
    assert sell.displayed_seconds_at_or_above_threshold == 300


def test_second_aggregator_uses_nanosecond_order_within_microsecond():
    agg = SecondAggregator("event")
    agg.add_trade(normalize_trade(raw_trade("2026-07-15T13:31:05.123456789Z", 10.2), "ABC", "event"))
    agg.add_trade(normalize_trade(raw_trade("2026-07-15T13:31:05.123456100Z", 10.0), "ABC", "event"))
    row = agg.rows()[0]
    assert row["trade_open"] == 10.0
    assert row["trade_close"] == 10.2


def test_displayed_duration_merges_adjacent_above_threshold_quote_states():
    trades = [
        raw_trade("2026-07-15T13:31:05.000000000Z", 15.00, 10),
        raw_trade("2026-07-15T13:31:09.000000000Z", 15.05, 20),
    ]
    quotes = [
        raw_quote("2026-07-15T13:31:04.000000000Z", 15.00, 100),
        raw_quote("2026-07-15T13:31:07.000000000Z", 15.01, 100),
        raw_quote("2026-07-15T13:31:10.000000000Z", 14.99, 100),
    ]
    sell = SellabilityAnalyzer(FakeSellabilityAlpaca(trades, quotes)).analyze(
        result_row(), {"high": 15.2}, minimum_notional=500,
        window_seconds=300, require_subsequent_trade=True,
    )
    assert sell.displayed_seconds_at_or_above_threshold == 5
    assert sell.max_contiguous_displayed_seconds == 5


def test_supabase_secret_key_is_not_sent_as_bearer_token():
    store = SupabaseStore(Settings(supabase_url="https://example.supabase.co", supabase_service_role_key="sb_secret_example"))
    try:
        assert store.auth_headers == {"apikey": "sb_secret_example"}
    finally:
        store.close()


def test_legacy_service_role_jwt_is_sent_as_api_key_and_bearer():
    key = "eyJlegacy-service-role"
    store = SupabaseStore(Settings(supabase_url="https://example.supabase.co", supabase_service_role_key=key))
    try:
        assert store.auth_headers == {"apikey": key, "Authorization": f"Bearer {key}"}
    finally:
        store.close()
