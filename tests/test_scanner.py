from datetime import datetime, timezone

from app.config import Settings
from app.scanner import ScanRunner

UTC = timezone.utc


class FakeStore:
    def __init__(self):
        self.scan_updates = []
        self.results = []
        self.event_bars = []
        self.snapshots = []

    def update_scan(self, scan_id, **values):
        self.scan_updates.append((scan_id, values))

    def save_asset_snapshot(self, snapshot_date, assets):
        self.snapshots.append((snapshot_date, assets))

    def insert(self, table, rows):
        if table == "stock25_scan_results":
            row = dict(rows)
            row["id"] = "result-1"
            self.results.append(row)
            return [row]
        raise AssertionError(table)

    def upsert(self, table, rows, **kwargs):
        if table == "stock25_event_bars":
            self.event_bars.extend(rows)
            return
        raise AssertionError(table)


class FakeAlpaca:
    def get_calendar(self, start, end):
        return [
            {"date": "2026-07-14", "open": "09:30", "close": "16:00"},
            {"date": "2026-07-15", "open": "09:30", "close": "16:00"},
            {"date": "2026-07-16", "open": "09:30", "close": "16:00"},
        ]

    def get_assets(self, all_statuses=True):
        return [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "class": "us_equity",
                "symbol": "ABC",
                "name": "ABC Corp",
                "exchange": "NASDAQ",
                "status": "active",
                "tradable": True,
                "fractionable": True,
                "shortable": False,
                "easy_to_borrow": False,
                "marginable": True,
                "attributes": [],
            }
        ]

    def get_bars(self, symbols, *, timeframe, start, end, feed, adjustment, asof):
        assert symbols == ["ABC"]
        assert feed == "sip"
        if timeframe == "1Day":
            return {
                "ABC": [
                    {"t": "2026-07-14T04:00:00Z", "o": 9, "h": 10.5, "l": 8.8, "c": 10, "v": 100000, "n": 1000, "vw": 9.8},
                    {"t": "2026-07-15T04:00:00Z", "o": 12.0, "h": 13.1, "l": 11.8, "c": 12.7, "v": 2000000, "n": 20000, "vw": 12.5},
                    {"t": "2026-07-16T04:00:00Z", "o": 13, "h": 14, "l": 12, "c": 12.5, "v": 500000, "n": 5000, "vw": 13.0},
                ]
            }
        if timeframe == "1Min":
            return {
                "ABC": [
                    {"t": "2026-07-15T13:30:00Z", "o": 12.0, "h": 12.4, "l": 11.8, "c": 12.2, "v": 100000, "n": 1000, "vw": 12.1},
                    {"t": "2026-07-15T13:31:00Z", "o": 12.2, "h": 12.6, "l": 12.1, "c": 12.5, "v": 150000, "n": 1500, "vw": 12.4},
                    {"t": "2026-07-15T13:32:00Z", "o": 12.5, "h": 13.1, "l": 12.4, "c": 12.8, "v": 120000, "n": 1200, "vw": 12.8},
                ]
            }
        raise AssertionError(timeframe)


def test_full_scan_finds_regular_session_cross_and_saves_bars():
    settings = Settings(
        default_lookback_days=20,
        default_threshold_pct=25,
        request_pause_seconds=0,
        include_otc=False,
        save_event_bars=True,
    )
    store = FakeStore()
    runner = ScanRunner(settings, store, FakeAlpaca())
    runner.run(
        {
            "id": "scan-1",
            "parameters": {
                "lookback_days": 20,
                "threshold_pct": 25,
                "universe_mode": "current_tradable",
                "feed": "sip",
                "save_event_bars": True,
            },
        }
    )

    assert len(store.results) == 1
    result = store.results[0]
    assert result["symbol"] == "ABC"
    assert result["event_date"] == "2026-07-15"
    assert round(result["high_vs_prior_close_pct"], 1) == 31.0
    assert round(result["opening_gap_pct"], 1) == 20.0
    assert result["minutes_from_open_to_cross"] == 1
    assert len(store.event_bars) == 3
    assert any(update.get("status") == "completed" for _, update in store.scan_updates)
