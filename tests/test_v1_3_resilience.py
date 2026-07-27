from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from app.binance import BinanceClient
from app.matched_controls import LoadedSymbol, MatchedControlBuilder
from app.scanner import Scanner
from app.worker import _recover_table


class Response:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def test_aggregate_trades_are_streamed_page_by_page(monkeypatch):
    client = BinanceClient(("https://example.invalid",))
    pages = [
        [{"a": i, "T": i, "p": "1", "q": "1", "m": False} for i in range(1000)],
        [{"a": 1000 + i, "T": 1000 + i, "p": "1", "q": "1", "m": False} for i in range(10)],
    ]

    def fake_get(path, params=None):
        return Response(pages.pop(0) if pages else [])

    monkeypatch.setattr(client, "_get", fake_get)
    state = {}
    yielded = list(client.iter_aggregate_trade_pages("ABCUSDT", 0, 2000, state=state))
    assert [len(page) for page in yielded] == [1000, 10]
    assert state["rows"] == 1010
    assert state["truncated"] is False


class RecoveryDB:
    def __init__(self, rows):
        self.rows = rows
        self.updates = []

    def select_all(self, table, **kwargs):
        return [dict(row) for row in self.rows]

    def update(self, table, filters, payload):
        self.updates.append((table, filters, payload))


def test_worker_recovery_requeues_with_checkpoint_and_caps_retries():
    db = RecoveryDB([{"id": "a", "resume_count": 1}])
    _recover_table(db, "jobs", max_auto_resumes=3)
    assert db.updates[0][2]["status"] == "queued"
    assert db.updates[0][2]["resume_count"] == 2

    db = RecoveryDB([{"id": "b", "resume_count": 3}])
    _recover_table(db, "jobs", max_auto_resumes=3)
    assert db.updates[0][2]["status"] == "failed"
    assert db.updates[0][2]["last_stage"] == "resume_limit_exceeded"


class SnapshotDB:
    def select_all(self, table, **kwargs):
        assert table == "binance_symbol_snapshots"
        return [
            {
                "symbol": "ABCUSDT",
                "base_asset": "ABC",
                "quote_asset": "USDT",
                "quote_priority": 0,
                "status": "TRADING",
                "spot_permission": True,
                "is_spot_trading_allowed": True,
                "stablecoin_like": False,
                "leveraged_token_like": False,
                "raw_json": {},
            }
        ]


class NoExchangeBinance:
    def exchange_info(self):
        raise AssertionError("resume must use the frozen symbol snapshot")


def test_scan_resume_uses_existing_symbol_snapshot(tmp_path):
    scanner = Scanner(SnapshotDB(), NoExchangeBinance(), tmp_path)
    symbols = scanner._symbol_universe("scan", ["USDT"])
    assert [row["symbol"] for row in symbols] == ["ABCUSDT"]


class MemoryDB:
    def __init__(self):
        self.tables = {
            "binance_gainer_events": [
                {
                    "id": "event-1",
                    "scan_id": "scan-1",
                    "symbol": "ABCUSDT",
                    "base_asset": "ABC",
                    "quote_asset": "USDT",
                    "event_date": "2026-01-05",
                    "first_cross_time": "2026-01-05T12:00:00+00:00",
                    "sellability_pass": True,
                },
                {
                    "id": "event-2",
                    "scan_id": "scan-1",
                    "symbol": "ABCUSDT",
                    "base_asset": "ABC",
                    "quote_asset": "USDT",
                    "event_date": "2026-01-06",
                    "first_cross_time": "2026-01-06T12:00:00+00:00",
                    "sellability_pass": True,
                },
            ],
            "binance_scan_jobs": [
                {
                    "id": "scan-1",
                    "event_definition_version": "v1_25pct_rolling_8h",
                    "window_minutes": 480,
                    "threshold_pct": 25,
                    "research_purpose": "external_validation_c2_c4",
                    "result_json": {
                        "window_start": "2026-01-01T00:00:00+00:00",
                        "window_end_exclusive": "2026-01-10T00:00:00+00:00",
                    },
                }
            ],
            "binance_matched_control_progress": [
                {
                    "matched_control_job_id": "job-1",
                    "event_id": "event-1",
                    "symbol": "ABCUSDT",
                    "status": "completed",
                    "controls_created": 1,
                    "rejection_json": {},
                }
            ],
            "binance_control_matches": [
                {
                    "matched_control_job_id": "job-1",
                    "event_id": "event-1",
                    "control_id": "old-control",
                    "symbol": "ABCUSDT",
                    "split": "external_validation",
                    "event_anchor_time": "2026-01-05T12:00:00+00:00",
                    "control_anchor_time": "2026-01-02T12:00:00+00:00",
                    "control_rank": 1,
                    "clock_offset_minutes": 0,
                    "calendar_distance_days": 3,
                    "weekday_match": False,
                    "match_tier": "exact_clock",
                    "prior_global_reuse_count": 0,
                    "minimum_5m_quote_volume": 1000,
                    "prior_history_observed_fraction": 1.0,
                    "quality_status": "pass",
                }
            ],
        }
        self.updated = []
        self.deleted = []

    @staticmethod
    def _matches(row, filters):
        if not filters:
            return True
        for key, value in filters.items():
            if value.startswith("eq.") and str(row.get(key)).lower() != value[3:].lower():
                return False
        return True

    def select_all(self, table, filters=None, **kwargs):
        return [dict(row) for row in self.tables.get(table, []) if self._matches(row, filters)]

    def select(self, table, filters=None, **kwargs):
        return self.select_all(table, filters=filters, **kwargs)

    def update(self, table, filters, payload):
        self.updated.append((table, payload))

    def insert(self, table, payload):
        self.tables.setdefault(table, []).append(dict(payload))
        return [payload]

    def delete(self, table, filters):
        self.deleted.append((table, dict(filters)))
        self.tables[table] = [
            row for row in self.tables.get(table, []) if not self._matches(row, filters)
        ]

    def upsert(self, table, payload, on_conflict, **kwargs):
        keys = on_conflict.split(",")
        target = self.tables.setdefault(table, [])
        for row in payload:
            found = next((item for item in target if all(str(item.get(k)) == str(row.get(k)) for k in keys)), None)
            if found:
                found.update(row)
            else:
                target.append(dict(row))


def test_matched_controls_resume_skips_completed_event(monkeypatch, tmp_path):
    import app.matched_controls as module

    db = MemoryDB()
    builder = MatchedControlBuilder(db, SimpleNamespace(), tmp_path, minimum_disk_free_bytes=1)
    index = pd.date_range("2025-12-15", "2026-01-10", freq="1min", tz="UTC")
    frame = pd.DataFrame(
        {"high": 1.0, "low": 1.0, "close": 1.0, "quote_volume": 1000.0, "observed": True},
        index=index,
    )
    monkeypatch.setattr(builder.cache, "load_symbol", lambda *a, **k: LoadedSymbol(frame, []))
    monkeypatch.setattr(module, "rolling_crossing_mask", lambda f, **k: pd.Series(False, index=f.index))
    called = []

    def fake_select_controls_for_event(**kwargs):
        event = kwargs["event"]
        called.append(event["id"])
        controls = []
        for rank in range(1, 6):
            controls.append(
                {
                    "control_id": f"{event['id']}-c{rank}",
                    "event_id": event["id"],
                    "symbol": event["symbol"],
                    "split": "external_validation",
                    "event_anchor_time": event["first_cross_time"],
                    "control_anchor_time": f"2026-01-0{rank}T10:00:00+00:00",
                    "control_rank": rank,
                    "clock_offset_minutes": 0,
                    "calendar_distance_days": rank,
                    "weekday_match": False,
                    "match_tier": "exact_clock",
                    "prior_global_reuse_count": 0,
                    "minimum_5m_quote_volume": 1000,
                    "prior_history_observed_fraction": 1.0,
                    "quality_status": "pass",
                }
            )
        return controls, {}

    monkeypatch.setattr(module, "select_controls_for_event", fake_select_controls_for_event)
    result = builder.run(
        {
            "id": "job-1",
            "scan_id": "scan-1",
            "research_purpose": "external_validation_c2_c4",
            "controls_per_event": 5,
            "prior_days": 10,
            "horizons_minutes": [480],
            "contamination_before_minutes": 480,
            "contamination_after_minutes": 480,
            "min_entry_notional": 500,
        }
    )
    assert called == ["event-2"]
    assert result["events_processed"] == 2
    assert result["controls_created"] == 6


def test_external_validation_source_uses_durable_feature_checkpoints():
    source = Path(__file__).parents[1].joinpath("app/external_validation.py").read_text()
    assert "binance_external_validation_feature_rows" in source
    assert "completed_sample_ids" in source
    assert "one event-symbol frame at a time" in source


def test_matched_controls_resume_removes_uncommitted_partial_event(monkeypatch, tmp_path):
    import app.matched_controls as module

    db = MemoryDB()
    # Simulate a hard kill after event-2 controls were written but before its
    # progress checkpoint was committed.
    db.tables["binance_control_matches"].append(
        {
            **db.tables["binance_control_matches"][0],
            "event_id": "event-2",
            "control_id": "partial-control",
            "event_anchor_time": "2026-01-06T12:00:00+00:00",
            "control_anchor_time": "2026-01-03T12:00:00+00:00",
        }
    )
    builder = MatchedControlBuilder(db, SimpleNamespace(), tmp_path, minimum_disk_free_bytes=1)
    index = pd.date_range("2025-12-15", "2026-01-10", freq="1min", tz="UTC")
    frame = pd.DataFrame(
        {"high": 1.0, "low": 1.0, "close": 1.0, "quote_volume": 1000.0, "observed": True},
        index=index,
    )
    monkeypatch.setattr(builder.cache, "load_symbol", lambda *a, **k: LoadedSymbol(frame, []))
    monkeypatch.setattr(module, "rolling_crossing_mask", lambda f, **k: pd.Series(False, index=f.index))

    def fake_select_controls_for_event(**kwargs):
        event = kwargs["event"]
        return [
            {
                "control_id": f"{event['id']}-fresh",
                "event_id": event["id"],
                "symbol": event["symbol"],
                "split": "external_validation",
                "event_anchor_time": event["first_cross_time"],
                "control_anchor_time": "2026-01-04T10:00:00+00:00",
                "control_rank": 1,
                "clock_offset_minutes": 0,
                "calendar_distance_days": 2,
                "weekday_match": False,
                "match_tier": "exact_clock",
                "prior_global_reuse_count": 0,
                "minimum_5m_quote_volume": 1000,
                "prior_history_observed_fraction": 1.0,
                "quality_status": "pass",
            }
        ], {}

    monkeypatch.setattr(module, "select_controls_for_event", fake_select_controls_for_event)
    result = builder.run(
        {
            "id": "job-1",
            "scan_id": "scan-1",
            "research_purpose": "external_validation_c2_c4",
            "controls_per_event": 5,
            "prior_days": 10,
            "horizons_minutes": [480],
            "contamination_before_minutes": 480,
            "contamination_after_minutes": 480,
            "min_entry_notional": 500,
        }
    )
    assert any(filters.get("event_id") == "eq.event-2" for _, filters in db.deleted)
    event2 = [row for row in db.tables["binance_control_matches"] if row["event_id"] == "event-2"]
    assert [row["control_id"] for row in event2] == ["event-2-fresh"]
    assert result["controls_created"] == 2
