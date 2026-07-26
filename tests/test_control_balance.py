from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.control import ControlRunner
from app.matching import MatchConfig


class FakeStore:
    def __init__(self) -> None:
        self.files: list[dict] = []
        self.job_updates: list[dict] = []

    def upload_file(self, path: Path, storage_path: str, *, content_type: str):
        assert Path(path).exists()
        return {"Key": storage_path}

    def upsert(self, table, row, *, on_conflict, return_representation=False, **_kwargs):
        assert table == "stock25_control_files"
        stored = {"id": f"file-{len(self.files)+1}", **row}
        self.files.append(stored)
        return [stored] if return_representation else []

    def update_control_job(self, _job_id: str, **values):
        self.job_updates.append(values)


class FakeAlpaca:
    pass


def sample_pair() -> dict:
    return {
        "positive_research_event_id": "positive-1",
        "event_date": "2026-06-01",
        "positive_symbol": "WIN",
        "control_symbol": "CTRL",
        "control_exchange": "NYSE",
        "match_score": 1.2,
        "match_quality": "excellent",
        "positive_features": {
            "exchange": "NASDAQ",
            "prior_close": 3.0,
            "median_dollar_volume_10": 1_000_000,
            "realized_vol_10": 0.08,
            "price_band": "2_5",
        },
        "control_features": {
            "prior_close": 3.1,
            "median_dollar_volume_10": 1_100_000,
            "realized_vol_10": 0.081,
            "price_band": "2_5",
        },
        "standardized_deltas": {
            "log_prior_close": 0.1,
            "log_median_dollar_volume_10": 0.1,
            "realized_vol_10": 0.1,
            "atr_pct_10": 0.1,
            "prior_day_return": 0.1,
            "momentum_10": 0.1,
            "log_listing_sessions": 0.0,
            "exchange_mismatch": 1.0,
            "corporate_action_mismatch": 0.0,
        },
    }


def test_pre_download_balance_report_is_uploaded_before_collection(tmp_path):
    store = FakeStore()
    settings = SimpleNamespace(temp_root=str(tmp_path))
    runner = ControlRunner(settings, store, FakeAlpaca())
    report = runner._write_pre_download_balance_report(
        job_id="job-1",
        positives=[{"id": "positive-1"}],
        pairs=[sample_pair()],
        diagnostics=[],
        cfg=MatchConfig(),
    )
    assert report["gate_status"] == "passed"
    assert report["weak_pair_count"] == 0
    assert {row["file_kind"] for row in store.files} == {
        "pre_download_balance_report",
        "pre_download_balance_pairs",
        "control_match_shortfalls",
    }
    assert store.job_updates[-1]["balance_gate_status"] == "passed"


def test_pre_download_balance_gate_fails_without_strong_controls(tmp_path):
    store = FakeStore()
    settings = SimpleNamespace(temp_root=str(tmp_path))
    runner = ControlRunner(settings, store, FakeAlpaca())
    report = runner._write_pre_download_balance_report(
        job_id="job-2",
        positives=[{"id": "positive-1"}],
        pairs=[],
        diagnostics=[{
            "event": {"id": "positive-1", "symbol": "WIN", "event_date": "2026-06-01"},
            "reason": "no_strong_control",
            "selected_count": 0,
            "requested_count": 5,
            "rejection_counts": {"overall_match_score_too_large": 10},
        }],
        cfg=MatchConfig(),
    )
    assert report["gate_status"] == "failed"
    assert "no_strong_controls_selected" in report["gate_violations"]
