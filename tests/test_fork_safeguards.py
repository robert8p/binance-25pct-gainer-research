import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.config import settings
from app.main import BacktestJobRequest, ScanRequest, create_backtest_job


def test_scan_request_is_locked_to_25_percent():
    assert ScanRequest().threshold_pct == 25.0
    with pytest.raises(ValidationError):
        ScanRequest(threshold_pct=50.0)


def test_backtest_stage_is_disabled_until_25pct_rules_exist():
    assert settings.enable_backtest_stage is False
    payload = BacktestJobRequest(source_entry_job_id="entry-job")
    with pytest.raises(HTTPException) as error:
        create_backtest_job(payload, "test-user")
    assert error.value.status_code == 409
    assert "50% cohort" in error.value.detail
