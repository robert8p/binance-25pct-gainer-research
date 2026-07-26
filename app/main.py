from __future__ import annotations

import csv
import io
import secrets
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.config import RESEARCH_TARGET_PCT, settings
from app.supabase_store import SupabaseStore

app = FastAPI(title="Alpaca 25% Gainer Research Lab", version="4.0.1-25pct")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
security = HTTPBasic()


class ScanRequest(BaseModel):
    lookback_days: int = Field(default=90, ge=5, le=365)
    threshold_pct: float = Field(default=25.0, ge=25.0, le=25.0)
    universe_mode: str = Field(default="current_tradable")
    feed: str = Field(default="sip")
    include_partial_current_day: bool = False
    save_event_bars: bool = True


class ResearchJobRequest(BaseModel):
    source_scan_id: str
    prior_sessions: int = Field(default=10, ge=1, le=30)
    minimum_sellable_notional: float = Field(default=500.0, ge=1, le=1_000_000)
    sellability_window_seconds: int = Field(default=300, ge=30, le=1800)
    require_subsequent_trade: bool = True
    include_raw_trades: bool = True
    include_raw_quotes: bool = True
    derive_one_second: bool = True
    include_news: bool = True
    include_auctions: bool = True
    include_corporate_actions: bool = True
    max_events: int = Field(default=0, ge=0, le=100000)


class ControlJobRequest(BaseModel):
    source_research_job_id: str
    controls_per_event: int = Field(default=5, ge=1, le=20)
    feature_sessions: int = Field(default=10, ge=5, le=60)
    history_calendar_days: int = Field(default=120, ge=45, le=730)
    max_control_symbol_uses: int = Field(default=20, ge=1, le=500)
    prior_sessions: int = Field(default=10, ge=1, le=30)
    feed: str = Field(default="sip")
    exact_exchange_first: bool = False  # retained for V3.0.x request compatibility; ignored by strict global matching
    allow_exchange_fallback: bool = True
    require_corporate_action_match: bool = True
    max_match_score: float = Field(default=4.0, ge=0.5, le=20.0)
    max_abs_log_prior_close_z: float = Field(default=1.5, ge=0.25, le=10.0)
    max_abs_log_median_dollar_volume_z: float = Field(default=1.5, ge=0.25, le=10.0)
    max_abs_realized_vol_z: float = Field(default=2.0, ge=0.25, le=10.0)
    max_abs_atr_pct_z: float = Field(default=2.0, ge=0.25, le=10.0)
    max_abs_prior_day_return_z: float = Field(default=2.0, ge=0.25, le=10.0)
    max_abs_momentum_z: float = Field(default=2.0, ge=0.25, le=10.0)
    max_abs_log_listing_sessions_z: float = Field(default=2.0, ge=0.25, le=10.0)
    include_raw_trades: bool = True
    include_raw_quotes: bool = True
    derive_one_second: bool = True
    include_news: bool = True
    include_auctions: bool = True
    include_corporate_actions: bool = True
    build_analysis_export: bool = True
    max_positive_events: int = Field(default=0, ge=0, le=100000)


class EntryJobRequest(BaseModel):
    source_control_job_id: str
    minimum_entry_notional: float = Field(default=500.0, ge=100.0, le=5000.0)
    reaction_delay_seconds: float = Field(default=5.0, ge=0.0, le=300.0)
    minimum_opportunity_seconds: float = Field(default=30.0, ge=0.0, le=1800.0)
    minimum_gross_edge_pct: float = Field(default=0.0, ge=0.0, le=100.0)
    require_subsequent_trade: bool = True
    build_fixed_time_snapshots: bool = True
    fail_on_assessment_error: bool = False
    max_positive_events: int = Field(default=0, ge=0, le=100000)


class BacktestJobRequest(BaseModel):
    source_entry_job_id: str
    window_mode: str = Field(default="source_study")
    feed: str = Field(default="sip")
    enable_preopen: bool = True
    enable_midday: bool = True
    position_notional: float = Field(default=500.0, ge=100.0, le=5000.0)
    reaction_delay_seconds: float = Field(default=5.0, ge=0.0, le=60.0)
    stop_loss_pct: float = Field(default=5.0, ge=0.5, le=25.0)
    slippage_bps: float = Field(default=5.0, ge=0.0, le=100.0)
    max_trades_per_day: int = Field(default=5, ge=1, le=5)
    close_exit_minutes_before: int = Field(default=5, ge=0, le=60)
    maximum_dates: int = Field(default=3, ge=0, le=500)


def authenticate(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> str:
    valid_user = secrets.compare_digest(credentials.username, settings.app_username)
    valid_password = secrets.compare_digest(credentials.password, settings.app_password)
    if not (valid_user and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def store() -> SupabaseStore:
    settings.validate_web()
    return SupabaseStore(settings)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": "4.0.1-25pct", "target_gain_pct": "25"}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, _: str = Depends(authenticate)) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "default_lookback": settings.default_lookback_days,
            "default_threshold": settings.default_threshold_pct,
            "default_universe": settings.default_universe_mode,
            "default_prior_sessions": settings.default_prior_sessions,
            "default_sellability_notional": settings.default_sellability_notional,
            "default_sellability_window": settings.default_sellability_window_seconds,
            "default_controls_per_event": settings.default_controls_per_event,
            "default_control_feature_sessions": settings.default_control_feature_sessions,
            "default_control_history_days": settings.default_control_history_calendar_days,
            "default_max_control_symbol_uses": settings.default_max_control_symbol_uses,
            "default_entry_notional": settings.default_entry_notional,
            "default_entry_reaction_delay": settings.default_entry_reaction_delay_seconds,
            "default_entry_minimum_opportunity": settings.default_entry_minimum_opportunity_seconds,
            "default_entry_minimum_gross_edge": settings.default_entry_minimum_gross_edge_pct,
            "default_backtest_notional": settings.default_backtest_position_notional,
            "default_backtest_reaction_delay": settings.default_backtest_reaction_delay_seconds,
            "default_backtest_stop_loss": settings.default_backtest_stop_loss_pct,
            "default_backtest_slippage": settings.default_backtest_slippage_bps,
            "default_backtest_max_trades": settings.default_backtest_max_trades_per_day,
            "default_backtest_close_minutes": settings.default_backtest_close_exit_minutes_before,
            "backtest_enabled": settings.enable_backtest_stage,
        },
    )


@app.get("/api/scans")
def list_scans(_: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_scans", order="created_at.desc", limit=50)
    finally:
        db.close()


@app.post("/api/scans", status_code=202)
def create_scan(payload: ScanRequest, _: str = Depends(authenticate)) -> dict:
    if abs(float(payload.threshold_pct) - RESEARCH_TARGET_PCT) > 1e-9:
        raise HTTPException(status_code=400, detail="This research fork is locked to a 25% gain threshold")
    if payload.universe_mode not in {"current_tradable", "all_recent_alpaca_assets"}:
        raise HTTPException(status_code=400, detail="Invalid universe_mode")
    if payload.feed not in {"sip", "iex"}:
        raise HTTPException(status_code=400, detail="feed must be sip or iex")
    db = store()
    try:
        return db.enqueue_scan(payload.model_dump(), source="web")
    finally:
        db.close()


@app.get("/api/scans/{scan_id}")
def get_scan(scan_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_scans", filters={"id": f"eq.{scan_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Scan not found")
        return rows[0]
    finally:
        db.close()


@app.get("/api/results")
def list_results(
    scan_id: str | None = None,
    min_gain_pct: float | None = None,
    currently_tradable: bool | None = None,
    search: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    filters: dict[str, str] = {}
    if scan_id:
        filters["scan_id"] = f"eq.{scan_id}"
    if min_gain_pct is not None:
        filters["high_vs_prior_close_pct"] = f"gte.{min_gain_pct}"
    if currently_tradable is not None:
        filters["currently_tradable"] = f"eq.{str(currently_tradable).lower()}"
    if search:
        safe = search.replace(",", " ").strip()
        filters["or"] = f"(symbol.ilike.*{safe}*,company_name.ilike.*{safe}*)"
    db = store()
    try:
        return db.select(
            "stock25_scan_results",
            filters=filters,
            order="event_date.desc,high_vs_prior_close_pct.desc",
            limit=limit,
            offset=offset,
        )
    finally:
        db.close()


@app.get("/api/results/{result_id}/bars")
def result_bars(result_id: str, _: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select_all(
            "stock25_event_bars",
            filters={"result_id": f"eq.{result_id}"},
            order="bar_timestamp.asc",
        )
    finally:
        db.close()


@app.get("/api/export.csv")
def export_csv(scan_id: str, _: str = Depends(authenticate)) -> StreamingResponse:
    db = store()
    try:
        # Explicit pagination fixes the former 1,000-row export truncation.
        rows = db.select_all(
            "stock25_scan_results",
            filters={"scan_id": f"eq.{scan_id}"},
            order="event_date.asc,symbol.asc",
        )
    finally:
        db.close()
    if not rows:
        raise HTTPException(status_code=404, detail="No results for this scan")

    output = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {key: (value if not isinstance(value, (list, dict)) else str(value)) for key, value in row.items()}
        )
    output.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="alpaca_25pct_{scan_id}_complete.csv"'}
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)


@app.get("/api/research-jobs")
def list_research_jobs(_: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_research_jobs", order="created_at.desc", limit=50)
    finally:
        db.close()


@app.post("/api/research-jobs", status_code=202)
def create_research_job(payload: ResearchJobRequest, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        scans = db.select("stock25_scans", filters={"id": f"eq.{payload.source_scan_id}"}, limit=1)
        if not scans:
            raise HTTPException(status_code=404, detail="Source scan not found")
        if scans[0].get("status") != "completed":
            raise HTTPException(status_code=400, detail="Source scan must be completed")
        parameters = payload.model_dump(exclude={"source_scan_id"})
        return db.enqueue_research_job(payload.source_scan_id, parameters)
    finally:
        db.close()


@app.get("/api/research-jobs/{job_id}")
def get_research_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_research_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Research job not found")
        return rows[0]
    finally:
        db.close()


@app.get("/api/research-events")
def list_research_events(
    job_id: str,
    eligible: bool | None = None,
    event_status: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    filters = {"research_job_id": f"eq.{job_id}"}
    if eligible is not None:
        filters["eligible"] = f"eq.{str(eligible).lower()}"
    if event_status:
        if event_status not in {"processing", "collecting", "completed", "failed"}:
            raise HTTPException(status_code=400, detail="Invalid research event status")
        filters["status"] = f"eq.{event_status}"
    db = store()
    try:
        return db.select(
            "stock25_research_events",
            filters=filters,
            order="event_date.desc,symbol.asc",
            limit=limit,
            offset=offset,
        )
    finally:
        db.close()


@app.get("/api/research-files")
def list_research_files(
    job_id: str,
    research_event_id: str | None = None,
    include_raw: bool = False,
    include_event_packages: bool = False,
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    filters: dict[str, str] = {"research_job_id": f"eq.{job_id}"}
    if research_event_id:
        filters["research_event_id"] = f"eq.{research_event_id}"
        if not include_raw:
            filters["file_kind"] = "not.in.(raw_trades,raw_quotes)"
    elif include_event_packages:
        if not include_raw:
            filters["file_kind"] = "in.(research_index,event_compact_package)"
    else:
        # Job-level downloads only. Event files are loaded on demand to avoid returning
        # thousands of raw chunk records every time the dashboard refreshes.
        filters["research_event_id"] = "is.null"
    db = store()
    try:
        return db.select(
            "stock25_research_files",
            filters=filters,
            order="file_kind.asc,created_at.asc",
            limit=limit,
            offset=offset,
        )
    finally:
        db.close()


@app.post("/api/research-jobs/{job_id}/retry", status_code=202)
def retry_research_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_research_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Research job not found")
        if rows[0].get("status") != "failed":
            raise HTTPException(status_code=400, detail="Only failed research jobs can be retried")
        db.update_research_job(
            job_id,
            status="queued",
            progress_stage="queued_retry",
            completed_at=None,
            error_message=None,
        )
        return db.select("stock25_research_jobs", filters={"id": f"eq.{job_id}"}, limit=1)[0]
    finally:
        db.close()


@app.get("/api/research-files/{file_id}/download")
def download_research_file(file_id: str, _: str = Depends(authenticate)) -> RedirectResponse:
    db = store()
    try:
        rows = db.select("stock25_research_files", filters={"id": f"eq.{file_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Research file not found")
        signed = db.create_signed_url(rows[0]["storage_path"])
        return RedirectResponse(signed, status_code=302)
    finally:
        db.close()


@app.get("/api/control-jobs")
def list_control_jobs(_: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_control_jobs", order="created_at.desc", limit=50)
    finally:
        db.close()


@app.post("/api/control-jobs", status_code=202)
def create_control_job(payload: ControlJobRequest, _: str = Depends(authenticate)) -> dict:
    if payload.feed not in {"sip", "iex"}:
        raise HTTPException(status_code=400, detail="feed must be sip or iex")
    db = store()
    try:
        sources = db.select("stock25_research_jobs", filters={"id": f"eq.{payload.source_research_job_id}"}, limit=1)
        if not sources:
            raise HTTPException(status_code=404, detail="Source research job not found")
        if sources[0].get("status") != "completed":
            raise HTTPException(status_code=400, detail="Source research job must be completed")
        parameters = payload.model_dump(exclude={"source_research_job_id"})
        return db.enqueue_control_job(payload.source_research_job_id, parameters)
    finally:
        db.close()


@app.get("/api/control-jobs/{job_id}")
def get_control_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_control_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Control job not found")
        return rows[0]
    finally:
        db.close()


@app.get("/api/control-pairs")
def list_control_pairs(
    job_id: str,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    db = store()
    try:
        return db.select(
            "stock25_control_pairs", filters={"control_job_id": f"eq.{job_id}"},
            order="event_date.desc,positive_symbol.asc,control_rank.asc", limit=limit, offset=offset,
        )
    finally:
        db.close()


@app.get("/api/control-observations")
def list_control_observations(
    job_id: str,
    observation_status: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    filters = {"control_job_id": f"eq.{job_id}"}
    if observation_status:
        if observation_status not in {"matched", "collecting", "completed", "failed"}:
            raise HTTPException(status_code=400, detail="Invalid control observation status")
        filters["status"] = f"eq.{observation_status}"
    db = store()
    try:
        return db.select(
            "stock25_control_observations", filters=filters, order="event_date.desc,symbol.asc",
            limit=limit, offset=offset,
        )
    finally:
        db.close()


@app.get("/api/control-match-diagnostics")
def list_control_match_diagnostics(
    job_id: str,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    db = store()
    try:
        return db.select(
            "stock25_control_match_diagnostics", filters={"control_job_id": f"eq.{job_id}"},
            order="event_date.desc,positive_symbol.asc", limit=limit, offset=offset,
        )
    finally:
        db.close()


@app.get("/api/control-files")
def list_control_files(
    job_id: str,
    observation_id: str | None = None,
    dataset_id: str | None = None,
    include_raw: bool = False,
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    filters: dict[str, str] = {"control_job_id": f"eq.{job_id}"}
    if observation_id:
        filters["control_observation_id"] = f"eq.{observation_id}"
    elif dataset_id:
        filters["control_dataset_id"] = f"eq.{dataset_id}"
        if not include_raw:
            filters["file_kind"] = "not.in.(raw_trades,raw_quotes)"
    else:
        filters["control_observation_id"] = "is.null"
        filters["control_dataset_id"] = "is.null"
    db = store()
    try:
        return db.select("stock25_control_files", filters=filters, order="file_kind.asc,created_at.asc", limit=limit, offset=offset)
    finally:
        db.close()


@app.post("/api/control-jobs/{job_id}/retry", status_code=202)
def retry_control_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_control_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Control job not found")
        if rows[0].get("status") != "failed":
            raise HTTPException(status_code=400, detail="Only failed control jobs can be retried")
        db.update_control_job(job_id, status="queued", progress_stage="queued_retry", completed_at=None, error_message=None)
        return db.select("stock25_control_jobs", filters={"id": f"eq.{job_id}"}, limit=1)[0]
    finally:
        db.close()


@app.get("/api/control-files/{file_id}/download")
def download_control_file(file_id: str, _: str = Depends(authenticate)) -> RedirectResponse:
    db = store()
    try:
        rows = db.select("stock25_control_files", filters={"id": f"eq.{file_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Control file not found")
        return RedirectResponse(db.create_signed_url(rows[0]["storage_path"]), status_code=302)
    finally:
        db.close()


@app.get("/api/entry-jobs")
def list_entry_jobs(_: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_entry_jobs", order="created_at.desc", limit=50)
    finally:
        db.close()


@app.post("/api/entry-jobs", status_code=202)
def create_entry_job(payload: EntryJobRequest, _: str = Depends(authenticate)) -> dict:
    if payload.minimum_entry_notional not in {100.0, 500.0, 1000.0, 5000.0}:
        raise HTTPException(status_code=400, detail="minimum_entry_notional must be 100, 500, 1000 or 5000")
    db = store()
    try:
        sources = db.select("stock25_control_jobs", filters={"id": f"eq.{payload.source_control_job_id}"}, limit=1)
        if not sources:
            raise HTTPException(status_code=404, detail="Source matched-control job not found")
        if sources[0].get("status") != "completed":
            raise HTTPException(status_code=400, detail="Source matched-control job must be completed")
        parameters = payload.model_dump(exclude={"source_control_job_id"})
        return db.enqueue_entry_job(payload.source_control_job_id, parameters)
    finally:
        db.close()


@app.get("/api/entry-jobs/{job_id}")
def get_entry_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_entry_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Entry/export job not found")
        return rows[0]
    finally:
        db.close()


@app.get("/api/entry-assessments")
def list_entry_assessments(
    job_id: str,
    actionable_only: bool = False,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(authenticate),
) -> list[dict]:
    filters: dict[str, str] = {"entry_job_id": f"eq.{job_id}"}
    if actionable_only:
        filters["primary_actionable"] = "eq.true"
    db = store()
    try:
        return db.select(
            "stock25_entry_assessments", filters=filters, order="event_date.desc,symbol.asc",
            limit=limit, offset=offset,
        )
    finally:
        db.close()


@app.get("/api/entry-files")
def list_entry_files(
    job_id: str,
    _: str = Depends(authenticate),
) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_entry_files", filters={"entry_job_id": f"eq.{job_id}"}, order="file_kind.asc")
    finally:
        db.close()


@app.post("/api/entry-jobs/{job_id}/retry", status_code=202)
def retry_entry_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_entry_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Entry/export job not found")
        if rows[0].get("status") != "failed":
            raise HTTPException(status_code=400, detail="Only failed entry/export jobs can be retried")
        db.update_entry_job(
            job_id, status="queued", progress_stage="queued_retry", completed_at=None, error_message=None
        )
        return db.select("stock25_entry_jobs", filters={"id": f"eq.{job_id}"}, limit=1)[0]
    finally:
        db.close()


@app.get("/api/entry-files/{file_id}/download")
def download_entry_file(file_id: str, _: str = Depends(authenticate)) -> RedirectResponse:
    db = store()
    try:
        rows = db.select("stock25_entry_files", filters={"id": f"eq.{file_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Entry/export file not found")
        return RedirectResponse(db.create_signed_url(rows[0]["storage_path"]), status_code=302)
    finally:
        db.close()


@app.get("/api/backtest-jobs")
def list_backtest_jobs(_: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_backtest_jobs", order="created_at.desc", limit=50)
    finally:
        db.close()


@app.post("/api/backtest-jobs", status_code=202)
def create_backtest_job(payload: BacktestJobRequest, _: str = Depends(authenticate)) -> dict:
    if not settings.enable_backtest_stage:
        raise HTTPException(
            status_code=409,
            detail="Step 5 is locked: the attached frozen signals were derived from the 50% cohort. Complete and analyse the 25% discovery, control and sealed-test stages before freezing a 25%-specific rule set.",
        )
    if payload.feed not in {"sip", "iex"}:
        raise HTTPException(status_code=400, detail="feed must be sip or iex")
    if payload.window_mode not in {"source_study", "prior_90_days"}:
        raise HTTPException(status_code=400, detail="window_mode must be source_study or prior_90_days")
    if not payload.enable_preopen or not payload.enable_midday:
        raise HTTPException(status_code=400, detail="Both frozen strategies must remain enabled")
    frozen = {
        "position_notional": 500.0, "reaction_delay_seconds": 5.0,
        "stop_loss_pct": 5.0, "slippage_bps": 5.0,
        "max_trades_per_day": 5, "close_exit_minutes_before": 5,
    }
    for name, expected in frozen.items():
        if float(getattr(payload, name)) != float(expected):
            raise HTTPException(status_code=400, detail=f"{name} is frozen at {expected}")
    if payload.feed != "sip":
        raise HTTPException(status_code=400, detail="The frozen backtest requires SIP")
    db = store()
    try:
        sources = db.select("stock25_entry_jobs", filters={"id": f"eq.{payload.source_entry_job_id}"}, limit=1)
        if not sources:
            raise HTTPException(status_code=404, detail="Source entry-feasibility job not found")
        if sources[0].get("status") != "completed":
            raise HTTPException(status_code=400, detail="Source entry-feasibility job must be completed")
        return db.enqueue_backtest_job(payload.source_entry_job_id, payload.model_dump(exclude={"source_entry_job_id"}))
    finally:
        db.close()


@app.get("/api/backtest-jobs/{job_id}")
def get_backtest_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_backtest_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Backtest job not found")
        return rows[0]
    finally:
        db.close()


@app.get("/api/backtest-trades")
def list_backtest_trades(job_id: str, limit: int = Query(default=500, ge=1, le=5000), offset: int = Query(default=0, ge=0), _: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_backtest_trades", filters={"backtest_job_id": f"eq.{job_id}"}, order="trade_date.desc,strategy.asc,rank.asc", limit=limit, offset=offset)
    finally:
        db.close()


@app.get("/api/backtest-files")
def list_backtest_files(job_id: str, _: str = Depends(authenticate)) -> list[dict]:
    db = store()
    try:
        return db.select("stock25_backtest_files", filters={"backtest_job_id": f"eq.{job_id}"}, order="file_kind.asc")
    finally:
        db.close()


@app.post("/api/backtest-jobs/{job_id}/retry", status_code=202)
def retry_backtest_job(job_id: str, _: str = Depends(authenticate)) -> dict:
    db = store()
    try:
        rows = db.select("stock25_backtest_jobs", filters={"id": f"eq.{job_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Backtest job not found")
        if rows[0].get("status") != "failed":
            raise HTTPException(status_code=400, detail="Only failed backtests can be retried")
        db.update_backtest_job(job_id, status="queued", progress_stage="queued_retry", completed_at=None, error_message=None)
        return db.select("stock25_backtest_jobs", filters={"id": f"eq.{job_id}"}, limit=1)[0]
    finally:
        db.close()


@app.get("/api/backtest-files/{file_id}/download")
def download_backtest_file(file_id: str, _: str = Depends(authenticate)) -> RedirectResponse:
    db = store()
    try:
        rows = db.select("stock25_backtest_files", filters={"id": f"eq.{file_id}"}, limit=1)
        if not rows:
            raise HTTPException(status_code=404, detail="Backtest file not found")
        return RedirectResponse(db.create_signed_url(rows[0]["storage_path"]), status_code=302)
    finally:
        db.close()
