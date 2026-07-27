from __future__ import annotations

import base64
import csv
import io
import uuid
from datetime import date, datetime, timezone
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from .config import Settings
from .frozen_candidates import (
    EXTERNAL_WINDOW_END_EXCLUSIVE,
    EXTERNAL_WINDOW_START,
    register_sha256,
)
from .supabase import SupabaseClient

APP_VERSION = "1.3.0"
EVENT_DEFINITION_VERSION = "v1_25pct_rolling_8h"
FIXED_THRESHOLD_PCT = 25.0
FIXED_WINDOW_MINUTES = 480

settings = Settings.from_env()
db = SupabaseClient(settings.supabase_url, settings.supabase_service_role_key, settings.storage_bucket)
app = FastAPI(title="Binance 25% Memory-Safe Resumable External Validation", version=APP_VERSION)
templates = Jinja2Templates(directory="app/templates")


def _auth(request: Request) -> None:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Basic "):
        raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})
    try:
        username, password = base64.b64decode(header[6:]).decode().split(":", 1)
    except Exception as exc:
        raise HTTPException(401, headers={"WWW-Authenticate": "Basic"}) from exc
    if username != "rob" or password != settings.app_password:
        raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})


def _completed_25pct_scans(scans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in scans
        if row.get("status") in {"completed", "completed_with_warnings"}
        and row.get("event_definition_version") == EVENT_DEFINITION_VERSION
        and int(row.get("window_minutes") or 0) == FIXED_WINDOW_MINUTES
        and abs(float(row.get("threshold_pct") or 0) - FIXED_THRESHOLD_PCT) < 1e-12
    ]


def _completed_external_scans(scans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in _completed_25pct_scans(scans)
        if row.get("research_purpose") == "external_validation_c2_c4"
        and str(row.get("window_start_date")) == EXTERNAL_WINDOW_START
        and str(row.get("window_end_date_exclusive")) == EXTERNAL_WINDOW_END_EXCLUSIVE
    ]


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "version": APP_VERSION,
        "target": "25pct_within_8h",
        "event_definition_version": EVENT_DEFINITION_VERSION,
        "purpose": "frozen_c2_c4_external_validation",
        "execution_model": "memory_bounded_resumable",
        "candidate_register_sha256": register_sha256(),
    }


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    _auth(request)
    scans = db.select("binance_scan_jobs", order="created_at.desc", limit=25)
    research_jobs = db.select("binance_research_jobs", order="created_at.desc", limit=25)
    matched_jobs = db.select("binance_matched_control_jobs", order="created_at.desc", limit=25)
    context_jobs = db.select("binance_context_jobs", order="created_at.desc", limit=25)
    baseline_jobs = db.select("binance_baseline_context_jobs", order="created_at.desc", limit=25)
    external_jobs = db.select("binance_external_validation_jobs", order="created_at.desc", limit=25)
    heartbeat = db.select("binance_worker_heartbeats", filters={"worker_name": "eq.main"}, limit=1)

    completed_scans = _completed_25pct_scans(scans)
    completed_external_scans = _completed_external_scans(scans)
    completed_scan_ids = {str(row["id"]) for row in completed_scans}
    completed_matched_jobs = [
        row for row in matched_jobs
        if row.get("status") in {"completed", "completed_with_warnings"}
        and str(row.get("scan_id")) in completed_scan_ids
    ]
    external_scan_ids = {str(row["id"]) for row in completed_external_scans}
    completed_external_matched_jobs = [
        row for row in matched_jobs
        if row.get("status") in {"completed", "completed_with_warnings"}
        and row.get("research_purpose") == "external_validation_c2_c4"
        and str(row.get("scan_id")) in external_scan_ids
    ]

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "version": APP_VERSION,
            "scans": scans,
            "research_jobs": research_jobs,
            "matched_jobs": matched_jobs,
            "context_jobs": context_jobs,
            "baseline_context_jobs": baseline_jobs,
            "completed_scans": completed_scans,
            "completed_matched_jobs": completed_matched_jobs,
            "completed_external_scans": completed_external_scans,
            "completed_external_matched_jobs": completed_external_matched_jobs,
            "external_validation_jobs": external_jobs,
            "external_window_start": EXTERNAL_WINDOW_START,
            "external_window_end_exclusive": EXTERNAL_WINDOW_END_EXCLUSIVE,
            "candidate_register_sha256": register_sha256(),
            "heartbeat": heartbeat[0] if heartbeat else None,
            "files": db.select("binance_research_files", order="created_at.desc", limit=100),
            "matched_files": db.select("binance_matched_control_files", order="created_at.desc", limit=100),
            "context_files": db.select("binance_context_files", order="created_at.desc", limit=100),
            "baseline_context_files": db.select("binance_baseline_context_files", order="created_at.desc", limit=100),
            "external_validation_files": db.select("binance_external_validation_files", order="created_at.desc", limit=100),
        },
    )


@app.post("/scans")
def create_scan(
    request: Request,
    lookback_days: int = Form(60),
    quote_assets: str = Form("USDT,USDC,FDUSD"),
    min_exit_notional: float = Form(500),
    confirmation_window_seconds: int = Form(300),
    window_start_date: str = Form(""),
    window_end_date_exclusive: str = Form(""),
) -> RedirectResponse:
    _auth(request)
    raise HTTPException(410, "This V1.3 release accepts only the locked external-validation workflow")
    if not 1 <= lookback_days <= 180:
        raise HTTPException(400, "lookback_days must be between 1 and 180")
    if min_exit_notional < 0:
        raise HTTPException(400, "min_exit_notional cannot be negative")
    if not 30 <= confirmation_window_seconds <= 3600:
        raise HTTPException(400, "confirmation_window_seconds must be between 30 and 3600")

    start_value = window_start_date.strip() or None
    end_value = window_end_date_exclusive.strip() or None
    if bool(start_value) != bool(end_value):
        raise HTTPException(400, "Enter both historical start and end dates, or leave both blank")
    if start_value and end_value:
        try:
            start_day = date.fromisoformat(start_value)
            end_day = date.fromisoformat(end_value)
        except ValueError as exc:
            raise HTTPException(400, "Historical dates must use YYYY-MM-DD") from exc
        span = (end_day - start_day).days
        if not 1 <= span <= 180:
            raise HTTPException(400, "Historical window must contain 1 to 180 completed UTC days")
        if end_day > datetime.now(timezone.utc).date():
            raise HTTPException(400, "Historical end cannot be after today")

    quotes = [value.strip().upper() for value in quote_assets.split(",") if value.strip()]
    if not quotes:
        raise HTTPException(400, "At least one quote asset is required")

    db.insert(
        "binance_scan_jobs",
        {
            "id": str(uuid.uuid4()),
            "status": "queued",
            "research_purpose": "chatgpt_discovery",
            "event_definition_version": EVENT_DEFINITION_VERSION,
            "lookback_days": lookback_days,
            "threshold_pct": FIXED_THRESHOLD_PCT,
            "window_minutes": FIXED_WINDOW_MINUTES,
            "quote_assets": quotes,
            "min_exit_notional": min_exit_notional,
            "confirmation_window_seconds": confirmation_window_seconds,
            "window_start_date": start_value,
            "window_end_date_exclusive": end_value,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/external-validation-scan")
def create_external_validation_scan(request: Request) -> RedirectResponse:
    _auth(request)
    existing = db.select(
        "binance_scan_jobs",
        filters={
            "research_purpose": "eq.external_validation_c2_c4",
            "window_start_date": f"eq.{EXTERNAL_WINDOW_START}",
            "window_end_date_exclusive": f"eq.{EXTERNAL_WINDOW_END_EXCLUSIVE}",
            "status": "in.(queued,running,completed,completed_with_warnings)",
        },
        order="created_at.desc",
        limit=1,
    )
    if existing:
        raise HTTPException(409, "An active or completed fixed-period external-validation scan already exists")
    start_day = date.fromisoformat(EXTERNAL_WINDOW_START)
    end_day = date.fromisoformat(EXTERNAL_WINDOW_END_EXCLUSIVE)
    db.insert(
        "binance_scan_jobs",
        {
            "id": str(uuid.uuid4()),
            "status": "queued",
            "research_purpose": "external_validation_c2_c4",
            "event_definition_version": EVENT_DEFINITION_VERSION,
            "lookback_days": (end_day - start_day).days,
            "threshold_pct": FIXED_THRESHOLD_PCT,
            "window_minutes": FIXED_WINDOW_MINUTES,
            "quote_assets": ["USDT", "USDC", "FDUSD"],
            "min_exit_notional": 500,
            "confirmation_window_seconds": 300,
            "window_start_date": EXTERNAL_WINDOW_START,
            "window_end_date_exclusive": EXTERNAL_WINDOW_END_EXCLUSIVE,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/external-matched-controls")
def create_external_matched_controls(
    request: Request,
    scan_id: str = Form(...),
) -> RedirectResponse:
    _auth(request)
    scans = db.select("binance_scan_jobs", filters={"id": f"eq.{scan_id}"}, limit=1)
    if not scans or not _completed_external_scans(scans):
        raise HTTPException(400, "Select the completed fixed-period external-validation scan")
    existing = db.select(
        "binance_matched_control_jobs",
        filters={
            "scan_id": f"eq.{scan_id}",
            "research_purpose": "eq.external_validation_c2_c4",
            "status": "in.(queued,running,completed,completed_with_warnings)",
        },
        order="created_at.desc",
        limit=1,
    )
    if existing:
        raise HTTPException(409, "An active or completed external-validation matched-control job already exists")
    db.insert(
        "binance_matched_control_jobs",
        {
            "id": str(uuid.uuid4()),
            "scan_id": scan_id,
            "status": "queued",
            "research_purpose": "external_validation_c2_c4",
            "controls_per_event": 5,
            "prior_days": 10,
            "horizons_minutes": [480],
            "contamination_before_minutes": 480,
            "contamination_after_minutes": 480,
            "min_entry_notional": 500,
            "discovery_pct": 70,
            "validation_pct": 15,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/external-validation")
def create_external_validation(
    request: Request,
    matched_control_job_id: str = Form(...),
) -> RedirectResponse:
    _auth(request)
    matched = db.select(
        "binance_matched_control_jobs",
        filters={"id": f"eq.{matched_control_job_id}"},
        limit=1,
    )
    if not matched or matched[0].get("status") not in {"completed", "completed_with_warnings"}:
        raise HTTPException(400, "Select a completed external-validation matched-control job")
    if matched[0].get("research_purpose") != "external_validation_c2_c4":
        raise HTTPException(400, "The selected job is not the V1.3 external-validation cohort")
    existing = db.select(
        "binance_external_validation_jobs",
        filters={
            "matched_control_job_id": f"eq.{matched_control_job_id}",
            "status": "in.(queued,running,completed,completed_with_warnings)",
        },
        order="created_at.desc",
        limit=1,
    )
    if existing:
        raise HTTPException(409, "An active or completed frozen-rule evaluation already exists")
    db.insert(
        "binance_external_validation_jobs",
        {
            "id": str(uuid.uuid4()),
            "matched_control_job_id": matched_control_job_id,
            "status": "queued",
            "candidate_register_sha256": register_sha256(),
            "decision_horizon_minutes": 480,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/research")
def create_research(
    request: Request,
    scan_id: str = Form(...),
    prior_days: int = Form(10),
    maximum_events: int = Form(1),
    include_1s_klines: bool = Form(False),
    include_agg_trades: bool = Form(False),
    include_raw_trades: bool = Form(False),
) -> RedirectResponse:
    _auth(request)
    raise HTTPException(410, "This V1.3 release accepts only the locked external-validation workflow")
    if not 1 <= prior_days <= 30:
        raise HTTPException(400, "prior_days must be between 1 and 30")
    db.insert(
        "binance_research_jobs",
        {
            "id": str(uuid.uuid4()),
            "scan_id": scan_id,
            "status": "queued",
            "prior_days": prior_days,
            "maximum_events": max(0, maximum_events),
            "include_1s_klines": include_1s_klines,
            "include_agg_trades": include_agg_trades,
            "include_raw_trades": include_raw_trades,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/matched-controls")
def create_matched_controls(
    request: Request,
    scan_id: str = Form(...),
    controls_per_event: int = Form(5),
    prior_days: int = Form(10),
    horizons_minutes: str = Form("15,30,60,120,180,480"),
    min_entry_notional: float = Form(500),
) -> RedirectResponse:
    _auth(request)
    raise HTTPException(410, "This V1.3 release accepts only the locked external-validation workflow")
    if not 1 <= controls_per_event <= 10:
        raise HTTPException(400, "controls_per_event must be between 1 and 10")
    if not 1 <= prior_days <= 30:
        raise HTTPException(400, "prior_days must be between 1 and 30")
    try:
        horizons = sorted({int(value.strip()) for value in horizons_minutes.split(",") if value.strip()})
    except ValueError as exc:
        raise HTTPException(400, "horizons_minutes must be comma-separated integers") from exc
    if not horizons or any(value < 5 or value > 720 for value in horizons):
        raise HTTPException(400, "decision horizons must be between 5 and 720 minutes")
    if 480 not in horizons:
        raise HTTPException(400, "25% matched controls must include the 480-minute horizon")
    if min_entry_notional < 0:
        raise HTTPException(400, "min_entry_notional cannot be negative")

    rows = db.select("binance_scan_jobs", filters={"id": f"eq.{scan_id}"}, limit=1)
    if not rows or not _completed_25pct_scans(rows):
        raise HTTPException(400, "Select a completed 25% eight-hour scan")

    db.insert(
        "binance_matched_control_jobs",
        {
            "id": str(uuid.uuid4()),
            "scan_id": scan_id,
            "status": "queued",
            "research_purpose": "chatgpt_discovery",
            "controls_per_event": controls_per_event,
            "prior_days": prior_days,
            "horizons_minutes": horizons,
            "contamination_before_minutes": max(horizons),
            "contamination_after_minutes": 480,
            "min_entry_notional": min_entry_notional,
            "discovery_pct": 70,
            "validation_pct": 15,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/ten-day-context")
def create_ten_day_context(
    request: Request,
    matched_control_job_id: str = Form(...),
    research_mode: str = Form("fresh_staged"),
    horizons_minutes: str = Form("15,30,60,120,180,480"),
    min_entry_notional: float = Form(500),
) -> RedirectResponse:
    _auth(request)
    raise HTTPException(410, "This V1.3 release accepts only the locked external-validation workflow")
    if research_mode not in {"exploratory_reuse", "fresh_staged"}:
        raise HTTPException(400, "Invalid research mode")
    try:
        horizons = sorted({int(value.strip()) for value in horizons_minutes.split(",") if value.strip()})
    except ValueError as exc:
        raise HTTPException(400, "horizons_minutes must be comma-separated integers") from exc
    if not horizons or any(value < 5 or value > 720 for value in horizons):
        raise HTTPException(400, "decision horizons must be between 5 and 720 minutes")
    if min_entry_notional < 0:
        raise HTTPException(400, "min_entry_notional cannot be negative")
    db.insert(
        "binance_context_jobs",
        {
            "id": str(uuid.uuid4()),
            "matched_control_job_id": matched_control_job_id,
            "status": "queued",
            "research_mode": research_mode,
            "prior_days": 10,
            "horizons_minutes": horizons,
            "windows_minutes": [15,30,60,120,180,360,480,720,1440,2880,4320,7200,10080,14400],
            "min_entry_notional": min_entry_notional,
        },
    )
    return RedirectResponse("/", status_code=303)


@app.post("/baseline-context")
def create_baseline_context(
    request: Request,
    matched_control_job_id: str = Form(...),
    research_mode: str = Form("fresh_staged"),
    min_entry_notional: float = Form(500),
) -> RedirectResponse:
    _auth(request)
    raise HTTPException(410, "This V1.3 release accepts only the locked external-validation workflow")
    if research_mode not in {"exploratory_reuse", "fresh_staged"}:
        raise HTTPException(400, "Invalid research mode")
    if min_entry_notional < 0:
        raise HTTPException(400, "min_entry_notional cannot be negative")
    db.insert(
        "binance_baseline_context_jobs",
        {
            "id": str(uuid.uuid4()),
            "matched_control_job_id": matched_control_job_id,
            "status": "queued",
            "research_mode": research_mode,
            "prior_days": 10,
            "snapshot_offsets_minutes": [14400,10080,7200,4320,2880,1440,720,480,360,180,60,0],
            "pre_cross_horizons_minutes": [15,30,60,120,180,480],
            "min_entry_notional": min_entry_notional,
        },
    )
    return RedirectResponse("/", status_code=303)



@app.post("/resume-job")
def resume_job(
    request: Request,
    job_type: str = Form(...),
    job_id: str = Form(...),
) -> RedirectResponse:
    _auth(request)
    table_map = {
        "scan": "binance_scan_jobs",
        "matched": "binance_matched_control_jobs",
        "evaluation": "binance_external_validation_jobs",
    }
    table = table_map.get(job_type)
    if table is None:
        raise HTTPException(400, "Unsupported job type")
    rows = db.select(table, filters={"id": f"eq.{job_id}"}, limit=1)
    if not rows:
        raise HTTPException(404, "Job not found")
    row = rows[0]
    if row.get("status") not in {"failed", "queued"}:
        raise HTTPException(409, "Only failed or queued jobs can be resumed")
    if table in {"binance_scan_jobs", "binance_matched_control_jobs"}:
        if row.get("research_purpose") != "external_validation_c2_c4":
            raise HTTPException(400, "This release resumes only external-validation jobs")
    db.update(
        table,
        {"id": f"eq.{job_id}"},
        {
            "status": "queued",
            "completed_at": None,
            "heartbeat_at": None,
            "last_stage": "manual_resume_requested",
            "error_message": "Manual resume requested; durable checkpoint retained",
        },
    )
    return RedirectResponse("/", status_code=303)

def _csv_response(rows: list[dict[str, Any]], filename: str) -> StreamingResponse:
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=sorted({key for row in rows for key in row.keys()}))
        writer.writeheader()
        writer.writerows(rows)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/exports/events/{scan_id}.csv")
def events_csv(request: Request, scan_id: str) -> StreamingResponse:
    _auth(request)
    rows = db.select_all(
        "binance_gainer_events",
        filters={"scan_id": f"eq.{scan_id}", "sellability_pass": "eq.true"},
        order="event_date.asc,symbol.asc",
    )
    return _csv_response(rows, f"binance_saleable_25pct_8h_events_{scan_id}.csv")


@app.get("/exports/candidates/{scan_id}.csv")
def candidates_csv(request: Request, scan_id: str) -> StreamingResponse:
    _auth(request)
    rows = db.select_all(
        "binance_gainer_events", filters={"scan_id": f"eq.{scan_id}"}, order="event_date.asc,symbol.asc"
    )
    return _csv_response(rows, f"binance_all_25pct_8h_candidates_{scan_id}.csv")


@app.get("/exports/decisions/{scan_id}.csv")
def decisions_csv(request: Request, scan_id: str) -> StreamingResponse:
    _auth(request)
    rows = db.select_all(
        "binance_decision_observations",
        filters={"scan_id": f"eq.{scan_id}"},
        order="symbol.asc,decision_time_utc.asc",
    )
    return _csv_response(rows, f"binance_25pct_decisions_{scan_id}.csv")


@app.get("/exports/minutes/{event_id}.csv")
def minutes_csv(request: Request, event_id: str) -> StreamingResponse:
    _auth(request)
    rows = db.select_all(
        "binance_event_minute_bars", filters={"event_id": f"eq.{event_id}"}, order="open_time.asc"
    )
    return _csv_response(rows, f"binance_event_minutes_{event_id}.csv")


@app.get("/exports/agg-trades/{event_id}.csv")
def agg_trades_csv(request: Request, event_id: str) -> StreamingResponse:
    _auth(request)
    rows = db.select_all(
        "binance_event_agg_trades", filters={"event_id": f"eq.{event_id}"}, order="trade_time.asc"
    )
    return _csv_response(rows, f"binance_event_agg_trades_{event_id}.csv")


def _download(request: Request, table: str, file_id: str, missing: str) -> RedirectResponse:
    _auth(request)
    rows = db.select(table, filters={"id": f"eq.{file_id}"}, limit=1)
    if not rows:
        raise HTTPException(404, missing)
    return RedirectResponse(db.signed_url(rows[0]["storage_path"], expires_in=3600), status_code=302)


@app.get("/files/{file_id}")
def download_file(request: Request, file_id: str) -> RedirectResponse:
    return _download(request, "binance_research_files", file_id, "File not found")


@app.get("/matched-files/{file_id}")
def download_matched_file(request: Request, file_id: str) -> RedirectResponse:
    return _download(request, "binance_matched_control_files", file_id, "Matched-control file not found")


@app.get("/context-files/{file_id}")
def download_context_file(request: Request, file_id: str) -> RedirectResponse:
    return _download(request, "binance_context_files", file_id, "Ten-day context file not found")


@app.get("/baseline-context-files/{file_id}")
def download_baseline_context_file(request: Request, file_id: str) -> RedirectResponse:
    return _download(request, "binance_baseline_context_files", file_id, "Baseline-context file not found")


@app.get("/external-validation-files/{file_id}")
def download_external_validation_file(request: Request, file_id: str) -> RedirectResponse:
    return _download(request, "binance_external_validation_files", file_id, "External-validation file not found")


@app.get("/api/status")
def api_status(request: Request) -> dict[str, Any]:
    _auth(request)
    return {
        "version": APP_VERSION,
        "event_definition_version": EVENT_DEFINITION_VERSION,
        "fixed_threshold_pct": FIXED_THRESHOLD_PCT,
        "fixed_window_minutes": FIXED_WINDOW_MINUTES,
        "scans": db.select("binance_scan_jobs", order="created_at.desc", limit=20),
        "research_jobs": db.select("binance_research_jobs", order="created_at.desc", limit=20),
        "matched_control_jobs": db.select("binance_matched_control_jobs", order="created_at.desc", limit=20),
        "context_jobs": db.select("binance_context_jobs", order="created_at.desc", limit=20),
        "baseline_context_jobs": db.select("binance_baseline_context_jobs", order="created_at.desc", limit=20),
        "external_validation_jobs": db.select("binance_external_validation_jobs", order="created_at.desc", limit=20),
        "candidate_register_sha256": register_sha256(),
        "worker": db.select("binance_worker_heartbeats", filters={"worker_name": "eq.main"}, limit=1),
    }
