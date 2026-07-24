from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from .baseline_context import BaselineContextBuilder
from .binance import BinanceClient
from .config import Settings
from .context import TenDayContextBuilder
from .matched_controls import MatchedControlBuilder
from .research import ResearchBuilder
from .scanner import Scanner
from .supabase import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ACTIVE_JOB_TABLES = (
    "binance_scan_jobs",
    "binance_research_jobs",
    "binance_matched_control_jobs",
    "binance_context_jobs",
    "binance_baseline_context_jobs",
)


def _claim(db: SupabaseClient, table: str) -> dict | None:
    rows = db.select(table, filters={"status": "eq.queued"}, order="created_at.asc", limit=1)
    if not rows:
        return None
    row = rows[0]
    db.update(
        table,
        {"id": f"eq.{row['id']}", "status": "eq.queued"},
        {
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "error_message": None,
        },
    )
    fresh = db.select(table, filters={"id": f"eq.{row['id']}"}, limit=1)
    return fresh[0] if fresh and fresh[0]["status"] == "running" else None


def _recover_interrupted_jobs(db: SupabaseClient) -> None:
    for table in ACTIVE_JOB_TABLES:
        db.update(
            table,
            {"status": "eq.running"},
            {
                "status": "queued",
                "started_at": None,
                "heartbeat_at": None,
                "error_message": "Requeued automatically after worker restart",
            },
        )


def _fail(db: SupabaseClient, table: str, job_id: str, exc: Exception, label: str) -> None:
    logger.exception("%s failed", label)
    db.update(
        table,
        {"id": f"eq.{job_id}"},
        {
            "status": "failed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": str(exc)[:4000],
        },
    )


def main() -> None:
    settings = Settings.from_env()
    db = SupabaseClient(settings.supabase_url, settings.supabase_service_role_key, settings.storage_bucket)
    binance = BinanceClient(settings.binance_api_base_urls)
    scanner = Scanner(db, binance)
    research = ResearchBuilder(db, binance, settings.temp_data_dir)
    matched_controls = MatchedControlBuilder(db, binance, settings.temp_data_dir)
    context_builder = TenDayContextBuilder(db, binance, settings.temp_data_dir)
    baseline_builder = BaselineContextBuilder(db, binance, settings.temp_data_dir)

    _recover_interrupted_jobs(db)
    logger.info("25%% eight-hour research worker started; interrupted jobs recovered")

    while True:
        try:
            db.upsert(
                "binance_worker_heartbeats",
                [{"worker_name": "main", "heartbeat_at": datetime.now(timezone.utc).isoformat()}],
                on_conflict="worker_name",
            )

            scan_job = _claim(db, "binance_scan_jobs")
            if scan_job:
                job_id = str(scan_job["id"])
                try:
                    result = scanner.run(scan_job)
                    db.update(
                        "binance_scan_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if result["failures"] else "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _fail(db, "binance_scan_jobs", job_id, exc, "Scan")
                continue

            matched_job = _claim(db, "binance_matched_control_jobs")
            if matched_job:
                job_id = str(matched_job["id"])
                try:
                    result = matched_controls.run(matched_job)
                    warnings = (
                        result["failures"] > 0
                        or result["controls_created"] < result["controls_target"]
                        or result.get("quality_report", {}).get("event_entry_liquidity_failures", 0) > 0
                    )
                    db.update(
                        "binance_matched_control_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if warnings else "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "events_processed": result["events_processed"],
                            "controls_created": result["controls_created"],
                            "feature_rows": result["feature_rows"],
                            "failures": result["failures"],
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _fail(db, "binance_matched_control_jobs", job_id, exc, "Matched-control job")
                continue

            context_job = _claim(db, "binance_context_jobs")
            if context_job:
                job_id = str(context_job["id"])
                try:
                    result = context_builder.run(context_job)
                    warnings = result.get("failures", 0) > 0 or any(
                        key != "pass" and value
                        for key, value in (result.get("quality_report", {}).get("quality_counts", {}) or {}).items()
                    )
                    db.update(
                        "binance_context_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if warnings else "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "samples_processed": result["samples_processed"],
                            "feature_rows": result["feature_rows"],
                            "failures": result["failures"],
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _fail(db, "binance_context_jobs", job_id, exc, "Ten-day context job")
                continue

            baseline_job = _claim(db, "binance_baseline_context_jobs")
            if baseline_job:
                job_id = str(baseline_job["id"])
                try:
                    result = baseline_builder.run(baseline_job)
                    warnings = (
                        result.get("failures", 0) > 0
                        or any(
                            key != "pass" and value
                            for key, value in (result.get("quality_report", {}).get("quality_counts", {}) or {}).items()
                        )
                        or result.get("quality_report", {}).get("contaminated_controls", 0) > 0
                    )
                    db.update(
                        "binance_baseline_context_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if warnings else "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "samples_processed": result["samples_processed"],
                            "feature_rows": result["feature_rows"],
                            "continuation_rows": result["continuation_rows"],
                            "failures": result["failures"],
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _fail(db, "binance_baseline_context_jobs", job_id, exc, "Baseline-context job")
                continue

            research_job = _claim(db, "binance_research_jobs")
            if research_job:
                job_id = str(research_job["id"])
                try:
                    result = research.run(research_job)
                    db.update(
                        "binance_research_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if result["events_failed"] else "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _fail(db, "binance_research_jobs", job_id, exc, "Event archive job")
                continue
        except Exception:
            logger.exception("Worker loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
