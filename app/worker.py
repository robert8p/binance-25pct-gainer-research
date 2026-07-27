from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any

from .binance import BinanceClient
from .config import Settings
from .external_validation import ExternalValidationBuilder
from .matched_controls import MatchedControlBuilder
from .runtime import collect_memory, log_resources
from .scanner import Scanner
from .supabase import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXTERNAL_PURPOSE = "external_validation_c2_c4"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _claim(
    db: SupabaseClient,
    table: str,
    *,
    extra_filters: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    filters = {"status": "eq.queued", **(extra_filters or {})}
    rows = db.select(table, filters=filters, order="created_at.asc", limit=1)
    if not rows:
        return None
    row = rows[0]
    claim_filters = {"id": f"eq.{row['id']}", "status": "eq.queued", **(extra_filters or {})}
    db.update(
        table,
        claim_filters,
        {
            "status": "running",
            "started_at": row.get("started_at") or _now(),
            "heartbeat_at": _now(),
            "last_stage": "claimed_or_resumed",
        },
    )
    fresh = db.select(table, filters={"id": f"eq.{row['id']}"}, limit=1)
    return fresh[0] if fresh and fresh[0]["status"] == "running" else None


def _recover_table(
    db: SupabaseClient,
    table: str,
    *,
    max_auto_resumes: int,
    extra_filters: dict[str, str] | None = None,
) -> None:
    rows = db.select_all(
        table,
        filters={"status": "eq.running", **(extra_filters or {})},
        order="created_at.asc",
    )
    for row in rows:
        job_id = str(row["id"])
        next_resume = int(row.get("resume_count") or 0) + 1
        if next_resume > max_auto_resumes:
            db.update(
                table,
                {"id": f"eq.{job_id}", "status": "eq.running"},
                {
                    "status": "failed",
                    "completed_at": _now(),
                    "resume_count": next_resume,
                    "last_stage": "resume_limit_exceeded",
                    "error_message": (
                        f"Stopped after {next_resume - 1} automatic resumes. "
                        "The durable checkpoint remains available for diagnosis."
                    ),
                },
            )
        else:
            db.update(
                table,
                {"id": f"eq.{job_id}", "status": "eq.running"},
                {
                    "status": "queued",
                    "resume_count": next_resume,
                    "heartbeat_at": None,
                    "last_stage": "recovered_after_worker_restart",
                    "error_message": (
                        f"Worker restarted; resume {next_resume} will continue from the last durable checkpoint"
                    ),
                },
            )
            logger.warning("Recovered %s job %s for resume %s", table, job_id, next_resume)


def _recover_external_jobs(db: SupabaseClient, max_auto_resumes: int) -> None:
    for table in ("binance_scan_jobs", "binance_matched_control_jobs"):
        _recover_table(
            db,
            table,
            max_auto_resumes=max_auto_resumes,
            extra_filters={"research_purpose": f"eq.{EXTERNAL_PURPOSE}"},
        )
    _recover_table(
        db,
        "binance_external_validation_jobs",
        max_auto_resumes=max_auto_resumes,
    )


def _retry_or_fail(
    db: SupabaseClient,
    table: str,
    job: dict[str, Any],
    exc: Exception,
    label: str,
    *,
    max_auto_resumes: int,
) -> None:
    logger.exception("%s interrupted", label)
    job_id = str(job["id"])
    next_resume = int(job.get("resume_count") or 0) + 1
    payload: dict[str, Any] = {
        "resume_count": next_resume,
        "heartbeat_at": None,
        "error_message": str(exc)[:4000],
    }
    if next_resume <= max_auto_resumes:
        payload.update(
            {
                "status": "queued",
                "last_stage": "retry_queued_from_checkpoint",
                "completed_at": None,
            }
        )
        logger.warning(
            "%s job %s queued for checkpoint resume %s/%s",
            label,
            job_id,
            next_resume,
            max_auto_resumes,
        )
    else:
        payload.update(
            {
                "status": "failed",
                "last_stage": "resume_limit_exceeded",
                "completed_at": _now(),
            }
        )
    db.update(table, {"id": f"eq.{job_id}"}, payload)


def main() -> None:
    settings = Settings.from_env()
    db = SupabaseClient(settings.supabase_url, settings.supabase_service_role_key, settings.storage_bucket)
    binance = BinanceClient(settings.binance_api_base_urls)
    scanner = Scanner(
        db,
        binance,
        settings.temp_data_dir,
        persist_event_agg_trades=settings.persist_event_agg_trades,
        minimum_disk_free_bytes=settings.minimum_disk_free_bytes,
    )
    matched_controls = MatchedControlBuilder(
        db,
        binance,
        settings.temp_data_dir,
        minimum_disk_free_bytes=settings.minimum_disk_free_bytes,
    )
    external_validation = ExternalValidationBuilder(
        db,
        binance,
        settings.temp_data_dir,
        minimum_disk_free_bytes=settings.minimum_disk_free_bytes,
    )

    current: dict[str, str] = {}

    def graceful_shutdown(signum: int, _frame: Any) -> None:
        logger.warning("Received signal %s; preserving current checkpoint", signum)
        if current:
            try:
                table = current["table"]
                job_id = current["id"]
                rows = db.select(table, filters={"id": f"eq.{job_id}"}, limit=1)
                resume_count = int(rows[0].get("resume_count") or 0) + 1 if rows else 1
                db.update(
                    table,
                    {"id": f"eq.{job_id}"},
                    {
                        "status": "queued",
                        "resume_count": resume_count,
                        "heartbeat_at": None,
                        "last_stage": "graceful_shutdown_checkpoint_preserved",
                        "error_message": "Worker stopped gracefully; job will resume from its last checkpoint",
                    },
                )
            except Exception:
                logger.exception("Could not mark current job resumable during shutdown")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    _recover_external_jobs(db, settings.max_auto_resumes)
    logger.info("V1.3 memory-safe resumable C2/C4 external-validation worker started")
    log_resources("worker_start", path=settings.temp_data_dir)

    while True:
        try:
            db.upsert(
                "binance_worker_heartbeats",
                [{"worker_name": "main", "heartbeat_at": _now()}],
                on_conflict="worker_name",
            )

            scan_job = _claim(
                db,
                "binance_scan_jobs",
                extra_filters={"research_purpose": f"eq.{EXTERNAL_PURPOSE}"},
            )
            if scan_job:
                job_id = str(scan_job["id"])
                current.update({"table": "binance_scan_jobs", "id": job_id})
                try:
                    result = scanner.run(scan_job)
                    db.update(
                        "binance_scan_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if result["failures"] else "completed",
                            "completed_at": _now(),
                            "heartbeat_at": None,
                            "error_message": None,
                            "last_stage": "complete",
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _retry_or_fail(
                        db,
                        "binance_scan_jobs",
                        scan_job,
                        exc,
                        "External-validation scan",
                        max_auto_resumes=settings.max_auto_resumes,
                    )
                finally:
                    current.clear()
                    collect_memory()
                continue

            matched_job = _claim(
                db,
                "binance_matched_control_jobs",
                extra_filters={"research_purpose": f"eq.{EXTERNAL_PURPOSE}"},
            )
            if matched_job:
                job_id = str(matched_job["id"])
                current.update({"table": "binance_matched_control_jobs", "id": job_id})
                try:
                    result = matched_controls.run(matched_job)
                    warnings = result["controls_created"] < result["controls_target"]
                    db.update(
                        "binance_matched_control_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if warnings else "completed",
                            "completed_at": _now(),
                            "heartbeat_at": None,
                            "error_message": None,
                            "last_stage": "complete",
                            "events_processed": result["events_processed"],
                            "controls_created": result["controls_created"],
                            "feature_rows": 0,
                            "failures": result["failures"],
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _retry_or_fail(
                        db,
                        "binance_matched_control_jobs",
                        matched_job,
                        exc,
                        "External-validation matched controls",
                        max_auto_resumes=settings.max_auto_resumes,
                    )
                finally:
                    current.clear()
                    collect_memory()
                continue

            validation_job = _claim(db, "binance_external_validation_jobs")
            if validation_job:
                job_id = str(validation_job["id"])
                current.update({"table": "binance_external_validation_jobs", "id": job_id})
                try:
                    result = external_validation.run(validation_job)
                    db.update(
                        "binance_external_validation_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed",
                            "completed_at": _now(),
                            "heartbeat_at": None,
                            "error_message": None,
                            "last_stage": "complete",
                            "samples_processed": result["samples_processed"],
                            "feature_rows": result["feature_rows"],
                            "usable_groups": result["usable_groups"],
                            "failures": result["failures"],
                            "overall_decision": result["overall_decision"],
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _retry_or_fail(
                        db,
                        "binance_external_validation_jobs",
                        validation_job,
                        exc,
                        "Frozen C2/C4 evaluation",
                        max_auto_resumes=settings.max_auto_resumes,
                    )
                finally:
                    current.clear()
                    collect_memory()
                continue
        except Exception:
            logger.exception("External-validation worker loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
