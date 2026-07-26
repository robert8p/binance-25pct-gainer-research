from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from .binance import BinanceClient
from .config import Settings
from .external_validation import ExternalValidationBuilder
from .matched_controls import MatchedControlBuilder
from .scanner import Scanner
from .supabase import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXTERNAL_PURPOSE = "external_validation_c2_c4"


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
            "started_at": datetime.now(timezone.utc).isoformat(),
            "error_message": None,
        },
    )
    fresh = db.select(table, filters={"id": f"eq.{row['id']}"}, limit=1)
    return fresh[0] if fresh and fresh[0]["status"] == "running" else None


def _recover_external_jobs(db: SupabaseClient) -> None:
    # Deliberately do not touch legacy discovery/context jobs. This V1.2 worker
    # only resumes jobs explicitly tagged for the fixed C2/C4 validation chain.
    for table in ("binance_scan_jobs", "binance_matched_control_jobs"):
        db.update(
            table,
            {"status": "eq.running", "research_purpose": f"eq.{EXTERNAL_PURPOSE}"},
            {
                "status": "queued",
                "started_at": None,
                "heartbeat_at": None,
                "error_message": "Requeued after V1.2 external-validation worker restart",
            },
        )
    db.update(
        "binance_external_validation_jobs",
        {"status": "eq.running"},
        {
            "status": "queued",
            "started_at": None,
            "heartbeat_at": None,
            "error_message": "Requeued after V1.2 external-validation worker restart",
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
    matched_controls = MatchedControlBuilder(db, binance, settings.temp_data_dir)
    external_validation = ExternalValidationBuilder(db, binance, settings.temp_data_dir)

    _recover_external_jobs(db)
    logger.info("V1.2 frozen C2/C4 external-validation worker started")

    while True:
        try:
            db.upsert(
                "binance_worker_heartbeats",
                [{"worker_name": "main", "heartbeat_at": datetime.now(timezone.utc).isoformat()}],
                on_conflict="worker_name",
            )

            scan_job = _claim(
                db,
                "binance_scan_jobs",
                extra_filters={"research_purpose": f"eq.{EXTERNAL_PURPOSE}"},
            )
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
                    _fail(db, "binance_scan_jobs", job_id, exc, "External-validation scan")
                continue

            matched_job = _claim(
                db,
                "binance_matched_control_jobs",
                extra_filters={"research_purpose": f"eq.{EXTERNAL_PURPOSE}"},
            )
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
                    _fail(db, "binance_matched_control_jobs", job_id, exc, "External-validation matched controls")
                continue

            validation_job = _claim(db, "binance_external_validation_jobs")
            if validation_job:
                job_id = str(validation_job["id"])
                try:
                    result = external_validation.run(validation_job)
                    warnings = result.get("failures", 0) > 0
                    db.update(
                        "binance_external_validation_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "status": "completed_with_warnings" if warnings else "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "samples_processed": result["samples_processed"],
                            "feature_rows": result["feature_rows"],
                            "usable_groups": result["usable_groups"],
                            "failures": result["failures"],
                            "overall_decision": result["overall_decision"],
                            "result_json": result,
                        },
                    )
                except Exception as exc:
                    _fail(db, "binance_external_validation_jobs", job_id, exc, "Frozen C2/C4 evaluation")
                continue
        except Exception:
            logger.exception("External-validation worker loop error")
        time.sleep(settings.poll_seconds)


if __name__ == "__main__":
    main()
