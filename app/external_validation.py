from __future__ import annotations

import json
import math
import shutil
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .baseline_context import (
    compute_pre_cross_feature_row,
    control_sample,
    event_sample,
    pseudo_window_audit,
)
from .binance import BinanceClient, sha256_file
from .context import REFERENCE_SYMBOLS, _zip_directory
from .frozen_candidates import (
    CANDIDATES,
    DECISION_HORIZON_MINUTES,
    EXTERNAL_WINDOW_END_EXCLUSIVE,
    EXTERNAL_WINDOW_START,
    PASS_CRITERIA,
    PARENT_CANDIDATE_REGISTER_SHA256,
    evaluate_candidates,
    register_payload,
    register_sha256,
    write_register,
)
from .matched_controls import MinuteArchiveCache, parse_datetime
from .package_utils import build_grouped_zip_parts
from .runtime import collect_memory, ensure_disk_headroom, log_resources
from .supabase import SupabaseClient

RNG_SEED = 20260726
BOOTSTRAP_REPETITIONS = 10_000
RANDOMISATION_REPETITIONS = 50_000


def _json_ready(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _event_date(row: pd.Series) -> str:
    value = row.get("cross_anchor_time") or row.get("anchor_time")
    return parse_datetime(value).date().isoformat()


def _group_table(frame: pd.DataFrame, pass_column: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_id, group in frame.groupby("match_group_id", sort=False):
        events = group[group["label"] == 1]
        controls = group[group["label"] == 0]
        if len(events) != 1 or controls.empty:
            continue
        event = events.iloc[0]
        event_pass = float(bool(event[pass_column]))
        control_pass_fraction = float(controls[pass_column].astype(bool).mean())
        rows.append(
            {
                "match_group_id": str(group_id),
                "symbol": str(event["symbol"]),
                "event_date": _event_date(event),
                "event_pass": event_pass,
                "controls_available": int(len(controls)),
                "controls_passing": int(controls[pass_column].astype(bool).sum()),
                "control_pass_fraction": control_pass_fraction,
                "matched_difference": event_pass - control_pass_fraction,
            }
        )
    result = pd.DataFrame(rows)
    if not result.empty:
        result["month"] = result["event_date"].astype(str).str.slice(0, 7)
    return result


def _cluster_bootstrap_ci(
    groups: pd.DataFrame,
    cluster_column: str,
    *,
    repetitions: int = BOOTSTRAP_REPETITIONS,
    seed: int = RNG_SEED,
) -> tuple[float | None, float | None]:
    if groups.empty or cluster_column not in groups:
        return None, None
    summary = groups.groupby(cluster_column, dropna=True)["matched_difference"].agg(["sum", "count"])
    if len(summary) < 2:
        return None, None
    sums = summary["sum"].to_numpy(float)
    counts = summary["count"].to_numpy(float)
    cluster_count = len(summary)
    rng = np.random.default_rng(seed + (1 if cluster_column == "symbol" else 0))
    values = np.empty(repetitions, dtype=float)
    batch_size = 500
    cursor = 0
    while cursor < repetitions:
        size = min(batch_size, repetitions - cursor)
        chosen = rng.integers(0, cluster_count, size=(size, cluster_count))
        numerator = sums[chosen].sum(axis=1)
        denominator = counts[chosen].sum(axis=1)
        values[cursor : cursor + size] = numerator / denominator
        cursor += size
    return float(np.quantile(values, 0.025)), float(np.quantile(values, 0.975))


def _within_group_randomisation_pvalue(
    frame: pd.DataFrame,
    pass_column: str,
    observed: float,
    *,
    repetitions: int = RANDOMISATION_REPETITIONS,
    seed: int = RNG_SEED,
) -> float | None:
    group_sizes: list[float] = []
    group_successes: list[float] = []
    for _, group in frame.groupby("match_group_id", sort=False):
        if int((group["label"] == 1).sum()) != 1 or int((group["label"] == 0).sum()) < 1:
            continue
        passes = group[pass_column].astype(bool).astype(float).to_numpy()
        group_sizes.append(float(len(passes)))
        group_successes.append(float(passes.sum()))
    if not group_sizes:
        return None
    n = np.asarray(group_sizes, dtype=float)
    successes = np.asarray(group_successes, dtype=float)
    probabilities = successes / n
    rng = np.random.default_rng(seed + (2 if pass_column == "C4_pass" else 0))
    exceed = 0
    batch_size = 1000
    cursor = 0
    while cursor < repetitions:
        size = min(batch_size, repetitions - cursor)
        pseudo_event = (rng.random((size, len(n))) < probabilities).astype(float)
        control_fraction = (successes[None, :] - pseudo_event) / (n[None, :] - 1.0)
        simulated = (pseudo_event - control_fraction).mean(axis=1)
        exceed += int((simulated >= observed - 1e-15).sum())
        cursor += size
    return float((exceed + 1) / (repetitions + 1))


def _symbol_concentration(groups: pd.DataFrame) -> tuple[float | None, str | None]:
    if groups.empty:
        return None, None
    positive = groups.assign(positive=groups["matched_difference"].clip(lower=0.0))
    total = float(positive["positive"].sum())
    if total <= 0:
        return None, None
    by_symbol = positive.groupby("symbol", as_index=False)["positive"].sum()
    winner = by_symbol.sort_values("positive", ascending=False).iloc[0]
    return float(winner["positive"] / total), str(winner["symbol"])


def _monthly_table(groups: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if groups.empty:
        return pd.DataFrame(rows)
    for month, part in groups.groupby("month", sort=True):
        event_hit = float(part["event_pass"].mean())
        control_pass = float(part["control_pass_fraction"].mean())
        lift = event_hit / control_pass if control_pass > 0 else (math.inf if event_hit > 0 else None)
        rows.append(
            {
                "month": month,
                "usable_groups": int(len(part)),
                "event_hit_rate": event_hit,
                "matched_control_pass_rate": control_pass,
                "matched_lift": lift,
                "mean_matched_difference": float(part["matched_difference"].mean()),
            }
        )
    return pd.DataFrame(rows)


def evaluate_rule(frame: pd.DataFrame, candidate_id: str) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    pass_column = f"{candidate_id}_pass"
    groups = _group_table(frame, pass_column)
    if groups.empty:
        return {
            "candidate_id": candidate_id,
            "status": "insufficient_data",
            "usable_matched_groups": 0,
        }, groups, pd.DataFrame()

    event_hit = float(groups["event_pass"].mean())
    control_pass = float(groups["control_pass_fraction"].mean())
    raw_controls = frame[frame["label"] == 0]
    raw_control_pass = float(raw_controls[pass_column].astype(bool).mean()) if not raw_controls.empty else None
    difference = float(groups["matched_difference"].mean())
    lift = event_hit / control_pass if control_pass > 0 else (math.inf if event_hit > 0 else None)
    date_low, date_high = _cluster_bootstrap_ci(groups, "event_date")
    symbol_low, symbol_high = _cluster_bootstrap_ci(groups, "symbol")
    pvalue = _within_group_randomisation_pvalue(frame, pass_column, difference)
    max_symbol_share, max_symbol = _symbol_concentration(groups)
    monthly = _monthly_table(groups)
    eligible_monthly = monthly[
        monthly["usable_groups"] >= PASS_CRITERIA["minimum_groups_per_eligible_month"]
    ].copy()
    positive_month_fraction = (
        float((eligible_monthly["mean_matched_difference"] > 0).mean()) if not eligible_monthly.empty else None
    )
    finite_month_lifts = eligible_monthly["matched_lift"].replace([np.inf, -np.inf], np.nan).dropna()
    median_month_lift = float(finite_month_lifts.median()) if not finite_month_lifts.empty else None

    criteria = {
        "usable_groups": len(groups) >= PASS_CRITERIA["minimum_usable_matched_groups"],
        "event_dates": groups["event_date"].nunique() >= PASS_CRITERIA["minimum_event_dates"],
        "symbols": groups["symbol"].nunique() >= PASS_CRITERIA["minimum_symbols"],
        "matched_lift": lift is not None and lift >= PASS_CRITERIA["minimum_matched_lift"],
        "date_cluster_ci": date_low is not None and date_low > 0.0,
        "symbol_cluster_ci": symbol_low is not None and symbol_low > 0.0,
        "symbol_concentration": max_symbol_share is not None and max_symbol_share <= PASS_CRITERIA["maximum_single_symbol_share_of_positive_advantage"],
        "eligible_month_count": len(eligible_monthly) >= PASS_CRITERIA["minimum_eligible_months"],
        "monthly_positive_fraction": positive_month_fraction is not None and positive_month_fraction >= PASS_CRITERIA["minimum_fraction_of_eligible_months_with_positive_effect"],
        "median_month_lift": median_month_lift is not None and median_month_lift >= PASS_CRITERIA["minimum_median_eligible_month_lift"],
    }
    result = {
        "candidate_id": candidate_id,
        "status": "pass" if all(criteria.values()) else "fail",
        "usable_matched_groups": int(len(groups)),
        "event_dates": int(groups["event_date"].nunique()),
        "symbols": int(groups["symbol"].nunique()),
        "event_hit_rate": event_hit,
        "matched_control_pass_rate": control_pass,
        "raw_control_pass_rate": raw_control_pass,
        "matched_lift": lift,
        "mean_matched_difference": difference,
        "within_group_randomisation_pvalue_one_sided": pvalue,
        "date_cluster_ci_95_low": date_low,
        "date_cluster_ci_95_high": date_high,
        "symbol_cluster_ci_95_low": symbol_low,
        "symbol_cluster_ci_95_high": symbol_high,
        "maximum_single_symbol_share_of_positive_advantage": max_symbol_share,
        "maximum_contributing_symbol": max_symbol,
        "eligible_months": int(len(eligible_monthly)),
        "positive_eligible_month_fraction": positive_month_fraction,
        "median_eligible_month_lift": median_month_lift,
        "criteria": criteria,
    }
    return result, groups, monthly


def evaluate_external_validation(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], dict[str, pd.DataFrame]]:
    evaluated = evaluate_candidates(frame)
    result_rows: list[dict[str, Any]] = []
    detail_tables: dict[str, pd.DataFrame] = {}
    for candidate in CANDIDATES:
        candidate_id = str(candidate["id"])
        result, groups, monthly = evaluate_rule(evaluated, candidate_id)
        result["candidate_name"] = candidate["name"]
        result["definition"] = candidate["definition"]
        result_rows.append(result)
        detail_tables[f"{candidate_id}_groups"] = groups
        detail_tables[f"{candidate_id}_monthly"] = monthly
    overall = {
        "candidate_register_sha256": register_sha256(),
        "parent_candidate_register_sha256": PARENT_CANDIDATE_REGISTER_SHA256,
        "decision_horizon_minutes": DECISION_HORIZON_MINUTES,
        "candidate_results": result_rows,
        "overall_decision": (
            "one_or_more_candidates_passed_external_validation"
            if any(row.get("status") == "pass" for row in result_rows)
            else "no_candidate_passed_external_validation"
        ),
        "sealed_test_instruction": "Do not open the prior sealed-test package at this stage.",
        "trading_instruction": "This validates association only; it is not an executable trading backtest.",
    }
    return evaluated, overall, detail_tables


def _report_markdown(overall: dict[str, Any]) -> str:
    lines = [
        "# Frozen C2/C4 external-validation result",
        "",
        "This release evaluated only the two ChatGPT-frozen candidates. It did not search for new patterns, alter thresholds, combine C2 and C4, or inspect the previous sealed-test package.",
        "",
        f"- Candidate register SHA-256: `{overall['candidate_register_sha256']}`",
        f"- Parent register SHA-256: `{overall['parent_candidate_register_sha256']}`",
        f"- Overall result: **{overall['overall_decision']}**",
        "",
        "## Candidate results",
        "",
        "| Candidate | Status | Groups | Dates | Symbols | Event hit | Control pass | Lift | Date CI low | Symbol CI low |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in overall["candidate_results"]:
        def f(value: Any) -> str:
            if value is None:
                return "—"
            if isinstance(value, float):
                if math.isinf(value):
                    return "∞"
                return f"{value:.4f}"
            return str(value)
        lines.append(
            "| {candidate_id} | {status} | {usable_matched_groups} | {event_dates} | {symbols} | {event_hit_rate} | {matched_control_pass_rate} | {matched_lift} | {date_cluster_ci_95_low} | {symbol_cluster_ci_95_low} |".format(
                **{key: f(value) for key, value in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "A pass means the frozen association survived the pre-specified external historical criteria. It does not establish entry execution, alert frequency, profit targets, stops, fees, slippage, position conflicts or drawdown.",
            "",
            "The next stage after a pass is a continuous historical execution backtest with entry and exit logic frozen before any sealed-test review.",
        ]
    )
    return "\n".join(lines) + "\n"


class ExternalValidationBuilder:
    def __init__(
        self,
        db: SupabaseClient,
        binance: BinanceClient,
        temp_root: Path,
        *,
        minimum_disk_free_bytes: int = 750_000_000,
    ):
        self.db = db
        self.binance = binance
        self.temp_root = temp_root
        self.minimum_disk_free_bytes = minimum_disk_free_bytes
        self.cache = MinuteArchiveCache(binance, temp_root)

    def _validate_source(self, matched_job: dict[str, Any]) -> dict[str, Any]:
        if str(matched_job.get("research_purpose") or "") != "external_validation_c2_c4":
            raise ValueError("Select a V1.3 external-validation matched-control job")
        if int(matched_job.get("controls_per_event") or 0) != 5:
            raise ValueError("External validation requires exactly five requested controls per event")
        if int(matched_job.get("prior_days") or 0) != 10:
            raise ValueError("External validation requires ten predictor-history days")
        if int(matched_job.get("contamination_before_minutes") or 0) < 480:
            raise ValueError("External validation requires at least 480 minutes of pre-anchor contamination protection")
        if int(matched_job.get("contamination_after_minutes") or 0) < 480:
            raise ValueError("External validation requires at least 480 minutes of post-anchor contamination protection")
        scans = self.db.select("binance_scan_jobs", filters={"id": f"eq.{matched_job['scan_id']}"}, limit=1)
        if not scans:
            raise RuntimeError("Source scan not found")
        scan = scans[0]
        if str(scan.get("research_purpose") or "") != "external_validation_c2_c4":
            raise ValueError("Source scan is not tagged for frozen C2/C4 external validation")
        if str(scan.get("window_start_date")) != EXTERNAL_WINDOW_START:
            raise ValueError(f"External validation start must be {EXTERNAL_WINDOW_START}")
        if str(scan.get("window_end_date_exclusive")) != EXTERNAL_WINDOW_END_EXCLUSIVE:
            raise ValueError(f"External validation end-exclusive must be {EXTERNAL_WINDOW_END_EXCLUSIVE}")
        if float(scan.get("threshold_pct") or 0) != 25.0 or int(scan.get("window_minutes") or 0) != 480:
            raise ValueError("Source scan must use the fixed 25% / 480-minute event definition")
        return scan

    def _samples(self, matched_job: dict[str, Any]) -> list[dict[str, Any]]:
        scan_id = str(matched_job["scan_id"])
        events = self.db.select_all(
            "binance_gainer_events",
            filters={"scan_id": f"eq.{scan_id}", "sellability_pass": "eq.true"},
            order="event_date.asc,symbol.asc",
        )
        matches = self.db.select_all(
            "binance_control_matches",
            filters={"matched_control_job_id": f"eq.{matched_job['id']}"},
            order="symbol.asc,control_anchor_time.asc",
        )
        event_by_id = {str(event["id"]): event for event in events}
        samples: list[dict[str, Any]] = []
        for event in events:
            sample = event_sample(event, "external_validation")
            samples.append(sample)
        for match in matches:
            event = event_by_id.get(str(match["event_id"]))
            if event is None:
                continue
            sample = control_sample(match, event)
            sample["split"] = "external_validation"
            samples.append(sample)
        return samples

    def _persist_manifest(self, job_id: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        payload = []
        for row in rows:
            payload.append(
                {
                    "external_validation_job_id": job_id,
                    "symbol": str(row.get("symbol") or ""),
                    "source_date": str(row.get("date") or "1970-01-01"),
                    "manifest_json": row,
                }
            )
        self.db.upsert(
            "binance_external_validation_source_manifest",
            payload,
            on_conflict="external_validation_job_id,symbol,source_date",
        )

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["id"])
        matched_job_id = str(job["matched_control_job_id"])
        matched_rows = self.db.select(
            "binance_matched_control_jobs", filters={"id": f"eq.{matched_job_id}"}, limit=1
        )
        if not matched_rows:
            raise RuntimeError("Matched-control job not found")
        matched_job = matched_rows[0]
        if matched_job.get("status") not in {"completed", "completed_with_warnings"}:
            raise RuntimeError("Matched-control job must be completed")
        scan = self._validate_source(matched_job)
        samples = self._samples(matched_job)
        if not samples:
            raise RuntimeError("No event/control samples found")

        crosses = [parse_datetime(sample["cross_anchor_time"]) for sample in samples]
        load_start = min(crosses).date() - timedelta(days=13)
        load_end = max(crosses).date() + timedelta(days=1)
        existing_rows = self.db.select_all(
            "binance_external_validation_feature_rows",
            filters={"external_validation_job_id": f"eq.{job_id}"},
            order="symbol.asc,sample_id.asc",
        )
        completed_sample_ids = {str(row["sample_id"]) for row in existing_rows}
        checkpoint = dict(job.get("checkpoint_json") or {})
        checkpoint.update(
            {
                "schema_version": 1,
                "phase": "feature_generation",
                "samples_total": len(samples),
                "samples_completed": len(completed_sample_ids),
            }
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        self.db.update(
            "binance_external_validation_jobs",
            {"id": f"eq.{job_id}"},
            {
                "samples_total": len(samples),
                "events_total": sum(sample["label"] == 1 for sample in samples),
                "controls_total": sum(sample["label"] == 0 for sample in samples),
                "samples_processed": len(completed_sample_ids),
                "feature_rows": len(completed_sample_ids),
                "heartbeat_at": now_iso,
                "checkpoint_json": checkpoint,
                "last_stage": "feature_generation",
                "last_checkpoint_at": now_iso,
            },
        )
        log_resources(
            "external_validation_resume",
            path=self.temp_root,
            extra={"samples_completed": len(completed_sample_ids), "samples_total": len(samples)},
        )

        reference_frames: dict[str, pd.DataFrame] = {}
        for symbol in REFERENCE_SYMBOLS:
            ensure_disk_headroom(self.temp_root, self.minimum_disk_free_bytes)
            loaded = self.cache.load_symbol(
                symbol,
                load_start,
                load_end,
                required_columns=("close",),
            )
            self._persist_manifest(job_id, loaded.source_manifest)
            reference_frames[symbol] = loaded.frame[["close", "observed"]].copy()
            del loaded
            collect_memory()

        by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for sample in samples:
            if str(sample["sample_id"]) not in completed_sample_ids:
                by_symbol[str(sample["symbol"])].append(sample)

        processed = len(completed_sample_ids)
        for symbol, symbol_samples in sorted(by_symbol.items()):
            ensure_disk_headroom(self.temp_root, self.minimum_disk_free_bytes)
            loaded = None
            frame = None
            try:
                loaded = self.cache.load_symbol(
                    symbol,
                    load_start,
                    load_end,
                    required_columns=(
                        "open",
                        "high",
                        "low",
                        "close",
                        "quote_volume",
                        "trade_count",
                        "taker_buy_quote_volume",
                    ),
                )
                frame = loaded.frame
                self._persist_manifest(job_id, loaded.source_manifest)
                feature_payload: list[dict[str, Any]] = []
                for sample in symbol_samples:
                    audit = pseudo_window_audit(frame, sample)
                    audit_row = {
                        "sample_id": sample["sample_id"],
                        "match_group_id": sample["match_group_id"],
                        "sample_type": sample["sample_type"],
                        "symbol": sample["symbol"],
                        "baseline_anchor_time": sample["baseline_anchor_time"],
                        "cross_anchor_time": sample["cross_anchor_time"],
                        **audit,
                    }
                    feature = compute_pre_cross_feature_row(
                        frame,
                        sample=sample,
                        horizon_minutes=DECISION_HORIZON_MINUTES,
                        prior_days=10,
                        min_entry_notional=500.0,
                        reference_frames=reference_frames,
                        audit=audit,
                    )
                    feature_payload.append(
                        {
                            "external_validation_job_id": job_id,
                            "sample_id": str(sample["sample_id"]),
                            "match_group_id": str(sample["match_group_id"]),
                            "sample_type": str(sample["sample_type"]),
                            "label": int(sample["label"]),
                            "symbol": symbol,
                            "feature_json": {key: _json_ready(value) for key, value in feature.items()},
                            "audit_json": {key: _json_ready(value) for key, value in audit_row.items()},
                        }
                    )
                self.db.upsert(
                    "binance_external_validation_feature_rows",
                    feature_payload,
                    on_conflict="external_validation_job_id,sample_id",
                    chunk_size=100,
                )
                processed += len(feature_payload)
                completed_sample_ids.update(str(row["sample_id"]) for row in feature_payload)
                checkpoint.update(
                    {
                        "phase": "feature_generation",
                        "last_symbol": symbol,
                        "samples_completed": processed,
                    }
                )
                now_iso = datetime.now(timezone.utc).isoformat()
                self.db.update(
                    "binance_external_validation_jobs",
                    {"id": f"eq.{job_id}"},
                    {
                        "samples_processed": processed,
                        "feature_rows": processed,
                        "heartbeat_at": now_iso,
                        "checkpoint_json": checkpoint,
                        "last_stage": "feature_generation",
                        "last_unit": symbol,
                        "last_checkpoint_at": now_iso,
                    },
                )
            except Exception as exc:
                self.db.insert(
                    "binance_external_validation_issues",
                    {
                        "external_validation_job_id": job_id,
                        "symbol": symbol,
                        "stage": "feature_generation",
                        "message": str(exc)[:4000],
                    },
                )
                raise
            finally:
                del frame
                del loaded
                collect_memory()
                log_resources(
                    "external_validation_symbol_checkpoint",
                    path=self.temp_root,
                    extra={"symbol": symbol, "samples_completed": processed},
                )

        # The durable feature table is the source of truth. If the process dies
        # during evaluation or packaging, the next run skips all completed symbols
        # and rebuilds only the final outputs.
        durable_rows = self.db.select_all(
            "binance_external_validation_feature_rows",
            filters={"external_validation_job_id": f"eq.{job_id}"},
            order="symbol.asc,sample_id.asc",
        )
        feature_rows = [row["feature_json"] for row in durable_rows]
        audit_rows = [row.get("audit_json") or {} for row in durable_rows]
        feature_df = pd.DataFrame(feature_rows)
        audit_df = pd.DataFrame(audit_rows)
        if feature_df.empty:
            raise RuntimeError("No external-validation feature rows were produced")
        feature_df["pseudo_window_contaminated_control"] = feature_df[
            "pseudo_window_contaminated_control"
        ].fillna(False).astype(bool)
        eligible = feature_df[
            (feature_df["feature_quality_status"] == "pass")
            & ~(
                (feature_df["label"] == 0)
                & feature_df["pseudo_window_contaminated_control"]
            )
        ].copy()
        group_counts = eligible.groupby("match_group_id")["label"].agg(
            events=lambda values: int((values == 1).sum()),
            controls=lambda values: int((values == 0).sum()),
        )
        usable_group_ids = group_counts[
            (group_counts["events"] == 1) & (group_counts["controls"] >= 1)
        ].index.astype(str)
        eligible = eligible[
            eligible["match_group_id"].astype(str).isin(set(usable_group_ids))
        ].copy()
        checkpoint.update(
            {
                "phase": "evaluation_and_packaging",
                "samples_completed": len(durable_rows),
                "usable_groups": int(len(usable_group_ids)),
            }
        )
        self.db.update(
            "binance_external_validation_jobs",
            {"id": f"eq.{job_id}"},
            {
                "checkpoint_json": checkpoint,
                "last_stage": "evaluation_and_packaging",
                "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        evaluated, overall, detail_tables = evaluate_external_validation(eligible)
        source_records = self.db.select_all(
            "binance_external_validation_source_manifest",
            filters={"external_validation_job_id": f"eq.{job_id}"},
            order="symbol.asc,source_date.asc",
        )
        source_manifest = [row.get("manifest_json") or {} for row in source_records]
        overall.update(
            {
                "source_scan_id": str(scan["id"]),
                "source_matched_control_job_id": matched_job_id,
                "external_validation_job_id": job_id,
                "window_start": EXTERNAL_WINDOW_START,
                "window_end_exclusive": EXTERNAL_WINDOW_END_EXCLUSIVE,
                "samples_total": len(samples),
                "samples_processed": len(durable_rows),
                "raw_feature_rows": len(feature_df),
                "eligible_feature_rows": len(eligible),
                "usable_matched_groups": int(len(usable_group_ids)),
                "quality_status_counts": feature_df["feature_quality_status"].value_counts(dropna=False).to_dict(),
                "contaminated_controls": int(
                    ((feature_df["label"] == 0) & feature_df["pseudo_window_contaminated_control"]).sum()
                ),
                "symbol_failures": 0,
                "memory_model": "one event-symbol frame at a time; durable per-sample feature rows",
                "resume_model": "sample-level Supabase checkpoints",
            }
        )

        work = self.temp_root / "external-validation-output" / job_id
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True, exist_ok=True)
        try:
            uploaded: list[dict[str, Any]] = []
            storage_prefix = f"external-validation/{job_id}"

            def upload(path: Path, role: str) -> str:
                storage_path = f"{storage_prefix}/{path.name}"
                self.db.upload_file(storage_path, path, "application/zip")
                record = {
                    "external_validation_job_id": job_id,
                    "storage_path": storage_path,
                    "filename": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "content_type": "application/zip",
                    "role": role,
                }
                self.db.upsert(
                    "binance_external_validation_files",
                    [record],
                    on_conflict="external_validation_job_id,storage_path",
                )
                uploaded.append(record)
                return storage_path

            group_ids = evaluated["match_group_id"].astype(str).drop_duplicates().tolist()

            def feature_writer(folder: Path, selected_groups: list[str]) -> None:
                selected = set(selected_groups)
                chunk = evaluated[
                    evaluated["match_group_id"].astype(str).isin(selected)
                ].copy()
                folder.mkdir(parents=True, exist_ok=True)
                chunk.to_parquet(
                    folder / "external_validation_features.parquet", index=False, compression="zstd"
                )
                audit_chunk = audit_df[
                    audit_df["match_group_id"].astype(str).isin(selected)
                ].copy()
                audit_chunk.to_csv(folder / "control_contamination_audit.csv", index=False)
                write_register(folder / "FROZEN_CANDIDATE_REGISTER.json")
                (folder / "README.md").write_text(
                    "# External validation feature part\n\n"
                    "Load every numbered part before independently checking the fixed C2 and C4 results. "
                    "Do not search for new patterns or alter thresholds in this dataset.\n",
                    encoding="utf-8",
                )

            feature_parts = build_grouped_zip_parts(
                work_dir=work,
                base_name="external_validation_features",
                group_ids=group_ids,
                writer=feature_writer,
            )
            feature_paths = [upload(path, "external_validation_features") for path in feature_parts]

            index = work / "index"
            index.mkdir(parents=True, exist_ok=True)
            write_register(index / "FROZEN_CANDIDATE_REGISTER.json")
            (index / "CANDIDATE_REGISTER_SHA256.txt").write_text(
                register_sha256() + "  FROZEN_CANDIDATE_REGISTER.json\n", encoding="utf-8"
            )
            (index / "parent_candidate_register_sha256.txt").write_text(
                PARENT_CANDIDATE_REGISTER_SHA256 + "\n", encoding="utf-8"
            )
            (index / "external_validation_result.json").write_text(
                json.dumps(overall, indent=2, default=_json_ready), encoding="utf-8"
            )
            pd.DataFrame(overall["candidate_results"]).drop(columns=["criteria"], errors="ignore").to_csv(
                index / "candidate_results.csv", index=False
            )
            for key, table in detail_tables.items():
                table.to_csv(index / f"{key}.csv", index=False)
            pd.DataFrame(source_manifest).drop_duplicates().to_csv(
                index / "source_archive_manifest.csv", index=False
            )
            pd.DataFrame(uploaded).to_csv(index / "feature_package_manifest.csv", index=False)
            (index / "PASS_FAIL_REPORT.md").write_text(_report_markdown(overall), encoding="utf-8")
            (index / "CHATGPT_EXTERNAL_VALIDATION_PROMPT.md").write_text(
                "# ChatGPT external-validation instruction\n\n"
                "Independently verify only C2 and C4 exactly as defined in FROZEN_CANDIDATE_REGISTER.json. "
                "Load all feature parts, reproduce quality filtering and matched-group statistics, inspect "
                "date/symbol/month concentration, and give a pass/fail conclusion without threshold changes. "
                "Do not search for new patterns, combine the rules, inspect any prior sealed-test package, "
                "or infer profitability.\n",
                encoding="utf-8",
            )
            index_zip = work / "external_validation_index.zip"
            _zip_directory(index, index_zip)
            index_storage = upload(index_zip, "external_validation_index")
            checkpoint.update({"phase": "complete", "feature_package_count": len(feature_paths)})
            self.db.update(
                "binance_external_validation_jobs",
                {"id": f"eq.{job_id}"},
                {
                    "checkpoint_json": checkpoint,
                    "last_stage": "complete",
                    "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return {
                "samples_total": len(samples),
                "samples_processed": len(durable_rows),
                "feature_rows": len(feature_df),
                "usable_groups": int(len(usable_group_ids)),
                "failures": 0,
                "candidate_register_sha256": register_sha256(),
                "index_storage_path": index_storage,
                "feature_package_paths": feature_paths,
                "overall_decision": overall["overall_decision"],
                "candidate_results": overall["candidate_results"],
            }
        finally:
            shutil.rmtree(work, ignore_errors=True)
            collect_memory()

