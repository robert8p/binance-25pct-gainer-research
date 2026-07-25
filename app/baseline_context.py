from __future__ import annotations

import json
import shutil
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .analysis_contract import write_analysis_contract
from .binance import BinanceClient, sha256_file
from .context import CONTEXT_WINDOWS, REFERENCE_SYMBOLS, _zip_directory, compute_context_feature_row
from .matched_controls import (
    MinuteArchiveCache,
    SPLITS,
    _json_ready,
    assign_temporal_splits,
    floor_minute,
    parse_datetime,
    safe_pct,
)
from .supabase import SupabaseClient

# Fixed measurement grids are data-design choices, not candidate signal thresholds.
BASELINE_SNAPSHOT_OFFSETS = (14400, 10080, 7200, 4320, 2880, 1440, 720, 480, 360, 180, 60, 0)
PRE_CROSS_HORIZONS = (15, 30, 60, 120, 180, 480)

def _event_cross_minute(event: dict[str, Any]) -> datetime:
    return floor_minute(parse_datetime(event.get("first_cross_trade_time") or event["first_cross_time"]))


def _event_baseline_minute(event: dict[str, Any]) -> tuple[datetime, str]:
    if event.get("baseline_trade_time") and not bool(event.get("baseline_trade_unresolved")):
        return floor_minute(parse_datetime(event["baseline_trade_time"])), "resolved_trade_minute"
    if event.get("baseline_time"):
        return floor_minute(parse_datetime(event["baseline_time"])), "scanner_minute_fallback"
    raise ValueError(f"Event {event.get('id')} has no baseline time")


def _event_duration_minutes(event: dict[str, Any]) -> int:
    value = event.get("minutes_baseline_open_to_cross_open")
    if value is not None:
        minutes = int(value)
    else:
        baseline, _ = _event_baseline_minute(event)
        minutes = int((_event_cross_minute(event) - baseline).total_seconds() // 60)
    if not 1 <= minutes <= 480:
        raise ValueError(f"Event {event.get('id')} baseline-to-cross duration {minutes} is outside 1..480 minutes")
    return minutes


def event_sample(event: dict[str, Any], split: str) -> dict[str, Any]:
    baseline, method = _event_baseline_minute(event)
    cross = _event_cross_minute(event)
    duration = _event_duration_minutes(event)
    return {
        "sample_id": f"event:{event['id']}",
        "match_group_id": str(event["id"]),
        "sample_type": "event",
        "label": 1,
        "split": split,
        "symbol": str(event["symbol"]),
        "base_asset": event.get("base_asset"),
        "quote_asset": event.get("quote_asset"),
        "event_id": str(event["id"]),
        "control_id": None,
        "control_rank": None,
        "baseline_anchor_time": baseline.isoformat(),
        "cross_anchor_time": cross.isoformat(),
        "baseline_to_cross_minutes": duration,
        "baseline_resolution": method,
        "source_event_baseline_price": event.get("baseline_price"),
        "source_event_cross_price": event.get("crossing_trade_price"),
        "source_event_exact_duration_seconds": event.get("exact_baseline_to_cross_seconds"),
    }


def control_sample(match: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    duration = _event_duration_minutes(event)
    cross = floor_minute(parse_datetime(match["control_anchor_time"]))
    baseline = cross - timedelta(minutes=duration)
    return {
        "sample_id": f"control:{match['control_id']}",
        "match_group_id": str(match["event_id"]),
        "sample_type": "control",
        "label": 0,
        "split": str(match["split"]),
        "symbol": str(match["symbol"]),
        "base_asset": event.get("base_asset"),
        "quote_asset": event.get("quote_asset"),
        "event_id": str(match["event_id"]),
        "control_id": str(match["control_id"]),
        "control_rank": int(match["control_rank"]),
        "baseline_anchor_time": baseline.isoformat(),
        "cross_anchor_time": cross.isoformat(),
        "baseline_to_cross_minutes": duration,
        "baseline_resolution": "pseudo_baseline_same_event_duration",
        "source_event_baseline_price": event.get("baseline_price"),
        "source_event_cross_price": event.get("crossing_trade_price"),
        "source_event_exact_duration_seconds": event.get("exact_baseline_to_cross_seconds"),
    }


def _prior_interval_return(long_return_pct: Any, short_return_pct: Any) -> float | None:
    if long_return_pct is None or short_return_pct is None:
        return None
    long_factor = 1.0 + float(long_return_pct) / 100.0
    short_factor = 1.0 + float(short_return_pct) / 100.0
    if long_factor <= 0 or short_factor <= 0:
        return None
    return (long_factor / short_factor - 1.0) * 100.0


def add_baseline_derived_features(row: dict[str, Any]) -> None:
    row["ret_prior_1d_to_3d_pct"] = _prior_interval_return(row.get("ret_4320m_pct"), row.get("ret_1440m_pct"))
    row["ret_prior_1d_to_5d_pct"] = _prior_interval_return(row.get("ret_7200m_pct"), row.get("ret_1440m_pct"))
    row["ret_prior_1d_to_7d_pct"] = _prior_interval_return(row.get("ret_10080m_pct"), row.get("ret_1440m_pct"))
    row["ret_prior_1d_to_10d_pct"] = _prior_interval_return(row.get("ret_14400m_pct"), row.get("ret_1440m_pct"))

    last_1d = row.get("quote_volume_1440m")
    total_3d = row.get("quote_volume_4320m")
    if last_1d is not None and total_3d is not None:
        prior_2d = float(total_3d) - float(last_1d)
        row["volume_last1d_vs_prior2d_daily_rate"] = float(last_1d) / (prior_2d / 2.0) if prior_2d > 0 else None
    else:
        row["volume_last1d_vs_prior2d_daily_rate"] = None

    last_1d_trades = row.get("trade_count_1440m")
    total_3d_trades = row.get("trade_count_4320m")
    if last_1d_trades is not None and total_3d_trades is not None:
        prior_2d_trades = float(total_3d_trades) - float(last_1d_trades)
        row["trade_count_last1d_vs_prior2d_daily_rate"] = (
            float(last_1d_trades) / (prior_2d_trades / 2.0) if prior_2d_trades > 0 else None
        )
    else:
        row["trade_count_last1d_vs_prior2d_daily_rate"] = None


def pseudo_window_audit(frame: pd.DataFrame, sample: dict[str, Any], threshold_pct: float = 25.0) -> dict[str, Any]:
    baseline = pd.Timestamp(parse_datetime(sample["baseline_anchor_time"]))
    cross = pd.Timestamp(parse_datetime(sample["cross_anchor_time"]))
    segment = frame.loc[baseline:cross]
    if segment.empty:
        return {
            "pseudo_window_observed_fraction": 0.0,
            "pseudo_window_crossing_detected": None,
            "pseudo_window_max_sequential_gain_pct": None,
            "pseudo_window_contaminated_control": sample["sample_type"] == "control",
        }
    observed_fraction = float(segment["observed"].mean())
    prior_low = segment["low"].cummin().shift(1)
    valid = prior_low.notna() & segment["high"].notna() & (prior_low > 0)
    gains = (segment.loc[valid, "high"] / prior_low.loc[valid] - 1.0) * 100.0
    maximum = float(gains.max()) if not gains.empty else None
    crossed = bool((gains >= threshold_pct).any()) if not gains.empty else False
    contaminated = bool(sample["sample_type"] == "control" and (crossed or observed_fraction < 0.99))
    return {
        "pseudo_window_observed_fraction": observed_fraction,
        "pseudo_window_crossing_detected": crossed,
        "pseudo_window_max_sequential_gain_pct": maximum,
        "pseudo_window_contaminated_control": contaminated,
    }


def _outcome_diagnostics(frame: pd.DataFrame, sample: dict[str, Any], entry_price: float | None) -> dict[str, Any]:
    baseline = pd.Timestamp(parse_datetime(sample["baseline_anchor_time"]))
    cross = pd.Timestamp(parse_datetime(sample["cross_anchor_time"]))
    segment = frame.loc[baseline:cross]
    next_8h = frame.loc[baseline : baseline + pd.Timedelta(minutes=479)]
    result: dict[str, Any] = {
        "outcome_baseline_to_cross_observed_fraction": float(segment["observed"].mean()) if len(segment) else 0.0,
        "outcome_next_8h_from_baseline_observed_fraction": float(next_8h["observed"].mean()) if len(next_8h) else 0.0,
    }
    baseline_low = segment["low"].iloc[0] if len(segment) and pd.notna(segment["low"].iloc[0]) else None
    baseline_close = segment["close"].iloc[0] if len(segment) and pd.notna(segment["close"].iloc[0]) else None
    for prefix, values in (("baseline_to_cross", segment), ("next_8h_from_baseline", next_8h)):
        high = values["high"].max(skipna=True) if len(values) else None
        low = values["low"].min(skipna=True) if len(values) else None
        denominator = float(baseline_low) if baseline_low is not None and float(baseline_low) > 0 else baseline_close
        result[f"outcome_{prefix}_max_gain_from_baseline_minute_low_pct"] = (
            safe_pct(float(high), float(denominator)) if high is not None and pd.notna(high) and denominator is not None else None
        )
        result[f"outcome_{prefix}_max_drawdown_from_baseline_minute_close_pct"] = (
            safe_pct(float(low), float(baseline_close)) if low is not None and pd.notna(low) and baseline_close is not None else None
        )
    if entry_price is not None and len(segment):
        high = segment["high"].max(skipna=True)
        result["outcome_snapshot_to_cross_max_gain_pct"] = safe_pct(float(high), entry_price) if pd.notna(high) else None
    return result


def compute_baseline_feature_row(
    frame: pd.DataFrame,
    *,
    sample: dict[str, Any],
    snapshot_offset_minutes: int,
    prior_days: int,
    min_entry_notional: float,
    reference_frames: dict[str, pd.DataFrame],
    audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    adapted = dict(sample)
    adapted["anchor_time"] = sample["baseline_anchor_time"]
    row = compute_context_feature_row(
        frame,
        sample=adapted,
        horizon_minutes=int(snapshot_offset_minutes),
        prior_days=prior_days,
        min_entry_notional=min_entry_notional,
        reference_frames=reference_frames,
    )
    row.update(
        {
            "analysis_alignment": "baseline_start",
            "baseline_anchor_time": sample["baseline_anchor_time"],
            "cross_anchor_time": sample["cross_anchor_time"],
            "baseline_to_cross_minutes": sample["baseline_to_cross_minutes"],
            "baseline_resolution": sample["baseline_resolution"],
            "baseline_snapshot_offset_minutes": int(snapshot_offset_minutes),
            "decision_stage": "baseline_start" if int(snapshot_offset_minutes) == 0 else "pre_baseline",
            "source_event_baseline_price": sample.get("source_event_baseline_price"),
            "source_event_cross_price": sample.get("source_event_cross_price"),
            "source_event_exact_duration_seconds": sample.get("source_event_exact_duration_seconds"),
        }
    )
    add_baseline_derived_features(row)
    row.update(audit or {})
    row.update(_outcome_diagnostics(frame, sample, row.get("entry_price")))
    if row.get("pseudo_window_contaminated_control"):
        row["feature_quality_status"] = "contaminated_control"
    return row


def compute_pre_cross_feature_row(
    frame: pd.DataFrame,
    *,
    sample: dict[str, Any],
    horizon_minutes: int,
    prior_days: int,
    min_entry_notional: float,
    reference_frames: dict[str, pd.DataFrame],
    audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    adapted = dict(sample)
    adapted["anchor_time"] = sample["cross_anchor_time"]
    row = compute_context_feature_row(
        frame,
        sample=adapted,
        horizon_minutes=int(horizon_minutes),
        prior_days=prior_days,
        min_entry_notional=min_entry_notional,
        reference_frames=reference_frames,
    )
    row.update(
        {
            "analysis_alignment": "pre_cross_snapshot",
            "baseline_anchor_time": sample["baseline_anchor_time"],
            "cross_anchor_time": sample["cross_anchor_time"],
            "baseline_to_cross_minutes": sample["baseline_to_cross_minutes"],
            "pre_cross_horizon_minutes": int(horizon_minutes),
            "decision_stage": "pre_cross",
        }
    )
    row.update(audit or {})
    if row.get("pseudo_window_contaminated_control"):
        row["feature_quality_status"] = "contaminated_control"
    return row


class BaselineContextBuilder:
    def __init__(self, db: SupabaseClient, binance: BinanceClient, temp_root: Path):
        self.db = db
        self.binance = binance
        self.temp_root = temp_root
        self.cache = MinuteArchiveCache(binance, temp_root)

    def _samples(self, matched_job: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        matched_job_id = str(matched_job["id"])
        scan_id = str(matched_job["scan_id"])
        events = self.db.select_all(
            "binance_gainer_events",
            filters={"scan_id": f"eq.{scan_id}", "sellability_pass": "eq.true"},
            order="event_date.asc,symbol.asc",
        )
        matches = self.db.select_all(
            "binance_control_matches",
            filters={"matched_control_job_id": f"eq.{matched_job_id}"},
            order="symbol.asc,control_anchor_time.asc",
        )
        split_map, split_summary = assign_temporal_splits(
            events,
            int(matched_job.get("discovery_pct") or 70),
            int(matched_job.get("validation_pct") or 15),
        )
        event_by_id = {str(event["id"]): event for event in events}
        samples: list[dict[str, Any]] = []
        for event in events:
            split = split_map[date.fromisoformat(str(event["event_date"]))]
            samples.append(event_sample(event, split))
        for match in matches:
            event = event_by_id.get(str(match["event_id"]))
            if event is not None:
                samples.append(control_sample(match, event))
        return samples, split_summary, {"events": events, "matches": matches}

    def _validate_fresh_design(self, matched_job: dict[str, Any]) -> None:
        if int(matched_job.get("contamination_before_minutes") or 0) < 480:
            raise ValueError(
                "Fresh baseline-aligned evidence requires matched controls created with at least 480 minutes of pre-anchor contamination protection"
            )
        scan_rows = self.db.select("binance_scan_jobs", filters={"id": f"eq.{matched_job['scan_id']}"}, limit=1)
        if not scan_rows:
            raise RuntimeError("Source scan not found")
        scan = scan_rows[0]
        if scan.get("event_definition_version") != "v1_25pct_rolling_8h" or int(scan.get("window_minutes") or 0) != 480:
            raise ValueError("25% baseline context requires a 480-minute v1_25pct_rolling_8h source scan")
        if int(matched_job.get("contamination_after_minutes") or 0) < 480:
            raise ValueError(
                "Fresh staged evidence requires at least 480 minutes of post-anchor contamination protection"
            )

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["id"])
        matched_job_id = str(job["matched_control_job_id"])
        prior_days = int(job.get("prior_days") or 10)
        if prior_days != 10:
            raise ValueError("This release uses exactly 10 context days")
        offsets = tuple(int(value) for value in (job.get("snapshot_offsets_minutes") or BASELINE_SNAPSHOT_OFFSETS))
        if offsets != BASELINE_SNAPSHOT_OFFSETS:
            raise ValueError("Baseline snapshot offsets are fixed measurement grid points")
        pre_cross_horizons = tuple(
            int(value) for value in (job.get("pre_cross_horizons_minutes") or PRE_CROSS_HORIZONS)
        )
        if pre_cross_horizons != PRE_CROSS_HORIZONS:
            raise ValueError("Pre-cross snapshot horizons must be 15,30,60,120,180,480 minutes")
        min_entry_notional = float(job.get("min_entry_notional") or 500)
        research_mode = str(job.get("research_mode") or "exploratory_reuse")
        if research_mode not in {"exploratory_reuse", "fresh_staged"}:
            raise ValueError("research_mode must be exploratory_reuse or fresh_staged")

        matched_rows = self.db.select("binance_matched_control_jobs", filters={"id": f"eq.{matched_job_id}"}, limit=1)
        if not matched_rows:
            raise RuntimeError("Matched-control job not found")
        matched_job = matched_rows[0]
        if matched_job.get("status") not in {"completed", "completed_with_warnings"}:
            raise RuntimeError("Matched-control job must be completed")
        if research_mode == "fresh_staged":
            self._validate_fresh_design(matched_job)

        samples, split_summary, _source = self._samples(matched_job)
        if not samples:
            raise RuntimeError("No event/control samples found")
        baselines = [parse_datetime(row["baseline_anchor_time"]) for row in samples]
        crosses = [parse_datetime(row["cross_anchor_time"]) for row in samples]
        # Ten days of context at the snapshot ten days before baseline needs twenty days of history.
        load_start = min(baselines).date() - timedelta(days=prior_days + max(offsets) // 1440 + 2)
        load_end = max(crosses).date() + timedelta(days=1)

        self.db.update(
            "binance_baseline_context_jobs",
            {"id": f"eq.{job_id}"},
            {
                "samples_total": len(samples),
                "events_total": sum(row["label"] == 1 for row in samples),
                "controls_total": sum(row["label"] == 0 for row in samples),
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        work = Path(tempfile.mkdtemp(prefix=f"baseline-context-{job_id}-", dir=self.temp_root))
        feature_rows: list[dict[str, Any]] = []
        pre_cross_rows: list[dict[str, Any]] = []
        quality_rows: list[dict[str, Any]] = []
        audit_rows: list[dict[str, Any]] = []
        source_manifest: list[dict[str, Any]] = []
        failures = 0
        samples_failed = 0
        try:
            reference_frames: dict[str, pd.DataFrame] = {}
            for symbol in REFERENCE_SYMBOLS:
                try:
                    loaded_reference = self.cache.load_symbol(symbol, load_start, load_end)
                    source_manifest.extend(loaded_reference.source_manifest)
                    reference_frames[symbol] = loaded_reference.frame[["close", "observed"]].copy()
                except Exception as exc:
                    failures += 1
                    self.db.insert(
                        "binance_baseline_context_issues",
                        {
                            "baseline_context_job_id": job_id,
                            "symbol": symbol,
                            "stage": "load_reference",
                            "message": str(exc)[:4000],
                        },
                    )

            samples_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for sample in samples:
                samples_by_symbol[sample["symbol"]].append(sample)
            processed = 0
            for symbol, symbol_samples in sorted(samples_by_symbol.items()):
                try:
                    loaded_symbol = self.cache.load_symbol(symbol, load_start, load_end)
                    frame = loaded_symbol.frame
                    source_manifest.extend(loaded_symbol.source_manifest)
                except Exception as exc:
                    failures += 1
                    samples_failed += len(symbol_samples)
                    self.db.insert(
                        "binance_baseline_context_issues",
                        {
                            "baseline_context_job_id": job_id,
                            "symbol": symbol,
                            "stage": "load_symbol",
                            "message": str(exc)[:4000],
                        },
                    )
                    continue

                for sample in symbol_samples:
                    audit = pseudo_window_audit(frame, sample)
                    audit_row = {
                        "sample_id": sample["sample_id"],
                        "match_group_id": sample["match_group_id"],
                        "sample_type": sample["sample_type"],
                        "split": sample["split"],
                        "symbol": sample["symbol"],
                        "baseline_anchor_time": sample["baseline_anchor_time"],
                        "cross_anchor_time": sample["cross_anchor_time"],
                        "baseline_to_cross_minutes": sample["baseline_to_cross_minutes"],
                        **audit,
                    }
                    audit_rows.append(audit_row)
                    for offset in offsets:
                        feature = compute_baseline_feature_row(
                            frame,
                            sample=sample,
                            snapshot_offset_minutes=offset,
                            prior_days=prior_days,
                            min_entry_notional=min_entry_notional,
                            reference_frames=reference_frames,
                            audit=audit,
                        )
                        feature_rows.append(feature)
                        quality_rows.append(
                            {
                                "sample_id": feature.get("sample_id"),
                                "split": feature.get("split"),
                                "symbol": feature.get("symbol"),
                                "baseline_snapshot_offset_minutes": offset,
                                "feature_quality_status": feature.get("feature_quality_status"),
                                "observed_fraction_prior_window": feature.get("observed_fraction_prior_window"),
                                "entry_liquidity_pass": feature.get("entry_liquidity_pass"),
                                "pseudo_window_contaminated_control": feature.get("pseudo_window_contaminated_control"),
                            }
                        )
                    for horizon in pre_cross_horizons:
                        pre_cross_rows.append(
                            compute_pre_cross_feature_row(
                                frame,
                                sample=sample,
                                horizon_minutes=horizon,
                                prior_days=prior_days,
                                min_entry_notional=min_entry_notional,
                                reference_frames=reference_frames,
                                audit=audit,
                            )
                        )
                processed += len(symbol_samples)
                self.db.update(
                    "binance_baseline_context_jobs",
                    {"id": f"eq.{job_id}"},
                    {
                        "samples_processed": processed,
                        "feature_rows": len(feature_rows),
                        "pre_cross_rows": len(pre_cross_rows),
                        "failures": failures,
                        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                del frame

            sample_df = pd.DataFrame(samples)
            feature_df = pd.DataFrame(feature_rows)
            pre_cross_df = pd.DataFrame(pre_cross_rows)
            quality_df = pd.DataFrame(quality_rows)
            audit_df = pd.DataFrame(audit_rows)
            source_df = pd.DataFrame(source_manifest).drop_duplicates() if source_manifest else pd.DataFrame()
            split_df = pd.DataFrame(split_summary)
            design = {
                "version": "v1_1_chatgpt_led_baseline_context",
                "source_matched_control_job_id": matched_job_id,
                "source_scan_id": str(matched_job["scan_id"]),
                "research_mode": research_mode,
                "baseline_definition": (
                    "Event: scanner baseline minute containing the resolved low trade. "
                    "Control: pseudo-baseline equal to control anchor minus its matched event's minute-level baseline-to-cross duration."
                ),
                "snapshot_offsets_minutes_before_baseline": list(offsets),
                "pre_cross_horizons_minutes_before_cross": list(pre_cross_horizons),
                "context_windows_minutes_at_each_snapshot": list(CONTEXT_WINDOWS),
                "feature_cutoff": "only fully completed one-minute bars strictly before each snapshot time",
                "control_audit": "controls with an accidental 25% sequential low-to-later-high move inside the pseudo-window are flagged contaminated_control",
                "symmetry_note": "baseline and pseudo-baseline are minute-level for both events and controls; exact event trade prices are metadata/outcomes only",
                "retrospective_alignment_warning": "The baseline is known retrospectively. Baseline-aligned association does not by itself specify when a live scanner would alert; any surviving rule must later be evaluated at every completed minute.",
                "outcome_columns_rule": "columns beginning outcome_ are labels/diagnostics and must never be predictors",
                "research_integrity": (
                    "This package is exploratory and may be used only for ChatGPT-led hypothesis generation."
                    if research_mode == "exploratory_reuse"
                    else "Open discovery first; ChatGPT freezes candidate rules before one validation pass; sealed_test remains closed until the complete rule is frozen."
                ),
                "cluster_warning": "Rows are clustered by symbol and matched event; row count is not independent sample size.",
            }
            quality_report = {
                "samples_total": len(samples),
                "events_total": int(sum(row["label"] == 1 for row in samples)),
                "controls_total": int(sum(row["label"] == 0 for row in samples)),
                "baseline_feature_rows": len(feature_rows),
                "pre_cross_rows": len(pre_cross_rows),
                "quality_counts": quality_df["feature_quality_status"].value_counts(dropna=False).to_dict() if not quality_df.empty else {},
                "contaminated_controls": int(
                    audit_df.loc[audit_df["sample_type"] == "control", "pseudo_window_contaminated_control"].fillna(False).sum()
                ) if not audit_df.empty else 0,
                "symbols": int(sample_df["symbol"].nunique()) if not sample_df.empty else 0,
                "symbol_failures": failures,
                "source_status_counts": source_df["status"].value_counts(dropna=False).to_dict() if not source_df.empty and "status" in source_df else {},
                "load_start": load_start.isoformat(),
                "load_end_exclusive": load_end.isoformat(),
            }

            uploaded: list[dict[str, Any]] = []
            storage_prefix = f"baseline-context/{job_id}"

            def upload(path: Path, role: str, split: str | None = None) -> str:
                storage_path = f"{storage_prefix}/{path.name}"
                self.db.upload_file(storage_path, path, "application/zip")
                record = {
                    "baseline_context_job_id": job_id,
                    "split": split,
                    "storage_path": storage_path,
                    "filename": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "content_type": "application/zip",
                    "role": role,
                }
                self.db.upsert(
                    "binance_baseline_context_files",
                    [record],
                    on_conflict="baseline_context_job_id,storage_path",
                )
                uploaded.append(record)
                return storage_path

            def write_package(folder: Path, sample_part: pd.DataFrame, feature_part: pd.DataFrame,
                              pre_cross_part: pd.DataFrame, quality_part: pd.DataFrame,
                              audit_part: pd.DataFrame, readme: str) -> None:
                folder.mkdir(parents=True, exist_ok=True)
                sample_part.to_csv(folder / "sample_anchors.csv", index=False)
                feature_part.to_csv(folder / "baseline_context_features.csv", index=False)
                feature_part.to_parquet(folder / "baseline_context_features.parquet", index=False, compression="zstd")
                pre_cross_part.to_csv(folder / "pre_cross_snapshot_features.csv", index=False)
                pre_cross_part.to_parquet(folder / "pre_cross_snapshot_features.parquet", index=False, compression="zstd")
                quality_part.to_csv(folder / "data_quality.csv", index=False)
                audit_part.to_csv(folder / "control_contamination_audit.csv", index=False)
                (folder / "design.json").write_text(json.dumps(design, indent=2, default=_json_ready), encoding="utf-8")
                write_analysis_contract(folder, "baseline_context_package")
                (folder / "README.txt").write_text(readme, encoding="utf-8")

            package_paths: dict[str, str] = {}
            if research_mode == "exploratory_reuse":
                folder = work / "exploratory"
                write_package(
                    folder,
                    sample_df,
                    feature_df,
                    pre_cross_df,
                    quality_df,
                    audit_df,
                    "EXPLORATORY ONLY. Split-sealing is not enforced in this package. Use it for ChatGPT-led discovery or implementation audit, not final confirmation.\n",
                )
                path = work / "baseline_context_exploratory.zip"
                _zip_directory(folder, path)
                package_paths["exploratory"] = upload(path, "baseline_context_exploratory")
            else:
                for split in SPLITS:
                    folder = work / split
                    write_package(
                        folder,
                        sample_df[sample_df["split"] == split].copy(),
                        feature_df[feature_df["split"] == split].copy(),
                        pre_cross_df[pre_cross_df["split"] == split].copy(),
                        quality_df[quality_df["split"] == split].copy(),
                        audit_df[audit_df["split"] == split].copy(),
                        f"Baseline-aligned {split} package.\n" + (
                            "DO NOT OPEN UNTIL THE COMPLETE CHATGPT-DISCOVERED RULE IS FROZEN.\n"
                            if split == "sealed_test" else ""
                        ),
                    )
                    path = work / f"baseline_context_{split}.zip"
                    _zip_directory(folder, path)
                    package_paths[split] = upload(path, f"baseline_context_{split}", split)

            index = work / "index"
            index.mkdir(parents=True, exist_ok=True)
            split_df.to_csv(index / "split_summary.csv", index=False)
            source_df.to_csv(index / "source_archive_manifest.csv", index=False)
            audit_df.to_csv(index / "control_contamination_audit.csv", index=False)
            (index / "design.json").write_text(json.dumps(design, indent=2, default=_json_ready), encoding="utf-8")
            (index / "quality_report.json").write_text(json.dumps(quality_report, indent=2, default=_json_ready), encoding="utf-8")
            pd.DataFrame(uploaded).to_csv(index / "package_manifest.csv", index=False)
            write_analysis_contract(index, "baseline_context_index")
            index_path = work / "baseline_context_index.zip"
            _zip_directory(index, index_path)
            index_storage = upload(index_path, "baseline_context_index")
            return {
                "samples_total": len(samples),
                "samples_processed": len(samples) - samples_failed,
                "events_total": int(sum(row["label"] == 1 for row in samples)),
                "controls_total": int(sum(row["label"] == 0 for row in samples)),
                "feature_rows": len(feature_rows),
                "pre_cross_rows": len(pre_cross_rows),
                "failures": failures,
                "research_mode": research_mode,
                "index_storage_path": index_storage,
                "package_paths": package_paths,
                "quality_report": quality_report,
            }
        finally:
            shutil.rmtree(work, ignore_errors=True)
