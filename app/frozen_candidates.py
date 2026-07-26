from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

# These definitions are intentionally immutable in V1.2. They were selected by
# ChatGPT from discovery, survived one small validation pass, and are now being
# tested on a larger non-overlapping historical period. The app evaluates them;
# it does not search for, rank, combine or retune patterns.
PARENT_CANDIDATE_REGISTER_SHA256 = (
    "a5ca94ff9b464cb1f28def2e1c3db90bfab5e707d2c6d373a8587207d9f2ecd8"
)
DECISION_HORIZON_MINUTES = 480
EXTERNAL_WINDOW_START = "2026-01-01"
EXTERNAL_WINDOW_END_EXCLUSIVE = "2026-05-26"  # includes all of 25 May 2026 UTC

CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "id": "C2",
        "name": "24h rebound plus acute 60m volume spike",
        "definition": (
            "close_vs_1440m_low_pct >= 6.0 AND "
            "quote_volume_60m_vs_prior_7d_same_time >= 3.0"
        ),
        "required_columns": (
            "close_vs_1440m_low_pct",
            "quote_volume_60m_vs_prior_7d_same_time",
        ),
    },
    {
        "id": "C4",
        "name": "12h run-up plus elevated short/long volatility ratio",
        "definition": (
            "max_runup_720m_pct >= 10.0 AND "
            "volatility_1d_to_7d_ratio >= 0.30"
        ),
        "required_columns": (
            "max_runup_720m_pct",
            "volatility_1d_to_7d_ratio",
        ),
    },
)

PASS_CRITERIA: dict[str, Any] = {
    "minimum_usable_matched_groups": 200,
    "minimum_event_dates": 30,
    "minimum_symbols": 50,
    "minimum_matched_lift": 1.5,
    "date_cluster_95pct_ci_lower_bound_must_exceed": 0.0,
    "symbol_cluster_95pct_ci_lower_bound_must_exceed": 0.0,
    "maximum_single_symbol_share_of_positive_advantage": 0.10,
    "minimum_eligible_months": 4,
    "minimum_groups_per_eligible_month": 20,
    "minimum_fraction_of_eligible_months_with_positive_effect": 0.75,
    "minimum_median_eligible_month_lift": 1.25,
    "no_threshold_retuning": True,
    "rules_evaluated_separately": True,
}


def register_payload() -> dict[str, Any]:
    return {
        "release": "1.2.0",
        "purpose": "larger_non_overlapping_external_validation_only",
        "parent_candidate_register_sha256": PARENT_CANDIDATE_REGISTER_SHA256,
        "event_definition": "saleable 25% low-to-later-high crossing within 480 completed minutes",
        "decision_horizon_minutes_before_cross": DECISION_HORIZON_MINUTES,
        "external_validation_window": {
            "start_utc_inclusive": EXTERNAL_WINDOW_START,
            "end_utc_exclusive": EXTERNAL_WINDOW_END_EXCLUSIVE,
            "human_readable": "1 January through 25 May 2026 UTC",
        },
        "row_filters": {
            "feature_quality_status": "pass",
            "pseudo_window_contaminated_control": False,
            "matched_group_requires": "exactly one event and at least one control",
        },
        "candidates": [
            {key: value for key, value in candidate.items() if key != "required_columns"}
            for candidate in CANDIDATES
        ],
        "pass_criteria": PASS_CRITERIA,
        "prohibitions": [
            "no new feature search",
            "no threshold retuning",
            "no C2+C4 combination",
            "no access to the prior sealed-test package",
            "no trading or profitability conclusion from this stage alone",
        ],
    }


def canonical_register_bytes() -> bytes:
    return (json.dumps(register_payload(), indent=2, sort_keys=True) + "\n").encode("utf-8")


def register_sha256() -> str:
    return hashlib.sha256(canonical_register_bytes()).hexdigest()


def write_register(path: Path) -> None:
    path.write_bytes(canonical_register_bytes())


def evaluate_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "close_vs_1440m_low_pct",
        "quote_volume_60m_vs_prior_7d_same_time",
        "max_runup_720m_pct",
        "volatility_1d_to_7d_ratio",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Frozen-rule feature columns are missing: {', '.join(missing)}")

    result = frame.copy()
    for column in required:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    result["C2_pass"] = (
        result["close_vs_1440m_low_pct"].ge(6.0)
        & result["quote_volume_60m_vs_prior_7d_same_time"].ge(3.0)
    )
    result["C4_pass"] = (
        result["max_runup_720m_pct"].ge(10.0)
        & result["volatility_1d_to_7d_ratio"].ge(0.30)
    )
    return result
