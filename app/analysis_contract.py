from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROLE_CONTRACT: dict[str, Any] = {
    "version": "v1_1_1_chatgpt_led_pattern_discovery",
    "application_role": [
        "download and validate Binance market data",
        "identify events using the fixed 25% within 480-minute event definition",
        "test post-cross saleability",
        "construct same-symbol matched controls",
        "calculate neutral continuous measurements available before each decision timestamp",
        "separate discovery, validation and sealed-test packages",
        "preserve provenance, data-quality and contamination diagnostics",
    ],
    "pattern_discovery_engine": "ChatGPT",
    "application_must_not": [
        "name or evaluate candidate precursor hypotheses",
        "select predictor thresholds or directions",
        "combine features into a signal or score",
        "label any predictor combination as confirmed, frozen or profitable",
        "open validation or sealed-test data automatically",
        "perform a trading-rule backtest before ChatGPT has frozen a rule",
    ],
    "permitted_fixed_thresholds": {
        "event_gain_pct": 25,
        "event_window_minutes": 480,
        "control_contamination_gain_pct": 25,
        "configured_execution_liquidity_floor": "user supplied",
        "data_completeness_thresholds": "quality classification only; never a market signal",
    },
}

ANALYSIS_GUARDRAILS = """# Analysis guardrails

1. **ChatGPT is the pattern-discovery engine.** The application only collects, aligns and packages evidence.
2. Start with the discovery package only. Do not inspect validation until candidate rules are fully specified.
3. Do not inspect sealed-test data until feature definitions, thresholds, directions, exclusions, entry timing, holding period and exit logic are frozen.
4. Exclude `label`, identifiers, split fields, source-event metadata and every column beginning `outcome_` from predictors.
5. Exclude or separately audit rows whose quality status is not `pass`, and controls flagged as contaminated.
6. Respect matching: controls are linked to events through `match_group_id`; rows are not independent.
7. Cluster inference by symbol and matched event, and check that findings are not driven by one coin or short date cluster.
8. Search both directions, non-linear effects and interactions without assuming inherited 50% hypotheses.
9. Control for multiple testing and report all tested feature families, not only successful results.
10. Prefer stable regions and neighbouring-threshold robustness over a single maximised cut-off.
11. Validation is for rejection or confirmation only. Do not retune after viewing it.
12. A predictive association is not a trade. Profitability requires a later continuous, executable-entry backtest with fees, slippage and fixed exits.
"""

CHATGPT_ANALYSIS_PROMPT = """# ChatGPT blank-canvas analysis prompt

You are the **pattern-discovery engine** for a Binance 25%-within-eight-hours matched-control research programme. The application that generated these files has deliberately made no candidate signal, hypothesis, score or trading-rule decision.

## Objective

Identify pre-event measurements or combinations that distinguish saleable 25% events from comparable same-symbol non-event controls materially more often than chance, while avoiding look-ahead leakage, overfitting and false confidence from clustered observations.

## Required workflow

1. Read `ROLE_CONTRACT.json`, `ANALYSIS_GUARDRAILS.md`, the design file, quality report and data dictionary first.
2. Audit sample counts, date coverage, symbols, event/control balance, missingness, contaminated controls, duplicated rows and split integrity.
3. Construct a predictor list that excludes labels, IDs, split fields, source-event metadata and all `outcome_` columns.
4. Use **discovery data only** for pattern generation and threshold selection.
5. Compare events with their matched controls at each decision horizon separately before pooling horizons.
6. Examine:
   - continuous distribution shifts;
   - monotonic and non-monotonic relationships;
   - neighbouring time windows;
   - pairwise and limited higher-order interactions;
   - event clusters or regimes;
   - relative-market, price, volume, trade-intensity, volatility, liquidity and range-position families.
7. Use symbol- and event-cluster-aware inference. Report effective sample size and concentration by symbol/date.
8. Apply multiple-testing control and distinguish exploratory ranking from confirmatory evidence.
9. Reject patterns that depend on a single symbol, narrow date cluster, isolated threshold or poor-quality rows.
10. Produce a candidate-rule register containing exact definitions, threshold directions, rationale, discovery hit rates, matched effect sizes, uncertainty, robustness checks and failure modes.
11. Stop after discovery and ask for the validation package only when the candidate register is frozen.
12. After one validation pass, retain or reject candidates without retuning. Request sealed-test data only after the complete rule is frozen.

## Required output

- Data-integrity verdict.
- Strongest robust differences between events and controls.
- Candidate patterns ranked by evidence quality, not headline accuracy.
- Exact reproducible candidate rules suitable for independent validation.
- Multiplicity, clustering, leakage and concentration assessment.
- A clear **no reliable pattern** verdict when evidence is insufficient.

Do not assume any rule previously found in the 50% event population generalises to this 25% population.
"""


EXTERNAL_VALIDATION_GUARDRAILS = """# External-validation guardrails

1. Evaluate only C2 and C4 exactly as frozen.
2. Do not search for new features, change thresholds, reverse directions, add exclusions or combine the rules.
3. Use only rows at the 480-minute pre-cross horizon with `feature_quality_status == pass`.
4. Exclude contaminated controls and require exactly one event plus at least one control per matched group.
5. Reproduce matched hit rates, lift, within-group randomisation, date/symbol cluster uncertainty, concentration and monthly stability.
6. A pass is an association result, not a trading or profitability result.
7. Do not inspect the prior sealed-test package.
"""

EXTERNAL_VALIDATION_PROMPT = """# ChatGPT fixed-rule external-validation prompt

Independently reproduce the external historical test of the two rules in `FROZEN_CANDIDATE_REGISTER.json`. Evaluate C2 and C4 separately, with no threshold changes or new pattern search. Audit coverage, matching, quality, contamination, date/symbol concentration and monthly stability. Compare your results with the app-generated report and explain discrepancies. Do not infer profitability and do not open any prior sealed-test package.
"""

FEATURE_DICTIONARY = """# Feature dictionary

The matrices contain neutral measurements, labels and diagnostics. Column names are generally self-describing and include the measurement window in minutes.

## Never use as predictors

- `label`, `sample_type`, `split`
- identifiers such as `sample_id`, `event_id`, `control_id`, `match_group_id`
- source-event prices and exact event duration metadata
- every column beginning `outcome_`
- quality and contamination flags as market predictors; use them only to filter or audit

## Timing and alignment

- `anchor_time`, `decision_time`, `baseline_anchor_time`, `cross_anchor_time`
- `decision_horizon_minutes`, `baseline_snapshot_offset_minutes`, `pre_cross_horizon_minutes`
- These identify when the neutral measurements end. Predictor bars must be strictly earlier than `decision_time`.

## Price and path measurements

- `ret_<window>m_pct`: return over the stated completed-bar window
- `range_<window>m_pct`: high-low range
- `position_in_<window>m_range`: current price position in the historical range
- `close_vs_<window>m_high_pct`, `close_vs_<window>m_low_pct`
- `max_drawdown_<window>m_pct`, `max_runup_<window>m_pct`
- `positive_return_fraction_<window>m`
- `log_price_trend_<window>m_pct_per_day` and its fit statistic

## Volume and trading activity

- `quote_volume_<window>m`, `trade_count_<window>m`
- `average_trade_quote_<window>m`
- `taker_buy_ratio_<window>m`
- same-clock-time ratios compare activity with the same time on preceding days

## Volatility and acceleration

- `realized_vol_<window>m_pct`
- continuous ratios and differences between neighbouring windows
- daily trend, range and volume slopes

## Distributional ten-day measurements

- minute-return and hourly-return quantiles, mean, standard deviation, skewness, extrema and positive fractions
- hourly quote-volume quantiles, dispersion and maximum-to-median ratio
- counts of observed hours and rolling 24-hour highs; these are descriptive, not signals

## Relative-market measurements

- BTC, ETH and BNB reference returns
- coin return minus each reference return
- equal-weight market-proxy return and coin-minus-proxy differences

## Execution and quality diagnostics

- `entry_quote_volume_5m`, `entry_liquidity_pass`
- observed fractions, missing-minute counts and `feature_quality_status`
- control contamination fields identify invalid controls and must not be treated as predictive features
"""


def write_analysis_contract(folder: Path, package_role: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    contract = dict(ROLE_CONTRACT)
    contract["package_role"] = package_role
    if "external_validation" in package_role:
        contract["version"] = "v1_3_memory_safe_resumable_external_validation"
        contract["pattern_discovery_engine"] = "ChatGPT completed discovery before this release"
        contract["application_role"] = [
            "collect the fixed non-overlapping historical cohort",
            "construct same-symbol matched controls",
            "calculate the neutral 480-minute feature snapshot",
            "mechanically evaluate the frozen C2 and C4 definitions",
            "package evidence so ChatGPT can independently reproduce the result",
        ]
        contract["application_must_not"] = [
            "search for new patterns",
            "retune thresholds or directions",
            "combine C2 and C4",
            "inspect the prior sealed-test package",
            "claim trading profitability",
        ]
        guardrails = EXTERNAL_VALIDATION_GUARDRAILS
        prompt = EXTERNAL_VALIDATION_PROMPT
    else:
        guardrails = ANALYSIS_GUARDRAILS
        prompt = CHATGPT_ANALYSIS_PROMPT
    (folder / "ROLE_CONTRACT.json").write_text(json.dumps(contract, indent=2), encoding="utf-8")
    (folder / "ANALYSIS_GUARDRAILS.md").write_text(guardrails, encoding="utf-8")
    (folder / "CHATGPT_ANALYSIS_PROMPT.md").write_text(prompt, encoding="utf-8")
    (folder / "FEATURE_DICTIONARY.md").write_text(FEATURE_DICTIONARY, encoding="utf-8")
