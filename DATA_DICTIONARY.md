# 25% fork data dictionary

All Supabase tables are isolated with the `stock25_` prefix.

## Core discovery

- `stock25_scans` — queued, running and completed +25% discovery scans.
- `stock25_scan_results` — verified regular-session +25% stock-day events.
- `stock25_event_bars` — qualifying-day one-minute bars.
- `stock25_asset_snapshots` — point-in-time Alpaca asset metadata.

## Sellability research

- `stock25_research_jobs` — Step 2 jobs.
- `stock25_research_events` — exact crossing and displayed-liquidity assessments.
- `stock25_research_files` — uploaded raw and derived research files.

## Matched controls

- `stock25_control_jobs` — Step 3 jobs.
- `stock25_control_observations` — positive and matched-control stock-days.
- `stock25_control_pairs` — deterministic positive/control matches.
- `stock25_control_match_diagnostics` — balance and shortfall diagnostics.
- `stock25_control_datasets` — collected control data.
- `stock25_control_files` — control exports and raw files.

## Entry feasibility

- `stock25_entry_jobs` — Step 4 jobs.
- `stock25_entry_assessments` — realistic pre-threshold entry opportunities.
- `stock25_entry_files` — split-safe analysis exports.

## Reference execution stage

- `stock25_backtest_jobs`
- `stock25_backtest_days`
- `stock25_backtest_triggers`
- `stock25_backtest_trades`
- `stock25_backtest_files`

These tables are created for structural parity with the source package, but Step 5 is disabled until 25%-specific signal rules are frozen and approved.
