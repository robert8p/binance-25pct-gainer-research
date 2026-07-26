# Binance 25% Frozen C2/C4 External Validation — V1.2.0

This is a dedicated historical validation release. It does **not** discover patterns.

ChatGPT previously identified and froze two provisional precursor rules. This app now:

1. scans a fixed non-overlapping historical period;
2. constructs five same-symbol controls per saleable 25% event;
3. calculates one neutral feature snapshot exactly 480 minutes before each event/control anchor;
4. mechanically evaluates C2 and C4 without changing them;
5. packages the raw evidence and results for independent reproduction in ChatGPT.

## Fixed period

- Start: `2026-01-01` UTC, inclusive
- End: `2026-05-26` UTC, exclusive
- Human-readable coverage: 1 January through 25 May 2026

## Frozen candidates

### C2

```text
close_vs_1440m_low_pct >= 6.0
AND quote_volume_60m_vs_prior_7d_same_time >= 3.0
```

### C4

```text
max_runup_720m_pct >= 10.0
AND volatility_1d_to_7d_ratio >= 0.30
```

The app cannot search for new rules, retune thresholds, combine C2 and C4, or inspect the prior sealed-test package.

## Upgrade

Follow `DEPLOYMENT.md`. Run `supabase/migrate_v1_2_0_external_validation.sql` before deploying the new worker.
