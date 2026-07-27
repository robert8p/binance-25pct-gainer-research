# Binance 25% Frozen C2/C4 External Validation — V1.3.0

V1.3 is the memory-safe and resumable replacement for V1.2.

It still performs only the fixed historical validation of ChatGPT-frozen C2 and C4. It does not discover, combine or retune patterns.

## Fixed research scope

- Historical start: `2026-01-01` UTC, inclusive
- Historical end: `2026-05-26` UTC, exclusive
- Event: saleable 25% rise within 480 completed minutes
- Controls: five same-symbol controls per event
- Evaluation horizon: exactly 480 minutes before each event/control anchor

## Memory changes

- Matched controls keep only one coin's minute frame in memory at a time.
- The matched-control stage no longer builds a duplicate feature matrix; Step 3 calculates the only features required for C2/C4.
- Binance aggregate trades are processed page-by-page rather than accumulated in one Python list.
- Raw event aggregate trades are not persisted by default because the frozen validation uses the derived execution statistics, not the full trade tape.
- Reference data is reduced to only the columns required by the evaluator.
- Python and glibc memory are explicitly released between symbols.

## Resume changes

- Scan checkpoints after every completed symbol.
- Matched controls checkpoint after every completed event and write controls immediately to Supabase.
- Frozen evaluation checkpoints every sample group by storing feature rows in Supabase.
- A Render restart automatically requeues the interrupted job and resumes from the durable checkpoint.
- Automatic retries stop after `MAX_AUTO_RESUMES` rather than looping forever.
- Failed jobs have a **Resume** button on the dashboard that retains existing progress.

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

Deploy using `DEPLOYMENT.md` and run `supabase/migrate_v1_3_0_memory_resume.sql` before starting the new worker.
