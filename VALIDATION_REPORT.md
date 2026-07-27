# V1.3.0 build validation

## Failure addressed

V1.2 could exceed Render memory because the matched-control stage retained the full multi-month minute frame for every event symbol in one process and also built a feature matrix that was duplicated again in the frozen-evaluation stage.

V1.3 removes that architecture rather than relying on a larger Render instance.

## Memory-bounded execution checks

- The scanner processes Binance aggregate trades page-by-page; it no longer accumulates the entire hot-market trade window in a Python list.
- Raw aggregate-trade persistence is disabled by default. The scanner retains the derived sellability evidence required by the frozen external validation.
- Daily archive frames are reduced to the required columns before they are retained or concatenated.
- Matched controls retain only one event-symbol minute frame at a time.
- Matched controls no longer calculate or retain the external-validation feature matrix.
- Frozen evaluation retains only three narrow reference-price frames plus one event-symbol frame at a time.
- Memory is explicitly collected and glibc arenas are trimmed between symbols where supported.
- Disk-headroom checks run before archive-intensive work.

## Durable resume checks

- Scan progress is checkpointed after every completed symbol.
- The scan symbol universe is frozen in Supabase and reused after restart.
- Event rows use deterministic identifiers and database upserts, so repeating the interrupted symbol does not duplicate events.
- Matched controls are checkpointed after every completed event and written immediately.
- Uncommitted partial control rows are removed on resume before that event is recalculated, preventing duplicate or excess controls.
- External-validation features are stored per sample in Supabase and skipped after restart.
- A failure during final evaluation or ZIP creation rebuilds outputs from durable feature rows rather than repeating data collection.
- SIGTERM/SIGINT preserves the current checkpoint and requeues the job.
- A hard process death is recovered on worker startup.
- Automatic retries are capped by `MAX_AUTO_RESUMES` to prevent an endless crash loop.
- Failed jobs expose a manual Resume action without deleting prior progress.

## Automated checks

- 6 focused resilience tests passed.
- All Python application modules compiled successfully.
- Dashboard template compiled successfully.
- `render.yaml` parsed successfully and does not force the worker back to Starter.
- Health payload reports version `1.3.0` and execution model `memory_bounded_resumable`.
- Parent candidate-register bytes still reproduce SHA-256 `a5ca94ff9b464cb1f28def2e1c3db90bfab5e707d2c6d373a8587207d9f2ecd8`.
- Active candidates remain exactly frozen C2 and C4; their definitions and thresholds were not changed.

## Frozen-evaluator reproduction check

The V1.3 evaluator was run locally against the previously opened 48-group validation cohort. It reproduced the frozen-rule results:

| Candidate | Groups | Event hit | Matched control pass | Lift | Status |
|---|---:|---:|---:|---:|---|
| C2 | 48 | 0.333333 | 0.162500 | 2.051282 | Fail |
| C4 | 48 | 0.395833 | 0.228472 | 1.732523 | Fail |

This confirms that the resilience rebuild did not alter the research rules or historical verdict.

## Upgrade limitation

A partially completed V1.2 scan can be bootstrapped from its recorded symbol count and frozen symbol snapshot. V1.2 did not persist event-level matched-control progress or sample-level evaluation features, so an interrupted V1.2 Step 2 or Step 3 may need to repeat that current stage once. Every checkpoint written after V1.3 starts is resumable at symbol, event or sample level.

## Scope boundary

No live Binance, Supabase or Render integration test was possible from the offline build environment. Network-dependent execution must be verified after deployment with the existing `/health` endpoint, worker heartbeat and a resumed or small test job.
