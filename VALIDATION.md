# Validation report

Build-time validation completed successfully on 26 July 2026:

- 55 automated tests passed.
- Python source compilation passed.
- JavaScript syntax validation passed with `node --check`.
- Jinja dashboard rendering passed with the 25% threshold read-only and Step 5 visibly locked.
- Render Blueprint YAML parsed successfully with both isolated 25% services.
- Both services use `DEFAULT_THRESHOLD_PCT=25`, `ENABLE_BACKTEST_STAGE=false`, and the `alpaca-25pct-research` bucket.
- Supabase schema contains exactly 21 isolated `stock25_` tables.
- Application database references match the schema.
- The scanner and API reject thresholds other than 25%.
- The execution engine target is 1.25 times the previous split-adjusted regular-session close.
- The inherited 50% predictor rules are marked reference-only and cannot be queued through the application.

The package has not been run against live Alpaca, Supabase or Render accounts. Run a small Step 1 pilot after deployment before launching the full scan.

- Supabase REST retry tests cover dropped connections, transient 503 responses, and duplicate-safe plain INSERT behaviour.
