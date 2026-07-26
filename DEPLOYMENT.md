# Alpaca 25% Gainer Research Lab — deployment

Use a new private GitHub repository and preferably a new Supabase project. The package also prefixes all tables with `stock25_`, so it can coexist with the 50% app if necessary.

## 1. Create the database

1. Create or open the Supabase project.
2. Open **SQL Editor**.
3. Copy all of `supabase/schema.sql`.
4. Run it once.
5. Confirm tables such as `stock25_scans`, `stock25_research_jobs`, `stock25_control_jobs`, `stock25_entry_jobs` and `stock25_backtest_jobs` exist.

## 2. Upload to GitHub

Upload everything inside this extracted folder to the repository root. `render.yaml`, `Dockerfile`, `requirements.txt`, `app/` and `supabase/` must be visible at the top level.

## 3. Deploy the Render Blueprint

Create a Render Blueprint from the repository. It creates:

- `alpaca-25pct-scanner-web`
- `alpaca-25pct-scanner-worker`

Enter the requested Supabase, Alpaca and app-password secrets. Keep:

- `DEFAULT_THRESHOLD_PCT=25`
- `ENABLE_BACKTEST_STAGE=false`
- `SUPABASE_STORAGE_BUCKET=alpaca-25pct-research`

## 4. Verify

Open `/health`. It should report version `4.0.1-25pct` and target gain `25`.

## 5. Research sequence

Run Steps 1–4 in order, using a small pilot first and then the full run. Do not enable Step 5 merely because the source 50% package contained frozen rules. Freeze and validate a new 25%-specific rule set first.
