# V1.2.0 upgrade and run instructions

Use the existing 25% Supabase project, GitHub repository and Render Blueprint. This release is additive and preserves earlier outputs.

## 1. Stop the old worker

Suspend the current Render worker before changing the database or code.

Do not delete historical jobs or the previous sealed-test files.

## 2. Update Supabase

Open Supabase **SQL Editor**, create a new query, and run the full contents of:

```text
supabase/migrate_v1_2_0_external_validation.sql
```

The migration:

- tags discovery and external-validation jobs separately;
- permits an `external_validation` control cohort;
- creates the new evaluation job/file tables;
- includes the earlier baseline-column correction;
- changes no prior research results.

## 3. Replace the GitHub files

Upload everything inside this ZIP to the root of the existing private GitHub repository and replace the old files.

Repository root must show:

```text
app/
docs/
supabase/
render.yaml
requirements.txt
README.md
DEPLOYMENT.md
```

Commit with:

```text
Upgrade to V1.2 frozen C2 C4 external validation
```

## 4. Redeploy Render

Deploy the latest commit for both services, or allow Auto-Deploy to finish.

Keep the existing environment variables. No new secret is required.

The worker is intentionally dedicated to V1.2 external-validation jobs and will not resume legacy discovery, ten-day-context or baseline-context jobs.

## 5. Verify

Open:

```text
https://YOUR-WEB-SERVICE.onrender.com/health
```

Expected fields include:

```json
{
  "status": "ok",
  "version": "1.2.0",
  "purpose": "frozen_c2_c4_external_validation"
}
```

Then open the dashboard and confirm a recent worker heartbeat.

## 6. Run the three locked steps

### Step 1

Click **Queue fixed external-validation scan**.

The app locks the period to 1 January–25 May 2026 and locks all event/execution settings.

### Step 2

After the scan is `completed` or `completed_with_warnings`, select it and click **Queue external-validation controls**.

### Step 3

After matched controls complete, select that job and click **Run frozen-rule evaluation**.

The evaluator calculates only the 480-minute snapshot needed for C2 and C4. It does not build another discovery, validation or sealed split.

## 7. Download the evidence

Download:

```text
external_validation_index.zip
```

and every file beginning:

```text
external_validation_features
```

Upload all of them to ChatGPT together. ChatGPT should independently reproduce and interpret the fixed-rule test.

## Important boundaries

- Do not open the old sealed-test ZIP.
- Do not change C2 or C4 thresholds after seeing the results.
- A positive association is not yet a trading strategy.
- Entry, exit, fees, slippage, signal frequency and drawdown require a later continuous execution backtest.
