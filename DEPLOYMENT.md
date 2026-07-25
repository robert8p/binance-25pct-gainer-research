# Simple deployment instructions — V1.1.0

## 1. Create a new Supabase project

Do not reuse the 50% app database. In the new project, open **SQL Editor**, paste the entire contents of `supabase/schema.sql`, and run it once.

## 2. Create a new private GitHub repository

Upload the contents of this ZIP so `app`, `docs`, `supabase`, `render.yaml` and `requirements.txt` are at repository root.

## 3. Create a Render Blueprint

Connect the new repository. Render creates:

- `binance-25pct-scanner-web`
- `binance-25pct-scanner-worker`

Enter the new Supabase URL, new server-side secret key and a strong app password when prompted. The Supabase secret can begin `sb_secret_`; place it in `SUPABASE_SERVICE_ROLE_KEY`.

## 4. Verify

Open the Render web-service URL followed by `/health`. It should show:

```json
{"status":"ok","version":"1.1.0","target":"25pct_within_8h","event_definition_version":"v1_25pct_rolling_8h"}
```

Then open the main URL. Username: `rob`. Password: the `APP_PASSWORD` entered in Render.

## 5. First run

Run a two-day scan first, then a 60-day scan. The 25% threshold and eight-hour window are fixed in code and database.

Build matched controls, then choose **Fresh staged discovery/validation/sealed** for the ten-day and baseline-context packages. Download and upload the discovery packages to ChatGPT first. Keep validation and sealed-test packages closed until ChatGPT freezes candidate rules.

Each analysis package includes a dedicated ChatGPT prompt, role contract, feature dictionary and guardrails.
