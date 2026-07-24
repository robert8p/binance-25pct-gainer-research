# Simple deployment instructions

## 1. Create a new Supabase project

Do not reuse the 50% app database. In the new project, open **SQL Editor**, paste the entire contents of `supabase/schema.sql`, and run it once.

## 2. Create a new private GitHub repository

Upload the contents of this ZIP so `app`, `supabase`, `render.yaml` and `requirements.txt` are at repository root.

## 3. Create a Render Blueprint

Connect the new repository. Render will create:

- `binance-25pct-scanner-web`
- `binance-25pct-scanner-worker`

Enter the new Supabase URL, new service-role key, and a strong app password when prompted.

## 4. Verify

Open the Render web service URL followed by `/health`. It should show:

```json
{"status":"ok","version":"1.0.0","target":"25pct_within_8h","event_definition_version":"v1_25pct_rolling_8h"}
```

Then open the main web URL. Username: `rob`. Password: the `APP_PASSWORD` you entered in Render.

## 5. First run

Queue a 60-day scan using the default settings. The threshold and eight-hour window are fixed in the code and database.
