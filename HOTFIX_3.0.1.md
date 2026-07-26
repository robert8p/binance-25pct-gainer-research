# V3.0.1 invalid-symbol hotfix

This update fixes matched-control jobs that fail with an Alpaca error such as:

```text
Alpaca HTTP 400: {"message":"invalid symbol: 0029900E0"}
```

`0029900E0` is a CUSIP-like identifier present in Alpaca's all-status asset master, not a market-data ticker. One rejected identifier caused Alpaca's multi-symbol endpoint to reject the whole batch.

## Deploy

1. Extract the V3.0.1 ZIP.
2. Open the existing GitHub repository connected to Render.
3. Upload everything inside the extracted folder, replacing the existing files.
4. Commit the changes.
5. Wait for both Render services to redeploy, or use **Manual Deploy → Deploy latest commit**.
6. Open `/health` and confirm the version is `3.0.1`.
7. Return to the existing failed matched-control job and select **Retry and resume**.

Do not run a new Supabase migration, change credentials, recreate the job, or rerun V1/V2 collection.

The repaired worker filters obvious non-ticker identifiers and also removes any symbol explicitly rejected by Alpaca from a multi-symbol request before retrying the remaining batch.
