# Hotfix 4.0.1 — resilient Supabase REST connections

Step 2 could fail an otherwise healthy long-running job when Supabase/PostgREST or an intermediate proxy closed an idle HTTP connection with:

`Server disconnected without sending a response.`

The app already retried Alpaca market-data calls and Supabase Storage uploads, but ordinary Supabase REST reads and updates had no retry layer.

This hotfix:

- retries replay-safe GET, PATCH and DELETE requests;
- retries deterministic PostgREST upserts with `on_conflict`;
- recreates the HTTP client after a network disconnect;
- retries transient 408, 425, 429, 520 and 5xx responses with exponential backoff;
- deliberately does not replay plain INSERT requests, avoiding duplicate jobs if a response is lost after commit;
- preserves all existing Step 2 resume semantics and database tables.

No SQL migration is required. Deploy over the existing GitHub repository, wait for both Render services to become Live, then use **Retry and resume** on the existing failed Step 2 job.
