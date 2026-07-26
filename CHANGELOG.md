# Changelog

## 4.0.1-25pct

- Added automatic retry and connection-pool renewal for replay-safe Supabase REST requests.
- Prevented transient `Server disconnected without sending a response` errors from failing long Step 2 jobs.
- Kept plain INSERT operations non-retrying to avoid accidental duplicate job creation.
- No database migration required; existing failed jobs can be resumed.

## 4.0.0-25pct

- Forked from the attached Alpaca 50% Execution Backtester V4.0.0.
- Changed the discovery event threshold and execution target to +25% / 125% of prior close.
- Locked the scan API to exactly 25%.
- Prefixed every database table with `stock25_`.
- Renamed Render services and storage bucket for isolation.
- Retained the Step 5 engine but disabled it until 25%-specific frozen signals exist.
- Added tests for the 25% crossing, strict non-hit controls and 125% execution target.
