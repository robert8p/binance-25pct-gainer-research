# Alpaca 25% Gainer Research Lab V4.0.1

This package is a **25% research fork** rebuilt from the attached Alpaca 50% Execution Backtester V4.0.0. It preserves the source architecture and the first four research stages while changing the event definition and execution target from +50% to **+25% versus the previous split-adjusted regular-session close**.

## Included stages

1. Full-universe 90-day +25% event discovery using split-adjusted daily bars and regular-session one-minute verification.
2. Sellability and exact-tick research collection.
3. Strict matched non-hit controls whose event-day high remains below 125% of prior close.
4. Entry-feasibility reconstruction and discovery/validation/sealed-test exports.
5. Execution-aware backtest engine retained from the source package, with its profit target changed to 125% of prior close.

## Important scientific safeguard

**Step 5 is disabled by default.** The two frozen predictive rules in the attached source ZIP were discovered and sealed on +50% events. Reusing them as though they were validated for +25% events would contaminate the research. The source rule file and engine remain in the package as reference code only.

Complete Steps 1–4 for the 25% cohort, analyse discovery and validation, freeze the 25%-specific rule before opening the sealed test, then replace the reference constants and set `ENABLE_BACKTEST_STAGE=true`.

## Isolation from the 50% app

- Render services are named `alpaca-25pct-scanner-web` and `alpaca-25pct-scanner-worker`.
- Every database table is prefixed `stock25_`.
- The storage bucket is `alpaca-25pct-research`.
- The API rejects scan thresholds other than 25%.

## Primary event definition

A stock-day qualifies only when its regular-session one-minute high reaches at least:

`previous split-adjusted regular-session close × 1.25`

Premarket-only or after-hours-only crossings do not qualify.

## No trading capability

The application collects and analyses historical data only. It cannot place orders.
