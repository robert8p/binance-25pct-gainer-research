# Binance 8-Hour 25% Gainer Research — V1.1.0

A direct-deployment application for finding saleable Binance Spot assets that rise at least **25% within 480 completed minutes** and packaging neutral evidence for **ChatGPT-led pattern discovery**.

## Fixed event definition

- Threshold: 25%
- Window: 480 minutes
- Canonical quote preference: USDT, USDC, FDUSD
- Default saleability requirement: 500 quote units of seller-initiated execution within 300 seconds of crossing
- Event identity: `v1_25pct_rolling_8h`

## Division of responsibility

The application:

1. identifies events and verifies saleability;
2. constructs same-symbol non-event controls;
3. computes neutral continuous measurements using only data available before each decision timestamp;
4. creates chronological discovery, validation and sealed-test packages;
5. records provenance, quality and contamination diagnostics.

ChatGPT:

1. audits the research packages;
2. identifies patterns, directions, interactions and regimes;
3. selects candidate thresholds using discovery data only;
4. freezes candidate rules;
5. validates them once and later reviews the sealed test.

No inherited 50% hypothesis, confirmation signal, composite score or trading-rule backtest is included.

Every research export includes `CHATGPT_ANALYSIS_PROMPT.md`, `ROLE_CONTRACT.json`, `FEATURE_DICTIONARY.md` and `ANALYSIS_GUARDRAILS.md`.

## Deployment

Follow `DEPLOYMENT.md`. Use a new Supabase project, GitHub repository and Render Blueprint.
