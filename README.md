# Binance 8-Hour 25% Gainer Research — V1.0.0

A direct-deployment application for identifying saleable Binance Spot assets that rise at least **25% within 480 completed minutes**.

## Fixed event definition

- Threshold: 25%
- Window: 480 minutes
- Canonical quote preference: USDT, USDC, FDUSD
- Default saleability requirement: 500 quote units of seller-initiated execution within 300 seconds of crossing
- Event identity: `v1_25pct_rolling_8h`

## Workflow

1. Scan for 25% events.
2. Build positive-event archives.
3. Build same-coin matched controls, excluding 25%-contaminated windows.
4. Build ten-day context.
5. Build baseline-aligned context.

The 50%-specific H3 confirmation and continuous trading-rule backtest are deliberately not included. A 25% event population is materially broader and requires its own historical discovery and validation before any rule is backtested.

## Deployment

Follow `DEPLOYMENT.md`. Use a new Supabase project, GitHub repository and Render Blueprint.
