# Data contract — Binance 25% research V1.1

## Event

A saleable event is the earliest later one-minute high at least **25%** above the latest occurrence of the lowest prior one-minute low in a conservative **480-minute** rolling window.

Default saleability requires at least **500 quote units** of seller-initiated execution at any price during the **300 seconds** after the exact crossing trade. This tests whether a position could be sold, not whether it could be sold at the threshold price.

## Controls

Each event is paired with same-symbol historical controls where available. At each control decision time, the application uses prior completed bars only, rejects 25%-contaminated windows, applies the configured pre-decision liquidity floor and preserves chronological splits.

## Neutral measurements

The application may calculate continuous returns, ranges, volume, trade intensity, volatility, market-relative measurements, distribution summaries, data-quality diagnostics and execution-liquidity fields. It must not turn those measurements into hypotheses, pass/fail signal components, scores or trading rules.

Ten-day and baseline-aligned feature rows end before their stated decision timestamp. Every column beginning `outcome_` is a label or diagnostic and must not be used as a predictor.

## Research boundary

ChatGPT is the pattern-discovery engine. No 25%-specific confirmation signal or trading rule is frozen in this release.
