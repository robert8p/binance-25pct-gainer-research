# Data contract — Binance 25% research V1

## Event

A saleable event is the earliest later one-minute high at least **25%** above the latest occurrence of the lowest prior one-minute low in a conservative **480-minute** rolling window.

Default saleability requires at least **500 quote units** of seller-initiated execution at any price during the **300 seconds** after the exact crossing trade. This tests whether the position could be sold, not whether it could be sold at the threshold price.

## Controls

Each event is paired with same-symbol historical controls where available. At each control decision time:

- use the applicable prior completed bars only;
- require the same 480-minute event framework;
- reject windows contaminated by a 25% crossing;
- require the configured pre-decision liquidity floor;
- keep discovery, validation and sealed-test splits separate.

## Context

Ten-day and baseline-aligned feature rows must end at or before their stated decision timestamp. Outcome fields must remain separately labelled and must not leak into predictors.

## Research boundary

No confirmation signal or trading rule is frozen in this release. A rule must first be discovered and historically validated specifically on the 25% event population.
