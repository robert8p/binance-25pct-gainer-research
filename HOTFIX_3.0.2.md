# V3.0.2 strict-control hotfix

## Problem corrected

The V3.0.1 pilot completed operationally but selected ten weak controls out of twenty. Exact-exchange-first selection and forced five-control allocation permitted large mismatches in previous-day return, volatility, ATR and momentum.

## Changes

- Global same-date candidate ranking; exchange is only a soft penalty.
- Hard balance gates for price, dollar volume, volatility, ATR, previous-day return, momentum and listing history.
- Same/adjacent price-band requirement.
- Matching corporate-action status required by default.
- Overall match score capped at 4.0.
- Weak controls cannot be selected or downloaded.
- One to five strong controls accepted instead of forcing five.
- Per-positive shortfall and nearest-rejected diagnostics.
- Pre-download balance report and gate.
- Explicit auction-coverage diagnostics.
- Pilot exports are restricted to the positives selected by that job.
- Balance files survive Render restart/resume through Supabase recovery.

## Upgrade

1. Run `supabase/migration_v3_0_2.sql`.
2. Upload the complete V3.0.2 package to the existing GitHub repository.
3. Confirm `/health` shows `3.0.2`.
4. Create a new five-positive pilot.

Do not run the full job until the pilot balance report passes with zero weak controls.
