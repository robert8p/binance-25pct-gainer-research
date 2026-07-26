# V1.2 external-validation protocol

## Purpose

Test only the two ChatGPT-frozen candidates C2 and C4 on a larger period that does not overlap the original discovery/validation/sealed cohort.

## Fixed design

- Historical window: 2026-01-01 inclusive to 2026-05-26 exclusive.
- Saleable event: 25% low-to-later-high crossing within 480 completed minutes.
- Five same-symbol controls requested per event.
- Ten days of point-in-time history.
- Decision snapshot: exactly 480 minutes before event/control anchor.
- Quality: `feature_quality_status == pass`.
- Contaminated controls excluded.
- Exactly one usable event and at least one usable control per matched group.

## Rules

C2 and C4 are reproduced from `FROZEN_EXTERNAL_VALIDATION_REGISTER.json`. No other candidate is evaluated.

## Passing standard

The release freezes the criteria in the register before the new cohort is processed, including sample/date/symbol breadth, lift, date- and symbol-cluster confidence intervals, symbol concentration and monthly stability.

## Boundaries

This is validation of predictive association, not continuous signal incidence or executable profitability. The prior sealed-test package remains untouched.
