# 50% to 25% conversion notes

Source: `alpaca_50pct_execution_backtester_v4.0.0(1).zip`.

Converted:

- event threshold: 50% → 25%;
- target multiple: 1.50 → 1.25;
- control non-hit ceiling: below 150% → below 125% of prior close;
- product names, export names, Render services and storage bucket;
- database resources to isolated `stock25_` tables;
- tests and fixtures.

Not converted as evidence:

- the source package’s frozen pre-open and midday predictor rules. They remain reference-only because they were derived from 50% outcomes. Step 5 is disabled until valid 25% rules replace them.
