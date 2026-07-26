# 25% fork architecture

The package preserves the source five-stage architecture. Steps 1–4 operate on an immutable +25% event definition. Database resources are isolated with a `stock25_` prefix and a dedicated storage bucket.

Step 5 code is retained, and its target multiplier is 1.25, but job creation and worker execution are disabled by default because the source predictive constants came from the 50% study. This prevents accidental cross-cohort leakage while allowing the engine to be activated later after valid 25% rules are supplied.
