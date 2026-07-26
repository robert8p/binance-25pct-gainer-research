# Signal provenance in the 25% fork

The attached source package contained two rules frozen for a **50% event cohort**. Those constants remain in this fork only to preserve the source implementation and facilitate later replacement. They are **not evidence about 25% events**.

`ENABLE_BACKTEST_STAGE` is therefore false by default, the dashboard labels Step 5 as locked, and the API rejects backtest creation while locked.

A valid 25% Step 5 requires:

1. complete 25% discovery and matched-control datasets;
2. a candidate rule fixed using discovery data only;
3. successful validation without retuning;
4. a sealed-test decision using the frozen rule;
5. replacement of the reference constants and provenance file before enabling execution backtesting.
