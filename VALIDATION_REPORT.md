# V1.2.0 build validation

## Automated checks

- 13 focused tests passed.
- All Python application modules compiled successfully.
- Dashboard template rendered successfully with strict undefined-variable checking.
- Health payload reports version `1.2.0` and the frozen register checksum.
- Parent candidate-register bytes reproduce SHA-256 `a5ca94ff9b464cb1f28def2e1c3db90bfab5e707d2c6d373a8587207d9f2ecd8`.
- Active candidates are exactly C2 and C4.
- Threshold boundaries are inclusive and immutable.
- The worker cannot claim legacy event-archive, ten-day-context or baseline-context jobs.
- Legacy POST routes are disabled.
- The migration is additive and creates the external-validation job/file/issue tables.

## Reproduction check against the previously opened validation cohort

The V1.2 evaluator was run locally against the already analysed 48-group validation dataset. It reproduced the earlier headline results:

| Candidate | Groups | Event hit | Matched control pass | Lift | One-sided randomisation p |
|---|---:|---:|---:|---:|---:|
| C2 | 48 | 0.333333 | 0.162500 | 2.051282 | 0.005600 |
| C4 | 48 | 0.395833 | 0.228472 | 1.732523 | 0.008500 |

This verifies that the frozen-rule implementation matches the ChatGPT analysis rather than silently changing either rule.

## Scope boundary

No live Binance or Supabase integration test was run from the build environment. Network-dependent behaviour is covered through the inherited, previously deployed scanner/control/data-loading architecture plus static and local functional validation.
