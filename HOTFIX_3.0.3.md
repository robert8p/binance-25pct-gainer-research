# V3.0.3 — entry-feasibility and frozen analysis exporter

This upgrade corrects the key actionability blind spot in the completed stock dataset: a threshold could be sellable even though a retail investor had no realistic chance to buy after 09:30 ET and before the first +25% crossing.

V3.0.3:

- reclassifies all sellable positives using pre-cross NBBO asks and trades;
- applies position-size, reaction-time, continuous-duration and gross-edge gates;
- removes matched controls for non-actionable positives;
- creates fixed 14:00, 17:00 and 19:00 London predictor snapshots;
- freezes discovery, validation and sealed-test periods;
- isolates the sealed-test predictor matrix in `SEALED_TEST_DO_NOT_OPEN.zip`;
- adds repeated-symbol cross-split diagnostics;
- reuses existing V2/V3 data and does not recollect market history.
