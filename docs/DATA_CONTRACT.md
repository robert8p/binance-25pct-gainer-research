# V1.3 external-validation data contract

The canonical raw file is `external_validation_features.parquet` inside one or more numbered ZIP parts.

Each sample has one feature row at `pre_cross_horizon_minutes = 480`.

Key grouping fields:

- `match_group_id`: one saleable event and its same-symbol controls
- `label`: 1 event, 0 control
- `symbol`
- `cross_anchor_time`

Quality filters applied by the evaluator:

- `feature_quality_status == pass`
- controls with `pseudo_window_contaminated_control == true` excluded
- matched group requires one usable event and at least one usable control

The package contains `C2_pass` and `C4_pass` only because these definitions were frozen before this external period was opened. They are mechanical evaluations, not app-discovered signals.

All columns beginning `outcome_` are diagnostic labels and must not be used to modify the frozen rules.
