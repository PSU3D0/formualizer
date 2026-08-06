use formualizer_common::LiteralValue;

/// Finalize a formula result immediately before it is published to the grid.
///
/// Excel exposes a blank-cell passthrough as numeric zero once it becomes a
/// formula cell's result. `Number(0.0)` matches the evaluator's existing
/// coercion results; stored blank cells remain `Empty` because only formula
/// publication calls this function.
pub(crate) fn finalize_formula_result(value: LiteralValue) -> LiteralValue {
    match value {
        LiteralValue::Empty => LiteralValue::Number(0.0),
        LiteralValue::Array(rows) => LiteralValue::Array(
            rows.into_iter()
                .map(|row| row.into_iter().map(finalize_formula_result).collect())
                .collect(),
        ),
        other => other,
    }
}
