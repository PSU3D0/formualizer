use crate::traits::CalcValue;
use formualizer_common::{ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};

/// Admit only the final range result, before decoding it into owned cells.
/// A 1x1 view is a scalar at this boundary, even when the spill cap is zero.
/// This must not be used for arguments or intermediate expression values.
pub(crate) fn range_spill_error(value: &CalcValue<'_>, max_cells: u32) -> Option<ExcelError> {
    let CalcValue::Range(view) = value else {
        return None;
    };
    let (rows, cols) = view.dims();
    if (rows == 1 && cols == 1) || (rows as u64).saturating_mul(cols as u64) <= u64::from(max_cells)
    {
        return None;
    }
    Some(
        ExcelError::new(ExcelErrorKind::Spill)
            .with_message("SpillTooLarge")
            .with_extra(ExcelErrorExtra::Spill {
                expected_rows: rows as u32,
                expected_cols: cols as u32,
            }),
    )
}

pub(crate) fn materialize_published_calc_result(
    value: CalcValue<'_>,
    max_cells: u32,
) -> LiteralValue {
    if let Some(error) = range_spill_error(&value, max_cells) {
        LiteralValue::Error(error)
    } else {
        value.into_literal()
    }
}

pub(crate) fn finalize_published_calc_result(value: CalcValue<'_>, max_cells: u32) -> LiteralValue {
    finalize_formula_result(materialize_published_calc_result(value, max_cells))
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::range_view::RangeView;
    use crate::traits::RANGE_MATERIALIZED_CELLS;
    use formualizer_common::DateSystem;

    #[test]
    fn range_admission_preserves_spill_dimensions_without_decoding() {
        let value = CalcValue::Range(RangeView::from_owned_rows(
            vec![vec![LiteralValue::Number(1.0); 2]; 3],
            DateSystem::Excel1900,
        ));
        RANGE_MATERIALIZED_CELLS.with(|c| c.set(0));
        let LiteralValue::Error(error) = finalize_published_calc_result(value, 5) else {
            panic!("expected cap rejection");
        };
        assert_eq!(error.kind, ExcelErrorKind::Spill);
        assert_eq!(error.message.as_deref(), Some("SpillTooLarge"));
        assert_eq!(
            error.extra,
            ExcelErrorExtra::Spill {
                expected_rows: 3,
                expected_cols: 2
            }
        );
        assert_eq!(RANGE_MATERIALIZED_CELLS.with(|c| c.get()), 0);
    }

    #[test]
    fn scalar_range_is_not_a_spill_even_with_zero_cap() {
        let value = CalcValue::Range(RangeView::from_owned_rows(
            vec![vec![LiteralValue::Number(7.0)]],
            DateSystem::Excel1900,
        ));
        assert_eq!(
            finalize_published_calc_result(value, 0),
            LiteralValue::Number(7.0)
        );
    }
}
