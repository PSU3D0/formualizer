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

#[cfg(test)]
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

/// Fit an already evaluated result; intermediate arrays remain fully materialized.
fn fit_fixed_result(value: LiteralValue, rows: u32, cols: u32) -> LiteralValue {
    let LiteralValue::Array(source) = value else {
        return LiteralValue::Array(vec![vec![value; cols as usize]; rows as usize]);
    };
    let height = source.len();
    let width = source.first().map_or(0, Vec::len);
    LiteralValue::Array(
        (0..rows as usize)
            .map(|r| {
                (0..cols as usize)
                    .map(|c| {
                        let r = if height == 1 { 0 } else { r };
                        let c = if width == 1 { 0 } else { c };
                        source
                            .get(r)
                            .and_then(|row| row.get(c))
                            .cloned()
                            .unwrap_or_else(|| {
                                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))
                            })
                    })
                    .collect()
            })
            .collect(),
    )
}

impl<R: crate::traits::EvaluationContext> super::Engine<R> {
    #[cfg(debug_assertions)]
    pub(super) fn assert_fixed_result_fitted(
        &self,
        vertex: super::vertex::VertexId,
        value: &LiteralValue,
    ) {
        if self.graph.fixed_single_arrays.contains(&vertex) {
            debug_assert!(!matches!(value, LiteralValue::Array(_)));
        }
        if let Some(&(rows, cols)) = self.graph.fixed_array_shapes.get(&vertex) {
            debug_assert!(matches!(value, LiteralValue::Array(a)
                if a.len() == rows as usize && a.iter().all(|r| r.len() == cols as usize)));
        }
    }

    pub(super) fn fit_formula_error(
        &self,
        vertex: super::vertex::VertexId,
        error: ExcelError,
    ) -> Result<LiteralValue, ExcelError> {
        if self.graph.fixed_array_shapes.contains_key(&vertex) {
            Ok(self.fit_formula_result(vertex, LiteralValue::Error(error)))
        } else {
            Err(error)
        }
    }

    /// Scalar results never consult the single-cell flag. Multi-cell fixed
    /// declarations broadcast scalars, so their separate map is consulted.
    pub(super) fn fit_formula_result(
        &self,
        vertex: super::vertex::VertexId,
        value: LiteralValue,
    ) -> LiteralValue {
        let value = if matches!(value, LiteralValue::Array(_))
            && self.graph.fixed_single_arrays.contains(&vertex)
        {
            let LiteralValue::Array(rows) = value else {
                unreachable!()
            };
            rows.into_iter()
                .next()
                .and_then(|row| row.into_iter().next())
                .unwrap_or(LiteralValue::Empty)
        } else {
            value
        };
        let value = if let Some(&(rows, cols)) = self.graph.fixed_array_shapes.get(&vertex) {
            fit_fixed_result(value, rows, cols)
        } else {
            value
        };
        finalize_formula_result(value)
    }

    pub(super) fn materialize_formula_result(
        &self,
        vertex: super::vertex::VertexId,
        value: CalcValue<'_>,
    ) -> LiteralValue {
        let fixed_range = matches!(value, CalcValue::Range(_))
            && (self.graph.fixed_single_arrays.contains(&vertex)
                || self.graph.fixed_array_shapes.contains_key(&vertex));
        let value = if fixed_range {
            value.into_literal()
        } else {
            materialize_published_calc_result(value, self.config.spill.max_spill_cells)
        };
        self.fit_formula_result(vertex, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::range_view::RangeView;
    use crate::traits::RANGE_MATERIALIZED_CELLS;
    use formualizer_common::DateSystem;

    #[test]
    fn fixed_shape_broadcast_padding_truncation_and_error_fill() {
        let n = |n| LiteralValue::Number(n as f64);
        assert_eq!(
            fit_fixed_result(n(4), 2, 3),
            LiteralValue::Array(vec![vec![n(4); 3]; 2])
        );
        assert_eq!(
            fit_fixed_result(LiteralValue::Array(vec![vec![n(1), n(2)]]), 2, 2),
            LiteralValue::Array(vec![vec![n(1), n(2)]; 2])
        );
        assert_eq!(
            fit_fixed_result(LiteralValue::Array(vec![vec![n(1)], vec![n(2)]]), 2, 2),
            LiteralValue::Array(vec![vec![n(1); 2], vec![n(2); 2]])
        );
        let padded = fit_fixed_result(
            LiteralValue::Array(vec![vec![n(1), n(2)], vec![n(3), n(4)]]),
            3,
            3,
        );
        let LiteralValue::Array(rows) = padded else {
            panic!()
        };
        assert!(matches!(&rows[2][2], LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
        assert_eq!(
            fit_fixed_result(
                LiteralValue::Array(vec![vec![n(1), n(2)], vec![n(3), n(4)]]),
                1,
                1
            ),
            LiteralValue::Array(vec![vec![n(1)]])
        );
        let error = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div));
        assert_eq!(
            fit_fixed_result(error.clone(), 2, 2),
            LiteralValue::Array(vec![vec![error; 2]; 2])
        );
        assert_eq!(
            finalize_formula_result(fit_fixed_result(LiteralValue::Empty, 2, 2)),
            LiteralValue::Array(vec![vec![n(0); 2]; 2])
        );
    }

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
