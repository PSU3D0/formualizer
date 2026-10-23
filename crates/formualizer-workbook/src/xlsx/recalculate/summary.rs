//! Result validation and receipt accounting for evaluated source formulas.
//! Reporting reads current engine results; publication alone edits source bytes.
use super::*;

pub(super) fn unchanged(
    bytes: &[u8],
    formula_count: usize,
    summary: RecalculateSummary,
) -> XlsxRecalculateResult {
    XlsxRecalculateResult {
        bytes: bytes.to_vec(),
        summary,
        formula_cells: formula_count,
        cache_cells_changed: 0,
        worksheet_parts_changed: 0,
        clock_now_utc: None,
        external_links_used: 0,
    }
}
/// The validated value of one source formula, exactly as the scalar writer
/// requires it: present, 1x1 arrays coerced, ingested as a formula (or a
/// coerced parse error) and current.
pub(super) fn validated_result(
    value: Option<LiteralValue>,
    has_formula: bool,
    staleness: Staleness,
    coerced: bool,
    location: impl Fn() -> String,
) -> Result<LiteralValue, IoError> {
    let mut value = value.ok_or_else(|| unsupported("absent formula result", location()))?;
    if matches!(&value,LiteralValue::Array(rows) if rows.len()==1 && rows[0].len()==1) {
        value = value
            .coerce_to_single_value()
            .map_err(|_| unsupported("non-scalar result", "cache-only writer"))?;
    }
    if !(has_formula || (matches!(value, LiteralValue::Error(_)) && coerced)) {
        return Err(unsupported("source formula was not ingested", location()));
    }
    if staleness != Staleness::Current {
        return Err(unsupported(
            format!("formula result is not current: {staleness:?} after evaluation"),
            location(),
        ));
    }
    Ok(value)
}
/// Count one evaluated source formula (anchors included) in the summary.
/// A `#NAME?` without a message gets the reason its own formula gives.
pub(super) fn record_result(
    summary: &mut RecalculateSummary,
    engine: &Engine<WBResolver>,
    sheet: &str,
    cell: &sheet::FormulaMark,
    value: &LiteralValue,
    limit: usize,
) {
    let location = || format!("{sheet}!{}", cell.address());
    match value {
        LiteralValue::Error(error)
            if error.kind == formualizer_common::ExcelErrorKind::Name
                && error.message().is_none() =>
        {
            let mut error = error.clone();
            error.set_message(error_reasons::name_error_reason(
                engine, sheet, cell.row, cell.col,
            ));
            summary.record(sheet, location, &LiteralValue::Error(error), limit);
        }
        _ => summary.record(sheet, location, value, limit),
    }
}
