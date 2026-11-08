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
        calculation: None,
        coverage: crate::SourceCalculationCoverage::Complete,
        coverage_scope: crate::SourceCoverageScope::None,
        caches_invalidated: 0,
        calculation_flags_changed: false,
        operation_diagnostics: Default::default(),
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
/// Missing payloads stay unknown except for proven root unknown calls.
pub(super) fn record_result(
    summary: &mut RecalculateSummary,
    engine: &Engine<WBResolver>,
    sheet: &str,
    cell: &sheet::FormulaMark,
    value: &LiteralValue,
    limit: usize,
    coverage: &availability::Coverage,
) {
    let location = || format!("{sheet}!{}", cell.address());
    summary.record(sheet, location, value, limit);
    let LiteralValue::Error(error) = value else {
        return;
    };
    if error.kind != formualizer_common::ExcelErrorKind::Name
        || error.message().is_some_and(|message| !message.is_empty())
    {
        return;
    }
    let entry = summary
        .error_summary
        .get_mut("#NAME?")
        .expect("recorded error");
    // Only enrich a retained location, never allocate reasons for truncated cells.
    if entry.count > limit || entry.messages.last().is_none_or(Option::is_some) {
        return;
    }
    if let Some(message) = root_unknown_reason(engine, sheet, cell, coverage) {
        *entry.messages.last_mut().expect("retained location") = Some(message);
    }
}

/// Unknown dispatch returns #NAME? before evaluating arguments (tree and arena
/// interpreters). Match executed evidence as well as the unmodified source root;
/// syntax alone, or an unknown call inside a wrapper, cannot prove a final cause.
fn root_unknown_reason(
    engine: &Engine<WBResolver>,
    sheet: &str,
    cell: &sheet::FormulaMark,
    coverage: &availability::Coverage,
) -> Option<String> {
    // Exclude declared arrays, dynamic metadata and shared formula envelopes.
    // In particular, never infer through implicit intersection or postfix calls.
    if cell.formula_kind != "normal" || cell.cm().is_some() || cell.array_ref().is_some() {
        return None;
    }
    let source = if cell.formula_text.starts_with('=') {
        std::borrow::Cow::Borrowed(cell.formula_text.as_str())
    } else {
        std::borrow::Cow::Owned(format!("={}", cell.formula_text))
    };
    let stream = formualizer_parse::TokenStream::new(&source).ok()?;
    let ast = formualizer_parse::parser::Parser::from_token_stream(&stream)
        .parse()
        .ok()?;
    let formualizer_parse::ASTNodeType::Function { name, .. } = ast.node_type else {
        return None;
    };
    if name.starts_with('@') || engine.has_name(&name, Some(sheet)) {
        return None;
    }
    let sheet_id = engine.sheet_id(sheet)?;
    coverage.final_reason_roots.iter().find_map(|root| {
        let at = root.cell?;
        if at.sheet_id != sheet_id
            || at.coord.row() + 1 != cell.row
            || at.coord.col() + 1 != cell.col
        {
            return None;
        }
        match root.operation.as_ref() {
            formualizer_eval::operation_diagnostics::UnavailableOperation::UnknownFunction {
                name: executed,
            } if executed.eq_ignore_ascii_case(&name) => Some(format!("Unknown function: {name}")),
            _ => None,
        }
    })
}
