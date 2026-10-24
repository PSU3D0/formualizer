//! Native recalc orchestration. Original worksheet marks remain immutable;
//! evaluation text and typed Arrow planes never pass through an ingestion ZIP.
use super::*;

pub(super) fn ingest_source<'a>(
    admission: SourceAdmission<'a>,
    options: &XlsxRecalculateOptions,
    clock: &mut PhaseClock,
) -> Result<Ingested<'a>, IoError> {
    let SourceAdmission {
        mut archive,
        sheets,
        date_system,
        plans,
        formula_count: _,
        values,
        native_metadata_error,
        metadata,
        external,
    } = admission;
    let anchors = plans.iter().any(|p| !p.ownership.anchors.is_empty());
    checkpoint(&options.cancel)?;
    // Cache/child masking and array normalization are logical native-plane
    // operations. Only evaluation text is transformed; no ingestion-view XML
    // or ZIP is created and the authoritative package stays intact.
    let has_tables = plans.iter().any(|p| !p.tables.is_empty());
    // Legacy implicit intersection for formulas Excel calculated (listed in
    // the calc chain); see `legacy_intersection`.
    let calc_chain = if legacy_intersection::needs_calc_chain(&plans, options)? {
        let calc_chain_part = package::calc_chain_part(&mut archive, options);
        legacy_intersection::calc_chain(&mut archive, calc_chain_part.as_deref(), &sheets, options)?
    } else {
        Default::default()
    };
    let table_names: Vec<String> = plans
        .iter()
        .flat_map(|p| &p.tables)
        .map(|t| t.name.to_lowercase())
        .collect();
    let mut evaluation_texts = Vec::with_capacity(plans.len());
    for (index, (sheet, plan)) in sheets.iter().zip(&plans).enumerate() {
        let mut transforms = Vec::new();
        if !calc_chain.is_empty() {
            let (legacy, _) = legacy_intersection::lowered_texts(
                index,
                plan,
                &calc_chain,
                &table_names,
                external.as_ref().map(|e| &e.range_names),
                options,
            )?;
            transforms.extend(legacy);
        }
        if has_tables {
            transforms.extend(table_lowering::lowered_texts(
                &sheet.name,
                plan,
                &sheets,
                &plans,
                options,
            )?);
        }
        evaluation_texts.push(formulas::EvaluationTexts::new(transforms)?);
    }
    inert_sheets::validate_projection_names(&mut archive, &sheets, options)?;
    clock.lap("native evaluation-text transforms (no XML/ZIP view)");
    if let Some(error) = native_metadata_error {
        return Err(error);
    }
    let workbook = package::read_part(
        &mut archive,
        "xl/workbook.xml",
        options.limits.max_worksheet_bytes,
    )?;
    let mut names = names::CalculationNames::new(
        &workbook,
        &sheets,
        options.cancel.clone(),
        names::NameImportPolicy::STRICT_REFERENCED,
    );
    checkpoint(&options.cancel)?;
    let mut config = options.eval_config.clone();
    config.date_system = date_system;
    let refuse_parse_failures = config.formula_parse_policy == FormulaParsePolicy::Strict;
    if refuse_parse_failures {
        config.formula_parse_policy = FormulaParsePolicy::CoerceToError;
    }
    // Policy: CSE evaluation bypasses family memoization, whose admitted
    // cached results are not declaration-sensitive. Non-CSE runs are unchanged.
    if plans
        .iter()
        .any(|p| p.ownership.anchors.values().any(|a| a.binding.is_none()))
    {
        config.family_execution = false;
    }
    // XLSX dates are serial caches. Native chrono materialization cannot retain
    // Excel-1900 phantom serial 60 and can discard fractional duration precision.
    config.temporal_egress = formualizer_eval::engine::TemporalEgress::Serial;
    // External references read the link caches through engine sources named
    // by their reference text, defined before any formula is staged.
    let mut skipped_external_names = Vec::new();
    let (external_values, external_links_used, external_first_read) = match external {
        Some(plan) => {
            skipped_external_names = plan.skipped_names;
            (
                Some(std::sync::Arc::new(plan.values)),
                plan.links_used,
                plan.first_read,
            )
        }
        None => (None, 0, None),
    };
    let resolver = external_values
        .clone()
        .map_or_else(WBResolver::default, WBResolver::with_external_values);
    let mut engine: Engine<WBResolver> = Engine::new(resolver, config);
    engine.set_workbook_load_limits(admission::load_limits(options));
    if let Some(values) = &external_values {
        engine.adopt_file_sheets(sheets.iter().map(|s| s.name.as_str()))?;
        // Cached values never change during a run: a fixed source version.
        for name in values.scalar_names() {
            engine.define_source_scalar(name, Some(0))?;
        }
        for name in values.range_names() {
            engine.define_source_table(name, Some(0))?;
        }
        names.admit_external(values.clone(), skipped_external_names);
    }
    names.validate_calculation_names(&[], sheets.iter().any(|s| s.inert))?;
    if sheets.iter().any(|sheet| sheet.inert) {
        inert_sheets::validate_native(&sheets, &plans, names.calculation_name_asts()?, options)?;
    }
    if plans.iter().any(|p| !p.tables.is_empty()) {
        use formualizer_eval::reference::{CellRef, Coord, RangeRef};
        engine.adopt_file_sheets(sheets.iter().map(|s| s.name.as_str()))?;
        for (sheet, plan) in sheets.iter().zip(&plans) {
            let id = engine.sheet_id(&sheet.name).expect("adopted worksheet");
            for table in &plan.tables {
                checkpoint(&options.cancel)?;
                let r = table.rect;
                engine.define_table(
                    &table.name,
                    RangeRef::new(
                        CellRef::new(id, Coord::from_excel(r.first_row, r.first_col, true, true)),
                        CellRef::new(id, Coord::from_excel(r.last_row, r.last_col, true, true)),
                    ),
                    table.header,
                    table.columns.iter().map(|c| c.name.clone()).collect(),
                    table.totals,
                )?;
            }
        }
    }
    checkpoint(&options.cancel)?;
    clock.lap("native names, tables");
    let ingested = stream(
        &mut engine,
        &sheets,
        &plans,
        (values, evaluation_texts),
        &mut names,
        external_values.is_some(),
        options,
    );
    checkpoint(&options.cancel)?;
    ingested?;
    clock.lap("stream into engine");
    refuse_parse_failure(&engine, refuse_parse_failures)?;
    for (sheet, plan) in sheets.iter().zip(&plans) {
        for table in &plan.tables {
            if table.header {
                for (i, column) in table.columns.iter().enumerate() {
                    checkpoint(&options.cancel)?;
                    let address = CellAddress::new(
                        &sheet.name,
                        table.rect.first_row,
                        table.rect.first_col + i as u32,
                    )
                    .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
                    let snapshot = engine
                        .inspect_cell(&address, &SnapshotOptions::default())
                        .map_err(|e| IoError::from_backend("xlsx-inspect", e))?
                        .cell;
                    if snapshot.formula.is_some()
                        || snapshot.value != Some(LiteralValue::Text(column.name.clone()))
                    {
                        return Err(unsupported(
                            "table header cached text mismatch",
                            format!("table {} column {}", table.name, column.name),
                        ));
                    }
                }
            }
        }
    }
    let unimported: std::collections::HashSet<_> =
        names.unimported_document_names().into_iter().collect();
    let guard_indirect = names.has_new_unimported_names();
    if !unimported.is_empty() {
        // Metadata-only names may be omitted from the engine only if no source
        // calculation references them. Only a formula whose source text names
        // one can; a shared formula's copies name what its anchor names. Inspect
        // those after shared-formula replay but before evaluation/publication.
        let mut source_formulas = Vec::new();
        for (sheet, plan) in sheets.iter().zip(&plans) {
            for cell in &plan.cells {
                checkpoint(&options.cancel)?;
                let text = cell.formula_text.to_ascii_lowercase();
                if !formula_mentions_unimported_name(
                    &cell.formula_text,
                    &unimported,
                    guard_indirect,
                ) && !(guard_indirect && text.contains("indirect"))
                {
                    continue;
                }
                let address = CellAddress::new(&sheet.name, cell.row, cell.col)
                    .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
                if let Some(formula) = engine
                    .inspect_cell(&address, &SnapshotOptions::default())
                    .map_err(|e| IoError::from_backend("xlsx-inspect", e))?
                    .cell
                    .formula
                {
                    source_formulas.push((sheet.name.clone(), formula));
                }
            }
        }
        names.validate_calculation_names(&source_formulas, sheets.iter().any(|s| s.inert))?;
        clock.lap("document-name reference check");
    }
    visibility_guard::validate(&engine, &sheets, &plans, options)?;
    checkpoint(&options.cancel)?;
    drop(names);
    if anchors {
        declare_anchors(&mut engine, &sheets, &plans, options)?;
        refuse_parse_failure(&engine, refuse_parse_failures)?;
    }
    clock.lap("post-ingest checks (headers, visibility, anchors)");
    Ok(Ingested {
        archive,
        sheets,
        plans,
        engine,
        metadata,
        refuse_parse_failures,
        external_links_used,
        external_first_read,
    })
}

fn ingest_error(error: impl std::fmt::Display) -> IoError {
    IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Io(
        std::io::Error::other(error.to_string()),
    ))
}
use crate::xlsx::ingest::stream::{FormulaBatches, LoadConfig, SheetFormulas, install_values};

fn stream(
    engine: &mut Engine<WBResolver>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    planes: (Vec<admission::ValueResult>, Vec<formulas::EvaluationTexts>),
    names: &mut names::CalculationNames,
    external: bool,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    let (values, texts) = planes;
    engine
        .adopt_file_sheets(sheets.iter().map(|sheet| sheet.name.as_str()))
        .map_err(ingest_error)?;
    let previous = LoadConfig::begin(engine, external, false);
    let result = (|| -> Result<(), IoError> {
        let mut usage = formulas::WorkbookSpoolUsage::default();
        let mut batches = FormulaBatches::default();
        for (instance, (((sheet, plan), values), texts)) in
            sheets.iter().zip(plans).zip(values).zip(texts).enumerate()
        {
            checkpoint(&options.cancel)?;
            let values = match values {
                Ok(values) => values,
                Err(deferred) => {
                    formulas::validate_reader_prefix(plan, &texts, deferred.position)?;
                    return Err(deferred.error);
                }
            };
            let formula = formulas::stage(
                engine,
                &sheet.name,
                instance as u32,
                plan,
                &texts,
                usage,
                options,
            )?;
            usage.bytes = usage
                .bytes
                .checked_add(formula.spool_bytes)
                .expect("preflighted workbook spool bytes");
            usage.files = usage
                .files
                .checked_add(u32::from(formula.spool_spilled))
                .expect("preflighted workbook spool files");
            if std::env::var("FZ_DEBUG_LOAD")
                .ok()
                .is_some_and(|value| value != "0")
            {
                eprintln!(
                    "[fz][native] {} rows={} cols={} sparse={} values={}/{} formulas={} shared_tags={}",
                    sheet.name,
                    values.dimensions.0,
                    values.dimensions.1,
                    values.used_sparse_fallback,
                    values.values_observed,
                    values.values_handed_to_engine,
                    formula.formulas_observed,
                    formula.shared_tags
                );
            }
            install_values(engine, &sheet.name, values.arrow);
            batches.push(
                engine,
                &sheet.name,
                SheetFormulas {
                    records: formula.records,
                    report: formula.report,
                    families: formula.families,
                    partitioned: formula.partitioned,
                    preparation: formula.preparation,
                    deferred: formula.deferred,
                },
            );
        }
        checkpoint(&options.cancel)?;
        if !batches.replay.is_empty() {
            batches.install_replay(engine).map_err(IoError::XlsxRead)?;
        }
        checkpoint(&options.cancel)?;
        if !batches.direct.is_empty() {
            batches.install_direct(engine).map_err(IoError::XlsxRead)?;
        }
        checkpoint(&options.cancel)?;
        names.register(engine).map_err(IoError::XlsxRead)?;
        checkpoint(&options.cancel)?;
        Ok(())
    })();
    previous.restore(engine);
    if result.is_ok() {
        engine.finalize_all_sheet_indexes(sheets.iter().map(|sheet| sheet.name.as_str()));
    }
    result
}
