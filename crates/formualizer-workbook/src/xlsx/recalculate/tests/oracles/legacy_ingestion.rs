//! Retained compatibility ingestion used only for differential migration tests.
//! Its Calamine reader and replay decisions remain independent of native sinks.
use super::super::super::*;

pub(in crate::xlsx::recalculate) fn ingest_source<'a>(
    bytes: &[u8],
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
        values: _,
        native_metadata_error: _,
        metadata,
        external,
    } = admission;
    let anchors = plans.iter().any(|p| !p.ownership.anchors.is_empty());
    checkpoint(&options.cancel)?;
    // Calamine 0.36 cannot decode every legal/stale cache representation,
    // although formula ingestion ignores cached results. Clear those caches,
    // and mask/normalize dynamic arrays, in a bounded transient ingestion
    // view; the authoritative package stays intact.
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
    let mut view_parts = BTreeMap::new();
    for (index, (sheet, plan)) in sheets.iter().zip(&plans).enumerate() {
        let mut patches = ingest_view::patches(plan, &options.cancel)?;
        if !calc_chain.is_empty() {
            let (legacy, _) = legacy_intersection::patches(
                index,
                plan,
                &calc_chain,
                &table_names,
                external.as_ref().map(|e| &e.range_names),
                options,
            )?;
            patches.extend(legacy);
        }
        if has_tables {
            patches.extend(table_lowering::patches(
                &sheet.name,
                plan,
                &sheets,
                &plans,
                options,
            )?);
        }
        if !patches.is_empty() {
            view_parts.insert(
                sheet.part.clone(),
                apply_patches(&plan.data, patches, options.limits.max_worksheet_bytes)?,
            );
        }
    }
    let mut view_edits = package::Edits {
        replace: view_parts,
        add: BTreeMap::new(),
    };
    inert_sheets::project(&mut archive, &sheets, &mut view_edits, options)?;
    let ingest_bytes = if view_edits.replace.is_empty() && view_edits.add.is_empty() {
        bytes.to_vec()
    } else {
        package::rewrite(bytes, &mut archive, &view_edits, options)?
    };
    clock.lap("ingest view (calc chain, patches, package rewrite)");
    let opened = if let Some(cancel) = options.cancel.clone() {
        CalamineAdapter::open_bytes_cancellable(ingest_bytes, cancel)
    } else {
        CalamineAdapter::open_bytes(ingest_bytes)
    };
    checkpoint(&options.cancel)?;
    let mut adapter = opened.map_err(IoError::Calamine)?;
    if adapter.sheet_names().map_err(IoError::Calamine)?
        != sheets.iter().map(|s| s.name.clone()).collect::<Vec<_>>()
    {
        return Err(unsupported(
            "adapter/preflight sheet mapping mismatch",
            "workbook",
        ));
    }
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
    let (external_values, external_links_used, external_first_read) = match external {
        Some(plan) => {
            adapter.skip_external_names(plan.skipped_names);
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
    let mut load_limits = engine.workbook_load_limits().clone();
    load_limits.max_sheet_cols = load_limits.max_sheet_cols.min(options.limits.max_columns);
    load_limits.max_sheet_logical_cells = load_limits
        .max_sheet_logical_cells
        .min(options.limits.max_cells as u64);
    load_limits.max_formula_spool_bytes_per_sheet = load_limits
        .max_formula_spool_bytes_per_sheet
        .min(options.limits.max_expanded_bytes as u64);
    load_limits.max_formula_spool_bytes_per_workbook = load_limits
        .max_formula_spool_bytes_per_workbook
        .min(options.limits.max_expanded_bytes as u64);
    engine.set_workbook_load_limits(load_limits);
    if let Some(values) = &external_values {
        engine.adopt_file_sheets(sheets.iter().map(|s| s.name.as_str()))?;
        // Cached values never change during a run: a fixed source version.
        for name in values.scalar_names() {
            engine.define_source_scalar(name, Some(0))?;
        }
        for name in values.range_names() {
            engine.define_source_table(name, Some(0))?;
        }
        adapter.admit_external_references(values.clone());
    }
    adapter.validate_calculation_names(&[], sheets.iter().any(|s| s.inert))?;
    inert_sheets::validate(&sheets, &plans, &adapter, options)?;
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
    clock.lap("calamine open, names, tables");
    let ingested = adapter.stream_into_engine(&mut engine);
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
        adapter.unimported_document_names().into_iter().collect();
    let guard_indirect = adapter.has_new_unimported_names();
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
        adapter.validate_calculation_names(&source_formulas, sheets.iter().any(|s| s.inert))?;
        clock.lap("document-name reference check");
    }
    visibility_guard::validate(&engine, &sheets, &plans, options)?;
    checkpoint(&options.cancel)?;
    drop(adapter);
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
