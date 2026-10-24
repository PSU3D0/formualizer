//! Source-preserving scalar and spill cache publication.
//! Evaluation text stays separate; all edits target authoritative source bytes.
use super::*;
mod coalesce;
mod mask_child;
pub(in crate::xlsx) mod patches;
#[cfg(test)]
pub(in crate::xlsx) use super::tests::oracles::ingestion_view::patches;
pub(in crate::xlsx) use coalesce::coalesce;
pub(in crate::xlsx) use mask_child::mask_child;
use patches::cache_patches;

/// Package edits produced by a publication path.
struct Publication {
    summary: RecalculateSummary,
    changed: usize,
    /// Worksheet replacements, plus metadata, relationship
    /// and content-type edits.
    edits: package::Edits,
    worksheets: usize,
}
/// Account a replaced (`old` bytes) or added (`old == 0`) member against
/// the expanded-output bound.
fn expand(
    expanded: &mut usize,
    old: usize,
    new: usize,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    *expanded = expanded
        .checked_sub(old)
        .and_then(|n| n.checked_add(new))
        .ok_or_else(|| unsupported("expanded output overflow", "workbook"))?;
    if *expanded > options.limits.max_expanded_bytes {
        return Err(unsupported("expanded output byte limit", "workbook"));
    }
    Ok(())
}
/// Apply one worksheet's patches within the worksheet and expanded-output
/// bounds.
fn replace_sheet(
    publication: &mut Publication,
    expanded: &mut usize,
    sheet: &package::Sheet,
    data: &[u8],
    patches: Vec<Patch>,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if patches.is_empty() {
        return Ok(());
    }
    let patched = apply_patches(data, patches, options.limits.max_worksheet_bytes)?;
    expand(expanded, data.len(), patched.len(), options)?;
    publication
        .edits
        .replace
        .insert(sheet.part.clone(), patched);
    publication.worksheets += 1;
    Ok(())
}
/// Validate every evaluated result, build the worksheet edits and write the
/// package. Nothing is published before every check passed.
pub(super) fn publish_caches(
    bytes: &[u8],
    ingested: Ingested<'_>,
    formula_count: usize,
    options: &XlsxRecalculateOptions,
    clock: &mut PhaseClock,
) -> Result<XlsxRecalculateResult, IoError> {
    let Ingested {
        mut archive,
        sheets,
        plans,
        engine,
        metadata,
        refuse_parse_failures: _,
        external_links_used,
        external_first_read: _,
    } = ingested;
    let coerced: HashSet<_> = engine
        .formula_parse_diagnostics()
        .iter()
        .filter(|d| d.policy == FormulaParsePolicy::CoerceToError)
        .map(|d| (d.sheet.clone(), d.row, d.col))
        .collect();
    // Declared central-directory sizes: ZIP7's decompressed_size() is None
    // for members with data descriptors, which admission accepts.
    let mut expanded = package::expanded_size(&mut archive)?;
    let Publication {
        mut summary,
        changed,
        edits,
        worksheets,
    } = publish_spill_caches(
        &engine,
        &mut archive,
        &sheets,
        &plans,
        metadata.as_ref(),
        &coerced,
        &mut expanded,
        options,
    )?;
    clock.lap("publication (validate results, build patches)");
    summary.status = if summary.errors == 0 {
        RecalculateStatus::Success
    } else {
        RecalculateStatus::ErrorsFound
    };
    checkpoint(&options.cancel)?;
    if edits.is_empty() {
        if bytes.len() > options.limits.max_output_bytes {
            return Err(unsupported("output byte limit", "XLSX package"));
        }
        let mut result = unchanged(bytes, formula_count, summary);
        result.external_links_used = external_links_used;
        return Ok(result);
    }
    let output = package::rewrite(bytes, &mut archive, &edits, options)?;
    clock.lap("package rewrite");
    if !edits.add.is_empty() {
        // An added part must agree with its relationship and content type.
        package::check_output(&output, options)?;
    }
    checkpoint(&options.cancel)?;
    Ok(XlsxRecalculateResult {
        bytes: output,
        summary,
        formula_cells: formula_count,
        cache_cells_changed: changed,
        worksheet_parts_changed: worksheets,
        clock_now_utc: None,
        external_links_used,
    })
}
/// Publication: scalar caches and dynamic-array geometry, plus, when an
/// anchor needs
/// a new XLDAPR binding, the metadata part edit (or addition with its
/// relationship and content-type override).
#[allow(clippy::too_many_arguments)]
fn publish_spill_caches(
    engine: &Engine<WBResolver>,
    archive: &mut package::Archive<'_>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    metadata: Option<&dynamic_metadata::DynamicMetadata>,
    coerced: &HashSet<(String, u32, u32)>,
    expanded: &mut usize,
    options: &XlsxRecalculateOptions,
) -> Result<Publication, IoError> {
    let mut binder = dynamic_metadata::Binder::new(metadata);
    let planned = plan_spill_publication(
        engine,
        sheets,
        plans,
        coerced,
        options,
        &mut |sheet, request| {
            if !request.multi_cell {
                return Err(unsupported(
                    "dynamic array binding requested for a one-cell result",
                    format!("{sheet} R{}C{}", request.row, request.col),
                ));
            }
            binder.bind(options)
        },
    )?;
    let mut publication = Publication {
        summary: planned.summary,
        changed: planned.changed,
        edits: package::Edits::default(),
        worksheets: 0,
    };
    for ((sheet, plan), patches) in sheets.iter().zip(plans).zip(planned.patches) {
        checkpoint(&options.cancel)?;
        replace_sheet(
            &mut publication,
            expanded,
            sheet,
            &plan.data,
            patches,
            options,
        )?;
    }
    if let Some(part) = binder.finish(options)? {
        checkpoint(&options.cancel)?;
        let edits = &mut publication.edits;
        if part.added {
            expand(expanded, 0, part.bytes.len(), options)?;
            for (name, data) in [
                (
                    package::WORKBOOK_RELS,
                    package::add_relationship(
                        archive,
                        "xl/workbook.xml",
                        dynamic_metadata::SHEET_METADATA_RELATIONSHIP,
                        "metadata.xml",
                        options,
                    )?,
                ),
                (
                    "[Content_Types].xml",
                    package::add_override(
                        archive,
                        &part.part,
                        dynamic_metadata::SHEET_METADATA_CONTENT_TYPE,
                        options,
                    )?,
                ),
            ] {
                let old = archive
                    .by_name(name)
                    .map_err(|e| IoError::from_backend("zip", e))?
                    .size();
                expand(expanded, old as usize, data.len(), options)?;
                edits.replace.insert(name.to_owned(), data);
            }
            edits.add.insert(part.part, part.bytes);
        } else {
            let old = metadata.map_or(0, |m| m.source_len());
            expand(expanded, old, part.bytes.len(), options)?;
            edits.replace.insert(part.part, part.bytes);
        }
    }
    Ok(publication)
}
/// [`geometry::BindingResolver`] with the worksheet name.
type SheetBindingResolver<'a> =
    dyn FnMut(&str, geometry::BindingRequest) -> Result<u32, IoError> + 'a;
/// Validated projection and geometry of every worksheet, not yet applied.
pub(super) struct SpillPlan {
    pub(super) summary: RecalculateSummary,
    pub(super) changed: usize,
    /// Ordered patches per worksheet, parallel to the sheet plans.
    pub(super) patches: Vec<Vec<Patch>>,
}
/// Project every source formula (nothing materialized), check the spill
/// bounds, then build one ordered geometry plan per worksheet, reading each
/// member at the final engine state. `bind` resolves a requested XLDAPR
/// binding to a one-based `cm`.
pub(super) fn plan_spill_publication(
    engine: &Engine<WBResolver>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    coerced: &HashSet<(String, u32, u32)>,
    options: &XlsxRecalculateOptions,
    bind: &mut SheetBindingResolver<'_>,
) -> Result<SpillPlan, IoError> {
    let date_system = engine.config.date_system;
    let mut publication = SpillPlan {
        summary: RecalculateSummary::default(),
        changed: 0,
        patches: Vec::with_capacity(plans.len()),
    };
    // Borrow sheet names and copy coordinates once, instead of cloning a
    // worksheet name for every membership probe (including an empty set).
    let mut coerced_by_sheet: HashMap<&str, HashSet<(u32, u32)>> = HashMap::new();
    for (sheet, row, col) in coerced {
        coerced_by_sheet
            .entry(sheet.as_str())
            .or_default()
            .insert((*row, *col));
    }
    // 1. Project every source formula; nothing is materialized yet.
    let mut projected = Vec::with_capacity(plans.len());
    for (sheet, plan) in sheets.iter().zip(plans) {
        // Scalar caches are encoded into patches right away, as on the
        // pre-spill writer; only anchors are retained for the bounds pass.
        let mut scalar_patches = Vec::new();
        let mut anchors = Vec::new();
        let mut address = CellAddress {
            sheet: sheet.name.clone(),
            row: 1,
            column: 1,
        };
        let sheet_coerced = coerced_by_sheet.get(sheet.name.as_str());
        for (i, cell) in plan.cells.iter().enumerate() {
            checkpoint(&options.cancel)?;
            let prior = plan
                .ownership
                .anchors
                .get(&(cell.row, cell.col))
                .filter(|a| a.formula == i);
            let summary = &mut publication.summary;
            address.row = cell.row;
            address.column = cell.col;
            let projection = projection::project_formula(
                engine,
                &address,
                cell,
                i,
                prior,
                sheet_coerced.is_some_and(|cells| cells.contains(&(cell.row, cell.col))),
                date_system,
                &mut |value| {
                    record_result(
                        summary,
                        engine,
                        &sheet.name,
                        cell,
                        value,
                        options.error_location_limit,
                    )
                },
            )?;
            match projection {
                projection::Projection::Scalar(cache) => {
                    if !cache.matches(cell, &plan.data) {
                        publication.changed += 1;
                        cache_patches(&plan.data, cell, &cache, &mut scalar_patches);
                    }
                }
                projection::Projection::Anchor(anchor) => {
                    check_spill_policy(&options.eval_config.spill)?;
                    use projection::Shape;
                    let spill = match anchor.shape {
                        Shape::Spill(extent) => Some(extent),
                        Shape::Collapsed | Shape::Blocked | Shape::Error => None,
                    };
                    // A source record keeps describing the anchor unless it
                    // is a collapsed record and the result spills again.
                    let binding = match anchor.prior {
                        Some(prior)
                            if !prior
                                .binding
                                .is_some_and(|b| b.collapsed && spill.is_some()) =>
                        {
                            geometry::Binding::Keep
                        }
                        _ => geometry::Binding::NeedsXldapr,
                    };
                    anchors.push(geometry::AnchorEdit {
                        row: anchor.row,
                        col: anchor.col,
                        formula: anchor.formula,
                        spill,
                        cache: anchor.cache,
                        binding,
                    });
                }
            }
        }
        projected.push((scalar_patches, anchors));
    }
    // 2. Bounds before any member value is read: width, merges, generated
    //    serialized cells and the logical area, within the existing limits.
    let mut serialized = 0u64;
    let mut inserted = 0u64;
    let mut logical = 0u64;
    // A sheet with a new spill but no admitted anchor gets its source index
    // now, from the retained worksheet bytes; sheets without any anchor
    // projection never build one.
    let mut built = Vec::with_capacity(plans.len());
    for (plan, (_, anchors)) in plans.iter().zip(&projected) {
        checkpoint(&options.cancel)?;
        built.push(if plan.index.is_none() && !anchors.is_empty() {
            match sheet::scan(
                &plan.data,
                options,
                sheet::ScanStrategy::Indexed,
                &mut 0,
                &mut 0,
            )? {
                sheet::Scanned::Done(scan) => scan.index,
                sheet::Scanned::NeedsIndex => None,
            }
        } else {
            None
        });
    }
    let indexes: Vec<Option<&sheet::SourceIndex>> = plans
        .iter()
        .zip(&built)
        .map(|(plan, built)| plan.index.as_ref().or(built.as_ref()))
        .collect();
    for ((plan, (_, anchors)), index) in plans.iter().zip(&projected).zip(&indexes) {
        serialized = serialized.saturating_add(plan.serialized as u64);
        let preflight = match index {
            Some(index) => geometry::preflight(index, anchors, options)?,
            None if anchors.is_empty() => geometry::Preflight::default(),
            None => {
                return Err(unsupported(
                    "spill geometry without a source index",
                    "worksheet",
                ));
            }
        };
        inserted = inserted.saturating_add(preflight.inserted);
        let rows = plan.bounds.0.max(preflight.bounds.0);
        let cols = plan.bounds.1.max(preflight.bounds.1);
        logical = logical.saturating_add(u64::from(rows) * u64::from(cols));
    }
    if serialized.saturating_add(inserted) > options.limits.max_cells as u64 {
        return Err(unsupported("generated spill cell limit", "workbook"));
    }
    if logical > options.limits.max_cells as u64 {
        return Err(unsupported("workbook logical cell limit", "workbook"));
    }
    // 3. One ordered geometry plan per worksheet.
    for (((sheet, plan), (mut patches, anchors)), index) in
        sheets.iter().zip(plans).zip(projected).zip(indexes)
    {
        let Some(index) = index.filter(|_| !anchors.is_empty()) else {
            // No dynamic array on this sheet: scalar caches only.
            ingest_view::coalesce(&mut patches)?;
            publication.patches.push(patches);
            continue;
        };
        let edits = geometry::plan(
            plan,
            index,
            &anchors,
            &mut |anchor, row, col| {
                projection::member(
                    engine,
                    &sheet.name,
                    (anchor.row, anchor.col),
                    row,
                    col,
                    date_system,
                )
            },
            &mut |request| bind(&sheet.name, request),
            &options.cancel,
        )?;
        publication.changed += edits.caches_changed;
        patches.extend(edits.patches);
        // Scalar and geometry edits touch disjoint cells; merging them
        // refuses any equal-offset insertion conflict or overlap.
        ingest_view::coalesce(&mut patches)?;
        publication.patches.push(patches);
    }
    Ok(publication)
}
