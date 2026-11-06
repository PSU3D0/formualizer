//! Source policy consumes engine relation facts; it reconstructs no dependency graph.
use super::*;
use crate::{
    CalculationDeferredReason, SourceDependencyEvidence, SourceFunctionIncidence,
    SourceFunctionModeIncidence, SourceModeVerification, SourceOperationDiagnostics,
    SourceOperationSample, SourceUnavailableOperation,
};
use formualizer_eval::{
    engine::affected_closure::{
        AffectedRoot, ClosureError, ClosureLimits, ClosureResource, RegionPiece,
    },
    operation_diagnostics::UnavailableOperation,
};
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct Coverage {
    pub has_gaps: bool,
    pub regions: Vec<(String, sheet::SourceRect)>,
    witnessed: Vec<RegionPiece>,
    uncertain: Vec<RegionPiece>,
    pub diagnostics: SourceOperationDiagnostics,
    pub unknown_functions: BTreeMap<String, usize>,
}
impl Coverage {
    pub fn affects(&self, sheet_id: u16, row: u32, col: u32) -> bool {
        self.witnessed
            .iter()
            .chain(&self.uncertain)
            .any(|piece| piece.sheet == sheet_id && piece.rect.contains(row - 1, col - 1))
    }
}
fn defer(reason: CalculationDeferredReason, context: impl Into<String>) -> IoError {
    IoError::CalculationDeferred {
        reason,
        context: context.into(),
    }
}
fn resource(
    reason: formualizer_common::ResourceExhaustionReason,
    limit: u64,
    observed: u64,
    message: &'static str,
) -> IoError {
    formualizer_eval::engine::ResourceLedgerError::Exhausted(
        formualizer_common::ResourceExhaustionDetail {
            reason,
            limit,
            observed,
            request_id: None,
        },
    )
    .into_excel_error()
    .with_message(message)
    .into()
}
fn closure_error(error: ClosureError) -> IoError {
    use formualizer_common::ResourceExhaustionReason as Reason;
    match error {
        ClosureError::Limit {
            resource: kind,
            limit,
            attempted,
        } => resource(
            match kind {
                ClosureResource::Work => Reason::WorkUnits,
                ClosureResource::Output => Reason::DependencyClosureOutputItems,
                ClosureResource::Scratch => Reason::ScratchMemory,
            },
            limit,
            attempted,
            "source affected-dependency query limit",
        ),
        ClosureError::ArithmeticOverflow => resource(
            Reason::ArithmeticOverflow,
            u64::MAX,
            u64::MAX,
            "source affected-dependency query arithmetic overflow",
        ),
        ClosureError::Allocation => resource(
            Reason::ScratchMemory,
            u64::MAX,
            0,
            "source affected-dependency query allocation failed",
        ),
        ClosureError::Busy | ClosureError::StaleSnapshot => defer(
            CalculationDeferredReason::DependencyCoverageUnavailable,
            "source dependency snapshot is not quiescent/current",
        ),
        other => IoError::Backend {
            backend: "source-dependency-integrity".into(),
            message: format!("{other:?}"),
        },
    }
}
fn push_root(
    roots: &mut Vec<AffectedRoot>,
    root: AffectedRoot,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if options.profile != crate::XlsxRecalculateProfile::Workbook || options.require_complete {
        return Ok(());
    }
    let attempted = roots
        .len()
        .checked_add(1)
        .ok_or_else(|| closure_error(ClosureError::ArithmeticOverflow))?;
    if attempted > options.limits.max_affected_regions {
        return Err(resource(
            formualizer_common::ResourceExhaustionReason::DependencyClosureOutputItems,
            options.limits.max_affected_regions as u64,
            attempted as u64,
            "source dependency root descriptor limit",
        ));
    }
    roots
        .try_reserve(1)
        .map_err(|_| closure_error(ClosureError::Allocation))?;
    roots.push(root);
    Ok(())
}
fn incidence(
    count: &mut SourceFunctionIncidence,
    root: &formualizer_eval::operation_diagnostics::OperationRoot,
) {
    if root.cell.is_some() {
        count.cell_reason_count += 1;
    } else if root.named_vertex.is_some() {
        count.named_reason_count += 1;
    } else {
        count.unlocated_reason_count += 1;
    }
}
fn source_operation(operation: &UnavailableOperation) -> SourceUnavailableOperation {
    match operation {
        UnavailableOperation::FunctionPanicked { name } => {
            SourceUnavailableOperation::FunctionPanicked { name: name.clone() }
        }
        UnavailableOperation::UnknownFunction { name } => {
            SourceUnavailableOperation::UnknownFunction { name: name.clone() }
        }
        UnavailableOperation::UnimplementedFunction { name } => {
            SourceUnavailableOperation::UnimplementedFunction { name: name.clone() }
        }
        UnavailableOperation::UnimplementedFunctionMode { name, mode } => {
            SourceUnavailableOperation::UnimplementedFunctionMode {
                name: name.clone(),
                mode: (*mode).into(),
            }
        }
        UnavailableOperation::UnverifiedFunctionMode { name, mode } => {
            SourceUnavailableOperation::UnverifiedFunctionMode {
                name: name.clone(),
                mode: (*mode).into(),
            }
        }
        UnavailableOperation::UnverifiedProvider { name } => {
            SourceUnavailableOperation::UnverifiedProvider { name: name.clone() }
        }
        UnavailableOperation::PostfixInvocation => SourceUnavailableOperation::PostfixInvocation,
    }
}

pub(super) fn collect(
    engine: &mut Engine<WBResolver>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    options: &XlsxRecalculateOptions,
    cycle_errors: usize,
    precision_gap: Option<&'static str>,
) -> Result<Coverage, IoError> {
    let mut out = Coverage::default();
    // Fallible and one-shot: capacity/collector failure must reach the caller
    // before publication, never become an empty Complete diagnostic report.
    let events = engine
        .take_operation_diagnostics()?
        .ok_or_else(|| IoError::Backend {
            backend: "source-execution-evidence".into(),
            message: "calculation evidence is unavailable".into(),
        })?;
    out.diagnostics.logical_cell_reason_count = events.logical_cell_reason_count;
    out.diagnostics.logical_root_cell_count = events.logical_root_cell_count;
    out.diagnostics.logical_named_reason_count = events.logical_named_reason_count;
    out.diagnostics.physical_unavailable_observations = events.physical_unavailable_observations;
    out.diagnostics.physical_function_dispatches = events.physical_function_dispatches;
    let mut function_counts: BTreeMap<&str, SourceFunctionIncidence> = BTreeMap::new();
    let mut mode_counts: BTreeMap<(&str, &str, SourceModeVerification), SourceFunctionIncidence> =
        BTreeMap::new();
    let mut roots = Vec::new();
    let mut unlocated = false;
    for root in &events.roots {
        checkpoint(&options.cancel)?;
        let operation = root.operation.as_ref();
        let name = match operation {
            UnavailableOperation::FunctionPanicked { name }
            | UnavailableOperation::UnknownFunction { name }
            | UnavailableOperation::UnimplementedFunction { name }
            | UnavailableOperation::UnimplementedFunctionMode { name, .. }
            | UnavailableOperation::UnverifiedFunctionMode { name, .. }
            | UnavailableOperation::UnverifiedProvider { name } => Some(name.as_str()),
            UnavailableOperation::PostfixInvocation => None,
        };
        if let Some(name) = name {
            incidence(function_counts.entry(name).or_default(), root);
        }
        match operation {
            UnavailableOperation::UnknownFunction { name } if root.cell.is_some() => {
                *out.unknown_functions.entry(name.clone()).or_default() += 1;
            }
            UnavailableOperation::UnimplementedFunctionMode { name, mode } => {
                incidence(
                    mode_counts
                        .entry((name.as_str(), *mode, SourceModeVerification::Unavailable))
                        .or_default(),
                    root,
                );
            }
            UnavailableOperation::UnverifiedFunctionMode { name, mode } => {
                incidence(
                    mode_counts
                        .entry((name.as_str(), *mode, SourceModeVerification::Unverified))
                        .or_default(),
                    root,
                );
            }
            _ => {}
        }
        let location = if let Some(cell) = root.cell {
            push_root(&mut roots, AffectedRoot::Cell(cell), options)?;
            let name = engine.sheet_name(cell.sheet_id);
            Some(format!(
                "{name}!{}{}",
                formualizer_common::col_letters_from_1based(cell.coord.col() + 1)
                    .unwrap_or_else(|_| (cell.coord.col() + 1).to_string()),
                cell.coord.row() + 1
            ))
        } else {
            None
        };
        if let Some(vertex) = root.named_vertex {
            push_root(&mut roots, AffectedRoot::Symbol(vertex), options)?;
        }
        if root.cell.is_none() && root.named_vertex.is_none() {
            unlocated = true;
        }
        if out.diagnostics.samples.len() < options.error_location_limit {
            out.diagnostics.samples.push(SourceOperationSample {
                operation: source_operation(operation),
                location,
            });
        }
    }
    out.diagnostics.unavailable_functions = function_counts
        .into_iter()
        .map(|(name, count)| (name.to_owned(), count))
        .collect();
    out.diagnostics.function_modes = mode_counts
        .into_iter()
        .map(
            |((name, mode, verification), incidence)| SourceFunctionModeIncidence {
                name: name.to_owned(),
                mode: mode.to_owned(),
                verification,
                incidence,
            },
        )
        .collect();
    for (sheet, plan) in sheets.iter().zip(plans) {
        let id = engine
            .sheet_id(&sheet.name)
            .ok_or_else(|| unsupported("missing admitted engine sheet", &sheet.name))?;
        for cell in &plan.cells {
            if let Some(rect) = cell.data_table_ref() {
                push_root(
                    &mut roots,
                    AffectedRoot::Region(RegionPiece {
                        sheet: id,
                        rect: formualizer_eval::engine::authority::geom::Rect::new(
                            rect.first_row - 1,
                            rect.first_col - 1,
                            rect.last_row - 1,
                            rect.last_col - 1,
                        ),
                    }),
                    options,
                )?;
                out.regions.push((sheet.name.clone(), rect));
                if out.diagnostics.samples.len() < options.error_location_limit {
                    out.diagnostics.samples.push(SourceOperationSample {
                        operation: SourceUnavailableOperation::SourceFeature {
                            name: "data_table".into(),
                        },
                        location: Some(format!("{}!{}", sheet.name, cell.address())),
                    });
                }
            }
        }
    }
    out.diagnostics.source_regions = out.regions.len();
    let mut internal = 0;
    if cycle_errors > 0 && options.profile == crate::XlsxRecalculateProfile::Workbook {
        for (sheet, plan) in sheets.iter().zip(plans) {
            let id = engine
                .sheet_id(&sheet.name)
                .ok_or_else(|| unsupported("missing admitted engine sheet", &sheet.name))?;
            for cell in &plan.cells {
                checkpoint(&options.cancel)?;
                let address = CellAddress::new(&sheet.name, cell.row, cell.col)
                    .map_err(|e| IoError::from_backend("coverage-coordinate", e))?;
                let state = engine
                    .inspect_cell_result(&address)
                    .map_err(|e| IoError::from_backend("coverage-inspect", e))?;
                if matches!(state.value,Some(LiteralValue::Error(ref e)) if e.kind==formualizer_common::ExcelErrorKind::Circ)
                {
                    push_root(
                        &mut roots,
                        AffectedRoot::Region(RegionPiece {
                            sheet: id,
                            rect: formualizer_eval::engine::authority::geom::Rect::cell(
                                cell.row - 1,
                                cell.col - 1,
                            ),
                        }),
                        options,
                    )?;
                    internal += 1;
                    if out.diagnostics.samples.len() < options.error_location_limit {
                        out.diagnostics.samples.push(SourceOperationSample {
                            operation: SourceUnavailableOperation::InternalResult {
                                token: "#CIRC!".into(),
                            },
                            location: Some(format!("{}!{}", sheet.name, cell.address())),
                        });
                    }
                }
            }
        }
    }
    out.diagnostics.unavailable_result_cells = internal;
    if let Some(gap) = precision_gap {
        out.diagnostics.workbook_features.push(gap.into());
        if out.diagnostics.samples.len() < options.error_location_limit {
            out.diagnostics.samples.push(SourceOperationSample {
                operation: SourceUnavailableOperation::SourceFeature { name: gap.into() },
                location: None,
            });
        }
    }
    let total =
        events.roots.len() + internal + out.regions.len() + usize::from(precision_gap.is_some());
    out.has_gaps = total > 0;
    out.diagnostics.samples_truncated = total.saturating_sub(out.diagnostics.samples.len());
    if out.has_gaps && options.require_complete {
        return Err(defer(
            CalculationDeferredReason::KnownCoverageIncomplete,
            "requireComplete prevents known or unverified partial output",
        ));
    }
    if out.has_gaps && options.profile == crate::XlsxRecalculateProfile::Workbook {
        if unlocated {
            return Err(defer(
                CalculationDeferredReason::UnlocatedOperation,
                "cannot isolate source caches",
            ));
        }
        let facts = engine
            .current_read_affected_closure(
                &roots,
                ClosureLimits {
                    max_work: options.limits.max_affected_work,
                    max_output_items: options.limits.max_affected_regions,
                    max_scratch_bytes: options.limits.max_affected_scratch_bytes,
                },
            )
            .map_err(closure_error)?;
        if facts.unlocalized_spill_geometry {
            // Ambiguous current ownership cannot become a recoverable selective
            // publication or an apparently safe negative cache decision.
            return Err(IoError::Backend {
                backend: "source-dependency-geometry".into(),
                message: "current committed spill geometry is unavailable or inconsistent".into(),
            });
        }
        let mut unknown_reasons = BTreeMap::new();
        let mut conservative_unknown_dynamic = false;
        for fact in &facts.unknowns {
            use formualizer_eval::engine::affected_closure::UnknownReason;
            let reason = match fact.reason {
                UnknownReason::MissingObservation => {
                    conservative_unknown_dynamic = true;
                    "missing_observation"
                }
                UnknownReason::UncommittedObservation => {
                    conservative_unknown_dynamic = true;
                    "uncommitted_observation"
                }
                UnknownReason::DirtyReader => {
                    conservative_unknown_dynamic = true;
                    "dirty_reader"
                }
                UnknownReason::OpaqueStaticRelation => "opaque_static_relation",
                UnknownReason::RuntimeSymbolsUnrepresented => {
                    conservative_unknown_dynamic = true;
                    "runtime_symbols_unrepresented"
                }
                UnknownReason::UnavailableSpillGeometry => "unavailable_spill_geometry",
            };
            *unknown_reasons.entry(reason.to_owned()).or_default() += 1;
        }
        // Publication probes each projected source mark against these pieces.
        // Pre-admit their conservative comparison bound before making patches;
        // it shares the configured work allowance with the engine query.
        let marks = plans
            .iter()
            .try_fold(0u64, |count, plan| {
                count
                    .checked_add(plan.cells.len() as u64)
                    .ok_or(ClosureError::ArithmeticOverflow)
            })
            .map_err(closure_error)?;
        let pieces = facts
            .witnessed
            .len()
            .checked_add(facts.uncertain_downstream.len())
            .ok_or_else(|| closure_error(ClosureError::ArithmeticOverflow))?;
        let membership_work = marks
            .checked_mul(pieces as u64)
            .ok_or_else(|| closure_error(ClosureError::ArithmeticOverflow))?;
        let admitted_work = facts
            .usage
            .work
            .checked_add(membership_work)
            .ok_or_else(|| closure_error(ClosureError::ArithmeticOverflow))?;
        if admitted_work > options.limits.max_affected_work {
            return Err(resource(
                formualizer_common::ResourceExhaustionReason::WorkUnits,
                options.limits.max_affected_work,
                admitted_work,
                "source publication dependency membership work bound",
            ));
        }
        out.diagnostics.dependency_evidence = Some(SourceDependencyEvidence {
            relation: "static_authority_and_committed_grid_rectangles".into(),
            source_domain: "admitted_workbook_and_fixed_cached_sources".into(),
            semantic_reads_unattested: facts.semantic_reads_unattested,
            conservative_unknown_dynamic,
            unknown_reasons,
            unknown_facts: facts.unknowns.len(),
            witnessed_regions: facts.witnessed.len(),
            uncertain_regions: facts.uncertain_downstream.len(),
            work: admitted_work,
            query_work: facts.usage.work,
            publication_membership_work_bound: membership_work,
            requested_capacity_peak_bytes: facts.usage.requested_capacity_peak_bytes,
        });
        // These facts are consumed for this source snapshot only. Moving the
        // ingested engine into publication performs no further recalculation.
        out.witnessed = facts.witnessed;
        out.uncertain = facts.uncertain_downstream;
        checkpoint(&options.cancel)?;
    }
    Ok(out)
}

#[cfg(test)]
mod evidence_boundary_tests {
    use super::*;
    #[test]
    fn source_policy_boundary_named_mode_adapter_preserves_symbol_origin() {
        // Generic engine named-formula support exceeds the public XLSX name
        // admission grammar. This exercises the typed adapter, not admission.
        use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
        let options = XlsxRecalculateOptions::workbook();
        let mut engine = Engine::new(WBResolver::default(), options.eval_config.clone());
        engine.configure_operation_diagnostics(
            formualizer_eval::operation_diagnostics::OperationDiagnosticOptions {
                sample_limit: 16,
                track_unverified_providers: true,
                ..Default::default()
            },
        );
        engine
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(7.0))
            .unwrap();
        engine
            .define_name(
                "GapName",
                NamedDefinition::Formula {
                    ast: formualizer_parse::parser::parse("=CELL(\"format\",Sheet1!$B$1)").unwrap(),
                    dependencies: vec![],
                    range_deps: vec![],
                },
                NameScope::Workbook,
            )
            .unwrap();
        engine
            .set_cell_formula(
                "Sheet1",
                1,
                1,
                formualizer_parse::parser::parse("=GapName+1").unwrap(),
            )
            .unwrap();
        engine.evaluate_all_for_snapshot(None).unwrap();
        let coverage = collect(&mut engine, &[], &[], &options, 0, None).unwrap();
        assert!(coverage.has_gaps);
        assert!(coverage.diagnostics.logical_named_reason_count > 0);
        assert!(
            coverage
                .diagnostics
                .function_modes
                .iter()
                .any(|mode| mode.name == "CELL" && mode.incidence.named_reason_count > 0)
        );
        assert!(coverage.affects(engine.sheet_id("Sheet1").unwrap(), 1, 1));
    }

    #[test]
    fn source_policy_boundary_missing_collector_cannot_be_complete() {
        // Private invariant probe; source ingestion normally installs a collector.
        let mut engine = Engine::new(WBResolver::default(), Default::default());
        let result = collect(&mut engine, &[], &[], &Default::default(), 0, None);
        assert!(
            matches!(result, Err(IoError::Backend { .. })),
            "missing evidence must not be empty Complete"
        );
    }
}
