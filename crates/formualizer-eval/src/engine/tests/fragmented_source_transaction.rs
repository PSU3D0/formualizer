use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::fragmented_transaction::{
    FragmentedCommitDecision, FragmentedCommitFault, FragmentedReplayReason,
    FragmentedTransactionPrepareError, PreparedFragmentedSourceTransaction,
};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    DeferredFormulaPackage, DeferredFormulaReplay, DeferredReplayFormula, Engine, EvalConfig,
    ExplicitPartitionLegacyMembers, FormulaCompressedSourceReport, FormulaIngestBatch,
    FormulaParsePolicy, FormulaReplayDisposition, PartitionLegacyMember, PartitionLegacyMemberKind,
    PartitionReconciliation, PartitionedSourceFormulaFamily, PlacementDomainTransport, SourceCoord,
    SourceFamilyId, SourceFamilyMembers, SourceFormulaFamily, SourceRect,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[derive(Clone, Copy)]
enum Shape {
    Row,
    Column,
    Rect,
}

fn family(shape: Shape, source_index: usize) -> PartitionedSourceFormulaFamily {
    let (template_origin0, declared, fragments, legacy, reconciliation, surviving_member_count) =
        match shape {
            Shape::Row => (
                SourceCoord { row: 0, col: 2 },
                SourceRect {
                    start: SourceCoord { row: 0, col: 2 },
                    end: SourceCoord { row: 6, col: 2 },
                },
                vec![
                    PlacementDomainTransport::RowRun {
                        row_start: 0,
                        row_end: 1,
                        col: 2,
                    },
                    PlacementDomainTransport::RowRun {
                        row_start: 4,
                        row_end: 5,
                        col: 2,
                    },
                ],
                vec![
                    PartitionLegacyMember {
                        coord: SourceCoord { row: 2, col: 2 },
                        kind: PartitionLegacyMemberKind::SharedFamilyMember,
                    },
                    PartitionLegacyMember {
                        coord: SourceCoord { row: 3, col: 2 },
                        kind: PartitionLegacyMemberKind::OrdinaryException,
                    },
                ],
                PartitionReconciliation {
                    shared_members: 5,
                    ordinary_exceptions: 1,
                    holes: 1,
                },
                5,
            ),
            Shape::Column => (
                SourceCoord { row: 2, col: 2 },
                SourceRect {
                    start: SourceCoord { row: 2, col: 2 },
                    end: SourceCoord { row: 2, col: 8 },
                },
                vec![
                    PlacementDomainTransport::ColRun {
                        row: 2,
                        col_start: 2,
                        col_end: 3,
                    },
                    PlacementDomainTransport::ColRun {
                        row: 2,
                        col_start: 6,
                        col_end: 7,
                    },
                ],
                vec![
                    PartitionLegacyMember {
                        coord: SourceCoord { row: 2, col: 4 },
                        kind: PartitionLegacyMemberKind::SharedFamilyMember,
                    },
                    PartitionLegacyMember {
                        coord: SourceCoord { row: 2, col: 5 },
                        kind: PartitionLegacyMemberKind::OrdinaryException,
                    },
                ],
                PartitionReconciliation {
                    shared_members: 5,
                    ordinary_exceptions: 1,
                    holes: 1,
                },
                5,
            ),
            Shape::Rect => (
                SourceCoord { row: 2, col: 2 },
                SourceRect {
                    start: SourceCoord { row: 2, col: 2 },
                    end: SourceCoord { row: 4, col: 5 },
                },
                vec![
                    PlacementDomainTransport::Rect(SourceRect {
                        start: SourceCoord { row: 2, col: 2 },
                        end: SourceCoord { row: 2, col: 5 },
                    }),
                    PlacementDomainTransport::Rect(SourceRect {
                        start: SourceCoord { row: 4, col: 2 },
                        end: SourceCoord { row: 4, col: 5 },
                    }),
                ],
                vec![
                    PartitionLegacyMember {
                        coord: SourceCoord { row: 3, col: 2 },
                        kind: PartitionLegacyMemberKind::SharedFamilyMember,
                    },
                    PartitionLegacyMember {
                        coord: SourceCoord { row: 3, col: 3 },
                        kind: PartitionLegacyMemberKind::OrdinaryException,
                    },
                ],
                PartitionReconciliation {
                    shared_members: 9,
                    ordinary_exceptions: 1,
                    holes: 2,
                },
                9,
            ),
        };
    PartitionedSourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(source_index as u64),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index,
        },
        template_origin0,
        template_text: Arc::from("$A$1+1"),
        declared,
        surviving_member_count,
        fragments,
        legacy_members: ExplicitPartitionLegacyMembers::try_new(legacy).unwrap(),
        reconciliation,
    }
}

fn relative_replay_family(source_index: usize) -> PartitionedSourceFormulaFamily {
    PartitionedSourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(source_index as u64),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index,
        },
        template_origin0: SourceCoord { row: 0, col: 2 },
        template_text: Arc::from("A1+1"),
        declared: SourceRect {
            start: SourceCoord { row: 0, col: 2 },
            end: SourceCoord { row: 104, col: 2 },
        },
        surviving_member_count: 103,
        fragments: vec![
            PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 1,
                col: 2,
            },
            PlacementDomainTransport::RowRun {
                row_start: 5,
                row_end: 104,
                col: 2,
            },
        ],
        legacy_members: ExplicitPartitionLegacyMembers::try_new(vec![
            PartitionLegacyMember {
                coord: SourceCoord { row: 2, col: 2 },
                kind: PartitionLegacyMemberKind::SharedFamilyMember,
            },
            PartitionLegacyMember {
                coord: SourceCoord { row: 3, col: 2 },
                kind: PartitionLegacyMemberKind::OrdinaryException,
            },
        ])
        .unwrap(),
        reconciliation: PartitionReconciliation {
            shared_members: 103,
            ordinary_exceptions: 1,
            holes: 1,
        },
    }
}

fn cross_fragment_family(source_index: usize, cycle: bool) -> PartitionedSourceFormulaFamily {
    PartitionedSourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(source_index as u64),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index,
        },
        template_origin0: SourceCoord { row: 100, col: 2 },
        template_text: Arc::from(if cycle { "C201+C1" } else { "C201+1" }),
        declared: SourceRect {
            start: SourceCoord { row: 100, col: 2 },
            end: SourceCoord { row: 299, col: 2 },
        },
        surviving_member_count: 200,
        fragments: vec![
            PlacementDomainTransport::RowRun {
                row_start: 100,
                row_end: 199,
                col: 2,
            },
            PlacementDomainTransport::RowRun {
                row_start: 200,
                row_end: 299,
                col: 2,
            },
        ],
        legacy_members: ExplicitPartitionLegacyMembers::try_new(Vec::new()).unwrap(),
        reconciliation: PartitionReconciliation {
            shared_members: 200,
            ordinary_exceptions: 0,
            holes: 0,
        },
    }
}

fn function_family(source_index: usize) -> PartitionedSourceFormulaFamily {
    let mut source = family(Shape::Row, source_index);
    source.template_text = Arc::from("ABS($A$1)+1");
    source
}

fn formulas_for_with_ordinary(
    engine: &mut Engine<TestWorkbook>,
    source: &PartitionedSourceFormulaFamily,
    ordinary_text: &str,
) -> Vec<crate::engine::fragmented_transaction::PreparedFragmentedLegacyFormula> {
    source
        .legacy_members
        .as_slice()
        .iter()
        .map(|member| {
            let text = match member.kind {
                PartitionLegacyMemberKind::SharedFamilyMember
                    if source.template_text.as_ref() == "A1+1" =>
                {
                    format!("=A{}+1", member.coord.row + 1)
                }
                PartitionLegacyMemberKind::SharedFamilyMember => {
                    format!("={}", source.template_text)
                }
                PartitionLegacyMemberKind::OrdinaryException => ordinary_text.to_string(),
            };
            let replay = crate::engine::DeferredReplayFormula {
                source_order: crate::engine::SourceFormulaOrder::new(0),
                row: member.coord.row + 1,
                col: member.coord.col + 1,
                text,
                family: (member.kind == PartitionLegacyMemberKind::SharedFamilyMember)
                    .then_some(source.source_id),
                partition_owner: Some(source.source_id),
            };
            engine
                .analyze_fragmented_exact_replay_record_for_test(
                    "Sheet1",
                    source.source_id,
                    *member,
                    replay,
                )
                .unwrap()
        })
        .collect()
}

fn formulas_for(
    engine: &mut Engine<TestWorkbook>,
    source: &PartitionedSourceFormulaFamily,
) -> Vec<crate::engine::fragmented_transaction::PreparedFragmentedLegacyFormula> {
    formulas_for_with_ordinary(engine, source, "=$A$1+5")
}

fn ordinary_only_family(source_index: usize) -> PartitionedSourceFormulaFamily {
    let mut source = family(Shape::Row, source_index);
    source.legacy_members = ExplicitPartitionLegacyMembers::try_new(vec![PartitionLegacyMember {
        coord: SourceCoord { row: 3, col: 2 },
        kind: PartitionLegacyMemberKind::OrdinaryException,
    }])
    .unwrap();
    source.surviving_member_count = 4;
    source.reconciliation = PartitionReconciliation {
        shared_members: 4,
        ordinary_exceptions: 1,
        holes: 2,
    };
    source
}

fn make_engine_and_revision() -> (Engine<TestWorkbook>, Arc<AtomicU64>) {
    crate::builtins::load_builtins();
    let workbook = TestWorkbook::default();
    let revision = workbook.planning_revision_handle();
    let mut engine = Engine::new(
        workbook,
        EvalConfig::default()
            .with_formula_plane_mode(crate::engine::FormulaPlaneMode::AuthoritativeExperimental),
    );
    for row in 1..=10 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(10.0))
            .unwrap();
    }
    (engine, revision)
}

fn make_engine() -> Engine<TestWorkbook> {
    make_engine_and_revision().0
}

fn transaction_for_source(
    source: PartitionedSourceFormulaFamily,
) -> (
    Engine<TestWorkbook>,
    PartitionedSourceFormulaFamily,
    FormulaReplayDisposition,
    PreparedFragmentedSourceTransaction,
    Arc<AtomicU64>,
) {
    let (mut engine, revision) = make_engine_and_revision();
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for(&mut engine, &source);
    let mut disposition = FormulaReplayDisposition::default();
    disposition.register_partition(&source, true).unwrap();
    let prepared = engine
        .prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy)
        .unwrap();
    (engine, source, disposition, prepared, revision)
}

fn transaction(
    shape: Shape,
    source_index: usize,
) -> (
    Engine<TestWorkbook>,
    PartitionedSourceFormulaFamily,
    FormulaReplayDisposition,
    PreparedFragmentedSourceTransaction,
    Arc<AtomicU64>,
) {
    transaction_for_source(family(shape, source_index))
}

struct EmptyDeferredReplay;

impl DeferredFormulaReplay for EmptyDeferredReplay {
    fn replay(
        &mut self,
        _disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        Ok(Vec::new())
    }

    fn formula_at(
        &mut self,
        _row: u32,
        _col: u32,
    ) -> Result<Option<DeferredReplayFormula>, String> {
        Ok(None)
    }
}

struct PartitionSetReplay {
    source: PartitionedSourceFormulaFamily,
    initial: Vec<DeferredReplayFormula>,
    full: Vec<DeferredReplayFormula>,
    fail_full: bool,
    require_legacy_partition_routing: bool,
}

impl DeferredFormulaReplay for PartitionSetReplay {
    fn replay(
        &mut self,
        _disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        if self.fail_full {
            Err("injected whole-family replay failure".to_string())
        } else {
            Ok(self.full.clone())
        }
    }

    fn replay_partitioned(
        &mut self,
        disposition: &FormulaReplayDisposition,
        partitions: &[PartitionedSourceFormulaFamily],
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        if self.require_legacy_partition_routing {
            if !partitions
                .iter()
                .any(|partition| partition.source_id == self.source.source_id)
            {
                return Err("invalidated partition missing from exact replay routing".to_string());
            }
            let ordinary = self
                .source
                .legacy_members
                .as_slice()
                .iter()
                .find(|member| member.kind == PartitionLegacyMemberKind::OrdinaryException)
                .ok_or_else(|| "test partition has no ordinary exception".to_string())?;
            if disposition.ordinary_disposition(ordinary.coord).1 != Some(self.source.source_id) {
                return Err("invalidated ordinary exception lost partition ownership".to_string());
            }
        }
        if disposition.partition_disposition(&self.source, self.source.template_origin0)
            == Some(crate::engine::FormulaReplayCoordinateDisposition::Direct)
        {
            Ok(self.initial.clone())
        } else if self.fail_full {
            Err("injected whole-family replay failure".to_string())
        } else {
            Ok(self
                .full
                .iter()
                .filter(|record| {
                    let coord = SourceCoord {
                        row: record.row - 1,
                        col: record.col - 1,
                    };
                    match record.family {
                        Some(family) => {
                            disposition.shared_disposition(family, coord)
                                == crate::engine::FormulaReplayCoordinateDisposition::LegacyShared
                        }
                        None => {
                            disposition.ordinary_disposition(coord).0
                                == crate::engine::FormulaReplayCoordinateDisposition::LegacyOrdinary
                        }
                    }
                })
                .cloned()
                .collect())
        }
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        Ok(self
            .full
            .iter()
            .find(|record| record.row == row && record.col == col)
            .cloned())
    }
}

fn replay_record(
    source: SourceFamilyId,
    coord: SourceCoord,
    kind: PartitionLegacyMemberKind,
) -> DeferredReplayFormula {
    DeferredReplayFormula {
        source_order: crate::engine::SourceFormulaOrder::new(0),
        row: coord.row + 1,
        col: coord.col + 1,
        text: "$A$1+1".to_string(),
        family: (kind == PartitionLegacyMemberKind::SharedFamilyMember).then_some(source),
        partition_owner: Some(source),
    }
}

fn deferred_partition_package(
    source: PartitionedSourceFormulaFamily,
    fail_full: bool,
    require_legacy_partition_routing: bool,
) -> DeferredFormulaPackage {
    let make_record = |coord: SourceCoord, kind: PartitionLegacyMemberKind| DeferredReplayFormula {
        source_order: crate::engine::SourceFormulaOrder::new(u64::from(coord.row)),
        row: coord.row + 1,
        col: coord.col + 1,
        text: match kind {
            PartitionLegacyMemberKind::SharedFamilyMember => source.template_text.to_string(),
            PartitionLegacyMemberKind::OrdinaryException => "$A$1+5".to_string(),
        },
        family: (kind == PartitionLegacyMemberKind::SharedFamilyMember).then_some(source.source_id),
        partition_owner: Some(source.source_id),
    };
    let initial = source
        .legacy_members
        .as_slice()
        .iter()
        .map(|member| make_record(member.coord, member.kind))
        .collect();
    let full = (0..=5)
        .map(|row| {
            make_record(
                SourceCoord { row, col: 2 },
                if row == 3 {
                    PartitionLegacyMemberKind::OrdinaryException
                } else {
                    PartitionLegacyMemberKind::SharedFamilyMember
                },
            )
        })
        .collect();
    let report = FormulaCompressedSourceReport {
        source_formula_records_spooled: 6,
        families_seen: 1,
        family_cells_seen: source.surviving_member_count,
        source_fragmentable_families: 1,
        source_fragmentable_cells: source.surviving_member_count,
        source_fragment_count: source.fragments.len() as u64,
        source_hole_exclusions: source.reconciliation.holes,
        source_ordinary_exclusions: source.reconciliation.ordinary_exceptions,
        ..FormulaCompressedSourceReport::default()
    };
    DeferredFormulaPackage::new(
        "Sheet1".to_string(),
        report,
        Vec::new(),
        vec![source.clone()],
        Box::new(PartitionSetReplay {
            source,
            initial,
            full,
            fail_full,
            require_legacy_partition_routing,
        }),
    )
}

fn deferred_engine() -> Engine<TestWorkbook> {
    let mut engine = make_engine();
    engine.config.defer_graph_building = true;
    engine
}

fn deferred_cross_fragment_package(
    source: PartitionedSourceFormulaFamily,
) -> DeferredFormulaPackage {
    let full = (100..=299)
        .map(|row| DeferredReplayFormula {
            source_order: crate::engine::SourceFormulaOrder::new(u64::from(row - 100)),
            row: row + 1,
            col: 3,
            text: format!("C{}+1", row + 101),
            family: Some(source.source_id),
            partition_owner: Some(source.source_id),
        })
        .collect();
    let report = FormulaCompressedSourceReport {
        source_formula_records_spooled: 200,
        families_seen: 1,
        family_cells_seen: 200,
        source_fragmentable_families: 1,
        source_fragmentable_cells: 200,
        source_fragment_count: 2,
        ..FormulaCompressedSourceReport::default()
    };
    DeferredFormulaPackage::new(
        "Sheet1".to_string(),
        report,
        Vec::new(),
        vec![source.clone()],
        Box::new(PartitionSetReplay {
            source,
            initial: Vec::new(),
            full,
            fail_full: false,
            require_legacy_partition_routing: false,
        }),
    )
}

fn state(engine: &Engine<TestWorkbook>) -> String {
    format!(
        "{:?}|{:?}|{:?}|{:?}|{:?}",
        engine.graph,
        engine.last_formula_ingest_report(),
        engine.formula_ingest_report_total(),
        engine.last_virtual_dep_telemetry(),
        engine.last_cycle_telemetry(),
    )
}

#[test]
fn deferred_partition_commits_through_composed_transaction() {
    let mut engine = deferred_engine();
    let source = family(Shape::Row, 110);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_partition_package(source, false, false));

    engine.build_graph_all().unwrap();
    let report = engine.last_formula_ingest_report().unwrap();
    assert!(!engine.has_staged_formulas());
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);
    assert_eq!(report.source_family_promoted, 1, "{report:?}");
    assert_eq!(report.source_partitioned_families_prepared, 1, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 2, "{report:?}");
    assert_eq!(report.source_spool_replays, 1, "{report:?}");
}

#[test]
fn deferred_invalidated_partition_keeps_ordinary_exception_ownership() {
    let mut engine = deferred_engine();
    let source = family(Shape::Row, 115);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_partition_package(source, false, true));

    // Replacing one shared member invalidates the whole partition. Exact replay
    // must still receive the partition as routing evidence so its ordinary
    // exception remains owned by the same family during atomic fallback.
    engine.stage_formula_text("Sheet1", 1, 3, "=$A$1+99".to_string());
    engine.build_graph_all().unwrap();

    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 6);
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback, 1, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 6, "{report:?}");
    assert_eq!(report.source_spool_replays, 1, "{report:?}");
}

#[test]
fn deferred_partition_precommit_fault_replays_whole_family_once() {
    let mut engine = deferred_engine();
    let source = family(Shape::Row, 111);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_partition_package(source, false, false));
    engine.set_fragmented_commit_fault_for_test(FragmentedCommitFault::FormulaPlaneFinalCheck);

    engine.build_graph_all().unwrap();
    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 6);
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback, 1, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 6, "{report:?}");
    assert_eq!(report.source_spool_replays, 2, "{report:?}");
    assert_eq!(
        report
            .fallback_reasons
            .get("Injected(FormulaPlaneFinalCheck)"),
        Some(&1),
        "{report:?}"
    );
}

#[test]
fn deferred_partition_provider_change_falls_back_before_mutation() {
    let (mut engine, revision) = make_engine_and_revision();
    engine.config.defer_graph_building = true;
    let source = function_family(113);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_partition_package(source, false, false));
    engine.set_before_prepared_span_commit_hook(move || {
        revision.fetch_add(1, Ordering::AcqRel);
    });

    engine.build_graph_all().unwrap();
    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 6);
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback, 1, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 6, "{report:?}");
    assert_eq!(report.source_spool_replays, 2, "{report:?}");
    assert!(
        report
            .fallback_reasons
            .contains_key("FunctionProviderRevisionChanged"),
        "{report:?}"
    );
}

#[test]
fn deferred_cross_fragment_dependency_replays_without_partial_authority() {
    let mut engine = deferred_engine();
    let source = cross_fragment_family(114, false);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_cross_fragment_package(source));

    engine.build_graph_all().unwrap();
    let report = engine.last_formula_ingest_report().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 200);
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback, 1, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 200, "{report:?}");
    assert_eq!(report.source_spool_replays, 2, "{report:?}");
    assert!(
        report
            .fallback_reasons
            .keys()
            .any(|reason| reason.contains("CrossFragmentDependency")),
        "{report:?}"
    );
}

#[test]
fn deferred_partition_late_replay_failure_leaves_current_family_unmodified() {
    let mut engine = deferred_engine();
    let source = family(Shape::Row, 112);
    engine
        .source_formula_ingress()
        .stage_deferred(deferred_partition_package(source, true, false));
    engine.set_fragmented_commit_fault_for_test(FragmentedCommitFault::FormulaPlaneFinalCheck);

    let error = engine.build_graph_all().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected whole-family replay failure"),
        "{error}"
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert!(engine.last_formula_ingest_report().is_none());
    assert_eq!(
        engine
            .formula_ingest_report_total()
            .graph_formula_cells_materialized,
        0
    );
    assert!(!engine.has_staged_formulas());
}

#[test]
fn eager_partition_initial_replay_rejects_missing_and_extra_legacy_ownership() {
    for extra in [false, true] {
        let mut engine = make_engine();
        let source = family(Shape::Row, 90 + usize::from(extra));
        let expected: Vec<_> = source
            .legacy_members
            .as_slice()
            .iter()
            .map(|member| replay_record(source.source_id, member.coord, member.kind))
            .collect();
        let mut initial = if extra {
            expected.clone()
        } else {
            expected.iter().take(1).cloned().collect()
        };
        if extra {
            initial.push(replay_record(
                source.source_id,
                SourceCoord { row: 6, col: 2 },
                PartitionLegacyMemberKind::SharedFamilyMember,
            ));
        }
        let full = (0..=5)
            .map(|row| {
                replay_record(
                    source.source_id,
                    SourceCoord { row, col: 2 },
                    if row == 3 {
                        PartitionLegacyMemberKind::OrdinaryException
                    } else {
                        PartitionLegacyMemberKind::SharedFamilyMember
                    },
                )
            })
            .collect();
        let preparation = engine
            .source_formula_ingress()
            .prepare_eager_proposals(
                "Sheet1",
                &[],
                std::slice::from_ref(&source),
                6,
                Box::new(PartitionSetReplay {
                    source: source.clone(),
                    initial,
                    full,
                    fail_full: false,
                    require_legacy_partition_routing: false,
                }),
            )
            .unwrap();
        assert!(preparation.fragmented.is_empty());
        assert_eq!(
            preparation
                .rejected
                .get(&source.source_id)
                .map(String::as_str),
            Some("ExactReplayLegacySetMismatch")
        );
        assert_eq!(preparation.eager_replay.len(), 6);
        assert_eq!(preparation.preparation_spool_replays, 2);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
        assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    }
}

#[test]
fn clean_families_follow_source_order_not_shared_index_order() {
    struct EmptyReplay;
    impl DeferredFormulaReplay for EmptyReplay {
        fn replay(
            &mut self,
            _disposition: &FormulaReplayDisposition,
        ) -> Result<Vec<DeferredReplayFormula>, String> {
            Ok(Vec::new())
        }

        fn formula_at(
            &mut self,
            _row: u32,
            _col: u32,
        ) -> Result<Option<DeferredReplayFormula>, String> {
            Ok(None)
        }
    }

    let mut engine = make_engine();
    let later_low_si = SourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(10),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 0, col: 6 },
        anchor_text: Arc::from("$A$1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 1,
            col: 6,
        }),
        member_count: 2,
    };
    let earlier_high_si = SourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(0),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index: 99,
        },
        anchor_coord0: SourceCoord { row: 0, col: 5 },
        anchor_text: Arc::from("$A$1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 1,
            col: 5,
        }),
        member_count: 2,
    };
    let preparation = engine
        .source_formula_ingress()
        .prepare_eager_proposals(
            "Sheet1",
            &[later_low_si, earlier_high_si],
            &[],
            4,
            Box::new(EmptyReplay),
        )
        .unwrap();
    engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            FormulaCompressedSourceReport {
                families_seen: 2,
                family_cells_seen: 4,
                source_clean_families: 2,
                source_clean_cells: 4,
                ..FormulaCompressedSourceReport::default()
            },
            preparation,
        )])
        .unwrap();

    let spans = engine.graph.formula_authority().active_span_refs();
    assert_eq!(spans.len(), 2);
    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let first = engine
        .graph
        .formula_authority()
        .plane
        .spans
        .get(spans[0])
        .unwrap();
    assert!(
        first
            .domain
            .contains(crate::formula_plane::runtime::PlacementCoord::new(
                sheet_id, 0, 5
            ))
    );
}

#[test]
fn deferred_clean_families_follow_source_order_not_shared_index_order() {
    let later_low_si = SourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(10),
        source_id: SourceFamilyId {
            sheet_instance: 405,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 0, col: 6 },
        anchor_text: Arc::from("$A$1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 1,
            col: 6,
        }),
        member_count: 2,
    };
    let earlier_high_si = SourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(0),
        source_id: SourceFamilyId {
            sheet_instance: 405,
            source_index: 99,
        },
        anchor_coord0: SourceCoord { row: 0, col: 5 },
        anchor_text: Arc::from("$A$1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 1,
            col: 5,
        }),
        member_count: 2,
    };
    let package = DeferredFormulaPackage::new(
        "Sheet1".to_string(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: 4,
            families_seen: 2,
            family_cells_seen: 4,
            source_clean_families: 2,
            source_clean_cells: 4,
            ..FormulaCompressedSourceReport::default()
        },
        vec![later_low_si, earlier_high_si],
        Vec::new(),
        Box::new(EmptyDeferredReplay),
    );
    let mut engine = deferred_engine();
    engine.source_formula_ingress().stage_deferred(package);
    engine.build_graph_all().unwrap();

    let spans = engine.graph.formula_authority().active_span_refs();
    assert_eq!(spans.len(), 2);
    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let first = engine
        .graph
        .formula_authority()
        .plane
        .spans
        .get(spans[0])
        .unwrap();
    assert!(
        first
            .domain
            .contains(crate::formula_plane::runtime::PlacementCoord::new(
                sheet_id, 0, 5
            ))
    );
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_spool_replays,
        0
    );
}

#[test]
fn late_fallback_replay_failure_keeps_current_family_untouched_and_publishes_prior_clean_commit() {
    let mut engine = make_engine();
    let source = family(Shape::Row, 99);
    let initial = source
        .legacy_members
        .as_slice()
        .iter()
        .map(|member| replay_record(source.source_id, member.coord, member.kind))
        .collect();
    let full = (0..=5)
        .map(|row| {
            replay_record(
                source.source_id,
                SourceCoord { row, col: 2 },
                if row == 3 {
                    PartitionLegacyMemberKind::OrdinaryException
                } else {
                    PartitionLegacyMemberKind::SharedFamilyMember
                },
            )
        })
        .collect();
    let clean = SourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(0),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index: 1,
        },
        anchor_coord0: SourceCoord { row: 0, col: 5 },
        anchor_text: Arc::from("$A$1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 1,
            col: 5,
        }),
        member_count: 2,
    };
    let preparation = engine
        .source_formula_ingress()
        .prepare_eager_proposals(
            "Sheet1",
            std::slice::from_ref(&clean),
            std::slice::from_ref(&source),
            8,
            Box::new(PartitionSetReplay {
                source: source.clone(),
                initial,
                full,
                fail_full: true,
                require_legacy_partition_routing: false,
            }),
        )
        .unwrap();
    engine.set_fragmented_commit_fault_for_test(FragmentedCommitFault::FormulaPlaneFinalCheck);
    let report = FormulaCompressedSourceReport {
        families_seen: 2,
        family_cells_seen: 7,
        source_clean_families: 1,
        source_clean_cells: 2,
        source_fragmentable_families: 1,
        source_fragmentable_cells: 5,
        source_fragment_count: 2,
        source_hole_exclusions: 1,
        source_ordinary_exclusions: 1,
        replay_families: 2,
        replay_cells: 7,
        ..FormulaCompressedSourceReport::default()
    };
    let error = engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            report,
            preparation,
        )])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected whole-family replay failure"),
        "{error}"
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    let published = engine
        .last_formula_ingest_report()
        .expect("prior clean commit report published");
    assert_eq!(published.source_family_promoted, 1, "{published:?}");
    assert_eq!(published.source_family_promoted_cells, 2, "{published:?}");
    assert_eq!(published.source_partitioned_families_prepared, 0);
}

#[test]
fn late_fallback_parse_failure_keeps_current_family_untouched_and_reports_prior_commit() {
    let mut engine = make_engine();
    engine.config.formula_parse_policy = FormulaParsePolicy::Strict;
    let source = family(Shape::Row, 98);
    let initial = source
        .legacy_members
        .as_slice()
        .iter()
        .map(|member| replay_record(source.source_id, member.coord, member.kind))
        .collect();
    let mut full: Vec<_> = (0..=5)
        .map(|row| {
            replay_record(
                source.source_id,
                SourceCoord { row, col: 2 },
                if row == 3 {
                    PartitionLegacyMemberKind::OrdinaryException
                } else {
                    PartitionLegacyMemberKind::SharedFamilyMember
                },
            )
        })
        .collect();
    full[0].text = "(".to_string();
    let clean = SourceFormulaFamily {
        source_order: crate::engine::SourceFormulaOrder::new(0),
        source_id: SourceFamilyId {
            sheet_instance: 404,
            source_index: 2,
        },
        anchor_coord0: SourceCoord { row: 0, col: 5 },
        anchor_text: Arc::from("$A$1+1"),
        members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
            row_start: 0,
            row_end: 1,
            col: 5,
        }),
        member_count: 2,
    };
    let preparation = engine
        .source_formula_ingress()
        .prepare_eager_proposals(
            "Sheet1",
            std::slice::from_ref(&clean),
            std::slice::from_ref(&source),
            8,
            Box::new(PartitionSetReplay {
                source: source.clone(),
                initial,
                full,
                fail_full: false,
                require_legacy_partition_routing: false,
            }),
        )
        .unwrap();
    engine.set_fragmented_commit_fault_for_test(FragmentedCommitFault::FormulaPlaneFinalCheck);
    let report = FormulaCompressedSourceReport {
        families_seen: 2,
        family_cells_seen: 7,
        source_clean_families: 1,
        source_clean_cells: 2,
        source_fragmentable_families: 1,
        source_fragmentable_cells: 5,
        source_fragment_count: 2,
        source_hole_exclusions: 1,
        source_ordinary_exclusions: 1,
        replay_families: 2,
        replay_cells: 7,
        ..FormulaCompressedSourceReport::default()
    };
    let error = engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            report,
            preparation,
        )])
        .unwrap_err();
    assert!(error.to_string().contains("ParserError"), "{error}");
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    let published = engine.last_formula_ingest_report().unwrap();
    assert_eq!(published.source_family_promoted, 1, "{published:?}");
    assert_eq!(published.source_family_promoted_cells, 2, "{published:?}");
    assert_eq!(published.source_partitioned_families_prepared, 0);
}

#[test]
fn late_fallback_success_reports_exact_whole_family_e2_deltas_once() {
    let mut engine = make_engine();
    let source = family(Shape::Row, 100);
    let initial = source
        .legacy_members
        .as_slice()
        .iter()
        .map(|member| replay_record(source.source_id, member.coord, member.kind))
        .collect();
    let full = (0..=5)
        .map(|row| {
            replay_record(
                source.source_id,
                SourceCoord { row, col: 2 },
                if row == 3 {
                    PartitionLegacyMemberKind::OrdinaryException
                } else {
                    PartitionLegacyMemberKind::SharedFamilyMember
                },
            )
        })
        .collect();
    let preparation = engine
        .source_formula_ingress()
        .prepare_eager_proposals(
            "Sheet1",
            &[],
            std::slice::from_ref(&source),
            6,
            Box::new(PartitionSetReplay {
                source: source.clone(),
                initial,
                full,
                fail_full: false,
                require_legacy_partition_routing: false,
            }),
        )
        .unwrap();
    engine.set_fragmented_commit_fault_for_test(FragmentedCommitFault::FormulaPlaneFinalCheck);
    let report = FormulaCompressedSourceReport {
        families_seen: 1,
        family_cells_seen: 5,
        source_fragmentable_families: 1,
        source_fragmentable_cells: 5,
        source_fragment_count: 2,
        source_hole_exclusions: 1,
        source_ordinary_exclusions: 1,
        replay_families: 1,
        replay_cells: 5,
        ..FormulaCompressedSourceReport::default()
    };
    let report = engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            report,
            preparation,
        )])
        .unwrap();
    assert_eq!(report.formula_cells_seen, 6, "{report:?}");
    assert_eq!(report.graph_formula_cells_materialized, 6, "{report:?}");
    assert_eq!(report.graph_vertices_created, 6, "{report:?}");
    assert_eq!(report.graph_edges_created, 6, "{report:?}");
    assert_eq!(report.source_spool_replays, 2, "{report:?}");
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback, 1, "{report:?}");
    assert_eq!(report.source_family_fallback_cells, 5, "{report:?}");
    assert_eq!(report.source_partitioned_families_prepared, 0, "{report:?}");
    assert_eq!(report.source_partitioned_families_rejected, 1, "{report:?}");
    assert_eq!(report.source_partition_failures, 1, "{report:?}");
    assert_eq!(report.source_partition_fallback_cells, 5, "{report:?}");
    assert_eq!(report.source_anchor_parses, 1, "{report:?}");
    assert_eq!(report.source_anchor_asts, 1, "{report:?}");
    assert_eq!(report.source_anchor_analyses, 1, "{report:?}");
    assert_eq!(
        report
            .fallback_reasons
            .get("Injected(FormulaPlaneFinalCheck)"),
        Some(&1),
        "{report:?}"
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 6);
}

#[test]
fn provider_change_after_fallback_validation_fails_before_mutation() {
    let (mut engine, revision) = make_engine_and_revision();
    let source = family(Shape::Row, 101);
    let initial = source
        .legacy_members
        .as_slice()
        .iter()
        .map(|member| replay_record(source.source_id, member.coord, member.kind))
        .collect();
    let full = (0..=5)
        .map(|row| {
            replay_record(
                source.source_id,
                SourceCoord { row, col: 2 },
                if row == 3 {
                    PartitionLegacyMemberKind::OrdinaryException
                } else {
                    PartitionLegacyMemberKind::SharedFamilyMember
                },
            )
        })
        .collect();
    let preparation = engine
        .source_formula_ingress()
        .prepare_eager_proposals(
            "Sheet1",
            &[],
            std::slice::from_ref(&source),
            6,
            Box::new(PartitionSetReplay {
                source: source.clone(),
                initial,
                full,
                fail_full: false,
                require_legacy_partition_routing: false,
            }),
        )
        .unwrap();
    engine.set_fragmented_commit_fault_for_test(FragmentedCommitFault::FormulaPlaneFinalCheck);
    engine.set_before_legacy_fallback_final_provider_sample_hook(move || {
        revision.fetch_add(1, Ordering::AcqRel);
    });
    let report = FormulaCompressedSourceReport {
        families_seen: 1,
        family_cells_seen: 5,
        source_fragmentable_families: 1,
        source_fragmentable_cells: 5,
        source_fragment_count: 2,
        source_hole_exclusions: 1,
        source_ordinary_exclusions: 1,
        replay_families: 1,
        replay_cells: 5,
        ..FormulaCompressedSourceReport::default()
    };
    let error = engine
        .source_formula_ingress()
        .finish_prepared(vec![(
            FormulaIngestBatch::new("Sheet1", Vec::new()),
            report,
            preparation,
        )])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("changed after whole-family fallback validation"),
        "{error}"
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
}

#[test]
fn composition_is_read_only_and_rejects_inexact_ownership() {
    let mut engine = make_engine();
    let source = family(Shape::Row, 1);
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for(&mut engine, &source);
    let mut disposition = FormulaReplayDisposition::default();
    disposition.register_partition(&source, true).unwrap();
    disposition.extend_suppressed_excel_coords([(7, 3)]);
    assert_eq!(
        disposition.partition_disposition(&source, source.template_origin0),
        Some(crate::engine::FormulaReplayCoordinateDisposition::Direct)
    );
    assert_eq!(
        disposition.partition_disposition(&source, SourceCoord { row: 2, col: 2 }),
        Some(crate::engine::FormulaReplayCoordinateDisposition::LegacyShared)
    );
    assert_eq!(
        disposition.partition_disposition(&source, SourceCoord { row: 3, col: 2 }),
        Some(crate::engine::FormulaReplayCoordinateDisposition::LegacyOrdinary)
    );
    assert_eq!(
        disposition.partition_disposition(&source, SourceCoord { row: 6, col: 2 }),
        None
    );
    let before = state(&engine);
    let _transaction = engine
        .prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy)
        .unwrap();
    assert_eq!(state(&engine), before);

    let mut engine = make_engine();
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let mut legacy = formulas_for(&mut engine, &source);
    legacy.pop();
    assert!(matches!(
        engine.prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy,),
        Err(FragmentedTransactionPrepareError::LegacyOwnership)
    ));

    let mut engine = make_engine();
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for(&mut engine, &source);
    let stale = FormulaReplayDisposition::default();
    assert!(matches!(
        engine.prepare_fragmented_source_transaction(&source, &stale, prepared, legacy),
        Err(FragmentedTransactionPrepareError::DispositionOwnership)
    ));
}

#[test]
fn cross_fragment_chain_and_cycle_select_exact_replay_before_composed_mutation() {
    for (source_index, cycle) in [(2, false), (3, true)] {
        let mut engine = make_engine();
        let source = cross_fragment_family(source_index, cycle);
        let prepared = engine
            .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
            .unwrap();
        let mut disposition = FormulaReplayDisposition::default();
        disposition.register_partition(&source, true).unwrap();
        let before = state(&engine);
        let error = engine
            .prepare_fragmented_source_transaction(&source, &disposition, prepared, Vec::new())
            .unwrap_err();
        assert_eq!(
            error,
            FragmentedTransactionPrepareError::CrossFragmentDependency
        );
        assert!(error.selects_whole_family_replay());
        assert_eq!(state(&engine), before);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
        assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    }
}

#[test]
fn every_pre_mutation_fault_replays_without_publication_or_state_change() {
    let faults = [
        FragmentedCommitFault::DispositionCheck,
        FragmentedCommitFault::SemanticRevisionCheck,
        FragmentedCommitFault::LegacyGraphFinalCheck,
        FragmentedCommitFault::FormulaPlaneFinalCheck,
        FragmentedCommitFault::BeforeFirstMutation,
    ];
    for (index, fault) in faults.into_iter().enumerate() {
        let (mut engine, source, disposition, prepared, _) = transaction(Shape::Row, 10 + index);
        let before = state(&engine);
        let dirty_before = engine.graph.formula_dirty_stats();
        let revision_before = engine.graph.topology_revision();
        let disposition_before = disposition.clone();
        let decision = engine.commit_fragmented_source_transaction_with_fault_for_test(
            prepared,
            &disposition,
            fault,
        );
        assert!(matches!(
            decision,
            FragmentedCommitDecision::ReplayWholeFamily {
                source_id,
                reason: FragmentedReplayReason::Injected(actual),
            } if source_id == source.source_id && actual == fault
        ));
        assert_eq!(state(&engine), before, "fault {fault:?} mutated state");
        assert_eq!(engine.graph.formula_dirty_stats(), dirty_before);
        assert_eq!(engine.graph.topology_revision(), revision_before);
        assert_eq!(disposition, disposition_before);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
        assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    }
}

#[test]
fn stale_disposition_provider_and_registry_choose_exact_family_replay() {
    let (mut engine, source, disposition, prepared, _) = transaction(Shape::Row, 30);
    let before = state(&engine);
    let mut stale = disposition.clone();
    stale.force_family_legacy(source.source_id);
    assert!(matches!(
        engine.commit_fragmented_source_transaction(prepared, &stale),
        FragmentedCommitDecision::ReplayWholeFamily {
            reason: FragmentedReplayReason::DispositionChanged,
            ..
        }
    ));
    assert_eq!(state(&engine), before);
    assert_eq!(
        disposition.shared_disposition(source.source_id, source.template_origin0),
        crate::engine::FormulaReplayCoordinateDisposition::Direct
    );

    let (mut engine, _, disposition, prepared, revision) =
        transaction_for_source(function_family(31));
    let before = state(&engine);
    revision.fetch_add(1, Ordering::AcqRel);
    assert!(matches!(
        engine.commit_fragmented_source_transaction(prepared, &disposition),
        FragmentedCommitDecision::ReplayWholeFamily {
            reason: FragmentedReplayReason::FunctionProviderRevisionChanged,
            ..
        }
    ));
    assert_eq!(state(&engine), before);

    let (mut engine, _, disposition, prepared, revision) =
        transaction_for_source(function_family(33));
    let before = state(&engine);
    assert!(matches!(
        engine.commit_fragmented_source_transaction_with_provider_flip_for_test(
            prepared,
            &disposition,
            move || {
                revision.fetch_add(1, Ordering::AcqRel);
            },
        ),
        FragmentedCommitDecision::ReplayWholeFamily {
            reason: FragmentedReplayReason::FunctionProviderRevisionChanged,
            ..
        }
    ));
    assert_eq!(state(&engine), before);

    let (mut engine, _, disposition, prepared, _) = transaction_for_source(function_family(32));
    let (epoch, provider) = prepared.semantic_revisions_for_test();
    let before = state(&engine);
    assert!(matches!(
        engine.commit_fragmented_source_transaction_with_revisions_for_test(
            prepared,
            &disposition,
            epoch + 1,
            provider,
        ),
        FragmentedCommitDecision::ReplayWholeFamily {
            reason: FragmentedReplayReason::FunctionSemanticEpochChanged,
            ..
        }
    ));
    assert_eq!(state(&engine), before);
}

#[test]
fn exact_relative_replay_owns_relocated_ast_text_plan_and_value() {
    let mut engine = make_engine();
    let source = relative_replay_family(39);
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for(&mut engine, &source);
    let exact_texts: Vec<_> = legacy
        .iter()
        .map(|formula| formula.exact_formula_text_for_test())
        .collect();
    assert!(exact_texts.contains(&"=A3+1"));
    assert!(exact_texts.contains(&"=$A$1+5"));
    let mut disposition = FormulaReplayDisposition::default();
    disposition.register_partition(&source, true).unwrap();
    let prepared = engine
        .prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy)
        .unwrap();
    assert!(matches!(
        engine.commit_fragmented_source_transaction(prepared, &disposition),
        FragmentedCommitDecision::Committed(_)
    ));

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(11.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(11.0))
    );
    let sheet = engine.graph.sheet_id("Sheet1").unwrap();
    let fallback = CellRef::new(sheet, Coord::from_excel(3, 3, true, true));
    let vertex = engine.graph.get_vertex_for_cell(&fallback).unwrap();
    let ast = engine.graph.get_formula(vertex).unwrap();
    assert_eq!(
        formualizer_parse::pretty::canonical_formula(&ast),
        "=A3 + 1"
    );
}

#[test]
fn direct_name_definition_change_replays_whole_family_without_mutation() {
    let mut engine = make_engine();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(20.0))
        .unwrap();
    let sheet = engine.graph.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "N",
            NamedDefinition::Cell(CellRef::new(sheet, Coord::from_excel(1, 1, true, true))),
            NameScope::Workbook,
        )
        .unwrap();
    let mut source = family(Shape::Row, 40);
    source.template_text = Arc::from("N+1");
    source.legacy_members = ExplicitPartitionLegacyMembers::try_new(vec![PartitionLegacyMember {
        coord: SourceCoord { row: 3, col: 2 },
        kind: PartitionLegacyMemberKind::OrdinaryException,
    }])
    .unwrap();
    source.surviving_member_count = 4;
    source.reconciliation = PartitionReconciliation {
        shared_members: 4,
        ordinary_exceptions: 1,
        holes: 2,
    };
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for(&mut engine, &source);
    let mut disposition = FormulaReplayDisposition::default();
    disposition.register_partition(&source, true).unwrap();
    let prepared = engine
        .prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy)
        .unwrap();

    engine
        .update_name(
            "N",
            NamedDefinition::Cell(CellRef::new(sheet, Coord::from_excel(1, 2, true, true))),
            NameScope::Workbook,
        )
        .unwrap();
    let before = state(&engine);
    match engine.commit_fragmented_source_transaction(prepared, &disposition) {
        FragmentedCommitDecision::ReplayWholeFamily {
            source_id,
            reason: FragmentedReplayReason::NameBindingChanged,
        } if source_id == source.source_id => {}
        other => panic!("unexpected name-stale decision: {other:?}"),
    }
    assert_eq!(state(&engine), before);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
}

#[test]
fn e4_legacy_table_and_source_metadata_changes_replay_before_mutation() {
    let mut engine = make_engine();
    let sheet = engine.graph.sheet_id("Sheet1").unwrap();
    let table_range = RangeRef::new(
        CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
        CellRef::new(sheet, Coord::from_excel(2, 2, true, true)),
    );
    engine
        .define_table(
            "T",
            table_range,
            true,
            vec!["A".to_string(), "B".to_string()],
            false,
        )
        .unwrap();
    let source = ordinary_only_family(41);
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for_with_ordinary(&mut engine, &source, "=SUM(T[A])");
    let mut disposition = FormulaReplayDisposition::default();
    disposition.register_partition(&source, true).unwrap();
    let prepared = engine
        .prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy)
        .unwrap();
    let changed_range = RangeRef::new(
        CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
        CellRef::new(sheet, Coord::from_excel(2, 3, true, true)),
    );
    engine
        .graph
        .update_table(
            "T",
            changed_range,
            true,
            vec!["A".to_string(), "B".to_string(), "C".to_string()],
            true,
        )
        .unwrap();
    let before = state(&engine);
    assert!(matches!(
        engine.commit_fragmented_source_transaction(prepared, &disposition),
        FragmentedCommitDecision::ReplayWholeFamily {
            reason: FragmentedReplayReason::LegacyGraphStale(
                crate::engine::graph::prepared_legacy_graph::PreparedLegacyGraphError::Stale
            ),
            ..
        }
    ));
    assert_eq!(state(&engine), before);

    let mut engine = make_engine();
    engine.define_source_scalar("S", Some(1)).unwrap();
    let source = ordinary_only_family(42);
    let prepared = engine
        .analyze_partitioned_source_family_for_transaction("Sheet1", &source)
        .unwrap();
    let legacy = formulas_for_with_ordinary(&mut engine, &source, "=S+1");
    let mut disposition = FormulaReplayDisposition::default();
    disposition.register_partition(&source, true).unwrap();
    let prepared = engine
        .prepare_fragmented_source_transaction(&source, &disposition, prepared, legacy)
        .unwrap();
    engine.set_source_scalar_version("S", Some(2)).unwrap();
    let before = state(&engine);
    assert!(matches!(
        engine.commit_fragmented_source_transaction(prepared, &disposition),
        FragmentedCommitDecision::ReplayWholeFamily {
            reason: FragmentedReplayReason::LegacyGraphStale(
                crate::engine::graph::prepared_legacy_graph::PreparedLegacyGraphError::Stale
            ),
            ..
        }
    ));
    assert_eq!(state(&engine), before);
}

#[test]
fn row_column_and_rect_success_commit_graph_then_plane_and_return_local_deltas() {
    for (index, shape) in [Shape::Row, Shape::Column, Shape::Rect]
        .into_iter()
        .enumerate()
    {
        let (mut engine, source, disposition, prepared, _) = transaction(shape, 50 + index);
        let published_before = (
            engine.last_formula_ingest_report().cloned(),
            engine.formula_ingest_report_total().clone(),
            format!("{:?}", engine.last_virtual_dep_telemetry()),
        );
        let success = match engine.commit_fragmented_source_transaction(prepared, &disposition) {
            FragmentedCommitDecision::Committed(success) => success,
            other => panic!("unexpected replay: {other:?}"),
        };
        assert_eq!(success.source_id, source.source_id);
        assert_eq!(success.graph_formulas, 2);
        assert_eq!(success.plane.spans.len(), source.fragments.len());
        assert_eq!(success.work.fragments_checked, source.fragments.len());
        assert_eq!(success.work.legacy_members_checked, 2);
        assert_eq!(success.work.legacy_formulas_staged, 2);
        assert_eq!(success.work.holes_observed, source.reconciliation.holes);
        assert_eq!(success.work.plane.placements, source.fragments.len());
        assert_eq!(success.report_delta.source_partitioned_families_prepared, 1);
        assert_eq!(
            success.report_delta.source_partition_holes,
            source.reconciliation.holes
        );
        assert_eq!(success.report_delta.graph_formula_cells_materialized, 2);
        assert_eq!(success.report_delta.graph_vertices_created, 2);
        assert_eq!(success.report_delta.graph_edges_created, 2);
        assert_eq!(
            engine.baseline_stats().formula_plane_active_span_count,
            source.fragments.len()
        );
        assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);
        assert_eq!(engine.baseline_stats().graph_edge_count, 2);
        assert_eq!(
            engine
                .graph
                .pending_formula_dirty_whole_spans()
                .collect::<Vec<_>>(),
            success.plane.spans
        );
        assert_eq!(
            engine
                .baseline_stats()
                .formula_plane_dirty_whole_span_seeds_recorded,
            source.fragments.len() as u64
        );
        assert_eq!(
            (
                engine.last_formula_ingest_report().cloned(),
                engine.formula_ingest_report_total().clone(),
                format!("{:?}", engine.last_virtual_dep_telemetry()),
            ),
            published_before,
        );

        let sheet = engine.graph.sheet_id("Sheet1").unwrap();
        for member in source.legacy_members.as_slice() {
            let cell = CellRef::new(
                sheet,
                Coord::from_excel(member.coord.row + 1, member.coord.col + 1, true, true),
            );
            let vertex = engine.graph.get_vertex_for_cell(&cell).unwrap();
            assert!(engine.graph.get_formula_id(vertex).is_some());
        }
        engine.evaluate_all().unwrap();
        assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
        let hole = match shape {
            Shape::Row | Shape::Column => source.declared.end,
            Shape::Rect => SourceCoord { row: 3, col: 4 },
        };
        assert_eq!(
            engine.get_cell_value("Sheet1", hole.row + 1, hole.col + 1),
            None,
        );
        for span in success.plane.spans {
            let authority = engine.graph.formula_authority();
            let record = authority.plane.spans.get(span).unwrap();
            assert_eq!(
                authority
                    .plane
                    .templates
                    .get(record.template_id)
                    .and_then(|template| template.formula_text.as_deref()),
                Some(source.template_text.as_ref()),
            );
            for coord in record.domain.iter() {
                assert_eq!(
                    engine.get_cell_value("Sheet1", coord.row + 1, coord.col + 1),
                    Some(LiteralValue::Number(11.0))
                );
            }
        }
        let legacy_value = |kind, expected| {
            let coord = source
                .legacy_members
                .as_slice()
                .iter()
                .find(|member| member.kind == kind)
                .unwrap()
                .coord;
            assert_eq!(
                engine.get_cell_value("Sheet1", coord.row + 1, coord.col + 1),
                Some(LiteralValue::Number(expected))
            );
        };
        legacy_value(PartitionLegacyMemberKind::SharedFamilyMember, 11.0);
        legacy_value(PartitionLegacyMemberKind::OrdinaryException, 15.0);
    }
}
