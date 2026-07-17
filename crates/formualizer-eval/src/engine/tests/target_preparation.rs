use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use formualizer_common::{ExcelErrorExtra, LiteralValue, RangeAddress, ResourceExhaustionReason};

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::target_preparation::TargetPreparationFault;
use crate::engine::{
    AdmissionResourceBudget, DeferredFormulaPackage, DeferredFormulaReplay, DeferredReplayFormula,
    Engine, EvalConfig, EvaluationBudgets, EvaluationTarget, ExplicitPartitionLegacyMembers,
    FormulaCompressedSourceReport, FormulaParsePolicy, FormulaPlaneMode,
    FormulaReplayCoordinateDisposition, FormulaReplayDisposition, OpaquePreparePolicy,
    OpaqueReason, PartitionLegacyMember, PartitionLegacyMemberKind, PartitionReconciliation,
    PartitionedSourceFormulaFamily, PlacementDomainTransport, PreparationOutcome, PrepareScope,
    PrepareTargetsOptions, SourceCoord, SourceFamilyId, SourceFamilyMembers, SourceFormulaFamily,
    SourceFormulaOrder, SourceRect, TableSelection,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;

fn engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    static BUILTINS_READY: OnceLock<()> = OnceLock::new();
    BUILTINS_READY.get_or_init(crate::builtins::load_builtins);
    let mut config = EvalConfig::default().with_formula_plane_mode(mode);
    config.defer_graph_building = true;
    let mut engine = Engine::new(TestWorkbook::new(), config);
    for sheet in ["Inputs", "Middle", "Outputs"] {
        engine.add_sheet(sheet).unwrap();
    }
    engine
}

fn cell(sheet: &str, row: u32, col: u32) -> EvaluationTarget {
    EvaluationTarget::Cell {
        sheet: sheet.to_string(),
        row,
        col,
    }
}

#[test]
fn three_sheet_transitive_chain_prepares_only_reachable_units_in_off_and_shadow() {
    for mode in [FormulaPlaneMode::Off, FormulaPlaneMode::Shadow] {
        let mut engine = engine(mode);
        engine.stage_formula_text("Inputs", 1, 1, "=1".into());
        engine.stage_formula_text("Middle", 1, 2, "=Inputs!A1+1".into());
        engine.stage_formula_text("Outputs", 1, 3, "=Middle!B1+1".into());
        engine.stage_formula_text("Inputs", 10, 10, "=99".into());

        let report = engine
            .prepare_graph_for_targets(&[cell("Outputs", 1, 3)], Default::default())
            .unwrap();
        assert_eq!(report.selected_staged_cells, 3);
        assert_eq!(report.retained_staged_cells, 1);
        assert_eq!(report.widened_scope, PrepareScope::Exact);
        assert_eq!(
            engine.get_staged_formula_text("Inputs", 10, 10).as_deref(),
            Some("=99")
        );
        assert!(engine.staged_formula_index_is_consistent_for_test());

        engine.config.defer_graph_building = false;
        assert_eq!(
            engine.evaluate_cell("Outputs", 1, 3).unwrap(),
            Some(LiteralValue::Number(3.0))
        );
        engine.config.defer_graph_building = true;
        let later = engine
            .prepare_graph_for_targets(&[cell("Inputs", 10, 10)], Default::default())
            .unwrap();
        assert_eq!(later.selected_staged_cells, 1);
        assert_eq!(later.retained_staged_cells, 0);
    }
}

#[test]
fn exact_range_intersection_keeps_independent_same_sheet_chain_staged() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.stage_formula_text("Inputs", 1, 1, "=1".into());
    engine.stage_formula_text("Inputs", 1, 2, "=A1+1".into());
    engine.stage_formula_text("Inputs", 1, 4, "=10".into());
    engine.stage_formula_text("Inputs", 1, 5, "=D1+1".into());

    let target = EvaluationTarget::Range(RangeAddress::new("Inputs", 1, 2, 1, 2).unwrap());
    let first = engine
        .prepare_graph_for_targets(&[target], Default::default())
        .unwrap();
    assert_eq!(first.selected_staged_cells, 2);
    assert_eq!(first.retained_staged_cells, 2);
    assert!(engine.get_staged_formula_text("Inputs", 1, 4).is_some());
    assert!(engine.get_staged_formula_text("Inputs", 1, 5).is_some());

    let second = engine
        .prepare_graph_for_targets(&[cell("Inputs", 1, 5)], Default::default())
        .unwrap();
    assert_eq!(second.selected_staged_cells, 2);
    assert_eq!(second.retained_staged_cells, 0);
}

#[test]
fn target_preparation_then_ordinary_evaluation_matches_prepare_all_oracle() {
    let setup = || {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine
            .set_cell_value("Inputs", 1, 1, LiteralValue::Number(10.0))
            .unwrap();
        engine.stage_formula_text("Middle", 1, 1, "=Inputs!A1*2".into());
        engine.stage_formula_text("Outputs", 1, 1, "=Middle!A1+5".into());
        engine.stage_formula_text("Inputs", 9, 9, "=999".into());
        engine
    };
    let mut target = setup();
    let mut oracle = setup();
    target
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    oracle.build_graph_all().unwrap();
    target.config.defer_graph_building = false;
    oracle.config.defer_graph_building = false;
    let target_value = target.evaluate_cell("Outputs", 1, 1).unwrap();
    let oracle_value = oracle.evaluate_cell("Outputs", 1, 1).unwrap();
    assert_eq!(target_value, oracle_value);
    assert_eq!(target.staged_formula_count(), 1);
}

#[test]
fn staged_cycle_terminates_and_commits_as_one_plan() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.stage_formula_text("Inputs", 1, 1, "=Middle!A1".into());
    engine.stage_formula_text("Middle", 1, 1, "=Inputs!A1".into());
    engine.stage_formula_text("Outputs", 5, 5, "=5".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Inputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.selected_staged_cells, 2);
    assert_eq!(report.retained_staged_cells, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);
}

#[test]
fn name_and_table_targets_resolve_to_concrete_regions() {
    let mut engine = engine(FormulaPlaneMode::Off);
    let inputs = engine.sheet_id("Inputs").unwrap();
    let named_cell = CellRef::new(inputs, Coord::from_excel(4, 2, true, true));
    engine
        .define_name(
            "Chosen",
            NamedDefinition::Cell(named_cell),
            NameScope::Workbook,
        )
        .unwrap();
    engine.stage_formula_text("Inputs", 4, 2, "=40+2".into());

    let start = CellRef::new(inputs, Coord::from_excel(10, 1, true, true));
    let end = CellRef::new(inputs, Coord::from_excel(12, 2, true, true));
    engine
        .define_table(
            "Sales",
            RangeRef::new(start, end),
            true,
            vec!["Region".into(), "Amount".into()],
            false,
        )
        .unwrap();
    engine.stage_formula_text("Inputs", 11, 2, "=21*2".into());

    let name_report = engine
        .prepare_graph_for_targets(
            &[EvaluationTarget::Name {
                name: "Chosen".into(),
                scope_sheet: None,
            }],
            Default::default(),
        )
        .unwrap();
    assert_eq!(name_report.selected_staged_cells, 1);

    let table_report = engine
        .prepare_graph_for_targets(
            &[EvaluationTarget::Table {
                name: "Sales".into(),
                selection: TableSelection::Data,
            }],
            Default::default(),
        )
        .unwrap();
    assert_eq!(table_report.selected_staged_cells, 1);
    assert_eq!(table_report.widened_scope, PrepareScope::Exact);
}

#[test]
fn opaque_dynamic_formula_widens_once_to_workbook_and_retains_reason() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.stage_formula_text("Outputs", 1, 1, "=INDIRECT(\"Inputs!A1\")".into());
    engine.stage_formula_text("Inputs", 20, 1, "=20".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    assert!(
        report
            .widening_reasons
            .contains(&OpaqueReason::RuntimeTextReference)
    );
    assert_eq!(report.selected_staged_cells, 2);
    assert_eq!(report.retained_staged_cells, 0);
}

#[test]
fn package_fallback_table_reference_discovers_the_same_staged_closure_as_ordinary() {
    let setup = || {
        let mut engine = engine(FormulaPlaneMode::Off);
        let inputs = engine.sheet_id("Inputs").unwrap();
        engine
            .define_table(
                "Sales",
                RangeRef::new(
                    CellRef::new(inputs, Coord::from_excel(10, 1, true, true)),
                    CellRef::new(inputs, Coord::from_excel(12, 2, true, true)),
                ),
                true,
                vec!["Region".into(), "Amount".into()],
                false,
            )
            .unwrap();
        engine.stage_formula_text("Inputs", 11, 2, "=40+2".into());
        engine
            .source_formula_ingress()
            .stage_deferred(fallback_package(
                "Outputs",
                &[(1, 1, "=SUM(Sales[Amount])")],
            ));
        engine
    };

    let mut target = setup();
    let mut oracle = setup();
    let report = target
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.selected_staged_cells, 2);
    assert_eq!(report.widened_scope, PrepareScope::Exact);
    oracle.build_graph_all().unwrap();
    target.config.defer_graph_building = false;
    oracle.config.defer_graph_building = false;
    assert_eq!(
        target.evaluate_cell("Outputs", 1, 1).unwrap(),
        oracle.evaluate_cell("Outputs", 1, 1).unwrap()
    );
}

#[test]
fn package_fallback_opaque_3d_reference_widens_and_matches_prepare_all_error() {
    let setup = || {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine.stage_formula_text("Inputs", 9, 9, "=99".into());
        engine
            .source_formula_ingress()
            .stage_deferred(fallback_package("Outputs", &[(1, 1, "=Inputs:Middle!A1")]));
        engine
    };
    let mut target = setup();
    let mut oracle = setup();
    let report = target
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    assert!(
        report
            .widening_reasons
            .contains(&OpaqueReason::UnresolvedCrossSheetBinding)
    );
    assert_eq!(report.selected_staged_cells, 2);
    oracle.build_graph_all().unwrap();
    target.config.defer_graph_building = false;
    oracle.config.defer_graph_building = false;
    assert_eq!(
        target.evaluate_cell("Outputs", 1, 1).unwrap(),
        oracle.evaluate_cell("Outputs", 1, 1).unwrap()
    );
    assert!(matches!(
        target.evaluate_cell("Outputs", 1, 1).unwrap(),
        Some(LiteralValue::Error(ref error))
            if error.kind == formualizer_common::ExcelErrorKind::NImpl
    ));
}

#[test]
fn strict_opaque_policy_is_preserved_for_package_fallback_and_authoritative_compatibility() {
    let strict = PrepareTargetsOptions {
        opaque_policy: OpaquePreparePolicy::Error,
        ..Default::default()
    };

    for formula in [
        "=INDIRECT(\"Inputs!A1\")",
        "=MYSTERY(A1)",
        "=Inputs:Middle!A1",
    ] {
        let mut fallback = engine(FormulaPlaneMode::Off);
        fallback
            .source_formula_ingress()
            .stage_deferred(fallback_package("Outputs", &[(1, 1, formula)]));
        let error = fallback
            .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], strict.clone())
            .unwrap_err();
        assert_eq!(
            error.kind,
            formualizer_common::ExcelErrorKind::NImpl,
            "{formula}"
        );
        assert!(fallback.has_staged_formulas(), "{formula}");
    }

    let mut authoritative = engine(FormulaPlaneMode::AuthoritativeExperimental);
    authoritative.stage_formula_text("Outputs", 1, 1, "=1".into());
    let error = authoritative
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], strict)
        .unwrap_err();
    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::NImpl);
    assert!(authoritative.has_staged_formulas());
}

#[derive(Default)]
struct EmptyReplay;

impl DeferredFormulaReplay for EmptyReplay {
    fn replay(
        &mut self,
        _disposition: &crate::engine::FormulaReplayDisposition,
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

struct FamilyReplay {
    records: Vec<DeferredReplayFormula>,
}

impl DeferredFormulaReplay for FamilyReplay {
    fn replay(
        &mut self,
        disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        Ok(self
            .records
            .iter()
            .filter(|record| {
                let coord = SourceCoord {
                    row: record.row - 1,
                    col: record.col - 1,
                };
                record.family.is_none_or(|family| {
                    disposition.shared_disposition(family, coord)
                        == FormulaReplayCoordinateDisposition::LegacyShared
                })
            })
            .cloned()
            .collect())
    }

    fn replay_partitioned(
        &mut self,
        disposition: &FormulaReplayDisposition,
        _partitions: &[PartitionedSourceFormulaFamily],
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        self.replay(disposition)
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        Ok(self
            .records
            .iter()
            .find(|record| record.row == row && record.col == col)
            .cloned())
    }
}

fn fallback_package(sheet: &str, formulas: &[(u32, u32, &str)]) -> DeferredFormulaPackage {
    let records = formulas
        .iter()
        .enumerate()
        .map(|(source_order, &(row, col, text))| DeferredReplayFormula {
            source_order: SourceFormulaOrder::new(source_order as u64),
            row,
            col,
            text: text.to_string(),
            family: None,
            partition_owner: None,
        })
        .collect::<Vec<_>>();
    let coordinates = formulas
        .iter()
        .map(|&(row, col, _)| SourceCoord {
            row: row - 1,
            col: col - 1,
        })
        .collect();
    DeferredFormulaPackage::new_with_source_coordinates(
        sheet.to_string(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: records.len() as u64,
            ..Default::default()
        },
        Vec::new(),
        Vec::new(),
        coordinates,
        Box::new(FamilyReplay { records }),
    )
}

struct CountingReplay {
    records: Vec<DeferredReplayFormula>,
    replay_count: Arc<AtomicUsize>,
}

impl DeferredFormulaReplay for CountingReplay {
    fn replay(
        &mut self,
        disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        self.replay_count.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .records
            .iter()
            .filter(|record| {
                let coord = SourceCoord {
                    row: record.row - 1,
                    col: record.col - 1,
                };
                record.family.is_none_or(|family| {
                    disposition.shared_disposition(family, coord)
                        == FormulaReplayCoordinateDisposition::LegacyShared
                })
            })
            .cloned()
            .collect())
    }

    fn replay_partitioned(
        &mut self,
        disposition: &FormulaReplayDisposition,
        _partitions: &[PartitionedSourceFormulaFamily],
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        self.replay(disposition)
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        Ok(self
            .records
            .iter()
            .find(|record| record.row == row && record.col == col)
            .cloned())
    }
}

fn counting_fallback_package(
    sheet: &str,
    formula: &str,
    replay_count: Arc<AtomicUsize>,
) -> DeferredFormulaPackage {
    let record = DeferredReplayFormula {
        source_order: SourceFormulaOrder::new(0),
        row: 1,
        col: 1,
        text: formula.to_string(),
        family: None,
        partition_owner: None,
    };
    DeferredFormulaPackage::new_with_source_coordinates(
        sheet.to_string(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: 1,
            ..Default::default()
        },
        Vec::new(),
        Vec::new(),
        vec![SourceCoord { row: 0, col: 0 }],
        Box::new(CountingReplay {
            records: vec![record],
            replay_count,
        }),
    )
}

fn fragmented_family_package(sheet: &str, sheet_instance: u32) -> DeferredFormulaPackage {
    let source_id = SourceFamilyId {
        sheet_instance,
        source_index: 0,
    };
    let ordinary_coord = SourceCoord { row: 151, col: 2 };
    let family = PartitionedSourceFormulaFamily {
        source_id,
        source_order: SourceFormulaOrder::new(0),
        template_origin0: SourceCoord { row: 0, col: 2 },
        template_text: Arc::from("$A$1+1"),
        declared: SourceRect {
            start: SourceCoord { row: 0, col: 2 },
            end: SourceCoord { row: 301, col: 2 },
        },
        surviving_member_count: 300,
        fragments: vec![
            PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 149,
                col: 2,
            },
            PlacementDomainTransport::RowRun {
                row_start: 152,
                row_end: 301,
                col: 2,
            },
        ],
        legacy_members: ExplicitPartitionLegacyMembers::try_new(vec![PartitionLegacyMember {
            coord: ordinary_coord,
            kind: PartitionLegacyMemberKind::OrdinaryException,
        }])
        .unwrap(),
        reconciliation: PartitionReconciliation {
            shared_members: 300,
            ordinary_exceptions: 1,
            holes: 1,
        },
    };
    let mut records = Vec::new();
    let mut coordinates = Vec::new();
    for row0 in 0..=301 {
        if row0 == 150 {
            continue;
        }
        let coord = SourceCoord { row: row0, col: 2 };
        coordinates.push(coord);
        records.push(DeferredReplayFormula {
            source_order: SourceFormulaOrder::new(u64::from(row0)),
            row: row0 + 1,
            col: 3,
            text: if coord == ordinary_coord {
                "=$A$1+5".to_string()
            } else {
                "=$A$1+1".to_string()
            },
            family: (coord != ordinary_coord).then_some(source_id),
            partition_owner: Some(source_id),
        });
    }
    DeferredFormulaPackage::new_with_source_coordinates(
        sheet.to_string(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: 301,
            families_seen: 1,
            family_cells_seen: 300,
            source_fragmentable_families: 1,
            source_fragmentable_cells: 300,
            source_fragment_count: 2,
            source_hole_exclusions: 1,
            source_ordinary_exclusions: 1,
            ..Default::default()
        },
        Vec::new(),
        vec![family],
        coordinates,
        Box::new(FamilyReplay { records }),
    )
}

fn complete_family_package(
    sheet: &str,
    sheet_instance: u32,
    family_count: u32,
) -> DeferredFormulaPackage {
    let mut families = Vec::new();
    let mut records = Vec::new();
    let mut coordinates = Vec::new();
    for family_index in 0..family_count {
        let col0 = family_index + 1;
        let source_id = SourceFamilyId {
            sheet_instance,
            source_index: family_index as usize,
        };
        families.push(SourceFormulaFamily {
            source_id,
            source_order: SourceFormulaOrder::new(u64::from(family_index) * 100),
            anchor_coord0: SourceCoord { row: 0, col: col0 },
            anchor_text: Arc::from(format!("$A$1+{}", family_index + 1)),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 99,
                col: col0,
            }),
            member_count: 100,
        });
        for row0 in 0..100 {
            coordinates.push(SourceCoord {
                row: row0,
                col: col0,
            });
            records.push(DeferredReplayFormula {
                source_order: SourceFormulaOrder::new(
                    u64::from(family_index) * 100 + u64::from(row0),
                ),
                row: row0 + 1,
                col: col0 + 1,
                text: format!("=$A$1+{}", family_index + 1),
                family: Some(source_id),
                partition_owner: Some(source_id),
            });
        }
    }
    families.reverse();
    DeferredFormulaPackage::new_with_source_coordinates(
        sheet.to_string(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: u64::from(family_count) * 100,
            families_seen: u64::from(family_count),
            family_cells_seen: u64::from(family_count) * 100,
            source_clean_families: u64::from(family_count),
            source_clean_cells: u64::from(family_count) * 100,
            ..Default::default()
        },
        families,
        Vec::new(),
        coordinates,
        Box::new(FamilyReplay { records }),
    )
}

fn overlapping_families_package(sheet: &str, sheet_instance: u32) -> DeferredFormulaPackage {
    let mut families = Vec::new();
    let mut records = Vec::new();
    for family_index in 0..2u32 {
        let source_id = SourceFamilyId {
            sheet_instance,
            source_index: family_index as usize,
        };
        families.push(SourceFormulaFamily {
            source_id,
            source_order: SourceFormulaOrder::new(u64::from(family_index) * 2),
            anchor_coord0: SourceCoord { row: 0, col: 1 },
            anchor_text: Arc::from(format!("$A$1+{}", family_index + 1)),
            members: SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 1,
                col: 1,
            }),
            member_count: 2,
        });
        for row0 in 0..2 {
            records.push(DeferredReplayFormula {
                source_order: SourceFormulaOrder::new(
                    u64::from(family_index) * 2 + u64::from(row0),
                ),
                row: row0 + 1,
                col: 2,
                text: format!("=$A$1+{}", family_index + 1),
                family: Some(source_id),
                partition_owner: Some(source_id),
            });
        }
    }
    DeferredFormulaPackage::new_with_source_coordinates(
        sheet.to_string(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: 4,
            families_seen: 2,
            family_cells_seen: 4,
            source_clean_families: 2,
            source_clean_cells: 4,
            ..Default::default()
        },
        families,
        Vec::new(),
        vec![
            SourceCoord { row: 0, col: 1 },
            SourceCoord { row: 1, col: 1 },
        ],
        Box::new(FamilyReplay { records }),
    )
}

#[test]
fn authoritative_mode_uses_prepare_all_compatibility_without_partial_c2_ownership() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine.stage_formula_text("Outputs", 1, 1, "=1".into());
    engine.stage_formula_text("Inputs", 2, 2, "=2".into());
    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.outcome, PreparationOutcome::CompatibilityPrepared);
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    assert!(
        report
            .widening_reasons
            .contains(&OpaqueReason::UnsupportedSourceSemantics)
    );
    assert!(!engine.has_staged_formulas());
}

#[test]
fn empty_deferred_package_does_not_force_unrelated_ordinary_target_compatibility() {
    let mut engine = engine(FormulaPlaneMode::Shadow);
    engine.stage_formula_text("Outputs", 1, 1, "=1".into());
    engine
        .source_formula_ingress()
        .stage_deferred(DeferredFormulaPackage::new(
            "Outputs".into(),
            FormulaCompressedSourceReport::default(),
            Vec::new(),
            Vec::new(),
            Box::<EmptyReplay>::default(),
        ));

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.outcome, PreparationOutcome::Prepared);
    assert_eq!(report.widened_scope, PrepareScope::Exact);
    assert_eq!(report.selected_staged_cells, 1);
    assert!(engine.has_staged_formulas());
}

#[test]
fn demanding_one_family_consumes_whole_package_and_retains_unrelated_package() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = engine(mode);
        engine
            .source_formula_ingress()
            .stage_deferred(complete_family_package("Outputs", 800, 2));
        engine
            .source_formula_ingress()
            .stage_deferred(complete_family_package("Middle", 801, 1));

        let first = engine
            .prepare_graph_for_targets(&[cell("Outputs", 1, 2)], Default::default())
            .unwrap();
        assert_eq!(first.outcome, PreparationOutcome::Prepared, "{mode:?}");
        assert_eq!(first.selected_source_families, 2, "{mode:?}");
        assert!(engine.get_staged_formula_text("Outputs", 1, 2).is_none());
        assert!(engine.get_staged_formula_text("Middle", 1, 2).is_some());
        let stats = engine.baseline_stats();
        match mode {
            FormulaPlaneMode::AuthoritativeExperimental => {
                assert_eq!(stats.formula_plane_active_span_count, 2);
                assert_eq!(stats.graph_formula_vertex_count, 0);
            }
            FormulaPlaneMode::Off | FormulaPlaneMode::Shadow => {
                assert_eq!(stats.formula_plane_active_span_count, 0);
                assert_eq!(stats.graph_formula_vertex_count, 200);
            }
        }

        let second = engine
            .prepare_graph_for_targets(&[cell("Middle", 1, 2)], Default::default())
            .unwrap();
        assert_eq!(second.selected_source_families, 1, "{mode:?}");
        assert!(!engine.has_staged_formulas());
    }
}

#[test]
fn fragmented_package_reuses_complete_disposition_with_exact_exception() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = engine(mode);
        engine
            .source_formula_ingress()
            .stage_deferred(fragmented_family_package("Outputs", 825));
        let target_report = engine
            .prepare_graph_for_targets(&[cell("Outputs", 1, 3)], Default::default())
            .unwrap();
        assert_eq!(target_report.selected_source_families, 1, "{mode:?}");
        assert!(!engine.has_staged_formulas());
        let ingest = engine.last_formula_ingest_report().unwrap();
        assert_eq!(ingest.formula_cells_seen, 301, "{mode:?} {ingest:?}");
        assert_eq!(ingest.source_spool_replays, 1, "{mode:?} {ingest:?}");
        let stats = engine.baseline_stats();
        match mode {
            FormulaPlaneMode::AuthoritativeExperimental => {
                assert_eq!(stats.formula_plane_active_span_count, 2);
                assert_eq!(stats.graph_formula_vertex_count, 1);
                assert_eq!(ingest.source_partitioned_families_prepared, 1);
                assert_eq!(ingest.source_partition_fragments_prepared, 2);
                assert_eq!(ingest.source_partition_span_cells_prepared, 300);
                assert_eq!(ingest.graph_formula_cells_materialized, 1);
            }
            FormulaPlaneMode::Off | FormulaPlaneMode::Shadow => {
                assert_eq!(stats.formula_plane_active_span_count, 0);
                assert_eq!(stats.graph_formula_vertex_count, 301);
            }
        }
    }
}

#[test]
fn package_replacement_preserves_ordinary_last_writer_in_both_staging_orders() {
    for mode in [FormulaPlaneMode::Off, FormulaPlaneMode::Shadow] {
        for ordinary_first in [false, true] {
            let mut engine = engine(mode);
            if ordinary_first {
                engine.stage_formula_text("Outputs", 1, 2, "=99".into());
            }
            engine
                .source_formula_ingress()
                .stage_deferred(complete_family_package("Outputs", 850, 2));
            if !ordinary_first {
                engine.stage_formula_text("Outputs", 1, 2, "=99".into());
            }

            let report = engine
                .prepare_graph_for_targets(&[cell("Outputs", 1, 2)], Default::default())
                .unwrap();
            assert_eq!(
                report.selected_source_families, 2,
                "{mode:?} ordinary_first={ordinary_first}"
            );
            assert!(!engine.has_staged_formulas());
            engine.config.defer_graph_building = false;
            assert_eq!(
                engine.evaluate_cell("Outputs", 1, 2).unwrap(),
                Some(LiteralValue::Number(99.0)),
                "{mode:?} ordinary_first={ordinary_first}"
            );
            assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 200);
        }
    }
}

#[test]
fn package_after_ordinary_reconciles_with_one_replay_reused_by_preparation() {
    let replay_count = Arc::new(AtomicUsize::new(0));
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.stage_formula_text("Outputs", 1, 1, "=99".into());
    engine
        .source_formula_ingress()
        .stage_deferred(counting_fallback_package(
            "Outputs",
            "=1",
            Arc::clone(&replay_count),
        ));
    assert_eq!(replay_count.load(Ordering::Acquire), 1);

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.outcome, PreparationOutcome::Prepared);
    assert_eq!(replay_count.load(Ordering::Acquire), 1);
    assert!(!engine.has_staged_formulas());
}

#[test]
fn compatibility_after_package_discovery_replays_each_package_once() {
    let replay_count = Arc::new(AtomicUsize::new(0));
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine
        .source_formula_ingress()
        .stage_deferred(counting_fallback_package(
            "Outputs",
            "=1",
            Arc::clone(&replay_count),
        ));
    engine.stage_formula_text("Inputs", 1, 1, "=2".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.outcome, PreparationOutcome::CompatibilityPrepared);
    assert_eq!(replay_count.load(Ordering::Acquire), 1);
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_spool_replays,
        1
    );
}

#[test]
fn unknown_sheet_package_enters_compatibility_before_any_package_replay() {
    let known_replays = Arc::new(AtomicUsize::new(0));
    let unknown_replays = Arc::new(AtomicUsize::new(0));
    let mut engine = engine(FormulaPlaneMode::Off);
    engine
        .source_formula_ingress()
        .stage_deferred(counting_fallback_package(
            "Outputs",
            "=INDIRECT(\"Inputs!A1\")",
            Arc::clone(&known_replays),
        ));
    engine
        .source_formula_ingress()
        .stage_deferred(counting_fallback_package(
            "Future",
            "=1",
            Arc::clone(&unknown_replays),
        ));

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.outcome, PreparationOutcome::CompatibilityPrepared);
    assert_eq!(known_replays.load(Ordering::Acquire), 1);
    assert_eq!(unknown_replays.load(Ordering::Acquire), 1);
    assert!(engine.sheet_id("Future").is_some());
}

#[test]
fn incomplete_package_geometry_fails_safe_to_workbook_discovery() {
    let mut engine = engine(FormulaPlaneMode::Off);
    let package = DeferredFormulaPackage::new(
        "Outputs".into(),
        FormulaCompressedSourceReport {
            source_formula_records_spooled: 2,
            ..Default::default()
        },
        Vec::new(),
        Vec::new(),
        Box::new(FamilyReplay {
            records: vec![DeferredReplayFormula {
                source_order: SourceFormulaOrder::new(0),
                row: 1,
                col: 1,
                text: "=1".into(),
                family: None,
                partition_owner: None,
            }],
        }),
    );
    engine.source_formula_ingress().stage_deferred(package);
    engine.stage_formula_text("Inputs", 4, 4, "=4".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 10, 10)], Default::default())
        .unwrap();
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    assert!(
        report
            .widening_reasons
            .contains(&OpaqueReason::DeferredSourcePackage)
    );
    assert!(!engine.has_staged_formulas());
}

#[test]
fn package_fallback_parse_policy_matrix_is_transactional() {
    for policy in [
        FormulaParsePolicy::Strict,
        FormulaParsePolicy::KeepCachedValue,
        FormulaParsePolicy::AsText,
        FormulaParsePolicy::CoerceToError,
    ] {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine.config.formula_parse_policy = policy;
        engine
            .source_formula_ingress()
            .stage_deferred(fallback_package("Outputs", &[(1, 1, "=BROKEN(")]));
        let result = engine.prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default());
        if policy == FormulaParsePolicy::Strict {
            assert!(result.is_err());
            assert!(engine.has_staged_formulas());
            assert!(engine.formula_parse_diagnostics().is_empty());
        } else {
            let report = result.unwrap();
            assert_eq!(report.selected_staged_cells, 1, "{policy:?}");
            assert!(!engine.has_staged_formulas());
            assert_eq!(engine.formula_parse_diagnostics().len(), 1, "{policy:?}");
            let expected_vertices = usize::from(policy != FormulaParsePolicy::KeepCachedValue);
            assert_eq!(
                engine.baseline_stats().graph_formula_vertex_count,
                expected_vertices,
                "{policy:?}"
            );
        }
    }
}

#[test]
fn plane_append_failure_materializes_every_direct_coordinate_without_losing_last_writer() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine
        .set_cell_value("Outputs", 1, 1, LiteralValue::Number(40.0))
        .unwrap();
    engine
        .source_formula_ingress()
        .stage_deferred(overlapping_families_package("Outputs", 870));

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 2)], Default::default())
        .unwrap();
    assert_eq!(report.outcome, PreparationOutcome::Prepared);
    assert!(!engine.has_staged_formulas());
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);
    assert!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .fallback_reasons
            .keys()
            .any(|reason| reason.starts_with("TargetFormulaPlaneAppend:"))
    );
    engine.config.defer_graph_building = false;
    assert_eq!(
        engine.evaluate_cell("Outputs", 1, 2).unwrap(),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn authoritative_direct_package_does_not_charge_hypothetical_legacy_materialization() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine
        .source_formula_ingress()
        .stage_deferred(complete_family_package("Outputs", 875, 1));
    let budgets = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(0),
            graph_edge_hard_limit: Some(0),
            materialization_cells: Some(0),
            materialized_graph_bytes: Some(0),
        },
        ..Default::default()
    };
    let report = engine
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 2)],
            PrepareTargetsOptions {
                budgets: Some(&budgets),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(report.selected_source_families, 1);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_vertex_count, 0);
}

#[test]
fn direct_heavy_package_honors_target_work_budget_and_remains_staged() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine
        .source_formula_ingress()
        .stage_deferred(complete_family_package("Outputs", 885, 1));
    let budgets = EvaluationBudgets {
        work: crate::engine::WorkResourceBudget {
            max_work_units: Some(10),
        },
        ..Default::default()
    };
    let error = engine
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 2)],
            PrepareTargetsOptions {
                budgets: Some(&budgets),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::Resource { ref detail }
            if detail.reason == ResourceExhaustionReason::WorkUnits
    ));
    assert!(engine.has_staged_formulas());
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn authoritative_target_preparation_matches_prepare_all_for_values_and_errors() {
    for input in [
        LiteralValue::Number(41.0),
        LiteralValue::Error(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Div,
        )),
    ] {
        let setup = || {
            let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
            engine
                .set_cell_value("Outputs", 1, 1, input.clone())
                .unwrap();
            engine
                .source_formula_ingress()
                .stage_deferred(complete_family_package("Outputs", 890, 1));
            engine
        };
        let mut target = setup();
        let mut oracle = setup();
        target
            .prepare_graph_for_targets(&[cell("Outputs", 1, 2)], Default::default())
            .unwrap();
        oracle.build_graph_all().unwrap();
        target.config.defer_graph_building = false;
        oracle.config.defer_graph_building = false;
        assert_eq!(
            target.evaluate_cell("Outputs", 1, 2).unwrap(),
            oracle.evaluate_cell("Outputs", 1, 2).unwrap(),
            "{input:?}"
        );
    }
}

#[test]
fn selected_package_survives_every_target_precommit_fault_without_publication() {
    let seams = [
        TargetPreparationFault::AfterDiscovery,
        TargetPreparationFault::FinalRevisionValidation,
        TargetPreparationFault::FinalGraphValidation,
        TargetPreparationFault::Admission,
        TargetPreparationFault::Reservation,
        TargetPreparationFault::BeforeFirstMutation,
    ];
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for seam in seams {
            let mut engine = engine(mode);
            engine
                .source_formula_ingress()
                .stage_deferred(complete_family_package("Outputs", 900, 1));
            let before = engine.baseline_stats();
            let index_revision = engine.staged_formula_index_revision_for_test();
            engine.set_target_preparation_fault_for_test(seam);
            assert!(
                engine
                    .prepare_graph_for_targets(&[cell("Outputs", 1, 2)], Default::default())
                    .is_err(),
                "{mode:?} {seam:?}"
            );
            let after = engine.baseline_stats();
            assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
            assert_eq!(after.graph_edge_count, before.graph_edge_count);
            assert_eq!(
                after.graph_formula_vertex_count,
                before.graph_formula_vertex_count
            );
            assert_eq!(
                after.formula_plane_active_span_count,
                before.formula_plane_active_span_count
            );
            assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);
            assert_eq!(
                engine.staged_formula_index_revision_for_test(),
                index_revision,
                "{mode:?} {seam:?}"
            );
            assert!(engine.get_staged_formula_text("Outputs", 1, 2).is_some());
            assert!(engine.last_formula_ingest_report().is_none());
            assert!(engine.formula_parse_diagnostics().is_empty());
            assert!(engine.staged_formula_index_is_consistent_for_test());
        }
    }
}

#[test]
fn every_staged_storage_mutation_moves_index_revision_and_keeps_exact_parity() {
    let mut engine = engine(FormulaPlaneMode::Off);
    let mut revision = engine.staged_formula_index_revision_for_test();

    engine.stage_formula_text("Inputs", 1, 1, "=1".into());
    assert!(engine.staged_formula_index_revision_for_test() > revision);
    assert!(engine.staged_formula_index_is_consistent_for_test());
    revision = engine.staged_formula_index_revision_for_test();

    engine.stage_formula_text("Inputs", 1, 1, "=2".into());
    assert!(engine.staged_formula_index_revision_for_test() > revision);
    assert!(engine.staged_formula_index_is_consistent_for_test());
    revision = engine.staged_formula_index_revision_for_test();

    engine.stage_formula_text("Inputs", 2, 1, "=3".into());
    assert!(engine.staged_formula_index_revision_for_test() > revision);
    revision = engine.staged_formula_index_revision_for_test();

    assert_eq!(
        engine.clear_staged_formula_text("Inputs", 1, 1).as_deref(),
        Some("=2")
    );
    assert!(engine.staged_formula_index_revision_for_test() > revision);
    assert!(engine.staged_formula_index_is_consistent_for_test());
    revision = engine.staged_formula_index_revision_for_test();

    engine.rename_staged_formula_sheet("Inputs", "Renamed");
    assert!(engine.staged_formula_index_revision_for_test() > revision);
    assert!(engine.staged_formula_index_is_consistent_for_test());
    revision = engine.staged_formula_index_revision_for_test();

    engine.clear_staged_formulas_for_sheet("Renamed");
    assert!(engine.staged_formula_index_revision_for_test() > revision);
    assert!(engine.staged_formula_index_is_consistent_for_test());
}

#[test]
fn provider_revision_change_at_final_validation_is_stale_and_atomic() {
    use std::sync::atomic::Ordering;

    let workbook = TestWorkbook::default();
    let revision = workbook.planning_revision_handle();
    let config = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(workbook, config);
    engine.add_sheet("Outputs").unwrap();
    engine.stage_formula_text("Outputs", 1, 1, "=1".into());
    let before = engine.baseline_stats();
    let bump = revision.clone();
    engine.set_before_target_preparation_commit_hook(move || {
        bump.fetch_add(1, Ordering::AcqRel);
    });

    let error = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::PreparationStale {
            reason: formualizer_common::PreparationStaleReason::Provider
        }
    ));
    assert_eq!(
        engine.baseline_stats().graph_vertex_count,
        before.graph_vertex_count
    );
    assert_eq!(engine.staged_formula_count(), 1);
    assert!(engine.staged_formula_index_is_consistent_for_test());
}

#[test]
fn cancellation_and_all_injected_precommit_seams_preserve_semantic_state() {
    let seams = [
        TargetPreparationFault::AfterDiscovery,
        TargetPreparationFault::FinalRevisionValidation,
        TargetPreparationFault::FinalGraphValidation,
        TargetPreparationFault::Admission,
        TargetPreparationFault::Reservation,
        TargetPreparationFault::BeforeFirstMutation,
    ];
    for seam in seams {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine
            .set_cell_value("Inputs", 3, 3, LiteralValue::Number(7.0))
            .unwrap();
        engine.stage_formula_text("Outputs", 1, 1, "=Inputs!A1+1".into());
        engine.stage_formula_text("Inputs", 1, 1, "=1".into());
        let before = engine.baseline_stats();
        let before_revision = engine.staged_formula_index_revision_for_test();
        let report_before = engine.last_formula_ingest_report().cloned();
        let total_before = engine.formula_ingest_report_total().clone();
        let recalc_before = engine.recalc_epoch;
        let value_before = engine.get_cell_value("Inputs", 3, 3);
        engine.set_target_preparation_fault_for_test(seam);
        assert!(
            engine
                .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
                .is_err()
        );
        let after = engine.baseline_stats();
        assert_eq!(
            after.graph_vertex_count, before.graph_vertex_count,
            "{seam:?}"
        );
        assert_eq!(after.graph_edge_count, before.graph_edge_count, "{seam:?}");
        assert_eq!(after.graph_formula_vertex_count, 0, "{seam:?}");
        assert_eq!(engine.staged_formula_count(), 2, "{seam:?}");
        assert_eq!(
            engine.staged_formula_index_revision_for_test(),
            before_revision,
            "{seam:?}"
        );
        assert!(engine.formula_parse_diagnostics().is_empty(), "{seam:?}");
        assert_eq!(engine.last_formula_ingest_report(), report_before.as_ref());
        assert_eq!(engine.formula_ingest_report_total(), &total_before);
        assert_eq!(engine.recalc_epoch, recalc_before);
        assert_eq!(engine.get_cell_value("Inputs", 3, 3), value_before);
        assert!(engine.staged_formula_index_is_consistent_for_test());
    }

    let mut cancelled = engine(FormulaPlaneMode::Off);
    cancelled.stage_formula_text("Outputs", 1, 1, "=1".into());
    let flag = AtomicBool::new(true);
    let error = cancelled
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 1)],
            PrepareTargetsOptions {
                cancel: Some(&flag),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Cancelled);
    assert_eq!(cancelled.staged_formula_count(), 1);

    let mut expired = engine(FormulaPlaneMode::Off);
    expired.stage_formula_text("Outputs", 1, 1, "=1".into());
    let error = expired
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 1)],
            PrepareTargetsOptions {
                deadline: Some(std::time::Instant::now()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(matches!(error.extra, ExcelErrorExtra::Resource { .. }));
    assert_eq!(expired.staged_formula_count(), 1);
}

#[test]
fn target_admission_boundaries_accept_cap_and_reject_cap_plus_one() {
    let prepare = |formula_count: u32, budgets: EvaluationBudgets| {
        let mut engine = engine(FormulaPlaneMode::Off);
        for col in 1..=formula_count {
            engine.stage_formula_text("Outputs", 1, col, "=1".into());
        }
        let target =
            EvaluationTarget::Range(RangeAddress::new("Outputs", 1, 1, 1, formula_count).unwrap());
        let result = engine.prepare_graph_for_targets(
            &[target],
            PrepareTargetsOptions {
                budgets: Some(&budgets),
                ..Default::default()
            },
        );
        (engine, result)
    };

    let vertex_cap = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(1),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    assert!(prepare(1, vertex_cap.clone()).1.is_ok());
    let (over_vertex, error) = prepare(2, vertex_cap);
    assert!(error.is_err());
    assert_eq!(over_vertex.staged_formula_count(), 2);

    let cell_cap = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            materialization_cells: Some(1),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    assert!(prepare(1, cell_cap.clone()).1.is_ok());
    assert!(prepare(2, cell_cap).1.is_err());

    let byte_cap = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            materialized_graph_bytes: Some(64),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    assert!(prepare(1, byte_cap.clone()).1.is_ok());
    assert!(prepare(2, byte_cap).1.is_err());

    let mut edge_engine = engine(FormulaPlaneMode::Off);
    edge_engine.stage_formula_text("Outputs", 1, 1, "=Inputs!A1".into());
    let edge_cap = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_edge_hard_limit: Some(1),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    assert!(
        edge_engine
            .prepare_graph_for_targets(
                &[cell("Outputs", 1, 1)],
                PrepareTargetsOptions {
                    budgets: Some(&edge_cap),
                    ..Default::default()
                },
            )
            .is_ok()
    );

    let mut over_edge = engine(FormulaPlaneMode::Off);
    over_edge.stage_formula_text("Outputs", 1, 1, "=Inputs!A1".into());
    let edge_zero = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_edge_hard_limit: Some(0),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    let error = over_edge
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 1)],
            PrepareTargetsOptions {
                budgets: Some(&edge_zero),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::Resource { ref detail }
            if detail.reason == ResourceExhaustionReason::GraphEdges
    ));
    assert_eq!(over_edge.staged_formula_count(), 1);
}

#[test]
fn sheets_widening_restarts_and_unions_only_proven_local_sheets() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.stage_formula_text("Outputs", 1, 2, "=OFFSET(A1,0,0)".into());
    engine.stage_formula_text("Outputs", 8, 8, "=8".into());
    engine.stage_formula_text("Inputs", 9, 9, "=9".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 2)], Default::default())
        .unwrap();
    assert_eq!(
        report.widened_scope,
        PrepareScope::Sheets(vec!["Outputs".to_string()])
    );
    assert_eq!(report.selected_staged_cells, 2);
    assert_eq!(engine.staged_formula_count(), 1);
    assert!(engine.get_staged_formula_text("Inputs", 9, 9).is_some());
}

#[test]
fn graph_source_scratch_zero_cap_and_success_release_to_checkpoint() {
    let run = |limit| {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine.stage_formula_text("Outputs", 1, 1, "=Inputs!A1+1".into());
        let budgets = EvaluationBudgets {
            scratch: crate::engine::ScratchResourceBudget {
                graph_source_bytes: limit,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = engine.prepare_graph_for_targets(
            &[cell("Outputs", 1, 1)],
            PrepareTargetsOptions {
                budgets: Some(&budgets),
                ..Default::default()
            },
        );
        let ledger = engine
            .last_evaluation_resource_request_stats()
            .unwrap()
            .ledger;
        (engine, result, ledger)
    };

    let (zero, result, ledger) = run(Some(0));
    let error = result.unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::Resource { ref detail }
            if detail.reason == ResourceExhaustionReason::ScratchMemory
    ));
    assert_eq!(ledger.scratch_current, 0);
    assert_eq!(zero.staged_formula_count(), 1);

    let (_, unlimited, ledger) = run(None);
    let report = unlimited.unwrap();
    assert_eq!(ledger.scratch_current, 0);
    assert!(report.observed_scratch_bytes > 0);
    let cap = report.observed_scratch_bytes;
    assert!(run(Some(cap)).1.is_ok());
    let (below, result, ledger) = run(Some(cap - 1));
    assert!(result.is_err());
    assert_eq!(ledger.scratch_current, 0);
    assert_eq!(below.staged_formula_count(), 1);
}

#[test]
fn staging_unknown_sheet_is_name_only_until_build_and_failed_restore_is_exact() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            defer_graph_building: true,
            formula_parse_policy: crate::engine::FormulaParsePolicy::Strict,
            ..Default::default()
        },
    );
    engine.stage_formula_text("Future", 2, 2, "=BROKEN(".into());
    assert!(engine.sheet_id("Future").is_none());
    let revision = engine.staged_formula_index_revision_for_test();
    assert!(engine.build_graph_all().is_err());
    assert_eq!(engine.staged_formula_index_revision_for_test(), revision);
    assert_eq!(
        engine.get_staged_formula_text("Future", 2, 2).as_deref(),
        Some("=BROKEN(")
    );
    assert!(engine.staged_formula_index_is_consistent_for_test());
}

#[test]
fn target_preparation_replaces_existing_formula_in_the_prepared_transaction() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.config.defer_graph_building = false;
    engine
        .set_cell_formula(
            "Outputs",
            1,
            1,
            formualizer_parse::parser::parse("=1").unwrap(),
        )
        .unwrap();
    engine.config.defer_graph_building = true;
    engine.stage_formula_text("Outputs", 1, 1, "=2".into());
    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.selected_staged_cells, 1);
    engine.config.defer_graph_building = false;
    assert_eq!(
        engine.evaluate_cell("Outputs", 1, 1).unwrap(),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn common_admission_direct_bulk_replacement_and_staged_seams_are_atomic() {
    let zero_vertices = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(0),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut direct = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_evaluation_budgets(zero_vertices.clone()),
    );
    direct.add_sheet("Sheet1").unwrap();
    let before = direct.baseline_stats();
    let error = direct
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap_err();
    assert!(matches!(error.extra, ExcelErrorExtra::Resource { .. }));
    assert_eq!(
        direct.baseline_stats().graph_vertex_count,
        before.graph_vertex_count
    );

    let mut bulk = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_evaluation_budgets(zero_vertices.clone()),
    );
    bulk.add_sheet("Sheet1").unwrap();
    let ast = bulk.intern_formula_ast(&formualizer_parse::parser::parse("=1").unwrap());
    let before = bulk.baseline_stats();
    let mut builder = bulk.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formula_ids(sheet, [(1, 1, ast)]);
    assert!(builder.finish().is_err());
    assert_eq!(
        bulk.baseline_stats().graph_vertex_count,
        before.graph_vertex_count
    );

    let mut replacement = Engine::new(TestWorkbook::new(), EvalConfig::default());
    replacement.add_sheet("Sheet1").unwrap();
    replacement
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            formualizer_parse::parser::parse("=1").unwrap(),
        )
        .unwrap();
    replacement.set_evaluation_budgets_for_test(EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_edge_hard_limit: Some(0),
            ..Default::default()
        },
        ..Default::default()
    });
    let before = replacement.baseline_stats();
    assert!(
        replacement
            .set_cell_formula(
                "Sheet1",
                1,
                1,
                formualizer_parse::parser::parse("=A2").unwrap(),
            )
            .is_err()
    );
    let after = replacement.baseline_stats();
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );

    let mut staged = engine(FormulaPlaneMode::Off);
    staged.set_evaluation_budgets_for_test(zero_vertices);
    staged.stage_formula_text("Outputs", 1, 1, "=1".into());
    let before = staged.baseline_stats();
    assert!(staged.build_graph_all().is_err());
    assert_eq!(
        staged.baseline_stats().graph_vertex_count,
        before.graph_vertex_count
    );
    assert_eq!(staged.staged_formula_count(), 1);
}

#[test]
fn c2_admission_caps_are_typed_and_atomic_for_target_preparation() {
    for mode in [FormulaPlaneMode::Off, FormulaPlaneMode::Shadow] {
        let mut engine = engine(mode);
        engine.stage_formula_text("Outputs", 1, 1, "=Inputs!A1+1".into());
        let budgets = EvaluationBudgets {
            admission: AdmissionResourceBudget {
                graph_vertex_hard_limit: Some(0),
                graph_edge_hard_limit: Some(0),
                materialization_cells: Some(0),
                materialized_graph_bytes: Some(0),
            },
            ..EvaluationBudgets::default()
        };
        let error = engine
            .prepare_graph_for_targets(
                &[cell("Outputs", 1, 1)],
                PrepareTargetsOptions {
                    budgets: Some(&budgets),
                    ..Default::default()
                },
            )
            .unwrap_err();
        match error.extra {
            ExcelErrorExtra::Resource { detail } => {
                assert_eq!(detail.reason, ResourceExhaustionReason::GraphVertices)
            }
            other => panic!("expected typed resource detail, got {other:?}"),
        }
        assert_eq!(engine.staged_formula_count(), 1);
        assert_eq!(engine.baseline_stats().graph_vertex_count, 0);
    }
}

#[test]
fn cross_sheet_offset_and_index_never_claim_sheet_local_dynamic_scope() {
    for formula in [
        "=OFFSET(Inputs!A1,0,Inputs!B1)",
        "=INDEX(Inputs!A1:C1,1,Inputs!B1)",
    ] {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine
            .set_cell_value("Inputs", 1, 2, LiteralValue::Number(2.0))
            .unwrap();
        engine.stage_formula_text("Inputs", 20, 20, "=42".into());
        engine.stage_formula_text("Outputs", 1, 1, formula.into());

        let report = engine
            .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
            .unwrap();
        assert_eq!(report.widened_scope, PrepareScope::Workbook, "{formula}");
        assert_eq!(report.selected_staged_cells, 2, "{formula}");
    }
}

#[test]
fn cross_sheet_offset_runtime_selected_staged_cell_matches_prepare_all_value() {
    let setup = || {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine
            .set_cell_value("Inputs", 1, 1, LiteralValue::Number(0.0))
            .unwrap();
        engine
            .set_cell_value("Inputs", 1, 2, LiteralValue::Number(2.0))
            .unwrap();
        engine.stage_formula_text("Inputs", 1, 3, "=42".into());
        engine.stage_formula_text("Outputs", 1, 1, "=OFFSET(Inputs!A1,0,Inputs!B1)".into());
        engine
    };
    let mut target = setup();
    let mut oracle = setup();
    let report = target
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    oracle.build_graph_all().unwrap();
    target.config.defer_graph_building = false;
    oracle.config.defer_graph_building = false;
    assert_eq!(
        target.evaluate_cell("Outputs", 1, 1).unwrap(),
        oracle.evaluate_cell("Outputs", 1, 1).unwrap()
    );
    assert_eq!(
        target.evaluate_cell("Outputs", 1, 1).unwrap(),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn committed_dynamic_vertex_widens_before_completeness_is_claimed() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Inputs", 1, 2, LiteralValue::Number(2.0))
        .unwrap();
    engine.stage_formula_text("Inputs", 1, 3, "=42".into());
    engine.stage_formula_text("Outputs", 1, 1, "=OFFSET(Inputs!A1,0,Inputs!B1)".into());
    engine.build_graph_for_sheets(["Outputs"]).unwrap();
    assert_eq!(engine.staged_formula_count(), 1);

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    assert_eq!(report.selected_staged_cells, 1);
    assert_eq!(engine.staged_formula_count(), 0);
    engine.config.defer_graph_building = false;
    assert_eq!(
        engine.evaluate_cell("Outputs", 1, 1).unwrap(),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn lazy_partial_sheet_index_is_rebuilt_for_target_discovery() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.set_sheet_index_mode(crate::engine::SheetIndexMode::Lazy);
    engine.stage_formula_text("Outputs", 1, 1, "=Inputs!A1".into());
    engine.build_graph_for_sheets(["Outputs"]).unwrap();
    engine
        .set_cell_value("Outputs", 20, 20, LiteralValue::Number(1.0))
        .unwrap();
    engine.stage_formula_text("Inputs", 1, 1, "=7".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap();
    assert_eq!(report.selected_staged_cells, 1);
    assert_eq!(engine.staged_formula_count(), 0);
}

#[test]
fn widened_staged_lease_scan_charges_and_checkpoints_each_lease() {
    let mut engine = engine(FormulaPlaneMode::Off);
    for col in 1..=100 {
        engine.stage_formula_text("Outputs", 2, col, "=1".into());
    }
    engine.stage_formula_text("Outputs", 1, 1, "=OFFSET(A1,0,0)".into());
    let budgets = EvaluationBudgets {
        work: crate::engine::WorkResourceBudget {
            max_work_units: Some(8),
        },
        ..Default::default()
    };
    let error = engine
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 1)],
            PrepareTargetsOptions {
                budgets: Some(&budgets),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::Resource { ref detail }
            if detail.reason == ResourceExhaustionReason::WorkUnits
    ));
    assert_eq!(engine.staged_formula_count(), 101);
}

#[test]
fn indexed_spill_region_query_finds_anchor_without_scanning_unrelated_spills() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.config.defer_graph_building = false;
    engine
        .set_cell_formula(
            "Outputs",
            1,
            1,
            formualizer_parse::parser::parse("=SEQUENCE(1,200)+Inputs!Z1").unwrap(),
        )
        .unwrap();
    engine.evaluate_cell("Outputs", 1, 1).unwrap();
    engine
        .set_cell_formula(
            "Middle",
            50,
            1,
            formualizer_parse::parser::parse("=SEQUENCE(100,20)").unwrap(),
        )
        .unwrap();
    engine.evaluate_cell("Middle", 50, 1).unwrap();
    engine.config.defer_graph_building = true;
    engine.stage_formula_text("Inputs", 1, 26, "=5".into());

    let report = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 150)], Default::default())
        .unwrap();
    assert_eq!(report.selected_staged_cells, 1);
    assert_eq!(engine.staged_formula_count(), 0);
}

#[test]
fn staged_replacement_rejects_active_spill_anchor_and_child_without_mutation() {
    for col in [1, 2] {
        let mut engine = engine(FormulaPlaneMode::Off);
        engine.config.defer_graph_building = false;
        engine
            .set_cell_formula(
                "Outputs",
                1,
                1,
                formualizer_parse::parser::parse("=SEQUENCE(1,2)").unwrap(),
            )
            .unwrap();
        engine.evaluate_cell("Outputs", 1, 1).unwrap();
        let counts = engine.graph.spill_registry_counts();
        let before = engine.baseline_stats();
        engine.config.defer_graph_building = true;
        engine.stage_formula_text("Outputs", 1, col, "=99".into());

        let error = engine
            .prepare_graph_for_targets(&[cell("Outputs", 1, col)], Default::default())
            .unwrap_err();
        assert!(
            error
                .message
                .as_deref()
                .is_some_and(|message| message.contains("active spill"))
        );
        assert_eq!(engine.graph.spill_registry_counts(), counts);
        assert_eq!(
            engine.baseline_stats().graph_vertex_count,
            before.graph_vertex_count
        );
        assert_eq!(engine.staged_formula_count(), 1);
    }
}

#[test]
fn replay_admission_deduplicates_vertex_and_cell_event_keys() {
    use crate::engine::ChangeEvent;
    let mut engine = engine(FormulaPlaneMode::Off);
    let sheet_id = engine.sheet_id("Outputs").unwrap();
    let ast = formualizer_parse::parser::parse("=1").unwrap();
    engine.set_evaluation_budgets_for_test(EvaluationBudgets {
        admission: AdmissionResourceBudget {
            materialization_cells: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let addr = CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true));
    let events = vec![
        ChangeEvent::AddVertex {
            id: crate::engine::VertexId::new(10_000),
            coord: formualizer_common::Coord::new(0, 0),
            sheet_id,
            value: None,
            formula: Some(ast.clone()),
            kind: Some(crate::engine::VertexKind::FormulaScalar),
            flags: None,
        },
        ChangeEvent::SetFormula {
            addr,
            old_value: None,
            old_formula: None,
            new: ast,
        },
    ];
    assert!(engine.preflight_replay_admission(&events, true).is_ok());
}

#[test]
fn mid_discovery_provider_mismatch_returns_typed_stale() {
    use std::sync::atomic::Ordering;
    let workbook = TestWorkbook::default();
    let revision = workbook.planning_revision_handle();
    let mut engine = Engine::new(
        workbook,
        EvalConfig {
            defer_graph_building: true,
            ..Default::default()
        },
    );
    engine.add_sheet("Outputs").unwrap();
    engine.stage_formula_text("Outputs", 1, 1, "=SUM(1,2)".into());
    let bump = revision.clone();
    engine.set_before_target_planning_snapshot_hook_for_test(move || {
        bump.fetch_add(1, Ordering::AcqRel);
    });
    let error = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::PreparationStale {
            reason: formualizer_common::PreparationStaleReason::Provider
        }
    ));
    assert_eq!(engine.staged_formula_count(), 1);
}

#[test]
fn package_fallback_checks_each_planning_snapshot_against_request_assumptions() {
    let workbook = TestWorkbook::default();
    let revision = workbook.planning_revision_handle();
    let mut engine = Engine::new(
        workbook,
        EvalConfig {
            defer_graph_building: true,
            ..Default::default()
        },
    );
    engine.add_sheet("Outputs").unwrap();
    engine
        .source_formula_ingress()
        .stage_deferred(fallback_package("Outputs", &[(1, 1, "=SUM(1,2)")]));
    let bump = revision.clone();
    engine.set_before_target_planning_snapshot_hook_for_test(move || {
        bump.fetch_add(1, Ordering::AcqRel);
    });
    let error = engine
        .prepare_graph_for_targets(&[cell("Outputs", 1, 1)], Default::default())
        .unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::PreparationStale {
            reason: formualizer_common::PreparationStaleReason::Provider
        }
    ));
    assert!(engine.has_staged_formulas());
}

#[test]
fn request_budget_override_restores_engine_and_graph_admission() {
    let mut engine = engine(FormulaPlaneMode::Off);
    let zero = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(0),
            ..Default::default()
        },
        ..Default::default()
    };
    engine.set_evaluation_budgets_for_test(zero);
    engine.stage_formula_text("Outputs", 1, 1, "=1".into());
    let one = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(1),
            ..Default::default()
        },
        ..Default::default()
    };
    engine
        .prepare_graph_for_targets(
            &[cell("Outputs", 1, 1)],
            PrepareTargetsOptions {
                budgets: Some(&one),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        engine
            .set_cell_value("Inputs", 10, 10, LiteralValue::Number(1.0))
            .is_err()
    );
}

#[test]
fn evaluation_spill_placeholder_creation_obeys_common_admission_atomically() {
    let mut engine = engine(FormulaPlaneMode::Off);
    engine.config.defer_graph_building = false;
    engine
        .set_cell_formula(
            "Outputs",
            1,
            1,
            formualizer_parse::parser::parse("=SEQUENCE(1,3)").unwrap(),
        )
        .unwrap();
    engine.set_evaluation_budgets_for_test(EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(2),
            materialization_cells: Some(3),
            ..Default::default()
        },
        ..Default::default()
    });
    let before = engine.baseline_stats();
    let error = engine.evaluate_cell("Outputs", 1, 1).unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::Resource { ref detail }
            if detail.reason == ResourceExhaustionReason::GraphVertices
    ));
    assert_eq!(
        engine.baseline_stats().graph_vertex_count,
        before.graph_vertex_count
    );
    assert_eq!(engine.graph.spill_registry_counts(), (0, 0));
}
