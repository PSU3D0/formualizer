use std::sync::atomic::AtomicBool;

use formualizer_common::{ExcelErrorExtra, LiteralValue, RangeAddress, ResourceExhaustionReason};

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::target_preparation::TargetPreparationFault;
use crate::engine::{
    AdmissionResourceBudget, DeferredFormulaPackage, DeferredFormulaReplay, DeferredReplayFormula,
    Engine, EvalConfig, EvaluationBudgets, EvaluationTarget, FormulaCompressedSourceReport,
    FormulaPlaneMode, OpaqueReason, PreparationOutcome, PrepareScope, PrepareTargetsOptions,
    TableSelection,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;

fn engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
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
fn deferred_package_selects_whole_workbook_compatibility_without_splitting() {
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
    assert_eq!(report.outcome, PreparationOutcome::CompatibilityPrepared);
    assert_eq!(report.widened_scope, PrepareScope::Workbook);
    assert!(
        report
            .widening_reasons
            .contains(&OpaqueReason::DeferredSourcePackage)
    );
    assert!(!engine.has_staged_formulas());
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
