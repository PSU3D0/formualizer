use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{Engine, EvalConfig, EvaluationTarget};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue, PlanStaleReason};
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

fn make_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

#[test]
fn recalc_plan_matches_evaluate_all() -> Result<(), ExcelError> {
    let mut engine = make_engine();

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))?;
    engine.set_cell_value("Sheet1", 1, 2, LiteralValue::Int(5))?;

    let sum_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        create_cell_ref_ast(None, 1, 2),
        "+",
    );
    engine.set_cell_formula("Sheet1", 2, 1, sum_ast)?;

    let double_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 2, 1),
        create_cell_ref_ast(None, 2, 1),
        "+",
    );
    engine.set_cell_formula("Sheet1", 3, 1, double_ast)?;

    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan()?;

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))?;
    let plan_result = engine.evaluate_recalc_plan(&plan)?;

    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(50.0))
    );
    assert_eq!(plan_result.computed_vertices, 2);

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(30))?;
    let all_result = engine.evaluate_all()?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(70.0))
    );
    assert_eq!(all_result.computed_vertices, 2);

    Ok(())
}

#[test]
fn target_plan_reuses_recipe_across_value_edits_and_leaves_unrelated_dirty()
-> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))?;
    engine.set_cell_value("Sheet1", 1, 2, LiteralValue::Int(10))?;
    engine.set_cell_formula(
        "Sheet1",
        2,
        1,
        create_binary_op_ast(
            create_cell_ref_ast(None, 1, 1),
            create_cell_ref_ast(None, 1, 1),
            "+",
        ),
    )?;
    engine.set_cell_formula(
        "Sheet1",
        2,
        2,
        create_binary_op_ast(
            create_cell_ref_ast(None, 1, 2),
            create_cell_ref_ast(None, 1, 2),
            "+",
        ),
    )?;
    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan_for_targets(&[EvaluationTarget::Cell {
        sheet: "Sheet1".to_string(),
        row: 2,
        col: 1,
    }])?;

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(3))?;
    engine.set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))?;
    engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(20.0))
    );

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(4))?;
    engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(8.0))
    );
    engine.evaluate_all()?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(40.0))
    );
    Ok(())
}

#[test]
fn plan_rejects_cross_engine_and_graph_changes_with_typed_reasons() -> Result<(), ExcelError> {
    let mut first = make_engine();
    first.set_cell_formula("Sheet1", 1, 1, create_cell_ref_ast(None, 1, 2))?;
    let plan = first.build_recalc_plan()?;
    let mut second = make_engine();
    let error = second.evaluate_recalc_plan(&plan).unwrap_err();
    assert!(matches!(
        error.extra,
        formualizer_common::ExcelErrorExtra::PlanStale {
            reason: formualizer_common::PlanStaleReason::Engine
        }
    ));

    first.set_cell_formula("Sheet1", 1, 1, create_cell_ref_ast(None, 1, 3))?;
    let error = first.evaluate_recalc_plan(&plan).unwrap_err();
    assert!(matches!(
        error.extra,
        formualizer_common::ExcelErrorExtra::PlanStale {
            reason: formualizer_common::PlanStaleReason::Graph
        }
    ));
    Ok(())
}

#[test]
fn recalc_plan_no_dirty_is_noop() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))?;
    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan()?;
    let result = engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(result.computed_vertices, 0);
    assert_eq!(result.cycle_errors, 0);
    Ok(())
}

#[test]
fn plan_stale_reason_matrix_and_precedence_are_deterministic() -> Result<(), ExcelError> {
    let reasons = [
        PlanStaleReason::Provider,
        PlanStaleReason::Semantic,
        PlanStaleReason::Budget,
        PlanStaleReason::Staged,
        PlanStaleReason::Symbols,
        PlanStaleReason::Authority,
        PlanStaleReason::SpanGeneration,
    ];
    for reason in reasons {
        let mut engine = make_engine();
        let mut plan = engine.build_recalc_plan()?;
        plan.force_stale_reasons_for_test(&[reason]);
        let error = engine.evaluate_recalc_plan(&plan).unwrap_err();
        assert!(matches!(
            error.extra,
            formualizer_common::ExcelErrorExtra::PlanStale { reason: actual }
                if actual == reason
        ));
    }

    let mut engine = make_engine();
    let mut plan = engine.build_recalc_plan()?;
    plan.force_stale_reasons_for_test(&reasons.iter().copied().rev().collect::<Vec<_>>());
    let error = engine.evaluate_recalc_plan(&plan).unwrap_err();
    assert!(matches!(
        error.extra,
        formualizer_common::ExcelErrorExtra::PlanStale {
            reason: PlanStaleReason::Provider
        }
    ));
    Ok(())
}

#[test]
fn source_versions_do_not_stale_plans_but_source_bindings_do() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.define_source_scalar("Scalar", Some(1))?;
    engine.define_source_table("Table", Some(1))?;
    let plan = engine.build_recalc_plan()?;

    engine.set_source_scalar_version("Scalar", Some(2))?;
    engine.set_source_table_version("Table", Some(2))?;
    engine.invalidate_source("Scalar")?;
    engine.evaluate_recalc_plan(&plan)?;

    engine.define_source_scalar("Other", Some(1))?;
    let error = engine.evaluate_recalc_plan(&plan).unwrap_err();
    assert!(matches!(
        error.extra,
        formualizer_common::ExcelErrorExtra::PlanStale {
            reason: PlanStaleReason::Symbols
        }
    ));
    Ok(())
}

#[test]
fn target_plan_matches_ordinary_target_values_errors_volatile_and_dirty_ownership()
-> Result<(), ExcelError> {
    fn setup(engine: &mut Engine<TestWorkbook>) -> Result<(), ExcelError> {
        engine.set_workbook_seed(77);
        engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(4))?;
        engine.set_cell_formula("Sheet1", 2, 1, parse("=A1*2").unwrap())?;
        engine.set_cell_formula("Sheet1", 2, 2, parse("=1/0").unwrap())?;
        engine.set_cell_formula("Sheet1", 2, 3, parse("=RAND()").unwrap())?;
        engine.set_cell_formula("Sheet1", 2, 4, parse("=A1+100").unwrap())?;
        Ok(())
    }
    let targets = vec![EvaluationTarget::Range(
        formualizer_common::RangeAddress::new("Sheet1", 2, 1, 2, 3).unwrap(),
    )];
    let mut ordinary = make_engine();
    let mut cells = make_engine();
    let mut retained = make_engine();
    setup(&mut ordinary)?;
    setup(&mut cells)?;
    setup(&mut retained)?;
    let plan = retained.build_recalc_plan_for_targets(&targets)?;

    let ordinary_result = ordinary.evaluate_targets(&targets)?;
    let cell_values =
        cells.evaluate_cells(&[("Sheet1", 2, 1), ("Sheet1", 2, 2), ("Sheet1", 2, 3)])?;
    let retained_result = retained.evaluate_recalc_plan(&plan)?;
    for col in 1..=4 {
        assert_eq!(
            retained.get_cell_value("Sheet1", 2, col),
            ordinary.get_cell_value("Sheet1", 2, col)
        );
        assert_eq!(
            retained.get_cell_value("Sheet1", 2, col),
            cells.get_cell_value("Sheet1", 2, col)
        );
    }
    assert_eq!(
        cell_values,
        (1..=3)
            .map(|col| retained.get_cell_value("Sheet1", 2, col))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        retained_result.computed_vertices,
        ordinary_result.computed_vertices
    );
    assert_eq!(retained_result.cycle_errors, ordinary_result.cycle_errors);
    assert_eq!(
        retained.baseline_stats().dirty_vertex_count,
        ordinary.baseline_stats().dirty_vertex_count
    );
    assert_eq!(
        retained.baseline_stats().dirty_vertex_count,
        cells.baseline_stats().dirty_vertex_count
    );
    assert_eq!(
        retained.baseline_stats().evaluation_vertex_count,
        ordinary.baseline_stats().evaluation_vertex_count
    );
    assert_eq!(
        retained.baseline_stats().evaluation_vertex_count,
        cells.baseline_stats().evaluation_vertex_count
    );
    Ok(())
}

#[test]
fn retained_target_plan_tracks_spill_growth_shrink_and_value_only_to_child()
-> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
    engine.set_cell_formula("Sheet1", 1, 2, parse("=SEQUENCE(A1)").unwrap())?;
    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan_for_targets(&[EvaluationTarget::Range(
        formualizer_common::RangeAddress::new("Sheet1", 1, 2, 3, 2).unwrap(),
    )])?;
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), None);

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(3))?;
    engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(3.0))
    );

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
    engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), None);
    assert_eq!(engine.get_cell_value("Sheet1", 3, 2), None);
    Ok(())
}

#[test]
fn cancelled_or_expired_plan_acknowledges_nothing_and_retry_converges() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))?;
    engine.set_cell_formula("Sheet1", 2, 1, parse("=A1*3").unwrap())?;
    let plan = engine.build_recalc_plan()?;
    let dirty_before = engine.baseline_stats().dirty_vertex_count;

    let error = engine
        .evaluate_recalc_plan_cancellable(&plan, Arc::new(AtomicBool::new(true)))
        .unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(engine.baseline_stats().dirty_vertex_count, dirty_before);
    engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(6.0))
    );

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(4))?;
    let dirty_before = engine.baseline_stats().dirty_vertex_count;
    let error = engine
        .evaluate_recalc_plan_with_controls(
            &plan,
            None,
            Some(Instant::now() - Duration::from_millis(1)),
        )
        .unwrap_err();
    assert!(matches!(
        error.extra,
        formualizer_common::ExcelErrorExtra::Resource { ref detail }
            if detail.reason == formualizer_common::ResourceExhaustionReason::Deadline
    ));
    assert_eq!(engine.baseline_stats().dirty_vertex_count, dirty_before);
    engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(12.0))
    );
    Ok(())
}

#[test]
fn target_plan_layer_count_documents_run_local_recipe() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    let target = EvaluationTarget::Cell {
        sheet: "Sheet1".to_string(),
        row: 1,
        col: 1,
    };
    assert_eq!(
        engine
            .build_recalc_plan_for_targets(&[target])?
            .layer_count(),
        0
    );
    Ok(())
}

#[test]
fn recalc_plan_reused_for_multiple_runs() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;

    let chain_ast = |row: u32| {
        create_binary_op_ast(
            create_cell_ref_ast(None, row - 1, 1),
            create_cell_ref_ast(None, row - 1, 1),
            "+",
        )
    };

    engine.set_cell_formula("Sheet1", 2, 1, chain_ast(2))?;
    engine.set_cell_formula("Sheet1", 3, 1, chain_ast(3))?;
    engine.set_cell_formula("Sheet1", 4, 1, chain_ast(4))?;

    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan()?;

    for value in [2, 3, 4] {
        engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(value))?;
        let result = engine.evaluate_recalc_plan(&plan)?;
        assert_eq!(result.computed_vertices, 3);
        let expected = LiteralValue::Number((value * 8) as f64);
        assert_eq!(engine.get_cell_value("Sheet1", 4, 1), Some(expected));
    }

    Ok(())
}
