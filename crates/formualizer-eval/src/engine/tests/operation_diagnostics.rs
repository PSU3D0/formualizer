use super::common::arrow_eval_config;
use crate::{
    engine::{Engine, EvalConfig},
    test_workbook::TestWorkbook,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn engine(formulas: &[&str]) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    e.enable_operation_diagnostics(3);
    for (i, formula) in formulas.iter().enumerate() {
        e.set_cell_formula("Sheet1", i as u32 + 1, 2, parse(formula).unwrap())
            .unwrap();
    }
    e
}

#[test]
fn recalc_contract_diagnostics_caught_and_inactive() {
    let mut e = engine(&[
        "=IFERROR(GAP_ALPHA(1),7)",
        "=IF(FALSE,GAP_INACTIVE(1),MISSING_NAME)",
        "=IFERROR(GAP_ALPHA(1),1)+IFERROR(GAP_BETA(1),2)",
        "=IFERROR(1/0,7)",
        "=IFERROR(7,GAP_INACTIVE(1))",
        "=ISERROR(GAP_ALPHA(1))",
    ]);
    e.evaluate_all().unwrap();
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 4);
    assert_eq!(r.logical_root_cell_count, 3);
    assert!(
        r.roots
            .iter()
            .all(|root| !format!("{:?}", root.operation).contains("INACTIVE"))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn recalc_contract_diagnostics_callable_and_postfix() {
    let mut e = engine(&[
        "=LET(f,LAMBDA(x,IFERROR(GAP_LOCAL(x),7)),f(1))",
        "=IFERROR(LAMBDA(x,GAP_BODY(x))(1),7)",
        "=IFERROR(LET(x,GAP_BOUND(1),x),7)",
    ]);
    e.evaluate_all().unwrap();
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 3, "{r:?}");
    assert!(r.roots.iter().any(|root| matches!(
        root.operation.as_ref(),
        crate::operation_diagnostics::UnavailableOperation::PostfixInvocation
    )));
    assert!(!format!("{r:?}").contains("GAP_BODY"));
    for row in 1..=3 {
        assert_eq!(
            e.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(7.0))
        );
    }
}

#[test]
fn recalc_contract_diagnostics_registered_partial_modes_report_at_leaf() {
    let mut e = engine(&[
        "=IFERROR(AGGREGATE(12,6,A1:A2),7)",
        "=IFERROR(SUM(INDIRECT(\"R1C1:R2C1\",FALSE)),7)",
        "=IFERROR(ISREF(INDIRECT(\"R1C1:R2C1\",FALSE)),7)",
        "=AGGREGATE(9,6,A1:A2)",
    ]);
    e.evaluate_all().unwrap();
    let report = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(report.logical_cell_reason_count, 3, "{report:?}");
    for root in &report.roots {
        assert!(
            matches!(root.operation.as_ref(),
            crate::operation_diagnostics::UnavailableOperation::UnimplementedFunction { name }
            if name == "AGGREGATE" || name == "INDIRECT"),
            "{root:?}"
        );
    }
}

#[test]
fn recalc_contract_diagnostics_inherited_error_not_new_root() {
    let mut e = engine(&[
        "=IFERROR(SUM(A1),7)",
        "=IF(FALSE,GAP_INACTIVE(1),A1)",
        "=IFERROR(LET(x,A1,x),7)",
        "=IFERROR(LET(f,LAMBDA(x,x),f(A1)),7)",
    ]);
    e.set_cell_value(
        "Sheet1",
        1,
        1,
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::NImpl)),
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.operation_diagnostics()
            .unwrap()
            .unwrap()
            .logical_cell_reason_count,
        0
    );
}

#[test]
fn recalc_contract_diagnostics_named_formula_symbol_origin() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = engine(&["=IFERROR(GAP_NAME,7)"]);
    e.define_name(
        "GAP_NAME",
        NamedDefinition::Formula {
            ast: parse("=IFERROR(GAP_NAMED_BODY(1),9)").unwrap(),
            dependencies: Vec::new(),
            range_deps: Vec::new(),
        },
        NameScope::Workbook,
    )
    .unwrap();
    e.evaluate_all().unwrap();
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_named_reason_count, 1, "{r:?}");
    assert_eq!(r.logical_cell_reason_count, 0, "{r:?}");
    assert_eq!(r.roots.len(), 1);
    assert!(r.roots[0].named_vertex.is_some());
    assert!(r.roots[0].cell.is_none());
}

#[test]
fn recalc_contract_diagnostics_tree_and_arena_agree() {
    let e = engine(&["=1"]);
    let cell = e.graph.make_cell_ref("Sheet1", 0, 1);
    let interpreter = crate::interpreter::Interpreter::new_with_cell(&e, "Sheet1", cell);
    let value = interpreter
        .evaluate_ast(
            &parse("=IFERROR(GAP_TREE(1),7)+IFERROR(LAMBDA(x,GAP_BODY(x))(1),2)").unwrap(),
        )
        .unwrap()
        .into_literal();
    assert_eq!(value, LiteralValue::Number(9.0));
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 2, "{r:?}");
    assert!(!format!("{r:?}").contains("GAP_BODY"));
}

#[test]
fn recalc_contract_diagnostics_resource_failure_remains_failure() {
    use crate::engine::{CancelToken, EvaluationBudgets, WorkResourceBudget};
    let mut e = engine(&["=IFERROR(GAP_ALPHA(1),7)"]);
    e.set_evaluation_budgets_for_test(EvaluationBudgets {
        work: WorkResourceBudget {
            max_work_units: Some(0),
        },
        ..EvaluationBudgets::default()
    });
    let error = e.evaluate_all_cancellable(CancelToken::new()).unwrap_err();
    assert!(matches!(
        error.extra(),
        formualizer_common::ExcelErrorExtra::Resource { .. }
    ));
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 0);
    assert_eq!(r.physical_function_dispatches, Some(0));
}

#[test]
fn recalc_contract_diagnostics_samples_and_request_reset() {
    for limit in [0, 1, 3] {
        let mut e = engine(&["=IFERROR(GAP_ALPHA(1),1)+IFERROR(GAP_BETA(1),2)"]);
        e.enable_operation_diagnostics(limit);
        e.evaluate_all().unwrap();
        let r = e.operation_diagnostics().unwrap().unwrap();
        assert_eq!(r.logical_cell_reason_count, 2);
        assert_eq!(r.samples.len(), limit.min(2));
        e.evaluate_all().unwrap();
        assert_eq!(
            e.operation_diagnostics()
                .unwrap()
                .unwrap()
                .logical_cell_reason_count,
            0,
            "request events are not a retained-result coverage certificate"
        );
    }
}

#[test]
fn recalc_contract_cell_valid_unsupported_mode_is_not_invalid_input() {
    let mut e = engine(&[
        "=IFERROR(CELL(\"format\",A1),\"fallback\")",
        "=IF(FALSE,CELL(\"format\",A1),7)",
        "=CELL(\"not_an_info_type\",A1)",
        "=CELL(\"row\",A1)",
    ]);
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    e.evaluate_all().unwrap();
    let report = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(report.logical_root_cell_count, 1, "{report:?}");
    assert_eq!(report.logical_cell_reason_count, 1, "{report:?}");
}
fn pivot_mode_report(values: &str) -> crate::operation_diagnostics::OperationDiagnostics {
    crate::builtins::load_builtins();
    let mut e = Engine::new(TestWorkbook::new(), arrow_eval_config());
    e.enable_operation_diagnostics(8);
    for row in 1..=2 {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Text("x".into()))
            .unwrap();
        e.set_cell_value("Sheet1", row, 2, LiteralValue::Text("p".into()))
            .unwrap();
        e.set_cell_value("Sheet1", row, 3, LiteralValue::Number(10.0 * row as f64))
            .unwrap();
        e.set_cell_value("Sheet1", row, 4, LiteralValue::Number(100.0 * row as f64))
            .unwrap();
    }
    let formula = format!("=PIVOTBY(A1:A2,B1:B2,{values},\"SUM\",0,0,0,0,0)");
    e.set_cell_formula("Sheet1", 1, 6, parse(&formula).unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    e.operation_diagnostics().unwrap().unwrap()
}
#[test]
fn recalc_contract_pivotby_supported_and_partial_shapes() {
    assert_eq!(pivot_mode_report("C1:C2").logical_root_cell_count, 0);
    assert_eq!(pivot_mode_report("C1:D2").logical_root_cell_count, 1);
}

fn telemetry_report(physical: bool) -> crate::operation_diagnostics::OperationDiagnostics {
    let mut e = engine(&[
        "=IFERROR(GAP_TELEMETRY(1),7)",
        "=ABS(-2)",
        "=IF(FALSE,GAP_INACTIVE(1),5)",
    ]);
    e.configure_operation_diagnostics(crate::operation_diagnostics::OperationDiagnosticOptions {
        sample_limit: 0,
        physical_telemetry: physical,
        ..Default::default()
    });
    e.evaluate_all().unwrap();
    e.take_operation_diagnostics().unwrap().unwrap()
}

#[test]
fn recalc_contract_sparse_evidence_does_not_require_physical_telemetry() {
    let sparse = telemetry_report(false);
    let full = telemetry_report(true);
    assert_eq!(sparse.roots, full.roots);
    assert_eq!(sparse.logical_root_cell_count, 1);
    assert!(sparse.samples.is_empty());
    assert_eq!(sparse.physical_function_dispatches, None);
    assert_eq!(sparse.physical_unavailable_observations, None);
    assert!(
        full.physical_function_dispatches
            .is_some_and(|count| count > 0)
    );
    assert!(
        full.physical_unavailable_observations
            .is_some_and(|count| count > 0)
    );
}

#[test]
fn recalc_contract_exact_mode_evidence_uses_executed_leaf_origin() {
    use crate::operation_diagnostics::UnavailableOperation;
    let mut e = engine(&[
        "=IFERROR(CELL(\"format\",A1),\"fallback\")",
        "=IF(TRUE,7,CELL(\"width\",A1))",
        "=CELL(\"not_an_info_type\",A1)",
        "=IFERROR(CELL(\"format\",1/0),8)",
    ]);
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    e.evaluate_all().unwrap();
    let report = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(report.roots.len(), 1, "{report:?}");
    assert!(matches!(
        report.roots[0].operation.as_ref(),
        UnavailableOperation::UnimplementedFunctionMode { name, mode }
        if name == "CELL" && *mode == "format"
    ));
    let cell = report.roots[0].cell.unwrap();
    assert_eq!((cell.coord.row(), cell.coord.col()), (0, 1));
    assert!(report.roots[0].named_vertex.is_none());
}

#[test]
fn recalc_contract_evidence_failure_survives_caught_values_and_request_reset() {
    let mut e = engine(&["=IFERROR(GAP_ONE(1),7)", "=IFERROR(GAP_TWO(1),8)"]);
    e.configure_operation_diagnostics(crate::operation_diagnostics::OperationDiagnosticOptions {
        sample_limit: 0,
        max_roots: Some(1),
        ..Default::default()
    });
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(7.0))
    );
    let error = e.take_operation_diagnostics().unwrap_err();
    assert!(matches!(
        error.extra(),
        formualizer_common::ExcelErrorExtra::Resource { detail }
        if detail.reason == formualizer_common::ResourceExhaustionReason::OperationEvidenceRoots
    ));
    e.set_cell_formula("Sheet1", 1, 2, parse("=1+1").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 2, 2, parse("=2+2").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let next = e.take_operation_diagnostics().unwrap().unwrap();
    assert!(next.roots.is_empty());
    assert_eq!(next.physical_function_dispatches, None);
    assert!(e.take_operation_diagnostics().is_err());
}
