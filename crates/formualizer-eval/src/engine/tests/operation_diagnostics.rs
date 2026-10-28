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
    let r = e.operation_diagnostics().unwrap();
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
    let r = e.operation_diagnostics().unwrap();
    assert_eq!(r.logical_cell_reason_count, 3, "{r:?}");
    assert!(r.roots.iter().any(|root| matches!(
        root.operation,
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
    let report = e.operation_diagnostics().unwrap();
    assert_eq!(report.logical_cell_reason_count, 3, "{report:?}");
    for root in &report.roots {
        assert!(
            matches!(&root.operation,
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
        e.operation_diagnostics().unwrap().logical_cell_reason_count,
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
    let r = e.operation_diagnostics().unwrap();
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
    let r = e.operation_diagnostics().unwrap();
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
    let r = e.operation_diagnostics().unwrap();
    assert_eq!(r.logical_cell_reason_count, 0);
    assert_eq!(r.physical_function_dispatches, 0);
}

#[test]
fn recalc_contract_diagnostics_samples_and_request_reset() {
    for limit in [0, 1, 3] {
        let mut e = engine(&["=IFERROR(GAP_ALPHA(1),1)+IFERROR(GAP_BETA(1),2)"]);
        e.enable_operation_diagnostics(limit);
        e.evaluate_all().unwrap();
        let r = e.operation_diagnostics().unwrap();
        assert_eq!(r.logical_cell_reason_count, 2);
        assert_eq!(r.samples.len(), limit.min(2));
        e.evaluate_all().unwrap();
        assert_eq!(
            e.operation_diagnostics().unwrap().logical_cell_reason_count,
            0,
            "request events are not a retained-result coverage certificate"
        );
    }
}
