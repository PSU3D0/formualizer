//! Regressions for diagnostics review R1–R4 on the integrated engine.
use super::common::arrow_eval_config;
use crate::{
    engine::{
        CancelToken, Engine, EvalConfig,
        named_range::{NameScope, NamedDefinition},
    },
    function::{FnCaps, Function},
    test_workbook::TestWorkbook,
    traits::{ArgumentHandle, CalcValue, FunctionContext},
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ReferenceType, parse};
use std::sync::Arc;

fn engine() -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    e.enable_operation_diagnostics(3);
    e
}
fn formula(e: &mut Engine<TestWorkbook>, row: u32, col: u32, text: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(text).unwrap())
        .unwrap();
}
fn number(e: &mut Engine<TestWorkbook>, row: u32, col: u32, value: f64) {
    e.set_cell_value("Sheet1", row, col, LiteralValue::Number(value))
        .unwrap();
}
fn gap(name: &str) -> crate::operation_diagnostics::UnavailableOperation {
    crate::operation_diagnostics::UnavailableOperation::UnimplementedFunction { name: name.into() }
}

struct ReferenceGap {
    fail: bool,
}
impl Function for ReferenceGap {
    fn name(&self) -> &'static str {
        if self.fail { "REFGAP_ERR" } else { "REFGAP_OK" }
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE | FnCaps::RETURNS_REFERENCE
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::NImpl))
    }
    fn eval_reference<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Option<Result<ReferenceType, ExcelError>> {
        // Diagnostic location must not change established reference semantics.
        assert_eq!(ctx.current_cell(), None);
        ctx.report_unavailable_operation(gap(self.name()));
        Some(if self.fail {
            Err(ExcelError::new(ExcelErrorKind::NImpl))
        } else {
            Ok(ReferenceType::from_string("A1").unwrap())
        })
    }
}

#[test]
fn recalc_contract_diagnostics_corrections_reference_origins() {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new()
        .with_function(Arc::new(ReferenceGap { fail: false }))
        .with_function(Arc::new(ReferenceGap { fail: true }));
    let mut e = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    e.enable_operation_diagnostics(3);
    number(&mut e, 1, 1, 5.0);
    formula(&mut e, 1, 2, "=SUM(REFGAP_OK())");
    formula(&mut e, 2, 2, "=IFERROR(SUM(REFGAP_ERR()),7)");
    e.define_name(
        "NAMED_GAP",
        NamedDefinition::Formula {
            ast: parse("=SUM(REFGAP_OK())").unwrap(),
            dependencies: vec![],
            range_deps: vec![],
        },
        NameScope::Workbook,
    )
    .unwrap();
    e.evaluate_all().unwrap();
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 2, "{r:?}");
    assert_eq!(r.logical_named_reason_count, 1, "{r:?}");
    assert!(
        r.roots
            .iter()
            .all(|root| root.cell.is_some() || root.named_vertex.is_some())
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(7.0))
    );
    // Direct parsed-tree reference path uses the same distinct diagnostic origin.
    e.enable_operation_diagnostics(3);
    let cell = e.graph.make_cell_ref("Sheet1", 2, 1);
    let i = crate::interpreter::Interpreter::new_with_cell(&e, "Sheet1", cell);
    i.evaluate_ast_as_reference(&parse("=REFGAP_OK()").unwrap())
        .unwrap();
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.roots.len(), 1);
    assert_eq!(r.roots[0].cell, Some(cell));
}

#[test]
fn recalc_contract_diagnostics_corrections_registered_tree_and_inactive() {
    let mut e = engine();
    number(&mut e, 1, 1, 5.0);
    let cell = e.graph.make_cell_ref("Sheet1", 1, 2);
    let i = crate::interpreter::Interpreter::new_with_cell(&e, "Sheet1", cell);
    for src in [
        "=IFERROR(SUM(INDIRECT(\"R1C1:R2C1\",FALSE)),7)",
        "=IF(FALSE,SUM(INDIRECT(\"R1C1:R2C1\",FALSE)),7)",
    ] {
        assert_eq!(
            i.evaluate_ast(&parse(src).unwrap()).unwrap().into_literal(),
            LiteralValue::Number(7.0)
        );
    }
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 1, "{r:?}");
    assert_eq!(r.roots[0].operation.as_ref(), &gap("INDIRECT"));
    assert_eq!(r.physical_unavailable_observations, Some(1));
    formula(
        &mut e,
        1,
        2,
        "=IF(FALSE,SUM(INDIRECT(\"R1C1:R2C1\",FALSE)),7)",
    );
    e.evaluate_all().unwrap();
    assert!(e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
}

#[test]
fn recalc_contract_diagnostics_corrections_early_cancel_and_nonformula_reset() {
    let mut e = engine();
    formula(&mut e, 1, 2, "=IFERROR(GAP_PREVIOUS(1),7)");
    e.evaluate_all_for_snapshot(None).unwrap();
    assert_eq!(
        e.operation_diagnostics()
            .unwrap()
            .unwrap()
            .logical_cell_reason_count,
        1
    );
    let token = CancelToken::new();
    token.cancel();
    assert_eq!(
        e.evaluate_all_for_snapshot(Some(token)).unwrap_err().kind,
        ExcelErrorKind::Cancelled
    );
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert!(r.roots.is_empty() && r.samples.is_empty(), "{r:?}");
    assert_eq!(r.physical_unavailable_observations, Some(0));
    assert_eq!(r.physical_function_dispatches, Some(0));
    // A literal defined name provides a public non-formula vertex request.
    e.define_name(
        "LITERAL_ONLY",
        NamedDefinition::Literal(LiteralValue::Number(4.0)),
        NameScope::Workbook,
    )
    .unwrap();
    formula(&mut e, 1, 2, "=IFERROR(GAP_AGAIN(1),7)");
    e.evaluate_all().unwrap();
    assert!(!e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
    let vertex = e
        .resolve_name_entry("LITERAL_ONLY", e.sheet_id("Sheet1").unwrap())
        .unwrap()
        .vertex;
    assert_eq!(
        e.evaluate_vertex(vertex).unwrap(),
        LiteralValue::Number(4.0)
    );
    assert!(e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
    e.evaluate_all().unwrap();
    assert!(e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
}

#[test]
fn recalc_contract_diagnostics_corrections_dirty_branch_probe_is_speculative() {
    for (retained, next) in [(0.0, 1.0), (1.0, 0.0)] {
        let mut e = engine();
        number(&mut e, 1, 1, retained);
        number(&mut e, 1, 3, 9.0);
        formula(&mut e, 1, 1, &format!("={next}"));
        formula(
            &mut e,
            1,
            2,
            "=IF(A1=0,IFERROR(GAP_PROBE(1),7),INDIRECT(\"C1\"))",
        );
        e.evaluate_all_for_snapshot(None).unwrap();
        let r = e.operation_diagnostics().unwrap().unwrap();
        assert_eq!(
            r.logical_cell_reason_count,
            if next == 0.0 { 1 } else { 0 },
            "retained={retained} next={next}: {r:?}"
        );
        assert!(
            r.physical_unavailable_observations
                .is_some_and(|count| count > 0),
            "{r:?}"
        );
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(if next == 0.0 { 7.0 } else { 9.0 }))
        );
    }
}

#[test]
fn recalc_contract_diagnostics_corrections_outside_query_does_not_mutate_report() {
    let mut e = engine();
    formula(&mut e, 1, 4, "=IFERROR(GAP_RETAINED(1),7)");
    e.evaluate_all().unwrap();
    formula(
        &mut e,
        1,
        2,
        "=IFERROR(GAP_QUERY(1),7)+IFERROR(INDIRECT(\"C1\"),0)",
    );
    let cell = e.graph.make_cell_ref("Sheet1", 1, 2);
    let vertex = e.graph.get_vertex_for_cell(&cell).unwrap();
    let before = format!("{:?}", e.operation_diagnostics().unwrap().unwrap());
    crate::engine::virtual_deps::DynamicRefVirtualDepProvider::get_virtual_deps(&e, vertex);
    assert_eq!(
        format!("{:?}", e.operation_diagnostics().unwrap().unwrap()),
        before
    );
}

#[test]
fn recalc_contract_diagnostics_corrections_snapshot_replan_preserves_union() {
    let mut e = engine();
    number(&mut e, 1, 1, 1.0);
    formula(&mut e, 2, 1, "=A1+1");
    formula(&mut e, 3, 1, "=A2+1");
    formula(&mut e, 4, 1, "=A3+1");
    formula(&mut e, 1, 4, "=IF(A1>1,\"A4\",\"A1\")");
    formula(&mut e, 1, 2, "=IFERROR(GAP_DYNAMIC(1),0)+INDIRECT(D1)");
    formula(&mut e, 1, 3, "=B1+1");
    formula(&mut e, 1, 6, "=IFERROR(GAP_FIRST_PASS(1),7)");
    e.evaluate_all().unwrap();
    number(&mut e, 1, 1, 5.0);
    formula(&mut e, 1, 6, "=IFERROR(GAP_EARLY(1),7)");
    let before = e.freshness_counters_for_test();
    e.evaluate_all_for_snapshot(None).unwrap();
    let (stale, stops) = e.freshness_counters_for_test();
    assert!(
        stale > before.0 && stops > before.1,
        "before={before:?} stale={stale} stops={stops}"
    );
    let r = e.operation_diagnostics().unwrap().unwrap();
    assert_eq!(r.logical_cell_reason_count, 2, "{r:?}");
    assert!(format!("{r:?}").contains("GAP_EARLY"));
    assert!(format!("{r:?}").contains("GAP_DYNAMIC"));
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(8.0))
    );
}

struct Canceller;
impl Function for Canceller {
    fn name(&self) -> &'static str {
        "DIAG_CANCEL"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        ctx.cancellation_token().unwrap().cancel();
        Err(ExcelError::new(ExcelErrorKind::Cancelled))
    }
}
#[test]
fn recalc_contract_diagnostics_corrections_live_cancellation_parity() {
    for enabled in [false, true] {
        let mut e = Engine::new(
            TestWorkbook::new().with_function(Arc::new(Canceller)),
            EvalConfig {
                enable_parallel: false,
                ..arrow_eval_config()
            },
        );
        if enabled {
            e.enable_operation_diagnostics(3);
        }
        formula(&mut e, 1, 2, "=IFERROR(DIAG_CANCEL(),7)");
        let error = e
            .evaluate_all_for_snapshot(Some(CancelToken::new()))
            .unwrap_err();
        assert_eq!(error.kind, ExcelErrorKind::Cancelled);
        assert_eq!(e.get_cell_value("Sheet1", 1, 2), None);
        if enabled {
            assert!(e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
        }
    }
}
