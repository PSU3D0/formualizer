use super::common::arrow_eval_config;
use crate::{
    engine::{Engine, EvalConfig},
    function::{FnCaps, Function},
    test_workbook::TestWorkbook,
    traits::{ArgumentHandle, CalcValue, FunctionContext},
};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

struct IntentProbe {
    expected: Arc<dyn Function>,
}
impl Function for IntentProbe {
    fn name(&self) -> &'static str {
        "INTENT_PROBE"
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn dispatch<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.eval(args, ctx)
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let code = match args[0].function_value_reference_intent() {
            None => 0.0,
            Some(intent)
                if Arc::ptr_eq(&intent.function, &self.expected) && intent.binding_absent =>
            {
                1.0
            }
            Some(_) => 2.0,
        };
        Ok(CalcValue::Scalar(LiteralValue::Number(code)))
    }
}
fn engine() -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let expected = crate::function_registry::get("", "SUM").unwrap();
    Engine::new(
        TestWorkbook::new().with_function(Arc::new(IntentProbe { expected })),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    )
}
#[test]
fn callable_intent_resolves_actual_provider_without_invocation() {
    let mut e = engine();
    for (row, text, expected) in [
        (1, "=INTENT_PROBE(SUM)", 1.0),
        (2, "=INTENT_PROBE(_xlfn.SUM)", 1.0),
        (3, "=INTENT_PROBE(NO_SUCH_FN_OR_NAME)", 0.0),
        (4, "=INTENT_PROBE(A1)", 0.0),
        (5, "=LET(SUM,7,INTENT_PROBE(SUM))", 0.0),
    ] {
        e.set_cell_formula("Sheet1", row, 1, parse(text).unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(
            e.get_cell_value("Sheet1", row, 1),
            Some(LiteralValue::Number(expected)),
            "{text}"
        );
    }
}
#[test]
fn callable_intent_real_name_shadows_function_including_error_values() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    for value in [
        LiteralValue::Number(42.0),
        LiteralValue::Error(ExcelError::new(formualizer_common::ExcelErrorKind::Name)),
    ] {
        let mut e = engine();
        e.define_name("SUM", NamedDefinition::Literal(value), NameScope::Workbook)
            .unwrap();
        e.set_cell_formula("Sheet1", 1, 1, parse("=INTENT_PROBE(SUM)").unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(0.0))
        );
    }
}
#[test]
fn callable_intent_array_materialization_is_reported_at_actual_loss() {
    for text in [
        "=HSTACK(SUM,AVERAGE)",
        "=VSTACK(LAMBDA(x,SUM(x)),LAMBDA(x,MAX(x)))",
        "=IFERROR(HSTACK(SUM,AVERAGE),7)",
    ] {
        let mut e = engine();
        e.enable_operation_diagnostics(0);
        e.set_cell_formula("Sheet1", 1, 1, parse(text).unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        let report = e.operation_diagnostics().unwrap().unwrap();
        assert_eq!(report.logical_root_cell_count, 1, "{text}: {report:?}");
    }
    let mut e = engine();
    e.enable_operation_diagnostics(0);
    e.set_cell_formula(
        "Sheet1",
        1,
        1,
        parse("=IF(FALSE,HSTACK(SUM,AVERAGE),7)").unwrap(),
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert!(e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
}

struct NeverInvokeSum;
impl Function for NeverInvokeSum {
    fn name(&self) -> &'static str {
        "SUM"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["LOCAL_SUM_ALIAS"]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        panic!("intent metadata must not invoke a callback")
    }
}
#[test]
fn callable_intent_local_provider_and_alias_keep_actual_arc_identity() {
    let expected: Arc<dyn Function> = Arc::new(NeverInvokeSum);
    let workbook = TestWorkbook::new()
        .with_function(Arc::clone(&expected))
        .with_function(Arc::new(IntentProbe { expected }));
    let mut e = Engine::new(
        workbook,
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    for (row, text) in [
        (1, "=INTENT_PROBE(SUM)"),
        (2, "=INTENT_PROBE(LOCAL_SUM_ALIAS)"),
    ] {
        e.set_cell_formula("Sheet1", row, 1, parse(text).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    for row in [1, 2] {
        assert_eq!(
            e.get_cell_value("Sheet1", row, 1),
            Some(LiteralValue::Number(1.0))
        );
    }
}
#[test]
fn callable_intent_sheet_name_binding_has_correct_scope() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = engine();
    for sheet in ["Sheet1", "Sheet2"] {
        e.set_cell_value(sheet, 1, 2, LiteralValue::Number(2.0))
            .unwrap();
    }
    let first = e.sheet_id("Sheet1").unwrap();
    e.define_name(
        "SUM",
        NamedDefinition::Literal(LiteralValue::Number(42.0)),
        NameScope::Sheet(first),
    )
    .unwrap();
    for sheet in ["Sheet1", "Sheet2"] {
        e.set_cell_formula(sheet, 1, 1, parse("=INTENT_PROBE(SUM)").unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(0.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet2", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
}
#[test]
fn callable_intent_normal_array_errors_and_names_are_neutral() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    for text in [
        "=HSTACK(1,2)",
        "=HSTACK(1/0,2)",
        "=HSTACK(NO_SUCH_FN_OR_NAME,1)",
        "=LAMBDA(x,x)",
    ] {
        let mut e = engine();
        e.enable_operation_diagnostics(0);
        e.set_cell_formula("Sheet1", 1, 1, parse(text).unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        assert!(
            e.operation_diagnostics().unwrap().unwrap().roots.is_empty(),
            "{text}"
        );
    }
    let mut e = engine();
    e.define_name(
        "SUM",
        NamedDefinition::Literal(LiteralValue::Error(ExcelError::new(
            formualizer_common::ExcelErrorKind::Name,
        ))),
        NameScope::Workbook,
    )
    .unwrap();
    e.enable_operation_diagnostics(0);
    e.set_cell_formula("Sheet1", 1, 1, parse("=HSTACK(SUM,1)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert!(e.operation_diagnostics().unwrap().unwrap().roots.is_empty());
}
