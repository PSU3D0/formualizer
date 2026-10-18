//! Function resolution refresh regressions.
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::function::Function;
use crate::test_workbook::TestWorkbook;
use crate::traits::ArgumentHandle;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

struct Late;
impl Function for Late {
    fn name(&self) -> &'static str {
        "FNREVIEW_LATE_NAMED"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(5.0)))
    }
}

#[test]
fn review_named_vertex_sees_function_registered_after_previous_pass() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    engine
        .define_name(
            "LateName",
            NamedDefinition::Formula {
                ast: parse("=FNREVIEW_LATE_NAMED()").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let vertex = engine
        .graph
        .resolve_name_entry("LateName", sheet_id)
        .expect("name")
        .vertex;
    crate::function_registry::register_function(Arc::new(Late));
    let value = engine.evaluate_vertex(vertex);
    assert_eq!(value.unwrap(), LiteralValue::Number(5.0));
}

struct Replacement(f64);
impl Function for Replacement {
    fn name(&self) -> &'static str {
        "FNREVIEW_ENGINE_REPLACE"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            self.0,
        )))
    }
}
#[test]
fn engine_sees_global_replacement_between_passes() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    crate::function_registry::register_function(Arc::new(Replacement(1.0)));
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            parse("=FNREVIEW_ENGINE_REPLACE()+A2").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(0.0))
        .unwrap();
    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 1).unwrap(),
        Some(LiteralValue::Number(1.0))
    );
    crate::function_registry::register_function(Arc::new(Replacement(2.0)));
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(1.0))
        .unwrap();
    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 1).unwrap(),
        Some(LiteralValue::Number(3.0))
    );
}
