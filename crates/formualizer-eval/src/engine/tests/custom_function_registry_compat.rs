use crate::function::{FnCaps, Function};
use crate::function_registry;
use crate::traits::{ArgumentHandle, FunctionContext};
use crate::{engine::Engine, engine::EvalConfig, test_workbook::TestWorkbook};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

struct DynamicCompatFn;

impl Function for DynamicCompatFn {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }

    fn name(&self) -> &'static str {
        "GLOBAL_DYN_COMPAT"
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(77.0)))
    }
}

#[test]
fn global_dynamic_registration_remains_usable() {
    function_registry::register_function(Arc::new(DynamicCompatFn));

    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=global_dyn_compat()").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(77.0))
    );
}
