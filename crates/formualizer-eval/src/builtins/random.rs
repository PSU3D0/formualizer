//! Volatile functions like RAND, RANDBETWEEN.
use crate::traits::{ArgumentHandle, EvaluationContext, Function};
use formualizer_common::{ExcelError, LiteralValue};
use rand::Rng;

#[derive(Debug)]
pub struct RandFn;

impl Function for RandFn {
    fn name(&self) -> &'static str {
        "RAND"
    }

    fn volatile(&self) -> bool {
        true
    }

    fn eval<'a, 'b>(
        &self,
        _args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Number(rand::thread_rng().r#gen()))
    }
}

use crate::function_registry;
use std::sync::Arc;

pub fn register_builtins() {
    function_registry::register(Arc::new(RandFn));
}
