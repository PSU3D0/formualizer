//! Regression tests for lazy dispatch of `FnCaps::SHORT_CIRCUIT` functions.
//!
//! `Function::dispatch` must not eagerly materialize arguments of
//! short-circuit functions: doing so executes reads (and arbitrary
//! subexpressions) in untaken branches, defeating the documented
//! short-circuit semantics and double-evaluating taken branches. This is
//! load-bearing for live-edge collection (RFC #112 Stage 1): untaken branches
//! must produce no observable reads.

use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CountFn(Arc<AtomicUsize>);
impl crate::function::Function for CountFn {
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "COUNTING"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(7)))
    }
}

fn harness() -> (TestWorkbook, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new()
        .with_function(Arc::new(crate::builtins::logical::IfFn))
        .with_function(Arc::new(CountFn(counter.clone())));
    (wb, counter)
}

fn eval(wb: &TestWorkbook, formula: &str) -> LiteralValue {
    let interp = wb.interpreter();
    let ast = formualizer_parse::parser::parse(formula).unwrap();
    interp.evaluate_ast(&ast).unwrap().into_literal()
}

#[test]
fn if_untaken_branch_is_not_evaluated_through_dispatch() {
    let (wb, counter) = harness();

    let v = eval(&wb, "=IF(TRUE,1,COUNTING())");
    assert_eq!(v, LiteralValue::Number(1.0));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "untaken IF branch must not be evaluated"
    );

    let v = eval(&wb, "=IF(FALSE,1,COUNTING())");
    assert_eq!(v, LiteralValue::Int(7));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "taken IF branch must be evaluated exactly once"
    );
}

#[test]
fn if_arity_error_is_preserved_with_lazy_dispatch() {
    let (wb, counter) = harness();
    let v = eval(&wb, "=IF(COUNTING())");
    match v {
        LiteralValue::Error(e) => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Value);
        }
        other => panic!("expected #VALUE! arity error, got {other:?}"),
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "arity failure must not evaluate arguments"
    );
}
