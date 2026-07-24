//! Dynamic-array builtins must accept computed array arguments.
//!
//! `FILTER`, `SORT`, `UNIQUE`, `TRANSPOSE` and friends take *data*, so a
//! reference is an optimization rather than a requirement. Their schemas used
//! to mark those arguments `by_ref`, and `ArgumentHandle::range_view` demanded
//! a reference for computed nodes, so every idiomatic call that passed a
//! computed array (`B1:B3="x"`, `SEQUENCE(3)`, `{1,2}`) was rejected with
//! `#REF!` during argument preparation.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

/// `(formula, expected)` pairs evaluated in both FormulaPlane modes.
fn cases() -> Vec<(&'static str, LiteralValue)> {
    vec![
        // Reference arguments keep working (the fast path is unchanged).
        ("=COUNT(SORT(A1:A3))", LiteralValue::Number(3.0)),
        ("=COUNT(FILTER(A1:A3,B1:B3))", LiteralValue::Number(3.0)),
        // The originally reported form: a computed boolean include array.
        (
            "=COUNT(FILTER(A1:A3,B1:B3=\"x\"))",
            LiteralValue::Number(2.0),
        ),
        (
            "=SUM(FILTER(A1:A3,B1:B3=\"x\"))",
            LiteralValue::Number(40.0),
        ),
        // A computed *source* array is equally valid.
        (
            "=SUM(FILTER(SEQUENCE(3),{TRUE;FALSE;TRUE}))",
            LiteralValue::Number(4.0),
        ),
        ("=SUM(SORT(SEQUENCE(3)))", LiteralValue::Number(6.0)),
        // Inline array literals across the dynamic-array family.
        ("=COUNT(TRANSPOSE({1,2}))", LiteralValue::Number(2.0)),
        ("=SUM(SORT({3;1;2}))", LiteralValue::Number(6.0)),
        ("=COUNT(UNIQUE({1;1;2}))", LiteralValue::Number(2.0)),
        ("=SUM(TAKE({1;2;3},2))", LiteralValue::Number(3.0)),
        ("=SUM(DROP({1;2;3},1))", LiteralValue::Number(5.0)),
        ("=SUM(SORTBY({3;1;2},{3;1;2}))", LiteralValue::Number(6.0)),
        // Lookup family over a computed haystack.
        ("=XLOOKUP(20,A1:A3*1,A1:A3)", LiteralValue::Number(20.0)),
        ("=XMATCH(20,A1:A3*1)", LiteralValue::Number(2.0)),
        // Nesting dynamic arrays is the common real-world shape.
        (
            "=SUM(SORT(FILTER(A1:A3,B1:B3=\"x\")))",
            LiteralValue::Number(40.0),
        ),
    ]
}

fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (row, value) in [(1, 10.0), (2, 20.0), (3, 30.0)] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(value))
            .unwrap();
    }
    for (row, text) in [(1, "x"), (2, "y"), (3, "x")] {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Text(text.into()))
            .unwrap();
    }
    for (idx, (formula, _)) in cases().into_iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", idx as u32 + 1, 5, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn dynamic_array_builtins_accept_computed_array_arguments() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let engine = build(mode);
        for (idx, (formula, expected)) in cases().into_iter().enumerate() {
            let got = engine.get_cell_value("Sheet1", idx as u32 + 1, 5);
            assert_eq!(
                got,
                Some(expected),
                "{formula} evaluated unexpectedly in {mode:?} mode"
            );
        }
    }
}

/// Flipping these arguments off `by_ref` must not swallow a genuine reference
/// failure: `OFFSET` still owns reference semantics and its errors surface as
/// formula values rather than being re-evaluated into something else.
#[test]
fn genuine_reference_errors_are_still_preserved() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    let cases = [
        (1, "=SUM(SORT(OFFSET(A1,-1,0)))", ExcelErrorKind::Ref),
        (
            2,
            "=SUM(FILTER(OFFSET(A1,-1,0),{TRUE}))",
            ExcelErrorKind::Ref,
        ),
        // INDIRECT reports an unresolvable name as #NAME?; the point is that
        // the failure still propagates instead of being masked.
        (
            3,
            "=COUNT(TRANSPOSE(INDIRECT(\"not a reference\")))",
            ExcelErrorKind::Name,
        ),
    ];
    for (row, formula, _) in cases {
        engine
            .set_cell_formula("Sheet1", row, 5, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    for (row, formula, expected_kind) in cases {
        match engine.get_cell_value("Sheet1", row, 5) {
            Some(LiteralValue::Error(error)) => {
                assert_eq!(error.kind, expected_kind, "{formula}")
            }
            other => panic!("{formula}: expected an error value, got {other:?}"),
        }
    }
}

/// A computed array argument must be evaluated exactly once.
///
/// `Function::dispatch` runs `validate_and_prepare` and then throws the
/// prepared arguments away before calling `eval`, so "evaluated once" rests
/// entirely on the `ArgumentHandle` memos. A regression here would be silent
/// and expensive rather than loud, so it is pinned with a counting function.
#[test]
fn computed_array_arguments_are_evaluated_once() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingArrayFn(Arc<AtomicUsize>);
    impl crate::function::Function for CountingArrayFn {
        fn caps(&self) -> crate::function::FnCaps {
            crate::function::FnCaps::PURE
        }
        fn name(&self) -> &'static str {
            "COUNTINGARRAY"
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
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
                vec![LiteralValue::Number(3.0)],
                vec![LiteralValue::Number(1.0)],
                vec![LiteralValue::Number(2.0)],
            ])))
        }
    }

    for formula in [
        "=SUM(SORT(COUNTINGARRAY()))",
        "=SUM(UNIQUE(COUNTINGARRAY()))",
        "=SUM(FILTER(COUNTINGARRAY(),{TRUE;TRUE;TRUE}))",
    ] {
        let counter = Arc::new(AtomicUsize::new(0));
        let workbook =
            TestWorkbook::default().with_function(Arc::new(CountingArrayFn(counter.clone())));
        let mut engine = Engine::new(workbook, EvalConfig::default());
        engine
            .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();

        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(6.0)),
            "{formula}"
        );
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "{formula} must evaluate its computed argument exactly once"
        );
    }
}

/// Unbounded references must keep resolving through the lazy view rather than
/// being materialized by the argument-shape path that `by_ref` removal now
/// routes them through.
#[test]
fn whole_column_references_still_resolve() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for (row, value) in [(1, 10.0), (2, 20.0), (3, 30.0)] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(value))
            .unwrap();
    }
    for (row, text) in [(1, "x"), (2, "y"), (3, "x")] {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Text(text.into()))
            .unwrap();
    }
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            5,
            parse("=SUM(FILTER(A:A,B:B=\"x\"))").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Number(40.0))
    );
}

/// GROUPBY and PIVOTBY carry five of the sixteen flipped arguments between
/// them, so they need explicit computed-argument coverage.
#[test]
fn groupby_and_pivotby_accept_computed_arguments() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for (row, group, value) in [(1, "a", 1.0), (2, "b", 2.0), (3, "a", 3.0)] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Text(group.into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(value))
            .unwrap();
    }
    for (row, formula) in [
        (1, "=COUNT(GROUPBY(A1:A3,B1:B3*1,\"SUM\"))"),
        (2, "=COUNT(PIVOTBY(A1:A3,A1:A3,B1:B3*1,\"SUM\"))"),
    ] {
        engine
            .set_cell_formula("Sheet1", row, 5, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    for (row, formula) in [
        (1, "=COUNT(GROUPBY(A1:A3,B1:B3*1,\"SUM\"))"),
        (2, "=COUNT(PIVOTBY(A1:A3,A1:A3,B1:B3*1,\"SUM\"))"),
    ] {
        match engine.get_cell_value("Sheet1", row, 5) {
            Some(LiteralValue::Error(error)) => {
                panic!("{formula} rejected its computed argument with {error:?}")
            }
            None => panic!("{formula} produced no value"),
            Some(_) => {}
        }
    }
}

/// Documents (rather than endorses) the current handling of a computed
/// *scalar*: only arrays gained acceptance here, so a non-array scalar is still
/// rejected. Excel instead treats `=TRANSPOSE(2)` as a 1x1 array and returns
/// `2`; closing that gap changes every range-consuming builtin and is
/// deliberately left to a separate change.
#[test]
fn computed_scalars_are_still_rejected_as_ranges() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=COUNT(TRANSPOSE(1+1))").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 5) {
        Some(LiteralValue::Error(error)) => assert_eq!(error.kind, ExcelErrorKind::Ref),
        other => panic!("expected a #REF! value, got {other:?}"),
    }
}
