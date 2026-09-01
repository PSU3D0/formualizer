//! FormulaPlane fails closed when span evaluation yields an array (refs #388).
//!
//! A span publishes exactly one value per placement, while the legacy evaluator
//! routes `LiteralValue::Array` results into the spill planner. The span
//! evaluator used to collapse an array to its top-left element, which would
//! broadcast one value across the whole span while legacy spilled a rectangle.
//! No admitted stock template produces an array today, so this pins the
//! backstop with a test-only function whose *declared* semantics are scalar
//! (admitted into a span) but whose runtime result is an array.

use std::sync::Arc;

use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const ROWS: u32 = 120;

/// Declared scalar (`trusted_builtin_default`, no `MAY_SPILL`), so the
/// FormulaPlane admission gate accepts it into a span; returns a 1x2 array at
/// runtime, which the legacy evaluator spills across two columns.
struct ArrayResultFn;

impl crate::function::Function for ArrayResultFn {
    fn name(&self) -> &'static str {
        "FP388_ARRAY_RESULT"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        static SCHEMA: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
            std::sync::LazyLock::new(|| vec![crate::args::ArgSchema::any()]);
        &SCHEMA
    }

    fn semantic_contract(
        &self,
        _arity: usize,
    ) -> Option<crate::function_contract::FunctionSemanticContract> {
        Some(crate::function_contract::FunctionSemanticContract::trusted_builtin_default(None))
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let seed = match args[0].value()?.into_literal() {
            LiteralValue::Number(n) => n,
            LiteralValue::Int(i) => i as f64,
            other => return Ok(crate::traits::CalcValue::Scalar(other)),
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
            vec![
                LiteralValue::Number(seed),
                LiteralValue::Number(seed * 10.0),
            ],
        ])))
    }
}

/// Register exactly once: every registration bumps the global function semantic
/// epoch, which invalidates the plane's cached semantics snapshot and would
/// route the workbook through the capacity fallback instead of the array-result
/// backstop under test.
fn register_once() {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| {
        crate::function_registry::register_function(Arc::new(ArrayResultFn));
    });
}

fn fixture(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    register_once();
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    engine.add_sheet("Sheet1").ok();

    let mut records = Vec::new();
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(f64::from(row)))
            .unwrap();
        let formula = format!("=FP388_ARRAY_RESULT(A{row})");
        let ast = parse(&formula).unwrap();
        let ast_id = engine.intern_formula_ast(&ast);
        records.push(FormulaIngestRecord::new(
            row,
            2,
            ast_id,
            Some(Arc::<str>::from(formula.as_str())),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .expect("ingest");
    engine
}

fn num(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<f64> {
    match engine.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(i)) => Some(i as f64),
        _ => None,
    }
}

/// The array-producing family is admitted into a span (declared scalar caps),
/// so the backstop is genuinely exercised rather than short-circuited at
/// ingest.
#[test]
fn array_producing_function_is_admitted_into_a_span() {
    let engine = fixture(FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(
        engine.baseline_stats().formula_plane_active_span_count,
        1,
        "the array-producing family must be admitted as a span before evaluation"
    );
}

/// The plane must never publish the top-left element across the span: the span
/// is demoted before anything is published and the legacy spill planner owns
/// the result, so authoritative and Off agree on every cell.
#[test]
fn array_span_result_demotes_instead_of_collapsing_to_top_left() {
    let mut authoritative = fixture(FormulaPlaneMode::AuthoritativeExperimental);
    let mut off = fixture(FormulaPlaneMode::Off);

    authoritative.evaluate_all().expect("authoritative eval");
    off.evaluate_all().expect("off eval");

    for row in 1..=ROWS {
        for col in 1..=3 {
            assert_eq!(
                authoritative.get_cell_value("Sheet1", row, col),
                off.get_cell_value("Sheet1", row, col),
                "authoritative/Off divergence at Sheet1!R{row}C{col}"
            );
        }
        // Legacy spill semantics: the anchor keeps the top-left element and the
        // second element lands in the neighbouring column. A collapsed span
        // would leave column C empty for every row.
        assert_eq!(num(&authoritative, row, 2), Some(f64::from(row)));
        assert_eq!(num(&authoritative, row, 3), Some(f64::from(row) * 10.0));
    }

    let stats = authoritative.baseline_stats();
    assert_eq!(
        stats.formula_plane_array_result_span_demotions, 1,
        "the array-producing span must be demoted exactly once"
    );
    assert_eq!(
        stats.formula_plane_active_span_count, 0,
        "the demoted span must no longer be plane-owned"
    );
    assert_eq!(
        authoritative
            .formula_ingest_report_total()
            .fallback_reasons
            .get("ArrayResult")
            .copied(),
        Some(1),
        "the ArrayResult fallback reason must be recorded in diagnostics"
    );
}

/// The demotion survives a second evaluation and an edit: no span is recreated
/// for the array-producing family, and recalculated rows keep spilling.
#[test]
fn demoted_array_span_stays_legacy_across_recalculation() {
    let mut engine = fixture(FormulaPlaneMode::AuthoritativeExperimental);
    engine.evaluate_all().expect("first eval");

    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(1_000.0))
        .unwrap();
    engine.evaluate_all().expect("second eval");

    assert_eq!(
        engine
            .baseline_stats()
            .formula_plane_array_result_span_demotions,
        1,
        "the span is demoted once; later evaluations stay on the legacy path"
    );
    assert_eq!(num(&engine, 3, 2), Some(1_000.0));
    assert_eq!(num(&engine, 3, 3), Some(10_000.0));
    assert_eq!(num(&engine, 4, 2), Some(4.0));
    assert_eq!(num(&engine, 4, 3), Some(40.0));
}
