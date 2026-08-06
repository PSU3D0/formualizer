use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Expected {
    Number(f64),
    Boolean(bool),
    Text(&'static str),
    Error(ExcelErrorKind),
}

fn eval_formula(formula: &str) -> LiteralValue {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 1)
        .unwrap_or(LiteralValue::Empty)
}

fn assert_expected(formula: &str, oracle: &str, expected: Expected) {
    let actual = eval_formula(formula);
    match expected {
        Expected::Number(value) => {
            assert_eq!(actual, LiteralValue::Number(value), "{formula} ({oracle})")
        }
        Expected::Boolean(value) => {
            assert_eq!(actual, LiteralValue::Boolean(value), "{formula} ({oracle})")
        }
        Expected::Text(value) => assert_eq!(
            actual,
            LiteralValue::Text(value.to_string()),
            "{formula} ({oracle})"
        ),
        Expected::Error(kind) => match actual {
            LiteralValue::Error(error) => assert_eq!(error.kind, kind, "{formula} ({oracle})"),
            other => panic!("{formula} ({oracle}): expected {kind:?}, got {other:?}"),
        },
    }
}

#[test]
fn omitted_argument_oracle_table() {
    // Each row is tagged with its evidence source from issue #277's oracle table.
    let cases = [
        (
            "=IFERROR(1/0,)",
            "oracle: lo-verified",
            Expected::Number(0.0),
        ),
        (
            "=IFERROR(1/0,\"\")",
            "oracle: lo-verified",
            Expected::Text(""),
        ),
        ("=IF(TRUE,,5)", "oracle: lo-verified", Expected::Number(0.0)),
        (
            "=IF(FALSE,5,)",
            "oracle: lo-verified",
            Expected::Number(0.0),
        ),
        (
            "=CHOOSE(1,,5)",
            "oracle: lo-verified",
            Expected::Number(0.0),
        ),
        ("=SUM(1,)", "oracle: lo-verified", Expected::Number(1.0)),
        ("=MAX(-5,)", "oracle: lo-verified", Expected::Number(0.0)),
        ("=MIN(5,)", "oracle: lo-verified", Expected::Number(0.0)),
        ("=AVERAGE(2,)", "oracle: lo-verified", Expected::Number(1.0)),
        ("=PRODUCT(2,)", "oracle: lo-verified", Expected::Number(0.0)),
        ("=COUNT(1,)", "oracle: lo-verified", Expected::Number(2.0)),
        ("=COUNTA(1,)", "oracle: lo-verified", Expected::Number(2.0)),
        (
            "=AND(TRUE,)",
            "oracle: lo-verified",
            Expected::Boolean(false),
        ),
        ("=OR(TRUE,)", "oracle: lo-verified", Expected::Boolean(true)),
        (
            "=OR(FALSE,)",
            "oracle: lo-verified",
            Expected::Boolean(false),
        ),
        ("=ROUND(1.6,)", "oracle: lo-verified", Expected::Number(2.0)),
        (
            "=ROUNDUP(1.1,)",
            "oracle: lo-verified",
            Expected::Number(2.0),
        ),
        (
            "=MOD(5,)",
            "oracle: lo-verified",
            Expected::Error(ExcelErrorKind::Div),
        ),
        ("=POWER(2,)", "oracle: lo-verified", Expected::Number(1.0)),
        ("=LEFT(\"abc\",)", "oracle: lo-verified", Expected::Text("")),
        (
            "=RIGHT(\"abc\",)",
            "oracle: lo-verified",
            Expected::Text(""),
        ),
        (
            "=MID(\"abc\",,2)",
            "oracle: lo-verified",
            Expected::Error(ExcelErrorKind::Value),
        ),
        ("=REPT(\"x\",)", "oracle: lo-verified", Expected::Text("")),
        (
            "=FIND(\"a\",\"abc\",)",
            "oracle: lo-verified",
            Expected::Error(ExcelErrorKind::Value),
        ),
        (
            "=CONCATENATE(\"a\",)",
            "oracle: lo-verified",
            Expected::Text("a"),
        ),
        (
            "=MATCH(2,{1,2,3},)",
            "oracle: lo-verified",
            Expected::Number(2.0),
        ),
        (
            "=INDEX({1,2;3,4},,2)",
            "oracle: lo-verified",
            Expected::Number(2.0),
        ),
        ("=IFS(TRUE,)", "oracle: uniform-rule", Expected::Number(0.0)),
        (
            "=SWITCH(1,1,)",
            "oracle: uniform-rule",
            Expected::Number(0.0),
        ),
        (
            "=CONCAT(\"a\",)",
            "oracle: uniform-rule",
            Expected::Text("a"),
        ),
        (
            "=TEXTJOIN(\",\",TRUE,\"a\",)",
            "oracle: uniform-rule",
            Expected::Text("a"),
        ),
        (
            "=XMATCH(2,{1,2,3},)",
            "oracle: uniform-rule",
            Expected::Number(2.0),
        ),
        (
            "=IFNA(NA(),)",
            "oracle: uniform-rule",
            Expected::Number(0.0),
        ),
        (
            "=REPLACE(\"abc\",1,1,)",
            "oracle: lo-verified",
            Expected::Text("bc"),
        ),
        (
            "=SUBSTITUTE(\"a0b\",\"0\",)",
            "oracle: lo-verified",
            Expected::Text("ab"),
        ),
        (
            "=SUBSTITUTE(\"a0b\",,\"x\")",
            "oracle: lo-verified",
            Expected::Text("a0b"),
        ),
        ("=LEFT(,2)", "oracle: lo-verified", Expected::Text("")),
        ("=RIGHT(,2)", "oracle: lo-verified", Expected::Text("")),
        ("=MID(,1,2)", "oracle: lo-verified", Expected::Text("")),
        (
            "=EXACT(\"\",)",
            "oracle: lo-verified",
            Expected::Boolean(true),
        ),
        ("=EXACT(,)", "oracle: lo-verified", Expected::Boolean(true)),
        ("=TEXT(1,)", "oracle: lo-verified", Expected::Text("")),
        (
            "=TEXT(1,\"\")",
            "oracle: lo-verified explicit-empty control",
            Expected::Text(""),
        ),
        ("=REPT(,2)", "oracle: lo-verified", Expected::Text("")),
        (
            "=SUBSTITUTE(\"abc\",\"b\",,)",
            "oracle: lo-verified",
            Expected::Error(ExcelErrorKind::Value),
        ),
        (
            "=FIND(,\"abc\")",
            "oracle: uniform-rule",
            Expected::Number(1.0),
        ),
        (
            "=FIND(\"\",\"abc\")",
            "oracle: uniform-rule explicit-empty control",
            Expected::Number(1.0),
        ),
        (
            "=SEARCH(,\"abc\")",
            "oracle: uniform-rule",
            Expected::Number(1.0),
        ),
        (
            "=SEARCH(\"\",\"abc\")",
            "oracle: uniform-rule explicit-empty control",
            Expected::Number(1.0),
        ),
        (
            "=VALUE(,)",
            "oracle: uniform-rule",
            Expected::Error(ExcelErrorKind::Value),
        ),
        ("=LEFT(0,2)", "negative control", Expected::Text("0")),
        (
            "=EXACT(\"\",0)",
            "negative control",
            Expected::Boolean(false),
        ),
    ];

    for (formula, oracle, expected) in cases {
        assert_expected(formula, oracle, expected);
    }
}

#[test]
fn omitted_find_text_matches_explicit_empty_text() {
    for name in ["FIND", "SEARCH"] {
        assert_eq!(
            eval_formula(&format!("={name}(,\"abc\")")),
            eval_formula(&format!("={name}(\"\",\"abc\")")),
            "{name} must apply the same text coercion to omission and explicit empty text"
        );
    }
}

#[test]
fn lazy_branch_omission_remains_distinct_from_explicit_empty_text() {
    assert_eq!(eval_formula("=IF(TRUE,,)"), LiteralValue::Number(0.0));
    assert_eq!(
        eval_formula("=IF(TRUE,\"\",)"),
        LiteralValue::Text(String::new())
    );
}

#[test]
fn omitted_arguments_remain_distinct_from_blank_references() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, formula) in [
        (1, "=COUNT(1,A100)"),
        (2, "=COUNT(1,)"),
        (3, "=AVERAGE(2,A100)"),
        (4, "=AVERAGE(2,)"),
    ] {
        engine
            .set_cell_formula("Sheet1", row, 1, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn absent_text_default_remains_distinct_from_omission() {
    assert_eq!(
        eval_formula("=LEFT(\"abc\")"),
        LiteralValue::Text("a".into())
    );
    assert_eq!(
        eval_formula("=LEFT(\"abc\",)"),
        LiteralValue::Text(String::new())
    );
}

#[test]
fn omitted_lazy_branch_does_not_evaluate_the_unselected_error() {
    assert_eq!(eval_formula("=IF(FALSE,1/0,)"), LiteralValue::Number(0.0));
    assert_eq!(eval_formula("=IF(TRUE,,1/0)"), LiteralValue::Number(0.0));
}

#[test]
fn offset_omitted_dimensions_retain_source_dimensions() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, col, value) in [(1, 1, 1), (1, 2, 2), (2, 1, 3), (2, 2, 4)] {
        engine
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(value))
            .unwrap();
    }
    for (row, formula) in [
        (1, "=SUM(OFFSET(A1:B2,0,0,,))"),
        (2, "=SUM(OFFSET(A1:B2,0,0,2,2))"),
        (3, "=SUM(OFFSET(A1:B2,0,0,0,0))"),
    ] {
        engine
            .set_cell_formula("Sheet1", row, 4, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 4),
        Some(LiteralValue::Number(10.0))
    );
    assert!(matches!(
        engine.get_cell_value("Sheet1", 3, 4),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Ref
    ));
}

#[test]
fn match_absent_and_omitted_use_different_modes() {
    let absent = eval_formula("=MATCH(2,{2,1,3})");
    let omitted = eval_formula("=MATCH(2,{2,1,3},)");
    assert_eq!(omitted, LiteralValue::Number(1.0));
    assert_ne!(absent, omitted);
}

fn ingest_omitted_formula_set(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    let formulas = [
        "=SUM(1,)",
        "=IF(TRUE,,5)",
        "=ROUND(1.6,)",
        "=CONCATENATE(\"a\",)",
    ];
    let mut records = Vec::new();
    for (column, formula) in formulas.iter().enumerate() {
        for row in 1..=20 {
            let ast = parse(formula).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            records.push(FormulaIngestRecord::new(
                row,
                column as u32 + 1,
                ast_id,
                Some(Arc::<str>::from(*formula)),
            ));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn formula_plane_omitted_argument_values_match_legacy_evaluation() {
    let legacy = ingest_omitted_formula_set(FormulaPlaneMode::Off);
    let formula_plane = ingest_omitted_formula_set(FormulaPlaneMode::AuthoritativeExperimental);
    assert!(
        formula_plane
            .baseline_stats()
            .formula_plane_active_span_count
            > 0
    );
    for row in 1..=20 {
        for col in 1..=4 {
            assert_eq!(
                formula_plane.get_cell_value("Sheet1", row, col),
                legacy.get_cell_value("Sheet1", row, col),
                "FormulaPlane mismatch at ({row}, {col})"
            );
        }
    }
}

#[derive(Debug)]
struct OmissionProbeFn;

impl crate::function::Function for OmissionProbeFn {
    fn name(&self) -> &'static str {
        "ISSUE277_OMISSION_PROBE"
    }

    fn variadic(&self) -> bool {
        true
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        let omitted = args.iter().filter(|arg| arg.is_omitted()).count() as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            args.len() as i64 * 100 + omitted,
        )))
    }
}

#[test]
fn custom_functions_can_observe_omission_without_conflating_other_states() {
    crate::function_registry::register_function(Arc::new(OmissionProbeFn));
    assert_eq!(
        eval_formula("=ISSUE277_OMISSION_PROBE()"),
        LiteralValue::Number(0.0)
    );
    assert_eq!(
        eval_formula("=ISSUE277_OMISSION_PROBE(\"\")"),
        LiteralValue::Number(100.0)
    );
    assert_eq!(
        eval_formula("=ISSUE277_OMISSION_PROBE(,)"),
        LiteralValue::Number(202.0)
    );
}
