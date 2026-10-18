//! Long decimal columns reduce exactly.
//!
//! Plain `f64` accumulation of a column of `19.99` drifts by about an ulp
//! per row (500 rows: `SUM` reads 9994.99999999998), and the drift grows
//! with row count, so it reaches the 15 significant digits a spreadsheet
//! displays. Excel compensates its reductions; so does every reduction
//! here, on every evaluation path (scalar builtin, family-lifted member,
//! tier-3 range kernel, parallel or not).

use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const ROWS: u32 = 500;
const VALUE: f64 = 19.99;
const EXACT_SUM: f64 = 9995.0;
/// Below the data and the database, so formulas never overlap inputs.
const FORMULA_ROW: u32 = ROWS + 100;

/// `(formula, exact result)` over columns A1:A500 of 19.99, B1:B500 of 1,
/// C1:C500 of 1.1 and G1:G500 of 1.01.
fn cases() -> Vec<(String, f64)> {
    let a = format!("A1:A{ROWS}");
    let b = format!("B1:B{ROWS}");
    let c = format!("C1:C{ROWS}");
    vec![
        (format!("=SUM({a})"), EXACT_SUM),
        (format!("=SUM({a}, {a})"), 2.0 * EXACT_SUM),
        (format!("=AVERAGE({a})"), VALUE),
        (format!("=SUMIF({a},\">0\")"), EXACT_SUM),
        (format!("=SUMIFS({a},{b},1)"), EXACT_SUM),
        (format!("=AVERAGEIF({a},\">0\")"), VALUE),
        (format!("=AVERAGEIFS({a},{b},1)"), VALUE),
        (format!("=SUMPRODUCT({a},{b})"), EXACT_SUM),
        // The squares are inexact too. `1.1_f64` is 1.1000000000000000888…,
        // so the correctly rounded 500 × 1.1² is one ulp above 605.
        (format!("=SUMSQ({c})"), 605.000_000_000_000_1),
        (format!("=INDEX(GROUPBY({b},{a},\"SUM\",0),1,2)"), EXACT_SUM),
        (format!("=INDEX(GROUPBY({b},{a},\"AVERAGE\",0),1,2)"), VALUE),
        // Database F1:F501 ("Amt" header + the 500 values); criteria H1:H2
        // match every row.
        (format!("=DSUM(F1:F{},\"Amt\",H1:H2)", ROWS + 1), EXACT_SUM),
        (format!("=DAVERAGE(F1:F{},\"Amt\",H1:H2)", ROWS + 1), VALUE),
        (format!("=DVARP(F1:F{},\"Amt\",H1:H2)", ROWS + 1), 0.0),
        // A constant column has zero spread only if its mean is exact.
        (format!("=VAR.P({a})"), 0.0),
        (format!("=STDEV.P({a})"), 0.0),
        (format!("=STDEV.S({a})"), 0.0),
        (format!("=AVEDEV({a})"), 0.0),
        (format!("=DEVSQ({a})"), 0.0),
        (format!("=INDEX(GROUPBY({b},{a},\"VAR.P\",0),1,2)"), 0.0),
        (format!("=COVARIANCE.P({a},{c})"), 0.0),
        (format!("=DEVSQ({c})"), 0.0),
        (format!("=SUBTOTAL(9,{a})"), EXACT_SUM),
        (format!("=SUBTOTAL(1,{a})"), VALUE),
        (format!("=AGGREGATE(9,4,{a})"), EXACT_SUM),
        (format!("=AGGREGATE(1,4,{a})"), VALUE),
        // Correctly rounded 500 × 19.99² + 500 (19.99² is inexact).
        (format!("=SUMX2PY2({a},{b})"), 200_300.049_999_999_96),
        (format!("=SUMXMY2({a},{a})"), 0.0),
        // Products lose about an ulp per factor too. Column G holds 1.01;
        // these are the correctly rounded 1.01^500 and 1.01^200.
        (format!("=PRODUCT(G1:G{ROWS})"), 144.772_772_432_573_96),
        (format!("=SUBTOTAL(6,G1:G{ROWS})"), 144.772_772_432_573_96),
        ("=PRODUCT(G1:G200)".to_string(), 7.316_017_851_829_954),
        (
            format!("=INDEX(GROUPBY({b},G1:G{ROWS},\"PRODUCT\",0),1,2)"),
            144.772_772_432_573_96,
        ),
    ]
}

fn engine(family: bool, parallel: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        family_execution: family,
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    Engine::new(TestWorkbook::new(), config)
}

fn evaluate(family: bool, parallel: bool) -> Vec<(String, LiteralValue)> {
    let mut e = engine(family, parallel);
    for (row, col) in [(1, 6), (1, 8)] {
        e.set_cell_value("Sheet1", row, col, LiteralValue::Text("Amt".into()))
            .unwrap();
    }
    e.set_cell_value("Sheet1", 2, 8, LiteralValue::Text(">0".into()))
        .unwrap();
    for r in 1..=ROWS {
        e.set_cell_value("Sheet1", r + 1, 6, LiteralValue::Number(VALUE))
            .unwrap();
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(VALUE))
            .unwrap();
        e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(1.0))
            .unwrap();
        e.set_cell_value("Sheet1", r, 3, LiteralValue::Number(1.1))
            .unwrap();
        e.set_cell_value("Sheet1", r, 7, LiteralValue::Number(1.01))
            .unwrap();
    }
    let cases = cases();
    for (i, (formula, _)) in cases.iter().enumerate() {
        e.set_cell_formula("Sheet1", FORMULA_ROW, 4 + i as u32, parse(formula).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    cases
        .into_iter()
        .enumerate()
        .map(|(i, (formula, _))| {
            let v = e
                .get_cell_value("Sheet1", FORMULA_ROW, 4 + i as u32)
                .unwrap();
            (formula, v)
        })
        .collect()
}

fn assert_exact(family: bool, parallel: bool) {
    let expected = cases();
    for ((formula, got), (_, exact)) in evaluate(family, parallel).into_iter().zip(expected) {
        assert_eq!(
            got,
            LiteralValue::Number(exact),
            "{formula} (family={family}, parallel={parallel})"
        );
    }
}

#[test]
fn reductions_over_long_decimal_column_are_exact_scalar_path() {
    assert_exact(false, false);
}

#[test]
fn reductions_over_long_decimal_column_are_exact_family_path() {
    assert_exact(true, false);
    assert_exact(true, true);
}

/// A column with one blank in every chunk takes the nullable lane path.
#[test]
fn reductions_skip_blanks_and_stay_exact() {
    let mut e = engine(true, false);
    for r in 1..=ROWS {
        if r % 64 != 0 {
            e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(VALUE))
                .unwrap();
        }
    }
    let filled = f64::from(ROWS - ROWS / 64);
    e.set_cell_formula("Sheet1", 1, 4, parse(format!("=SUM(A1:A{ROWS})")).unwrap())
        .unwrap();
    e.set_cell_formula(
        "Sheet1",
        1,
        5,
        parse(format!("=AVERAGE(A1:A{ROWS})")).unwrap(),
    )
    .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(filled * VALUE))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Number(VALUE))
    );
}
