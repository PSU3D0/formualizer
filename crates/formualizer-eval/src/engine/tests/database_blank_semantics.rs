//! Database-function semantics for explicit empty text vs genuinely blank fields (#281).
//!
//! Oracle: LibreOffice 24.2.7 headless recalculation (`oracle: lo-verified`).
//! Note: LO reports DGET's multiple-match error as `#VALUE!` on xlsx export while
//! Excel documents `#NUM!`; the engine follows Excel, so LO must not be used as an
//! oracle for DGET error *kinds* (adversarial review N1).

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

/// Builds the #281 database grid: headers `k`,`v` in F1:G1, keys 1..=4 in F2:F5,
/// `v` values: "x", explicit `=""`, then either "y" at row 4 (compact) or a
/// genuinely blank field at row 4 (k=3) with "y" at row 5 (blank-row variant).
fn build_database(engine: &mut Engine<TestWorkbook>, include_blank_row: bool) {
    for (column, value) in [(6, "k"), (7, "v")] {
        engine
            .set_cell_value("Sheet1", 1, column, LiteralValue::Text(value.into()))
            .unwrap();
    }
    for (row, key) in [(2, 1), (3, 2), (4, 3), (5, 4)] {
        engine
            .set_cell_value("Sheet1", row, 6, LiteralValue::Int(key))
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 2, 7, LiteralValue::Text("x".into()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 7, parse("=\"\"").unwrap())
        .unwrap();
    let y_row = if include_blank_row { 5 } else { 4 };
    engine
        .set_cell_value("Sheet1", y_row, 7, LiteralValue::Text("y".into()))
        .unwrap();
    // Criteria blocks: I (k>0), K (k=2), L (k>2), M (k=3).
    for (column, header, criterion) in [
        (9, "k", LiteralValue::Text(">0".into())),
        (11, "k", LiteralValue::Int(2)),
        (12, "k", LiteralValue::Text(">2".into())),
        (13, "k", LiteralValue::Int(3)),
    ] {
        engine
            .set_cell_value("Sheet1", 1, column, LiteralValue::Text(header.into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 2, column, criterion)
            .unwrap();
    }
}

#[test]
fn database_functions_distinguish_empty_text_from_blank_fields() {
    let cases = [
        (
            "explicit empty text counts",
            false,
            "=DGET(F1:G4,\"v\",K1:K2)",
            "",
        ),
        (
            "matching blank field is skipped by DGET",
            true,
            "=DGET(F1:G5,\"v\",L1:L2)",
            "y",
        ),
    ];

    for (description, include_blank_row, dget_formula, dget_expected) in cases {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        build_database(&mut engine, include_blank_row);

        let end_row = if include_blank_row { 5 } else { 4 };
        engine
            .set_cell_formula(
                "Sheet1",
                1,
                1,
                parse(format!("=DCOUNTA(F1:G{end_row},\"v\",I1:I2)")).unwrap(),
            )
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 2, 1, parse(dget_formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();

        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(3.0)),
            "{description}: DCOUNTA"
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 2, 1),
            Some(LiteralValue::Text(dget_expected.to_string())),
            "{description}: DGET"
        );
    }
}

#[test]
fn dget_single_match_with_blank_field_returns_value_error() {
    // oracle: lo-verified — the only matching record (k=3) has a genuinely blank
    // field, LO/Excel skip it, leaving zero matches -> #VALUE!.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    build_database(&mut engine, true);
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=DGET(F1:G5,\"v\",M1:M2)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Value),
        other => panic!("expected #VALUE! for blank-only single match, got {other:?}"),
    }
}
