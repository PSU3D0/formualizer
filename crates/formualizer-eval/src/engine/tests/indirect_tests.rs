//! Tests for INDIRECT function — both runtime evaluation and constant-folding.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse as parse_formula;
use formualizer_parse::parser::ASTNode;

fn parse(formula: &str) -> ASTNode {
    parse_formula(formula).unwrap()
}

// ── Runtime INDIRECT (set_cell_formula path) ──────────────────────────

#[test]
fn indirect_literal_string_returns_cell_value() {
    // =INDIRECT("A1") where A1 = 42
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=INDIRECT(\"A1\")"))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn indirect_literal_string_range() {
    // =SUM(INDIRECT("A1:A3"))
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(30.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 1, parse("=SUM(INDIRECT(\"A1:A3\"))"))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(60.0))
    );
}

#[test]
fn indirect_invalid_ref_string_returns_error() {
    // =INDIRECT("!!!") should return an error (invalid reference string)
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"!!!\")"))
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(_)) => {} // Any error is acceptable
        other => panic!("expected error, got {other:?}"),
    }
}

// ── Constant-folding via bulk ingest path ─────────────────────────────

#[test]
fn indirect_constant_fold_with_concatenation() {
    // B2 = "Sheet1", formula on C1 = INDIRECT("'"&B2&"'!A1")
    // This should fold at ingest time since B2 is a plain value.
    let cfg = EvalConfig::default();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // Set up static value cells via Arrow store
    {
        let mut aib = engine.begin_bulk_ingest_arrow();
        aib.add_sheet("Sheet1", 3, 1024);
        // Row 1: A1=100
        aib.append_row(
            "Sheet1",
            &[LiteralValue::Number(100.0), LiteralValue::Empty, LiteralValue::Empty],
        )
        .unwrap();
        // Row 2: A2=_, B2="Sheet1"
        aib.append_row(
            "Sheet1",
            &[
                LiteralValue::Empty,
                LiteralValue::Text("Sheet1".to_string()),
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        aib.finish().unwrap();
    }

    // Stage formula via bulk ingest (this triggers the folding pass)
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    // =INDIRECT("'"&B2&"'!A1")
    builder.add_formulas(
        sheet,
        vec![(1, 3, parse("=INDIRECT(\"'\"&B2&\"'!A1\")"))],
    );
    builder.finish().unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn indirect_constant_fold_cell_ref_building_column() {
    // R1 = "C", Q1 = "5", formula = INDIRECT(A1&B1)
    // This constructs "C5" from plain value cells.
    let cfg = EvalConfig::default();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    {
        let mut aib = engine.begin_bulk_ingest_arrow();
        aib.add_sheet("Sheet1", 5, 1024);
        // Row 1: A1="C", B1="5", C1=_, D1=_, E1=_
        aib.append_row(
            "Sheet1",
            &[
                LiteralValue::Text("C".to_string()),
                LiteralValue::Text("5".to_string()),
                LiteralValue::Empty,
                LiteralValue::Empty,
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        // Rows 2-5
        for _ in 0..4 {
            aib.append_row(
                "Sheet1",
                &[LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty],
            )
            .unwrap();
        }
        aib.finish().unwrap();
    }

    // Set C5 = 999
    engine
        .set_cell_value("Sheet1", 5, 3, LiteralValue::Number(999.0))
        .unwrap();

    // Stage formula: D1 = INDIRECT(A1&B1) → should fold to =C5
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formulas(sheet, vec![(1, 4, parse("=INDIRECT(A1&B1)"))]);
    builder.finish().unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(999.0))
    );
}

#[test]
fn indirect_cross_sheet_via_bulk_ingest() {
    // INDIRECT("'Other Sheet'!A1") with cross-sheet reference
    let cfg = EvalConfig::default();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    {
        let mut aib = engine.begin_bulk_ingest_arrow();
        aib.add_sheet("Sheet1", 2, 1024);
        aib.add_sheet("Other Sheet", 2, 1024);
        aib.append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Empty])
            .unwrap();
        aib.append_row(
            "Other Sheet",
            &[LiteralValue::Number(777.0), LiteralValue::Empty],
        )
        .unwrap();
        aib.finish().unwrap();
    }

    // Stage formula on Sheet1: =INDIRECT("'Other Sheet'!A1")
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formulas(
        sheet,
        vec![(1, 1, parse("=INDIRECT(\"'Other Sheet'!A1\")"))],
    );
    builder.finish().unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(777.0))
    );
}

#[test]
fn indirect_with_formula_upstream_does_not_fold() {
    // A1 has a formula (=B1), so INDIRECT referencing A1's value should NOT fold.
    // The runtime IndirectFn should still resolve it (assuming the formula produces a valid ref string).
    let cfg = EvalConfig::default();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    {
        let mut aib = engine.begin_bulk_ingest_arrow();
        aib.add_sheet("Sheet1", 3, 1024);
        // Row 1: A1=_, B1="A3"
        aib.append_row(
            "Sheet1",
            &[
                LiteralValue::Empty,
                LiteralValue::Text("A3".to_string()),
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        // Row 2: empty
        aib.append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty])
            .unwrap();
        // Row 3: A3=500
        aib.append_row(
            "Sheet1",
            &[LiteralValue::Number(500.0), LiteralValue::Empty, LiteralValue::Empty],
        )
        .unwrap();
        aib.finish().unwrap();
    }

    // A1 = =B1 (formula, not static value)
    // C1 = =INDIRECT(A1) — A1 is a formula cell, so folding should be skipped
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formulas(
        sheet,
        vec![
            (1, 1, parse("=B1")),       // A1 = =B1 (produces "A3")
            (1, 3, parse("=INDIRECT(A1)")), // C1 = =INDIRECT(A1)
        ],
    );
    builder.finish().unwrap();

    engine.evaluate_all().unwrap();

    // A1 should evaluate to "A3" (the text value from B1)
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("A3".to_string()))
    );

    // C1: INDIRECT(A1) → INDIRECT("A3") → value of A3 = 500
    // Even without folding, the runtime IndirectFn should handle this
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(500.0))
    );
}

#[test]
fn indirect_inside_sum_with_constant_fold() {
    // =SUM(INDIRECT("A1:A3")) via bulk ingest with literal string
    let cfg = EvalConfig::default();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    {
        let mut aib = engine.begin_bulk_ingest_arrow();
        aib.add_sheet("Sheet1", 1, 1024);
        aib.append_row("Sheet1", &[LiteralValue::Number(10.0)])
            .unwrap();
        aib.append_row("Sheet1", &[LiteralValue::Number(20.0)])
            .unwrap();
        aib.append_row("Sheet1", &[LiteralValue::Number(30.0)])
            .unwrap();
        aib.append_row("Sheet1", &[LiteralValue::Empty]).unwrap();
        aib.finish().unwrap();
    }

    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formulas(
        sheet,
        vec![(4, 1, parse("=SUM(INDIRECT(\"A1:A3\"))"))],
    );
    builder.finish().unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(60.0))
    );
}

#[test]
fn indirect_xlfn_prefix_handled() {
    // Some Excel exports prefix as _xlfn.INDIRECT
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=_xlfn.INDIRECT(\"A1\")"))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(42.0))
    );
}
