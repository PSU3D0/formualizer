//! Extended coverage for issue #158 (`<>X` criteria must count blank cells),
//! complementing the basic repro in `sumifs_ne_blank_158.rs` (landed in #160).
//!
//! The core fix (Arrow `nilike` returns NULL for blank inputs, so `<>X` dropped
//! blanks) is bounded to the criteria range's materialized/used region: a finite
//! range counts its interior blanks, but a whole-column `A:A` reference must NOT
//! count the ~1M trailing empty cells. These tests pin that boundary plus the
//! surrounding predicate semantics on blanks (AVERAGEIFS, `<>`, `""`/`"="`,
//! numeric comparisons, wildcards, multi-criteria).

use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn num(engine: &mut Engine<TestWorkbook>, formula: &str) -> LiteralValue {
    let ast = parse(formula).unwrap();
    // Put the formula away from the data columns/rows so whole-column refs are
    // not self-inclusive (would be circular per #120).
    engine.set_cell_formula("Sheet1", 1, 20, ast).unwrap();
    engine.evaluate_cell("Sheet1", 1, 20).unwrap();
    engine.get_cell_value("Sheet1", 1, 20).unwrap()
}

/// A1="Debt" B1=10 ; A2=blank B2=20 ; A3="Equity" B3=30
fn setup_finite(engine: &mut Engine<TestWorkbook>) {
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("Debt".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(10.0))
        .unwrap();
    // Row 2 col A intentionally left blank.
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Text("Equity".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 2, LiteralValue::Number(30.0))
        .unwrap();
}

#[test]
fn averageifs_ne_blank_includes_blank_row() {
    // AVERAGEIFS over the two matching rows: blank (20) + Equity (30) => 25.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    setup_finite(&mut engine);
    assert_eq!(
        num(&mut engine, "=AVERAGEIFS(B1:B3, A1:A3, \"<>Debt\")"),
        LiteralValue::Number(25.0),
        "AVERAGEIFS <>Debt must average the blank + Equity rows"
    );
}

#[test]
fn ne_empty_operator_does_not_count_blanks() {
    // `<>` (empty pattern) means "non-blank": a blank cell must NOT match.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    setup_finite(&mut engine);
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"<>\")"),
        LiteralValue::Number(2.0),
        "<> must count non-blank cells only"
    );
    assert_eq!(
        num(&mut engine, "=SUMIFS(B1:B3, A1:A3, \"<>\")"),
        LiteralValue::Number(40.0),
        "<> sums Debt+Equity rows (10+30), not the blank row"
    );
}

#[test]
fn empty_string_and_equality_semantics_on_blanks() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    setup_finite(&mut engine);
    // "" and "=" match blanks (blank == empty string).
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"\")"),
        LiteralValue::Number(1.0),
        "empty-string criteria matches the single blank cell"
    );
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"=\")"),
        LiteralValue::Number(1.0),
        "\"=\" matches the single blank cell"
    );
    // "Debt" / "=Debt" match the populated cell only, never the blank.
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"Debt\")"),
        LiteralValue::Number(1.0),
    );
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"=Debt\")"),
        LiteralValue::Number(1.0),
    );
}

#[test]
fn numeric_predicates_still_exclude_blanks() {
    // Excel does not count blanks for numeric comparisons like >0; confirm the
    // #158 fix (scoped to the `<>` text path) leaves this unchanged.
    //   A1=5 A2=blank A3=-3
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    // A2 blank.
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(-3.0))
        .unwrap();
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \">0\")"),
        LiteralValue::Number(1.0),
        ">0 must not count the blank cell"
    );
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"<5\")"),
        LiteralValue::Number(1.0),
        "<5 counts only -3, not the blank"
    );
}

#[test]
fn wildcard_on_blanks_matches_excel() {
    //   A1="apple" A2=blank A3="apricot"
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("apple".into()))
        .unwrap();
    // A2 blank.
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Text("apricot".into()))
        .unwrap();
    // "ap*" matches the two populated cells; blank does not match a wildcard.
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A3, \"ap*\")"),
        LiteralValue::Number(2.0),
    );
}

#[test]
fn multi_criteria_ne_with_blanks() {
    // Combine a `<>` criterion (that should include a blank) with a second
    // numeric criterion. Row 2 has a blank category but a matching amount.
    //   A (category): "Debt", blank, "Equity", "Debt"
    //   B (amount):    10,      20,    30,       40
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let cats = [Some("Debt"), None, Some("Equity"), Some("Debt")];
    let amts = [10.0, 20.0, 30.0, 40.0];
    for (i, (cat, amt)) in cats.iter().zip(amts.iter()).enumerate() {
        let row = (i + 1) as u32;
        if let Some(c) = cat {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Text((*c).into()))
                .unwrap();
        }
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(*amt))
            .unwrap();
    }
    // <>Debt AND amount >= 20 : rows 2 (blank, 20) and 3 (Equity, 30) qualify.
    assert_eq!(
        num(
            &mut engine,
            "=SUMIFS(B1:B4, A1:A4, \"<>Debt\", B1:B4, \">=20\")"
        ),
        LiteralValue::Number(50.0),
        "multi-criteria <> must include the blank-category row"
    );
    assert_eq!(
        num(&mut engine, "=COUNTIFS(A1:A4, \"<>Debt\", B1:B4, \">=20\")"),
        LiteralValue::Number(2.0),
    );
}

#[test]
fn whole_column_ne_does_not_explode() {
    // Whole-column `A:A` reference: `<>Debt` must be bounded to the used region,
    // NOT count the ~1M trailing empty cells. Uses bulk-ingest so the cached
    // Arrow criteria-mask path (build_criteria_mask/compute_criteria_mask) runs.
    let mut cfg = arrow_eval_config();
    cfg.enable_parallel = false;
    cfg.range_expansion_limit = 2_000_000; // allow whole-column expansion
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    // Populate 128 rows across two chunks: category col A, amount col B.
    // Interior blanks in A at a few rows; last populated row sets the used region.
    let chunk_rows = 64usize;
    let total_rows = 128u32;
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 2, chunk_rows);
        for i in 0..total_rows {
            // "Debt" every 4th row; blank at rows where i%4==1; else "Equity".
            let a = match i % 4 {
                0 => LiteralValue::Text("Debt".into()),
                1 => LiteralValue::Empty, // interior blank
                _ => LiteralValue::Text("Equity".into()),
            };
            let b = LiteralValue::Int((i + 1) as i64);
            ab.append_row("Sheet1", &[a, b]).unwrap();
        }
        ab.finish().unwrap();
    }

    // i%4 == 0 => Debt (32 rows). Everything else is <>Debt: 128 - 32 = 96 rows
    // (including the 32 interior blanks). It must NOT explode to ~1,048,544.
    let count = num(&mut engine, "=COUNTIFS(A:A, \"<>Debt\")");
    assert_eq!(
        count,
        LiteralValue::Number(96.0),
        "whole-column <>Debt must count only used-region rows (incl. interior blanks), not 1M blanks"
    );

    // And the interior blanks ARE included: compare against the same finite range.
    let finite = num(&mut engine, "=COUNTIFS(A1:A128, \"<>Debt\")");
    assert_eq!(finite, LiteralValue::Number(96.0));
}
