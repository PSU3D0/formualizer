//! Multi-character wildcard semantics for lookup and criteria consumers (#284).
//!
//! Oracle: LibreOffice 24.2.7 headless recalculation (`oracle: lo-verified`).
//! LibreOffice 24.2.7 does not implement XLOOKUP/XMATCH and returns `#NAME?`;
//! those rows use an equivalent MATCH formula as the LibreOffice pattern oracle
//! and Excel's documented `match_mode=2` behavior for the function result. Its
//! xlsx import also returns `#N/A` for the issue's inline-array VLOOKUP while the
//! equivalent range formula returns 3, so that row follows Excel's result.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn build_wildcard_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let lookup_rows = [
        ("alpha", 1),
        ("beta", 2),
        ("bravo", 3),
        ("abcd", 4),
        ("abcbd", 5),
        ("br", 6),
        ("ИВАНОВИЧ", 7),
        ("*", 8),
        ("?", 9),
        ("~", 10),
        ("a*middle?", 11),
    ];

    for (row, (key, result)) in lookup_rows.iter().enumerate() {
        let row = row as u32 + 1;
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Text((*key).into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Int(*result))
            .unwrap();

        let column = row + 3;
        engine
            .set_cell_value("Sheet1", 1, column, LiteralValue::Text((*key).into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 2, column, LiteralValue::Int(*result))
            .unwrap();
    }

    for (row, (key, result)) in [("bravo", 10), ("beta", 20), ("br", 30)].iter().enumerate() {
        let row = row as u32 + 1;
        engine
            .set_cell_value("Sheet1", row, 20, LiteralValue::Text((*key).into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 21, LiteralValue::Int(*result))
            .unwrap();
    }

    for (column, header) in [(23, "name"), (24, "score"), (26, "name")] {
        engine
            .set_cell_value("Sheet1", 1, column, LiteralValue::Text(header.into()))
            .unwrap();
    }
    for (row, (key, result)) in [("bravo", 10), ("beta", 20), ("br", 30)].iter().enumerate() {
        let row = row as u32 + 2;
        engine
            .set_cell_value("Sheet1", row, 23, LiteralValue::Text((*key).into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 24, LiteralValue::Int(*result))
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 2, 26, LiteralValue::Text("br*".into()))
        .unwrap();

    engine
}

fn assert_number(value: Option<LiteralValue>, expected: f64, description: &str, oracle: &str) {
    let actual = match value {
        Some(LiteralValue::Number(value)) => value,
        Some(LiteralValue::Int(value)) => value as f64,
        other => panic!("{description} ({oracle}): expected {expected}, got {other:?}"),
    };
    assert_eq!(actual, expected, "{description} ({oracle})");
}

#[test]
fn lookup_families_match_multi_character_stars_and_escapes() {
    let cases = [
        (
            "VLOOKUP issue 284 array literal",
            "=VLOOKUP(\"br*\",{\"alpha\",1;\"beta\",2;\"bravo\",3},2,FALSE)",
            3.0,
            "oracle: Excel documented; LO xlsx inline-array divergence",
        ),
        (
            "VLOOKUP trailing star and ASCII case folding",
            "=VLOOKUP(\"BR*\",A1:B11,2,FALSE)",
            3.0,
            "oracle: lo-verified",
        ),
        (
            "HLOOKUP leading star",
            "=HLOOKUP(\"*avo\",D1:N2,2,FALSE)",
            3.0,
            "oracle: lo-verified",
        ),
        (
            "MATCH star adjacent to question mark",
            "=MATCH(\"a*?d\",A1:A11,0)",
            4.0,
            "oracle: lo-verified",
        ),
        (
            "MATCH multiple stars",
            "=MATCH(\"a**b***d\",A1:A11,0)",
            4.0,
            "oracle: lo-verified",
        ),
        (
            "MATCH star matching empty text",
            "=MATCH(\"br*\",A6:A6,0)",
            1.0,
            "oracle: lo-verified",
        ),
        (
            "XMATCH star retry after a later mismatch",
            "=XMATCH(\"*b*d\",A5:A5,2)",
            1.0,
            "oracle: lo-verified via MATCH; Excel match_mode=2",
        ),
        (
            "XLOOKUP non-ASCII case folding with multi-character star",
            "=XLOOKUP(\"ив?н*\",A7:A7,B7:B7,\"NF\",2)",
            7.0,
            "oracle: lo-verified via MATCH; Excel match_mode=2",
        ),
        (
            "escaped literal asterisk",
            "=MATCH(\"~*\",A1:A11,0)",
            8.0,
            "oracle: lo-verified",
        ),
        (
            "escaped literal question mark",
            "=MATCH(\"~?\",A1:A11,0)",
            9.0,
            "oracle: lo-verified",
        ),
        (
            "escaped literal tilde",
            "=MATCH(\"~~\",A1:A11,0)",
            10.0,
            "oracle: lo-verified",
        ),
        (
            "escaped literals combined with a real wildcard",
            "=VLOOKUP(\"a~**~?\",A1:B11,2,FALSE)",
            11.0,
            "oracle: lo-verified",
        ),
    ];

    let mut engine = build_wildcard_engine();
    for (index, (_, formula, _, _)) in cases.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", index as u32 + 1, 29, parse(*formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    for (index, (description, _, expected, oracle)) in cases.iter().enumerate() {
        assert_number(
            engine.get_cell_value("Sheet1", index as u32 + 1, 29),
            *expected,
            description,
            oracle,
        );
    }
}

/// Guards that the #284 compiled-matcher change did not regress the criteria and
/// database families, which use a SEPARATE matcher (`builtins::utils::wildcard_match`)
/// and never had the multi-character `*` defect.
///
/// This is a non-regression control ONLY. It does not certify the criteria matcher as
/// Excel-correct: that matcher ignores `~` escapes, matches `?` on bytes rather than
/// characters, has no memoization, and the Arrow criteria path leaks SQL `LIKE`
/// metacharacters. Those are pre-existing defects tracked separately in #295.
/// Note the `br*` patterns below short-circuit into the `ends_with('*')` fast path in
/// `utils.rs`, so they do not exercise the criteria matcher body at all.
#[test]
fn criteria_families_are_not_regressed_by_wildcard_star_fix() {
    let cases = [
        ("COUNTIF", "=COUNTIF(T1:T3,\"br*\")", 2.0),
        ("SUMIF", "=SUMIF(T1:T3,\"br*\",U1:U3)", 40.0),
        ("AVERAGEIF", "=AVERAGEIF(T1:T3,\"br*\",U1:U3)", 20.0),
        ("COUNTIFS", "=COUNTIFS(T1:T3,\"br*\")", 2.0),
        ("SUMIFS", "=SUMIFS(U1:U3,T1:T3,\"br*\")", 40.0),
        ("AVERAGEIFS", "=AVERAGEIFS(U1:U3,T1:T3,\"br*\")", 20.0),
        ("DCOUNT", "=DCOUNT(W1:X4,\"score\",Z1:Z2)", 2.0),
    ];

    let mut engine = build_wildcard_engine();
    for (index, (_, formula, _)) in cases.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", index as u32 + 1, 29, parse(*formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    for (index, (description, _, expected)) in cases.iter().enumerate() {
        assert_number(
            engine.get_cell_value("Sheet1", index as u32 + 1, 29),
            *expected,
            description,
            "oracle: lo-verified independent-matcher control",
        );
    }
}
