//! A text criterion matches the same cells whichever path evaluates it. The
//! criteria mask compares lowered text with Arrow `ilike`, which for
//! non-ASCII text is a regex under Unicode simple case folding (`ος` = `οσ`,
//! `s` = `ſ`) whose `_` is one character. Rows the mask does not cover go
//! through `criteria_match`: a 1x1 `COUNTIFS` range, `MAXIFS`, and the mask's
//! own scalar branch for a wildcard over a column that holds a number.

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

fn n(v: f64) -> LiteralValue {
    LiteralValue::Number(v)
}

fn t(s: &str) -> LiteralValue {
    LiteralValue::Text(s.into())
}

#[test]
fn text_criteria_fold_case_as_the_mask_does() {
    let texts = [
        "οσ", "ΟΣ", "ος", "Σ", "σ", "ς", "ſ", "s", "S", "\u{212A}", "k", "β", "ϐ", "ß", "ẞ", "İ",
        "i", "café", "CAFÉ", "cafe", "£m", "Ǆ", "ǅ", "ǆ", "é", "x",
    ];
    let last = texts.len() as u32;
    // Equality, inequality and wildcards; `<>` is left out of the mixed
    // column, where the number meets it.
    let literals = [
        "ΟΣ", "ος", "σ", "s", "ſ", "ϐ", "K", "ẞ", "CAFÉ", "ǆ", "<>ος", "<>ſ", "caf?", "?", "ο?",
        "?ς", "*ς", "ſ*", "*É", "Ǆ*",
    ];
    let mut differ = Vec::new();
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default().with_formula_plane_mode(mode),
        );
        // A: the texts. B: ones. C: the texts and a number below them.
        for (i, text) in texts.iter().enumerate() {
            let row = i as u32 + 1;
            e.set_cell_value("Sheet1", row, 1, t(text)).unwrap();
            e.set_cell_value("Sheet1", row, 2, n(1.0)).unwrap();
            e.set_cell_value("Sheet1", row, 3, t(text)).unwrap();
        }
        e.set_cell_value("Sheet1", last + 1, 3, n(7.0)).unwrap();
        let mut formula = |row: u32, col: u32, text: String| {
            e.set_cell_formula("Sheet1", row, col, parse(&text).unwrap())
                .unwrap();
        };
        for (i, literal) in literals.iter().enumerate() {
            let col = 4 + i as u32;
            let lit = format!("\"{literal}\"");
            // Row 101: the mask over text-only cells.
            formula(101, col, format!("=COUNTIF(A1:A{last},{lit})"));
            // Row 102: each cell through a 1x1 COUNTIFS range, summed.
            for row in 1..=last {
                formula(200 + row, col, format!("=COUNTIFS(A{row},{lit})"));
            }
            formula(
                102,
                col,
                format!("=SUM({c}201:{c}{})", 200 + last, c = col_name(col)),
            );
            // Row 103: MAXIFS over ones, against whether the mask met any.
            formula(103, col, format!("=MAXIFS(B1:B{last},A1:A{last},{lit})"));
            formula(104, col, format!("=IF({}101>0,1,0)", col_name(col)));
            // Row 105: a column holding a number, where a wildcard's mask
            // is built cell by cell.
            if !literal.starts_with("<>") {
                formula(105, col, format!("=COUNTIF(C1:C{},{lit})", last + 1));
            }
        }
        // The divergence the review noted: `ΟΣ` over `οσ` alone.
        formula(99, 1, "=COUNTIFS(A1,\"ΟΣ\")".into());
        formula(99, 2, "=COUNTIF(A1,\"ΟΣ\")".into());
        e.evaluate_all().unwrap();
        let value = |row: u32, col: u32| e.get_cell_value("Sheet1", row, col);
        let mut check = |what: String, got: Option<LiteralValue>, want: Option<LiteralValue>| {
            if got != want {
                differ.push(format!("{mode:?} {what}: {got:?}, mask {want:?}"));
            }
        };
        check("COUNTIFS(A1,\"ΟΣ\")".into(), value(99, 1), Some(n(1.0)));
        check("COUNTIF(A1,\"ΟΣ\")".into(), value(99, 2), Some(n(1.0)));
        for (i, literal) in literals.iter().enumerate() {
            let col = 4 + i as u32;
            let mask = value(101, col);
            check(
                format!("1x1 COUNTIFS {literal}"),
                value(102, col),
                mask.clone(),
            );
            check(
                format!("MAXIFS {literal}"),
                value(103, col),
                value(104, col),
            );
            if !literal.starts_with("<>") {
                check(format!("mixed column {literal}"), value(105, col), mask);
            }
        }
    }
    assert!(differ.is_empty(), "\n{}", differ.join("\n"));
}

fn col_name(col: u32) -> String {
    let mut name = String::new();
    let mut col = col;
    while col > 0 {
        let rem = (col - 1) % 26;
        name.insert(0, (b'A' + rem as u8) as char);
        col = (col - 1) / 26;
    }
    name
}
