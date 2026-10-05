//! Excel's criteria rules for COUNTIF/SUMIF/AVERAGEIF, their *IFS forms and
//! MAXIFS/MINIFS, on every evaluation path (engine mask, scalar matcher and
//! family criteria kernel):
//! - a criteria argument that references an empty cell is 0, so blank cells
//!   do not match it and zero values do (COUNTIFS/AVERAGEIF docs);
//! - wildcards (`?`, `*`, `~`) match text cells only, never numbers;
//! - `""` matches blank cells and empty text, `"="` only blank cells, `"<>"`
//!   every cell that is not blank;
//! - numeric comparisons match numbers only (not blanks, text or booleans),
//!   `<>n` matches every other cell, and `=n` matches numeric text by value;
//! - text comparisons (`">b"`) and wildcards after `=`/`<>` (`"=a*"`,
//!   `"<>a*"`) apply to text.

use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn engine(family: bool) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            ..arrow_eval_config()
        },
    )
}

/// Column A (rows 1-8), with B the bit weight of each row, so a SUMIF over
/// B identifies exactly which rows matched:
/// 1: 5, 2: blank, 3: "5", 4: "" (empty text), 5: 0, 6: "abc", 7: TRUE,
/// 8: "Apple".
fn setup(e: &mut Engine<TestWorkbook>) {
    let a = [
        LiteralValue::Number(5.0),
        LiteralValue::Empty,
        LiteralValue::Text("5".into()),
        LiteralValue::Text(String::new()),
        LiteralValue::Number(0.0),
        LiteralValue::Text("abc".into()),
        LiteralValue::Boolean(true),
        LiteralValue::Text("Apple".into()),
    ];
    for (i, v) in a.into_iter().enumerate() {
        let r = i as u32 + 1;
        if !matches!(v, LiteralValue::Empty) {
            e.set_cell_value("Sheet1", r, 1, v).unwrap();
        }
        e.set_cell_value("Sheet1", r, 2, LiteralValue::Number((1u32 << i) as f64))
            .unwrap();
    }
}

/// Evaluate each formula in its own cell (column F, from row 20) and return
/// the values.
fn eval_all(e: &mut Engine<TestWorkbook>, formulas: &[String]) -> Vec<LiteralValue> {
    for (i, f) in formulas.iter().enumerate() {
        e.set_cell_formula("Sheet1", 20 + i as u32, 6, parse(f).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    (0..formulas.len())
        .map(|i| {
            e.get_cell_value("Sheet1", 20 + i as u32, 6)
                .unwrap_or(LiteralValue::Empty)
        })
        .collect()
}

/// (criterion expression, bit sum of the rows Excel matches)
const CASES: &[(&str, u32)] = &[
    // C1 is empty: the criterion is 0 (row 5 only, not the blank row 2).
    ("C1", 16),
    ("0", 16),
    ("\"=0\"", 16),
    // 5 matches the number and the numeric text; TRUE is not 1.
    ("5", 1 + 4),
    ("\"5\"", 1 + 4),
    ("1", 0),
    ("TRUE", 64),
    ("\"=TRUE\"", 64),
    // "" matches blank and empty text; "=" blank only; "<>" all but blank.
    ("\"\"", 2 + 8),
    ("\"=\"", 2),
    ("\"<>\"", 255 - 2),
    // Numeric comparisons: numbers only.
    ("\"<6\"", 1 + 16),
    ("\">=0\"", 1 + 16),
    // <>5: everything but the number 5 and the text "5".
    ("\"<>5\"", 255 - 1 - 4),
    ("\"<>0\"", 255 - 16),
    // Wildcards: text only (the number 5 and TRUE never match).
    ("\"*\"", 4 + 8 + 32 + 128),
    ("\"?\"", 4),
    ("\"a*\"", 32 + 128),
    ("\"=a*\"", 32 + 128),
    ("\"<>a*\"", 255 - 32 - 128),
    // Text comparisons: text only.
    ("\">b\"", 0),
    ("\"<b\"", 4 + 8 + 32 + 128),
    ("\">=apple\"", 128),
    ("\"<>abc\"", 255 - 32),
];

fn expected_count(bits: u32) -> f64 {
    bits.count_ones() as f64
}

#[test]
fn criteria_follow_excel_rules_on_every_path() {
    for family in [false, true] {
        let mut e = engine(family);
        setup(&mut e);
        let mut formulas = Vec::new();
        for (crit, _) in CASES {
            formulas.push(format!("=SUMIF(A1:A8,{crit},B1:B8)"));
            formulas.push(format!("=SUMIFS(B1:B8,A1:A8,{crit})"));
            formulas.push(format!("=COUNTIF(A1:A8,{crit})"));
            formulas.push(format!("=COUNTIFS(A1:A8,{crit},B1:B8,\">0\")"));
        }
        let got = eval_all(&mut e, &formulas);
        for (k, (crit, bits)) in CASES.iter().enumerate() {
            let expect = [
                *bits as f64,
                *bits as f64,
                expected_count(*bits),
                expected_count(*bits),
            ];
            for (m, want) in expect.iter().enumerate() {
                assert_eq!(
                    got[k * 4 + m],
                    LiteralValue::Number(*want),
                    "{} (family={family})",
                    formulas[k * 4 + m]
                );
            }
        }
    }
}

#[test]
fn empty_criteria_cell_is_zero_for_every_criteria_function() {
    let mut e = engine(true);
    setup(&mut e);
    let formulas: Vec<String> = [
        "=AVERAGEIF(A1:A8,C1,B1:B8)",
        "=AVERAGEIFS(B1:B8,A1:A8,C1)",
        "=MAXIFS(B1:B8,A1:A8,C1)",
        "=MINIFS(B1:B8,A1:A8,C1)",
        // A range with no zero: nothing matches.
        "=COUNTIF(D1:D8,C1)",
        "=SUMIF(D1:D8,C1,B1:B8)",
    ]
    .iter()
    .map(|f| f.to_string())
    .collect();
    let got = eval_all(&mut e, &formulas);
    assert_eq!(
        got,
        vec![
            LiteralValue::Number(16.0),
            LiteralValue::Number(16.0),
            LiteralValue::Number(16.0),
            LiteralValue::Number(16.0),
            LiteralValue::Number(0.0),
            LiteralValue::Number(0.0),
        ]
    );
}

/// A family run of SUMIF/COUNTIFS (the criteria kernel) over a column of
/// numbers, blanks and text, with per-member criteria including empty cells,
/// `<>n`, `"="`, `"<>"` and wildcards: the kernel agrees with the per-cell
/// path and with Excel's rules.
#[test]
fn family_kernel_agrees_on_blank_and_wildcard_criteria() {
    const FACTS: u32 = 60;
    let fact = |r: u32| -> LiteralValue {
        match r % 6 {
            0 => LiteralValue::Empty,
            1 => LiteralValue::Number(0.0),
            2 => LiteralValue::Number(2025.0),
            3 => LiteralValue::Text("2025".into()),
            4 => LiteralValue::Text("ab".into()),
            _ => LiteralValue::Number(r as f64),
        }
    };
    let crit = |r: u32| -> LiteralValue {
        match r % 8 {
            0 => LiteralValue::Empty,
            1 => LiteralValue::Text("<>0".into()),
            2 => LiteralValue::Text("=".into()),
            3 => LiteralValue::Text("<>".into()),
            4 => LiteralValue::Text("????".into()),
            5 => LiteralValue::Number(2025.0),
            6 => LiteralValue::Text("<>2025".into()),
            _ => LiteralValue::Number(0.0),
        }
    };
    let run = |family: bool| -> (Vec<LiteralValue>, u64) {
        let mut e = engine(family);
        for r in 1..=FACTS {
            let v = fact(r);
            if !matches!(v, LiteralValue::Empty) {
                e.set_cell_value("Sheet1", r, 1, v).unwrap();
            }
            e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(r as f64))
                .unwrap();
        }
        let mut cells = Vec::new();
        for r in 1..=24u32 {
            let c = crit(r);
            if !matches!(c, LiteralValue::Empty) {
                e.set_cell_value("Sheet1", r, 4, c).unwrap();
            }
            for (k, f) in [
                "=SUMIF($A$1:$A$60,D{r},$B$1:$B$60)",
                "=COUNTIFS($A$1:$A$60,D{r},$B$1:$B$60,\">0\")",
                "=SUMIFS($B$1:$B$60,$A$1:$A$60,D{r})",
            ]
            .iter()
            .enumerate()
            {
                let col = 6 + k as u32;
                e.set_cell_formula(
                    "Sheet1",
                    r,
                    col,
                    parse(f.replace("{r}", &r.to_string())).unwrap(),
                )
                .unwrap();
                cells.push((r, col));
            }
        }
        e.evaluate_all().unwrap();
        let values = cells
            .iter()
            .map(|&(r, c)| e.get_cell_value("Sheet1", r, c).unwrap())
            .collect();
        (values, e.family_members_for_test())
    };
    let (oracle, _) = run(false);
    let (family, members) = run(true);
    assert!(members > 0, "no family run executed");
    assert_eq!(family, oracle);

    // Excel's expected values, from the row rules.
    let matches = |c: &LiteralValue, v: &LiteralValue| -> bool {
        use LiteralValue::*;
        match (c, v) {
            (Empty, Number(x)) | (Number(_), Number(x)) if *x == crit_num(c) => true,
            (Number(n), Text(t)) => t.parse::<f64>().ok() == Some(*n),
            (Text(t), v) => match t.as_str() {
                "<>0" => !matches!(v, Number(x) if *x == 0.0),
                "=" => matches!(v, Empty),
                "<>" => !matches!(v, Empty),
                "????" => matches!(v, Text(s) if s.chars().count() == 4),
                "<>2025" => {
                    !matches!(v, Number(x) if *x == 2025.0) && !matches!(v, Text(s) if s == "2025")
                }
                _ => unreachable!(),
            },
            _ => false,
        }
    };
    for r in 1..=24u32 {
        let c = crit(r);
        let rows: Vec<u32> = (1..=FACTS).filter(|&i| matches(&c, &fact(i))).collect();
        let sum: u32 = rows.iter().sum();
        let k = (r - 1) as usize * 3;
        assert_eq!(oracle[k], LiteralValue::Number(sum as f64), "SUMIF row {r}");
        assert_eq!(
            oracle[k + 1],
            LiteralValue::Number(rows.len() as f64),
            "COUNTIFS row {r}"
        );
        assert_eq!(
            oracle[k + 2],
            LiteralValue::Number(sum as f64),
            "SUMIFS row {r}"
        );
    }
}

fn crit_num(c: &LiteralValue) -> f64 {
    match c {
        LiteralValue::Number(n) => *n,
        _ => 0.0,
    }
}

/// Criteria text is matched exactly (trailing spaces are significant), and
/// date text in a criterion (`"=2/1/02"`, `">2/15/02"`, `"2/1/2002"`) is the
/// date's serial, on the scalar, mask and family paths.
#[test]
fn criteria_text_is_exact_and_date_text_is_a_date() {
    // 37288 = 2002-02-01, 37316 = 2002-03-01.
    let a = [
        LiteralValue::Text("ABQ Energy Group, Ltd ".into()),
        LiteralValue::Text("AEP ".into()),
        LiteralValue::Text("ABQ Energy Group, Ltd".into()),
        LiteralValue::Number(37288.0),
        LiteralValue::Number(37316.0),
    ];
    // (criterion, bit sum of the matching rows)
    let cases: &[(&str, u32)] = &[
        ("D1", 1),
        ("\"ABQ Energy Group, Ltd \"", 1),
        ("\"ABQ Energy Group, Ltd\"", 4),
        ("\"AEP \"", 2),
        ("\"=AEP \"", 2),
        ("\"AEP\"", 0),
        ("\"ABQ*\"", 1 + 4),
        ("\"=2/1/02\"", 8),
        ("\"2/1/2002\"", 8),
        ("\">2/15/02\"", 16),
        ("\"<=1-Feb-2002\"", 8),
        ("\"<>2/1/02\"", 1 + 2 + 4 + 16),
    ];
    for family in [false, true] {
        let mut e = engine(family);
        for (i, v) in a.iter().enumerate() {
            let r = i as u32 + 1;
            e.set_cell_value("Sheet1", r, 1, v.clone()).unwrap();
            e.set_cell_value("Sheet1", r, 2, LiteralValue::Number((1u32 << i) as f64))
                .unwrap();
        }
        e.set_cell_value(
            "Sheet1",
            1,
            4,
            LiteralValue::Text("ABQ Energy Group, Ltd ".into()),
        )
        .unwrap();
        let formulas: Vec<String> = cases
            .iter()
            .flat_map(|(c, _)| {
                [
                    format!("=SUMIF($A$1:$A$5,{c},$B$1:$B$5)"),
                    format!("=SUMIFS($B$1:$B$5,$A$1:$A$5,{c})"),
                    format!("=COUNTIF($A$1:$A$5,{c})"),
                ]
            })
            .collect();
        let got = eval_all(&mut e, &formulas);
        for (k, (c, bits)) in cases.iter().enumerate() {
            let want = [
                LiteralValue::Number(*bits as f64),
                LiteralValue::Number(*bits as f64),
                LiteralValue::Number(expected_count(*bits)),
            ];
            for j in 0..3 {
                assert_eq!(
                    got[k * 3 + j], want[j],
                    "family={family} criterion {c}: {}",
                    formulas[k * 3 + j]
                );
            }
        }
    }
}
