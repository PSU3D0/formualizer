//! Excel's criteria rules for COUNTIF/SUMIF/AVERAGEIF, their *IFS forms and
//! MAXIFS/MINIFS, on every evaluation path (engine mask, scalar matcher and
//! family criteria kernel):
//! - a criteria argument that references an empty cell is 0, so blank cells
//!   do not match it and zero values do (COUNTIFS/AVERAGEIF docs);
//! - wildcards (`?`, `*`, `~`) match text cells only, never numbers;
//! - `""` matches blank cells and empty text, `"="` only blank cells, `"<>"`
//!   every cell that is not blank;
//! - numeric comparisons match numbers only (not blanks, text or booleans),
//!   `<>n` matches every other cell, and `=n` matches numeric text and date
//!   text by value;
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
                    got[k * 3 + j],
                    want[j],
                    "family={family} criterion {c}: {}",
                    formulas[k * 3 + j]
                );
            }
        }
    }
}

/// Cells that hold a date as text (an apostrophe-escaped `'10/26/1999`, or ISO
/// text imported from another tool). Excel keeps them as text (`ISTEXT` is
/// TRUE), and an exact-match date criterion such as `"10/26/1999"` counts them
/// (#576). Serials (Excel 1900 system): 10/26/1999 = 36459.
mod date_text_cells {
    use crate::engine::{Engine, EvalConfig};
    use crate::test_workbook::TestWorkbook;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::parse;

    #[derive(Clone, Copy, Debug)]
    enum Expected {
        Number(f64),
    }

    /// A1:A3 hold date text and B1:B3 the numbers 10, 20, 30; A5:A7 hold the
    /// inputs of the saved Excel workbook in #576 as typed there; A9 holds ISO
    /// text with a `T`, A10 the time text `0:00` and A11 ISO text with a
    /// lowercase `t`. C1 is blank. G1:G4 is a database with field `d`, and
    /// H1:H2 / I1:I2 are `<>10/26/1999` / `10/26/1999` criteria ranges for it.
    /// `formula` goes in D1.
    fn eval_over_date_text(formula: &str) -> LiteralValue {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let texts = [
            (1, "10/26/1999"),
            (2, "1999-10-27"),
            (3, "2023-05-04 14:05"),
            (5, "10/26/1999"),
            (6, "10/26/1999"),
            (7, "5/4/2023  2:05:00 PM"),
            (9, "1999-10-26T00:00"),
            (10, "0:00"),
            (11, "1999-10-26t00:00"),
        ];
        for (row, text) in texts {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Text(text.into()))
                .unwrap();
            engine
                .set_cell_value("Sheet1", row, 2, LiteralValue::Number(10.0 * row as f64))
                .unwrap();
        }
        // A database: header `d` in G1, records G2:G4; criteria H1:H2, I1:I2.
        let database = [
            (1, 7, "d"),
            (2, 7, "10/26/1999"),
            (3, 7, "1999-10-27"),
            (4, 7, "abc"),
            (1, 8, "d"),
            (2, 8, "<>10/26/1999"),
            (1, 9, "d"),
            (2, 9, "10/26/1999"),
        ];
        for (row, col, text) in database {
            engine
                .set_cell_value("Sheet1", row, col, LiteralValue::Text(text.into()))
                .unwrap();
        }
        engine
            .set_cell_formula("Sheet1", 1, 4, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        engine
            .get_cell_value("Sheet1", 1, 4)
            .unwrap_or(LiteralValue::Empty)
    }

    fn check(cases: &[(&str, Expected)]) {
        let mut failures = Vec::new();
        for (formula, expected) in cases {
            let actual = eval_over_date_text(formula);
            let Expected::Number(n) = expected;
            let ok = actual == LiteralValue::Number(*n);
            if !ok {
                failures.push(format!("{formula}: expected {expected:?}, got {actual:?}"));
            }
        }
        assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    }

    #[test]
    fn exact_match_criteria_count_date_text() {
        check(&[
            ("=COUNTIF(A1:A3,\"10/26/1999\")", Expected::Number(1.0)),
            ("=COUNTIF(A1:A3,\"1999-10-27\")", Expected::Number(1.0)),
            ("=SUMIF(A1:A3,\"10/26/1999\",B1:B3)", Expected::Number(10.0)),
            ("=COUNTIFS(A1:A3,\"10/26/1999\")", Expected::Number(1.0)),
            (
                "=AVERAGEIF(A1:A3,\"10/26/1999\",B1:B3)",
                Expected::Number(10.0),
            ),
            ("=COUNTIF(A1:A3,\"=10/26/1999\")", Expected::Number(1.0)),
            (
                "=SUMIFS(B1:B3,A1:A3,\"10/26/1999\")",
                Expected::Number(10.0),
            ),
            (
                "=AVERAGEIFS(B1:B3,A1:A3,\"10/26/1999\")",
                Expected::Number(10.0),
            ),
            (
                "=MAXIFS(B1:B3,A1:A3,\"10/26/1999\")",
                Expected::Number(10.0),
            ),
            (
                "=MINIFS(B1:B3,A1:A3,\"1999-10-27\")",
                Expected::Number(20.0),
            ),
            // The Excel workbook's own inputs (#576).
            ("=COUNTIF(A5:A7,\"10/26/1999\")", Expected::Number(2.0)),
            ("=COUNTIF(A5:A7,\"1999-10-26\")", Expected::Number(2.0)),
            // A number criterion, or numeric text, is the date's serial too.
            ("=COUNTIF(A1:A3,36459)", Expected::Number(1.0)),
            ("=COUNTIF(A1:A3,DATE(1999,10,26))", Expected::Number(1.0)),
            ("=COUNTIF(A1:A3,\"36459\")", Expected::Number(1.0)),
            ("=COUNTIF(A1:A3,\"=36459\")", Expected::Number(1.0)),
            ("=SUMIF(A1:A3,\"36459\",B1:B3)", Expected::Number(10.0)),
            // Time text: `0:00` is serial 0, which a blank criterion cell
            // also stands for (Excel: 1 and 1).
            ("=COUNTIF(A10,0)", Expected::Number(1.0)),
            ("=COUNTIF(A10,C1)", Expected::Number(1.0)),
            // ISO text with a `T` is a date, as `A9+0` reads it and as in
            // Google Sheets; Excel 2609 counts 0 (#576).
            ("=COUNTIF(A9,\"10/26/1999\")", Expected::Number(1.0)),
            ("=COUNTIF(A9:A10,\"10/26/1999\")", Expected::Number(1.0)),
            // Lowercase `t` too: the fast paths see lowercased text, so every
            // path reads it as the date functions do (`YEAR` is 1999).
            ("=COUNTIF(A11,\"10/26/1999\")", Expected::Number(1.0)),
            // The D-functions use the per-cell matcher.
            ("=DCOUNTA(G1:G4,1,I1:I2)", Expected::Number(1.0)),
            // An array criterion: 32 or more elements are counted from one
            // pass (`EqualityCounts`), fewer one element at a time.
            // A1 and A2 are serials 36459 and 36460, both in each array.
            (
                "=SUM(COUNTIF(A1:A3,SEQUENCE(40,1,36440)))",
                Expected::Number(2.0),
            ),
            (
                "=SUM(COUNTIF(A1:A3,SEQUENCE(31,1,36440)))",
                Expected::Number(2.0),
            ),
            (
                "=SUM(COUNTIF(A1:A3,SEQUENCE(40,1,36440)&\"\"))",
                Expected::Number(2.0),
            ),
            (
                "=SUM(COUNTIF(A1:A3,SEQUENCE(31,1,36440)&\"\"))",
                Expected::Number(2.0),
            ),
        ]);
    }

    /// `<>date` still counts date text: Excel and Google Sheets read date text
    /// for `=` only.
    #[test]
    fn not_equal_date_criteria_keep_counting_date_text() {
        check(&[
            ("=COUNTIF(A1:A3,\"<>10/26/1999\")", Expected::Number(3.0)),
            ("=COUNTIF(A1:A3,\"<>1999-10-27\")", Expected::Number(3.0)),
            (
                "=SUMIF(A1:A3,\"<>10/26/1999\",B1:B3)",
                Expected::Number(60.0),
            ),
            ("=COUNTIFS(A1:A3,\"<>10/26/1999\")", Expected::Number(3.0)),
            (
                "=AVERAGEIF(A1:A3,\"<>10/26/1999\",B1:B3)",
                Expected::Number(20.0),
            ),
            ("=COUNTIF(A1:A3,\"<>abc\")", Expected::Number(3.0)),
            ("=COUNTIF(A5:A7,\"<>10/26/1999\")", Expected::Number(3.0)),
            ("=COUNTIF(A1:A3,\"<>36459\")", Expected::Number(3.0)),
            // The D-functions use the per-cell matcher.
            ("=DCOUNTA(G1:G4,1,H1:H2)", Expected::Number(3.0)),
        ]);
    }

    /// Criteria have no workbook context and read date text in the 1900
    /// system, criterion and cell alike, so in a 1904 workbook date text
    /// matches date text, but a 1904 serial does not match it. Pinned as a
    /// known limitation (date-typed cells behave the same way).
    #[test]
    fn criteria_read_date_text_in_the_1900_system_in_1904_workbooks() {
        use crate::engine::DateSystem;
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_date_system(DateSystem::Excel1904),
        );
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("10/26/1999".into()))
            .unwrap();
        let cases = [
            ("=COUNTIF(A1,\"10/26/1999\")", 1.0),
            ("=COUNTIF(A1,36459)", 1.0),
            ("=COUNTIF(A1,DATE(1999,10,26))", 0.0),
        ];
        for (row, (formula, _)) in cases.iter().enumerate() {
            engine
                .set_cell_formula("Sheet1", row as u32 + 1, 4, parse(formula).unwrap())
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        for (row, (formula, expected)) in cases.iter().enumerate() {
            assert_eq!(
                engine.get_cell_value("Sheet1", row as u32 + 1, 4),
                Some(LiteralValue::Number(*expected)),
                "{formula}"
            );
        }
    }

    /// The per-cell matcher, used where no fast path applies (the D-functions,
    /// for one), reads date text for `=n` only, like the sheet paths.
    #[test]
    fn per_cell_matcher_reads_date_text_for_equality_only() {
        use crate::args::CriteriaPredicate as P;
        use crate::builtins::criteria_match;
        let serial = LiteralValue::Number(36_459.0);
        for text in ["10/26/1999", "1999-10-26"] {
            let cell = LiteralValue::Text(text.into());
            assert!(criteria_match(&P::Eq(serial.clone()), &cell), "={text}");
            assert!(criteria_match(&P::Ne(serial.clone()), &cell), "<>{text}");
        }
        let numeric = LiteralValue::Text("36459".into());
        assert!(criteria_match(&P::Eq(serial.clone()), &numeric));
        assert!(!criteria_match(&P::Ne(serial), &numeric));
        // A bare numeric criterion (`"36459"`) is text equality, and matches
        // date text by serial too; `<>` keeps it.
        let bare = LiteralValue::Text("36459".into());
        let date = LiteralValue::Text("10/26/1999".into());
        assert!(criteria_match(&P::Eq(bare.clone()), &date));
        assert!(criteria_match(&P::Ne(bare), &date));
    }

    /// The sheet paths count date text the way the per-cell matcher
    /// (`criteria_match`) does. A1:A300 cycle through date texts (including
    /// an ISO `T` form, which the Arrow mask sees lowercased), a number with
    /// the same serial, numeric text and plain text; B holds the row number.
    /// Columns D and E are filled-down COUNTIF and SUMIF families whose
    /// criterion is the cell in C. Each run is compared with `criteria_match`
    /// over the cells, with family execution off (the Arrow mask) and on (the
    /// lifted criteria index, which must decline: it cannot express date text).
    #[test]
    fn criteria_fast_paths_count_date_text_like_the_walk() {
        use super::super::common::arrow_eval_config;
        const ROWS: u32 = 300;
        // `numeric_text: false` swaps the numeric-text and number rows for more
        // date text, so the column is text only: the criteria index then
        // declines on date text alone, and the Arrow mask has no number lane.
        let column = |r: u32, numeric_text: bool| match r % 6 {
            0 => LiteralValue::Text("10/26/1999".into()),
            1 => LiteralValue::Text("1999-10-26".into()),
            2 if numeric_text => LiteralValue::Number(36_459.0),
            2 => LiteralValue::Text("10/26/1999 ".into()),
            3 if numeric_text => LiteralValue::Text("36459".into()),
            3 => LiteralValue::Text("10/26/99".into()),
            4 => LiteralValue::Text("abc".into()),
            _ => LiteralValue::Text("1999-10-26T00:00".into()),
        };
        // (COUNTIF, SUMIF) per row, and how many criteria indexes the run built.
        let run = |family: bool, numeric_text: bool, criterion: &str| {
            let mut e = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    family_execution: family,
                    ..arrow_eval_config()
                },
            );
            for r in 1..=ROWS {
                e.set_cell_value("Sheet1", r, 1, column(r, numeric_text))
                    .unwrap();
                e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(r as f64))
                    .unwrap();
                e.set_cell_value("Sheet1", r, 3, LiteralValue::Text(criterion.into()))
                    .unwrap();
            }
            for r in 1..=ROWS {
                let count = format!("=COUNTIF($A$1:$A${ROWS},C{r})");
                let sum = format!("=SUMIF($A$1:$A${ROWS},C{r},$B$1:$B${ROWS})");
                e.set_cell_formula("Sheet1", r, 4, parse(&count).unwrap())
                    .unwrap();
                e.set_cell_formula("Sheet1", r, 5, parse(&sum).unwrap())
                    .unwrap();
            }
            let builds = e.criteria_index_builds_for_test();
            e.evaluate_all().unwrap();
            let builds = e.criteria_index_builds_for_test() - builds;
            let values: Vec<(LiteralValue, LiteralValue)> = (1..=ROWS)
                .map(|r| {
                    (
                        e.get_cell_value("Sheet1", r, 4)
                            .unwrap_or(LiteralValue::Empty),
                        e.get_cell_value("Sheet1", r, 5)
                            .unwrap_or(LiteralValue::Empty),
                    )
                })
                .collect();
            (values, builds)
        };
        let expect = |rows: Vec<u32>| {
            (
                LiteralValue::Number(rows.len() as f64),
                LiteralValue::Number(rows.iter().map(|&r| r as f64).sum()),
            )
        };
        // Every row but the plain-text ones reads as serial 36459, whichever date
        // format the criterion and the cell use (Excel 2609 counts a `1999-10-26`
        // criterion against a `10/26/1999` text cell; see #576).
        let matching = expect((1..=ROWS).filter(|r| r % 6 != 4).collect());
        let mut lifted = false;
        for numeric_text in [true, false] {
            // `<>` is not the complement of `=`: it excludes the number and the
            // numeric text only, and still counts date text (Excel 2609, #576).
            let not_equal = expect(
                (1..=ROWS)
                    .filter(|r| !(numeric_text && (r % 6 == 2 || r % 6 == 3)))
                    .collect(),
            );
            for (criterion, expected) in [
                ("10/26/1999", &matching),
                ("1999-10-26", &matching),
                ("=10/26/1999", &matching),
                ("36459", &matching),
                ("<>10/26/1999", &not_equal),
            ] {
                let (walk, _) = run(false, numeric_text, criterion);
                let (family, builds) = run(true, numeric_text, criterion);
                lifted |= builds > 0;
                let what = format!("criterion {criterion:?}, numeric text {numeric_text}");
                let pred =
                    crate::args::parse_criteria(&LiteralValue::Text(criterion.into())).unwrap();
                let oracle = expect(
                    (1..=ROWS)
                        .filter(|&r| {
                            crate::builtins::criteria_match(&pred, &column(r, numeric_text))
                        })
                        .collect(),
                );
                assert_eq!(&oracle, expected, "{what}: criteria_match");
                assert_eq!(walk, family, "{what}: family differs from walk");
                for (row, value) in walk.iter().enumerate() {
                    assert_eq!(value, expected, "{what}, row {}", row + 1);
                }
            }
        }
        // The family runs built the criteria index, so they reached it and it
        // declined; the values above show the decline gave the walk's answer.
        assert!(lifted, "no run used the criteria index");
    }
}
