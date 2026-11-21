//! A criteria argument of `SUMIF(S)`, `COUNTIF(S)` or `AVERAGEIF(S)` that is
//! an array or a multi-cell range lifts the call, as in Excel: the result is
//! an array with one aggregate per criterion element. Several array criteria
//! broadcast against each other (a row by a column gives a grid), and an
//! element past a shorter array's end is `#N/A`, which as a criterion matches
//! nothing (0), not an `#N/A` result. Expected values were measured in Excel
//! 16 on the same grid.

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

/// A1:A6 = a,b,a,c,b,a; B1:B6 = 1..6; F1:F2 = a,b.
fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (i, a) in ["a", "b", "a", "c", "b", "a"].iter().enumerate() {
        let row = i as u32 + 1;
        e.set_cell_value("Sheet1", row, 1, t(a)).unwrap();
        e.set_cell_value("Sheet1", row, 2, n(row as f64)).unwrap();
    }
    e.set_cell_value("Sheet1", 1, 6, t("a")).unwrap();
    e.set_cell_value("Sheet1", 2, 6, t("b")).unwrap();
    e
}

/// `(formula, expected grid)`; each formula gets its own block of cells
/// below the data so a spill has room.
fn cases() -> Vec<(&'static str, Vec<Vec<LiteralValue>>)> {
    vec![
        (
            "=SUM(SUMIFS(B1:B6,A1:A6,{\"a\",\"b\"}))",
            vec![vec![n(17.0)]],
        ),
        (
            "=SUMPRODUCT(COUNTIFS(A1:A6,{\"a\",\"b\"}))",
            vec![vec![n(5.0)]],
        ),
        (
            "=SUMIF(A1:A6,F1:F2,B1:B6)",
            vec![vec![n(10.0)], vec![n(7.0)]],
        ),
        ("=COUNTIF(A1:A6,F1:F2)", vec![vec![n(3.0)], vec![n(2.0)]]),
        (
            "=COUNTIFS(A1:A6,{\"a\",\"b\"},B1:B6,{\">1\";\">3\"})",
            vec![vec![n(2.0), n(2.0)], vec![n(1.0), n(1.0)]],
        ),
        (
            "=COUNTIFS(A1:A6,{\"a\",\"b\",\"c\"},B1:B6,{\">0\",\">3\"})",
            vec![vec![n(3.0), n(1.0), n(0.0)]],
        ),
        (
            "=AVERAGEIFS(B1:B6,A1:A6,{\"a\";\"b\"})",
            vec![vec![n(10.0 / 3.0)], vec![n(3.5)]],
        ),
        ("=SUM(1/COUNTIF(A1:A6,A1:A6))", vec![vec![n(3.0)]]),
        ("=FILTER(F1:F2,COUNTIF(A1:A6,F1:F2)>2)", vec![vec![t("a")]]),
        // An error element matches only cells holding that error (none here).
        ("=COUNTIFS(A1:A6,{\"a\",#N/A})", vec![vec![n(3.0), n(0.0)]]),
        (
            "=SUMIFS(B1:B6,A1:A6,{\"a\",#N/A})",
            vec![vec![n(10.0), n(0.0)]],
        ),
        // A 1x1 array is one criterion: the result stays scalar.
        ("=COUNTIF(A1:A6,{\"a\"})", vec![vec![n(3.0)]]),
    ]
}

fn close(got: &LiteralValue, want: &LiteralValue) -> bool {
    match (got, want) {
        (LiteralValue::Number(a), LiteralValue::Number(b)) => (a - b).abs() < 1e-9,
        (LiteralValue::Int(a), LiteralValue::Number(b)) => (*a as f64 - b).abs() < 1e-9,
        _ => got == want,
    }
}

#[test]
fn array_criteria_lift_criteria_aggregates() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = build(mode);
        let cases = cases();
        for (i, (formula, _)) in cases.iter().enumerate() {
            e.set_cell_formula("Sheet1", 20 + 10 * i as u32, 12, parse(formula).unwrap())
                .unwrap();
        }
        e.evaluate_all().unwrap();
        for (i, (formula, want)) in cases.iter().enumerate() {
            for (r, row) in want.iter().enumerate() {
                for (c, w) in row.iter().enumerate() {
                    let got = e
                        .get_cell_value("Sheet1", 20 + 10 * i as u32 + r as u32, 12 + c as u32)
                        .unwrap_or(LiteralValue::Empty);
                    assert!(
                        close(&got, w),
                        "{formula} [{r},{c}] = {got:?}, Excel gives {w:?} ({mode:?})"
                    );
                }
            }
            // Nothing spills past the expected shape.
            let below = e.get_cell_value("Sheet1", 20 + 10 * i as u32 + want.len() as u32, 12);
            assert!(
                matches!(below, None | Some(LiteralValue::Empty)),
                "{formula} spilled too far down: {below:?} ({mode:?})"
            );
        }
    }
}

/// `COUNTIF` over a long array of criteria answers from one pass over the range;
/// each element must still give what `COUNTIF` gives for that one criterion.
#[test]
fn array_countif_agrees_with_single_criterion_countif() {
    use chrono::NaiveDate;
    let date = NaiveDate::from_ymd_opt(2002, 2, 1).unwrap();
    let serial = LiteralValue::Date(date).as_serial_number().unwrap();
    let cells = [
        n(5.0),
        LiteralValue::Int(5),
        n(5.0 + 1e-13),
        n(5.0 + 1e-11),
        t("5"),
        t("05"),
        t("5.0"),
        t("Ab"),
        t("aB"),
        t("ab "),
        t(""),
        LiteralValue::Empty,
        LiteralValue::Boolean(true),
        LiteralValue::Boolean(false),
        t("TRUE"),
        n(1.0),
        n(0.0),
        LiteralValue::Date(date),
        n(serial),
        t("2/1/2002"),
        LiteralValue::Error(formualizer_common::ExcelErrorKind::Na.into()),
    ];
    // Twice over: long enough for the array to be answered from one pass.
    let cells = [cells.as_slice(), cells.as_slice()].concat();
    let last = cells.len() as u32;
    // Elements beyond the cells themselves: equality spellings, then the
    // predicates that keep the walk, mixed in one array.
    let literals = [
        "5",
        "\"5\"",
        "\"5.0\"",
        "\"05\"",
        "\"=5\"",
        "\"ab\"",
        "\"AB\"",
        "\"ab \"",
        "\"\"",
        "\"=\"",
        "\"<>\"",
        "TRUE",
        "FALSE",
        "\"true\"",
        "1",
        "0",
        "\"2/1/2002\"",
        "\">1\"",
        "\"a*\"",
        "\"<>ab\"",
    ];
    let literals = [literals.as_slice(), literals.as_slice()].concat();
    let array = format!("{{{}}}", literals.join(","));
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default().with_formula_plane_mode(mode),
        );
        for (i, cell) in cells.iter().enumerate() {
            e.set_cell_value("Sheet1", i as u32 + 1, 1, cell.clone())
                .unwrap();
        }
        let mut pairs = Vec::new();
        let mut formula = |row: u32, col: u32, text: String| {
            e.set_cell_formula("Sheet1", row, col, parse(&text).unwrap())
                .unwrap();
        };
        // Column C: each cell as a criterion. Rows from 101: its lift.
        formula(101, 3, format!("=COUNTIF(A1:A{last},A1:A{last})"));
        // Not the error cells: an error criterion is the result of a single
        // COUNTIF, while an error element matches nothing.
        for row in
            (1..=last).filter(|row| !matches!(cells[*row as usize - 1], LiteralValue::Error(_)))
        {
            formula(row, 3, format!("=COUNTIF(A1:A{last},A{row})"));
            pairs.push(((row, 3), (100 + row, 3)));
        }
        // Row 201 from column D: a literal array over stored cells, then over
        // a whole column with stored cells and one with none.
        for (block, range) in [
            (201, format!("A1:A{last}")),
            (301, "A:A".into()),
            (401, "AZ:AZ".into()),
        ] {
            formula(block, 4, format!("=COUNTIF({range},{array})"));
            for (i, literal) in literals.iter().enumerate() {
                let col = 4 + i as u32;
                formula(block + 1, col, format!("=COUNTIF({range},{literal})"));
                pairs.push(((block, col), (block + 1, col)));
            }
        }
        e.evaluate_all().unwrap();
        for ((r1, c1), (r2, c2)) in pairs {
            let (a, b) = (
                e.get_cell_value("Sheet1", r1, c1),
                e.get_cell_value("Sheet1", r2, c2),
            );
            assert_eq!(a, b, "R{r1}C{c1} and R{r2}C{c2} differ ({mode:?})");
        }
    }
}

/// Text equality ignores case as single-criterion `COUNTIF` does, by Unicode
/// simple case folding: `ΟΣ` meets `οσ` though it lowercases to `ος`, and an
/// ASCII criterion meets the Kelvin sign and the long s.
#[test]
fn array_countif_folds_case_as_single_criterion_countif() {
    let texts = [
        "οσ", "ΟΣ", "ος", "Σ", "σ", "ς", "\u{212A}", "k", "K", "ſ", "s", "Straße", "STRASSE", "ẞ",
        "ß", "İ", "i", "café", "CAFÉ", "£m", "£M", "Ǆ", "ǅ", "ǆ",
    ];
    // Twice over: long enough for the cells' own lift to be answered from one pass.
    let texts = [texts.as_slice(), texts.as_slice()].concat();
    let last = texts.len() as u32;
    let literals: Vec<String> = ["ΟΣ", "k", "S", "ß", "ẞ", "i", "İ", "CAFÉ", "£m", "ǆ", "ss"]
        .iter()
        .map(|text| format!("\"{text}\""))
        .collect();
    let literals = [
        literals.as_slice(),
        literals.as_slice(),
        literals.as_slice(),
    ]
    .concat();
    let array = format!("{{{}}}", literals.join(","));
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default().with_formula_plane_mode(mode),
        );
        for (i, text) in texts.iter().enumerate() {
            e.set_cell_value("Sheet1", i as u32 + 1, 1, t(text))
                .unwrap();
        }
        let mut pairs = Vec::new();
        let mut formula = |row: u32, col: u32, text: String| {
            e.set_cell_formula("Sheet1", row, col, parse(&text).unwrap())
                .unwrap();
        };
        // Column C: each cell as a criterion. Rows from 101: its lift.
        formula(101, 3, format!("=COUNTIF(A1:A{last},A1:A{last})"));
        for row in 1..=last {
            formula(row, 3, format!("=COUNTIF(A1:A{last},A{row})"));
            pairs.push(((row, 3), (100 + row, 3)));
        }
        // From column D: the literals over every cell, then over `οσ` alone.
        for (block, range) in [(201, format!("A1:A{last}")), (301, "A1:A1".into())] {
            formula(block, 4, format!("=COUNTIF({range},{array})"));
            for (i, literal) in literals.iter().enumerate() {
                let col = 4 + i as u32;
                formula(block + 1, col, format!("=COUNTIF({range},{literal})"));
                pairs.push(((block, col), (block + 1, col)));
            }
        }
        e.evaluate_all().unwrap();
        for ((r1, c1), (r2, c2)) in pairs {
            let (a, b) = (
                e.get_cell_value("Sheet1", r1, c1),
                e.get_cell_value("Sheet1", r2, c2),
            );
            assert_eq!(a, b, "R{r1}C{c1} and R{r2}C{c2} differ ({mode:?})");
        }
        // The case the review found: `ΟΣ` over `οσ`.
        assert_eq!(e.get_cell_value("Sheet1", 301, 4), Some(n(1.0)), "{mode:?}");
    }
}

#[test]
fn distinct_count_idiom_over_a_long_column() {
    const ROWS: u32 = 5_000;
    let mut e = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for row in 1..=ROWS {
        e.set_cell_value("Sheet1", row, 1, t(&format!("k{}", row % 1_000)))
            .unwrap();
    }
    // Walking the column per element is quadratic: 25 million visits here.
    let formula = format!("=SUM(1/COUNTIF(A1:A{ROWS},A1:A{ROWS}))");
    e.set_cell_formula("Sheet1", 1, 3, parse(&formula).unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let got = e
        .get_cell_value("Sheet1", 1, 3)
        .unwrap_or(LiteralValue::Empty);
    assert!(close(&got, &n(1_000.0)), "{got:?}");
}

/// A criteria range narrower than another in the same formula: its masks for the wider
/// columns must not read the cells beside it.
#[test]
fn criteria_masks_stay_inside_their_range() {
    let mut e = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for col in 1..=6 {
        e.set_cell_value("Sheet1", 1, col, n(1.0)).unwrap();
    }
    e.set_cell_formula("Sheet1", 3, 1, parse("=COUNTIFS(A1:B1,1,D1:F1,1)").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 3, 2, parse("=SUMIFS(A1:C1,A1:B1,1)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(e.get_cell_value("Sheet1", 3, 1), Some(n(2.0)));
    assert_eq!(e.get_cell_value("Sheet1", 3, 2), Some(n(2.0)));
}

/// Over a range along a row, COUNTIF and SUMIF give the sum of what they give over each of its
/// cells alone, for every kind of cell and criterion, as ingested and over edits still in the
/// user overlay; COUNTIFS and
/// SUMIFS over the row agree with them. A one-cell range is one column, so this compares a mask
/// built for a column of a wide view with one built for that column's own view. (The *IFS
/// functions take a one-cell criteria range as a scalar, so they are compared row with row.)
#[test]
fn criteria_along_a_row_add_up_cell_by_cell() {
    use chrono::NaiveDate;
    use formualizer_common::coord::col_letters_from_1based as letters;
    let date = NaiveDate::from_ymd_opt(2002, 2, 1).unwrap();
    let serial = LiteralValue::Date(date).as_serial_number().unwrap();
    let cells = [
        n(5.0),
        LiteralValue::Int(5),
        n(5.0 + 1e-13),
        n(-0.0),
        n(0.0),
        t("5"),
        t("05"),
        t("5.0"),
        t("50%"),
        t("Ab"),
        t("ab "),
        t(""),
        LiteralValue::Empty,
        LiteralValue::Boolean(true),
        LiteralValue::Boolean(false),
        t("TRUE"),
        n(1.0),
        n(0.5),
        LiteralValue::Date(date),
        n(serial),
        t("2/1/2002"),
        LiteralValue::Error(formualizer_common::ExcelErrorKind::Na.into()),
    ];
    let criteria = [
        "5",
        "\"5\"",
        "\"=5\"",
        "\"<>5\"",
        "\">=5\"",
        "\"<0\"",
        "\"<=-0\"",
        "\">=0\"",
        "\"0\"",
        "\"50%\"",
        "\"<>50%\"",
        "\"ab\"",
        "\"a*\"",
        "\"*\"",
        "\"?b\"",
        "\"<>ab\"",
        "\"\"",
        "\"=\"",
        "\"<>\"",
        "TRUE",
        "\"true\"",
        "\"2/1/2002\"",
        "\">a\"",
        "\"<>\"\"1\"\"\"",
    ];
    let width = cells.len() as u32;
    let last = letters(width).unwrap();
    let mut e = Engine::new(TestWorkbook::default(), EvalConfig::default());
    {
        // Ingested in one 128-row chunk, so that later edits stay in the user overlay instead
        // of being compacted into the base lanes at once.
        let mut ingest = e.begin_bulk_ingest_arrow();
        ingest.add_sheet("Sheet1", width as usize, 128);
        ingest.append_row("Sheet1", &cells).unwrap();
        // Sums by powers of two, so a sum names exactly which cells were selected.
        let weights: Vec<_> = (0..width).map(|i| n((1_u64 << i) as f64)).collect();
        ingest.append_row("Sheet1", &weights).unwrap();
        // A second criteria row of all ones.
        ingest
            .append_row("Sheet1", &vec![n(1.0); width as usize])
            .unwrap();
        let empty = vec![LiteralValue::Empty; width as usize];
        for _ in 3..128 {
            ingest.append_row("Sheet1", &empty).unwrap();
        }
        ingest.finish().unwrap();
    }
    for (i, criterion) in criteria.iter().enumerate() {
        let row = 10 + i as u32;
        let whole = [
            format!("=COUNTIF(A1:{last}1,{criterion})"),
            format!("=SUMIF(A1:{last}1,{criterion},A2:{last}2)"),
            format!("=COUNTIFS(A1:{last}1,{criterion},A3:{last}3,1)"),
            format!("=SUMIFS(A2:{last}2,A1:{last}1,{criterion})"),
        ];
        for (k, formula) in (1..).zip(&whole) {
            e.set_cell_formula("Sheet1", row, k, parse(formula).unwrap())
                .unwrap();
        }
        for col in 1..=width {
            let c = letters(col).unwrap();
            let parts = [
                format!("=COUNTIF({c}1,{criterion})"),
                format!("=SUMIF({c}1,{criterion},{c}2)"),
            ];
            for (k, formula) in (0..).zip(&parts) {
                let at = 10 + k * (width + 1) + col;
                e.set_cell_formula("Sheet1", row, at, parse(formula).unwrap())
                    .unwrap();
            }
        }
    }
    let number =
        |e: &Engine<TestWorkbook>, row: u32, col: u32| match e.get_cell_value("Sheet1", row, col) {
            Some(LiteralValue::Number(x)) => x,
            Some(LiteralValue::Int(x)) => x as f64,
            other => panic!("R{row}C{col}: {other:?}"),
        };
    // As ingested, then after edits that put values in the overlay over the base lanes.
    for pass in ["as ingested", "after edits"] {
        if pass == "after edits" {
            let edits = [
                (2, t("05")),
                (6, n(-0.0)),
                (10, LiteralValue::Date(date)),
                (13, t("x")),
            ];
            for (col, value) in &edits {
                e.set_cell_value("Sheet1", 1, *col, value.clone()).unwrap();
            }
            e.evaluate_all().unwrap();
            // The edits are still in the user overlay, so the masks are built over it.
            let sheet = e.sheet_store().sheet("Sheet1").unwrap();
            for (col, _) in &edits {
                let chunk = &sheet.columns[*col as usize - 1].chunks[0];
                assert_eq!(
                    chunk.overlay.len(),
                    1,
                    "column {col} keeps its edit in the overlay"
                );
            }
        }
        e.evaluate_all().unwrap();
        for (i, criterion) in criteria.iter().enumerate() {
            let row = 10 + i as u32;
            for k in 0..2u32 {
                let whole = number(&e, row, 1 + k);
                let parts: f64 = (1..=width)
                    .map(|col| number(&e, row, 10 + k * (width + 1) + col))
                    .sum();
                assert_eq!(
                    whole.to_bits(),
                    parts.to_bits(),
                    "{pass}: formula {k} with {criterion}: the row gives {whole}, its cells {parts}"
                );
            }
            for (k, same_as) in [(3u32, 1u32), (4, 2)] {
                let (got, want) = (number(&e, row, k), number(&e, row, same_as));
                assert_eq!(
                    got.to_bits(),
                    want.to_bits(),
                    "{pass}: formula {k} with {criterion} gives {got}, formula {same_as} {want}"
                );
            }
        }
    }
}
