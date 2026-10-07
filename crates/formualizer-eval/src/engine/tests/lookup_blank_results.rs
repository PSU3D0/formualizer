//! VLOOKUP and HLOOKUP on an empty matched cell pass a blank to the
//! enclosing formula, as in Excel: `ISBLANK(VLOOKUP(..))` is TRUE and
//! `VLOOKUP(..)&""` is "", while a cell holding just the lookup shows 0.
//! Per-cell and family execution agree, sequential and parallel.

use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const ROWS: u32 = 40;

/// Keys `k1..k40` in column A; column B holds a number on odd rows and is
/// empty on even rows. Row 1 of columns H.. holds the same table across.
fn engine(family: bool, parallel: bool) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            enable_parallel: parallel,
            ..arrow_eval_config()
        },
    );
    for r in 1..=ROWS {
        let key = LiteralValue::Text(format!("k{r}"));
        e.set_cell_value("Sheet1", r, 1, key.clone()).unwrap();
        e.set_cell_value("Data", 1, r, key).unwrap();
        if r % 2 == 1 {
            let v = LiteralValue::Number(f64::from(r) * 10.0);
            e.set_cell_value("Sheet1", r, 2, v.clone()).unwrap();
            e.set_cell_value("Data", 2, r, v).unwrap();
        }
    }
    e
}

/// Each case is filled down rows 1..=ROWS of its own column, so it runs as
/// a family; `(odd, even)` are the expected values on row 1 (a number)
/// and on even rows (an empty match).
fn cases() -> Vec<(&'static str, LiteralValue, LiteralValue)> {
    use LiteralValue::{Boolean, Number, Text};
    let v = "VLOOKUP(\"k\"&ROW(),$A$1:$B$40,2,FALSE)";
    let h = "HLOOKUP(\"k\"&ROW(),Data!$A$1:$AN$2,2,FALSE)";
    let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
    let n = |r: u32| Number(f64::from(r) * 10.0);
    // `odd` is the expected value on row 1.
    vec![
        (leak(format!("={v}")), n(1), Number(0.0)),
        (
            leak(format!("=ISBLANK({v})")),
            Boolean(false),
            Boolean(true),
        ),
        (
            leak(format!("={v}&\"\"")),
            Text("10".into()),
            Text(String::new()),
        ),
        (leak(format!("={v}=\"\"")), Boolean(false), Boolean(true)),
        (leak(format!("={v}=0")), Boolean(false), Boolean(true)),
        (leak(format!("={v}+1")), Number(11.0), Number(1.0)),
        (leak(format!("=LEN({v})")), Number(2.0), Number(0.0)),
        (
            leak(format!("=ISNUMBER({v})")),
            Boolean(true),
            Boolean(false),
        ),
        (
            leak(format!("=IF({v}=\"\",\"none\",{v})")),
            n(1),
            Text("none".into()),
        ),
        (leak(format!("={h}")), n(1), Number(0.0)),
        (
            leak(format!("=ISBLANK({h})")),
            Boolean(false),
            Boolean(true),
        ),
        (
            leak(format!("={h}&\"\"")),
            Text("10".into()),
            Text(String::new()),
        ),
    ]
}

fn run(family: bool, parallel: bool) -> Vec<LiteralValue> {
    let mut e = engine(family, parallel);
    let cases = cases();
    for (i, (f, _, _)) in cases.iter().enumerate() {
        for r in 1..=ROWS {
            e.set_cell_formula("Sheet1", r, 4 + i as u32, parse(f).unwrap())
                .unwrap();
        }
    }
    e.evaluate_all().unwrap();
    let mut out = Vec::new();
    for i in 0..cases.len() as u32 {
        for r in 1..=ROWS {
            out.push(
                e.get_cell_value("Sheet1", r, 4 + i)
                    .unwrap_or(LiteralValue::Empty),
            );
        }
    }
    out
}

#[test]
fn empty_lookup_match_is_blank_to_enclosing_formulas() {
    let oracle = run(false, false);
    let cases = cases();
    for (i, (f, odd, even)) in cases.iter().enumerate() {
        let at = |r: u32| &oracle[i * ROWS as usize + (r - 1) as usize];
        assert_eq!(at(2), even, "{f} on an empty match");
        assert_eq!(at(ROWS), even, "{f} on an empty match");
        assert_eq!(at(1), odd, "{f} on a number");
    }
    for (family, parallel) in [(true, false), (false, true), (true, true)] {
        assert_eq!(
            run(family, parallel),
            oracle,
            "family={family} parallel={parallel}"
        );
    }
}
