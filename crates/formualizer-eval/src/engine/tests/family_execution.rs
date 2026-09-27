//! Program 2 family execution (P2-M1): a family run evaluated through its
//! node template is bit-identical to the per-cell oracle
//! (`EvalConfig::family_execution = false`), sequential and parallel,
//! at first evaluation and after edits.
use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

type Cell = (&'static str, u32, u32);

fn engine(family: bool, parallel: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        family_execution: family,
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    Engine::new(TestWorkbook::new(), config)
}

/// Bit-level key of a value (`-0.0`, NaN payloads and error kinds count).
fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        Some(LiteralValue::Error(e)) => format!("E{:?}", e.kind),
        other => format!("{other:?}"),
    }
}

struct Case {
    values: Vec<(Cell, LiteralValue)>,
    formulas: Vec<(Cell, String)>,
    edits: Vec<(Cell, LiteralValue)>,
}

fn run(case: &Case, family: bool, parallel: bool) -> (Vec<Vec<String>>, u64) {
    let mut e = engine(family, parallel);
    for &((s, r, c), ref v) in &case.values {
        e.set_cell_value(s, r, c, v.clone()).unwrap();
    }
    for &((s, r, c), ref f) in &case.formulas {
        e.set_cell_formula(s, r, c, parse(f).unwrap()).unwrap();
    }
    let snapshot = |e: &Engine<TestWorkbook>| {
        case.formulas
            .iter()
            .map(|&((s, r, c), _)| key(e.get_cell_value(s, r, c)))
            .collect::<Vec<_>>()
    };
    let mut out = Vec::new();
    e.evaluate_all().unwrap();
    out.push(snapshot(&e));
    for &((s, r, c), ref v) in &case.edits {
        e.set_cell_value(s, r, c, v.clone()).unwrap();
        e.evaluate_all().unwrap();
        out.push(snapshot(&e));
    }
    (out, e.family_members_for_test())
}

fn check(case: &Case) {
    for parallel in [false, true] {
        let (oracle, none) = run(case, false, parallel);
        let (family, members) = run(case, true, parallel);
        assert_eq!(none, 0, "oracle must not use family execution");
        assert!(members > 0, "no family run executed (parallel={parallel})");
        assert_eq!(family, oracle, "parallel={parallel}");
    }
}

fn mixed_value(r: u32) -> LiteralValue {
    match r % 9 {
        0 => LiteralValue::Number(-0.0),
        1 => LiteralValue::Text(format!("t{r}")),
        2 => LiteralValue::Boolean(r.is_multiple_of(2)),
        3 => LiteralValue::Error(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Na,
        )),
        4 => LiteralValue::Empty,
        5 => LiteralValue::Number(0.0),
        _ => LiteralValue::Number(r as f64 * 1.25 - 40.0),
    }
}

const N: u32 = 120;

#[test]
fn family_operators_and_mixed_inputs_match_per_cell() {
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    for r in 1..=N {
        values.push((("Sheet1", r, 1), mixed_value(r)));
        values.push((("Sheet1", r, 2), LiteralValue::Number(r as f64)));
        formulas.push((("Sheet1", r, 3), format!("=A{r}*2+B{r}")));
        formulas.push((("Sheet1", r, 4), format!("=IFERROR(1/A{r},-0)")));
        formulas.push((("Sheet1", r, 5), format!("=C{r}&\"x\"")));
        formulas.push((("Sheet1", r, 6), format!("=-A{r}")));
    }
    let edits = vec![
        (("Sheet1", 7, 1), LiteralValue::Number(3.0)),
        (("Sheet1", 8, 2), LiteralValue::Text("q".into())),
        (("Sheet1", 60, 1), LiteralValue::Number(-0.0)),
    ];
    check(&Case {
        values,
        formulas,
        edits,
    });
}

#[test]
fn family_literal_slots_including_hash_consed_duplicates() {
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    for r in 1..=N {
        values.push((("Sheet1", r, 1), LiteralValue::Number(r as f64 / 3.0)));
        // Literal-erased template A*k+2: k varies (2 duplicates the "+2" node).
        let k = r % 3 + 1;
        formulas.push((("Sheet1", r, 2), format!("=A{r}*{k}+2")));
        formulas.push((("Sheet1", r, 3), format!("=IF(A{r}>{k},\"hi{k}\",{k}.5)")));
    }
    check(&Case {
        values,
        formulas,
        edits: vec![(("Sheet1", 5, 1), LiteralValue::Number(100.0))],
    });
}

#[test]
fn family_windows_rows_cross_sheet_and_chained_layers() {
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    for r in 1..=N {
        values.push((("Sheet1", r, 1), mixed_value(r)));
        values.push((("Data", r, 1), LiteralValue::Number(r as f64 * 0.1)));
        formulas.push((("Sheet1", r, 2), format!("=SUM(A{r}:A{})", r + 2)));
        formulas.push((("Sheet1", r, 3), format!("=ROW()*COLUMN()+Data!A{r}")));
        formulas.push((("Sheet1", r, 4), format!("=B{r}+C{r}")));
        formulas.push((("Sheet1", r, 5), format!("=AVERAGE($A$1:A{r})")));
        formulas.push((("Sheet1", r, 6), format!("=MAX(D{r},D$1)")));
    }
    check(&Case {
        values,
        formulas,
        edits: vec![
            (("Data", 3, 1), LiteralValue::Number(-7.0)),
            (("Sheet1", 1, 1), LiteralValue::Number(1e300)),
        ],
    });
}

fn run_with(case: &Case, config: EvalConfig) -> Vec<Vec<String>> {
    let mut e = Engine::new(TestWorkbook::new(), config);
    for &((s, r, c), ref v) in &case.values {
        e.set_cell_value(s, r, c, v.clone()).unwrap();
    }
    for &((s, r, c), ref f) in &case.formulas {
        e.set_cell_formula(s, r, c, parse(f).unwrap()).unwrap();
    }
    let snapshot = |e: &Engine<TestWorkbook>| {
        case.formulas
            .iter()
            .map(|&((s, r, c), _)| key(e.get_cell_value(s, r, c)))
            .collect::<Vec<_>>()
    };
    let mut out = Vec::new();
    e.evaluate_all().unwrap();
    out.push(snapshot(&e));
    for &((s, r, c), ref v) in &case.edits {
        e.set_cell_value(s, r, c, v.clone()).unwrap();
        e.evaluate_all().unwrap();
        out.push(snapshot(&e));
    }
    out
}

/// Windowed, anchored, row-wise and cross-sheet SUM/AVERAGE over mixed
/// lanes (errors, text, booleans, empties, -0.0) and computed inputs:
/// kernels on, kernels off (tier 1) and the per-cell oracle agree.
#[test]
fn family_aggregate_kernels_match_per_cell() {
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    let n = 70_000u32; // spans several Arrow chunks
    for r in (1..=n).step_by(97) {
        values.push((("Sheet1", r, 1), mixed_value(r)));
    }
    for r in 1..=N {
        values.push((("Sheet1", r, 1), mixed_value(r)));
        for c in 2..=6 {
            values.push((("Sheet1", r, c), mixed_value(r * 7 + c)));
        }
        values.push((("Data", r, 2), mixed_value(r + 3)));
        formulas.push((("Sheet1", r, 8), format!("=SUM(A{r}:A{})", r + 4)));
        formulas.push((("Sheet1", r, 9), format!("=AVERAGE(B{r}:F{r})")));
        formulas.push((("Sheet1", r, 10), format!("=SUM($A$1:A{r})")));
        formulas.push((
            ("Sheet1", r, 11),
            format!("=AVERAGE(A{r},C{r},Data!B{r}:B{})", r + 1),
        ));
        formulas.push((("Sheet1", r, 12), format!("=SUM(H{r}:I{r})")));
        formulas.push((
            ("Sheet1", r, 13),
            format!("=SUM(A{}:A{})", r * 500, r * 500 + 40_000),
        ));
        formulas.push((("Sheet1", r, 14), format!("=AVERAGE(D{r})")));
    }
    let case = Case {
        values,
        formulas,
        edits: vec![
            (("Sheet1", 4, 1), LiteralValue::Number(-0.0)),
            (("Sheet1", 9, 3), LiteralValue::Text("x".into())),
            (
                ("Data", 10, 2),
                LiteralValue::Error(formualizer_common::ExcelError::new(
                    formualizer_common::ExcelErrorKind::Div,
                )),
            ),
            (("Sheet1", 20_000, 1), LiteralValue::Number(1e308)),
        ],
    };
    for parallel in [false, true] {
        let base = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let oracle = run_with(
            &case,
            EvalConfig {
                family_execution: false,
                ..base.clone()
            },
        );
        let tier1 = run_with(
            &case,
            EvalConfig {
                family_kernels: false,
                ..base.clone()
            },
        );
        let kernels = run_with(&case, base);
        assert_eq!(tier1, oracle, "tier 1, parallel={parallel}");
        assert_eq!(kernels, oracle, "kernels, parallel={parallel}");
    }
}
