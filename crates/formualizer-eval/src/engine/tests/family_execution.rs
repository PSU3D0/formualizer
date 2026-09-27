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

/// Values and derived formats (the lift records each member's format).
fn run_formats(case: &Case, config: EvalConfig) -> (Vec<Vec<String>>, u64) {
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
            .map(|&((s, r, c), _)| {
                format!(
                    "{} {:?}",
                    key(e.get_cell_value(s, r, c)),
                    e.debug_derived_format_0based(s, r - 1, c - 1)
                )
            })
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
    (out, e.lifted_members_for_test())
}

fn check_lift(case: &Case) {
    for parallel in [false, true] {
        let base = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let oracle = run_formats(
            case,
            EvalConfig {
                family_execution: false,
                ..base.clone()
            },
        );
        let walk = run_formats(
            case,
            EvalConfig {
                family_lift: false,
                ..base.clone()
            },
        );
        let lift = run_formats(case, base);
        assert_eq!(oracle.1, 0);
        assert_eq!(walk.1, 0);
        assert!(lift.1 > 0, "no run was lifted (parallel={parallel})");
        assert_eq!(walk.0, oracle.0, "walk, parallel={parallel}");
        assert_eq!(lift.0, oracle.0, "lift, parallel={parallel}");
    }
}

fn lift_value(r: u32) -> LiteralValue {
    use chrono::NaiveDate;
    match r % 13 {
        7 => LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 1, 1 + r % 27).unwrap()),
        8 => LiteralValue::Number(f64::MAX / 2.0),
        9 => LiteralValue::Text(format!("{}", r as f64 / 4.0)),
        10 => LiteralValue::Int(r as i64 - 60),
        11 => LiteralValue::Error(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Div,
        )),
        12 => LiteralValue::Number(-(r as f64)),
        _ => mixed_value(r),
    }
}

/// The elementwise lift (P2-M3): every operator over mixed inputs (errors,
/// text, numeric text, booleans, empties, -0.0, huge values, dates),
/// absolute and relative, same- and cross-sheet references, a missing
/// sheet, and error precedence between operands: lift, walk and per-cell
/// oracle agree on values and derived formats, first eval and after edits.
#[test]
fn family_lift_operators_match_per_cell() {
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    for r in 1..=N {
        values.push((("Sheet1", r, 1), lift_value(r)));
        values.push((("Sheet1", r, 2), lift_value(r * 5 + 1)));
        values.push((("Data", r, 1), lift_value(r + 2)));
        let fs = [
            format!("=A{r}+B{r}"),
            format!("=A{r}-B{r}*2"),
            format!("=A{r}/B{r}"),
            format!("=A{r}^B{r}"),
            format!("=(A{r}-1)^0.5"),
            format!("=A{r}&B{r}"),
            format!("=A{r}=B{r}"),
            format!("=A{r}<>B{r}"),
            format!("=A{r}<B{r}"),
            format!("=A{r}>=B{r}"),
            format!("=-A{r}%"),
            format!("=+A{r}"),
            format!("=A{r}+1"),
            format!("=A{r}+$B$3"),
            format!("=Data!A{r}-A{r}"),
            format!("=Missing!A{r}+1"),
            format!("=B{r}/A{r}+A{r}/B{r}"),
            format!("=A{r}*B{r}-Data!A{r}/2"),
            format!("=A{r}+B{r}-A{r}"),
            format!("=IF(A{r},B{r},A{r}+1)"),
            format!("=IF(A{r}>B{r},A{r})"),
            format!("=IF(A{r}=\"\",\"\",A{r}*B{r})"),
            format!("=IF(B{r}<0,IF(A{r}<1,\"lo\",A{r}),B{r}+1)*2"),
            format!("=IF(Data!A{r},A{r}+1,B{r}-1)"),
            format!("=IF(A{r}+0,,5)"),
        ];
        for (k, f) in fs.into_iter().enumerate() {
            formulas.push((("Sheet1", r, 4 + k as u32), f));
        }
    }
    check_lift(&Case {
        values,
        formulas,
        edits: vec![
            (("Sheet1", 7, 1), LiteralValue::Number(3.0)),
            (("Sheet1", 8, 2), LiteralValue::Text("q".into())),
            (("Data", 60, 1), LiteralValue::Number(-0.0)),
            (("Sheet1", 20, 1), LiteralValue::Boolean(true)),
        ],
    });
}

/// Randomized templates (seeded): nested unary and binary operators over
/// relative/absolute references and literals.
#[test]
fn family_lift_random_templates_match_per_cell() {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut next = move |m: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % m
    };
    fn expr(next: &mut impl FnMut(u64) -> u64, depth: u32) -> String {
        if depth == 0 || next(3) == 0 {
            return match next(6) {
                0 => "A{r}".to_string(),
                1 => "B{r}".to_string(),
                2 => "$A$2".to_string(),
                3 => "Data!A{r}".to_string(),
                4 => format!("{}", next(7) as f64 - 2.5),
                _ => "\"3\"".to_string(),
            };
        }
        match next(10) {
            0 => format!("-({})", expr(next, depth - 1)),
            1 => format!("({})%", expr(next, depth - 1)),
            9 => format!(
                "IF({},{},{})",
                expr(next, depth - 1),
                expr(next, depth - 1),
                expr(next, depth - 1)
            ),
            k => {
                let op = ["+", "-", "*", "/", "^", "&", "<", "="][(k as usize) % 8];
                format!("({}){op}({})", expr(next, depth - 1), expr(next, depth - 1))
            }
        }
    }
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    for r in 1..=40u32 {
        values.push((("Sheet1", r, 1), lift_value(r)));
        values.push((("Sheet1", r, 2), lift_value(r * 3 + 2)));
        values.push((("Data", r, 1), lift_value(r + 5)));
    }
    for t in 0..48u32 {
        let template = format!("={}", expr(&mut next, 3));
        for r in 1..=40u32 {
            formulas.push((
                ("Sheet1", r, 4 + t),
                template.replace("{r}", &r.to_string()),
            ));
        }
    }
    check_lift(&Case {
        values,
        formulas,
        edits: vec![(("Sheet1", 9, 1), LiteralValue::Number(-0.0))],
    });
}

/// Bulk-ingested formulas store each literal under its own ref: a member
/// whose literal values equal the template's (by value, numbers by bits)
/// needs no binding, so its run is lifted (and the walk binds nothing).
#[test]
fn family_lift_bulk_ingested_literals_compare_by_value() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    engine.add_sheet("S").unwrap();
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 2, 1024);
        for r in 0..300u32 {
            ab.append_row("S", &[LiteralValue::Number(r as f64), LiteralValue::Empty])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("S");
    let batch: Vec<_> = (1..=300u32)
        .map(|r| (r, 2, parse(format!("=A{r}*2-0.5")).unwrap()))
        .collect();
    builder.add_formulas(sheet, batch);
    builder.finish().unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.lifted_members_for_test(), 300);
    for r in 1..=300u32 {
        assert_eq!(
            engine.get_cell_value("S", r, 2),
            Some(LiteralValue::Number((r - 1) as f64 * 2.0 - 0.5))
        );
    }
}
