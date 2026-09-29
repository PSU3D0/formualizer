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
        // Windowed MIN/MAX/COUNT kernels (P2-M4).
        formulas.push((("Sheet1", r, 7), format!("=MIN(A{r}:A{})", r + 3)));
        formulas.push((("Sheet1", r, 8), format!("=MAX(A{r}:B{})", r + 1)));
        formulas.push((("Sheet1", r, 9), format!("=COUNT($A$1:A{r})")));
        formulas.push((
            ("Sheet1", r, 10),
            format!("=MIN(A{r}:A{},Data!A{r}:A{})", r + 2, r + 1),
        ));
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
        match next(15) {
            0 => format!("-({})", expr(next, depth - 1)),
            1 => format!("({})%", expr(next, depth - 1)),
            9 => format!(
                "IF({},{},{})",
                expr(next, depth - 1),
                expr(next, depth - 1),
                expr(next, depth - 1)
            ),
            10 => format!("ROUND({},{})", expr(next, depth - 1), next(5) as i64 - 2),
            11 => format!(
                "{}({},{})",
                ["MAX", "MIN", "SUM"][next(3) as usize],
                expr(next, depth - 1),
                expr(next, depth - 1)
            ),
            12 => format!(
                "IFERROR({},{})",
                expr(next, depth - 1),
                expr(next, depth - 1)
            ),
            13 => format!(
                "{}({},{})",
                ["AND", "OR"][next(2) as usize],
                expr(next, depth - 1),
                expr(next, depth - 1)
            ),
            14 => format!("ABS({})", expr(next, depth - 1)),
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

/// Memoized runs (P2-M4): SUMIFS/COUNTIFS/AVERAGEIF/SUMIF/COUNTIF and
/// VLOOKUP/HLOOKUP/MATCH families over absolute (and whole-column) ranges
/// with repeating keys, including keys that differ only by type (1 vs "1",
/// TRUE), empties, errors, dates and a relative-range template (not
/// memoized): memo, no memo (kernels off) and the per-cell oracle agree on
/// values and formats, and repeated keys hit.
#[test]
fn family_memo_criteria_and_lookups_match_per_cell() {
    use chrono::NaiveDate;
    let key = |r: u32| match r % 7 {
        0 => LiteralValue::Text("North".into()),
        1 => LiteralValue::Text("south".into()),
        2 => LiteralValue::Number(1.0),
        3 => LiteralValue::Text("1".into()),
        4 => LiteralValue::Boolean(true),
        5 => LiteralValue::Empty,
        _ => LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 3, 1 + r % 3).unwrap()),
    };
    let mut values = Vec::new();
    let mut formulas = Vec::new();
    for r in 1..=60u32 {
        values.push((("Facts", r, 1), key(r * 3 + 1)));
        values.push((("Facts", r, 2), mixed_value(r)));
        values.push((("Facts", r, 3), LiteralValue::Number(r as f64 * 0.5 - 7.0)));
    }
    values.push((
        ("Facts", 61, 1),
        LiteralValue::Error(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Na,
        )),
    ));
    for r in 1..=N {
        values.push((("Report", r, 1), key(r)));
        values.push((("Report", r, 2), LiteralValue::Number((r % 4) as f64)));
        let fs = [
            format!("=SUMIFS(Facts!$C:$C,Facts!$A:$A,A{r})"),
            format!("=SUMIFS(Facts!$C$1:$C$61,Facts!$A$1:$A$61,A{r},Facts!$C$1:$C$61,\">\"&B{r})"),
            format!("=COUNTIFS(Facts!$A:$A,A{r},Facts!$B:$B,\"<>\")"),
            format!("=AVERAGEIF(Facts!$A:$A,A{r},Facts!$C:$C)"),
            format!("=SUMIF(Facts!$A$1:$A$61,A{r},Facts!$B$1:$B$61)"),
            format!("=COUNTIF(Facts!$A:$A,A{r})"),
            format!("=VLOOKUP(A{r},Facts!$A$1:$C$61,3,FALSE)"),
            format!("=MATCH(A{r},Facts!$A$1:$A$61,0)"),
            format!("=HLOOKUP(B{r},$A$1:$B$2,1,FALSE)"),
            format!("=SUMIF(A{r}:A{},A{r})", r + 2),
        ];
        for (k, f) in fs.into_iter().enumerate() {
            formulas.push((("Report", r, 4 + k as u32), f));
        }
    }
    let case = Case {
        values,
        formulas,
        edits: vec![
            (("Facts", 10, 3), LiteralValue::Number(-0.0)),
            (("Report", 5, 1), LiteralValue::Text("North".into())),
            (("Facts", 4, 1), LiteralValue::Text("NORTH".into())),
        ],
    };
    for parallel in [false, true] {
        let base = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let oracle = run_formats(
            &case,
            EvalConfig {
                family_execution: false,
                ..base.clone()
            },
        );
        let plain = run_formats(
            &case,
            EvalConfig {
                family_kernels: false,
                ..base.clone()
            },
        );
        let memo = run_formats(&case, base);
        assert_eq!(plain.0, oracle.0, "no memo, parallel={parallel}");
        assert_eq!(memo.0, oracle.0, "memo, parallel={parallel}");
    }
    // Hits happen (sequential run, one engine).
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..arrow_eval_config()
        },
    );
    for &((s, r, c), ref v) in &case.values {
        e.set_cell_value(s, r, c, v.clone()).unwrap();
    }
    for &((s, r, c), ref f) in &case.formulas {
        e.set_cell_formula(s, r, c, parse(f).unwrap()).unwrap();
    }
    e.evaluate_all().unwrap();
    assert!(e.memo_hits_for_test() > 0, "no memo hit");
}

/// Typed lanes (P2-M3 on f64 lanes): a lifted family reads its operands as
/// the merged number lanes with type-tag and format masks, and falls back
/// per element to the scalar read and operator. Base lanes are ingested in
/// 16-row chunks (runs cross chunk boundaries) with dates (a format lane),
/// -0.0, huge values, text and booleans; families chain through computed
/// overlays and derived formats (date arithmetic); edits put user overlay
/// points of every type mid-run. Lift, walk and per-cell oracle agree on
/// values and derived formats, sequential and parallel.
#[test]
fn family_lift_typed_lanes_match_per_cell() {
    let formulas: Vec<(u32, String)> = vec![
        (4, "=A{r}*B{r}".into()),
        (5, "=A{r}/B{r}".into()),
        (6, "=A{r}^0.5+B{r}^2".into()),
        (7, "=-A{r}%".into()),
        (8, "=A{r}*1E+300*B{r}".into()),
        (9, "=A{r}=B{r}".into()),
        (10, "=IF(A{r}>B{r},A{r}-B{r},B{r}-A{r})".into()),
        (11, "=IF(A{r},1,2)+IF(I{r},C{r},-C{r})".into()),
        // Computed-overlay operands (another family's results).
        (12, "=D{r}+E{r}*2".into()),
        (13, "=J{r}/(D{r}-L{r})".into()),
        // Date arithmetic: derived DATE formats, read by the next family.
        (14, "=C{r}+1".into()),
        (15, "=N{r}-C{r}".into()),
        // Past the last ingested row for the bottom members.
        (16, "=A{r20}*2+1".into()),
    ];
    check_typed(formulas);
}

/// Windowed MIN/MAX/COUNT kernels over ingested base lanes in 16-row
/// chunks (windows cross chunk boundaries and run past the sheet end).
#[test]
fn family_window_kernels_on_base_lanes_match_per_cell() {
    let formulas: Vec<(u32, String)> = vec![
        (4, "=MIN(A{r}:B{r20})".into()),
        (5, "=MAX(A{r}:C{r20})".into()),
        (6, "=COUNT(A{r}:C{r20})".into()),
        (7, "=MAX($A$1:A{r})-MIN($B$1:B{r})".into()),
        (8, "=A{r}+1".into()),
    ];
    check_typed(formulas);
}

/// Builtins on typed lanes (ROUND, ABS, MIN, MAX, SUM of scalars, AND, OR,
/// IFERROR): clean members take the builtin's own core, others the walk.
#[test]
fn family_lift_builtins_match_per_cell() {
    let formulas: Vec<(u32, String)> = vec![
        (4, "=ROUND(A{r}*B{r},2)".into()),
        (5, "=ROUND(A{r}/7,-1)+ABS(B{r}-A{r})".into()),
        (6, "=MAX(A{r},B{r})-MIN(A{r},B{r},0)".into()),
        (7, "=MAX(A{r}*2,C{r})".into()),
        (8, "=SUM(A{r},B{r},1)*2".into()),
        (9, "=IF(AND(A{r}>0,B{r}),1,0)+IF(OR(A{r},FALSE),2,3)".into()),
        (10, "=IFERROR(A{r}/B{r},-1)".into()),
        (11, "=IFERROR(J{r}*2,0)+ROUND(J{r},C{r})".into()),
        (12, "=AND(A{r},B{r},C{r})".into()),
        (13, "=MIN(D{r},E{r})+MAX(F{r},0)".into()),
        (14, "=ABS(C{r})+SUM(C{r},D{r})".into()),
    ];
    check_typed(formulas);
}

/// Type tests and date parts on typed lanes (Program 3): a clean operand's
/// result without the walk; anything else (text, dates with a format,
/// serials off the calendar, errors) is walked.
#[test]
fn family_lift_type_tests_and_date_parts_match_per_cell() {
    let formulas: Vec<(u32, String)> = vec![
        (4, "=ISNUMBER(A{r})".into()),
        (5, "=IF(ISNA(B{r}),0,1)+ISERROR(A{r}/B{r})".into()),
        (6, "=AND(ISERR(C{r}),ISTEXT(A{r}))".into()),
        (7, "=OR(ISLOGICAL(B{r}),ISBLANK(C{r}))".into()),
        (8, "=MONTH(A{r}*100+40000)".into()),
        (9, "=YEAR(B{r}*300)+DAY(C{r}*7)".into()),
        (10, "=MONTH(A{r})".into()),
        (11, "=WEEKDAY(A{r}*11+30000)".into()),
        (12, "=WEEKDAY(B{r}*13,2)+WEEKDAY(C{r},C{r})".into()),
        (13, "=YEAR(A{r})*12+MONTH(A{r})".into()),
    ];
    check_typed_with_names(formulas, &[], true);
}

fn check_typed(formulas: Vec<(u32, String)>) {
    check_typed_with_names(formulas, &[], false);
}

/// Defined names (Program 3): a name with one value for the run (a cell,
/// a literal, a formula; a range as a call's argument) is a lift constant.
#[test]
fn family_lift_defined_names_match_per_cell() {
    let formulas: Vec<(u32, String)> = vec![
        (4, "=A{r}/Rate".into()),
        (5, "=A{r}*Lit+Rate".into()),
        (6, "=B{r}-Twice".into()),
        (7, "=A{r}+SUM(Block)".into()),
        (8, "=IF(A{r}>Rate,1,2)*Lit".into()),
        (9, "=A{r}+COUNT(Column)*Lit".into()),
        (10, "=Lit*2".into()),
    ];
    check_typed_with_names(
        formulas,
        &[
            ("Rate", "=Sheet1!$B$5"),
            ("Lit", "2.5"),
            ("Twice", "=Sheet1!$A$3*2"),
            ("Block", "=Sheet1!$A$1:$C$10"),
            ("Column", "=Sheet1!$C$1:$C$100"),
        ],
        true,
    );
}

fn check_typed_with_names(formulas: Vec<(u32, String)>, names: &[(&str, &str)], all_lifted: bool) {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord, RangeRef};
    use chrono::NaiveDate;
    const ROWS: u32 = 100;
    let base_value = |r: u32, c: u32| -> LiteralValue {
        match (r * 7 + c * 3) % 17 {
            0 => LiteralValue::Number(-0.0),
            1 => LiteralValue::Number(f64::MAX / 3.0),
            2 => LiteralValue::Date(NaiveDate::from_ymd_opt(2023, 3, 1 + r % 27).unwrap()),
            3 => LiteralValue::Text(format!("{}", r as f64 / 8.0)),
            4 => LiteralValue::Boolean(r.is_multiple_of(2)),
            5 => LiteralValue::Empty,
            6 => LiteralValue::Number(0.0),
            7 => LiteralValue::Int(r as i64 - 50),
            _ => LiteralValue::Number((r as f64 - 40.0) * 0.37 + c as f64),
        }
    };
    let run_typed = |config: EvalConfig| -> (Vec<Vec<String>>, u64) {
        let mut e = Engine::new(TestWorkbook::new(), config);
        {
            let mut ab = e.begin_bulk_ingest_arrow();
            ab.add_sheet("Sheet1", 3, 16);
            for r in 1..=ROWS {
                ab.append_row(
                    "Sheet1",
                    &[base_value(r, 1), base_value(r, 2), base_value(r, 3)],
                )
                .unwrap();
            }
            ab.finish().unwrap();
        }
        let sheet = e.sheet_id("Sheet1").unwrap();
        for (name, def) in names {
            let at = |r: u32, c: u32| CellRef::new(sheet, Coord::from_excel(r, c, true, true));
            let definition = match *def {
                "=Sheet1!$B$5" => NamedDefinition::Cell(at(5, 2)),
                "=Sheet1!$A$1:$C$10" => NamedDefinition::Range(RangeRef::new(at(1, 1), at(10, 3))),
                "=Sheet1!$C$1:$C$100" => {
                    NamedDefinition::Range(RangeRef::new(at(1, 3), at(100, 3)))
                }
                "2.5" => NamedDefinition::Literal(LiteralValue::Number(2.5)),
                formula => NamedDefinition::Formula {
                    ast: parse(formula).unwrap(),
                    dependencies: Vec::new(),
                    range_deps: Vec::new(),
                },
            };
            e.define_name(name, definition, NameScope::Workbook)
                .unwrap();
        }
        let mut cells = Vec::new();
        for r in 1..=ROWS {
            for (c, f) in &formulas {
                let f = f
                    .replace("{r20}", &(r + 20).to_string())
                    .replace("{r}", &r.to_string());
                e.set_cell_formula("Sheet1", r, *c, parse(&f).unwrap())
                    .unwrap();
                cells.push((r, *c));
            }
        }
        let snapshot = |e: &Engine<TestWorkbook>| {
            cells
                .iter()
                .map(|&(r, c)| {
                    format!(
                        "{} {:?}",
                        key(e.get_cell_value("Sheet1", r, c)),
                        e.debug_derived_format_0based("Sheet1", r - 1, c - 1)
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut out = Vec::new();
        e.evaluate_all().unwrap();
        out.push(snapshot(&e));
        let edits = [
            ((5, 2), LiteralValue::Number(4.0)),
            ((3, 1), LiteralValue::Number(-7.5)),
            ((10, 1), LiteralValue::Text("x".into())),
            ((17, 1), LiteralValue::Number(-0.0)),
            ((33, 2), LiteralValue::Boolean(true)),
            (
                (47, 1),
                LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()),
            ),
            ((48, 2), LiteralValue::Empty),
            (
                (64, 1),
                LiteralValue::Error(formualizer_common::ExcelError::new(
                    formualizer_common::ExcelErrorKind::Na,
                )),
            ),
            ((65, 1), LiteralValue::Number(f64::MIN_POSITIVE)),
        ];
        for ((r, c), v) in edits {
            e.set_cell_value("Sheet1", r, c, v).unwrap();
            e.evaluate_all().unwrap();
            out.push(snapshot(&e));
        }
        assert!(
            e.lifted_members_for_test() == 0 || e.lane_clean_reads_for_test() > 0,
            "no clean typed-lane element was read"
        );
        (out, e.lifted_members_for_test())
    };
    for parallel in [false, true] {
        let base = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let oracle = run_typed(EvalConfig {
            family_execution: false,
            ..base.clone()
        });
        let walk = run_typed(EvalConfig {
            family_lift: false,
            ..base.clone()
        });
        let lift = run_typed(base);
        assert_eq!(oracle.1, 0);
        assert!(lift.1 > 0, "no run was lifted (parallel={parallel})");
        if all_lifted {
            // Every formula's run was lifted at the first evaluation (but
            // for single-member pieces the plan may split off).
            assert!(
                lift.1 * 100 >= formulas.len() as u64 * u64::from(ROWS) * 95,
                "a formula's runs were not lifted: {}",
                lift.1
            );
        }
        assert_eq!(walk.0, oracle.0, "walk, parallel={parallel}");
        assert_eq!(lift.0, oracle.0, "lift, parallel={parallel}");
    }
}

/// Criteria kernels (P2-M4): SUMIF(S)/COUNTIF(S)/AVERAGEIF(S) over
/// invariant ranges index them once per run. Fact columns (ingested in
/// 16-row chunks, so sums cross chunk boundaries) mix numbers, numeric
/// text, dates, blanks, booleans, errors, mixed-case text and text with
/// LIKE metacharacters; report criteria vary per member (numeric text,
/// numeric comparisons, text equality and inequality, empty text, wildcards,
/// blanks). Kernel, walk and per-cell oracle agree on values after load
/// and after edits to facts and criteria, sequential and parallel.
#[test]
fn family_criteria_kernel_matches_per_cell() {
    const FACTS: u32 = 90;
    const REPORT: u32 = 40;
    let fact = |r: u32, c: u32| -> LiteralValue {
        match c {
            // Region: mixed case, one LIKE metacharacter, blanks, numbers.
            1 => match r % 7 {
                0 => LiteralValue::Text("North".into()),
                1 => LiteralValue::Text("north".into()),
                2 => LiteralValue::Text("So%th".into()),
                3 => LiteralValue::Text("East_1".into()),
                4 => LiteralValue::Empty,
                5 => LiteralValue::Number(5.0),
                _ => LiteralValue::Text("West".into()),
            },
            // Amount: numbers, numeric text, dates, booleans, errors.
            2 => match r % 12 {
                0 => LiteralValue::Text("12".into()),
                1 => LiteralValue::Date(chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
                2 => LiteralValue::Boolean(true),
                3 => LiteralValue::Error(formualizer_common::ExcelError::new(
                    formualizer_common::ExcelErrorKind::Na,
                )),
                4 => LiteralValue::Number(-0.0),
                5 => LiteralValue::Empty,
                // A number that numeric-text criteria (`"12"`) must match.
                11 => LiteralValue::Number(12.0),
                _ => LiteralValue::Number((r as f64) * 1.25 - 30.0),
            },
            // Qty.
            _ => LiteralValue::Number(((r * 7) % 13) as f64 + 0.1),
        }
    };
    let crit = |r: u32| -> (LiteralValue, LiteralValue) {
        let region = match r % 10 {
            0 => LiteralValue::Text("north".into()),
            1 => LiteralValue::Text("NORTH".into()),
            2 => LiteralValue::Text("so%th".into()),
            3 => LiteralValue::Text("<>north".into()),
            4 => LiteralValue::Text("East_1".into()),
            5 => LiteralValue::Text("".into()),
            6 => LiteralValue::Text("W*".into()),
            7 => LiteralValue::Number(5.0),
            // Numeric text matches the number 5 in a mixed column.
            9 => LiteralValue::Text("5".into()),
            _ => LiteralValue::Text("=West".into()),
        };
        let amount = match r % 7 {
            0 => LiteralValue::Text(format!(">={}", r as f64 - 20.0)),
            1 => LiteralValue::Text(format!("<{}", r)),
            2 => LiteralValue::Number(0.0),
            3 => LiteralValue::Text("<>0".into()),
            4 => LiteralValue::Text(">-1E+300".into()),
            // Numeric text matches both the number 12 and the text "12".
            5 => LiteralValue::Text("12".into()),
            _ => LiteralValue::Number(r as f64 * 1.25 - 30.0),
        };
        (region, amount)
    };
    let formulas: [&str; 9] = [
        "=COUNTIFS(Facts!$A$1:$A$90,A{r},Facts!$B$1:$B$90,B{r})",
        "=SUMIFS(Facts!$C$1:$C$90,Facts!$A$1:$A$90,A{r},Facts!$B$1:$B$90,B{r})",
        "=AVERAGEIFS(Facts!$B$1:$B$90,Facts!$A$1:$A$90,A{r})",
        "=SUMIF(Facts!$B$1:$B$90,B{r})",
        "=SUMIF(Facts!$A$1:$A$90,A{r},Facts!$C$1:$C$90)",
        "=COUNTIF(Facts!$B$1:$B$90,B{r})",
        "=AVERAGEIF(Facts!$C$1:$C$90,\">\"&B{r})",
        "=SUMIFS(Facts!$B$1:$B$90,Facts!$C$1:$C$90,\">\"&(ROW()/5),Facts!$A$1:$A$90,\"<>west\")",
        "=COUNTIFS(Facts!$C$1:$C$90,\"<\"&ROW(),Facts!$C$1:$C$90,\">=\"&(ROW()/3))",
    ];
    // The same formulas over whole and open-ended columns (views of the
    // used extent).
    let whole: Vec<String> = formulas
        .iter()
        .enumerate()
        .map(|(k, f)| {
            let f = f.replace("$1:$", ":$").replace("$90", "");
            if k % 2 == 0 {
                f
            } else {
                // `$A$1:$A` style for the odd ones.
                f.replace("Facts!$A:$A", "Facts!$A$1:$A")
                    .replace("Facts!$C:$C", "Facts!$C$1:$C")
            }
        })
        .collect();
    for formulas in [
        formulas.iter().map(|f| f.to_string()).collect::<Vec<_>>(),
        whole,
    ] {
        let run = |config: EvalConfig| -> (Vec<Vec<String>>, u64) {
            let mut e = Engine::new(TestWorkbook::new(), config);
            e.add_sheet("Report").unwrap();
            {
                let mut ab = e.begin_bulk_ingest_arrow();
                ab.add_sheet("Facts", 3, 16);
                for r in 1..=FACTS {
                    ab.append_row("Facts", &[fact(r, 1), fact(r, 2), fact(r, 3)])
                        .unwrap();
                }
                ab.finish().unwrap();
            }
            let mut cells = Vec::new();
            for r in 1..=REPORT {
                let (a, b) = crit(r);
                e.set_cell_value("Report", r, 1, a).unwrap();
                e.set_cell_value("Report", r, 2, b).unwrap();
                for (k, f) in formulas.iter().enumerate() {
                    let c = 4 + k as u32;
                    e.set_cell_formula(
                        "Report",
                        r,
                        c,
                        parse(f.replace("{r}", &r.to_string())).unwrap(),
                    )
                    .unwrap();
                    cells.push((r, c));
                }
            }
            let snapshot = |e: &Engine<TestWorkbook>| {
                cells
                    .iter()
                    .map(|&(r, c)| key(e.get_cell_value("Report", r, c)))
                    .collect::<Vec<_>>()
            };
            let mut out = Vec::new();
            e.evaluate_all().unwrap();
            out.push(snapshot(&e));
            let edits = [
                ("Facts", 20, 2, LiteralValue::Number(1e308)),
                ("Facts", 21, 2, LiteralValue::Number(1e308)),
                ("Facts", 33, 1, LiteralValue::Text("NORTH".into())),
                ("Facts", 34, 3, LiteralValue::Text("x".into())),
                ("Report", 3, 1, LiteralValue::Text("west".into())),
                ("Report", 8, 2, LiteralValue::Text(">=0".into())),
            ];
            for (s, r, c, v) in edits {
                e.set_cell_value(s, r, c, v).unwrap();
                e.evaluate_all().unwrap();
                out.push(snapshot(&e));
            }
            (out, e.criteria_kernel_members_for_test())
        };
        for parallel in [false, true] {
            let base = EvalConfig {
                enable_parallel: parallel,
                ..arrow_eval_config()
            };
            let oracle = run(EvalConfig {
                family_execution: false,
                ..base.clone()
            });
            let kernel = run(base);
            assert_eq!(oracle.1, 0);
            assert!(
                kernel.1 > 0,
                "no member took the criteria kernel (parallel={parallel}) {formulas:?}"
            );
            assert_eq!(kernel.0, oracle.0, "parallel={parallel} {formulas:?}");
        }
    }
}

/// Program 3: a family template's run-invariant calls (a lookup's column
/// index from a MATCH over a named header row, an INDEX's column) are
/// computed once per run and bound for the walk of every member; values
/// equal the per-cell oracle's, at first evaluation and after edits
/// (including edits to the invariant call's inputs).
#[test]
fn family_walk_binds_run_invariant_calls() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord, RangeRef};
    const ROWS: u32 = 60;
    let run = |config: EvalConfig| -> (Vec<String>, u64) {
        let mut e = Engine::new(TestWorkbook::new(), config);
        e.add_sheet("Data").unwrap();
        // Data: a key column A (sorted numbers), header row 1 (texts).
        for c in 1..=5u32 {
            e.set_cell_value("Data", 1, c, LiteralValue::Text(format!("h{c}")))
                .unwrap();
        }
        for r in 2..=40u32 {
            e.set_cell_value("Data", r, 1, LiteralValue::Number(f64::from(r * 3)))
                .unwrap();
            for c in 2..=5u32 {
                e.set_cell_value("Data", r, c, LiteralValue::Number(f64::from(r * c)))
                    .unwrap();
            }
        }
        let data = e.sheet_id("Data").unwrap();
        let at = |r: u32, c: u32| CellRef::new(data, Coord::from_excel(r, c, true, true));
        e.define_name(
            "Table",
            NamedDefinition::Range(RangeRef::new(at(2, 1), at(40, 5))),
            NameScope::Workbook,
        )
        .unwrap();
        e.define_name(
            "Heads",
            NamedDefinition::Range(RangeRef::new(at(1, 1), at(1, 5))),
            NameScope::Workbook,
        )
        .unwrap();
        // Sheet1 row 1 holds the header each column looks up.
        for c in 2..=4u32 {
            e.set_cell_value("Sheet1", 1, c, LiteralValue::Text(format!("h{}", c + 1)))
                .unwrap();
        }
        let mut cells = Vec::new();
        for r in 2..=ROWS {
            e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r * 2)))
                .unwrap();
            let fs = [
                (2, format!("=VLOOKUP($A{r},Table,MATCH(B$1,Heads,0))")),
                (3, format!("=VLOOKUP($A{r},Table,MATCH(C$1,Heads,0),FALSE)")),
                (
                    4,
                    format!(
                        "=INDEX(Data!$A$2:$E$40,MATCH($A{r},Data!$A$2:$A$40,1),MATCH(D$1,Data!$A$1:$E$1,0))+SUM(Data!$B$2:$B$5)"
                    ),
                ),
            ];
            for (c, f) in fs {
                e.set_cell_formula("Sheet1", r, c, parse(&f).unwrap())
                    .unwrap();
                cells.push((r, c));
            }
        }
        let snapshot = |e: &Engine<TestWorkbook>| {
            cells
                .iter()
                .map(|&(r, c)| key(e.get_cell_value("Sheet1", r, c)))
                .collect::<Vec<_>>()
                .join(",")
        };
        let mut out = Vec::new();
        e.evaluate_all().unwrap();
        out.push(snapshot(&e));
        let edits = [
            ("Sheet1", 1, 3, LiteralValue::Text("h2".into())),
            ("Data", 1, 4, LiteralValue::Text("zz".into())),
            ("Data", 3, 2, LiteralValue::Number(-1.0)),
            ("Sheet1", 7, 1, LiteralValue::Number(50.0)),
        ];
        for (s, r, c, v) in edits {
            e.set_cell_value(s, r, c, v).unwrap();
            e.evaluate_all().unwrap();
            out.push(snapshot(&e));
        }
        (out, e.invariant_bound_members_for_test())
    };
    for parallel in [false, true] {
        let base = EvalConfig {
            enable_parallel: parallel,
            ..arrow_eval_config()
        };
        let oracle = run(EvalConfig {
            family_execution: false,
            ..base.clone()
        });
        let bound = run(base);
        assert_eq!(oracle.1, 0);
        assert!(
            bound.1 > 0,
            "no member was walked with bound invariant calls"
        );
        assert_eq!(bound.0, oracle.0, "parallel={parallel}");
    }
}
