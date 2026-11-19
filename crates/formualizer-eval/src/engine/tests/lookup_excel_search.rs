//! LOOKUP searches the way Excel does (#586). Every case is a cell of one of
//! two probe workbooks that Excel desktop recalculated
//! (`lookup-586-probe_computed.xlsx`, sheet `Probe`, and
//! `lookup-586-probe2_computed.xlsx`, sheet `Probe2`), on the same grid, with
//! the value Excel cached. Left out: cases 37 and 38 (Excel's text order, a
//! follow-up), 6m, 20m and 20v (MATCH and VLOOKUP, which keep their `#N/A` on
//! unsorted data, #283). The dependency of the cells read past the end of a
//! result reference (cases 33, R5–R7, R12, R13) is tested in
//! `lookup_result_extension.rs`.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

#[derive(Debug, Clone, Copy)]
enum Excel {
    Number(f64),
    Text(&'static str),
    Error(ExcelErrorKind),
}

use Excel::{Number, Text};
const NA: Excel = Excel::Error(ExcelErrorKind::Na);

/// `"K12"` as 1-based (row, column).
fn a1(cell: &str) -> (u32, u32) {
    let split = cell.find(|c: char| c.is_ascii_digit()).unwrap();
    let col = cell[..split]
        .bytes()
        .fold(0, |acc, b| acc * 26 + u32::from(b - b'A' + 1));
    (cell[split..].parse().unwrap(), col)
}

/// A cell of the grid: a number, text, or a formula (`=` first).
#[derive(Clone, Copy)]
enum Cell {
    N(f64),
    T(&'static str),
    F(&'static str),
}

use Cell::{F, N, T};

fn put(e: &mut Engine<TestWorkbook>, sheet: &str, cell: &str, value: Cell) {
    let (row, col) = a1(cell);
    match value {
        N(v) => e
            .set_cell_value(sheet, row, col, LiteralValue::Number(v))
            .unwrap(),
        T(s) => e
            .set_cell_value(sheet, row, col, LiteralValue::Text(s.into()))
            .unwrap(),
        F(f) => e
            .set_cell_formula(sheet, row, col, parse(f).unwrap())
            .unwrap(),
    }
}

/// Sheet `Probe` of the first probe (its generator's data, cell for cell).
fn probe_grid() -> Vec<(String, Cell)> {
    let mut g: Vec<(String, Cell)> = [
        ("K1", N(1.)),
        ("L1", N(0.)),
        ("M1", N(1.)),
        ("K2", N(10.)),
        ("L2", N(20.)),
        ("M2", N(30.)),
        ("K4", N(5.)),
        ("N4", N(7.)),
        ("O4", N(9.)),
        ("K12", F("=0.1+0.2")),
        ("K13", F("=\"\"")),
        ("K14", T("a")),
        ("L13", N(1.)),
        ("L14", N(2.)),
        ("K16", N(1.)),
        ("K17", T("2")),
        ("K18", N(3.)),
        ("L24", N(100.)),
        ("M24", N(200.)),
        ("N24", N(300.)),
    ]
    .map(|(c, v)| (c.to_string(), v))
    .to_vec();
    for (r, k, m) in [
        ("6", Some(3.), Some(3.)),
        ("7", Some(1.), Some(1.)),
        ("8", None, None),
        ("9", Some(3.), None),
        ("10", Some(1.), Some(3.)),
    ] {
        if let Some(k) = k {
            g.push((format!("K{r}"), N(k)));
        }
        if let Some(m) = m {
            g.push((format!("M{r}"), N(m)));
        }
        let pos = f64::from(r.parse::<u32>().unwrap() - 5);
        g.push((format!("L{r}"), N(pos)));
        g.push((format!("N{r}"), N(pos)));
    }
    g.extend(
        [
            ("K20", N(1.)),
            ("K21", N(2.)),
            ("K22", N(3.)),
            ("L20", N(10.)),
            ("L21", N(20.)),
            ("L22", N(30.)),
            ("K26", N(1.)),
            ("L26", N(4.)),
            ("M26", N(10.)),
            ("K27", N(2.)),
            ("L27", N(5.)),
            ("M27", N(20.)),
            ("K28", N(3.)),
            ("L28", N(6.)),
            ("M28", N(30.)),
            ("T1", N(5.)),
            ("T2", N(6.)),
            ("T3", N(9.)),
            ("T4", N(1.)),
            ("T5", N(8.)),
            ("T6", N(4.)),
            ("V1", N(1.)),
            ("W1", N(2.)),
            ("X1", N(3.)),
            ("V2", N(4.)),
            ("W2", N(5.)),
            ("X2", N(6.)),
        ]
        .map(|(c, v)| (c.to_string(), v)),
    );
    g
}

/// The LOOKUP cases of sheet `Probe` (see the module comment).
const PROBE: &[(&str, &str, Excel)] = &[
    ("1", "=LOOKUP(2,{1,#DIV/0!,1},{10,20,30})", Number(30.)),
    ("2", "=LOOKUP(2,1/(K1:M1<>0),K2:M2)", Number(30.)),
    ("3", "=LOOKUP(2,1/(K4:O4<>\"\"),K4:O4)", Number(9.)),
    (
        "4",
        "=LOOKUP(33,{33;33;33;33;100;33;33;33;33;33},{1;2;3;4;5;6;7;8;9;10})",
        Number(4.),
    ),
    (
        "5",
        "=LOOKUP(33,{33;33;33;33;33;33;33;33;33;33;8;33;33;6;33;33;4;33;33;33;33},\
         {1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21})",
        Number(16.),
    ),
    (
        "6",
        "=LOOKUP(1,{1,1,1,1,1,0,1},{1,2,3,4,5,6,7})",
        Number(5.),
    ),
    (
        "7",
        "=LOOKUP(1,{1,1,1,1,1,0,1,-1,1,1,1,1,1,0,1},{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15})",
        Number(13.),
    ),
    ("8", "=LOOKUP(2,K6:K10,L6:L10)", Number(2.)),
    ("9", "=LOOKUP(2,M6:M10,N6:N10)", Number(2.)),
    ("10", "=LOOKUP(2,{1,\"2\",3})", Number(1.)),
    ("10g", "=LOOKUP(2,K16:K18)", Number(1.)),
    ("11", "=LOOKUP(2,{1,TRUE,3},{10,20,30})", Number(10.)),
    ("12", "=LOOKUP(1,{TRUE})", NA),
    (
        "13",
        "=LOOKUP(2,{1,\"a\",2,\"b\",3},{1,2,3,4,5})",
        Number(3.),
    ),
    ("14", "=LOOKUP(TRUE,{FALSE,TRUE},{1,2})", Number(2.)),
    (
        "15",
        "=LOOKUP(\"b\",{\"A\",\"B\",\"C\"},{1,2,3})",
        Number(2.),
    ),
    ("15b", "=LOOKUP(\"a\",{\"A\",\"a\"},{1,2})", Number(2.)),
    ("16", "=LOOKUP(Z1,{-1,0,1},{1,2,3})", Number(2.)),
    (
        "17",
        "=LOOKUP(2,{5,#N/A,#N/A,#N/A,1},{1,2,3,4,5})",
        Number(5.),
    ),
    (
        "18",
        "=LOOKUP(2,{1,#N/A,#N/A,2,2,5},{1,2,3,4,5,6})",
        Number(5.),
    ),
    ("19", "=LOOKUP(7,{9,8,7,6,5},{1,2,3,4,5})", Number(3.)),
    ("20a", "=LOOKUP(1,T1:T6)", NA),
    ("20b", "=LOOKUP(1,T1:T100)", NA),
    ("20c", "=LOOKUP(1,T:T)", Number(1.)),
    ("20d", "=LOOKUP(1,INDIRECT(\"T:T\"))", Number(1.)),
    ("21", "=LOOKUP(2,V:X)", Number(3.)),
    ("21b", "=LOOKUP(2,V1:X2)", Number(5.)),
    ("22", "=LOOKUP(\"2\",{1,2,3})", NA),
    (
        "23",
        "=LOOKUP(\"2\",{\"1\",\"10\",\"2\"},{\"a\",\"b\",\"c\"})",
        Text("c"),
    ),
    ("24", "=LOOKUP(TRUE,{1,2,3})", NA),
    ("25", "=LOOKUP(0.3,K12)", NA),
    ("26", "=LOOKUP(0,{0,0.0000000000001},{10,20})", Number(10.)),
    (
        "27",
        "=LOOKUP(DATE(2024,1,2),{45292,45293,45294},{1,2,3})",
        Number(2.),
    ),
    ("28", "=LOOKUP(\"\",K13:K14,L13:L14)", Number(1.)),
    ("29", "=LOOKUP(Z1,K13:K14,L13:L14)", NA),
    ("30", "=LOOKUP(2,{1,2},{10,#N/A})", NA),
    (
        "31",
        "=LOOKUP(1/0,{1,2})",
        Excel::Error(ExcelErrorKind::Div),
    ),
    ("32", "=LOOKUP(3,K20:K22,L20)", Number(0.)),
    ("33", "=LOOKUP(3,K20:K22,L20:L21)", Number(30.)),
    ("34", "=LOOKUP(3,{1,2,3},{10,20})", NA),
    ("35", "=LOOKUP(2,K20:K22,L24:N24)", Number(200.)),
    ("36", "=LOOKUP(2,K26:L28,M26:M28)", Number(20.)),
];

/// Sheet `Probe2` of the second probe.
fn probe2_grid() -> Vec<(String, Cell)> {
    let mut g = Vec::new();
    let col_letter = |c: u32| char::from(b'A' + (c - 1) as u8);
    // Step-rule data in columns G, I, K, M (`#` is =NA()), positions next to it.
    for (c, vals) in [
        (7, "3,1,_,_,_,_,3"),
        (9, "_,2,1,_,#,_,3"),
        (11, "1,2,1,_,_,_,3"),
        (13, "3,2,#,#,3"),
    ] {
        for (i, v) in vals.split(',').enumerate() {
            let row = i + 1;
            let cell = match v {
                "_" => None,
                "#" => Some(F("=NA()")),
                num => Some(N(num.parse().unwrap())),
            };
            if let Some(cell) = cell {
                g.push((format!("{}{row}", col_letter(c)), cell));
            }
            g.push((format!("{}{row}", col_letter(c + 1)), N(row as f64)));
        }
    }
    g.extend(
        [
            ("G20", N(1.)),
            ("G21", N(2.)),
            ("G22", N(3.)),
            ("H20", N(10.)),
            ("H21", N(20.)),
            ("H22", N(30.)),
            ("I20", N(1000.)),
            ("I21", N(2000.)),
            ("I22", N(3000.)),
            ("P20", N(10.)),
            ("P21", N(21.)),
            ("P22", N(22.)),
            ("Q20", N(120.)),
            ("R20", N(130.)),
            ("G30", N(1.)),
            ("H30", N(2.)),
            ("I30", N(3.)),
            ("P30", N(100.)),
            ("P31", N(31.)),
            ("P32", N(32.)),
            ("Q30", N(130.)),
            ("R30", N(140.)),
            ("H24", N(100.)),
            ("I24", N(200.)),
            ("J24", N(300.)),
            ("G40", N(1.)),
            ("H40", N(2.)),
            ("I40", N(3.)),
            ("G41", N(4.)),
            ("H41", N(5.)),
            ("I41", N(6.)),
            ("G44", N(1.)),
            ("H44", N(4.)),
            ("G45", N(2.)),
            ("H45", N(5.)),
            ("I44", N(10.)),
            ("I45", N(20.)),
        ]
        .map(|(c, v)| (c.to_string(), v)),
    );
    g
}

/// The cases of sheet `Probe2` (see the module comment).
const PROBE2: &[(&str, &str, Excel)] = &[
    ("R1", "=LOOKUP(2,G1:G7,H1:H7)", NA),
    ("R2", "=LOOKUP(2,I1:I7,J1:J7)", Number(3.)),
    ("R3", "=LOOKUP(2,K1:K7,L1:L7)", Number(3.)),
    ("R4", "=LOOKUP(2,M1:M5,N1:N5)", Number(2.)),
    ("R5", "=LOOKUP(3,G20:G22,P20)", Number(130.)),
    ("R6", "=LOOKUP(3,G30:I30,P30)", Number(140.)),
    ("R7", "=LOOKUP(3,G20:G22,H24:I24)", Number(300.)),
    ("R8", "=LOOKUP(3,G20:G22,H20:H21*1)", NA),
    ("R9", "=LOOKUP(2,G20:G22,H20:I22)", NA),
    ("R10", "=LOOKUP(5,G40:I41,G20:G22)", Number(3.)),
    ("R11", "=LOOKUP(2,G44:H45,I44:I45)", Number(20.)),
    ("R12", "=LOOKUP(3,G20:G22,H21)", Number(0.)),
    ("R13", "=LOOKUP(2,G20:G22,P21:P22)", Number(22.)),
];

fn matches_excel(got: &LiteralValue, want: Excel) -> bool {
    match (got, want) {
        (LiteralValue::Number(a), Number(b)) => *a == b,
        (LiteralValue::Int(a), Number(b)) => *a as f64 == b,
        (LiteralValue::Text(a), Text(b)) => a == b,
        (LiteralValue::Error(a), Excel::Error(b)) => a.kind == b,
        _ => false,
    }
}

/// Builds `grid` on `sheet`, writes each case in column C from row 3 (as in
/// the probe), evaluates, and returns the cases that differ from Excel.
fn run(
    mode: FormulaPlaneMode,
    sheet: &str,
    grid: &[(String, Cell)],
    cases: &[(&str, &str, Excel)],
) -> Vec<String> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (cell, value) in grid {
        put(&mut e, sheet, cell, *value);
    }
    for (i, (_, formula, _)) in cases.iter().enumerate() {
        e.set_cell_formula(sheet, 3 + i as u32, 3, parse(formula).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    cases
        .iter()
        .enumerate()
        .filter_map(|(i, (id, formula, want))| {
            let got = e
                .get_cell_value(sheet, 3 + i as u32, 3)
                .unwrap_or(LiteralValue::Empty);
            (!matches_excel(&got, *want))
                .then(|| format!("case {id} {formula}: {got:?}, Excel {want:?} ({mode:?})"))
        })
        .collect()
}

const MODES: [FormulaPlaneMode; 2] = [
    FormulaPlaneMode::Off,
    FormulaPlaneMode::AuthoritativeExperimental,
];

#[test]
fn lookup_gives_excel_answers_on_the_first_probe() {
    for mode in MODES {
        let wrong = run(mode, "Probe", &probe_grid(), PROBE);
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}

#[test]
fn lookup_gives_excel_answers_on_the_second_probe() {
    for mode in MODES {
        let wrong = run(mode, "Probe2", &probe2_grid(), PROBE2);
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}

/// What 999 more lookup values may add to one LOOKUP over 100,000 cells
/// (which reads the vectors once, #549): an O(n) step or scan per lookup
/// value would take seconds.
const STRESS_BOUND: std::time::Duration = std::time::Duration::from_millis(100);

/// Loads `A1:A100000` from `column_a`, `B` = the row number and
/// `C1:C1000` from `needle`, then times `=SUM(LOOKUP(C1:C1000,A:..,B:..))`
/// against the same LOOKUP of `C1` alone, and returns the sum.
fn stress(column_a: impl Fn(u32) -> LiteralValue, needle: impl Fn(u32) -> f64) -> f64 {
    const ROWS: u32 = 100_000;
    const NEEDLES: u32 = 1_000;
    let mut config = super::common::arrow_eval_config();
    config.enable_parallel = false;
    let mut e = Engine::new(TestWorkbook::default(), config);
    {
        let mut ingest = e.begin_bulk_ingest_arrow();
        ingest.add_sheet("Sheet1", 3, 4096);
        for row in 1..=ROWS {
            let c = if row <= NEEDLES {
                LiteralValue::Number(needle(row))
            } else {
                LiteralValue::Empty
            };
            ingest
                .append_row(
                    "Sheet1",
                    &[column_a(row), LiteralValue::Number(f64::from(row)), c],
                )
                .unwrap();
        }
        ingest.finish().unwrap();
    }
    let mut time = |row: u32, needles: &str| -> std::time::Duration {
        let formula = format!("=SUM(LOOKUP({needles},A1:A{ROWS},B1:B{ROWS}))");
        e.set_cell_formula("Sheet1", row, 4, parse(&formula).unwrap())
            .unwrap();
        let start = std::time::Instant::now();
        e.evaluate_all().unwrap();
        start.elapsed()
    };
    let one = time(1, "C1");
    let all = time(2, &format!("C1:C{NEEDLES}"));
    assert!(
        all.saturating_sub(one) < STRESS_BOUND,
        "1 lookup value: {one:?}, {NEEDLES}: {all:?}"
    );
    match e.get_cell_value("Sheet1", 2, 4) {
        Some(LiteralValue::Number(v)) => v,
        other => panic!("{other:?}"),
    }
}

/// 1,000 equal lookup values over 100,000 equal cells: each search hits an
/// equal cell and returns the end of its run, the last row.
#[test]
fn equal_lookup_values_over_a_long_equal_run() {
    let got = stress(|_| LiteralValue::Number(1.0), |_| 1.0);
    assert_eq!(got, 1_000.0 * 100_000.0);
}

/// 1,000 distinct lookup values over 100,000 errors with the number k at row
/// 10,000·k: each search steps forward over thousands of errors, and finds
/// the largest k not above its lookup value. (With a number every 1,000 rows,
/// a linear step forward is still fast enough to pass.)
#[test]
fn distinct_lookup_values_over_errors_with_sparse_numbers() {
    let got = stress(
        |row| {
            if row % 10_000 == 0 {
                LiteralValue::Number(f64::from(row / 10_000))
            } else {
                LiteralValue::Error(ExcelErrorKind::Div.into())
            }
        },
        f64::from,
    );
    let want: f64 = (1..=1_000u32).map(|v| f64::from(v.min(10) * 10_000)).sum();
    assert_eq!(got, want);
}
