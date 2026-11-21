//! Regression and semantic tests for issue #589 (last-row/last-col array ranges)
//! and issue #590 correctness guards (trailing blanks must remain semantically faithful).

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn eval_at(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, formula: &str) -> LiteralValue {
    engine
        .set_cell_formula("S", row, col, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("S", row, col).unwrap()
}

fn eval_single(engine: &mut Engine<TestWorkbook>, formula: &str) -> LiteralValue {
    eval_at(engine, 1, 10, formula)
}

fn seeded_engine(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    for r in 1..=rows {
        engine
            .set_cell_value("S", r, 1, LiteralValue::Int(i64::from(r % 2)))
            .unwrap();
        engine
            .set_cell_value("S", r, 2, LiteralValue::Int(i64::from(r)))
            .unwrap();
    }
    engine
}

#[test]
fn last_row_array_formulas_match_excel_and_preserve_blank_semantics() {
    let rows = 50u32;
    let total_rows = 1_048_576u64;
    let trailing = total_rows - u64::from(rows);
    // Odd rows in 1..=50: 1, 3, ..., 49 (25 rows, sum = 25^2 = 625)
    // Odd rows in 2..=50: 3, 5, ..., 49 (24 rows, sum = 624)
    // Even rows in 2..=50: 2, 4, ..., 50 (25 zeros)

    for (formula, expected) in [
        ("=SUM((A1:A1048576=1)*B1:B1048576)", 625.0),
        ("=SUM(($A$2:$A$1048576=1)*$B$2:$B$1048576)", 624.0),
        ("=SUM(--(A2:A1048576=1))", 24.0),
        // #590 guard: trailing blanks satisfy ="" and =0, do NOT satisfy <>0
        ("=SUM(--(A1:A1048576=\"\"))", trailing as f64),
        ("=SUM(--(A2:A1048576=0))", (25 + trailing) as f64),
        ("=SUM(--(A1:A1048576<>0))", 25.0),
        // IF with last-row ranges (both inert and blank-counting)
        ("=SUM(IF(A2:A1048576=1,B2:B1048576,0))", 624.0),
        ("=SUM(IF(A1:A1048576=\"\",1,0))", trailing as f64),
        // SUMPRODUCT (product expression and multi-arg)
        ("=SUMPRODUCT((A2:A1048576=1)*(B2:B1048576))", 624.0),
        ("=SUMPRODUCT(--(A2:A1048576=1),B2:B1048576)", 624.0),
        ("=SUMPRODUCT(--(A1:A1048576=\"\"))", trailing as f64),
        // FILTER skipping trailing false rows
        ("=SUM(FILTER(B2:B1048576,A2:A1048576=1))", 624.0),
        // FILTER where trailing rows DO match
        ("=COUNT(FILTER(A1:A1048576,A1:A1048576=\"\"))", 0.0),
        ("=COUNTA(FILTER(A1:A1048576,A1:A1048576=\"\"))", 0.0),
        (
            "=ROWS(FILTER(A1:A1048576,A1:A1048576=\"\"))",
            trailing as f64,
        ),
        // Aggregate functions directly on tailed array outputs
        ("=COUNT(--(A1:A1048576=\"\"))", total_rows as f64),
        ("=COUNTA(A1:A1048576=\"\")", total_rows as f64),
        ("=COUNTBLANK(A1:A1048576=\"\")", 0.0),
        (
            "=COUNTBLANK(IF(A1:A1048576=1,\"x\",\"\"))",
            (25 + trailing) as f64,
        ),
        ("=MIN(--(A1:A1048576=\"\"))", 0.0),
        ("=MAX(--(A1:A1048576=\"\"))", 1.0),
        (
            "=AVERAGE(--(A1:A1048576=\"\"))",
            (trailing as f64) / (total_rows as f64),
        ),
        // Boolean array passed to SUM ignores booleans in ranges
        ("=SUM(A1:A1048576=1)", 0.0),
        // Unary numeric elementwise builtin (COS) over tailed expression (cos(0) == 1)
        ("=SUM(COS(A1:A1048576*0))", total_rows as f64),
        // INDEX past used extent returns blank -> 0, not #REF!
        ("=INDEX(A1:A1048576,1048000)", 0.0),
        ("=INDEX(--(A1:A1048576=\"\"),1048000)", 1.0),
        // ROWS preserves full logical extent
        ("=ROWS(A1:A1048576=1)", total_rows as f64),
        // Non-tail-aware consumer (MATCH) still finds first blank row in tail
        ("=MATCH(1,--(A1:A1048576=\"\"),0)", f64::from(rows + 1)),
    ] {
        let mut engine = seeded_engine(rows);
        assert_eq!(
            eval_single(&mut engine, formula),
            LiteralValue::Number(expected),
            "formula: {formula}"
        );
    }
}

#[test]
fn logical_reductions_and_or_xor_respect_uniform_tail_multiplicity() {
    // 50 filled rows -> 1,048,526 trailing blank rows (EVEN)
    let mut even_engine = seeded_engine(50);
    assert_eq!(
        eval_single(&mut even_engine, "=AND(A1:A1048576<>\"nope\")"),
        LiteralValue::Boolean(true)
    );
    assert_eq!(
        eval_single(&mut even_engine, "=OR(A1:A1048576=\"\")"),
        LiteralValue::Boolean(true)
    );
    assert_eq!(
        eval_single(&mut even_engine, "=XOR(A1:A1048576=\"\")"),
        LiteralValue::Boolean(false)
    );

    // 49 filled rows -> 1,048,527 trailing blank rows (ODD)
    let mut odd_engine = seeded_engine(49);
    assert_eq!(
        eval_single(&mut odd_engine, "=XOR(A1:A1048576=\"\")"),
        LiteralValue::Boolean(true)
    );
}

#[test]
fn wide_last_col_and_2d_tailed_ranges_are_exact() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    for c in 1..=20u32 {
        engine
            .set_cell_value("S", 1, c, LiteralValue::Int(i64::from(c % 2)))
            .unwrap();
        engine
            .set_cell_value("S", 2, c, LiteralValue::Int(i64::from(c)))
            .unwrap();
    }
    // Odd cols 1,3,...,19 => 10 cols, sum = 100
    assert_eq!(
        eval_at(&mut engine, 4, 1, "=SUM((A1:XFD1=1)*A2:XFD2)"),
        LiteralValue::Number(100.0)
    );
    assert_eq!(
        eval_at(&mut engine, 4, 1, "=SUM(--(A1:XFD1=\"\"))"),
        LiteralValue::Number((16_384 - 20) as f64)
    );
    assert_eq!(
        eval_at(&mut engine, 4, 1, "=COLUMNS(A1:XFD1=1)"),
        LiteralValue::Number(16_384.0)
    );

    // 2D range with both right tail (cols B..C beyond used col) and bottom tail + corner tail
    let mut grid_engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    grid_engine.add_sheet("S").unwrap();
    for r in 1..=5u32 {
        grid_engine
            .set_cell_value("S", r, 1, LiteralValue::Int(i64::from(r)))
            .unwrap();
    }
    // A1:C1048576 has 3 * 1_048_576 cells, only 5 populated in col A
    assert_eq!(
        eval_single(&mut grid_engine, "=SUM(--(A1:C1048576=\"\"))"),
        LiteralValue::Number((3 * 1_048_576 - 5) as f64)
    );
}

#[test]
fn empty_sheet_and_tail_errors_and_iferror_and_recalc_growth() {
    // Completely empty sheet: all 1,048,576 cells are blank
    let mut empty_engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    empty_engine.add_sheet("S").unwrap();
    assert_eq!(
        eval_single(&mut empty_engine, "=SUM(--(A1:A1048576=\"\"))"),
        LiteralValue::Number(1_048_576.0)
    );

    // Division by trailing blanks produces #DIV/0! in tail; IFERROR catches it cleanly
    let mut err_engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    err_engine.add_sheet("S").unwrap();
    err_engine
        .set_cell_value("S", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    err_engine
        .set_cell_value("S", 2, 1, LiteralValue::Number(4.0))
        .unwrap();
    assert!(matches!(
        eval_single(&mut err_engine, "=SUM(1/A1:A1048576)"),
        LiteralValue::Error(e) if e.kind == ExcelErrorKind::Div
    ));
    assert_eq!(
        eval_single(&mut err_engine, "=SUM(IFERROR(1/A1:A1048576,0))"),
        LiteralValue::Number(0.75)
    );
    assert_eq!(
        eval_single(&mut err_engine, "=SUM(IFERROR(1/A1:A1048576,2))"),
        LiteralValue::Number(0.75 + 2.0 * (1_048_576 - 2) as f64)
    );

    // Recalculation after writing a cell far beyond the previous active extent
    let mut grow_engine = seeded_engine(10);
    // Odd rows 1,3,5,7,9 => sum = 25
    assert_eq!(
        eval_single(&mut grow_engine, "=SUM((A1:A1048576=1)*B1:B1048576)"),
        LiteralValue::Number(25.0)
    );
    grow_engine
        .set_cell_value("S", 500_000, 1, LiteralValue::Int(1))
        .unwrap();
    grow_engine
        .set_cell_value("S", 500_000, 2, LiteralValue::Int(42))
        .unwrap();
    grow_engine.evaluate_all().unwrap();
    assert_eq!(
        grow_engine.get_cell_value("S", 1, 10),
        Some(LiteralValue::Number(67.0))
    );
}

#[test]
fn oversized_tailed_array_still_respects_budget_and_spill_limits() {
    let mut engine = seeded_engine(10);
    // 26 * 1,048,576 = 27,262,976 > default 2^24 budget => #NUM!
    assert!(matches!(
        eval_at(&mut engine, 1, 30, "=SUM((A1:Z1048576=1)*1)"),
        LiteralValue::Error(e) if e.kind == ExcelErrorKind::Num
    ));
    // Direct spill of 1,048,576 rows => #SPILL!
    assert!(matches!(
        eval_at(&mut engine, 1, 30, "=A1:A1048576=1"),
        LiteralValue::Error(e) if e.kind == ExcelErrorKind::Spill
    ));
}
