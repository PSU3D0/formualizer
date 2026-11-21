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

#[test]
fn blocker1_criteria_mask_preserves_rangetail_on_empty_sheet() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    engine.add_sheet("Out").unwrap();

    for (row, formula, expected) in [
        (1, "=COUNTIF(S!A1:A100+1, 1)", 100.0),
        (2, "=SUMIF(S!A1:A100+1, 1)", 100.0),
        (3, "=COUNTIF(S!A1:A100+1, \"\")", 0.0),
        (4, "=AVERAGEIF(S!A1:A100+1, 1)", 1.0),
    ] {
        engine
            .set_cell_formula("Out", row, 1, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Out", row, 1),
            Some(LiteralValue::Number(expected)),
            "formula: {formula}"
        );
    }
}

#[test]
fn blocker2_filter_uses_corner_tail_when_array_taller_than_include_active() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Data").unwrap();
    engine.add_sheet("Left").unwrap();
    engine.add_sheet("Right").unwrap();
    engine.add_sheet("Out").unwrap();

    for r in 1..=10u32 {
        engine
            .set_cell_value("Data", r, 1, LiteralValue::Int(i64::from(r)))
            .unwrap();
    }
    // Left!B1:D10 has active=(2, 1), Right!E1:G1 has active=(1, 1).
    // In cols C..D vs F..G, Empty=Empty is TRUE in right_tail (rows 0..2) AND corner_tail (rows 2..10).
    engine
        .set_cell_value("Left", 1, 2, LiteralValue::Int(5))
        .unwrap();
    engine
        .set_cell_value("Left", 2, 2, LiteralValue::Int(5))
        .unwrap();
    engine
        .set_cell_value("Right", 1, 5, LiteralValue::Int(1))
        .unwrap();

    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=SUM(FILTER(Data!A1:A10, Left!B1:D10=Right!E1:G1))").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Out",
            2,
            1,
            parse("=ROWS(FILTER(Data!A1:A10, Left!B1:D10=Right!E1:G1))").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Out", 1, 1),
        Some(LiteralValue::Number(55.0))
    );
    assert_eq!(
        engine.get_cell_value("Out", 2, 1),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn blocker3_expand_to_preserves_rangetail_invariants_and_empty_padding() {
    // Repro A: shrink rows & grow cols on a tailed view must not panic or underflow.
    let mut engine_a = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine_a.add_sheet("S").unwrap();
    for r in 1..=10u32 {
        engine_a
            .set_cell_value("S", r, 1, LiteralValue::Int(1))
            .unwrap();
        engine_a
            .set_cell_value("S", r, 2, LiteralValue::Int(1))
            .unwrap();
    }
    for r in 1..=50u32 {
        engine_a
            .set_cell_value("S", r, 3, LiteralValue::Int(10))
            .unwrap();
    }
    // C1:C100+1 has rows=100, cols=1, active=(50,1), right_tail=[], bottom_tail=[1.0].
    // expand_to(10, 2) must keep rows 0..10 col 0 (10 * 11 = 110) and pad col 1 with Empty (0).
    assert_eq!(
        eval_at(&mut engine_a, 1, 10, "=SUMIF(A1:B10, \">0\", C1:C100+1)"),
        LiteralValue::Number(110.0)
    );

    // Repro B: growing rows past a tailed view's original logical bounds must pad with Empty,
    // not extrapolate bottom_tail.
    let mut engine_b = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine_b.add_sheet("Crit").unwrap();
    engine_b.add_sheet("Val").unwrap();
    engine_b.add_sheet("Out").unwrap();
    for r in 1..=20u32 {
        engine_b
            .set_cell_value("Crit", r, 1, LiteralValue::Int(1))
            .unwrap();
    }
    engine_b
        .set_cell_value("Val", 1, 2, LiteralValue::Int(10))
        .unwrap();
    engine_b
        .set_cell_value("Val", 2, 2, LiteralValue::Int(10))
        .unwrap();
    // Val!B1:B10+1 has 11, 11, and 8 ones => sum 30; rows 11..20 must be Empty, not 1.
    engine_b
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=SUMIF(Crit!A1:A20, \">0\", Val!B1:B10+1)").unwrap(),
        )
        .unwrap();
    engine_b.evaluate_all().unwrap();
    assert_eq!(
        engine_b.get_cell_value("Out", 1, 1),
        Some(LiteralValue::Number(30.0))
    );
}

#[test]
fn blocker4_eval_if_core_visits_tail_rows_when_driver_is_sparse_untailed() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    engine.add_sheet("Out").unwrap();

    // Only row 1 populated on S (sheet.nrows == 1).
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("S", 1, 2, LiteralValue::Int(10))
        .unwrap();

    for (row, formula, expected) in [
        (1, "=SUMIF(S!A1:A100, \"\", S!B1:B100+1)", 99.0),
        (2, "=AVERAGEIF(S!A1:A100, \"\", S!B1:B100+1)", 1.0),
        (3, "=COUNTIFS(S!A1:A100, \"\", S!B1:B100+1, 1)", 99.0),
        (4, "=SUMIFS(S!B1:B100+1, S!A1:A100, \"\")", 99.0),
        (5, "=AVERAGEIFS(S!B1:B100+1, S!A1:A100, \"\")", 1.0),
        // Combined BLOCKER 3 + 4: shorter tailed sum_range expanded over sparse criteria range
        (6, "=SUMIF(S!A1:A100, \"\", S!B1:B10+1)", 9.0),
    ] {
        engine
            .set_cell_formula("Out", row, 1, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Out", row, 1),
            Some(LiteralValue::Number(expected)),
            "formula: {formula}"
        );
    }
}

#[test]
fn should_fix5_row_major_error_precedence_on_2d_tailed_ranges() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Left").unwrap();
    engine.add_sheet("Out").unwrap();

    // Left!A1:D10 has active=(2, 1):
    //   (0, 0) = 1, (0, 1..4) = Empty -> IF(...="", 1/0, ...) gives (0,0)=1, right_tail[0]=#DIV/0!
    //   (1, 0) = #N/A                 -> core (1,0)=#N/A
    // Row-major first error MUST be #DIV/0! (row 0 right_tail), not #N/A (row 1 core).
    engine
        .set_cell_value("Left", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value(
            "Left",
            2,
            1,
            LiteralValue::Error(formualizer_common::ExcelError::new(ExcelErrorKind::Na)),
        )
        .unwrap();

    for (row, formula) in [
        (1, "=AND(IF(Left!A1:D10=\"\", 1/0, Left!A1:D10))"),
        (2, "=OR(IF(Left!A1:D10=\"\", 1/0, Left!A1:D10))"),
        (3, "=XOR(IF(Left!A1:D10=\"\", 1/0, Left!A1:D10))"),
        (4, "=SUMPRODUCT(IF(Left!A1:D10=\"\", 1/0, Left!A1:D10))"),
        (5, "=SUMPRODUCT(IF(Left!A1:D10=\"\", 1/0, Left!A1:D10), 1)"),
    ] {
        engine
            .set_cell_formula("Out", row, 1, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        assert!(
            matches!(
                engine.get_cell_value("Out", row, 1),
                Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Div
            ),
            "formula {formula} got {:?}",
            engine.get_cell_value("Out", row, 1)
        );
    }
}
