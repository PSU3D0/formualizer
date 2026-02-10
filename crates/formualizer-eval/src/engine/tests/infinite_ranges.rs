//! Tests for infinite and partial ranges resolved to used-region (Milestone 10)

use crate::engine::graph::editor::vertex_editor::VertexEditor;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ReferenceType;
use formualizer_parse::parser::parse;

fn range_limit_config(limit: usize) -> EvalConfig {
    EvalConfig {
        range_expansion_limit: limit,
        ..Default::default()
    }
}

#[test]
fn infinite_column_empty_sheet_sum_count_are_zero() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, range_limit_config(16));

    // =SUM(A:A) in B1
    let ast_sum = parse("=SUM(A:A)").unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast_sum).unwrap();
    // =COUNT(A:A) in B2
    let ast_cnt = parse("=COUNT(A:A)").unwrap();
    engine.set_cell_formula("Sheet1", 2, 2, ast_cnt).unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2).unwrap(),
        LiteralValue::Number(0.0)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2).unwrap(),
        LiteralValue::Number(0.0)
    );
}

#[test]
fn infinite_column_sparse_sum_and_count_correct() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, range_limit_config(16));

    // Sparse values in column A
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1000, 1, LiteralValue::Int(5))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 500_000, 1, LiteralValue::Int(2))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A:A)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=COUNT(A:A)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2).unwrap(),
        LiteralValue::Number(17.0)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2).unwrap(),
        LiteralValue::Number(3.0)
    );
}

#[test]
fn infinite_row_sum_and_count_correct() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, range_limit_config(16));

    // Values across row 1 (cols 2..=27 i.e. B..AA)
    let mut sum: i64 = 0;
    let mut count: i64 = 0;
    for c in 2..=27 {
        let v = (c - 1) as i64; // 1..=26
        sum += v;
        count += 1;
        engine
            .set_cell_value("Sheet1", 1, c, LiteralValue::Int(v))
            .unwrap();
    }

    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=SUM(1:1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=COUNT(1:1)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1).unwrap(),
        LiteralValue::Number(sum as f64)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1).unwrap(),
        LiteralValue::Number(count as f64)
    );
}

#[test]
fn partial_ranges_column_tail_and_head_bounds() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, range_limit_config(16));

    // For A1:A (open end) and A:A10 (open start)
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(5))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 20, 1, LiteralValue::Int(7))
        .unwrap();

    // SUM(A1:A) = 10+5+7
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A)").unwrap())
        .unwrap();
    // COUNT(A1:A) = 3
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=COUNT(A1:A)").unwrap())
        .unwrap();
    // SUM(A:A10) = rows 1..10 only => 10 + 5 = 15
    engine
        .set_cell_formula("Sheet1", 3, 2, parse("=SUM(A:A10)").unwrap())
        .unwrap();
    // COUNT(A:A10) = 2
    engine
        .set_cell_formula("Sheet1", 4, 2, parse("=COUNT(A:A10)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2).unwrap(),
        LiteralValue::Number(22.0)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2).unwrap(),
        LiteralValue::Number(3.0)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2).unwrap(),
        LiteralValue::Number(15.0)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2).unwrap(),
        LiteralValue::Number(2.0)
    );
}

#[test]
fn invalidation_on_growth_column_and_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, range_limit_config(16));

    // Column case: A1..A10 = 1
    for r in 1..=10u32 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Int(1))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=SUM(A:A)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3).unwrap(),
        LiteralValue::Number(10.0)
    );

    // Grow used region: A1000 = 1
    engine
        .set_cell_value("Sheet1", 1000, 1, LiteralValue::Int(1))
        .unwrap();
    let _res = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3).unwrap(),
        LiteralValue::Number(11.0)
    );

    // Row case: row 2 usage; 1:1 sum in A3
    for c in 1..=10u32 {
        engine
            .set_cell_value("Sheet1", 1, c, LiteralValue::Int(1))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=SUM(1:1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1).unwrap(),
        LiteralValue::Number(10.0)
    );

    // Grow used region horizontally: (1, 1000) = 1
    engine
        .set_cell_value("Sheet1", 1, 1000, LiteralValue::Int(1))
        .unwrap();
    let _res2 = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1).unwrap(),
        LiteralValue::Number(11.0)
    );
}

#[test]
fn invalidation_on_shrink_via_empty() {
    // Shrink by setting a previously numeric cell to Empty
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, range_limit_config(16));

    for r in 1..=10u32 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Int(1))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A:A)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2).unwrap(),
        LiteralValue::Number(10.0)
    );

    // Set A10 to Empty, sum should drop to 9
    engine
        .set_cell_value("Sheet1", 10, 1, LiteralValue::Empty)
        .unwrap();
    let _res = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2).unwrap(),
        LiteralValue::Number(9.0)
    );
}

#[test]
fn unbounded_ranges_resolve_with_expected_dims() {
    let engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet = "Sheet1";
    // A:A
    let r1 = ReferenceType::range(Some(sheet.into()), None, Some(1), None, Some(1));
    let rv1 = engine.resolve_range_view(&r1, sheet).unwrap();
    let (r1_rows, r1_cols) = rv1.dims();
    assert_eq!(r1_cols, 1);
    assert!(r1_rows >= 1_000_000, "expected full column height");
    // 1:1
    let r2 = ReferenceType::range(Some(sheet.into()), Some(1), None, Some(1), None);
    let rv2 = engine.resolve_range_view(&r2, sheet).unwrap();
    let (r2_rows, r2_cols) = rv2.dims();
    assert_eq!(r2_rows, 1);
    assert!(r2_cols >= 10_000, "expected wide row");
    // A1:A (partial)
    let r3 = ReferenceType::range(Some(sheet.into()), Some(1), Some(1), None, Some(1));
    let rv3 = engine.resolve_range_view(&r3, sheet).unwrap();
    let (r3_rows, r3_cols) = rv3.dims();
    assert_eq!(r3_cols, 1);
    assert!(r3_rows >= 1_000_000);
    // A:A10 (partial)
    let r4 = ReferenceType::range(Some(sheet.into()), None, Some(1), Some(10), Some(1));
    let rv4 = engine.resolve_range_view(&r4, sheet).unwrap();
    let (r4_rows, r4_cols) = rv4.dims();
    assert_eq!(r4_cols, 1);
    assert_eq!(r4_rows, 10);
}

#[test]
fn used_region_growth_shrink_has_zero_stripe_churn() {
    let cfg = EvalConfig {
        enable_parallel: false, // isolate potential rayon contention in this churn test
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "Sheet1";

    // Seed baseline and dependent formula C1 = SUM(A:A)
    engine
        .set_cell_value(sheet, 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    let ast = parse("=SUM(A:A)").unwrap();
    engine.set_cell_formula(sheet, 1, 3, ast).unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Reset instrumentation
    engine.graph.reset_instr();

    // Growth: A1000 = 1
    engine
        .set_cell_value(sheet, 1000, 1, LiteralValue::Number(1.0))
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value(sheet, 1, 3).unwrap(),
        LiteralValue::Number(6.0)
    );
    let instr1 = engine.graph.instr();
    assert_eq!(instr1.stripe_inserts, 0);
    assert_eq!(instr1.stripe_removes, 0);
    assert_eq!(instr1.edges_added, 0);

    // Shrink: clear A1
    engine.graph.reset_instr();
    engine
        .set_cell_value(sheet, 1, 1, LiteralValue::Empty)
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value(sheet, 1, 3).unwrap(),
        LiteralValue::Number(1.0)
    );
    let instr2 = engine.graph.instr();
    assert_eq!(instr2.stripe_inserts, 0);
    assert_eq!(instr2.stripe_removes, 0);
    assert_eq!(instr2.edges_added, 0);
}

#[test]
fn edge_churn_on_insert_delete_rows_is_bounded() {
    let cfg = EvalConfig {
        enable_parallel: false, // isolate potential rayon contention in this churn test
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = engine.default_sheet_name().to_string();

    // Seed A1..A100 = 1 and C1 = SUM(A:A)
    for r in 1..=100u32 {
        engine
            .set_cell_value(&sheet, r, 1, LiteralValue::Int(1))
            .unwrap();
    }
    engine
        .set_cell_formula(&sheet, 1, 3, parse("=SUM(A:A)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Reset counters
    engine.graph.reset_instr();

    // Insert 5 rows at row 50
    let sheet_id = engine.default_sheet_id();
    {
        let mut editor = VertexEditor::new(&mut engine.graph);
        editor.insert_rows(sheet_id, 50, 5).unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    // Bounded churn: some stripes may shift but should not explode
    let instr = engine.graph.instr();
    assert!(
        instr.stripe_inserts <= 10,
        "too many stripe inserts: {}",
        instr.stripe_inserts
    );
    assert!(
        instr.stripe_removes <= 10,
        "too many stripe removes: {}",
        instr.stripe_removes
    );
    // Edges remain compressed; direct edges should not blow up
    assert!(
        instr.edges_added <= 50,
        "too many edges added: {}",
        instr.edges_added
    );

    // Delete 5 rows starting at 20
    engine.graph.reset_instr();
    {
        let mut editor = VertexEditor::new(&mut engine.graph);
        editor.delete_rows(sheet_id, 20, 5).unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    let instr2 = engine.graph.instr();
    assert!(instr2.stripe_inserts <= 10);
    assert!(instr2.stripe_removes <= 10);
    assert!(instr2.edges_added <= 50);
}

#[test]
fn edge_churn_on_insert_delete_columns_is_bounded() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet = engine.default_sheet_name().to_string();

    // Seed row 1, B..K = 1 and A3 = SUM(1:1)
    for c in 2..=11u32 {
        engine
            .set_cell_value(&sheet, 1, c, LiteralValue::Int(1))
            .unwrap();
    }
    engine
        .set_cell_formula(&sheet, 3, 1, parse("=SUM(1:1)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    engine.graph.reset_instr();
    let sheet_id = engine.default_sheet_id();
    {
        let mut editor = VertexEditor::new(&mut engine.graph);
        editor.insert_columns(sheet_id, 5, 3).unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    let instr = engine.graph.instr();
    assert!(instr.stripe_inserts <= 10);
    assert!(instr.stripe_removes <= 10);
    assert!(instr.edges_added <= 50);

    engine.graph.reset_instr();
    {
        let mut editor = VertexEditor::new(&mut engine.graph);
        editor.delete_columns(sheet_id, 4, 2).unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    let instr2 = engine.graph.instr();
    assert!(instr2.stripe_inserts <= 10);
    assert!(instr2.stripe_removes <= 10);
    assert!(instr2.edges_added <= 50);
}
