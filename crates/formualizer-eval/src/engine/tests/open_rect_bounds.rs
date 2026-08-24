use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, parse};

fn runtime_engine() -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig {
            detection: CycleDetection::Runtime,
            policy: CyclePolicy::Error,
        }),
    )
}

fn formula(text: &str) -> ASTNode {
    parse(text).expect("parse formula")
}

fn set_number(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, value: f64) {
    engine
        .set_cell_value("Sheet1", row, col, LiteralValue::Number(value))
        .expect("set number")
}

fn set_text(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, value: &str) {
    engine
        .set_cell_value("Sheet1", row, col, LiteralValue::Text(value.to_string()))
        .expect("set text")
}

fn number_at(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> f64 {
    match engine.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Number(value)) => value,
        Some(LiteralValue::Int(value)) => value as f64,
        other => panic!("expected number at Sheet1!R{row}C{col}, got {other:?}"),
    }
}

fn seed_constants(engine: &mut Engine<TestWorkbook>, occupy_column_a: bool) {
    set_number(engine, 1, 8, 2000.0);
    set_number(engine, 3, 8, 2005.0);
    if occupy_column_a {
        set_text(engine, 2, 1, "tag");
    }
}

fn d_to_f_formulas(consumer_row: u32, consumer: &str) -> Vec<(u32, u32, ASTNode)> {
    let mut formulas = vec![(3, 4, formula("=$H$1"))];
    for row in 4..=13 {
        formulas.push((row, 4, formula(&format!("=D{}+1", row - 1))));
    }
    for row in 3..=13 {
        formulas.push((row, 6, formula(&format!("=E{row}*10"))));
    }
    formulas.push((consumer_row, 14, formula(consumer)));
    formulas
}

fn build_d_to_f_bulk(
    occupy_column_a: bool,
    consumer_row: u32,
    consumer: &str,
) -> Engine<TestWorkbook> {
    let mut engine = runtime_engine();
    seed_constants(&mut engine, occupy_column_a);
    for row in 3..=13 {
        set_number(&mut engine, row, 5, f64::from(row - 2) * 100.0);
    }

    let mut ingest = engine.begin_bulk_ingest();
    let sheet = ingest.add_sheet("Sheet1");
    ingest.add_formulas(sheet, d_to_f_formulas(consumer_row, consumer));
    ingest.finish().expect("finish bulk ingest");
    engine
}

fn assert_plan_has_precedents(engine: &Engine<TestWorkbook>, row: u32) {
    let plan = engine
        .get_eval_plan(&[("Sheet1", row, 14)])
        .expect("get evaluation plan");
    assert!(
        plan.total_vertices_to_evaluate > 1,
        "expected precedent chain in plan, got {plan:?}"
    );
}

fn evaluate_and_expect(engine: &mut Engine<TestWorkbook>, row: u32, expected: f64) {
    engine.evaluate_all().expect("evaluate all");
    assert_eq!(number_at(engine, row, 14), expected);
}

#[test]
fn open_rect_multi_column_a2_pinned_vlookup() {
    let mut engine = build_d_to_f_bulk(true, 3, "=VLOOKUP(H3,$D:$F,3,FALSE)");
    assert_plan_has_precedents(&engine, 3);
    evaluate_and_expect(&mut engine, 3, 6000.0);
}

#[test]
fn open_rect_multi_column_a2_pinned_countif() {
    let mut engine = build_d_to_f_bulk(true, 5, "=COUNTIF($D:$F,2005)");
    assert_plan_has_precedents(&engine, 5);
    evaluate_and_expect(&mut engine, 5, 1.0);
}

#[test]
fn open_rect_column_a_key_no_panic() {
    let mut engine = runtime_engine();
    seed_constants(&mut engine, true);
    for row in 3..=13 {
        set_number(&mut engine, row, 2, f64::from(row - 2) * 100.0);
    }

    let mut formulas = vec![(3, 1, formula("=$H$1"))];
    for row in 4..=13 {
        formulas.push((row, 1, formula(&format!("=A{}+1", row - 1))));
    }
    for row in 3..=13 {
        formulas.push((row, 3, formula(&format!("=B{row}*10"))));
    }
    formulas.push((3, 14, formula("=VLOOKUP(H3,$A:$C,3,FALSE)")));

    let mut ingest = engine.begin_bulk_ingest();
    let sheet = ingest.add_sheet("Sheet1");
    ingest.add_formulas(sheet, formulas);
    ingest.finish().expect("finish bulk ingest");

    assert_plan_has_precedents(&engine, 3);
    evaluate_and_expect(&mut engine, 3, 6000.0);
}

#[test]
fn open_rect_single_column_whole_col_unchanged() {
    let mut engine = build_d_to_f_bulk(true, 3, "=VLOOKUP(H3,$D:$D,1,FALSE)");
    assert_plan_has_precedents(&engine, 3);
    evaluate_and_expect(&mut engine, 3, 2005.0);
}

#[test]
fn open_rect_bounded_range_unchanged() {
    let mut engine = build_d_to_f_bulk(true, 3, "=VLOOKUP(H3,$D$1:$F$50,3,FALSE)");
    assert_plan_has_precedents(&engine, 3);
    evaluate_and_expect(&mut engine, 3, 6000.0);
}

#[test]
fn open_rect_column_a_empty_control() {
    let mut engine = build_d_to_f_bulk(false, 3, "=VLOOKUP(H3,$D:$F,3,FALSE)");
    assert_plan_has_precedents(&engine, 3);
    evaluate_and_expect(&mut engine, 3, 6000.0);
}

#[test]
fn open_rect_incremental_edit_path_unchanged() {
    let mut engine = runtime_engine();
    seed_constants(&mut engine, true);
    for row in 3..=13 {
        set_number(&mut engine, row, 5, f64::from(row - 2) * 100.0);
    }
    for (row, col, ast) in d_to_f_formulas(3, "=VLOOKUP(H3,$D:$F,3,FALSE)") {
        engine
            .set_cell_formula("Sheet1", row, col, ast)
            .expect("set formula");
    }

    assert_plan_has_precedents(&engine, 3);
    evaluate_and_expect(&mut engine, 3, 6000.0);
}

#[test]
fn open_rect_row_mirror_sum_pinned() {
    // Axis mirror of the multi-column shapes above: an open row band with
    // formula-valued precedents through the bulk-ingest path. Before the
    // open-rect bounds fix the collapsed row bounds lost scheduling edges and
    // the consumer committed a stale 76605 instead of 96660.
    let mut engine = build_d_to_f_bulk(true, 20, "=SUM($3:$13)");
    assert_plan_has_precedents(&engine, 20);
    evaluate_and_expect(&mut engine, 20, 96660.0);
}

#[test]
fn open_rect_stripe_set_preserves_start_col() {
    // Precision pin for the stripe index: an open range that does not start at
    // column A must register exactly its own column stripes. Dropping
    // `start_col` on either the key-producer or the stripe-consumer side
    // degrades into a "safe" over-widening to column A that value-level tests
    // cannot observe.
    use crate::engine::graph::StripeType;

    let engine = build_d_to_f_bulk(true, 3, "=VLOOKUP(H3,$D:$F,3,FALSE)");
    let consumer = *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 3, 14))
        .expect("consumer vertex");
    let sheet_id = engine.graph.sheet_id("Sheet1").expect("sheet id");

    let mut column_stripes: Vec<u32> = Vec::new();
    let mut other_stripes: Vec<(StripeType, u32)> = Vec::new();
    for (key, dependents) in engine.graph.stripe_to_dependents() {
        if key.sheet_id != sheet_id || !dependents.contains(&consumer) {
            continue;
        }
        match key.stripe_type {
            StripeType::Column => column_stripes.push(key.index),
            ref other => other_stripes.push((other.clone(), key.index)),
        }
    }
    column_stripes.sort_unstable();

    assert_eq!(
        column_stripes,
        vec![3, 4, 5],
        "=VLOOKUP over $D:$F must register exactly the D..F column stripes (0-based 3..5)"
    );
    assert!(
        other_stripes.is_empty(),
        "no row or block stripes expected for an open multi-column range, got {other_stripes:?}"
    );
}
