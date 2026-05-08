use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const FORMULA_ROWS: u32 = 200;
const TABLE_ROWS: u32 = 1_000;

fn engine_with_mode(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    )
}

fn authoritative_engine() -> Engine<TestWorkbook> {
    engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental)
}

fn off_engine() -> Engine<TestWorkbook> {
    engine_with_mode(FormulaPlaneMode::Off)
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn ingest(engine: &mut Engine<TestWorkbook>, formulas: Vec<FormulaIngestRecord>) {
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("ingest formulas");
}

fn populate_position_column(engine: &mut Engine<TestWorkbook>, rows: u32) {
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
}

fn populate_lookup_table(engine: &mut Engine<TestWorkbook>) {
    for row in 1..=TABLE_ROWS {
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Number(table_value(row)))
            .unwrap();
    }
}

fn populate_key_table(engine: &mut Engine<TestWorkbook>) {
    for row in 1..=TABLE_ROWS {
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Number(row as f64))
            .unwrap();
    }
}

fn index_family(formula: impl Fn(u32) -> String) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    populate_position_column(&mut engine, FORMULA_ROWS);
    populate_lookup_table(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(&mut engine, row, 2, &formula(row)));
    }
    ingest(&mut engine, formulas);
    engine
}

fn assert_span_count(engine: &Engine<TestWorkbook>, expected: usize) {
    assert_eq!(
        engine.baseline_stats().formula_plane_active_span_count,
        expected,
        "ingest report: {:?}",
        engine.last_formula_ingest_report()
    );
}

fn assert_number(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    assert_eq!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Number(expected))
    );
}

fn table_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}

fn nested_if_expected(position: u32) -> f64 {
    if position > 0 {
        if position < 150 {
            if position.is_multiple_of(2) {
                if position > 50 {
                    table_value(position)
                } else {
                    -1.0
                }
            } else {
                -2.0
            }
        } else {
            -3.0
        }
    } else {
        -4.0
    }
}

#[test]
fn index_with_constant_table_varying_position_promotes() {
    let mut engine = index_family(|row| format!("=INDEX($D$1:$D$1000, A{row})"));

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, table_value(row));
    }
}

#[test]
fn index_inside_arithmetic_promotes() {
    let mut engine = index_family(|row| format!("=A{row} + INDEX($D$1:$D$1000, A{row})"));

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, row as f64 + table_value(row));
    }
}

#[test]
fn index_inside_if_promotes_at_depth_5() {
    let mut engine = index_family(|row| {
        format!(
            "=IF(A{row}>0, IF(A{row}<150, IF(MOD(A{row},2)=0, IF(A{row}>50, INDEX($D$1:$D$1000, A{row}), -1), -2), -3), -4)"
        )
    });

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [10, 51, 149, 151] {
        assert_number(&engine, row, 2, nested_if_expected(row));
    }
}

#[test]
fn index_match_classic_pattern_promotes() {
    let mut engine = authoritative_engine();
    populate_position_column(&mut engine, FORMULA_ROWS);
    populate_lookup_table(&mut engine);
    populate_key_table(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=INDEX($D$1:$D$1000, MATCH(A{row}, $E$1:$E$1000, 0))"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 0);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, table_value(row));
    }
}

#[test]
fn index_dependency_on_table_correctly_marks_dirty() {
    let mut engine = index_family(|row| format!("=INDEX($D$1:$D$1000, A{row})"));

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();
    assert_number(&engine, 100, 2, table_value(100));

    engine
        .set_cell_value("Sheet1", 50, 4, LiteralValue::Number(9_999.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number(&engine, 50, 2, 9_999.0);
    assert_number(&engine, 100, 2, table_value(100));
}

#[test]
fn index_in_range_constructor_remains_rejected() {
    let mut engine = authoritative_engine();
    for row in 1..=10 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    let formula = record(
        &mut engine,
        1,
        2,
        "=SUM(INDEX($A$1:$A$10, 1):INDEX($A$1:$A$10, 5))",
    );
    ingest(&mut engine, vec![formula]);

    assert_span_count(&engine, 0);
    engine.evaluate_all().unwrap();
    assert_number(&engine, 1, 2, 15.0);
}

#[test]
fn offset_indirect_remain_rejected() {
    let mut offset_engine = authoritative_engine();
    populate_position_column(&mut offset_engine, FORMULA_ROWS);
    populate_lookup_table(&mut offset_engine);
    let mut offset_formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        offset_formulas.push(record(
            &mut offset_engine,
            row,
            2,
            &format!("=OFFSET($D$1, A{row}-1, 0)"),
        ));
    }
    ingest(&mut offset_engine, offset_formulas);
    assert_span_count(&offset_engine, 0);
    offset_engine.evaluate_all().unwrap();
    assert_number(&offset_engine, 50, 2, table_value(50));

    let mut indirect_engine = authoritative_engine();
    populate_position_column(&mut indirect_engine, FORMULA_ROWS);
    populate_lookup_table(&mut indirect_engine);
    let mut indirect_formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        indirect_formulas.push(record(
            &mut indirect_engine,
            row,
            2,
            &format!("=INDIRECT(\"D\"&A{row})"),
        ));
    }
    ingest(&mut indirect_engine, indirect_formulas);
    assert_span_count(&indirect_engine, 0);
    indirect_engine.evaluate_all().unwrap();
    assert_number(&indirect_engine, 50, 2, table_value(50));
}

#[test]
fn row_column_with_relative_ref_remain_rejected_or_correctly_handle_byref() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = engine_with_mode(mode);
        populate_position_column(&mut engine, FORMULA_ROWS);
        let mut formulas = Vec::new();
        for row in 1..=FORMULA_ROWS {
            formulas.push(record(
                &mut engine,
                row,
                2,
                &format!("=ROW(A{row}) + COLUMN(A{row})"),
            ));
        }
        ingest(&mut engine, formulas);
        engine.evaluate_all().unwrap();

        for row in [1, 50, 100, 200] {
            assert_number(&engine, row, 2, row as f64 + 1.0);
        }
    }
}

#[test]
fn index_duplicate_position_args_memoize() {
    let mut engine = authoritative_engine();
    populate_lookup_table(&mut engine);
    let mut formulas = Vec::new();
    for row in 1..=120 {
        let position = (row - 1) % 3 + 1;
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(position as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=INDEX($D$1:$D$1000, A{row})"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert!(report.memo_eval_count > 0);
    assert!(report.memo_broadcast_count > report.memo_eval_count);
    assert_number(&engine, 1, 2, table_value(1));
    assert_number(&engine, 2, 2, table_value(2));
    assert_number(&engine, 3, 2, table_value(3));
    assert_number(&engine, 4, 2, table_value(1));
}

#[test]
fn index_constant_position_broadcasts() {
    let mut engine = authoritative_engine();
    populate_lookup_table(&mut engine);
    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(&mut engine, row, 2, "=INDEX($D$1:$D$1000, 5)"));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.span_eval_placement_count, FORMULA_ROWS as u64);
    assert_eq!(report.transient_ast_relocation_count, 1);
    assert_number(&engine, 1, 2, table_value(5));
    assert_number(&engine, 200, 2, table_value(5));
}

#[test]
fn index_inside_arithmetic_evaluates_in_off_mode() {
    let mut engine = off_engine();
    populate_position_column(&mut engine, 3);
    populate_lookup_table(&mut engine);
    let formula = record(&mut engine, 1, 2, "=A1 + INDEX($D$1:$D$1000, A1)");
    ingest(&mut engine, vec![formula]);

    assert_span_count(&engine, 0);
    engine.evaluate_all().unwrap();
    assert_number(&engine, 1, 2, 1.0 + table_value(1));
}
