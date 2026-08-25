use std::sync::Arc;

use chrono::NaiveDate;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::format::FormatId;
use crate::test_workbook::TestWorkbook;

const SHEET: &str = "Sheet1";
const ROWS: u32 = 200;

fn engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    )
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
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");
    engine.evaluate_all().expect("evaluate formulas");
}

fn results(engine: &Engine<TestWorkbook>, col: u32) -> Vec<LiteralValue> {
    (1..=ROWS)
        .map(|row| {
            engine
                .get_cell_value(SHEET, row, col)
                .unwrap_or_else(|| panic!("missing {SHEET}!R{row}C{col}"))
        })
        .collect()
}

fn assert_computed_overlay_formats(
    engine: &Engine<TestWorkbook>,
    col: u32,
    expected: impl Fn(u32) -> Option<FormatId>,
) {
    for row in 1..=ROWS {
        assert_eq!(
            engine.debug_computed_overlay_format_0based(SHEET, row - 1, col - 1),
            expected(row),
            "overlay format at {SHEET}!R{row}C{col}"
        );
    }
}

fn constant_result_fixture(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = engine(mode);
    engine
        .set_cell_value(
            SHEET,
            10,
            6,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()),
        )
        .unwrap();
    let formulas = (1..=ROWS)
        .map(|row| record(&mut engine, row, 7, "=$F$10+0"))
        .collect();
    ingest(&mut engine, formulas);
    engine
}

fn memoized_fixture(mode: FormulaPlaneMode, mixed_formats: bool) -> Engine<TestWorkbook> {
    let mut engine = engine(mode);
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    let mut formulas = Vec::with_capacity(ROWS as usize);
    for row in 1..=ROWS {
        let value = if mixed_formats && row % 2 == 0 {
            LiteralValue::Number(45_627.0)
        } else {
            LiteralValue::Date(date)
        };
        engine.set_cell_value(SHEET, row, 1, value).unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=A{row}+{}", if mixed_formats { 0 } else { 1 }),
        ));
    }
    ingest(&mut engine, formulas);
    engine
}

#[test]
fn formula_plane_constant_result_broadcast_preserves_date_format_parity() {
    let off = constant_result_fixture(FormulaPlaneMode::Off);
    let authoritative = constant_result_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    let expected = LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap());

    let off_results = results(&off, 7);
    let authoritative_results = results(&authoritative, 7);
    assert_eq!(authoritative_results, off_results);
    assert!(off_results.iter().all(|value| value == &expected));
    assert_computed_overlay_formats(&authoritative, 7, |_| Some(FormatId::DATE));
    assert_eq!(
        authoritative
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        ROWS as u64
    );
}

#[test]
fn formula_plane_memo_broadcast_preserves_equal_date_format_parity() {
    let off = memoized_fixture(FormulaPlaneMode::Off, false);
    let authoritative = memoized_fixture(FormulaPlaneMode::AuthoritativeExperimental, false);
    let expected = LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 2).unwrap());

    let off_results = results(&off, 2);
    let authoritative_results = results(&authoritative, 2);
    assert_eq!(authoritative_results, off_results);
    assert!(off_results.iter().all(|value| value == &expected));
    assert_computed_overlay_formats(&authoritative, 2, |_| Some(FormatId::DATE));
    let report = authoritative.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 1, "{report:?}");
    assert_eq!(report.memo_broadcast_count, (ROWS - 1) as u64, "{report:?}");
}

#[test]
fn formula_plane_memo_broadcast_preserves_mixed_format_parity() {
    let off = memoized_fixture(FormulaPlaneMode::Off, true);
    let authoritative = memoized_fixture(FormulaPlaneMode::AuthoritativeExperimental, true);

    let off_results = results(&off, 2);
    let authoritative_results = results(&authoritative, 2);
    assert_eq!(authoritative_results, off_results);
    for (index, value) in off_results.iter().enumerate() {
        let expected = if index % 2 == 0 {
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap())
        } else {
            LiteralValue::Number(45_627.0)
        };
        assert_eq!(*value, expected, "row {}", index + 1);
    }
    assert_computed_overlay_formats(&authoritative, 2, |row| {
        (row % 2 == 1).then_some(FormatId::DATE)
    });
    let report = authoritative.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 2, "{report:?}");
    assert_eq!(report.memo_broadcast_count, (ROWS - 2) as u64, "{report:?}");
}

#[test]
fn formula_plane_date_output_resolves_from_overlay_lane_without_side_band() {
    let mut engine = constant_result_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    assert!(engine.debug_computed_overlay_chunk_has_formats_0based(SHEET, 0, 6));
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );

    engine.debug_clear_derived_format_0based(SHEET, 0, 6);
    assert_eq!(
        engine.get_cell_value(SHEET, 1, 7),
        Some(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        ))
    );
}
