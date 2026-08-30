use std::sync::Arc;

use chrono::NaiveDate;
use formualizer_common::{ExcelErrorExtra, LiteralValue, ResourceExhaustionReason};
use formualizer_parse::parser::parse;

use crate::engine::{
    CancelToken, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
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

fn arrow_engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut config = crate::engine::tests::common::arrow_eval_config();
    config.formula_plane_mode = mode;
    Engine::new(TestWorkbook::default(), config)
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

fn arrow_source_lane_fixture(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = arrow_engine(mode);
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet(SHEET, 2, 4096);
        ingest
            .append_row(SHEET, &[LiteralValue::Date(date), LiteralValue::Date(date)])
            .unwrap();
        for _ in 0..ROWS {
            ingest
                .append_row(SHEET, &[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        ingest.finish().unwrap();
    }
    let formulas = (2..=ROWS + 1)
        .map(|row| record(&mut engine, row, 2, "=$A$1+0"))
        .collect();
    ingest(&mut engine, formulas);
    engine
}

fn newly_active_span_with_real_legacy_date() -> Engine<TestWorkbook> {
    let mut engine = engine(FormulaPlaneMode::Off);
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    engine
        .set_cell_value(SHEET, 10, 6, LiteralValue::Date(date))
        .unwrap();
    let formulas = (1..=ROWS)
        .map(|row| record(&mut engine, row, 7, "=$F$10+0"))
        .collect();
    ingest(&mut engine, formulas);
    assert_eq!(
        engine.debug_derived_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );

    engine.config.formula_plane_mode = FormulaPlaneMode::AuthoritativeExperimental;
    let formulas = (1..=ROWS)
        .map(|row| record(&mut engine, row, 7, "=$F$10+0"))
        .collect();
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(
        engine.debug_derived_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.get_cell_value(SHEET, 1, 7),
        Some(LiteralValue::Date(date))
    );
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
fn formula_plane_source_general_run_falls_through_to_computed_format_parity() {
    let off = arrow_source_lane_fixture(FormulaPlaneMode::Off);
    let authoritative = arrow_source_lane_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();

    assert_eq!(off.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count,
        1
    );
    assert_eq!(
        authoritative.debug_computed_overlay_format_0based(SHEET, 1, 1),
        Some(FormatId::DATE)
    );
    for row in 2..=ROWS + 1 {
        assert_eq!(
            authoritative.effective_format_id(SHEET, row, 2),
            off.effective_format_id(SHEET, row, 2),
            "effective format at row {row}"
        );
        assert_eq!(
            authoritative.get_cell_value(SHEET, row, 2),
            off.get_cell_value(SHEET, row, 2),
            "temporal egress at row {row}"
        );
    }
    assert_eq!(
        authoritative.get_cell_value(SHEET, 2, 2),
        Some(LiteralValue::Date(date))
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

#[test]
fn formula_plane_admission_invariant_purges_stale_legacy_side_band() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine
        .set_cell_value(SHEET, 10, 6, LiteralValue::Number(45_627.0))
        .unwrap();
    engine.debug_record_derived_format_0based(SHEET, 0, 6, Some(FormatId::DATE));

    let formulas = (1..=ROWS)
        .map(|row| record(&mut engine, row, 7, "=$F$10+0"))
        .collect();
    ingest(&mut engine, formulas);

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.debug_derived_format_0based(SHEET, 0, 6), None);
    assert_eq!(
        engine.get_cell_value(SHEET, 1, 7),
        Some(LiteralValue::Number(45_627.0))
    );
}

#[test]
fn formula_plane_real_legacy_date_to_general_transition_matches_authoritative_admission() {
    let mut engine = engine(FormulaPlaneMode::Off);
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
    assert_eq!(
        engine.debug_derived_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );

    engine.config.formula_plane_mode = FormulaPlaneMode::AuthoritativeExperimental;
    engine
        .set_cell_value(SHEET, 10, 6, LiteralValue::Number(45_627.0))
        .unwrap();
    let formulas = (1..=ROWS)
        .map(|row| record(&mut engine, row, 7, "=$F$10+0"))
        .collect();
    ingest(&mut engine, formulas);

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.debug_derived_format_0based(SHEET, 0, 6), None);
    assert_eq!(
        engine.get_cell_value(SHEET, 1, 7),
        Some(LiteralValue::Number(45_627.0))
    );
}

#[test]
fn formula_plane_date_to_general_recomputation_clears_computed_format_lane() {
    let mut engine = constant_result_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );

    engine
        .set_cell_value(SHEET, 10, 6, LiteralValue::Number(7.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_computed_overlay_formats(&engine, 7, |_| None);
    assert!(!engine.debug_computed_overlay_chunk_has_formats_0based(SHEET, 0, 6));
    assert_eq!(
        engine.get_cell_value(SHEET, 1, 7),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn formula_plane_sparse_general_recomputation_clears_only_written_offsets() {
    let mut engine = memoized_fixture(FormulaPlaneMode::AuthoritativeExperimental, false);
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 1),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 1, 1),
        Some(FormatId::DATE)
    );

    engine
        .set_cell_value(SHEET, 2, 1, LiteralValue::Number(100.0))
        .unwrap();
    engine
        .set_cell_value(SHEET, 4, 1, LiteralValue::Number(200.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 1),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 1, 1),
        None
    );
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 2, 1),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 3, 1),
        None
    );
}

#[test]
fn formula_plane_point_general_recomputation_clears_one_format_offset() {
    let mut engine = memoized_fixture(FormulaPlaneMode::AuthoritativeExperimental, false);
    engine.debug_reset_format_write_operation_counts();
    engine
        .set_cell_value(SHEET, 2, 1, LiteralValue::Number(100.0))
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 1),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 1, 1),
        None
    );
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 2, 1),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.debug_format_write_operation_counts(),
        (0, 0, 0, 0, 1)
    );
}

#[test]
fn formula_plane_mixed_actual_and_none_chunks_choose_independent_format_effects() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    engine
        .set_cell_value(SHEET, 1, 1, LiteralValue::Date(date))
        .unwrap();
    engine
        .set_cell_value(SHEET, 1, 4, LiteralValue::Date(date))
        .unwrap();
    let mut formulas = Vec::with_capacity((ROWS * 2) as usize);
    for row in 1..=ROWS {
        formulas.push(record(&mut engine, row, 2, "=$A$1+0"));
        formulas.push(record(&mut engine, row, 5, "=$D$1+0"));
    }
    ingest(&mut engine, formulas);
    engine.debug_reset_format_write_operation_counts();

    engine
        .set_cell_value(
            SHEET,
            1,
            1,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 2).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(SHEET, 1, 4, LiteralValue::Number(7.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_computed_overlay_formats(&engine, 2, |_| Some(FormatId::DATE));
    assert_computed_overlay_formats(&engine, 5, |_| None);
    assert_eq!(
        engine.debug_format_write_operation_counts(),
        (0, ROWS as u64, 1, 1, 0)
    );
}

#[test]
fn formula_plane_mid_span_cancellation_preserves_stale_side_band_and_overlay() {
    let mut engine = newly_active_span_with_real_legacy_date();
    let before_value = engine.get_cell_value(SHEET, 1, 7);
    let before_overlay_format = engine.debug_computed_overlay_format_0based(SHEET, 0, 6);
    engine.cancel_before_formula_plane_layer_commit_once_for_test();

    let error = engine
        .evaluate_all_cancellable(CancelToken::new())
        .unwrap_err();

    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Cancelled);
    assert!(
        engine
            .last_formula_plane_span_eval_report()
            .is_some_and(|report| report.span_eval_placement_count == ROWS as u64),
        "span evaluation must complete before the deterministic cancellation hook"
    );
    assert_eq!(
        engine.debug_derived_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );
    assert_eq!(engine.get_cell_value(SHEET, 1, 7), before_value);
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 6),
        before_overlay_format
    );
}

#[test]
fn formula_plane_commit_preflight_failure_preserves_stale_side_band_and_egress() {
    let mut engine = newly_active_span_with_real_legacy_date();
    let before_value = engine.get_cell_value(SHEET, 1, 7);
    let before_overlay_format = engine.debug_computed_overlay_format_0based(SHEET, 0, 6);
    engine.fail_evaluation_commit_preflight_once_for_test();

    let error = engine.evaluate_all().unwrap_err();
    let ExcelErrorExtra::Resource { detail } = &error.extra else {
        panic!("expected typed resource failure, got {error:?}");
    };

    assert_eq!(detail.reason, ResourceExhaustionReason::Deadline);
    assert_eq!(
        engine.debug_derived_format_0based(SHEET, 0, 6),
        Some(FormatId::DATE)
    );
    assert_eq!(engine.get_cell_value(SHEET, 1, 7), before_value);
    assert_eq!(
        engine.debug_computed_overlay_format_0based(SHEET, 0, 6),
        before_overlay_format
    );

    engine.evaluate_all().unwrap();
    assert_eq!(engine.debug_derived_format_0based(SHEET, 0, 6), None);
    assert_eq!(engine.get_cell_value(SHEET, 1, 7), before_value);
}

#[test]
fn formula_plane_general_100k_span_has_zero_per_cell_format_operations() {
    const FAST_ROWS: u32 = 100_000;
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    engine
        .set_cell_value(SHEET, 1, 1, LiteralValue::Number(3.0))
        .unwrap();
    let ast = parse("=$A$1+0").unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    let formulas = (1..=FAST_ROWS)
        .map(|row| FormulaIngestRecord::new(row, 2, ast_id, Some(Arc::<str>::from("=$A$1+0"))))
        .collect();

    engine.debug_reset_format_write_operation_counts();
    ingest(&mut engine, formulas);

    assert_eq!(
        engine.debug_format_write_operation_counts(),
        (0, 0, 0, 0, 0)
    );
    assert!(!engine.debug_computed_overlay_chunk_has_formats_0based(SHEET, 0, 1));
    assert!(!engine.debug_computed_overlay_chunk_has_formats_0based(SHEET, FAST_ROWS - 1, 1));
}
