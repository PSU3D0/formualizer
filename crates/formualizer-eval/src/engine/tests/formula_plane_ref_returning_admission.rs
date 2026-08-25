use std::sync::Arc;

use chrono::NaiveDate;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{
    CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig, FormulaIngestBatch,
    FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const ROWS: u32 = 120;

fn engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    )
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    engine.add_sheet(sheet).ok();
    let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn ingest(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    records: Vec<FormulaIngestRecord>,
) -> crate::engine::FormulaIngestReport {
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, records)])
        .expect("ingest")
}

fn assert_cells_equal(
    left: &Engine<TestWorkbook>,
    right: &Engine<TestWorkbook>,
    sheet: &str,
    cells: impl IntoIterator<Item = (u32, u32)>,
) {
    for (row, col) in cells {
        assert_eq!(
            left.get_cell_value(sheet, row, col),
            right.get_cell_value(sheet, row, col),
            "{sheet}!R{row}C{col}"
        );
    }
}

fn fill_down_fixture(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = engine(mode);
    engine.add_sheet("Aux").unwrap();
    let mut formulas = Vec::new();
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Boolean(row % 2 == 0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(1_000.0 + row as f64))
            .unwrap();
        engine
            .set_cell_value("Aux", row, 4, LiteralValue::Number(10_000.0 + row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            5,
            &format!("=IF(A{row},B{row},C{row})"),
        ));
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            6,
            &format!("=IF($A{row},Aux!D{row},C$1)"),
        ));
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            7,
            &format!("=IFS(A{row},B{row},TRUE,C{row})"),
        ));
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            8,
            &format!("=CHOOSE(IF(A{row},1,2),B{row},Aux!D{row})"),
        ));
    }
    let report = ingest(&mut engine, "Sheet1", formulas);
    if mode == FormulaPlaneMode::AuthoritativeExperimental {
        assert_eq!(
            report.shadow_accepted_span_cells,
            u64::from(ROWS) * 4,
            "{report:?}"
        );
        assert_eq!(report.shadow_fallback_cells, 0, "{report:?}");
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn reference_returning_fill_down_parity_covers_anchors_and_cross_sheet_arms() {
    let off = fill_down_fixture(FormulaPlaneMode::Off);
    let authoritative = fill_down_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    assert_cells_equal(
        &off,
        &authoritative,
        "Sheet1",
        (1..=ROWS).flat_map(|row| (5..=8).map(move |col| (row, col))),
    );
    assert_eq!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count,
        4
    );
}

fn semantic_fixture(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = engine(mode);
    let values = [
        (
            1,
            1,
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
        ),
        (1, 2, LiteralValue::Number(7.0)),
        (
            1,
            3,
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)),
        ),
        (2, 1, LiteralValue::Boolean(true)),
        (2, 2, LiteralValue::Number(11.0)),
        (
            2,
            3,
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)),
        ),
        (3, 1, LiteralValue::Text("not logical".into())),
        (1, 4, LiteralValue::Number(1.0)),
        (2, 4, LiteralValue::Number(2.0)),
        (3, 4, LiteralValue::Number(3.0)),
    ];
    for (row, col, value) in values {
        engine.set_cell_value("Sheet1", row, col, value).unwrap();
    }
    let cases = [
        "=IF($A$1,$B$1,$C$1)",
        "=IF($A$2,$B$2,$C$2)",
        "=IF(FALSE,$B$2)",
        "=IF(TRUE,,1)",
        "=IF($A$3,$B$2,$C$2)",
        "=IF($D$1:$D$3>0,$B$2,$C$2)",
        "=IF(TRUE,IF(FALSE,$B$1,$B$2),$C$1)",
        "=CHOOSE(4,$B$1,$B$2)",
        "=CHOOSE(1.5,$B$1,$B$2)",
        "=CHOOSE($A$1,$B$1,$B$2)",
        "=IFS(FALSE,$B$1,FALSE,$B$2)",
    ];
    let mut formulas = Vec::new();
    for (case_index, formula) in cases.iter().enumerate() {
        let col = 10 + case_index as u32;
        for row in 10..10 + ROWS {
            formulas.push(record(&mut engine, "Sheet1", row, col, formula));
        }
    }
    let report = ingest(&mut engine, "Sheet1", formulas);
    if mode == FormulaPlaneMode::AuthoritativeExperimental {
        assert_eq!(
            report.shadow_accepted_span_cells,
            cases.len() as u64 * u64::from(ROWS),
            "{report:?}"
        );
        assert_eq!(report.shadow_fallback_cells, 0, "{report:?}");
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn reference_returning_error_empty_and_short_circuit_semantics_match_legacy() {
    let off = semantic_fixture(FormulaPlaneMode::Off);
    let authoritative = semantic_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    assert_cells_equal(
        &off,
        &authoritative,
        "Sheet1",
        (0..11).flat_map(|case| (10..10 + ROWS).map(move |row| (row, 10 + case))),
    );
    assert_eq!(
        authoritative.get_cell_value("Sheet1", 10, 10),
        Some(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)))
    );
    assert_eq!(
        authoritative.get_cell_value("Sheet1", 10, 11),
        Some(LiteralValue::Number(11.0)),
        "untaken error arm must not propagate"
    );
    assert_eq!(
        authoritative.get_cell_value("Sheet1", 10, 12),
        Some(LiteralValue::Boolean(false))
    );
    assert_eq!(
        authoritative.get_cell_value("Sheet1", 10, 13),
        Some(LiteralValue::Number(0.0))
    );
    assert_eq!(
        authoritative.get_cell_value("Sheet1", 10, 18),
        Some(LiteralValue::Number(7.0)),
        "CHOOSE truncates a non-integer numeric selector"
    );
    for (col, kind) in [
        (14, ExcelErrorKind::Value),
        (15, ExcelErrorKind::Value),
        (17, ExcelErrorKind::Value),
        (19, ExcelErrorKind::Div),
        (20, ExcelErrorKind::Na),
    ] {
        assert!(
            matches!(
                authoritative.get_cell_value("Sheet1", 10, col),
                Some(LiteralValue::Error(error)) if error.kind == kind
            ),
            "unexpected error at column {col}"
        );
    }
}

#[test]
fn iferror_wrapped_if_remains_legacy_but_value_identical() {
    let formula = "=IFERROR(IF(TRUE,$A$1,$B$1),99)";
    let mut off = engine(FormulaPlaneMode::Off);
    let mut authoritative = engine(FormulaPlaneMode::AuthoritativeExperimental);
    for target in [&mut off, &mut authoritative] {
        target
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(42.0))
            .unwrap();
        let records = (10..10 + ROWS)
            .map(|row| record(target, "Sheet1", row, 4, formula))
            .collect();
        let report = ingest(target, "Sheet1", records);
        if target.config.formula_plane_mode == FormulaPlaneMode::AuthoritativeExperimental {
            assert_eq!(report.shadow_accepted_span_cells, 0);
            assert_eq!(report.shadow_fallback_cells, u64::from(ROWS));
        }
        target.evaluate_all().unwrap();
    }
    assert_cells_equal(
        &off,
        &authoritative,
        "Sheet1",
        (10..10 + ROWS).map(|row| (row, 4)),
    );
}

fn spill_firewall_fixture(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = engine(mode);
    for row in 1..=3 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64 * 10.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64 * 100.0))
            .unwrap();
    }
    let formulas = [
        (4, "=IF(TRUE,A1:A3,0)"),
        (5, "=CHOOSE(2,A1,B1:B3)"),
        (6, "=SQRT(IF(TRUE,A1:A3,B1:B3))"),
        (7, "=SUM(IF(FALSE,A1:A3,B1:B3))"),
    ];
    let records = formulas
        .iter()
        .map(|(col, formula)| record(&mut engine, "Sheet1", 1, *col, formula))
        .collect();
    let report = ingest(&mut engine, "Sheet1", records);
    if mode == FormulaPlaneMode::AuthoritativeExperimental {
        assert_eq!(report.shadow_accepted_span_cells, 0, "{report:?}");
        assert_eq!(report.shadow_fallback_cells, 4, "{report:?}");
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn range_arm_firewall_preserves_spills_and_scalar_consumption() {
    let off = spill_firewall_fixture(FormulaPlaneMode::Off);
    let authoritative = spill_firewall_fixture(FormulaPlaneMode::AuthoritativeExperimental);
    assert_cells_equal(
        &off,
        &authoritative,
        "Sheet1",
        (4..=7).flat_map(|col| (1..=3).map(move |row| (row, col))),
    );
    assert_eq!(
        authoritative.get_cell_value("Sheet1", 1, 7),
        Some(LiteralValue::Number(600.0))
    );
    assert_eq!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count,
        0
    );
}

#[test]
fn union_dependencies_dirty_taken_and_untaken_arms() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    let mut formulas = Vec::new();
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Boolean(true))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(1_000.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            4,
            &format!("=IF(A{row},B{row},C{row})"),
        ));
    }
    ingest(&mut engine, "Sheet1", formulas);
    engine.evaluate_all().unwrap();

    engine
        .set_cell_value("Sheet1", 10, 2, LiteralValue::Number(77.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 4),
        Some(LiteralValue::Number(77.0))
    );
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        1
    );

    engine
        .set_cell_value("Sheet1", 10, 3, LiteralValue::Number(88.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 4),
        Some(LiteralValue::Number(77.0))
    );
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        1
    );

    engine
        .set_cell_value("Sheet1", 10, 1, LiteralValue::Boolean(false))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 4),
        Some(LiteralValue::Number(88.0))
    );
}

#[test]
fn memo_groups_equal_branch_triples() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    let mut formulas = Vec::new();
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Boolean(true))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(9.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            4,
            &format!("=IF(A{row},B{row},C{row})"),
        ));
    }
    ingest(&mut engine, "Sheet1", formulas);
    engine.evaluate_all().unwrap();
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 1, "{report:?}");
    assert_eq!(
        report.memo_broadcast_count,
        u64::from(ROWS - 1),
        "{report:?}"
    );
}

#[test]
fn memo_preserves_equal_values_with_different_selected_formats() {
    let mut off = engine(FormulaPlaneMode::Off);
    let mut authoritative = engine(FormulaPlaneMode::AuthoritativeExperimental);
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    for target in [&mut off, &mut authoritative] {
        let mut formulas = Vec::new();
        for row in 1..=ROWS {
            target
                .set_cell_value("Sheet1", row, 1, LiteralValue::Boolean(true))
                .unwrap();
            let selected = if row % 2 == 0 {
                LiteralValue::Number(45_627.0)
            } else {
                LiteralValue::Date(date)
            };
            target.set_cell_value("Sheet1", row, 2, selected).unwrap();
            target
                .set_cell_value("Sheet1", row, 3, LiteralValue::Number(5.0))
                .unwrap();
            formulas.push(record(
                target,
                "Sheet1",
                row,
                4,
                &format!("=IF(A{row},B{row},C{row})"),
            ));
        }
        ingest(target, "Sheet1", formulas);
        target.evaluate_all().unwrap();
    }
    assert_cells_equal(
        &off,
        &authoritative,
        "Sheet1",
        (1..=ROWS).map(|row| (row, 4)),
    );
    let report = authoritative.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 2, "{report:?}");
    assert_eq!(
        report.memo_broadcast_count,
        u64::from(ROWS - 2),
        "{report:?}"
    );
}

#[test]
fn guarded_self_reference_stays_legacy_via_internal_dependency() {
    let mut engine = engine(FormulaPlaneMode::AuthoritativeExperimental);
    let mut formulas = Vec::new();
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Boolean(false))
            .unwrap();
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            2,
            &format!("=IF(A{row},B{row},0)"),
        ));
    }
    let report = ingest(&mut engine, "Sheet1", formulas);
    assert_eq!(report.shadow_accepted_span_cells, 0, "{report:?}");
    assert_eq!(
        report.fallback_reasons.get("InternalDependency"),
        Some(&u64::from(ROWS))
    );
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value("Sheet1", 60, 2),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
    ));
}

#[test]
fn conditional_cycle_demotes_and_runtime_witnessing_converges() {
    let cfg = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_cycle(CycleConfig {
            detection: CycleDetection::Runtime,
            policy: CyclePolicy::Error,
        });
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Boolean(false))
            .unwrap();
        formulas.push(record(
            &mut engine,
            "Sheet1",
            row,
            2,
            &format!("=IF(A{row},C{row},0)"),
        ));
    }
    ingest(&mut engine, "Sheet1", formulas);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine
        .set_cell_formula("Sheet1", 5, 3, parse("=B5").unwrap())
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.cycle_errors, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        engine
            .formula_ingest_report_total()
            .fallback_reasons
            .get("CycleMember"),
        Some(&1)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(0.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 3),
        Some(LiteralValue::Number(0.0))
    );
}
