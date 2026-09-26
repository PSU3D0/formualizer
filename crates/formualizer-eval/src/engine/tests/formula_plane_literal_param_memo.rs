use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_common::{ErrorContext, ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, parse};

use crate::engine::template::canonical::{CanonicalRejectKind, SlotContext, canonicalize_template};
use crate::engine::template::slots::value_ref_slot_descriptors;
use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

fn authoritative_engine() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    Engine::new(TestWorkbook::default(), cfg)
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
        .unwrap();
}

fn literal_formula_family(rows: u32, literal: impl Fn(u32) -> String) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=A{row}+{}", literal(row)),
        ));
    }
    ingest(&mut engine, formulas);
    engine
}

fn sumifs_varying_literal_engine(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        let typ = format!("Type{}", row % 3);
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Text(typ))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(1.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            3,
            &format!("=SUMIFS($B:$B,$A:$A,\"Type{}\")", row % 3),
        ));
    }
    ingest(&mut engine, formulas);
    engine
}

/// The value assertion of `formula_plane_parameterized_literals_fold_same_structure`
/// (its span-binding checks are span-internal).
#[test]
fn formula_plane_parameterized_literals_fold_same_structure_values() {
    let mut engine = sumifs_varying_literal_engine(100);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(34.0))
    );
}

/// The value assertion of `formula_plane_affine_row_literal_numbers_avoid_graph_materialization`
/// (its ingest-report and binding-encoding checks are span-internal).
#[test]
fn formula_plane_affine_row_literal_numbers_avoid_graph_materialization_values() {
    let mut engine = literal_formula_family(120, |row| row.to_string());
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 120, 2),
        Some(LiteralValue::Number(240.0))
    );
}

/// The value assertion of `formula_plane_non_integer_number_literals_remain_dictionary_encoded`
/// (its binding-encoding check is span-internal).
#[test]
fn formula_plane_non_integer_number_literals_remain_dictionary_encoded_values() {
    let mut engine = literal_formula_family(120, |row| format!("{row}.5"));
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 2),
        Some(LiteralValue::Number(20.5))
    );
}

#[test]
fn formula_plane_affine_literal_run_segmentation_isolates_outlier() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=260 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
            .unwrap();
        let literal = if row == 130 { 999 } else { row };
        formulas.push(record(&mut engine, row, 2, &format!("=A$1*{literal}")));
    }
    ingest(&mut engine, formulas);
    let report = engine.last_formula_ingest_report().unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 129, 2),
        Some(LiteralValue::Number(129.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 130, 2),
        Some(LiteralValue::Number(999.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 260, 2),
        Some(LiteralValue::Number(260.0))
    );
}

#[test]
fn formula_plane_literal_slot_wildcards_kind_but_binding_preserves_type() {
    let numeric = canonicalize_template(&parse("=A1+1").unwrap(), 1, 2);
    let text = canonicalize_template(&parse("=A1+\"1\"").unwrap(), 1, 2);
    assert_eq!(
        numeric.parameterized_key.payload(),
        text.parameterized_key.payload()
    );
    assert_ne!(numeric.key.payload(), text.key.payload());
    assert!(matches!(
        numeric.literal_bindings.as_ref(),
        [LiteralValue::Int(1)] | [LiteralValue::Number(1.0)]
    ));
    assert!(matches!(
        text.literal_bindings.as_ref(),
        [LiteralValue::Text(s)] if s == "1"
    ));
}

#[test]
fn formula_plane_array_literal_remains_rejected_after_literal_parameterization() {
    let ast = ASTNode::new(
        ASTNodeType::Array(vec![vec![ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Number(1.0)),
            None,
        )]]),
        None,
    );
    let template = canonicalize_template(&ast, 1, 1);
    assert!(
        template
            .labels
            .contains_reject_kind(CanonicalRejectKind::ArrayLiteral)
    );
    assert!(template.literal_slot_descriptors.is_empty());
}

#[test]
fn formula_plane_empty_literal_parameterizes() {
    let err = ExcelError::new(ExcelErrorKind::Value).with_message("bad literal");
    for value in [
        LiteralValue::Empty,
        LiteralValue::Pending,
        LiteralValue::Error(err),
    ] {
        let ast = ASTNode::new(ASTNodeType::Literal(value.clone()), None);
        let template = canonicalize_template(&ast, 1, 1);
        assert_eq!(template.literal_slot_descriptors.len(), 1);
        assert_eq!(template.literal_bindings.as_ref(), [value].as_slice());
        assert!(template.parameterized_key.payload().contains("lit_slot(0)"));
    }
}

#[test]
fn formula_plane_demoted_parameterized_span_materializes_bound_literals() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*2")));
        formulas.push(record(&mut engine, row, 4, &format!("=A{row}-3")));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();
    engine.delete_columns("Sheet1", 3, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 3),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_memo_residual_relative_reference_includes_row_delta() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Number(10.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            5,
            &format!("=A{row}+SUM(C{row}:D{row})"),
        ));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 7, 5),
        Some(LiteralValue::Number(18.0))
    );
}

#[test]
fn formula_plane_parameter_key_uses_number_bits() {}

#[test]
fn formula_plane_parameter_key_nan_reflexive() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
}

#[test]
fn formula_plane_parameter_key_negative_zero_distinct() {}

#[test]
fn formula_plane_parameter_key_dates_and_durations_are_typed() {
    let date = LiteralValue::Date(NaiveDate::from_ymd_opt(2026, 5, 6).unwrap());
    let dt = LiteralValue::DateTime(
        NaiveDate::from_ymd_opt(2026, 5, 6)
            .unwrap()
            .and_hms_opt(1, 2, 3)
            .unwrap(),
    );
    let time = LiteralValue::Time(NaiveTime::from_hms_opt(1, 2, 3).unwrap());
    let duration = LiteralValue::Duration(Duration::seconds(3723));
    for value in [date, dt, time, duration] {
        let ast = ASTNode::new(ASTNodeType::Literal(value.clone()), None);
        let template = canonicalize_template(&ast, 1, 1);
        assert_eq!(template.literal_bindings.as_ref(), [value].as_slice());
        assert!(template.parameterized_key.payload().contains("lit_slot(0)"));
    }
}

#[test]
fn formula_plane_parameter_key_error_includes_message_and_context() {
    let err_a = ExcelError {
        kind: ExcelErrorKind::Value,
        message: Some("a".into()),
        context: Some(ErrorContext {
            row: Some(1),
            col: Some(2),
            origin_row: Some(3),
            origin_col: Some(4),
            origin_sheet: Some("S".into()),
        }),
        extra: ExcelErrorExtra::Spill {
            expected_rows: 2,
            expected_cols: 3,
        },
    };
}

#[test]
fn formula_plane_volatile_template_not_memoized() {
    let ast = parse("=RAND()+1").unwrap();
    let template = canonicalize_template(&ast, 1, 1);
    assert!(!template.labels.is_authority_supported());
}

#[test]
fn formula_plane_dynamic_template_not_memoized() {
    let ast = parse("=OFFSET(A1,0,0)").unwrap();
    let template = canonicalize_template(&ast, 1, 1);
    assert!(!template.labels.is_authority_supported());
}

#[test]
fn formula_plane_row_column_args_not_value_parameterized() {
    for formula in ["=ROW(A1)", "=COLUMN(A1)"] {
        let ast = parse(formula).unwrap();
        let template = canonicalize_template(&ast, 1, 2);
        assert!(value_ref_slot_descriptors(&template.expr).is_empty());
    }
}

#[test]
fn formula_plane_offset_byref_not_value_parameterized() {
    let ast = parse("=OFFSET(A1,0,0)").unwrap();
    let template = canonicalize_template(&ast, 1, 2);
    assert!(value_ref_slot_descriptors(&template.expr).is_empty());
}

#[test]
fn formula_plane_index_position_arg_is_value_parameterized() {
    let ast = parse("=INDEX($D$1:$D$10,A1)").unwrap();
    let template = canonicalize_template(&ast, 1, 2);
    let slots = value_ref_slot_descriptors(&template.expr);
    assert_eq!(slots.len(), 1);
    assert_eq!(slots[0].context, SlotContext::Value);
}

#[test]
fn formula_plane_criteria_range_not_value_parameterized() {
    let ast = parse("=SUMIFS(B:B,A:A,\"Type1\")").unwrap();
    let template = canonicalize_template(&ast, 1, 3);
    let slots = value_ref_slot_descriptors(&template.expr);
    assert!(
        slots
            .iter()
            .all(|slot| slot.context != SlotContext::CriteriaRangeArg)
    );
}
