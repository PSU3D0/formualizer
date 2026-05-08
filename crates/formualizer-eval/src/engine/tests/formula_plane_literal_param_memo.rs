use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_common::{ErrorContext, ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, parse};

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::formula_plane::placement::{FormulaPlacementCandidate, PlacementFallbackReason};
use crate::formula_plane::placement::{place_candidate_family, value_ref_slot_descriptors};
use crate::formula_plane::runtime::FormulaSpanRef;
use crate::formula_plane::span_eval::{ErrorExtraAtom, ParameterAtom, ParameterKey};
use crate::formula_plane::template_canonical::{
    CanonicalRejectKind, SlotContext, canonicalize_template,
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

fn span_binding_unique_count(engine: &Engine<TestWorkbook>) -> usize {
    let authority = engine.graph.formula_authority();
    let span = authority
        .plane
        .spans
        .active_spans()
        .next()
        .expect("active span");
    let binding_set_id = span.binding_set_id.expect("binding set");
    authority
        .plane
        .binding_sets
        .unique_vector_count(binding_set_id)
        .unwrap()
}

fn first_template_keys(engine: &Engine<TestWorkbook>) -> (String, String) {
    let authority = engine.graph.formula_authority();
    let span = authority
        .plane
        .spans
        .active_spans()
        .next()
        .expect("active span");
    let template = authority.plane.templates.get(span.template_id).unwrap();
    (
        template.exact_canonical_key.to_string(),
        template.parameterized_canonical_key.to_string(),
    )
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

#[test]
fn formula_plane_parameterized_literals_fold_same_structure() {
    let mut engine = sumifs_varying_literal_engine(100);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(span_binding_unique_count(&engine), 3);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(34.0))
    );
}

#[test]
fn formula_plane_exact_canonical_key_retained_for_diagnostics() {
    let engine = literal_formula_family(100, |row| (row % 3).to_string());
    let (exact, parameterized) = first_template_keys(&engine);
    assert_ne!(exact, parameterized);
    assert!(exact.contains("int:") || exact.contains("num_bits:"));
    assert!(parameterized.contains("lit_slot(0)"));
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
fn formula_plane_binding_store_dictionary_encodes_repeated_vectors() {
    let engine = literal_formula_family(120, |row| (row % 3).to_string());
    assert_eq!(span_binding_unique_count(&engine), 3);
}

#[test]
fn formula_plane_binding_set_removed_with_span() {
    let mut engine = literal_formula_family(100, |row| (row % 3).to_string());
    let (span_ref, binding_set_id) = {
        let authority = engine.graph.formula_authority();
        let span = authority.plane.spans.active_spans().next().unwrap();
        (
            FormulaSpanRef {
                id: span.id,
                generation: span.generation,
                version: span.version,
            },
            span.binding_set_id.unwrap(),
        )
    };
    assert!(
        engine
            .graph
            .formula_authority()
            .plane
            .binding_sets
            .get(binding_set_id)
            .is_some()
    );
    engine
        .graph
        .formula_authority_mut()
        .plane
        .remove_span(span_ref);
    assert!(
        engine
            .graph
            .formula_authority()
            .plane
            .binding_sets
            .get(binding_set_id)
            .is_none()
    );
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
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 3);
    engine.evaluate_all().unwrap();
    engine.delete_columns("Sheet1", 3, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
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
fn formula_plane_memoizes_value_context_relative_cell_refs() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value(
                "Sheet1",
                row,
                1,
                LiteralValue::Text(format!("Type{}", row % 3)),
            )
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(1.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            3,
            &format!("=SUMIFS($B:$B,$A:$A,A{row})"),
        ));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 3);
    assert_eq!(report.memo_broadcast_count, 117);
}

#[test]
fn formula_plane_memoizes_varying_literal_slots() {
    let mut engine = sumifs_varying_literal_engine(120);
    engine.evaluate_all().unwrap();
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 3);
    assert_eq!(report.memo_broadcast_count, 117);
}

#[test]
fn formula_plane_memoizes_mixed_literal_and_value_ref_parameters() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number((row % 2) as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+{}", row % 3)));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 6);
    assert_eq!(report.memo_broadcast_count, 114);
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
    // The relative range is not value-parameterized, so row deltas make the sample unique and
    // memoization is skipped rather than reusing the A-row value across all placements.
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 7, 5),
        Some(LiteralValue::Number(18.0))
    );
}

#[test]
fn formula_plane_memo_skips_all_unique_literal_bindings() {
    let mut engine = literal_formula_family(120, |row| row.to_string());
    engine.evaluate_all().unwrap();
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 0);
    assert_eq!(report.sample_only_key_build_count, 64);
}

#[test]
fn formula_plane_memo_sampling_skips_all_unique_value_refs() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.memo_eval_count, 0);
    assert_eq!(report.sample_only_key_build_count, 64);
}

#[test]
fn formula_plane_parameter_key_uses_number_bits() {
    assert_ne!(
        ParameterAtom::NumberBits(1.0f64.to_bits()),
        ParameterAtom::NumberBits(1.0000000000000002f64.to_bits())
    );
}

#[test]
fn formula_plane_parameter_key_nan_reflexive() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let key = ParameterKey {
        atoms: vec![ParameterAtom::NumberBits(nan.to_bits())].into_boxed_slice(),
    };
    assert_eq!(key, key.clone());
}

#[test]
fn formula_plane_parameter_key_negative_zero_distinct() {
    assert_ne!(
        ParameterAtom::NumberBits(0.0f64.to_bits()),
        ParameterAtom::NumberBits((-0.0f64).to_bits())
    );
}

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
    let atom_a = ParameterAtom::Error {
        kind: err_a.kind,
        message: err_a.message.as_deref().map(Arc::from),
        context_row: err_a.context.as_ref().and_then(|c| c.row),
        context_col: err_a.context.as_ref().and_then(|c| c.col),
        origin_row: err_a.context.as_ref().and_then(|c| c.origin_row),
        origin_col: err_a.context.as_ref().and_then(|c| c.origin_col),
        origin_sheet: err_a
            .context
            .as_ref()
            .and_then(|c| c.origin_sheet.as_deref().map(Arc::from)),
        extra: ErrorExtraAtom::Spill {
            expected_rows: 2,
            expected_cols: 3,
        },
    };
    let atom_b = ParameterAtom::Error {
        kind: err_a.kind,
        message: Some(Arc::from("b")),
        context_row: err_a.context.as_ref().and_then(|c| c.row),
        context_col: err_a.context.as_ref().and_then(|c| c.col),
        origin_row: err_a.context.as_ref().and_then(|c| c.origin_row),
        origin_col: err_a.context.as_ref().and_then(|c| c.origin_col),
        origin_sheet: err_a
            .context
            .as_ref()
            .and_then(|c| c.origin_sheet.as_deref().map(Arc::from)),
        extra: ErrorExtraAtom::Spill {
            expected_rows: 2,
            expected_cols: 3,
        },
    };
    assert_ne!(atom_a, atom_b);
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

#[test]
fn formula_plane_parameter_key_hash_collision_does_not_merge_results() {
    let mut map = rustc_hash::FxHashMap::default();
    map.insert(
        ParameterKey {
            atoms: vec![ParameterAtom::Int(1)].into_boxed_slice(),
        },
        "int",
    );
    map.insert(
        ParameterKey {
            atoms: vec![ParameterAtom::Text(Arc::from("1"))].into_boxed_slice(),
        },
        "text",
    );
    assert_eq!(map.len(), 2);
}

#[test]
fn formula_plane_parameterized_canonical_hash_collision_does_not_merge_family() {
    let mut plane = crate::formula_plane::runtime::FormulaPlane::default();
    let mut data_store = crate::engine::arena::DataStore::new();
    let sheet_registry = crate::engine::sheet_registry::SheetRegistry::new();
    let candidates = ["=A1+1", "=A2*1"]
        .into_iter()
        .enumerate()
        .map(|(idx, formula)| {
            let ast = parse(formula).unwrap();
            let ast_id = data_store.store_ast(&ast, &sheet_registry);
            FormulaPlacementCandidate::new(
                0,
                idx as u32,
                1,
                ast_id,
                Some(Arc::<str>::from(formula)),
            )
        })
        .collect::<Vec<_>>();
    let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);
    assert_eq!(
        report
            .counters
            .fallback_reasons
            .get(&PlacementFallbackReason::NonEquivalentTemplate),
        Some(&2)
    );
}

#[test]
fn formula_plane_literal_binding_memory_cap_falls_back() {
    let mut plane = crate::formula_plane::runtime::FormulaPlane::default();
    let mut data_store = crate::engine::arena::DataStore::new();
    let sheet_registry = crate::engine::sheet_registry::SheetRegistry::new();
    let candidates = (0..100)
        .map(|idx| {
            let text = format!("{}{}", "x".repeat(90 * 1024), idx);
            let formula = format!("=\"{text}\"");
            let ast = parse(&formula).unwrap();
            let ast_id = data_store.store_ast(&ast, &sheet_registry);
            FormulaPlacementCandidate::new(0, idx, 0, ast_id, None)
        })
        .collect::<Vec<_>>();
    let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);
    assert_eq!(
        report
            .counters
            .fallback_reasons
            .get(&PlacementFallbackReason::BindingMemoryCapExceeded),
        Some(&100)
    );
}

#[test]
fn formula_plane_memo_cache_is_per_evaluate_task() {
    let mut engine = sumifs_varying_literal_engine(120);
    engine.evaluate_all().unwrap();
    let first = engine
        .last_formula_plane_span_eval_report()
        .unwrap()
        .memo_eval_count;
    engine
        .graph
        .formula_authority_mut()
        .mark_all_active_spans_dirty();
    engine.evaluate_all().unwrap();
    let second = engine
        .last_formula_plane_span_eval_report()
        .unwrap()
        .memo_eval_count;
    assert_eq!((first, second), (3, 3));
}
