use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

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
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn active_span_count(engine: &Engine<TestWorkbook>) -> usize {
    engine.baseline_stats().formula_plane_active_span_count
}

fn assert_number(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    assert_eq!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Number(expected))
    );
}

fn build_column_family(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity((rows * 5) as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        for col in 7..=11 {
            engine
                .set_cell_value(
                    "Sheet1",
                    row,
                    col,
                    LiteralValue::Number((row * 100 + col) as f64),
                )
                .unwrap();
        }
        for (idx, col) in (2..=6).enumerate() {
            let addend = idx + 1;
            formulas.push(record(&mut engine, row, col, &format!("=A{row}+{addend}")));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(active_span_count(&engine), 5);
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn column_delete_outside_span_region_with_dirty_closure_no_recompute() {
    let mut engine = build_column_family(1000);
    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 123, col, 123.0 + f64::from(col - 1));
    }

    engine.delete_columns("Sheet1", 7, 1).unwrap();

    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 123, col, 123.0 + f64::from(col - 1));
    }
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0, "result={result:?}");
    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 987, col, 987.0 + f64::from(col - 1));
    }
}

#[test]
fn column_insert_outside_span_region_with_dirty_closure_no_recompute() {
    let mut engine = build_column_family(1000);
    assert_eq!(active_span_count(&engine), 5);

    engine.insert_columns("Sheet1", 7, 1).unwrap();

    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 321, col, 321.0 + f64::from(col - 1));
    }
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0, "result={result:?}");
    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 654, col, 654.0 + f64::from(col - 1));
    }
}

fn build_row_run(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(active_span_count(&engine), 1);
    engine.evaluate_all().unwrap();
    engine
}

fn build_col_run(cols: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(21.0))
        .unwrap();
    super::formula_plane_structural::seed_absolute_read_span(
        &mut engine,
        "=$A$1*2",
        crate::formula_plane::runtime::PlacementDomain::col_run(0, 0, 1, cols),
    );
    assert_eq!(active_span_count(&engine), 1);
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn row_structural_before_inside_after_publish_exact_bounded_span_regions() {
    let mut inside = build_row_run(1000);
    let sheet_id = inside.graph.sheet_id("Sheet1").unwrap();
    let globals_before = inside
        .baseline_stats()
        .formula_plane_dirty_global_invalidations;
    inside.insert_rows("Sheet1", 901, 1).unwrap();
    let exact = inside
        .graph
        .pending_formula_dirty_span_regions()
        .collect::<Vec<_>>();
    assert_eq!(exact.len(), 1, "only the shifted lower split is dirty");
    assert_eq!(
        exact[0].1,
        crate::formula_plane::region_index::Region::rect(sheet_id, 901, 1000, 1, 1)
    );
    assert_eq!(
        inside
            .graph
            .pending_formula_dirty_regions()
            .collect::<Vec<_>>(),
        vec![crate::formula_plane::region_index::Region::rows_from(
            sheet_id, 900
        )]
    );
    inside.evaluate_all().unwrap();
    assert_eq!(
        inside
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        100
    );
    assert_eq!(
        inside
            .baseline_stats()
            .formula_plane_dirty_global_invalidations,
        globals_before,
        "structural geometry must not use global WholeAll-equivalent dirtiness"
    );

    let mut before = build_row_run(1000);
    before.insert_rows("Sheet1", 1, 1).unwrap();
    before.evaluate_all().unwrap();
    assert_eq!(
        before
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        1000
    );

    let mut after = build_row_run(1000);
    after.insert_rows("Sheet1", 1002, 1).unwrap();
    after.evaluate_all().unwrap();
    assert!(after.last_formula_plane_span_eval_report().is_none());
}

#[test]
fn row_delete_tail_recomputes_only_compacted_interval() {
    let mut engine = build_row_run(1000);
    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    engine.delete_rows("Sheet1", 901, 50).unwrap();
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_span_regions()
            .map(|(_, region)| region)
            .collect::<Vec<_>>(),
        vec![crate::formula_plane::region_index::Region::rect(
            sheet_id, 900, 949, 1, 1
        )]
    );
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        50
    );
}

#[test]
fn column_structural_inside_and_after_are_precise() {
    let mut inside = build_col_run(1000);
    let sheet_id = inside.graph.sheet_id("Sheet1").unwrap();
    inside.insert_columns("Sheet1", 902, 1).unwrap();
    assert_eq!(
        inside
            .graph
            .pending_formula_dirty_span_regions()
            .map(|(_, region)| region)
            .collect::<Vec<_>>(),
        vec![crate::formula_plane::region_index::Region::rect(
            sheet_id, 0, 0, 902, 1001
        )]
    );
    inside.evaluate_all().unwrap();
    assert_eq!(
        inside
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        100
    );

    let mut after = build_col_run(1000);
    after.insert_columns("Sheet1", 1003, 1).unwrap();
    after.evaluate_all().unwrap();
    assert!(after.last_formula_plane_span_eval_report().is_none());
}

#[test]
fn failed_duplicate_validation_publishes_no_structural_dirty_delta() {
    let mut engine = build_row_run(120);
    let refs = engine.graph.formula_authority().active_span_refs();
    let before = engine.baseline_stats();
    assert!(engine.duplicate_sheet("Sheet1", "Sheet1").is_err());
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
    let after = engine.baseline_stats();
    assert_eq!(
        after.formula_plane_dirty_span_region_events_recorded,
        before.formula_plane_dirty_span_region_events_recorded
    );
    assert_eq!(
        after.formula_plane_dirty_region_events_recorded,
        before.formula_plane_dirty_region_events_recorded
    );
}

fn ingest_row_run_on_sheet(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    rows: u32,
    formula_col: u32,
) {
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value(sheet, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(engine, row, formula_col, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, formulas)])
        .unwrap();
}

#[test]
fn structural_span_region_isolated_to_edited_sheet() {
    let mut engine = authoritative_engine();
    engine.add_sheet("Sheet2").unwrap();
    ingest_row_run_on_sheet(&mut engine, "Sheet1", 1000, 2);
    ingest_row_run_on_sheet(&mut engine, "Sheet2", 1000, 2);
    assert_eq!(active_span_count(&engine), 2);
    engine.evaluate_all().unwrap();

    let sheet1_id = engine.graph.sheet_id("Sheet1").unwrap();
    let sheet2_id = engine.graph.sheet_id("Sheet2").unwrap();
    engine.insert_rows("Sheet1", 901, 1).unwrap();
    let exact = engine
        .graph
        .pending_formula_dirty_span_regions()
        .collect::<Vec<_>>();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].1.sheet_id(), sheet1_id);
    let authority = engine.graph.formula_authority();
    assert_eq!(
        authority.plane.spans.get(exact[0].0).unwrap().sheet_id,
        sheet1_id
    );
    assert!(
        authority
            .active_span_refs()
            .into_iter()
            .any(|span_ref| authority.plane.spans.get(span_ref).unwrap().sheet_id == sheet2_id)
    );

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        100
    );
    assert_eq!(
        engine.get_cell_value("Sheet2", 999, 2),
        Some(LiteralValue::Number(1000.0))
    );
}

#[test]
fn sheet_and_unrelated_name_table_lifecycle_do_not_dirty_surviving_spans() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord, RangeRef};

    let mut engine = authoritative_engine();
    engine.add_sheet("Sheet2").unwrap();
    ingest_row_run_on_sheet(&mut engine, "Sheet1", 120, 2);
    ingest_row_run_on_sheet(&mut engine, "Sheet2", 120, 2);
    engine.evaluate_all().unwrap();

    engine
        .rename_sheet(engine.graph.sheet_id("Sheet2").unwrap(), "Renamed")
        .unwrap();
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);

    engine
        .define_name(
            "Unrelated",
            NamedDefinition::Literal(LiteralValue::Number(7.0)),
            NameScope::Workbook,
        )
        .unwrap();
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);

    let sheet1_id = engine.graph.sheet_id("Sheet1").unwrap();
    engine
        .define_table(
            "UnrelatedTable",
            RangeRef::new(
                CellRef::new(sheet1_id, Coord::from_excel(1, 10, true, true)),
                CellRef::new(sheet1_id, Coord::from_excel(2, 10, true, true)),
            ),
            true,
            vec!["Value".into()],
            false,
        )
        .unwrap();
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);

    let refs_before_duplicate = engine.graph.formula_authority().active_span_refs();
    engine.duplicate_sheet("Sheet1", "Copy").unwrap();
    let surviving = engine.graph.formula_authority().active_span_refs();
    assert_eq!(surviving.len(), 1, "only the source-sheet span is demoted");
    assert!(refs_before_duplicate.contains(&surviving[0]));
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
    engine.evaluate_all().unwrap();
    assert!(engine.last_formula_plane_span_eval_report().is_none());

    engine.add_sheet("Empty").unwrap();
    let empty_id = engine.graph.sheet_id("Empty").unwrap();
    let globals_before = engine
        .baseline_stats()
        .formula_plane_dirty_global_invalidations;
    engine.remove_sheet(empty_id).unwrap();
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        surviving
    );
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_whole_spans()
            .collect::<Vec<_>>(),
        surviving
    );
    assert_eq!(
        engine
            .baseline_stats()
            .formula_plane_dirty_global_invalidations,
        globals_before + 1
    );
}

#[test]
fn structural_insert_action_undo_redo_preserves_values_without_unlogged_span_geometry() {
    use crate::engine::graph::editor::undo_engine::UndoEngine;

    let mut engine = build_row_run(120);
    let mut undo = UndoEngine::new();
    let (_value, journal) = engine
        .action_atomic_journal("insert row".to_string(), |tx| {
            tx.insert_rows("Sheet1", 61, 1)?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);
    assert_eq!(
        active_span_count(&engine),
        0,
        "journaled geometry is materialized"
    );
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 62, 2),
        Some(LiteralValue::Number(62.0))
    );

    engine.undo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 61, 2),
        Some(LiteralValue::Number(62.0))
    );

    engine.redo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 62, 2),
        Some(LiteralValue::Number(62.0))
    );
}

#[test]
fn structural_candidate_overflow_is_atomic_and_retryable() {
    let mut engine = build_row_run(120);
    let refs_before = engine.graph.formula_authority().active_span_refs();
    let topology_before = engine.topology_epoch_for_test();
    let graph_revision_before = engine.graph_topology_revision_for_test();
    let (plane_epoch_before, indexes_epoch_before) = {
        let authority = engine.graph.formula_authority();
        (authority.plane.epoch(), authority.indexes_epoch())
    };
    let dirty_stats_before = engine.graph.formula_dirty_stats();
    let dirty_regions_before = engine
        .graph
        .pending_formula_dirty_regions()
        .collect::<Vec<_>>();
    let dirty_span_regions_before = engine
        .graph
        .pending_formula_dirty_span_regions()
        .collect::<Vec<_>>();
    let dirty_spans_before = engine
        .graph
        .pending_formula_dirty_whole_spans()
        .collect::<Vec<_>>();
    let candidates_before = engine
        .baseline_stats()
        .formula_plane_structural_span_candidates;

    engine.config.max_formula_plane_cache_candidates = 0;
    assert!(engine.insert_rows("Sheet1", 111, 1).is_err());

    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        refs_before
    );
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    assert_eq!(
        engine.graph_topology_revision_for_test(),
        graph_revision_before
    );
    {
        let authority = engine.graph.formula_authority();
        assert_eq!(authority.plane.epoch(), plane_epoch_before);
        assert_eq!(authority.indexes_epoch(), indexes_epoch_before);
    }
    assert_eq!(engine.graph.formula_dirty_stats(), dirty_stats_before);
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_regions()
            .collect::<Vec<_>>(),
        dirty_regions_before
    );
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_span_regions()
            .collect::<Vec<_>>(),
        dirty_span_regions_before
    );
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_whole_spans()
            .collect::<Vec<_>>(),
        dirty_spans_before
    );
    assert_eq!(
        engine
            .baseline_stats()
            .formula_plane_structural_span_candidates,
        candidates_before
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 111, 1),
        Some(LiteralValue::Number(111.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 121, 1), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 120, 2),
        Some(LiteralValue::Number(121.0))
    );

    engine.config.max_formula_plane_cache_candidates = 100_000;
    engine.insert_rows("Sheet1", 111, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 121, 2),
        Some(LiteralValue::Number(121.0))
    );
}

#[test]
fn indexed_structural_selection_classifies_only_affected_candidate_among_many_sheets() {
    const SHEETS: u32 = 24;
    let mut engine = authoritative_engine();
    for index in 0..SHEETS {
        let sheet = if index == 0 {
            "Sheet1".to_string()
        } else {
            let name = format!("Unrelated{index}");
            engine.add_sheet(&name).unwrap();
            name
        };
        ingest_row_run_on_sheet(&mut engine, &sheet, 120, 2);
    }
    assert_eq!(active_span_count(&engine), SHEETS as usize);
    engine.evaluate_all().unwrap();
    let candidates_before = engine
        .baseline_stats()
        .formula_plane_structural_span_candidates;

    engine.insert_rows("Sheet1", 111, 1).unwrap();

    let candidates_after = engine
        .baseline_stats()
        .formula_plane_structural_span_candidates;
    assert_eq!(
        candidates_after - candidates_before,
        1,
        "result/read indexes must select only the edited sheet's span"
    );
    assert_eq!(engine.graph.pending_formula_dirty_span_regions().count(), 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        10
    );
    assert_eq!(
        engine.get_cell_value("Unrelated23", 120, 2),
        Some(LiteralValue::Number(121.0))
    );
}
