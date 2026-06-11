//! Event-log equivalence + undo/redo tests for changelog old-state capture on
//! BATCH edit paths (`write_range`, `set_values`).
//!
//! Context: in Arrow-truth mode the graph value cache is disabled, so the
//! graph-level `VertexEditor` cannot capture `old_value` on its own. The
//! workbook captures old state from Arrow truth before the batch and (before
//! the O(N^2) fix) patched it into the log afterwards via
//! `patch_last_cell_event_old_state`. These tests pin the EXACT event-log
//! content produced by that append-then-patch flow (goldens derived from the
//! pre-fix code at 16b1fa8d) so the direct pass-through implementation can be
//! proven byte-for-byte equivalent.

use std::collections::BTreeMap;

use formualizer_common::LiteralValue;
use formualizer_eval::engine::ChangeEvent;
use formualizer_workbook::Workbook;
use formualizer_workbook::traits::CellData;

fn wb_changelog_graph_mode() -> Workbook {
    // Changelog on, defer_graph_building OFF so formulas go through the
    // VertexEditor (and write_range emits SetFormula events).
    let mut cfg = formualizer_workbook::WorkbookConfig::interactive();
    cfg.eval.defer_graph_building = false;
    Workbook::new_with_config(cfg)
}

/// Render one change event as a stable, content-complete line.
fn render_event(ev: &ChangeEvent) -> String {
    let fmt_ast = |a: &Option<formualizer_parse::ASTNode>| match a {
        Some(ast) => format!("Some({})", formualizer_parse::pretty::canonical_formula(ast)),
        None => "None".to_string(),
    };
    match ev {
        ChangeEvent::SetValue {
            addr,
            old_value,
            old_formula,
            new,
        } => format!(
            "SetValue addr=({},{},{}) old_value={:?} old_formula={} new={:?}",
            addr.sheet_id,
            addr.coord.row(),
            addr.coord.col(),
            old_value,
            fmt_ast(old_formula),
            new
        ),
        ChangeEvent::SetFormula {
            addr,
            old_value,
            old_formula,
            new,
        } => format!(
            "SetFormula addr=({},{},{}) old_value={:?} old_formula={} new={}",
            addr.sheet_id,
            addr.coord.row(),
            addr.coord.col(),
            old_value,
            fmt_ast(old_formula),
            formualizer_parse::pretty::canonical_formula(new)
        ),
        ChangeEvent::CompoundStart { description, .. } => {
            format!("CompoundStart {description}")
        }
        ChangeEvent::CompoundEnd { .. } => "CompoundEnd".to_string(),
        ChangeEvent::SpillCleared { .. } => "SpillCleared".to_string(),
        other => format!(
            "Other({})",
            // Variant name only; payload content is not under test here.
            format!("{other:?}")
                .split([' ', '(', '{'])
                .next()
                .unwrap_or("?")
        ),
    }
}

fn assert_numeric_eq(v: Option<LiteralValue>, expected: f64) {
    match v {
        Some(LiteralValue::Number(n)) => assert!((n - expected).abs() < 1e-9, "{n} != {expected}"),
        Some(LiteralValue::Int(i)) => assert!(((i as f64) - expected).abs() < 1e-9),
        other => panic!("expected numeric {expected}, got {other:?}"),
    }
}

fn render_log_from(wb: &Workbook, mark: usize) -> Vec<String> {
    wb.changelog().events()[mark..]
        .iter()
        .map(render_event)
        .collect()
}

/// Seed a workbook with a mix of values, formulas and a spill anchor, then
/// evaluate so Arrow truth holds computed results. Returns the changelog mark
/// (length) after seeding.
fn seed(wb: &mut Workbook) -> usize {
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(10)).unwrap(); // A1 = 10
    wb.set_formula("S", 1, 2, "=A1*2").unwrap(); // B1 = formula
    wb.set_value("S", 1, 3, LiteralValue::Int(5)).unwrap(); // C1 = 5
    wb.set_formula("S", 1, 4, "=A1+C1").unwrap(); // D1 = formula
    wb.set_formula("S", 1, 5, "=SEQUENCE(2,1)").unwrap(); // E1 spills E1:E2
    wb.evaluate_all().unwrap();
    wb.changelog().events().len()
}

/// Golden event log for the `write_range` batch path (changelog on, graph
/// mode). Mixed batch: value-over-value, value-over-formula,
/// formula-over-value, formula-over-formula, value-over-spill-anchor, and a
/// single cell receiving BOTH a value and a formula in one batch (two events
/// for one cell — only the last one historically received the patched old
/// state).
#[test]
fn write_range_batch_event_log_golden() {
    let mut wb = wb_changelog_graph_mode();
    let mark = seed(&mut wb);

    let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
    cells.insert((1, 1), CellData::from_value(LiteralValue::Int(11))); // A1: value over value
    cells.insert((1, 2), CellData::from_value(LiteralValue::Int(7))); // B1: value over formula
    cells.insert(
        (1, 3),
        CellData {
            value: None,
            formula: Some("=A1*3".into()),
            style: None,
        },
    ); // C1: formula over value
    cells.insert(
        (1, 4),
        CellData {
            value: None,
            formula: Some("=A1*4".into()),
            style: None,
        },
    ); // D1: formula over formula
    cells.insert((1, 5), CellData::from_value(LiteralValue::Int(99))); // E1: value over spill anchor
    cells.insert(
        (1, 6),
        CellData {
            value: Some(LiteralValue::Int(1)),
            formula: Some("=A1+1".into()),
            style: None,
        },
    ); // F1: value AND formula in the same batch item
    cells.insert((1, 7), CellData::from_value(LiteralValue::Int(2))); // G1: fresh cell

    wb.write_range("S", (1, 1), cells).unwrap();

    let got = render_log_from(&wb, mark);
    let expected = vec![
        "SetValue addr=(1,0,0) old_value=Some(Number(10.0)) old_formula=None new=Int(11)",
        "SetValue addr=(1,0,1) old_value=Some(Number(20.0)) old_formula=Some(=A1 * 2) new=Int(7)",
        "SetFormula addr=(1,0,2) old_value=Some(Number(5.0)) old_formula=None new==A1 * 3",
        "SetFormula addr=(1,0,3) old_value=Some(Number(15.0)) old_formula=Some(=A1 + C1) new==A1 * 4",
        "CompoundStart SetValueWithSpillClear sheet=1 row=0 col=4",
        "SpillCleared",
        "SetValue addr=(1,0,4) old_value=Some(Number(1.0)) old_formula=Some(=SEQUENCE(2, 1)) new=Int(99)",
        "CompoundEnd",
        "SetValue addr=(1,0,5) old_value=None old_formula=None new=Int(1)",
        "SetFormula addr=(1,0,5) old_value=None old_formula=None new==A1 + 1",
        "SetValue addr=(1,0,6) old_value=None old_formula=None new=Int(2)",
    ];
    assert_eq!(
        got, expected,
        "write_range batch changelog content diverged from pre-fix golden"
    );
}

/// Golden event log for the `set_values` batch path (changelog on, graph
/// mode): values over value/formula/spill-anchor/fresh cells.
#[test]
fn set_values_batch_event_log_golden() {
    let mut wb = wb_changelog_graph_mode();
    let mark = seed(&mut wb);

    // Rectangle A1:F1 — covers value-over-value (A1), value-over-formula
    // (B1, D1), value-over-value (C1), value-over-spill-anchor (E1), fresh (F1).
    wb.set_values(
        "S",
        1,
        1,
        &[vec![
            LiteralValue::Int(21),
            LiteralValue::Int(22),
            LiteralValue::Int(23),
            LiteralValue::Int(24),
            LiteralValue::Int(25),
            LiteralValue::Int(26),
        ]],
    )
    .unwrap();

    let got = render_log_from(&wb, mark);
    let expected = vec![
        "SetValue addr=(1,0,0) old_value=Some(Number(10.0)) old_formula=None new=Int(21)",
        "SetValue addr=(1,0,1) old_value=Some(Number(20.0)) old_formula=Some(=A1 * 2) new=Int(22)",
        "SetValue addr=(1,0,2) old_value=Some(Number(5.0)) old_formula=None new=Int(23)",
        "SetValue addr=(1,0,3) old_value=Some(Number(15.0)) old_formula=Some(=A1 + C1) new=Int(24)",
        "CompoundStart SetValueWithSpillClear sheet=1 row=0 col=4",
        "SpillCleared",
        "SetValue addr=(1,0,4) old_value=Some(Number(1.0)) old_formula=Some(=SEQUENCE(2, 1)) new=Int(25)",
        "CompoundEnd",
        "SetValue addr=(1,0,5) old_value=None old_formula=None new=Int(26)",
    ];
    assert_eq!(
        got, expected,
        "set_values batch changelog content diverged from pre-fix golden"
    );
}

/// Golden for the interactive default (defer_graph_building ON): write_range
/// values are graph edits, formulas are STAGED (no editor event). A cell with
/// both value+formula emits only the SetValue event, which historically
/// received the patched old state (it is the cell's last event).
#[test]
fn write_range_batch_event_log_golden_deferred() {
    let mut wb = Workbook::new(); // interactive: changelog on, defer on
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(10)).unwrap(); // A1
    wb.set_value("S", 1, 2, LiteralValue::Int(20)).unwrap(); // B1
    let mark = wb.changelog().events().len();

    let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
    cells.insert(
        (1, 1),
        CellData {
            value: Some(LiteralValue::Int(11)),
            formula: Some("=B1*2".into()),
            style: None,
        },
    ); // A1: value (graph) + formula (staged)
    cells.insert((1, 2), CellData::from_value(LiteralValue::Int(21))); // B1: value over value
    cells.insert(
        (1, 3),
        CellData {
            value: None,
            formula: Some("=B1*3".into()),
            style: None,
        },
    ); // C1: staged formula only — no graph event
    wb.write_range("S", (1, 1), cells).unwrap();

    let got = render_log_from(&wb, mark);
    let expected = vec![
        "SetValue addr=(1,0,0) old_value=Some(Number(10.0)) old_formula=None new=Int(11)",
        "SetValue addr=(1,0,1) old_value=Some(Number(20.0)) old_formula=None new=Int(21)",
        "Other(StagedFormulaCellChanged)",
        "Other(StagedFormulaCellChanged)",
    ];
    assert_eq!(
        got, expected,
        "deferred write_range changelog content diverged from pre-fix golden"
    );
    assert_eq!(wb.get_formula("S", 1, 1), Some("=B1*2".to_string()));
    assert_eq!(wb.get_formula("S", 1, 3), Some("=B1*3".to_string()));
}

/// Undo/redo round-trip across batched set_values + write_range with the
/// changelog on. Undo must restore the exact pre-batch state (values AND
/// formulas AND staged text); redo must reapply. Includes the repeated-cell
/// case (same cell receives value+formula inside one write_range batch).
#[test]
fn undo_redo_batched_writes_round_trip() {
    let mut wb = wb_changelog_graph_mode();

    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(10)).unwrap(); // A1 value
    wb.set_formula("S", 1, 2, "=A1*2").unwrap(); // B1 formula
    wb.set_value("S", 1, 3, LiteralValue::Int(5)).unwrap(); // C1 value
    wb.set_formula("S", 1, 4, "=A1+C1").unwrap(); // D1 formula
    wb.evaluate_all().unwrap();

    let pre_b1_val = wb.get_value("S", 1, 2);
    let pre_d1_val = wb.get_value("S", 1, 4);

    // Action 1: write_range mixed batch (incl. same-cell value+formula at F1).
    wb.begin_action("batch write_range");
    let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
    cells.insert((1, 1), CellData::from_value(LiteralValue::Int(11)));
    cells.insert((1, 2), CellData::from_value(LiteralValue::Int(7)));
    cells.insert(
        (1, 4),
        CellData {
            value: None,
            formula: Some("=A1*4".into()),
            style: None,
        },
    );
    cells.insert(
        (1, 6),
        CellData {
            value: Some(LiteralValue::Int(1)),
            formula: Some("=A1+1".into()),
            style: None,
        },
    );
    wb.write_range("S", (1, 1), cells).unwrap();
    wb.end_action();

    // Action 2: set_values rectangle over A1:C1.
    wb.begin_action("batch set_values");
    wb.set_values(
        "S",
        1,
        1,
        &[vec![
            LiteralValue::Int(31),
            LiteralValue::Int(32),
            LiteralValue::Int(33),
        ]],
    )
    .unwrap();
    wb.end_action();

    // Final state.
    assert_numeric_eq(wb.get_value("S", 1, 1), 31.0);
    assert_numeric_eq(wb.get_value("S", 1, 2), 32.0);
    assert_numeric_eq(wb.get_value("S", 1, 3), 33.0);
    assert_eq!(wb.get_formula("S", 1, 2), None);
    assert_eq!(wb.get_formula("S", 1, 4), Some("=A1 * 4".to_string()));
    assert_eq!(wb.get_formula("S", 1, 6), Some("=A1 + 1".to_string()));

    // Undo both batches -> exact pre-batch state. (Computed results of restored
    // formulas refresh on the next evaluation, as on any formula edit.)
    wb.undo().unwrap();
    wb.undo().unwrap();
    assert_numeric_eq(wb.get_value("S", 1, 1), 10.0);
    assert_eq!(wb.get_formula("S", 1, 2), Some("=A1 * 2".to_string()));
    assert_numeric_eq(wb.get_value("S", 1, 3), 5.0);
    assert_eq!(wb.get_formula("S", 1, 4), Some("=A1 + C1".to_string()));
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 1, 2), pre_b1_val);
    assert_eq!(wb.get_value("S", 1, 4), pre_d1_val);
    assert_eq!(wb.get_formula("S", 1, 6), None);
    assert!(matches!(
        wb.get_value("S", 1, 6),
        None | Some(LiteralValue::Empty)
    ));

    // Redo both batches -> final state again.
    wb.redo().unwrap();
    wb.redo().unwrap();
    assert_numeric_eq(wb.get_value("S", 1, 1), 31.0);
    assert_numeric_eq(wb.get_value("S", 1, 2), 32.0);
    assert_numeric_eq(wb.get_value("S", 1, 3), 33.0);
    assert_eq!(wb.get_formula("S", 1, 4), Some("=A1 * 4".to_string()));
    assert_eq!(wb.get_formula("S", 1, 6), Some("=A1 + 1".to_string()));
}

/// Undo/redo for the interactive default (deferred graph build): batched
/// write_range stages formulas; undo must restore prior staged text exactly.
#[test]
fn undo_redo_batched_writes_staged_text_round_trip() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "1+1").unwrap(); // A1 staged
    wb.set_value("S", 1, 2, LiteralValue::Int(20)).unwrap(); // B1 value

    wb.begin_action("batch");
    let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
    cells.insert(
        (1, 1),
        CellData {
            value: None,
            formula: Some("=2+2".into()),
            style: None,
        },
    ); // A1: replace staged text
    cells.insert((1, 2), CellData::from_value(LiteralValue::Int(21))); // B1: value over value
    cells.insert(
        (1, 3),
        CellData {
            value: Some(LiteralValue::Int(3)),
            formula: Some("=3+3".into()),
            style: None,
        },
    ); // C1: value + staged formula on the same cell
    wb.write_range("S", (1, 1), cells).unwrap();
    wb.end_action();

    assert_eq!(wb.get_formula("S", 1, 1), Some("=2+2".to_string()));
    assert_numeric_eq(wb.get_value("S", 1, 2), 21.0);
    assert_eq!(wb.get_formula("S", 1, 3), Some("=3+3".to_string()));

    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), Some("1+1".to_string()));
    assert_numeric_eq(wb.get_value("S", 1, 2), 20.0);
    assert_eq!(wb.get_formula("S", 1, 3), None);

    wb.redo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), Some("=2+2".to_string()));
    assert_numeric_eq(wb.get_value("S", 1, 2), 21.0);
    assert_eq!(wb.get_formula("S", 1, 3), Some("=3+3".to_string()));
}
