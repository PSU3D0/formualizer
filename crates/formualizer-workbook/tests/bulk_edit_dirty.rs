//! Batched workbook edits (`set_values` / `set_formulas` / `write_range`)
//! run ONE deferred multi-source dirty propagation instead of a full BFS per
//! cell (O(edits × component) → O(component)); see
//! `Engine::begin_deferred_dirty`. Python's `set_values_batch` /
//! `set_formulas_batch` wrap these APIs and inherit the behavior.
//!
//! Perf shape is asserted via `Engine::dirty_propagation_visits` (BFS visit
//! counts), never wall time. Exact dirty-set/affected-set equality with
//! sequential edits is pinned at the graph level
//! (formualizer-eval `engine::tests::deferred_dirty`); here we pin the
//! workbook surface: visit scaling, per-cell vs batch value equivalence,
//! undo of a batched action, and flush-on-error.

use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

const SHEET: &str = "Sheet1";
const INPUTS: u32 = 200;
const CHAIN: u32 = 200;

fn workbook(changelog: bool) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.enable_changelog = changelog;
    Workbook::new_with_config(config)
}

fn num(wb: &Workbook, row: u32, col: u32) -> f64 {
    match wb.get_value(SHEET, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at r{row}c{col}, got {other:?}"),
    }
}

/// K inputs in column A all feed one shared component: B1 = SUM(A1:AK) plus
/// a CHAIN-long arithmetic chain off B1. Disjoint island: D1 = C1 + 1 off
/// input C1 (untouched by the batch edits unless a test edits it).
fn build_rollup(wb: &mut Workbook) {
    let rows: Vec<Vec<LiteralValue>> = (0..INPUTS).map(|_| vec![LiteralValue::Int(1)]).collect();
    wb.set_values(SHEET, 1, 1, &rows).unwrap();
    wb.set_formula(SHEET, 1, 2, &format!("=SUM(A1:A{INPUTS})"))
        .unwrap();
    for r in 2..=(CHAIN + 1) {
        wb.set_formula(SHEET, r, 2, &format!("=B{}+1", r - 1))
            .unwrap();
    }
    wb.set_value(SHEET, 1, 3, LiteralValue::Int(7)).unwrap();
    wb.set_formula(SHEET, 1, 4, "=C1+1").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(num(wb, 1, 2), INPUTS as f64);
    assert_eq!(num(wb, CHAIN + 1, 2), (INPUTS + CHAIN) as f64);
}

fn assert_post_edit_state(wb: &Workbook, input_value: f64) {
    assert_eq!(num(wb, 1, 2), input_value * INPUTS as f64, "rollup root");
    assert_eq!(
        num(wb, CHAIN + 1, 2),
        input_value * INPUTS as f64 + CHAIN as f64,
        "chain tail"
    );
}

/* ─────────────── visit scaling: O(component), not O(N × component) ────── */

fn batched_set_values_visits_o_component(changelog: bool) {
    let mut wb = workbook(changelog);
    build_rollup(&mut wb);

    let before = wb.engine().dirty_propagation_visits();
    let rows: Vec<Vec<LiteralValue>> = (0..INPUTS)
        .map(|_| vec![LiteralValue::Number(2.0)])
        .collect();
    wb.set_values(SHEET, 1, 1, &rows).unwrap();
    let delta = wb.engine().dirty_propagation_visits() - before;
    eprintln!(
        "[bulk-edit-dirty] batched set_values (changelog={changelog}): {delta} BFS visits \
         ({INPUTS} inputs × {}-component; per-cell loop was ≥ {})",
        CHAIN + 1,
        INPUTS as u64 * (CHAIN + 1) as u64
    );

    let component = (CHAIN + 1) as u64; // B1 + chain (inputs are value cells)
    assert!(
        delta <= 2 * component + INPUTS as u64,
        "batched set_values (changelog={changelog}) must be ~one component walk \
         (≈{component} visits), got {delta} (per-cell loop was ≥ {})",
        INPUTS as u64 * component
    );

    wb.evaluate_all().unwrap();
    assert_post_edit_state(&wb, 2.0);
}

#[test]
fn batched_set_values_visits_are_o_component_no_changelog() {
    batched_set_values_visits_o_component(false);
}

#[test]
fn batched_set_values_visits_are_o_component_with_changelog() {
    batched_set_values_visits_o_component(true);
}

#[test]
fn batched_set_formulas_visits_are_o_component() {
    // Rewrite every chain formula (same text) in one batch: each edit's
    // propagation reaches the rest of the chain, so a per-cell loop is
    // O(CHAIN²); the deferred flush walks the chain once.
    let mut wb = workbook(false);
    build_rollup(&mut wb);

    let before = wb.engine().dirty_propagation_visits();
    let rows: Vec<Vec<String>> = (2..=(CHAIN + 1))
        .map(|r| vec![format!("=B{}+1", r - 1)])
        .collect();
    wb.set_formulas(SHEET, 2, 2, &rows).unwrap();
    let delta = wb.engine().dirty_propagation_visits() - before;
    eprintln!(
        "[bulk-edit-dirty] batched set_formulas: {delta} BFS visits \
         ({CHAIN}-chain rewrite; per-cell loop was ≥ {})",
        (CHAIN as u64) * (CHAIN as u64) / 2
    );

    let component = (CHAIN + 1) as u64;
    assert!(
        delta <= 3 * component,
        "batched set_formulas must be ~one component walk (≈{component} visits), \
         got {delta} (per-cell loop was ≥ {})",
        (CHAIN as u64) * (CHAIN as u64) / 2
    );

    wb.evaluate_all().unwrap();
    assert_post_edit_state(&wb, 1.0);
}

/* ─────────── per-cell loop vs batch: identical post-recalc values ─────── */

#[test]
fn batch_equals_per_cell_loop_values() {
    // Shared + disjoint downstream structure; the edit set includes values
    // overwriting a formula cell and a formula overwriting a value cell.
    fn apply_per_cell(wb: &mut Workbook) {
        for r in 1..=INPUTS {
            wb.set_value(SHEET, r, 1, LiteralValue::Number(3.0))
                .unwrap();
        }
        // Value over formula (D1 was =C1+1), formula over value (C1 was 7).
        wb.set_value(SHEET, 1, 4, LiteralValue::Number(99.0))
            .unwrap();
        wb.set_formula(SHEET, 1, 3, "=B1*2").unwrap();
    }
    fn apply_batch(wb: &mut Workbook) {
        let rows: Vec<Vec<LiteralValue>> = (0..INPUTS)
            .map(|_| vec![LiteralValue::Number(3.0)])
            .collect();
        wb.set_values(SHEET, 1, 1, &rows).unwrap();
        wb.set_values(SHEET, 1, 4, &[vec![LiteralValue::Number(99.0)]])
            .unwrap();
        wb.set_formulas(SHEET, 1, 3, &[vec!["=B1*2".to_string()]])
            .unwrap();
    }

    let mut wb_seq = workbook(false);
    build_rollup(&mut wb_seq);
    apply_per_cell(&mut wb_seq);
    wb_seq.evaluate_all().unwrap();

    let mut wb_batch = workbook(false);
    build_rollup(&mut wb_batch);
    apply_batch(&mut wb_batch);
    wb_batch.evaluate_all().unwrap();

    for (r, c) in (1..=(CHAIN + 1))
        .map(|r| (r, 2u32))
        .chain([(1u32, 3u32), (1, 4)])
        .chain((1..=INPUTS).map(|r| (r, 1u32)))
    {
        assert_eq!(
            wb_seq.get_value(SHEET, r, c),
            wb_batch.get_value(SHEET, r, c),
            "value mismatch at r{r}c{c}"
        );
    }
    assert_eq!(num(&wb_batch, 1, 3), 2.0 * 3.0 * INPUTS as f64);
}

/* ───────────────────────── changelog / undo ───────────────────────────── */

#[test]
fn undo_restores_pre_batch_values() {
    let mut wb = workbook(true);
    build_rollup(&mut wb);

    wb.begin_action("bulk input update");
    let rows: Vec<Vec<LiteralValue>> = (0..INPUTS)
        .map(|_| vec![LiteralValue::Number(5.0)])
        .collect();
    wb.set_values(SHEET, 1, 1, &rows).unwrap();
    wb.end_action();
    wb.evaluate_all().unwrap();
    assert_post_edit_state(&wb, 5.0);

    wb.undo().unwrap();
    for r in 1..=INPUTS {
        assert_eq!(
            num(&wb, r, 1),
            1.0,
            "input r{r} must be restored by one undo of the batched action"
        );
    }
    wb.evaluate_all().unwrap();
    assert_post_edit_state(&wb, 1.0);

    wb.redo().unwrap();
    wb.evaluate_all().unwrap();
    assert_post_edit_state(&wb, 5.0);
}

/* ─────────────────────────── error-path flush ─────────────────────────── */

#[test]
fn mid_batch_error_flushes_scope_and_later_edits_propagate() {
    // Row 2's formula fails to parse → set_formulas errors mid-batch. The
    // deferred scope must still flush (row 1's edit propagates), and the
    // graph must be fully consistent: a subsequent SINGLE edit propagates
    // normally and evaluation entry sees no active deferral (debug_assert).
    let mut wb = workbook(false);
    build_rollup(&mut wb);

    let rows = vec![
        vec!["=SUM(A1:A10)*1000".to_string()],
        vec!["=THIS IS NOT A FORMULA ((".to_string()],
        vec!["=A1+1".to_string()],
    ];
    let err = wb.set_formulas(SHEET, 10, 6, &rows);
    assert!(err.is_err(), "mid-batch parse failure must surface");

    // Row 1 of the failed batch was applied before the error; its dirty
    // marking must have been flushed, so a recalc evaluates it.
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, 10, 6), 10_000.0, "pre-error edit must evaluate");

    // Subsequent single edit must propagate through the shared component.
    wb.set_value(SHEET, 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, 1, 2), (INPUTS + 1) as f64, "rollup sees the edit");
    assert_eq!(
        num(&wb, 10, 6),
        11_000.0,
        "failed-batch survivor re-evaluates"
    );
}
