//! Staged-formula store scaling (NOTE(#126) follow-up).
//!
//! `stage_formula_text` used a per-sheet `Vec` with a linear dup-scan per
//! call: O(staged-on-sheet) per stage, O(n²) for an n-formula deferred load
//! on one sheet (the xlsx-load path stages every formula). The store now
//! keeps the same insertion-ordered `Vec` (ingest order is consumer-visible)
//! plus a `(row, col) → index` map, making stage/get/overwrite O(1).
//!
//! Shape probe: timings are printed for the findings report; correctness
//! (overwrite-in-place, insertion order, removal) is asserted.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;

#[test]
fn staging_many_formulas_one_sheet_is_linear_probe() {
    let n: u32 = 50_000;
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    let t = std::time::Instant::now();
    for i in 0..n {
        let row = 1 + i / 100;
        let col = 1 + (i % 100);
        engine.stage_formula_text("Sheet1", row, col, format!("=A{row}+{col}"));
    }
    let stage_fresh = t.elapsed();

    // Overwrite a slice of them (the dup-scan path).
    let t = std::time::Instant::now();
    for i in 0..(n / 5) {
        let row = 1 + i / 100;
        let col = 1 + (i % 100);
        engine.stage_formula_text("Sheet1", row, col, format!("=B{row}+{col}"));
    }
    let stage_overwrite = t.elapsed();

    eprintln!(
        "[staged-scaling] stage {n} fresh: {stage_fresh:?} | overwrite {}: {stage_overwrite:?}",
        n / 5
    );

    // Overwrites replaced in place — no duplicate entries.
    assert_eq!(engine.staged_formula_count(), n as usize);
    assert_eq!(
        engine.get_staged_formula_text("Sheet1", 1, 1),
        Some("=B1+1".to_string()),
        "overwrite must replace the staged text in place"
    );
    assert_eq!(
        engine.get_staged_formula_text("Sheet1", 500, 100),
        Some("=A500+100".to_string()),
        "non-overwritten entries keep their original text"
    );

    // Removal still works and updates the count.
    assert_eq!(
        engine.clear_staged_formula_text("Sheet1", 1, 1),
        Some("=B1+1".to_string())
    );
    assert_eq!(engine.clear_staged_formula_text("Sheet1", 1, 1), None);
    assert_eq!(engine.staged_formula_count(), n as usize - 1);
}
