use super::compute_criteria_mask;
use super::criteria_mask_cache::CriteriaMaskCache;
use crate::args::{CriteriaPredicate, parse_criteria};
use crate::arrow_store::{ArrowSheet, IngestBuilder, OverlayFragment, OverlayValue};
use crate::engine::{CancelToken, DateSystem};
use arrow_array::BooleanArray;
use formualizer_common::LiteralValue;
use std::cell::Cell;
use std::sync::Arc;

fn engine_mask(
    engine: &super::Engine<crate::test_workbook::TestWorkbook>,
    name: &str,
) -> Arc<BooleanArray> {
    use crate::traits::EvaluationContext;
    let sheet = engine.arrow_sheets.sheet(name);
    assert!(sheet.is_some());
    sheet
        .and_then(|sheet| {
            engine.build_criteria_mask(
                &sheet.range_view(0, 0, 0, 0),
                0,
                &CriteriaPredicate::Eq(LiteralValue::Number(1.0)),
            )
        })
        .unwrap_or_else(|| Arc::new(BooleanArray::from(Vec::<bool>::new())))
}

#[test]
fn engine_structural_edits_and_sheet_lifecycle_invalidate_masks() {
    let mut engine = super::Engine::new(
        crate::test_workbook::TestWorkbook::new(),
        crate::engine::EvalConfig::default(),
    );
    let id = engine.add_sheet("S");
    assert!(id.is_ok());
    if let Some(sheet) = engine.arrow_sheets.sheet_mut("S") {
        *sheet = fixture(&[LiteralValue::Number(1.0), LiteralValue::Number(2.0)]);
    }
    let before = engine_mask(&engine, "S");
    assert_eq!(before.true_count(), 1);
    assert!(Arc::ptr_eq(&before, &engine_mask(&engine, "S")));
    assert!(engine.insert_rows("S", 1, 1).is_ok());
    assert_eq!(engine_mask(&engine, "S").true_count(), 0);
    assert!(engine.delete_rows("S", 1, 1).is_ok());
    let before_add = engine_mask(&engine, "S");
    assert_eq!(before_add.true_count(), 1);
    let other = engine.add_sheet("T");
    assert!(other.is_ok());
    let after_add = engine_mask(&engine, "S");
    assert!(!Arc::ptr_eq(&before_add, &after_add));
    if let Ok(other) = other {
        assert!(engine.remove_sheet(other).is_ok());
    }
    let after_remove = engine_mask(&engine, "S");
    assert!(!Arc::ptr_eq(&after_add, &after_remove));
    if let Ok(id) = id {
        assert!(engine.rename_sheet(id, "R").is_ok());
        let renamed = engine_mask(&engine, "R");
        assert!(!Arc::ptr_eq(&after_remove, &renamed));
        assert_eq!(renamed.true_count(), 1);
        assert!(engine.remove_sheet(id).is_ok());
    }
    assert!(engine.add_sheet("S").is_ok());
    if let Some(sheet) = engine.arrow_sheets.sheet_mut("S") {
        *sheet = fixture(&[LiteralValue::Number(2.0)]);
    }
    assert_eq!(engine_mask(&engine, "S").true_count(), 0);
}

fn fixture(values: &[LiteralValue]) -> ArrowSheet {
    let mut ingest = IngestBuilder::new("S", 1, 131_072, DateSystem::Excel1900);
    for value in values {
        assert!(ingest.append_row(std::slice::from_ref(value)).is_ok());
    }
    ingest.finish()
}

fn verdict(
    cache: &CriteriaMaskCache,
    sheet: &ArrowSheet,
    builds: &Cell<usize>,
) -> Arc<BooleanArray> {
    let view = sheet.range_view(0, 0, sheet.nrows as usize - 1, 0);
    let pred = CriteriaPredicate::Eq(LiteralValue::Number(1.0));
    cache
        .get_or_build(&view, 0, &pred, (0, 0, 0), || {
            builds.set(builds.get() + 1);
            compute_criteria_mask(&view, 0, &pred)
        })
        .unwrap_or_else(|| Arc::new(BooleanArray::from(Vec::<bool>::new())))
}

#[test]
fn exact_masks_share_1000_formulas_as_97_builds() {
    let sheet = fixture(
        &(0..1000)
            .map(|r| LiteralValue::Text(format!("key{}", r % 97)))
            .collect::<Vec<_>>(),
    );
    let cache = CriteriaMaskCache::default();
    let builds = Cell::new(0);
    for r in 0..1000 {
        let view = sheet.range_view(0, 0, 999, 0);
        let pred = CriteriaPredicate::Eq(LiteralValue::Text(format!("key{}", r % 97)));
        let result = cache.get_or_build(&view, 0, &pred, (0, 0, 0), || {
            builds.set(builds.get() + 1);
            compute_criteria_mask(&view, 0, &pred)
        });
        assert!(result.is_some());
    }
    assert_eq!(builds.get(), 97);
}

#[test]
fn user_computed_and_same_layer_writes_invalidate_exact_masks() {
    let mut sheet = fixture(&[LiteralValue::Number(1.0)]);
    let cache = CriteriaMaskCache::default();
    let builds = Cell::new(0);
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    assert_eq!(builds.get(), 1);
    if let Some(chunk) = sheet.columns.first_mut().and_then(|c| c.chunks.first_mut()) {
        chunk.computed_overlay.set(0, OverlayValue::Number(2.0));
    }
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 0);
    // A second publication in the same recalc/layer must not reuse the first mask.
    if let Some(chunk) = sheet.columns.first_mut().and_then(|c| c.chunks.first_mut()) {
        chunk.computed_overlay.set(0, OverlayValue::Number(1.0));
    }
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    if let Some(chunk) = sheet.columns.first_mut().and_then(|c| c.chunks.first_mut()) {
        chunk.overlay.set(0, OverlayValue::Empty);
    }
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 0);
    assert_eq!(builds.get(), 4);
}

#[test]
fn spill_fragment_replacement_and_clear_invalidate_exact_masks() {
    let mut sheet = fixture(&[LiteralValue::Number(1.0)]);
    let cache = CriteriaMaskCache::default();
    let builds = Cell::new(0);
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    if let Some(chunk) = sheet.columns.first_mut().and_then(|c| c.chunks.first_mut()) {
        let fragment = OverlayFragment::dense_range(0, vec![OverlayValue::Number(2.0)]);
        assert!(fragment.is_some());
        if let Some(fragment) = fragment {
            chunk.computed_overlay.apply_fragment(fragment);
        }
    }
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 0);
    if let Some(chunk) = sheet.columns.first_mut().and_then(|c| c.chunks.first_mut()) {
        chunk.computed_overlay.clear();
    }
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    assert_eq!(builds.get(), 3);
}

#[test]
fn base_replacement_and_sheet_recreation_do_not_reuse_masks() {
    let cache = CriteriaMaskCache::default();
    let builds = Cell::new(0);
    let mut sheet = fixture(&[LiteralValue::Number(1.0)]);
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    // Replace base lanes while retaining both overlay objects, as structural
    // re-encoding can do. Epochs alone are insufficient.
    let replacement = fixture(&[LiteralValue::Number(2.0)]);
    if let (Some(old), Some(new)) = (
        sheet.columns.first_mut().and_then(|c| c.chunks.first_mut()),
        replacement.columns.first().and_then(|c| c.chunks.first()),
    ) {
        old.numbers = new.numbers.clone();
    }
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 0);
    drop(sheet);
    let sheet = fixture(&[LiteralValue::Number(1.0)]);
    assert_eq!(verdict(&cache, &sheet, &builds).true_count(), 1);
    assert_eq!(builds.get(), 3);
}

#[test]
fn typed_criteria_match_uncached_results_and_cancelled_hits_bypass() {
    let sheet = fixture(&[
        LiteralValue::Text("MiXeD".into()),
        LiteralValue::Text("5".into()),
        LiteralValue::Number(5.0),
        LiteralValue::Boolean(true),
        LiteralValue::Empty,
        LiteralValue::Text("".into()),
        LiteralValue::Text("a*".into()),
        LiteralValue::Text("İ".into()),
        LiteralValue::Text("é".into()),
        LiteralValue::Text("a ".into()),
        LiteralValue::Text("%_\\~".into()),
    ]);
    let cache = CriteriaMaskCache::default();
    let view = sheet.range_view(0, 0, 10, 0);
    for criterion in [
        "mixed", "5", "TRUE", "", "=", "<>", "a~*", "*", "?", "İ", "é", "a ", "%_\\~", "<>a*",
    ] {
        let pred = parse_criteria(&LiteralValue::Text(criterion.into()));
        assert!(pred.is_ok());
        if let Ok(pred) = pred {
            let oracle = compute_criteria_mask(&view, 0, &pred);
            for _ in 0..2 {
                let cached = cache.get_or_build(&view, 0, &pred, (0, 0, 0), || {
                    compute_criteria_mask(&view, 0, &pred)
                });
                assert_eq!(
                    cached.as_deref(),
                    oracle.as_deref(),
                    "criterion {criterion:?}"
                );
            }
        }
    }
    let token = CancelToken::new();
    let cancelled_view = view.with_cancel_token(Some(token.clone()));
    token.cancel();
    let pred = CriteriaPredicate::Eq(LiteralValue::Text("mixed".into()));
    let called = Cell::new(false);
    let result = cache.get_or_build(&cancelled_view, 0, &pred, (0, 0, 0), || {
        called.set(true);
        None
    });
    assert!(called.get());
    assert!(result.is_none());
}
