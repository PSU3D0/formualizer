/// Program 1 M2 reclassification (see formualizer-eval `engine/tests/mod.rs`):
/// the wrapped statements assert FormulaPlane span internals. They are
/// skipped when the engine ignores the FormulaPlane mode (unified authority,
/// or the `FZ_M2_FORCE_PLANE_OFF` oracle); the reason literal records why.
macro_rules! span_internal {
    ($reason:literal; $($body:tt)*) => {
        if !formualizer_eval::engine::eval::formula_plane_mode_ignored_for_test() {
            $($body)*
        }
    };
}

// Shared test helpers (umya workbook builders, etc.)
#[path = "../common.rs"]
mod common;

#[cfg(feature = "calamine")]
mod calcpr;
#[cfg(feature = "calamine")]
mod criteria_ingest_blank;
#[cfg(feature = "calamine")]
mod criteria_wildcard_parity;
#[cfg(feature = "calamine")]
mod date_arithmetic;
#[cfg(feature = "calamine")]
mod dates;
#[cfg(feature = "calamine")]
mod deltas;
#[cfg(feature = "calamine")]
mod engine;
#[cfg(feature = "calamine")]
mod format_channel;
#[cfg(feature = "calamine")]
mod formulas;
#[cfg(feature = "calamine")]
mod issue162_unbounded_index;
#[cfg(feature = "calamine")]
mod it;
#[cfg(feature = "calamine")]
mod iterate_corpus_calcpr_fuzz;
#[cfg(feature = "calamine")]
mod large;
#[cfg(feature = "calamine")]
mod load_fast_batches;
#[cfg(feature = "calamine")]
mod named_ranges;
#[cfg(feature = "calamine")]
mod offsets;
#[cfg(feature = "calamine")]
mod row_visibility;
#[cfg(feature = "calamine")]
mod semantic_epoch_replay;
#[cfg(feature = "calamine")]
mod shared_formulas;
#[cfg(feature = "calamine")]
mod sheet_load;
#[cfg(feature = "umya")]
mod temporal_roundtrip;

/// `WorkbookConfig::interactive()` with `PreparationPolicy::Strict`. Tests
/// that use a missing sheet to provoke a preparation failure opt into the
/// pre-0.10 policy explicitly (the default became `BestEffort`).
#[cfg(feature = "calamine")]
pub(crate) fn strict_interactive() -> formualizer_workbook::WorkbookConfig {
    let mut config = formualizer_workbook::WorkbookConfig::interactive();
    config.eval.preparation_policy = formualizer_eval::engine::PreparationPolicy::Strict;
    config
}
