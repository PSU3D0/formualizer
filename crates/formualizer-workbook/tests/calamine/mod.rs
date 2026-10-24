//! Regression coverage for the legacy CalamineAdapter name, now a native alias.
//! Historical test names are retained; these are not Calamine differential oracles.
// Shared test helpers (umya workbook builders, etc.)
#[path = "../common.rs"]
mod common;

#[cfg(feature = "xlsx-recalc")]
mod external_links;

#[cfg(feature = "xlsx-recalc")]
mod calcpr;
#[cfg(feature = "xlsx-recalc")]
mod criteria_ingest_blank;
#[cfg(feature = "xlsx-recalc")]
mod criteria_wildcard_parity;
#[cfg(feature = "xlsx-recalc")]
mod date_arithmetic;
#[cfg(feature = "xlsx-recalc")]
mod dates;
#[cfg(feature = "xlsx-recalc")]
mod deltas;
#[cfg(feature = "xlsx-recalc")]
mod engine;
#[cfg(feature = "xlsx-recalc")]
mod family_grouping;
#[cfg(feature = "xlsx-recalc")]
mod format_channel;
#[cfg(feature = "xlsx-recalc")]
mod formulas;
mod inert_sheets;
#[cfg(feature = "xlsx-recalc")]
mod issue162_unbounded_index;
#[cfg(feature = "xlsx-recalc")]
mod it;
#[cfg(feature = "xlsx-recalc")]
mod iterate_corpus_calcpr_fuzz;
#[cfg(feature = "xlsx-recalc")]
mod iterative_cycle_order;
#[cfg(feature = "xlsx-recalc")]
mod large;
#[cfg(feature = "xlsx-recalc")]
mod load_fast_batches;
#[cfg(feature = "xlsx-recalc")]
mod load_hardening;
#[cfg(feature = "xlsx-recalc")]
mod name_omissions;
#[cfg(feature = "xlsx-recalc")]
mod named_ranges;
#[cfg(feature = "xlsx-recalc")]
mod offsets;
#[cfg(feature = "xlsx-recalc")]
mod row_visibility;
#[cfg(feature = "xlsx-recalc")]
mod semantic_epoch_replay;
#[cfg(feature = "xlsx-recalc")]
mod shared_formulas;
#[cfg(feature = "xlsx-recalc")]
mod sheet_load;
#[cfg(feature = "xlsx-recalc")]
mod source_families;
#[cfg(feature = "xlsx-recalc")]
mod temporal_consumers;
#[cfg(feature = "umya")]
mod temporal_roundtrip;

/// `WorkbookConfig::interactive()` with `PreparationPolicy::Strict`. Tests
/// that use a missing sheet to provoke a preparation failure opt into the
/// pre-0.10 policy explicitly (the default became `BestEffort`).
#[cfg(feature = "xlsx-recalc")]
pub(crate) fn strict_interactive() -> formualizer_workbook::WorkbookConfig {
    let mut config = formualizer_workbook::WorkbookConfig::interactive();
    config.eval.preparation_policy = formualizer_eval::engine::PreparationPolicy::Strict;
    config
}
#[cfg(feature = "xlsx-recalc")]
mod referenced_extent;
