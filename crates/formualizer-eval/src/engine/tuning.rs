//! Warmup configuration and tuning parameters

/// Configuration for global pass warmup
#[derive(Clone, Debug)]
pub struct WarmupConfig {
    // Control
    pub warmup_enabled: bool,
    pub warmup_time_budget_ms: u64,
    pub warmup_parallelism_cap: usize,

    // Selection limits
    pub warmup_topk_refs: usize,
    pub warmup_topk_criteria_sets: usize,

    // Thresholds for when to build artifacts
    pub min_flat_cells: usize,
    pub min_mask_cells: usize,
    pub min_index_rows: usize,

    // Reuse thresholds
    pub flat_reuse_threshold: usize,
    pub mask_reuse_threshold: usize,
    pub index_reuse_threshold: usize,

    // Memory budgets
    pub flat_cache_mb_cap: usize,
    pub mask_cache_entries_cap: usize,
    pub index_memory_budget_mb: usize,
}

impl Default for WarmupConfig {
    fn default() -> Self {
        Self {
            // Enabled by default: warmup executes at evaluation time with conservative budgets
            warmup_enabled: false,
            warmup_time_budget_ms: 250,
            warmup_parallelism_cap: 4,

            // Conservative selection limits
            warmup_topk_refs: 10,
            warmup_topk_criteria_sets: 10,

            // Conservative thresholds to avoid overhead on small data
            min_flat_cells: 1000,
            min_mask_cells: 1000,
            min_index_rows: 10000,

            // Require multiple uses to justify caching
            flat_reuse_threshold: 3,
            mask_reuse_threshold: 3,
            index_reuse_threshold: 5,

            // Reasonable memory limits
            flat_cache_mb_cap: 100,
            mask_cache_entries_cap: 1000,
            index_memory_budget_mb: 50,
        }
    }
}

impl WarmupConfig {
    /// Check if flat warmup should be performed
    pub fn should_warmup_flats(&self) -> bool {
        self.warmup_enabled
    }

    /// Check if mask warmup should be performed
    pub fn should_warmup_masks(&self) -> bool {
        self.warmup_enabled
    }

    /// Check if index warmup should be performed
    pub fn should_warmup_indexes(&self) -> bool {
        self.warmup_enabled
    }
}
