use crate::engine::tuning::WarmupConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threshold_defaults_are_conservative() {
        let config = WarmupConfig::default();

        // Minimum cell thresholds to avoid overhead on small ranges
        assert_eq!(
            config.min_flat_cells, 1000,
            "Minimum flat cells should be 1000"
        );
        assert_eq!(
            config.min_mask_cells, 1000,
            "Minimum mask cells should be 1000"
        );
        assert_eq!(
            config.min_index_rows, 10000,
            "Minimum index rows should be 10000"
        );

        // Reuse thresholds - must be referenced multiple times to be worth caching
        assert_eq!(
            config.flat_reuse_threshold, 3,
            "Flat reuse threshold should be 3"
        );
        assert_eq!(
            config.mask_reuse_threshold, 3,
            "Mask reuse threshold should be 3"
        );
        assert_eq!(
            config.index_reuse_threshold, 5,
            "Index reuse threshold should be 5"
        );
    }

    #[test]
    fn test_memory_budget_defaults() {
        let config = WarmupConfig::default();

        // Memory budgets should be reasonable
        assert_eq!(
            config.flat_cache_mb_cap, 100,
            "Flat cache cap should be 100MB"
        );
        assert_eq!(
            config.mask_cache_entries_cap, 1000,
            "Mask cache entries cap should be 1000"
        );
        assert_eq!(
            config.index_memory_budget_mb, 50,
            "Index memory budget should be 50MB"
        );
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_config_can_be_customized() {
        let mut config = WarmupConfig::default();

        // Should be able to enable warmup
        config.warmup_enabled = true;
        assert!(config.warmup_enabled);

        // Should be able to adjust budgets
        config.warmup_time_budget_ms = 500;
        assert_eq!(config.warmup_time_budget_ms, 500);

        // Should be able to adjust thresholds
        config.min_flat_cells = 500;
        assert_eq!(config.min_flat_cells, 500);
    }

    #[test]
    fn test_disabled_warmup_prevents_all_warmup_activity() {
        let config = WarmupConfig {
            warmup_enabled: false,
            ..WarmupConfig::default()
        };

        // When disabled, these settings shouldn't matter
        // This test documents the expectation that warmup_enabled=false
        // should completely bypass all warmup logic
        assert!(!config.warmup_enabled);

        // Even with low thresholds, warmup shouldn't run
        let config = WarmupConfig {
            warmup_enabled: false,
            min_flat_cells: 1,
            min_mask_cells: 1,
            min_index_rows: 1,
            ..WarmupConfig::default()
        };

        assert!(!config.should_warmup_flats());
        assert!(!config.should_warmup_masks());
        assert!(!config.should_warmup_indexes());
    }
}
