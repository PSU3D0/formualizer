//! Test mask cache reuse across multiple SUMIFS with same criteria

#[cfg(test)]
mod tests {
    use crate::engine::cache::CriteriaMaskCache;
    use crate::engine::masks::DenseMask;

    #[test]
    fn test_same_criteria_yields_one_mask() {
        // Same (ranges, predicates) should yield one cached mask
        let mut cache = CriteriaMaskCache::new(100);

        // Simulate building a mask for (A:A, "=foo")
        let key = "range:_:*:0:*:0|eq:foo".to_string();
        let mask = DenseMask::new(1000);

        // First insert should succeed
        assert!(cache.insert(key.clone(), mask.clone()));

        // Should be able to retrieve it
        assert!(cache.get(&key).is_some());

        // Same key should reuse existing mask
        let retrieved = cache.get(&key).unwrap();
        assert_eq!(retrieved.len(), mask.len());
    }

    #[test]
    fn test_repeated_sumifs_use_cached_mask() {
        // Multiple SUMIFS with same criteria should hit the cache
        let mut cache = CriteriaMaskCache::new(100);

        // Build mask once
        let key = "range:Sheet1:0:0:999:0|eq:2024".to_string();
        let mut mask = DenseMask::new(1000);
        // Set some bits (rows matching criteria)
        for i in (100..200).step_by(3) {
            mask.set(i, true);
        }

        cache.insert(key.clone(), mask.clone());
        let initial_count = mask.count_ones();

        // Simulate multiple SUMIFS accessing the same mask
        let mut hits = 0;
        for _ in 0..10 {
            if let Some(cached) = cache.get(&key) {
                hits += 1;
                assert_eq!(cached.count_ones(), initial_count);
            }
        }

        assert_eq!(hits, 10, "All SUMIFS should hit the cache");
    }

    #[test]
    fn test_mask_cache_invalidation_on_snapshot() {
        // Masks should be invalidated when snapshot changes
        let mut cache = CriteriaMaskCache::new(100);

        let snapshot_v1 = 1;
        let snapshot_v2 = 2;

        // Key includes snapshot ID
        let key_v1 = format!("snap:{snapshot_v1}|range:A:A|eq:test");
        let key_v2 = format!("snap:{snapshot_v2}|range:A:A|eq:test");

        let mask = DenseMask::new(100);

        // Insert with v1 snapshot
        assert!(cache.insert(key_v1.clone(), mask.clone()));
        assert!(cache.get(&key_v1).is_some());

        // V2 snapshot should not find v1's mask
        assert!(cache.get(&key_v2).is_none());

        // Can insert new mask for v2
        assert!(cache.insert(key_v2.clone(), mask));
        assert!(cache.get(&key_v2).is_some());
    }
}
