use crate::engine::cache::{FlatKind, FlatView, RangeFlatCache};
use crate::engine::pass_planner::PassPlanner;
use crate::engine::reference_fingerprint::ReferenceFingerprint;
use crate::engine::tuning::WarmupConfig;
use formualizer_core::parser::ReferenceType;
use std::sync::Arc;

#[test]
fn test_flatten_reuse() {
    let mut config = WarmupConfig::default();
    config.warmup_enabled = true;
    config.min_flat_cells = 10;
    config.flat_reuse_threshold = 2;

    let mut cache = RangeFlatCache::new(100);

    // Create a reference that appears multiple times
    let ref1 = ReferenceType::Range {
        sheet: None,
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(9),
        end_col: Some(0),
    };

    // First build
    let key1 = ref1.fingerprint();
    let flat1 = FlatView {
        kind: FlatKind::Numeric {
            values: Arc::from([1.0; 10]),
            valid: None,
        },
        row_count: 10,
        col_count: 1,
    };
    cache.insert(key1.clone(), flat1.clone());

    // Verify reuse
    let retrieved = cache.get(&key1);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().row_count, 10);

    // Test that same reference produces same key
    let key2 = ref1.fingerprint();
    assert_eq!(key1, key2);
}

#[test]
fn test_snapshot_invalidation() {
    let mut cache = RangeFlatCache::new(100);

    let ref1 = ReferenceType::Range {
        sheet: None,
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(4),
        end_col: Some(0),
    };

    let key = ref1.fingerprint();
    let flat = FlatView {
        kind: FlatKind::Numeric {
            values: Arc::from([1.0, 2.0, 3.0, 4.0, 5.0]),
            valid: None,
        },
        row_count: 5,
        col_count: 1,
    };

    cache.insert(key.clone(), flat);
    assert!(cache.get(&key).is_some());

    // Simulate snapshot invalidation
    cache.clear();
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_min_cells_threshold() {
    let mut config = WarmupConfig::default();
    config.warmup_enabled = true;
    config.min_flat_cells = 100;

    let planner = PassPlanner::new(config.clone());

    // Small range - should not be selected for flattening
    let small_ref = ReferenceType::Range {
        sheet: None,
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(9),
        end_col: Some(0), // 10 cells
    };

    // Large range - should be selected
    let large_ref = ReferenceType::Range {
        sheet: None,
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(99),
        end_col: Some(0), // 100 cells
    };

    // Verify planner respects threshold
    assert!(!planner.should_flatten(&small_ref, 10));
    assert!(planner.should_flatten(&large_ref, 100));
}

#[test]
fn test_memory_budget_enforcement() {
    let mut config = WarmupConfig::default();
    config.warmup_enabled = true;
    config.flat_cache_mb_cap = 1; // 1MB cap

    let mut cache = RangeFlatCache::new(config.flat_cache_mb_cap);

    // Try to insert data that exceeds budget
    let large_flat = FlatView {
        kind: FlatKind::Numeric {
            values: Arc::from(vec![1.0; 1_000_000].as_slice()),
            valid: None,
        },
        row_count: 1_000_000,
        col_count: 1,
    };

    let key = "large_key".to_string();
    cache.insert(key.clone(), large_flat);

    // Verify cache enforces memory limit
    assert!(cache.memory_usage_mb() <= config.flat_cache_mb_cap);
}

#[test]
fn test_parallel_build_coordination() {
    use std::sync::{Arc as StdArc, Mutex};
    use std::thread;
    use std::time::Duration;

    let cache = StdArc::new(Mutex::new(RangeFlatCache::new(100)));
    let build_count = StdArc::new(Mutex::new(0));

    let ref1 = ReferenceType::Range {
        sheet: None,
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(99),
        end_col: Some(0),
    };
    let key = ref1.fingerprint();

    // Spawn multiple threads trying to build the same range
    let mut handles = vec![];
    for _ in 0..5 {
        let cache_clone = StdArc::clone(&cache);
        let build_count_clone = StdArc::clone(&build_count);
        let key_clone = key.clone();

        let handle = thread::spawn(move || {
            let mut cache = cache_clone.lock().unwrap();

            // Check if already built
            if cache.get(&key_clone).is_none() {
                // Simulate build
                thread::sleep(Duration::from_millis(10));
                *build_count_clone.lock().unwrap() += 1;

                let flat = FlatView {
                    kind: FlatKind::Numeric {
                        values: Arc::from([1.0; 100]),
                        valid: None,
                    },
                    row_count: 100,
                    col_count: 1,
                };
                cache.insert(key_clone, flat);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Only one build should have occurred
    assert_eq!(*build_count.lock().unwrap(), 1);
}
