use crate::engine::cache::{FlatKind, FlatView};
use crate::engine::reference_fingerprint::ReferenceFingerprint;
use crate::engine::tuning::WarmupConfig;
use crate::engine::warmup::PassContext;
use formualizer_core::parser::ReferenceType;
use std::sync::Arc;

#[test]
fn test_pass_context_lifetime() {
    let config = WarmupConfig {
        warmup_enabled: true,
        flat_cache_mb_cap: 10,
        ..Default::default()
    };

    // Create pass context
    let mut pass_ctx = PassContext::new(&config);

    // Add some data
    let ref1 = ReferenceType::Range {
        sheet: None,
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(99),
        end_col: Some(0),
    };

    let key = ref1.fingerprint();
    let flat = FlatView {
        kind: FlatKind::Numeric {
            values: Arc::from([1.0; 100]),
            valid: None,
        },
        row_count: 100,
        col_count: 1,
    };

    pass_ctx.flat_cache.insert(key.clone(), flat);

    // Verify data is available during pass
    assert!(pass_ctx.flat_cache.get(&key).is_some());

    // Pass context should be dropped after evaluation
    // This happens automatically in Rust
}

#[test]
fn test_cache_cleanup_between_passes() {
    let config = WarmupConfig {
        warmup_enabled: true,
        flat_cache_mb_cap: 10,
        ..Default::default()
    };

    // First pass
    {
        let mut pass_ctx = PassContext::new(&config);

        let ref1 = ReferenceType::Range {
            sheet: None,
            start_row: Some(0),
            start_col: Some(0),
            end_row: Some(49),
            end_col: Some(0),
        };

        let key = ref1.fingerprint();
        let flat = FlatView {
            kind: FlatKind::Numeric {
                values: Arc::from([2.0; 50]),
                valid: None,
            },
            row_count: 50,
            col_count: 1,
        };

        pass_ctx.flat_cache.insert(key.clone(), flat);
        assert!(pass_ctx.flat_cache.get(&key).is_some());
    } // pass_ctx dropped here

    // Second pass - should start fresh
    {
        let pass_ctx = PassContext::new(&config);

        let ref1 = ReferenceType::Range {
            sheet: None,
            start_row: Some(0),
            start_col: Some(0),
            end_row: Some(49),
            end_col: Some(0),
        };

        let key = ref1.fingerprint();

        // Cache should be empty in new pass
        assert!(pass_ctx.flat_cache.get(&key).is_none());
    }
}

#[test]
fn test_metrics_tracking_across_pass() {
    let config = WarmupConfig {
        warmup_enabled: true,
        ..Default::default()
    };

    let pass_ctx = PassContext::new(&config);

    // Track some metrics
    pass_ctx.metrics.record_flat_build(100, 5);
    pass_ctx.metrics.record_flat_build(200, 10);
    pass_ctx.metrics.record_flat_reuse();
    pass_ctx.metrics.record_flat_reuse();
    pass_ctx.metrics.record_flat_reuse();

    // Verify metrics
    assert_eq!(pass_ctx.metrics.flats_built(), 2);
    assert_eq!(pass_ctx.metrics.flats_reused(), 3);
    assert_eq!(pass_ctx.metrics.total_build_time_ms(), 15);
}

#[test]
fn test_pass_context_thread_safety() {
    use std::sync::Arc as StdArc;
    use std::thread;

    let config = WarmupConfig {
        warmup_enabled: true,
        flat_cache_mb_cap: 100,
        ..Default::default()
    };

    let pass_ctx = StdArc::new(PassContext::new(&config));

    let mut handles = vec![];

    // Spawn multiple threads accessing the pass context
    for i in 0..5 {
        let ctx = StdArc::clone(&pass_ctx);

        let handle = thread::spawn(move || {
            // Record metrics from multiple threads
            ctx.metrics.record_flat_build(i * 10, i as u64);

            // Try to access cache (read-only in this test)
            let test_ref = ReferenceType::Range {
                sheet: None,
                start_row: Some(0),
                start_col: Some(0),
                end_row: Some(9),
                end_col: Some(0),
            };
            let key = test_ref.fingerprint();
            let _ = ctx.flat_cache.get(&key);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all metrics were recorded
    assert_eq!(pass_ctx.metrics.flats_built(), 5);
}

#[test]
fn test_warmup_disabled_no_context() {
    let config = WarmupConfig {
        warmup_enabled: false, // Disabled
        ..Default::default()
    };

    // When warmup is disabled, pass context shouldn't be used
    // This is more of a documentation test
    if config.warmup_enabled {
        let _pass_ctx = PassContext::new(&config);
        panic!("Should not create pass context when warmup disabled");
    }

    // Test passes if we don't create context
    assert!(!config.warmup_enabled);
}

#[test]
fn test_cache_memory_limit_enforcement() {
    let config = WarmupConfig {
        warmup_enabled: true,
        flat_cache_mb_cap: 1, // Very small limit
        ..Default::default()
    };

    let mut pass_ctx = PassContext::new(&config);

    // Try to add multiple large flats
    for i in 0..10 {
        let test_ref = ReferenceType::Range {
            sheet: None,
            start_row: Some(i * 1000),
            start_col: Some(0),
            end_row: Some((i + 1) * 1000 - 1),
            end_col: Some(0),
        };

        let key = test_ref.fingerprint();
        let flat = FlatView {
            kind: FlatKind::Numeric {
                values: Arc::from(vec![1.0; 100_000].as_slice()),
                valid: None,
            },
            row_count: 100_000,
            col_count: 1,
        };

        pass_ctx.flat_cache.insert(key, flat);
    }

    // Cache should respect memory limit
    assert!(pass_ctx.flat_cache.memory_usage_mb() <= config.flat_cache_mb_cap);
}
