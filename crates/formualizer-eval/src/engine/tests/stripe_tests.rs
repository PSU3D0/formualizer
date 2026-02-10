//! Tests for stripe key hashing and evaluation configuration

use crate::engine::{EvalConfig, StripeKey, StripeType, block_index};
use rustc_hash::FxHashSet;

#[test]
fn test_stripe_key_hashing_and_equality() {
    let key1 = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 10,
    };

    let key2 = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 10,
    };

    let key3 = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 10,
    };

    // Test equality
    assert_eq!(key1, key2);
    assert_ne!(key1, key3);

    // Test hashing by using as HashMap keys
    let mut set = FxHashSet::default();
    set.insert(key1.clone());
    set.insert(key2.clone());
    set.insert(key3.clone());

    // Should have 2 unique keys
    assert_eq!(set.len(), 2);
    assert!(set.contains(&key1));
    assert!(set.contains(&key3));
}

#[test]
fn test_stripe_type_equality() {
    assert_eq!(StripeType::Row, StripeType::Row);
    assert_eq!(StripeType::Column, StripeType::Column);
    assert_eq!(StripeType::Block, StripeType::Block);

    assert_ne!(StripeType::Row, StripeType::Column);
    assert_ne!(StripeType::Row, StripeType::Block);
    assert_ne!(StripeType::Column, StripeType::Block);
}

#[test]
fn test_block_index_calculation() {
    // Test block index calculation with BLOCK_H = BLOCK_W = 256

    // Top-left corner of first block
    assert_eq!(block_index(0, 0), 0);
    assert_eq!(block_index(1, 1), 0);
    assert_eq!(block_index(255, 255), 0);

    // First block in next row
    assert_eq!(block_index(256, 0), 1 << 16);
    assert_eq!(block_index(256, 255), 1 << 16);

    // First block in next column
    assert_eq!(block_index(0, 256), 1);
    assert_eq!(block_index(255, 256), 1);

    // Block at row 256, col 256
    assert_eq!(block_index(256, 256), (1 << 16) | 1);

    // Test larger coordinates
    assert_eq!(block_index(512, 512), (2 << 16) | 2);
    assert_eq!(block_index(1024, 768), (4 << 16) | 3);
}

#[test]
fn test_eval_config_range_thresholds() {
    // Test default configuration
    let config = EvalConfig::default();
    assert_eq!(config.range_expansion_limit, 64);
    assert_eq!(config.stripe_height, 256);
    assert_eq!(config.stripe_width, 256);
    assert!(!config.enable_block_stripes);

    // Test custom configuration
    let custom_config = EvalConfig {
        enable_parallel: false,
        range_expansion_limit: 32,
        stripe_height: 128,
        stripe_width: 128,
        enable_block_stripes: true,
        ..Default::default()
    };

    assert_eq!(custom_config.range_expansion_limit, 32);
    assert_eq!(custom_config.stripe_height, 128);
    assert_eq!(custom_config.stripe_width, 128);
    assert!(custom_config.enable_block_stripes);
    assert!(!custom_config.enable_parallel);
}

#[test]
fn test_stripe_key_different_sheets() {
    let key1 = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 10,
    };

    let key2 = StripeKey {
        sheet_id: 1,
        stripe_type: StripeType::Row,
        index: 10,
    };

    assert_ne!(key1, key2);

    let mut set = FxHashSet::default();
    set.insert(key1);
    set.insert(key2);
    assert_eq!(set.len(), 2);
}

#[test]
fn test_stripe_key_different_indices() {
    let key1 = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 10,
    };

    let key2 = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 20,
    };

    assert_ne!(key1, key2);

    let mut set = FxHashSet::default();
    set.insert(key1);
    set.insert(key2);
    assert_eq!(set.len(), 2);
}
