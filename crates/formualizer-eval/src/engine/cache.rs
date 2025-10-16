//! Pass-scoped caches for criteria masks (flats removed)

use std::collections::HashMap;

use crate::engine::masks::DenseMask;

/// Key for criteria mask cache
pub type CriteriaKey = String;

/// Pass-scoped cache for criteria masks
pub struct CriteriaMaskCache {
    cache: HashMap<String, DenseMask>,
    entries_cap: usize,
}

impl CriteriaMaskCache {
    pub fn new(entries_cap: usize) -> Self {
        Self {
            cache: HashMap::new(),
            entries_cap,
        }
    }

    pub fn get(&self, key: &str) -> Option<DenseMask> {
        self.cache.get(key).cloned()
    }

    pub fn insert(&mut self, key: String, mask: DenseMask) -> bool {
        // Simple LRU-like behavior: if at capacity, remove oldest
        if self.cache.len() >= self.entries_cap && !self.cache.contains_key(&key)
            && let Some(first_key) = self.cache.keys().next().cloned() {
                self.cache.remove(&first_key);
            }
        self.cache.insert(key, mask);
        true
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}
