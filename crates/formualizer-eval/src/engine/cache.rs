//! Pass-scoped caches for flattened ranges and criteria masks

use formualizer_common::LiteralValue;
use std::collections::HashMap;
use std::sync::Arc;

/// A view into flattened data (numeric, text, or mixed)
#[derive(Clone)]
pub struct FlatView {
    pub kind: FlatKind,
    pub row_count: usize,
    pub col_count: usize,
}

#[derive(Clone)]
pub enum FlatKind {
    Numeric {
        values: Arc<[f64]>,
        valid: Option<Arc<[bool]>>, // None means all valid
    },
    Text {
        values: Arc<[Arc<str>]>,
        empties: Option<Arc<[bool]>>, // None means no empties
    },
    Mixed {
        values: Arc<[LiteralValue]>,
    },
}

impl FlatView {
    /// Get the length of the flattened data
    pub fn len(&self) -> usize {
        match &self.kind {
            FlatKind::Numeric { values, .. } => values.len(),
            FlatKind::Text { values, .. } => values.len(),
            FlatKind::Mixed { values } => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Key for range flattening cache
pub type RangeKey = String;

/// Pass-scoped cache for flattened ranges
pub struct RangeFlatCache {
    cache: HashMap<String, FlatView>,
    memory_used_bytes: usize,
    memory_cap_bytes: usize,
}

impl RangeFlatCache {
    pub fn new(memory_cap_mb: usize) -> Self {
        Self {
            cache: HashMap::new(),
            memory_used_bytes: 0,
            memory_cap_bytes: memory_cap_mb * 1024 * 1024,
        }
    }

    pub fn get(&self, key: &str) -> Option<FlatView> {
        self.cache.get(key).cloned()
    }

    pub fn insert(&mut self, key: String, flat: FlatView) -> bool {
        let flat_size = Self::estimate_flat_size(&flat);

        // Check memory budget
        if self.memory_used_bytes + flat_size > self.memory_cap_bytes {
            return false;
        }

        self.cache.insert(key, flat);
        self.memory_used_bytes += flat_size;
        true
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.memory_used_bytes = 0;
    }

    pub fn memory_usage_mb(&self) -> usize {
        self.memory_used_bytes / (1024 * 1024)
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    fn estimate_flat_size(flat: &FlatView) -> usize {
        match &flat.kind {
            FlatKind::Numeric { values, valid } => {
                values.len() * std::mem::size_of::<f64>() + valid.as_ref().map_or(0, |v| v.len())
            }
            FlatKind::Text { values, empties } => {
                values.len() * std::mem::size_of::<Arc<str>>()
                    + values.iter().map(|s| s.len()).sum::<usize>()
                    + empties.as_ref().map_or(0, |e| e.len())
            }
            FlatKind::Mixed { values } => values.len() * std::mem::size_of::<LiteralValue>(),
        }
    }
}

use crate::engine::masks::DenseMask;

/// Key for criteria mask cache
pub type CriteriaKey = String;

/// Pass-scoped cache for criteria masks (Phase 3)
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
        if self.cache.len() >= self.entries_cap && !self.cache.contains_key(&key) {
            // Remove first entry (not true LRU, but simple)
            if let Some(first_key) = self.cache.keys().next().cloned() {
                self.cache.remove(&first_key);
            }
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
