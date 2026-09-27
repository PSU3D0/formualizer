//! Derived number formats of formula results (`CellRef` -> format), read on
//! every cell read and written on every formula result. Parallel layer
//! evaluation hammers it from every worker: one `RwLock` over one map made
//! its lock word a contention point (futex waits in first-eval profiles of
//! date-heavy workbooks). Sharded by cell, with an entry count that lets the
//! common no-formats case skip locking altogether.

use crate::format::FormatId;
use crate::reference::CellRef;
use rustc_hash::FxHashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

const SHARDS: usize = 16;

#[derive(Debug)]
pub(crate) struct DerivedFormats {
    shards: [RwLock<FxHashMap<CellRef, FormatId>>; SHARDS],
    len: AtomicUsize,
}

impl Default for DerivedFormats {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(FxHashMap::default())),
            len: AtomicUsize::new(0),
        }
    }
}

impl DerivedFormats {
    /// Consecutive rows (a family run, split across workers) land in
    /// different shards.
    #[inline]
    fn shard(&self, cell: &CellRef) -> &RwLock<FxHashMap<CellRef, FormatId>> {
        let h = cell.coord.row() ^ cell.coord.col().rotate_left(7) ^ u32::from(cell.sheet_id);
        &self.shards[h as usize % SHARDS]
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    #[inline]
    pub(crate) fn get(&self, cell: &CellRef) -> Option<FormatId> {
        if self.is_empty() {
            return None;
        }
        self.shard(cell).read().unwrap().get(cell).copied()
    }

    /// Set (`Some`) or clear (`None`) a cell's format. Unchanged entries
    /// take no write lock.
    pub(crate) fn set(&self, cell: CellRef, format: Option<FormatId>) {
        match format {
            Some(format) => {
                let shard = self.shard(&cell);
                if shard.read().unwrap().get(&cell) == Some(&format) {
                    return;
                }
                if shard.write().unwrap().insert(cell, format).is_none() {
                    self.len.fetch_add(1, Ordering::AcqRel);
                }
            }
            None => {
                if self.is_empty() {
                    return;
                }
                let shard = self.shard(&cell);
                if !shard.read().unwrap().contains_key(&cell) {
                    return;
                }
                if shard.write().unwrap().remove(&cell).is_some() {
                    self.len.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }

    /// Keep the entries `keep` accepts.
    pub(crate) fn retain(&self, mut keep: impl FnMut(&CellRef) -> bool) {
        if self.is_empty() {
            return;
        }
        for shard in &self.shards {
            let mut map = shard.write().unwrap();
            let before = map.len();
            map.retain(|cell, _| keep(cell));
            self.len.fetch_sub(before - map.len(), Ordering::AcqRel);
        }
    }

    /// Whether any entry satisfies `pred`.
    pub(crate) fn any(&self, mut pred: impl FnMut(&CellRef) -> bool) -> bool {
        !self.is_empty()
            && self
                .shards
                .iter()
                .any(|shard| shard.read().unwrap().keys().any(&mut pred))
    }
}
