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

type Shards = [RwLock<FxHashMap<CellRef, FormatId>>; SHARDS];

/// The shards are allocated on the first format (most engines never have
/// one), so an engine without formats pays one pointer.
#[derive(Debug, Default)]
pub(crate) struct DerivedFormats {
    shards: std::sync::OnceLock<Box<Shards>>,
    len: AtomicUsize,
}

impl DerivedFormats {
    /// Consecutive rows (a family run, split across workers) land in
    /// different shards.
    #[inline]
    fn index(cell: &CellRef) -> usize {
        let h = cell.coord.row() ^ cell.coord.col().rotate_left(7) ^ u32::from(cell.sheet_id);
        h as usize % SHARDS
    }

    #[inline]
    fn shard(&self, cell: &CellRef) -> Option<&RwLock<FxHashMap<CellRef, FormatId>>> {
        self.shards.get().map(|s| &s[Self::index(cell)])
    }

    fn shard_or_init(&self, cell: &CellRef) -> &RwLock<FxHashMap<CellRef, FormatId>> {
        &self
            .shards
            .get_or_init(|| Box::new(std::array::from_fn(|_| RwLock::new(FxHashMap::default()))))
            [Self::index(cell)]
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
        self.shard(cell)?.read().unwrap().get(cell).copied()
    }

    /// Set (`Some`) or clear (`None`) a cell's format. Unchanged entries
    /// take no write lock.
    pub(crate) fn set(&self, cell: CellRef, format: Option<FormatId>) {
        match format {
            Some(format) => {
                let shard = self.shard_or_init(&cell);
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
                let Some(shard) = self.shard(&cell) else {
                    return;
                };
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
        let Some(shards) = self.shards.get() else {
            return;
        };
        if self.is_empty() {
            return;
        }
        for shard in shards.iter() {
            let mut map = shard.write().unwrap();
            let before = map.len();
            map.retain(|cell, _| keep(cell));
            self.len.fetch_sub(before - map.len(), Ordering::AcqRel);
        }
    }

    /// Whether any entry satisfies `pred`.
    pub(crate) fn any(&self, mut pred: impl FnMut(&CellRef) -> bool) -> bool {
        !self.is_empty()
            && self.shards.get().is_some_and(|shards| {
                shards
                    .iter()
                    .any(|shard| shard.read().unwrap().keys().any(&mut pred))
            })
    }
}
