//! Spill interfaces (shim) for reserve/write/commit/rollback lifecycle.
//! Phase 2: Introduce types only; implementation can delegate to current graph methods.

use crate::engine::graph::DependencyGraph;
use crate::engine::interval_tree::IntervalTree;
use crate::engine::vertex::VertexId;
use crate::engine::{SpillBufferMode, SpillCancellationPolicy, SpillConfig, SpillVisibility};
use crate::reference::CellRef;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use rustc_hash::{FxHashMap, FxHashSet};

#[derive(Debug, Clone, Copy)]
pub struct SpillShape {
    pub rows: u32,
    pub cols: u32,
}

#[derive(Debug)]
pub struct SpillMeta {
    pub epoch: u64,
    pub config: SpillConfig,
}

#[derive(Debug)]
pub struct SpillReservation {
    pub anchor: CellRef,
    pub shape: SpillShape,
    pub writer_mode: SpillBufferMode,
    pub visibility: SpillVisibility,
    pub cancellation: SpillCancellationPolicy,
}

pub trait SpillWriter {
    fn write_row(&mut self, r: u32, values: &[LiteralValue]) -> Result<(), ExcelError>;
}

pub trait SpillManager {
    fn reserve(
        &mut self,
        graph: &DependencyGraph,
        anchor: CellRef,
        shape: SpillShape,
        meta: SpillMeta,
    ) -> Result<(SpillReservation, Box<dyn SpillWriter>), ExcelError>;

    fn commit(&mut self, reservation: SpillReservation) -> Result<(), ExcelError>;

    fn rollback(&mut self, reservation: SpillReservation);
}

/// Generic, reusable region lock manager per sheet.
#[derive(Default, Debug)]
pub struct RegionLockManager {
    next_id: u64,
    // Per-sheet interval indexes for rows and cols
    row_trees: FxHashMap<u32, IntervalTree<u64>>, // sheet_id -> row intervals → lock ids
    col_trees: FxHashMap<u32, IntervalTree<u64>>, // sheet_id -> col intervals → lock ids
    locks: FxHashMap<u64, RegionLock>,            // id -> lock
}

#[derive(Debug, Clone, Copy)]
pub struct Region {
    pub sheet_id: u32,
    pub row_start: u32,
    pub row_end: u32,
    pub col_start: u32,
    pub col_end: u32,
}

#[derive(Debug)]
struct RegionLock {
    id: u64,
    owner: VertexId,
    region: Region,
}

impl RegionLockManager {
    pub fn reserve(&mut self, region: Region, owner: VertexId) -> Result<u64, ExcelError> {
        // Fast path: zero-size or invalid region is treated as no-op
        if region.row_end < region.row_start || region.col_end < region.col_start {
            return Ok(0);
        }
        let row_tree = self.row_trees.entry(region.sheet_id).or_default();
        let col_tree = self.col_trees.entry(region.sheet_id).or_default();

        // Gather overlapping row locks
        let mut row_ids: FxHashSet<u64> = FxHashSet::default();
        for (_low, _high, set) in row_tree.query(region.row_start, region.row_end) {
            for id in set {
                row_ids.insert(id);
            }
        }
        if !row_ids.is_empty() {
            let mut col_ids: FxHashSet<u64> = FxHashSet::default();
            for (_low, _high, set) in col_tree.query(region.col_start, region.col_end) {
                for id in set {
                    col_ids.insert(id);
                }
            }
            // Intersect
            let conflict = row_ids.iter().any(|id| col_ids.contains(id));
            if conflict {
                return Err(ExcelError::new(ExcelErrorKind::Spill)
                    .with_message("Region reserved by another spill"));
            }
        }

        // Allocate lock id and insert
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let id = self.next_id;
        row_tree.insert(region.row_start, region.row_end, id);
        col_tree.insert(region.col_start, region.col_end, id);
        self.locks.insert(id, RegionLock { id, owner, region });
        Ok(id)
    }

    pub fn release(&mut self, id: u64) {
        if id == 0 {
            return;
        }
        if let Some(lock) = self.locks.remove(&id) {
            if let Some(tree) = self.row_trees.get_mut(&lock.region.sheet_id) {
                tree.remove(lock.region.row_start, lock.region.row_end, &lock.id);
            }
            if let Some(tree) = self.col_trees.get_mut(&lock.region.sheet_id) {
                tree.remove(lock.region.col_start, lock.region.col_end, &lock.id);
            }
        }
    }

    #[cfg(test)]
    pub fn active_count(&self, sheet_id: u32) -> usize {
        let r = self.row_trees.get(&sheet_id).map(|t| t.len()).unwrap_or(0);
        let c = self.col_trees.get(&sheet_id).map(|t| t.len()).unwrap_or(0);
        r.max(c)
    }
}
