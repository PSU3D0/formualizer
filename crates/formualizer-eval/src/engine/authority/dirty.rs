//! Dirty store (design §4.4): one exact cover per sheet, as per-column row
//! interval sets. Marking inserts rectangles (a family's dirty members are
//! one interval per column, not one entry per cell); the dirty-at-read
//! check (§8.2, M1c) is `O(cols · log m)` per rectangle.
//!
//! In M1a the store is marked from every legacy dirty propagation with the
//! authority's own closure of the same seeds, and cleaned when legacy
//! clears dirty flags after evaluation; M1b plans from it.

use super::geom::{Cell, Cover, Rect};
use super::store::{Store, TagFilter};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DirtyStore {
    cover: Cover,
}

impl DirtyStore {
    pub fn is_empty(&self) -> bool {
        self.cover.is_empty()
    }

    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    pub fn mark_rect(&mut self, sheet: u16, r: &Rect) {
        self.cover.insert_rect(sheet, r);
    }

    /// Mark the transitive dependents of `seeds` (the seeds themselves only
    /// if they lie on a cycle). Returns the closure's cell count.
    pub fn mark_closure(&mut self, store: &Store, seeds: &[(u16, Rect)]) -> u64 {
        let (closure, _) = store.dependents(seeds, TagFilter::All);
        for (s, c, a, b) in closure.column_intervals() {
            self.cover.insert_rect(s, &Rect::new(a, c, b, c));
        }
        closure.cell_count()
    }

    pub fn is_dirty(&self, cell: Cell) -> bool {
        self.cover.contains(cell)
    }

    /// Whether any cell of `r` is dirty (the dirty-at-read query).
    pub fn any_dirty(&self, sheet: u16, r: &Rect) -> bool {
        self.cover.intersects_rect(sheet, r)
    }

    pub fn clean(&mut self, sheet: u16, r: &Rect) {
        self.cover.remove_rect(sheet, r);
    }

    pub fn clear(&mut self) {
        self.cover.clear();
    }

    pub fn cell_count(&self) -> u64 {
        self.cover.cell_count()
    }

    pub fn cells(&self) -> Vec<Cell> {
        self.cover.cells()
    }
}
