//! Dirty store (design §4.4): one exact cover per sheet, as per-column row
//! interval sets. Marking inserts rectangles (a family's dirty members are
//! one interval per column, not one entry per cell); the dirty-at-read
//! check (§8.2, M1c) is `O(cols · log m)` per rectangle.
//!
//! In M1a the store is marked from every legacy dirty propagation with the
//! authority's own propagation of the same seeds (formula seeds and their
//! closure), and cleaned when legacy clears dirty flags after evaluation;
//! M1b plans from it.

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

    /// What a dirty propagation from `seeds` dirties, as legacy's
    /// `mark_dirty_many` does: every formula cell among the seeds, and the
    /// transitive dependents (positive length) of all seeds. Value and empty
    /// seeds are not dirtied themselves. Marks the cover and returns what
    /// this call marked.
    pub fn mark_propagation(&mut self, store: &Store, seeds: &[(u16, Rect)]) -> Cover {
        let (mut marked, _) = store.dependents(seeds, TagFilter::All);
        let mut runs = Vec::new();
        for &(s, r) in seeds {
            for col in r.c0..=r.c1 {
                runs.clear();
                store.ids().runs_in(s, col, r.r0, r.r1, &mut runs);
                for &h in &runs {
                    let run = store.ids().run(h);
                    let a = run.row_start.max(r.r0);
                    let b = (run.row_start + run.len - 1).min(r.r1);
                    marked.insert_rect(s, &Rect::new(a, col, b, col));
                }
            }
        }
        for (s, c, a, b) in marked.column_intervals() {
            self.cover.insert_rect(s, &Rect::new(a, c, b, c));
        }
        marked
    }

    /// Mark the transitive dependents of `seeds` (the seeds themselves only
    /// if they lie on a cycle): relation closure, not dirty propagation
    /// (see [`Self::mark_propagation`]). Returns the closure's cell count.
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
