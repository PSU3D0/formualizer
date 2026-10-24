//! Inline shared-family validation with deferred source-error precedence.
use super::*;
use crate::xlsx::options::ReadOptions;

type SharedAnchor = (u32, u32, u32, u32);
#[derive(Clone, Copy)]
struct Follower {
    row: u32,
    col: u32,
    order: usize,
}
#[derive(Default)]
pub(super) struct Families {
    dense: Vec<Option<SharedAnchor>>,
    /// Arbitrarily large si remains admitted without a hostile resize.
    sparse: HashMap<u32, SharedAnchor>,
    pending: HashMap<u32, Vec<Follower>>,
    anchor_error: Option<IoError>,
    follower_error: Option<(usize, IoError)>,
}
impl Families {
    fn anchor(&self, id: u32) -> Option<SharedAnchor> {
        self.dense
            .get(id as usize)
            .copied()
            .flatten()
            .or_else(|| self.sparse.get(&id).copied())
    }
    fn insert(&mut self, id: u32, range: SharedAnchor, limit: usize) -> bool {
        if (id as usize) < limit.min(1 << 16) {
            let size = id as usize + 1;
            if self.dense.len() < size {
                self.dense.resize(size, None);
            }
            if let Some(slot) = self.dense.get_mut(id as usize) {
                return slot.replace(range).is_some();
            }
        }
        self.sparse.insert(id, range).is_some()
    }
    fn follower(&mut self, follower: Follower, anchor: SharedAnchor) {
        let (r1, c1, r2, c2) = anchor;
        if (!(r1..=r2).contains(&follower.row) || !(c1..=c2).contains(&follower.col))
            && self
                .follower_error
                .as_ref()
                .is_none_or(|(order, _)| follower.order < *order)
        {
            self.follower_error = Some((
                follower.order,
                unsupported("shared formula outside declared range", "worksheet"),
            ));
        }
    }
    pub(super) fn observe(&mut self, cell: &FormulaMark, order: usize, options: &ReadOptions) {
        let Some(id) = cell.shared_id else { return };
        if cell.formula_text.trim().is_empty() {
            if cell.shared_range.is_some() {
                self.anchor_error.get_or_insert_with(|| {
                    unsupported("shared descendant declares range", "worksheet")
                });
            }
            let follower = Follower {
                row: cell.row,
                col: cell.col,
                order,
            };
            if let Some(anchor) = self.anchor(id) {
                self.follower(follower, anchor);
            } else {
                // Bounded by the already-enforced formula-cell budget.
                self.pending.entry(id).or_default().push(follower);
            }
            return;
        }
        let Some(range) = cell.shared_range else {
            self.anchor_error
                .get_or_insert_with(|| unsupported("unbounded shared formula anchor", "worksheet"));
            return;
        };
        if (cell.row, cell.col) != (range.0, range.1) {
            self.anchor_error.get_or_insert_with(|| {
                unsupported("non-top-left shared formula anchor", "worksheet")
            });
            return;
        }
        let area = u64::from(range.2 - range.0 + 1) * u64::from(range.3 - range.1 + 1);
        if area > options.limits.max_cells as u64
            || self.insert(id, range, options.limits.max_formula_cells)
        {
            self.anchor_error.get_or_insert_with(|| {
                unsupported("duplicate/oversized shared formula anchor", "worksheet")
            });
            return;
        }
        if area > 1
            && let Err(error) = crate::xlsx::recalculate::shared_qualifiers::validate(
                &cell.formula_text,
                &cell.address(),
            )
        {
            self.anchor_error.get_or_insert(error);
        }
        if let Some(pending) = self.pending.remove(&id) {
            for follower in pending {
                self.follower(follower, range);
            }
        }
    }
    /// XML/cell and logical-area errors outrank all family errors. The first
    /// anchor-pass error outranks every follower-pass error, regardless of
    /// where it was discovered. Within each phase retain source order.
    pub(super) fn finish(mut self) -> Result<(), IoError> {
        if let Some(error) = self.anchor_error {
            return Err(error);
        }
        for follower in self.pending.into_values().flatten() {
            if self
                .follower_error
                .as_ref()
                .is_none_or(|(order, _)| follower.order < *order)
            {
                self.follower_error = Some((
                    follower.order,
                    unsupported("orphan shared formula descendant", "worksheet"),
                ));
            }
        }
        self.follower_error.map_or(Ok(()), |(_, error)| Err(error))
    }
}
