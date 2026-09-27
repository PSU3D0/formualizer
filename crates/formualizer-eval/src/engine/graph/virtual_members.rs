//! Program 2 compression (P2-M2): family members without per-cell maps.
//!
//! A compressed family member (`FormulaRef::Member`) whose vertex ids run
//! consecutively down a column is stored here as one [`MemberRun`]
//! instead of one entry in each of the graph's per-cell maps
//! (`cell_to_vertex`, `vertex_formulas`, the sheet interval index). The
//! member keeps its `VertexId` and its vertex-store row (position, kind,
//! flags), so everything keyed by vertex id (dirty and volatile state,
//! values, the legacy oracle's edges, schedules) is unchanged.
//!
//! A run is valid only while none of its members is mutated: every graph
//! mutation of a member *materializes* it first (moves it back into the
//! per-cell maps; see `DependencyGraph::materialize_vertex`), and
//! structural or sheet-wide operations materialize everything. Runs are
//! re-formed after the next authority build (`virtualize_family_members`).
//!
//! Lookups: cell → member by the predecessor run of `(col, row)` in the
//! sheet's ordered map, vertex → member by the predecessor run of the id in
//! the id-ordered map; both O(log runs).

use std::collections::BTreeMap;

use rustc_hash::FxHashMap;

use super::FormulaRef;
use crate::SheetId;
use crate::engine::arena::AstNodeId;
use crate::engine::vertex::VertexId;

/// Members `0..len` at rows `row0 + i` of column `col`, vertex ids
/// `first + i`, formula `template` relocated from `anchor` (0-based).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MemberRun {
    pub(crate) sheet: SheetId,
    pub(crate) col: u32,
    pub(crate) row0: u32,
    pub(crate) len: u32,
    pub(crate) first: u32,
    pub(crate) template: AstNodeId,
    pub(crate) anchor: (u32, u32),
}

impl MemberRun {
    #[inline]
    pub(crate) fn formula(&self) -> FormulaRef {
        FormulaRef::Member {
            template: self.template,
            anchor: self.anchor,
        }
    }

    #[inline]
    pub(crate) fn vertex(&self, i: u32) -> VertexId {
        VertexId(self.first + i)
    }

    /// `(vertex, row)` of every member.
    pub(crate) fn members(&self) -> impl Iterator<Item = (VertexId, u32)> + '_ {
        (0..self.len).map(|i| (VertexId(self.first + i), self.row0 + i))
    }

    #[inline]
    fn end_row(&self) -> u32 {
        self.row0 + self.len - 1
    }
}

/// A member found by cell or by vertex.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VirtualMember {
    pub(crate) vertex: VertexId,
    pub(crate) sheet: SheetId,
    pub(crate) row: u32,
    pub(crate) col: u32,
    pub(crate) formula: FormulaRef,
}

#[derive(Debug, Default)]
pub(crate) struct VirtualMembers {
    /// Run slab; a free slot has `len == 0` and is listed in `free`.
    runs: Vec<MemberRun>,
    free: Vec<u32>,
    /// Per sheet, `(col, row0)` → slot.
    by_cell: FxHashMap<SheetId, BTreeMap<(u32, u32), u32>>,
    /// `first` → slot.
    by_vertex: BTreeMap<u32, u32>,
    members: usize,
}

impl VirtualMembers {
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.members == 0
    }

    /// Number of virtual members.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.members
    }

    pub(crate) fn run_count(&self) -> usize {
        self.by_vertex.len()
    }

    fn slot_of_vertex(&self, v: VertexId) -> Option<u32> {
        if self.members == 0 {
            return None;
        }
        let (_, &slot) = self.by_vertex.range(..=v.0).next_back()?;
        let r = &self.runs[slot as usize];
        (v.0 - r.first < r.len).then_some(slot)
    }

    fn slot_of_cell(&self, sheet: SheetId, row: u32, col: u32) -> Option<u32> {
        if self.members == 0 {
            return None;
        }
        let map = self.by_cell.get(&sheet)?;
        let (&(c, _), &slot) = map.range(..=(col, row)).next_back()?;
        if c != col {
            return None;
        }
        let r = &self.runs[slot as usize];
        (row <= r.end_row()).then_some(slot)
    }

    fn member(r: &MemberRun, i: u32) -> VirtualMember {
        VirtualMember {
            vertex: r.vertex(i),
            sheet: r.sheet,
            row: r.row0 + i,
            col: r.col,
            formula: r.formula(),
        }
    }

    #[inline]
    pub(crate) fn contains_vertex(&self, v: VertexId) -> bool {
        self.slot_of_vertex(v).is_some()
    }

    pub(crate) fn by_vertex(&self, v: VertexId) -> Option<VirtualMember> {
        let slot = self.slot_of_vertex(v)?;
        let r = &self.runs[slot as usize];
        Some(Self::member(r, v.0 - r.first))
    }

    pub(crate) fn by_cell(&self, sheet: SheetId, row: u32, col: u32) -> Option<VirtualMember> {
        let slot = self.slot_of_cell(sheet, row, col)?;
        let r = &self.runs[slot as usize];
        Some(Self::member(r, row - r.row0))
    }

    /// The run holding vertex `v`.
    pub(crate) fn run_of(&self, v: VertexId) -> Option<&MemberRun> {
        self.slot_of_vertex(v).map(|s| &self.runs[s as usize])
    }

    /// Add a run. The caller guarantees its cells and ids are disjoint from
    /// every live run (and that its members are in no per-cell map).
    pub(crate) fn insert(&mut self, run: MemberRun) {
        debug_assert!(run.len > 0);
        debug_assert!(self.slot_of_vertex(VertexId(run.first)).is_none());
        debug_assert!(
            self.slot_of_vertex(VertexId(run.first + run.len - 1))
                .is_none()
        );
        let slot = match self.free.pop() {
            Some(s) => {
                self.runs[s as usize] = run;
                s
            }
            None => {
                self.runs.push(run);
                (self.runs.len() - 1) as u32
            }
        };
        self.by_cell
            .entry(run.sheet)
            .or_default()
            .insert((run.col, run.row0), slot);
        self.by_vertex.insert(run.first, slot);
        self.members += run.len as usize;
    }

    fn remove_slot(&mut self, slot: u32) -> MemberRun {
        let r = self.runs[slot as usize];
        if let Some(map) = self.by_cell.get_mut(&r.sheet) {
            map.remove(&(r.col, r.row0));
            if map.is_empty() {
                self.by_cell.remove(&r.sheet);
            }
        }
        self.by_vertex.remove(&r.first);
        self.runs[slot as usize].len = 0;
        self.free.push(slot);
        self.members -= r.len as usize;
        r
    }

    /// Remove the run starting at vertex `first` (its members leave the
    /// virtual set; the caller re-inserts or restores them).
    pub(crate) fn remove_run(&mut self, first: u32) -> Option<MemberRun> {
        let &slot = self.by_vertex.get(&first)?;
        let r = self.remove_slot(slot);
        if self.members == 0 {
            self.clear();
        }
        Some(r)
    }

    /// Remove member `v`, splitting its run.
    pub(crate) fn take(&mut self, v: VertexId) -> Option<VirtualMember> {
        let slot = self.slot_of_vertex(v)?;
        let r = self.remove_slot(slot);
        let i = v.0 - r.first;
        if i > 0 {
            self.insert(MemberRun { len: i, ..r });
        }
        if i + 1 < r.len {
            self.insert(MemberRun {
                row0: r.row0 + i + 1,
                first: r.first + i + 1,
                len: r.len - i - 1,
                ..r
            });
        }
        if self.members == 0 {
            self.clear();
        }
        Some(Self::member(&r, i))
    }

    /// Remove every run (their members go back to the per-cell maps).
    pub(crate) fn drain(&mut self) -> Vec<MemberRun> {
        let out: Vec<MemberRun> = self.runs().copied().collect();
        self.clear();
        out
    }

    /// Remove the runs of `sheet`.
    pub(crate) fn drain_sheet(&mut self, sheet: SheetId) -> Vec<MemberRun> {
        let Some(map) = self.by_cell.get(&sheet) else {
            return Vec::new();
        };
        let slots: Vec<u32> = map.values().copied().collect();
        let out = slots.into_iter().map(|s| self.remove_slot(s)).collect();
        if self.members == 0 {
            self.clear();
        }
        out
    }

    fn clear(&mut self) {
        self.runs = Vec::new();
        self.free = Vec::new();
        self.by_cell = FxHashMap::default();
        self.by_vertex = BTreeMap::new();
        self.members = 0;
    }

    /// Live runs in vertex-id order.
    pub(crate) fn runs(&self) -> impl Iterator<Item = &MemberRun> + '_ {
        self.by_vertex.values().map(|&s| &self.runs[s as usize])
    }

    /// Live runs of `sheet` in `(col, row)` order.
    pub(crate) fn runs_in_sheet(&self, sheet: SheetId) -> impl Iterator<Item = &MemberRun> + '_ {
        self.by_cell
            .get(&sheet)
            .into_iter()
            .flat_map(|m| m.values().map(|&s| &self.runs[s as usize]))
    }

    /// Live runs of `sheet` in columns `c0..=c1`.
    pub(crate) fn runs_in_cols(
        &self,
        sheet: SheetId,
        c0: u32,
        c1: u32,
    ) -> impl Iterator<Item = &MemberRun> + '_ {
        self.by_cell.get(&sheet).into_iter().flat_map(move |m| {
            m.range((c0, 0)..=(c1, u32::MAX))
                .map(|(_, &s)| &self.runs[s as usize])
        })
    }

    /// Every virtual member.
    pub(crate) fn iter(&self) -> impl Iterator<Item = VirtualMember> + '_ {
        self.runs()
            .flat_map(|r| (0..r.len).map(move |i| Self::member(r, i)))
    }

    pub(crate) fn remap(&mut self, map: &impl Fn(AstNodeId) -> AstNodeId) {
        for s in self.by_vertex.values() {
            let r = &mut self.runs[*s as usize];
            r.template = map(r.template);
        }
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        use crate::engine::authority::dir::hash_table_bytes;
        // BTreeMap nodes hold up to 11 entries; count ~2/3 occupancy.
        let btree = |entries: usize, entry: usize| entries * entry * 3 / 2 + 64;
        self.runs.capacity() * size_of::<MemberRun>()
            + self.free.capacity() * 4
            + hash_table_bytes::<(SheetId, BTreeMap<(u32, u32), u32>)>(self.by_cell.capacity())
            + self
                .by_cell
                .values()
                .map(|m| btree(m.len(), 12))
                .sum::<usize>()
            + btree(self.by_vertex.len(), 8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(col: u32, row0: u32, len: u32, first: u32) -> MemberRun {
        MemberRun {
            sheet: 0,
            col,
            row0,
            len,
            first,
            template: AstNodeId::from_u32(7),
            anchor: (0, col),
        }
    }

    #[test]
    fn lookups_and_splits() {
        let mut m = VirtualMembers::default();
        m.insert(run(2, 10, 5, 100));
        m.insert(run(3, 10, 5, 105));
        assert_eq!(m.len(), 10);
        assert_eq!(m.by_cell(0, 12, 2).unwrap().vertex, VertexId(102));
        assert!(m.by_cell(0, 15, 2).is_none());
        assert!(m.by_cell(0, 9, 2).is_none());
        assert!(m.by_cell(0, 12, 4).is_none());
        assert_eq!(m.by_vertex(VertexId(106)).unwrap().row, 11);
        assert!(m.by_vertex(VertexId(110)).is_none());
        let t = m.take(VertexId(102)).unwrap();
        assert_eq!((t.row, t.col), (12, 2));
        assert_eq!(m.len(), 9);
        assert!(m.by_cell(0, 12, 2).is_none());
        assert_eq!(m.by_cell(0, 13, 2).unwrap().vertex, VertexId(103));
        assert_eq!(m.by_cell(0, 11, 2).unwrap().vertex, VertexId(101));
        assert_eq!(m.run_count(), 3);
        assert_eq!(m.runs_in_cols(0, 3, 3).count(), 1);
        assert_eq!(m.iter().count(), 9);
        let d = m.drain_sheet(0);
        assert_eq!(d.iter().map(|r| r.len).sum::<u32>(), 9);
        assert!(m.is_empty());
    }
}
