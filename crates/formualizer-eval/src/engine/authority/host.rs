//! Engine wiring (feature `unified_authority`, M1a).
//!
//! The authority lives inside `DependencyGraph` beside the legacy graph,
//! which stays the runtime evaluation path until M1b. The graph's formula
//! map records every vertex whose formula changes; `authority_sync` turns
//! those into authority mutations (set formula / clear) and rebuilds from
//! scratch at load and when a symbol (name, table, sheet) changes, because
//! symbol rebinding is M4. Structural edits and sheet rename/removal are M3:
//! they put the host into a typed "unsupported under unified_authority"
//! state, and every query answers that error instead of a stale relation.
//! FormulaPlane spans are M2: while spans exist the same state applies.

use super::dirty::DirtyStore;
use super::store::{AuthorityError, Store};
use crate::engine::VertexId;
use rustc_hash::FxHashMap;

/// An observed read `(sheet, r0, c0, r1, c1)`, 0-based inclusive.
pub type ObservedRect = (u16, u32, u32, u32, u32);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum HostState {
    /// Not built yet (a load is in progress or nothing was queried).
    #[default]
    Unbuilt,
    Ready,
    /// A mutation outside M1a scope happened; queries fail with this error.
    Failed(AuthorityError),
}

/// Counters for the differential self-check (gates).
#[derive(Clone, Debug, Default)]
pub struct DiffCounters {
    pub propagations: u64,
    pub checked_seeds: u64,
    /// Dirty-propagation mismatches against legacy's actual dirty set
    /// (Δ(a)).
    pub closure_mismatches: u64,
    /// Propagations where legacy's bounding-rectangle value path dirtied
    /// more than the authority (a strict superset; not a mismatch).
    pub closure_conservative: u64,
    pub direct_mismatches: u64,
    pub skipped: u64,
}

#[derive(Debug, Default)]
pub struct AuthorityHost {
    pub(crate) store: Store,
    pub(crate) state: HostState,
    /// Symbol revision the store was built against.
    pub(crate) symbol_rev: u64,
    /// Dirty cover (design §4.4): marked by every legacy propagation with
    /// the authority's propagation of the same seeds.
    pub(crate) dirty: DirtyStore,
    pub(crate) builds: u64,
    /// Bumped by every change to `store` (build, incremental mutation):
    /// the schedule-cache key (design §8.4).
    pub(crate) revision: u64,
    pub(crate) incremental_mutations: u64,
    pub(crate) diff: DiffCounters,
    /// Symbol nodes (design §4.1): every defined name is one node on the
    /// symbol plane (`geom::SYMBOL_SHEET`, row = slot, column 0), with its
    /// references as precedents and its readers as dependents. The plane is
    /// planned with the cells, so a name's vertex is scheduled as a unit
    /// between its precedents and its readers.
    pub(crate) symbols: SymbolSlots,
    /// Authority formula id → executor vertex (`u32::MAX` = unknown), so
    /// the Schedule adapter and demand walk translate ordered cells without
    /// a per-cell hash lookup. Filled at every build and incremental
    /// `set_formula`; ids are never reused (decision 9), and readers verify
    /// the vertex still sits at the cell, falling back to the hash map.
    pub(crate) vertex_of_id: Vec<u32>,
    /// `rdi_dyn` (design §8.2, OR1): for each dynamic reader with a
    /// published, fresh value, the rectangles its last evaluation read,
    /// `(sheet, r0, c0, r1, c1)`. Plans order it after them; demand walks
    /// them. Dropped when the formula changes and on every rebuild.
    pub(crate) observed: FxHashMap<VertexId, Vec<ObservedRect>>,
    /// Bumped whenever some reader's observed set changes: the schedule
    /// cache key's `rev.dyn` (design §8.4).
    pub(crate) rev_dyn: u64,
}

/// Name vertex ↔ symbol-plane row. Assigned at symbol-revision rebuilds;
/// a surviving name keeps its row (and so its authority id).
#[derive(Debug, Default)]
pub struct SymbolSlots {
    slot_of: FxHashMap<VertexId, u32>,
    vertex_of: Vec<Option<VertexId>>,
    free: Vec<u32>,
}

impl SymbolSlots {
    pub fn slot(&self, vertex: VertexId) -> Option<u32> {
        self.slot_of.get(&vertex).copied()
    }

    pub fn vertex(&self, slot: u32) -> Option<VertexId> {
        self.vertex_of.get(slot as usize).copied().flatten()
    }

    pub fn len(&self) -> usize {
        self.slot_of.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slot_of.is_empty()
    }

    /// Live `(slot, vertex)` pairs in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (u32, VertexId)> + '_ {
        self.vertex_of
            .iter()
            .enumerate()
            .filter_map(|(s, v)| v.map(|v| (s as u32, v)))
    }

    /// Keep the rows of names still in `live` (sorted), retire the rest and
    /// give new names the lowest free rows. Returns the retired rows.
    pub fn sync(&mut self, live: &[VertexId]) -> Vec<u32> {
        let mut retired = Vec::new();
        for (slot, entry) in self.vertex_of.iter_mut().enumerate() {
            if let Some(v) = *entry
                && live.binary_search(&v).is_err()
            {
                *entry = None;
                self.slot_of.remove(&v);
                retired.push(slot as u32);
            }
        }
        self.free.extend(retired.iter().copied());
        // Lowest rows first, so the plane stays dense.
        self.free.sort_unstable_by(|a, b| b.cmp(a));
        for &v in live {
            if self.slot_of.contains_key(&v) {
                continue;
            }
            let slot = self.free.pop().unwrap_or_else(|| {
                self.vertex_of.push(None);
                (self.vertex_of.len() - 1) as u32
            });
            self.vertex_of[slot as usize] = Some(v);
            self.slot_of.insert(v, slot);
        }
        retired
    }

    pub fn heap_bytes(&self) -> usize {
        super::dir::hash_table_bytes::<(VertexId, u32)>(self.slot_of.capacity())
            + self.vertex_of.capacity() * size_of::<Option<VertexId>>()
            + self.free.capacity() * size_of::<u32>()
    }
}

impl AuthorityHost {
    pub fn state(&self) -> &HostState {
        &self.state
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn dirty(&self) -> &DirtyStore {
        &self.dirty
    }

    pub fn builds(&self) -> u64 {
        self.builds
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn rev_dyn(&self) -> u64 {
        self.rev_dyn
    }

    /// The observed reads of dynamic reader `reader`, if recorded.
    pub fn observed(&self, reader: VertexId) -> Option<&[ObservedRect]> {
        self.observed.get(&reader).map(Vec::as_slice)
    }

    /// Record `reader`'s reads from a fresh commit (normalized).
    pub(crate) fn set_observed(&mut self, reader: VertexId, mut reads: Vec<ObservedRect>) {
        reads.sort_unstable();
        reads.dedup();
        if self.observed.get(&reader) != Some(&reads) {
            self.observed.insert(reader, reads);
            self.rev_dyn += 1;
        }
    }

    pub(crate) fn forget_observed(&mut self, reader: VertexId) {
        if self.observed.remove(&reader).is_some() {
            self.rev_dyn += 1;
        }
    }

    pub(crate) fn clear_observed(&mut self) {
        if !self.observed.is_empty() {
            self.observed.clear();
            self.rev_dyn += 1;
        }
    }

    /// The executor vertex recorded for authority formula id `id`.
    #[inline]
    pub fn vertex_of_id(&self, id: u32) -> Option<VertexId> {
        match self.vertex_of_id.get(id as usize) {
            Some(&v) if v != u32::MAX => Some(VertexId(v)),
            _ => None,
        }
    }

    pub(crate) fn set_vertex_of_id(&mut self, id: u32, vertex: VertexId) {
        let i = id as usize;
        if i >= self.vertex_of_id.len() {
            self.vertex_of_id.resize(i + 1, u32::MAX);
        }
        self.vertex_of_id[i] = vertex.0;
    }

    pub fn vertex_of_id_bytes(&self) -> usize {
        self.vertex_of_id.capacity() * size_of::<u32>()
    }

    pub fn incremental_mutations(&self) -> u64 {
        self.incremental_mutations
    }

    pub fn diff_counters(&self) -> &DiffCounters {
        &self.diff
    }

    pub fn symbols(&self) -> &SymbolSlots {
        &self.symbols
    }
}
