//! Engine wiring (feature `unified_authority`, M1a).
//!
//! The authority lives inside `DependencyGraph` beside the legacy graph,
//! which stays the runtime evaluation path until M1b. The graph's formula
//! map records every vertex whose formula changes; `authority_sync` turns
//! those into authority mutations (set formula / clear) and rebuilds from
//! scratch at the first request after load. Name, table and source
//! revisions are applied incrementally, in work proportional to the changed
//! symbols and their readers (their rows, binding identities, symbol nodes,
//! and the direct readers of the changed nodes). Structural edits, moves and sheet operations (M3)
//! make the next sync a rebuild from the already-transformed formulas; ids
//! are the executor's vertex ids, which the graph keeps (`history`). FormulaPlane spans
//! are M2: while spans exist the host is in a typed "unsupported under
//! unified_authority" state, and every query answers that error instead of
//! a stale relation.

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
    /// A structural edit, move or sheet operation mutated the graph since
    /// the last sync: the next sync rebuilds (M3). Identities need no
    /// capture: a cell's id is its executor vertex's, which the graph
    /// keeps when it moves the cell (Program 2).
    pub(crate) structural_pending: bool,
    /// An undo/redo replay is in progress (its per-cell moves stay batched
    /// until it ends).
    pub(crate) replaying: bool,
    pub(crate) structural_rebuilds: u64,
    /// Program 2 compression, sequential engines only: formula vertices with
    /// a reference text that is not its reference's rendering, recorded when
    /// the authority reads the formula. (With a thread pool the check runs
    /// in compression's parallel decision instead: off the load path.)
    pub(crate) texts_unrendered: std::sync::Mutex<rustc_hash::FxHashSet<VertexId>>,
    /// `rdi_dyn` (design §8.2, OR1): for each dynamic reader with a
    /// published, fresh value, the rectangles its last evaluation read,
    /// `(sheet, r0, c0, r1, c1)`. Plans order it after them; demand walks
    /// them. Dropped when the formula changes and on every rebuild.
    pub(crate) observed: FxHashMap<VertexId, Vec<ObservedRect>>,
    /// Bumped whenever some reader's observed set changes: the schedule
    /// cache key's `rev.dyn` (design §8.4).
    pub(crate) rev_dyn: u64,
    /// Symbol definitions changed since the last sync (names by vertex;
    /// `other` for tables and sources). A sync applies name changes
    /// incrementally; anything else, or an unlogged symbol revision,
    /// rebuilds.
    pub(crate) symbol_changes: SymbolChanges,
    /// Symbol revisions applied without a rebuild (tests, perf probes).
    pub(crate) symbol_incremental: u64,
    /// Work of the incremental symbol syncs (rows assigned or retired,
    /// binding entries, readers and nodes visited): the scaling gates'
    /// counter.
    pub(crate) symbol_sync_work: u64,
    /// The symbol revision the operation that logged `symbol_changes` will
    /// bump to (`authority_note_symbol`): a sync that applies the logged
    /// changes before the bump is current with that revision.
    pub(crate) symbol_rev_target: Option<u64>,
    /// Name nodes whose definition names a symbol (name, table, source,
    /// sheet) that is missing or has no node, by binding key: re-derived
    /// when a symbol with that key appears. Entries may be stale (a node
    /// since redefined or deleted); the re-derivation checks.
    pub(crate) unbound: FxHashMap<Box<str>, rustc_hash::FxHashSet<VertexId>>,
    /// Dirty-propagation seeds waiting for the store to catch up (marked
    /// mid structural edit, when the store is still pre-edit); their
    /// closure is marked at the end of the next sync.
    pub(crate) pending_dirty: Vec<VertexId>,
    /// The same for cells without a vertex (value edits, decision 27).
    pub(crate) pending_dirty_rects: Vec<(u16, crate::engine::authority::geom::Rect)>,
    /// `mark_dependents_dirty` vertices waiting for the resync (their direct
    /// in-edge readers are flagged, not propagated).
    pub(crate) pending_direct_dirty: Vec<VertexId>,
    /// The same for virtual member runs a row/column edit moved as one
    /// (Program 2): `(sheet, col, r0, r1)` after the edit, every cell of
    /// which is a moved member.
    pub(crate) pending_direct_dirty_runs: Vec<(crate::SheetId, u32, u32, u32)>,
}

/// See [`AuthorityHost::symbol_changes`].
#[derive(Debug, Default)]
pub(crate) struct SymbolChanges {
    pub(crate) names: Vec<VertexId>,
    pub(crate) other: bool,
}

/// Name vertex ↔ symbol-plane row. Assigned at symbol-revision rebuilds;
/// a surviving name keeps its row (and so its authority id).
#[derive(Debug, Default)]
pub struct SymbolSlots {
    slot_of: FxHashMap<VertexId, u32>,
    vertex_of: Vec<Option<VertexId>>,
    /// Retired rows, lowest first.
    free: std::collections::BinaryHeap<std::cmp::Reverse<u32>>,
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
        self.free
            .extend(retired.iter().map(|&s| std::cmp::Reverse(s)));
        for &v in live {
            if !self.slot_of.contains_key(&v) {
                self.insert(v);
            }
        }
        retired
    }

    /// Give `vertex` a row (the lowest free one, so the plane stays dense);
    /// a vertex that has one keeps it. O(log free).
    pub fn insert(&mut self, vertex: VertexId) -> u32 {
        if let Some(&slot) = self.slot_of.get(&vertex) {
            return slot;
        }
        let slot = match self.free.pop() {
            Some(std::cmp::Reverse(slot)) => slot,
            None => {
                self.vertex_of.push(None);
                (self.vertex_of.len() - 1) as u32
            }
        };
        self.vertex_of[slot as usize] = Some(vertex);
        self.slot_of.insert(vertex, slot);
        slot
    }

    /// Retire `vertex`'s row, if it has one. O(log free).
    pub fn remove(&mut self, vertex: VertexId) -> Option<u32> {
        let slot = self.slot_of.remove(&vertex)?;
        self.vertex_of[slot as usize] = None;
        self.free.push(std::cmp::Reverse(slot));
        Some(slot)
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
    /// Whether any dynamic reader has recorded observed reads.
    pub fn has_observed(&self) -> bool {
        !self.observed.is_empty()
    }

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

    pub fn incremental_mutations(&self) -> u64 {
        self.incremental_mutations
    }

    pub fn diff_counters(&self) -> &DiffCounters {
        &self.diff
    }

    pub fn symbols(&self) -> &SymbolSlots {
        &self.symbols
    }

    pub fn symbol_incremental(&self) -> u64 {
        self.symbol_incremental
    }

    pub fn symbol_sync_work(&self) -> u64 {
        self.symbol_sync_work
    }

    /// Record that name node `vertex` waits on the symbols `keys`.
    pub(crate) fn note_unbound(&mut self, vertex: VertexId, keys: Vec<Box<str>>) {
        for key in keys {
            self.unbound.entry(key).or_default().insert(vertex);
        }
    }

    pub fn structural_rebuilds(&self) -> u64 {
        self.structural_rebuilds
    }
}
