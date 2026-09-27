//! The maintained authority structure (design §4.1–§4.3, §5.1, §5.6, §5.7):
//! identity runs, owners (family nodes and singletons) grouped by L, literal
//! slot rows, and edge records grouped by `(dependent sheet, projection,
//! relation tag, lookup-slot owner)`, with three per-sheet level indexes.
//!
//! Every non-structural mutation (set formula, clear cells) runs as one
//! **mutation scope**: an exact dry run of every container it will touch
//! (§5.1), admission against the budget (§5.6), exact reservation of every
//! container, then an infallible apply. A rejected mutation leaves the
//! state unchanged. After the apply, groups whose trigger fired are
//! repartitioned to `canon` (§5.7) under byte-safe acceptance: a
//! repartition commits only if the live model bytes do not grow and its
//! retained capacity and transient peak are admitted against the budget.
//!
//! Mutations never merge in place: a refill creates a singleton record and
//! owner, and the group's trigger re-forms families. That keeps the dry run
//! exact (SP-2 F-4 is removed by construction, addendum B-4).
//!
//! M1a correction (addendum B-15…B-20): retained bytes and index entry
//! counts are maintained incrementally from the touched containers, so a
//! mutation's accounting is independent of the store size (B2; the full
//! census is `census_heap_bytes`/`census_counts`, checked by `check`);
//! empty groups and slot pages are reclaimed (B3); every allocation of a
//! scope, planning scratch included, is fallible and happens before the
//! first logical change, and the apply allocates nothing (B5).

use super::avl::AvlMap;
use super::canon::{self, CanonWork};
use super::dir::{DirPlan, Directory};
use super::geom::{Cell, Cover, Rect, SYMBOL_SHEET, sheet_slot};
pub use super::groups::Group;
use super::groups::{
    GroupKey, GroupPlan, GroupTable, Members, l_hash, members_cap_after, members_heap,
    members_heap_for,
};
use super::identity::{
    CellCut, FAMILY, HOST_SYMBOL_ID_BASE, IdError, IdRun, IdShadow, IdentityTable, Vid,
};
use super::level_index::{IndexShadow, IndexStage, LevelIndex, NONE};
use super::proj::RefProj;
use super::slots::SlotStore;
use super::symbols::{SymbolId, SymbolTable};
use crate::engine::arena::{AstNodeId, ValueRef};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// Relation tag (design §4.3): `R1` answers R-1 verbatim; `X` is the
/// extended relation R-1X (formula-defined names, tables, …).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Tag {
    R1,
    X,
}

/// Which edges a query traverses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagFilter {
    R1Only,
    All,
}

impl TagFilter {
    fn admits(self, t: Tag) -> bool {
        matches!(self, TagFilter::All) || t == Tag::R1
    }
}

/// Lookup slot key (§7.2.5): the lookup context sheet and the symbol text
/// as the reference spells it (case-folded when the engine folds case).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LkKey {
    pub ctx: u16,
    pub kind: u8,
    pub name: Box<str>,
}

/// Where an edge comes from: formula text, or a symbol through its LK.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OriginSpec {
    Text,
    Symbol(LkKey),
}

/// One extracted reference of a formula.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeSpec {
    pub proj: RefProj,
    pub tag: Tag,
    pub origin: OriginSpec,
}

pub const F_VOLATILE: u16 = 1;
pub const F_DYNAMIC: u16 = 2;
/// The formula has references outside the static relation (unresolved
/// names, 3-D, external sources, spill extents): R-1 "opaque".
pub const F_OPAQUE: u16 = 4;

/// Everything the authority stores about one formula cell.
#[derive(Clone, Debug)]
pub struct FormulaFacts {
    pub edges: Vec<EdgeSpec>,
    /// The L token stream; `None` for non-relocatable templates.
    pub ltokens: Option<Box<[u64]>>,
    pub template: AstNodeId,
    /// The cell `template` is valid at, when it is not the formula's own
    /// cell (a family member's facts are its template's at the anchor).
    pub template_anchor: Option<(u32, u32)>,
    pub literals: SmallVec<[ValueRef; 4]>,
    pub flags: u16,
}

impl FormulaFacts {
    /// Heap bytes the facts own: the edge list, symbol names, the L token
    /// stream and spilled literals (build scratch, re-review R5).
    pub fn owned_heap_bytes(&self) -> usize {
        self.edges.capacity() * size_of::<EdgeSpec>()
            + self
                .edges
                .iter()
                .map(|e| match &e.origin {
                    OriginSpec::Symbol(k) => k.name.len(),
                    OriginSpec::Text => 0,
                })
                .sum::<usize>()
            + self
                .ltokens
                .as_ref()
                .map_or(0, |t| t.len() * size_of::<u64>())
            + if self.literals.spilled() {
                self.literals.capacity() * size_of::<ValueRef>()
            } else {
                0
            }
    }
}

/// Interned edge-group key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EdgeKey {
    pub dep_sheet: u16,
    pub tag: Tag,
    /// `NO_LK` for text-derived edges.
    pub lk: u32,
    pub proj: RefProj,
}

/// Bound packed in 32 bits: top two bits 0 = open, 1 = absolute, 2 =
/// relative (offset biased by 2^29; grid offsets are below 2^21).
fn pack_bound(b: super::proj::Bound) -> u32 {
    use super::proj::Bound;
    match b {
        Bound::Open => 0,
        Bound::Abs(v) => (1 << 30) | v,
        Bound::Rel(k) => (2 << 30) | (k + (1 << 29)) as u32,
    }
}

fn unpack_bound(x: u32) -> super::proj::Bound {
    use super::proj::Bound;
    match x >> 30 {
        0 => Bound::Open,
        1 => Bound::Abs(x & ((1 << 30) - 1)),
        _ => Bound::Rel((x & ((1 << 30) - 1)) as i32 - (1 << 29)),
    }
}

/// `EdgeKey` as stored: 28 bytes instead of 48.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PackedEdgeKey {
    b: [u32; 4],
    lk: u32,
    target: u16,
    dep_sheet: u16,
    tag: Tag,
}

impl GroupKey for EdgeKey {
    type Packed = PackedEdgeKey;
    fn pack(&self) -> PackedEdgeKey {
        let p = &self.proj;
        PackedEdgeKey {
            b: [
                pack_bound(p.rows.lo),
                pack_bound(p.rows.hi),
                pack_bound(p.cols.lo),
                pack_bound(p.cols.hi),
            ],
            lk: self.lk,
            target: p.sheet,
            dep_sheet: self.dep_sheet,
            tag: self.tag,
        }
    }
    fn unpack(p: &PackedEdgeKey) -> EdgeKey {
        use super::proj::AxisMap;
        EdgeKey {
            dep_sheet: p.dep_sheet,
            tag: p.tag,
            lk: p.lk,
            proj: RefProj {
                sheet: p.target,
                rows: AxisMap {
                    lo: unpack_bound(p.b[0]),
                    hi: unpack_bound(p.b[1]),
                },
                cols: AxisMap {
                    lo: unpack_bound(p.b[2]),
                    hi: unpack_bound(p.b[3]),
                },
            },
        }
    }
}

pub const NO_LK: u32 = u32::MAX;
const UNGROUPED: u32 = u32::MAX - 1;
const DEAD: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
struct Rec {
    dep: Rect,
    /// Edge group; `DEAD` when the slot is free.
    group: u32,
    /// Position in the group's member list; next free slot when dead.
    pos: u32,
}

#[derive(Clone, Copy, Debug)]
struct Owner {
    dom: Rect,
    sheet: u16,
    flags: u16,
    /// Node group, `UNGROUPED`, or `DEAD` when the slot is free.
    group: u32,
    pos: u32,
    /// A formula valid at `anchor`; member `x` is `template` relocated by
    /// `x - anchor` with `x`'s slot row.
    template: AstNodeId,
    anchor: (u32, u32),
}

impl Owner {
    fn is_family(&self) -> bool {
        !self.dom.is_cell()
    }
}

/// Resource budget for the authority (bytes).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Budget {
    /// Limit on retained heap (capacity) bytes.
    pub retained: Option<u64>,
    /// Limit on transient bytes above the retained size.
    pub scratch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorityError {
    /// Budget rejection; nothing was applied.
    Admission {
        resource: &'static str,
        needed: u64,
        limit: u64,
    },
    Identity(IdError),
    /// An allocation could not be reserved; nothing was applied.
    Alloc,
    /// The operation is not supported under `unified_authority` yet.
    Unsupported {
        operation: &'static str,
    },
    /// A read-only planning path found graph changes the host has not
    /// synced yet; planning from the stale store would be wrong.
    Stale,
}

impl std::fmt::Display for AuthorityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthorityError::Admission {
                resource,
                needed,
                limit,
            } => write!(
                f,
                "authority admission rejected: {resource} needs {needed} bytes, limit {limit}"
            ),
            AuthorityError::Identity(e) => write!(f, "{e}"),
            AuthorityError::Alloc => write!(f, "authority allocation failed"),
            AuthorityError::Unsupported { operation } => {
                write!(f, "{operation} is unsupported under unified_authority")
            }
            AuthorityError::Stale => write!(f, "authority host has unsynced graph changes"),
        }
    }
}

impl std::error::Error for AuthorityError {}

/// Counters (never timed).
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub mutations: u64,
    pub rejected: u64,
    pub records_created: u64,
    pub owners_created: u64,
    pub repartition_checks: u64,
    pub repartitions_committed: u64,
    pub repartitions_kept: u64,
    pub repartition_skipped: u64,
    /// Committed repartitions whose live model bytes grew (must stay 0:
    /// byte-safe acceptance).
    pub repartition_live_up: u64,
    /// Retained capacity added by committed repartitions (admitted against
    /// the retained budget, like a mutation).
    pub repartition_retained_growth: u64,
    /// Run owner writes (singleton ↔ family transitions and new singletons).
    pub run_relabels: u64,
    /// Max run owner writes in one mutation's apply (≤ 5: four 1×1 cut
    /// pieces and the edited cell), independent of family width. A
    /// repartition's writes are part of `repartition_work`.
    pub max_run_relabels_one_op: u64,
    /// Σ over repartitions of (L_g + |canon| + relabels) and the max of that
    /// work per created piece at a trigger (≤ 8 + relabels by §5.7.2).
    pub repartition_work: u64,
    pub max_work_per_creation: f64,
    pub canon_work: CanonWork,
    /// Index query work (`Q_idx`: buffer tests + tree nodes).
    pub index_work: u64,
    /// Symbol inventory validation and identity-table merge visits at rebuild.
    pub symbol_work: u64,
    /// Slot planning visits: rows, delta runs, touched pages (B1).
    pub slot_work: u64,
    /// Dry-run and accounting visits of mutations and repartitions, slot
    /// planning included (B2): cut items, touched groups, index shadows,
    /// identity sheets. Independent of the store size.
    pub plan_work: u64,
    /// Scopes refused or repartitions skipped because an allocation failed.
    pub alloc_failures: u64,
    /// Groups reclaimed when their last member left (B3).
    pub groups_reclaimed: u64,
    /// LK directory compactions committed, and skipped (budget or
    /// allocation; the directory keeps its dead keys until the next).
    pub lk_compactions: u64,
    pub lk_compaction_skips: u64,
    /// Work of committed compactions: directory keys plus edge-group slots
    /// visited (each compaction is O(keys + slots), no sheet-sized work).
    pub lk_compaction_work: u64,
}

/// Exact counts and bytes of a mutation, predicted and actual.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub records: u64,
    pub owners: u64,
    pub nodes: u64,
    pub runs: u64,
    pub next_id: u64,
    pub dep_entries: u64,
    pub prec_entries: u64,
    pub node_entries: u64,
    pub slot_rows: u64,
    /// Retained heap (capacity) bytes of every container.
    pub bytes: u64,
}

/// Result of an admitted mutation.
#[derive(Clone, Debug)]
pub struct MutationReport {
    pub before: Counts,
    pub predicted: Counts,
    /// Counts right after the apply (before any repartition).
    pub actual: Counts,
    /// Counts after repartitions.
    pub after: Counts,
    /// Predicted transient bytes above the retained size.
    pub predicted_transient: u64,
    /// Index transient observed during the apply (merge buffers and
    /// coexisting levels); must not exceed the prediction.
    pub observed_index_transient: u64,
    pub run_relabels: u64,
    pub repartitions: u32,
    /// Predicted heap peak of the whole scope above `before.bytes`
    /// (planning scratch, reservations, repartitions): an upper bound on
    /// what the allocator can observe (checked by the counting-allocator
    /// tests).
    pub predicted_peak_above_before: u64,
}

/// A family owner: `(sheet, domain, flags, template, anchor)`.
pub type FamilyOwner = (u16, Rect, u16, AstNodeId, (u32, u32));

/// A family piece left by a cut: `(group, dom, template, anchor, flags)`.
type OwnerPiece = (u32, Rect, AstNodeId, (u32, u32), u16);

/// `formula_view`: template, anchor, offset of the cell, literal row.
pub type FormulaView<'a> = (AstNodeId, (u32, u32), (i64, i64), Option<&'a [ValueRef]>);

/// A mutation's cut: every structure intersecting `q` on `sheet`.
#[derive(Clone, Debug, Default)]
struct Cut {
    recs: Vec<u32>,
    rec_pieces: Vec<(u32, Rect)>,
    owners: Vec<u32>,
    /// `(group, dom, template, anchor, flags)`.
    owner_pieces: Vec<OwnerPiece>,
    id_cuts: Vec<CellCut>,
    /// Index of the id cut that keeps its id (formula→formula).
    keep: Option<usize>,
}

/// A planned new formula at one cell (borrowing the caller's facts: the
/// plan copies nothing).
#[derive(Clone, Debug)]
struct NewFormula<'a> {
    cell: Cell,
    /// `(existing group or None, key)` per edge.
    edges: Vec<(Option<u32>, PendingEdgeKey<'a>)>,
    /// Existing node group, or the key of a new one.
    ngroup: Option<Result<u32, (u16, u64)>>,
    template: AstNodeId,
    /// The cell `template` is valid at.
    anchor: (u32, u32),
    literals: &'a [ValueRef],
    flags: u16,
    /// The id kept from the cell's previous formula.
    kept_id: Option<Vid>,
    /// A retired id history replay restores (M3; only without `kept_id`).
    revive: Option<Vid>,
}

#[derive(Clone, Debug)]
struct PendingEdgeKey<'a> {
    dep_sheet: u16,
    tag: Tag,
    lk: Result<u32, &'a LkKey>,
    proj: RefProj,
}

/// Per-sheet index vectors of the store.
#[derive(Clone, Debug, Default)]
struct Indexes {
    dep: Vec<LevelIndex>,
    prec: Vec<LevelIndex>,
    node: Vec<LevelIndex>,
}

/// `Clone` recounts the maintained index totals (cloned containers have
/// their lengths as capacities).
#[derive(Debug)]
pub struct Store {
    ids: IdentityTable,
    symbols: SymbolTable,
    owners: Vec<Owner>,
    own_free: u32,
    own_nfree: usize,
    nnodes: u64,
    ngroups: GroupTable<(u16, u64)>,
    node_loc: Vec<u32>,
    slots: SlotStore,
    recs: Vec<Rec>,
    rec_free: u32,
    rec_nfree: usize,
    egroups: GroupTable<EdgeKey>,
    lks: Directory<LkKey>,
    idx: Indexes,
    dep_loc: Vec<u32>,
    prec_loc: Vec<u32>,
    /// Maintained index totals per role (dep, prec, node): heap bytes of
    /// the indexes (not the index vectors) and stored entries.
    idx_bytes: [usize; 3],
    idx_entries: [usize; 3],
    /// Index buffers pre-allocated for the running scope, per `(role,
    /// sheet)`; empty between scopes.
    stage: Vec<(u8, u16, IndexStage)>,
    /// Predicted heap peak of the running scope, repartitions included
    /// (absolute bytes), and scratch that outlives the mutation's apply.
    scope_peak: u64,
    scope_extra: usize,
    pub budget: Budget,
    pub stats: Stats,
}

#[cfg(test)]
thread_local! {
    static CENSUS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Censuses run on this thread (tests: none may run on a mutation path).
#[cfg(test)]
pub(crate) fn census_calls() -> u64 {
    CENSUS.with(|c| c.get())
}

pub(super) const DEP: usize = 0;
pub(super) const PREC: usize = 1;
pub(super) const NODE: usize = 2;

// ---------------------------------------------------------------- helpers

fn slab_after(
    len: usize,
    nfree: usize,
    cap: usize,
    releases: usize,
    allocs: usize,
) -> (usize, usize) {
    // Releases first, then allocations reuse free slots, then push.
    let free = nfree + releases;
    let pushes = allocs.saturating_sub(free);
    let len2 = len + pushes;
    (len2, super::avl::grown(cap, len2))
}

fn vec_bytes<T>(cap: usize) -> usize {
    cap * size_of::<T>()
}

fn grow_exact<T>(v: &mut Vec<T>, total: usize) -> Result<(), AuthorityError> {
    if total > v.capacity() {
        v.try_reserve_exact(total - v.len())
            .map_err(|_| AuthorityError::Alloc)?;
    }
    Ok(())
}

/// An index operation routed through [`Store::index_op`].
#[derive(Clone, Copy, Debug)]
pub(super) enum IndexOp {
    Insert(super::geom::BoxT, u32),
    Remove(u32),
    Settle,
}

/// Planned operations on one index role: a shadow per touched sheet.
#[derive(Debug)]
struct IndexPlan {
    touched: Vec<(u16, IndexShadow)>,
    /// Sheet → position in `touched` (O(log d) per operation, where a
    /// linear search made a scope touching d sheets cost d²).
    pos: AvlMap,
    /// Upper bound on lookup probes (tree height + 1 per operation), for
    /// the planning work counter.
    probes: u64,
    /// Sheet count after the plan (the index vector grows to it).
    sheets: usize,
}

impl IndexPlan {
    fn new(idx: &[LevelIndex]) -> Self {
        Self {
            touched: Vec::new(),
            pos: AvlMap::new(),
            probes: 0,
            sheets: idx.len(),
        }
    }

    fn get<'a>(
        &'a mut self,
        idx: &[LevelIndex],
        s: u16,
    ) -> Result<&'a mut IndexShadow, AuthorityError> {
        self.probes += u64::from(self.pos.height()) + 1;
        let i = match self.pos.get(u64::from(s)) {
            Some(i) => i as usize,
            None => {
                let sh = idx
                    .get(sheet_slot(s))
                    .map(LevelIndex::shadow)
                    .unwrap_or_default();
                // Shadows are large; grow by doubling from one.
                if self.touched.len() == self.touched.capacity() {
                    self.touched
                        .try_reserve_exact(self.touched.len().max(1))
                        .map_err(|_| AuthorityError::Alloc)?;
                }
                let n = self.pos.len();
                self.pos
                    .try_reserve_slots(super::avl::grown(n, n + 1))
                    .map_err(|_| AuthorityError::Alloc)?;
                self.pos.insert(u64::from(s), self.touched.len() as u32);
                self.touched.push((s, sh));
                self.sheets = self.sheets.max(sheet_slot(s) + 1);
                self.touched.len() - 1
            }
        };
        Ok(&mut self.touched[i].1)
    }

    fn settle(&mut self) -> Result<(), AuthorityError> {
        for (_, sh) in self.touched.iter_mut() {
            sh.try_settle().map_err(|_| AuthorityError::Alloc)?;
        }
        Ok(())
    }

    /// Account the plan: retained bytes of the touched indexes and of the
    /// index vector, in-place growth, and the staged buffers. Returns the
    /// change in stored entries.
    fn account(&self, idx: &Vec<LevelIndex>, b: &mut mutate::Bytes) -> i64 {
        let mut entries = 0i64;
        for (s, sh) in &self.touched {
            match idx.get(sheet_slot(*s)) {
                Some(cur) => {
                    b.retained(cur.heap_bytes(), sh.heap_bytes());
                    for (o, n) in sh.vec_growth(cur) {
                        if n > o {
                            b.alloc += n - o;
                            b.max_old = b.max_old.max(o);
                        }
                    }
                    entries += sh.entries() as i64 - cur.entries() as i64;
                }
                None => {
                    let fresh = LevelIndex::default();
                    b.retained(0, sh.heap_bytes());
                    for (o, n) in sh.vec_growth(&fresh) {
                        b.alloc += n - o;
                    }
                    entries += sh.entries() as i64;
                }
            }
            b.alloc += sh.stage_bytes() + size_of::<(u8, u16, IndexStage)>();
        }
        b.grow(
            vec_bytes::<LevelIndex>(idx.capacity()),
            vec_bytes::<LevelIndex>(idx.capacity().max(self.sheets)),
        );
        entries
    }

    fn scratch_bytes(&self) -> usize {
        self.touched.capacity() * size_of::<(u16, IndexShadow)>()
            + self.pos.heap_bytes()
            + self
                .touched
                .iter()
                .map(|(_, sh)| sh.scratch_bytes())
                .sum::<usize>()
    }
}

impl Clone for Store {
    fn clone(&self) -> Self {
        let mut s = Self {
            ids: self.ids.clone(),
            symbols: self.symbols.clone(),
            owners: self.owners.clone(),
            own_free: self.own_free,
            own_nfree: self.own_nfree,
            nnodes: self.nnodes,
            ngroups: self.ngroups.clone(),
            node_loc: self.node_loc.clone(),
            slots: self.slots.clone(),
            recs: self.recs.clone(),
            rec_free: self.rec_free,
            rec_nfree: self.rec_nfree,
            egroups: self.egroups.clone(),
            lks: self.lks.clone(),
            idx: self.idx.clone(),
            dep_loc: self.dep_loc.clone(),
            prec_loc: self.prec_loc.clone(),
            idx_bytes: self.idx_bytes,
            idx_entries: self.idx_entries,
            stage: self.stage.clone(),
            scope_peak: self.scope_peak,
            scope_extra: self.scope_extra,
            budget: self.budget,
            stats: self.stats.clone(),
        };
        s.init_accounting();
        s
    }
}

impl Default for Store {
    fn default() -> Self {
        Self {
            ids: IdentityTable::new(),
            symbols: SymbolTable::default(),
            owners: Vec::new(),
            own_free: DEAD,
            own_nfree: 0,
            nnodes: 0,
            ngroups: GroupTable::default(),
            node_loc: Vec::new(),
            slots: SlotStore::default(),
            recs: Vec::new(),
            rec_free: DEAD,
            rec_nfree: 0,
            egroups: GroupTable::default(),
            lks: Directory::default(),
            idx: Indexes::default(),
            dep_loc: Vec::new(),
            prec_loc: Vec::new(),
            idx_bytes: [0; 3],
            idx_entries: [0; 3],
            stage: Vec::new(),
            scope_peak: 0,
            scope_extra: 0,
            budget: Budget::default(),
            stats: Stats::default(),
        }
    }
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id_limit(limit: Vid) -> Self {
        Self {
            ids: IdentityTable::with_limit(limit),
            ..Self::default()
        }
    }

    pub fn ids(&self) -> &IdentityTable {
        &self.ids
    }

    // Consumed by host lifecycle tests; planner identity translation follows.
    #[allow(dead_code)]
    pub(crate) fn symbol_id(&self, symbol: SymbolId) -> Option<Vid> {
        self.symbols.lookup(symbol)
    }

    pub fn slots(&self) -> &SlotStore {
        &self.slots
    }

    // ------------------------------------------------------------ bytes

    fn idx_bytes(v: &Vec<LevelIndex>) -> usize {
        vec_bytes::<LevelIndex>(v.capacity()) + v.iter().map(LevelIndex::heap_bytes).sum::<usize>()
    }

    /// Retained heap bytes: the capacity of every container (§5.1). O(1):
    /// every component's total is maintained (correction B2).
    pub fn heap_bytes(&self) -> u64 {
        (self.ids.heap_bytes()
            + self.symbols.heap_bytes() as usize
            + vec_bytes::<Owner>(self.owners.capacity())
            + self.ngroups.heap_bytes()
            + vec_bytes::<u32>(self.node_loc.capacity())
            + self.slots.heap_bytes()
            + vec_bytes::<Rec>(self.recs.capacity())
            + self.egroups.heap_bytes()
            + self.lks.heap_bytes()
            + vec_bytes::<LevelIndex>(self.idx.dep.capacity())
            + vec_bytes::<LevelIndex>(self.idx.prec.capacity())
            + vec_bytes::<LevelIndex>(self.idx.node.capacity())
            + self.idx_bytes.iter().sum::<usize>()
            + vec_bytes::<u32>(self.dep_loc.capacity())
            + vec_bytes::<u32>(self.prec_loc.capacity())) as u64
    }

    /// [`Self::heap_bytes`] by a full walk of every container (the census:
    /// tests and `check` only, never on the mutation path).
    pub fn census_heap_bytes(&self) -> u64 {
        #[cfg(test)]
        CENSUS.with(|c| c.set(c.get() + 1));
        (self.ids.census_bytes()
            + self.symbols.heap_bytes() as usize
            + vec_bytes::<Owner>(self.owners.capacity())
            + self.ngroups.census_bytes()
            + vec_bytes::<u32>(self.node_loc.capacity())
            + self.slots.census_bytes()
            + vec_bytes::<Rec>(self.recs.capacity())
            + self.egroups.census_bytes()
            + self.lks.heap_bytes()
            + Self::idx_bytes(&self.idx.dep)
            + Self::idx_bytes(&self.idx.prec)
            + Self::idx_bytes(&self.idx.node)
            + vec_bytes::<u32>(self.dep_loc.capacity())
            + vec_bytes::<u32>(self.prec_loc.capacity())) as u64
    }

    /// Retained bytes by component (reports; a census).
    pub fn bytes_breakdown(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("identity", self.ids.census_bytes()),
            ("symbols", self.symbols.heap_bytes() as usize),
            ("owners", vec_bytes::<Owner>(self.owners.capacity())),
            ("node_groups", self.ngroups.census_bytes()),
            ("node_loc", vec_bytes::<u32>(self.node_loc.capacity())),
            ("slots", self.slots.census_bytes()),
            ("records", vec_bytes::<Rec>(self.recs.capacity())),
            ("edge_groups", self.egroups.census_bytes()),
            ("lk_dir", self.lks.heap_bytes()),
            ("dep_index", Self::idx_bytes(&self.idx.dep)),
            ("prec_index", Self::idx_bytes(&self.idx.prec)),
            ("node_index", Self::idx_bytes(&self.idx.node)),
            (
                "dep_prec_loc",
                vec_bytes::<u32>(self.dep_loc.capacity() + self.prec_loc.capacity()),
            ),
        ]
    }

    /// Counts and bytes, O(1) (maintained totals).
    pub fn counts(&self) -> Counts {
        Counts {
            records: (self.recs.len() - self.rec_nfree) as u64,
            owners: (self.owners.len() - self.own_nfree) as u64,
            nodes: self.nnodes,
            runs: self.ids.run_count() as u64,
            next_id: u64::from(self.ids.next_id()),
            dep_entries: self.idx_entries[DEP] as u64,
            prec_entries: self.idx_entries[PREC] as u64,
            node_entries: self.idx_entries[NODE] as u64,
            slot_rows: self.slots.rows() as u64,
            bytes: self.heap_bytes(),
        }
    }

    /// [`Self::counts`] by a full census (tests and `check`).
    pub fn census_counts(&self) -> Counts {
        let e = |v: &[LevelIndex]| v.iter().map(LevelIndex::entries).sum::<usize>() as u64;
        Counts {
            dep_entries: e(&self.idx.dep),
            prec_entries: e(&self.idx.prec),
            node_entries: e(&self.idx.node),
            bytes: self.census_heap_bytes(),
            ..self.counts()
        }
    }

    /// The maintained totals equal the census (B2 cross-check).
    pub fn check_accounting(&self) -> Result<(), String> {
        let parts = [
            ("identity", self.ids.heap_bytes(), self.ids.census_bytes()),
            (
                "edge groups",
                self.egroups.heap_bytes(),
                self.egroups.census_bytes(),
            ),
            (
                "node groups",
                self.ngroups.heap_bytes(),
                self.ngroups.census_bytes(),
            ),
            ("slots", self.slots.heap_bytes(), self.slots.census_bytes()),
        ];
        for (name, a, b) in parts {
            if a != b {
                return Err(format!("{name}: maintained {a} != census {b}"));
            }
        }
        for (role, v) in [
            (DEP, &self.idx.dep),
            (PREC, &self.idx.prec),
            (NODE, &self.idx.node),
        ] {
            let b: usize = v.iter().map(LevelIndex::heap_bytes).sum();
            if b != self.idx_bytes[role] {
                return Err(format!(
                    "index role {role}: maintained {} != census {b}",
                    self.idx_bytes[role]
                ));
            }
        }
        let (c, k) = (self.counts(), self.census_counts());
        if c != k {
            return Err(format!("maintained counts {c:?} != census {k:?}"));
        }
        if !self.stage.is_empty() {
            return Err("index stage left over from a scope".into());
        }
        Ok(())
    }

    /// Initialise the maintained totals from a census (after a build).
    pub(super) fn init_accounting(&mut self) {
        for (role, v) in [
            (DEP, &self.idx.dep),
            (PREC, &self.idx.prec),
            (NODE, &self.idx.node),
        ] {
            self.idx_bytes[role] = v.iter().map(LevelIndex::heap_bytes).sum();
            self.idx_entries[role] = v.iter().map(LevelIndex::entries).sum();
        }
        self.egroups.recount_members();
        self.ngroups.recount_members();
    }

    // ------------------------------------------------------------ index ops

    /// Order the staged indexes by (role, sheet) for `index_op`'s binary
    /// search. In place: no allocation after the reservation.
    pub(super) fn seal_stage(&mut self) {
        self.stage.sort_unstable_by_key(|(r, s, _)| (*r, *s));
    }

    /// One index operation on `(role, sheet)`, with its allocations taken
    /// from the scope's stage when there is one, keeping the role totals.
    pub(super) fn index_op(&mut self, role: usize, sheet: u16, op: IndexOp) {
        // The stage is sorted by (role, sheet) when sealed.
        let stage = match self
            .stage
            .binary_search_by_key(&(role as u8, sheet), |(r, s, _)| (*r, *s))
        {
            Ok(i) => Some(&mut self.stage[i].2),
            Err(_) => None,
        };
        let (v, loc) = match role {
            DEP => (&mut self.idx.dep, &mut self.dep_loc),
            PREC => (&mut self.idx.prec, &mut self.prec_loc),
            _ => (&mut self.idx.node, &mut self.node_loc),
        };
        while v.len() <= sheet_slot(sheet) {
            debug_assert!(
                v.len() < v.capacity() || stage.is_none(),
                "unreserved index vector"
            );
            v.push(LevelIndex::default());
        }
        let idx = &mut v[sheet_slot(sheet)];
        let (b0, e0) = (idx.heap_bytes(), idx.entries());
        match op {
            IndexOp::Insert(b, id) => idx.insert_in(b, id, loc, stage),
            IndexOp::Remove(id) => idx.remove(id, loc),
            IndexOp::Settle => idx.settle_in(loc, stage),
        }
        let (b1, e1) = (idx.heap_bytes(), idx.entries());
        self.idx_bytes[role] = self.idx_bytes[role] + b1 - b0;
        self.idx_entries[role] = self.idx_entries[role] + e1 - e0;
    }

    fn index_ref(&self, role: usize) -> &Vec<LevelIndex> {
        match role {
            DEP => &self.idx.dep,
            PREC => &self.idx.prec,
            _ => &self.idx.node,
        }
    }

    /// Live model bytes: what the structures hold, independent of spare
    /// capacity (records with their two index entries, owners, family index
    /// entries). Byte-safe repartition acceptance compares this.
    pub fn live_model_bytes(&self) -> u64 {
        let entry = size_of::<(super::geom::BoxT, u32)>() as u64;
        let c = self.counts();
        c.records * (size_of::<Rec>() as u64 + 2 * entry)
            + c.owners * size_of::<Owner>() as u64
            + c.nodes * entry
    }

    /// Whether the store holds any grid (non-symbol) formula. O(1).
    pub fn has_grid_formulas(&self) -> bool {
        self.ids.has_grid_runs()
    }

    pub fn formula_count(&self) -> u64 {
        self.ids.live_runs().map(|(_, r)| u64::from(r.len)).sum()
    }

    // ------------------------------------------------------------ lookups

    fn owner_ref(&self, o: u32) -> &Owner {
        &self.owners[o as usize]
    }

    /// The owner of a formula cell: the singleton handle from its run, or
    /// the family node containing it (one node-index point query).
    pub fn owner_at(&self, cell: Cell) -> Option<u32> {
        let (_, h) = self.ids.lookup(cell)?;
        let run = self.ids.run(h);
        if run.owner != FAMILY {
            return Some(run.owner);
        }
        let idx = self.idx.node.get(sheet_slot(cell.0))?;
        let mut found = None;
        idx.query(&[cell.1, cell.2, cell.1, cell.2], &mut |o| found = Some(o));
        debug_assert!(found.is_some(), "family run without a family node");
        found
    }

    /// Node group key of an owner: `(sheet, L hash)`, if grouped.
    pub fn owner_group_key(&self, o: u32) -> Option<(u16, u64)> {
        let g = self.owners[o as usize].group;
        (g != UNGROUPED && g != DEAD).then(|| self.ngroups.key(g))
    }

    /// The representative template and anchor of the node group a formula
    /// with L tokens `tokens` on `sheet` would join, if any. The engine
    /// host re-derives the representative's tokens from the arena and
    /// compares; a mismatch (64-bit hash collision) makes the formula an
    /// ungrouped singleton.
    pub fn l_representative(&self, sheet: u16, tokens: &[u64]) -> Option<(AstNodeId, (u32, u32))> {
        let g = self.ngroups.get(&(sheet, l_hash(tokens)))?;
        let o = *self.ngroups[g as usize].members.first()?;
        let w = &self.owners[o as usize];
        Some((w.template, w.anchor))
    }

    /// `formula_view` (§9.1, §11): the template, its anchor, the offset of
    /// `cell` from it, and the cell's literal row.
    pub fn formula_view(&self, cell: Cell) -> Option<FormulaView<'_>> {
        let (id, _) = self.ids.lookup(cell)?;
        let o = self.owner_ref(self.owner_at(cell)?);
        let off = (
            i64::from(cell.1) - i64::from(o.anchor.0),
            i64::from(cell.2) - i64::from(o.anchor.1),
        );
        Some((o.template, o.anchor, off, self.slots.get(id)))
    }

    /// Live family owners: `(sheet, domain, flags, template, anchor)`.
    pub fn family_owners(
        &self,
    ) -> impl Iterator<Item = (u16, Rect, u16, AstNodeId, (u32, u32))> + '_ {
        self.owners
            .iter()
            .filter(|o| o.group != DEAD && o.is_family())
            .map(|o| (o.sheet, o.dom, o.flags, o.template, o.anchor))
    }

    /// A live family owner's `(sheet, domain, flags, template, anchor)`.
    pub fn family_owner(&self, o: u32) -> Option<FamilyOwner> {
        let w = self.owners.get(o as usize)?;
        (w.group != DEAD && w.is_family())
            .then_some((w.sheet, w.dom, w.flags, w.template, w.anchor))
    }

    /// Every live owner's template root (arena compaction roots).
    pub fn owner_templates(&self) -> impl Iterator<Item = AstNodeId> + '_ {
        self.owners
            .iter()
            .filter(|o| o.group != DEAD)
            .map(|o| o.template)
    }

    /// Remap every live owner's template after an arena compaction.
    /// `remap` is indexed by old id (`u32::MAX`: dropped); ids outside it
    /// (sentinels) are kept.
    pub fn remap_templates(&mut self, remap: &[u32]) {
        for o in self.owners.iter_mut().filter(|o| o.group != DEAD) {
            if let Some(&new) = remap.get(o.template.as_u32() as usize) {
                debug_assert_ne!(new, u32::MAX, "live owner template dropped");
                o.template = AstNodeId::from_u32(new);
            }
        }
    }

    /// An owner's template and the anchor it is valid at.
    pub fn owner_template(&self, o: u32) -> (AstNodeId, (u32, u32)) {
        let w = &self.owners[o as usize];
        (w.template, w.anchor)
    }

    /// Owner domain and whether it is a family node.
    pub fn owner_dom(&self, o: u32) -> (u16, Rect, bool) {
        let w = &self.owners[o as usize];
        (w.sheet, w.dom, w.is_family())
    }

    // ------------------------------------------------------------ queries

    /// Families have sentinel run owners; discover their actual owner handles
    /// through the maintained node index, exposing every index visit.
    pub(super) fn visit_plan_owners(
        &self,
        sheet: u16,
        domain: &Rect,
        visit: &mut dyn FnMut(u32),
    ) -> u64 {
        self.idx
            .node
            .get(sheet_slot(sheet))
            .map_or(0, |idx| idx.query(&domain.as_box(), visit))
    }

    /// Allocation-free planner discovery over independently partitioned
    /// edge records. Returns actual dependent-index work; the caller counts
    /// incidences and admits its own output before filling it.
    pub(super) fn visit_plan_edges(
        &self,
        sheet: u16,
        domain: &Rect,
        visit: &mut dyn FnMut(EdgeKey, Rect),
    ) -> u64 {
        let Some(idx) = self.idx.dep.get(sheet_slot(sheet)) else {
            return 0;
        };
        idx.query(&domain.as_box(), &mut |id| {
            let rec = &self.recs[id as usize];
            visit(self.egroups.key(rec.group), rec.dep);
        })
    }

    /// Direct dependents of cells `q` on `sheet` (§4.3 queries), as
    /// `(dependent sheet, rect)` pieces (they may overlap).
    pub fn direct_dependents(
        &self,
        sheet: u16,
        q: &Rect,
        filter: TagFilter,
        out: &mut Vec<(u16, Rect)>,
    ) -> u64 {
        let Some(idx) = self.idx.prec.get(sheet_slot(sheet)) else {
            return 0;
        };
        idx.query(&q.as_box(), &mut |id| {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            if filter.admits(key.tag)
                && let Some(d) = key.proj.invert(&r.dep, q)
            {
                out.push((key.dep_sheet, d));
            }
        })
    }

    /// Direct dependents of `q` through edges the formula text spells
    /// (not through a name, table or source): what legacy's inspection
    /// reports as a cell's readers. `(dependent sheet, rect)` pieces.
    pub fn direct_text_dependents(&self, sheet: u16, q: &Rect, out: &mut Vec<(u16, Rect)>) -> u64 {
        let Some(idx) = self.idx.prec.get(sheet_slot(sheet)) else {
            return 0;
        };
        idx.query(&q.as_box(), &mut |id| {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            if key.lk == NO_LK
                && let Some(d) = key.proj.invert(&r.dep, q)
            {
                out.push((key.dep_sheet, d));
            }
        })
    }

    /// [`Self::direct_text_dependents`], streamed: `f(dependent sheet,
    /// dependent rect)` per edge record until it returns false (bounded
    /// inspection materializes nothing past its budget). Records come in
    /// index order and may overlap. Returns whether the walk ran to the end.
    pub fn visit_text_dependents(
        &self,
        sheet: u16,
        q: &Rect,
        f: &mut dyn FnMut(u16, Rect) -> bool,
    ) -> bool {
        let Some(idx) = self.idx.prec.get(sheet_slot(sheet)) else {
            return true;
        };
        idx.query_until(&q.as_box(), &mut |id| {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            match key.proj.invert(&r.dep, q) {
                Some(d) if key.lk == NO_LK => f(key.dep_sheet, d),
                _ => true,
            }
        })
        .1
    }

    /// Direct dependents of `q` whose reference image has at most `limit`
    /// cells, through text-origin edges of grid formulas or any edge of a
    /// symbol row: the readers legacy held as expanded in-edges (a cell,
    /// a range within the expansion limit, a name over it).
    pub fn direct_small_dependents(
        &self,
        sheet: u16,
        q: &Rect,
        limit: u64,
        out: &mut Vec<(u16, Rect)>,
    ) -> u64 {
        let Some(idx) = self.idx.prec.get(sheet_slot(sheet)) else {
            return 0;
        };
        idx.query(&q.as_box(), &mut |id| {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            if (key.lk == NO_LK || key.dep_sheet == SYMBOL_SHEET)
                && key.proj.instantiate(r.dep.r0, r.dep.c0).is_some_and(|img| {
                    u64::from(img.r1 - img.r0 + 1) * u64::from(img.c1 - img.c0 + 1) <= limit
                })
                && let Some(d) = key.proj.invert(&r.dep, q)
            {
                out.push((key.dep_sheet, d));
            }
        })
    }

    /// [`Self::direct_small_dependents`] per dependent cell:
    /// `f(dependent sheet, row, col, image)` for every dependent cell whose
    /// (small, per the limit) reference image meets `q`.
    pub fn visit_direct_small_dependents(
        &self,
        sheet: u16,
        q: &Rect,
        limit: u64,
        f: &mut dyn FnMut(u16, u32, u32, Rect),
    ) {
        let Some(idx) = self.idx.prec.get(sheet_slot(sheet)) else {
            return;
        };
        let mut hits: Vec<u32> = Vec::new();
        idx.query(&q.as_box(), &mut |id| hits.push(id));
        for id in hits {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            if !(key.lk == NO_LK || key.dep_sheet == SYMBOL_SHEET)
                || !key.proj.instantiate(r.dep.r0, r.dep.c0).is_some_and(|img| {
                    u64::from(img.r1 - img.r0 + 1) * u64::from(img.c1 - img.c0 + 1) <= limit
                })
            {
                continue;
            }
            let Some(d) = key.proj.invert(&r.dep, q) else {
                continue;
            };
            for row in d.r0..=d.r1 {
                for col in d.c0..=d.c1 {
                    if let Some(img) = key.proj.instantiate(row, col) {
                        f(key.dep_sheet, row, col, img);
                    }
                }
            }
        }
    }

    /// Every dependent member whose reference image meets `q` on `sheet`:
    /// `f(dependent sheet, row, col, image)`, per member cell (relative
    /// references give each member its own image).
    pub fn visit_dependent_images(
        &self,
        sheet: u16,
        q: &Rect,
        f: &mut dyn FnMut(u16, u32, u32, Rect),
    ) {
        let Some(idx) = self.idx.prec.get(sheet_slot(sheet)) else {
            return;
        };
        let mut hits: Vec<u32> = Vec::new();
        idx.query(&q.as_box(), &mut |id| hits.push(id));
        for id in hits {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            let Some(d) = key.proj.invert(&r.dep, q) else {
                continue;
            };
            for row in d.r0..=d.r1 {
                for col in d.c0..=d.c1 {
                    if let Some(img) = key.proj.instantiate(row, col) {
                        f(key.dep_sheet, row, col, img);
                    }
                }
            }
        }
    }

    /// Direct precedents of one formula cell: `(tag, target sheet, rect)`.
    pub fn direct_precedents(
        &self,
        cell: Cell,
        filter: TagFilter,
        out: &mut Vec<(Tag, u16, Rect)>,
    ) -> u64 {
        let Some(idx) = self.idx.dep.get(sheet_slot(cell.0)) else {
            return 0;
        };
        idx.query(&[cell.1, cell.2, cell.1, cell.2], &mut |id| {
            let r = &self.recs[id as usize];
            let key = self.egroups.key(r.group);
            if filter.admits(key.tag)
                && let Some(p) = key.proj.instantiate(cell.1, cell.2)
            {
                out.push((key.tag, key.proj.sheet, p));
            }
        })
    }

    /// Grid direct dependents of `q` on `sheet`, looking through symbol
    /// nodes (a reader of a name whose node reads `q` is a direct
    /// dependent of `q`, as legacy's name links make it). Symbol nodes form
    /// a small graph; each is expanded once.
    pub fn direct_grid_dependents(&self, sheet: u16, q: &Rect, filter: TagFilter) -> Cover {
        let mut cover = Cover::new();
        let mut seen = rustc_hash::FxHashSet::default();
        let mut hits = Vec::new();
        self.direct_dependents(sheet, q, filter, &mut hits);
        while let Some((s, r)) = hits.pop() {
            if s != SYMBOL_SHEET {
                cover.insert_rect(s, &r);
                continue;
            }
            for slot in r.r0..=r.r1 {
                if seen.insert(slot) {
                    self.direct_dependents(SYMBOL_SHEET, &Rect::cell(slot, 0), filter, &mut hits);
                }
            }
        }
        cover
    }

    /// Grid direct precedents of one cell, looking through symbol nodes
    /// (a name's targets count as precedents of its readers). Each symbol
    /// node is expanded once.
    pub fn direct_grid_precedents(
        &self,
        cell: Cell,
        filter: TagFilter,
        out: &mut Vec<(Tag, u16, Rect)>,
    ) {
        let mut seen = rustc_hash::FxHashSet::default();
        let mut hits = Vec::new();
        self.direct_precedents(cell, filter, &mut hits);
        while let Some((tag, s, r)) = hits.pop() {
            if s != SYMBOL_SHEET {
                out.push((tag, s, r));
                continue;
            }
            for slot in r.r0..=r.r1 {
                if seen.insert(slot) {
                    self.direct_precedents((SYMBOL_SHEET, slot, 0), filter, &mut hits);
                }
            }
        }
    }

    /// Transitive dependents (positive length) of `seeds`, by the
    /// coverage-difference traversal (§5.2): each popped piece is queried
    /// once and only uncovered parts of its answer are pushed.
    pub fn dependents(&self, seeds: &[(u16, Rect)], filter: TagFilter) -> (Cover, u64) {
        let mut cover = Cover::new();
        let mut work = 0;
        let mut frontier: Vec<(u16, Rect)> = seeds.to_vec();
        let mut hits = Vec::new();
        while let Some((s, q)) = frontier.pop() {
            hits.clear();
            work += self.direct_dependents(s, &q, filter, &mut hits);
            for &(ds, d) in &hits {
                cover.insert_rect_fresh(ds, &d, &mut frontier);
            }
        }
        (cover, work)
    }

    /// Precedent cover of the formula cells in `seeds` (transitive,
    /// positive length): the cells read, and recursively the formula cells
    /// among them.
    pub fn precedents(&self, seeds: &[Cell], filter: TagFilter) -> Cover {
        let mut cover = Cover::new();
        let mut queue: Vec<Cell> = seeds.to_vec();
        let mut seen: rustc_hash::FxHashSet<Cell> = seeds.iter().copied().collect();
        let mut hits = Vec::new();
        let mut fresh = Vec::new();
        while let Some(c) = queue.pop() {
            hits.clear();
            self.direct_precedents(c, filter, &mut hits);
            for &(_, s, r) in &hits {
                fresh.clear();
                cover.insert_rect_fresh(s, &r, &mut fresh);
                for &(fs, fr) in &fresh {
                    // Formula cells inside the fresh part.
                    for col in fr.c0..=fr.c1 {
                        let mut hs = Vec::new();
                        self.ids.runs_in(fs, col, fr.r0, fr.r1, &mut hs);
                        for h in hs {
                            let run = self.ids.run(h);
                            let a = run.row_start.max(fr.r0);
                            let b = (run.row_start + run.len - 1).min(fr.r1);
                            for row in a..=b {
                                if seen.insert((fs, row, col)) {
                                    queue.push((fs, row, col));
                                }
                            }
                        }
                    }
                }
            }
        }
        cover
    }

    /// Every edge group's key and its records' dependent rects.
    pub fn edge_groups(&self) -> impl Iterator<Item = (EdgeKey, Vec<Rect>)> + '_ {
        self.egroups.iter().map(|(_, key, grp)| {
            (
                key,
                grp.members
                    .iter()
                    .map(|&r| self.recs[r as usize].dep)
                    .collect(),
            )
        })
    }

    /// Every node group's `(sheet, L hash)` and its owners' domains.
    pub fn node_groups(&self) -> impl Iterator<Item = ((u16, u64), Vec<Rect>)> + '_ {
        self.ngroups.iter().map(|(_, key, grp)| {
            (
                key,
                grp.members
                    .iter()
                    .map(|&o| self.owners[o as usize].dom)
                    .collect(),
            )
        })
    }

    /// LK directory keys (live and dead: the directory is append-only;
    /// see [`Self::lk_compaction_due`]).
    pub fn lk_len(&self) -> usize {
        self.lks.len()
    }

    /// Edge-group slots, live and free.
    pub fn egroup_slots(&self) -> usize {
        self.egroups.slots()
    }

    /// Compact the LK directory once dead keys dominate it (M1a parent fix
    /// B-23, re-review R3). A key is live while a live edge group refers to
    /// it; the directory is compacted when it holds more than twice the
    /// edge-group slots plus a constant, so at least half its keys are dead.
    /// The compaction renumbers the live keys and rekeys the edge groups:
    /// O(keys + edge-group slots) work, paid for by the ≥ half of the keys
    /// it drops (each interned once), with no sheet-sized or formula-sized
    /// work. It runs as its own admitted scope after a mutation: planned,
    /// admitted against the budget, reserved fallibly, then applied without
    /// allocating. A rejection or allocation failure skips it (counted) and
    /// changes nothing.
    pub(super) fn maybe_compact_lks(&mut self) {
        if self.lks.len() <= 2 * (self.egroups.slots() + 8) {
            return;
        }
        let k = self.lks.len();
        let mut remap: Vec<u32> = Vec::new();
        if remap.try_reserve_exact(k).is_err() {
            self.stats.lk_compaction_skips += 1;
            return;
        }
        remap.resize(k, DEAD);
        for (_, key, _) in self.egroups.iter() {
            if key.lk != NO_LK {
                remap[key.lk as usize] = 0;
            }
        }
        let (mut live, mut live_heap) = (0u32, 0usize);
        for (id, r) in remap.iter_mut().enumerate() {
            if *r != DEAD {
                *r = live;
                live += 1;
                live_heap += self.lks.key(id as u32).name.len();
            }
        }
        let plan = self.lks.compact_plan(live as usize, live_heap);
        let lk_now = self.lks.heap_bytes() as u64;
        let lk_after = Directory::<LkKey>::compact_bytes(&plan) as u64;
        let before = self.heap_bytes();
        let after = before - lk_now + lk_after;
        // Old and new directories, the remap, the staged group index and
        // anything the enclosing scope still holds coexist until the apply
        // frees the old containers. Moved key copies are not reallocated.
        let peak = before
            + (Directory::<LkKey>::compact_alloc_bytes(&plan)
                + remap.capacity() * size_of::<u32>()
                + self.egroups.rekey_stage_bytes()
                + self.scope_extra) as u64;
        let transient = peak.saturating_sub(after);
        if self.admit(after, transient).is_err() {
            self.stats.lk_compaction_skips += 1;
            return;
        }
        let staged = match self.lks.try_stage_compact(&plan) {
            Ok(x) => x,
            Err(_) => {
                self.stats.lk_compaction_skips += 1;
                self.stats.alloc_failures += 1;
                return;
            }
        };
        if self.egroups.try_stage_rekey().is_err() {
            self.stats.lk_compaction_skips += 1;
            self.stats.alloc_failures += 1;
            return;
        }
        self.scope_peak = self.scope_peak.max(peak);
        // ---- apply: no allocation.
        self.egroups.rekey(|p| {
            let mut q = *p;
            if q.lk != NO_LK {
                q.lk = remap[q.lk as usize];
            }
            q
        });
        self.lks.apply_compact(&remap, DEAD, staged, &plan);
        self.stats.lk_compactions += 1;
        self.stats.lk_compaction_work += (k + self.egroups.slots()) as u64;
        debug_assert_eq!(self.heap_bytes(), after, "compaction bytes");
    }

    pub fn lk_key(&self, lk: u32) -> &LkKey {
        self.lks.key(lk)
    }

    pub fn edge_group_count(&self) -> usize {
        self.egroups.len()
    }

    pub fn node_group_count(&self) -> usize {
        self.ngroups.len()
    }

    pub fn egroup(&self, g: u32) -> &Group {
        &self.egroups[g as usize]
    }

    pub fn ngroup(&self, g: u32) -> &Group {
        &self.ngroups[g as usize]
    }

    /// The live formula cells (tests, rebuild comparisons).
    pub fn formula_cells(&self) -> Vec<Cell> {
        let mut v = Vec::new();
        for (_, r) in self.ids.live_runs() {
            for i in 0..r.len {
                v.push((r.sheet, r.row_start + i, r.col));
            }
        }
        v.sort_unstable();
        v
    }
}

mod build;
mod mutate;
mod repartition;
mod verify;

pub use build::{BuildInput, GivenId, IdentifiedFacts, SharedBuildInput};
pub use verify::Digest;
