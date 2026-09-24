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

use super::canon::{self, CanonWork};
use super::dir::{DirPlan, Directory};
use super::geom::{Cell, Cover, Rect};
pub use super::groups::Group;
use super::groups::{GroupKey, GroupTable, Members, l_hash};
use super::identity::{CellCut, FAMILY, IdError, IdentityTable, Vid};
use super::level_index::{IndexShadow, LevelIndex, NONE};
use super::proj::RefProj;
use super::slots::SlotStore;
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
    pub literals: SmallVec<[ValueRef; 4]>,
    pub flags: u16,
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
}

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

/// A planned new formula at one cell.
#[derive(Clone, Debug)]
struct NewFormula {
    cell: Cell,
    /// `(existing group or None, key)` per edge.
    edges: Vec<(Option<u32>, PendingEdgeKey)>,
    /// Existing node group, or the key of a new one.
    ngroup: Option<Result<u32, (u16, u64)>>,
    template: AstNodeId,
    literals: SmallVec<[ValueRef; 4]>,
    flags: u16,
    /// The id kept from the cell's previous formula.
    kept_id: Option<Vid>,
}

#[derive(Clone, Debug)]
struct PendingEdgeKey {
    dep_sheet: u16,
    tag: Tag,
    lk: Result<u32, LkKey>,
    proj: RefProj,
}

/// Per-sheet index vectors of the store.
#[derive(Clone, Debug, Default)]
struct Indexes {
    dep: Vec<LevelIndex>,
    prec: Vec<LevelIndex>,
    node: Vec<LevelIndex>,
}

#[derive(Clone, Debug)]
pub struct Store {
    ids: IdentityTable,
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
    pub budget: Budget,
    pub stats: Stats,
}

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

fn grow_members(v: &mut Members, cap: usize) -> Result<(), AuthorityError> {
    if cap > v.capacity() {
        v.try_reserve_exact(cap - v.len())
            .map_err(|_| AuthorityError::Alloc)?;
    }
    Ok(())
}

fn grow_exact<T>(v: &mut Vec<T>, total: usize) -> Result<(), AuthorityError> {
    if total > v.capacity() {
        v.try_reserve_exact(total - v.len())
            .map_err(|_| AuthorityError::Alloc)?;
    }
    Ok(())
}

/// Planned growth of one index role.
struct IndexPlan {
    shadows: Vec<Option<IndexShadow>>,
    /// Sheet count after the plan (index vectors grow to it).
    sheets: usize,
}

impl IndexPlan {
    fn new(idx: &[LevelIndex]) -> Self {
        Self {
            shadows: vec![None; idx.len()],
            sheets: idx.len(),
        }
    }

    fn get<'a>(&'a mut self, idx: &[LevelIndex], s: u16) -> &'a mut IndexShadow {
        let s = s as usize;
        if self.shadows.len() <= s {
            self.shadows.resize(s + 1, None);
            self.sheets = self.sheets.max(s + 1);
        }
        self.shadows[s]
            .get_or_insert_with(|| idx.get(s).map(LevelIndex::shadow).unwrap_or_default())
    }

    fn settle(&mut self) {
        for sh in self.shadows.iter_mut().flatten() {
            sh.settle();
        }
    }

    /// (retained bytes, transient bytes above retained) after the plan.
    fn bytes(&self, idx: &[LevelIndex]) -> (usize, usize) {
        let cap = idx.len().max(self.sheets);
        let mut retained = 0;
        let mut transient = 0;
        for s in 0..cap {
            match self.shadows.get(s).and_then(Option::as_ref) {
                Some(sh) => {
                    retained += sh.heap_bytes();
                    transient += sh.peak() - sh.heap_bytes();
                }
                None => retained += idx.get(s).map_or(0, LevelIndex::heap_bytes),
            }
        }
        (retained, transient)
    }

    fn entries(&self, idx: &[LevelIndex]) -> u64 {
        let cap = idx.len().max(self.sheets);
        (0..cap)
            .map(|s| match self.shadows.get(s).and_then(Option::as_ref) {
                Some(sh) => sh.entries(),
                None => idx.get(s).map_or(0, LevelIndex::entries),
            })
            .sum::<usize>() as u64
    }
}

impl Default for Store {
    fn default() -> Self {
        Self {
            ids: IdentityTable::new(),
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

    pub fn slots(&self) -> &SlotStore {
        &self.slots
    }

    // ------------------------------------------------------------ bytes

    fn idx_bytes(v: &Vec<LevelIndex>) -> usize {
        vec_bytes::<LevelIndex>(v.capacity()) + v.iter().map(LevelIndex::heap_bytes).sum::<usize>()
    }

    /// Retained heap bytes: the capacity of every container (§5.1).
    pub fn heap_bytes(&self) -> u64 {
        (self.ids.heap_bytes()
            + vec_bytes::<Owner>(self.owners.capacity())
            + self.ngroups.heap_bytes()
            + vec_bytes::<u32>(self.node_loc.capacity())
            + self.slots.heap_bytes()
            + vec_bytes::<Rec>(self.recs.capacity())
            + self.egroups.heap_bytes()
            + self.lks.heap_bytes()
            + Self::idx_bytes(&self.idx.dep)
            + Self::idx_bytes(&self.idx.prec)
            + Self::idx_bytes(&self.idx.node)
            + vec_bytes::<u32>(self.dep_loc.capacity())
            + vec_bytes::<u32>(self.prec_loc.capacity())) as u64
    }

    /// Retained bytes by component (reports).
    pub fn bytes_breakdown(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("identity", self.ids.heap_bytes()),
            ("owners", vec_bytes::<Owner>(self.owners.capacity())),
            ("node_groups", self.ngroups.heap_bytes()),
            ("node_loc", vec_bytes::<u32>(self.node_loc.capacity())),
            ("slots", self.slots.heap_bytes()),
            ("records", vec_bytes::<Rec>(self.recs.capacity())),
            ("edge_groups", self.egroups.heap_bytes()),
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

    pub fn counts(&self) -> Counts {
        let e = |v: &[LevelIndex]| v.iter().map(LevelIndex::entries).sum::<usize>() as u64;
        Counts {
            records: (self.recs.len() - self.rec_nfree) as u64,
            owners: (self.owners.len() - self.own_nfree) as u64,
            nodes: self.nnodes,
            runs: self.ids.run_count() as u64,
            next_id: u64::from(self.ids.next_id()),
            dep_entries: e(&self.idx.dep),
            prec_entries: e(&self.idx.prec),
            node_entries: e(&self.idx.node),
            slot_rows: self.slots.rows() as u64,
            bytes: self.heap_bytes(),
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
        let idx = self.idx.node.get(cell.0 as usize)?;
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

    /// Owner domain and whether it is a family node.
    pub fn owner_dom(&self, o: u32) -> (u16, Rect, bool) {
        let w = &self.owners[o as usize];
        (w.sheet, w.dom, w.is_family())
    }

    // ------------------------------------------------------------ queries

    /// Direct dependents of cells `q` on `sheet` (§4.3 queries), as
    /// `(dependent sheet, rect)` pieces (they may overlap).
    pub fn direct_dependents(
        &self,
        sheet: u16,
        q: &Rect,
        filter: TagFilter,
        out: &mut Vec<(u16, Rect)>,
    ) -> u64 {
        let Some(idx) = self.idx.prec.get(sheet as usize) else {
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

    /// Direct precedents of one formula cell: `(tag, target sheet, rect)`.
    pub fn direct_precedents(
        &self,
        cell: Cell,
        filter: TagFilter,
        out: &mut Vec<(Tag, u16, Rect)>,
    ) -> u64 {
        let Some(idx) = self.idx.dep.get(cell.0 as usize) else {
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
mod verify;

pub use build::BuildInput;
pub use verify::Digest;
