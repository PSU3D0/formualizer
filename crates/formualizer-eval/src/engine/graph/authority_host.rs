//! `DependencyGraph` side of the Program 1 authority host (M1a).
//!
//! See `engine::authority::host` for the model. This module owns the
//! feature-gated hooks the graph calls and the legacy mirror used by the
//! differential gates Δ(a) (dirty closure) and Δ(e) (direct dependents).

use super::*;
use crate::engine::arena::string_interner::StringGarbage;

/// A family member that can reference its template: vertex and cell.
type CompressibleMember = (VertexId, (u16, u32, u32));
use crate::engine::authority::extract::{extract_formula, extract_symbol_binding};
use crate::engine::authority::geom::{Cell, Cover, Rect, SYMBOL_SHEET};
use crate::engine::authority::host::{AuthorityHost, HostState};
use crate::engine::authority::store::{
    AuthorityError, BuildInput, FormulaFacts, IdentifiedFacts, SharedBuildInput, Store, TagFilter,
};

/// Key of facts shared by family members: template, anchor cell, volatile, dynamic.
type SharedFactsKey = (AstNodeId, Cell, bool, bool);
use std::sync::OnceLock;

/// Differential self-check mode, from `FZ_AUTHORITY_DIFF`:
/// unset/`off`, `count` (counters only), `log:<path>` (append mismatches),
/// `strict` (panic on the first mismatch).
#[derive(Clone, Debug, PartialEq, Eq)]
enum DiffMode {
    Count,
    Log(String),
    Strict,
}

fn diff_mode() -> Option<&'static DiffMode> {
    static MODE: OnceLock<Option<DiffMode>> = OnceLock::new();
    MODE.get_or_init(|| {
        let v = std::env::var("FZ_AUTHORITY_DIFF").ok()?;
        match v.as_str() {
            "" | "off" => None,
            "count" => Some(DiffMode::Count),
            "strict" => Some(DiffMode::Strict),
            s => s.strip_prefix("log:").map(|p| DiffMode::Log(p.to_string())),
        }
    })
    .as_ref()
}

/// `FZ_AUTHORITY_CHECK=1`: run the store's invariant check after every
/// incremental mutation (debugging aid; quadratic).
fn debug_checks() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("FZ_AUTHORITY_CHECK").is_some())
}

fn append_lines(path: &str, lines: &[String]) {
    use std::io::Write as _;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let mut buf = String::new();
        for l in lines {
            buf.push_str(l);
            buf.push('\n');
        }
        let _ = f.write_all(buf.as_bytes());
    }
}

/// Formula-count ceiling for the propagation self-check (it runs the
/// legacy mirror, which is output-sensitive but unindexed).
const DIFF_MAX_FORMULAS: usize = 20_000;
const DIFF_MAX_SEEDS: usize = 256;

/// A precedent rectangle `(sheet, r0, c0, r1, c1)`, 0-based inclusive.
pub(crate) type PrecedentRect = (SheetId, u32, u32, u32, u32);

fn cell_of(c: &CellRef) -> Cell {
    (c.sheet_id, c.coord.row(), c.coord.col())
}

fn cell_ref(c: Cell) -> CellRef {
    CellRef::new(c.0, Coord::new(c.1, c.2, true, true))
}

impl DependencyGraph {
    pub(crate) fn authority_host(&self) -> &AuthorityHost {
        &self.authority
    }

    /// Put the host into the typed unsupported state (M3/M2 scope).
    pub(crate) fn authority_mark_unsupported(&mut self, operation: &'static str) {
        if !matches!(self.authority.state, HostState::Failed(_)) {
            self.authority.state = HostState::Failed(AuthorityError::Unsupported { operation });
            if let Some(DiffMode::Log(path)) = diff_mode() {
                let thread = std::thread::current();
                let who = thread.name().unwrap_or("?").to_string();
                append_lines(path, &[format!("{who}\tUNSUPPORTED {operation}")]);
            }
        }
    }

    fn authority_formula_input(&self, vid: VertexId) -> Option<BuildInput> {
        let (cell, facts) = self.authority_formula_input_shared(vid, None)?;
        Some((cell, std::sync::Arc::unwrap_or_clone(facts)))
    }

    /// [`Self::authority_formula_input`] with facts shared between the
    /// members of one family: a member's facts are its template's at the
    /// anchor, computed once per (template, anchor, flags) in `shared`.
    fn authority_formula_input_shared(
        &self,
        vid: VertexId,
        shared: Option<&mut FxHashMap<SharedFactsKey, (std::sync::Arc<FormulaFacts>, bool)>>,
    ) -> Option<SharedBuildInput> {
        let cell = self.get_cell_ref(vid)?;
        if self.store.is_deleted(vid) {
            return None;
        }
        let (sheet, row, col) = cell_of(&cell);
        // A compressed member's facts are its template's at the anchor
        // (relative, so equal to the member's own).
        let (ast, (arow, acol)) = match self.vertex_formulas.get(&vid)? {
            super::FormulaRef::Own(ast) => (ast, (row, col)),
            super::FormulaRef::Member { template, anchor } => (template, anchor),
        };
        let (volatile, dynamic) = (self.is_volatile(vid), self.is_dynamic(vid));
        let check_texts = !self.config.enable_parallel && self.config.formula_compression;
        let compute = || {
            let mut facts = extract_formula(self, sheet, arow, acol, ast, volatile, dynamic);
            // The template is valid at the anchor, not at the member: an
            // owner whose piece starts at this cell keeps that anchor.
            if (arow, acol) != (row, col) {
                facts.template_anchor = Some((arow, acol));
            }
            let rendered = !check_texts
                || formula_texts_rendered(
                    &self.data_store,
                    &self.sheet_reg,
                    ast,
                    &mut SheetPrefixes::new(),
                );
            (std::sync::Arc::new(facts), rendered)
        };
        let (mut facts, rendered) = match (shared, (arow, acol) != (row, col)) {
            (Some(shared), true) => shared
                .entry((ast, (sheet, arow, acol), volatile, dynamic))
                .or_insert_with(compute)
                .clone(),
            _ => compute(),
        };
        if self.authority_range_self_use_applies(vid, (sheet, row, col), &facts) {
            let facts = std::sync::Arc::make_mut(&mut facts);
            self.authority_apply_range_self_use(vid, (sheet, row, col), facts);
        }
        if !rendered {
            self.authority.texts_unrendered.lock().unwrap().insert(vid);
        }
        Some(((sheet, row, col), facts))
    }

    /// Legacy's #120 rule: a compressed range (open, or larger than the
    /// expansion limit) covering its own formula's cell is a self-loop,
    /// unless every use of it is a static `INDEX` whose selection excludes
    /// the cell (`compressed_range_self_use`). In that case the edge keeps
    /// the range minus the cell: up to four absolute pieces, and the formula
    /// stays an ungrouped singleton because its edges are no longer the
    /// template's.
    /// Whether [`Self::authority_apply_range_self_use`] would change
    /// `facts` at `cell` (read-only; shared facts are copied only then).
    fn authority_range_self_use_applies(
        &self,
        vid: VertexId,
        cell: Cell,
        facts: &crate::engine::authority::store::FormulaFacts,
    ) -> bool {
        use super::range_deps::RangeSelfUse;
        use crate::engine::authority::proj::Bound;
        use crate::engine::authority::store::OriginSpec;
        let (sheet, row, col) = cell;
        let limit = self.config.range_expansion_limit as u64;
        facts.edges.iter().any(|e| {
            if !matches!(e.origin, OriginSpec::Text) || e.proj.sheet != sheet {
                return false;
            }
            let Some(img) = e.proj.instantiate(row, col) else {
                return false;
            };
            if !(img.r0 <= row && row <= img.r1 && img.c0 <= col && col <= img.c1) {
                return false;
            }
            let (rows, cols) = (e.proj.rows, e.proj.cols);
            let open = [rows.lo, rows.hi, cols.lo, cols.hi].contains(&Bound::Open);
            let area = u64::from(img.r1 - img.r0 + 1) * u64::from(img.c1 - img.c0 + 1);
            if !open && area <= limit {
                return false;
            }
            let raw = |b: Bound, at: u32| match b {
                Bound::Open => None,
                Bound::Abs(v) => Some(v),
                Bound::Rel(d) => u32::try_from(i64::from(at) + i64::from(d)).ok(),
            };
            let range = (
                raw(rows.lo, row),
                raw(rows.hi, row),
                raw(cols.lo, col),
                raw(cols.hi, col),
            );
            self.compressed_range_self_use(vid, sheet, range) == RangeSelfUse::Excluded
        })
    }

    fn authority_apply_range_self_use(
        &self,
        vid: VertexId,
        cell: Cell,
        facts: &mut crate::engine::authority::store::FormulaFacts,
    ) {
        use super::range_deps::RangeSelfUse;
        use crate::engine::authority::proj::{AxisMap, Bound, RefProj};
        use crate::engine::authority::store::OriginSpec;
        let (sheet, row, col) = cell;
        let limit = self.config.range_expansion_limit as u64;
        let excluded = |e: &crate::engine::authority::store::EdgeSpec| -> Option<Rect> {
            if !matches!(e.origin, OriginSpec::Text) || e.proj.sheet != sheet {
                return None;
            }
            let img = e.proj.instantiate(row, col)?;
            if !(img.r0 <= row && row <= img.r1 && img.c0 <= col && col <= img.c1) {
                return None;
            }
            let (rows, cols) = (e.proj.rows, e.proj.cols);
            let open = [rows.lo, rows.hi, cols.lo, cols.hi].contains(&Bound::Open);
            let area = u64::from(img.r1 - img.r0 + 1) * u64::from(img.c1 - img.c0 + 1);
            if !open && area <= limit {
                return None;
            }
            let raw = |b: Bound, at: u32| match b {
                Bound::Open => None,
                Bound::Abs(v) => Some(v),
                Bound::Rel(d) => u32::try_from(i64::from(at) + i64::from(d)).ok(),
            };
            let range = (
                raw(rows.lo, row),
                raw(rows.hi, row),
                raw(cols.lo, col),
                raw(cols.hi, col),
            );
            (self.compressed_range_self_use(vid, sheet, range) == RangeSelfUse::Excluded)
                .then_some(img)
        };
        if !facts.edges.iter().any(|e| excluded(e).is_some()) {
            return;
        }
        let mut edges = Vec::with_capacity(facts.edges.len() + 3);
        for e in facts.edges.drain(..) {
            let Some(img) = excluded(&e) else {
                edges.push(e);
                continue;
            };
            let mut piece = |r0: u32, c0: u32, r1: u32, c1: u32| {
                if r0 <= r1 && c0 <= c1 {
                    edges.push(crate::engine::authority::store::EdgeSpec {
                        proj: RefProj {
                            sheet,
                            rows: AxisMap::fixed(r0, r1),
                            cols: AxisMap::fixed(c0, c1),
                        },
                        tag: e.tag,
                        origin: OriginSpec::Text,
                    });
                }
            };
            if row > img.r0 {
                piece(img.r0, img.c0, row - 1, img.c1);
            }
            if row < img.r1 {
                piece(row + 1, img.c0, img.r1, img.c1);
            }
            if col > img.c0 {
                piece(row, img.c0, row, col - 1);
            }
            if col < img.c1 {
                piece(row, col + 1, row, img.c1);
            }
        }
        edges.sort_unstable();
        edges.dedup();
        facts.edges = edges;
        facts.ltokens = None;
    }

    /// Rebuild the store from the graph's formulas (load, symbol revision,
    /// large batch). Identities are kept (decision 9): the previous store's
    /// live cells keep their ids and its counter continues. The candidate
    /// goes through the store's admission; a rejection fails the host with
    /// the typed error instead of installing a store above the budget.
    fn authority_rebuild(&mut self) {
        self.authority_sync_symbol_slots();
        let mut input = self.authority_formula_inputs();
        let symbols = self.authority_symbol_inputs();
        input.reserve(symbols.len());
        for (cell, facts) in symbols {
            // A name's node carries the name vertex's id.
            let Some(v) = self.authority.symbols.vertex(cell.1) else {
                continue;
            };
            input.push((
                cell,
                IdentifiedFacts {
                    id: v.0,
                    facts: std::sync::Arc::new(facts),
                },
            ));
        }
        self.authority_rebuild_from(input);
    }

    /// The rebuild itself. Every cell's id is its executor vertex's
    /// (Program 2: one id space), so identities follow the graph, which
    /// keeps a moved cell's vertex.
    fn authority_rebuild_from(&mut self, input: Vec<(Cell, IdentifiedFacts)>) {
        let budget = self.authority.store.budget;
        let prior = (self.authority.state != HostState::Unbuilt).then_some(&self.authority.store);
        // Enumerate live binding maps, not the vertex slab: deleted vertices
        // remain in the slab forever, so scanning it would charge live names
        // for all historical cell edits. This is identity inventory only;
        // executable symbol edges and planner units are installed separately.
        let count = self.name_vertex_lookup.len()
            + self.table_vertex_lookup.len()
            + self.source_vertex_lookup.len();
        let inventory_bytes = count * size_of::<SymbolAddr>();
        let input_bytes = input.capacity() * size_of::<(Cell, IdentifiedFacts)>()
            + input
                .iter()
                .map(|(_, f)| f.owned_heap_bytes())
                .sum::<usize>();
        let needed = (inventory_bytes + input_bytes) as u64 + prior.map_or(0, Store::heap_bytes);
        if let Some(limit) = budget.scratch.filter(|&limit| needed > limit) {
            self.authority.state = HostState::Failed(AuthorityError::Admission {
                resource: "scratch",
                needed,
                limit,
            });
            return;
        }
        let mut symbols = Vec::new();
        if symbols.try_reserve_exact(count).is_err() {
            self.authority.state = HostState::Failed(AuthorityError::Alloc);
            return;
        }
        let mut inventory_work = 0;
        for &vid in self
            .name_vertex_lookup
            .keys()
            .chain(self.table_vertex_lookup.keys())
            .chain(self.source_vertex_lookup.keys())
        {
            inventory_work += 1;
            if !self.store.is_deleted(vid)
                && let Some(symbol) = self.store.addr(vid).as_symbol()
            {
                symbols.push(symbol);
            }
        }
        symbols.sort_unstable_by(|a, b| {
            inventory_work += 1;
            a.cmp(b)
        });
        match Store::rebuild_carrying(input, symbols, prior, None, true, budget) {
            Ok(mut store) => {
                store.stats.symbol_work += inventory_work;
                self.authority.store = store;
                self.authority.symbol_rev = self.authority_applied_symbol_rev();
                self.authority.symbol_changes = Default::default();
                self.authority.builds += 1;
                self.authority.revision += 1;
                self.authority.clear_observed();
                self.authority.state = HostState::Ready;
            }
            Err(e) => self.authority.state = HostState::Failed(e),
        }
    }

    /// Bring the authority up to date with the graph's formulas, then mark
    /// the closure of dirty seeds that waited for it.
    ///
    /// Never inside a load scope (`first_load_assume_new`): the loader's
    /// cell map is flushed only when the scope ends, so formulas would not
    /// be found at their cells; the scope's end syncs.
    pub(crate) fn authority_sync(&mut self) {
        if self.first_load_assume_new {
            return;
        }
        self.authority_sync_store();
        if !self.authority.pending_dirty.is_empty()
            || !self.authority.pending_dirty_rects.is_empty()
            || !self.authority.pending_direct_dirty.is_empty()
            || !self.authority.pending_direct_dirty_runs.is_empty()
        {
            self.authority_flush_pending_dirty();
        }
    }

    fn authority_sync_store(&mut self) {
        let touched = self.vertex_formulas.take_touched();
        if matches!(self.authority.state, HostState::Failed(_)) {
            return;
        }
        if std::mem::take(&mut self.authority.structural_pending) {
            self.authority_structural_rebuild();
            return;
        }
        let mut touched = touched;
        let symbols_moved = self.authority.symbol_rev != self.symbol_revision
            || !self.authority.symbol_changes.names.is_empty()
            || self.authority.symbol_changes.other;
        // A bulk build is ~5x cheaper per formula than incremental
        // `set_formula`. Rebuild when the store holds no grid formula yet
        // and a batch arrives (the end of a load scope after an early empty
        // build, e.g. from `add_sheet`: applying every loaded formula one by
        // one made small-workbook loads ~1.6x legacy).
        // O(1): a maintained count, not a walk of the identity runs (a
        // per-sync walk made unrelated post-load mutations quadratic).
        let mut rebuild = self.authority.state == HostState::Unbuilt
            || (touched.len() > 1 && !self.authority.store.has_grid_formulas());
        if !rebuild && symbols_moved {
            match self.authority_sync_symbols_incremental(&mut touched) {
                Some(Ok(())) => {}
                Some(Err(e)) => {
                    self.authority.state = HostState::Failed(e);
                    return;
                }
                None => rebuild = true,
            }
        }
        rebuild =
            rebuild || (touched.len() > 4096 && touched.len() * 4 > self.vertex_formulas.len());
        if rebuild {
            // Every id is its vertex's: a rebuild keeps them all.
            self.authority_rebuild();
            return;
        }
        // A name whose formula re-bound (a pending reference resolved)
        // re-derives its symbol node.
        let names: Vec<VertexId> = touched
            .iter()
            .copied()
            .filter(|&v| self.get_cell_ref(v).is_none() && self.authority.symbols.slot(v).is_some())
            .collect();
        if let Err(e) = self.authority_set_symbol_nodes(&names) {
            self.authority.state = HostState::Failed(e);
            return;
        }
        let mut cells: Vec<Cell> = touched
            .iter()
            .filter(|&&v| self.store.vertex_exists(v))
            .filter_map(|&v| self.get_cell_ref(v).map(|c| cell_of(&c)))
            .collect();
        cells.sort_unstable();
        cells.dedup();
        for &v in &touched {
            self.authority.forget_observed(v);
        }
        for cell in cells {
            let current = self
                .get_vertex_for_cell(&cell_ref(cell))
                .filter(|v| self.vertex_formulas.contains_key(v));
            // The formula's id is its vertex's (one id space).
            let result = match current.and_then(|v| Some((v, self.authority_formula_input(v)?))) {
                Some((v, (c, mut facts))) => {
                    self.authority_verify_l(c.0, &mut facts);
                    self.authority.store.set_formula_given(c, &facts, v.0)
                }
                None => self.authority.store.clear_cell(cell),
            };
            self.authority.incremental_mutations += 1;
            self.authority.revision += 1;
            if debug_checks()
                && let Err(m) = self.authority.store.check()
            {
                panic!(
                    "after {cell:?} ({:?}): {m}\n{}",
                    current.is_some(),
                    self.authority.store.debug_cell(cell)
                );
            }
            if let Err(e) = result {
                self.authority.state = HostState::Failed(e);
                return;
            }
        }
    }

    /// A structural edit, move or sheet operation is about to mutate the
    /// graph (M3): the next sync rebuilds from the transformed formulas.
    /// Identities need no capture: a formula cell's id is its vertex's,
    /// which the graph keeps when it moves the cell (Program 2).
    ///
    /// `op` marks an operation boundary (row/column insert or delete, range
    /// move, sheet operation): a structural edit still pending from an
    /// earlier operation is synced first. Per-vertex moves (`move_vertex`,
    /// which replay issues once per moved cell) share the pending edit.
    pub(crate) fn authority_note_structural(&mut self, op: bool) {
        // Operation boundaries other than row/column shifts (range moves,
        // sheet operations) move cells and rewrite formulas one vertex at a
        // time: members get their per-cell maps and own ASTs back first
        // (Program 2 compression resumes after the next authority build).
        // A per-vertex move (`op` false) decompresses its own vertex only.
        if op {
            self.decompress_family_formulas();
        }
        self.authority_note_structural_inner(op);
    }

    /// A row/column insert or delete is about to mutate the graph: as
    /// [`Self::authority_note_structural`], but virtual member runs stay
    /// (the editor shifts them as blocks; `shift_virtual_runs`).
    pub(crate) fn authority_note_structural_shift(&mut self) {
        self.authority_note_structural_inner(true);
    }

    fn authority_note_structural_inner(&mut self, op: bool) {
        if self.authority.structural_pending {
            if !op {
                return;
            }
            self.authority_sync();
        }
        match self.authority.state {
            // Nothing is built yet, or the host failed: the next sync
            // builds from scratch or stays failed.
            HostState::Unbuilt | HostState::Failed(_) => return,
            HostState::Ready => {}
        }
        self.authority_sync();
        if self.authority.state != HostState::Ready {
            return;
        }
        self.authority.structural_pending = true;
    }

    /// The rebuild after structural mutations, from the transformed
    /// formulas (every moved cell keeps its vertex, and so its id).
    fn authority_structural_rebuild(&mut self) {
        // The dirty cover is in pre-edit coordinates; it is observational
        // (legacy's dirty flags drive scheduling) and restarts here.
        self.authority.dirty.clear();
        self.authority_rebuild();
        self.authority.structural_rebuilds += 1;
    }

    /// A top-level structural operation (row/column insert or delete, range
    /// move, sheet operation) returned: resync from the transformed
    /// formulas now, so dirty marks the operation queued reach their
    /// closure before anyone reads a flag. Per-cell moves of an undo/redo
    /// replay stay batched until the replay ends.
    pub(crate) fn authority_end_structural(&mut self) {
        if self.authority.structural_pending && self.authority.replaying {
            return;
        }
        if self.authority.structural_pending
            || !self.authority.pending_dirty.is_empty()
            || !self.authority.pending_dirty_rects.is_empty()
            || !self.authority.pending_direct_dirty.is_empty()
            || !self.authority.pending_direct_dirty_runs.is_empty()
        {
            self.authority_sync();
        }
    }

    /// Undo/redo replay begins (`mode`) or ends (`Forward`). Pending edits
    /// are synced first; while a replay runs, its per-cell moves stay
    /// batched until it ends.
    pub(crate) fn authority_set_replay(&mut self, mode: crate::engine::authority::history::Replay) {
        if self.authority.state == HostState::Ready || self.authority.structural_pending {
            self.authority_sync();
        }
        self.authority.replaying = mode != crate::engine::authority::history::Replay::Forward;
        if mode == crate::engine::authority::history::Replay::Forward {
            // A backward replay that stopped between a structural edit's
            // end and start markers leaves no pending extent undo behind.
            self.clear_extent_undone();
        }
        self.set_replay_mode(mode);
    }

    /// Live formula id at `cell` (tests: identity stability).
    pub(crate) fn authority_id_at(
        &mut self,
        cell: CellRef,
    ) -> Option<crate::engine::authority::identity::Vid> {
        self.authority().ok()?.store.ids().id_of(cell_of(&cell))
    }

    /// A formula joins an existing node group only if its L tokens equal
    /// the group representative's, re-derived from the arena: the store
    /// keys node groups by a 64-bit hash, and a collision must only lose
    /// sharing (the formula stays an ungrouped singleton).
    fn authority_verify_l(
        &self,
        sheet: SheetId,
        facts: &mut crate::engine::authority::store::FormulaFacts,
    ) {
        let Some(tokens) = facts.ltokens.as_deref() else {
            return;
        };
        if let Some((template, anchor)) = self.authority.store.l_representative(sheet, tokens) {
            let rep = crate::engine::authority::template::template_facts(
                &self.data_store,
                template,
                anchor.0,
                anchor.1,
            );
            if rep.tokens[..] != *tokens {
                facts.ltokens = None;
            }
        }
    }

    /// Direct readers of symbol `symbol` (a name, table or source): the
    /// formula cells and names with an edge to its row. Conservative (every
    /// formula and name) when the host is not ready.
    pub(crate) fn authority_symbol_readers(&mut self, symbol: VertexId) -> Vec<VertexId> {
        self.authority_sync_eager();
        let slot = self.authority.symbols.slot(symbol);
        if self.authority.state != HostState::Ready
            || self.authority.structural_pending
            || self.first_load_assume_new
        {
            let mut all: Vec<VertexId> = self
                .vertex_formulas
                .keys()
                .chain(self.name_vertex_lookup.keys().copied())
                .filter(|&v| v != symbol && !self.store.is_deleted(v))
                .collect();
            all.sort_unstable();
            return all;
        }
        let Some(slot) = slot else {
            return Vec::new();
        };
        let mut hits = Vec::new();
        self.authority.store.direct_dependents(
            SYMBOL_SHEET,
            &Rect::cell(slot, 0),
            TagFilter::All,
            &mut hits,
        );
        let mut out = Vec::new();
        for (sheet, r) in hits {
            for row in r.r0..=r.r1 {
                for col in r.c0..=r.c1 {
                    if let Some(v) = self.authority_vertex_of_cell((sheet, row, col))
                        && v != symbol
                    {
                        out.push(v);
                    }
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The readers legacy held as in-edges of `vertex` (cell references,
    /// ranges within `range_expansion_limit`, names over it): the cells
    /// `remove_vertex` marks `#REF!`. Read from the current store (the
    /// pre-edit store inside a structural operation, before anything moved).
    pub(crate) fn authority_in_edge_readers(&mut self, vertex: VertexId) -> Vec<VertexId> {
        if !self.authority.structural_pending {
            self.authority_sync_eager();
        }
        if self.authority.state != HostState::Ready {
            return Vec::new();
        }
        let Some(cell) = self.authority_cell_of_vertex(vertex) else {
            return Vec::new();
        };
        let limit = u64::try_from(self.config.range_expansion_limit)
            .unwrap_or(u64::MAX)
            .max(1);
        let mut hits = Vec::new();
        self.authority.store.direct_small_dependents(
            cell.0,
            &Rect::cell(cell.1, cell.2),
            limit,
            &mut hits,
        );
        let mut out = Vec::new();
        for (sheet, r) in hits {
            for row in r.r0..=r.r1 {
                for col in r.c0..=r.c1 {
                    if let Some(v) = self.authority_vertex_of_cell((sheet, row, col))
                        && v != vertex
                    {
                        out.push(v);
                    }
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Readers whose range contents a structural edit on `sheet` changes
    /// without their text changing: legacy's compressed-range rule per
    /// reader image (an insertion strictly inside the image, or an image
    /// meeting the deleted band) with the editor's cross-axis occupancy.
    /// Cell references never straddle; readers whose text the edit
    /// rewrites are dirtied by the rewrite. From the pre-edit store; every
    /// formula when the host is not ready.
    pub(crate) fn authority_structural_band_readers(
        &mut self,
        sheet: SheetId,
        edit: StructuralEdit,
        occupancy: &StructuralOccupancy,
    ) -> Vec<VertexId> {
        if !self.authority.structural_pending {
            self.authority_sync_eager();
        }
        if self.authority.state != HostState::Ready {
            let mut all: Vec<VertexId> = self.vertex_formulas.keys().collect();
            all.sort_unstable();
            return all;
        }
        use crate::engine::authority::geom::{MAX_COL, MAX_ROW};
        let band = match edit {
            StructuralEdit::DeleteRows { start, end } => Rect::new(start, 0, end, MAX_COL),
            StructuralEdit::InsertRows { before } => {
                Rect::new(before.saturating_sub(1), 0, before, MAX_COL)
            }
            StructuralEdit::DeleteColumns { start, end } => Rect::new(0, start, MAX_ROW, end),
            StructuralEdit::InsertColumns { before } => {
                Rect::new(0, before.saturating_sub(1), MAX_ROW, before)
            }
        };
        let mut cells: Vec<Cell> = Vec::new();
        self.authority
            .store
            .visit_dependent_images(sheet, &band, &mut |ds, row, col, img| {
                // An open range (whole column `A:A`, whole row `1:1`)
                // straddles an insertion at its first row/column too.
                let open_rows = img.r0 == 0 && img.r1 == MAX_ROW;
                let open_cols = img.c0 == 0 && img.c1 == MAX_COL;
                let axis = match edit {
                    StructuralEdit::DeleteRows { start, end } => img.r0 <= end && img.r1 >= start,
                    StructuralEdit::InsertRows { before } => {
                        (open_rows || img.r0 < before) && before <= img.r1
                    }
                    StructuralEdit::DeleteColumns { start, end } => {
                        img.c0 <= end && img.c1 >= start
                    }
                    StructuralEdit::InsertColumns { before } => {
                        (open_cols || img.c0 < before) && before <= img.c1
                    }
                };
                let (cross0, cross1) = match edit {
                    StructuralEdit::InsertRows { .. } | StructuralEdit::DeleteRows { .. } => {
                        (img.c0, img.c1)
                    }
                    StructuralEdit::InsertColumns { .. } | StructuralEdit::DeleteColumns { .. } => {
                        (img.r0, img.r1)
                    }
                };
                if axis && StructuralOccupancy::cross_axis_occupied(occupancy, edit, cross0, cross1)
                {
                    cells.push((ds, row, col));
                }
            });
        cells.sort_unstable();
        cells.dedup();
        let mut out: Vec<VertexId> = cells
            .into_iter()
            .filter_map(|c| self.authority_vertex_of_cell(c))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Direct precedents `(sheet, rect)` of `vertex`'s formula cell (symbol
    /// rows included, not looked through); `None` when the host cannot
    /// answer.
    pub(crate) fn authority_vertex_precedents(
        &mut self,
        vertex: VertexId,
    ) -> Option<Vec<(u16, Rect)>> {
        self.authority_sync_eager();
        if self.authority.state != HostState::Ready || self.vertex_formulas.has_touched() {
            return None;
        }
        let cell = self.authority_cell_of_vertex(vertex)?;
        let mut hits = Vec::new();
        self.authority
            .store
            .direct_precedents(cell, TagFilter::All, &mut hits);
        let mut out: Vec<(u16, Rect)> = hits.into_iter().map(|(_, s, r)| (s, r)).collect();
        out.sort_unstable();
        out.dedup();
        Some(out)
    }

    /// Whether dirty marks must wait for a resync (structural capture open
    /// or load scope).
    pub(crate) fn authority_defers_marks(&self) -> bool {
        self.authority.structural_pending || self.first_load_assume_new
    }

    /// Queue `vertex` as a dirty-propagation seed for after the resync.
    pub(crate) fn authority_queue_dirty(&mut self, vertex: VertexId) {
        self.authority.pending_dirty.push(vertex);
    }

    /// Queue `vertex` for `mark_dependents_dirty` after the resync: its
    /// direct in-edge readers (legacy's in-edges) get their dirty flag,
    /// without propagation, as legacy did.
    pub(crate) fn authority_queue_direct_dirty(&mut self, vertex: VertexId) {
        self.authority.pending_direct_dirty.push(vertex);
    }

    /// `mark_dependents_dirty` for every member of a moved run at once:
    /// rows `r0..=r1` of `col` on `sheet` (after the move).
    pub(crate) fn authority_queue_direct_dirty_run(
        &mut self,
        sheet: SheetId,
        col: u32,
        r0: u32,
        r1: u32,
    ) {
        if self.authority_defers_marks() {
            self.authority
                .pending_direct_dirty_runs
                .push((sheet, col, r0, r1));
        } else {
            self.authority_mark_direct_readers_of_run(sheet, col, r0, r1);
        }
    }

    /// Flag the direct in-edge readers of every cell of a run, exactly as
    /// `authority_in_edge_readers` per member would (a member reading only
    /// itself is not its own reader).
    fn authority_mark_direct_readers_of_run(&mut self, sheet: SheetId, col: u32, r0: u32, r1: u32) {
        if !self.authority.structural_pending {
            self.authority_sync_eager();
        }
        if self.authority.state != HostState::Ready {
            return;
        }
        let limit = u64::try_from(self.config.range_expansion_limit)
            .unwrap_or(u64::MAX)
            .max(1);
        let q = Rect::new(r0, col, r1, col);
        let mut readers: Vec<Cell> = Vec::new();
        self.authority.store.visit_direct_small_dependents(
            sheet,
            &q,
            limit,
            &mut |ds, row, c, img| {
                if img.r0 > q.r1 || img.r1 < q.r0 || img.c0 > col || img.c1 < col {
                    return;
                }
                if ds == sheet && q.contains(row, c) {
                    // Only a self read of (row, c) when the image meets q
                    // in that one cell.
                    let lo = img.r0.max(q.r0);
                    let hi = img.r1.min(q.r1);
                    let only_self = lo == hi && lo == row && img.c0 <= c && c <= img.c1;
                    if only_self {
                        return;
                    }
                }
                readers.push((ds, row, c));
            },
        );
        readers.sort_unstable();
        readers.dedup();
        for cell in readers {
            if let Some(v) = self.authority_vertex_of_cell(cell) {
                self.store.set_dirty(v, true);
                self.formula_dirty.legacy_insert(v);
            }
        }
    }

    /// Sync unless a load scope or a structural capture is open.
    pub(crate) fn authority_sync_eager(&mut self) {
        if !self.first_load_assume_new && !self.authority.structural_pending {
            self.authority_sync();
        }
    }

    /// Sync when the host is built and no structural capture is open.
    pub(crate) fn authority_sync_if_ready(&mut self) {
        if self.authority.state == HostState::Ready && !self.authority.structural_pending {
            self.authority_sync();
        }
    }

    /// Sync, then the host if it is ready.
    pub(crate) fn authority(&mut self) -> Result<&AuthorityHost, AuthorityError> {
        self.authority_sync();
        match &self.authority.state {
            HostState::Failed(e) => Err(e.clone()),
            _ => Ok(&self.authority),
        }
    }

    /// The store a read-only planning path may use: the host must be
    /// ready and synced with the graph (no pending formula changes, no
    /// symbol revision since the last build).
    pub(crate) fn authority_plan_store(&self) -> Result<&Store, AuthorityError> {
        match &self.authority.state {
            HostState::Failed(e) => Err(e.clone()),
            HostState::Unbuilt => Err(AuthorityError::Stale),
            HostState::Ready
                if self.authority.symbol_rev != self.symbol_revision
                    || self.vertex_formulas.has_touched() =>
            {
                Err(AuthorityError::Stale)
            }
            HostState::Ready => Ok(&self.authority.store),
        }
    }

    /// Mutable host access (tests: budgets).
    pub(crate) fn authority_host_mut(&mut self) -> &mut AuthorityHost {
        &mut self.authority
    }

    /// Direct dependents of `cell` (R-1X): sorted formula cells.
    pub(crate) fn authority_direct_dependents(
        &mut self,
        cell: CellRef,
    ) -> Result<Vec<CellRef>, AuthorityError> {
        let host = self.authority()?;
        let c = cell_of(&cell);
        let cover = Self::authority_direct_grid_dependents(&host.store, c.0, &Rect::cell(c.1, c.2));
        Ok(cover.cells().into_iter().map(cell_ref).collect())
    }

    /// Transitive dependents (the dirty closure) of `cells`: sorted.
    pub(crate) fn authority_dependents(
        &mut self,
        cells: &[CellRef],
    ) -> Result<Vec<CellRef>, AuthorityError> {
        let host = self.authority()?;
        let seeds: Vec<(u16, Rect)> = cells
            .iter()
            .map(|c| (c.sheet_id, Rect::cell(c.coord.row(), c.coord.col())))
            .collect();
        let (cover, _) = host.store.dependents(&seeds, TagFilter::All);
        Ok(cover
            .cells()
            .into_iter()
            .filter(|c| c.0 != SYMBOL_SHEET)
            .map(cell_ref)
            .collect())
    }

    /// Inspection's direct readers of `cell` (text-origin edges, grid
    /// formula cells), bounded by `remaining` work units (one per reader);
    /// `Ok(false)` when the budget ran out. Needs a synced host.
    pub(crate) fn authority_visit_text_dependents(
        &self,
        cell: Cell,
        remaining: &mut u64,
        visitor: &mut dyn FnMut(Cell) -> bool,
    ) -> Result<bool, AuthorityError> {
        let store = self.authority_plan_store()?;
        if *remaining == 0 {
            return Ok(false);
        }
        // Streamed from the index: each reader is produced (and charged)
        // once, and the walk stops at the budget, so a small budget
        // enumerates no more than it reports (red team #2). `seen` holds
        // only readers already reported.
        let mut seen = Cover::new();
        let mut miss: Vec<(u32, u32)> = Vec::new();
        let mut complete = true;
        store.visit_text_dependents(cell.0, &Rect::cell(cell.1, cell.2), &mut |s, d| {
            if s == SYMBOL_SHEET {
                return true;
            }
            for c in d.c0..=d.c1 {
                miss.clear();
                seen.missing_in(s, c, d.r0, d.r1, &mut miss);
                for &(a, b) in &miss {
                    for r in a..=b {
                        if *remaining == 0 {
                            complete = false;
                            return false;
                        }
                        *remaining -= 1;
                        seen.insert_rect(s, &Rect::cell(r, c));
                        if !visitor((s, r, c)) {
                            return false;
                        }
                    }
                }
            }
            true
        });
        Ok(complete)
    }

    /// Direct precedents of a formula cell: `(sheet, r0, c0, r1, c1)`.
    pub(crate) fn authority_precedents(
        &mut self,
        cell: CellRef,
    ) -> Result<Vec<PrecedentRect>, AuthorityError> {
        let host = self.authority()?;
        let mut hits = Vec::new();
        host.store
            .direct_grid_precedents(cell_of(&cell), TagFilter::All, &mut hits);
        let mut v: Vec<_> = hits
            .into_iter()
            .map(|(_, s, r)| (s, r.r0, r.c0, r.r1, r.c1))
            .collect();
        v.sort_unstable();
        v.dedup();
        Ok(v)
    }

    // ------------------------------------------------------------ hooks

    fn is_dirtyable_kind(&self, v: VertexId) -> bool {
        matches!(
            self.store.kind(v),
            VertexKind::FormulaScalar
                | VertexKind::FormulaArray
                | VertexKind::NamedScalar
                | VertexKind::NamedArray
        )
    }

    /// Dirty propagation (M5: the authority is the dependency path). Marks
    /// every formula or name among `seeds` and the transitive dependents of
    /// all seeds, like legacy's BFS; returns the affected set (the seeds,
    /// value sources included, and the dependents). Three cases do not ask
    /// the store:
    /// - load before any flag was cleared: everything is dirty already
    ///   (`authority_load_skips_closures`), only the seeds are marked;
    /// - mid structural edit the store is pre-edit: the seeds wait in
    ///   `pending_dirty` and their closure is marked right after the resync;
    /// - a failed host (a typed error evaluation will report): every
    ///   formula is marked, conservatively.
    pub(super) fn authority_mark_dirty(&mut self, seeds: &[VertexId]) -> Vec<VertexId> {
        let mut affected: FxHashSet<VertexId> = seeds.iter().copied().collect();
        for &v in seeds {
            if self.is_dirtyable_kind(v) {
                self.store.set_dirty(v, true);
            }
        }
        if !self.authority_load_skips_closures() {
            if self.authority.structural_pending || self.first_load_assume_new {
                self.authority.pending_dirty.extend_from_slice(seeds);
            } else {
                self.authority_sync();
                self.authority_mark_closure(seeds, &mut affected);
            }
        }
        // Only evaluable kinds enter the dirty set: value seeds are never
        // evaluated, so nothing would ever remove them, and the set (which
        // every pass iterates) would grow with each distinct edited cell.
        let dirtyable: Vec<VertexId> = affected
            .iter()
            .copied()
            .filter(|&v| self.is_dirtyable_kind(v))
            .collect();
        self.formula_dirty.legacy_extend(dirtyable);
        affected.into_iter().collect()
    }

    /// Mark the closure of `seeds` (the host is synced), adding it to
    /// `affected`.
    fn authority_mark_closure(&mut self, seeds: &[VertexId], affected: &mut FxHashSet<VertexId>) {
        if self.authority.state != HostState::Ready {
            let all: Vec<VertexId> = self
                .vertex_formulas
                .keys()
                .chain(self.name_vertex_lookup.keys().copied())
                .filter(|&v| !self.store.is_deleted(v))
                .collect();
            for v in all {
                self.store.set_dirty(v, true);
                affected.insert(v);
            }
            return;
        }
        let before = affected.len();
        let closure = self.authority_closure_vertices(seeds);
        self.dirty_propagation_visits += closure.len() as u64;
        for &v in &closure {
            self.store.set_dirty(v, true);
            affected.insert(v);
        }
        let _ = before;
        if diff_mode().is_some() {
            self.authority_diff_propagation(seeds, &closure);
        }
    }

    /// Mark the closure of seeds queued while the store lagged (after a
    /// structural resync).
    fn authority_flush_pending_dirty(&mut self) {
        if self.authority.structural_pending {
            return;
        }
        let runs = std::mem::take(&mut self.authority.pending_direct_dirty_runs);
        for (sheet, col, r0, r1) in runs {
            self.authority_mark_direct_readers_of_run(sheet, col, r0, r1);
        }
        let direct = std::mem::take(&mut self.authority.pending_direct_dirty);
        // Moved grid cells in vertical runs are queried a run at a time
        // (the union of the per-cell queries; a cell queued for one vertex
        // only); anything else per vertex.
        let mut cells: Vec<(Cell, VertexId)> = Vec::with_capacity(direct.len());
        let mut single: Vec<VertexId> = Vec::new();
        for v in direct {
            if !self.store.vertex_exists(v) || self.store.is_deleted(v) {
                continue;
            }
            match self.get_cell_ref(v) {
                Some(c) => cells.push((cell_of(&c), v)),
                None => single.push(v),
            }
        }
        cells.sort_unstable();
        cells.dedup();
        let mut i = 0;
        while i < cells.len() {
            let ((sheet, row, col), _) = cells[i];
            if cells.get(i + 1).is_some_and(|n| n.0 == cells[i].0) {
                // One cell for several vertices (a stale one): per vertex.
                let mut j = i;
                while j < cells.len() && cells[j].0 == cells[i].0 {
                    single.push(cells[j].1);
                    j += 1;
                }
                i = j;
                continue;
            }
            let mut j = i + 1;
            while j < cells.len()
                && cells[j].0 == (sheet, row + (j - i) as u32, col)
                && cells.get(j + 1).is_none_or(|n| n.0 != cells[j].0)
            {
                j += 1;
            }
            self.authority_mark_direct_readers_of_run(sheet, col, row, row + (j - i - 1) as u32);
            i = j;
        }
        for v in single {
            for reader in self.authority_in_edge_readers(v) {
                self.store.set_dirty(reader, true);
                self.formula_dirty.legacy_insert(reader);
            }
        }
        let rects = std::mem::take(&mut self.authority.pending_dirty_rects);
        if !rects.is_empty() {
            let mut affected = FxHashSet::default();
            self.authority_mark_rect_closure(&rects, &mut affected);
            self.formula_dirty.legacy_extend(affected.iter().copied());
        }
        if self.authority.pending_dirty.is_empty() {
            return;
        }
        let seeds: Vec<VertexId> = std::mem::take(&mut self.authority.pending_dirty)
            .into_iter()
            .filter(|&v| self.store.vertex_exists(v) && !self.store.is_deleted(v))
            .collect();
        let mut affected = FxHashSet::default();
        self.authority_mark_closure(&seeds, &mut affected);
        self.formula_dirty.legacy_extend(affected.iter().copied());
    }

    /// The executor vertices of the transitive dependents (positive
    /// length) of `seeds`: formula cells through the identity side array,
    /// symbol rows to their name vertices.
    pub(crate) fn authority_closure_vertices(&self, seeds: &[VertexId]) -> Vec<VertexId> {
        let cells: Vec<Cell> = seeds
            .iter()
            .filter_map(|&v| self.authority_cell_of_vertex(v))
            .collect();
        self.authority_closure_of_cells(&cells)
    }

    /// Dirty propagation seeded by cells (a value edit: the cell has no
    /// vertex, decision 27): marks the transitive dependents of `cells`,
    /// with the same deferral cases as [`Self::authority_mark_dirty`];
    /// returns them.
    pub(super) fn authority_mark_dirty_cells(&mut self, cells: &[Cell]) -> Vec<VertexId> {
        let rects: Vec<(u16, Rect)> = cells.iter().map(|c| (c.0, Rect::cell(c.1, c.2))).collect();
        self.authority_mark_dirty_rects(&rects)
    }

    /// [`Self::authority_mark_dirty_cells`] for rectangles.
    pub(super) fn authority_mark_dirty_rects(&mut self, rects: &[(u16, Rect)]) -> Vec<VertexId> {
        let mut affected: FxHashSet<VertexId> = FxHashSet::default();
        if rects.is_empty() || self.authority_load_skips_closures() {
            return Vec::new();
        }
        if self.authority.structural_pending || self.first_load_assume_new {
            self.authority.pending_dirty_rects.extend_from_slice(rects);
            return Vec::new();
        }
        self.authority_sync();
        self.authority_mark_rect_closure(rects, &mut affected);
        let dirtyable: Vec<VertexId> = affected
            .iter()
            .copied()
            .filter(|&v| self.is_dirtyable_kind(v))
            .collect();
        self.formula_dirty.legacy_extend(dirtyable);
        affected.into_iter().collect()
    }

    /// Mark the closure of `rects` (the host is synced).
    fn authority_mark_rect_closure(
        &mut self,
        rects: &[(u16, Rect)],
        affected: &mut FxHashSet<VertexId>,
    ) {
        if self.authority.state != HostState::Ready {
            let all: Vec<VertexId> = self
                .vertex_formulas
                .keys()
                .chain(self.name_vertex_lookup.keys().copied())
                .filter(|&v| !self.store.is_deleted(v))
                .collect();
            for v in all {
                self.store.set_dirty(v, true);
                affected.insert(v);
            }
            return;
        }
        let closure = self.authority_closure_of_rects(rects);
        self.dirty_propagation_visits += closure.len() as u64;
        for &v in &closure {
            self.store.set_dirty(v, true);
            affected.insert(v);
        }
        #[cfg(any(test, feature = "legacy_oracle"))]
        if let Some(mode) = diff_mode()
            && matches!(mode, DiffMode::Strict)
            && self.vertex_formulas.len() <= DIFF_MAX_FORMULAS
            && rects.iter().all(|(_, r)| r.r0 == r.r1 && r.c0 == r.c1)
        {
            let grid: Vec<Cell> = rects
                .iter()
                .filter(|(s, _)| *s != SYMBOL_SHEET)
                .map(|(s, r)| (*s, r.r0, r.c0))
                .collect();
            let legacy = self.legacy_closure_cells(&grid);
            let mut mine: Vec<Cell> = closure
                .iter()
                .filter_map(|&v| self.get_cell_ref(v).map(|c| cell_of(&c)))
                .collect();
            mine.sort_unstable();
            mine.dedup();
            assert_eq!(legacy, mine, "cell-seeded dirty closure of {grid:?}");
        }
    }

    /// The executor vertices of the transitive dependents of `cells`.
    pub(crate) fn authority_closure_of_cells(&self, cells: &[Cell]) -> Vec<VertexId> {
        let rects: Vec<(u16, Rect)> = cells.iter().map(|c| (c.0, Rect::cell(c.1, c.2))).collect();
        self.authority_closure_of_rects(&rects)
    }

    /// The executor vertices of the transitive dependents of `rects`.
    pub(crate) fn authority_closure_of_rects(&self, rects: &[(u16, Rect)]) -> Vec<VertexId> {
        if rects.is_empty() {
            return Vec::new();
        }
        let (cover, _) = self.authority.store.dependents(rects, TagFilter::All);
        let ids = self.authority.store.ids();
        let mut out = Vec::new();
        let mut runs = Vec::new();
        for (s, c, a, b) in cover.column_intervals() {
            if s == SYMBOL_SHEET {
                out.extend((a..=b).filter_map(|r| self.authority.symbols.vertex(r)));
                continue;
            }
            runs.clear();
            ids.runs_in(s, c, a, b, &mut runs);
            for &h in &runs {
                let run = ids.run(h);
                let r0 = run.row_start.max(a);
                let r1 = (run.row_start + run.len - 1).min(b);
                for row in r0..=r1 {
                    let id = run.first_id + (row - run.row_start);
                    if let Some(v) = self.authority_vertex_of_formula(id, (s, row, c)) {
                        out.push(v);
                    }
                }
            }
        }
        out
    }

    /// Differential self-check (`FZ_AUTHORITY_DIFF`): compare the
    /// authority's closure of `seeds` with legacy's mirror (Δ(a)), and
    /// direct dependents per seed (Δ(e)).
    fn authority_diff_propagation(&mut self, seeds: &[VertexId], closure: &[VertexId]) {
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        let _ = (&seeds, &closure);
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            let Some(mode) = diff_mode() else {
                return;
            };
            let cells: Vec<Cell> = seeds
                .iter()
                .filter_map(|&v| self.authority_cell_of_vertex(v))
                .collect();
            self.authority.diff.propagations += 1;
            if self.vertex_formulas.len() > DIFF_MAX_FORMULAS || cells.len() > DIFF_MAX_SEEDS {
                self.authority.diff.skipped += 1;
                return;
            }
            let grid: Vec<Cell> = cells
                .iter()
                .copied()
                .filter(|c| c.0 != SYMBOL_SHEET)
                .collect();
            let legacy = self.legacy_closure_cells(&grid);
            let mut mine: Vec<Cell> = closure
                .iter()
                .filter_map(|&v| self.get_cell_ref(v).map(|c| cell_of(&c)))
                .collect();
            mine.sort_unstable();
            mine.dedup();
            let mut problems: Vec<String> = Vec::new();
            if legacy != mine && cells.iter().all(|c| c.0 != SYMBOL_SHEET) {
                self.authority.diff.closure_mismatches += 1;
                let only_legacy: Vec<&Cell> = legacy
                    .iter()
                    .filter(|c| mine.binary_search(c).is_err())
                    .collect();
                let only_mine: Vec<&Cell> = mine
                    .iter()
                    .filter(|c| legacy.binary_search(c).is_err())
                    .collect();
                problems.push(format!(
                "dirty seeds={cells:?} legacy={} authority={} only_legacy={:?} only_authority={:?}",
                legacy.len(),
                mine.len(),
                only_legacy.iter().take(8).collect::<Vec<_>>(),
                only_mine.iter().take(8).collect::<Vec<_>>(),
            ));
            }
            for &c in grid.iter() {
                self.authority.diff.checked_seeds += 1;
                let legacy = self.legacy_direct_dependent_cells(c);
                let mine = Self::authority_direct_grid_dependents(
                    &self.authority.store,
                    c.0,
                    &Rect::cell(c.1, c.2),
                )
                .cells();
                if legacy != mine {
                    self.authority.diff.direct_mismatches += 1;
                    problems.push(format!(
                        "direct seed={c:?} legacy={legacy:?} authority={mine:?}"
                    ));
                }
            }
            let thread = std::thread::current();
            let who = thread.name().unwrap_or("?");
            match mode {
                DiffMode::Count => {}
                DiffMode::Strict => {
                    if !problems.is_empty() {
                        panic!("unified_authority differential mismatch in {who}: {problems:?}");
                    }
                }
                DiffMode::Log(path) => {
                    let mut lines = vec![format!("{who}\tCHECK seeds={}", cells.len())];
                    lines.extend(problems);
                    append_lines(path, &lines);
                }
            }
        }
    }

    /// Legacy cleared dirty flags after evaluating `vertices`.
    pub(super) fn authority_observe_clean(&mut self, vertices: &[VertexId]) {
        if self.authority.dirty.is_empty() {
            return;
        }
        for &v in vertices {
            if let Some((s, r, col)) = self.authority_cell_of_vertex(v) {
                self.authority.dirty.clean(s, &Rect::cell(r, col));
            }
        }
    }

    // ------------------------------------------------------------ legacy mirror

    fn is_formula_cell_vertex(&self, v: VertexId) -> bool {
        matches!(
            self.store.kind(v),
            VertexKind::FormulaScalar | VertexKind::FormulaArray
        ) && self.store.grid_addr(v).is_some()
            && self.vertex_formulas.contains_key(&v)
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Legacy's direct dependents of one cell: CSR in-edges, name links and
    /// precise range subscriptions, expanding symbol vertices (names,
    /// tables, sources) transparently. Sorted formula cells.
    pub(crate) fn legacy_direct_dependent_cells(&self, cell: Cell) -> Vec<Cell> {
        let mut out: FxHashSet<Cell> = FxHashSet::default();
        let mut symbols: Vec<VertexId> = Vec::new();
        let mut seen: FxHashSet<VertexId> = FxHashSet::default();
        let mut take = |g: &Self, v: VertexId, symbols: &mut Vec<VertexId>| {
            if g.is_formula_cell_vertex(v) {
                if let Some(c) = g.get_cell_ref(v) {
                    out.insert(cell_of(&c));
                }
            } else if g.store.grid_addr(v).is_none() {
                symbols.push(v);
            }
        };
        if let Some(v) = self.get_vertex_for_cell(&cell_ref(cell)) {
            for d in self.get_dependents(v) {
                take(self, d, &mut symbols);
            }
            if let Some(names) = self.cell_to_name_dependents.get(&v) {
                for &n in names {
                    take(self, n, &mut symbols);
                }
            }
        } else {
            // Readers of a cell without a vertex (decision 27).
            for d in self.oracle_vertexless_readers_of(cell) {
                take(self, d, &mut symbols);
            }
        }
        for d in self.collect_range_dependents_for_rect(cell.0, cell.1, cell.2, cell.1, cell.2) {
            take(self, d, &mut symbols);
        }
        while let Some(s) = symbols.pop() {
            if !seen.insert(s) {
                continue;
            }
            for d in self.get_dependents(s) {
                take(self, d, &mut symbols);
            }
        }
        let mut v: Vec<Cell> = out.into_iter().collect();
        v.sort_unstable();
        v
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Legacy's dirty closure of `cells` with exact per-source semantics
    /// (`mark_dirty_many`'s BFS without its bounding-rect shortcut and
    /// without mutating dirty flags): formula cells reached by a path of
    /// positive length. Sorted.
    pub(crate) fn legacy_closure_cells(&self, cells: &[Cell]) -> Vec<Cell> {
        let mut visited: FxHashSet<VertexId> = FxHashSet::default();
        let mut to_visit: Vec<VertexId> = Vec::new();
        for &c in cells {
            if let Some(v) = self.get_vertex_for_cell(&cell_ref(c)) {
                to_visit.extend(self.get_dependents(v));
                if let Some(names) = self.cell_to_name_dependents.get(&v) {
                    to_visit.extend(names.iter().copied());
                }
            } else {
                to_visit.extend(self.oracle_vertexless_readers_of(c));
            }
            to_visit.extend(self.collect_range_dependents_for_rect(c.0, c.1, c.2, c.1, c.2));
        }
        while let Some(id) = to_visit.pop() {
            if !visited.insert(id) {
                continue;
            }
            to_visit.extend(self.get_dependents(id));
            to_visit.extend(self.collect_range_dependents_for_vertex(id));
        }
        let mut v: Vec<Cell> = visited
            .into_iter()
            .filter(|&v| self.is_formula_cell_vertex(v))
            .filter_map(|v| self.get_cell_ref(v).map(|c| cell_of(&c)))
            .collect();
        v.sort_unstable();
        v
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Formula cells among what a legacy dirty propagation affected (the
    /// Δ(a) comparator: value sources and symbol vertices are filtered out).
    /// Sorted.
    pub(crate) fn legacy_dirty_cells(&self, affected: &FxHashSet<VertexId>) -> Vec<Cell> {
        let mut v: Vec<Cell> = affected
            .iter()
            .filter(|&&v| self.is_formula_cell_vertex(v))
            .filter_map(|&v| self.get_cell_ref(v).map(|c| cell_of(&c)))
            .collect();
        v.sort_unstable();
        v
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Dirty propagation from `cells` (their vertices; cells without a
    /// vertex are skipped): legacy's mirror closure (formula seeds plus
    /// `legacy_closure_cells`) and the formula cells the authority's actual
    /// propagation dirtied. Gates only.
    pub(crate) fn dirty_propagation_pair(
        &mut self,
        cells: &[Cell],
    ) -> Result<(Vec<Cell>, Vec<Cell>), AuthorityError> {
        self.authority()?;
        let vids: Vec<VertexId> = cells
            .iter()
            .filter_map(|&c| self.get_vertex_for_cell(&cell_ref(c)))
            .collect();
        // Cells without a vertex (value cells, decision 27) seed by cell.
        let vertexless: Vec<Cell> = cells
            .iter()
            .copied()
            .filter(|&c| self.get_vertex_for_cell(&cell_ref(c)).is_none())
            .collect();
        let mut affected: FxHashSet<VertexId> = self.mark_dirty_many(&vids).into_iter().collect();
        affected.extend(self.mark_dirty_cells(&vertexless));
        if let HostState::Failed(e) = &self.authority.state {
            return Err(e.clone());
        }
        let mine = self.legacy_dirty_cells(&affected);
        let mut legacy = self.legacy_closure_cells(cells);
        legacy.extend(
            vids.iter()
                .filter(|&&v| self.is_formula_cell_vertex(v))
                .filter_map(|&v| self.get_cell_ref(v).map(|c| cell_of(&c))),
        );
        legacy.sort_unstable();
        legacy.dedup();
        Ok((legacy, mine))
    }

    // ------------------------------------------------------------ memory gate

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Heap bytes of the legacy dependency structures by component (Packet
    /// B's accounting: capacities, hashbrown allocations; the per-sheet
    /// vertex interval index is reported but excluded from the gate total,
    /// which is conservative for the authority).
    pub(crate) fn legacy_dependency_bytes(&self) -> Vec<(&'static str, usize)> {
        use crate::engine::authority::dir::hash_table_bytes;
        let (csr, delta, side) = self.edges.authority_gate_heap_bytes();
        let range_deps = hash_table_bytes::<(VertexId, Vec<SharedRangeRef<'static>>)>(
            self.formula_to_range_deps.capacity(),
        ) + self
            .formula_to_range_deps
            .values()
            .map(|v| v.capacity() * size_of::<SharedRangeRef<'static>>())
            .sum::<usize>();
        let stripes = hash_table_bytes::<(StripeKey, FxHashSet<VertexId>)>(
            self.stripe_to_dependents.capacity(),
        ) + self
            .stripe_to_dependents
            .values()
            .map(|s| hash_table_bytes::<VertexId>(s.capacity()))
            .sum::<usize>();
        let names = hash_table_bytes::<(VertexId, Vec<VertexId>)>(self.vertex_to_names.capacity())
            + hash_table_bytes::<(VertexId, FxHashSet<VertexId>)>(
                self.cell_to_name_dependents.capacity(),
            )
            + hash_table_bytes::<(VertexId, Vec<VertexId>)>(
                self.name_to_cell_dependencies.capacity(),
            );
        vec![
            ("vertex_store", self.store.authority_gate_heap_bytes()),
            ("csr_base", csr),
            ("csr_delta", delta),
            ("csr_side_tables", side),
            (
                "cell_to_vertex",
                hash_table_bytes::<(CellRef, VertexId)>(self.cell_to_vertex.capacity()),
            ),
            (
                "load_packed_to_vertex",
                hash_table_bytes::<(PackedSheetCell, VertexId)>(
                    self.load_packed_to_vertex.capacity(),
                ),
            ),
            ("formula_to_range_deps", range_deps),
            ("stripe_to_dependents", stripes),
            ("name_links", names),
        ]
    }

    /// The build input the host would use for a full rebuild: every formula
    /// cell, then every name's symbol node.
    pub(crate) fn authority_build_input(&self) -> Vec<BuildInput> {
        let mut input: Vec<BuildInput> = self
            .authority_formula_inputs()
            .into_iter()
            .map(|(cell, facts)| (cell, std::sync::Arc::unwrap_or_clone(facts.facts)))
            .collect();
        input.extend(
            self.authority
                .symbols
                .iter()
                .filter_map(|(slot, v)| self.authority_symbol_binding(slot, v).map(|(i, _)| i)),
        );
        input
    }

    /// Every formula cell's build input, with its vertex as its id.
    fn authority_formula_inputs(&self) -> Vec<(Cell, IdentifiedFacts)> {
        let vids: Vec<VertexId> = self.vertex_formulas.keys().collect();
        // Family members share their template's facts (one allocation per
        // family instead of one per cell).
        let mut shared = FxHashMap::default();
        vids.into_iter()
            .filter_map(|v| {
                let (cell, facts) = self.authority_formula_input_shared(v, Some(&mut shared))?;
                Some((cell, IdentifiedFacts { id: v.0, facts }))
            })
            .collect()
    }

    /// Name `vertex`'s node at row `slot`, with the binding keys its
    /// definition waits on.
    fn authority_symbol_binding(
        &self,
        slot: u32,
        vertex: VertexId,
    ) -> Option<(BuildInput, Vec<Box<str>>)> {
        let (_, name) = self.name_vertex_lookup.get(&vertex)?;
        let entry = self.named_range_by_vertex(vertex)?;
        let (facts, missing) = extract_symbol_binding(
            self,
            name,
            entry,
            self.is_volatile(vertex),
            self.is_dynamic(vertex),
        );
        Some((((SYMBOL_SHEET, slot, 0), facts), missing))
    }

    /// Every name node's build input; the bindings they wait on replace
    /// `AuthorityHost::unbound` (a rebuild re-derives every node).
    fn authority_symbol_inputs(&mut self) -> Vec<BuildInput> {
        let mut input = Vec::with_capacity(self.authority.symbols.len());
        let mut unbound: Vec<(VertexId, Vec<Box<str>>)> = Vec::new();
        for (slot, v) in self.authority.symbols.iter() {
            if let Some((i, missing)) = self.authority_symbol_binding(slot, v) {
                input.push(i);
                if !missing.is_empty() {
                    unbound.push((v, missing));
                }
            }
        }
        self.authority.unbound.clear();
        for (v, missing) in unbound {
            self.authority.note_unbound(v, missing);
        }
        input
    }

    /// Load (`first_load_assume_new`) before any dirty flag was ever
    /// cleared: every formula is dirty, so a dependency closure cannot
    /// change a flag, and the authority is not synced (must-fix 1: it is
    /// built once, at the first request, instead of at every propagation
    /// and symbol revision of the load).
    pub(crate) fn authority_load_skips_closures(&self) -> bool {
        if !self.first_load_assume_new || self.store.dirty_ever_cleared() {
            return false;
        }
        #[cfg(test)]
        for v in self.vertex_formulas.keys() {
            debug_assert!(
                self.store.is_deleted(v) || self.store.is_dirty(v),
                "load closure skip requires every formula dirty; {v:?} is clean"
            );
        }
        true
    }

    /// Log a symbol definition change for the next sync: `Some(vertex)` of
    /// the name, table or source; `None` forces a rebuild.
    pub(crate) fn authority_note_symbol(&mut self, name: Option<VertexId>) {
        // Every logging site bumps the symbol revision once, after its
        // logs: a sync in between applies the change for that revision.
        self.authority.symbol_rev_target = Some(self.symbol_revision.wrapping_add(1));
        match name {
            Some(v) => self.authority.symbol_changes.names.push(v),
            None => self.authority.symbol_changes.other = true,
        }
    }

    /// Apply logged symbol changes without a rebuild (M4), in work
    /// proportional to the changed symbols and their readers:
    /// - a new name, table or source gets a symbol-plane row and a binding
    ///   identity, and the name nodes waiting on its binding key re-derive;
    /// - a changed symbol's name node re-derives, and its direct readers
    ///   (and those of a workbook name a new sheet-scoped name shadows) go
    ///   to `touched` (cells) or re-derive (name nodes): readers bound to a
    ///   cell or range name carry an edge to its target, so a redefinition
    ///   changes their facts;
    /// - a deleted symbol's readers re-derive the same way, then its row
    ///   and binding identity retire.
    ///
    /// Formula readers written before their symbol existed are re-bound by
    /// the graph (pending links); name nodes by `AuthorityHost::unbound`.
    /// `None`: an unlogged symbol change; the caller rebuilds.
    fn authority_sync_symbols_incremental(
        &mut self,
        touched: &mut Vec<VertexId>,
    ) -> Option<Result<(), AuthorityError>> {
        let changes = std::mem::take(&mut self.authority.symbol_changes);
        if changes.other {
            return None;
        }
        if changes.names.is_empty() {
            // An unlogged symbol revision: rebuild. (A logged change applied
            // before its revision bump already set `symbol_rev` to the
            // bumped revision, so it does not come here.)
            return None;
        }
        let mut work = 0u64;
        let mut changed = changes.names;
        changed.sort_unstable();
        changed.dedup();
        let mut sources: Vec<u32> = Vec::new();
        let mut created: Vec<VertexId> = Vec::new();
        let mut retired: Vec<VertexId> = Vec::new();
        let mut nodes: Vec<VertexId> = Vec::new();
        for &v in &changed {
            work += 1;
            let live = self.authority_symbol_live(v);
            match (live, self.authority.symbols.slot(v)) {
                (true, Some(slot)) => sources.push(slot),
                (true, None) => created.push(v),
                (false, Some(slot)) => {
                    sources.push(slot);
                    retired.push(v);
                }
                (false, None) => {}
            }
            if live && self.name_vertex_lookup.contains_key(&v) {
                nodes.push(v);
            }
            if let Some((NameScope::Sheet(_), name)) = self.name_vertex_lookup.get(&v)
                && let Some(entry) = self.resolve_name_entry_in_scope(name, NameScope::Workbook)
                && let Some(slot) = self.authority.symbols.slot(entry.vertex)
            {
                sources.push(slot);
            }
        }
        let mut hits = Vec::new();
        for &slot in &sources {
            self.authority.store.direct_dependents(
                SYMBOL_SHEET,
                &Rect::cell(slot, 0),
                TagFilter::All,
                &mut hits,
            );
        }
        for (sheet, r) in hits {
            for row in r.r0..=r.r1 {
                for col in r.c0..=r.c1 {
                    work += 1;
                    if let Some(v) = self.authority_vertex_of_cell((sheet, row, col)) {
                        if sheet == SYMBOL_SHEET {
                            nodes.push(v);
                        } else {
                            touched.push(v);
                        }
                    }
                }
            }
        }
        for v in retired {
            work += 1;
            if let Some(slot) = self.authority.symbols.remove(v) {
                let cell = (SYMBOL_SHEET, slot, 0);
                self.authority
                    .dirty
                    .clean(SYMBOL_SHEET, &Rect::cell(slot, 0));
                if self.authority.store.ids().id_of(cell).is_some()
                    && let Err(e) = self.authority.store.clear_cell(cell)
                {
                    return Some(Err(e));
                }
            }
            if let Some(symbol) = self.store.addr(v).as_symbol() {
                self.authority.store.remove_symbol(symbol);
            }
        }
        for &v in &created {
            work += 1;
            self.authority.symbols.insert(v);
            for key in self.authority_binding_keys(v) {
                if let Some(waiting) = self.authority.unbound.remove(&key) {
                    work += waiting.len() as u64;
                    nodes.extend(waiting);
                }
            }
        }
        nodes.sort_unstable();
        nodes.dedup();
        work += nodes.len() as u64;
        if let Err(e) = self.authority_set_symbol_nodes(&nodes) {
            return Some(Err(e));
        }
        // Binding identities after the nodes' cell ids, as a rebuild
        // allocates them.
        for &v in &created {
            if let Some(symbol) = self.store.addr(v).as_symbol()
                && let Err(e) = self.authority.store.insert_symbol(symbol)
            {
                return Some(Err(e));
            }
        }
        self.authority.symbol_sync_work += work;
        self.authority.symbol_rev = self.authority_applied_symbol_rev();
        self.authority.symbol_incremental += 1;
        self.authority.revision += 1;
        Some(Ok(()))
    }

    /// The symbol revision a sync that applied every logged change is
    /// current with: the revision the logging operation's pending bump
    /// will produce, else the current one.
    fn authority_applied_symbol_rev(&mut self) -> u64 {
        match self.authority.symbol_rev_target.take() {
            Some(target) if target != self.symbol_revision => target,
            _ => self.symbol_revision,
        }
    }

    /// A live name, table or source vertex.
    fn authority_symbol_live(&self, v: VertexId) -> bool {
        (self.name_vertex_lookup.contains_key(&v)
            || self.table_vertex_lookup.contains_key(&v)
            || self.source_vertex_lookup.contains_key(&v))
            && !self.store.is_deleted(v)
    }

    /// The binding keys under which references to symbol `v` wait
    /// (`AuthorityHost::unbound`): a name's lookup key; a table's table
    /// key; a source's name and table keys.
    fn authority_binding_keys(&self, v: VertexId) -> Vec<Box<str>> {
        let mut keys: Vec<Box<str>> = Vec::new();
        if let Some((_, name)) = self.name_vertex_lookup.get(&v) {
            keys.push(self.name_lookup_key(name).into_boxed_str());
        }
        if let Some(name) = self.table_vertex_lookup.get(&v) {
            keys.push(Self::unbound_symbol_key("table", name).into_boxed_str());
        }
        if let Some(name) = self.source_vertex_lookup.get(&v) {
            keys.push(self.name_lookup_key(name).into_boxed_str());
            keys.push(Self::unbound_symbol_key("table", name).into_boxed_str());
        }
        keys
    }

    /// Set the symbol nodes of live names `names` from their definitions,
    /// recording the bindings each still waits on.
    fn authority_set_symbol_nodes(&mut self, names: &[VertexId]) -> Result<(), AuthorityError> {
        for &v in names {
            let Some(slot) = self.authority.symbols.slot(v) else {
                continue;
            };
            let Some(((cell, facts), missing)) = self.authority_symbol_binding(slot, v) else {
                continue;
            };
            self.authority.note_unbound(v, missing);
            // A name's node carries the name vertex's id.
            self.authority.store.set_formula_given(cell, &facts, v.0)?;
            self.authority.incremental_mutations += 1;
            self.authority.revision += 1;
        }
        Ok(())
    }

    /// Give every live symbol (name, table, source) a symbol-plane row
    /// (survivors keep theirs) and drop the dirty marks of retired rows;
    /// returns the retired rows. Only names have facts at their row (their
    /// definition's precedents); a table's or source's row is a plain cell
    /// its readers point at, so dirtying the table or source vertex reaches
    /// them through the closure. The rebuild path: O(S log S).
    fn authority_sync_symbol_slots(&mut self) -> Vec<u32> {
        let mut live: Vec<VertexId> = self
            .name_vertex_lookup
            .keys()
            .chain(self.table_vertex_lookup.keys())
            .chain(self.source_vertex_lookup.keys())
            .copied()
            .filter(|&v| !self.store.is_deleted(v))
            .collect();
        live.sort_unstable();
        let retired = self.authority.symbols.sync(&live);
        for &slot in &retired {
            self.authority
                .dirty
                .clean(SYMBOL_SHEET, &Rect::cell(slot, 0));
        }
        retired
    }

    /// The authority cell of an executor vertex: its grid cell, or its
    /// symbol-plane node for a name.
    pub(crate) fn authority_cell_of_vertex(&self, v: VertexId) -> Option<Cell> {
        match self.get_cell_ref(v) {
            Some(c) => Some(cell_of(&c)),
            None => self
                .authority
                .symbols
                .slot(v)
                .map(|slot| (SYMBOL_SHEET, slot, 0)),
        }
    }

    /// The executor vertex of an ordered formula cell with authority id
    /// `id`: the vertex `id` itself (one id space) when it sits at `cell`,
    /// else the cell map (a store not yet synced with a graph edit).
    #[inline]
    pub(crate) fn authority_vertex_of_formula(&self, id: u32, cell: Cell) -> Option<VertexId> {
        let v = VertexId(id);
        if cell.0 != SYMBOL_SHEET
            && id < crate::engine::authority::identity::HOST_SYMBOL_ID_BASE
            && self.store.vertex_exists(v)
            && !self.store.is_deleted(v)
            && self.store.sheet_id(v) == cell.0
            && self
                .store
                .grid_addr(v)
                .is_some_and(|a| a.row() == cell.1 && a.col() == cell.2)
        {
            return Some(v);
        }
        self.authority_vertex_of_cell(cell)
    }

    /// The executor vertex of an authority cell (grid or symbol plane).
    pub(crate) fn authority_vertex_of_cell(&self, cell: Cell) -> Option<VertexId> {
        if cell.0 == SYMBOL_SHEET {
            self.authority.symbols.vertex(cell.1)
        } else {
            self.get_vertex_for_cell(&cell_ref(cell))
        }
    }

    /// Direct dependents of `(sheet, q)` among grid cells, looking through
    /// symbol nodes transparently.
    pub(crate) fn authority_direct_grid_dependents(store: &Store, sheet: u16, q: &Rect) -> Cover {
        store.direct_grid_dependents(sheet, q, TagFilter::All)
    }
}

// ------------------------------------------------------------ compression

/// A `fmt::Write` that checks the written text equals `expected`
/// without allocating.
struct TextEq<'a> {
    expected: &'a str,
    at: usize,
    ok: bool,
}

impl std::fmt::Write for TextEq<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.ok {
            let end = self.at + s.len();
            if self.expected.get(self.at..end) == Some(s) {
                self.at = end;
            } else {
                self.ok = false;
            }
        }
        Ok(())
    }
}

/// Whether `text` is the rendering of `reference`.
fn is_rendering(text: &str, reference: &formualizer_parse::parser::ReferenceType) -> bool {
    use std::fmt::Write as _;
    let mut w = TextEq {
        expected: text,
        at: 0,
        ok: true,
    };
    let _ = write!(w, "{reference}");
    w.ok && w.at == text.len()
}

/// Append `[$]COL` (1-based) to `out`.
fn push_col(out: &mut smallvec::SmallVec<[u8; 32]>, col: u32, abs: bool) {
    if abs {
        out.push(b'$');
    }
    let mut letters = [0u8; 8];
    let mut n = 0;
    let mut c = col;
    while c > 0 {
        let rem = ((c - 1) % 26) as u8;
        letters[n] = b'A' + rem;
        n += 1;
        c = (c - 1) / 26;
    }
    for i in (0..n).rev() {
        out.push(letters[i]);
    }
}

/// Append `[$]ROW` to `out`.
fn push_row(out: &mut smallvec::SmallVec<[u8; 32]>, row: u32, abs: bool) {
    if abs {
        out.push(b'$');
    }
    let mut digits = [0u8; 10];
    let mut n = 0;
    let mut r = row;
    loop {
        digits[n] = b'0' + (r % 10) as u8;
        n += 1;
        r /= 10;
        if r == 0 {
            break;
        }
    }
    for i in (0..n).rev() {
        out.push(digits[i]);
    }
}

/// The coordinate part of `ReferenceType`'s rendering of a cell or range
/// (everything after `Sheet!`), without allocating; `None` for other kinds.
fn coords_rendering(
    r: &crate::engine::arena::CompactRefType,
) -> Option<smallvec::SmallVec<[u8; 32]>> {
    use crate::engine::arena::CompactRefType as R;
    let mut out = smallvec::SmallVec::new();
    match *r {
        R::Cell {
            row,
            col,
            row_abs,
            col_abs,
            ..
        } => {
            push_col(&mut out, col, col_abs);
            push_row(&mut out, row, row_abs);
        }
        R::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => {
            let part = |out: &mut smallvec::SmallVec<[u8; 32]>,
                        col: Option<u32>,
                        col_abs,
                        row: Option<u32>,
                        row_abs| {
                if let Some(c) = col {
                    push_col(out, c, col_abs);
                }
                if let Some(r) = row {
                    push_row(out, r, row_abs);
                }
            };
            let open = |v: u32, sentinel: u32| (v != sentinel).then_some(v);
            part(
                &mut out,
                open(start_col, 0),
                start_col_abs,
                open(start_row, 0),
                start_row_abs,
            );
            out.push(b':');
            part(
                &mut out,
                open(end_col, u32::MAX),
                end_col_abs,
                open(end_row, u32::MAX),
                end_row_abs,
            );
        }
        _ => return None,
    }
    Some(out)
}

/// Literal rows equal by value (numbers by bits).
/// Literal rows equal by value (numbers by bits): the same literal may be
/// stored under different refs by different formulas.
pub(crate) fn literal_rows_equal(
    ds: &crate::engine::arena::DataStore,
    a: &[crate::engine::arena::ValueRef],
    b: &[crate::engine::arena::ValueRef],
) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x == y || {
                let (x, y) = (ds.retrieve_value(*x), ds.retrieve_value(*y));
                match (&x, &y) {
                    (LiteralValue::Number(p), LiteralValue::Number(q)) => {
                        p.to_bits() == q.to_bits()
                    }
                    _ => x == y,
                }
            }
        })
}

/// Rendered `sheet!` prefixes by sheet key (few per template: a linear
/// scan beats hashing).
type SheetPrefixes = smallvec::SmallVec<[(crate::engine::arena::SheetKey, String); 4]>;

/// Whether `text` is the rendering (`ReferenceType` Display) of `ref_type`,
/// without formatting it for cells and ranges: the quoted `sheet!` prefix
/// (cached in `prefixes`) plus the coordinates.
fn reference_text_rendered(
    ds: &crate::engine::arena::DataStore,
    reg: &crate::engine::sheet_registry::SheetRegistry,
    text: &str,
    ref_type: &crate::engine::arena::CompactRefType,
    prefixes: &mut SheetPrefixes,
) -> bool {
    use crate::engine::arena::{CompactRefType as R, SheetKey};
    match (ref_type, coords_rendering(ref_type)) {
        (R::Cell { sheet: None, .. } | R::Range { sheet: None, .. }, Some(c)) => {
            text.as_bytes() == &c[..]
        }
        // `Display` renders a sheet-qualified cell or range as
        // the quoted sheet name, `!`, then the coordinates.
        (
            R::Cell {
                sheet: Some(key), ..
            }
            | R::Range {
                sheet: Some(key), ..
            },
            Some(c),
        ) => {
            let prefix = match prefixes.iter().position(|(k, _)| k == key) {
                Some(i) => &prefixes[i].1,
                None => {
                    let name = match *key {
                        SheetKey::Id(id) => reg.name(id),
                        SheetKey::Name(name) => ds.resolve_ast_string(name),
                    };
                    let mut p = formualizer_common::format_a1_sheet_name(name).into_owned();
                    p.push('!');
                    prefixes.push((*key, p));
                    &prefixes[prefixes.len() - 1].1
                }
            };
            let bytes = text.as_bytes();
            bytes.len() == prefix.len() + c.len()
                && bytes.starts_with(prefix.as_bytes())
                && bytes[prefix.len()..] == c[..]
        }
        _ => is_rendering(text, &ds.reconstruct_reference_type_for_eval(ref_type, reg)),
    }
}

/// Whether every reference text of `ast` is its reference's rendering
/// (the instantiation rule reproduces such texts at any offset).
fn formula_texts_rendered(
    ds: &crate::engine::arena::DataStore,
    reg: &crate::engine::sheet_registry::SheetRegistry,
    ast: AstNodeId,
    prefixes: &mut SheetPrefixes,
) -> bool {
    use crate::engine::arena::AstNodeData as N;
    let mut stack: smallvec::SmallVec<[AstNodeId; 16]> = smallvec::smallvec![ast];
    while let Some(id) = stack.pop() {
        let Some(node) = ds.get_node(id) else {
            return false;
        };
        match node {
            N::Reference {
                original_id,
                ref_type,
            } => {
                let text = ds.resolve_ast_string(*original_id);
                let ok = reference_text_rendered(ds, reg, text, ref_type, prefixes);
                if !ok {
                    return false;
                }
            }
            N::UnaryOp { expr_id, .. } => stack.push(*expr_id),
            N::BinaryOp {
                left_id, right_id, ..
            } => {
                stack.push(*left_id);
                stack.push(*right_id);
            }
            N::Function { .. } => {
                if let Some(x) = ds.get_args(id) {
                    stack.extend(x.iter().copied());
                }
            }
            N::Array { .. } => {
                if let Some((_, _, x)) = ds.get_array_elems(id) {
                    stack.extend(x.iter().copied());
                }
            }
            N::Literal(_) | N::Omitted => {}
        }
    }
    true
}

/// Per template: for each reference node in the walk order of
/// [`ast_equal_relocated`], whether its text is its reference's rendering
/// (the instantiation rule re-renders those texts).
pub(crate) fn template_rendered_refs(
    ds: &crate::engine::arena::DataStore,
    reg: &crate::engine::sheet_registry::SheetRegistry,
    tmpl: AstNodeId,
) -> Vec<bool> {
    use crate::engine::arena::AstNodeData as N;
    let mut out = Vec::new();
    let mut prefixes = SheetPrefixes::new();
    let mut stack = vec![tmpl];
    while let Some(b) = stack.pop() {
        let Some(nb) = ds.get_node(b) else {
            continue;
        };
        match nb {
            N::Reference {
                original_id,
                ref_type,
            } => {
                let text = ds.resolve_ast_string(*original_id);
                let rendered = reference_text_rendered(ds, reg, text, ref_type, &mut prefixes);
                debug_assert_eq!(
                    rendered,
                    is_rendering(text, &ds.reconstruct_reference_type_for_eval(ref_type, reg)),
                    "reference text rendering check disagrees with Display for {text:?}"
                );
                out.push(rendered);
            }
            N::UnaryOp { expr_id, .. } => stack.push(*expr_id),
            N::BinaryOp {
                left_id, right_id, ..
            } => {
                stack.push(*left_id);
                stack.push(*right_id);
            }
            N::Function { .. } => {
                if let Some(x) = ds.get_args(b) {
                    stack.extend(x.iter().copied());
                }
            }
            N::Array { .. } => {
                if let Some((_, _, x)) = ds.get_array_elems(b) {
                    stack.extend(x.iter().copied());
                }
            }
            _ => {}
        }
    }
    out
}

/// Whether arena tree `own` is exactly `tmpl` relocated by `(dr, dc)` as
/// [`crate::engine::template::relocate::instantiate_member_ast`] builds
/// it: same shape, operators, function names, literal values (numbers by
/// bits), every relative axis of every cell or range reference shifted by
/// the offset, named references unchanged, and each reference text given
/// by the instantiation rule (`rendered[k]`: re-rendered when the
/// template's text is its reference's rendering, else the template's
/// text). Any other reference kind declines. `stack` is scratch.
#[allow(clippy::too_many_arguments)]
fn ast_equal_relocated(
    ds: &crate::engine::arena::DataStore,
    reg: &crate::engine::sheet_registry::SheetRegistry,
    own: AstNodeId,
    tmpl: AstNodeId,
    dr: i64,
    dc: i64,
    rendered: &[bool],
    stack: &mut Vec<(AstNodeId, AstNodeId)>,
) -> bool {
    use crate::engine::arena::{AstNodeData as N, CompactRefType as R};
    let mut ref_index = 0usize;
    stack.clear();
    stack.push((own, tmpl));
    while let Some((a, b)) = stack.pop() {
        let (Some(na), Some(nb)) = (ds.get_node(a), ds.get_node(b)) else {
            return false;
        };
        match (na, nb) {
            (N::Literal(x), N::Literal(y)) => {
                if x != y {
                    let (x, y) = (ds.retrieve_value(*x), ds.retrieve_value(*y));
                    let same = match (&x, &y) {
                        (LiteralValue::Number(p), LiteralValue::Number(q)) => {
                            p.to_bits() == q.to_bits()
                        }
                        _ => x == y,
                    };
                    if !same {
                        return false;
                    }
                }
            }
            (N::Omitted, N::Omitted) => {}
            (
                N::Reference {
                    original_id: oa,
                    ref_type: ra,
                },
                N::Reference {
                    original_id: ob,
                    ref_type: rb,
                },
            ) => {
                let Some(&rerender) = rendered.get(ref_index) else {
                    return false;
                };
                ref_index += 1;
                if let R::NamedRange(_) = rb {
                    if ra != rb || oa != ob {
                        return false;
                    }
                    continue;
                }
                let Some(relocated) = relocate_compact_ref(rb, dr, dc) else {
                    return false;
                };
                if *ra != relocated {
                    return false;
                }
                let text_ok = if rerender {
                    // The template text is its reference's rendering, so it
                    // is `Sheet!` (unchanged by relocation) + coordinates.
                    let own_text = ds.resolve_ast_string(*oa);
                    let tmpl_text = ds.resolve_ast_string(*ob);
                    match (coords_rendering(rb), coords_rendering(ra)) {
                        (Some(tc), Some(mc)) if tc.len() <= tmpl_text.len() => {
                            let prefix = &tmpl_text[..tmpl_text.len() - tc.len()];
                            own_text.len() == prefix.len() + mc.len()
                                && own_text.as_bytes()[..prefix.len()] == *prefix.as_bytes()
                                && own_text.as_bytes()[prefix.len()..] == mc[..]
                        }
                        _ => {
                            is_rendering(own_text, &ds.reconstruct_reference_type_for_eval(ra, reg))
                        }
                    }
                } else {
                    oa == ob
                };
                if !text_ok {
                    return false;
                }
            }
            (
                N::UnaryOp {
                    op_id: oa,
                    expr_id: ea,
                },
                N::UnaryOp {
                    op_id: ob,
                    expr_id: eb,
                },
            ) => {
                if oa != ob {
                    return false;
                }
                stack.push((*ea, *eb));
            }
            (
                N::BinaryOp {
                    op_id: oa,
                    left_id: la,
                    right_id: ra,
                },
                N::BinaryOp {
                    op_id: ob,
                    left_id: lb,
                    right_id: rb,
                },
            ) => {
                if oa != ob {
                    return false;
                }
                stack.push((*la, *lb));
                stack.push((*ra, *rb));
            }
            (
                N::Function {
                    name_id: fa,
                    args_count: ca,
                    ..
                },
                N::Function {
                    name_id: fb,
                    args_count: cb,
                    ..
                },
            ) => {
                if fa != fb || ca != cb {
                    return false;
                }
                let (Some(xa), Some(xb)) = (ds.get_args(a), ds.get_args(b)) else {
                    return false;
                };
                stack.extend(xa.iter().copied().zip(xb.iter().copied()));
            }
            (N::Array { .. }, N::Array { .. }) => {
                let (Some((r1, c1, xa)), Some((r2, c2, xb))) =
                    (ds.get_array_elems(a), ds.get_array_elems(b))
                else {
                    return false;
                };
                if (r1, c1) != (r2, c2) {
                    return false;
                }
                stack.extend(xa.iter().copied().zip(xb.iter().copied()));
            }
            _ => return false,
        }
    }
    ref_index == rendered.len()
}

/// A cell or range reference relocated by `(dr, dc)` as
/// [`crate::engine::template::relocate::instantiate_member_ast`] moves it:
/// relative axes shift (and must stay in 1..=u32::MAX), absolute axes and
/// open range bounds (start 0, end u32::MAX) stay. `None` for other kinds
/// or a shift out of bounds.
pub(crate) fn relocate_compact_ref(
    r: &crate::engine::arena::CompactRefType,
    dr: i64,
    dc: i64,
) -> Option<crate::engine::arena::CompactRefType> {
    use crate::engine::arena::CompactRefType as R;
    let shift = |v: u32, abs: bool, d: i64| -> Option<u32> {
        if abs {
            return Some(v);
        }
        let x = i64::from(v) + d;
        (1..=i64::from(u32::MAX)).contains(&x).then_some(x as u32)
    };
    let shift_start = |v: u32, abs: bool, d: i64| if v == 0 { Some(0) } else { shift(v, abs, d) };
    let shift_end = |v: u32, abs: bool, d: i64| {
        if v == u32::MAX {
            Some(u32::MAX)
        } else {
            shift(v, abs, d)
        }
    };
    match *r {
        R::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => Some(R::Cell {
            sheet,
            row: shift(row, row_abs, dr)?,
            col: shift(col, col_abs, dc)?,
            row_abs,
            col_abs,
        }),
        R::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => Some(R::Range {
            sheet,
            start_row: shift_start(start_row, start_row_abs, dr)?,
            start_col: shift_start(start_col, start_col_abs, dc)?,
            end_row: shift_end(end_row, end_row_abs, dr)?,
            end_col: shift_end(end_col, end_col_abs, dc)?,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        }),
        _ => None,
    }
}

/// [`ast_equal_relocated`] with the member given as a parsed tree that
/// was never interned (load-time family grouping, P2-M2): whether
/// interning `own` would give exactly `tmpl` relocated by `(dr, dc)`.
/// Each parsed node is compared as `DataStore::store_ast` would store it:
/// literals that round-trip unchanged through the arena (empty, number
/// by bits, integer, text, boolean), sheet names the registry resolves,
/// operator and function names as written. Anything whose stored form
/// is not reproduced here (other literals, arrays, calls, unregistered
/// sheets, other reference kinds) declines, so a `true` is the same
/// verdict `ast_equal_relocated` gives on the interned member.
#[allow(clippy::too_many_arguments)]
pub(crate) fn parsed_equal_relocated<'a>(
    ds: &crate::engine::arena::DataStore,
    reg: &crate::engine::sheet_registry::SheetRegistry,
    own: &'a formualizer_parse::parser::ASTNode,
    tmpl: AstNodeId,
    dr: i64,
    dc: i64,
    rendered: &[bool],
    stack: &mut Vec<(&'a formualizer_parse::parser::ASTNode, AstNodeId)>,
) -> bool {
    use crate::engine::arena::{AstNodeData as N, CompactRefType as R, SheetKey};
    use formualizer_parse::parser::{ASTNodeType as P, ReferenceType as PR};
    let sheet_key = |s: &Option<String>| -> Option<Option<SheetKey>> {
        match s {
            None => Some(None),
            Some(name) => reg.get_id(name).map(|id| Some(SheetKey::Id(id))),
        }
    };
    let mut ref_index = 0usize;
    stack.clear();
    stack.push((own, tmpl));
    while let Some((a, b)) = stack.pop() {
        let Some(nb) = ds.get_node(b) else {
            return false;
        };
        match (&a.node_type, nb) {
            (P::Literal(x), N::Literal(y)) => {
                let y = ds.retrieve_value(*y);
                let same = match (x, &y) {
                    (LiteralValue::Number(p), LiteralValue::Number(q)) => {
                        p.to_bits() == q.to_bits()
                    }
                    (
                        LiteralValue::Empty
                        | LiteralValue::Int(_)
                        | LiteralValue::Text(_)
                        | LiteralValue::Boolean(_),
                        _,
                    ) => *x == y,
                    _ => false,
                };
                if !same {
                    return false;
                }
            }
            (P::Omitted, N::Omitted) => {}
            (
                P::Reference {
                    original,
                    reference,
                },
                N::Reference {
                    original_id: ob,
                    ref_type: rb,
                },
            ) => {
                let Some(&rerender) = rendered.get(ref_index) else {
                    return false;
                };
                ref_index += 1;
                let tmpl_text = ds.resolve_ast_string(*ob);
                if let R::NamedRange(name) = rb {
                    match reference {
                        PR::NamedRange(n)
                            if n.as_str() == ds.resolve_ast_string(*name)
                                && original.as_str() == tmpl_text => {}
                        _ => return false,
                    }
                    continue;
                }
                let ra = match reference {
                    PR::Cell {
                        sheet,
                        row,
                        col,
                        row_abs,
                        col_abs,
                    } => {
                        let Some(sheet) = sheet_key(sheet) else {
                            return false;
                        };
                        R::Cell {
                            sheet,
                            row: *row,
                            col: *col,
                            row_abs: *row_abs,
                            col_abs: *col_abs,
                        }
                    }
                    PR::Range {
                        sheet,
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                        start_row_abs,
                        start_col_abs,
                        end_row_abs,
                        end_col_abs,
                    } => {
                        let Some(sheet) = sheet_key(sheet) else {
                            return false;
                        };
                        R::Range {
                            sheet,
                            start_row: start_row.unwrap_or(0),
                            start_col: start_col.unwrap_or(0),
                            end_row: end_row.unwrap_or(u32::MAX),
                            end_col: end_col.unwrap_or(u32::MAX),
                            start_row_abs: *start_row_abs,
                            start_col_abs: *start_col_abs,
                            end_row_abs: *end_row_abs,
                            end_col_abs: *end_col_abs,
                        }
                    }
                    _ => return false,
                };
                if relocate_compact_ref(rb, dr, dc) != Some(ra) {
                    return false;
                }
                let own_text = original.as_str();
                let text_ok = if rerender {
                    match (coords_rendering(rb), coords_rendering(&ra)) {
                        (Some(tc), Some(mc)) if tc.len() <= tmpl_text.len() => {
                            let prefix = &tmpl_text[..tmpl_text.len() - tc.len()];
                            own_text.len() == prefix.len() + mc.len()
                                && own_text.as_bytes()[..prefix.len()] == *prefix.as_bytes()
                                && own_text.as_bytes()[prefix.len()..] == mc[..]
                        }
                        _ => is_rendering(
                            own_text,
                            &ds.reconstruct_reference_type_for_eval(&ra, reg),
                        ),
                    }
                } else {
                    own_text == tmpl_text
                };
                if !text_ok {
                    return false;
                }
            }
            (P::UnaryOp { op, expr }, N::UnaryOp { op_id, expr_id }) => {
                if op.as_str() != ds.resolve_ast_string(*op_id) {
                    return false;
                }
                stack.push((expr, *expr_id));
            }
            (
                P::BinaryOp { op, left, right },
                N::BinaryOp {
                    op_id,
                    left_id,
                    right_id,
                },
            ) => {
                if op.as_str() != ds.resolve_ast_string(*op_id) {
                    return false;
                }
                stack.push((left, *left_id));
                stack.push((right, *right_id));
            }
            (
                P::Function { name, args },
                N::Function {
                    name_id,
                    args_count,
                    ..
                },
            ) => {
                if name.as_str() != ds.resolve_ast_string(*name_id)
                    || args.len() != usize::from(*args_count)
                {
                    return false;
                }
                let Some(xb) = ds.get_args(b) else {
                    return false;
                };
                stack.extend(args.iter().zip(xb.iter().copied()));
            }
            _ => return false,
        }
    }
    ref_index == rendered.len()
}

impl DependencyGraph {
    /// Load-time family grouping (P2-M2): the family of the formula above
    /// `(row0, col0)` (else the one to its left) in `grouper`, if `ast` is
    /// exactly its template relocated to this cell.
    pub(crate) fn group_formula_member(
        &self,
        grouper: &mut crate::engine::formula_ingest::FormulaFamilyGrouper,
        row0: u32,
        col0: u32,
        ast: &formualizer_parse::parser::ASTNode,
    ) -> Option<crate::engine::formula_ingest::GroupedFamily> {
        let mut stack = Vec::new();
        let above = row0
            .checked_sub(1)
            .and_then(|r| grouper.by_col.get_mut(&col0).filter(|(row, _)| *row == r))
            .map(|(_, family)| family);
        if let Some(family) = above
            && self.formula_member_of(family, row0, col0, ast, &mut stack)
        {
            return Some(family.clone());
        }
        let left = col0.checked_sub(1).and_then(|c| {
            grouper
                .last
                .as_mut()
                .filter(|(row, col, _)| *row == row0 && *col == c)
        });
        if let Some((_, _, family)) = left
            && self.formula_member_of(family, row0, col0, ast, &mut stack)
        {
            return Some(family.clone());
        }
        None
    }

    fn formula_member_of<'a>(
        &self,
        family: &mut crate::engine::formula_ingest::GroupedFamily,
        row0: u32,
        col0: u32,
        ast: &'a formualizer_parse::parser::ASTNode,
        stack: &mut Vec<(&'a formualizer_parse::parser::ASTNode, AstNodeId)>,
    ) -> bool {
        let template = family.template;
        let rendered = family.rendered.get_or_insert_with(|| {
            (!self.data_store.ast_needs_structural_rewrite(template))
                .then(|| template_rendered_refs(&self.data_store, &self.sheet_reg, template).into())
        });
        let Some(rendered) = rendered.as_deref() else {
            return false;
        };
        let dr = i64::from(row0) - i64::from(family.anchor.0);
        let dc = i64::from(col0) - i64::from(family.anchor.1);
        let equal = parsed_equal_relocated(
            &self.data_store,
            &self.sheet_reg,
            ast,
            template,
            dr,
            dc,
            rendered,
            stack,
        );
        #[cfg(debug_assertions)]
        {
            // The verdict is the one `ast_equal_relocated` gives on the
            // interned member (checked in a scratch arena holding only the
            // template), and the member instantiates to its own formula.
            let tree = self
                .data_store
                .retrieve_ast(template, &self.sheet_reg)
                .expect("template");
            let mut ds = crate::engine::arena::DataStore::new();
            let tmpl = ds.store_ast(&tree, &self.sheet_reg);
            let own = ds.store_ast(ast, &self.sheet_reg);
            let mut scratch = Vec::new();
            let interned = ast_equal_relocated(
                &ds,
                &self.sheet_reg,
                own,
                tmpl,
                dr,
                dc,
                rendered,
                &mut scratch,
            );
            assert!(
                !equal || interned,
                "load-time family member at {row0},{col0} is not its template relocated"
            );
            if equal {
                let member =
                    crate::engine::template::relocate::instantiate_member_ast(&tree, dr, dc)
                        .expect("member relocation");
                assert_eq!(
                    Some(member),
                    ds.retrieve_ast(own, &self.sheet_reg),
                    "load-time family member at {row0},{col0} does not instantiate to its formula"
                );
            }
        }
        equal
    }

    /// Program 2 compression (P2-M2): every non-dynamic member of a family
    /// node whose own formula is exactly the node's template relocated to
    /// it references the template instead; then the arena keeps only live
    /// roots (formula vertices and authority owners). Returns the number of
    /// members compressed. The caller guarantees no other arena id holders
    /// are live (staged or deferred formula packages).
    pub(crate) fn compress_family_formulas(
        &mut self,
        pool: Option<&rayon::ThreadPool>,
    ) -> (usize, StringGarbage) {
        if self.authority.state != HostState::Ready || self.vertex_formulas.has_touched() {
            return (0, StringGarbage::default());
        }
        // Only owners holding an own-AST member other than their template
        // can compress anything: after load-time grouping most members are
        // already references, so find those owners from the (few) own
        // formulas instead of visiting every member of every owner.
        let store = &self.authority.store;
        let mut candidates: Vec<u32> = Vec::new();
        for (v, f) in self.vertex_formulas.map_iter() {
            let super::FormulaRef::Own(own) = f else {
                continue;
            };
            let Some(cell) = self.get_cell_ref(v) else {
                continue;
            };
            let Some(o) = store.owner_at(cell_of(&cell)) else {
                continue;
            };
            if let Some((_, _, _, template, _)) = store.family_owner(o)
                && template != own
            {
                candidates.push(o);
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        let owners: Vec<_> = candidates
            .into_iter()
            .filter_map(|o| store.family_owner(o))
            .filter(|&(_, _, flags, _, _)| flags & crate::engine::authority::store::F_DYNAMIC == 0)
            .collect();
        // Decide per owner (read-only, independent: in parallel when a pool
        // is given), then apply in owner order.
        // Reference texts are part of AST equality: a member is rebuilt from
        // the template only when both render their references. Sequential
        // engines checked that at load (`texts_unrendered`); with a pool it is
        // checked here, in the parallel decision.
        let unrendered = (!self.config.enable_parallel)
            .then(|| std::mem::take(&mut *self.authority.texts_unrendered.lock().unwrap()));
        let decide = |owner: &(u16, _, _, AstNodeId, (u32, u32))| {
            self.compressible_members(*owner, unrendered.as_ref())
        };
        let decided: Vec<Vec<CompressibleMember>> = match pool {
            Some(pool) if owners.len() > 1 => {
                use rayon::prelude::*;
                pool.install(|| owners.par_iter().map(decide).collect())
            }
            _ => owners.iter().map(decide).collect(),
        };
        let mut compressed = 0usize;
        for ((_, _, _, template, anchor), members) in owners.iter().zip(decided) {
            for (v, cell) in members {
                #[cfg(debug_assertions)]
                let before = {
                    let (_, row, col) = cell;
                    let Some(super::FormulaRef::Own(own)) = self.vertex_formulas.get(&v) else {
                        unreachable!("decided member has its own formula");
                    };
                    let dr = i64::from(row) - i64::from(anchor.0);
                    let dc = i64::from(col) - i64::from(anchor.1);
                    let rendered =
                        template_rendered_refs(&self.data_store, &self.sheet_reg, *template);
                    let mut stack = Vec::new();
                    assert!(
                        ast_equal_relocated(
                            &self.data_store,
                            &self.sheet_reg,
                            own,
                            *template,
                            dr,
                            dc,
                            &rendered,
                            &mut stack,
                        ),
                        "family member {cell:?} is not its template relocated"
                    );
                    self.get_formula(v)
                };
                #[cfg(not(debug_assertions))]
                let _ = cell;
                self.vertex_formulas.compress(v, *template, *anchor);
                #[cfg(debug_assertions)]
                assert_eq!(
                    self.get_formula(v),
                    before,
                    "compressed member {cell:?} does not instantiate to its own formula"
                );
                compressed += 1;
            }
        }
        let garbage = if compressed > 0 {
            self.compact_formula_arena()
        } else {
            StringGarbage::default()
        };
        (compressed, garbage)
    }

    /// The members of one family owner that can reference its template:
    /// non-dynamic, reference texts rendered, own formula not the template,
    /// literal row equal to the template's. Members share the template's
    /// literal-erased relative tokens (the authority groups by them); what
    /// they may not share is literals (checked against the slot row) and
    /// reference texts (checked when the authority read each formula).
    fn compressible_members(
        &self,
        (sheet, dom, _, template, anchor): (u16, Rect, u16, AstNodeId, (u32, u32)),
        unrendered: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Vec<CompressibleMember> {
        let mut out = Vec::new();
        let anchor_vertex = self
            .authority
            .store
            .ids()
            .id_of((sheet, anchor.0, anchor.1))
            .and_then(|id| self.authority_vertex_of_formula(id, (sheet, anchor.0, anchor.1)));
        let mut prefixes = SheetPrefixes::new();
        let mut rendered = |v: VertexId, ast: AstNodeId| match unrendered {
            Some(set) => !set.contains(&v),
            None => formula_texts_rendered(&self.data_store, &self.sheet_reg, ast, &mut prefixes),
        };
        let Some(anchor_vertex) = anchor_vertex else {
            return out;
        };
        if !rendered(anchor_vertex, template) {
            return out;
        }
        let tmpl_literals = crate::engine::authority::template::template_facts(
            &self.data_store,
            template,
            anchor.0,
            anchor.1,
        )
        .literals;
        for col in dom.c0..=dom.c1 {
            // Members of a column are mostly one identity run (consecutive
            // ids, which are their vertices). Checked by cell, with the cell
            // map as the fallback.
            let id0 = self.authority.store.ids().id_of((sheet, dom.r0, col));
            for row in dom.r0..=dom.r1 {
                let cell = (sheet, row, col);
                // `authority_vertex_of_formula` verifies the cell, so a wrong
                // id guess only falls back to the cell map.
                let guess = id0.map(|id0| id0 + (row - dom.r0));
                let v = guess
                    .and_then(|id| self.authority_vertex_of_formula(id, cell))
                    .or_else(|| self.get_vertex_for_cell(&cell_ref(cell)));
                let Some(v) = v else {
                    continue;
                };
                let Some(super::FormulaRef::Own(own)) = self.vertex_formulas.get(&v) else {
                    continue;
                };
                if own == template || self.store.is_dynamic(v) {
                    continue;
                }
                // One id space: the member's id is its vertex's.
                let id = v.0;
                debug_assert_eq!(self.authority.store.ids().id_of(cell), Some(id));
                let row_lits = self.authority.store.slots().get(id).unwrap_or(&[]);
                if !literal_rows_equal(&self.data_store, row_lits, &tmpl_literals)
                    || !rendered(v, own)
                {
                    continue;
                }
                out.push((v, cell));
            }
        }
        out
    }

    /// Give every compressed member its own AST again (same formulas).
    pub(crate) fn decompress_family_formulas(&mut self) {
        self.materialize_all();
        let members: Vec<VertexId> = self
            .vertex_formulas
            .map_iter()
            .filter(|(_, f)| matches!(f, super::FormulaRef::Member { .. }))
            .map(|(v, _)| v)
            .collect();
        for v in members {
            let _ = self.own_formula_id(v);
        }
    }

    /// Drop arena nodes unreachable from live formulas and authority
    /// owners, remapping both.
    pub(crate) fn compact_formula_arena(&mut self) -> StringGarbage {
        let roots: Vec<AstNodeId> = self
            .vertex_formulas
            .roots()
            .chain(self.authority.store.owner_templates())
            .collect();
        let (remap, garbage) = self.data_store.compact_asts(roots);
        let map = |id: AstNodeId| {
            let new = remap
                .get(id.as_u32() as usize)
                .copied()
                .unwrap_or(id.as_u32());
            debug_assert_ne!(new, u32::MAX, "live formula root dropped");
            AstNodeId::from_u32(new)
        };
        self.vertex_formulas.remap(&map);
        self.authority.store.remap_templates(&remap);
        garbage
    }
}
