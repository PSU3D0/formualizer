//! `DependencyGraph` side of the Program 1 authority host (M1a).
//!
//! See `engine::authority::host` for the model. This module owns the
//! feature-gated hooks the graph calls and the legacy mirror used by the
//! differential gates Δ(a) (dirty closure) and Δ(e) (direct dependents).

use super::*;
use crate::engine::authority::extract::{extract_formula, extract_symbol};
use crate::engine::authority::geom::{Cell, Cover, Rect, SYMBOL_SHEET};
use crate::engine::authority::host::{AuthorityHost, HostState};
use crate::engine::authority::store::{AuthorityError, BuildInput, Store, TagFilter};
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
        let cell = self.get_cell_ref(vid)?;
        if self.store.is_deleted(vid) {
            return None;
        }
        let ast = *self.vertex_formulas.get(&vid)?;
        let (sheet, row, col) = cell_of(&cell);
        let mut facts = extract_formula(
            self,
            sheet,
            row,
            col,
            ast,
            self.is_volatile(vid),
            self.is_dynamic(vid),
        );
        self.authority_apply_range_self_use(vid, (sheet, row, col), &mut facts);
        Some(((sheet, row, col), facts))
    }

    /// Legacy's #120 rule: a compressed range (open, or larger than the
    /// expansion limit) covering its own formula's cell is a self-loop,
    /// unless every use of it is a static `INDEX` whose selection excludes
    /// the cell (`compressed_range_self_use`). In that case the edge keeps
    /// the range minus the cell: up to four absolute pieces, and the formula
    /// stays an ungrouped singleton because its edges are no longer the
    /// template's.
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
        let input = self.authority_build_input();
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
        let input_bytes = input.capacity() * size_of::<BuildInput>()
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
        match Store::rebuild_with_symbols(input, symbols, prior, budget) {
            Ok(mut store) => {
                store.stats.symbol_work += inventory_work;
                self.authority.store = store;
                self.authority.symbol_rev = self.symbol_revision;
                self.authority.builds += 1;
                self.authority.revision += 1;
                self.authority.clear_observed();
                self.authority.state = HostState::Ready;
                self.authority_fill_vertex_of_id();
            }
            Err(e) => self.authority.state = HostState::Failed(e),
        }
    }

    /// Bring the authority up to date with the graph's formulas.
    pub(crate) fn authority_sync(&mut self) {
        let touched = self.vertex_formulas.take_touched();
        // Spans are the one transient unsupported state: once they are gone
        // (demoted to per-cell formulas) the graph holds every formula again
        // and a rebuild is exact.
        if self.authority.state
            == HostState::Failed(AuthorityError::Unsupported {
                operation: "formula_plane_spans",
            })
            && self.formula_authority.active_span_count() == 0
        {
            self.authority.state = HostState::Unbuilt;
        }
        if matches!(self.authority.state, HostState::Failed(_)) {
            return;
        }
        if self.formula_authority.active_span_count() > 0 {
            self.authority_mark_unsupported("formula_plane_spans");
            return;
        }
        let rebuild = self.authority.state == HostState::Unbuilt
            || self.authority.symbol_rev != self.symbol_revision
            || (touched.len() > 4096 && touched.len() * 4 > self.vertex_formulas.len());
        if rebuild {
            self.authority_rebuild();
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
            let result = match current.and_then(|v| self.authority_formula_input(v)) {
                Some((c, mut facts)) => {
                    self.authority_verify_l(c.0, &mut facts);
                    let result = self.authority.store.set_formula(c, &facts);
                    if result.is_ok()
                        && let (Some(v), Some(id)) = (current, self.authority.store.ids().id_of(c))
                    {
                        self.authority.set_vertex_of_id(id, v);
                    }
                    result
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
    /// symbol revision since the last build, no FormulaPlane spans).
    pub(crate) fn authority_plan_store(&self) -> Result<&Store, AuthorityError> {
        if self.formula_authority.active_span_count() > 0 {
            return Err(AuthorityError::Unsupported {
                operation: "formula_plane_spans",
            });
        }
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

    /// Legacy dirty propagation from `seeds` just ran and dirtied
    /// `legacy_affected`: mark the authority's propagation of the same
    /// seeds (formula seeds and their closure, `DirtyStore::mark_propagation`)
    /// and, when enabled, compare it with what legacy actually dirtied.
    ///
    /// Δ(a) filtering: legacy's affected set holds value sources (affected,
    /// not dirtied) and symbol vertices (names, tables); only formula cells
    /// are compared. `exact` is false for `mark_dirty_many_value_cells`,
    /// whose range lookup uses the sources' bounding rectangle per sheet and
    /// may over-dirty with several sources: there the authority must be a
    /// subset, and legacy-only cells are counted as conservative, not as a
    /// mismatch.
    pub(super) fn authority_observe_propagation(
        &mut self,
        seeds: &[VertexId],
        legacy_affected: &FxHashSet<VertexId>,
        exact: bool,
    ) {
        self.authority_sync();
        if self.authority.state != HostState::Ready {
            return;
        }
        let cells: Vec<Cell> = seeds
            .iter()
            .filter_map(|&v| self.authority_cell_of_vertex(v))
            .collect();
        if cells.is_empty() {
            return;
        }
        let rects: Vec<(u16, Rect)> = cells.iter().map(|c| (c.0, Rect::cell(c.1, c.2))).collect();
        let marked = self
            .authority
            .dirty
            .mark_propagation(&self.authority.store, &rects);
        let Some(mode) = diff_mode() else {
            return;
        };
        self.authority.diff.propagations += 1;
        if self.vertex_formulas.len() > DIFF_MAX_FORMULAS || cells.len() > DIFF_MAX_SEEDS {
            self.authority.diff.skipped += 1;
            return;
        }
        let legacy = self.legacy_dirty_cells(legacy_affected);
        let mine: Vec<Cell> = marked
            .cells()
            .into_iter()
            .filter(|c| c.0 != SYMBOL_SHEET)
            .collect();
        let mut problems: Vec<String> = Vec::new();
        let only_legacy: Vec<&Cell> = legacy
            .iter()
            .filter(|c| mine.binary_search(c).is_err())
            .collect();
        let only_mine: Vec<&Cell> = mine
            .iter()
            .filter(|c| legacy.binary_search(c).is_err())
            .collect();
        if !only_mine.is_empty() || (exact && !only_legacy.is_empty()) {
            self.authority.diff.closure_mismatches += 1;
            problems.push(format!(
                "dirty seeds={cells:?} exact={exact} legacy={} authority={} only_legacy={:?} only_authority={:?}",
                legacy.len(),
                mine.len(),
                only_legacy.iter().take(8).collect::<Vec<_>>(),
                only_mine.iter().take(8).collect::<Vec<_>>(),
            ));
        }
        let conservative = only_mine.is_empty() && !exact && !only_legacy.is_empty();
        if conservative {
            self.authority.diff.closure_conservative += 1;
        }
        for &c in cells.iter().filter(|c| c.0 != SYMBOL_SHEET) {
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
                // One CHECK line per compared propagation (seed count), then
                // one line per mismatch.
                let mut lines = vec![format!("{who}\tCHECK seeds={}", cells.len())];
                if conservative {
                    lines.push(format!(
                        "{who}\tCONSERVATIVE seeds={} legacy_only={}",
                        cells.len(),
                        only_legacy.len()
                    ));
                }
                lines.extend(problems);
                append_lines(path, &lines);
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

    /// Run legacy's actual dirty propagation from `cells` (their vertices;
    /// cells without a vertex are skipped) and return the formula cells it
    /// dirtied, with the cover the authority marked for the same
    /// propagation. The authority cover is cleared first, so the second
    /// result is exactly this propagation's marking. Gates only.
    pub(crate) fn dirty_propagation_pair(
        &mut self,
        cells: &[Cell],
    ) -> Result<(Vec<Cell>, Vec<Cell>), AuthorityError> {
        self.authority()?;
        let vids: Vec<VertexId> = cells
            .iter()
            .filter_map(|&c| self.get_vertex_for_cell(&cell_ref(c)))
            .collect();
        self.authority.dirty.clear();
        let affected: FxHashSet<VertexId> = self.mark_dirty_many(&vids).into_iter().collect();
        if let HostState::Failed(e) = &self.authority.state {
            return Err(e.clone());
        }
        let legacy = self.legacy_dirty_cells(&affected);
        let mine = self
            .authority
            .dirty
            .cells()
            .into_iter()
            .filter(|c| c.0 != SYMBOL_SHEET)
            .collect();
        Ok((legacy, mine))
    }

    // ------------------------------------------------------------ memory gate

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
        let vids: Vec<VertexId> = self.vertex_formulas.keys().copied().collect();
        let mut input: Vec<BuildInput> = vids
            .into_iter()
            .filter_map(|v| self.authority_formula_input(v))
            .collect();
        input.extend(
            self.authority
                .symbols
                .iter()
                .filter_map(|(slot, v)| self.authority_symbol_input(slot, v)),
        );
        input
    }

    fn authority_symbol_input(&self, slot: u32, vertex: VertexId) -> Option<BuildInput> {
        let (_, name) = self.name_vertex_lookup.get(&vertex)?;
        let entry = self.named_range_by_vertex(vertex)?;
        let facts = extract_symbol(
            self,
            name,
            entry,
            self.is_volatile(vertex),
            self.is_dynamic(vertex),
        );
        Some(((SYMBOL_SHEET, slot, 0), facts))
    }

    /// Give every live name a symbol-plane row (surviving names keep theirs)
    /// and drop the dirty marks of retired rows.
    fn authority_sync_symbol_slots(&mut self) {
        let mut live: Vec<VertexId> = self
            .name_vertex_lookup
            .keys()
            .copied()
            .filter(|&v| !self.store.is_deleted(v))
            .collect();
        live.sort_unstable();
        for slot in self.authority.symbols.sync(&live) {
            self.authority
                .dirty
                .clean(SYMBOL_SHEET, &Rect::cell(slot, 0));
        }
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

    /// Record the executor vertex of every formula id after a build.
    fn authority_fill_vertex_of_id(&mut self) {
        let next = self.authority.store.ids().next_id() as usize;
        let mut table = std::mem::take(&mut self.authority.vertex_of_id);
        table.clear();
        table.resize(next, u32::MAX);
        for &v in self.vertex_formulas.keys() {
            if self.store.is_deleted(v) {
                continue;
            }
            let Some(addr) = self.store.grid_addr(v) else {
                continue;
            };
            let cell = (self.store.sheet_id(v), addr.row(), addr.col());
            if let Some(id) = self.authority.store.ids().id_of(cell)
                && let Some(slot) = table.get_mut(id as usize)
            {
                *slot = v.0;
            }
        }
        self.authority.vertex_of_id = table;
    }

    /// The executor vertex of an ordered formula cell with authority id
    /// `id`: the side array when its vertex still sits at `cell`, else the
    /// cell map.
    #[inline]
    pub(crate) fn authority_vertex_of_formula(&self, id: u32, cell: Cell) -> Option<VertexId> {
        if cell.0 != SYMBOL_SHEET
            && let Some(v) = self.authority.vertex_of_id(id)
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
