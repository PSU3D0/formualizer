//! `DependencyGraph` side of the Program 1 authority host (M1a).
//!
//! See `engine::authority::host` for the model. This module owns the
//! feature-gated hooks the graph calls and the legacy mirror used by the
//! differential gates Δ(a) (dirty closure) and Δ(e) (direct dependents).

use super::*;
use crate::engine::authority::extract::extract_formula;
use crate::engine::authority::geom::{Cell, Cover, Rect};
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
        let facts = extract_formula(
            self,
            sheet,
            row,
            col,
            ast,
            self.is_volatile(vid),
            self.is_dynamic(vid),
        );
        Some(((sheet, row, col), facts))
    }

    /// Rebuild the store from the graph's formulas (load, symbol revision,
    /// large batch). Identities are kept (decision 9): the previous store's
    /// live cells keep their ids and its counter continues. The candidate
    /// goes through the store's admission; a rejection fails the host with
    /// the typed error instead of installing a store above the budget.
    fn authority_rebuild(&mut self) {
        let input = self.authority_build_input();
        let budget = self.authority.store.budget;
        let prior = (self.authority.state != HostState::Unbuilt).then_some(&self.authority.store);
        match Store::rebuild(input, prior, budget) {
            Ok(store) => {
                self.authority.store = store;
                self.authority.symbol_rev = self.symbol_revision;
                self.authority.builds += 1;
                self.authority.state = HostState::Ready;
            }
            Err(e) => self.authority.state = HostState::Failed(e),
        }
    }

    /// Bring the authority up to date with the graph's formulas.
    pub(crate) fn authority_sync(&mut self) {
        let touched = self.vertex_formulas.take_touched();
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
        for cell in cells {
            let current = self
                .get_vertex_for_cell(&cell_ref(cell))
                .filter(|v| self.vertex_formulas.contains_key(v));
            let result = match current.and_then(|v| self.authority_formula_input(v)) {
                Some((c, mut facts)) => {
                    self.authority_verify_l(c.0, &mut facts);
                    self.authority.store.set_formula(c, &facts)
                }
                None => self.authority.store.clear_cell(cell),
            };
            self.authority.incremental_mutations += 1;
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
        // Dead LK keys outgrew the live state (re-review R3): rebuild,
        // keeping identities, to drop them.
        if self
            .authority
            .store
            .lk_compaction_due(self.vertex_formulas.len())
        {
            self.authority.lk_compactions += 1;
            self.authority_rebuild();
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
        let mut hits = Vec::new();
        host.store
            .direct_dependents(c.0, &Rect::cell(c.1, c.2), TagFilter::All, &mut hits);
        let mut cover = Cover::new();
        for (s, r) in hits {
            cover.insert_rect(s, &r);
        }
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
        Ok(cover.cells().into_iter().map(cell_ref).collect())
    }

    /// Direct precedents of a formula cell: `(sheet, r0, c0, r1, c1)`.
    pub(crate) fn authority_precedents(
        &mut self,
        cell: CellRef,
    ) -> Result<Vec<PrecedentRect>, AuthorityError> {
        let host = self.authority()?;
        let mut hits = Vec::new();
        host.store
            .direct_precedents(cell_of(&cell), TagFilter::All, &mut hits);
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
            .filter_map(|&v| self.get_cell_ref(v).map(|c| cell_of(&c)))
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
        let mine: Vec<Cell> = marked.cells();
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
        for &c in &cells {
            self.authority.diff.checked_seeds += 1;
            let legacy = self.legacy_direct_dependent_cells(c);
            let mut hits = Vec::new();
            self.authority.store.direct_dependents(
                c.0,
                &Rect::cell(c.1, c.2),
                TagFilter::All,
                &mut hits,
            );
            let mut cover = Cover::new();
            for (s, r) in hits {
                cover.insert_rect(s, &r);
            }
            let mine = cover.cells();
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
            if let Some(c) = self.get_cell_ref(v) {
                let (s, r, col) = cell_of(&c);
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
        Ok((legacy, self.authority.dirty.cells()))
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

    /// The build input the host would use for a full rebuild.
    pub(crate) fn authority_build_input(&self) -> Vec<BuildInput> {
        let vids: Vec<VertexId> = self.vertex_formulas.keys().copied().collect();
        vids.into_iter()
            .filter_map(|v| self.authority_formula_input(v))
            .collect()
    }
}
