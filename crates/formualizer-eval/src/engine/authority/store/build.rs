//! Bulk build (§5.7.1: the production build of a group *is* canon of its
//! extracted cell set).

use super::*;

/// One formula cell and its facts.
pub type BuildInput = (Cell, FormulaFacts);

/// A build input whose facts may be shared between cells (the members of
/// a load-time family share their template's facts, P2-M2).
pub type SharedBuildInput = (Cell, std::sync::Arc<FormulaFacts>);

use std::borrow::Borrow;

/// The id a host assigns a build input's cell (Program 2: a formula
/// cell's id is its executor `VertexId`). `None` keeps decision 9's
/// assignment (kept from `prior` or `carried`, else fresh).
pub trait GivenId {
    fn given_id(&self) -> Option<Vid> {
        None
    }
}

impl GivenId for FormulaFacts {}
impl GivenId for std::sync::Arc<FormulaFacts> {}
impl GivenId for &FormulaFacts {}

/// Facts with the host's id for their cell.
#[derive(Clone, Debug)]
pub struct IdentifiedFacts {
    pub id: Vid,
    pub facts: std::sync::Arc<FormulaFacts>,
}

impl Borrow<FormulaFacts> for IdentifiedFacts {
    fn borrow(&self) -> &FormulaFacts {
        &self.facts
    }
}

impl GivenId for IdentifiedFacts {
    fn given_id(&self) -> Option<Vid> {
        Some(self.id)
    }
}

impl IdentifiedFacts {
    pub fn owned_heap_bytes(&self) -> usize {
        self.facts.owned_heap_bytes()
    }
}

impl Store {
    /// Build from scratch. Groups are canon of their cells, owners get
    /// column-major contiguous ids (one run per column), and every
    /// container is allocated at its exact size.
    pub fn build(input: Vec<BuildInput>) -> Store {
        Self::build_keeping(input, None)
            .expect("identity counter at build")
            .0
    }

    /// A host rebuild (load, symbol revision, large batch) under the same
    /// admission as a mutation (§5.6, M1a correction B5): the candidate's
    /// retained bytes are admitted against the retained budget, and the
    /// coexisting previous store plus the build scratch against the scratch
    /// budget. A rejected candidate is dropped; the caller keeps `prior`.
    /// Identities are kept (decision 9, correction B4): every cell live in
    /// `prior` keeps its id, and new formula cells get fresh ids from
    /// `prior`'s counter.
    pub fn rebuild(
        input: Vec<BuildInput>,
        prior: Option<&Store>,
        budget: Budget,
    ) -> Result<Store, AuthorityError> {
        Self::rebuild_with_symbols(input, Vec::new(), prior, budget)
    }

    /// Rebuild with a sorted live symbol inventory. Its full capacity is
    /// charged as build scratch, including during cell-store construction.
    /// Cell and symbol IDs share the candidate's counter and commit together.
    pub(crate) fn rebuild_with_symbols(
        input: Vec<BuildInput>,
        live_symbols: Vec<SymbolId>,
        prior: Option<&Store>,
        budget: Budget,
    ) -> Result<Store, AuthorityError> {
        Self::rebuild_carrying(input, live_symbols, prior, None, false, budget)
    }

    /// [`Self::rebuild_with_symbols`] after a structural edit (M3): cell
    /// ids come from `carried` (post-edit cell → id, every id below
    /// `prior`'s counter, not live twice) instead of `prior`'s positions;
    /// cells absent from it get fresh ids from `prior`'s counter.
    pub(crate) fn rebuild_carrying<F: Borrow<FormulaFacts> + GivenId>(
        input: Vec<(Cell, F)>,
        live_symbols: Vec<SymbolId>,
        prior: Option<&Store>,
        carried: Option<&FxHashMap<Cell, Vid>>,
        host_ids: bool,
        budget: Budget,
    ) -> Result<Store, AuthorityError> {
        let symbol_input_bytes = (live_symbols.capacity() * size_of::<SymbolId>()) as u64;
        // Preflight before the build allocates: the input it consumes and
        // the previous store already coexist, so a scratch budget below
        // them rejects without building. The full preview (legacy's
        // `preview_formula_mutations` → `preflight_graph_admission` at bulk
        // ingest) lands with the resource-ledger link (B-10, addendum B-22).
        let input_bytes = input.capacity() * size_of::<(Cell, F)>()
            + input
                .iter()
                .map(|(_, f)| f.borrow().owned_heap_bytes())
                .sum::<usize>();
        let gate = Store {
            budget,
            ..Store::new()
        };
        gate.admit(
            0,
            input_bytes as u64 + symbol_input_bytes + prior.map_or(0, Store::heap_bytes),
        )?;
        drop(gate);
        let (mut s, scratch) =
            Self::build_keeping_with(input, prior.map(Store::ids), carried, host_ids)?;
        s.budget = budget;
        let transient = scratch + symbol_input_bytes + prior.map_or(0, Store::heap_bytes);
        s.admit(s.heap_bytes(), transient)?;
        // The previous table is already included in `transient`. Admit the
        // symbol output against the remaining retained capacity before allocating;
        // do not charge the prior table twice as table-local scratch.
        let remaining_retained = budget.retained.map(|n| n - s.heap_bytes());
        let empty = SymbolTable::default();
        let (symbols, work) = SymbolTable::rebuild(
            &live_symbols,
            prior.map_or(&empty, |p| &p.symbols),
            &mut s.ids,
            Budget {
                retained: remaining_retained,
                scratch: None,
            },
        )?;
        s.symbols = symbols;
        s.stats.symbol_work = work.visits;
        s.admit(s.heap_bytes(), transient)?;
        Ok(s)
    }

    /// Add one live symbol to the binding-identity table (the incremental
    /// symbol-revision path; O(log S) amortized, a new symbol appends). A
    /// symbol already live keeps its id; a new one takes the next id from
    /// the shared counter, as a rebuild would.
    pub(crate) fn insert_symbol(&mut self, symbol: SymbolId) -> Result<Vid, AuthorityError> {
        let remaining_retained = self
            .budget
            .retained
            .map(|n| n.saturating_sub(self.heap_bytes() - self.symbols.heap_bytes()));
        let (vid, work) = self.symbols.insert(
            symbol,
            &mut self.ids,
            Budget {
                retained: remaining_retained,
                scratch: None,
            },
        )?;
        self.stats.symbol_work += work;
        Ok(vid)
    }

    /// Retire one symbol from the binding-identity table (incremental;
    /// its id is never reused).
    pub(crate) fn remove_symbol(&mut self, symbol: SymbolId) {
        let work = self.symbols.remove(symbol);
        self.stats.symbol_work += work;
    }

    /// The build, keeping the identities of `prior` when given. Returns the
    /// store and an upper bound on the build's scratch bytes (the sum of
    /// its temporary containers' capacities).
    pub(crate) fn build_keeping(
        input: Vec<BuildInput>,
        prior: Option<&IdentityTable>,
    ) -> Result<(Store, u64), AuthorityError> {
        Self::build_keeping_with(input, prior, None, false)
    }

    /// [`Self::build_keeping`] with an explicit kept-id map (see
    /// [`Self::rebuild_carrying`]); `prior` then supplies only the counter.
    pub(crate) fn build_keeping_with<F: Borrow<FormulaFacts> + GivenId>(
        mut input: Vec<(Cell, F)>,
        prior: Option<&IdentityTable>,
        carried: Option<&FxHashMap<Cell, Vid>>,
        host_ids: bool,
    ) -> Result<(Store, u64), AuthorityError> {
        // The input and everything it owns coexist with the whole build
        // (re-review R5).
        let mut scratch = input.capacity() * size_of::<(Cell, F)>()
            + input
                .iter()
                .map(|(_, f)| f.borrow().owned_heap_bytes())
                .sum::<usize>();
        let mut s = Store::new();
        input.sort_unstable_by_key(|(c, _)| *c);
        input.dedup_by_key(|(c, _)| *c);

        // Intern keys (append-only directories).
        let mut ecells: FxHashMap<u32, Vec<Rect>> = FxHashMap::default();
        let mut ncells: FxHashMap<u32, Vec<Rect>> = FxHashMap::default();
        let mut ungrouped: Vec<usize> = Vec::new();
        let mut by_cell: FxHashMap<Cell, usize> = FxHashMap::default();
        let mut rep_tokens: FxHashMap<u32, usize> = FxHashMap::default();
        for (i, (cell, f)) in input.iter().enumerate() {
            let f: &FormulaFacts = f.borrow();
            by_cell.insert(*cell, i);
            for e in &f.edges {
                let lk = match &e.origin {
                    OriginSpec::Text => NO_LK,
                    OriginSpec::Symbol(k) => {
                        let heap = k.name.len();
                        s.lks
                            .try_reserve(DirPlan {
                                new_keys: 1,
                                new_key_heap: heap,
                            })
                            .expect("build allocation");
                        s.lks.intern(k.clone(), heap)
                    }
                };
                let key = EdgeKey {
                    dep_sheet: cell.0,
                    tag: e.tag,
                    lk,
                    proj: e.proj,
                };
                let g = match s.egroups.get(&key) {
                    Some(g) => g,
                    None => {
                        s.egroups.try_reserve(1).expect("build allocation");
                        s.egroups.insert(key, Group::default())
                    }
                };
                ecells
                    .entry(g)
                    .or_default()
                    .push(Rect::cell(cell.1, cell.2));
            }
            match &f.ltokens {
                Some(t) => {
                    let key = (cell.0, l_hash(t));
                    let g = match s.ngroups.get(&key) {
                        Some(g) => Some(g),
                        None => {
                            s.ngroups.try_reserve(1).expect("build allocation");
                            let g = s.ngroups.insert(key, Group::default());
                            rep_tokens.insert(g, i);
                            Some(g)
                        }
                    };
                    // Verify against the group's first member: a 64-bit
                    // collision leaves the formula ungrouped.
                    match g {
                        Some(g)
                            if input[rep_tokens[&g]].1.borrow().ltokens.as_deref()
                                == Some(&t[..]) =>
                        {
                            ncells
                                .entry(g)
                                .or_default()
                                .push(Rect::cell(cell.1, cell.2));
                        }
                        _ => ungrouped.push(i),
                    }
                }
                None => ungrouped.push(i),
            }
        }
        s.egroups.shrink_entries();
        s.ngroups.shrink_entries();

        {
            use super::super::dir::hash_table_bytes;
            let cells_bytes = |m: &FxHashMap<u32, Vec<Rect>>| {
                hash_table_bytes::<(u32, Vec<Rect>)>(m.capacity())
                    + m.values()
                        .map(|v| v.capacity() * size_of::<Rect>())
                        .sum::<usize>()
            };
            scratch += cells_bytes(&ecells)
                + cells_bytes(&ncells)
                + hash_table_bytes::<(u32, usize)>(rep_tokens.capacity())
                + ungrouped.capacity() * size_of::<usize>();
        }

        // Canon per group, in group order (deterministic). Groups run one at
        // a time, so the canon scratch peak is the largest group's bound.
        scratch += ecells
            .values()
            .chain(ncells.values())
            .map(|v| canon::scratch_bound(v.len()))
            .max()
            .unwrap_or(0);
        let mut work = CanonWork::default();
        let mut epieces: Vec<(u32, Vec<Rect>)> = ecells
            .into_iter()
            .map(|(g, mut cells)| (g, canon::canon_cells(&mut cells, &mut work)))
            .collect();
        epieces.sort_unstable_by_key(|(g, _)| *g);
        let mut npieces: Vec<(u32, Vec<Rect>)> = ncells
            .into_iter()
            .map(|(g, mut cells)| (g, canon::canon_cells(&mut cells, &mut work)))
            .collect();
        npieces.sort_unstable_by_key(|(g, _)| *g);
        s.stats.canon_work = work;
        let pieces_bytes = |v: &[(u32, Vec<Rect>)]| {
            v.iter()
                .map(|(_, p)| p.capacity() * size_of::<Rect>())
                .sum::<usize>()
        };
        scratch += (epieces.capacity() + npieces.capacity()) * size_of::<(u32, Vec<Rect>)>()
            + pieces_bytes(&epieces)
            + pieces_bytes(&npieces);

        // Records.
        let nrecs: usize = epieces.iter().map(|(_, p)| p.len()).sum();
        s.recs.reserve_exact(nrecs);
        s.dep_loc = vec![NONE; nrecs];
        s.prec_loc = vec![NONE; nrecs];
        // Each role's directories are sized to the last sheet it uses, not
        // to the highest sheet any formula sits on (a sparse high sheet
        // must not allocate sheet-sized directories that are then
        // truncated; final gate R5).
        let mut dep_used = 0usize;
        let mut prec_used = 0usize;
        for (g, pieces) in &epieces {
            if !pieces.is_empty() {
                let key = s.egroups.key(*g);
                dep_used = dep_used.max(sheet_slot(key.dep_sheet) + 1);
                prec_used = prec_used.max(sheet_slot(key.proj.sheet) + 1);
            }
        }
        let node_used = npieces
            .iter()
            .filter(|(_, p)| p.iter().any(|r| !r.is_cell()))
            .map(|(g, _)| sheet_slot(s.ngroups.key(*g).0) + 1)
            .max()
            .unwrap_or(0);
        s.idx.dep = (0..dep_used).map(|_| LevelIndex::default()).collect();
        s.idx.prec = (0..prec_used).map(|_| LevelIndex::default()).collect();
        s.idx.node = (0..node_used).map(|_| LevelIndex::default()).collect();
        let mut dep_items: Vec<Vec<(super::super::geom::BoxT, u32)>> = vec![Vec::new(); dep_used];
        let mut prec_items: Vec<Vec<(super::super::geom::BoxT, u32)>> = vec![Vec::new(); prec_used];
        for (g, pieces) in epieces {
            let key = s.egroups.key(g);
            let grp = &mut s.egroups[g as usize];
            grp.set_b(pieces.len());
            grp.members = Members::with_capacity(pieces.len());
            for p in pieces {
                let id = s.recs.len() as u32;
                grp.members.push(id);
                s.recs.push(Rec {
                    dep: p,
                    group: g,
                    pos: (grp.members.len() - 1) as u32,
                });
                dep_items[sheet_slot(key.dep_sheet)].push((p.as_box(), id));
                let pb = key
                    .proj
                    .forward(&p)
                    .expect("members instantiate on the grid");
                prec_items[sheet_slot(key.proj.sheet)].push((pb.as_box(), id));
            }
        }
        for (sheet, items) in dep_items.iter().enumerate().take(s.idx.dep.len()) {
            s.idx.dep[sheet].bulk_load(items, &mut s.dep_loc);
        }
        for (sheet, items) in prec_items.iter().enumerate().take(s.idx.prec.len()) {
            s.idx.prec[sheet].bulk_load(items, &mut s.prec_loc);
        }
        let items_bytes = |v: &[Vec<(super::super::geom::BoxT, u32)>]| {
            v.iter()
                .map(|x| x.capacity() * size_of::<(super::super::geom::BoxT, u32)>())
                .sum::<usize>()
        };
        // Outer item lists, plus the entry copy each bulk load lays out
        // (bounded by the role's total items).
        scratch += items_bytes(&dep_items)
            + items_bytes(&prec_items)
            + (dep_used + prec_used) * size_of::<Vec<(super::super::geom::BoxT, u32)>>()
            + 2 * nrecs * LevelIndex::bulk_entry_bytes();
        drop(dep_items);
        drop(prec_items);

        // Owners: grouped pieces, then ungrouped singletons.
        let nowners = npieces.iter().map(|(_, p)| p.len()).sum::<usize>() + ungrouped.len();
        s.owners.reserve_exact(nowners);
        s.node_loc = vec![NONE; nowners];
        let mut node_items: Vec<Vec<(super::super::geom::BoxT, u32)>> = vec![Vec::new(); node_used];
        let mut placed: Vec<u32> = Vec::with_capacity(nowners);
        for (g, pieces) in npieces {
            let (sheet, _) = s.ngroups.key(g);
            let grp = &mut s.ngroups[g as usize];
            grp.set_b(pieces.len());
            grp.members = Members::with_capacity(pieces.len());
            for p in pieces {
                let id = s.owners.len() as u32;
                let f: &FormulaFacts = input[by_cell[&(sheet, p.r0, p.c0)]].1.borrow();
                grp.members.push(id);
                s.owners.push(Owner {
                    dom: p,
                    sheet,
                    flags: f.flags,
                    group: g,
                    pos: (grp.members.len() - 1) as u32,
                    template: f.template,
                    anchor: f.template_anchor.unwrap_or((p.r0, p.c0)),
                });
                if !p.is_cell() {
                    s.nnodes += 1;
                    node_items[sheet_slot(sheet)].push((p.as_box(), id));
                }
                placed.push(id);
            }
        }
        for i in ungrouped {
            let (cell, f) = &input[i];
            let f: &FormulaFacts = f.borrow();
            let id = s.owners.len() as u32;
            s.owners.push(Owner {
                dom: Rect::cell(cell.1, cell.2),
                sheet: cell.0,
                flags: f.flags,
                group: UNGROUPED,
                pos: 0,
                template: f.template,
                anchor: f.template_anchor.unwrap_or((cell.1, cell.2)),
            });
            placed.push(id);
        }
        for (sheet, items) in node_items.iter().enumerate().take(s.idx.node.len()) {
            s.idx.node[sheet].bulk_load(items, &mut s.node_loc);
        }
        scratch += items_bytes(&node_items)
            + node_used * size_of::<Vec<(super::super::geom::BoxT, u32)>>()
            + (s.nnodes as usize) * LevelIndex::bulk_entry_bytes();

        // Identity (decision 9): cells live in `prior` keep their ids; new
        // cells get fresh ids from its counter, in (sheet, c0, r0) owner
        // order. A run is a maximal row segment of one owner column whose
        // ids are consecutive: all kept and id-contiguous in `prior`, or
        // all new. Without `prior` this is one run per owner column.
        // Given ids (a host store): every cell's id is the host's; the
        // counter only allocates symbol binding identities, from
        // `HOST_SYMBOL_ID_BASE` up.
        let given = host_ids;
        debug_assert!(
            !given || input.iter().all(|(_, f)| f.given_id().is_some()),
            "a host build gives every cell's id"
        );
        if let Some(p) = prior {
            let next = if given || p.next_id() >= HOST_SYMBOL_ID_BASE {
                p.next_id().max(HOST_SYMBOL_ID_BASE)
            } else {
                p.next_id()
            };
            s.ids = IdentityTable::continuing(next, p.limit());
        } else if given {
            s.ids = IdentityTable::continuing(HOST_SYMBOL_ID_BASE, s.ids.limit());
        }
        placed.sort_unstable_by_key(|&o| {
            let w = &s.owners[o as usize];
            (w.sheet, w.dom.c0, w.dom.r0)
        });
        let kept_id = |sheet: u16, row: u32, col: u32| {
            if given {
                return by_cell
                    .get(&(sheet, row, col))
                    .and_then(|&i| input[i].1.given_id());
            }
            match carried {
                Some(m) => m.get(&(sheet, row, col)).copied(),
                None => prior.and_then(|p| p.id_of((sheet, row, col))),
            }
        };
        // (owner, column, first row, length, kept first id).
        let mut segs: Vec<(u32, u32, u32, u32, Option<Vid>)> = Vec::new();
        let mut fresh = 0u64;
        for &o in &placed {
            let w = s.owners[o as usize];
            for c in w.dom.c0..=w.dom.c1 {
                let mut r = w.dom.r0;
                while r <= w.dom.r1 {
                    let first = kept_id(w.sheet, r, c);
                    let mut len = 1u32;
                    while r + len <= w.dom.r1 {
                        let joins = match (first, kept_id(w.sheet, r + len, c)) {
                            (None, None) => true,
                            (Some(a), Some(b)) => u64::from(b) == u64::from(a) + u64::from(len),
                            _ => false,
                        };
                        if !joins {
                            break;
                        }
                        len += 1;
                    }
                    if first.is_none() {
                        fresh += u64::from(len);
                    }
                    segs.push((o, c, r, len, first));
                    r += len;
                }
            }
        }
        let mut target = s.ids.shadow();
        for &(o, _, _, len, first) in &segs {
            let new = if first.is_none() { u64::from(len) } else { 0 };
            IdentityTable::shadow_place(&mut target, s.owners[o as usize].sheet, 1, new);
        }
        s.ids.check_alloc(fresh).map_err(AuthorityError::Identity)?;
        s.ids.try_reserve_for(&target).expect("build allocation");
        scratch += target.fwd.capacity() * size_of::<super::super::avl::SlabShadow>();
        let mut rows: Vec<(Vid, usize)> = Vec::new();
        for &(o, c, r, len, first) in &segs {
            let w = s.owners[o as usize];
            let run_owner = if w.is_family() { FAMILY } else { o };
            let h = match first {
                None => s.ids.place(w.sheet, r, c, len, run_owner),
                Some(f) => s.ids.place_existing(w.sheet, r, c, len, f, run_owner),
            };
            let first_id = s.ids.run(h).first_id;
            for i in 0..len {
                let f: &FormulaFacts = input[by_cell[&(w.sheet, r + i, c)]].1.borrow();
                if !f.literals.is_empty() {
                    rows.push((first_id + i, f.literals.len()));
                }
            }
        }
        scratch += segs.capacity() * size_of::<(u32, u32, u32, u32, Option<Vid>)>()
            + placed.capacity() * 4
            + rows.capacity() * size_of::<(Vid, usize)>();
        let plan = s.slots.plan(&[], &rows);
        s.stats.slot_work += plan.work;
        s.slots.try_reserve(&plan).expect("build allocation");
        let mut payload: Vec<(Vid, &[ValueRef])> = Vec::with_capacity(rows.len());
        for &(id, _) in &rows {
            let cell = s.ids.locate(id).expect("placed id");
            payload.push((id, &input[by_cell[&cell]].1.borrow().literals[..]));
        }
        s.slots.apply(&plan, &[], &payload);
        scratch += payload.capacity() * size_of::<(Vid, &[ValueRef])>()
            + plan.scratch_bytes()
            + super::super::dir::hash_table_bytes::<(Cell, usize)>(by_cell.capacity());
        s.init_accounting();
        Ok((s, scratch as u64))
    }
}
