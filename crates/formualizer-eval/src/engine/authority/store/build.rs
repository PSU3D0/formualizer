//! Bulk build (§5.7.1: the production build of a group *is* canon of its
//! extracted cell set).

use super::*;

/// One formula cell and its facts.
pub type BuildInput = (Cell, FormulaFacts);

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
        let (mut s, scratch) = Self::build_keeping(input, prior.map(Store::ids))?;
        s.budget = budget;
        let transient = scratch + prior.map_or(0, Store::heap_bytes);
        s.admit(s.heap_bytes(), transient)?;
        Ok(s)
    }

    /// The build, keeping the identities of `prior` when given. Returns the
    /// store and an upper bound on the build's scratch bytes (the sum of
    /// its temporary containers' capacities).
    pub(crate) fn build_keeping(
        mut input: Vec<BuildInput>,
        prior: Option<&IdentityTable>,
    ) -> Result<(Store, u64), AuthorityError> {
        let mut scratch = input.capacity() * size_of::<BuildInput>();
        let mut s = Store::new();
        input.sort_unstable_by_key(|(c, _)| *c);
        input.dedup_by_key(|(c, _)| *c);

        // Intern keys (append-only directories).
        let mut ecells: FxHashMap<u32, Vec<Rect>> = FxHashMap::default();
        let mut ncells: FxHashMap<u32, Vec<Rect>> = FxHashMap::default();
        let mut ungrouped: Vec<usize> = Vec::new();
        let mut max_sheet = 0usize;
        let mut by_cell: FxHashMap<Cell, usize> = FxHashMap::default();
        let mut rep_tokens: FxHashMap<u32, usize> = FxHashMap::default();
        for (i, (cell, f)) in input.iter().enumerate() {
            by_cell.insert(*cell, i);
            max_sheet = max_sheet.max(cell.0 as usize + 1);
            for e in &f.edges {
                max_sheet = max_sheet.max(e.proj.sheet as usize + 1);
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
                        Some(g) if input[rep_tokens[&g]].1.ltokens.as_deref() == Some(&t[..]) => {
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

        // Canon per group, in group order (deterministic).
        let mut work = CanonWork::default();
        let mut epieces: Vec<(u32, Vec<Rect>)> = ecells
            .into_iter()
            .map(|(g, cells)| (g, canon::canon(&cells, &mut work)))
            .collect();
        epieces.sort_unstable_by_key(|(g, _)| *g);
        let mut npieces: Vec<(u32, Vec<Rect>)> = ncells
            .into_iter()
            .map(|(g, cells)| (g, canon::canon(&cells, &mut work)))
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
        s.idx.dep = (0..max_sheet).map(|_| LevelIndex::default()).collect();
        s.idx.prec = (0..max_sheet).map(|_| LevelIndex::default()).collect();
        s.idx.node = (0..max_sheet).map(|_| LevelIndex::default()).collect();
        let mut dep_items: Vec<Vec<(super::super::geom::BoxT, u32)>> = vec![Vec::new(); max_sheet];
        let mut prec_items: Vec<Vec<(super::super::geom::BoxT, u32)>> = vec![Vec::new(); max_sheet];
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
                dep_items[key.dep_sheet as usize].push((p.as_box(), id));
                let pb = key
                    .proj
                    .forward(&p)
                    .expect("members instantiate on the grid");
                prec_items[key.proj.sheet as usize].push((pb.as_box(), id));
            }
        }
        // Index vectors only as long as the last sheet each role uses.
        let used = |v: &[Vec<(super::super::geom::BoxT, u32)>]| {
            v.iter().rposition(|x| !x.is_empty()).map_or(0, |i| i + 1)
        };
        s.idx.dep.truncate(used(&dep_items));
        s.idx.dep.shrink_to_fit();
        s.idx.prec.truncate(used(&prec_items));
        s.idx.prec.shrink_to_fit();
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
        scratch += items_bytes(&dep_items) + items_bytes(&prec_items);
        drop(dep_items);
        drop(prec_items);

        // Owners: grouped pieces, then ungrouped singletons.
        let nowners = npieces.iter().map(|(_, p)| p.len()).sum::<usize>() + ungrouped.len();
        s.owners.reserve_exact(nowners);
        s.node_loc = vec![NONE; nowners];
        let mut node_items: Vec<Vec<(super::super::geom::BoxT, u32)>> = vec![Vec::new(); max_sheet];
        let mut placed: Vec<u32> = Vec::with_capacity(nowners);
        for (g, pieces) in npieces {
            let (sheet, _) = s.ngroups.key(g);
            let grp = &mut s.ngroups[g as usize];
            grp.set_b(pieces.len());
            grp.members = Members::with_capacity(pieces.len());
            for p in pieces {
                let id = s.owners.len() as u32;
                let f = &input[by_cell[&(sheet, p.r0, p.c0)]].1;
                grp.members.push(id);
                s.owners.push(Owner {
                    dom: p,
                    sheet,
                    flags: f.flags,
                    group: g,
                    pos: (grp.members.len() - 1) as u32,
                    template: f.template,
                    anchor: (p.r0, p.c0),
                });
                if !p.is_cell() {
                    s.nnodes += 1;
                    node_items[sheet as usize].push((p.as_box(), id));
                }
                placed.push(id);
            }
        }
        for i in ungrouped {
            let (cell, f) = &input[i];
            let id = s.owners.len() as u32;
            s.owners.push(Owner {
                dom: Rect::cell(cell.1, cell.2),
                sheet: cell.0,
                flags: f.flags,
                group: UNGROUPED,
                pos: 0,
                template: f.template,
                anchor: (cell.1, cell.2),
            });
            placed.push(id);
        }
        s.idx.node.truncate(used(&node_items));
        s.idx.node.shrink_to_fit();
        for (sheet, items) in node_items.iter().enumerate().take(s.idx.node.len()) {
            s.idx.node[sheet].bulk_load(items, &mut s.node_loc);
        }
        scratch += items_bytes(&node_items);

        // Identity (decision 9): cells live in `prior` keep their ids; new
        // cells get fresh ids from its counter, in (sheet, c0, r0) owner
        // order. A run is a maximal row segment of one owner column whose
        // ids are consecutive: all kept and id-contiguous in `prior`, or
        // all new. Without `prior` this is one run per owner column.
        if let Some(p) = prior {
            s.ids = IdentityTable::continuing(p.next_id(), p.limit());
        }
        placed.sort_unstable_by_key(|&o| {
            let w = &s.owners[o as usize];
            (w.sheet, w.dom.c0, w.dom.r0)
        });
        let kept_id =
            |sheet: u16, row: u32, col: u32| prior.and_then(|p| p.id_of((sheet, row, col)));
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
                let f = &input[by_cell[&(w.sheet, r + i, c)]].1;
                if !f.literals.is_empty() {
                    rows.push((first_id + i, f.literals.len()));
                }
            }
        }
        scratch += segs.capacity() * size_of::<(u32, u32, u32, u32, Option<Vid>)>()
            + placed.capacity() * 4
            + rows.capacity() * size_of::<(Vid, usize)>();
        let plan = s.slots.plan(&[], &rows);
        s.slots.try_reserve(&plan).expect("build allocation");
        let mut payload: Vec<(Vid, &[ValueRef])> = Vec::with_capacity(rows.len());
        for &(id, _) in &rows {
            let cell = s.ids.locate(id).expect("placed id");
            payload.push((id, &input[by_cell[&cell]].1.literals[..]));
        }
        s.slots.apply(&plan, &[], &payload);
        scratch += payload.capacity() * size_of::<(Vid, &[ValueRef])>()
            + super::super::dir::hash_table_bytes::<(Cell, usize)>(by_cell.capacity());
        Ok((s, scratch as u64))
    }
}
