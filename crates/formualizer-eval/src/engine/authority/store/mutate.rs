//! Non-structural mutations as admitted mutation scopes (§5, §5.1, §5.6).
//!
//! A scope is plan → dry run → admit → reserve → apply → repartition.
//!
//! M1a correction B5 (addendum B-19): the plan and the dry run allocate
//! only through fallible reservations; the reservation makes every
//! allocation the apply needs (in-place growth of existing containers,
//! pre-built index levels and merge buffers, new slot pages, group-index
//! arrays, owned directory keys); the apply allocates nothing. An
//! allocation failure before the apply returns `Alloc` with the logical
//! state unchanged. Containers grown in place by the reservations that did
//! succeed keep their spare capacity, which the retained bytes count (it is
//! capacity, not live model bytes); every staged buffer is freed.
//!
//! The scratch budget is charged with the scope's peak above its retained
//! bytes after the apply: all planning scratch, everything the reservation
//! allocates, and the old buffer of the largest in-place reallocation. The
//! accounting reads maintained totals of the touched containers only
//! (correction B2), so it is independent of the store size.

use super::*;

/// Push within capacity or grow fallibly (planning scratch).
pub(super) fn tpush<T>(v: &mut Vec<T>, x: T) -> Result<(), AuthorityError> {
    if v.len() == v.capacity() {
        v.try_reserve(1).map_err(|_| AuthorityError::Alloc)?;
    }
    v.push(x);
    Ok(())
}

/// An empty vector with capacity `n`, fallibly.
pub(super) fn try_vec<T>(n: usize) -> Result<Vec<T>, AuthorityError> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| AuthorityError::Alloc)?;
    Ok(v)
}

fn alloc_err<E>(_: E) -> AuthorityError {
    AuthorityError::Alloc
}

/// An owned copy of a lookup-slot key, fallibly.
fn try_clone_lk(k: &LkKey) -> Result<LkKey, AuthorityError> {
    let mut s = String::new();
    s.try_reserve_exact(k.name.len()).map_err(alloc_err)?;
    s.push_str(&k.name);
    Ok(LkKey {
        ctx: k.ctx,
        kind: k.kind,
        name: s.into_boxed_str(),
    })
}

/// Byte plan of one scope: retained bytes replaced (old → new), bytes
/// allocated before the apply, and the old buffer of the largest in-place
/// reallocation.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Bytes {
    pub old: usize,
    pub new: usize,
    pub alloc: usize,
    pub max_old: usize,
}

impl Bytes {
    /// A container reallocated in place from `old` to `new` bytes.
    pub fn grow(&mut self, old: usize, new: usize) {
        self.old += old;
        self.new += new;
        if new > old {
            self.alloc += new - old;
            self.max_old = self.max_old.max(old);
        }
    }

    /// Retained bytes that change without an in-place reallocation (freed,
    /// or built from buffers counted in `alloc`).
    pub fn retained(&mut self, old: usize, new: usize) {
        self.old += old;
        self.new += new;
    }

    pub fn after(&self, before: u64) -> u64 {
        before - self.old as u64 + self.new as u64
    }

    /// Peak heap of the scope: the retained bytes, the planning scratch and
    /// everything the reservation allocates coexist; a reallocation also
    /// holds its old buffer briefly. The apply only frees.
    pub fn peak(&self, before: u64, scratch: usize) -> u64 {
        before + (scratch + self.alloc + self.max_old) as u64
    }
}

/// Everything a mutation's dry run predicts.
#[derive(Debug)]
pub(super) struct Prediction<'a> {
    pub counts: Counts,
    pub transient: u64,
    recs_cap: usize,
    owners_cap: usize,
    eplan: GroupPlan,
    nplan: GroupPlan,
    /// `(group, member-list capacity after)` for touched surviving groups.
    emembers: Vec<(u32, usize)>,
    nmembers: Vec<(u32, usize)>,
    /// Existing groups whose last member leaves: reclaimed by the apply.
    ereclaim: Vec<u32>,
    nreclaim: Vec<u32>,
    dir_lk: DirPlan,
    new_lks: Vec<&'a LkKey>,
    id_target: IdShadow,
    slot_plan: super::super::slots::SlotPlan,
    slot_removes: Vec<Vid>,
    slot_rows: Vec<(Vid, &'a [ValueRef])>,
    idx: [IndexPlan; 3],
    /// Repartition candidates (capacity for the new groups the apply adds).
    eg: Vec<u32>,
    ng: Vec<u32>,
    /// Edge group of each new edge (filled by the apply).
    egroup_of: Vec<u32>,
    work: u64,
}

impl Store {
    // ------------------------------------------------------------ public API

    /// Set a formula at `cell` (value/empty → formula, or formula →
    /// formula keeping the cell's id). One admitted mutation scope.
    pub fn set_formula(
        &mut self,
        cell: Cell,
        facts: &FormulaFacts,
    ) -> Result<MutationReport, AuthorityError> {
        self.mutate(
            cell.0,
            Rect::cell(cell.1, cell.2),
            Some((cell, facts)),
            None,
        )
    }

    /// [`Self::set_formula`] for history replay (ID4): a value/empty cell
    /// gets the retired id `revive` back instead of a fresh one. The id
    /// must have been allocated and must not be live. A cell that already
    /// holds a formula keeps its own id.
    pub fn set_formula_reviving(
        &mut self,
        cell: Cell,
        facts: &FormulaFacts,
        revive: Vid,
    ) -> Result<MutationReport, AuthorityError> {
        if revive >= self.ids.next_id() {
            return Err(AuthorityError::Identity(IdError::Conflict(format!(
                "id {revive} was never allocated"
            ))));
        }
        if let Some(c) = self.ids.locate(revive)
            && c != cell
        {
            return Err(AuthorityError::Identity(IdError::Conflict(format!(
                "id {revive} is live at {c:?}"
            ))));
        }
        self.mutate(
            cell.0,
            Rect::cell(cell.1, cell.2),
            Some((cell, facts)),
            Some(revive),
        )
    }

    /// [`Self::set_formula`] for a host store (Program 2): the formula's id
    /// is `id`, the host's (the executor vertex at the cell). A cell whose
    /// live id is another one (the host replaced the cell's vertex) is
    /// cleared first, retiring that id.
    pub fn set_formula_given(
        &mut self,
        cell: Cell,
        facts: &FormulaFacts,
        id: Vid,
    ) -> Result<MutationReport, AuthorityError> {
        debug_assert!(id < HOST_SYMBOL_ID_BASE, "host id in the symbol range");
        match self.ids.id_of(cell) {
            Some(live) if live == id => self.set_formula(cell, facts),
            Some(_) => {
                self.clear_cell(cell)?;
                self.set_formula_reviving(cell, facts, id)
            }
            None => self.set_formula_reviving(cell, facts, id),
        }
    }

    /// Clear the formula at `cell` (formula → value/empty). A cell without a
    /// formula is a no-op scope.
    pub fn clear_cell(&mut self, cell: Cell) -> Result<MutationReport, AuthorityError> {
        self.mutate(cell.0, Rect::cell(cell.1, cell.2), None, None)
    }

    /// Clear every formula in `q` on `sheet` (range clear).
    pub fn clear_rect(&mut self, sheet: u16, q: Rect) -> Result<MutationReport, AuthorityError> {
        self.mutate(sheet, q, None, None)
    }

    // ------------------------------------------------------------ plan

    fn plan_cut(&self, sheet: u16, q: &Rect, keep: Option<Cell>) -> Result<Cut, AuthorityError> {
        let mut cut = Cut::default();
        let mut oom = false;
        if let Some(idx) = self.idx.dep.get(sheet_slot(sheet)) {
            idx.query(&q.as_box(), &mut |id| {
                oom |= tpush(&mut cut.recs, id).is_err();
            });
        }
        if oom {
            return Err(AuthorityError::Alloc);
        }
        cut.recs.sort_unstable();
        for &id in &cut.recs {
            let r = &self.recs[id as usize];
            for p in r.dep.subtract(q).as_slice() {
                tpush(&mut cut.rec_pieces, (r.group, *p))?;
            }
        }
        self.ids
            .try_plan_cuts_rect(sheet, (q.r0, q.c0, q.r1, q.c1), &mut cut.id_cuts)
            .map_err(alloc_err)?;
        if let Some(k) = keep {
            cut.keep = cut.id_cuts.iter().position(|c| {
                c.is_cell() && c.run_value.row_start + c.lo == k.1 && c.run_value.col == k.2
            });
        }
        // Owners: families through the node index, singletons through runs.
        if let Some(idx) = self.idx.node.get(sheet_slot(sheet)) {
            idx.query(&q.as_box(), &mut |o| {
                oom |= tpush(&mut cut.owners, o).is_err();
            });
        }
        if oom {
            return Err(AuthorityError::Alloc);
        }
        for c in &cut.id_cuts {
            if c.run_value.owner != FAMILY {
                tpush(&mut cut.owners, c.run_value.owner)?;
            }
        }
        cut.owners.sort_unstable();
        cut.owners.dedup();
        for &o in &cut.owners {
            let w = &self.owners[o as usize];
            if w.is_family() {
                for p in w.dom.subtract(q).as_slice() {
                    tpush(
                        &mut cut.owner_pieces,
                        (w.group, *p, w.template, w.anchor, w.flags),
                    )?;
                }
            }
        }
        Ok(cut)
    }

    fn plan_new<'a>(
        &self,
        cell: Cell,
        f: &'a FormulaFacts,
        cut: &Cut,
        revive: Option<Vid>,
    ) -> Result<NewFormula<'a>, AuthorityError> {
        let mut edges = try_vec(f.edges.len())?;
        for e in &f.edges {
            let lk = match &e.origin {
                OriginSpec::Text => Ok(NO_LK),
                OriginSpec::Symbol(k) => self.lks.get(k).ok_or(k),
            };
            let group = match lk {
                Ok(lk) => self.egroups.get(&EdgeKey {
                    dep_sheet: cell.0,
                    tag: e.tag,
                    lk,
                    proj: e.proj,
                }),
                Err(_) => None,
            };
            edges.push((
                group,
                PendingEdgeKey {
                    dep_sheet: cell.0,
                    tag: e.tag,
                    lk,
                    proj: e.proj,
                },
            ));
        }
        let ngroup = f.ltokens.as_ref().map(|t| {
            let key = (cell.0, l_hash(t));
            self.ngroups.get(&key).ok_or(key)
        });
        Ok(NewFormula {
            cell,
            edges,
            ngroup,
            template: f.template,
            anchor: f.template_anchor.unwrap_or((cell.1, cell.2)),
            literals: &f.literals[..],
            flags: f.flags,
            kept_id: cut.keep.map(|i| cut.id_cuts[i].id()),
            revive: revive.filter(|_| cut.keep.is_none()),
        })
    }

    // ------------------------------------------------------------ dry run

    /// Fold `(group, ±1)` member deltas (sorted in place) into surviving
    /// groups' member capacities and reclaimed groups.
    fn fold_members<K: GroupKey>(
        table: &GroupTable<K>,
        deltas: &mut [(u32, i64)],
        b: &mut Bytes,
        keep: &mut Vec<(u32, usize)>,
        reclaim: &mut Vec<u32>,
        work: &mut u64,
    ) -> Result<(), AuthorityError> {
        deltas.sort_unstable_by_key(|d| d.0);
        let mut i = 0;
        while i < deltas.len() {
            let g = deltas[i].0;
            let mut net = 0i64;
            while i < deltas.len() && deltas[i].0 == g {
                net += deltas[i].1;
                i += 1;
            }
            *work += 1;
            let m = &table[g as usize].members;
            let fin = (m.len() as i64 + net) as usize;
            if fin == 0 {
                b.retained(members_heap(m), 0);
                tpush(reclaim, g)?;
            } else {
                let cap = members_cap_after(m, fin);
                b.grow(members_heap(m), members_heap_for(cap));
                tpush(keep, (g, cap))?;
            }
        }
        Ok(())
    }

    /// Account a group table's planned entries, index and staged arrays.
    fn account_groups<K: GroupKey>(table: &GroupTable<K>, plan: &GroupPlan, b: &mut Bytes) {
        let (entries, index) = table.fixed_parts();
        b.grow(entries, plan.entries_cap * GroupTable::<K>::ENTRY_BYTES);
        b.retained(index, plan.index.cap * size_of::<u32>());
        b.alloc += plan.staged_bytes;
    }

    /// Exact prediction of every container after `cut` + `newf` (§5.1),
    /// with the scope's transient bytes.
    fn predict<'a>(
        &self,
        sheet: u16,
        cut: &Cut,
        newf: Option<&NewFormula<'a>>,
    ) -> Result<Prediction<'a>, AuthorityError> {
        let mut b = Bytes::default();
        let before = self.counts();
        let mut work = (cut.recs.len()
            + cut.rec_pieces.len()
            + cut.owners.len()
            + cut.owner_pieces.len()
            + cut.id_cuts.len()) as u64;

        // New directory keys and edge groups; member deltas of existing
        // groups (removals first, then additions: the apply's order).
        let mut new_lks: Vec<&'a LkKey> = Vec::new();
        let mut new_ekeys = 0usize;
        let mut new_ngroup = false;
        let mut ed: Vec<(u32, i64)> = Vec::new();
        let mut nd: Vec<(u32, i64)> = Vec::new();
        for &r in &cut.recs {
            tpush(&mut ed, (self.recs[r as usize].group, -1))?;
        }
        for &(g, _) in &cut.rec_pieces {
            tpush(&mut ed, (g, 1))?;
        }
        if let Some(n) = newf {
            for (g, k) in &n.edges {
                work += 1;
                match g {
                    Some(g) => tpush(&mut ed, (*g, 1))?,
                    None => {
                        if let Err(key) = k.lk {
                            tpush(&mut new_lks, key)?;
                        }
                        new_ekeys += 1;
                    }
                }
            }
            new_ngroup = matches!(n.ngroup, Some(Err(_)));
        }
        new_lks.sort_unstable();
        new_lks.dedup();
        let dir_lk = DirPlan {
            new_keys: new_lks.len(),
            new_key_heap: new_lks.iter().map(|k| k.name.len()).sum(),
        };
        let [hash, keys, heap] = self.lks.plan_parts(dir_lk);
        b.grow(hash.0, hash.1);
        b.grow(keys.0, keys.1);
        b.retained(heap.0, heap.1);
        b.alloc += (heap.1 - heap.0)
            + if dir_lk.new_keys > 0 {
                Directory::<LkKey>::stage_list_bytes(dir_lk.new_keys)
            } else {
                0
            };

        for &o in &cut.owners {
            let g = self.owners[o as usize].group;
            if g != UNGROUPED {
                tpush(&mut nd, (g, -1))?;
            }
        }
        for &(g, ..) in &cut.owner_pieces {
            if g != UNGROUPED {
                tpush(&mut nd, (g, 1))?;
            }
        }
        if let Some(Some(Ok(g))) = newf.map(|n| &n.ngroup) {
            tpush(&mut nd, (*g, 1))?;
        }
        let deltas_scratch = (ed.capacity() + nd.capacity()) * size_of::<(u32, i64)>();
        let (mut emembers, mut ereclaim) = (Vec::new(), Vec::new());
        Self::fold_members(
            &self.egroups,
            &mut ed,
            &mut b,
            &mut emembers,
            &mut ereclaim,
            &mut work,
        )?;
        let (mut nmembers, mut nreclaim) = (Vec::new(), Vec::new());
        Self::fold_members(
            &self.ngroups,
            &mut nd,
            &mut b,
            &mut nmembers,
            &mut nreclaim,
            &mut work,
        )?;
        drop((ed, nd));
        let eplan = self.egroups.plan(new_ekeys, ereclaim.len());
        let nplan = self.ngroups.plan(usize::from(new_ngroup), nreclaim.len());
        Self::account_groups(&self.egroups, &eplan, &mut b);
        Self::account_groups(&self.ngroups, &nplan, &mut b);

        // Records arena and location maps.
        let new_recs = newf.map_or(0, |n| n.edges.len());
        let (_, recs_cap) = slab_after(
            self.recs.len(),
            self.rec_nfree,
            self.recs.capacity(),
            cut.recs.len(),
            cut.rec_pieces.len() + new_recs,
        );
        b.grow(
            vec_bytes::<Rec>(self.recs.capacity()),
            vec_bytes::<Rec>(recs_cap),
        );
        for loc in [&self.dep_loc, &self.prec_loc] {
            b.grow(
                vec_bytes::<u32>(loc.capacity()),
                vec_bytes::<u32>(loc.capacity().max(recs_cap)),
            );
        }

        // Owners arena and node location map.
        let new_owner = usize::from(newf.is_some());
        let (_, owners_cap) = slab_after(
            self.owners.len(),
            self.own_nfree,
            self.owners.capacity(),
            cut.owners.len(),
            cut.owner_pieces.len() + new_owner,
        );
        b.grow(
            vec_bytes::<Owner>(self.owners.capacity()),
            vec_bytes::<Owner>(owners_cap),
        );
        b.grow(
            vec_bytes::<u32>(self.node_loc.capacity()),
            vec_bytes::<u32>(self.node_loc.capacity().max(owners_cap)),
        );

        // Indexes: removes → settle → inserts, per role.
        let mut dep = IndexPlan::new(&self.idx.dep);
        let mut prec = IndexPlan::new(&self.idx.prec);
        let mut node = IndexPlan::new(&self.idx.node);
        for &r in &cut.recs {
            let rec = &self.recs[r as usize];
            let key = self.egroups.key(rec.group);
            dep.get(&self.idx.dep, key.dep_sheet)?
                .remove(self.dep_loc[r as usize]);
            prec.get(&self.idx.prec, key.proj.sheet)?
                .remove(self.prec_loc[r as usize]);
        }
        for &o in &cut.owners {
            let w = &self.owners[o as usize];
            if w.is_family() {
                node.get(&self.idx.node, w.sheet)?
                    .remove(self.node_loc[o as usize]);
            }
        }
        dep.settle()?;
        prec.settle()?;
        node.settle()?;
        for &(g, p) in &cut.rec_pieces {
            let key = self.egroups.key(g);
            dep.get(&self.idx.dep, key.dep_sheet)?
                .try_insert(p.is_cell())
                .map_err(alloc_err)?;
            let pb = key
                .proj
                .forward(&p)
                .expect("members instantiate on the grid");
            prec.get(&self.idx.prec, key.proj.sheet)?
                .try_insert(pb.is_cell())
                .map_err(alloc_err)?;
        }
        if let Some(n) = newf {
            let x = Rect::cell(n.cell.1, n.cell.2);
            for (_, k) in &n.edges {
                dep.get(&self.idx.dep, k.dep_sheet)?
                    .try_insert(true)
                    .map_err(alloc_err)?;
                let pb = k.proj.forward(&x).expect("new formula instantiates");
                prec.get(&self.idx.prec, k.proj.sheet)?
                    .try_insert(pb.is_cell())
                    .map_err(alloc_err)?;
            }
        }
        for &(_, p, ..) in &cut.owner_pieces {
            if !p.is_cell() {
                node.get(&self.idx.node, sheet)?
                    .try_insert(false)
                    .map_err(alloc_err)?;
            }
        }
        // A new formula needs index vectors for its sheet.
        if newf.is_some() {
            dep.get(&self.idx.dep, sheet)?;
            node.get(&self.idx.node, sheet)?;
        }
        let mut entries = [0i64; 3];
        for (role, plan, v) in [
            (DEP, &dep, &self.idx.dep),
            (PREC, &prec, &self.idx.prec),
            (NODE, &node, &self.idx.node),
        ] {
            entries[role] = plan.account(v, &mut b);
            work += plan.touched.len() as u64 + plan.probes;
        }

        // Identity (the shadow's forward list is reserved for `sheet`, so
        // the replay never allocates).
        let mut id_target = self.ids.try_shadow_sheet(sheet).map_err(alloc_err)?;
        for (i, c) in cut.id_cuts.iter().enumerate() {
            IdentityTable::shadow_cut(&mut id_target, c, cut.keep == Some(i));
        }
        let mut new_id = None;
        if let Some(n) = newf
            && n.kept_id.is_none()
        {
            if n.revive.is_some() {
                IdentityTable::shadow_place(&mut id_target, n.cell.0, 1, 0);
            } else {
                self.ids.check_alloc(1).map_err(AuthorityError::Identity)?;
                new_id = Some(self.ids.next_id());
                IdentityTable::shadow_place(&mut id_target, n.cell.0, 1, 1);
            }
        }
        {
            let cur = self.ids.shadow_caps();
            b.grow(
                cur.runs * size_of::<IdRun>(),
                id_target.runs.cap * size_of::<IdRun>(),
            );
            b.grow(
                cur.fwd_dir * size_of::<AvlMap>(),
                id_target.fwd_dir_cap.max(cur.fwd_dir) * size_of::<AvlMap>(),
            );
            // The window holds the mutated sheet only.
            for (i, t) in id_target.fwd.iter().enumerate() {
                work += 1;
                b.grow(
                    self.ids.fwd_bytes_of(id_target.fwd_base + i),
                    t.cap * AvlMap::NODE_BYTES,
                );
            }
            b.grow(
                cur.rev * AvlMap::NODE_BYTES,
                id_target.rev.cap * AvlMap::NODE_BYTES,
            );
        }

        // Slots: retired ids and the cell's replaced row.
        let nremove: usize = cut.id_cuts.iter().map(|c| (c.hi - c.lo + 1) as usize).sum();
        let mut slot_removes: Vec<Vid> = try_vec(nremove)?;
        for c in &cut.id_cuts {
            slot_removes.extend(c.ids());
        }
        let mut slot_rows: Vec<(Vid, &'a [ValueRef])> = try_vec(usize::from(newf.is_some()))?;
        let mut slot_inserts: Vec<(Vid, usize)> = try_vec(usize::from(newf.is_some()))?;
        if let Some(n) = newf {
            let id = n
                .kept_id
                .or(n.revive)
                .or(new_id)
                .expect("an id for the new formula");
            slot_rows.push((id, n.literals));
            slot_inserts.push((id, n.literals.len()));
        }
        let slot_plan = self
            .slots
            .try_plan(&slot_removes, &slot_inserts)
            .map_err(alloc_err)?;
        work += slot_plan.work;
        b.retained(self.slots.heap_bytes(), slot_plan.bytes_after);
        b.alloc += slot_plan.alloc_bytes;
        b.max_old = b.max_old.max(slot_plan.max_realloc_old);

        // Repartition candidates, with room for the groups the apply adds.
        let mut eg = try_vec(cut.rec_pieces.len() + new_recs + emembers.len())?;
        eg.extend(cut.rec_pieces.iter().map(|p| p.0));
        eg.extend(emembers.iter().map(|m| m.0));
        let mut ng = try_vec(nmembers.len() + 1)?;
        ng.extend(nmembers.iter().map(|m| m.0));
        let egroup_of = try_vec(new_recs)?;

        // All planning scratch coexists with the reservation.
        let scratch = cut.recs.capacity() * 4
            + cut.rec_pieces.capacity() * size_of::<(u32, Rect)>()
            + cut.owners.capacity() * 4
            + cut.owner_pieces.capacity() * size_of::<OwnerPiece>()
            + cut.id_cuts.capacity() * size_of::<CellCut>()
            + newf.map_or(0, |n| {
                n.edges.capacity() * size_of::<(Option<u32>, PendingEdgeKey<'_>)>()
            })
            + new_lks.capacity() * size_of::<&LkKey>()
            + deltas_scratch
            + (emembers.capacity() + nmembers.capacity()) * size_of::<(u32, usize)>()
            + (ereclaim.capacity() + nreclaim.capacity()) * 4
            + dep.scratch_bytes()
            + prec.scratch_bytes()
            + node.scratch_bytes()
            + id_target.fwd.capacity() * size_of::<super::super::avl::SlabShadow>()
            + slot_removes.capacity() * size_of::<Vid>()
            + slot_rows.capacity() * size_of::<(Vid, &[ValueRef])>()
            + slot_inserts.capacity() * size_of::<(Vid, usize)>()
            + slot_plan.scratch_bytes()
            + (eg.capacity() + ng.capacity() + egroup_of.capacity()) * 4;

        let removed_nodes = cut
            .owners
            .iter()
            .filter(|&&o| self.owners[o as usize].is_family())
            .count() as u64;
        let added_nodes = cut.owner_pieces.iter().filter(|p| !p.1.is_cell()).count() as u64;
        let slot_rows_after = (before.slot_rows as i64 + slot_plan.rows_delta) as u64;
        let after = b.after(before.bytes);
        let peak = b.peak(before.bytes, scratch);
        let counts = Counts {
            records: before.records - cut.recs.len() as u64
                + (cut.rec_pieces.len() + new_recs) as u64,
            owners: before.owners - cut.owners.len() as u64
                + (cut.owner_pieces.len() + new_owner) as u64,
            nodes: before.nodes - removed_nodes + added_nodes,
            runs: id_target.live_runs as u64,
            next_id: id_target.next_id,
            dep_entries: (before.dep_entries as i64 + entries[DEP]) as u64,
            prec_entries: (before.prec_entries as i64 + entries[PREC]) as u64,
            node_entries: (before.node_entries as i64 + entries[NODE]) as u64,
            slot_rows: slot_rows_after,
            bytes: after,
        };
        Ok(Prediction {
            counts,
            transient: peak.saturating_sub(after),
            recs_cap,
            owners_cap,
            eplan,
            nplan,
            emembers,
            nmembers,
            ereclaim,
            nreclaim,
            dir_lk,
            new_lks,
            id_target,
            slot_plan,
            slot_removes,
            slot_rows,
            idx: [dep, prec, node],
            eg,
            ng,
            egroup_of,
            work,
        })
    }

    /// Admission (§5.6): predicted retained bytes against the retained
    /// budget, predicted transient bytes against the scratch budget.
    pub(super) fn admit(&self, retained: u64, transient: u64) -> Result<(), AuthorityError> {
        if let Some(limit) = self.budget.retained
            && retained > limit
        {
            return Err(AuthorityError::Admission {
                resource: "retained",
                needed: retained,
                limit,
            });
        }
        if let Some(limit) = self.budget.scratch
            && transient > limit
        {
            return Err(AuthorityError::Admission {
                resource: "scratch",
                needed: transient,
                limit,
            });
        }
        Ok(())
    }

    /// Reserve every allocation of the apply (fallible, before any logical
    /// change). On error every staged buffer is freed.
    fn reserve(&mut self, p: &Prediction<'_>) -> Result<(), AuthorityError> {
        let r = self.try_reserve_all(p);
        if r.is_err() {
            self.abort_stage();
        }
        r
    }

    /// Free every buffer staged for a scope that will not apply.
    pub(super) fn abort_stage(&mut self) {
        self.stage = Vec::new();
        self.slots.drop_staged();
        self.egroups.drop_staged();
        self.ngroups.drop_staged();
        self.lks.drop_staged();
    }

    fn try_reserve_all(&mut self, p: &Prediction<'_>) -> Result<(), AuthorityError> {
        grow_exact(&mut self.recs, p.recs_cap)?;
        grow_exact(&mut self.owners, p.owners_cap)?;
        grow_exact(&mut self.dep_loc, p.recs_cap)?;
        grow_exact(&mut self.prec_loc, p.recs_cap)?;
        grow_exact(&mut self.node_loc, p.owners_cap)?;
        self.egroups.try_reserve_ops(&p.eplan).map_err(alloc_err)?;
        self.ngroups.try_reserve_ops(&p.nplan).map_err(alloc_err)?;
        for &(g, cap) in &p.emembers {
            self.egroups.grow_members(g, cap).map_err(alloc_err)?;
        }
        for &(g, cap) in &p.nmembers {
            self.ngroups.grow_members(g, cap).map_err(alloc_err)?;
        }
        self.lks.try_reserve(p.dir_lk).map_err(alloc_err)?;
        if !p.new_lks.is_empty() {
            let mut staged = try_vec(p.new_lks.len())?;
            for k in &p.new_lks {
                staged.push((try_clone_lk(k)?, try_clone_lk(k)?, k.name.len()));
            }
            self.lks.stage(staged);
        }
        self.ids.try_reserve_for(&p.id_target).map_err(alloc_err)?;
        self.slots.try_reserve(&p.slot_plan).map_err(alloc_err)?;
        let [dep, prec, node] = &p.idx;
        grow_exact(&mut self.idx.dep, dep.sheets)?;
        grow_exact(&mut self.idx.prec, prec.sheets)?;
        grow_exact(&mut self.idx.node, node.sheets)?;
        let total = dep.touched.len() + prec.touched.len() + node.touched.len();
        self.stage = try_vec(total)?;
        for (role, plan) in [(DEP, dep), (PREC, prec), (NODE, node)] {
            for (s, sh) in &plan.touched {
                self.stage_index(role, *s, sh)?;
            }
        }
        self.seal_stage();
        Ok(())
    }

    /// Pre-allocate one index's operation sequence into the scope's stage.
    pub(super) fn stage_index(
        &mut self,
        role: usize,
        sheet: u16,
        sh: &IndexShadow,
    ) -> Result<(), AuthorityError> {
        let v = match role {
            DEP => &mut self.idx.dep,
            PREC => &mut self.idx.prec,
            _ => &mut self.idx.node,
        };
        while v.len() <= sheet_slot(sheet) {
            debug_assert!(v.len() < v.capacity(), "unreserved index vector");
            v.push(LevelIndex::default());
        }
        let idx = &mut v[sheet_slot(sheet)];
        let before = idx.heap_bytes();
        let st = idx.try_stage(sh);
        idx.reset_peak();
        let grew = idx.heap_bytes() - before;
        self.idx_bytes[role] += grew;
        let st = st.map_err(alloc_err)?;
        tpush(&mut self.stage, (role as u8, sheet, st))
    }

    /// Transient observed in the staged indexes since the scope began
    /// (merge buffers and coexisting levels, the index peak model).
    fn staged_index_transient(&self) -> u64 {
        self.stage
            .iter()
            .map(|(r, s, _)| {
                let i = &self.index_ref(usize::from(*r))[sheet_slot(*s)];
                (i.peak() - i.heap_bytes()) as u64
            })
            .sum()
    }

    // ------------------------------------------------------------ apply

    /// Keep every location map as long as its arena's capacity (within the
    /// reserved capacity: no allocation).
    pub(super) fn sync_locs(&mut self) {
        let cap = self.recs.capacity();
        for v in [&mut self.dep_loc, &mut self.prec_loc] {
            if v.len() < cap {
                debug_assert!(v.capacity() >= cap, "unreserved location map");
                v.resize(cap, NONE);
            }
        }
        let cap = self.owners.capacity();
        if self.node_loc.len() < cap {
            debug_assert!(self.node_loc.capacity() >= cap, "unreserved location map");
            self.node_loc.resize(cap, NONE);
        }
    }

    pub(super) fn add_rec(&mut self, g: u32, dep: Rect) -> u32 {
        let id = if self.rec_free != DEAD {
            let id = self.rec_free;
            self.rec_free = self.recs[id as usize].pos;
            self.rec_nfree -= 1;
            id
        } else {
            debug_assert!(self.recs.len() < self.recs.capacity(), "unreserved record");
            self.recs.push(Rec {
                dep,
                group: g,
                pos: 0,
            });
            (self.recs.len() - 1) as u32
        };
        let grp = &mut self.egroups[g as usize];
        debug_assert!(
            grp.members.len() < grp.members.capacity(),
            "unreserved member list"
        );
        grp.members.push(id);
        grp.c += 1;
        self.recs[id as usize] = Rec {
            dep,
            group: g,
            pos: (grp.members.len() - 1) as u32,
        };
        let key = self.egroups.key(g);
        self.index_op(DEP, key.dep_sheet, IndexOp::Insert(dep.as_box(), id));
        let pb = key
            .proj
            .forward(&dep)
            .expect("members instantiate on the grid");
        self.index_op(PREC, key.proj.sheet, IndexOp::Insert(pb.as_box(), id));
        self.stats.records_created += 1;
        id
    }

    /// Remove a record; the caller settles the indexes.
    pub(super) fn remove_rec(&mut self, id: u32) {
        let r = self.recs[id as usize];
        debug_assert_ne!(r.group, DEAD);
        let grp = &mut self.egroups[r.group as usize];
        grp.members.swap_remove(r.pos as usize);
        if let Some(&moved) = grp.members.get(r.pos as usize) {
            self.recs[moved as usize].pos = r.pos;
        }
        let key = self.egroups.key(r.group);
        self.index_op(DEP, key.dep_sheet, IndexOp::Remove(id));
        self.index_op(PREC, key.proj.sheet, IndexOp::Remove(id));
        self.recs[id as usize] = Rec {
            dep: r.dep,
            group: DEAD,
            pos: self.rec_free,
        };
        self.rec_free = id;
        self.rec_nfree += 1;
    }

    pub(super) fn add_owner(
        &mut self,
        sheet: u16,
        g: u32,
        dom: Rect,
        template: AstNodeId,
        anchor: (u32, u32),
        flags: u16,
    ) -> u32 {
        let id = if self.own_free != DEAD {
            let id = self.own_free;
            self.own_free = self.owners[id as usize].pos;
            self.own_nfree -= 1;
            id
        } else {
            debug_assert!(
                self.owners.len() < self.owners.capacity(),
                "unreserved owner"
            );
            self.owners.push(Owner {
                dom,
                sheet,
                flags,
                group: g,
                pos: 0,
                template,
                anchor,
            });
            (self.owners.len() - 1) as u32
        };
        let mut pos = 0;
        if g != UNGROUPED {
            let grp = &mut self.ngroups[g as usize];
            debug_assert!(
                grp.members.len() < grp.members.capacity(),
                "unreserved member list"
            );
            grp.members.push(id);
            grp.c += 1;
            pos = (grp.members.len() - 1) as u32;
        }
        self.owners[id as usize] = Owner {
            dom,
            sheet,
            flags,
            group: g,
            pos,
            template,
            anchor,
        };
        while self.idx.node.len() <= sheet_slot(sheet) {
            debug_assert!(self.idx.node.len() < self.idx.node.capacity());
            self.idx.node.push(LevelIndex::default());
        }
        if !dom.is_cell() {
            self.nnodes += 1;
            self.index_op(NODE, sheet, IndexOp::Insert(dom.as_box(), id));
        }
        self.stats.owners_created += 1;
        id
    }

    pub(super) fn remove_owner(&mut self, id: u32) {
        let o = self.owners[id as usize];
        debug_assert_ne!(o.group, DEAD);
        if o.group != UNGROUPED {
            let grp = &mut self.ngroups[o.group as usize];
            grp.members.swap_remove(o.pos as usize);
            if let Some(&moved) = grp.members.get(o.pos as usize) {
                self.owners[moved as usize].pos = o.pos;
            }
        }
        if o.is_family() {
            self.nnodes -= 1;
            self.index_op(NODE, o.sheet, IndexOp::Remove(id));
        }
        self.owners[id as usize] = Owner {
            group: DEAD,
            pos: self.own_free,
            ..o
        };
        self.own_free = id;
        self.own_nfree += 1;
    }

    /// Settle every staged index (the dry run settles every touched one).
    pub(super) fn settle_staged(&mut self) {
        for i in 0..self.stage.len() {
            let (r, s, _) = self.stage[i];
            self.index_op(usize::from(r), s, IndexOp::Settle);
        }
    }

    /// Plan and dry-run a scope (read-only; every allocation fallible).
    #[allow(clippy::type_complexity)]
    fn plan_scope<'a>(
        &self,
        sheet: u16,
        q: &Rect,
        new: Option<(Cell, &'a FormulaFacts)>,
        revive: Option<Vid>,
    ) -> Result<(Cut, Option<NewFormula<'a>>, Prediction<'a>), AuthorityError> {
        let cut = self.plan_cut(sheet, q, new.map(|(c, _)| c))?;
        let newf = match new {
            Some((c, f)) => Some(self.plan_new(c, f, &cut, revive)?),
            None => None,
        };
        let pred = self.predict(sheet, &cut, newf.as_ref())?;
        Ok((cut, newf, pred))
    }

    /// One mutation scope: plan → dry run → admit → reserve → apply →
    /// repartition.
    fn mutate(
        &mut self,
        sheet: u16,
        q: Rect,
        new: Option<(Cell, &FormulaFacts)>,
        revive: Option<Vid>,
    ) -> Result<MutationReport, AuthorityError> {
        let before = self.counts();
        let (cut, newf, mut pred) = match self.plan_scope(sheet, &q, new, revive) {
            Ok(x) => x,
            Err(e) => {
                self.stats.rejected += 1;
                if e == AuthorityError::Alloc {
                    self.stats.alloc_failures += 1;
                }
                return Err(e);
            }
        };
        self.stats.plan_work += pred.work;
        self.stats.slot_work += pred.slot_plan.work;
        if let Err(e) = self.admit(pred.counts.bytes, pred.transient) {
            self.stats.rejected += 1;
            return Err(e);
        }
        if let Err(e) = self.reserve(&pred) {
            self.stats.rejected += 1;
            self.stats.alloc_failures += 1;
            return Err(e);
        }
        self.scope_peak = pred.counts.bytes + pred.transient;
        // ---- apply: no allocation from here to the end of the scope.
        self.sync_locs();
        self.stats.mutations += 1;
        let mut relabels = 0u64;

        // 1–3: removals, then one settle per touched index.
        for &r in &cut.recs {
            self.remove_rec(r);
        }
        for &o in &cut.owners {
            self.remove_owner(o);
        }
        self.settle_staged();
        // 4: identity cuts (the kept cell's owner is set below).
        let mut kept_run = None;
        for (i, c) in cut.id_cuts.iter().enumerate() {
            let keep = cut.keep == Some(i);
            let h = self.ids.apply_cut(c, keep, FAMILY);
            if keep {
                kept_run = h;
            }
        }
        // 5: directory keys and new groups.
        let mut ngroup_of: Option<u32> = None;
        if let Some(n) = &newf {
            for (g, k) in &n.edges {
                let g = match g {
                    Some(g) => *g,
                    None => {
                        let lk = match k.lk {
                            Ok(l) => l,
                            Err(key) => self.lks.intern_staged(key),
                        };
                        let key = EdgeKey {
                            dep_sheet: k.dep_sheet,
                            tag: k.tag,
                            lk,
                            proj: k.proj,
                        };
                        match self.egroups.get(&key) {
                            Some(g) => g,
                            None => {
                                let g = self.egroups.insert(key, Group::default());
                                debug_assert!(pred.eg.len() < pred.eg.capacity());
                                pred.eg.push(g);
                                g
                            }
                        }
                    }
                };
                pred.egroup_of.push(g);
            }
            ngroup_of = n.ngroup.as_ref().map(|g| match g {
                Ok(g) => *g,
                Err(key) => {
                    let g = self.ngroups.insert(*key, Group::default());
                    pred.ng.push(g);
                    g
                }
            });
        }
        // 6: records.
        for &(g, p) in &cut.rec_pieces {
            self.add_rec(g, p);
        }
        if let Some(n) = &newf {
            for i in 0..pred.egroup_of.len() {
                let g = pred.egroup_of[i];
                self.add_rec(g, Rect::cell(n.cell.1, n.cell.2));
            }
        }
        // 7: owners; 1×1 pieces of a family become singletons (run relabel).
        for &(g, p, template, anchor, flags) in &cut.owner_pieces {
            let o = self.add_owner(sheet, g, p, template, anchor, flags);
            if p.is_cell() {
                let (_, h) = self
                    .ids
                    .lookup((sheet, p.r0, p.c0))
                    .expect("owner piece cell");
                debug_assert_eq!(self.ids.run(h).len, 1, "a 1x1 owner piece has its own run");
                self.ids.set_owner(h, o);
                relabels += 1;
            }
        }
        if let Some(n) = &newf {
            let g = ngroup_of.unwrap_or(UNGROUPED);
            let o = self.add_owner(
                sheet,
                g,
                Rect::cell(n.cell.1, n.cell.2),
                n.template,
                n.anchor,
                n.flags,
            );
            match kept_run {
                Some(h) => {
                    self.ids.set_owner(h, o);
                    relabels += 1;
                }
                None => match n.revive {
                    Some(id) => {
                        self.ids.place_existing(sheet, n.cell.1, n.cell.2, 1, id, o);
                    }
                    None => {
                        self.ids.place(sheet, n.cell.1, n.cell.2, 1, o);
                    }
                },
            }
        }
        // 8: slot rows (ids known at the dry run).
        debug_assert!(
            newf.as_ref().is_none_or(|n| n.kept_id.is_some()
                || n.revive.is_some()
                || self.ids.id_of(n.cell) == pred.slot_rows.first().map(|r| r.0)),
            "dry-run id differs from the placed id"
        );
        self.slots
            .apply(&pred.slot_plan, &pred.slot_removes, &pred.slot_rows);
        // 9: reclaim emptied groups.
        for &g in &pred.ereclaim {
            self.egroups.reclaim(g);
        }
        for &g in &pred.nreclaim {
            self.ngroups.reclaim(g);
        }
        self.stats.groups_reclaimed += (pred.ereclaim.len() + pred.nreclaim.len()) as u64;

        self.stats.run_relabels += relabels;
        self.stats.max_run_relabels_one_op = self.stats.max_run_relabels_one_op.max(relabels);
        let observed_index_transient = self.staged_index_transient();
        // Frees the consumed stage (merge buffers).
        self.stage = Vec::new();
        let actual = self.counts();
        debug_assert_eq!(actual, pred.counts, "dry run != actual");

        // Repartition touched groups whose trigger fired (§5.7.2). The
        // planning scratch is freed first; the candidate lists survive.
        let predicted = pred.counts;
        let predicted_transient = pred.transient;
        let mut eg = std::mem::take(&mut pred.eg);
        let mut ng = std::mem::take(&mut pred.ng);
        drop(pred);
        drop(newf);
        drop(cut);
        eg.sort_unstable();
        eg.dedup();
        ng.sort_unstable();
        ng.dedup();
        self.scope_extra = (eg.capacity() + ng.capacity()) * 4;
        let mut reps = 0;
        for &g in &eg {
            if self.egroups[g as usize].triggered() {
                reps += u32::from(self.repartition_edges(g));
            }
        }
        for &g in &ng {
            if self.ngroups[g as usize].triggered() {
                reps += u32::from(self.repartition_nodes(g));
            }
        }
        // The candidate lists are done; the compaction scope runs with
        // nothing else of this scope alive.
        drop(eg);
        drop(ng);
        self.scope_extra = 0;
        self.maybe_compact_lks();
        Ok(MutationReport {
            before,
            predicted,
            actual,
            after: self.counts(),
            predicted_transient,
            observed_index_transient,
            run_relabels: relabels,
            repartitions: reps,
            predicted_peak_above_before: self.scope_peak.saturating_sub(before.bytes),
        })
    }

    /// Worst index transient observed since the last scope began: Σ over
    /// indexes of (peak − retained). A census over every index (reports).
    pub fn observed_index_transient(&self) -> u64 {
        self.idx
            .dep
            .iter()
            .chain(self.idx.prec.iter())
            .chain(self.idx.node.iter())
            .map(|i| (i.peak() - i.heap_bytes()) as u64)
            .sum()
    }
}
