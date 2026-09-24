//! Non-structural mutations as admitted mutation scopes (§5, §5.1, §5.6).

use super::*;

/// Byte accounting of one plan: components replaced (old → new bytes) and
/// transient bytes above the retained size.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Bytes {
    pub old: usize,
    pub new: usize,
    pub transient: usize,
}

impl Bytes {
    /// A component going from `old` to `new` retained bytes. Growth
    /// reallocates, so the old buffer coexists with the new one.
    pub fn component(&mut self, old: usize, new: usize) {
        self.old += old;
        self.new += new;
        if new > old {
            self.transient += old;
        }
    }

    pub fn after(&self, before: u64) -> u64 {
        before - self.old as u64 + self.new as u64
    }
}

/// Everything a mutation's dry run predicts.
#[derive(Debug)]
pub(super) struct Prediction {
    pub counts: Counts,
    pub transient: u64,
    recs_slots: usize,
    recs_cap: usize,
    owners_slots: usize,
    owners_cap: usize,
    new_egroups: usize,
    new_ngroups: usize,
    /// `(group, member-list capacity after)` for touched existing groups.
    emembers: Vec<(u32, usize)>,
    nmembers: Vec<(u32, usize)>,
    dir_lk: DirPlan,
    id_target: IdShadow,
    slot_plan: super::super::slots::SlotPlan,
    slot_removes: Vec<Vid>,
    slot_inserts: Vec<(Vid, usize)>,
    idx_sheets: (usize, usize, usize),
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
        self.mutate(cell.0, Rect::cell(cell.1, cell.2), Some((cell, facts)))
    }

    /// Clear the formula at `cell` (formula → value/empty). A cell without a
    /// formula is a no-op scope.
    pub fn clear_cell(&mut self, cell: Cell) -> Result<MutationReport, AuthorityError> {
        self.mutate(cell.0, Rect::cell(cell.1, cell.2), None)
    }

    /// Clear every formula in `q` on `sheet` (range clear).
    pub fn clear_rect(&mut self, sheet: u16, q: Rect) -> Result<MutationReport, AuthorityError> {
        self.mutate(sheet, q, None)
    }

    // ------------------------------------------------------------ plan

    fn plan_cut(&self, sheet: u16, q: &Rect, keep: Option<Cell>) -> Cut {
        let mut cut = Cut::default();
        if let Some(idx) = self.idx.dep.get(sheet as usize) {
            idx.query(&q.as_box(), &mut |id| cut.recs.push(id));
        }
        cut.recs.sort_unstable();
        for &id in &cut.recs {
            let r = &self.recs[id as usize];
            for p in r.dep.subtract(q).as_slice() {
                cut.rec_pieces.push((r.group, *p));
            }
        }
        cut.id_cuts = self.ids.plan_cuts_rect(sheet, q.r0, q.c0, q.r1, q.c1);
        if let Some(k) = keep {
            cut.keep = cut.id_cuts.iter().position(|c| {
                c.is_cell() && c.run_value.row_start + c.lo == k.1 && c.run_value.col == k.2
            });
        }
        // Owners: families through the node index, singletons through runs.
        if let Some(idx) = self.idx.node.get(sheet as usize) {
            idx.query(&q.as_box(), &mut |o| cut.owners.push(o));
        }
        for c in &cut.id_cuts {
            if c.run_value.owner != FAMILY {
                cut.owners.push(c.run_value.owner);
            }
        }
        cut.owners.sort_unstable();
        cut.owners.dedup();
        for &o in &cut.owners {
            let w = &self.owners[o as usize];
            if w.is_family() {
                for p in w.dom.subtract(q).as_slice() {
                    cut.owner_pieces
                        .push((w.group, *p, w.template, w.anchor, w.flags));
                }
            }
        }
        cut
    }

    fn plan_new(&self, cell: Cell, f: &FormulaFacts, cut: &Cut) -> NewFormula {
        let mut edges = Vec::with_capacity(f.edges.len());
        for e in &f.edges {
            let lk = match &e.origin {
                OriginSpec::Text => Ok(NO_LK),
                OriginSpec::Symbol(k) => self.lks.get(k).ok_or_else(|| k.clone()),
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
        NewFormula {
            cell,
            edges,
            ngroup,
            template: f.template,
            literals: f.literals.clone(),
            flags: f.flags,
            kept_id: cut.keep.map(|i| cut.id_cuts[i].id()),
        }
    }

    // ------------------------------------------------------------ dry run

    /// Exact prediction of every container after `cut` + `newf` (§5.1).
    fn predict(
        &self,
        sheet: u16,
        cut: &Cut,
        newf: Option<&NewFormula>,
    ) -> Result<Prediction, AuthorityError> {
        let mut b = Bytes::default();
        let before = self.counts();

        // New directory keys and groups (distinct).
        let mut new_lks: Vec<&LkKey> = Vec::new();
        let mut new_ekeys: Vec<(u16, Tag, Result<u32, &LkKey>, RefProj)> = Vec::new();
        let mut new_ngroup = false;
        let mut eadds: FxHashMap<u32, usize> = FxHashMap::default();
        let mut new_group_sizes: Vec<usize> = Vec::new();
        if let Some(n) = newf {
            for (g, k) in &n.edges {
                match g {
                    Some(g) => *eadds.entry(*g).or_default() += 1,
                    None => {
                        let lk = match &k.lk {
                            Ok(l) => Ok(*l),
                            Err(key) => {
                                if !new_lks.contains(&key) {
                                    new_lks.push(key);
                                }
                                Err(key)
                            }
                        };
                        let nk = (k.dep_sheet, k.tag, lk, k.proj);
                        // Extraction deduplicates edges, so each new key is new once.
                        debug_assert!(!new_ekeys.contains(&nk));
                        new_ekeys.push(nk);
                        new_group_sizes.push(1);
                    }
                }
            }
            new_ngroup = matches!(n.ngroup, Some(Err(_)));
        }
        let dir_lk = DirPlan {
            new_keys: new_lks.len(),
            new_key_heap: new_lks.iter().map(|k| k.name.len()).sum(),
        };
        b.component(self.lks.heap_bytes(), self.lks.planned_bytes(dir_lk));
        // Group tables: entry vectors and hash indexes (new groups hold one
        // member inline, so they allocate no member list).
        let (o, n) = self.egroups.planned_fixed_bytes(new_ekeys.len());
        b.component(o, n);
        let (o, n) = self.ngroups.planned_fixed_bytes(usize::from(new_ngroup));
        b.component(o, n);

        // Records arena and location maps.
        let new_recs = newf.map_or(0, |n| n.edges.len());
        let (recs_slots, recs_cap) = slab_after(
            self.recs.len(),
            self.rec_nfree,
            self.recs.capacity(),
            cut.recs.len(),
            cut.rec_pieces.len() + new_recs,
        );
        b.component(
            vec_bytes::<Rec>(self.recs.capacity()),
            vec_bytes::<Rec>(recs_cap),
        );
        for loc in [&self.dep_loc, &self.prec_loc] {
            b.component(
                vec_bytes::<u32>(loc.capacity()),
                vec_bytes::<u32>(loc.capacity().max(recs_cap)),
            );
        }
        // Edge groups: member lists of touched groups, new groups.
        let mut erm: FxHashMap<u32, usize> = FxHashMap::default();
        for &r in &cut.recs {
            *erm.entry(self.recs[r as usize].group).or_default() += 1;
        }
        for &(g, _) in &cut.rec_pieces {
            *eadds.entry(g).or_default() += 1;
        }
        let mut emembers: Vec<(u32, usize)> = Vec::new();
        let mut touched: Vec<u32> = erm.keys().chain(eadds.keys()).copied().collect();
        touched.sort_unstable();
        touched.dedup();
        for g in touched {
            let m = &self.egroups[g as usize].members;
            let fin =
                m.len() - erm.get(&g).copied().unwrap_or(0) + eadds.get(&g).copied().unwrap_or(0);
            let cap = members_cap_after(m, fin);
            b.component(members_heap(m), members_heap_for(cap));
            emembers.push((g, cap));
        }
        let _ = &new_group_sizes;

        // Owners arena, node location map, node groups.
        let new_owner = usize::from(newf.is_some());
        let (owners_slots, owners_cap) = slab_after(
            self.owners.len(),
            self.own_nfree,
            self.owners.capacity(),
            cut.owners.len(),
            cut.owner_pieces.len() + new_owner,
        );
        b.component(
            vec_bytes::<Owner>(self.owners.capacity()),
            vec_bytes::<Owner>(owners_cap),
        );
        b.component(
            vec_bytes::<u32>(self.node_loc.capacity()),
            vec_bytes::<u32>(self.node_loc.capacity().max(owners_cap)),
        );
        let mut nrm: FxHashMap<u32, usize> = FxHashMap::default();
        let mut nadd: FxHashMap<u32, usize> = FxHashMap::default();
        for &o in &cut.owners {
            let g = self.owners[o as usize].group;
            if g != UNGROUPED {
                *nrm.entry(g).or_default() += 1;
            }
        }
        for &(g, ..) in &cut.owner_pieces {
            if g != UNGROUPED {
                *nadd.entry(g).or_default() += 1;
            }
        }
        if let Some(Some(Ok(g))) = newf.map(|n| &n.ngroup) {
            *nadd.entry(*g).or_default() += 1;
        }
        let mut nmembers: Vec<(u32, usize)> = Vec::new();
        let mut ntouched: Vec<u32> = nrm.keys().chain(nadd.keys()).copied().collect();
        ntouched.sort_unstable();
        ntouched.dedup();
        for g in ntouched {
            let m = &self.ngroups[g as usize].members;
            let fin =
                m.len() - nrm.get(&g).copied().unwrap_or(0) + nadd.get(&g).copied().unwrap_or(0);
            let cap = members_cap_after(m, fin);
            b.component(members_heap(m), members_heap_for(cap));
            nmembers.push((g, cap));
        }

        // Indexes: removes → settle → inserts, per role.
        let mut dep = IndexPlan::new(&self.idx.dep);
        let mut prec = IndexPlan::new(&self.idx.prec);
        let mut node = IndexPlan::new(&self.idx.node);
        for &r in &cut.recs {
            let rec = &self.recs[r as usize];
            let key = self.egroups.key(rec.group);
            dep.get(&self.idx.dep, key.dep_sheet)
                .remove(self.dep_loc[r as usize]);
            prec.get(&self.idx.prec, key.proj.sheet)
                .remove(self.prec_loc[r as usize]);
        }
        for &o in &cut.owners {
            let w = &self.owners[o as usize];
            if w.is_family() {
                node.get(&self.idx.node, w.sheet)
                    .remove(self.node_loc[o as usize]);
            }
        }
        dep.settle();
        prec.settle();
        node.settle();
        for &(g, p) in &cut.rec_pieces {
            let key = self.egroups.key(g);
            dep.get(&self.idx.dep, key.dep_sheet).insert(p.is_cell());
            let pb = key
                .proj
                .forward(&p)
                .expect("members instantiate on the grid");
            prec.get(&self.idx.prec, key.proj.sheet)
                .insert(pb.is_cell());
        }
        if let Some(n) = newf {
            let x = Rect::cell(n.cell.1, n.cell.2);
            for (_, k) in &n.edges {
                dep.get(&self.idx.dep, k.dep_sheet).insert(true);
                let pb = k.proj.forward(&x).expect("new formula instantiates");
                prec.get(&self.idx.prec, k.proj.sheet).insert(pb.is_cell());
            }
        }
        for &(_, p, ..) in &cut.owner_pieces {
            if !p.is_cell() {
                node.get(&self.idx.node, sheet).insert(false);
            }
        }
        // Owner pieces may leave the sheet's node index untouched otherwise;
        // a new formula needs index vectors for its sheet.
        if newf.is_some() {
            dep.get(&self.idx.dep, sheet);
            node.get(&self.idx.node, sheet);
        }
        for (plan, v) in [
            (&dep, &self.idx.dep),
            (&prec, &self.idx.prec),
            (&node, &self.idx.node),
        ] {
            let (ret, tr) = plan.bytes(v);
            let new = ret + vec_bytes::<LevelIndex>(v.capacity().max(plan.sheets));
            b.component(Self::idx_bytes(v), new);
            b.transient += tr;
        }

        // Identity.
        let mut id_target = self.ids.shadow();
        for (i, c) in cut.id_cuts.iter().enumerate() {
            IdentityTable::shadow_cut(&mut id_target, c, cut.keep == Some(i));
        }
        let mut new_id = None;
        if let Some(n) = newf
            && n.kept_id.is_none()
        {
            self.ids.check_alloc(1).map_err(AuthorityError::Identity)?;
            new_id = Some(self.ids.next_id());
            IdentityTable::shadow_place(&mut id_target, n.cell.0, 1, 1);
        }
        b.component(self.ids.heap_bytes(), id_target.heap_bytes());

        // Slots: retired ids and the cell's replaced row.
        let mut slot_removes: Vec<Vid> = Vec::new();
        for c in &cut.id_cuts {
            slot_removes.extend(c.ids());
        }
        let mut slot_inserts: Vec<(Vid, usize)> = Vec::new();
        if let Some(n) = newf {
            let id = n.kept_id.or(new_id).expect("an id for the new formula");
            slot_inserts.push((id, n.literals.len()));
        }
        let slot_plan = self.slots.plan(&slot_removes, &slot_inserts);
        b.component(
            self.slots.heap_bytes(),
            self.slots.planned_bytes(&slot_plan),
        );

        // Plan scratch (the cut's own vectors) is transient.
        b.transient += cut.recs.capacity() * 4
            + cut.rec_pieces.capacity() * size_of::<(u32, Rect)>()
            + cut.owners.capacity() * 4
            + cut.owner_pieces.capacity() * size_of::<OwnerPiece>()
            + cut.id_cuts.capacity() * size_of::<CellCut>();

        let removed_nodes = cut
            .owners
            .iter()
            .filter(|&&o| self.owners[o as usize].is_family())
            .count() as u64;
        let added_nodes = cut.owner_pieces.iter().filter(|p| !p.1.is_cell()).count() as u64;
        let mut slot_rows = before.slot_rows;
        for &id in &slot_removes {
            if self.slots.get(id).is_some() {
                slot_rows -= 1;
            }
        }
        slot_rows += slot_inserts.iter().filter(|(_, n)| *n > 0).count() as u64;
        let counts = Counts {
            records: before.records - cut.recs.len() as u64
                + (cut.rec_pieces.len() + new_recs) as u64,
            owners: before.owners - cut.owners.len() as u64
                + (cut.owner_pieces.len() + new_owner) as u64,
            nodes: before.nodes - removed_nodes + added_nodes,
            runs: id_target.live_runs as u64,
            next_id: id_target.next_id,
            dep_entries: dep.entries(&self.idx.dep),
            prec_entries: prec.entries(&self.idx.prec),
            node_entries: node.entries(&self.idx.node),
            slot_rows,
            bytes: b.after(before.bytes),
        };
        Ok(Prediction {
            counts,
            transient: b.transient as u64,
            recs_slots,
            recs_cap,
            owners_slots,
            owners_cap,
            new_egroups: new_ekeys.len(),
            new_ngroups: usize::from(new_ngroup),
            emembers,
            nmembers,
            dir_lk,
            id_target,
            slot_plan,
            slot_removes,
            slot_inserts,
            idx_sheets: (dep.sheets, prec.sheets, node.sheets),
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

    /// Grow every container to the prediction (fallible, before any change).
    fn reserve(&mut self, p: &Prediction) -> Result<(), AuthorityError> {
        grow_exact(&mut self.recs, p.recs_cap)?;
        grow_exact(&mut self.owners, p.owners_cap)?;
        grow_exact(&mut self.dep_loc, p.recs_cap)?;
        grow_exact(&mut self.prec_loc, p.recs_cap)?;
        grow_exact(&mut self.node_loc, p.owners_cap)?;
        self.egroups
            .try_reserve(p.new_egroups)
            .map_err(|_| AuthorityError::Alloc)?;
        self.ngroups
            .try_reserve(p.new_ngroups)
            .map_err(|_| AuthorityError::Alloc)?;
        for &(g, cap) in &p.emembers {
            grow_members(&mut self.egroups[g as usize].members, cap)?;
        }
        for &(g, cap) in &p.nmembers {
            grow_members(&mut self.ngroups[g as usize].members, cap)?;
        }
        self.lks
            .try_reserve(p.dir_lk)
            .map_err(|_| AuthorityError::Alloc)?;
        self.ids
            .try_reserve_for(&p.id_target)
            .map_err(|_| AuthorityError::Alloc)?;
        self.slots
            .try_reserve(&p.slot_plan)
            .map_err(|_| AuthorityError::Alloc)?;
        let (d, pr, n) = p.idx_sheets;
        grow_exact(&mut self.idx.dep, d)?;
        grow_exact(&mut self.idx.prec, pr)?;
        grow_exact(&mut self.idx.node, n)?;
        let _ = (p.recs_slots, p.owners_slots);
        Ok(())
    }

    // ------------------------------------------------------------ apply

    /// Keep every location map as long as its arena's capacity.
    pub(super) fn sync_locs(&mut self) {
        let cap = self.recs.capacity();
        for v in [&mut self.dep_loc, &mut self.prec_loc] {
            if v.len() < cap {
                v.reserve_exact(cap - v.len());
                v.resize(cap, NONE);
            }
        }
        let cap = self.owners.capacity();
        if self.node_loc.len() < cap {
            self.node_loc.reserve_exact(cap - self.node_loc.len());
            self.node_loc.resize(cap, NONE);
        }
    }

    fn ensure_sheet(v: &mut Vec<LevelIndex>, sheet: u16) {
        while v.len() <= sheet as usize {
            v.push(LevelIndex::default());
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
        Self::ensure_sheet(&mut self.idx.dep, key.dep_sheet);
        Self::ensure_sheet(&mut self.idx.prec, key.proj.sheet);
        self.idx.dep[key.dep_sheet as usize].insert(dep.as_box(), id, &mut self.dep_loc);
        let pb = key
            .proj
            .forward(&dep)
            .expect("members instantiate on the grid");
        self.idx.prec[key.proj.sheet as usize].insert(pb.as_box(), id, &mut self.prec_loc);
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
        self.idx.dep[key.dep_sheet as usize].remove(id, &mut self.dep_loc);
        self.idx.prec[key.proj.sheet as usize].remove(id, &mut self.prec_loc);
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
        Self::ensure_sheet(&mut self.idx.node, sheet);
        if !dom.is_cell() {
            self.nnodes += 1;
            self.idx.node[sheet as usize].insert(dom.as_box(), id, &mut self.node_loc);
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
            self.idx.node[o.sheet as usize].remove(id, &mut self.node_loc);
        }
        self.owners[id as usize] = Owner {
            group: DEAD,
            pos: self.own_free,
            ..o
        };
        self.own_free = id;
        self.own_nfree += 1;
    }

    pub(super) fn settle_all(&mut self) {
        for i in &mut self.idx.dep {
            i.settle(&mut self.dep_loc);
        }
        for i in &mut self.idx.prec {
            i.settle(&mut self.prec_loc);
        }
        for i in &mut self.idx.node {
            i.settle(&mut self.node_loc);
        }
    }

    pub(super) fn reset_peaks(&mut self) {
        for i in self
            .idx
            .dep
            .iter_mut()
            .chain(self.idx.prec.iter_mut())
            .chain(self.idx.node.iter_mut())
        {
            i.reset_peak();
        }
    }

    /// One mutation scope: plan → dry run → admit → reserve → apply →
    /// repartition.
    fn mutate(
        &mut self,
        sheet: u16,
        q: Rect,
        new: Option<(Cell, &FormulaFacts)>,
    ) -> Result<MutationReport, AuthorityError> {
        let before = self.counts();
        let cut = self.plan_cut(sheet, &q, new.map(|(c, _)| c));
        let newf = new.map(|(c, f)| self.plan_new(c, f, &cut));
        let pred = match self.predict(sheet, &cut, newf.as_ref()) {
            Ok(p) => p,
            Err(e) => {
                self.stats.rejected += 1;
                return Err(e);
            }
        };
        if let Err(e) = self.admit(pred.counts.bytes, pred.transient) {
            self.stats.rejected += 1;
            return Err(e);
        }
        self.reserve(&pred)?;
        self.sync_locs();
        self.reset_peaks();
        self.stats.mutations += 1;
        let mut relabels = 0u64;

        // 1–3: removals, then one settle per index.
        for &r in &cut.recs {
            self.remove_rec(r);
        }
        for &o in &cut.owners {
            self.remove_owner(o);
        }
        self.settle_all();
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
        let mut egroup_of: Vec<u32> = Vec::new();
        let mut ngroup_of: Option<u32> = None;
        if let Some(n) = &newf {
            for (g, k) in &n.edges {
                let g = match g {
                    Some(g) => *g,
                    None => {
                        let lk = match &k.lk {
                            Ok(l) => *l,
                            Err(key) => self.lks.intern(key.clone(), key.name.len()),
                        };
                        let key = EdgeKey {
                            dep_sheet: k.dep_sheet,
                            tag: k.tag,
                            lk,
                            proj: k.proj,
                        };
                        match self.egroups.get(&key) {
                            Some(g) => g,
                            None => self.egroups.insert(key, Group::default()),
                        }
                    }
                };
                egroup_of.push(g);
            }
            ngroup_of = n.ngroup.as_ref().map(|g| match g {
                Ok(g) => *g,
                Err(key) => self.ngroups.insert(*key, Group::default()),
            });
        }
        // 6: records.
        for &(g, p) in &cut.rec_pieces {
            self.add_rec(g, p);
        }
        if let Some(n) = &newf {
            for &g in &egroup_of {
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
                (n.cell.1, n.cell.2),
                n.flags,
            );
            match kept_run {
                Some(h) => {
                    self.ids.set_owner(h, o);
                    relabels += 1;
                }
                None => {
                    self.ids.place(sheet, n.cell.1, n.cell.2, 1, o);
                }
            }
        }
        // 8: slot rows.
        let rows: Vec<(Vid, &[ValueRef])> = match &newf {
            Some(n) => vec![(
                n.kept_id
                    .unwrap_or_else(|| self.ids.id_of(n.cell).expect("placed")),
                &n.literals[..],
            )],
            None => Vec::new(),
        };
        let rows: Vec<(Vid, Vec<ValueRef>)> =
            rows.into_iter().map(|(i, r)| (i, r.to_vec())).collect();
        let rows_ref: Vec<(Vid, &[ValueRef])> = rows.iter().map(|(i, r)| (*i, &r[..])).collect();
        self.slots
            .apply(&pred.slot_plan, &pred.slot_removes, &rows_ref);
        debug_assert_eq!(
            pred.slot_inserts.iter().map(|x| x.0).collect::<Vec<_>>(),
            rows_ref.iter().map(|x| x.0).collect::<Vec<_>>()
        );

        self.stats.run_relabels += relabels;
        self.stats.max_run_relabels_one_op = self.stats.max_run_relabels_one_op.max(relabels);
        let actual = self.counts();
        debug_assert_eq!(actual, pred.counts, "dry run != actual");
        let observed_index_transient = self.observed_index_transient();

        // Repartition touched groups whose trigger fired (§5.7.2).
        let mut eg: Vec<u32> = cut.rec_pieces.iter().map(|p| p.0).collect();
        eg.extend(egroup_of);
        eg.extend(pred.emembers.iter().map(|m| m.0));
        eg.sort_unstable();
        eg.dedup();
        let mut ng: Vec<u32> = pred.nmembers.iter().map(|m| m.0).collect();
        ng.extend(ngroup_of);
        ng.sort_unstable();
        ng.dedup();
        let mut reps = 0;
        for g in eg {
            if self.egroups[g as usize].triggered() {
                reps += u32::from(self.repartition_edges(g));
            }
        }
        for g in ng {
            if self.ngroups[g as usize].triggered() {
                reps += u32::from(self.repartition_nodes(g));
            }
        }
        Ok(MutationReport {
            before,
            predicted: pred.counts,
            actual,
            after: self.counts(),
            predicted_transient: pred.transient,
            observed_index_transient,
            run_relabels: relabels,
            repartitions: reps,
        })
    }

    /// Worst index transient observed since the last mutation began:
    /// Σ over indexes of (peak − retained).
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
