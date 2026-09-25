//! Canonical repartition with byte-safe acceptance (§5.7.2 as amended by
//! final-review §5 item 2; addendum B-5).
//!
//! When a group's trigger fires (`L_g > 2·B_g + 2` or `C_g > 2·B_g + 2`):
//! 1. reserve the canon sweep's scratch against the scratch budget, or skip
//!    (counted, the group is suspended);
//! 2. compute `canon(X_g)`;
//! 3. dry-run the swap exactly: arena slots, member list, every index
//!    touched (tombstones, settle, flushes, peak);
//! 4. commit only if `|canon| < L_g`, the live model bytes do not grow
//!    (SP-2 F-3: counting pieces is not enough once singletons become a
//!    node), and the retained capacity after the swap and its transient
//!    peak are admitted like a mutation's (F-5: re-indexing can grow
//!    index capacity); otherwise keep the current pieces.
//!
//! Either way `B_g := |canon|` and `C_g := 0`, so a kept (unproductive)
//! repartition is still paid for by the `C_g > B_g + 2` creations that
//! triggered it: its work `O(L_g + |canon|) ≤ O(8·C_g)` (C2: `|canon| ≤
//! 3·L_g`; either trigger gives `L_g < 2·C_g`). Node repartitions also
//! rewrite run owners, but only at singleton ↔ family transitions: at most
//! one write per singleton piece before or after, so `≤ L_g + |canon|`
//! (runs of family members carry no owner, SP-2 F-2).

use super::mutate::{Bytes, try_vec};
use super::*;

impl Store {
    fn note_repartition(&mut self, l: usize, canon_len: usize, created: u32, relabels: u64) {
        let work = (l + canon_len) as u64 + relabels;
        self.stats.repartition_work += work;
        if created > 0 {
            let per = work as f64 / f64::from(created);
            if per > self.stats.max_work_per_creation {
                self.stats.max_work_per_creation = per;
            }
        }
    }

    /// A repartition skipped for lack of an allocation (planning or
    /// reservation): counted, the group suspended, nothing else changes.
    fn skip_alloc(&mut self, edge: bool, g: u32) -> bool {
        self.abort_stage();
        self.stats.alloc_failures += 1;
        self.stats.repartition_skipped += 1;
        let grp = if edge {
            &mut self.egroups[g as usize]
        } else {
            &mut self.ngroups[g as usize]
        };
        grp.set_suspended(true);
        grp.c = 0;
        false
    }

    /// Repartition edge group `g`. Returns whether new pieces committed.
    pub(super) fn repartition_edges(&mut self, g: u32) -> bool {
        self.stats.repartition_checks += 1;
        let l = self.egroups[g as usize].members.len();
        let scratch = canon::scratch_bound(l) + l * 4 + l * size_of::<Rect>();
        if self.budget.scratch.is_some_and(|lim| scratch as u64 > lim) {
            let grp = &mut self.egroups[g as usize];
            grp.set_suspended(true);
            grp.c = 0;
            self.stats.repartition_skipped += 1;
            return false;
        }
        match self.plan_edges(g, scratch) {
            Ok(Some(done)) => done,
            Ok(None) => self.skip_alloc(true, g),
            Err(_) => self.skip_alloc(true, g),
        }
    }

    /// Plan, admit, reserve and (if accepted) commit an edge repartition.
    /// `Ok(None)` or `Err` = an allocation failed before any change.
    fn plan_edges(&mut self, g: u32, canon_scratch: usize) -> Result<Option<bool>, AuthorityError> {
        let mut members: Vec<u32> = try_vec(self.egroups[g as usize].members.len())?;
        members.extend_from_slice(&self.egroups[g as usize].members);
        let l = members.len();
        let created = self.egroups[g as usize].c;
        let mut rects: Vec<Rect> = try_vec(l)?;
        rects.extend(members.iter().map(|&r| self.recs[r as usize].dep));
        let Ok(pieces) = canon::try_canon(&rects, &mut self.stats.canon_work) else {
            return Ok(None);
        };
        let key = self.egroups.key(g);
        let n = pieces.len();

        // Dry run of the swap.
        let mut b = Bytes::default();
        let mut work = (l + n) as u64;
        let (_, recs_cap) = slab_after(self.recs.len(), self.rec_nfree, self.recs.capacity(), l, n);
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
        {
            let m = &self.egroups[g as usize].members;
            b.grow(members_heap(m), members_heap_for(members_cap_after(m, n)));
        }
        let mut dep = IndexPlan::new(&self.idx.dep);
        let mut prec = IndexPlan::new(&self.idx.prec);
        for &r in &members {
            dep.get(&self.idx.dep, key.dep_sheet)?
                .remove(self.dep_loc[r as usize]);
            prec.get(&self.idx.prec, key.proj.sheet)?
                .remove(self.prec_loc[r as usize]);
        }
        dep.settle()?;
        prec.settle()?;
        for p in &pieces {
            dep.get(&self.idx.dep, key.dep_sheet)?
                .try_insert(p.is_cell())
                .map_err(|_| AuthorityError::Alloc)?;
            let pb = key
                .proj
                .forward(p)
                .expect("members instantiate on the grid");
            prec.get(&self.idx.prec, key.proj.sheet)?
                .try_insert(pb.is_cell())
                .map_err(|_| AuthorityError::Alloc)?;
        }
        dep.account(&self.idx.dep, &mut b);
        prec.account(&self.idx.prec, &mut b);
        work += (dep.touched.len() + prec.touched.len()) as u64 + dep.probes + prec.probes;
        self.stats.plan_work += work;
        let plan_scratch = canon_scratch
            + members.capacity() * 4
            + rects.capacity() * size_of::<Rect>()
            + pieces.capacity() * size_of::<Rect>()
            + dep.scratch_bytes()
            + prec.scratch_bytes()
            + self.scope_extra;
        let before = self.heap_bytes();
        let retained_after = b.after(before);
        let peak = b.peak(before, plan_scratch);
        self.scope_peak = self.scope_peak.max(peak);
        let transient = peak.saturating_sub(retained_after);
        let entry = size_of::<(super::super::geom::BoxT, u32)>();
        // Byte-safe acceptance: live model bytes must not grow, and the
        // capacity growth (index buffers, levels, arenas) plus the transient
        // peak must be admitted like a mutation's.
        let per = size_of::<Rec>() + 2 * entry;
        let live_grows = n * per > l * per;
        let admitted = self.admit(retained_after, transient).is_ok();
        {
            let grp = &mut self.egroups[g as usize];
            grp.set_b(n);
            grp.c = 0;
        }
        if !admitted || live_grows || n >= l {
            self.stats.repartitions_kept += 1;
            let grp = &mut self.egroups[g as usize];
            grp.set_kept(live_grows && n < l);
            grp.set_suspended(!admitted);
            if !admitted {
                self.stats.repartition_skipped += 1;
            }
            self.note_repartition(l, n, created, 0);
            return Ok(Some(false));
        }
        // Reserve (fallible, before any change).
        let reserved = (|| -> Result<(), AuthorityError> {
            grow_exact(&mut self.recs, recs_cap)?;
            grow_exact(&mut self.dep_loc, recs_cap)?;
            grow_exact(&mut self.prec_loc, recs_cap)?;
            self.stage = try_vec(dep.touched.len() + prec.touched.len())?;
            for (s, sh) in &dep.touched {
                self.stage_index(DEP, *s, sh)?;
            }
            for (s, sh) in &prec.touched {
                self.stage_index(PREC, *s, sh)?;
            }
            self.seal_stage();
            Ok(())
        })();
        if reserved.is_err() {
            // Undo the bookkeeping written above: the group is suspended
            // by the caller.
            return Ok(None);
        }
        // Commit: removes → settle → inserts (the shadow's order). No
        // allocation from here on.
        let live_before = self.live_model_bytes();
        self.sync_locs();
        for &r in &members {
            self.remove_rec(r);
        }
        self.settle_staged();
        for p in &pieces {
            self.add_rec(g, *p);
        }
        let observed = self.staged_transient_sum();
        self.stage = Vec::new();
        self.stats.records_created -= n as u64;
        let grp = &mut self.egroups[g as usize];
        grp.c = 0;
        grp.set_suspended(false);
        grp.set_kept(false);
        if self.live_model_bytes() > live_before {
            self.stats.repartition_live_up += 1;
        }
        self.stats.repartition_retained_growth += self.heap_bytes().saturating_sub(before);
        debug_assert_eq!(self.heap_bytes(), retained_after, "repartition dry run");
        debug_assert!(observed as usize <= transient as usize);
        self.stats.repartitions_committed += 1;
        self.note_repartition(l, n, created, 0);
        Ok(Some(true))
    }

    /// Repartition node group `g`. Returns whether new pieces committed.
    pub(super) fn repartition_nodes(&mut self, g: u32) -> bool {
        self.stats.repartition_checks += 1;
        let l = self.ngroups[g as usize].members.len();
        let scratch = canon::scratch_bound(l) + l * 4 + l * size_of::<Rect>();
        if self.budget.scratch.is_some_and(|lim| scratch as u64 > lim) {
            let grp = &mut self.ngroups[g as usize];
            grp.set_suspended(true);
            grp.c = 0;
            self.stats.repartition_skipped += 1;
            return false;
        }
        match self.plan_nodes(g, scratch) {
            Ok(Some(done)) => done,
            Ok(None) | Err(_) => self.skip_alloc(false, g),
        }
    }

    fn plan_nodes(&mut self, g: u32, canon_scratch: usize) -> Result<Option<bool>, AuthorityError> {
        let mut members: Vec<u32> = try_vec(self.ngroups[g as usize].members.len())?;
        members.extend_from_slice(&self.ngroups[g as usize].members);
        let l = members.len();
        let created = self.ngroups[g as usize].c;
        let (sheet, _) = self.ngroups.key(g);
        let mut rects: Vec<Rect> = try_vec(l)?;
        rects.extend(members.iter().map(|&o| self.owners[o as usize].dom));
        let Ok(pieces) = canon::try_canon(&rects, &mut self.stats.canon_work) else {
            return Ok(None);
        };
        let n = pieces.len();

        // Dry run.
        let mut b = Bytes::default();
        let (_, owners_cap) = slab_after(
            self.owners.len(),
            self.own_nfree,
            self.owners.capacity(),
            l,
            n,
        );
        b.grow(
            vec_bytes::<Owner>(self.owners.capacity()),
            vec_bytes::<Owner>(owners_cap),
        );
        b.grow(
            vec_bytes::<u32>(self.node_loc.capacity()),
            vec_bytes::<u32>(self.node_loc.capacity().max(owners_cap)),
        );
        {
            let m = &self.ngroups[g as usize].members;
            b.grow(members_heap(m), members_heap_for(members_cap_after(m, n)));
        }
        let mut node = IndexPlan::new(&self.idx.node);
        node.get(&self.idx.node, sheet)?;
        for &o in &members {
            if self.owners[o as usize].is_family() {
                node.get(&self.idx.node, sheet)?
                    .remove(self.node_loc[o as usize]);
            }
        }
        node.settle()?;
        for p in &pieces {
            if !p.is_cell() {
                node.get(&self.idx.node, sheet)?
                    .try_insert(false)
                    .map_err(|_| AuthorityError::Alloc)?;
            }
        }
        node.account(&self.idx.node, &mut b);
        self.stats.plan_work += (l + n + node.touched.len()) as u64 + node.probes;
        let entry = size_of::<(super::super::geom::BoxT, u32)>();
        let fam_before = members
            .iter()
            .filter(|&&o| self.owners[o as usize].is_family())
            .count();
        let fam_after = pieces.iter().filter(|p| !p.is_cell()).count();
        let live_before_model = l * size_of::<Owner>() + fam_before * entry;
        let live_after_model = n * size_of::<Owner>() + fam_after * entry;
        let live_grows = live_after_model > live_before_model;
        // Sources for each new piece (the old owner of its anchor cell) and
        // the former singleton cells: planning scratch, reserved now.
        let mut srcs: Vec<(AstNodeId, (u32, u32), u16)> = try_vec(n)?;
        let mut old_singles: Vec<Cell> = try_vec(l)?;
        let plan_scratch = canon_scratch
            + members.capacity() * 4
            + rects.capacity() * size_of::<Rect>()
            + pieces.capacity() * size_of::<Rect>()
            + node.scratch_bytes()
            + srcs.capacity() * size_of::<(AstNodeId, (u32, u32), u16)>()
            + old_singles.capacity() * size_of::<Cell>()
            + self.scope_extra;
        let before = self.heap_bytes();
        let retained_after = b.after(before);
        let peak = b.peak(before, plan_scratch);
        self.scope_peak = self.scope_peak.max(peak);
        let transient = peak.saturating_sub(retained_after);
        let admitted = self.admit(retained_after, transient).is_ok();
        {
            let grp = &mut self.ngroups[g as usize];
            grp.set_b(n);
            grp.c = 0;
        }
        if !admitted || live_grows || n >= l {
            self.stats.repartitions_kept += 1;
            let grp = &mut self.ngroups[g as usize];
            grp.set_kept(live_grows && n < l);
            grp.set_suspended(!admitted);
            if !admitted {
                self.stats.repartition_skipped += 1;
            }
            self.note_repartition(l, n, created, 0);
            return Ok(Some(false));
        }
        for p in &pieces {
            let o = self
                .owner_at((sheet, p.r0, p.c0))
                .expect("piece cell has an owner");
            let w = &self.owners[o as usize];
            srcs.push((w.template, w.anchor, w.flags));
        }
        old_singles.extend(
            members
                .iter()
                .filter(|&&o| !self.owners[o as usize].is_family())
                .map(|&o| {
                    let d = self.owners[o as usize].dom;
                    (sheet, d.r0, d.c0)
                }),
        );
        let reserved = (|| -> Result<(), AuthorityError> {
            grow_exact(&mut self.owners, owners_cap)?;
            grow_exact(&mut self.node_loc, owners_cap)?;
            self.stage = try_vec(node.touched.len())?;
            for (s, sh) in &node.touched {
                self.stage_index(NODE, *s, sh)?;
            }
            self.seal_stage();
            Ok(())
        })();
        if reserved.is_err() {
            return Ok(None);
        }

        // Commit (no allocation).
        let live_before = self.live_model_bytes();
        self.sync_locs();
        for &o in &members {
            self.remove_owner(o);
        }
        self.settle_staged();
        let mut relabels = 0u64;
        for (p, &(template, anchor, flags)) in pieces.iter().zip(&srcs) {
            let o = self.add_owner(sheet, g, *p, template, anchor, flags);
            if p.is_cell() {
                let (_, h) = self
                    .ids
                    .lookup((sheet, p.r0, p.c0))
                    .expect("owner piece cell");
                if self.ids.run(h).owner != o {
                    self.ids.set_owner(h, o);
                    relabels += 1;
                }
            }
        }
        self.stats.owners_created -= n as u64;
        // Former singletons now inside a family: FAMILY runs, then coalesce
        // with id-contiguous neighbours of the same node.
        for &cell in &old_singles {
            let (_, h) = self.ids.lookup(cell).expect("formula cell");
            let o = self.owner_at_fresh(cell);
            if self.owners[o as usize].is_family() {
                self.ids.set_owner(h, FAMILY);
                relabels += 1;
                let node_idx = &self.idx.node;
                self.ids.coalesce_at(cell, &|a, b| {
                    let at = |row: u32, col: u32| {
                        let mut f = None;
                        node_idx[sheet_slot(a.sheet)]
                            .query(&[row, col, row, col], &mut |x| f = Some(x));
                        f
                    };
                    a.owner == FAMILY
                        && b.owner == FAMILY
                        && at(a.row_start + a.len - 1, a.col) == at(b.row_start, b.col)
                });
            }
        }
        let observed = self.staged_transient_sum();
        self.stage = Vec::new();
        let grp = &mut self.ngroups[g as usize];
        grp.c = 0;
        grp.set_suspended(false);
        grp.set_kept(false);
        if self.live_model_bytes() > live_before {
            self.stats.repartition_live_up += 1;
        }
        self.stats.repartition_retained_growth += self.heap_bytes().saturating_sub(before);
        debug_assert_eq!(self.heap_bytes(), retained_after, "repartition dry run");
        debug_assert!(observed as usize <= transient as usize);
        self.stats.run_relabels += relabels;
        self.stats.repartitions_committed += 1;
        self.note_repartition(l, n, created, relabels);
        Ok(Some(true))
    }

    /// Σ (peak − retained) over the staged indexes.
    fn staged_transient_sum(&self) -> u64 {
        self.stage
            .iter()
            .map(|(r, s, _)| {
                let i = &self.index_ref(usize::from(*r))[sheet_slot(*s)];
                (i.peak() - i.heap_bytes()) as u64
            })
            .sum()
    }

    /// Owner lookup that ignores a stale run owner (used mid-repartition,
    /// when a former singleton's run still names its dead owner).
    fn owner_at_fresh(&self, cell: Cell) -> u32 {
        let mut found = None;
        if let Some(idx) = self.idx.node.get(sheet_slot(cell.0)) {
            idx.query(&[cell.1, cell.2, cell.1, cell.2], &mut |o| found = Some(o));
        }
        if let Some(o) = found {
            return o;
        }
        let (_, h) = self.ids.lookup(cell).expect("formula cell");
        self.ids.run(h).owner
    }
}
