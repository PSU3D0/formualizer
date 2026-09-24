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

use super::mutate::Bytes;
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

    /// Repartition edge group `g`. Returns whether new pieces committed.
    pub(super) fn repartition_edges(&mut self, g: u32) -> bool {
        self.stats.repartition_checks += 1;
        let members = self.egroups[g as usize].members.clone();
        let l = members.len();
        let created = self.egroups[g as usize].c;
        let scratch = canon::scratch_bound(l) + members.capacity() * 4 + l * size_of::<Rect>();
        if self.budget.scratch.is_some_and(|lim| scratch as u64 > lim) {
            let grp = &mut self.egroups[g as usize];
            grp.set_suspended(true);
            grp.c = 0;
            self.stats.repartition_skipped += 1;
            return false;
        }
        let rects: Vec<Rect> = members.iter().map(|&r| self.recs[r as usize].dep).collect();
        let pieces = canon::canon(&rects, &mut self.stats.canon_work);
        let key = self.egroups.key(g);

        // Dry run of the swap.
        let mut b = Bytes::default();
        let (_, recs_cap) = slab_after(
            self.recs.len(),
            self.rec_nfree,
            self.recs.capacity(),
            l,
            pieces.len(),
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
        {
            let m = &self.egroups[g as usize].members;
            b.component(
                members_heap(m),
                members_heap_for(members_cap_after(m, pieces.len())),
            );
        }
        let mut dsh = self.idx.dep[key.dep_sheet as usize].shadow();
        let mut psh = self.idx.prec[key.proj.sheet as usize].shadow();
        for &r in &members {
            dsh.remove(self.dep_loc[r as usize]);
            psh.remove(self.prec_loc[r as usize]);
        }
        dsh.settle();
        psh.settle();
        for p in &pieces {
            dsh.insert(p.is_cell());
            let pb = key
                .proj
                .forward(p)
                .expect("members instantiate on the grid");
            psh.insert(pb.is_cell());
        }
        b.component(
            self.idx.dep[key.dep_sheet as usize].heap_bytes(),
            dsh.heap_bytes(),
        );
        b.component(
            self.idx.prec[key.proj.sheet as usize].heap_bytes(),
            psh.heap_bytes(),
        );
        let transient = b.transient
            + (dsh.peak() - dsh.heap_bytes())
            + (psh.peak() - psh.heap_bytes())
            + scratch
            + pieces.capacity() * size_of::<Rect>();
        let n = pieces.len();
        let entry = size_of::<(super::super::geom::BoxT, u32)>();
        // Byte-safe acceptance: live model bytes must not grow, and the
        // capacity growth (index buffers, levels, arenas) plus the transient
        // peak must be admitted like a mutation's.
        let per = size_of::<Rec>() + 2 * entry;
        let live_grows = n * per > l * per;
        let retained_after = self.heap_bytes() - b.old as u64 + b.new as u64;
        let admitted = self.admit(retained_after, transient as u64).is_ok();
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
            return false;
        }
        // Commit: removes → settle → inserts (the shadow's order).
        let bytes_before = self.heap_bytes();
        let live_before = self.live_model_bytes();
        grow_exact(&mut self.recs, recs_cap).expect("admitted repartition");
        self.sync_locs();
        self.reset_peaks();
        for &r in &members {
            self.remove_rec(r);
        }
        self.idx.dep[key.dep_sheet as usize].settle(&mut self.dep_loc);
        self.idx.prec[key.proj.sheet as usize].settle(&mut self.prec_loc);
        for p in &pieces {
            self.add_rec(g, *p);
        }
        self.stats.records_created -= n as u64;
        let grp = &mut self.egroups[g as usize];
        grp.c = 0;
        grp.set_suspended(false);
        grp.set_kept(false);
        if self.live_model_bytes() > live_before {
            self.stats.repartition_live_up += 1;
        }
        self.stats.repartition_retained_growth += self.heap_bytes().saturating_sub(bytes_before);
        debug_assert_eq!(
            self.idx.dep[key.dep_sheet as usize].heap_bytes(),
            dsh.heap_bytes()
        );
        debug_assert!(self.observed_index_transient() as usize <= transient);
        debug_assert_eq!(
            self.idx.prec[key.proj.sheet as usize].heap_bytes(),
            psh.heap_bytes()
        );
        self.stats.repartitions_committed += 1;
        self.note_repartition(l, n, created, 0);
        true
    }

    /// Repartition node group `g`. Returns whether new pieces committed.
    pub(super) fn repartition_nodes(&mut self, g: u32) -> bool {
        self.stats.repartition_checks += 1;
        let members = self.ngroups[g as usize].members.clone();
        let l = members.len();
        let created = self.ngroups[g as usize].c;
        let scratch = canon::scratch_bound(l) + members.capacity() * 4 + l * size_of::<Rect>();
        if self.budget.scratch.is_some_and(|lim| scratch as u64 > lim) {
            let grp = &mut self.ngroups[g as usize];
            grp.set_suspended(true);
            grp.c = 0;
            self.stats.repartition_skipped += 1;
            return false;
        }
        let (sheet, _) = self.ngroups.key(g);
        let rects: Vec<Rect> = members
            .iter()
            .map(|&o| self.owners[o as usize].dom)
            .collect();
        let pieces = canon::canon(&rects, &mut self.stats.canon_work);
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
        b.component(
            vec_bytes::<Owner>(self.owners.capacity()),
            vec_bytes::<Owner>(owners_cap),
        );
        b.component(
            vec_bytes::<u32>(self.node_loc.capacity()),
            vec_bytes::<u32>(self.node_loc.capacity().max(owners_cap)),
        );
        {
            let m = &self.ngroups[g as usize].members;
            b.component(members_heap(m), members_heap_for(members_cap_after(m, n)));
        }
        let mut nsh = self.idx.node[sheet as usize].shadow();
        for &o in &members {
            if self.owners[o as usize].is_family() {
                nsh.remove(self.node_loc[o as usize]);
            }
        }
        nsh.settle();
        for p in &pieces {
            if !p.is_cell() {
                nsh.insert(false);
            }
        }
        b.component(self.idx.node[sheet as usize].heap_bytes(), nsh.heap_bytes());
        let transient = b.transient
            + (nsh.peak() - nsh.heap_bytes())
            + scratch
            + pieces.capacity() * size_of::<Rect>();
        let entry = size_of::<(super::super::geom::BoxT, u32)>();
        let fam_before = members
            .iter()
            .filter(|&&o| self.owners[o as usize].is_family())
            .count();
        let fam_after = pieces.iter().filter(|p| !p.is_cell()).count();
        let live_before = l * size_of::<Owner>() + fam_before * entry;
        let live_after = n * size_of::<Owner>() + fam_after * entry;
        let live_grows = live_after > live_before;
        let retained_after = self.heap_bytes() - b.old as u64 + b.new as u64;
        let admitted = self.admit(retained_after, transient as u64).is_ok();
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
            return false;
        }
        // Sources for each new piece: the old owner of its anchor cell.
        let srcs: Vec<(AstNodeId, (u32, u32), u16)> = pieces
            .iter()
            .map(|p| {
                let o = self
                    .owner_at((sheet, p.r0, p.c0))
                    .expect("piece cell has an owner");
                let w = &self.owners[o as usize];
                (w.template, w.anchor, w.flags)
            })
            .collect();
        // Singleton cells before the swap (their runs may turn FAMILY).
        let old_singles: Vec<Cell> = members
            .iter()
            .filter(|&&o| !self.owners[o as usize].is_family())
            .map(|&o| {
                let d = self.owners[o as usize].dom;
                (sheet, d.r0, d.c0)
            })
            .collect();

        let bytes_before = self.heap_bytes();
        let live_before = self.live_model_bytes();
        grow_exact(&mut self.owners, owners_cap).expect("admitted repartition");
        self.sync_locs();
        self.reset_peaks();
        for &o in &members {
            self.remove_owner(o);
        }
        self.idx.node[sheet as usize].settle(&mut self.node_loc);
        let mut relabels = 0u64;
        for (p, (template, anchor, flags)) in pieces.iter().zip(srcs) {
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
        for cell in old_singles {
            let (_, h) = self.ids.lookup(cell).expect("formula cell");
            let o = self.owner_at_fresh(cell);
            if self.owners[o as usize].is_family() {
                self.ids.set_owner(h, FAMILY);
                relabels += 1;
                let node_idx = &self.idx.node;
                self.ids.coalesce_at(cell, &|a, b| {
                    let at = |row: u32, col: u32| {
                        let mut f = None;
                        node_idx[a.sheet as usize]
                            .query(&[row, col, row, col], &mut |x| f = Some(x));
                        f
                    };
                    a.owner == FAMILY
                        && b.owner == FAMILY
                        && at(a.row_start + a.len - 1, a.col) == at(b.row_start, b.col)
                });
            }
        }
        let grp = &mut self.ngroups[g as usize];
        grp.c = 0;
        grp.set_suspended(false);
        grp.set_kept(false);
        if self.live_model_bytes() > live_before {
            self.stats.repartition_live_up += 1;
        }
        self.stats.repartition_retained_growth += self.heap_bytes().saturating_sub(bytes_before);
        debug_assert_eq!(self.idx.node[sheet as usize].heap_bytes(), nsh.heap_bytes());
        debug_assert!(self.observed_index_transient() as usize <= transient);
        self.stats.run_relabels += relabels;
        self.stats.repartitions_committed += 1;
        self.note_repartition(l, n, created, relabels);
        true
    }

    /// Owner lookup that ignores a stale run owner (used mid-repartition,
    /// when a former singleton's run still names its dead owner).
    fn owner_at_fresh(&self, cell: Cell) -> u32 {
        let mut found = None;
        if let Some(idx) = self.idx.node.get(cell.0 as usize) {
            idx.query(&[cell.1, cell.2, cell.1, cell.2], &mut |o| found = Some(o));
        }
        if let Some(o) = found {
            return o;
        }
        let (_, h) = self.ids.lookup(cell).expect("formula cell");
        self.ids.run(h).owner
    }
}
