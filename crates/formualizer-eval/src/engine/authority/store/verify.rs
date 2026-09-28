//! Invariant checks (tests and gates) and the canonical relation digest
//! used for maintained == rebuild (I1 at the representation level: by
//! Lemma C1, `canon` of a group's cell set is independent of how the group
//! is fragmented).

use super::*;
use std::collections::BTreeMap;

/// Edge key with the lookup-slot owner spelled out (ids differ between a
/// maintained store and a rebuild).
pub type DigestKey = (u16, Tag, Option<LkKey>, RefProj);

/// Canonical content of a store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Digest {
    pub edges: BTreeMap<DigestKey, Vec<Rect>>,
    pub nodes: BTreeMap<(u16, u64), Vec<Rect>>,
    pub formulas: Vec<Cell>,
}

impl Store {
    /// Diagnostic dump of a cell's identity and owner (tests).
    pub fn debug_cell(&self, cell: Cell) -> String {
        let run = self.ids.lookup(cell).map(|(id, h)| (id, *self.ids.run(h)));
        let owner = self.owner_at(cell).map(|o| (o, self.owners[o as usize]));
        let near: Vec<(usize, Rect, u32)> = self
            .owners
            .iter()
            .enumerate()
            .filter(|(_, w)| {
                w.group != DEAD && w.sheet == cell.0 && w.dom.c0 <= cell.2 && cell.2 <= w.dom.c1
            })
            .map(|(i, w)| (i, w.dom, w.group))
            .collect();
        format!(
            "run {run:?} owner {owner:?} owners in column {near:?} stats {:?}",
            self.stats
        )
    }

    pub fn digest(&self) -> Digest {
        let mut d = Digest::default();
        let mut w = CanonWork::default();
        for (key, rects) in self.edge_groups() {
            if rects.is_empty() {
                continue;
            }
            let lk = (key.lk != NO_LK).then(|| self.lks.key(key.lk).clone());
            d.edges.insert(
                (key.dep_sheet, key.tag, lk, key.proj),
                canon::canon(&rects, &mut w),
            );
        }
        for (key, rects) in self.node_groups() {
            if rects.is_empty() {
                continue;
            }
            d.nodes.insert(key, canon::canon(&rects, &mut w));
        }
        d.formulas = self.formula_cells();
        d
    }

    /// Structural invariants: the maintained accounting equals the census
    /// (B2), ID1–ID5, owner partition, G-DISJ in every group, the ownership
    /// lemma, index consistency, and the §5.7.3 memory statement for
    /// unsuspended groups.
    pub fn check(&self) -> Result<(), String> {
        self.check_accounting()?;
        self.ids.check()?;
        self.symbols.check(&self.ids)?;
        // Formula cells, from the identity table.
        let mut formula = Cover::new();
        let mut nformula = 0u64;
        for (_, r) in self.ids.live_runs() {
            formula.insert_rect(
                r.sheet,
                &Rect::new(r.row_start, r.col, r.row_start + r.len - 1, r.col),
            );
            nformula += u64::from(r.len);
        }
        // Owners partition the formula cells.
        let mut owned = Cover::new();
        let mut owned_cells = 0u64;
        let mut nodes = 0u64;
        let mut fresh = Vec::new();
        for (o, w) in self.owners.iter().enumerate() {
            if w.group == DEAD {
                continue;
            }
            fresh.clear();
            owned.insert_rect_fresh(w.sheet, &w.dom, &mut fresh);
            let got: u64 = fresh.iter().map(|(_, r)| r.area()).sum();
            if got != w.dom.area() {
                return Err(format!("owner {o} overlaps another owner"));
            }
            owned_cells += got;
            if w.is_family() {
                nodes += 1;
                if self.node_loc.get(o).copied().unwrap_or(NONE) == NONE {
                    return Err(format!("family owner {o} is not indexed"));
                }
            }
            if w.group != UNGROUPED {
                let grp = &self.ngroups[w.group as usize];
                if grp.members.get(w.pos as usize) != Some(&(o as u32)) {
                    return Err(format!("owner {o} not at its group position"));
                }
                if self.ngroups.key(w.group).0 != w.sheet {
                    return Err(format!("owner {o} in a group of another sheet"));
                }
            }
        }
        if owned_cells != nformula || owned != formula {
            let a = owned.cells();
            let b = formula.cells();
            let only_ids: Vec<_> = b
                .iter()
                .filter(|c| a.binary_search(c).is_err())
                .take(5)
                .collect();
            let only_owned: Vec<_> = a
                .iter()
                .filter(|c| b.binary_search(c).is_err())
                .take(5)
                .collect();
            return Err(format!(
                "owners cover {owned_cells} cells, identity {nformula}; ids without owner {only_ids:?}, owned without id {only_owned:?}"
            ));
        }
        if nodes != self.nnodes {
            return Err("node count".into());
        }
        let node_live: usize = self.idx.node.iter().map(LevelIndex::live).sum();
        if node_live as u64 != nodes {
            return Err(format!("node index holds {node_live} of {nodes} families"));
        }
        // Runs: singleton runs name their owner; family runs lie in a node.
        for (h, r) in self.ids.live_runs() {
            for i in [0, r.len - 1] {
                let cell = (r.sheet, r.row_start + i, r.col);
                let Some(o) = self.owner_at(cell) else {
                    return Err(format!("run {h}: no owner at {cell:?}"));
                };
                let w = &self.owners[o as usize];
                if !w.dom.contains(cell.1, cell.2) || w.sheet != cell.0 || w.group == DEAD {
                    return Err(format!("run {h}: owner {o} does not hold {cell:?}"));
                }
                if (r.owner == FAMILY) != w.is_family() {
                    return Err(format!("run {h}: owner kind mismatch at {cell:?}"));
                }
            }
        }
        // Records: group positions, G-DISJ per group, ownership lemma.
        let mut live = 0usize;
        for (g, _, grp) in self.egroups.iter() {
            let mut cover = Cover::new();
            for (pos, &r) in grp.members.iter().enumerate() {
                let rec = &self.recs[r as usize];
                if rec.group != g || rec.pos != pos as u32 {
                    return Err(format!("record {r} not at its group position"));
                }
                fresh.clear();
                cover.insert_rect_fresh(self.egroups.key(g).dep_sheet, &rec.dep, &mut fresh);
                let got: u64 = fresh.iter().map(|(_, x)| x.area()).sum();
                if got != rec.dep.area() {
                    return Err(format!("G-DISJ violated in edge group {g}"));
                }
                let s = self.egroups.key(g).dep_sheet;
                for c in rec.dep.c0..=rec.dep.c1 {
                    for row in [rec.dep.r0, rec.dep.r1] {
                        if !formula.contains((s, row, c)) {
                            return Err(format!(
                                "record {r} covers non-formula cell {:?}",
                                (s, row, c)
                            ));
                        }
                    }
                }
                if self.dep_loc[r as usize] == NONE || self.prec_loc[r as usize] == NONE {
                    return Err(format!("record {r} is not indexed"));
                }
                live += 1;
            }
            if !grp.suspended()
                && !grp.kept()
                && grp.members.len() as u64 > 2 * u64::from(grp.b()) + 2
            {
                return Err(format!(
                    "memory statement: edge group {g} holds {} > 2·{}+2",
                    grp.members.len(),
                    grp.b()
                ));
            }
        }
        if live != self.recs.len() - self.rec_nfree {
            return Err("record accounting".into());
        }
        let dl: usize = self.idx.dep.iter().map(LevelIndex::live).sum();
        let pl: usize = self.idx.prec.iter().map(LevelIndex::live).sum();
        if dl != live || pl != live {
            return Err(format!("index sizes dep {dl} prec {pl} for {live} records"));
        }
        for (g, _, grp) in self.ngroups.iter() {
            if !grp.suspended()
                && !grp.kept()
                && grp.members.len() as u64 > 2 * u64::from(grp.b()) + 2
            {
                return Err(format!(
                    "memory statement: node group {g} holds {} > 2·{}+2",
                    grp.members.len(),
                    grp.b()
                ));
            }
        }
        Ok(())
    }
}
