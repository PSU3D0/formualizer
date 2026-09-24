//! Identity table (design §4.1, decision 9).
//!
//! Formula cells get a `Vid` from a checked `u32` counter at creation only.
//! An id is stable until the formula leaves its cell, is never renumbered,
//! rebased or reused, and survives every formula→formula edit (ID6).
//! Value and empty cells have no id.
//!
//! Ids live in **contiguous runs**: member `i` of a run sits at row
//! `row_start + i` of one column and has id `first_id + i`. The forward
//! directory is one ordered map per sheet keyed by `(col, row_start)`; the
//! reverse directory is one ordered map keyed by `first_id`. Both lookups
//! use the predecessor lemma of §4.1: runs are disjoint in rows per column
//! (ID2) and in ids (ID3), so the only run that can contain a coordinate is
//! the one with the greatest start at or below it.
//!
//! A run's `owner` is the owner handle only for singleton formulas. Family
//! members carry [`FAMILY`], and their owner is the family node whose domain
//! contains the cell, found through the per-sheet node index. This is the
//! SP-2 F-2 repair (addendum B-2): splitting or merging a family never
//! relabels the runs of its members.
//!
//! Every mutation is split into a read-only *plan* (the geometric decision)
//! and an *apply*. The admission dry run (§5.1) replays the same plan on a
//! counter-only [`IdShadow`], so predicted and actual slot use agree exactly.

use super::avl::{AvlMap, ReserveError, SlabShadow, grown};
use super::geom::Cell;

pub type Vid = u32;

/// Owner value of a run whose members belong to a family node.
pub const FAMILY: u32 = u32::MAX;
/// Sentinel id (never allocated).
pub const NO_VID: Vid = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdRun {
    pub row_start: u32,
    pub len: u32,
    pub first_id: Vid,
    pub col: u32,
    pub sheet: u16,
    /// Run flags (reserved: volatile, dynamic, unbound, non-relocatable).
    pub flags: u16,
    /// Singleton owner handle, or [`FAMILY`].
    pub owner: u32,
}

impl IdRun {
    fn end_row(&self) -> u32 {
        self.row_start + self.len - 1
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdError {
    /// The counter would pass its limit (maps to
    /// `ResourceExhaustionReason::GraphVertices` at the engine boundary).
    Exhausted {
        requested: u64,
        next: u32,
        limit: u32,
    },
    /// A resurrection named a live id, an unallocated id or an occupied cell.
    Conflict(String),
}

impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdError::Exhausted {
                requested,
                next,
                limit,
            } => write!(
                f,
                "identity counter exhausted: {requested} ids requested at {next} (limit {limit})"
            ),
            IdError::Conflict(m) => write!(f, "identity conflict: {m}"),
        }
    }
}

#[inline]
fn fwd_key(col: u32, row: u32) -> u64 {
    (u64::from(col) << 32) | u64::from(row)
}

/// Counter-only replica of the identity table's storage for dry runs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdShadow {
    pub runs: SlabShadow,
    pub live_runs: usize,
    pub fwd: Vec<SlabShadow>,
    pub fwd_dir_cap: usize,
    pub rev: SlabShadow,
    pub next_id: u64,
}

impl IdShadow {
    fn fwd(&mut self, sheet: u16) -> &mut SlabShadow {
        let s = sheet as usize;
        if self.fwd.len() <= s {
            self.fwd.resize(s + 1, SlabShadow::default());
            self.fwd_dir_cap = self.fwd_dir_cap.max(s + 1);
        }
        &mut self.fwd[s]
    }

    fn new_run(&mut self, sheet: u16) {
        self.runs.alloc();
        self.live_runs += 1;
        self.fwd(sheet).alloc();
        self.rev.alloc();
    }

    fn drop_run(&mut self, sheet: u16) {
        self.runs.release();
        self.live_runs -= 1;
        self.fwd(sheet).release();
        self.rev.release();
    }

    fn rekey(&mut self, sheet: u16) {
        self.fwd(sheet).release();
        self.fwd(sheet).alloc();
        self.rev.release();
        self.rev.alloc();
    }

    /// Heap bytes of the replica state.
    pub fn heap_bytes(&self) -> usize {
        self.runs.cap * size_of::<IdRun>()
            + self.fwd_dir_cap * size_of::<AvlMap>()
            + self
                .fwd
                .iter()
                .map(|s| s.cap * AvlMap::NODE_BYTES)
                .sum::<usize>()
            + self.rev.cap * AvlMap::NODE_BYTES
    }
}

/// A contiguous part `lo..=hi` (offsets) of one run leaving it: retired,
/// or — for a single cell — re-homed under a new owner with its id kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellCut {
    pub run: u32,
    pub run_value: IdRun,
    pub lo: u32,
    pub hi: u32,
}

impl CellCut {
    /// The first id in the cut (the cell's id for a single-cell cut).
    pub fn id(&self) -> Vid {
        self.run_value.first_id + self.lo
    }

    pub fn ids(&self) -> std::ops::RangeInclusive<Vid> {
        self.run_value.first_id + self.lo..=self.run_value.first_id + self.hi
    }

    pub fn is_cell(&self) -> bool {
        self.lo == self.hi
    }
}

#[derive(Clone, Debug)]
pub struct IdentityTable {
    runs: Vec<IdRun>,
    /// Free run slots, linked through `first_id`; a free slot has `len == 0`.
    free_run: u32,
    nfree_runs: usize,
    fwd: Vec<AvlMap>,
    rev: AvlMap,
    next_id: Vid,
    limit: Vid,
}

impl Default for IdentityTable {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentityTable {
    pub fn new() -> Self {
        Self::with_limit(u32::MAX - 1)
    }

    /// A table whose counter stops at `limit` (tests of the exhaustion path).
    pub fn with_limit(limit: Vid) -> Self {
        Self {
            runs: Vec::new(),
            free_run: NO_VID,
            nfree_runs: 0,
            fwd: Vec::new(),
            rev: AvlMap::new(),
            next_id: 0,
            limit: limit.min(u32::MAX - 1),
        }
    }

    pub fn next_id(&self) -> Vid {
        self.next_id
    }

    pub fn run_count(&self) -> usize {
        self.runs.len() - self.nfree_runs
    }

    pub fn run(&self, h: u32) -> &IdRun {
        &self.runs[h as usize]
    }

    pub fn heap_bytes(&self) -> usize {
        self.runs.capacity() * size_of::<IdRun>()
            + self.fwd.capacity() * size_of::<AvlMap>()
            + self.fwd.iter().map(AvlMap::heap_bytes).sum::<usize>()
            + self.rev.heap_bytes()
    }

    pub fn shadow(&self) -> IdShadow {
        IdShadow {
            runs: SlabShadow {
                slots: self.runs.len(),
                free: self.nfree_runs,
                cap: self.runs.capacity(),
            },
            live_runs: self.run_count(),
            fwd: self.fwd.iter().map(AvlMap::shadow).collect(),
            fwd_dir_cap: self.fwd.capacity(),
            rev: self.rev.shadow(),
            next_id: u64::from(self.next_id),
        }
    }

    /// Reserve every container to the sizes in `target` (a shadow produced
    /// by replaying the pending plan). Fallible; nothing is mutated on error.
    pub fn try_reserve_for(&mut self, target: &IdShadow) -> Result<(), ReserveError> {
        if target.runs.cap > self.runs.capacity() {
            self.runs
                .try_reserve_exact(target.runs.cap - self.runs.len())
                .map_err(|_| ReserveError)?;
        }
        if target.fwd.len() > self.fwd.capacity() {
            self.fwd
                .try_reserve_exact(target.fwd.len() - self.fwd.len())
                .map_err(|_| ReserveError)?;
        }
        while self.fwd.len() < target.fwd.len() {
            self.fwd.push(AvlMap::new());
        }
        for (m, t) in self.fwd.iter_mut().zip(&target.fwd) {
            m.try_reserve_slots(t.cap)?;
        }
        self.rev.try_reserve_slots(target.rev.cap)?;
        Ok(())
    }

    // ------------------------------------------------------------ counter

    /// Allocate `n` contiguous ids (checked; nothing changes on error).
    pub fn check_alloc(&self, n: u64) -> Result<(), IdError> {
        let end = u64::from(self.next_id) + n;
        if end > u64::from(self.limit) {
            return Err(IdError::Exhausted {
                requested: n,
                next: self.next_id,
                limit: self.limit,
            });
        }
        Ok(())
    }

    fn alloc_ids(&mut self, n: u32) -> Vid {
        debug_assert!(self.check_alloc(u64::from(n)).is_ok());
        let first = self.next_id;
        self.next_id += n;
        first
    }

    // ------------------------------------------------------------ lookups

    /// `cell → (id, run handle)`: the lookup lemma.
    pub fn lookup(&self, cell: Cell) -> Option<(Vid, u32)> {
        let (sheet, row, col) = cell;
        let map = self.fwd.get(sheet as usize)?;
        let (key, h) = map.pred(fwd_key(col, row))?;
        if (key >> 32) as u32 != col {
            return None;
        }
        let run = &self.runs[h as usize];
        (row <= run.end_row()).then(|| (run.first_id + (row - run.row_start), h))
    }

    pub fn id_of(&self, cell: Cell) -> Option<Vid> {
        self.lookup(cell).map(|(v, _)| v)
    }

    /// `id → cell`: predecessor search on the reverse directory.
    pub fn locate(&self, id: Vid) -> Option<Cell> {
        let (first, h) = self.rev.pred(u64::from(id))?;
        let run = &self.runs[h as usize];
        let off = id - first as u32;
        (off < run.len).then(|| (run.sheet, run.row_start + off, run.col))
    }

    /// The cut of `cell` out of its run, if it has an id.
    pub fn plan_cut(&self, cell: Cell) -> Option<CellCut> {
        let (id, h) = self.lookup(cell)?;
        let run = self.runs[h as usize];
        let off = id - run.first_id;
        Some(CellCut {
            run: h,
            run_value: run,
            lo: off,
            hi: off,
        })
    }

    /// Cuts retiring every id in rows `r0..=r1`, columns `c0..=c1` of
    /// `sheet`: one cut per intersecting run, column by column.
    pub fn plan_cuts_rect(&self, sheet: u16, r0: u32, c0: u32, r1: u32, c1: u32) -> Vec<CellCut> {
        let mut out = Vec::new();
        let mut hs = Vec::new();
        for col in c0..=c1 {
            hs.clear();
            self.runs_in(sheet, col, r0, r1, &mut hs);
            for &h in &hs {
                let run = self.runs[h as usize];
                let lo = r0.max(run.row_start) - run.row_start;
                let hi = r1.min(run.end_row()) - run.row_start;
                out.push(CellCut {
                    run: h,
                    run_value: run,
                    lo,
                    hi,
                });
            }
        }
        out
    }

    /// Runs of `sheet` intersecting column `col`, rows `r0..=r1`.
    pub fn runs_in(&self, sheet: u16, col: u32, r0: u32, r1: u32, out: &mut Vec<u32>) {
        let Some(map) = self.fwd.get(sheet as usize) else {
            return;
        };
        // The run with the greatest start ≤ r0 (it may start at r0).
        if let Some((key, h)) = map.pred(fwd_key(col, r0))
            && (key >> 32) as u32 == col
            && self.runs[h as usize].end_row() >= r0
        {
            out.push(h);
        }
        if r0 < r1 {
            let mut v = Vec::new();
            map.range(fwd_key(col, r0 + 1), fwd_key(col, r1), &mut v);
            out.extend(v.into_iter().map(|(_, h)| h));
        }
    }

    // ------------------------------------------------------------ run slots

    fn alloc_run(&mut self, run: IdRun) -> u32 {
        if self.free_run != NO_VID {
            let h = self.free_run;
            self.free_run = self.runs[h as usize].first_id;
            self.nfree_runs -= 1;
            self.runs[h as usize] = run;
            h
        } else {
            if self.runs.len() == self.runs.capacity() {
                let cap = grown(self.runs.capacity(), self.runs.len() + 1);
                self.runs.reserve_exact(cap - self.runs.len());
            }
            self.runs.push(run);
            (self.runs.len() - 1) as u32
        }
    }

    fn free_run_slot(&mut self, h: u32) {
        let r = &mut self.runs[h as usize];
        r.len = 0;
        r.first_id = self.free_run;
        self.free_run = h;
        self.nfree_runs += 1;
    }

    fn fwd_mut(&mut self, sheet: u16) -> &mut AvlMap {
        let s = sheet as usize;
        if self.fwd.len() <= s {
            if self.fwd.capacity() <= s {
                self.fwd.reserve_exact(s + 1 - self.fwd.len());
            }
            while self.fwd.len() <= s {
                self.fwd.push(AvlMap::new());
            }
        }
        &mut self.fwd[s]
    }

    fn link(&mut self, h: u32) {
        let r = self.runs[h as usize];
        self.fwd_mut(r.sheet).insert(fwd_key(r.col, r.row_start), h);
        self.rev.insert(u64::from(r.first_id), h);
    }

    fn unlink(&mut self, h: u32) {
        let r = self.runs[h as usize];
        let a = self.fwd_mut(r.sheet).remove(fwd_key(r.col, r.row_start));
        let b = self.rev.remove(u64::from(r.first_id));
        debug_assert!(a == Some(h) && b == Some(h), "run directories out of sync");
    }

    // ------------------------------------------------------------ mutations

    /// Shadow of [`Self::place`].
    pub fn shadow_place(sh: &mut IdShadow, sheet: u16, runs: u32, ids: u64) {
        for _ in 0..runs {
            sh.new_run(sheet);
        }
        sh.next_id += ids;
    }

    /// Place a column run of `len` new ids at `(sheet, row..row+len, col)`.
    /// The cells must have no id. Returns the run handle.
    pub fn place(&mut self, sheet: u16, row: u32, col: u32, len: u32, owner: u32) -> u32 {
        let first_id = self.alloc_ids(len);
        let h = self.alloc_run(IdRun {
            row_start: row,
            len,
            first_id,
            col,
            sheet,
            flags: 0,
            owner,
        });
        self.link(h);
        h
    }

    /// Shadow of [`Self::apply_cut`].
    pub fn shadow_cut(sh: &mut IdShadow, cut: &CellCut, keep: bool) {
        let r = cut.run_value;
        let last = r.len - 1;
        debug_assert!(!keep || cut.is_cell());
        if cut.lo == 0 && cut.hi == last {
            if !keep {
                sh.drop_run(r.sheet);
            }
            return;
        }
        if cut.lo == 0 {
            sh.rekey(r.sheet);
        } else if cut.hi != last {
            sh.new_run(r.sheet);
        }
        if keep {
            sh.new_run(r.sheet);
        }
    }

    /// Remove offsets `lo..=hi` from a run. With `keep` (single cell only)
    /// the cell keeps its id in a run of its own owned by `owner` (a
    /// formula→formula owner change, ID6); otherwise the ids are retired.
    /// Returns the cell's run handle when kept.
    pub fn apply_cut(&mut self, cut: &CellCut, keep: bool, owner: u32) -> Option<u32> {
        let h = cut.run;
        let r = self.runs[h as usize];
        debug_assert_eq!(r, cut.run_value, "stale cut plan");
        debug_assert!(!keep || cut.is_cell());
        let last = r.len - 1;
        let (lo, hi) = (cut.lo, cut.hi);
        if lo == 0 && hi == last {
            if keep {
                self.runs[h as usize].owner = owner;
                return Some(h);
            }
            self.unlink(h);
            self.free_run_slot(h);
            return None;
        }
        if lo == 0 {
            self.unlink(h);
            {
                let run = &mut self.runs[h as usize];
                run.row_start += hi + 1;
                run.first_id += hi + 1;
                run.len -= hi + 1;
            }
            self.link(h);
        } else if hi == last {
            self.runs[h as usize].len = lo;
        } else {
            self.runs[h as usize].len = lo;
            let below = IdRun {
                row_start: r.row_start + hi + 1,
                len: last - hi,
                first_id: r.first_id + hi + 1,
                ..r
            };
            let b = self.alloc_run(below);
            self.link(b);
        }
        keep.then(|| {
            let n = self.alloc_run(IdRun {
                row_start: r.row_start + lo,
                len: 1,
                first_id: r.first_id + lo,
                col: r.col,
                sheet: r.sheet,
                flags: r.flags,
                owner,
            });
            self.link(n);
            n
        })
    }

    /// Set the owner of a run (singleton ↔ family transitions).
    pub fn set_owner(&mut self, h: u32, owner: u32) {
        self.runs[h as usize].owner = owner;
    }

    /// History replay (ID4): put a retired `id` back at `cell` as a run of
    /// one. The id must have been allocated and must not be live, and the
    /// cell must have no id.
    pub fn resurrect(&mut self, id: Vid, cell: Cell, owner: u32) -> Result<u32, IdError> {
        if id >= self.next_id {
            return Err(IdError::Conflict(format!("id {id} was never allocated")));
        }
        if let Some(c) = self.locate(id) {
            return Err(IdError::Conflict(format!("id {id} is live at {c:?}")));
        }
        if let Some((other, _)) = self.lookup(cell) {
            return Err(IdError::Conflict(format!(
                "cell {cell:?} already has id {other}"
            )));
        }
        let h = self.alloc_run(IdRun {
            row_start: cell.1,
            len: 1,
            first_id: id,
            col: cell.2,
            sheet: cell.0,
            flags: 0,
            owner,
        });
        self.link(h);
        Ok(h)
    }

    /// Merge the run holding `cell` with its row- and id-contiguous
    /// neighbours in the column when `same_owner(upper, lower)` holds.
    /// Returns the number of merges.
    pub fn coalesce_at(&mut self, cell: Cell, same_owner: &dyn Fn(&IdRun, &IdRun) -> bool) -> u32 {
        let Some((_, mut h)) = self.lookup(cell) else {
            return 0;
        };
        let mut merges = 0;
        // Upward.
        let r = self.runs[h as usize];
        if r.row_start > 0
            && let Some((_, u)) = self.lookup((r.sheet, r.row_start - 1, r.col))
        {
            let up = self.runs[u as usize];
            if up.first_id + up.len == r.first_id && up.owner == r.owner && same_owner(&up, &r) {
                self.unlink(h);
                self.free_run_slot(h);
                self.runs[u as usize].len += r.len;
                h = u;
                merges += 1;
            }
        }
        // Downward.
        let r = self.runs[h as usize];
        if let Some((_, d)) = self.lookup((r.sheet, r.end_row() + 1, r.col)) {
            let down = self.runs[d as usize];
            if r.first_id + r.len == down.first_id && r.owner == down.owner && same_owner(&r, &down)
            {
                self.unlink(d);
                self.free_run_slot(d);
                self.runs[h as usize].len += down.len;
                merges += 1;
            }
        }
        merges
    }

    // ------------------------------------------------------------ checks

    /// Every live run `(handle, run)`, in no particular order.
    pub fn live_runs(&self) -> impl Iterator<Item = (u32, &IdRun)> + '_ {
        self.runs
            .iter()
            .enumerate()
            .filter(|(_, r)| r.len > 0)
            .map(|(h, r)| (h as u32, r))
    }

    /// ID1–ID5 as structural checks: runs are non-empty, disjoint in rows
    /// per column and in ids, contiguous, below the counter, and both
    /// directories index exactly the live runs.
    pub fn check(&self) -> Result<(), String> {
        let mut by_col: Vec<(u16, u32, u32, u32)> = Vec::new();
        let mut by_id: Vec<(u32, u32)> = Vec::new();
        let mut live = 0usize;
        for (h, r) in self.live_runs() {
            live += 1;
            if u64::from(r.first_id) + u64::from(r.len) > u64::from(self.next_id) {
                return Err(format!("run {h} holds ids past the counter"));
            }
            by_col.push((r.sheet, r.col, r.row_start, r.end_row()));
            by_id.push((r.first_id, r.first_id + r.len - 1));
            let fwd = self
                .fwd
                .get(r.sheet as usize)
                .and_then(|m| m.get(fwd_key(r.col, r.row_start)));
            if fwd != Some(h) {
                return Err(format!("run {h} missing from the forward directory"));
            }
            if self.rev.get(u64::from(r.first_id)) != Some(h) {
                return Err(format!("run {h} missing from the reverse directory"));
            }
        }
        if live != self.run_count() {
            return Err("free-slot accounting".into());
        }
        let fwd_len: usize = self.fwd.iter().map(AvlMap::len).sum();
        if fwd_len != live || self.rev.len() != live {
            return Err(format!(
                "directory sizes fwd {fwd_len} rev {} for {live} runs",
                self.rev.len()
            ));
        }
        by_col.sort_unstable();
        for w in by_col.windows(2) {
            if w[0].0 == w[1].0 && w[0].1 == w[1].1 && w[0].3 >= w[1].2 {
                return Err(format!("ID2: overlapping runs {:?} {:?}", w[0], w[1]));
            }
        }
        by_id.sort_unstable();
        for w in by_id.windows(2) {
            if w[0].1 >= w[1].0 {
                return Err(format!("ID3: overlapping id ranges {:?} {:?}", w[0], w[1]));
            }
        }
        for m in &self.fwd {
            m.check()?;
        }
        self.rev.check()?;
        Ok(())
    }
}
