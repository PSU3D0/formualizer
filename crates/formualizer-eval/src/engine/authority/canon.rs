//! Unit-stride `canon()` (design §5.7.1, promoted from SP-2's `canon.rs`).
//!
//! For a cell set X given as cell-disjoint unit-stride rectangles (G-DISJ):
//! per column, the maximal contiguous row intervals of X; then every maximal
//! run of consecutive columns with an identical interval is one rectangle.
//! The sweep never expands cells: it walks column events (ADD at a
//! rectangle's first column, DEL after its last) and only recomputes the
//! maximal intervals an event touches.
//!
//! - Lemma C1: the output depends only on the cell set.
//! - Lemma C2: at most `3k` rectangles for `k` disjoint inputs.

use super::avl::{AvlMap, ReserveError};
use super::geom::Rect;
#[cfg(test)]
use std::collections::BTreeMap;

/// Counted sweep work (never timed).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CanonWork {
    pub events: u64,
    pub touched: u64,
    pub emitted: u64,
}

impl CanonWork {
    pub fn total(&self) -> u64 {
        self.events + self.touched + self.emitted
    }
}

/// Upper bound on the heap `try_canon` allocates for `k` inputs (M1a
/// correction B5: every container is reserved up front): the event list
/// (`2k`), both arena maps (`k` nodes each), the interval-value slab and
/// its free list, the per-column lists at their proven bounds for a column
/// of `e ≤ 2k` events (spans, merged and after `4e`, before `3e`) and the
/// output (`3k`, Lemma C2). Charged to scratch before a repartition runs
/// (SP-2 F-5).
pub fn scratch_bound(k: usize) -> usize {
    let e = 2 * k;
    let events = e * size_of::<(u32, bool, u32, u32)>();
    let maps = 2 * k * AvlMap::NODE_BYTES + k * (size_of::<(u32, u32)>() + size_of::<u32>());
    let per_column = 4 * e * size_of::<(u32, u32)>() * 3 + 3 * e * size_of::<(u32, u32, u32)>();
    let output = 3 * k * size_of::<Rect>();
    events + maps + per_column + output
}

fn try_vec<T>(n: usize) -> Result<Vec<T>, ReserveError> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| ReserveError)?;
    Ok(v)
}

/// Push within the reservation; grow fallibly if a bound was loose.
fn tpush<T>(v: &mut Vec<T>, x: T) -> Result<(), ReserveError> {
    if v.len() == v.capacity() {
        v.try_reserve(1).map_err(|_| ReserveError)?;
    }
    v.push(x);
    Ok(())
}

/// Unit-stride canonical partition of `rects`, which must be cell-disjoint.
/// Output is sorted by `(c0, r0)`.
pub fn canon(rects: &[Rect], work: &mut CanonWork) -> Vec<Rect> {
    try_canon(rects, work).expect("canon allocation")
}

/// [`canon`] of distinct single cells (the build's group members), without
/// the sweep: sort by `(col, row)` and run-length the rows into each
/// column's maximal intervals, then sort by interval and merge runs of
/// consecutive columns. Same output as `canon` (Lemma C1: it depends only
/// on the cell set); `cells` is reordered in place. Work is counted as one
/// event per cell and one emission per rectangle.
pub fn canon_cells(cells: &mut [Rect], work: &mut CanonWork) -> Vec<Rect> {
    debug_assert!(cells.iter().all(Rect::is_cell));
    cells.sort_unstable_by_key(|r| (r.c0, r.r0));
    work.events += cells.len() as u64;
    // (r0, r1, col): each column's maximal row intervals.
    let mut runs: Vec<(u32, u32, u32)> = Vec::with_capacity(cells.len().min(1024));
    for r in cells.iter() {
        match runs.last_mut() {
            Some(last) if last.2 == r.c0 && last.1 + 1 == r.r0 => last.1 = r.r0,
            _ => runs.push((r.r0, r.r0, r.c0)),
        }
    }
    runs.sort_unstable();
    let mut out: Vec<Rect> = Vec::with_capacity(runs.len());
    let mut open: Option<(u32, u32, u32, u32)> = None;
    for (r0, r1, c) in runs {
        open = match open {
            Some((a, b, c0, c1)) if a == r0 && b == r1 && c1 + 1 == c => Some((a, b, c0, c)),
            Some((a, b, c0, c1)) => {
                out.push(Rect::new(a, c0, b, c1));
                Some((r0, r1, c, c))
            }
            None => Some((r0, r1, c, c)),
        };
    }
    if let Some((a, b, c0, c1)) = open {
        out.push(Rect::new(a, c0, b, c1));
    }
    out.sort_unstable_by_key(|r| (r.c0, r.r0));
    work.emitted += out.len() as u64;
    out
}

/// [`canon`] with every allocation reserved up front and fallible: on an
/// allocation failure it returns `ReserveError` and nothing else changes.
pub fn try_canon(rects: &[Rect], work: &mut CanonWork) -> Result<Vec<Rect>, ReserveError> {
    let k = rects.len();
    // (column, is_add, r0, r1). DEL sorts before ADD in one column; the
    // result does not depend on the order within a column.
    let mut ev: Vec<(u32, bool, u32, u32)> = try_vec(2 * k)?;
    for r in rects {
        ev.push((r.c0, true, r.r0, r.r1));
        ev.push((r.c1 + 1, false, r.r0, r.r1));
    }
    ev.sort_unstable();
    let mut emax = 0usize;
    let mut i = 0;
    while i < ev.len() {
        let mut j = i;
        while j < ev.len() && ev[j].0 == ev[i].0 {
            j += 1;
        }
        emax = emax.max(j - i);
        i = j;
    }
    // Input intervals present in the current column: r0 -> r1 (disjoint).
    let mut active = AvlMap::new();
    active.try_reserve_slots(k)?;
    // Maximal intervals of the current column: start -> slot of
    // (end, start column) in `mvals`.
    let mut maximal = AvlMap::new();
    maximal.try_reserve_slots(k)?;
    let mut mvals: Vec<(u32, u32)> = try_vec(k)?;
    let mut mfree: Vec<u32> = try_vec(k)?;
    let mut out: Vec<Rect> = try_vec(3 * k)?;
    let mut spans: Vec<(u32, u32)> = try_vec(4 * emax)?;
    let mut before: Vec<(u32, u32, u32)> = try_vec(3 * emax)?;
    let mut after: Vec<(u32, u32)> = try_vec(4 * emax)?;
    let mut merged: Vec<(u32, u32)> = try_vec(4 * emax)?;
    let mut oom = false;
    let mut i = 0;
    while i < ev.len() {
        let col = ev[i].0;
        let mut j = i;
        while j < ev.len() && ev[j].0 == col {
            j += 1;
        }
        // Maximal intervals touched by this column's events: containing,
        // overlapping or adjacent to an event interval (≤ 3 per event).
        spans.clear();
        before.clear();
        for &(_, _, a, b) in &ev[i..j] {
            work.events += 1;
            tpush(&mut spans, (a, b))?;
            let lo = a.saturating_sub(1);
            let hi = b.saturating_add(1);
            if let Some((s, v)) = maximal.pred(u64::from(lo)) {
                let (e, sc) = mvals[v as usize];
                if e >= lo {
                    tpush(&mut before, (s as u32, e, sc))?;
                }
            }
            maximal.range_visit(
                u64::from(lo.saturating_add(1)),
                u64::from(hi),
                &mut |s, v| {
                    let (e, sc) = mvals[v as usize];
                    oom |= tpush(&mut before, (s as u32, e, sc)).is_err();
                },
            );
            if oom {
                return Err(ReserveError);
            }
        }
        before.sort_unstable();
        before.dedup();
        work.touched += before.len() as u64;
        for &(s, e, _) in &before {
            tpush(&mut spans, (s, e))?;
        }
        for &(_, add, a, b) in &ev[i..j] {
            if add {
                active.insert(u64::from(a), b);
            } else {
                let removed = active.remove(u64::from(a));
                debug_assert_eq!(removed, Some(b), "canon input is not cell-disjoint");
            }
        }
        // Merge the affected spans; the new maximal intervals inside each
        // merged span are the maximal runs of `active` there.
        spans.sort_unstable();
        merged.clear();
        for &(a, b) in &spans {
            match merged.last_mut() {
                Some(last) if a <= last.1.saturating_add(1) => last.1 = last.1.max(b),
                _ => tpush(&mut merged, (a, b))?,
            }
        }
        after.clear();
        for &(a, b) in &merged {
            let mut run: Option<(u32, u32)> = None;
            active.range_visit(u64::from(a), u64::from(b), &mut |s, e| {
                let s = s as u32;
                run = match run {
                    Some((rs, re)) if s == re + 1 => Some((rs, e)),
                    Some(done) => {
                        oom |= tpush(&mut after, done).is_err();
                        Some((s, e))
                    }
                    None => Some((s, e)),
                };
            });
            if let Some(done) = run {
                tpush(&mut after, done)?;
            }
            if oom {
                return Err(ReserveError);
            }
        }
        work.touched += after.len() as u64;
        // before \ after: emit; after \ before: begin at `col`.
        for &(s, e, sc) in &before {
            if after.binary_search(&(s, e)).is_err() {
                let v = maximal.remove(u64::from(s)).expect("maximal interval");
                tpush(&mut mfree, v)?;
                tpush(&mut out, Rect::new(s, sc, e, col - 1))?;
                work.emitted += 1;
            }
        }
        for &(s, e) in &after {
            let continues = before
                .binary_search_by(|&(bs, be, _)| (bs, be).cmp(&(s, e)))
                .is_ok();
            if !continues {
                let v = match mfree.pop() {
                    Some(v) => {
                        mvals[v as usize] = (e, col);
                        v
                    }
                    None => {
                        tpush(&mut mvals, (e, col))?;
                        (mvals.len() - 1) as u32
                    }
                };
                maximal.insert(u64::from(s), v);
            }
        }
        i = j;
    }
    debug_assert!(active.is_empty());
    out.sort_unstable_by_key(|r| (r.c0, r.r0));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    /// Independent cell-expanding reference: enumerate cells, per-column
    /// maximal intervals, horizontal merge of identical intervals.
    fn reference(rects: &[Rect]) -> Vec<Rect> {
        let mut cells: BTreeSet<(u32, u32)> = BTreeSet::new();
        for r in rects {
            for c in r.c0..=r.c1 {
                for row in r.r0..=r.r1 {
                    cells.insert((c, row));
                }
            }
        }
        let mut per_col: BTreeMap<u32, Vec<(u32, u32)>> = BTreeMap::new();
        for &(c, row) in &cells {
            let v = per_col.entry(c).or_default();
            match v.last_mut() {
                Some(last) if last.1 + 1 == row => last.1 = row,
                _ => v.push((row, row)),
            }
        }
        let mut open: BTreeMap<(u32, u32), u32> = BTreeMap::new();
        let mut out = Vec::new();
        let mut prev_col: Option<u32> = None;
        for (&c, ivs) in &per_col {
            let now: BTreeSet<(u32, u32)> = ivs.iter().copied().collect();
            let contiguous = prev_col == Some(c.wrapping_sub(1));
            let keys: Vec<(u32, u32)> = open.keys().copied().collect();
            for k in keys {
                if !contiguous || !now.contains(&k) {
                    let sc = open.remove(&k).unwrap();
                    out.push(Rect::new(k.0, sc, k.1, prev_col.unwrap()));
                }
            }
            for &k in &now {
                open.entry(k).or_insert(c);
            }
            prev_col = Some(c);
        }
        for (k, sc) in open {
            out.push(Rect::new(k.0, sc, k.1, prev_col.unwrap()));
        }
        out.sort_unstable_by_key(|r| (r.c0, r.r0));
        out
    }

    /// Up to `k` random disjoint rects on a small grid (greedy placement).
    fn disjoint(seeds: &[(u32, u32, u32, u32)], grid: u32) -> Vec<Rect> {
        let mut taken: BTreeSet<(u32, u32)> = BTreeSet::new();
        let mut out = Vec::new();
        for &(r, c, h, w) in seeds {
            let r = r % grid;
            let c = c % grid;
            let r1 = (r + h % 5).min(grid - 1);
            let c1 = (c + w % 5).min(grid - 1);
            let cand = Rect::new(r, c, r1, c1);
            let clash = (c..=c1).any(|cc| (r..=r1).any(|rr| taken.contains(&(cc, rr))));
            if !clash {
                for cc in c..=c1 {
                    for rr in r..=r1 {
                        taken.insert((cc, rr));
                    }
                }
                out.push(cand);
            }
        }
        out
    }

    /// Split every rect into its single cells (a maximal fragmentation).
    fn cells_of(rects: &[Rect]) -> Vec<Rect> {
        let mut v = Vec::new();
        for r in rects {
            for c in r.c0..=r.c1 {
                for row in r.r0..=r.r1 {
                    v.push(Rect::cell(row, c));
                }
            }
        }
        v
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 2000,
            rng_algorithm: proptest::test_runner::RngAlgorithm::ChaCha,
            ..ProptestConfig::default()
        })]

        /// C1 and C2: canon equals the reference, is independent of the
        /// fragmentation, and has at most 3k rectangles.
        #[test]
        fn canon_c1_c2(seeds in proptest::collection::vec((0u32..40, 0u32..40, 0u32..6, 0u32..6), 0..40)) {
            let rects = disjoint(&seeds, 23);
            let mut w = CanonWork::default();
            let got = canon(&rects, &mut w);
            prop_assert_eq!(&got, &reference(&rects));
            prop_assert!(got.len() <= 3 * rects.len());
            let frag = cells_of(&rects);
            prop_assert_eq!(&canon(&frag, &mut w), &got);
            // Idempotence.
            prop_assert_eq!(&canon(&got, &mut w), &got);
            // The build's cell fast path agrees.
            let mut frag = frag;
            prop_assert_eq!(&canon_cells(&mut frag, &mut w), &got);
        }
    }

    /// The re-review's N7 case: two records become three rectangles.
    #[test]
    fn two_records_three_rects() {
        let mut w = CanonWork::default();
        let out = canon(&[Rect::new(1, 1, 1, 3), Rect::new(2, 2, 2, 2)], &mut w);
        assert_eq!(
            out,
            vec![
                Rect::new(1, 1, 1, 1),
                Rect::new(1, 2, 2, 2),
                Rect::new(1, 3, 1, 3)
            ]
        );
    }

    /// Spans closed under recomputation: DEL and adjacent ADD in one column.
    #[test]
    fn sweep_span_closure() {
        let mut w = CanonWork::default();
        let rects = [
            Rect::new(0, 0, 4, 0),
            Rect::new(5, 1, 9, 1),
            Rect::new(0, 1, 4, 2),
            Rect::new(5, 0, 9, 0),
        ];
        let out = canon(&rects, &mut w);
        assert_eq!(out, vec![Rect::new(0, 0, 9, 1), Rect::new(0, 2, 4, 2)]);
    }
}
