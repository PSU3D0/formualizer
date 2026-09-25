//! Unit-stride rectangles and exact per-column cell covers.
//!
//! Coordinates are 0-based and inclusive. The grid is the engine's
//! `1_048_576 × 16_384`; an open reference bound means the grid edge.

use std::collections::BTreeMap;

/// Last row index of the grid (0-based).
pub const MAX_ROW: u32 = 1_048_575;
/// Last column index of the grid (0-based).
pub const MAX_COL: u32 = 16_383;

/// The symbol plane: a reserved store sheet holding one node per defined
/// name (design §4.1). It is never a workbook sheet id (the engine's sheet
/// registry would need 65,535 sheets to reach it).
pub const SYMBOL_SHEET: u16 = u16::MAX;

/// Position of `sheet` in the store's per-sheet vectors. The symbol plane
/// takes slot 0, so it costs one entry, not a 65,536-entry directory.
#[inline]
pub fn sheet_slot(sheet: u16) -> usize {
    usize::from(sheet.wrapping_add(1))
}

/// A unit-stride rectangle: rows `r0..=r1`, columns `c0..=c1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rect {
    pub r0: u32,
    pub c0: u32,
    pub r1: u32,
    pub c1: u32,
}

impl Rect {
    pub fn new(r0: u32, c0: u32, r1: u32, c1: u32) -> Self {
        debug_assert!(r0 <= r1 && c0 <= c1, "empty rect {r0},{c0}..{r1},{c1}");
        Self { r0, c0, r1, c1 }
    }

    pub fn cell(r: u32, c: u32) -> Self {
        Self::new(r, c, r, c)
    }

    pub fn is_cell(&self) -> bool {
        self.r0 == self.r1 && self.c0 == self.c1
    }

    pub fn height(&self) -> u32 {
        self.r1 - self.r0 + 1
    }

    pub fn width(&self) -> u32 {
        self.c1 - self.c0 + 1
    }

    pub fn area(&self) -> u64 {
        u64::from(self.height()) * u64::from(self.width())
    }

    pub fn contains(&self, r: u32, c: u32) -> bool {
        self.r0 <= r && r <= self.r1 && self.c0 <= c && c <= self.c1
    }

    pub fn contains_rect(&self, o: &Rect) -> bool {
        self.r0 <= o.r0 && o.r1 <= self.r1 && self.c0 <= o.c0 && o.c1 <= self.c1
    }

    pub fn intersects(&self, o: &Rect) -> bool {
        self.r0 <= o.r1 && o.r0 <= self.r1 && self.c0 <= o.c1 && o.c0 <= self.c1
    }

    pub fn intersect(&self, o: &Rect) -> Option<Rect> {
        let r0 = self.r0.max(o.r0);
        let r1 = self.r1.min(o.r1);
        let c0 = self.c0.max(o.c0);
        let c1 = self.c1.min(o.c1);
        (r0 <= r1 && c0 <= c1).then_some(Rect { r0, c0, r1, c1 })
    }

    /// `self \ q` as at most four disjoint rectangles, column-major: the
    /// full-height strips left and right of `q`'s columns, then the parts
    /// above and below `q` inside its columns. Identity runs are per column,
    /// so a column-major cut splits runs only in `q`'s own columns (design
    /// §5.1, SP-2 F-6.1).
    pub fn subtract(&self, q: &Rect) -> Pieces {
        let mut out = Pieces::default();
        let Some(i) = self.intersect(q) else {
            out.push(*self);
            return out;
        };
        if self.c0 < i.c0 {
            out.push(Rect::new(self.r0, self.c0, self.r1, i.c0 - 1));
        }
        if i.c1 < self.c1 {
            out.push(Rect::new(self.r0, i.c1 + 1, self.r1, self.c1));
        }
        if self.r0 < i.r0 {
            out.push(Rect::new(self.r0, i.c0, i.r0 - 1, i.c1));
        }
        if i.r1 < self.r1 {
            out.push(Rect::new(i.r1 + 1, i.c0, self.r1, i.c1));
        }
        out
    }

    /// Number of pieces `subtract(q)` yields, without building them.
    pub fn subtract_count(&self, q: &Rect) -> usize {
        let Some(i) = self.intersect(q) else {
            return 1;
        };
        usize::from(self.c0 < i.c0)
            + usize::from(i.c1 < self.c1)
            + usize::from(self.r0 < i.r0)
            + usize::from(i.r1 < self.r1)
    }

    /// The level-index box `[r0, c0, r1, c1]`.
    pub fn as_box(&self) -> BoxT {
        [self.r0, self.c0, self.r1, self.c1]
    }
}

/// Index box: `[r0, c0, r1, c1]`, inclusive.
pub type BoxT = [u32; 4];

/// Inline list of at most four subtraction pieces.
#[derive(Clone, Copy, Debug, Default)]
pub struct Pieces {
    n: u8,
    v: [Rect; 4],
}

impl Default for Rect {
    fn default() -> Self {
        Rect::cell(0, 0)
    }
}

impl Pieces {
    fn push(&mut self, r: Rect) {
        self.v[self.n as usize] = r;
        self.n += 1;
    }

    pub fn as_slice(&self) -> &[Rect] {
        &self.v[..self.n as usize]
    }
}

/// A cell `(sheet, row, col)`.
pub type Cell = (u16, u32, u32);

/// Disjoint, non-adjacent row intervals of one column: `start -> end`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntervalSet {
    iv: BTreeMap<u32, u32>,
}

impl IntervalSet {
    pub fn is_empty(&self) -> bool {
        self.iv.is_empty()
    }

    pub fn contains(&self, r: u32) -> bool {
        self.iv
            .range(..=r)
            .next_back()
            .is_some_and(|(_, &e)| r <= e)
    }

    pub fn intervals(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.iv.iter().map(|(&s, &e)| (s, e))
    }

    pub fn cell_count(&self) -> u64 {
        self.iv.iter().map(|(&s, &e)| u64::from(e - s + 1)).sum()
    }

    /// Insert `[a, b]`, merging overlapping and adjacent intervals.
    pub fn insert(&mut self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        if let Some((&s, &e)) = self.iv.range(..=a).next_back()
            && e.saturating_add(1) >= a
        {
            a = s;
            b = b.max(e);
        }
        loop {
            let next = self
                .iv
                .range(a..)
                .next()
                .map(|(&s, &e)| (s, e))
                .filter(|&(s, _)| s <= b.saturating_add(1));
            match next {
                Some((s, e)) => {
                    self.iv.remove(&s);
                    b = b.max(e);
                }
                None => break,
            }
        }
        self.iv.insert(a, b);
    }

    /// The parts of `[a, b]` not in the set, in order.
    pub fn missing(&self, a: u32, b: u32, out: &mut Vec<(u32, u32)>) {
        let mut cur = a;
        if let Some((_, &e)) = self.iv.range(..=a).next_back()
            && e >= a
        {
            if e >= b {
                return;
            }
            cur = e + 1;
        }
        for (&s, &e) in self.iv.range(cur..) {
            if s > b {
                break;
            }
            if s > cur {
                out.push((cur, s - 1));
            }
            if e >= b {
                return;
            }
            cur = e + 1;
        }
        out.push((cur, b));
    }

    /// Remove `[a, b]`.
    pub fn remove(&mut self, a: u32, b: u32) {
        let mut hits: Vec<(u32, u32)> = Vec::new();
        if let Some((&s, &e)) = self.iv.range(..a).next_back()
            && e >= a
        {
            hits.push((s, e));
        }
        for (&s, &e) in self.iv.range(a..=b) {
            hits.push((s, e));
        }
        for (s, e) in hits {
            self.iv.remove(&s);
            if s < a {
                self.iv.insert(s, a - 1);
            }
            if e > b {
                self.iv.insert(b + 1, e);
            }
        }
    }
}

/// An exact cell set, per sheet and column as row interval sets. Used for
/// traversal coverage and for the dirty store (design §4.4).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cover {
    cols: BTreeMap<(u16, u32), IntervalSet>,
}

impl Cover {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn clear(&mut self) {
        self.cols.clear();
    }

    pub fn contains(&self, cell: Cell) -> bool {
        self.cols
            .get(&(cell.0, cell.2))
            .is_some_and(|s| s.contains(cell.1))
    }

    pub fn insert_rect(&mut self, sheet: u16, r: &Rect) {
        for c in r.c0..=r.c1 {
            self.cols.entry((sheet, c)).or_default().insert(r.r0, r.r1);
        }
    }

    pub fn remove_rect(&mut self, sheet: u16, r: &Rect) {
        for c in r.c0..=r.c1 {
            if let Some(s) = self.cols.get_mut(&(sheet, c)) {
                s.remove(r.r0, r.r1);
                if s.is_empty() {
                    self.cols.remove(&(sheet, c));
                }
            }
        }
    }

    /// Insert `r` and push the parts that were not yet covered (one rect
    /// per column interval) to `fresh`.
    pub fn insert_rect_fresh(&mut self, sheet: u16, r: &Rect, fresh: &mut Vec<(u16, Rect)>) {
        let mut miss = Vec::new();
        for c in r.c0..=r.c1 {
            let set = self.cols.entry((sheet, c)).or_default();
            miss.clear();
            set.missing(r.r0, r.r1, &mut miss);
            for &(a, b) in &miss {
                fresh.push((sheet, Rect::new(a, c, b, c)));
            }
            set.insert(r.r0, r.r1);
        }
    }

    /// The rows of `[a, b]` in column `col` of `sheet` not in the cover,
    /// as intervals, appended to `out`.
    pub fn missing_in(&self, sheet: u16, col: u32, a: u32, b: u32, out: &mut Vec<(u32, u32)>) {
        match self.cols.get(&(sheet, col)) {
            Some(set) => set.missing(a, b, out),
            None => out.push((a, b)),
        }
    }

    /// Whether any cell of `r` is in the cover.
    pub fn intersects_rect(&self, sheet: u16, r: &Rect) -> bool {
        let mut miss = Vec::new();
        for c in r.c0..=r.c1 {
            if let Some(s) = self.cols.get(&(sheet, c)) {
                miss.clear();
                s.missing(r.r0, r.r1, &mut miss);
                let missing: u64 = miss.iter().map(|&(a, b)| u64::from(b - a + 1)).sum();
                if missing < u64::from(r.height()) {
                    return true;
                }
            }
        }
        false
    }

    pub fn cell_count(&self) -> u64 {
        self.cols.values().map(IntervalSet::cell_count).sum()
    }

    /// Covered column intervals: `(sheet, col, r0, r1)`, sorted.
    pub fn column_intervals(&self) -> impl Iterator<Item = (u16, u32, u32, u32)> + '_ {
        self.cols
            .iter()
            .flat_map(|(&(s, c), set)| set.intervals().map(move |(a, b)| (s, c, a, b)))
    }

    /// Every covered cell (tests and small oracles only).
    pub fn cells(&self) -> Vec<Cell> {
        let mut out = Vec::new();
        for (s, c, a, b) in self.column_intervals() {
            for r in a..=b {
                out.push((s, r, c));
            }
        }
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtract_is_exact_column_major() {
        let a = Rect::new(2, 2, 8, 6);
        for q in [
            Rect::new(0, 0, 3, 3),
            Rect::new(4, 3, 5, 4),
            Rect::new(0, 0, 20, 20),
            Rect::new(9, 9, 9, 9),
            Rect::new(2, 6, 8, 6),
        ] {
            let pieces = a.subtract(&q);
            assert_eq!(pieces.as_slice().len(), a.subtract_count(&q));
            let mut cells = 0u64;
            for p in pieces.as_slice() {
                assert!(p.intersect(&q).is_none());
                assert!(a.contains_rect(p));
                cells += p.area();
            }
            let cut = a.intersect(&q).map_or(0, |i| i.area());
            assert_eq!(cells + cut, a.area());
            for (i, p) in pieces.as_slice().iter().enumerate() {
                for o in &pieces.as_slice()[i + 1..] {
                    assert!(!p.intersects(o));
                }
            }
        }
    }

    #[test]
    fn interval_set_matches_brute_force() {
        let mut set = IntervalSet::default();
        let mut brute = [false; 64];
        let mut x: u32 = 7;
        for step in 0..2000 {
            x = x.wrapping_mul(1_103_515_245).wrapping_add(12345);
            let a = (x >> 8) % 60;
            let b = (a + (x >> 20) % 5).min(63);
            if step % 3 == 0 {
                set.remove(a, b);
                brute[a as usize..=b as usize].fill(false);
            } else {
                let mut miss = Vec::new();
                set.missing(a, b, &mut miss);
                let want: Vec<u32> = (a..=b).filter(|&r| !brute[r as usize]).collect();
                let got: Vec<u32> = miss.iter().flat_map(|&(s, e)| s..=e).collect();
                assert_eq!(got, want);
                set.insert(a, b);
                brute[a as usize..=b as usize].fill(true);
            }
            for r in 0..64u32 {
                assert_eq!(set.contains(r), brute[r as usize], "step {step} row {r}");
            }
            let ivs: Vec<(u32, u32)> = set.intervals().collect();
            for w in ivs.windows(2) {
                assert!(w[0].1 + 1 < w[1].0, "not maximal: {ivs:?}");
            }
        }
    }
}
