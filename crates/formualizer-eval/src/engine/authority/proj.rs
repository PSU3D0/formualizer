//! Per-axis reference projections with exact unit-stride inverse and
//! forward images (promoted from the Packet B spike's `proj.rs`; strides
//! are gone because node domains and edge dependent rectangles are
//! unit-stride, design §5.7.1).
//!
//! A reference of a formula placed at 0-based `(row, col)` reads the
//! rectangle `[lo_r(row), hi_r(row)] × [lo_c(col), hi_c(col)]`. Each bound is
//! relative (`x + k`), absolute (a 0-based index) or open (the grid edge).
//! Every bound is a monotone non-decreasing function of the dependent
//! coordinate, so inversion factorises per axis and is O(1) per edge.

use super::geom::{MAX_COL, MAX_ROW, Rect};

/// One endpoint of a reference along one axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Bound {
    /// Offset from the placement coordinate.
    Rel(i32),
    /// 0-based index.
    Abs(u32),
    /// The grid edge on this bound's side (0 for `lo`, the maximum for `hi`).
    Open,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AxisMap {
    pub lo: Bound,
    pub hi: Bound,
}

#[derive(Clone, Copy, Debug)]
enum End {
    Var(i64),
    Const(i64),
}

const NEG: i64 = i64::MIN / 4;
const POS: i64 = i64::MAX / 4;

impl AxisMap {
    pub fn point(b: Bound) -> Self {
        Self { lo: b, hi: b }
    }

    pub fn fixed(lo: u32, hi: u32) -> Self {
        Self {
            lo: Bound::Abs(lo),
            hi: Bound::Abs(hi),
        }
    }

    fn ends(&self, max: u32) -> (End, End) {
        let lo = match self.lo {
            Bound::Rel(k) => End::Var(i64::from(k)),
            Bound::Abs(v) => End::Const(i64::from(v)),
            Bound::Open => End::Const(0),
        };
        let hi = match self.hi {
            Bound::Rel(k) => End::Var(i64::from(k)),
            Bound::Abs(v) => End::Const(i64::from(v)),
            Bound::Open => End::Const(i64::from(max)),
        };
        (lo, hi)
    }

    /// Window `[lo(x), hi(x)]` read by a dependent at coordinate `x`.
    pub fn window(&self, x: u32, max: u32) -> (i64, i64) {
        let (lo, hi) = self.ends(max);
        let x = i64::from(x);
        let f = |e: End| match e {
            End::Var(k) => x + k,
            End::Const(v) => v,
        };
        (f(lo), f(hi))
    }

    pub fn is_open(&self) -> bool {
        self.lo == Bound::Open || self.hi == Bound::Open
    }

    pub fn is_fixed(&self) -> bool {
        !matches!(self.lo, Bound::Rel(_)) && !matches!(self.hi, Bound::Rel(_))
    }

    /// `{ x ∈ [d0, d1] : [lo(x), hi(x)] ∩ [q0, q1] ≠ ∅ }`, exactly, as one
    /// interval. Requires `lo(x) ≤ hi(x)` on the domain.
    pub fn invert(&self, d0: u32, d1: u32, q0: u32, q1: u32, max: u32) -> Option<(u32, u32)> {
        let (lo, hi) = self.ends(max);
        let (q0, q1) = (i64::from(q0), i64::from(q1));
        // x qualifies iff lo(x) <= q1 and hi(x) >= q0.
        let (a, b) = match (lo, hi) {
            (End::Const(a), End::Const(b)) => {
                if a <= q1 && b >= q0 {
                    (NEG, POS)
                } else {
                    return None;
                }
            }
            (End::Var(a), End::Const(b)) => {
                if b < q0 {
                    return None;
                }
                (NEG, q1 - a)
            }
            (End::Const(a), End::Var(b)) => {
                if a > q1 {
                    return None;
                }
                (q0 - b, POS)
            }
            (End::Var(a), End::Var(b)) => (q0 - b, q1 - a),
        };
        let lo = a.max(i64::from(d0));
        let hi = b.min(i64::from(d1));
        (lo <= hi).then_some((lo as u32, hi as u32))
    }

    /// `∪_{x ∈ [x0, x1]} [lo(x), hi(x)]` clipped to `[0, max]`. Monotone
    /// bounds with `lo ≤ hi` make the union one interval.
    pub fn forward(&self, x0: u32, x1: u32, max: u32) -> Option<(u32, u32)> {
        let (lo, _) = self.window(x0, max);
        let (_, hi) = self.window(x1, max);
        let lo = lo.max(0);
        let hi = hi.min(i64::from(max));
        (lo <= hi).then_some((lo as u32, hi as u32))
    }
}

/// One reference of a formula relative to its placement: target sheet
/// plus per-axis maps. This is the edge-group projection of design §4.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefProj {
    pub sheet: u16,
    pub rows: AxisMap,
    pub cols: AxisMap,
}

impl RefProj {
    /// Absolute rectangle read by a dependent at `(row, col)`; `None` if the
    /// instantiated reference is reversed or leaves the grid.
    pub fn instantiate(&self, row: u32, col: u32) -> Option<Rect> {
        let (r0, r1) = self.rows.window(row, MAX_ROW);
        let (c0, c1) = self.cols.window(col, MAX_COL);
        let ok = r0 >= 0
            && c0 >= 0
            && r0 <= r1
            && c0 <= c1
            && r1 <= i64::from(MAX_ROW)
            && c1 <= i64::from(MAX_COL);
        ok.then(|| Rect::new(r0 as u32, c0 as u32, r1 as u32, c1 as u32))
    }

    /// Cells of the dependent rectangle `dep` whose instantiation meets `q`.
    pub fn invert(&self, dep: &Rect, q: &Rect) -> Option<Rect> {
        let (r0, r1) = self.rows.invert(dep.r0, dep.r1, q.r0, q.r1, MAX_ROW)?;
        let (c0, c1) = self.cols.invert(dep.c0, dep.c1, q.c0, q.c1, MAX_COL)?;
        Some(Rect::new(r0, c0, r1, c1))
    }

    /// Exact union of the instantiations over `dep` (the precedent box).
    pub fn forward(&self, dep: &Rect) -> Option<Rect> {
        let (r0, r1) = self.rows.forward(dep.r0, dep.r1, MAX_ROW)?;
        let (c0, c1) = self.cols.forward(dep.c0, dep.c1, MAX_COL)?;
        Some(Rect::new(r0, c0, r1, c1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds() -> Vec<Bound> {
        let mut v = vec![Bound::Open];
        for k in -3..=3 {
            v.push(Bound::Rel(k));
        }
        for a in [0u32, 2, 5, 9] {
            v.push(Bound::Abs(a));
        }
        v
    }

    /// Exhaustive check of `invert` and `forward` against brute force on a
    /// small axis (max = 14).
    #[test]
    fn axis_maps_are_exact_on_small_axes() {
        let max = 14u32;
        for lo in bounds() {
            for hi in bounds() {
                let map = AxisMap { lo, hi };
                for d0 in 0..10u32 {
                    for d1 in d0..(d0 + 4).min(max) {
                        let valid = (d0..=d1).all(|x| {
                            let (a, b) = map.window(x, max);
                            a >= 0 && a <= b && b <= i64::from(max)
                        });
                        if !valid {
                            continue;
                        }
                        for q0 in 0..=max {
                            for q1 in q0..(q0 + 3).min(max + 1) {
                                let got: Vec<u32> = map
                                    .invert(d0, d1, q0, q1, max)
                                    .map(|(a, b)| (a..=b).collect())
                                    .unwrap_or_default();
                                let want: Vec<u32> = (d0..=d1)
                                    .filter(|&x| {
                                        let (a, b) = map.window(x, max);
                                        a <= i64::from(q1) && b >= i64::from(q0)
                                    })
                                    .collect();
                                assert_eq!(got, want, "invert {map:?} [{d0},{d1}] q [{q0},{q1}]");
                            }
                        }
                        let mut want: Vec<u32> = (d0..=d1)
                            .flat_map(|x| {
                                let (a, b) = map.window(x, max);
                                (a as u32)..=(b as u32)
                            })
                            .collect();
                        want.sort_unstable();
                        want.dedup();
                        let got: Vec<u32> = map
                            .forward(d0, d1, max)
                            .map(|(a, b)| (a..=b).collect())
                            .unwrap_or_default();
                        assert_eq!(got, want, "forward {map:?} [{d0},{d1}]");
                    }
                }
            }
        }
    }
}
