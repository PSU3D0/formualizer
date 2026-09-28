//! Store-level test support: synthetic formula facts and an independent,
//! cell-expanding relation model (final-review §5 item 5: SP-2's oracle
//! shared its normalization with the store; this one uses plain hash sets
//! of cells and never calls the store's projection, cover or index code).

use super::super::geom::{Cell, Rect};
use super::super::proj::{AxisMap, Bound, RefProj};
use super::super::store::{BuildInput, EdgeSpec, FormulaFacts, OriginSpec, Store, Tag, TagFilter};
use crate::engine::arena::{AstNodeId, ValueRef};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::smallvec;

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, n: u32) -> u32 {
        (self.next() % u64::from(n.max(1))) as u32
    }
    pub fn chance(&mut self, pct: u32) -> bool {
        self.below(100) < pct
    }
}

/// A synthetic formula: its references (relative to the placement) and a
/// template id (the L key) with one literal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Formula {
    pub refs: Vec<RefProj>,
    pub l: u64,
    pub literal: u32,
}

impl Formula {
    pub fn facts(&self) -> FormulaFacts {
        let mut edges: Vec<EdgeSpec> = self
            .refs
            .iter()
            .map(|p| EdgeSpec {
                proj: *p,
                tag: Tag::R1,
                origin: OriginSpec::Text,
            })
            .collect();
        edges.sort_unstable();
        edges.dedup();
        // L: the template id plus the relative references (two formulas
        // share L only if they are the same relative template).
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in format!("{:?}", self.refs).bytes() {
            h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
        FormulaFacts {
            edges,
            ltokens: Some(vec![self.l, h].into_boxed_slice()),
            template: AstNodeId::from_u32(self.l as u32),
            template_anchor: None,
            literals: smallvec![ValueRef::from_raw(self.literal)],
            flags: 0,
        }
    }
}

pub fn rel(dr: i32, dc: i32, sheet: u16) -> RefProj {
    RefProj {
        sheet,
        rows: AxisMap::point(Bound::Rel(dr)),
        cols: AxisMap::point(Bound::Rel(dc)),
    }
}

pub fn abs(r: u32, c: u32, sheet: u16) -> RefProj {
    RefProj {
        sheet,
        rows: AxisMap::point(Bound::Abs(r)),
        cols: AxisMap::point(Bound::Abs(c)),
    }
}

/// `SUM` of a relative window `r+a .. r+b` in column `col` (absolute).
pub fn window(a: i32, b: i32, col: u32, sheet: u16) -> RefProj {
    RefProj {
        sheet,
        rows: AxisMap {
            lo: Bound::Rel(a),
            hi: Bound::Rel(b),
        },
        cols: AxisMap::point(Bound::Abs(col)),
    }
}

/// Whole column `col`.
pub fn column(col: u32, sheet: u16) -> RefProj {
    RefProj {
        sheet,
        rows: AxisMap {
            lo: Bound::Open,
            hi: Bound::Open,
        },
        cols: AxisMap::point(Bound::Abs(col)),
    }
}

/// Independent model: cell → formula.
#[derive(Clone, Default)]
pub struct Model {
    pub cells: FxHashMap<Cell, Formula>,
}

fn window_of(p: &RefProj, cell: Cell) -> Option<(u16, i64, i64, i64, i64)> {
    let f = |b: Bound, x: u32, open: i64| match b {
        Bound::Rel(k) => i64::from(x) + i64::from(k),
        Bound::Abs(v) => i64::from(v),
        Bound::Open => open,
    };
    let (r0, r1) = (f(p.rows.lo, cell.1, 0), f(p.rows.hi, cell.1, 1_048_575));
    let (c0, c1) = (f(p.cols.lo, cell.2, 0), f(p.cols.hi, cell.2, 16_383));
    (r0 >= 0 && c0 >= 0 && r0 <= r1 && c0 <= c1).then_some((p.sheet, r0, c0, r1, c1))
}

impl Model {
    /// Whether `f` placed at `cell` instantiates on the grid (the engine
    /// only installs such formulas).
    pub fn valid(f: &Formula, cell: Cell) -> bool {
        f.refs
            .iter()
            .all(|p| window_of(p, cell).is_some_and(|w| w.3 <= 1_048_575 && w.4 <= 16_383))
    }

    pub fn direct_dependents(&self, q: Cell) -> Vec<Cell> {
        let mut v: Vec<Cell> = self
            .cells
            .iter()
            .filter(|(c, f)| {
                f.refs.iter().any(|p| {
                    window_of(p, **c).is_some_and(|(s, r0, c0, r1, c1)| {
                        s == q.0
                            && r0 <= i64::from(q.1)
                            && i64::from(q.1) <= r1
                            && c0 <= i64::from(q.2)
                            && i64::from(q.2) <= c1
                    })
                })
            })
            .map(|(c, _)| *c)
            .collect();
        v.sort_unstable();
        v
    }

    pub fn dependents(&self, seed: Cell) -> Vec<Cell> {
        let mut seen: FxHashSet<Cell> = FxHashSet::default();
        let mut queue = vec![seed];
        while let Some(c) = queue.pop() {
            for d in self.direct_dependents(c) {
                if seen.insert(d) {
                    queue.push(d);
                }
            }
        }
        let mut v: Vec<Cell> = seen.into_iter().collect();
        v.sort_unstable();
        v
    }
}

pub fn store_direct(s: &Store, q: Cell) -> Vec<Cell> {
    let mut hits = Vec::new();
    s.direct_dependents(q.0, &Rect::cell(q.1, q.2), TagFilter::All, &mut hits);
    let mut set: FxHashSet<Cell> = FxHashSet::default();
    for (sh, r) in hits {
        for c in r.c0..=r.c1 {
            for row in r.r0..=r.r1 {
                set.insert((sh, row, c));
            }
        }
    }
    let mut v: Vec<Cell> = set.into_iter().collect();
    v.sort_unstable();
    v
}

pub fn store_closure(s: &Store, q: Cell) -> Vec<Cell> {
    let (cover, _) = s.dependents(&[(q.0, Rect::cell(q.1, q.2))], TagFilter::All);
    cover.cells()
}

/// A formula menu over a `rows × cols` grid on two sheets: families share
/// L keys; literals vary to exercise slot rows.
pub fn random_formula(g: &mut Rng, rows: u32) -> Formula {
    let l = u64::from(g.below(6));
    let refs = match l {
        0 => vec![rel(0, -1, 0)],
        1 => vec![rel(-1, 0, 0), rel(0, -2, 0)],
        2 => vec![window(-2, 0, 0, 0)],
        3 => vec![abs(g.below(rows), 0, 0), rel(0, -1, 0)],
        4 => vec![column(1, 0)],
        _ => vec![rel(0, 0, 1)],
    };
    Formula {
        refs,
        l,
        literal: if g.chance(30) { g.below(4) } else { 1 },
    }
}

/// A fresh build from a model.
pub fn build_from(m: &Model) -> Store {
    let input: Vec<BuildInput> = m.cells.iter().map(|(c, f)| (*c, f.facts())).collect();
    Store::build(input)
}
