//! Literal slot rows keyed by `Vid` (design §4.2, SP-3 F1).
//!
//! A formula's literal values, in pre-order, are per-id data: the family
//! template holds the anchor member's literals, and each member's row holds
//! its own. Rows exist for every formula cell whose template has at least
//! one literal slot, singletons included, so a node split, merge or
//! repartition never writes or drops a row (addendum B-3). A row is written
//! when the cell gets its formula and dropped when the id retires.
//!
//! Layout: one page per 64 consecutive ids, `pages[id >> 6]`. A page keeps
//! a presence mask, per-row `(bit, len, start)` metadata sorted by bit and
//! the packed `ValueRef` payloads. Lookup is O(1); insert and remove shift
//! at most 64 rows. Every allocation is exact, so the dry run predicts the
//! capacity from the touched pages alone.

use super::avl::{ReserveError, grown};
use super::identity::Vid;
use crate::engine::arena::ValueRef;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowMeta {
    bit: u8,
    len: u8,
    start: u16,
}

#[derive(Clone, Debug, Default)]
struct SlotPage {
    mask: u64,
    meta: Vec<RowMeta>,
    vals: Vec<ValueRef>,
}

impl SlotPage {
    fn heap_bytes(&self) -> usize {
        self.meta.capacity() * size_of::<RowMeta>() + self.vals.capacity() * size_of::<ValueRef>()
    }

    fn find(&self, bit: u8) -> Result<usize, usize> {
        self.meta.binary_search_by_key(&bit, |m| m.bit)
    }
}

/// Maximum literals per formula held in a row (`u8` length).
pub const MAX_ARITY: usize = 255;

#[derive(Clone, Debug, Default)]
pub struct SlotStore {
    pages: Vec<SlotPage>,
    rows: usize,
}

/// Dry-run delta for one page: `(page, rows after, values after)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotPlan {
    pub pages_len: usize,
    pub pages_cap: usize,
    /// Touched pages: `(page index, meta len after, vals len after)`.
    pub touched: Vec<(usize, usize, usize)>,
}

impl SlotStore {
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn heap_bytes(&self) -> usize {
        self.pages.capacity() * size_of::<SlotPage>()
            + self.pages.iter().map(SlotPage::heap_bytes).sum::<usize>()
    }

    pub fn get(&self, id: Vid) -> Option<&[ValueRef]> {
        let page = self.pages.get((id >> 6) as usize)?;
        let bit = (id & 63) as u8;
        if page.mask >> bit & 1 == 0 {
            return None;
        }
        let m = page.meta[page.find(bit).ok()?];
        Some(&page.vals[m.start as usize..m.start as usize + m.len as usize])
    }

    /// Bytes after applying `removes` then `inserts` (`(id, arity)`), with
    /// every touched container grown exactly. Read-only.
    pub fn plan(&self, removes: &[Vid], inserts: &[(Vid, usize)]) -> SlotPlan {
        let mut pages_len = self.pages.len();
        for &(id, n) in inserts {
            if n > 0 {
                pages_len = pages_len.max((id >> 6) as usize + 1);
            }
        }
        let mut touched: Vec<(usize, usize, usize)> = Vec::new();
        let state = |p: usize, touched: &mut Vec<(usize, usize, usize)>| -> usize {
            if let Some(i) = touched.iter().position(|t| t.0 == p) {
                return i;
            }
            let (m, v) = self
                .pages
                .get(p)
                .map_or((0, 0), |pg| (pg.meta.len(), pg.vals.len()));
            touched.push((p, m, v));
            touched.len() - 1
        };
        for &id in removes {
            if let Some(row) = self.get(id) {
                let i = state((id >> 6) as usize, &mut touched);
                touched[i].1 -= 1;
                touched[i].2 -= row.len();
            }
        }
        for &(id, n) in inserts {
            if n > 0 {
                let i = state((id >> 6) as usize, &mut touched);
                touched[i].1 += 1;
                touched[i].2 += n;
            }
        }
        SlotPlan {
            pages_len,
            pages_cap: grown(self.pages.capacity(), pages_len),
            touched,
        }
    }

    /// Heap bytes the store will have after `plan` is applied.
    pub fn planned_bytes(&self, plan: &SlotPlan) -> usize {
        let mut bytes = plan.pages_cap * size_of::<SlotPage>();
        for (i, p) in self.pages.iter().enumerate() {
            match plan.touched.iter().find(|t| t.0 == i) {
                Some(&(_, m, v)) => {
                    bytes += grown(p.meta.capacity(), m) * size_of::<RowMeta>()
                        + grown(p.vals.capacity(), v) * size_of::<ValueRef>();
                }
                None => bytes += p.heap_bytes(),
            }
        }
        for &(i, m, v) in &plan.touched {
            if i >= self.pages.len() {
                bytes += grown(0, m) * size_of::<RowMeta>() + grown(0, v) * size_of::<ValueRef>();
            }
        }
        bytes
    }

    /// Grow every container to the plan's final sizes. Fallible, no other
    /// change on error.
    pub fn try_reserve(&mut self, plan: &SlotPlan) -> Result<(), ReserveError> {
        if plan.pages_cap > self.pages.capacity() {
            self.pages
                .try_reserve_exact(plan.pages_cap - self.pages.len())
                .map_err(|_| ReserveError)?;
        }
        for &(i, m, v) in &plan.touched {
            if i < self.pages.len() {
                let p = &mut self.pages[i];
                let (mc, vc) = (grown(p.meta.capacity(), m), grown(p.vals.capacity(), v));
                if mc > p.meta.capacity() {
                    p.meta
                        .try_reserve_exact(mc - p.meta.len())
                        .map_err(|_| ReserveError)?;
                }
                if vc > p.vals.capacity() {
                    p.vals
                        .try_reserve_exact(vc - p.vals.len())
                        .map_err(|_| ReserveError)?;
                }
            }
        }
        // New pages are created (exactly sized) by `apply`; their
        // allocation is part of the admitted bytes.
        Ok(())
    }

    /// Apply a plan's removals and insertions (the same lists as `plan`).
    pub fn apply(&mut self, plan: &SlotPlan, removes: &[Vid], inserts: &[(Vid, &[ValueRef])]) {
        if self.pages.len() < plan.pages_len {
            self.pages.resize_with(plan.pages_len, SlotPage::default);
        }
        for &(i, m, v) in &plan.touched {
            let p = &mut self.pages[i];
            let (mc, vc) = (grown(p.meta.capacity(), m), grown(p.vals.capacity(), v));
            if mc > p.meta.capacity() {
                p.meta.reserve_exact(mc - p.meta.len());
            }
            if vc > p.vals.capacity() {
                p.vals.reserve_exact(vc - p.vals.len());
            }
        }
        for &id in removes {
            self.remove(id);
        }
        for &(id, row) in inserts {
            if !row.is_empty() {
                self.insert(id, row);
            }
        }
    }

    fn insert(&mut self, id: Vid, row: &[ValueRef]) {
        debug_assert!(row.len() <= MAX_ARITY);
        let page = &mut self.pages[(id >> 6) as usize];
        let bit = (id & 63) as u8;
        debug_assert_eq!(page.mask >> bit & 1, 0, "slot row exists");
        let at = page.find(bit).expect_err("absent row");
        let start = page
            .meta
            .get(at)
            .map_or(page.vals.len(), |m| m.start as usize);
        let n = row.len();
        for m in &mut page.meta[at..] {
            m.start += n as u16;
        }
        page.meta.insert(
            at,
            RowMeta {
                bit,
                len: n as u8,
                start: start as u16,
            },
        );
        page.vals.splice(start..start, row.iter().copied());
        page.mask |= 1 << bit;
        self.rows += 1;
    }

    fn remove(&mut self, id: Vid) {
        let Some(page) = self.pages.get_mut((id >> 6) as usize) else {
            return;
        };
        let bit = (id & 63) as u8;
        if page.mask >> bit & 1 == 0 {
            return;
        }
        let at = page.find(bit).expect("present row");
        let m = page.meta.remove(at);
        let (s, n) = (m.start as usize, m.len as usize);
        page.vals.drain(s..s + n);
        for m in &mut page.meta[at..] {
            m.start -= n as u16;
        }
        page.mask &= !(1 << bit);
        self.rows -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_round_trip_and_plan_predicts_bytes() {
        let mut s = SlotStore::default();
        let v = ValueRef::from_raw;
        let mut model: std::collections::BTreeMap<u32, Vec<ValueRef>> = Default::default();
        let mut x: u32 = 5;
        for step in 0..3000u32 {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let id = (x >> 8) % 300;
            let (removes, inserts): (Vec<u32>, Vec<(u32, Vec<ValueRef>)>) =
                if model.contains_key(&id) {
                    if step % 2 == 0 {
                        (vec![id], vec![])
                    } else {
                        let n = (x >> 24) as usize % 4;
                        (
                            vec![id],
                            vec![(id, (0..n as u32).map(|k| v(step + k)).collect())],
                        )
                    }
                } else {
                    let n = (x >> 24) as usize % 4;
                    (
                        vec![],
                        vec![(id, (0..n as u32).map(|k| v(step * 7 + k)).collect())],
                    )
                };
            let ins: Vec<(u32, usize)> = inserts.iter().map(|(i, r)| (*i, r.len())).collect();
            let plan = s.plan(&removes, &ins);
            let predicted = s.planned_bytes(&plan);
            s.try_reserve(&plan).unwrap();
            let rows: Vec<(u32, &[ValueRef])> =
                inserts.iter().map(|(i, r)| (*i, r.as_slice())).collect();
            s.apply(&plan, &removes, &rows);
            assert_eq!(s.heap_bytes(), predicted, "step {step}");
            for id in &removes {
                model.remove(id);
            }
            for (i, r) in &inserts {
                if !r.is_empty() {
                    model.insert(*i, r.clone());
                }
            }
            if step % 37 == 0 {
                for id in 0..300u32 {
                    assert_eq!(s.get(id), model.get(&id).map(Vec::as_slice), "id {id}");
                }
            }
        }
        assert_eq!(s.rows(), model.len());
    }
}
