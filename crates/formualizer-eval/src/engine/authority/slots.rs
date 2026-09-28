//! Literal slot rows keyed by `Vid` (design §4.2, SP-3 F1).
//!
//! A formula's literal values, in pre-order, are per-id data: the family
//! template holds the anchor member's literals, and each member's row holds
//! its own. Rows exist for every formula cell whose template has at least
//! one literal slot, singletons included, so a node split, merge or
//! repartition never writes or drops a row (addendum B-3). A row is written
//! when the cell gets its formula and dropped when the id retires.
//!
//! Layout: one page per 64 consecutive ids. A page keeps a presence mask,
//! per-row `(bit, len, start)` metadata sorted by bit and the packed
//! `ValueRef` payloads. Insert and remove shift at most 64 rows.
//!
//! **Sparse and reclaimable (M1a correction B3):** pages live in a slab
//! with a free list, found through an ordered page directory (`id >> 6` →
//! slot); a page whose last row leaves is freed with its payload in the
//! same scope. Ids are allocated monotonically (decision 9), so a store
//! that keeps re-creating one formula touches ever-new pages; the retained
//! bytes are those of the live pages plus the slab and directory slack of
//! the peak live page count, not the high-water id.
//!
//! **Planning (correction B1):** the dry run collects per-page deltas as
//! runs of equal pages, sorts the distinct pages once and visits each
//! touched page once: `O(rows + pages log pages)`, with no search over the
//! touched list per row and no pass over untouched pages. Every allocation
//! is exact, so the plan predicts the bytes after the apply exactly, and
//! the new pages are allocated before the apply (`try_reserve`).

use super::avl::{AvlMap, ReserveError, grown};
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
    /// Presence mask; the next free slot when the page is free.
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

const NO_PAGE: u32 = u32::MAX;

/// `Clone` recounts the maintained payload bytes.
#[derive(Debug)]
pub struct SlotStore {
    pages: Vec<SlotPage>,
    free: u32,
    nfree: usize,
    /// Page number (`id >> 6`) → slab slot.
    dir: AvlMap,
    rows: usize,
    /// Σ payload bytes of the pages in the slab (maintained).
    payload: usize,
    /// New pages allocated by `try_reserve`: `(page number, page)`, sorted.
    staged: Vec<(u32, SlotPage)>,
}

impl Clone for SlotStore {
    fn clone(&self) -> Self {
        let pages: Vec<SlotPage> = self.pages.clone();
        let payload = pages.iter().map(SlotPage::heap_bytes).sum();
        Self {
            pages,
            free: self.free,
            nfree: self.nfree,
            dir: self.dir.clone(),
            rows: self.rows,
            payload,
            staged: self.staged.clone(),
        }
    }
}

impl Default for SlotStore {
    fn default() -> Self {
        Self {
            pages: Vec::new(),
            free: NO_PAGE,
            nfree: 0,
            dir: AvlMap::new(),
            rows: 0,
            payload: 0,
            staged: Vec::new(),
        }
    }
}

/// Dry run of one slot change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotPlan {
    /// Touched pages, sorted: `(page number, rows after, values after)`.
    pub touched: Vec<(u32, usize, usize)>,
    pub new_pages: usize,
    pub freed_pages: usize,
    /// Heap bytes of the store after the apply.
    pub bytes_after: usize,
    /// Bytes allocated before the apply (growth of existing containers and
    /// the new pages).
    pub alloc_bytes: usize,
    /// Largest old buffer of a container grown in place (it coexists with
    /// the new one during the reallocation).
    pub max_realloc_old: usize,
    /// Planning visits: rows, delta runs and touched pages (B1 counter).
    pub work: u64,
    /// Change in stored rows.
    pub rows_delta: i64,
    /// Capacity of the planning delta list (scratch).
    pub deltas_cap: usize,
    slab_cap: usize,
    dir_cap: usize,
}

impl SlotPlan {
    /// Heap of the plan itself and its planning scratch.
    pub fn scratch_bytes(&self) -> usize {
        self.touched.capacity() * size_of::<(u32, usize, usize)>()
            + self.deltas_cap * size_of::<(u32, i64, i64)>()
    }
}

fn tpush<T>(v: &mut Vec<T>, x: T) -> Result<(), ReserveError> {
    if v.len() == v.capacity() {
        v.try_reserve(1).map_err(|_| ReserveError)?;
    }
    v.push(x);
    Ok(())
}

fn try_vec<T>(n: usize) -> Result<Vec<T>, ReserveError> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| ReserveError)?;
    Ok(v)
}

impl SlotStore {
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Retained heap bytes (O(1): the payload total is maintained).
    pub fn heap_bytes(&self) -> usize {
        self.pages.capacity() * size_of::<SlotPage>()
            + self.dir.heap_bytes()
            + self.payload
            + self.staged_bytes()
    }

    /// [`Self::heap_bytes`] by a walk over every page (accounting check).
    pub fn census_bytes(&self) -> usize {
        self.pages.capacity() * size_of::<SlotPage>()
            + self.dir.heap_bytes()
            + self.pages.iter().map(SlotPage::heap_bytes).sum::<usize>()
            + self.staged_bytes()
    }

    /// Live pages.
    pub fn pages(&self) -> usize {
        self.dir.len()
    }

    fn staged_bytes(&self) -> usize {
        self.staged.capacity() * size_of::<(u32, SlotPage)>()
            + self
                .staged
                .iter()
                .map(|(_, p)| p.heap_bytes())
                .sum::<usize>()
    }

    /// Free pages allocated for a reservation that will not be applied.
    pub fn drop_staged(&mut self) {
        self.staged = Vec::new();
    }

    fn slot_of_page(&self, page: u32) -> Option<u32> {
        self.dir.get(u64::from(page))
    }

    /// [`Self::get`] with the last page's slot cached in `cache` (runs of
    /// consecutive ids resolve their page once per 64 ids).
    pub fn get_cached(&self, id: Vid, cache: &mut Option<(u32, u32)>) -> Option<&[ValueRef]> {
        let number = id >> 6;
        let slot = match *cache {
            Some((p, slot)) if p == number => slot,
            _ => {
                let slot = self.slot_of_page(number)?;
                *cache = Some((number, slot));
                slot
            }
        };
        let page = &self.pages[slot as usize];
        let bit = (id & 63) as u8;
        if page.mask >> bit & 1 == 0 {
            return None;
        }
        let m = page.meta[page.find(bit).ok()?];
        Some(&page.vals[m.start as usize..m.start as usize + m.len as usize])
    }

    pub fn get(&self, id: Vid) -> Option<&[ValueRef]> {
        let page = &self.pages[self.slot_of_page(id >> 6)? as usize];
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
        self.try_plan(removes, inserts)
            .expect("slot plan allocation")
    }

    /// [`Self::plan`], fallible.
    pub fn try_plan(
        &self,
        removes: &[Vid],
        inserts: &[(Vid, usize)],
    ) -> Result<SlotPlan, ReserveError> {
        let mut work = 0u64;
        // Deltas as runs of equal pages: consecutive ids share a page, so a
        // run of ids costs one entry; one directory lookup per run.
        let mut deltas: Vec<(u32, i64, i64)> = Vec::new();
        let mut cached: Option<(u32, Option<u32>)> = None;
        for &id in removes {
            work += 1;
            let pg = id >> 6;
            let slot = match cached {
                Some((p, s)) if p == pg => s,
                _ => {
                    let s = self.slot_of_page(pg);
                    cached = Some((pg, s));
                    s
                }
            };
            let Some(slot) = slot else { continue };
            let page = &self.pages[slot as usize];
            let bit = (id & 63) as u8;
            if page.mask >> bit & 1 == 0 {
                continue;
            }
            let len = i64::from(page.meta[page.find(bit).expect("present row")].len);
            match deltas.last_mut() {
                Some(d) if d.0 == pg => {
                    d.1 -= 1;
                    d.2 -= len;
                }
                _ => tpush(&mut deltas, (pg, -1, -len))?,
            }
        }
        for &(id, n) in inserts {
            work += 1;
            if n == 0 {
                continue;
            }
            let pg = id >> 6;
            match deltas.last_mut() {
                Some(d) if d.0 == pg => {
                    d.1 += 1;
                    d.2 += n as i64;
                }
                _ => tpush(&mut deltas, (pg, 1, n as i64))?,
            }
        }
        work += deltas.len() as u64;
        deltas.sort_unstable_by_key(|d| d.0);
        let deltas_cap = deltas.capacity();
        let mut touched: Vec<(u32, usize, usize)> = Vec::new();
        let (mut new_pages, mut freed_pages) = (0usize, 0usize);
        let mut payload = self.payload;
        let mut alloc = 0usize;
        let mut max_old = 0usize;
        let rows_delta: i64 = deltas.iter().map(|d| d.1).sum();
        let mut i = 0;
        while i < deltas.len() {
            let pg = deltas[i].0;
            let (mut dm, mut dv) = (0i64, 0i64);
            while i < deltas.len() && deltas[i].0 == pg {
                dm += deltas[i].1;
                dv += deltas[i].2;
                i += 1;
            }
            work += 1;
            match self.slot_of_page(pg) {
                Some(slot) => {
                    let p = &self.pages[slot as usize];
                    let m = (p.meta.len() as i64 + dm) as usize;
                    let v = (p.vals.len() as i64 + dv) as usize;
                    payload -= p.heap_bytes();
                    if m == 0 {
                        freed_pages += 1;
                    } else {
                        let (mc, vc) = (grown(p.meta.capacity(), m), grown(p.vals.capacity(), v));
                        let grown_bytes = mc * size_of::<RowMeta>() + vc * size_of::<ValueRef>();
                        payload += grown_bytes;
                        if mc > p.meta.capacity() {
                            alloc += (mc - p.meta.capacity()) * size_of::<RowMeta>();
                            max_old = max_old.max(p.meta.capacity() * size_of::<RowMeta>());
                        }
                        if vc > p.vals.capacity() {
                            alloc += (vc - p.vals.capacity()) * size_of::<ValueRef>();
                            max_old = max_old.max(p.vals.capacity() * size_of::<ValueRef>());
                        }
                    }
                    tpush(&mut touched, (pg, m, v))?;
                }
                None => {
                    debug_assert!(dm > 0, "rows removed from an absent page");
                    let (m, v) = (dm as usize, dv as usize);
                    new_pages += 1;
                    let b = m * size_of::<RowMeta>() + v * size_of::<ValueRef>();
                    payload += b;
                    alloc += b;
                    tpush(&mut touched, (pg, m, v))?;
                }
            }
        }
        drop(deltas);
        // Slab: new pages reuse free slots first, then push; freed pages
        // join the free list after.
        let len = self.pages.len() + new_pages.saturating_sub(self.nfree);
        let slab_cap = grown(self.pages.capacity(), len);
        if slab_cap > self.pages.capacity() {
            alloc += (slab_cap - self.pages.capacity()) * size_of::<SlotPage>();
            max_old = max_old.max(self.pages.capacity() * size_of::<SlotPage>());
        }
        // Directory: inserts of new pages, then removals of freed ones.
        let mut dsh = self.dir.shadow();
        for _ in 0..new_pages {
            dsh.alloc();
        }
        for _ in 0..freed_pages {
            dsh.release();
        }
        let dir_cap = dsh.cap;
        if dir_cap > self.dir.shadow().cap {
            alloc += (dir_cap - self.dir.shadow().cap) * AvlMap::NODE_BYTES;
            max_old = max_old.max(self.dir.heap_bytes());
        }
        if new_pages > 0 {
            alloc += new_pages * size_of::<(u32, SlotPage)>();
        }
        Ok(SlotPlan {
            touched,
            new_pages,
            freed_pages,
            bytes_after: slab_cap * size_of::<SlotPage>() + dir_cap * AvlMap::NODE_BYTES + payload,
            alloc_bytes: alloc,
            max_realloc_old: max_old,
            work,
            rows_delta,
            deltas_cap,
            slab_cap,
            dir_cap,
        })
    }

    /// Heap bytes the store will have after `plan` is applied.
    pub fn planned_bytes(&self, plan: &SlotPlan) -> usize {
        plan.bytes_after
    }

    /// Grow every container to the plan's final sizes and allocate the new
    /// pages. Fallible; on error no row changes (grown capacity stays, and
    /// is counted; call `drop_staged` to free allocated pages).
    pub fn try_reserve(&mut self, plan: &SlotPlan) -> Result<(), ReserveError> {
        self.drop_staged();
        if plan.slab_cap > self.pages.capacity() {
            self.pages
                .try_reserve_exact(plan.slab_cap - self.pages.len())
                .map_err(|_| ReserveError)?;
        }
        self.dir.try_reserve_slots(plan.dir_cap)?;
        let mut staged: Vec<(u32, SlotPage)> = try_vec(plan.new_pages)?;
        for &(pg, m, v) in &plan.touched {
            match self.slot_of_page(pg) {
                Some(slot) if m > 0 => {
                    let p = &mut self.pages[slot as usize];
                    let before = p.heap_bytes();
                    let (mc, vc) = (grown(p.meta.capacity(), m), grown(p.vals.capacity(), v));
                    let mut r = Ok(());
                    if mc > p.meta.capacity() {
                        r = p.meta.try_reserve_exact(mc - p.meta.len());
                    }
                    if r.is_ok() && vc > p.vals.capacity() {
                        r = p.vals.try_reserve_exact(vc - p.vals.len());
                    }
                    self.payload += p.heap_bytes() - before;
                    r.map_err(|_| ReserveError)?;
                }
                Some(_) => {}
                None => staged.push((
                    pg,
                    SlotPage {
                        mask: 0,
                        meta: try_vec(m)?,
                        vals: try_vec(v)?,
                    },
                )),
            }
        }
        self.staged = staged;
        Ok(())
    }

    /// Apply a plan's removals and insertions (the same lists as `plan`).
    /// Allocates nothing: new pages come from `try_reserve`.
    pub fn apply(&mut self, plan: &SlotPlan, removes: &[Vid], inserts: &[(Vid, &[ValueRef])]) {
        for &id in removes {
            self.remove(id);
        }
        for &(id, row) in inserts {
            if !row.is_empty() {
                self.insert(id, row);
            }
        }
        for &(pg, m, _) in &plan.touched {
            if m == 0
                && let Some(slot) = self.slot_of_page(pg)
            {
                self.free_page(pg, slot);
            }
        }
        debug_assert!(
            self.staged.iter().all(|(_, p)| p.heap_bytes() == 0),
            "unused staged pages"
        );
        self.staged = Vec::new();
    }

    fn page_for_insert(&mut self, pg: u32) -> u32 {
        if let Some(slot) = self.slot_of_page(pg) {
            return slot;
        }
        // Taken in place (the list stays sorted); freed after the apply.
        let page = match self.staged.binary_search_by_key(&pg, |x| x.0) {
            Ok(i) => std::mem::take(&mut self.staged[i].1),
            Err(_) => {
                debug_assert!(false, "unstaged slot page");
                SlotPage::default()
            }
        };
        self.payload += page.heap_bytes();
        let slot = if self.free != NO_PAGE {
            let slot = self.free;
            self.free = self.pages[slot as usize].mask as u32;
            self.nfree -= 1;
            self.pages[slot as usize] = page;
            slot
        } else {
            debug_assert!(
                self.pages.len() < self.pages.capacity(),
                "unreserved page slab"
            );
            self.pages.push(page);
            (self.pages.len() - 1) as u32
        };
        self.dir.insert(u64::from(pg), slot);
        slot
    }

    fn free_page(&mut self, pg: u32, slot: u32) {
        let p = &mut self.pages[slot as usize];
        debug_assert_eq!(p.mask, 0, "freeing a page with rows");
        self.payload -= p.heap_bytes();
        *p = SlotPage {
            mask: u64::from(self.free),
            meta: Vec::new(),
            vals: Vec::new(),
        };
        self.free = slot;
        self.nfree += 1;
        self.dir.remove(u64::from(pg));
    }

    fn insert(&mut self, id: Vid, row: &[ValueRef]) {
        assert!(row.len() <= MAX_ARITY, "slot row over {MAX_ARITY} literals");
        let slot = self.page_for_insert(id >> 6);
        let page = &mut self.pages[slot as usize];
        let bit = (id & 63) as u8;
        debug_assert_eq!(page.mask >> bit & 1, 0, "slot row exists");
        debug_assert!(
            page.meta.len() < page.meta.capacity()
                && page.vals.len() + row.len() <= page.vals.capacity(),
            "unreserved slot row"
        );
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
        // Append then rotate into place: no allocation within capacity.
        page.vals.extend_from_slice(row);
        page.vals[start..].rotate_right(n);
        page.mask |= 1 << bit;
        self.rows += 1;
    }

    fn remove(&mut self, id: Vid) {
        let Some(slot) = self.slot_of_page(id >> 6) else {
            return;
        };
        let page = &mut self.pages[slot as usize];
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
