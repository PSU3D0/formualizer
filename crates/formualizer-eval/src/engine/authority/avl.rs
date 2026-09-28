//! An ordered map `u64 → u32` in an arena slab (AVL balanced).
//!
//! The identity table's forward and reverse run directories (design §4.1)
//! need predecessor search, insertion and deletion with bounded height, and
//! — for exact admission (§5.1) — a container whose capacity is a known
//! function of its operation sequence. `std::collections::BTreeMap` gives
//! the first but not the second, so nodes live in one `Vec` with a free
//! list: capacity changes only when the free list is empty, and then by
//! exactly the reserved amount. Height is at most `1.44 log2(n + 2)`.

const NIL: u32 = u32::MAX;

/// Capacity after growing a container of capacity `cap` to hold `need`
/// elements: unchanged if it fits, else at least 1.5× (amortized O(1)
/// appends; growing by exactly the missing amount would copy the whole
/// container on every growing edit). Deterministic, so a dry run predicts
/// it exactly.
pub fn grown(cap: usize, need: usize) -> usize {
    if need <= cap {
        cap
    } else {
        need.max(cap + cap / 2)
    }
}

/// An exact reservation could not be made (allocation failure).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReserveError;

#[derive(Clone, Copy, Debug)]
struct Node {
    key: u64,
    val: u32,
    left: u32,
    right: u32,
    height: u8,
}

#[derive(Clone, Debug)]
pub struct AvlMap {
    nodes: Vec<Node>,
    root: u32,
    free: u32,
    nfree: usize,
    len: usize,
}

impl Default for AvlMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Counter-only replica of an arena's slot use, for dry runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlabShadow {
    /// Slots ever pushed (`Vec::len`).
    pub slots: usize,
    /// Free slots.
    pub free: usize,
    /// `Vec::capacity`.
    pub cap: usize,
}

impl SlabShadow {
    pub fn alloc(&mut self) {
        if self.free > 0 {
            self.free -= 1;
        } else {
            self.slots += 1;
            self.cap = grown(self.cap, self.slots);
        }
    }

    pub fn release(&mut self) {
        self.free += 1;
    }

    /// Slots to reserve before an operation sequence that ends in `self`,
    /// starting from `start`.
    pub fn growth_from(&self, start: &SlabShadow) -> usize {
        self.slots.saturating_sub(start.cap)
    }
}

impl AvlMap {
    pub const NODE_BYTES: usize = size_of::<Node>();

    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            root: NIL,
            free: NIL,
            nfree: 0,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn heap_bytes(&self) -> usize {
        self.nodes.capacity() * Self::NODE_BYTES
    }

    pub fn shadow(&self) -> SlabShadow {
        SlabShadow {
            slots: self.nodes.len(),
            free: self.nfree,
            cap: self.nodes.capacity(),
        }
    }

    /// Grow the node slab to capacity `cap` (a shadow's predicted
    /// capacity) so the pending inserts never reallocate.
    pub fn try_reserve_slots(&mut self, cap: usize) -> Result<(), ReserveError> {
        if cap > self.nodes.capacity() {
            self.nodes
                .try_reserve_exact(cap - self.nodes.len())
                .map_err(|_| ReserveError)?;
        }
        Ok(())
    }

    fn h(&self, n: u32) -> i32 {
        if n == NIL {
            0
        } else {
            i32::from(self.nodes[n as usize].height)
        }
    }

    fn fix(&mut self, n: u32) {
        let h = 1 + self
            .h(self.nodes[n as usize].left)
            .max(self.h(self.nodes[n as usize].right));
        self.nodes[n as usize].height = h as u8;
    }

    fn rot_right(&mut self, n: u32) -> u32 {
        let l = self.nodes[n as usize].left;
        self.nodes[n as usize].left = self.nodes[l as usize].right;
        self.nodes[l as usize].right = n;
        self.fix(n);
        self.fix(l);
        l
    }

    fn rot_left(&mut self, n: u32) -> u32 {
        let r = self.nodes[n as usize].right;
        self.nodes[n as usize].right = self.nodes[r as usize].left;
        self.nodes[r as usize].left = n;
        self.fix(n);
        self.fix(r);
        r
    }

    fn balance(&mut self, n: u32) -> u32 {
        self.fix(n);
        let (l, r) = (self.nodes[n as usize].left, self.nodes[n as usize].right);
        let bf = self.h(l) - self.h(r);
        if bf > 1 {
            let ll = self.nodes[l as usize].left;
            let lr = self.nodes[l as usize].right;
            if self.h(ll) < self.h(lr) {
                let nl = self.rot_left(l);
                self.nodes[n as usize].left = nl;
            }
            return self.rot_right(n);
        }
        if bf < -1 {
            let rl = self.nodes[r as usize].left;
            let rr = self.nodes[r as usize].right;
            if self.h(rr) < self.h(rl) {
                let nr = self.rot_right(r);
                self.nodes[n as usize].right = nr;
            }
            return self.rot_left(n);
        }
        n
    }

    fn alloc(&mut self, key: u64, val: u32) -> u32 {
        let node = Node {
            key,
            val,
            left: NIL,
            right: NIL,
            height: 1,
        };
        if self.free != NIL {
            let id = self.free;
            self.free = self.nodes[id as usize].left;
            self.nfree -= 1;
            self.nodes[id as usize] = node;
            id
        } else {
            debug_assert!(
                self.nodes.len() < self.nodes.capacity(),
                "AvlMap grew without a reservation"
            );
            self.nodes.push(node);
            (self.nodes.len() - 1) as u32
        }
    }

    fn release(&mut self, id: u32) {
        self.nodes[id as usize].left = self.free;
        self.free = id;
        self.nfree += 1;
    }

    /// Insert a new key. Panics in debug builds if it exists.
    pub fn insert(&mut self, key: u64, val: u32) {
        if self.nodes.len() == self.nodes.capacity() && self.free == NIL {
            // Unreserved growth (callers that account capacity reserve
            // beforehand): the same rule the shadow applies.
            let cap = grown(self.nodes.capacity(), self.nodes.len() + 1);
            self.nodes.reserve_exact(cap - self.nodes.len());
        }
        let root = self.root;
        self.root = self.insert_at(root, key, val);
        self.len += 1;
    }

    fn insert_at(&mut self, n: u32, key: u64, val: u32) -> u32 {
        if n == NIL {
            return self.alloc(key, val);
        }
        let k = self.nodes[n as usize].key;
        debug_assert_ne!(k, key, "duplicate AvlMap key");
        if key < k {
            let l = self.nodes[n as usize].left;
            let nl = self.insert_at(l, key, val);
            self.nodes[n as usize].left = nl;
        } else {
            let r = self.nodes[n as usize].right;
            let nr = self.insert_at(r, key, val);
            self.nodes[n as usize].right = nr;
        }
        self.balance(n)
    }

    /// Remove `key`, returning its value.
    pub fn remove(&mut self, key: u64) -> Option<u32> {
        let mut out = None;
        let root = self.root;
        self.root = self.remove_at(root, key, &mut out);
        if out.is_some() {
            self.len -= 1;
        }
        out
    }

    fn remove_at(&mut self, n: u32, key: u64, out: &mut Option<u32>) -> u32 {
        if n == NIL {
            return NIL;
        }
        let k = self.nodes[n as usize].key;
        if key < k {
            let l = self.nodes[n as usize].left;
            let nl = self.remove_at(l, key, out);
            self.nodes[n as usize].left = nl;
        } else if key > k {
            let r = self.nodes[n as usize].right;
            let nr = self.remove_at(r, key, out);
            self.nodes[n as usize].right = nr;
        } else {
            *out = Some(self.nodes[n as usize].val);
            let (l, r) = (self.nodes[n as usize].left, self.nodes[n as usize].right);
            if l == NIL || r == NIL {
                self.release(n);
                return if l == NIL { r } else { l };
            }
            // Replace by the successor, detached from the right subtree.
            let (nr, succ) = self.take_min(r);
            self.nodes[succ as usize].right = nr;
            self.nodes[succ as usize].left = l;
            self.release(n);
            return self.balance(succ);
        }
        self.balance(n)
    }

    /// Detach the minimum of subtree `n`; returns (new subtree, min node).
    fn take_min(&mut self, n: u32) -> (u32, u32) {
        let l = self.nodes[n as usize].left;
        if l == NIL {
            return (self.nodes[n as usize].right, n);
        }
        let (nl, m) = self.take_min(l);
        self.nodes[n as usize].left = nl;
        (self.balance(n), m)
    }

    pub fn get(&self, key: u64) -> Option<u32> {
        let mut n = self.root;
        while n != NIL {
            let node = &self.nodes[n as usize];
            if key == node.key {
                return Some(node.val);
            }
            n = if key < node.key {
                node.left
            } else {
                node.right
            };
        }
        None
    }

    /// Replace the value of an existing key.
    pub fn set(&mut self, key: u64, val: u32) -> bool {
        let mut n = self.root;
        while n != NIL {
            let node = &mut self.nodes[n as usize];
            if key == node.key {
                node.val = val;
                return true;
            }
            n = if key < node.key {
                node.left
            } else {
                node.right
            };
        }
        false
    }

    /// Greatest entry with key ≤ `key` (the §4.1 lookup lemma's search).
    pub fn pred(&self, key: u64) -> Option<(u64, u32)> {
        self.pred_counted(key, &mut 0)
    }

    pub(super) fn pred_counted(&self, key: u64, work: &mut u64) -> Option<(u64, u32)> {
        let mut n = self.root;
        let mut best = None;
        while n != NIL {
            *work += 1;
            let node = &self.nodes[n as usize];
            if node.key <= key {
                best = Some((node.key, node.val));
                n = node.right;
            } else {
                n = node.left;
            }
        }
        best
    }

    /// Smallest entry with key ≥ `key`.
    pub fn succ(&self, key: u64) -> Option<(u64, u32)> {
        let mut n = self.root;
        let mut best = None;
        while n != NIL {
            let node = &self.nodes[n as usize];
            if node.key >= key {
                best = Some((node.key, node.val));
                n = node.left;
            } else {
                n = node.right;
            }
        }
        best
    }

    /// Entries with `lo ≤ key ≤ hi`, in key order.
    pub fn range(&self, lo: u64, hi: u64, out: &mut Vec<(u64, u32)>) {
        self.range_visit(lo, hi, &mut |k, v| out.push((k, v)));
    }

    /// Visit the entries with `lo ≤ key ≤ hi` in key order, without
    /// allocating (the stack is a fixed array: the height is at most
    /// `1.44 log2(n + 2) < 64` for `n < 2^32`).
    pub fn range_visit(&self, lo: u64, hi: u64, visit: &mut dyn FnMut(u64, u32)) {
        self.range_visit_counted(lo, hi, &mut 0, visit);
    }

    pub(super) fn range_visit_counted(
        &self,
        lo: u64,
        hi: u64,
        work: &mut u64,
        visit: &mut dyn FnMut(u64, u32),
    ) {
        let mut stack = [NIL; 64];
        let mut top = 0usize;
        let mut n = self.root;
        loop {
            while n != NIL {
                *work += 1;
                let node = &self.nodes[n as usize];
                if node.key < lo {
                    n = node.right;
                } else {
                    stack[top] = n;
                    top += 1;
                    n = node.left;
                }
            }
            if top == 0 {
                break;
            }
            top -= 1;
            *work += 1;
            let node = &self.nodes[stack[top] as usize];
            if node.key > hi {
                break;
            }
            visit(node.key, node.val);
            n = node.right;
        }
    }

    /// Height of the tree (tests).
    pub fn height(&self) -> u32 {
        self.h(self.root) as u32
    }

    /// Structural check (tests): order, heights, balance, length.
    pub fn check(&self) -> Result<(), String> {
        fn walk(
            m: &AvlMap,
            n: u32,
            lo: Option<u64>,
            hi: Option<u64>,
        ) -> Result<(i32, usize), String> {
            if n == NIL {
                return Ok((0, 0));
            }
            let node = &m.nodes[n as usize];
            if lo.is_some_and(|l| node.key <= l) || hi.is_some_and(|h| node.key >= h) {
                return Err(format!("order violated at key {}", node.key));
            }
            let (hl, cl) = walk(m, node.left, lo, Some(node.key))?;
            let (hr, cr) = walk(m, node.right, Some(node.key), hi)?;
            if (hl - hr).abs() > 1 {
                return Err(format!("unbalanced at key {}", node.key));
            }
            let h = 1 + hl.max(hr);
            if h != i32::from(node.height) {
                return Err(format!("stale height at key {}", node.key));
            }
            Ok((h, cl + cr + 1))
        }
        let (_, n) = walk(self, self.root, None, None)?;
        if n != self.len {
            return Err(format!("len {} but {} nodes reachable", self.len, n));
        }
        if self.nodes.len() != self.len + self.nfree {
            return Err("slot accounting".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn avl_matches_btreemap() {
        let mut m = AvlMap::new();
        let mut b: BTreeMap<u64, u32> = BTreeMap::new();
        let mut x: u64 = 99;
        for step in 0..20_000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = x % 3000;
            if x % 5 < 2 {
                assert_eq!(m.remove(k), b.remove(&k), "step {step}");
            } else if let std::collections::btree_map::Entry::Vacant(v) = b.entry(k) {
                m.insert(k, step);
                v.insert(step);
            }
            if step % 101 == 0 {
                m.check().unwrap();
                let q = (x >> 20) % 3100;
                assert_eq!(m.pred(q), b.range(..=q).next_back().map(|(k, v)| (*k, *v)));
                assert_eq!(m.succ(q), b.range(q..).next().map(|(k, v)| (*k, *v)));
                let mut got = Vec::new();
                m.range(q, q + 50, &mut got);
                let want: Vec<(u64, u32)> = b.range(q..=q + 50).map(|(k, v)| (*k, *v)).collect();
                assert_eq!(got, want);
            }
        }
        assert!(m.height() <= 2 * 12 + 2);
    }

    #[test]
    fn slab_shadow_predicts_capacity() {
        let mut m = AvlMap::new();
        for k in 0..100u64 {
            m.insert(k, 0);
        }
        for k in 0..40u64 {
            m.remove(k);
        }
        let start = m.shadow();
        let mut sh = start;
        for _ in 0..10 {
            sh.release();
        }
        for _ in 0..90 {
            sh.alloc();
        }
        m.try_reserve_slots(sh.cap).unwrap();
        for k in 40..50u64 {
            m.remove(k);
        }
        for k in 1000..1090u64 {
            m.insert(k, 1);
        }
        assert_eq!(m.shadow(), sh);
    }
}
