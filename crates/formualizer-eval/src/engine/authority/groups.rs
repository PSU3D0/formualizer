//! Group tables: every edge group or node group is one entry holding its
//! (packed) key and its bookkeeping, found through a hash index.
//!
//! Irregular workbooks have about one group per formula, so a group's fixed
//! cost is what their memory is made of. An entry is one vector slot
//! (key + member list with two inline slots + `B_g`, `C_g`, flags) and one
//! 4-byte slot of an open-addressing index; the key is stored once.
//!
//! **Reclamation (M1a correction B3):** a group whose last member leaves is
//! reclaimed in the same mutation: its spilled member list is freed, its
//! entry goes to a free list for reuse and its index slot becomes a
//! tombstone. The index never reuses tombstones on insert, so its state is
//! a pure function of `(capacity, live, tombstones)` and the operation
//! counts: an insert that would fill more than 7/8 of the slots rebuilds to
//! [`target`] of the live count (tombstones dropped); a removal that leaves
//! fewer than 1/8 live rebuilds down. Both rebuilds are amortized O(1)
//! (at least `cap/2` inserts or `cap/16` removals separate two of them),
//! and the capacity stays within `8·live` (or the 8-slot minimum), so a
//! table's bytes track its live groups, not its history. The dry run
//! replays the counts exactly ([`GroupTable::plan`]); every rebuild array
//! is allocated before the apply ([`GroupTable::try_reserve_ops`]).

use super::avl::{ReserveError, grown};
use rustc_hash::FxHasher;
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};

/// Member list with two inline slots (most irregular groups have one
/// member, so they allocate nothing).
pub type Members = SmallVec<[u32; 2]>;

/// Heap bytes of a member list.
pub fn members_heap(v: &Members) -> usize {
    if v.spilled() {
        v.capacity() * size_of::<u32>()
    } else {
        0
    }
}

/// Member-list capacity after growing to hold `fin` members.
pub fn members_cap_after(v: &Members, fin: usize) -> usize {
    grown(v.capacity(), fin)
}

/// Heap bytes of a member list of capacity `cap`.
pub fn members_heap_for(cap: usize) -> usize {
    if cap > 2 { cap * size_of::<u32>() } else { 0 }
}

/// Group bookkeeping, 32 bytes: the member list, `B_g` (low 30 bits of
/// `b_flags`, with the `suspended` and `kept` flags in the top two bits)
/// and `C_g`. A reclaimed entry has `b_flags == FREE` and links the free
/// list through `c`.
#[derive(Clone, Debug, Default)]
pub struct Group {
    pub members: Members,
    b_flags: u32,
    /// Pieces created since the last repartition (`C_g`).
    pub c: u32,
}

const SUSPENDED: u32 = 1 << 31;
const KEPT: u32 = 1 << 30;
const B_MASK: u32 = KEPT - 1;
/// `b_flags` of a reclaimed entry (unreachable for a live group: `B_g` is
/// clamped below `B_MASK`).
const FREE: u32 = u32::MAX;
const NO_ENTRY: u32 = u32::MAX;

impl Group {
    /// |canon(X_g)| at the last repartition (or the build).
    pub fn b(&self) -> u32 {
        self.b_flags & B_MASK
    }

    pub fn set_b(&mut self, b: usize) {
        self.b_flags = (self.b_flags & !B_MASK) | (b.min(B_MASK as usize - 1) as u32);
    }

    /// A repartition was skipped for lack of scratch or budget; the memory
    /// statement is suspended until the next one succeeds.
    pub fn suspended(&self) -> bool {
        self.b_flags & SUSPENDED != 0
    }

    pub fn set_suspended(&mut self, on: bool) {
        if on {
            self.b_flags |= SUSPENDED;
        } else {
            self.b_flags &= !SUSPENDED;
        }
    }

    /// The last repartition kept the current pieces because canon would
    /// have grown the live model bytes (byte-safe acceptance); `L_g ≤
    /// 2·B_g + 2` is not claimed for the group.
    pub fn kept(&self) -> bool {
        self.b_flags & KEPT != 0
    }

    pub fn set_kept(&mut self, on: bool) {
        if on {
            self.b_flags |= KEPT;
        } else {
            self.b_flags &= !KEPT;
        }
    }

    pub fn triggered(&self) -> bool {
        let lim = 2 * u64::from(self.b()) + 2;
        self.members.len() as u64 > lim || u64::from(self.c) > lim
    }

    fn is_free(&self) -> bool {
        self.b_flags == FREE
    }
}

/// A key stored packed in a group table.
pub trait GroupKey: Copy + Eq {
    type Packed: Copy + Eq + Hash + std::fmt::Debug;
    fn pack(&self) -> Self::Packed;
    fn unpack(p: &Self::Packed) -> Self;
}

#[derive(Clone, Debug)]
struct Entry<P> {
    key: P,
    g: Group,
}

const EMPTY: u32 = u32::MAX;
const TOMB: u32 = u32::MAX - 1;
const MIN_SLOTS: usize = 8;

/// Index capacity for `n` live keys after a rebuild: the smallest power of
/// two ≥ 8 with `n ≤ 3/8` of it (0 for no keys).
pub fn target(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let mut c = MIN_SLOTS;
    while n * 8 > c * 3 {
        c *= 2;
    }
    c
}

/// Replay of the index's counts: `(capacity, live, tombstones)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexCounts {
    pub cap: usize,
    pub live: usize,
    pub tombs: usize,
}

impl IndexCounts {
    /// Replay `inserts` insertions then `removes` removals, calling
    /// `rebuild(cap)` for every rebuild in order.
    pub fn replay(
        mut self,
        inserts: usize,
        removes: usize,
        rebuild: &mut dyn FnMut(usize),
    ) -> Self {
        for _ in 0..inserts {
            if self.cap == 0 || (self.live + self.tombs + 1) * 8 > self.cap * 7 {
                self.cap = target(self.live + 1);
                self.tombs = 0;
                rebuild(self.cap);
            }
            self.live += 1;
        }
        for _ in 0..removes {
            self.live -= 1;
            self.tombs += 1;
            if self.cap > MIN_SLOTS && self.live * 8 < self.cap {
                self.cap = target(self.live);
                self.tombs = 0;
                rebuild(self.cap);
            }
        }
        self
    }
}

/// Open-addressing index `slot → entry id` with linear probing and
/// tombstones; keys are compared through the entries.
#[derive(Clone, Debug, Default)]
struct OpenIndex {
    slots: Vec<u32>,
    live: usize,
    tombs: usize,
    /// Rebuild arrays allocated by `try_reserve_ops`, in reverse order of
    /// use.
    staged: Vec<Vec<u32>>,
}

impl OpenIndex {
    fn counts(&self) -> IndexCounts {
        IndexCounts {
            cap: self.slots.len(),
            live: self.live,
            tombs: self.tombs,
        }
    }

    fn bytes(&self) -> usize {
        self.slots.capacity() * size_of::<u32>()
            + self.staged.capacity() * size_of::<Vec<u32>>()
            + self
                .staged
                .iter()
                .map(|v| v.capacity() * size_of::<u32>())
                .sum::<usize>()
    }

    fn fresh(&mut self, cap: usize) -> Vec<u32> {
        let mut v = match self.staged.pop() {
            Some(v) => {
                debug_assert_eq!(v.capacity(), cap, "staged index array out of order");
                if self.staged.is_empty() {
                    self.staged = Vec::new();
                }
                v
            }
            None => Vec::with_capacity(cap),
        };
        v.clear();
        v.resize(cap, EMPTY);
        v
    }
}

/// `Clone` recounts the maintained member bytes (a cloned member list may
/// shrink or move inline).
#[derive(Debug)]
pub struct GroupTable<K: GroupKey> {
    entries: Vec<Entry<K::Packed>>,
    free: u32,
    nfree: usize,
    index: OpenIndex,
    /// Σ heap bytes of spilled member lists (maintained).
    members_bytes: usize,
}

impl<K: GroupKey> Clone for GroupTable<K> {
    fn clone(&self) -> Self {
        let mut t = Self {
            entries: self.entries.clone(),
            free: self.free,
            nfree: self.nfree,
            index: self.index.clone(),
            members_bytes: 0,
        };
        t.recount_members();
        t
    }
}

impl<K: GroupKey> Default for GroupTable<K> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            free: NO_ENTRY,
            nfree: 0,
            index: OpenIndex::default(),
            members_bytes: 0,
        }
    }
}

fn hash_of<P: Hash>(p: &P) -> u64 {
    let mut h = FxHasher::default();
    p.hash(&mut h);
    h.finish()
}

/// 32-bit hash of a key (its home slot is this modulo the capacity).
fn slot_of<P: Hash>(p: &P) -> u32 {
    let h = hash_of(p);
    (h ^ (h >> 32)) as u32
}

/// Planned change of one table: entries, index rebuilds, member bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct GroupPlan {
    pub inserts: usize,
    pub removes: usize,
    pub entries_cap: usize,
    pub index: IndexCounts,
    /// Bytes of the rebuild arrays allocated before the apply.
    pub staged_bytes: usize,
    pub rebuilds: usize,
}

impl<K: GroupKey> GroupTable<K> {
    pub const ENTRY_BYTES: usize = size_of::<Entry<K::Packed>>();

    /// Live groups.
    pub fn len(&self) -> usize {
        self.entries.len() - self.nfree
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    fn home(&self, p: &K::Packed) -> usize {
        slot_of(p) as usize & (self.index.slots.len() - 1)
    }

    pub fn get(&self, k: &K) -> Option<u32> {
        if self.index.slots.is_empty() {
            return None;
        }
        let p = k.pack();
        let mask = self.index.slots.len() - 1;
        let mut i = self.home(&p);
        loop {
            match self.index.slots[i] {
                EMPTY => return None,
                TOMB => {}
                id if self.entries[id as usize].key == p => return Some(id),
                _ => {}
            }
            i = (i + 1) & mask;
        }
    }

    pub fn key(&self, id: u32) -> K {
        K::unpack(&self.entries[id as usize].key)
    }

    /// Live groups `(id, key, group)`.
    pub fn iter(&self) -> impl Iterator<Item = (u32, K, &Group)> + '_ {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, e)| !e.g.is_free())
            .map(|(i, e)| (i as u32, K::unpack(&e.key), &e.g))
    }

    /// Retained bytes: entries, spilled member lists, index (O(1)).
    pub fn heap_bytes(&self) -> usize {
        self.entries.capacity() * Self::ENTRY_BYTES + self.members_bytes + self.index.bytes()
    }

    /// [`Self::heap_bytes`] by a walk over every entry (accounting check).
    pub fn census_bytes(&self) -> usize {
        self.entries.capacity() * Self::ENTRY_BYTES
            + self
                .entries
                .iter()
                .map(|e| members_heap(&e.g.members))
                .sum::<usize>()
            + self.index.bytes()
    }

    /// `(entry-vector bytes, index bytes)` now.
    pub fn fixed_parts(&self) -> (usize, usize) {
        (
            self.entries.capacity() * Self::ENTRY_BYTES,
            self.index.bytes(),
        )
    }

    /// Recompute the maintained member bytes (after a bulk build that set
    /// member lists directly).
    pub fn recount_members(&mut self) {
        self.members_bytes = self
            .entries
            .iter()
            .map(|e| members_heap(&e.g.members))
            .sum();
    }

    /// Plan `inserts` new groups, then `removes` reclaimed groups.
    pub fn plan(&self, inserts: usize, removes: usize) -> GroupPlan {
        // New groups reuse free entries first, then push.
        let len = self.entries.len() + inserts.saturating_sub(self.nfree);
        let mut staged_bytes = 0;
        let mut rebuilds = 0;
        let index = self.index.counts().replay(inserts, removes, &mut |cap| {
            staged_bytes += cap * size_of::<u32>();
            rebuilds += 1;
        });
        if rebuilds > 0 {
            staged_bytes += rebuilds * size_of::<Vec<u32>>();
        }
        GroupPlan {
            inserts,
            removes,
            entries_cap: grown(self.entries.capacity(), len),
            index,
            staged_bytes,
            rebuilds,
        }
    }

    /// Entry-vector and index bytes after the plan (member lists are
    /// accounted by the caller).
    pub fn planned_fixed(&self, plan: &GroupPlan) -> usize {
        plan.entries_cap * Self::ENTRY_BYTES + plan.index.cap * size_of::<u32>()
    }

    /// Bytes of the entry vector and index after `new` insertions
    /// (member lists are accounted separately by the caller).
    pub fn planned_fixed_bytes(&self, new: usize) -> (usize, usize) {
        let old = self.entries.capacity() * Self::ENTRY_BYTES + self.index.bytes();
        (old, self.planned_fixed(&self.plan(new, 0)))
    }

    pub fn entries_cap_after(&self, new: usize) -> usize {
        self.plan(new, 0).entries_cap
    }

    pub fn try_reserve(&mut self, new: usize) -> Result<(), ReserveError> {
        let plan = self.plan(new, 0);
        self.try_reserve_ops(&plan)
    }

    /// Grow the entry vector and allocate every index rebuild array the
    /// plan's inserts and removals need. Fallible; on error only spare
    /// capacity and staged arrays (dropped by the next reservation or
    /// [`Self::drop_staged`]) remain.
    pub fn try_reserve_ops(&mut self, plan: &GroupPlan) -> Result<(), ReserveError> {
        if plan.entries_cap > self.entries.capacity() {
            self.entries
                .try_reserve_exact(plan.entries_cap - self.entries.len())
                .map_err(|_| ReserveError)?;
        }
        self.drop_staged();
        if plan.rebuilds == 0 {
            return Ok(());
        }
        self.index
            .staged
            .try_reserve_exact(plan.rebuilds)
            .map_err(|_| ReserveError)?;
        let mut err = false;
        let mut staged = std::mem::take(&mut self.index.staged);
        self.index
            .counts()
            .replay(plan.inserts, plan.removes, &mut |cap| {
                if err {
                    return;
                }
                let mut v: Vec<u32> = Vec::new();
                if v.try_reserve_exact(cap).is_err() {
                    err = true;
                    return;
                }
                staged.push(v);
            });
        staged.reverse();
        self.index.staged = staged;
        if err {
            self.drop_staged();
            return Err(ReserveError);
        }
        Ok(())
    }

    /// Free the staged rebuild arrays (a failed reservation, or a plan
    /// that did not use them).
    pub fn drop_staged(&mut self) {
        self.index.staged = Vec::new();
    }

    /// Bytes of staged rebuild arrays still held.
    pub fn staged_bytes(&self) -> usize {
        self.index.staged.capacity() * size_of::<Vec<u32>>()
            + self
                .index
                .staged
                .iter()
                .map(|v| v.capacity() * size_of::<u32>())
                .sum::<usize>()
    }

    fn rebuild_index(&mut self, cap: usize) {
        let mut fresh = self.index.fresh(cap);
        let old = std::mem::take(&mut self.index.slots);
        if cap > 0 {
            let mask = cap - 1;
            for &id in &old {
                if id == EMPTY || id == TOMB {
                    continue;
                }
                let mut i = slot_of(&self.entries[id as usize].key) as usize & mask;
                while fresh[i] != EMPTY {
                    i = (i + 1) & mask;
                }
                fresh[i] = id;
            }
        }
        self.index.slots = fresh;
        self.index.tombs = 0;
    }

    /// Insert a new key (reserved beforehand); returns its id.
    pub fn insert(&mut self, k: K, g: Group) -> u32 {
        debug_assert!(self.get(&k).is_none(), "duplicate group key");
        let c = self.index.counts();
        if c.cap == 0 || (c.live + c.tombs + 1) * 8 > c.cap * 7 {
            self.rebuild_index(target(c.live + 1));
        }
        let p = k.pack();
        self.members_bytes += members_heap(&g.members);
        let id = if self.free != NO_ENTRY {
            let id = self.free;
            self.free = self.entries[id as usize].g.c;
            self.nfree -= 1;
            self.entries[id as usize] = Entry { key: p, g };
            id
        } else {
            self.entries.push(Entry { key: p, g });
            (self.entries.len() - 1) as u32
        };
        let mask = self.index.slots.len() - 1;
        let mut i = self.home(&p);
        while self.index.slots[i] != EMPTY {
            i = (i + 1) & mask;
        }
        self.index.slots[i] = id;
        self.index.live += 1;
        id
    }

    /// Reclaim an empty group: free its member list, tombstone its index
    /// slot, put its entry on the free list.
    pub fn reclaim(&mut self, id: u32) {
        let e = &mut self.entries[id as usize];
        debug_assert!(e.g.members.is_empty() && !e.g.is_free());
        self.members_bytes -= members_heap(&e.g.members);
        e.g.members = Members::new();
        let p = e.key;
        let mask = self.index.slots.len() - 1;
        let mut i = slot_of(&p) as usize & mask;
        while self.index.slots[i] != id {
            debug_assert_ne!(self.index.slots[i], EMPTY, "reclaimed key not indexed");
            i = (i + 1) & mask;
        }
        self.index.slots[i] = TOMB;
        self.index.live -= 1;
        self.index.tombs += 1;
        let e = &mut self.entries[id as usize];
        e.g.b_flags = FREE;
        e.g.c = self.free;
        self.free = id;
        self.nfree += 1;
        let c = self.index.counts();
        if c.cap > MIN_SLOTS && c.live * 8 < c.cap {
            self.rebuild_index(target(c.live));
        }
    }

    /// Grow a group's member list to capacity `cap` (fallible).
    pub fn grow_members(&mut self, id: u32, cap: usize) -> Result<(), ReserveError> {
        let m = &mut self.entries[id as usize].g.members;
        if cap > m.capacity() {
            let before = members_heap(m);
            m.try_reserve_exact(cap - m.len())
                .map_err(|_| ReserveError)?;
            self.members_bytes += members_heap(m) - before;
        }
        Ok(())
    }

    /// Entry slots, live and free (the slab's length).
    pub fn slots(&self) -> usize {
        self.entries.len()
    }

    /// Bytes of the index array a [`Self::rekey`] rebuilds into.
    pub fn rekey_stage_bytes(&self) -> usize {
        let cap = self.index.slots.len();
        if cap == 0 {
            0
        } else {
            // The staged list's one slot, plus the array itself.
            size_of::<Vec<u32>>() + cap * size_of::<u32>()
        }
    }

    /// Allocate the index array for a [`Self::rekey`] (fallible).
    pub fn try_stage_rekey(&mut self) -> Result<(), ReserveError> {
        self.drop_staged();
        let cap = self.index.slots.len();
        if cap == 0 {
            return Ok(());
        }
        self.index
            .staged
            .try_reserve_exact(1)
            .map_err(|_| ReserveError)?;
        let mut v: Vec<u32> = Vec::new();
        if v.try_reserve_exact(cap).is_err() {
            self.drop_staged();
            return Err(ReserveError);
        }
        self.index.staged.push(v);
        Ok(())
    }

    /// Rewrite every live entry's packed key (ids are kept) and rebuild the
    /// index at its capacity from the staged array. Allocates nothing after
    /// [`Self::try_stage_rekey`].
    pub fn rekey(&mut self, f: impl Fn(&K::Packed) -> K::Packed) {
        for e in self.entries.iter_mut() {
            if !e.g.is_free() {
                e.key = f(&e.key);
            }
        }
        let cap = self.index.slots.len();
        if cap > 0 {
            self.rebuild_index(cap);
        }
        self.drop_staged();
    }

    /// Shrink the entry vector to its length (bulk build only).
    pub fn shrink_entries(&mut self) {
        self.entries.shrink_to_fit();
    }
}

impl<K: GroupKey> std::ops::Index<usize> for GroupTable<K> {
    type Output = Group;
    fn index(&self, i: usize) -> &Group {
        &self.entries[i].g
    }
}

impl<K: GroupKey> std::ops::IndexMut<usize> for GroupTable<K> {
    fn index_mut(&mut self, i: usize) -> &mut Group {
        &mut self.entries[i].g
    }
}

/// Node-group key: sheet and the 64-bit hash of the L token stream. The
/// engine host verifies every hit against the group's representative
/// template before a formula joins (`Store::l_representative`), so a hash
/// collision only loses sharing.
impl GroupKey for (u16, u64) {
    type Packed = (u16, u64);
    fn pack(&self) -> Self::Packed {
        *self
    }
    fn unpack(p: &Self::Packed) -> Self {
        *p
    }
}

/// The 64-bit L hash of a token stream.
pub fn l_hash(tokens: &[u64]) -> u64 {
    hash_of(&tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    impl GroupKey for u64 {
        type Packed = u64;
        fn pack(&self) -> u64 {
            *self
        }
        fn unpack(p: &u64) -> u64 {
            *p
        }
    }

    #[test]
    fn table_finds_keys_and_predicts_bytes() {
        let mut t: GroupTable<u64> = GroupTable::default();
        let mut x = 7u64;
        let mut keys = Vec::new();
        for batch in 0..300usize {
            let n = batch % 5;
            let (old, new) = t.planned_fixed_bytes(n);
            assert_eq!(old, t.heap_bytes());
            t.try_reserve(n).unwrap();
            for _ in 0..n {
                x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                keys.push(x);
                t.insert(x, Group::default());
            }
            assert_eq!(t.heap_bytes(), new, "batch {batch}");
        }
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(t.get(k), Some(i as u32));
        }
        assert_eq!(t.get(&12345), None);
    }
}
