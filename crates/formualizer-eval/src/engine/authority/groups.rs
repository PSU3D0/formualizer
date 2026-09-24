//! Group tables: every edge group or node group is one entry holding its
//! (packed) key and its bookkeeping, found through a hash index.
//!
//! Irregular workbooks have about one group per formula, so a group's fixed
//! cost is what their memory is made of. An entry is one vector slot
//! (key + member list with two inline slots + `B_g`, `C_g`, flags) and one
//! `u32 → u32` hash-index slot; the key is stored once. Entries are never
//! removed (an emptied group stays until a rebuild), so the hash index has
//! no tombstones and its capacity is a pure function of `(len, capacity,
//! new keys)` (`dir::hash_capacity_after`). Collisions of the 32-bit slot
//! hash are resolved by probing `h, h+1, …` and comparing keys.

use super::avl::{ReserveError, grown};
use super::dir::{hash_capacity_after, hash_table_bytes};
use rustc_hash::{FxHashMap, FxHasher};
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
/// and `C_g`.
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

impl Group {
    /// |canon(X_g)| at the last repartition (or the build).
    pub fn b(&self) -> u32 {
        self.b_flags & B_MASK
    }

    pub fn set_b(&mut self, b: usize) {
        self.b_flags = (self.b_flags & !B_MASK) | (b.min(B_MASK as usize) as u32);
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

#[derive(Clone, Debug)]
pub struct GroupTable<K: GroupKey> {
    entries: Vec<Entry<K::Packed>>,
    index: FxHashMap<u32, u32>,
}

impl<K: GroupKey> Default for GroupTable<K> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            index: FxHashMap::default(),
        }
    }
}

fn hash_of<P: Hash>(p: &P) -> u64 {
    let mut h = FxHasher::default();
    p.hash(&mut h);
    h.finish()
}

/// 32-bit index slot of a key (collisions probe `h, h+1, …`).
fn slot_of<P: Hash>(p: &P) -> u32 {
    let h = hash_of(p);
    (h ^ (h >> 32)) as u32
}

impl<K: GroupKey> GroupTable<K> {
    pub const ENTRY_BYTES: usize = size_of::<Entry<K::Packed>>();

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn get(&self, k: &K) -> Option<u32> {
        let p = k.pack();
        let mut h = slot_of(&p);
        loop {
            let id = *self.index.get(&h)?;
            if self.entries[id as usize].key == p {
                return Some(id);
            }
            h = h.wrapping_add(1);
        }
    }

    pub fn key(&self, id: u32) -> K {
        K::unpack(&self.entries[id as usize].key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, K, &Group)> + '_ {
        self.entries
            .iter()
            .enumerate()
            .map(|(i, e)| (i as u32, K::unpack(&e.key), &e.g))
    }

    /// Retained bytes: entries, spilled member lists, hash index.
    pub fn heap_bytes(&self) -> usize {
        self.entries.capacity() * Self::ENTRY_BYTES
            + self
                .entries
                .iter()
                .map(|e| members_heap(&e.g.members))
                .sum::<usize>()
            + hash_table_bytes::<(u32, u32)>(self.index.capacity())
    }

    /// Bytes of the entry vector and hash index after `new` insertions
    /// (member lists are accounted separately by the caller).
    pub fn planned_fixed_bytes(&self, new: usize) -> (usize, usize) {
        let old = self.entries.capacity() * Self::ENTRY_BYTES
            + hash_table_bytes::<(u32, u32)>(self.index.capacity());
        let cap = grown(self.entries.capacity(), self.entries.len() + new);
        let icap = hash_capacity_after(self.index.len(), self.index.capacity(), new);
        (
            old,
            cap * Self::ENTRY_BYTES + hash_table_bytes::<(u32, u32)>(icap),
        )
    }

    pub fn entries_cap_after(&self, new: usize) -> usize {
        grown(self.entries.capacity(), self.entries.len() + new)
    }

    pub fn try_reserve(&mut self, new: usize) -> Result<(), ReserveError> {
        if new == 0 {
            return Ok(());
        }
        let cap = self.entries_cap_after(new);
        if cap > self.entries.capacity() {
            self.entries
                .try_reserve_exact(cap - self.entries.len())
                .map_err(|_| ReserveError)?;
        }
        self.index.try_reserve(new).map_err(|_| ReserveError)
    }

    /// Insert a new key (reserved beforehand); returns its id.
    pub fn insert(&mut self, k: K, g: Group) -> u32 {
        debug_assert!(self.get(&k).is_none(), "duplicate group key");
        let p = k.pack();
        let id = self.entries.len() as u32;
        let mut h = slot_of(&p);
        while self.index.contains_key(&h) {
            h = h.wrapping_add(1);
        }
        self.entries.push(Entry { key: p, g });
        self.index.insert(h, id);
        id
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
