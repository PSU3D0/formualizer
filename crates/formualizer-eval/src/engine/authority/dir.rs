//! Append-only interning directories with exactly predictable capacity.
//!
//! The authority's key directories (edge-group keys, node-group keys, L
//! keys, lookup-slot keys) only ever grow, so the hash table never holds
//! tombstones and its growth is a pure function of `(len, capacity, new
//! keys)`. [`hash_capacity_after`] mirrors hashbrown's sizing rules (the
//! std `HashMap` implementation), and `try_reserve` is always called with
//! the full count of new keys before a mutation inserts them, so predicted
//! and actual capacity agree (checked by `directory_capacity_is_predicted`).
//! Dead entries are dropped by a compaction (B-23) or a rebuild. A mutation stages the owned
//! copies of its new keys before its apply (`stage`), so interning in the
//! apply allocates nothing (M1a correction B5).

use super::avl::{ReserveError, grown};
use rustc_hash::FxHashMap;
use std::hash::Hash;

/// hashbrown `capacity_to_buckets`.
fn capacity_to_buckets(cap: usize) -> usize {
    if cap < 8 {
        return if cap < 4 { 4 } else { 8 };
    }
    let adjusted = cap.checked_mul(8).expect("capacity overflow") / 7;
    adjusted.next_power_of_two()
}

/// hashbrown `bucket_mask_to_capacity`.
fn buckets_to_capacity(buckets: usize) -> usize {
    if buckets < 8 {
        buckets - 1
    } else {
        buckets / 8 * 7
    }
}

/// Capacity after `try_reserve(additional)` on a tombstone-free table with
/// `len` items and `capacity`.
pub fn hash_capacity_after(len: usize, capacity: usize, additional: usize) -> usize {
    if additional == 0 || len + additional <= capacity {
        return capacity;
    }
    let want = (len + additional).max(capacity + 1);
    buckets_to_capacity(capacity_to_buckets(want))
}

/// Heap bytes of a hashbrown table of entry type `T` at `capacity`
/// (data rounded up to the 16-byte control alignment, plus control bytes).
pub fn hash_table_bytes<T>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = capacity_to_buckets(capacity);
    let data = (buckets * size_of::<T>()).div_ceil(16) * 16;
    data + buckets + 16
}

/// An interning directory: key → dense `u32` id, append-only.
#[derive(Clone, Debug)]
pub struct Directory<K: Eq + Hash + Clone> {
    map: FxHashMap<K, u32>,
    keys: Vec<K>,
    /// Extra owned bytes per key (e.g. boxed token streams), summed.
    key_heap: usize,
    /// Owned copies of pending new keys `(map copy, id-table copy, heap)`.
    staged: Vec<(K, K, usize)>,
}

impl<K: Eq + Hash + Clone> Default for Directory<K> {
    fn default() -> Self {
        Self {
            map: FxHashMap::default(),
            keys: Vec::new(),
            key_heap: 0,
            staged: Vec::new(),
        }
    }
}

/// Planned growth of one directory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DirPlan {
    pub new_keys: usize,
    pub new_key_heap: usize,
}

impl<K: Eq + Hash + Clone> Directory<K> {
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn get(&self, k: &K) -> Option<u32> {
        self.map.get(k).copied()
    }

    pub fn key(&self, id: u32) -> &K {
        &self.keys[id as usize]
    }

    pub fn heap_bytes(&self) -> usize {
        hash_table_bytes::<(K, u32)>(self.map.capacity())
            + self.keys.capacity() * size_of::<K>()
            + 2 * self.key_heap
            + self.staged.capacity() * size_of::<(K, K, usize)>()
            + self.staged.iter().map(|x| 2 * x.2).sum::<usize>()
    }

    /// `[(old, new)]` bytes of the hash table, the key vector and the owned
    /// key heap after a plan.
    pub fn plan_parts(&self, plan: DirPlan) -> [(usize, usize); 3] {
        let cap = hash_capacity_after(self.map.len(), self.map.capacity(), plan.new_keys);
        let keys_cap = grown(self.keys.capacity(), self.keys.len() + plan.new_keys);
        [
            (
                hash_table_bytes::<(K, u32)>(self.map.capacity()),
                hash_table_bytes::<(K, u32)>(cap),
            ),
            (
                self.keys.capacity() * size_of::<K>(),
                keys_cap * size_of::<K>(),
            ),
            (2 * self.key_heap, 2 * (self.key_heap + plan.new_key_heap)),
        ]
    }

    /// Bytes of a stage list for `n` keys (the owned copies are counted by
    /// `plan_parts`).
    pub fn stage_list_bytes(n: usize) -> usize {
        n * size_of::<(K, K, usize)>()
    }

    /// Hold owned copies of the pending new keys for `intern_staged`.
    pub fn stage(&mut self, staged: Vec<(K, K, usize)>) {
        self.staged = staged;
    }

    pub fn drop_staged(&mut self) {
        self.staged = Vec::new();
    }

    /// Intern `k` from the staged copies (reserved beforehand): allocates
    /// nothing.
    pub fn intern_staged(&mut self, k: &K) -> u32 {
        if let Some(id) = self.map.get(k) {
            return *id;
        }
        let i = self
            .staged
            .iter()
            .position(|x| x.0 == *k)
            .expect("staged directory key");
        let (a, b, heap) = self.staged.swap_remove(i);
        if self.staged.is_empty() {
            self.staged = Vec::new();
        }
        let id = self.keys.len() as u32;
        debug_assert!(self.keys.len() < self.keys.capacity(), "unreserved key");
        self.keys.push(b);
        self.map.insert(a, id);
        self.key_heap += heap;
        id
    }

    /// Bytes after inserting a plan's new keys.
    pub fn planned_bytes(&self, plan: DirPlan) -> usize {
        let cap = hash_capacity_after(self.map.len(), self.map.capacity(), plan.new_keys);
        let keys_cap = grown(self.keys.capacity(), self.keys.len() + plan.new_keys);
        hash_table_bytes::<(K, u32)>(cap)
            + keys_cap * size_of::<K>()
            + 2 * (self.key_heap + plan.new_key_heap)
    }

    pub fn try_reserve(&mut self, plan: DirPlan) -> Result<(), ReserveError> {
        if plan.new_keys == 0 {
            return Ok(());
        }
        self.map
            .try_reserve(plan.new_keys)
            .map_err(|_| ReserveError)?;
        let cap = grown(self.keys.capacity(), self.keys.len() + plan.new_keys);
        if cap > self.keys.capacity() {
            self.keys
                .try_reserve_exact(cap - self.keys.len())
                .map_err(|_| ReserveError)?;
        }
        Ok(())
    }

    /// Intern `k` (reserved beforehand). `heap` is the key's owned bytes;
    /// they are stored twice (map key and id table).
    pub fn intern(&mut self, k: K, heap: usize) -> u32 {
        if let Some(id) = self.map.get(&k) {
            return *id;
        }
        let id = self.keys.len() as u32;
        self.keys.push(k.clone());
        self.map.insert(k, id);
        self.key_heap += heap;
        id
    }
}

/// Sizes of a compacted directory holding `live` keys with `live_heap`
/// owned key bytes (M1a parent fix B-23).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactPlan {
    pub live: usize,
    pub live_heap: usize,
    pub map_cap: usize,
}

impl<K: Eq + Hash + Clone> Directory<K> {
    pub fn compact_plan(&self, live: usize, live_heap: usize) -> CompactPlan {
        CompactPlan {
            live,
            live_heap,
            map_cap: hash_capacity_after(0, 0, live),
        }
    }

    /// Retained bytes after the compaction (the fresh map and key vector
    /// are allocated at exactly `live`).
    pub fn compact_bytes(plan: &CompactPlan) -> usize {
        hash_table_bytes::<(K, u32)>(plan.map_cap) + plan.live * size_of::<K>() + 2 * plan.live_heap
    }

    /// Bytes the compaction newly allocates: the fresh table and key
    /// vector (the owned key copies are moved, not reallocated).
    pub fn compact_alloc_bytes(plan: &CompactPlan) -> usize {
        hash_table_bytes::<(K, u32)>(plan.map_cap) + plan.live * size_of::<K>()
    }

    /// Allocate the compacted containers (fallible; nothing changes).
    pub fn try_stage_compact(
        &self,
        plan: &CompactPlan,
    ) -> Result<(FxHashMap<K, u32>, Vec<K>), ReserveError> {
        let mut map = FxHashMap::default();
        map.try_reserve(plan.live).map_err(|_| ReserveError)?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(plan.live)
            .map_err(|_| ReserveError)?;
        Ok((map, keys))
    }

    /// Keep the keys whose `remap` entry is not `dead`, renumbered to it
    /// (live keys are renumbered in ascending old-id order), moving both
    /// owned copies into the staged containers. Allocates nothing; frees
    /// the old containers and the dead keys.
    pub fn apply_compact(
        &mut self,
        remap: &[u32],
        dead: u32,
        staged: (FxHashMap<K, u32>, Vec<K>),
        plan: &CompactPlan,
    ) {
        let old_map = std::mem::replace(&mut self.map, staged.0);
        let old_keys = std::mem::replace(&mut self.keys, staged.1);
        for (id, k) in old_keys.into_iter().enumerate() {
            if remap[id] != dead {
                debug_assert_eq!(remap[id] as usize, self.keys.len());
                self.keys.push(k);
            }
        }
        for (k, id) in old_map {
            let n = remap[id as usize];
            if n != dead {
                self.map.insert(k, n);
            }
        }
        debug_assert_eq!(self.map.capacity(), plan.map_cap, "compaction grew");
        self.key_heap = plan.live_heap;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_capacity_is_predicted() {
        let mut d: Directory<(u16, u64)> = Directory::default();
        let mut x = 1u64;
        for batch in 0..400usize {
            let n = batch % 7;
            let keys: Vec<(u16, u64)> = (0..n)
                .map(|_| {
                    x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    ((x >> 60) as u16, x)
                })
                .collect();
            let plan = DirPlan {
                new_keys: keys.len(),
                new_key_heap: 0,
            };
            let predicted = d.planned_bytes(plan);
            let cap = hash_capacity_after(d.map.len(), d.map.capacity(), n);
            d.try_reserve(plan).unwrap();
            assert_eq!(d.map.capacity(), cap, "batch {batch}");
            for k in keys {
                d.intern(k, 0);
            }
            assert_eq!(d.map.capacity(), cap, "insert grew past the reservation");
            assert_eq!(d.heap_bytes(), predicted);
        }
    }
}
