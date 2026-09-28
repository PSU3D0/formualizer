//! A set of vertex ids as a bitmap (Program 3): the dirty set and a pass's
//! committed set hold up to every formula of a workbook, and their per-id
//! insert/remove/contains are hot paths of every evaluation. Iteration is
//! in ascending id order over the words between the lowest and highest id
//! inserted since the set was last empty.

use crate::engine::VertexId;

#[derive(Debug, Default, Clone)]
pub(crate) struct DenseIdSet {
    words: Vec<u64>,
    len: usize,
    /// Word range that may hold set bits (`lo > hi` when empty).
    lo: usize,
    hi: usize,
}

impl DenseIdSet {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub(crate) fn contains(&self, id: &VertexId) -> bool {
        let (w, b) = (id.0 as usize >> 6, id.0 & 63);
        self.words.get(w).is_some_and(|word| word >> b & 1 == 1)
    }

    /// Inserts `id`; whether it was new.
    #[inline]
    pub(crate) fn insert(&mut self, id: VertexId) -> bool {
        let (w, b) = (id.0 as usize >> 6, id.0 & 63);
        if w >= self.words.len() {
            self.words.resize((w + 1).next_power_of_two().max(16), 0);
        }
        let word = &mut self.words[w];
        if *word >> b & 1 == 1 {
            return false;
        }
        *word |= 1 << b;
        if self.len == 0 {
            self.lo = w;
            self.hi = w;
        } else {
            self.lo = self.lo.min(w);
            self.hi = self.hi.max(w);
        }
        self.len += 1;
        true
    }

    /// Removes `id`; whether it was present.
    #[inline]
    pub(crate) fn remove(&mut self, id: &VertexId) -> bool {
        let (w, b) = (id.0 as usize >> 6, id.0 & 63);
        let Some(word) = self.words.get_mut(w) else {
            return false;
        };
        if *word >> b & 1 == 0 {
            return false;
        }
        *word &= !(1 << b);
        self.len -= 1;
        true
    }

    pub(crate) fn extend(&mut self, ids: impl IntoIterator<Item = VertexId>) {
        for id in ids {
            self.insert(id);
        }
    }

    /// Removes every id (the words stay allocated).
    pub(crate) fn clear(&mut self) {
        if self.len != 0 {
            self.words[self.lo..=self.hi].fill(0);
        }
        self.len = 0;
    }

    /// Ascending ids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = VertexId> + '_ {
        let words = if self.len == 0 {
            &self.words[0..0]
        } else {
            &self.words[self.lo..=self.hi]
        };
        let base = self.lo;
        words.iter().enumerate().flat_map(move |(i, &word)| {
            let mut w = word;
            std::iter::from_fn(move || {
                if w == 0 {
                    return None;
                }
                let b = w.trailing_zeros();
                w &= w - 1;
                Some(VertexId((((base + i) as u32) << 6) | b))
            })
        })
    }

    /// Heap bytes.
    pub(crate) fn heap_bytes(&self) -> usize {
        self.words.capacity() * 8
    }

    /// Frees the words when the set is empty and they are large.
    pub(crate) fn shrink_if_empty(&mut self) {
        if self.len == 0 && self.words.len() > 1 << 16 {
            self.words = Vec::new();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_id_set_matches_a_hash_set() {
        let mut set = DenseIdSet::default();
        let mut oracle = std::collections::BTreeSet::new();
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for step in 0..20_000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let id = VertexId((x % 5_000) as u32 + if step % 3 == 0 { 70_000 } else { 0 });
            match x % 5 {
                0..=2 => assert_eq!(set.insert(id), oracle.insert(id)),
                3 => assert_eq!(set.remove(&id), oracle.remove(&id)),
                _ => assert_eq!(set.contains(&id), oracle.contains(&id)),
            }
            if step % 4_999 == 0 {
                set.clear();
                oracle.clear();
            }
            assert_eq!(set.len(), oracle.len());
        }
        assert!(set.iter().eq(oracle.iter().copied()));
        set.clear();
        assert!(set.is_empty() && set.iter().next().is_none());
    }
}
