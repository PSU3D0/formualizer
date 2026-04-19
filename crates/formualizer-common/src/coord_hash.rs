//! Dedicated hasher for keys built around [`crate::Coord`] /
//! workbook cell addresses.
//!
//! Background
//! ----------
//!
//! `Coord` packs a 20-bit row and 14-bit column (plus small flag bits) into a
//! single `u64`, with bits `0..10` and bits `44..64` reserved/zero. On
//! row-major workloads the dynamic range of the packed value is narrow and
//! concentrated in the middle of the word.
//!
//! Combined with `FxHasher`'s weak avalanche (a single multiply), this causes
//! severe clustering in `FxHashMap<CellRef, _>` and
//! `FxHashMap<(SheetId, Coord), _>` keys: bulk-ingest phases that should be
//! O(N) exhibit O(N^2) behavior. See
//! `/tmp/issue-63-investigation/REPORT_PERF.md` for the full analysis and
//! micro-benchmark.
//!
//! This module provides [`CoordBuildHasher`] and [`CoordHasher`]: a
//! xor-rotate-multiply-fold finalizer that is strong enough to scatter the
//! structured packed-int keys across the table while still costing only a
//! handful of integer ops. It is *not* a general-purpose hasher — it is
//! tailored for keys dominated by `u64`/`u32`/`u16` fields, which is exactly
//! what the hot maps in the engine use.

use core::hash::{BuildHasher, Hasher};

/// Golden-ratio-derived odd multiplier (same class as the wyhash /
/// rapidhash finalizer constants).
const K: u64 = 0x9E37_79B9_7F4A_7C15;

/// Zero-sized `BuildHasher` that constructs [`CoordHasher`] instances.
///
/// Intended for use with `std::collections::HashMap` /
/// `std::collections::HashSet` on keys composed of `Coord`, `CellRef`,
/// `SheetId`, or tuples thereof.
#[derive(Default, Clone, Copy, Debug)]
pub struct CoordBuildHasher;

impl BuildHasher for CoordBuildHasher {
    type Hasher = CoordHasher;

    #[inline]
    fn build_hasher(&self) -> CoordHasher {
        CoordHasher(0)
    }
}

/// State for one hash computation. See module docs for the rationale.
#[derive(Clone, Copy, Debug)]
pub struct CoordHasher(u64);

impl Hasher for CoordHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    /// Byte-wise fallback for keys that don't go through the typed
    /// `write_uN` paths (e.g. `String` components of named-range keys).
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut state = self.0;
        for &b in bytes {
            state = (state ^ b as u64).wrapping_mul(K);
        }
        self.0 = state;
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        // xor-rotate-multiply-fold avalanche.
        let x = (self.0 ^ n).rotate_left(27).wrapping_mul(K);
        self.0 = x ^ (x >> 32);
    }

    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.write_u64(n as u64);
    }

    #[inline]
    fn write_u16(&mut self, n: u16) {
        self.write_u64(n as u64);
    }

    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.write_u64(n as u64);
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }

    #[inline]
    fn write_i64(&mut self, n: i64) {
        self.write_u64(n as u64);
    }

    #[inline]
    fn write_i32(&mut self, n: i32) {
        self.write_u64(n as u64);
    }
}

/// Convenience aliases mirroring the `FxHashMap` / `FxHashSet` shape used
/// throughout the engine.
pub type CoordHashMap<K, V> = std::collections::HashMap<K, V, CoordBuildHasher>;
pub type CoordHashSet<K> = std::collections::HashSet<K, CoordBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Coord;
    use std::collections::HashMap;

    #[test]
    fn basic_insert_lookup() {
        let mut m: CoordHashMap<Coord, u32> = HashMap::default();
        for r in 0..1000u32 {
            m.insert(Coord::new(r, r % 64), r);
        }
        for r in 0..1000u32 {
            assert_eq!(m.get(&Coord::new(r, r % 64)), Some(&r));
        }
        assert_eq!(m.len(), 1000);
    }

    #[test]
    fn distinct_coords_produce_distinct_hashes() {
        use core::hash::Hash;
        fn h(c: Coord) -> u64 {
            let mut hasher = CoordBuildHasher.build_hasher();
            c.hash(&mut hasher);
            hasher.finish()
        }
        // Adjacent row-major coords must not collide (this is exactly the
        // degenerate pattern FxHasher stumbles on).
        let a = h(Coord::new(0, 0));
        let b = h(Coord::new(0, 1));
        let c = h(Coord::new(1, 0));
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }
}
