//! Deterministic RNG helpers for functions
use rand::{SeedableRng, rngs::SmallRng};

/// Stable 64-bit FNV-1a hash (deterministic across platforms)
#[inline]
pub fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001B3;
    let mut hash = OFFSET;
    for b in data {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[inline]
pub fn mix64(a: u64, b: u64) -> u64 {
    // Simple reversible mix (xorshift-based)
    let mut x = a ^ (b.rotate_left(17));
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

/// Compose a deterministic 128-bit seed from components.
/// Returns two u64 lanes suitable to seed SmallRng.
pub fn compose_seed(
    workbook_seed: u64,
    sheet_id: u32,
    row: u32,
    col: u32,
    fn_salt: u64,
    recalc_epoch: u64,
) -> (u64, u64) {
    let pos = ((sheet_id as u64) << 40) ^ ((row as u64) << 20) ^ (col as u64);
    // Mix epoch into both lanes to guarantee effect on the first outputs
    let lane0 = mix64(workbook_seed ^ fn_salt ^ recalc_epoch, pos);
    let lane1 = mix64(recalc_epoch ^ 0xA5A5_A5A5_5A5A_5A5A_u64, pos.rotate_left(7));
    (lane0, lane1)
}

/// Build a SmallRng from the two seed lanes
pub fn small_rng_from_lanes(l0: u64, l1: u64) -> SmallRng {
    // Use a portable seed derivation regardless of SmallRng's internal Seed size
    let s = mix64(l0, l1);
    SmallRng::seed_from_u64(s)
}
