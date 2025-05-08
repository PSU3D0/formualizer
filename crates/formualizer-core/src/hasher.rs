use std::hash::Hasher;
/// FNV-1a 64-bit hasher implementation
/// This is a dependency-free implementation of the FNV-1a hash algorithm
pub struct FnvHasher {
    hash: u64,
}

const FNV_PRIME: u64 = 1099511628211;
const FNV_OFFSET_BASIS: u64 = 14695981039346656037;

impl FnvHasher {
    pub fn new() -> Self {
        FnvHasher {
            hash: FNV_OFFSET_BASIS,
        }
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.hash ^= byte as u64;
            self.hash = self.hash.wrapping_mul(FNV_PRIME);
        }
    }
}

pub type FormulaHasher = FnvHasher;
