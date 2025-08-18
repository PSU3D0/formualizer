//! Test different iteration strategies based on mask density

#[cfg(test)]
mod tests {
    use crate::engine::masks::DenseMask;

    #[test]
    fn test_sparse_mask_iterates_set_bits() {
        // Sparse mask (< 10% density) should iterate only set bits
        let mut mask = DenseMask::new(10000);

        // Set only 100 bits (1% density)
        for i in (0..10000).step_by(100) {
            mask.set(i, true);
        }

        assert_eq!(mask.count_ones(), 100);
        assert!(mask.density() < 0.02);

        // Iteration should visit exactly 100 positions
        let positions: Vec<u32> = mask.iter_ones().collect();
        assert_eq!(positions.len(), 100);

        // Verify correct positions
        for (idx, &pos) in positions.iter().enumerate() {
            assert_eq!(pos, (idx * 100) as u32);
        }
    }

    #[test]
    fn test_dense_mask_scans_linearly() {
        // Dense mask (> 50% density) should scan linearly
        let mut mask = DenseMask::new(1000);

        // Set 800 bits (80% density)
        for i in 0..800 {
            mask.set(i, true);
        }

        assert_eq!(mask.count_ones(), 800);
        assert!(mask.density() > 0.75);

        // Linear scan should be efficient for dense masks
        let mut count = 0;
        for i in 0..1000 {
            if mask.get(i) {
                count += 1;
            }
        }
        assert_eq!(count, 800);
    }

    #[test]
    fn test_mask_density_transition() {
        // Test mask transitioning from sparse to dense
        let mut mask = DenseMask::new(1000);

        // Start sparse
        for i in 0..50 {
            mask.set(i * 20, true);
        }
        assert!(mask.density() < 0.1);

        // Make it dense
        for i in 0..700 {
            mask.set(i, true);
        }
        assert!(mask.density() > 0.7);

        // Both iteration methods should yield same result
        let iter_count = mask.iter_ones().count();
        let scan_count = (0..1000).filter(|&i| mask.get(i)).count();

        assert_eq!(iter_count, scan_count);
        assert!(iter_count >= 700);
    }

    #[test]
    fn test_empty_mask_iteration() {
        // Empty mask should iterate nothing
        let mask = DenseMask::new(1000);

        assert_eq!(mask.count_ones(), 0);
        assert_eq!(mask.density(), 0.0);

        let positions: Vec<u32> = mask.iter_ones().collect();
        assert!(positions.is_empty());
    }

    #[test]
    fn test_full_mask_iteration() {
        // Full mask (100% density)
        let mut mask = DenseMask::new(100);

        for i in 0..100 {
            mask.set(i, true);
        }

        assert_eq!(mask.count_ones(), 100);
        assert_eq!(mask.density(), 1.0);

        let positions: Vec<u32> = mask.iter_ones().collect();
        assert_eq!(positions.len(), 100);
    }
}
