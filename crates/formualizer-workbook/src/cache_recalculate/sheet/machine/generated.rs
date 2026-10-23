//! Source-order advisory membership only. Ownership/refusal validation stays
//! in the existing end-of-sheet pass; overlapping extents are not refused here.
use super::SourceRect;
use std::{cmp::Reverse, collections::BinaryHeap};

const COLUMNS: usize = 16_384;

/// Row-expiring column coverage. A bounded segment tree supports overlapping
/// intervals without introducing a quadratic scan over previously seen anchors.
/// Allocated only after the first array anchor; the scanner has validated grid
/// coordinates and strictly increasing source order before calling this helper.
#[derive(Default)]
pub(super) struct GeneratedChildren {
    coverage: Vec<usize>,
    expiry: BinaryHeap<Reverse<(u32, u32, u32)>>,
}
impl GeneratedChildren {
    pub(super) fn observe(&mut self, extent: SourceRect) {
        self.expire(extent.first_row);
        if self.coverage.is_empty() {
            self.coverage.resize(COLUMNS * 2, 0);
        }
        self.update(extent.first_col, extent.last_col, true);
        self.expiry.push(Reverse((
            extent.last_row,
            extent.first_col,
            extent.last_col,
        )));
    }
    pub(super) fn contains(&mut self, row: u32, col: u32) -> bool {
        self.expire(row);
        if self.expiry.is_empty() {
            return false;
        }
        let mut at = COLUMNS + col as usize - 1;
        while at != 0 {
            if self.coverage.get(at).is_some_and(|count| *count != 0) {
                return true;
            }
            at /= 2;
        }
        false
    }
    fn expire(&mut self, row: u32) {
        while let Some(&Reverse((last_row, first_col, last_col))) = self.expiry.peek() {
            if last_row >= row {
                break;
            }
            self.expiry.pop();
            self.update(first_col, last_col, false);
        }
    }
    fn update(&mut self, first_col: u32, last_col: u32, add: bool) {
        let mut first = COLUMNS + first_col as usize - 1;
        let mut end = COLUMNS + last_col as usize;
        while first < end {
            if first & 1 != 0 {
                self.change(first, add);
                first += 1;
            }
            if end & 1 != 0 {
                end -= 1;
                self.change(end, add);
            }
            first /= 2;
            end /= 2;
        }
    }
    fn change(&mut self, at: usize, add: bool) {
        if let Some(count) = self.coverage.get_mut(at) {
            if add {
                // Each increment corresponds to one retained expiry entry, so
                // the number of allocated anchors bounds this count.
                *count += 1;
            } else {
                *count = count.saturating_sub(1);
            }
        }
    }
}

#[test]
fn overlapping_extents_expire_independently_without_ownership_decisions() {
    let mut hints = GeneratedChildren::default();
    assert!(!hints.contains(1, 1));
    hints.observe(SourceRect {
        first_row: 1,
        first_col: 1,
        last_row: 2,
        last_col: 16_384,
    });
    hints.observe(SourceRect {
        first_row: 1,
        first_col: 2,
        last_row: 3,
        last_col: 3,
    });
    assert!(hints.contains(1, 16_384));
    assert!(hints.contains(2, 1));
    assert!(!hints.contains(3, 1));
    assert!(hints.contains(3, 2));
    assert!(hints.contains(3, 3));
    assert!(!hints.contains(3, 4));
    assert!(!hints.contains(4, 2));
}
