//! Candidate cover ∩ identity runs ∩ independently maintained owners.
//! No cell expansion or whole-store scan. Output is sorted for refinement.

use super::geom::{Cover, Rect};
use super::identity::FAMILY;
use super::store::{AuthorityError, Store};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnerSlice {
    pub sheet: u16,
    pub col: u32,
    pub r0: u32,
    pub r1: u32,
    pub first_id: u32,
    pub owner: u32,
}
impl OwnerSlice {
    fn key(self) -> u64 {
        (u64::from(self.sheet) << 34) | (u64::from(self.col) << 20) | u64::from(self.r0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CandidateWork {
    pub intervals: u64,
    pub run_index: u64,
    pub runs: u64,
    pub owner_index: u64,
    pub owners: u64,
    pub initialize: u64,
    pub sort: u64,
}
impl CandidateWork {
    pub fn total(self) -> u64 {
        self.intervals
            + self.run_index
            + self.runs
            + self.owner_index
            + self.owners
            + self.initialize
            + self.sort
    }
}

#[derive(Debug)]
pub(crate) struct Candidates {
    pub slices: Vec<OwnerSlice>,
    pub cells: u64,
    pub work: CandidateWork,
    pub peak_heap_bytes: u64,
}
impl Candidates {
    pub fn heap_bytes(&self) -> u64 {
        (self.slices.capacity() * size_of::<OwnerSlice>()) as u64
    }
}

fn visit(
    store: &Store,
    cover: &Cover,
    work: &mut CandidateWork,
    output: &mut dyn FnMut(OwnerSlice),
) {
    for (sheet, col, r0, r1) in cover.column_intervals() {
        work.intervals += 1;
        work.run_index += store
            .ids()
            .visit_runs_in_counted(sheet, col, r0, r1, &mut |h| {
                work.runs += 1;
                let run = store.ids().run(h);
                let a = r0.max(run.row_start);
                let b = r1.min(run.row_start + run.len - 1);
                let mut emit_owner = |owner| {
                    work.owners += 1;
                    let (_, dom, _) = store.owner_dom(owner);
                    let lo = a.max(dom.r0);
                    let hi = b.min(dom.r1);
                    debug_assert!(lo <= hi && dom.c0 <= col && col <= dom.c1);
                    output(OwnerSlice {
                        sheet,
                        col,
                        r0: lo,
                        r1: hi,
                        first_id: run.first_id + (lo - run.row_start),
                        owner,
                    });
                };
                if run.owner == FAMILY {
                    work.owner_index +=
                        store.visit_plan_owners(sheet, &Rect::new(a, col, b, col), &mut emit_owner);
                } else {
                    emit_owner(run.owner);
                }
            });
    }
}

/// Slice counts sorted by comparison instead of radix passes.
const SMALL_DISCOVERY: usize = 32;
/// The key bits the radix passes order by (seven bytes).
const KEY_MASK: u64 = (1 << 56) - 1;

/// Read-only two-pass discovery, exact reservation, stable radix ordering.
/// Borrowed cover/store are not charged; caller subtracts other live scratch.
/// Index traversal work is exposed, not hidden in a claimed O(C) bound.
pub(crate) fn discover(
    store: &Store,
    cover: &Cover,
    scratch_limit: Option<u64>,
) -> Result<Candidates, AuthorityError> {
    let mut work = CandidateWork::default();
    let mut count = 0usize;
    let mut cells = 0u64;
    // The counting visit keeps a small request's slices on the stack (no
    // heap: admission is unchanged), so it is not visited twice.
    let mut inline: smallvec::SmallVec<[OwnerSlice; SMALL_DISCOVERY]> = smallvec::SmallVec::new();
    visit(store, cover, &mut work, &mut |s| {
        if count < SMALL_DISCOVERY {
            inline.push(s);
        }
        count += 1;
        cells += u64::from(s.r1 - s.r0 + 1);
    });
    let peak = count
        .checked_mul(size_of::<OwnerSlice>())
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(AuthorityError::Alloc)?;
    if let Some(limit) = scratch_limit
        && peak > limit
    {
        return Err(AuthorityError::Admission {
            resource: "scratch",
            needed: peak,
            limit,
        });
    }
    let mut slices = Vec::new();
    let mut temp = Vec::new();
    slices
        .try_reserve_exact(count)
        .map_err(|_| AuthorityError::Alloc)?;
    temp.try_reserve_exact(count)
        .map_err(|_| AuthorityError::Alloc)?;
    if count <= SMALL_DISCOVERY {
        slices.extend(inline);
    } else {
        visit(store, cover, &mut work, &mut |s| slices.push(s));
    }
    temp.extend((0..count).map(|_| {
        work.initialize += 1;
        OwnerSlice::default()
    }));
    if count <= SMALL_DISCOVERY {
        // A tiny request (an edit's handful of cells): a stable comparison
        // sort on the same 56 key bits orders exactly as the radix passes
        // below, without their seven 256-bucket counts.
        work.sort += count as u64;
        slices.sort_by_key(|s| s.key() & KEY_MASK);
    } else {
        // 50-bit (sheet, column, row), seven byte passes. No comparison tail.
        for shift in (0..56).step_by(8) {
            let mut counts = [0usize; 256];
            work.sort += 256;
            for s in &slices {
                work.sort += 1;
                counts[((s.key() >> shift) & 255) as usize] += 1;
            }
            let mut start = 0;
            for n in &mut counts {
                work.sort += 1;
                let len = *n;
                *n = start;
                start += len;
            }
            for &s in &slices {
                work.sort += 1;
                let digit = ((s.key() >> shift) & 255) as usize;
                temp[counts[digit]] = s;
                counts[digit] += 1;
            }
            for (s, t) in slices.iter_mut().zip(&temp) {
                work.sort += 1;
                *s = *t;
            }
        }
    }
    Ok(Candidates {
        slices,
        cells,
        work,
        peak_heap_bytes: peak,
    })
}
