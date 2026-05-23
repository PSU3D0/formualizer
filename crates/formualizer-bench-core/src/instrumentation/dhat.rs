//! Feature-gated dhat-rs heap profiler integration.
//!
//! Contract: when the `dhat-heap` feature is enabled, the corpus binary installs
//! `dhat::Alloc` as its global allocator and creates one `dhat::Profiler` at
//! process start. Per-phase allocation metrics are captured as deltas of
//! `dhat::HeapStats::get()`. When the feature is disabled, all functions are
//! no-ops and allocation fields are reported as `None`.

#[cfg(feature = "dhat-heap")]
pub type ProfilerGuard = dhat::Profiler;

#[cfg(not(feature = "dhat-heap"))]
pub struct ProfilerGuard;

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocationSnapshot {
    pub allocs_count: Option<u64>,
    pub allocs_bytes: Option<u64>,
    pub allocs_max_bytes: Option<u64>,
}

#[cfg(feature = "dhat-heap")]
pub fn init_profiler() -> ProfilerGuard {
    dhat::Profiler::new_heap()
}

#[cfg(not(feature = "dhat-heap"))]
pub fn init_profiler() -> ProfilerGuard {
    ProfilerGuard
}

#[cfg(feature = "dhat-heap")]
pub fn snapshot() -> AllocationSnapshot {
    let stats = dhat::HeapStats::get();
    AllocationSnapshot {
        allocs_count: Some(stats.total_blocks as u64),
        allocs_bytes: Some(stats.total_bytes as u64),
        allocs_max_bytes: Some(stats.max_bytes as u64),
    }
}

#[cfg(not(feature = "dhat-heap"))]
pub fn snapshot() -> AllocationSnapshot {
    AllocationSnapshot::default()
}

pub fn delta(start: AllocationSnapshot, end: AllocationSnapshot) -> AllocationSnapshot {
    AllocationSnapshot {
        allocs_count: opt_delta(start.allocs_count, end.allocs_count),
        allocs_bytes: opt_delta(start.allocs_bytes, end.allocs_bytes),
        allocs_max_bytes: end.allocs_max_bytes,
    }
}

fn opt_delta(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    match (start, end) {
        (Some(start), Some(end)) => Some(end.saturating_sub(start)),
        _ => None,
    }
}
