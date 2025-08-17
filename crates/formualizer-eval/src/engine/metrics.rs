//! Lightweight metrics for warmup performance tracking

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Metrics collected during warmup and evaluation
#[derive(Default)]
pub struct WarmupMetrics {
    // Timing
    pub warmup_time_ns: AtomicU64,

    // Candidates
    pub candidates_refs_considered: AtomicUsize,
    pub candidates_refs_selected: AtomicUsize,
    pub candidates_criteria_considered: AtomicUsize,
    pub candidates_criteria_selected: AtomicUsize,

    // Built artifacts
    pub flats_built: AtomicUsize,
    pub masks_built: AtomicUsize,
    pub indexes_built: AtomicUsize,

    // Cache performance
    pub flat_cache_hits: AtomicUsize,
    pub flat_cache_misses: AtomicUsize,
    pub mask_cache_hits: AtomicUsize,
    pub mask_cache_misses: AtomicUsize,
    pub index_hits: AtomicUsize,
    pub index_misses: AtomicUsize,
}

impl WarmupMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record warmup timing
    pub fn record_warmup_time(&self, duration: Duration) {
        self.warmup_time_ns
            .store(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Increment a counter
    pub fn inc_candidates_refs_considered(&self, count: usize) {
        self.candidates_refs_considered
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_candidates_refs_selected(&self, count: usize) {
        self.candidates_refs_selected
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_flats_built(&self, count: usize) {
        self.flats_built.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_flat_cache_hit(&self) {
        self.flat_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_flat_cache_miss(&self) {
        self.flat_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    // Phase 2 additions for tests
    pub fn record_flat_build(&self, _cell_count: usize, build_time_ms: u64) {
        self.flats_built.fetch_add(1, Ordering::Relaxed);
        self.warmup_time_ns
            .fetch_add(build_time_ms * 1_000_000, Ordering::Relaxed);
    }

    pub fn record_flat_reuse(&self) {
        self.flat_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn flats_built(&self) -> usize {
        self.flats_built.load(Ordering::Relaxed)
    }

    pub fn flats_reused(&self) -> usize {
        self.flat_cache_hits.load(Ordering::Relaxed)
    }

    pub fn total_build_time_ms(&self) -> u64 {
        self.warmup_time_ns.load(Ordering::Relaxed) / 1_000_000
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.warmup_time_ns.store(0, Ordering::Relaxed);
        self.candidates_refs_considered.store(0, Ordering::Relaxed);
        self.candidates_refs_selected.store(0, Ordering::Relaxed);
        self.candidates_criteria_considered
            .store(0, Ordering::Relaxed);
        self.candidates_criteria_selected
            .store(0, Ordering::Relaxed);
        self.flats_built.store(0, Ordering::Relaxed);
        self.masks_built.store(0, Ordering::Relaxed);
        self.indexes_built.store(0, Ordering::Relaxed);
        self.flat_cache_hits.store(0, Ordering::Relaxed);
        self.flat_cache_misses.store(0, Ordering::Relaxed);
        self.mask_cache_hits.store(0, Ordering::Relaxed);
        self.mask_cache_misses.store(0, Ordering::Relaxed);
        self.index_hits.store(0, Ordering::Relaxed);
        self.index_misses.store(0, Ordering::Relaxed);
    }

    /// Get a summary for debugging
    #[cfg(test)]
    pub fn summary(&self) -> String {
        format!(
            "WarmupMetrics {{ warmup_time_ms: {:.1}, refs_selected: {}/{}, flats_built: {}, cache_hits: {}/{} }}",
            self.warmup_time_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.candidates_refs_selected.load(Ordering::Relaxed),
            self.candidates_refs_considered.load(Ordering::Relaxed),
            self.flats_built.load(Ordering::Relaxed),
            self.flat_cache_hits.load(Ordering::Relaxed),
            self.flat_cache_hits.load(Ordering::Relaxed)
                + self.flat_cache_misses.load(Ordering::Relaxed),
        )
    }
}

/// Timer helper for measuring warmup phases
pub struct WarmupTimer {
    start: Instant,
}

impl WarmupTimer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}
