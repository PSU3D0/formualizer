//! Warmup executor for pre-building artifacts (flats removed; masks scaffold only)

use crate::engine::cache::{CriteriaKey, CriteriaMaskCache};
use crate::engine::masks::DenseMask;
use crate::engine::metrics::{WarmupMetrics, WarmupTimer};
use crate::engine::pass_planner::PassWarmupPlan;
use crate::engine::tuning::WarmupConfig;
use crate::traits::FunctionContext;
use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Per-key build coordination to avoid duplicate work
#[derive(Default)]
struct BuildCoordinator {
    /// Keys currently being built (in-flight)
    in_flight: Mutex<HashSet<String>>,
    /// Condition variable for waiting on in-flight builds
    cv: Condvar,
}

impl BuildCoordinator {
    /// Try to claim a key for building. Returns true if claimed, false if already in-flight.
    fn try_claim(&self, key: &str) -> bool {
        let mut in_flight = self.in_flight.lock().unwrap();
        if in_flight.contains(key) {
            false
        } else {
            in_flight.insert(key.to_string());
            true
        }
    }

    /// Wait for a key to be completed
    fn wait_for(&self, key: &str, timeout: Duration) -> bool {
        let start = Instant::now();
        let mut in_flight = self.in_flight.lock().unwrap();

        while in_flight.contains(key) {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return false; // Timeout
            }

            let (guard, timeout_result) = self.cv.wait_timeout(in_flight, remaining).unwrap();
            in_flight = guard;

            if timeout_result.timed_out() {
                return false;
            }
        }
        true
    }

    /// Release a key after building (success or failure)
    fn release(&self, key: &str) {
        let mut in_flight = self.in_flight.lock().unwrap();
        in_flight.remove(key);
        self.cv.notify_all();
    }
}

/// Context for a single evaluation pass
pub struct PassContext {
    pub mask_cache: CriteriaMaskCache,
    pub metrics: Arc<WarmupMetrics>,
    /// Coordinator for in-flight builds
    mask_coordinator: Arc<BuildCoordinator>,
}

impl PassContext {
    pub fn new(config: &WarmupConfig) -> Self {
        Self {
            mask_cache: CriteriaMaskCache::new(config.mask_cache_entries_cap),
            metrics: Arc::new(WarmupMetrics::new()),
            mask_coordinator: Arc::new(BuildCoordinator::default()),
        }
    }

    /// Clear all pass-scoped caches
    pub fn clear(&mut self) {
        self.mask_cache.clear();
        self.metrics.reset();
    }

    /// Try to get or build a mask for criteria (Phase 3)
    pub fn get_or_build_mask<C: FunctionContext>(
        &mut self,
        key: &CriteriaKey,
        _context: &C,
        _config: &WarmupConfig,
    ) -> Option<DenseMask> {
        // Check cache first
        if let Some(mask) = self.mask_cache.get(key) {
            self.metrics.record_mask_reuse();
            return Some(mask);
        }

        // Phase 3: Full mask building will be implemented here
        // For now, return None to use fallback path
        None
    }
}

/// Warmup executor
pub struct WarmupExecutor {
    config: WarmupConfig,
}

impl WarmupExecutor {
    pub fn new(config: WarmupConfig) -> Self {
        Self { config }
    }

    /// Execute warmup plan
    pub fn execute<C: FunctionContext>(
        &self,
        plan: &PassWarmupPlan,
        pass_ctx: &mut PassContext,
        context: &C,
    ) -> Result<(), String> {
        if !self.config.warmup_enabled {
            return Ok(());
        }

        let timer = WarmupTimer::start();
        // No-op for flats (removed). Mask warmup to be implemented later.

        // Record timing
        pass_ctx.metrics.record_warmup_time(timer.elapsed());

        Ok(())
    }
}

// Flats removed: no HotReference reconstruction required
