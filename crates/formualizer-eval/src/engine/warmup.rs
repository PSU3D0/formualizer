//! Warmup executor for pre-building artifacts

use crate::engine::cache::{CriteriaKey, CriteriaMaskCache, FlatKind, FlatView, RangeFlatCache};
use crate::engine::masks::DenseMask;
use crate::engine::metrics::{WarmupMetrics, WarmupTimer};
use crate::engine::pass_planner::{HotReference, PassWarmupPlan};
use crate::engine::range_stream::RangeStorage;
use crate::engine::reference_fingerprint::ReferenceFingerprint;
use crate::engine::tuning::WarmupConfig;
use crate::traits::FunctionContext;
use formualizer_common::LiteralValue;
use formualizer_core::parser::ReferenceType;
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
    pub flat_cache: RangeFlatCache,
    pub mask_cache: CriteriaMaskCache,
    pub metrics: Arc<WarmupMetrics>,
    /// Coordinator for in-flight builds
    build_coordinator: Arc<BuildCoordinator>,
    /// Coordinator for mask builds
    mask_coordinator: Arc<BuildCoordinator>,
}

impl PassContext {
    pub fn new(config: &WarmupConfig) -> Self {
        Self {
            flat_cache: RangeFlatCache::new(config.flat_cache_mb_cap),
            mask_cache: CriteriaMaskCache::new(config.mask_cache_entries_cap),
            metrics: Arc::new(WarmupMetrics::new()),
            build_coordinator: Arc::new(BuildCoordinator::default()),
            mask_coordinator: Arc::new(BuildCoordinator::default()),
        }
    }

    /// Clear all pass-scoped caches
    pub fn clear(&mut self) {
        self.flat_cache.clear();
        self.mask_cache.clear();
        self.metrics.reset();
    }

    /// Try to get or build a flat for a reference
    pub fn get_or_build_flat<C: FunctionContext>(
        &mut self,
        reference: &ReferenceType,
        context: &C,
        config: &WarmupConfig,
    ) -> Option<FlatView> {
        let key = reference.fingerprint();

        // Check cache first
        if let Some(flat) = self.flat_cache.get(&key) {
            self.metrics.record_flat_reuse();
            return Some(flat);
        }

        // Try to claim the build
        if !self.build_coordinator.try_claim(&key) {
            // Someone else is building, wait for them
            let timeout = Duration::from_millis(config.warmup_time_budget_ms.min(5000));
            if self.build_coordinator.wait_for(&key, timeout) {
                // Try cache again after waiting
                if let Some(flat) = self.flat_cache.get(&key) {
                    self.metrics.record_flat_reuse();
                    return Some(flat);
                }
            }
            return None;
        }

        // We have the claim, build it
        let start = Instant::now();
        let result = self.build_flat(reference, context);
        let build_time_ms = start.elapsed().as_millis() as u64;

        // Release the claim
        self.build_coordinator.release(&key);

        if let Some(ref flat) = result {
            // Try to insert into cache
            if self.flat_cache.insert(key.clone(), flat.clone()) {
                let cell_count = flat.row_count * flat.col_count;
                self.metrics.record_flat_build(cell_count, build_time_ms);
            }
        }

        result
    }

    /// Build a flat view from a reference
    fn build_flat<C: FunctionContext>(
        &self,
        reference: &ReferenceType,
        context: &C,
    ) -> Option<FlatView> {
        // Resolve the range to get storage
        // TODO: Need to pass the current sheet properly
        let storage = context.resolve_range_storage(reference, "").ok()?;

        match storage {
            RangeStorage::Stream(mut stream) => {
                // Get dimensions
                let (rows, cols) = stream.dimensions();
                let row_count = rows as usize;
                let col_count = cols as usize;

                if row_count == 0 || col_count == 0 {
                    return None;
                }

                // Collect values from stream
                let mut numeric_values: Vec<f64> = Vec::new();
                let mut text_values: Vec<String> = Vec::new();
                let mut mixed_values: Vec<LiteralValue> = Vec::new();
                let mut is_numeric = true;
                let mut is_text = true;

                for value in &mut stream {
                    mixed_values.push(value.as_ref().clone());

                    match value.as_ref() {
                        LiteralValue::Number(n) => {
                            if is_numeric {
                                numeric_values.push(*n);
                            }
                            is_text = false;
                        }
                        LiteralValue::Int(i) => {
                            if is_numeric {
                                numeric_values.push(*i as f64);
                            }
                            is_text = false;
                        }
                        LiteralValue::Text(s) => {
                            if is_text {
                                text_values.push(s.clone());
                            }
                            is_numeric = false;
                        }
                        LiteralValue::Empty => {
                            if is_numeric {
                                numeric_values.push(0.0);
                            }
                            if is_text {
                                text_values.push(String::new());
                            }
                        }
                        _ => {
                            is_numeric = false;
                            is_text = false;
                        }
                    }
                }

                // Build the appropriate flat type
                let kind = if is_numeric && !numeric_values.is_empty() {
                    FlatKind::Numeric {
                        values: Arc::from(numeric_values.into_boxed_slice()),
                        valid: None, // TODO: track validity in Phase 3
                    }
                } else if is_text && !text_values.is_empty() {
                    let text_arc: Vec<Arc<str>> = text_values
                        .into_iter()
                        .map(|s| Arc::<str>::from(s))
                        .collect();
                    FlatKind::Text {
                        values: Arc::from(text_arc.into_boxed_slice()),
                        empties: None,
                    }
                } else {
                    FlatKind::Mixed {
                        values: Arc::from(mixed_values.into_boxed_slice()),
                    }
                };

                Some(FlatView {
                    kind,
                    row_count,
                    col_count,
                })
            }
            RangeStorage::Owned(rows) => {
                // Handle owned storage
                let row_count = rows.len();
                if row_count == 0 {
                    return None;
                }

                let col_count = rows.get(0).map_or(0, |r| r.len());
                if col_count == 0 {
                    return None;
                }

                let mut numeric_values: Vec<f64> = Vec::new();
                let mut text_values: Vec<String> = Vec::new();
                let mut mixed_values: Vec<LiteralValue> = Vec::new();
                let mut is_numeric = true;
                let mut is_text = true;

                for row in rows.iter() {
                    for value in row {
                        mixed_values.push(value.clone());

                        match value {
                            LiteralValue::Number(n) => {
                                if is_numeric {
                                    numeric_values.push(*n);
                                }
                                is_text = false;
                            }
                            LiteralValue::Int(i) => {
                                if is_numeric {
                                    numeric_values.push(*i as f64);
                                }
                                is_text = false;
                            }
                            LiteralValue::Text(s) => {
                                if is_text {
                                    text_values.push(s.clone());
                                }
                                is_numeric = false;
                            }
                            LiteralValue::Empty => {
                                if is_numeric {
                                    numeric_values.push(0.0);
                                }
                                if is_text {
                                    text_values.push(String::new());
                                }
                            }
                            _ => {
                                is_numeric = false;
                                is_text = false;
                            }
                        }
                    }
                }

                // Build the appropriate flat type
                let kind = if is_numeric && !numeric_values.is_empty() {
                    FlatKind::Numeric {
                        values: Arc::from(numeric_values.into_boxed_slice()),
                        valid: None,
                    }
                } else if is_text && !text_values.is_empty() {
                    let text_arc: Vec<Arc<str>> = text_values
                        .into_iter()
                        .map(|s| Arc::<str>::from(s))
                        .collect();
                    FlatKind::Text {
                        values: Arc::from(text_arc.into_boxed_slice()),
                        empties: None,
                    }
                } else {
                    FlatKind::Mixed {
                        values: Arc::from(mixed_values.into_boxed_slice()),
                    }
                };

                Some(FlatView {
                    kind,
                    row_count,
                    col_count,
                })
            }
        }
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
        let time_budget = Duration::from_millis(self.config.warmup_time_budget_ms);
        let start = Instant::now();

        // Track what we've selected
        pass_ctx
            .metrics
            .inc_candidates_refs_considered(plan.flatten.len());

        // Process references with time budget enforcement
        for reference_info in &plan.flatten {
            // Check time budget
            if start.elapsed() >= time_budget {
                break;
            }

            // Build the flat
            if let Some(reference) = reference_info.to_reference() {
                pass_ctx.get_or_build_flat(&reference, context, &self.config);
            }
        }

        // Record selected count
        let selected_count = pass_ctx.flat_cache.len();
        pass_ctx
            .metrics
            .inc_candidates_refs_selected(selected_count);

        // Record timing
        pass_ctx.metrics.record_warmup_time(timer.elapsed());

        Ok(())
    }
}

/// Extension for HotReference to convert back to ReferenceType
trait HotReferenceExt {
    fn to_reference(&self) -> Option<ReferenceType>;
}

impl HotReferenceExt for HotReference {
    fn to_reference(&self) -> Option<ReferenceType> {
        // Parse the fingerprint back to a reference
        // This is a simplified version - in production, store the actual reference
        if self.key.starts_with("range:") {
            let parts: Vec<&str> = self.key.split(':').collect();
            if parts.len() >= 6 {
                let sheet = if parts[1] == "_" {
                    None
                } else {
                    Some(parts[1].to_string())
                };
                let start_row = parts[2].parse().ok();
                let start_col = parts[3].parse().ok();
                let end_row = parts[4].parse().ok();
                let end_col = parts[5].parse().ok();

                Some(ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}
