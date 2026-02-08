use crate::SheetId;
use crate::arrow_store::SheetStore;
use crate::engine::eval_delta::{DeltaCollector, DeltaMode, EvalDelta};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::range_view::RangeView;
use crate::engine::spill::{RegionLockManager, SpillMeta, SpillShape};
use crate::engine::{DependencyGraph, EvalConfig, Scheduler, VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::traits::FunctionProvider;
use crate::traits::{EvaluationContext, Resolver};
use chrono::Timelike;
use formualizer_common::{col_letters_from_1based, parse_a1_1based};
use formualizer_parse::parser::ReferenceType;
use formualizer_parse::{ASTNode, ExcelError, ExcelErrorKind, LiteralValue};
use rayon::ThreadPoolBuilder;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Engine<R> {
    pub(crate) graph: DependencyGraph,
    resolver: R,
    pub config: EvalConfig,
    clock: Arc<dyn crate::timezone::ClockProvider>,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    pub recalc_epoch: u64,
    snapshot_id: std::sync::atomic::AtomicU64,
    spill_mgr: ShimSpillManager,
    /// Arrow-backed storage for sheet values (Phase A)
    arrow_sheets: SheetStore,
    /// True if any edit after bulk load; disables Arrow reads for parity
    has_edited: bool,
    /// Overlay compaction counter (Phase C instrumentation)
    overlay_compactions: u64,

    // Overlay memory observability / budget (ticket 503)
    computed_overlay_bytes_estimate: usize,
    computed_overlay_mirroring_disabled: bool,
    /// When true, RangeView resolution materializes from graph/Arrow base per-cell.
    /// This preserves correctness if we stop mirroring formula/spill outputs into computed overlays.
    pub(crate) force_materialize_range_views: bool,
    // Pass-scoped cache for Arrow used-row bounds per column
    row_bounds_cache: std::sync::RwLock<Option<RowBoundsCache>>,
    source_cache: Arc<std::sync::RwLock<SourceCache>>,
    /// Staged formulas by sheet when `defer_graph_building` is enabled.
    staged_formulas: std::collections::HashMap<String, Vec<(u32, u32, String)>>,
    /// Transient cancellation flag used during evaluation
    active_cancel_flag: Option<Arc<AtomicBool>>,
}

#[derive(Default)]
struct SourceCache {
    scalars: FxHashMap<(String, Option<u64>), LiteralValue>,
    tables: FxHashMap<(String, Option<u64>), Arc<dyn crate::traits::Table>>,
}

struct SourceCacheSession {
    cache: Arc<std::sync::RwLock<SourceCache>>,
}

impl Drop for SourceCacheSession {
    fn drop(&mut self) {
        if let Ok(mut g) = self.cache.write() {
            *g = SourceCache::default();
        }
    }
}

#[derive(Debug)]
pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
}

/// Cached evaluation schedule that can be replayed across multiple recalculations.
#[derive(Debug)]
pub struct RecalcPlan {
    schedule: crate::engine::Schedule,
}

impl RecalcPlan {
    pub fn layer_count(&self) -> usize {
        self.schedule.layers.len()
    }
}

#[cfg(test)]
pub(crate) mod criteria_mask_test_hooks {
    use std::cell::Cell;

    thread_local! {
        static TEXT_SEGMENTS_TOTAL: Cell<usize> = const { Cell::new(0) };
        static TEXT_SEGMENTS_ALL_NULL: Cell<usize> = const { Cell::new(0) };
    }

    pub fn reset_text_segment_counters() {
        TEXT_SEGMENTS_TOTAL.with(|c| c.set(0));
        TEXT_SEGMENTS_ALL_NULL.with(|c| c.set(0));
    }

    pub fn text_segment_counters() -> (usize, usize) {
        let a = TEXT_SEGMENTS_TOTAL.with(|c| c.get());
        let b = TEXT_SEGMENTS_ALL_NULL.with(|c| c.get());
        (a, b)
    }

    pub(crate) fn inc_total() {
        TEXT_SEGMENTS_TOTAL.with(|c| c.set(c.get() + 1));
    }
    pub(crate) fn inc_all_null() {
        TEXT_SEGMENTS_ALL_NULL.with(|c| c.set(c.get() + 1));
    }
}

fn compute_criteria_mask(
    view: &RangeView<'_>,
    col_in_view: usize,
    pred: &crate::args::CriteriaPredicate,
) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
    use crate::compute_prelude::{boolean, cmp, concat_arrays};
    use arrow::compute::kernels::comparison::{ilike, nilike};
    use arrow_array::{
        Array as _, ArrayRef, BooleanArray, Float64Array, StringArray, builder::BooleanBuilder,
    };

    // Helper: apply a numeric predicate to a single Float64Array chunk
    fn apply_numeric_pred(
        chunk: &Float64Array,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<BooleanArray> {
        match pred {
            crate::args::CriteriaPredicate::Gt(n) => {
                cmp::gt(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Ge(n) => {
                cmp::gt_eq(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Lt(n) => {
                cmp::lt(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Le(n) => {
                cmp::lt_eq(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Eq(v) => match v {
                formualizer_common::LiteralValue::Number(x) => {
                    cmp::eq(chunk, &Float64Array::new_scalar(*x)).ok()
                }
                formualizer_common::LiteralValue::Int(i) => {
                    cmp::eq(chunk, &Float64Array::new_scalar(*i as f64)).ok()
                }
                _ => None,
            },
            crate::args::CriteriaPredicate::Ne(v) => match v {
                formualizer_common::LiteralValue::Number(x) => {
                    cmp::neq(chunk, &Float64Array::new_scalar(*x)).ok()
                }
                formualizer_common::LiteralValue::Int(i) => {
                    cmp::neq(chunk, &Float64Array::new_scalar(*i as f64)).ok()
                }
                _ => None,
            },
            _ => None,
        }
    }

    // Check if this is a numeric predicate that can be applied per-chunk
    let is_numeric_pred = matches!(
        pred,
        crate::args::CriteriaPredicate::Gt(_)
            | crate::args::CriteriaPredicate::Ge(_)
            | crate::args::CriteriaPredicate::Lt(_)
            | crate::args::CriteriaPredicate::Le(_)
            | crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Number(_))
            | crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Int(_))
            | crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Number(_))
            | crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Int(_))
    );

    // OPTIMIZED PATH: For numeric predicates, apply per-chunk and concatenate boolean masks.
    // This avoids materializing the full numeric column (64-bit per element) and instead
    // concatenates boolean masks (1-bit per element) - a 64x memory reduction.
    if is_numeric_pred {
        let mut bool_parts: Vec<BooleanArray> = Vec::new();
        for res in view.numbers_slices() {
            let (_rs, _rl, cols_seg) = res.ok()?;
            if col_in_view < cols_seg.len() {
                let chunk = cols_seg[col_in_view].as_ref();
                let mask = apply_numeric_pred(chunk, pred)?;
                bool_parts.push(mask);
            }
        }

        if bool_parts.is_empty() {
            return None;
        } else if bool_parts.len() == 1 {
            return Some(std::sync::Arc::new(bool_parts.remove(0)));
        } else {
            // Concatenate boolean masks (much cheaper than concatenating Float64 arrays)
            let anys: Vec<&dyn arrow_array::Array> = bool_parts
                .iter()
                .map(|a| a as &dyn arrow_array::Array)
                .collect();
            let conc: ArrayRef = concat_arrays(&anys).ok()?;
            let ba = conc.as_any().downcast_ref::<BooleanArray>()?.clone();
            return Some(std::sync::Arc::new(ba));
        }
    }

    // TEXT PATH: build masks per row-chunk using lowered text slices.
    // This avoids concatenating full-string columns just to compute a boolean mask.
    let (text_kind, text_pat, empty_special) = match pred {
        crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Text(t)) => {
            (0u8, t.to_ascii_lowercase(), t.is_empty())
        }
        crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Text(t)) => {
            (1u8, t.to_ascii_lowercase(), false)
        }
        crate::args::CriteriaPredicate::TextLike {
            pattern,
            case_insensitive,
        } => {
            let p = if *case_insensitive {
                pattern.to_ascii_lowercase()
            } else {
                pattern.clone()
            };
            (2u8, p.replace('*', "%").replace('?', "_"), false)
        }
        _ => return None,
    };

    let pat = StringArray::new_scalar(text_pat);
    let mut bool_parts: Vec<BooleanArray> = Vec::new();

    for res in view.iter_row_chunks() {
        let cs = res.ok()?;
        if cs.row_len == 0 {
            continue;
        }
        #[cfg(test)]
        criteria_mask_test_hooks::inc_total();

        let slices = view.slice_lowered_text(cs.row_start, cs.row_len);
        if col_in_view >= slices.len() {
            return None;
        }

        let seg_opt = slices[col_in_view].as_ref().map(|a| a.as_ref());
        let seg = match seg_opt {
            Some(s) => s,
            None => {
                #[cfg(test)]
                criteria_mask_test_hooks::inc_all_null();
                if text_kind == 0 && empty_special {
                    // Eq("") treats nulls (Empty) as equal.
                    let mut bb = BooleanBuilder::with_capacity(cs.row_len);
                    bb.append_n(cs.row_len, true);
                    bool_parts.push(bb.finish());
                } else {
                    // For non-empty patterns, ilike/nilike return null on null inputs.
                    bool_parts.push(BooleanArray::new_null(cs.row_len));
                }
                continue;
            }
        };

        let seg_sa = seg.as_any().downcast_ref::<StringArray>()?;
        let mut m = match text_kind {
            0 => ilike(seg_sa, &pat).ok()?,
            1 => nilike(seg_sa, &pat).ok()?,
            2 => ilike(seg_sa, &pat).ok()?,
            _ => return None,
        };

        if text_kind == 0 && empty_special {
            // Treat nulls as equal to empty string
            let mut bb = BooleanBuilder::with_capacity(seg_sa.len());
            for i in 0..seg_sa.len() {
                bb.append_value(seg_sa.is_null(i));
            }
            let nulls = bb.finish();
            m = boolean::or_kleene(&m, &nulls).ok()?;
        }

        bool_parts.push(m);
    }

    if bool_parts.is_empty() {
        None
    } else if bool_parts.len() == 1 {
        Some(std::sync::Arc::new(bool_parts.remove(0)))
    } else {
        let anys: Vec<&dyn arrow_array::Array> = bool_parts
            .iter()
            .map(|a| a as &dyn arrow_array::Array)
            .collect();
        let conc: ArrayRef = concat_arrays(&anys).ok()?;
        let ba = conc.as_any().downcast_ref::<BooleanArray>()?.clone();
        Some(std::sync::Arc::new(ba))
    }
}

#[derive(Debug, Clone)]
pub struct LayerInfo {
    pub vertex_count: usize,
    pub parallel_eligible: bool,
    pub sample_cells: Vec<String>, // Sample of up to 5 cell addresses
}

#[derive(Debug, Clone)]
pub struct EvalPlan {
    pub total_vertices_to_evaluate: usize,
    pub layers: Vec<LayerInfo>,
    pub cycles_detected: usize,
    pub dirty_count: usize,
    pub volatile_count: usize,
    pub parallel_enabled: bool,
    pub estimated_parallel_layers: usize,
    pub target_cells: Vec<String>,
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    pub fn new(resolver: R, config: EvalConfig) -> Self {
        crate::builtins::load_builtins();

        let clock = config.deterministic_mode.build_clock().unwrap_or_else(|_| {
            Arc::new(crate::timezone::SystemClock::new(
                crate::timezone::TimeZoneSpec::default(),
            ))
        });

        // Initialize thread pool based on config
        let thread_pool = if config.enable_parallel {
            let mut builder = ThreadPoolBuilder::new();
            if let Some(max_threads) = config.max_threads {
                builder = builder.num_threads(max_threads);
            }

            match builder.build() {
                Ok(pool) => Some(Arc::new(pool)),
                Err(_) => {
                    // Fall back to sequential evaluation if thread pool creation fails
                    None
                }
            }
        } else {
            None
        };

        let mut engine = Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            clock,
            thread_pool,
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            arrow_sheets: SheetStore::default(),
            has_edited: false,
            overlay_compactions: 0,
            computed_overlay_bytes_estimate: 0,
            computed_overlay_mirroring_disabled: false,
            force_materialize_range_views: false,
            row_bounds_cache: std::sync::RwLock::new(None),
            source_cache: Arc::new(std::sync::RwLock::new(SourceCache::default())),
            staged_formulas: std::collections::HashMap::new(),
            active_cancel_flag: None,
        };
        // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
        engine.config.arrow_storage_enabled = true;
        engine.config.delta_overlay_enabled = true;
        engine.config.write_formula_overlay_enabled = true;
        let default_sheet = engine.graph.default_sheet_name().to_string();
        engine.ensure_arrow_sheet(&default_sheet);
        engine
    }

    /// Create an Engine with a custom thread pool (for shared thread pool scenarios)
    pub fn with_thread_pool(
        resolver: R,
        config: EvalConfig,
        thread_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        crate::builtins::load_builtins();
        let clock = config.deterministic_mode.build_clock().unwrap_or_else(|_| {
            Arc::new(crate::timezone::SystemClock::new(
                crate::timezone::TimeZoneSpec::default(),
            ))
        });
        let mut engine = Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            clock,
            thread_pool: Some(thread_pool),
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            arrow_sheets: SheetStore::default(),
            has_edited: false,
            overlay_compactions: 0,
            computed_overlay_bytes_estimate: 0,
            computed_overlay_mirroring_disabled: false,
            force_materialize_range_views: false,
            row_bounds_cache: std::sync::RwLock::new(None),
            source_cache: Arc::new(std::sync::RwLock::new(SourceCache::default())),
            staged_formulas: std::collections::HashMap::new(),
            active_cancel_flag: None,
        };
        // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
        engine.config.arrow_storage_enabled = true;
        engine.config.delta_overlay_enabled = true;
        engine.config.write_formula_overlay_enabled = true;
        let default_sheet = engine.graph.default_sheet_name().to_string();
        engine.ensure_arrow_sheet(&default_sheet);
        engine
    }

    fn clear_source_cache(&self) {
        if let Ok(mut g) = self.source_cache.write() {
            *g = SourceCache::default();
        }
    }

    fn source_cache_session(&self) -> SourceCacheSession {
        self.clear_source_cache();
        SourceCacheSession {
            cache: self.source_cache.clone(),
        }
    }

    fn resolve_source_scalar_cached(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<LiteralValue, ExcelError> {
        let key = (name.to_string(), version);
        if let Ok(mut g) = self.source_cache.write() {
            if let Some(v) = g.scalars.get(&key) {
                return Ok(v.clone());
            }

            let v = self.resolver.resolve_source_scalar(name).map_err(|err| {
                if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("Unresolved source scalar: {name}"))
                } else {
                    err
                }
            })?;
            g.scalars.insert(key, v.clone());
            Ok(v)
        } else {
            self.resolver.resolve_source_scalar(name).map_err(|err| {
                if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("Unresolved source scalar: {name}"))
                } else {
                    err
                }
            })
        }
    }

    fn resolve_source_table_cached(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<Arc<dyn crate::traits::Table>, ExcelError> {
        let key = (name.to_string(), version);
        if let Ok(mut g) = self.source_cache.write() {
            if let Some(t) = g.tables.get(&key) {
                return Ok(t.clone());
            }

            let t = self.resolver.resolve_source_table(name).map_err(|err| {
                if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                    ExcelError::new(ExcelErrorKind::Ref)
                        .with_message(format!("Unresolved source table: {name}"))
                } else {
                    err
                }
            })?;
            let t: Arc<dyn crate::traits::Table> = Arc::from(t);
            g.tables.insert(key, t.clone());
            Ok(t)
        } else {
            self.resolver
                .resolve_source_table(name)
                .map_err(|err| {
                    if matches!(err.kind, ExcelErrorKind::Name | ExcelErrorKind::NImpl) {
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message(format!("Unresolved source table: {name}"))
                    } else {
                        err
                    }
                })
                .map(Arc::from)
        }
    }

    fn source_table_to_range_view(
        &self,
        table: &dyn crate::traits::Table,
        spec: &Option<formualizer_parse::parser::TableSpecifier>,
    ) -> Result<RangeView<'static>, ExcelError> {
        use formualizer_parse::parser::{SpecialItem, TableSpecifier};

        let owned = match spec {
            Some(TableSpecifier::Column(c)) => {
                let c = c.trim();
                if c == "@" || c.contains('[') || c.contains(']') || c.contains(',') {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Complex structured references not yet supported".to_string(),
                    ));
                }
                table.get_column(c)?.materialise().into_owned()
            }
            Some(TableSpecifier::ColumnRange(start, end)) => {
                let cols = table.columns();
                let start = start.trim();
                let end = end.trim();
                let start_idx = cols.iter().position(|n| n.eq_ignore_ascii_case(start));
                let end_idx = cols.iter().position(|n| n.eq_ignore_ascii_case(end));
                if let (Some(mut si), Some(mut ei)) = (start_idx, end_idx) {
                    if si > ei {
                        std::mem::swap(&mut si, &mut ei);
                    }
                    let h = table.data_height();
                    let w = ei - si + 1;
                    let mut rows = vec![vec![LiteralValue::Empty; w]; h];
                    for (offset, ci) in (si..=ei).enumerate() {
                        let cname = &cols[ci];
                        let col_range = table.get_column(cname)?;
                        let (rh, _) = col_range.dimensions();
                        for (r, row) in rows.iter_mut().enumerate().take(h.min(rh)) {
                            row[offset] = col_range.get(r, 0)?;
                        }
                    }
                    rows
                } else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Column range refers to unknown column(s)".to_string()));
                }
            }
            Some(TableSpecifier::SpecialItem(SpecialItem::Headers))
            | Some(TableSpecifier::Headers) => table
                .headers_row()
                .map(|r| r.materialise().into_owned())
                .unwrap_or_default(),
            Some(TableSpecifier::SpecialItem(SpecialItem::Totals))
            | Some(TableSpecifier::Totals) => table
                .totals_row()
                .map(|r| r.materialise().into_owned())
                .unwrap_or_default(),
            Some(TableSpecifier::SpecialItem(SpecialItem::Data)) | Some(TableSpecifier::Data) => {
                table
                    .data_body()
                    .map(|r| r.materialise().into_owned())
                    .unwrap_or_default()
            }
            Some(TableSpecifier::SpecialItem(SpecialItem::All)) | Some(TableSpecifier::All) => {
                let mut out: Vec<Vec<LiteralValue>> = Vec::new();
                if let Some(h) = table.headers_row() {
                    out.extend(h.iter_rows());
                }
                if let Some(body) = table.data_body() {
                    out.extend(body.iter_rows());
                }
                if let Some(tr) = table.totals_row() {
                    out.extend(tr.iter_rows());
                }
                out
            }
            Some(TableSpecifier::SpecialItem(SpecialItem::ThisRow)) => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                    "@ (This Row) requires table-aware context; not yet supported".to_string(),
                ));
            }
            Some(TableSpecifier::Row(_)) | Some(TableSpecifier::Combination(_)) => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("Complex structured references not yet supported".to_string()));
            }
            None => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("Table reference without specifier is unsupported".to_string()));
            }
        };

        Ok(RangeView::from_owned_rows(owned, self.config.date_system))
    }

    pub fn default_sheet_id(&self) -> SheetId {
        self.graph.default_sheet_id()
    }

    pub fn default_sheet_name(&self) -> &str {
        self.graph.default_sheet_name()
    }

    /// Update the workbook seed for deterministic RNGs in functions.
    pub fn set_workbook_seed(&mut self, seed: u64) {
        self.config.workbook_seed = seed;
    }

    /// Set the volatile level policy (Always/OnRecalc/OnOpen)
    pub fn set_volatile_level(&mut self, level: crate::traits::VolatileLevel) {
        self.config.volatile_level = level;
    }

    /// Enable/disable deterministic evaluation mode (fixed clock + timezone).
    pub fn set_deterministic_mode(
        &mut self,
        mode: crate::engine::DeterministicMode,
    ) -> Result<(), ExcelError> {
        let clock = mode.build_clock()?;
        self.config.deterministic_mode = mode;
        self.clock = clock;
        Ok(())
    }

    fn validate_deterministic_mode(&self) -> Result<(), ExcelError> {
        self.config.deterministic_mode.validate()
    }

    pub fn sheet_id(&self, name: &str) -> Option<SheetId> {
        self.graph.sheet_id(name)
    }

    pub fn sheet_id_mut(&mut self, name: &str) -> SheetId {
        self.add_sheet(name)
            .unwrap_or_else(|_| self.graph.sheet_id_mut(name))
    }

    pub fn sheet_name(&self, id: SheetId) -> &str {
        self.graph.sheet_name(id)
    }

    pub fn add_sheet(&mut self, name: &str) -> Result<SheetId, ExcelError> {
        let id = self.graph.add_sheet(name)?;
        self.ensure_arrow_sheet(name);
        Ok(id)
    }

    fn ensure_arrow_sheet(&mut self, name: &str) {
        if self.arrow_sheets.sheet(name).is_some() {
            return;
        }
        self.arrow_sheets
            .sheets
            .push(crate::arrow_store::ArrowSheet {
                name: std::sync::Arc::<str>::from(name),
                columns: Vec::new(),
                nrows: 0,
                chunk_starts: Vec::new(),
                chunk_rows: 32 * 1024,
            });
    }

    pub fn remove_sheet(&mut self, sheet_id: SheetId) -> Result<(), ExcelError> {
        let name = self.graph.sheet_name(sheet_id).to_string();
        self.graph.remove_sheet(sheet_id)?;
        self.arrow_sheets.sheets.retain(|s| s.name.as_ref() != name);
        Ok(())
    }

    pub fn rename_sheet(&mut self, sheet_id: SheetId, new_name: &str) -> Result<(), ExcelError> {
        let old_name = self.graph.sheet_name(sheet_id).to_string();
        self.graph.rename_sheet(sheet_id, new_name)?;
        self.ensure_arrow_sheet(&old_name);
        if let Some(asheet) = self.arrow_sheets.sheet_mut(&old_name) {
            asheet.name = std::sync::Arc::<str>::from(new_name);
        }
        Ok(())
    }

    pub fn named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&String, &crate::engine::named_range::NamedRange)> {
        self.graph.named_ranges_iter()
    }

    pub fn sheet_named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&(SheetId, String), &crate::engine::named_range::NamedRange)> {
        self.graph.sheet_named_ranges_iter()
    }

    pub fn resolve_name_entry(
        &self,
        name: &str,
        current_sheet: SheetId,
    ) -> Option<&crate::engine::named_range::NamedRange> {
        self.graph.resolve_name_entry(name, current_sheet)
    }

    pub fn define_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        self.graph.define_name(name, definition, scope)
    }

    pub fn update_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        self.graph.update_name(name, definition, scope)
    }

    pub fn delete_name(&mut self, name: &str, scope: NameScope) -> Result<(), ExcelError> {
        self.graph.delete_name(name, scope)
    }

    pub fn define_table(
        &mut self,
        name: &str,
        range: crate::reference::RangeRef,
        header_row: bool,
        headers: Vec<String>,
        totals_row: bool,
    ) -> Result<(), ExcelError> {
        self.graph
            .define_table(name, range, header_row, headers, totals_row)
    }

    pub fn define_source_scalar(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.define_source_scalar(name, version)
    }

    pub fn define_source_table(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.define_source_table(name, version)
    }

    pub fn set_source_scalar_version(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.set_source_scalar_version(name, version)
    }

    pub fn set_source_table_version(
        &mut self,
        name: &str,
        version: Option<u64>,
    ) -> Result<(), ExcelError> {
        self.graph.set_source_table_version(name, version)
    }

    pub fn invalidate_source(&mut self, name: &str) -> Result<(), ExcelError> {
        self.graph.invalidate_source(name)
    }

    pub fn vertex_value(&self, vertex: VertexId) -> Option<LiteralValue> {
        self.graph.get_value(vertex)
    }

    pub fn graph_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.graph.get_cell_value(sheet, row, col)
    }

    pub fn vertex_for_cell(&self, cell: &CellRef) -> Option<VertexId> {
        self.graph.get_vertex_for_cell(cell)
    }

    pub fn evaluation_vertices(&self) -> Vec<VertexId> {
        self.graph.get_evaluation_vertices()
    }

    pub fn set_first_load_assume_new(&mut self, enabled: bool) {
        self.graph.set_first_load_assume_new(enabled);
    }

    pub fn reset_ensure_touched(&mut self) {
        self.graph.reset_ensure_touched();
    }

    pub fn finalize_sheet_index(&mut self, sheet: &str) {
        self.graph.finalize_sheet_index(sheet);
    }

    pub fn edit_with_logger<T>(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        f: impl FnOnce(&mut crate::engine::VertexEditor) -> T,
    ) -> T {
        let mut editor = crate::engine::VertexEditor::with_logger(&mut self.graph, log);
        f(&mut editor)
    }

    pub fn undo_logged(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
        log: &mut crate::engine::ChangeLog,
    ) -> Result<(), crate::engine::EditorError> {
        let batch = undo.undo(&mut self.graph, log)?;
        self.mirror_undo_batch_to_arrow(&batch);
        Ok(())
    }

    pub fn redo_logged(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
        log: &mut crate::engine::ChangeLog,
    ) -> Result<(), crate::engine::EditorError> {
        let batch = undo.redo(&mut self.graph, log)?;
        self.mirror_redo_batch_to_arrow(&batch);
        Ok(())
    }

    fn cellref_to_sheet_row_col(&self, addr: &crate::reference::CellRef) -> (String, u32, u32) {
        let sheet = self.graph.sheet_name(addr.sheet_id).to_string();
        // Coord stores 0-based indices.
        let row = addr.coord.row() + 1;
        let col = addr.coord.col() + 1;
        (sheet, row, col)
    }

    fn mirror_undo_batch_to_arrow(
        &mut self,
        batch: &[crate::engine::graph::editor::undo_engine::UndoBatchItem],
    ) {
        // Undo applies inverses in reverse order.
        for item in batch.iter().rev() {
            self.mirror_inverse_change_to_arrow(&item.event);
        }
    }

    fn mirror_redo_batch_to_arrow(
        &mut self,
        batch: &[crate::engine::graph::editor::undo_engine::UndoBatchItem],
    ) {
        // Redo applies events in forward order.
        for item in batch.iter() {
            self.mirror_forward_change_to_arrow(&item.event);
        }
    }

    fn mirror_inverse_change_to_arrow(&mut self, ev: &crate::engine::ChangeEvent) {
        use crate::engine::ChangeEvent;
        use formualizer_common::LiteralValue;

        match ev {
            ChangeEvent::SetValue {
                addr,
                old_value,
                old_formula,
                ..
            } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                if old_formula.is_some() {
                    self.clear_delta_overlay_cell(&sheet, row, col);
                } else {
                    let v = old_value.clone().unwrap_or(LiteralValue::Empty);
                    self.mirror_value_to_overlay(&sheet, row, col, &v);
                }
            }
            ChangeEvent::SetFormula {
                addr,
                old_value,
                old_formula,
                ..
            } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                if old_formula.is_some() {
                    self.clear_delta_overlay_cell(&sheet, row, col);
                } else {
                    let v = old_value.clone().unwrap_or(LiteralValue::Empty);
                    self.mirror_value_to_overlay(&sheet, row, col, &v);
                }
            }
            ChangeEvent::SpillCommitted { old, new, .. } => {
                // Inverse: restore `old` (or clear if none).
                self.mirror_spill_snapshot(new, /*clear_only=*/ true);
                if let Some(snap) = old {
                    self.mirror_spill_snapshot(snap, /*clear_only=*/ false);
                }
            }
            ChangeEvent::SpillCleared { old, .. } => {
                // Inverse: restore prior spill.
                self.mirror_spill_snapshot(old, /*clear_only=*/ false);
            }
            _ => {}
        }
    }

    fn mirror_forward_change_to_arrow(&mut self, ev: &crate::engine::ChangeEvent) {
        use crate::engine::ChangeEvent;

        match ev {
            ChangeEvent::SetValue { addr, new, .. } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                self.mirror_value_to_overlay(&sheet, row, col, new);
            }
            ChangeEvent::SetFormula { addr, .. } => {
                let (sheet, row, col) = self.cellref_to_sheet_row_col(addr);
                self.clear_delta_overlay_cell(&sheet, row, col);
                // Keep any computed overlay for this cell as-is; it will be recomputed on demand.
            }
            ChangeEvent::SpillCommitted { old, new, .. } => {
                if let Some(snap) = old {
                    self.mirror_spill_snapshot(snap, /*clear_only=*/ true);
                }
                self.mirror_spill_snapshot(new, /*clear_only=*/ false);
            }
            ChangeEvent::SpillCleared { old, .. } => {
                self.mirror_spill_snapshot(old, /*clear_only=*/ true);
            }
            _ => {
                // Other graph structural operations do not have direct value effects in Arrow.
            }
        }
    }

    fn mirror_spill_snapshot(&mut self, snap: &crate::engine::graph::editor::change_log::SpillSnapshot, clear_only: bool) {
        use formualizer_common::LiteralValue;

        let mut i = 0usize;
        for row in &snap.values {
            for v in row {
                if let Some(cell) = snap.target_cells.get(i) {
                    let (sheet, r, c) = self.cellref_to_sheet_row_col(cell);
                    let out = if clear_only { LiteralValue::Empty } else { v.clone() };
                    self.mirror_value_to_computed_overlay(&sheet, r, c, &out);
                }
                i += 1;
            }
        }
        // If target_cells is longer than values (should not happen), clear remaining cells.
        if clear_only {
            for cell in snap.target_cells.iter().skip(i) {
                let (sheet, r, c) = self.cellref_to_sheet_row_col(cell);
                self.mirror_value_to_computed_overlay(&sheet, r, c, &LiteralValue::Empty);
            }
        }
    }

    pub fn set_default_sheet_by_name(&mut self, name: &str) {
        self.graph.set_default_sheet_by_name(name);
    }

    pub fn set_default_sheet_by_id(&mut self, id: SheetId) {
        self.graph.set_default_sheet_by_id(id);
    }

    pub fn set_sheet_index_mode(&mut self, mode: crate::engine::SheetIndexMode) {
        self.graph.set_sheet_index_mode(mode);
    }

    /// Mark data edited: bump snapshot and set edited flag
    pub fn mark_data_edited(&mut self) {
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
    }

    /// Access Arrow sheet store (read-only)
    pub fn sheet_store(&self) -> &SheetStore {
        &self.arrow_sheets
    }

    /// Access Arrow sheet store (mutable)
    pub fn sheet_store_mut(&mut self) -> &mut SheetStore {
        &mut self.arrow_sheets
    }

    /// Stage a formula text instead of inserting into the graph (used when deferring is enabled).
    pub fn stage_formula_text(&mut self, sheet: &str, row: u32, col: u32, text: String) {
        self.staged_formulas
            .entry(sheet.to_string())
            .or_default()
            .push((row, col, text));
    }

    /// Get a staged formula text for a given cell if present (cloned).
    pub fn get_staged_formula_text(&self, sheet: &str, row: u32, col: u32) -> Option<String> {
        self.staged_formulas.get(sheet).and_then(|v| {
            v.iter()
                .find(|(r, c, _)| *r == row && *c == col)
                .map(|(_, _, s)| s.clone())
        })
    }

    /// Build graph for all staged formulas.
    pub fn build_graph_all(&mut self) -> Result<(), formualizer_parse::ExcelError> {
        if self.staged_formulas.is_empty() {
            return Ok(());
        }
        // Take staged formulas before borrowing graph via builder
        let staged = std::mem::take(&mut self.staged_formulas);
        for sheet in staged.keys() {
            let _ = self.add_sheet(sheet);
        }
        let mut builder = self.begin_bulk_ingest();
        for (sheet, entries) in staged {
            let sid = builder.add_sheet(&sheet);
            // Parse with small cache for shared formulas
            let mut cache: rustc_hash::FxHashMap<String, formualizer_parse::ASTNode> =
                rustc_hash::FxHashMap::default();
            cache.reserve(4096);
            for (row, col, txt) in entries {
                let key = if txt.starts_with('=') {
                    txt
                } else {
                    format!("={txt}")
                };
                let ast = if let Some(p) = cache.get(&key) {
                    p.clone()
                } else {
                    let parsed = formualizer_parse::parser::parse(&key).map_err(|e| {
                        formualizer_parse::ExcelError::new(formualizer_parse::ExcelErrorKind::Value)
                            .with_message(e.to_string())
                    })?;
                    cache.insert(key.clone(), parsed.clone());
                    parsed
                };
                builder.add_formulas(sid, std::iter::once((row, col, ast)));
            }
        }
        let _ = builder.finish();
        Ok(())
    }

    /// Build graph for specific sheets (consuming only those staged entries).
    pub fn build_graph_for_sheets<'a, I: IntoIterator<Item = &'a str>>(
        &mut self,
        sheets: I,
    ) -> Result<(), formualizer_parse::ExcelError> {
        let mut any = false;
        // Collect entries first to avoid aliasing self while builder borrows graph
        struct SheetFormulas<'s> {
            sheet: &'s str,
            entries: Vec<(u32, u32, String)>,
        }
        let mut collected: Vec<SheetFormulas<'_>> = Vec::new();
        for s in sheets {
            if let Some(entries) = self.staged_formulas.remove(s) {
                collected.push(SheetFormulas { sheet: s, entries });
            }
        }
        for SheetFormulas { sheet, .. } in collected.iter() {
            let _ = self.add_sheet(sheet);
        }
        let mut builder = self.begin_bulk_ingest();
        // small parse cache per call
        let mut cache: rustc_hash::FxHashMap<String, formualizer_parse::ASTNode> =
            rustc_hash::FxHashMap::default();
        cache.reserve(4096);
        for SheetFormulas { sheet: s, entries } in collected.into_iter() {
            any = true;
            let sid = builder.add_sheet(s);
            for (row, col, txt) in entries {
                let key = if txt.starts_with('=') {
                    txt
                } else {
                    format!("={txt}")
                };
                let ast = if let Some(p) = cache.get(&key) {
                    p.clone()
                } else {
                    let parsed = formualizer_parse::parser::parse(&key).map_err(|e| {
                        formualizer_parse::ExcelError::new(formualizer_parse::ExcelErrorKind::Value)
                            .with_message(e.to_string())
                    })?;
                    cache.insert(key.clone(), parsed.clone());
                    parsed
                };
                builder.add_formulas(sid, std::iter::once((row, col, ast)));
            }
        }
        if any {
            let _ = builder.finish();
        }
        Ok(())
    }

    /// Begin bulk Arrow ingest for base values (Phase A)
    pub fn begin_bulk_ingest_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkIngestBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkIngestBuilder::new(self)
    }

    /// Begin bulk updates to Arrow store (Phase C)
    pub fn begin_bulk_update_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkUpdateBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkUpdateBuilder::new(self)
    }

    /// Insert rows (1-based) and mirror into Arrow store when enabled
    pub fn insert_rows(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.insert_rows(sheet_id, before, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before.saturating_sub(1) as usize;
            asheet.insert_rows(before0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Delete rows (1-based) and mirror into Arrow store when enabled
    pub fn delete_rows(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.delete_rows(sheet_id, start, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start.saturating_sub(1) as usize;
            asheet.delete_rows(start0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Insert columns (1-based) and mirror into Arrow store when enabled
    pub fn insert_columns(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.insert_columns(sheet_id, before, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before.saturating_sub(1) as usize;
            asheet.insert_columns(before0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Delete columns (1-based) and mirror into Arrow store when enabled
    pub fn delete_columns(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.delete_columns(sheet_id, start, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start.saturating_sub(1) as usize;
            asheet.delete_columns(start0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }
    /// Arrow-backed used row bounds across a column span (1-based inclusive cols).
    fn arrow_used_row_bounds(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sc0 = start_col.saturating_sub(1) as usize;
        let ec0 = end_col.saturating_sub(1) as usize;
        let col_hi = a.columns.len().saturating_sub(1);
        if sc0 > col_hi {
            return None;
        }
        let ec0 = ec0.min(col_hi);
        // Pass-scoped cache with snapshot guard
        let snap = self.data_snapshot_id();
        let mut min_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let sheet_id = self.graph.sheet_id(sheet)?;
            if let Some((Some(mv), _)) = self.row_bounds_cache.read().ok().and_then(|g| {
                g.as_ref()
                    .and_then(|c| c.get_row_bounds(sheet_id, ci, snap))
            }) {
                let mv = mv as usize;
                min_r0 = Some(min_r0.map(|m| m.min(mv)).unwrap_or(mv));
                continue;
            }
            // Compute and store
            let (min_c, max_c) = Self::scan_column_used_bounds(a, ci);
            if let Ok(mut g) = self.row_bounds_cache.write() {
                g.get_or_insert_with(|| RowBoundsCache::new(snap))
                    .put_row_bounds(sheet_id, ci, snap, (min_c, max_c));
            }
            if let Some(m) = min_c {
                min_r0 = Some(min_r0.map(|mm| mm.min(m as usize)).unwrap_or(m as usize));
            }
        }
        min_r0?;
        let mut max_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let sheet_id = self.graph.sheet_id(sheet)?;
            if let Some((_, Some(mv))) = self.row_bounds_cache.read().ok().and_then(|g| {
                g.as_ref()
                    .and_then(|c| c.get_row_bounds(sheet_id, ci, snap))
            }) {
                let mv = mv as usize;
                max_r0 = Some(max_r0.map(|m| m.max(mv)).unwrap_or(mv));
                continue;
            }
            let (_min_c, max_c) = Self::scan_column_used_bounds(a, ci);
            if let Ok(mut g) = self.row_bounds_cache.write() {
                g.get_or_insert_with(|| RowBoundsCache::new(snap))
                    .put_row_bounds(sheet_id, ci, snap, (_min_c, max_c));
            }
            if let Some(m) = max_c {
                max_r0 = Some(max_r0.map(|mm| mm.max(m as usize)).unwrap_or(m as usize));
            }
        }
        match (min_r0, max_r0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    fn scan_column_used_bounds(
        a: &crate::arrow_store::ArrowSheet,
        ci: usize,
    ) -> (Option<u32>, Option<u32>) {
        let col = &a.columns[ci];

        // Min: scan dense chunks first, then sparse chunks in ascending index order.
        let mut min_r0: Option<u32> = None;
        for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
            let tags = chunk.type_tag.values();
            for (off, &t) in tags.iter().enumerate() {
                let overlay_non_empty = chunk
                    .overlay
                    .get(off)
                    .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                    .unwrap_or(false)
                    || chunk
                        .computed_overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false);
                if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                    let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                        break;
                    };
                    let row0 = chunk_start + off;
                    min_r0 = Some(row0 as u32);
                    break;
                }
            }
            if min_r0.is_some() {
                break;
            }
        }
        if min_r0.is_none() && !col.sparse_chunks.is_empty() {
            let mut sparse_idxs: Vec<usize> = col.sparse_chunks.keys().copied().collect();
            sparse_idxs.sort_unstable();
            for chunk_idx in sparse_idxs {
                let Some(chunk) = col.sparse_chunks.get(&chunk_idx) else {
                    continue;
                };
                let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                    continue;
                };
                let tags = chunk.type_tag.values();
                for (off, &t) in tags.iter().enumerate() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(off)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let row0 = chunk_start + off;
                        min_r0 = Some(row0 as u32);
                        break;
                    }
                }
                if min_r0.is_some() {
                    break;
                }
            }
        }

        // Max: scan sparse chunks in descending index order, then dense chunks in reverse.
        let mut max_r0: Option<u32> = None;
        if !col.sparse_chunks.is_empty() {
            let mut sparse_idxs: Vec<usize> = col.sparse_chunks.keys().copied().collect();
            sparse_idxs.sort_unstable_by(|a, b| b.cmp(a));
            for chunk_idx in sparse_idxs {
                let Some(chunk) = col.sparse_chunks.get(&chunk_idx) else {
                    continue;
                };
                let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                    continue;
                };
                let tags = chunk.type_tag.values();
                for (rev_idx, &t) in tags.iter().enumerate().rev() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(rev_idx)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(rev_idx)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let row0 = chunk_start + rev_idx;
                        max_r0 = Some(row0 as u32);
                        break;
                    }
                }
                if max_r0.is_some() {
                    break;
                }
            }
        }
        if max_r0.is_none() {
            for (chunk_idx, chunk) in col.chunks.iter().enumerate().rev() {
                let tags = chunk.type_tag.values();
                for (rev_idx, &t) in tags.iter().enumerate().rev() {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(rev_idx)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(rev_idx)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                        let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                            break;
                        };
                        let row0 = chunk_start + rev_idx;
                        max_r0 = Some(row0 as u32);
                        break;
                    }
                }
                if max_r0.is_some() {
                    break;
                }
            }
        }

        (min_r0, max_r0)
    }

    /// Arrow-backed used column bounds across a row span (1-based inclusive rows).
    fn arrow_used_col_bounds(
        &self,
        sheet: &str,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sr0 = start_row.saturating_sub(1) as usize;
        let er0 = end_row.saturating_sub(1) as usize;
        if sr0 > er0 {
            return None;
        }
        // Map start/end rows into chunk ranges
        // We will scan each column for any non-empty within [sr0..=er0]
        let mut min_c0: Option<usize> = None;
        let mut max_c0: Option<usize> = None;
        // Precompute chunk bounds for row range
        for (ci, col) in a.columns.iter().enumerate() {
            let mut any_in_range = false;

            let scan_chunk = |chunk_idx: usize, chunk: &crate::arrow_store::ColumnChunk| -> bool {
                let Some(&chunk_start) = a.chunk_starts.get(chunk_idx) else {
                    return false;
                };
                let chunk_len = chunk.type_tag.len();
                if chunk_len == 0 {
                    return false;
                }
                let chunk_end = chunk_start + chunk_len.saturating_sub(1);
                // check intersection
                if sr0 > chunk_end || er0 < chunk_start {
                    return false;
                }
                let start_off = sr0.max(chunk_start) - chunk_start;
                let end_off = er0.min(chunk_end) - chunk_start;
                let tags = chunk.type_tag.values();
                for off in start_off..=end_off {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false)
                        || chunk
                            .computed_overlay
                            .get(off)
                            .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                            .unwrap_or(false);
                    if overlay_non_empty || tags[off] != crate::arrow_store::TypeTag::Empty as u8 {
                        return true;
                    }
                }
                false
            };

            for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
                if scan_chunk(chunk_idx, chunk) {
                    any_in_range = true;
                    break;
                }
            }

            if !any_in_range && !col.sparse_chunks.is_empty() {
                for (&chunk_idx, chunk) in col.sparse_chunks.iter() {
                    if scan_chunk(chunk_idx, chunk) {
                        any_in_range = true;
                        break;
                    }
                }
            }

            if any_in_range {
                min_c0 = Some(min_c0.map(|m| m.min(ci)).unwrap_or(ci));
                max_c0 = Some(max_c0.map(|m| m.max(ci)).unwrap_or(ci));
            }
        }
        match (min_c0, max_c0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    /// Mirror a single cell value into the Arrow overlay if enabled.
    /// Handles capacity growth, per-chunk overlay set, and heuristic compaction.
    fn mirror_value_to_overlay(&mut self, sheet: &str, row: u32, col: u32, value: &LiteralValue) {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return;
        }
        if self.arrow_sheets.sheet(sheet).is_none() {
            self.arrow_sheets
                .sheets
                .push(crate::arrow_store::ArrowSheet {
                    name: std::sync::Arc::<str>::from(sheet),
                    columns: Vec::new(),
                    nrows: 0,
                    chunk_starts: Vec::new(),
                    chunk_rows: 32 * 1024,
                });
        }

        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;

        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }

        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }
        if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
            use crate::arrow_store::OverlayValue;
            let ov = match value {
                LiteralValue::Empty => OverlayValue::Empty,
                LiteralValue::Int(i) => OverlayValue::Number(*i as f64),
                LiteralValue::Number(n) => OverlayValue::Number(*n),
                LiteralValue::Boolean(b) => OverlayValue::Boolean(*b),
                LiteralValue::Text(s) => OverlayValue::Text(std::sync::Arc::from(s.clone())),
                LiteralValue::Error(e) => {
                    OverlayValue::Error(crate::arrow_store::map_error_code(e.kind))
                }
                LiteralValue::Date(d) => {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                        self.config.date_system,
                        &dt,
                    );
                    OverlayValue::DateTime(serial)
                }
                LiteralValue::DateTime(dt) => {
                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                        self.config.date_system,
                        dt,
                    );
                    OverlayValue::DateTime(serial)
                }
                LiteralValue::Time(t) => {
                    let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                    OverlayValue::DateTime(serial)
                }
                LiteralValue::Duration(d) => {
                    let serial = d.num_seconds() as f64 / 86_400.0;
                    OverlayValue::Duration(serial)
                }
                LiteralValue::Pending => OverlayValue::Pending,
                LiteralValue::Array(_) => OverlayValue::Error(crate::arrow_store::map_error_code(
                    formualizer_common::ExcelErrorKind::Value,
                )),
            };
            if let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) {
                let _ = ch.overlay.set(in_off, ov);
                // A user edit must invalidate any computed (formula/spill) overlay entry at
                // this cell. Otherwise, if the delta overlay later compacts into the base lanes
                // (clearing `overlay`), a stale `computed_overlay=Empty` could incorrectly mask
                // the edited base value under the read cascade.
                let _ = ch.computed_overlay.remove(in_off);
            } else {
                return;
            }
            // Heuristic compaction: > len/50 or > 1024
            let abs_threshold = 1024usize;
            let frac_den = 50usize;
            let freed = asheet.maybe_compact_chunk(col0, ch_idx, abs_threshold, frac_den);
            if freed > 0 {
                self.overlay_compactions = self.overlay_compactions.saturating_add(1);
            }
        }
    }

    /// Remove a delta-overlay entry for a single cell (if present).
    ///
    /// This is used when transitioning a cell to a formula so that any previous user-edit overlay
    /// does not continue to mask computed overlay outputs.
    fn clear_delta_overlay_cell(&mut self, sheet: &str, row: u32, col: u32) {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return;
        }
        let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) else {
            return;
        };
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        if row0 >= asheet.nrows as usize {
            return;
        }
        if col0 >= asheet.columns.len() {
            return;
        }
        let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) else {
            return;
        };
        if let Some(ch) = asheet.columns[col0].chunk_mut(ch_idx) {
            let _ = ch.overlay.remove(in_off);
        }
    }

    /// Mirror a value into the computed overlay (formula/spill outputs).
    ///
    /// This path is subject to `EvalConfig.max_overlay_memory_bytes`.
    /// If the cap is exceeded, we deterministically stop mirroring additional computed overlay
    /// values and force RangeView resolution to materialize from graph values for correctness.
    fn mirror_value_to_computed_overlay(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }
        if self.computed_overlay_mirroring_disabled {
            return;
        }

        if self.arrow_sheets.sheet(sheet).is_none() {
            self.arrow_sheets
                .sheets
                .push(crate::arrow_store::ArrowSheet {
                    name: std::sync::Arc::<str>::from(sheet),
                    columns: Vec::new(),
                    nrows: 0,
                    chunk_starts: Vec::new(),
                    chunk_rows: 32 * 1024,
                });
        }

        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;

        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }

        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }

        if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
            use crate::arrow_store::OverlayValue;
            let ov = match value {
                LiteralValue::Empty => OverlayValue::Empty,
                LiteralValue::Int(i) => OverlayValue::Number(*i as f64),
                LiteralValue::Number(n) => OverlayValue::Number(*n),
                LiteralValue::Boolean(b) => OverlayValue::Boolean(*b),
                LiteralValue::Text(s) => OverlayValue::Text(std::sync::Arc::from(s.clone())),
                LiteralValue::Error(e) => {
                    OverlayValue::Error(crate::arrow_store::map_error_code(e.kind))
                }
                LiteralValue::Date(d) => {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                        self.config.date_system,
                        &dt,
                    );
                    OverlayValue::DateTime(serial)
                }
                LiteralValue::DateTime(dt) => {
                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                        self.config.date_system,
                        dt,
                    );
                    OverlayValue::DateTime(serial)
                }
                LiteralValue::Time(t) => {
                    let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                    OverlayValue::DateTime(serial)
                }
                LiteralValue::Duration(d) => {
                    let serial = d.num_seconds() as f64 / 86_400.0;
                    OverlayValue::Duration(serial)
                }
                LiteralValue::Pending => OverlayValue::Pending,
                LiteralValue::Array(_) => OverlayValue::Error(crate::arrow_store::map_error_code(
                    formualizer_common::ExcelErrorKind::Value,
                )),
            };

            let Some(ch) = asheet.ensure_column_chunk_mut(col0, ch_idx) else {
                return;
            };

            let delta = ch.computed_overlay.set(in_off, ov);
            self.adjust_computed_overlay_bytes(delta);

            if let Some(cap) = self.config.max_overlay_memory_bytes
                && self.computed_overlay_bytes_estimate > cap
            {
                self.disable_computed_overlay_mirroring_due_to_budget(cap);
            }
        }
    }

    #[inline]
    fn adjust_computed_overlay_bytes(&mut self, delta: isize) {
        if delta >= 0 {
            self.computed_overlay_bytes_estimate = self
                .computed_overlay_bytes_estimate
                .saturating_add(delta as usize);
        } else {
            self.computed_overlay_bytes_estimate = self
                .computed_overlay_bytes_estimate
                .saturating_sub((-delta) as usize);
        }
    }

    fn clear_all_computed_overlays(&mut self) {
        let mut freed_total = 0usize;
        for sh in self.arrow_sheets.sheets.iter_mut() {
            for col in sh.columns.iter_mut() {
                for ch in col.chunks.iter_mut() {
                    freed_total = freed_total.saturating_add(ch.computed_overlay.clear());
                }
                for ch in col.sparse_chunks.values_mut() {
                    freed_total = freed_total.saturating_add(ch.computed_overlay.clear());
                }
            }
        }
        self.computed_overlay_bytes_estimate = self
            .computed_overlay_bytes_estimate
            .saturating_sub(freed_total);
    }

    fn disable_computed_overlay_mirroring_due_to_budget(&mut self, _cap: usize) {
        // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
        // Handle budget pressure by compacting computed overlays into base lanes.
        self.compact_all_computed_overlays();
    }

    /// Fold all computed overlay entries across all sheets into their base arrays.
    /// This preserves data while freeing overlay memory, allowing mirroring to continue.
    fn compact_all_computed_overlays(&mut self) {
        let mut freed_total = 0usize;
        for sheet in self.arrow_sheets.sheets.iter_mut() {
            for col_idx in 0..sheet.columns.len() {
                // Dense chunks
                let num_dense = sheet.columns[col_idx].chunks.len();
                for ch_idx in 0..num_dense {
                    freed_total += sheet.compact_computed_overlay_chunk(col_idx, ch_idx);
                }
                // Sparse chunks
                let sparse_keys: Vec<usize> =
                    sheet.columns[col_idx].sparse_chunks.keys().copied().collect();
                for ch_idx in sparse_keys {
                    freed_total += sheet.compact_computed_overlay_sparse_chunk(col_idx, ch_idx);
                }
            }
        }
        self.computed_overlay_bytes_estimate = self
            .computed_overlay_bytes_estimate
            .saturating_sub(freed_total);
        self.overlay_compactions = self.overlay_compactions.saturating_add(1);
    }

    fn mirror_vertex_value_to_overlay(&mut self, vertex_id: VertexId, value: &LiteralValue) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }
        if !matches!(
            self.graph.get_vertex_kind(vertex_id),
            VertexKind::FormulaScalar | VertexKind::FormulaArray
        ) {
            return;
        }
        let Some(cell) = self.graph.get_cell_ref(vertex_id) else {
            return;
        };
        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
        self.mirror_value_to_computed_overlay(
            &sheet_name,
            cell.coord.row() + 1,
            cell.coord.col() + 1,
            value,
        );
    }

    /// Estimated memory usage for computed overlays (formula/spill mirroring).
    pub fn overlay_memory_usage(&self) -> usize {
        self.computed_overlay_bytes_estimate
    }

    fn resolve_sheet_locator_for_write(
        &mut self,
        loc: formualizer_common::SheetLocator<'_>,
        current_sheet: &str,
    ) -> Result<SheetId, ExcelError> {
        Ok(match loc {
            formualizer_common::SheetLocator::Id(id) => id,
            formualizer_common::SheetLocator::Name(name) => self.graph.sheet_id_mut(name.as_ref()),
            formualizer_common::SheetLocator::Current => self.graph.sheet_id_mut(current_sheet),
        })
    }

    fn resolve_sheet_locator_for_read(
        &self,
        loc: formualizer_common::SheetLocator<'_>,
        current_sheet: &str,
    ) -> Result<SheetId, ExcelError> {
        match loc {
            formualizer_common::SheetLocator::Id(id) => Ok(id),
            formualizer_common::SheetLocator::Name(name) => self
                .graph
                .sheet_id(name.as_ref())
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref)),
            formualizer_common::SheetLocator::Current => self
                .graph
                .sheet_id(current_sheet)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref)),
        }
    }

    /// Set a cell value
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        self.graph.set_cell_value(sheet, row, col, value.clone())?;
        // Mirror into Arrow overlay when enabled
        self.mirror_value_to_overlay(sheet, row, col, &value);
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(())
    }

    pub fn set_cell_value_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_write(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.set_cell_value(
            &sheet_name,
            owned.coord.row() + 1,
            owned.coord.col() + 1,
            value,
        )
    }

    pub fn set_cell_formula_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_write(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.set_cell_formula(
            &sheet_name,
            owned.coord.row() + 1,
            owned.coord.col() + 1,
            ast,
        )
    }

    pub fn get_cell_value_ref(
        &self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_read(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id);
        Ok(self.get_cell_value(sheet_name, owned.coord.row() + 1, owned.coord.col() + 1))
    }

    pub fn resolve_range_view_sheet_ref<'c>(
        &'c self,
        r: &formualizer_common::SheetRef<'_>,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        use formualizer_common::SheetLocator;

        let sheet_to_opt_name = |loc: SheetLocator<'_>| -> Result<Option<String>, ExcelError> {
            match loc {
                SheetLocator::Current => Ok(None),
                SheetLocator::Name(name) => Ok(Some(name.as_ref().to_string())),
                SheetLocator::Id(id) => Ok(Some(self.graph.sheet_name(id).to_string())),
            }
        };

        let rt = match r {
            formualizer_common::SheetRef::Cell(cell) => ReferenceType::Cell {
                sheet: sheet_to_opt_name(cell.sheet.clone())?,
                row: cell.coord.row() + 1,
                col: cell.coord.col() + 1,
                row_abs: cell.coord.row_abs(),
                col_abs: cell.coord.col_abs(),
            },
            formualizer_common::SheetRef::Range(range) => ReferenceType::Range {
                sheet: sheet_to_opt_name(range.sheet.clone())?,
                start_row: range.start_row.map(|b| b.index + 1),
                start_col: range.start_col.map(|b| b.index + 1),
                end_row: range.end_row.map(|b| b.index + 1),
                end_col: range.end_col.map(|b| b.index + 1),
                start_row_abs: range.start_row.map(|b| b.abs).unwrap_or(false),
                start_col_abs: range.start_col.map(|b| b.abs).unwrap_or(false),
                end_row_abs: range.end_row.map(|b| b.abs).unwrap_or(false),
                end_col_abs: range.end_col.map(|b| b.abs).unwrap_or(false),
            },
        };

        crate::traits::EvaluationContext::resolve_range_view(self, &rt, current_sheet)
    }

    /// Set a cell formula
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        let volatile = self.is_ast_volatile_with_provider(&ast);
        self.graph
            .set_cell_formula_with_volatility(sheet, row, col, ast, volatile)?;

        // If the cell previously held a user value in the delta overlay, it must not continue
        // to mask the formula result under Arrow-canonical reads (overlay precedence is
        // delta -> computed -> base). Remove the overlay entry instead of writing `Empty`,
        // because an explicit `Empty` overlay would still take precedence over computed values.
        self.clear_delta_overlay_cell(sheet, row, col);

        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(())
    }

    /// Bulk set many formulas on a sheet. Skips per-cell snapshot bumping and minimizes edge rebuilds.
    pub fn bulk_set_formulas<I>(&mut self, sheet: &str, items: I) -> Result<usize, ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let collected: Vec<(u32, u32, ASTNode)> = items.into_iter().collect();
        let vol_flags: Vec<bool> = collected
            .iter()
            .map(|(_, _, ast)| self.is_ast_volatile_with_provider(ast))
            .collect();
        let n = self
            .graph
            .bulk_set_formulas_with_volatility(sheet, collected, vol_flags)?;
        // Single snapshot bump after batch
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n > 0 {
            self.has_edited = true;
        }
        Ok(n)
    }

    #[inline]
    fn normalize_public_cell_read(v: LiteralValue) -> Option<LiteralValue> {
        match v {
            LiteralValue::Empty => None,
            LiteralValue::Int(i) => Some(LiteralValue::Number(i as f64)),
            other => Some(other),
        }
    }

    /// Get a cell value
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.read_cell_value(sheet, row, col)
            .and_then(Self::normalize_public_cell_read)
    }

    /// Unified internal read API for a single cell value (Arrow-truth).
    pub(crate) fn read_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        let asheet = self.sheet_store().sheet(sheet)?;
        let r0 = row.saturating_sub(1) as usize;
        let c0 = col.saturating_sub(1) as usize;
        let v = asheet.get_cell_value(r0, c0);
        if matches!(v, LiteralValue::Empty) {
            None
        } else {
            Some(v)
        }
    }

    /// Unified internal read API for a range of cell values (Arrow-truth).
    pub(crate) fn read_range_values(
        &self,
        sheet: &str,
        sr: u32,
        sc: u32,
        er: u32,
        ec: u32,
    ) -> RangeView<'_> {
        let Some(asheet) = self.sheet_store().sheet(sheet) else {
            return RangeView::from_owned_rows(Vec::new(), self.config.date_system);
        };
        if er < sr || ec < sc {
            return asheet.range_view(1, 1, 0, 0);
        }
        let sr0 = sr.saturating_sub(1) as usize;
        let sc0 = sc.saturating_sub(1) as usize;
        let er0 = er.saturating_sub(1) as usize;
        let ec0 = ec.saturating_sub(1) as usize;
        asheet.range_view(sr0, sc0, er0, ec0)
    }

    /// Get formula AST (if any) and current stored value for a cell
    pub fn get_cell(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Option<(Option<formualizer_parse::ASTNode>, Option<LiteralValue>)> {
        let v = self.get_cell_value(sheet, row, col);
        let sheet_id = self.graph.sheet_id(sheet)?;
        let coord = Coord::from_excel(row, col, true, true);
        let cell = CellRef::new(sheet_id, coord);
        let vid = self.graph.get_vertex_for_cell(&cell)?;
        let ast = self.graph.get_formula_id(vid).and_then(|ast_id| {
            self.graph
                .data_store()
                .retrieve_ast(ast_id, self.graph.sheet_reg())
        });
        Some((ast, v))
    }

    /// Begin batch operations - defer CSR rebuilds for better performance
    pub fn begin_batch(&mut self) {
        self.graph.begin_batch();
    }

    /// End batch operations and trigger CSR rebuild
    pub fn end_batch(&mut self) {
        self.graph.end_batch();
    }

    /// Evaluate a single vertex.
    /// This is the core of the sequential evaluation logic for Milestone 3.1.
    #[inline]
    fn record_cell_if_changed(
        delta: &mut DeltaCollector,
        cell: &CellRef,
        old: &LiteralValue,
        new: &LiteralValue,
    ) {
        if old != new {
            delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
        }
    }

    pub fn evaluate_vertex(&mut self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        self.evaluate_vertex_impl(vertex_id, None)
    }

    fn evaluate_vertex_impl(
        &mut self,
        vertex_id: VertexId,
        delta: Option<&mut DeltaCollector>,
    ) -> Result<LiteralValue, ExcelError> {
        let mut delta = delta;
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast_id = match kind {
            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                if let Some(ast_id) = self.graph.get_formula_id(vertex_id) {
                    ast_id
                } else {
                    return Ok(LiteralValue::Number(0.0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                if let Some(cell_ref) = self.graph.get_cell_ref(vertex_id) {
                    let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                    let row = cell_ref.coord.row() + 1;
                    let col = cell_ref.coord.col() + 1;
                    if let Some(v) = self.read_cell_value(sheet_name, row, col) {
                        return Ok(v);
                    }
                }
                return Ok(LiteralValue::Number(0.0));
            }
            VertexKind::NamedScalar => {
                let value = self.evaluate_named_scalar(vertex_id, sheet_id)?;
                return Ok(value);
            }
            VertexKind::NamedArray => {
                let value = self.evaluate_named_array(vertex_id, sheet_id)?;
                return Ok(value);
            }
            VertexKind::InfiniteRange
            | VertexKind::Range
            | VertexKind::External
            | VertexKind::Table => {
                // Not directly evaluatable here.
                return Ok(LiteralValue::Number(0.0));
            }
        };

        // The interpreter uses a reference to the engine as the context.
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);

        let result =
            interpreter.evaluate_arena_ast(ast_id, self.graph.data_store(), self.graph.sheet_reg());

        // If array result, perform spill from the anchor cell
        match result {
            Ok(cv) => {
                let result_literal = cv.into_literal();
                match result_literal {
                    LiteralValue::Array(rows) => {
                        // Update kind to FormulaArray for tracking
                        self.graph
                            .set_kind(vertex_id, crate::engine::vertex::VertexKind::FormulaArray);
                        // Build target cells rectangle starting from anchor
                        let anchor = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .expect("cell ref for vertex");
                        let sheet_id = anchor.sheet_id;
                        let h = rows.len() as u32;
                        let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;

                        // Hard cap to avoid vertex explosion from huge dynamic arrays.
                        let spill_cells = (h as u64).saturating_mul(w as u64);
                        if spill_cells > self.config.spill.max_spill_cells as u64 {
                            self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
                            let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                                .with_message("SpillTooLarge")
                                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                    expected_rows: h,
                                    expected_cols: w,
                                });
                            let spill_val = LiteralValue::Error(spill_err.clone());
                            if let Some(d) = delta.as_deref_mut() {
                                let old = self
                                    .read_cell_value(
                                        self.graph.sheet_name(anchor.sheet_id),
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                if old != spill_val {
                                    d.record_cell(
                                        anchor.sheet_id,
                                        anchor.coord.row(),
                                        anchor.coord.col(),
                                    );
                                }
                            }
                            self.graph.update_vertex_value(vertex_id, spill_val.clone());
                            if self.config.arrow_storage_enabled
                                && self.config.delta_overlay_enabled
                                && self.config.write_formula_overlay_enabled
                            {
                                let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                                self.mirror_value_to_computed_overlay(
                                    &sheet_name,
                                    anchor.coord.row() + 1,
                                    anchor.coord.col() + 1,
                                    &spill_val,
                                );
                            }
                            return Ok(spill_val);
                        }
                        // Bounds check to avoid out-of-range writes (align to AbsCoord capacity)
                        const PACKED_MAX_ROW: u32 = 1_048_575; // 20-bit max
                        const PACKED_MAX_COL: u32 = 16_383; // 14-bit max
                        let end_row = anchor.coord.row().saturating_add(h).saturating_sub(1);
                        let end_col = anchor.coord.col().saturating_add(w).saturating_sub(1);
                        if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
                            self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
                            let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                                .with_message("Spill exceeds sheet bounds")
                                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                    expected_rows: h,
                                    expected_cols: w,
                                });
                            let spill_val = LiteralValue::Error(spill_err.clone());
                            if let Some(d) = delta.as_deref_mut() {
                                let old = self
                                    .read_cell_value(
                                        self.graph.sheet_name(anchor.sheet_id),
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                if old != spill_val {
                                    d.record_cell(
                                        anchor.sheet_id,
                                        anchor.coord.row(),
                                        anchor.coord.col(),
                                    );
                                }
                            }
                            self.graph.update_vertex_value(vertex_id, spill_val.clone());
                            if self.config.arrow_storage_enabled
                                && self.config.delta_overlay_enabled
                                && self.config.write_formula_overlay_enabled
                            {
                                let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                                self.mirror_value_to_computed_overlay(
                                    &sheet_name,
                                    anchor.coord.row() + 1,
                                    anchor.coord.col() + 1,
                                    &spill_val,
                                );
                            }
                            return Ok(spill_val);
                        }
                        let mut targets = Vec::new();
                        for r in 0..h {
                            for c in 0..w {
                                targets.push(self.graph.make_cell_ref_internal(
                                    sheet_id,
                                    anchor.coord.row() + r,
                                    anchor.coord.col() + c,
                                ));
                            }
                        }

                        // Plan spill via spill manager shim
                        match self.spill_mgr.reserve(
                            vertex_id,
                            anchor,
                            SpillShape { rows: h, cols: w },
                            SpillMeta {
                                epoch: self.recalc_epoch,
                                config: self.config.spill,
                            },
                        ) {
                            Ok(()) => {
                                // Commit: write values to grid
                                // Default conflict policy is Error + FirstWins; reserve() enforces in-flight locks
                                // and plan_spill_region enforces overlap with committed formulas/spills/values.
                                if let Err(e) = self.commit_spill_and_mirror(
                                    vertex_id,
                                    &targets,
                                    rows.clone(),
                                    delta.as_deref_mut(),
                                    None,
                                ) {
                                    // If commit fails, mark as error
                                    self.clear_spill_projection_and_mirror(
                                        vertex_id,
                                        delta.as_deref_mut(),
                                    );
                                    if let Some(d) = delta.as_deref_mut() {
                                        let old = self
                                            .read_cell_value(
                                                self.graph.sheet_name(anchor.sheet_id),
                                                anchor.coord.row() + 1,
                                                anchor.coord.col() + 1,
                                            )
                                            .unwrap_or(LiteralValue::Empty);
                                        let new = LiteralValue::Error(e.clone());
                                        if old != new {
                                            d.record_cell(
                                                anchor.sheet_id,
                                                anchor.coord.row(),
                                                anchor.coord.col(),
                                            );
                                        }
                                    }
                                    let err_val = LiteralValue::Error(e.clone());
                                    self.graph.update_vertex_value(vertex_id, err_val.clone());
                                    if self.config.arrow_storage_enabled
                                        && self.config.delta_overlay_enabled
                                        && self.config.write_formula_overlay_enabled
                                    {
                                        let sheet_name =
                                            self.graph.sheet_name(anchor.sheet_id).to_string();
                                        self.mirror_value_to_computed_overlay(
                                            &sheet_name,
                                            anchor.coord.row() + 1,
                                            anchor.coord.col() + 1,
                                            &err_val,
                                        );
                                    }
                                    return Ok(err_val);
                                }
                                // Anchor shows the top-left value, like Excel
                                let top_left = rows
                                    .first()
                                    .and_then(|r| r.first())
                                    .cloned()
                                    .unwrap_or(LiteralValue::Empty);
                                self.graph.update_vertex_value(vertex_id, top_left.clone());
                                Ok(top_left)
                            }
                            Err(e) => {
                                self.clear_spill_projection_and_mirror(
                                    vertex_id,
                                    delta.as_deref_mut(),
                                );
                                let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                                    .with_message(
                                        e.message.unwrap_or_else(|| "Spill blocked".to_string()),
                                    )
                                    .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                        expected_rows: h,
                                        expected_cols: w,
                                    });
                                let spill_val = LiteralValue::Error(spill_err.clone());
                                if let Some(d) = delta.as_deref_mut() {
                                    let old = self
                                        .read_cell_value(
                                            self.graph.sheet_name(anchor.sheet_id),
                                            anchor.coord.row() + 1,
                                            anchor.coord.col() + 1,
                                        )
                                        .unwrap_or(LiteralValue::Empty);
                                    if old != spill_val {
                                        d.record_cell(
                                            anchor.sheet_id,
                                            anchor.coord.row(),
                                            anchor.coord.col(),
                                        );
                                    }
                                }
                                self.graph.update_vertex_value(vertex_id, spill_val.clone());
                                if self.config.arrow_storage_enabled
                                    && self.config.delta_overlay_enabled
                                    && self.config.write_formula_overlay_enabled
                                {
                                    let sheet_name =
                                        self.graph.sheet_name(anchor.sheet_id).to_string();
                                    self.mirror_value_to_computed_overlay(
                                        &sheet_name,
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                        &spill_val,
                                    );
                                }
                                Ok(spill_val)
                            }
                        }
                    }
                    other => {
                        // Scalar result: store value and ensure any previous spill is cleared
                        let spill_cells = self
                            .graph
                            .spill_cells_for_anchor(vertex_id)
                            .map(|cells| cells.to_vec())
                            .unwrap_or_default();
                        if let Some(d) = delta.as_deref_mut()
                            && let Some(anchor) = self.graph.get_cell_ref_for_vertex(vertex_id)
                        {
                            if spill_cells.is_empty() {
                                let old = self
                                    .read_cell_value(
                                        self.graph.sheet_name(anchor.sheet_id),
                                        anchor.coord.row() + 1,
                                        anchor.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                if old != other {
                                    d.record_cell(
                                        anchor.sheet_id,
                                        anchor.coord.row(),
                                        anchor.coord.col(),
                                    );
                                }
                            } else {
                                for cell in spill_cells.iter() {
                                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                                    let old = self
                                        .get_cell_value(
                                            sheet_name,
                                            cell.coord.row() + 1,
                                            cell.coord.col() + 1,
                                        )
                                        .unwrap_or(LiteralValue::Empty);
                                    let new = if cell.sheet_id == anchor.sheet_id
                                        && cell.coord.row() == anchor.coord.row()
                                        && cell.coord.col() == anchor.coord.col()
                                    {
                                        other.clone()
                                    } else {
                                        LiteralValue::Empty
                                    };
                                    Self::record_cell_if_changed(d, cell, &old, &new);
                                }
                            }
                        }
                        self.graph.clear_spill_region(vertex_id);
                        if self.config.arrow_storage_enabled
                            && self.config.delta_overlay_enabled
                            && self.config.write_formula_overlay_enabled
                        {
                            let empty = LiteralValue::Empty;
                            for cell in spill_cells.iter() {
                                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                                self.mirror_value_to_computed_overlay(
                                    &sheet_name,
                                    cell.coord.row() + 1,
                                    cell.coord.col() + 1,
                                    &empty,
                                );
                            }
                        }
                        self.graph.update_vertex_value(vertex_id, other.clone());
                        // Optionally mirror into Arrow overlay for Arrow-backed reads
                        if self.config.arrow_storage_enabled
                            && self.config.delta_overlay_enabled
                            && self.config.write_formula_overlay_enabled
                        {
                            let anchor = self
                                .graph
                                .get_cell_ref(vertex_id)
                                .expect("cell ref for vertex");
                            let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                            self.mirror_value_to_computed_overlay(
                                &sheet_name,
                                anchor.coord.row() + 1,
                                anchor.coord.col() + 1,
                                &other,
                            );
                        }
                        Ok(other)
                    }
                }
            }
            Err(e) => {
                // Runtime Excel error: store as a cell value instead of propagating
                // as an exception so bulk eval paths don't fail the whole pass.
                let spill_cells = self
                    .graph
                    .spill_cells_for_anchor(vertex_id)
                    .map(|cells| cells.to_vec())
                    .unwrap_or_default();
                let err_val = LiteralValue::Error(e.clone());
                if let Some(d) = delta
                    && let Some(anchor) = self.graph.get_cell_ref_for_vertex(vertex_id)
                {
                    if spill_cells.is_empty() {
                        let old = self
                            .read_cell_value(
                                self.graph.sheet_name(anchor.sheet_id),
                                anchor.coord.row() + 1,
                                anchor.coord.col() + 1,
                            )
                            .unwrap_or(LiteralValue::Empty);
                        if old != err_val {
                            d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                        }
                    } else {
                        for cell in spill_cells.iter() {
                            let sheet_name = self.graph.sheet_name(cell.sheet_id);
                            let old = self
                                .get_cell_value(
                                    sheet_name,
                                    cell.coord.row() + 1,
                                    cell.coord.col() + 1,
                                )
                                .unwrap_or(LiteralValue::Empty);
                            let new = if cell.sheet_id == anchor.sheet_id
                                && cell.coord.row() == anchor.coord.row()
                                && cell.coord.col() == anchor.coord.col()
                            {
                                err_val.clone()
                            } else {
                                LiteralValue::Empty
                            };
                            Self::record_cell_if_changed(d, cell, &old, &new);
                        }
                    }
                }
                self.graph.clear_spill_region(vertex_id);
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let empty = LiteralValue::Empty;
                    for cell in spill_cells.iter() {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_computed_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
                self.graph.update_vertex_value(vertex_id, err_val.clone());
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let anchor = self
                        .graph
                        .get_cell_ref(vertex_id)
                        .expect("cell ref for vertex");
                    let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                    self.mirror_value_to_computed_overlay(
                        &sheet_name,
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                        &err_val,
                    );
                }
                Ok(err_val)
            }
        }
    }

    fn evaluate_named_scalar(
        &mut self,
        vertex_id: VertexId,
        sheet_id: SheetId,
    ) -> Result<LiteralValue, ExcelError> {
        let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name)
                .with_message("Named range metadata missing".to_string())
        })?;

        match &named_range.definition {
            NamedDefinition::Cell(cell_ref) => {
                let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                let row = cell_ref.coord.row() + 1;
                let col = cell_ref.coord.col() + 1;

                if let Some(dep_vertex) = self.graph.get_vertex_for_cell(cell_ref)
                    && matches!(
                        self.graph.get_vertex_kind(dep_vertex),
                        VertexKind::FormulaScalar | VertexKind::FormulaArray
                    )
                {
                    // Graph does not cache cell/formula values; ensure the precedent is evaluated.
                    let value = self.evaluate_vertex(dep_vertex)?;
                    self.graph.update_vertex_value(vertex_id, value.clone());
                    Ok(value)
                } else {
                    let value = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    self.graph.update_vertex_value(vertex_id, value.clone());
                    Ok(value)
                }
            }
            NamedDefinition::Literal(v) => {
                let out = v.clone();
                self.graph.update_vertex_value(vertex_id, out.clone());
                Ok(out)
            }
            NamedDefinition::Formula { ast, .. } => {
                let context_sheet = match named_range.scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => sheet_id,
                };
                let sheet_name = self.graph.sheet_name(context_sheet);
                let cell_ref = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                match interpreter.evaluate_ast(ast) {
                    Ok(cv) => {
                        let value = cv.into_literal();
                        match value {
                            LiteralValue::Array(_) => {
                                let err = ExcelError::new(ExcelErrorKind::NImpl)
                                    .with_message("Array result in scalar named range".to_string());
                                let err_val = LiteralValue::Error(err.clone());
                                self.graph.update_vertex_value(vertex_id, err_val.clone());
                                Ok(err_val)
                            }
                            other => {
                                self.graph.update_vertex_value(vertex_id, other.clone());
                                Ok(other)
                            }
                        }
                    }
                    Err(err) => {
                        let err_val = LiteralValue::Error(err.clone());
                        self.graph.update_vertex_value(vertex_id, err_val.clone());
                        Ok(err_val)
                    }
                }
            }
            NamedDefinition::Range(_) => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Range-valued name evaluated as scalar".to_string())),
        }
    }

    fn evaluate_named_array(
        &mut self,
        vertex_id: VertexId,
        sheet_id: SheetId,
    ) -> Result<LiteralValue, ExcelError> {
        let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name)
                .with_message("Named range metadata missing".to_string())
        })?;

        let out = match &named_range.definition {
            NamedDefinition::Range(range_ref) => {
                if range_ref.start.sheet_id != range_ref.end.sheet_id {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Named range cannot span sheets".to_string()));
                }

                let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                let sr0 = range_ref.start.coord.row();
                let sc0 = range_ref.start.coord.col();
                let er0 = range_ref.end.coord.row();
                let ec0 = range_ref.end.coord.col();
                if sr0 > er0 || sc0 > ec0 {
                    return Err(ExcelError::new(ExcelErrorKind::Ref)
                        .with_message("Invalid named range bounds".to_string()));
                }

                let h = (er0 - sr0 + 1) as usize;
                let w = (ec0 - sc0 + 1) as usize;
                let cell_count = (h as u64).saturating_mul(w as u64);
                if cell_count > self.config.spill.max_spill_cells as u64 {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Named range too large to materialize as an array".to_string(),
                    ));
                }

                let mut rows = Vec::with_capacity(h);
                for r0 in sr0..=er0 {
                    let mut row = Vec::with_capacity(w);
                    for c0 in sc0..=ec0 {
                        let v = self
                            .get_cell_value(sheet_name, r0 + 1, c0 + 1)
                            .unwrap_or(LiteralValue::Empty);
                        row.push(v);
                    }
                    rows.push(row);
                }
                LiteralValue::Array(rows)
            }
            NamedDefinition::Cell(cell_ref) => {
                let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                let row = cell_ref.coord.row() + 1;
                let col = cell_ref.coord.col() + 1;
                let v = self
                    .get_cell_value(sheet_name, row, col)
                    .unwrap_or(LiteralValue::Empty);
                LiteralValue::Array(vec![vec![v]])
            }
            NamedDefinition::Literal(v) => LiteralValue::Array(vec![vec![v.clone()]]),
            NamedDefinition::Formula { ast, .. } => {
                let context_sheet = match named_range.scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => sheet_id,
                };
                let sheet_name = self.graph.sheet_name(context_sheet);
                let cell_ref = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                match interpreter.evaluate_ast(ast) {
                    Ok(cv) => {
                        let v = cv.into_literal();
                        match v {
                            LiteralValue::Array(_) => v,
                            other => LiteralValue::Array(vec![vec![other]]),
                        }
                    }
                    Err(err) => LiteralValue::Error(err),
                }
            }
        };

        self.graph.update_vertex_value(vertex_id, out.clone());
        Ok(out)
    }

    /// Evaluate only the necessary precedents for specific target cells (demand-driven)
    pub fn evaluate_until(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<EvalResult, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_until", targets = targets.len()).entered();
        let start = web_time::Instant::now();
        let _source_cache = self.source_cache_session();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            // For now, assume simple A1-style references on default sheet
            // TODO: Parse complex references with sheets
            let sheet_id = self.graph.sheet_id_mut(sheet);
            let coord = Coord::from_excel(*row, *col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Build demand subgraph with virtual edges for compressed ranges
        #[cfg(feature = "tracing")]
        let _span_sub = tracing::info_span!("demand_subgraph_build").entered();
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);
        #[cfg(feature = "tracing")]
        drop(_span_sub);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule for the minimal subgraph, honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        #[cfg(feature = "tracing")]
        let _span_sched =
            tracing::info_span!("schedule_build", vertices = precedents_to_eval.len()).entered();
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;
        #[cfg(feature = "tracing")]
        drop(_span_sched);

        // Handle cycles first
        let mut cycle_errors = 0;
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        // Evaluate layers (parallel when enabled, mirroring evaluate_all)
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(layer)?;
            }
        }

        // Clear warmup context at end of evaluation

        // Clear dirty flags for evaluated vertices
        self.graph.clear_dirty_flags(&precedents_to_eval);

        // Re-dirty volatile vertices
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    fn evaluate_until_with_delta_collector(
        &mut self,
        targets: &[(&str, u32, u32)],
        delta: &mut DeltaCollector,
    ) -> Result<EvalResult, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span_eval =
            tracing::info_span!("evaluate_until_with_delta", targets = targets.len()).entered();
        let start = web_time::Instant::now();
        let _source_cache = self.source_cache_session();

        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            let sheet_id = self.graph.sheet_id_mut(sheet);
            let coord = Coord::from_excel(*row, *col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        let mut cycle_errors = 0;
        let circ_error = LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string()),
        );
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            for &vertex_id in cycle {
                if delta.mode != DeltaMode::Off
                    && let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id)
                {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if old != circ_error {
                        delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel_with_delta(layer, delta)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential_with_delta(layer, delta)?;
            }
        }

        self.graph.clear_dirty_flags(&precedents_to_eval);
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Build a reusable evaluation plan that covers every formula vertex in the workbook.
    pub fn build_recalc_plan(&self) -> Result<RecalcPlan, ExcelError> {
        let mut vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();
        vertices.sort_unstable();
        if vertices.is_empty() {
            return Ok(RecalcPlan {
                schedule: crate::engine::Schedule {
                    layers: Vec::new(),
                    cycles: Vec::new(),
                },
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&vertices)?;
        Ok(RecalcPlan { schedule })
    }

    /// Evaluate using a previously constructed plan. This avoids rebuilding layer schedules for each run.
    pub fn evaluate_recalc_plan(&mut self, plan: &RecalcPlan) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }

        let start = web_time::Instant::now();
        let dirty_vertices = self.graph.get_evaluation_vertices();
        if dirty_vertices.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        let dirty_set: FxHashSet<VertexId> = dirty_vertices.iter().copied().collect();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        if !plan.schedule.cycles.is_empty() {
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for cycle in &plan.schedule.cycles {
                if !cycle.iter().any(|v| dirty_set.contains(v)) {
                    continue;
                }
                cycle_errors += 1;
                for &vertex_id in cycle {
                    if dirty_set.contains(&vertex_id) {
                        self.graph
                            .update_vertex_value(vertex_id, circ_error.clone());
                        self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
                    }
                }
            }
        }

        for layer in &plan.schedule.layers {
            let work: Vec<VertexId> = layer
                .vertices
                .iter()
                .copied()
                .filter(|v| dirty_set.contains(v))
                .collect();
            if work.is_empty() {
                continue;
            }
            let temp_layer = crate::engine::scheduler::Layer { vertices: work };
            if self.thread_pool.is_some() && temp_layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(&temp_layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(&temp_layer)?;
            }
        }

        self.graph.clear_dirty_flags(&dirty_vertices);
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        if self.config.defer_graph_building {
            // Build graph for all staged formulas before evaluating
            self.build_graph_all()?;
        }
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_all").entered();
        let start = web_time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        // Handle cycles first by marking them with #CIRC!
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        // Evaluate acyclic layers (parallel or sequential based on config)
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(layer)?;
            }
        }

        // Clear dirty flags for all evaluated vertices (including cycles)
        self.graph.clear_dirty_flags(&to_evaluate);

        // Re-dirty volatile vertices for the next evaluation cycle
        self.graph.redirty_volatiles();

        // Advance recalc epoch after a full evaluation pass finishes
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    pub fn evaluate_all_with_delta(&mut self) -> Result<(EvalResult, EvalDelta), ExcelError> {
        let mut collector = DeltaCollector::new(DeltaMode::Cells);
        let result = self.evaluate_all_with_delta_collector(&mut collector)?;
        Ok((result, collector.finish()))
    }

    fn evaluate_all_with_delta_collector(
        &mut self,
        delta: &mut DeltaCollector,
    ) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_all_with_delta").entered();
        let start = web_time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        let circ_error = LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string()),
        );
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            for &vertex_id in cycle {
                if delta.mode != DeltaMode::Off
                    && let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id)
                {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if old != circ_error {
                        delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel_with_delta(layer, delta)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential_with_delta(layer, delta)?;
            }
        }

        self.graph.clear_dirty_flags(&to_evaluate);
        self.graph.redirty_volatiles();
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Convenience: demand-driven evaluation of a single cell by sheet name and row/col.
    ///
    /// This will evaluate only the minimal set of dirty / volatile precedents required
    /// to bring the target cell up-to-date (as if a user asked for that single value),
    /// rather than scheduling a full workbook recalc. If the cell is already clean and
    /// non-volatile, no vertices will be recomputed.
    ///
    /// Returns the (possibly newly computed) value stored for the cell afterwards.
    /// Empty cells return None. Errors are surfaced via the Result type.
    pub fn evaluate_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        if row == 0 || col == 0 {
            return Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Row and column must be >= 1".to_string()));
        }

        if self.config.defer_graph_building {
            self.build_graph_for_sheets(std::iter::once(sheet))?;
        }

        let result = self.evaluate_cells(&[(sheet, row, col)])?;

        match result.len() {
            0 => Ok(None),
            1 => {
                let v = result.into_iter().next().unwrap();
                Ok(v)
            }
            _ => unreachable!("evaluate_cells returned unexpected length"),
        }
    }

    /// Convenience: demand-driven evaluation of multiple cells; accepts a slice of
    /// (sheet, row, col) triples. The union of required dirty / volatile precedents
    /// is computed once and evaluated, which is typically faster than calling
    /// `evaluate_cell` repeatedly for a related set of targets.
    ///
    /// Returns the resulting values for each requested target in the same order.
    pub fn evaluate_cells(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.validate_deterministic_mode()?;
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if self.config.defer_graph_building {
            let mut sheets: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for (s, _, _) in targets.iter() {
                sheets.insert(*s);
            }
            self.build_graph_for_sheets(sheets.iter().cloned())?;
        }
        self.evaluate_until(targets)?;
        Ok(targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect())
    }

    pub fn evaluate_cells_cancellable(
        &mut self,
        targets: &[(&str, u32, u32)],
        cancel_flag: Arc<AtomicBool>,
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.active_cancel_flag = Some(cancel_flag.clone());
        let res = self.evaluate_cells_cancellable_impl(targets, &cancel_flag);
        self.active_cancel_flag = None;
        res
    }

    fn evaluate_cells_cancellable_impl(
        &mut self,
        targets: &[(&str, u32, u32)],
        cancel_flag: &AtomicBool,
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        self.validate_deterministic_mode()?;
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if self.config.defer_graph_building {
            let mut sheets: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for (s, _, _) in targets.iter() {
                sheets.insert(*s);
            }
            self.build_graph_for_sheets(sheets.iter().cloned())?;
        }

        // evaluate_until_cancellable takes &[&str] in A1 notation, but we have (&str, u32, u32)
        // Let's implement evaluate_until_coords_cancellable or similar, or just convert
        let a1_targets: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| {
                format!("{}!{}", s, col_letters_from_1based(*c).unwrap()) + &r.to_string()
            })
            .collect();
        let a1_refs: Vec<&str> = a1_targets.iter().map(|s| s.as_str()).collect();

        self.evaluate_until_cancellable_impl(&a1_refs, cancel_flag)?;

        Ok(targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect())
    }

    pub fn evaluate_cells_with_delta(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<(Vec<Option<LiteralValue>>, EvalDelta), ExcelError> {
        self.validate_deterministic_mode()?;
        if targets.is_empty() {
            return Ok((Vec::new(), EvalDelta::default()));
        }
        if self.config.defer_graph_building {
            let mut sheets: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for (s, _, _) in targets.iter() {
                sheets.insert(*s);
            }
            self.build_graph_for_sheets(sheets.iter().cloned())?;
        }
        let mut collector = DeltaCollector::new(DeltaMode::Cells);
        self.evaluate_until_with_delta_collector(targets, &mut collector)?;
        let values = targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect();
        Ok((values, collector.finish()))
    }

    /// Get the evaluation plan for target cells without actually evaluating them
    pub fn get_eval_plan(&self, targets: &[(&str, u32, u32)]) -> Result<EvalPlan, ExcelError> {
        if targets.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: Vec::new(),
            });
        }
        if self.config.defer_graph_building {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                "Evaluation plan requested with deferred graph; build first or call evaluate_*",
            ));
        }

        // Convert targets to A1 notation for consistency
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            if let Some(sheet_id) = self.graph.sheet_id(sheet) {
                let coord = Coord::from_excel(*row, *col, true, true);
                target_addrs.push(CellRef::new(sheet_id, coord));
            }
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Build demand subgraph with virtual edges (same as evaluate_until)
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Count dirty and volatile vertices
        let mut dirty_count = 0;
        let mut volatile_count = 0;
        for &vertex_id in &precedents_to_eval {
            if self.graph.is_dirty(vertex_id) {
                dirty_count += 1;
            }
            if self.graph.is_volatile(vertex_id) {
                volatile_count += 1;
            }
        }

        // Create schedule for the minimal subgraph honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        // Build layer information
        let mut layers = Vec::new();
        let mut estimated_parallel_layers = 0;
        let parallel_enabled = self.config.enable_parallel && self.thread_pool.is_some();

        for layer in &schedule.layers {
            let parallel_eligible = parallel_enabled && layer.vertices.len() > 1;
            if parallel_eligible {
                estimated_parallel_layers += 1;
            }

            // Get sample cell addresses (up to 5)
            let sample_cells: Vec<String> = layer
                .vertices
                .iter()
                .take(5)
                .filter_map(|&vertex_id| {
                    self.graph
                        .get_cell_ref_for_vertex(vertex_id)
                        .map(|cell_ref| {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            format!(
                                "{}!{}{}",
                                sheet_name,
                                Self::col_to_letters(cell_ref.coord.col()),
                                cell_ref.coord.row() + 1
                            )
                        })
                })
                .collect();

            layers.push(LayerInfo {
                vertex_count: layer.vertices.len(),
                parallel_eligible,
                sample_cells,
            });
        }

        Ok(EvalPlan {
            total_vertices_to_evaluate: precedents_to_eval.len(),
            layers,
            cycles_detected: schedule.cycles.len(),
            dirty_count,
            volatile_count,
            parallel_enabled,
            estimated_parallel_layers,
            target_cells: addresses,
        })
    }

    /// Build a demand-driven subgraph for the given targets, including ephemeral edges for
    /// compressed ranges, and returning the set of dirty/volatile precedents and virtual deps.
    fn build_demand_subgraph(
        &self,
        target_vertices: &[VertexId],
    ) -> (
        Vec<VertexId>,
        rustc_hash::FxHashMap<VertexId, Vec<VertexId>>,
    ) {
        #[cfg(feature = "tracing")]
        let _span =
            tracing::info_span!("demand_subgraph", targets = target_vertices.len()).entered();
        use rustc_hash::{FxHashMap, FxHashSet};

        let mut to_evaluate: FxHashSet<VertexId> = FxHashSet::default();
        let mut visited: FxHashSet<VertexId> = FxHashSet::default();
        let mut stack: Vec<VertexId> = Vec::new();
        let mut vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default(); // incoming deps per vertex

        for &t in target_vertices {
            stack.push(t);
        }

        while let Some(v) = stack.pop() {
            if !visited.insert(v) {
                continue;
            }
            if !self.graph.vertex_exists(v) {
                continue;
            }
            // Only schedule dirty/volatile formulas
            match self.graph.get_vertex_kind(v) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                    if self.graph.is_dirty(v) || self.graph.is_volatile(v) {
                        to_evaluate.insert(v);
                    }
                }
                _ => {}
            }

            // Explicit dependencies (graph edges)
            for dep in self.graph.get_dependencies(v) {
                if self.graph.vertex_exists(dep) {
                    match self.graph.get_vertex_kind(dep) {
                        VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                            if !visited.contains(&dep) {
                                stack.push(dep);
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Compressed range dependencies → discover formula precedents in used/bounded window
            if let Some(ranges) = self.graph.get_range_dependencies(v) {
                let current_sheet_id = self.graph.get_vertex_sheet_id(v);
                for r in ranges {
                    let sheet_id = match r.sheet {
                        formualizer_common::SheetLocator::Id(id) => id,
                        _ => current_sheet_id,
                    };
                    let sheet_name = self.graph.sheet_name(sheet_id);

                    // Infer bounds like resolve_range_view (r bounds are 0-based)
                    let mut sr = r.start_row.map(|b| b.index + 1);
                    let mut sc = r.start_col.map(|b| b.index + 1);
                    let mut er = r.end_row.map(|b| b.index + 1);
                    let mut ec = r.end_col.map(|b| b.index + 1);

                    if sr.is_none() && er.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((min_r, max_r)) =
                            self.used_rows_for_columns(sheet_name, scv, ecv)
                        {
                            sr = Some(min_r);
                            er = Some(max_r);
                        } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                            sr = Some(1);
                            er = Some(self.config.max_open_ended_rows);
                        }
                    }
                    if sc.is_none() && ec.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((min_c, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv)
                        {
                            sc = Some(min_c);
                            ec = Some(max_c);
                        } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                            sc = Some(1);
                            ec = Some(self.config.max_open_ended_cols);
                        }
                    }
                    if sr.is_some() && er.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                            er = Some(max_r);
                        } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                            er = Some(self.config.max_open_ended_rows);
                        }
                    }
                    if er.is_some() && sr.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((min_r, _)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                            sr = Some(min_r);
                        } else {
                            sr = Some(1);
                        }
                    }
                    if sc.is_some() && ec.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                            ec = Some(max_c);
                        } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                            ec = Some(self.config.max_open_ended_cols);
                        }
                    }
                    if ec.is_some() && sc.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((min_c, _)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                            sc = Some(min_c);
                        } else {
                            sc = Some(1);
                        }
                    }

                    let sr = sr.unwrap_or(1);
                    let sc = sc.unwrap_or(1);
                    let er = er.unwrap_or(sr.saturating_sub(1));
                    let ec = ec.unwrap_or(sc.saturating_sub(1));
                    if er < sr || ec < sc {
                        continue;
                    }

                    if let Some(index) = self.graph.sheet_index(sheet_id) {
                        let sr0 = sr.saturating_sub(1);
                        let er0 = er.saturating_sub(1);
                        let sc0 = sc.saturating_sub(1);
                        let ec0 = ec.saturating_sub(1);
                        // enumerate vertices in col range, filter row and kind
                        for u in index.vertices_in_col_range(sc0, ec0) {
                            let pc = self.graph.vertex_coord(u);
                            let row0 = pc.row();
                            if row0 < sr0 || row0 > er0 {
                                continue;
                            }
                            match self.graph.get_vertex_kind(u) {
                                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                                    // only link and schedule if producer is dirty/volatile
                                    if (self.graph.is_dirty(u) || self.graph.is_volatile(u))
                                        && u != v
                                    {
                                        vdeps.entry(v).or_default().push(u);
                                        if !visited.contains(&u) {
                                            stack.push(u);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        // Dedup virtual deps
        for deps in vdeps.values_mut() {
            deps.sort_unstable();
            deps.dedup();
        }
        (result, vdeps)
    }

    /// Helper: convert 1-based column index to Excel-style letters (1 -> A, 27 -> AA)
    fn col_to_letters(col: u32) -> String {
        col_letters_from_1based(col).expect("column index must be >= 1")
    }

    /// Evaluate all dirty/volatile vertices with cancellation support
    pub fn evaluate_all_cancellable(
        &mut self,
        cancel_flag: Arc<AtomicBool>,
    ) -> Result<EvalResult, ExcelError> {
        self.active_cancel_flag = Some(cancel_flag.clone());
        let res = self.evaluate_all_cancellable_impl(&cancel_flag);
        self.active_cancel_flag = None;
        res
    }

    fn evaluate_all_cancellable_impl(
        &mut self,
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        self.validate_deterministic_mode()?;
        let start = web_time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        // Handle cycles first by marking them with #CIRC!
        for cycle in &schedule.cycles {
            // Check cancellation between cycles
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled during cycle handling".to_string()));
            }

            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        // Evaluate acyclic layers sequentially with cancellation checks
        for layer in &schedule.layers {
            // Check cancellation between layers
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled between layers".to_string()));
            }

            // Evaluate vertices in this layer (parallel or sequential)
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices +=
                    self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
            } else {
                computed_vertices +=
                    self.evaluate_layer_sequential_cancellable(layer, cancel_flag)?;
            }
        }

        // Clear dirty flags for all evaluated vertices (including cycles)
        self.graph.clear_dirty_flags(&to_evaluate);

        // Re-dirty volatile vertices for the next evaluation cycle
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate only the necessary precedents for specific target cells with cancellation support
    pub fn evaluate_until_cancellable(
        &mut self,
        targets: &[&str],
        cancel_flag: Arc<AtomicBool>,
    ) -> Result<EvalResult, ExcelError> {
        self.active_cancel_flag = Some(cancel_flag.clone());
        let res = self.evaluate_until_cancellable_impl(targets, &cancel_flag);
        self.active_cancel_flag = None;
        res
    }

    fn evaluate_until_cancellable_impl(
        &mut self,
        targets: &[&str],
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = web_time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for target in targets {
            let (sheet, row, col) = self.parse_a1_notation(target)?;
            let sheet_id = self.graph.sheet_id_mut(&sheet);
            let coord = Coord::from_excel(row, col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Build demand subgraph with virtual edges
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        // Handle cycles first
        let mut cycle_errors = 0;
        for cycle in &schedule.cycles {
            // Check cancellation between cycles
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                    "Demand-driven evaluation cancelled during cycle handling".to_string(),
                ));
            }

            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        // Evaluate layers with cancellation checks
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            // Check cancellation between layers
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                    "Demand-driven evaluation cancelled between layers".to_string(),
                ));
            }

            // Evaluate vertices in this layer (parallel or sequential)
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices +=
                    self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
            } else {
                computed_vertices +=
                    self.evaluate_layer_sequential_cancellable_demand_driven(layer, cancel_flag)?;
            }
        }

        // Clear dirty flags for evaluated vertices
        self.graph.clear_dirty_flags(&precedents_to_eval);

        // Re-dirty volatile vertices
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    fn parse_a1_notation(&self, address: &str) -> Result<(String, u32, u32), ExcelError> {
        let mut parts = address.splitn(2, '!');
        let first = parts.next().unwrap_or_default();
        let remainder = parts.next();

        let (sheet, cell_part) = match remainder {
            Some(cell) => (first.to_string(), cell),
            None => (self.default_sheet_name().to_string(), first),
        };

        let (row, col, _, _) = parse_a1_1based(cell_part).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Invalid cell reference `{cell_part}`: {err}"))
        })?;

        Ok((sheet, row, col))
    }

    /// Determine volatility using this engine's FunctionProvider, falling back to global registry.
    fn is_ast_volatile_with_provider(&self, ast: &ASTNode) -> bool {
        use formualizer_parse::parser::ASTNodeType;
        match &ast.node_type {
            ASTNodeType::Function { name, args, .. } => {
                if let Some(func) = self
                    .get_function("", name)
                    .or_else(|| crate::function_registry::get("", name))
                    && func.caps().contains(crate::function::FnCaps::VOLATILE)
                {
                    return true;
                }
                args.iter()
                    .any(|arg| self.is_ast_volatile_with_provider(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_volatile_with_provider(left)
                    || self.is_ast_volatile_with_provider(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_volatile_with_provider(expr),
            ASTNodeType::Array(rows) => rows.iter().any(|row| {
                row.iter()
                    .any(|cell| self.is_ast_volatile_with_provider(cell))
            }),
            _ => false,
        }
    }

    /// Find dirty precedents that need evaluation for the given target vertices
    fn find_dirty_precedents(&self, target_vertices: &[VertexId]) -> Vec<VertexId> {
        let mut to_evaluate = FxHashSet::default();
        let mut visited = FxHashSet::default();
        let mut stack = Vec::new();

        // Start reverse traversal from target vertices
        for &target in target_vertices {
            stack.push(target);
        }

        while let Some(vertex_id) = stack.pop() {
            if !visited.insert(vertex_id) {
                continue; // Already processed
            }

            if self.graph.vertex_exists(vertex_id) {
                // Check if this vertex needs evaluation
                let kind = self.graph.get_vertex_kind(vertex_id);
                let needs_eval = match kind {
                    super::vertex::VertexKind::FormulaScalar
                    | super::vertex::VertexKind::FormulaArray => {
                        self.graph.is_dirty(vertex_id) || self.graph.is_volatile(vertex_id)
                    }
                    _ => false, // Values and empty cells don't need evaluation
                };

                if needs_eval {
                    to_evaluate.insert(vertex_id);
                }

                // Continue traversal to dependencies (precedents)
                let dependencies = self.graph.get_dependencies(vertex_id);
                for &dep_id in &dependencies {
                    if !visited.contains(&dep_id) {
                        stack.push(dep_id);
                    }
                }
            }
        }

        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        result
    }

    /// Evaluate a layer sequentially
    fn evaluate_layer_sequential(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_sequential_effects(layer)
    }

    fn update_vertex_value_with_delta(
        &mut self,
        vertex_id: VertexId,
        new_value: LiteralValue,
        delta: &mut DeltaCollector,
    ) {
        if delta.mode != DeltaMode::Off
            && let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id)
        {
            let sheet_name = self.graph.sheet_name(cell.sheet_id);
            let old = self
                .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                .unwrap_or(LiteralValue::Empty);
            if old != new_value {
                delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
            }
        }
        self.graph.update_vertex_value(vertex_id, new_value.clone());
        self.mirror_vertex_value_to_overlay(vertex_id, &new_value);
    }

    fn evaluate_layer_sequential_with_delta(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_sequential_with_delta_effects(layer, delta)
    }

    /// Evaluate a layer sequentially with cancellation support
    fn evaluate_layer_sequential_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_sequential_cancellable_effects(layer, cancel_flag)
    }

    /// Evaluate a layer sequentially with more frequent cancellation checks for demand-driven evaluation
    fn evaluate_layer_sequential_cancellable_demand_driven(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_sequential_cancellable_demand_driven_effects(layer, cancel_flag)
    }

    /// Evaluate a layer in parallel using the thread pool
    fn evaluate_layer_parallel(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_parallel_effects(layer)
    }

    fn evaluate_layer_parallel_with_delta(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_parallel_with_delta_effects(layer, delta)
    }

    /// Evaluate a layer in parallel with cancellation support
    fn evaluate_layer_parallel_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        self.evaluate_layer_parallel_cancellable_effects(layer, cancel_flag)
    }

    /// Apply a computed result produced by `evaluate_vertex_immutable()`.
    ///
    /// This is the parallel equivalent of the "apply" portion of `evaluate_vertex_impl`.
    /// We keep apply sequential for correctness (spill commit is inherently stateful).
    fn apply_parallel_vertex_result(
        &mut self,
        vertex_id: VertexId,
        result: LiteralValue,
        mut delta: Option<&mut DeltaCollector>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        // If this vertex's cell is currently covered by a spill from a different anchor,
        // ignore the computed result. The spill's committed values own the grid.
        if let Some(cell) = self.graph.get_cell_ref(vertex_id)
            && let Some(owner) = self.graph.spill_registry_anchor_for_cell(cell)
            && owner != vertex_id
        {
            return Ok(());
        }

        let kind = self.graph.get_vertex_kind(vertex_id);

        // Only formula vertices spill dynamic arrays into the grid.
        let is_formula = matches!(kind, VertexKind::FormulaScalar | VertexKind::FormulaArray);
        if is_formula {
            match result {
                LiteralValue::Array(rows) => {
                    self.apply_array_result_from_parallel(
                        vertex_id,
                        rows,
                        delta.as_deref_mut(),
                        overwritable_formulas,
                    )?;
                }
                other => {
                    self.apply_non_array_result_from_parallel(
                        vertex_id,
                        other,
                        delta.as_deref_mut(),
                    );
                }
            }
            return Ok(());
        }

        // Non-formula vertices: store value as-is (arrays remain arrays; no spill).
        if let Some(d) = delta.as_deref_mut() {
            self.update_vertex_value_with_delta(vertex_id, result, d);
        } else {
            self.graph.update_vertex_value(vertex_id, result.clone());
            self.mirror_vertex_value_to_overlay(vertex_id, &result);
        }
        Ok(())
    }

    fn apply_non_array_result_from_parallel(
        &mut self,
        vertex_id: VertexId,
        value: LiteralValue,
        mut delta: Option<&mut DeltaCollector>,
    ) {
        // Scalar/error result: store value and ensure any previous spill is cleared.
        // This mirrors the sequential behavior in `evaluate_vertex_impl`.
        let spill_cells = self
            .graph
            .spill_cells_for_anchor(vertex_id)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();

        if let Some(d) = delta.as_deref_mut()
            && d.mode != DeltaMode::Off
            && let Some(anchor) = self.graph.get_cell_ref_for_vertex(vertex_id)
        {
            if spill_cells.is_empty() {
                let old = self
                    .read_cell_value(
                        self.graph.sheet_name(anchor.sheet_id),
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                    )
                    .unwrap_or(LiteralValue::Empty);
                if old != value {
                    d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                }
            } else {
                for cell in spill_cells.iter() {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    let new = if cell.sheet_id == anchor.sheet_id
                        && cell.coord.row() == anchor.coord.row()
                        && cell.coord.col() == anchor.coord.col()
                    {
                        value.clone()
                    } else {
                        LiteralValue::Empty
                    };
                    Self::record_cell_if_changed(d, cell, &old, &new);
                }
            }
        }

        self.graph.clear_spill_region(vertex_id);

        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &empty,
                );
            }
        }

        self.graph.update_vertex_value(vertex_id, value.clone());
        self.mirror_vertex_value_to_overlay(vertex_id, &value);
    }

    fn apply_array_result_from_parallel(
        &mut self,
        vertex_id: VertexId,
        rows: Vec<Vec<LiteralValue>>,
        mut delta: Option<&mut DeltaCollector>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        // Keep behavior consistent with the sequential spill path in `evaluate_vertex_impl`.
        self.graph
            .set_kind(vertex_id, crate::engine::vertex::VertexKind::FormulaArray);

        let anchor = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let sheet_id = anchor.sheet_id;
        let h = rows.len() as u32;
        let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;

        // Hard cap to avoid vertex explosion from huge dynamic arrays.
        let spill_cells = (h as u64).saturating_mul(w as u64);
        if spill_cells > self.config.spill.max_spill_cells as u64 {
            self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
            let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                .with_message("SpillTooLarge")
                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                    expected_rows: h,
                    expected_cols: w,
                });
            let spill_val = LiteralValue::Error(spill_err.clone());
            if let Some(d) = delta.as_deref_mut()
                && d.mode != DeltaMode::Off
            {
                let old = self
                    .read_cell_value(
                        self.graph.sheet_name(anchor.sheet_id),
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                    )
                    .unwrap_or(LiteralValue::Empty);
                if old != spill_val {
                    d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                }
            }
            self.graph.update_vertex_value(vertex_id, spill_val.clone());
            self.mirror_vertex_value_to_overlay(vertex_id, &spill_val);
            return Ok(());
        }

        // Bounds check to avoid out-of-range writes (align to AbsCoord capacity)
        const PACKED_MAX_ROW: u32 = 1_048_575; // 20-bit max
        const PACKED_MAX_COL: u32 = 16_383; // 14-bit max
        let end_row = anchor.coord.row().saturating_add(h).saturating_sub(1);
        let end_col = anchor.coord.col().saturating_add(w).saturating_sub(1);
        if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
            self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
            let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                .with_message("Spill exceeds sheet bounds")
                .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                    expected_rows: h,
                    expected_cols: w,
                });
            let spill_val = LiteralValue::Error(spill_err.clone());
            if let Some(d) = delta.as_deref_mut()
                && d.mode != DeltaMode::Off
            {
                let old = self
                    .read_cell_value(
                        self.graph.sheet_name(anchor.sheet_id),
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                    )
                    .unwrap_or(LiteralValue::Empty);
                if old != spill_val {
                    d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                }
            }
            self.graph.update_vertex_value(vertex_id, spill_val.clone());
            self.mirror_vertex_value_to_overlay(vertex_id, &spill_val);
            return Ok(());
        }

        let mut targets = Vec::new();
        for r in 0..h {
            for c in 0..w {
                targets.push(self.graph.make_cell_ref_internal(
                    sheet_id,
                    anchor.coord.row() + r,
                    anchor.coord.col() + c,
                ));
            }
        }

        match self.spill_mgr.reserve(
            vertex_id,
            anchor,
            SpillShape { rows: h, cols: w },
            SpillMeta {
                epoch: self.recalc_epoch,
                config: self.config.spill,
            },
        ) {
            Ok(()) => {
                if let Err(e) = self.commit_spill_and_mirror(
                    vertex_id,
                    &targets,
                    rows.clone(),
                    delta.as_deref_mut(),
                    overwritable_formulas,
                ) {
                    self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
                    let err_val = LiteralValue::Error(e.clone());
                    if let Some(d) = delta.as_deref_mut()
                        && d.mode != DeltaMode::Off
                    {
                        let old = self
                            .read_cell_value(
                                self.graph.sheet_name(anchor.sheet_id),
                                anchor.coord.row() + 1,
                                anchor.coord.col() + 1,
                            )
                            .unwrap_or(LiteralValue::Empty);
                        if old != err_val {
                            d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                        }
                    }
                    self.graph.update_vertex_value(vertex_id, err_val.clone());
                    self.mirror_vertex_value_to_overlay(vertex_id, &err_val);
                    return Ok(());
                }

                // Anchor shows the top-left value, like Excel
                let top_left = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                self.graph.update_vertex_value(vertex_id, top_left.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &top_left);
                Ok(())
            }
            Err(e) => {
                self.clear_spill_projection_and_mirror(vertex_id, delta.as_deref_mut());
                let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                    .with_message(e.message.unwrap_or_else(|| "Spill blocked".to_string()))
                    .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                        expected_rows: h,
                        expected_cols: w,
                    });
                let spill_val = LiteralValue::Error(spill_err.clone());
                if let Some(d) = delta.as_deref_mut()
                    && d.mode != DeltaMode::Off
                {
                    let old = self
                        .read_cell_value(
                            self.graph.sheet_name(anchor.sheet_id),
                            anchor.coord.row() + 1,
                            anchor.coord.col() + 1,
                        )
                        .unwrap_or(LiteralValue::Empty);
                    if old != spill_val {
                        d.record_cell(anchor.sheet_id, anchor.coord.row(), anchor.coord.col());
                    }
                }
                self.graph.update_vertex_value(vertex_id, spill_val.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &spill_val);
                Ok(())
            }
        }
    }

    /// Evaluate a single vertex without mutating the graph (for parallel evaluation)
    fn evaluate_vertex_immutable(&self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast_id = match kind {
            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                if let Some(ast_id) = self.graph.get_formula_id(vertex_id) {
                    ast_id
                } else {
                    return Ok(LiteralValue::Number(0.0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                if let Some(cell_ref) = self.graph.get_cell_ref(vertex_id) {
                    let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                    let row = cell_ref.coord.row() + 1;
                    let col = cell_ref.coord.col() + 1;
                    if let Some(v) = self.read_cell_value(sheet_name, row, col) {
                        return Ok(v);
                    }
                }
                return Ok(LiteralValue::Number(0.0));
            }
            VertexKind::NamedScalar => {
                let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message("Named range metadata missing".to_string())
                })?;

                return match &named_range.definition {
                    NamedDefinition::Cell(cell_ref) => {
                        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                        Ok(self
                            .get_cell_value(
                                sheet_name,
                                cell_ref.coord.row() + 1,
                                cell_ref.coord.col() + 1,
                            )
                            .unwrap_or(LiteralValue::Empty))
                    }
                    NamedDefinition::Literal(v) => Ok(v.clone()),
                    NamedDefinition::Formula { ast, .. } => {
                        let context_sheet = match named_range.scope {
                            NameScope::Sheet(id) => id,
                            NameScope::Workbook => sheet_id,
                        };
                        let sheet_name = self.graph.sheet_name(context_sheet);
                        let cell_ref = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                        interpreter.evaluate_ast(ast).map(|cv| cv.into_literal())
                    }
                    NamedDefinition::Range(_) => Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Range-valued name evaluated as scalar".to_string())),
                };
            }
            VertexKind::NamedArray => {
                let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message("Named range metadata missing".to_string())
                })?;

                return match &named_range.definition {
                    NamedDefinition::Range(range_ref) => {
                        if range_ref.start.sheet_id != range_ref.end.sheet_id {
                            return Err(ExcelError::new(ExcelErrorKind::Ref)
                                .with_message("Named range cannot span sheets".to_string()));
                        }
                        let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                        let sr0 = range_ref.start.coord.row();
                        let sc0 = range_ref.start.coord.col();
                        let er0 = range_ref.end.coord.row();
                        let ec0 = range_ref.end.coord.col();
                        if sr0 > er0 || sc0 > ec0 {
                            return Err(ExcelError::new(ExcelErrorKind::Ref)
                                .with_message("Invalid named range bounds".to_string()));
                        }

                        let h = (er0 - sr0 + 1) as usize;
                        let w = (ec0 - sc0 + 1) as usize;
                        let cell_count = (h as u64).saturating_mul(w as u64);
                        if cell_count > self.config.spill.max_spill_cells as u64 {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "Named range too large to materialize as an array".to_string(),
                            ));
                        }

                        let mut rows = Vec::with_capacity(h);
                        for r0 in sr0..=er0 {
                            let mut row = Vec::with_capacity(w);
                            for c0 in sc0..=ec0 {
                                let v = self
                                    .get_cell_value(sheet_name, r0 + 1, c0 + 1)
                                    .unwrap_or(LiteralValue::Empty);
                                row.push(v);
                            }
                            rows.push(row);
                        }
                        Ok(LiteralValue::Array(rows))
                    }
                    NamedDefinition::Cell(cell_ref) => {
                        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                        let row = cell_ref.coord.row() + 1;
                        let col = cell_ref.coord.col() + 1;
                        let v = self
                            .get_cell_value(sheet_name, row, col)
                            .unwrap_or(LiteralValue::Empty);
                        Ok(LiteralValue::Array(vec![vec![v]]))
                    }
                    NamedDefinition::Literal(v) => Ok(LiteralValue::Array(vec![vec![v.clone()]])),
                    NamedDefinition::Formula { ast, .. } => {
                        let context_sheet = match named_range.scope {
                            NameScope::Sheet(id) => id,
                            NameScope::Workbook => sheet_id,
                        };
                        let sheet_name = self.graph.sheet_name(context_sheet);
                        let cell_ref = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                        match interpreter.evaluate_ast(ast) {
                            Ok(cv) => {
                                let v = cv.into_literal();
                                match v {
                                    LiteralValue::Array(_) => Ok(v),
                                    other => Ok(LiteralValue::Array(vec![vec![other]])),
                                }
                            }
                            Err(err) => Ok(LiteralValue::Error(err)),
                        }
                    }
                };
            }
            VertexKind::InfiniteRange
            | VertexKind::Range
            | VertexKind::External
            | VertexKind::Table => {
                // Not directly evaluatable here.
                return Ok(LiteralValue::Number(0.0));
            }
        };

        // The interpreter uses a reference to the engine as the context
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);

        interpreter
            .evaluate_arena_ast(ast_id, self.graph.data_store(), self.graph.sheet_reg())
            .map(|cv| cv.into_literal())
    }

    /// Get access to the shared thread pool for parallel evaluation
    pub fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }
}

#[derive(Default)]
struct RowBoundsCache {
    snapshot: u64,
    // key: (sheet_id, col_idx)
    map: rustc_hash::FxHashMap<(u32, usize), (Option<u32>, Option<u32>)>,
}

impl RowBoundsCache {
    fn new(snapshot: u64) -> Self {
        Self {
            snapshot,
            map: Default::default(),
        }
    }
    fn get_row_bounds(
        &self,
        sheet_id: SheetId,
        col_idx: usize,
        snapshot: u64,
    ) -> Option<(Option<u32>, Option<u32>)> {
        if self.snapshot != snapshot {
            return None;
        }
        self.map.get(&(sheet_id as u32, col_idx)).copied()
    }
    fn put_row_bounds(
        &mut self,
        sheet_id: SheetId,
        col_idx: usize,
        snapshot: u64,
        bounds: (Option<u32>, Option<u32>),
    ) {
        if self.snapshot != snapshot {
            self.snapshot = snapshot;
            self.map.clear();
        }
        self.map.insert((sheet_id as u32, col_idx), bounds);
    }
}

// Phase 2 shim: in-process spill manager delegating to current graph methods.
#[derive(Default)]
pub struct ShimSpillManager {
    region_locks: RegionLockManager,
    pub(crate) active_locks: rustc_hash::FxHashMap<VertexId, u64>,
}

impl ShimSpillManager {
    pub(crate) fn reserve(
        &mut self,
        owner: VertexId,
        anchor_cell: CellRef,
        shape: SpillShape,
        _meta: SpillMeta,
    ) -> Result<(), ExcelError> {
        // Derive region from anchor + shape; enforce in-flight exclusivity only.
        let region = crate::engine::spill::Region {
            sheet_id: anchor_cell.sheet_id as u32,
            row_start: anchor_cell.coord.row(),
            row_end: anchor_cell
                .coord
                .row()
                .saturating_add(shape.rows)
                .saturating_sub(1),
            col_start: anchor_cell.coord.col(),
            col_end: anchor_cell
                .coord
                .col()
                .saturating_add(shape.cols)
                .saturating_sub(1),
        };
        match self.region_locks.reserve(region, owner) {
            Ok(id) => {
                if id != 0 {
                    self.active_locks.insert(owner, id);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn commit_array_with_value_probe<F>(
        &mut self,
        graph: &mut DependencyGraph,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
        mut value_probe: F,
    ) -> Result<(), ExcelError>
    where
        F: FnMut(&DependencyGraph, &CellRef) -> Option<LiteralValue>,
    {
        use formualizer_common::{ExcelErrorExtra, ExcelErrorKind};

        // Re-run plan on concrete targets before committing to respect blockers.
        // This plan checks formula/spill ownership in the graph, but when the graph value cache
        // is disabled (Arrow-canonical mode), it cannot see non-empty value blockers.
        let plan_res = graph.plan_spill_region_allowing_formula_overwrite(
            anchor_vertex,
            targets,
            overwritable_formulas,
        );
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        if !graph.value_cache_enabled() {
            // Compute expected spill shape from the target rectangle for diagnostics.
            let (expected_rows, expected_cols) = if targets.is_empty() {
                (0u32, 0u32)
            } else {
                let mut min_r = u32::MAX;
                let mut max_r = 0u32;
                let mut min_c = u32::MAX;
                let mut max_c = 0u32;
                for cell in targets {
                    let r = cell.coord.row();
                    let c = cell.coord.col();
                    min_r = min_r.min(r);
                    max_r = max_r.max(r);
                    min_c = min_c.min(c);
                    max_c = max_c.max(c);
                }
                (
                    max_r.saturating_sub(min_r).saturating_add(1),
                    max_c.saturating_sub(min_c).saturating_add(1),
                )
            };

            let anchor_cell = graph
                .get_cell_ref(anchor_vertex)
                .expect("anchor cell ref for spill commit");

            for cell in targets {
                // Never treat the anchor as a blocker.
                if *cell == anchor_cell {
                    continue;
                }
                // Skip cells already known to be owned by a spill; plan() handled spill conflicts.
                if graph.spill_registry_anchor_for_cell(*cell).is_some() {
                    continue;
                }
                // Skip formula vertices in the target region; plan() handled them (or allowed).
                if let Some(&vid) = graph.get_vertex_id_for_address(cell)
                    && vid != anchor_vertex
                {
                    match graph.get_vertex_kind(vid) {
                        crate::engine::vertex::VertexKind::FormulaScalar
                        | crate::engine::vertex::VertexKind::FormulaArray => {
                            // plan() already approved allowed overwrites.
                            continue;
                        }
                        _ => {}
                    }
                }

                if let Some(v) = value_probe(graph, cell)
                    && !matches!(v, LiteralValue::Empty)
                {
                    if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                        self.region_locks.release(id);
                    }
                    return Err(ExcelError::new(ExcelErrorKind::Spill)
                        .with_message("BlockedByValue")
                        .with_extra(ExcelErrorExtra::Spill {
                            expected_rows,
                            expected_cols,
                        }));
                }
            }
        }

        let commit_res = graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows,
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())
    }

    /// Commit a spill and mirror all written cells into Arrow overlay via the owning engine.
    pub(crate) fn commit_array_with_overlay<R: EvaluationContext>(
        &mut self,
        engine: &mut Engine<R>,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        // Re-run plan on concrete targets before committing to respect blockers.
        let plan_res = engine.graph.plan_spill_region_allowing_formula_overwrite(
            anchor_vertex,
            targets,
            overwritable_formulas,
        );
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        let commit_res = engine.graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows.clone(),
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())?;

        // Mirror into Arrow overlay when enabled
        if engine.config.arrow_storage_enabled
            && engine.config.delta_overlay_enabled
            && engine.config.write_formula_overlay_enabled
        {
            // Expect targets to be a contiguous rectangle row-major starting at some anchor
            for (idx, cell) in targets.iter().enumerate() {
                let (r_off, c_off) = {
                    if rows.is_empty() || rows[0].is_empty() {
                        (0usize, 0usize)
                    } else {
                        let width = rows[0].len();
                        (idx / width, idx % width)
                    }
                };
                let v = rows
                    .get(r_off)
                    .and_then(|r| r.get(c_off))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let sheet_name = engine.graph.sheet_name(cell.sheet_id).to_string();
                engine.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &v,
                );
            }
        }
        Ok(())
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_shared_ref(
        &self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<formualizer_common::SheetRef<'static>, ExcelError> {
        use formualizer_common::{
            SheetCellRef as SharedCellRef, SheetLocator, SheetRangeRef as SharedRangeRef,
            SheetRef as SharedRef,
        };

        // Preserve anchor flags from the parsed reference when possible.
        let sr = match reference {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                let row0 = row
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let col0 = col
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let coord = formualizer_common::RelativeCoord::new(row0, col0, *row_abs, *col_abs);
                SharedRef::Cell(SharedCellRef::new(sheet_loc, coord))
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let sr = start_row
                    .map(|r| {
                        r.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let sc = start_col
                    .map(|c| {
                        c.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let er = end_row
                    .map(|r| {
                        r.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let ec = end_col
                    .map(|c| {
                        c.checked_sub(1)
                            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                    })
                    .transpose()?;
                let range = SharedRangeRef::from_parts(
                    sheet_loc,
                    sr.map(|idx| formualizer_common::AxisBound::new(idx, *start_row_abs)),
                    sc.map(|idx| formualizer_common::AxisBound::new(idx, *start_col_abs)),
                    er.map(|idx| formualizer_common::AxisBound::new(idx, *end_row_abs)),
                    ec.map(|idx| formualizer_common::AxisBound::new(idx, *end_col_abs)),
                )
                .map_err(|_| ExcelError::new(ExcelErrorKind::Ref))?;
                SharedRef::Range(range)
            }
            _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
        };

        let current_id = self
            .graph
            .sheet_id(current_sheet)
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;

        let resolve_loc = |loc: SheetLocator<'_>| -> Result<SheetLocator<'static>, ExcelError> {
            match loc {
                SheetLocator::Current => Ok(SheetLocator::Id(current_id)),
                SheetLocator::Id(id) => Ok(SheetLocator::Id(id)),
                SheetLocator::Name(name) => {
                    let n = name.as_ref();
                    self.graph
                        .sheet_id(n)
                        .map(SheetLocator::Id)
                        .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                }
            }
        };

        match sr {
            SharedRef::Cell(cell) => {
                let owned = cell.into_owned();
                let sheet = resolve_loc(owned.sheet)?;
                Ok(SharedRef::Cell(SharedCellRef::new(sheet, owned.coord)))
            }
            SharedRef::Range(range) => {
                let owned = range.into_owned();
                let sheet = resolve_loc(owned.sheet)?;
                Ok(SharedRef::Range(SharedRangeRef {
                    sheet,
                    start_row: owned.start_row,
                    start_col: owned.start_col,
                    end_row: owned.end_row,
                    end_col: owned.end_col,
                }))
            }
        }
    }
}

// Implement the resolver traits for the Engine.
// This allows the interpreter to resolve references by querying the engine's graph.
impl<R> crate::traits::ReferenceResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        let sheet_name = sheet.unwrap_or_else(|| self.default_sheet_name()); // FIXME: should use formula current-sheet context
        // Prefer engine's unified accessor which consults Arrow store for base values
        // and falls back to graph for formulas and stored values.
        if let Some(v) = self.get_cell_value(sheet_name, row, col) {
            Ok(v)
        } else {
            // Excel semantics: empty cell coerces to 0 in numeric contexts
            Ok(LiteralValue::Number(0.0))
        }
    }
}

impl<R> crate::traits::RangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn crate::traits::Range>, ExcelError> {
        // For now, delegate range resolution to the external resolver.
        // A future optimization could be to handle this within the graph.
        self.resolver.resolve_range_reference(sheet, sr, sc, er, ec)
    }
}

impl<R> crate::traits::NamedRangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.resolver.resolve_named_range_reference(name)
    }
}

impl<R> crate::traits::TableResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_table_reference(
        &self,
        tref: &formualizer_parse::parser::TableReference,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_table_reference(tref)
    }
}

impl<R> crate::traits::SourceResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn source_scalar_version(&self, name: &str) -> Option<u64> {
        self.resolver.source_scalar_version(name)
    }

    fn resolve_source_scalar(&self, name: &str) -> Result<LiteralValue, ExcelError> {
        self.resolver.resolve_source_scalar(name)
    }

    fn source_table_version(&self, name: &str) -> Option<u64> {
        self.resolver.source_table_version(name)
    }

    fn resolve_source_table(
        &self,
        name: &str,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_source_table(name)
    }
}

// The Engine is a Resolver because it implements the constituent traits.
impl<R> crate::traits::Resolver for Engine<R> where R: EvaluationContext {}

// The Engine provides functions by delegating to its internal resolver.
impl<R> crate::traits::FunctionProvider for Engine<R>
where
    R: EvaluationContext,
{
    fn get_function(
        &self,
        prefix: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.resolver.get_function(prefix, name)
    }
}

// Override EvaluationContext to provide thread pool access
impl<R> crate::traits::EvaluationContext for Engine<R>
where
    R: EvaluationContext,
{
    fn clock(&self) -> &dyn crate::timezone::ClockProvider {
        self.clock.as_ref()
    }

    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }

    fn cancellation_token(&self) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        self.active_cancel_flag.clone()
    }

    fn chunk_hint(&self) -> Option<usize> {
        // Use a simple heuristic from configuration (stripe width * height) as a default hint.
        let hint =
            (self.config.stripe_height as usize).saturating_mul(self.config.stripe_width as usize);
        Some(hint.clamp(1024, 1 << 20)) // clamp between 1K and ~1M
    }

    fn volatile_level(&self) -> crate::traits::VolatileLevel {
        self.config.volatile_level
    }

    fn workbook_seed(&self) -> u64 {
        self.config.workbook_seed
    }

    fn recalc_epoch(&self) -> u64 {
        self.recalc_epoch
    }

    fn used_rows_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        // Prefer Arrow-backed used-region; fallback to graph if formulas intersect region
        let sheet_id = self.graph.sheet_id(sheet)?;
        let arrow_ok = self.sheet_store().sheet(sheet).is_some();
        if arrow_ok && let Some(bounds) = self.arrow_used_row_bounds(sheet, start_col, end_col) {
            return Some(bounds);
        }
        let sc0 = start_col.saturating_sub(1);
        let ec0 = end_col.saturating_sub(1);
        self.graph
            .used_row_bounds_for_columns(sheet_id, sc0, ec0)
            .map(|(a0, b0)| (a0 + 1, b0 + 1))
    }

    fn used_cols_for_rows(&self, sheet: &str, start_row: u32, end_row: u32) -> Option<(u32, u32)> {
        // Prefer Arrow-backed used-region; fallback to graph if formulas intersect region
        let sheet_id = self.graph.sheet_id(sheet)?;
        let arrow_ok = self.sheet_store().sheet(sheet).is_some();
        if arrow_ok && let Some(bounds) = self.arrow_used_col_bounds(sheet, start_row, end_row) {
            return Some(bounds);
        }
        let sr0 = start_row.saturating_sub(1);
        let er0 = end_row.saturating_sub(1);
        self.graph
            .used_col_bounds_for_rows(sheet_id, sr0, er0)
            .map(|(a0, b0)| (a0 + 1, b0 + 1))
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let _ = self.graph.sheet_id(sheet)?;
        // Excel-like upper bounds; we expose something finite but large.
        // Backends may override with real bounds.
        Some((1_048_576, 16_384)) // 1048576 rows, 16384 cols (XFD)
    }

    fn data_snapshot_id(&self) -> u64 {
        self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn backend_caps(&self) -> crate::traits::BackendCaps {
        crate::traits::BackendCaps {
            streaming: true,
            used_region: true,
            write: false,
            tables: false,
            async_stream: false,
        }
    }

    // Flats removed

    fn date_system(&self) -> crate::engine::DateSystem {
        self.config.date_system
    }
    /// New: resolve a reference into a RangeView (Phase 2 API)
    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        match reference {
            ReferenceType::External(ext) => {
                let name = ext.raw.as_str();
                match ext.kind {
                    formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                        let Some(source) = self.graph.resolve_source_scalar_entry(name) else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined name: {name}")));
                        };
                        let version = source
                            .version
                            .or_else(|| self.resolver.source_scalar_version(name));
                        let v = self.resolve_source_scalar_cached(name, version)?;
                        Ok(RangeView::from_owned_rows(
                            vec![vec![v]],
                            self.config.date_system,
                        ))
                    }
                    formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                        let Some(source) = self.graph.resolve_source_table_entry(name) else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined table: {name}")));
                        };
                        let version = source
                            .version
                            .or_else(|| self.resolver.source_table_version(name));
                        let table = self.resolve_source_table_cached(name, version)?;
                        let spec = Some(formualizer_parse::parser::TableSpecifier::Data);
                        self.source_table_to_range_view(table.as_ref(), &spec)
                    }
                }
            }
            ReferenceType::Range { .. } => {
                let shared = self.resolve_shared_ref(reference, current_sheet)?;
                let formualizer_common::SheetRef::Range(range) = shared else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                };
                let sheet_id = match range.sheet {
                    formualizer_common::SheetLocator::Id(id) => id,
                    _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
                };
                let sheet_name = self.graph.sheet_name(sheet_id);

                let bounded_range = if range.start_row.is_some()
                    && range.start_col.is_some()
                    && range.end_row.is_some()
                    && range.end_col.is_some()
                {
                    Some(RangeRef::try_from_shared(range.as_ref())?)
                } else {
                    None
                };

                let mut sr = bounded_range
                    .as_ref()
                    .map(|r| r.start.coord.row() + 1)
                    .or_else(|| range.start_row.map(|b| b.index + 1));
                let mut sc = bounded_range
                    .as_ref()
                    .map(|r| r.start.coord.col() + 1)
                    .or_else(|| range.start_col.map(|b| b.index + 1));
                let mut er = bounded_range
                    .as_ref()
                    .map(|r| r.end.coord.row() + 1)
                    .or_else(|| range.end_row.map(|b| b.index + 1));
                let mut ec = bounded_range
                    .as_ref()
                    .map(|r| r.end.coord.col() + 1)
                    .or_else(|| range.end_col.map(|b| b.index + 1));

                if sr.is_none() && er.is_none() {
                    // Full-column reference: anchor at row 1
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    sr = Some(1);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(self.config.max_open_ended_rows);
                    }
                }
                if sc.is_none() && ec.is_none() {
                    // Full-row reference: anchor at column 1
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    sc = Some(1);
                    if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        ec = Some(self.config.max_open_ended_cols);
                    }
                }
                if sr.is_some() && er.is_none() {
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(self.config.max_open_ended_rows);
                    }
                }
                if er.is_some() && sr.is_none() {
                    // Open start: anchor at row 1
                    sr = Some(1);
                }
                if sc.is_some() && ec.is_none() {
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        ec = Some(self.config.max_open_ended_cols);
                    }
                }
                if ec.is_some() && sc.is_none() {
                    // Open start: anchor at column 1
                    sc = Some(1);
                }

                let sr = sr.unwrap_or(1);
                let sc = sc.unwrap_or(1);
                let er = er.unwrap_or(sr.saturating_sub(1));
                let ec = ec.unwrap_or(sc.saturating_sub(1));

                if self.force_materialize_range_views {
                    if er < sr || ec < sc {
                        return Ok(RangeView::from_owned_rows(Vec::new(), self.config.date_system));
                    }
                    let h = (er - sr + 1) as u64;
                    let w = (ec - sc + 1) as u64;
                    let cell_count = h.saturating_mul(w);
                    if cell_count <= self.config.spill.max_spill_cells as u64 {
                        let mut rows: Vec<Vec<LiteralValue>> = Vec::with_capacity(h as usize);
                        for r in sr..=er {
                            let mut rowv: Vec<LiteralValue> = Vec::with_capacity(w as usize);
                            for c in sc..=ec {
                                rowv.push(
                                    self.get_cell_value(sheet_name, r, c)
                                        .unwrap_or(LiteralValue::Empty),
                                );
                            }
                            rows.push(rowv);
                        }
                        return Ok(RangeView::from_owned_rows(rows, self.config.date_system));
                    }
                }

                let Some(asheet) = self.sheet_store().sheet(sheet_name) else {
                    return Ok(RangeView::from_owned_rows(
                        Vec::new(),
                        self.config.date_system,
                    ));
                };

                let rv = if er < sr || ec < sc {
                    asheet.range_view(1, 1, 0, 0)
                } else {
                    let sr0 = sr.saturating_sub(1) as usize;
                    let sc0 = sc.saturating_sub(1) as usize;
                    let er0 = er.saturating_sub(1) as usize;
                    let ec0 = ec.saturating_sub(1) as usize;
                    asheet.range_view(sr0, sc0, er0, ec0)
                };

                Ok(rv)
            }
            ReferenceType::Cell { .. } => {
                let shared = self.resolve_shared_ref(reference, current_sheet)?;
                let formualizer_common::SheetRef::Cell(cell) = shared else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                };
                let addr = CellRef::try_from_shared(cell)?;
                let sheet_id = addr.sheet_id;
                let sheet_name = self.graph.sheet_name(sheet_id);
                let row = addr.coord.row() + 1;
                let col = addr.coord.col() + 1;

                if self.force_materialize_range_views {
                    let v = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    return Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ));
                }

                if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                    let r0 = row.saturating_sub(1) as usize;
                    let c0 = col.saturating_sub(1) as usize;
                    let rv = asheet.range_view(r0, c0, r0, c0);
                    Ok(rv)
                } else {
                    let v = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ))
                }
            }
            ReferenceType::NamedRange(name) => {
                if let Some(current_id) = self.graph.sheet_id(current_sheet)
                    && let Some(named) = self.graph.resolve_name_entry(name, current_id)
                {
                    match &named.definition {
                        NamedDefinition::Cell(cell_ref) => {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            if self.force_materialize_range_views {
                                let v = self
                                    .get_cell_value(
                                        sheet_name,
                                        cell_ref.coord.row() + 1,
                                        cell_ref.coord.col() + 1,
                                    )
                                    .unwrap_or(LiteralValue::Empty);
                                return Ok(RangeView::from_owned_rows(
                                    vec![vec![v]],
                                    self.config.date_system,
                                ));
                            } else {
                                let asheet = self
                                    .sheet_store()
                                    .sheet(sheet_name)
                                    .expect("Arrow sheet missing for named cell");
                                let r0 = cell_ref.coord.row() as usize;
                                let c0 = cell_ref.coord.col() as usize;
                                let rv = asheet.range_view(r0, c0, r0, c0);
                                return Ok(rv);
                            }
                        }
                        NamedDefinition::Range(range_ref) => {
                            let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                            let sr = range_ref.start.coord.row() + 1;
                            let sc = range_ref.start.coord.col() + 1;
                            let er = range_ref.end.coord.row() + 1;
                            let ec = range_ref.end.coord.col() + 1;
                            if self.force_materialize_range_views {
                                let h = (er.saturating_sub(sr) + 1) as u64;
                                let w = (ec.saturating_sub(sc) + 1) as u64;
                                let cell_count = h.saturating_mul(w);
                                if cell_count <= self.config.spill.max_spill_cells as u64 {
                                    let mut rows: Vec<Vec<LiteralValue>> =
                                        Vec::with_capacity(h as usize);
                                    for r in sr..=er {
                                        let mut rowv: Vec<LiteralValue> =
                                            Vec::with_capacity(w as usize);
                                        for c in sc..=ec {
                                            rowv.push(
                                                self.get_cell_value(sheet_name, r, c)
                                                    .unwrap_or(LiteralValue::Empty),
                                            );
                                        }
                                        rows.push(rowv);
                                    }
                                    return Ok(RangeView::from_owned_rows(
                                        rows,
                                        self.config.date_system,
                                    ));
                                }
                            }
                            let asheet = self
                                .sheet_store()
                                .sheet(sheet_name)
                                .expect("Arrow sheet missing for named range");
                            let sr0 = range_ref.start.coord.row() as usize;
                            let sc0 = range_ref.start.coord.col() as usize;
                            let er0 = range_ref.end.coord.row() as usize;
                            let ec0 = range_ref.end.coord.col() as usize;
                            let rv = asheet.range_view(sr0, sc0, er0, ec0);
                            return Ok(rv);
                        }
                        NamedDefinition::Literal(v) => {
                            return Ok(RangeView::from_owned_rows(
                                vec![vec![v.clone()]],
                                self.config.date_system,
                            ));
                        }
                        NamedDefinition::Formula { .. } => {
                            if let Some(value) = self.graph.get_value(named.vertex) {
                                return Ok(RangeView::from_owned_rows(
                                    vec![vec![value]],
                                    self.config.date_system,
                                ));
                            }
                        }
                    }
                }

                if let Some(source) = self.graph.resolve_source_scalar_entry(name) {
                    let version = source
                        .version
                        .or_else(|| self.resolver.source_scalar_version(name));
                    let v = self.resolve_source_scalar_cached(name, version)?;
                    return Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ));
                }

                let data = self.resolver.resolve_named_range_reference(name)?;
                Ok(RangeView::from_owned_rows(data, self.config.date_system))
            }
            ReferenceType::Table(tref) => {
                if let Some(table) = self.graph.resolve_table_entry(&tref.name) {
                    let sheet_name = self.graph.sheet_name(table.range.start.sheet_id);
                    let asheet = self
                        .sheet_store()
                        .sheet(sheet_name)
                        .expect("Arrow sheet missing for table reference");

                    let sr0 = table.range.start.coord.row() as usize;
                    let sc0 = table.range.start.coord.col() as usize;
                    let er0 = table.range.end.coord.row() as usize;
                    let ec0 = table.range.end.coord.col() as usize;

                    let has_totals = table.totals_row;
                    let has_headers = table.header_row;
                    let data_sr = if has_headers {
                        sr0.saturating_add(1)
                    } else {
                        sr0
                    };
                    let data_er = if has_totals {
                        er0.saturating_sub(1)
                    } else {
                        er0
                    };

                    let select = |sr: usize, sc: usize, er: usize, ec: usize| {
                        if sr > er || sc > ec {
                            asheet.range_view(1, 1, 0, 0)
                        } else {
                            asheet.range_view(sr, sc, er, ec)
                        }
                    };

                    let av = match &tref.specifier {
                        None => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "Table reference without specifier is unsupported".to_string(),
                            ));
                        }
                        Some(formualizer_parse::parser::TableSpecifier::Column(col)) => {
                            let Some(idx) = table.col_index(col) else {
                                return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                    "Column refers to unknown table column".to_string(),
                                ));
                            };
                            let c0 = sc0 + idx;
                            select(data_sr, c0, data_er, c0)
                        }
                        Some(formualizer_parse::parser::TableSpecifier::ColumnRange(
                            start,
                            end,
                        )) => {
                            let Some(si) = table.col_index(start) else {
                                return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                    "Column range refers to unknown column(s)".to_string(),
                                ));
                            };
                            let Some(ei) = table.col_index(end) else {
                                return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                    "Column range refers to unknown column(s)".to_string(),
                                ));
                            };
                            let (mut a, mut b) = (si, ei);
                            if a > b {
                                std::mem::swap(&mut a, &mut b);
                            }
                            let c_start = sc0 + a;
                            let c_end = sc0 + b;
                            select(data_sr, c_start, data_er, c_end)
                        }
                        Some(formualizer_parse::parser::TableSpecifier::All)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::All,
                        )) => select(sr0, sc0, er0, ec0),
                        Some(formualizer_parse::parser::TableSpecifier::Data)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::Data,
                        )) => select(data_sr, sc0, data_er, ec0),
                        Some(formualizer_parse::parser::TableSpecifier::Headers)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::Headers,
                        )) => {
                            if !has_headers {
                                asheet.range_view(1, 1, 0, 0)
                            } else {
                                select(sr0, sc0, sr0, ec0)
                            }
                        }
                        Some(formualizer_parse::parser::TableSpecifier::Totals)
                        | Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::Totals,
                        )) => {
                            if !has_totals {
                                asheet.range_view(1, 1, 0, 0)
                            } else {
                                select(er0, sc0, er0, ec0)
                            }
                        }
                        Some(formualizer_parse::parser::TableSpecifier::SpecialItem(
                            formualizer_parse::parser::SpecialItem::ThisRow,
                        )) => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "@ (This Row) requires table-aware context; not yet supported"
                                    .to_string(),
                            ));
                        }
                        Some(formualizer_parse::parser::TableSpecifier::Row(_))
                        | Some(formualizer_parse::parser::TableSpecifier::Combination(_)) => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                "Complex structured references not yet supported".to_string(),
                            ));
                        }
                    };

                    return Ok(av);
                }

                if let Some(source) = self.graph.resolve_source_table_entry(&tref.name) {
                    let version = source
                        .version
                        .or_else(|| self.resolver.source_table_version(&tref.name));
                    let table = self.resolve_source_table_cached(&tref.name, version)?;
                    return self.source_table_to_range_view(table.as_ref(), &tref.specifier);
                }

                // Fallback: materialize via Resolver::resolve_range_like tranche 1
                let boxed = self.resolve_range_like(&ReferenceType::Table(tref.clone()))?;
                let owned = boxed.materialise().into_owned();
                Ok(RangeView::from_owned_rows(owned, self.config.date_system))
            }
        }
    }

    fn build_criteria_mask(
        &self,
        view: &RangeView<'_>,
        col_in_view: usize,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        if view.dims().1 == 0 {
            return None;
        }
        // If the view is logically open-ended but the backing sheet has no physical rows,
        // treat the mask as empty (0-len) rather than attempting to build a huge mask.
        let sheet_rows = view.sheet().nrows as usize;
        if sheet_rows == 0 || view.start_row() >= sheet_rows {
            return Some(std::sync::Arc::new(arrow_array::BooleanArray::new_null(0)));
        }
        compute_criteria_mask(view, col_in_view, pred)
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn clear_spill_projection_and_mirror(
        &mut self,
        anchor_vertex: VertexId,
        delta: Option<&mut DeltaCollector>,
    ) {
        let spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();
        if spill_cells.is_empty() {
            return;
        }

        if let Some(delta) = delta
            && delta.mode != DeltaMode::Off
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                let old = self
                    .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                    .unwrap_or(LiteralValue::Empty);
                if old != empty {
                    delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }
        }

        self.graph.clear_spill_region(anchor_vertex);

        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &empty,
                );
            }
        }
    }

    /// Helper: commit spill via shim and mirror resulting cells into Arrow overlay when enabled.
    fn commit_spill_and_mirror(
        &mut self,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
        delta: Option<&mut DeltaCollector>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        let prev_spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();

        if let Some(delta) = delta
            && delta.mode != DeltaMode::Off
        {
            let target_set: FxHashSet<CellRef> = targets.iter().copied().collect();
            let empty = LiteralValue::Empty;

            // Clears (prev - targets)
            for cell in prev_spill_cells.iter() {
                if target_set.contains(cell) {
                    continue;
                }
                let sheet_name = self.graph.sheet_name(cell.sheet_id);
                let old = self
                    .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                    .unwrap_or(LiteralValue::Empty);
                if old != empty {
                    delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                }
            }

            // Writes (targets)
            if !targets.is_empty() && !rows.is_empty() && !rows[0].is_empty() {
                let width = rows[0].len();
                for (idx, cell) in targets.iter().enumerate() {
                    let r_off = idx / width;
                    let c_off = idx % width;
                    let new = rows
                        .get(r_off)
                        .and_then(|r| r.get(c_off))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if old != new {
                        delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
            } else {
                // Degenerate shapes: if we have targets but no rows, treat as writing Empty.
                for cell in targets.iter() {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if !matches!(old, LiteralValue::Empty) {
                        delta.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
            }
        }

        // Commit via shim (releases locks). When the graph value cache is disabled (Arrow-canonical
        // values), plan/commit must consult Arrow storage to detect non-empty value blockers.
        let arrow_sheets = &self.arrow_sheets;
        self.spill_mgr.commit_array_with_value_probe(
            &mut self.graph,
            anchor_vertex,
            targets,
            rows.clone(),
            overwritable_formulas,
            |g, cell| {
                let sheet_name = g.sheet_name(cell.sheet_id);
                let asheet = arrow_sheets.sheet(sheet_name)?;
                let r0 = cell.coord.row() as usize;
                let c0 = cell.coord.col() as usize;
                let v = asheet.get_cell_value(r0, c0);
                if matches!(v, LiteralValue::Empty) {
                    None
                } else {
                    Some(v)
                }
            },
        )?;

        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            if !prev_spill_cells.is_empty() {
                let target_set: FxHashSet<CellRef> = targets.iter().copied().collect();
                let empty = LiteralValue::Empty;
                for cell in prev_spill_cells.iter() {
                    if !target_set.contains(cell) {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_computed_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
            }

            for (idx, cell) in targets.iter().enumerate() {
                if rows.is_empty() || rows[0].is_empty() {
                    break;
                }
                let width = rows[0].len();
                let r_off = idx / width;
                let c_off = idx % width;
                let v = rows[r_off][c_off].clone();
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &v,
                );
            }
        }
        Ok(())
    }
}

// ── Effects pipeline (ticket 603) ──────────────────────────────────────────
//
// Compute → Plan → Apply separation for evaluation side-effects.

use crate::engine::effects::Effect;
use crate::engine::graph::editor::change_log::{ChangeEvent, ChangeLog, SpillSnapshot};

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Plan effects for a single vertex after its value has been computed.
    ///
    /// This reads graph state but only performs lightweight mutations
    /// (`set_kind`, `spill_mgr.reserve`) that are needed for correctness
    /// during the planning phase.  Value-changing mutations are deferred to
    /// `apply_effect`.
    pub(crate) fn plan_vertex_effects(
        &mut self,
        vertex_id: VertexId,
        computed_value: LiteralValue,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<Vec<Effect>, ExcelError> {
        let kind = self.graph.get_vertex_kind(vertex_id);
        let is_formula = matches!(kind, VertexKind::FormulaScalar | VertexKind::FormulaArray);

        // If this vertex's cell is currently covered by a spill from a different
        // anchor, ignore the computed result.  Formula vertices are exempt:
        // they must still evaluate so that overlapping spills produce #SPILL!.
        if !is_formula {
            if let Some(cell) = self.graph.get_cell_ref(vertex_id)
                && let Some(owner) = self.graph.spill_registry_anchor_for_cell(cell)
                && owner != vertex_id
            {
                return Ok(Vec::new());
            }
            // Non-formula vertices: store value as-is (arrays remain arrays; no spill).
            return Ok(vec![Effect::WriteCell {
                vertex_id,
                value: computed_value,
            }]);
        }

        match computed_value {
            LiteralValue::Array(rows) => {
                self.plan_array_effects(vertex_id, rows, overwritable_formulas)
            }
            other => self.plan_scalar_effects(vertex_id, other),
        }
    }

    /// Plan effects for a formula vertex that produced a scalar/error result.
    fn plan_scalar_effects(
        &self,
        vertex_id: VertexId,
        value: LiteralValue,
    ) -> Result<Vec<Effect>, ExcelError> {
        let has_spill = self
            .graph
            .spill_cells_for_anchor(vertex_id)
            .is_some_and(|c| !c.is_empty());

        let mut effects = Vec::new();
        if has_spill {
            effects.push(Effect::SpillClear {
                anchor_vertex: vertex_id,
            });
        }
        effects.push(Effect::WriteCell {
            vertex_id,
            value,
        });
        Ok(effects)
    }

    /// Plan effects for a formula vertex that produced an array result.
    fn plan_array_effects(
        &mut self,
        vertex_id: VertexId,
        rows: Vec<Vec<LiteralValue>>,
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<Vec<Effect>, ExcelError> {
        // Lightweight mutation needed for correct spill-blocking checks.
        self.graph
            .set_kind(vertex_id, VertexKind::FormulaArray);

        let anchor = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let sheet_id = anchor.sheet_id;
        let h = rows.len() as u32;
        let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;

        // Hard cap to avoid vertex explosion from huge dynamic arrays.
        let spill_cells = (h as u64).saturating_mul(w as u64);
        if spill_cells > self.config.spill.max_spill_cells as u64 {
            return self.plan_spill_error_effects(vertex_id, "SpillTooLarge", h, w);
        }

        // Bounds check to avoid out-of-range writes (align to AbsCoord capacity).
        const PACKED_MAX_ROW: u32 = 1_048_575;
        const PACKED_MAX_COL: u32 = 16_383;
        let end_row = anchor.coord.row().saturating_add(h).saturating_sub(1);
        let end_col = anchor.coord.col().saturating_add(w).saturating_sub(1);
        if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
            return self.plan_spill_error_effects(
                vertex_id,
                "Spill exceeds sheet bounds",
                h,
                w,
            );
        }

        let mut targets = Vec::new();
        for r in 0..h {
            for c in 0..w {
                targets.push(self.graph.make_cell_ref_internal(
                    sheet_id,
                    anchor.coord.row() + r,
                    anchor.coord.col() + c,
                ));
            }
        }

        // Region lock via spill manager.
        match self.spill_mgr.reserve(
            vertex_id,
            anchor,
            SpillShape { rows: h, cols: w },
            SpillMeta {
                epoch: self.recalc_epoch,
                config: self.config.spill,
            },
        ) {
            Ok(()) => {
                // Validate spill region is available.
                if let Err(_e) = self.graph.plan_spill_region_allowing_formula_overwrite(
                    vertex_id,
                    &targets,
                    overwritable_formulas,
                ) {
                    return self.plan_spill_error_effects(vertex_id, "Spill blocked", h, w);
                }

                // Arrow-canonical mode: graph planning cannot see non-empty value blockers because
                // cell values are not cached in the dependency graph. Consult Arrow storage to
                // detect occupied cells in the target region.
                if !self.graph.value_cache_enabled() {
                    let sheet_name = self.graph.sheet_name(sheet_id);
                    if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                        for cell in targets.iter() {
                            // Allow overwriting the anchor itself.
                            if *cell == anchor {
                                continue;
                            }
                            // Allow cells already owned by a spill (plan() validated spill ownership).
                            if self.graph.spill_registry_anchor_for_cell(*cell).is_some() {
                                continue;
                            }
                            // Skip formula blockers; plan() handled them (or allowed).
                            if let Some(&vid) = self.graph.get_vertex_id_for_address(cell)
                                && vid != vertex_id
                            {
                                match self.graph.get_vertex_kind(vid) {
                                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                                        continue;
                                    }
                                    _ => {}
                                }
                            }

                            let v = asheet.get_cell_value(
                                cell.coord.row() as usize,
                                cell.coord.col() as usize,
                            );
                            if !matches!(v, LiteralValue::Empty) {
                                return self.plan_spill_error_effects(
                                    vertex_id,
                                    "BlockedByValue",
                                    h,
                                    w,
                                );
                            }
                        }
                    }
                }

                let top_left = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);

                let mut effects = Vec::new();
                // Clear previous spill if any.
                let has_prev = self
                    .graph
                    .spill_cells_for_anchor(vertex_id)
                    .is_some_and(|c| !c.is_empty());
                if has_prev {
                    effects.push(Effect::SpillClear {
                        anchor_vertex: vertex_id,
                    });
                }
                effects.push(Effect::SpillCommit {
                    anchor_vertex: vertex_id,
                    anchor_cell: anchor,
                    target_cells: targets,
                    values: rows,
                });
                effects.push(Effect::WriteCell {
                    vertex_id,
                    value: top_left,
                });
                Ok(effects)
            }
            Err(e) => {
                let msg = e.message.unwrap_or_else(|| "Spill blocked".to_string());
                self.plan_spill_error_effects(vertex_id, &msg, h, w)
            }
        }
    }

    /// Build the effect list for a spill that failed validation.
    fn plan_spill_error_effects(
        &self,
        vertex_id: VertexId,
        message: &str,
        expected_rows: u32,
        expected_cols: u32,
    ) -> Result<Vec<Effect>, ExcelError> {
        let spill_err = ExcelError::new(ExcelErrorKind::Spill)
            .with_message(message)
            .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            });
        let spill_val = LiteralValue::Error(spill_err);

        let mut effects = Vec::new();
        effects.push(Effect::SpillClear {
            anchor_vertex: vertex_id,
        });
        effects.push(Effect::WriteCell {
            vertex_id,
            value: spill_val,
        });
        Ok(effects)
    }

    /// Apply a single effect, performing the actual graph mutations.
    pub(crate) fn apply_effect(
        &mut self,
        effect: &Effect,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
    ) -> Result<(), ExcelError> {
        match effect {
            Effect::WriteCell { vertex_id, value } => {
                self.apply_write_cell(*vertex_id, value, delta);
            }
            Effect::SpillClear { anchor_vertex } => {
                self.apply_spill_clear(*anchor_vertex, delta, log);
            }
            Effect::SpillCommit {
                anchor_vertex,
                anchor_cell: _,
                target_cells,
                values,
            } => {
                self.apply_spill_commit(
                    *anchor_vertex,
                    target_cells,
                    values.clone(),
                    delta,
                    log,
                )?;
            }
        }
        Ok(())
    }

    /// Apply a WriteCell effect.
    fn apply_write_cell(
        &mut self,
        vertex_id: VertexId,
        value: &LiteralValue,
        delta: Option<&mut DeltaCollector>,
    ) {
        if let Some(d) = delta {
            if d.mode != DeltaMode::Off {
                if let Some(cell) = self.graph.get_cell_ref_for_vertex(vertex_id) {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .read_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if old != *value {
                        d.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
            }
        }
        self.graph.update_vertex_value(vertex_id, value.clone());
        self.mirror_vertex_value_to_overlay(vertex_id, value);
    }

    /// Apply a SpillClear effect.
    fn apply_spill_clear(
        &mut self,
        anchor_vertex: VertexId,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
    ) {
        let spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();
        if spill_cells.is_empty() {
            return;
        }

        // Snapshot for ChangeLog before clearing.
        let snapshot = if log.is_some() {
            self.snapshot_spill_for_anchor(anchor_vertex)
        } else {
            None
        };

        // Record delta for cleared cells.
        if let Some(d) = delta {
            if d.mode != DeltaMode::Off {
                let empty = LiteralValue::Empty;
                for cell in spill_cells.iter() {
                    let sheet_name = self.graph.sheet_name(cell.sheet_id);
                    let old = self
                        .get_cell_value(sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                        .unwrap_or(LiteralValue::Empty);
                    if old != empty {
                        d.record_cell(cell.sheet_id, cell.coord.row(), cell.coord.col());
                    }
                }
            }
        }

        self.graph.clear_spill_region(anchor_vertex);

        // Mirror Empty to Arrow overlay for cleared cells.
        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            let empty = LiteralValue::Empty;
            for cell in spill_cells.iter() {
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_computed_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &empty,
                );
            }
        }

        // ChangeLog.
        if let Some(log) = log {
            if let Some(old) = snapshot {
                log.record(ChangeEvent::SpillCleared {
                    anchor: anchor_vertex,
                    old,
                });
            }
        }
    }

    /// Apply a SpillCommit effect.
    fn apply_spill_commit(
        &mut self,
        anchor_vertex: VertexId,
        target_cells: &[CellRef],
        values: Vec<Vec<LiteralValue>>,
        delta: Option<&mut DeltaCollector>,
        log: Option<&mut ChangeLog>,
    ) -> Result<(), ExcelError> {
        // Snapshot for ChangeLog before commit.
        let old_snapshot = if log.is_some() {
            self.snapshot_spill_for_anchor(anchor_vertex)
        } else {
            None
        };

        // Delegate to existing commit_spill_and_mirror for delta + overlay logic.
        self.commit_spill_and_mirror(
            anchor_vertex,
            target_cells,
            values.clone(),
            delta,
            None, // overwritable_formulas already validated in plan phase
        )?;

        // ChangeLog.
        if let Some(log) = log {
            log.record(ChangeEvent::SpillCommitted {
                anchor: anchor_vertex,
                old: old_snapshot,
                new: SpillSnapshot {
                    target_cells: target_cells.to_vec(),
                    values,
                },
            });
        }
        Ok(())
    }

    /// Snapshot a spill region for ChangeLog recording.
    ///
    /// Extracted from `VertexEditor::snapshot_spill_for_anchor` to be usable
    /// without creating a `VertexEditor`.
    fn snapshot_spill_for_anchor(&self, anchor: VertexId) -> Option<SpillSnapshot> {
        let cells = self.graph.spill_cells_for_anchor(anchor)?.to_vec();
        if cells.is_empty() {
            return None;
        }

        let max = self.config.spill.max_spill_cells as usize;
        let mut cells = cells;
        if cells.len() > max {
            cells.truncate(max);
        }

        let first = *cells.first().expect("non-empty spill cells");
        let sheet_name = self.graph.sheet_name(first.sheet_id).to_string();
        let row0 = first.coord.row();
        let col0 = first.coord.col();

        let mut max_row = row0;
        let mut max_col = col0;
        let mut by_coord: FxHashMap<(u32, u32), LiteralValue> = FxHashMap::default();
        for cell in &cells {
            max_row = max_row.max(cell.coord.row());
            max_col = max_col.max(cell.coord.col());
            let v = self
                .graph
                .get_cell_value(&sheet_name, cell.coord.row() + 1, cell.coord.col() + 1)
                .unwrap_or(LiteralValue::Empty);
            by_coord.insert((cell.coord.row(), cell.coord.col()), v);
        }

        let rows = (max_row - row0 + 1) as usize;
        let cols = (max_col - col0 + 1) as usize;
        let mut values: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut row: Vec<LiteralValue> = Vec::with_capacity(cols);
            for c in 0..cols {
                row.push(
                    by_coord
                        .get(&(row0 + r as u32, col0 + c as u32))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty),
                );
            }
            values.push(row);
        }

        Some(SpillSnapshot {
            target_cells: cells,
            values,
        })
    }

    // ── Layer evaluation via effects pipeline ──────────────────────────────

    /// Evaluate a layer sequentially using the effects pipeline.
    fn evaluate_layer_sequential_effects(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        for &vertex_id in &layer.vertices {
            let value = match self.evaluate_vertex_immutable(vertex_id) {
                Ok(v) => v,
                Err(e) => LiteralValue::Error(e),
            };
            let effects = self.plan_vertex_effects(vertex_id, value, None)?;
            for effect in &effects {
                self.apply_effect(effect, None, None)?;
            }
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with delta collection via effects pipeline.
    fn evaluate_layer_sequential_with_delta_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        for &vertex_id in &layer.vertices {
            let value = match self.evaluate_vertex_immutable(vertex_id) {
                Ok(v) => v,
                Err(e) => LiteralValue::Error(e),
            };
            let effects = self.plan_vertex_effects(vertex_id, value, None)?;
            for effect in &effects {
                self.apply_effect(effect, Some(delta), None)?;
            }
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with cancellation support via effects pipeline.
    fn evaluate_layer_sequential_cancellable_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            if i % 256 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled within layer".to_string()));
            }
            let value = match self.evaluate_vertex_immutable(vertex_id) {
                Ok(v) => v,
                Err(e) => LiteralValue::Error(e),
            };
            let effects = self.plan_vertex_effects(vertex_id, value, None)?;
            for effect in &effects {
                self.apply_effect(effect, None, None)?;
            }
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with more frequent cancellation for demand-driven eval.
    fn evaluate_layer_sequential_cancellable_demand_driven_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            if i % 128 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message(
                        "Demand-driven evaluation cancelled within layer".to_string(),
                    ));
            }
            let value = match self.evaluate_vertex_immutable(vertex_id) {
                Ok(v) => v,
                Err(e) => LiteralValue::Error(e),
            };
            let effects = self.plan_vertex_effects(vertex_id, value, None)?;
            for effect in &effects {
                self.apply_effect(effect, None, None)?;
            }
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer in parallel, applying via effects pipeline.
    fn evaluate_layer_parallel_effects(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap().clone();

        let mut phase1: Vec<VertexId> = Vec::new();
        let mut phase2: Vec<VertexId> = Vec::new();
        for &vid in &layer.vertices {
            if self.graph.get_range_dependencies(vid).is_some() {
                phase2.push(vid);
            } else {
                phase1.push(vid);
            }
        }

        let inflight: rustc_hash::FxHashSet<VertexId> = layer.vertices.iter().copied().collect();
        let mut applied = 0usize;

        for group in [&phase1[..], &phase2[..]] {
            if group.is_empty() {
                continue;
            }

            let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
                thread_pool.install(|| {
                    group
                        .par_iter()
                        .map(|&vertex_id| match self.evaluate_vertex_immutable(vertex_id) {
                            Ok(v) => Ok((vertex_id, v)),
                            Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                        })
                        .collect()
                });

            match results {
                Ok(vertex_results) => {
                    // Arrays first, then scalars — establishes spill regions before
                    // scalar results that might land inside a spilled region.
                    let mut arrays: Vec<(VertexId, LiteralValue)> = Vec::new();
                    let mut others: Vec<(VertexId, LiteralValue)> = Vec::new();
                    for (vertex_id, result) in vertex_results {
                        if matches!(result, LiteralValue::Array(_)) {
                            arrays.push((vertex_id, result));
                        } else {
                            others.push((vertex_id, result));
                        }
                    }
                    for (vertex_id, result) in arrays {
                        let effects =
                            self.plan_vertex_effects(vertex_id, result, Some(&inflight))?;
                        for effect in &effects {
                            self.apply_effect(effect, None, None)?;
                        }
                        applied = applied.saturating_add(1);
                    }
                    for (vertex_id, result) in others {
                        let effects =
                            self.plan_vertex_effects(vertex_id, result, Some(&inflight))?;
                        for effect in &effects {
                            self.apply_effect(effect, None, None)?;
                        }
                        applied = applied.saturating_add(1);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(applied)
    }

    /// Evaluate a layer in parallel with delta collection via effects pipeline.
    fn evaluate_layer_parallel_with_delta_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        delta: &mut DeltaCollector,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap().clone();

        let mut phase1: Vec<VertexId> = Vec::new();
        let mut phase2: Vec<VertexId> = Vec::new();
        for &vid in &layer.vertices {
            if self.graph.get_range_dependencies(vid).is_some() {
                phase2.push(vid);
            } else {
                phase1.push(vid);
            }
        }

        let inflight: rustc_hash::FxHashSet<VertexId> = layer.vertices.iter().copied().collect();
        let mut applied = 0usize;

        for group in [&phase1[..], &phase2[..]] {
            if group.is_empty() {
                continue;
            }
            let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
                thread_pool.install(|| {
                    group
                        .par_iter()
                        .map(|&vertex_id| match self.evaluate_vertex_immutable(vertex_id) {
                            Ok(v) => Ok((vertex_id, v)),
                            Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                        })
                        .collect()
                });

            match results {
                Ok(vertex_results) => {
                    let mut arrays: Vec<(VertexId, LiteralValue)> = Vec::new();
                    let mut others: Vec<(VertexId, LiteralValue)> = Vec::new();
                    for (vertex_id, result) in vertex_results {
                        if matches!(result, LiteralValue::Array(_)) {
                            arrays.push((vertex_id, result));
                        } else {
                            others.push((vertex_id, result));
                        }
                    }
                    for (vertex_id, result) in arrays {
                        let effects =
                            self.plan_vertex_effects(vertex_id, result, Some(&inflight))?;
                        for effect in &effects {
                            self.apply_effect(effect, Some(delta), None)?;
                        }
                        applied = applied.saturating_add(1);
                    }
                    for (vertex_id, result) in others {
                        let effects =
                            self.plan_vertex_effects(vertex_id, result, Some(&inflight))?;
                        for effect in &effects {
                            self.apply_effect(effect, Some(delta), None)?;
                        }
                        applied = applied.saturating_add(1);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(applied)
    }

    /// Evaluate a layer in parallel with cancellation support via effects pipeline.
    fn evaluate_layer_parallel_cancellable_effects(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap().clone();

        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Parallel evaluation cancelled before starting".to_string()));
        }

        let mut phase1: Vec<VertexId> = Vec::new();
        let mut phase2: Vec<VertexId> = Vec::new();
        for &vid in &layer.vertices {
            if self.graph.get_range_dependencies(vid).is_some() {
                phase2.push(vid);
            } else {
                phase1.push(vid);
            }
        }

        let inflight: rustc_hash::FxHashSet<VertexId> = layer.vertices.iter().copied().collect();
        let mut applied = 0usize;

        for group in [&phase1[..], &phase2[..]] {
            if group.is_empty() {
                continue;
            }

            let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
                thread_pool.install(|| {
                    group
                        .par_iter()
                        .map(|&vertex_id| {
                            if cancel_flag.load(Ordering::Relaxed) {
                                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                                    .with_message(
                                        "Parallel evaluation cancelled during execution"
                                            .to_string(),
                                    ));
                            }
                            match self.evaluate_vertex_immutable(vertex_id) {
                                Ok(v) => Ok((vertex_id, v)),
                                Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                            }
                        })
                        .collect()
                });

            match results {
                Ok(vertex_results) => {
                    let mut arrays: Vec<(VertexId, LiteralValue)> = Vec::new();
                    let mut others: Vec<(VertexId, LiteralValue)> = Vec::new();
                    for (vertex_id, result) in vertex_results {
                        if matches!(result, LiteralValue::Array(_)) {
                            arrays.push((vertex_id, result));
                        } else {
                            others.push((vertex_id, result));
                        }
                    }
                    for (vertex_id, result) in arrays {
                        let effects =
                            self.plan_vertex_effects(vertex_id, result, Some(&inflight))?;
                        for effect in &effects {
                            self.apply_effect(effect, None, None)?;
                        }
                        applied = applied.saturating_add(1);
                    }
                    for (vertex_id, result) in others {
                        let effects =
                            self.plan_vertex_effects(vertex_id, result, Some(&inflight))?;
                        for effect in &effects {
                            self.apply_effect(effect, None, None)?;
                        }
                        applied = applied.saturating_add(1);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(applied)
    }

    // ── Top-level evaluate_all_logged ───────────────────────────────────────

    /// Evaluate all dirty/volatile vertices, recording effects into a ChangeLog.
    ///
    /// This is the same flow as `evaluate_all` but threads a ChangeLog through
    /// every effect application so that spill commits/clears are captured.
    pub fn evaluate_all_logged(
        &mut self,
        log: &mut ChangeLog,
    ) -> Result<EvalResult, ExcelError> {
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }
        let start = web_time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        log.begin_compound(format!("evaluate_all(epoch={})", self.recalc_epoch));

        // Handle cycles.
        let circ_error = LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string()),
        );
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            for &vertex_id in cycle {
                self.graph.update_vertex_value(vertex_id, circ_error.clone());
                self.mirror_vertex_value_to_overlay(vertex_id, &circ_error);
            }
        }

        // Evaluate layers.
        for layer in &schedule.layers {
            computed_vertices += self.evaluate_layer_logged(layer, log)?;
        }

        log.end_compound();

        self.graph.clear_dirty_flags(&to_evaluate);
        self.graph.redirty_volatiles();
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate a single layer with ChangeLog recording.
    fn evaluate_layer_logged(
        &mut self,
        layer: &super::scheduler::Layer,
        log: &mut ChangeLog,
    ) -> Result<usize, ExcelError> {
        for &vertex_id in &layer.vertices {
            let value = match self.evaluate_vertex_immutable(vertex_id) {
                Ok(v) => v,
                Err(e) => LiteralValue::Error(e),
            };
            let effects = self.plan_vertex_effects(vertex_id, value, None)?;
            for effect in &effects {
                self.apply_effect(effect, None, Some(log))?;
            }
        }
        Ok(layer.vertices.len())
    }
}
