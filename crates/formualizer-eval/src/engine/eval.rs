use crate::SheetId;
use crate::engine::pass_planner::PassPlanner;
use crate::engine::range_stream::{RangeStorage, RangeStream};
use crate::engine::spill::{RegionLockManager, SpillMeta, SpillShape};
use crate::engine::warmup::{PassContext, WarmupExecutor};
use crate::engine::{DependencyGraph, EvalConfig, Scheduler, VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord};
use crate::traits::FunctionProvider;
use crate::traits::{EvaluationContext, Resolver};
use formualizer_core::parser::ReferenceType;
use formualizer_core::{ASTNode, ExcelError, ExcelErrorKind, LiteralValue};
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Engine<R> {
    pub graph: DependencyGraph,
    resolver: R,
    pub config: EvalConfig,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    pub recalc_epoch: u64,
    snapshot_id: std::sync::atomic::AtomicU64,
    spill_mgr: ShimSpillManager,
    /// Optional pass-scoped warmup context (flats/masks) available during evaluation
    warmup_pass_ctx: Option<PassContext>,
}

#[derive(Debug)]
pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
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

        Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            thread_pool,
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            warmup_pass_ctx: None,
        }
    }

    /// Create an Engine with a custom thread pool (for shared thread pool scenarios)
    pub fn with_thread_pool(
        resolver: R,
        config: EvalConfig,
        thread_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        crate::builtins::load_builtins();
        Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            thread_pool: Some(thread_pool),
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            warmup_pass_ctx: None,
        }
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

    pub fn sheet_id(&self, name: &str) -> Option<SheetId> {
        self.graph.sheet_id(name)
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

    /// Set a cell value
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        self.graph.set_cell_value(sheet, row, col, value)?;
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
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
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Bulk set many formulas on a sheet. Skips per-cell snapshot bumping and minimizes edge rebuilds.
    pub fn bulk_set_formulas<I>(&mut self, sheet: &str, items: I) -> Result<usize, ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let n = self.graph.bulk_set_formulas(sheet, items)?;
        // Single snapshot bump after batch
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }

    /// Get a cell value
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.graph.get_cell_value(sheet, row, col)
    }

    /// Get formula AST (if any) and current stored value for a cell
    pub fn get_cell(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Option<(Option<formualizer_core::ASTNode>, Option<LiteralValue>)> {
        let v = self.get_cell_value(sheet, row, col);
        let sheet_id = self.graph.sheet_id(sheet)?;
        let coord = Coord::new(row, col, true, true);
        let cell = CellRef::new(sheet_id, coord);
        let vid = self.graph.get_vertex_for_cell(&cell)?;
        let ast = self.graph.get_formula(vid);
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
    pub fn evaluate_vertex(&mut self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast = match kind {
            VertexKind::FormulaScalar => {
                // Get the formula AST
                if let Some(ast) = self.graph.get_formula(vertex_id) {
                    ast.clone()
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                // Check if there's a value stored
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0)); // Empty cells evaluate to 0
                }
            }
            VertexKind::FormulaArray => {
                if let Some(ast) = self.graph.get_formula(vertex_id) {
                    ast.clone()
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::InfiniteRange | VertexKind::Range | VertexKind::External => {
                // Not directly evaluatable here; return stored or 0
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
        };

        // The interpreter uses a reference to the engine as the context.
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
        let result = interpreter.evaluate_ast(&ast);

        // If array result, perform spill from the anchor cell
        match &result {
            Ok(LiteralValue::Array(rows)) => {
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
                // Bounds check to avoid out-of-range writes (align to PackedCoord capacity)
                const PACKED_MAX_ROW: u32 = 1_048_575; // 20-bit max
                const PACKED_MAX_COL: u32 = 16_383; // 14-bit max
                let end_row = anchor.coord.row.saturating_add(h).saturating_sub(1);
                let end_col = anchor.coord.col.saturating_add(w).saturating_sub(1);
                if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
                    let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                        .with_message("Spill exceeds sheet bounds")
                        .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                            expected_rows: h,
                            expected_cols: w,
                        });
                    let spill_val = LiteralValue::Error(spill_err.clone());
                    self.graph.update_vertex_value(vertex_id, spill_val.clone());
                    return Ok(spill_val);
                }
                let mut targets = Vec::new();
                for r in 0..h {
                    for c in 0..w {
                        targets.push(self.graph.make_cell_ref_internal(
                            sheet_id,
                            anchor.coord.row + r,
                            anchor.coord.col + c,
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
                        if let Err(e) = self.spill_mgr.commit_array(
                            &mut self.graph,
                            vertex_id,
                            &targets,
                            rows.clone(),
                        ) {
                            // If commit fails, mark as error
                            self.graph
                                .update_vertex_value(vertex_id, LiteralValue::Error(e.clone()));
                            return Ok(LiteralValue::Error(e));
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
                        let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                            .with_message(e.message.unwrap_or_else(|| "Spill blocked".to_string()))
                            .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                expected_rows: h,
                                expected_cols: w,
                            });
                        let spill_val = LiteralValue::Error(spill_err.clone());
                        self.graph.update_vertex_value(vertex_id, spill_val.clone());
                        Ok(spill_val)
                    }
                }
            }
            _ => {
                // Scalar result: store value and ensure any previous spill is cleared
                self.graph.clear_spill_region(vertex_id);
                self.graph.update_vertex_value(vertex_id, result.clone()?);
                result
            }
        }
    }

    /// Evaluate only the necessary precedents for specific target cells (demand-driven)
    pub fn evaluate_until(&mut self, targets: &[&str]) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for target in targets {
            // For now, assume simple A1-style references on default sheet
            // TODO: Parse complex references with sheets
            let (sheet, row, col) = Self::parse_a1_notation(target)?;
            let sheet_id = self.graph.sheet_id_mut(&sheet);
            let coord = Coord::new(row, col, true, true);
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

        // Phase 1: Global warmup planning (no-op by default)
        if self.config.warmup.warmup_enabled {
            let mut pass_ctx = PassContext::new(&self.config.warmup);
            let planner = PassPlanner::new(self.config.warmup.clone());

            // Collect ASTs from target vertices for analysis
            let mut target_asts = Vec::new();
            for &vid in &target_vertex_ids {
                if let Some(ast) = self.graph.get_formula(vid) {
                    target_asts.push(ast);
                }
            }

            // Analyze and plan warmup
            let target_refs: Vec<&ASTNode> = target_asts.iter().collect();
            let plan = planner.analyze_targets(&target_refs);

            // Execute warmup
            let executor = WarmupExecutor::new(self.config.warmup.clone());
            let fctx = crate::traits::DefaultFunctionContext::new(self, None);
            let _ = executor.execute(&plan, &mut pass_ctx, &fctx);

            // Store pass context for use during evaluation (read-only)
            self.warmup_pass_ctx = Some(pass_ctx);
        }

        // Find dirty precedents that need evaluation
        let precedents_to_eval = self.find_dirty_precedents(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule for the minimal subgraph
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&precedents_to_eval)?;

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
        self.warmup_pass_ctx = None;

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

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();
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
        let addr = format!("{}!{}{}", sheet, Self::col_to_letters(col), row);
        let _ = self.evaluate_until(&[addr.as_str()])?; // ignore detailed EvalResult here
        Ok(self.get_cell_value(sheet, row, col))
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
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();
        let addr_refs: Vec<&str> = addresses.iter().map(|s| s.as_str()).collect();
        let _ = self.evaluate_until(&addr_refs)?;
        Ok(targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect())
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

        // Convert targets to A1 notation for consistency
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            if let Some(sheet_id) = self.graph.sheet_id(sheet) {
                let coord = Coord::new(*row, *col, true, true);
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

        // Find dirty precedents that need evaluation
        let precedents_to_eval = self.find_dirty_precedents(&target_vertex_ids);

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

        // Create schedule for the minimal subgraph
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&precedents_to_eval)?;

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
                                Self::col_to_letters(cell_ref.coord.col),
                                cell_ref.coord.row
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

    /// Helper: convert 1-based column index to Excel-style letters (1 -> A, 27 -> AA)
    fn col_to_letters(mut col: u32) -> String {
        let mut s = String::new();
        while col > 0 {
            let rem = ((col - 1) % 26) as u8;
            s.push((b'A' + rem) as char);
            col = (col - 1) / 26;
        }
        s.chars().rev().collect()
    }

    /// Evaluate all dirty/volatile vertices with cancellation support
    pub fn evaluate_all_cancellable(
        &mut self,
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();
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
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for target in targets {
            let (sheet, row, col) = Self::parse_a1_notation(target)?;
            let sheet_id = self.graph.sheet_id_mut(&sheet);
            let coord = Coord::new(row, col, true, true);
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

        // Find dirty precedents that need evaluation
        let precedents_to_eval = self.find_dirty_precedents(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule for the minimal subgraph
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&precedents_to_eval)?;

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

    fn parse_a1_notation(address: &str) -> Result<(String, u32, u32), ExcelError> {
        let parts: Vec<&str> = address.split('!').collect();
        let (sheet, cell_part) = if parts.len() == 2 {
            (parts[0].to_string(), parts[1])
        } else {
            ("Sheet1".to_string(), address) // Assume default sheet if not specified
        };

        let mut col_end = 0;
        for (i, c) in cell_part.chars().enumerate() {
            if c.is_alphabetic() {
                col_end = i + 1;
            } else {
                break;
            }
        }

        let col_str = &cell_part[..col_end];
        let row_str = &cell_part[col_end..];

        let row = row_str.parse::<u32>().map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref).with_message(format!("Invalid row: {row_str}"))
        })?;

        let mut col = 0;
        for c in col_str.to_uppercase().chars() {
            col = col * 26 + (c as u32 - 'A' as u32) + 1; // +1 for 1-based indexing
        }

        Ok((sheet, row, col))
    }

    /// Determine volatility using this engine's FunctionProvider, falling back to global registry.
    fn is_ast_volatile_with_provider(&self, ast: &ASTNode) -> bool {
        use formualizer_core::parser::ASTNodeType;
        match &ast.node_type {
            ASTNodeType::Function { name, args, .. } => {
                if let Some(func) = self
                    .get_function("", name)
                    .or_else(|| crate::function_registry::get("", name))
                {
                    if func.caps().contains(crate::function::FnCaps::VOLATILE) {
                        return true;
                    }
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
        use rustc_hash::FxHashSet;

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
        for &vertex_id in &layer.vertices {
            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with cancellation support
    fn evaluate_layer_sequential_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            // Check cancellation every 256 vertices to balance responsiveness with performance
            if i % 256 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled within layer".to_string()));
            }

            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with more frequent cancellation checks for demand-driven evaluation
    fn evaluate_layer_sequential_cancellable_demand_driven(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            // Check cancellation more frequently for demand-driven evaluation (every 128 vertices)
            if i % 128 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Demand-driven evaluation cancelled within layer".to_string()));
            }

            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer in parallel using the thread pool
    fn evaluate_layer_parallel(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap();

        // Collect all evaluation results first, then update the graph sequentially
        let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
            thread_pool.install(|| {
                layer
                    .vertices
                    .par_iter()
                    .map(|&vertex_id| {
                        let result = self.evaluate_vertex_immutable(vertex_id)?;
                        Ok((vertex_id, result))
                    })
                    .collect()
            });

        // Update the graph with results sequentially (thread-safe)
        match results {
            Ok(vertex_results) => {
                for (vertex_id, result) in vertex_results {
                    self.graph.update_vertex_value(vertex_id, result);
                }
                Ok(layer.vertices.len())
            }
            Err(e) => Err(e),
        }
    }

    /// Evaluate a layer in parallel with cancellation support
    fn evaluate_layer_parallel_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap();

        // Check cancellation before starting parallel work
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Parallel evaluation cancelled before starting".to_string()));
        }

        // Collect all evaluation results first, then update the graph sequentially
        let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
            thread_pool.install(|| {
                layer
                    .vertices
                    .par_iter()
                    .map(|&vertex_id| {
                        // Check cancellation periodically during parallel work
                        if cancel_flag.load(Ordering::Relaxed) {
                            return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                                "Parallel evaluation cancelled during execution".to_string(),
                            ));
                        }

                        let result = self.evaluate_vertex_immutable(vertex_id)?;
                        Ok((vertex_id, result))
                    })
                    .collect()
            });

        // Update the graph with results sequentially (thread-safe)
        match results {
            Ok(vertex_results) => {
                for (vertex_id, result) in vertex_results {
                    self.graph.update_vertex_value(vertex_id, result);
                }
                Ok(layer.vertices.len())
            }
            Err(e) => Err(e),
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

        let ast = match kind {
            VertexKind::FormulaScalar => {
                // Get the formula AST
                if let Some(ast) = self.graph.get_formula(vertex_id) {
                    ast.clone()
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                // Check if there's a value stored
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0)); // Empty cells evaluate to 0
                }
            }
            _ => {
                return Ok(LiteralValue::Error(
                    ExcelError::new(formualizer_common::ExcelErrorKind::Na)
                        .with_message("Array formulas not yet supported".to_string()),
                ));
            }
        };

        // The interpreter uses a reference to the engine as the context
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
        interpreter.evaluate_ast(&ast)
    }

    /// Get access to the shared thread pool for parallel evaluation
    pub fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
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
            row_start: anchor_cell.coord.row,
            row_end: anchor_cell
                .coord
                .row
                .saturating_add(shape.rows)
                .saturating_sub(1),
            col_start: anchor_cell.coord.col,
            col_end: anchor_cell
                .coord
                .col
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

    pub(crate) fn commit_array(
        &mut self,
        graph: &mut DependencyGraph,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
    ) -> Result<(), ExcelError> {
        // Re-run plan on concrete targets before committing to respect blockers.
        let plan_res = graph.plan_spill_region(anchor_vertex, targets);
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
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
        let sheet_name = sheet.unwrap_or("Sheet1"); // FIXME: This should use the current sheet context
        Ok(self
            .graph
            .get_cell_value(sheet_name, row, col)
            .unwrap_or(LiteralValue::Int(0))) // Empty cells are 0
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
        tref: &formualizer_core::parser::TableReference,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_table_reference(tref)
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
    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }

    fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
        // Engine-wide cancellation is exposed via evaluate_all_cancellable APIs; default None here.
        None
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
        let sheet_id = self.graph.sheet_id(sheet)?;
        self.graph
            .used_row_bounds_for_columns(sheet_id, start_col, end_col)
    }

    fn used_cols_for_rows(&self, sheet: &str, start_row: u32, end_row: u32) -> Option<(u32, u32)> {
        let sheet_id = self.graph.sheet_id(sheet)?;
        self.graph
            .used_col_bounds_for_rows(sheet_id, start_row, end_row)
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

    fn get_or_flatten(
        &self,
        reference: &ReferenceType,
        _prefer_numeric: bool,
    ) -> Option<crate::engine::cache::FlatView> {
        // Read-only access to pass-scoped flat cache built during warmup
        let ctx = self.warmup_pass_ctx.as_ref()?;
        use crate::engine::reference_fingerprint::ReferenceFingerprint;
        let key = reference.fingerprint();
        ctx.flat_cache.get(&key)
    }

    fn resolve_range_storage<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeStorage<'c>, ExcelError> {
        match reference {
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                let sheet_id = self
                    .graph
                    .sheet_id(sheet_name)
                    .ok_or(ExcelError::new(ExcelErrorKind::Ref))?;
                // Detect if this was an unbounded (infinite/partial) range at the reference level
                let is_unbounded = start_row.is_none()
                    || end_row.is_none()
                    || start_col.is_none()
                    || end_col.is_none();
                // Resolve infinite/partial bounds via used-region + sheet bounds
                // Start with provided values, fill None from used-region or sheet bounds.
                let mut sr = *start_row;
                let mut sc = *start_col;
                let mut er = *end_row;
                let mut ec = *end_col;

                // Column-only: rows are None on both ends
                if sr.is_none() && er.is_none() {
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((min_r, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        sr = Some(min_r);
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        // Fallback to full sheet height when used-region is unavailable
                        sr = Some(1);
                        er = Some(max_rows);
                    }
                }

                // Row-only: cols are None on both ends
                if sc.is_none() && ec.is_none() {
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    if let Some((min_c, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        sc = Some(min_c);
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        // Fallback to full sheet width when used-region is unavailable
                        sc = Some(1);
                        ec = Some(max_cols);
                    }
                }

                // Partially bounded (e.g., A1:A or A:A10)
                if sr.is_some() && er.is_none() {
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(max_rows);
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
                        ec = Some(max_cols);
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
                let er = er.unwrap_or(sr.saturating_sub(1)); // may produce empty
                let ec = ec.unwrap_or(sc.saturating_sub(1)); // may produce empty

                let size = (er.saturating_sub(sr) + 1) as u64 * (ec.saturating_sub(sc) + 1) as u64;

                // Stream-only for unbounded (infinite/partial) ranges, regardless of final size
                if is_unbounded {
                    return Ok(RangeStorage::Stream(RangeStream::new(
                        &self.graph,
                        sheet_id,
                        sr,
                        sc,
                        er,
                        ec,
                    )));
                }

                if size > self.config.range_expansion_limit as u64 {
                    Ok(RangeStorage::Stream(RangeStream::new(
                        &self.graph,
                        sheet_id,
                        sr,
                        sc,
                        er,
                        ec,
                    )))
                } else {
                    // Materialize small bounded ranges
                    let mut data = Vec::new();
                    if er < sr || ec < sc {
                        // Empty window
                        return Ok(RangeStorage::Owned(std::borrow::Cow::Owned(Vec::new())));
                    }
                    for r in sr..=er {
                        let mut row_data = Vec::new();
                        for c in sc..=ec {
                            row_data.push(
                                self.graph
                                    .get_cell_value(sheet_name, r, c)
                                    .unwrap_or(LiteralValue::Empty),
                            );
                        }
                        data.push(row_data);
                    }
                    Ok(RangeStorage::Owned(std::borrow::Cow::Owned(data)))
                }
            }
            ReferenceType::Cell { sheet, row, col } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                let value = self
                    .graph
                    .get_cell_value(sheet_name, *row, *col)
                    .unwrap_or(LiteralValue::Empty);
                Ok(RangeStorage::Owned(std::borrow::Cow::Owned(vec![vec![
                    value,
                ]])))
            }
            ReferenceType::NamedRange(name) => {
                let data = self.resolver.resolve_named_range_reference(name)?;
                Ok(RangeStorage::Owned(std::borrow::Cow::Owned(data)))
            }
            ReferenceType::Table(tref) => {
                // Materialize via Resolver::resolve_range_like for tranche 1
                let boxed = self.resolve_range_like(&ReferenceType::Table(tref.clone()))?;
                Ok(RangeStorage::Owned(std::borrow::Cow::Owned(
                    boxed.materialise().into_owned(),
                )))
            }
        }
    }
}
