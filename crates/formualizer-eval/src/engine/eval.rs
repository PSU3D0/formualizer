use crate::SheetId;
use crate::engine::range_stream::{RangeStorage, RangeStream};
use crate::engine::{DependencyGraph, EvalConfig, Scheduler, VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord};
use crate::traits::EvaluationContext;
use crate::traits::FunctionProvider;
use formualizer_core::parser::ReferenceType;
use formualizer_core::{ASTNode, ExcelError, ExcelErrorKind, LiteralValue};
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Engine<R> {
    pub graph: DependencyGraph,
    resolver: R,
    config: EvalConfig,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    recalc_epoch: u64,
}

#[derive(Debug)]
pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
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

    /// Set a cell value
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        self.graph.set_cell_value(sheet, row, col, value)?;
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
        Ok(())
    }

    /// Get a cell value
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.graph.get_cell_value(sheet, row, col)
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
            _ => {
                return Ok(LiteralValue::Error(
                    ExcelError::new(formualizer_common::ExcelErrorKind::Na)
                        .with_message("Array formulas not yet supported".to_string()),
                ));
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

        // Store the result back into the graph.
        self.graph.update_vertex_value(vertex_id, result.clone()?);

        result
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

        // Evaluate layers
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            for &vertex_id in &layer.vertices {
                self.evaluate_vertex(vertex_id)?;
                computed_vertices += 1;
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
        Some(hint.max(1024).min(1 << 20)) // clamp between 1K and ~1M
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
                let sr = start_row.unwrap_or(1);
                let sc = start_col.unwrap_or(1);
                // For now, we don't support infinite ranges in streaming yet
                let er = end_row.ok_or(
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("Infinite row ranges not supported in streaming"),
                )?;
                let ec = end_col.ok_or(
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("Infinite column ranges not supported in streaming"),
                )?;

                let size = (er.saturating_sub(sr) + 1) as u64 * (ec.saturating_sub(sc) + 1) as u64;

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
                    // Materialize small ranges
                    let mut data = Vec::new();
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
            ReferenceType::Table(_) => Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("Table references not yet supported in streaming evaluation.")),
        }
    }
}
