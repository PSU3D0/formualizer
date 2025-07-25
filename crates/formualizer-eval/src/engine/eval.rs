use super::EvalConfig;
use super::graph::DependencyGraph;
use super::scheduler::Scheduler;
use super::vertex::{VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::ASTNode;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Engine<R> {
    graph: DependencyGraph,
    resolver: R,
    config: EvalConfig,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
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
            graph: DependencyGraph::new(),
            resolver,
            config,
            thread_pool,
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
            graph: DependencyGraph::new(),
            resolver,
            config,
            thread_pool: Some(thread_pool),
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
        self.graph.set_cell_formula(sheet, row, col, ast)?;
        Ok(())
    }

    /// Get a cell value
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.graph.get_cell_value(sheet, row, col)
    }

    /// Evaluate a single vertex.
    /// This is the core of the sequential evaluation logic for Milestone 3.1.
    pub fn evaluate_vertex(&mut self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        let ast = if let Some(vertex) = self.graph.get_vertex(vertex_id) {
            match &vertex.kind {
                VertexKind::FormulaScalar { ast, .. } => ast.clone(),
                // For now, empty cells evaluate to 0, consistent with Excel.
                VertexKind::Empty => return Ok(LiteralValue::Int(0)),
                // Already evaluated or a literal value.
                VertexKind::Value(v) => return Ok(v.clone()),
                _ => {
                    return Ok(LiteralValue::Error(
                        ExcelError::new(formualizer_common::ExcelErrorKind::Na)
                            .with_message("Array formulas not yet supported".to_string()),
                    ));
                }
            }
        } else {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {:?}", vertex_id)));
        };

        // The interpreter uses a reference to the engine as the context.
        let interpreter = Interpreter::new(self);
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
            let addr = self.parse_cell_address(target)?;
            target_addrs.push(addr);
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(&vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(vertex_id);
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
            // For now, assume simple A1-style references on default sheet
            // TODO: Parse complex references with sheets
            let addr = self.parse_cell_address(target)?;
            target_addrs.push(addr);
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(&vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(vertex_id);
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

    /// Parse a cell address string like "A1" into a CellAddr
    fn parse_cell_address(&self, address: &str) -> Result<super::graph::CellAddr, ExcelError> {
        // Simple A1-style parsing
        if address.is_empty() {
            return Err(
                ExcelError::new(ExcelErrorKind::Ref).with_message("Empty cell address".to_string())
            );
        }

        let chars: Vec<char> = address.chars().collect();
        let mut col_end = 0;

        // Find where letters end and numbers begin
        for (i, &ch) in chars.iter().enumerate() {
            if ch.is_ascii_alphabetic() {
                col_end = i + 1;
            } else if ch.is_ascii_digit() {
                break;
            } else {
                return Err(ExcelError::new(ExcelErrorKind::Ref)
                    .with_message(format!("Invalid character in cell address: {}", ch)));
            }
        }

        if col_end == 0 || col_end == chars.len() {
            return Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Invalid cell address format: {}", address)));
        }

        let col_str = &address[..col_end].to_uppercase();
        let row_str = &address[col_end..];

        // Parse row
        let row: u32 = row_str.parse().map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Invalid row number: {}", row_str))
        })?;

        // Parse column (A=1, B=2, AA=27, etc.)
        let mut col = 0u32;
        for ch in col_str.chars() {
            if !ch.is_ascii_alphabetic() {
                return Err(ExcelError::new(ExcelErrorKind::Ref)
                    .with_message(format!("Invalid column letter: {}", ch)));
            }
            col = col * 26 + (ch as u32 - 'A' as u32 + 1);
        }

        Ok(super::graph::CellAddr::new(
            self.graph.default_sheet().to_string(),
            row,
            col,
        ))
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

            if let Some(vertex) = self.graph.get_vertex(vertex_id) {
                // Check if this vertex needs evaluation
                let needs_eval = match &vertex.kind {
                    super::vertex::VertexKind::FormulaScalar {
                        dirty, volatile, ..
                    } => *dirty || *volatile,
                    super::vertex::VertexKind::FormulaArray {
                        dirty, volatile, ..
                    } => *dirty || *volatile,
                    _ => false, // Values and empty cells don't need evaluation
                };

                if needs_eval {
                    to_evaluate.insert(vertex_id);
                }

                // Continue traversal to dependencies (precedents)
                for &dep_id in &vertex.dependencies {
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
        let ast = if let Some(vertex) = self.graph.get_vertex(vertex_id) {
            match &vertex.kind {
                VertexKind::FormulaScalar { ast, .. } => ast.clone(),
                // For now, empty cells evaluate to 0, consistent with Excel.
                VertexKind::Empty => return Ok(LiteralValue::Int(0)),
                // Already evaluated or a literal value.
                VertexKind::Value(v) => return Ok(v.clone()),
                _ => {
                    return Ok(LiteralValue::Error(
                        ExcelError::new(formualizer_common::ExcelErrorKind::Na)
                            .with_message("Array formulas not yet supported".to_string()),
                    ));
                }
            }
        } else {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {:?}", vertex_id)));
        };

        // The interpreter uses a reference to the engine as the context
        let interpreter = Interpreter::new(self);
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
        let sheet_name = sheet.unwrap_or_else(|| self.graph.default_sheet());
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
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
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
}
