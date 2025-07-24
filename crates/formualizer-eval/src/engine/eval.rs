// Placeholder for evaluation engine implementation
use super::EvalConfig;
use super::graph::DependencyGraph;
use super::vertex::{VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_core::parser::{ASTNode, ReferenceType};

pub struct Engine<R> {
    graph: DependencyGraph,
    resolver: R,
    config: EvalConfig,
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
        Self {
            graph: DependencyGraph::new(),
            resolver,
            config,
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

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();

        // TODO: Implement full evaluation pipeline using the scheduler.

        Ok(EvalResult {
            computed_vertices: 0,
            cycle_errors: 0,
            elapsed: start.elapsed(),
        })
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

// Since the Engine is a Resolver and a FunctionProvider, the blanket implementation
// in traits.rs makes it an EvaluationContext automatically.
