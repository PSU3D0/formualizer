// Placeholder for evaluation engine implementation
use super::EvalConfig;
use super::graph::DependencyGraph;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_core::parser::ASTNode;

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

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        let start = std::time::Instant::now();

        // TODO: Implement full evaluation pipeline

        Ok(EvalResult {
            computed_vertices: 0,
            cycle_errors: 0,
            elapsed: start.elapsed(),
        })
    }
}
