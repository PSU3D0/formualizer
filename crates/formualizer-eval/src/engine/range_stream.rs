// Placeholder for range streaming implementation
use super::graph::{CellAddr, DependencyGraph};
use super::vertex::VertexId;

#[derive(Debug)]
pub enum Axis {
    Row,
    Col,
}

pub struct RangeStream<'g> {
    graph: &'g DependencyGraph,
    anchor: CellAddr,
    axis: Axis,
}

impl<'g> RangeStream<'g> {
    pub fn new(graph: &'g DependencyGraph, anchor: CellAddr, axis: Axis) -> Self {
        Self {
            graph,
            anchor,
            axis,
        }
    }
}

impl Iterator for RangeStream<'_> {
    type Item = VertexId;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Implement range streaming
        None
    }
}
