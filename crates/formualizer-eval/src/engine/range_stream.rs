//! Streaming iterator for large ranges.
use std::borrow::Cow;

use formualizer_common::LiteralValue;

use super::graph::{CellAddr, DependencyGraph};

/// A memory-efficient, streaming iterator over a large range in the dependency graph.
#[derive(Debug)]
pub struct RangeStream<'g> {
    graph: &'g DependencyGraph,
    sheet: String,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
    // Current position
    current_row: u32,
    current_col: u32,
}

impl<'g> RangeStream<'g> {
    pub fn new(
        graph: &'g DependencyGraph,
        sheet: String,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Self {
        // Debug: eprintln!("RangeStream::new - sheet: {}, range: {}:{} to {}:{}", sheet, start_row, start_col, end_row, end_col);
        Self {
            graph,
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            current_row: start_row,
            current_col: start_col,
        }
    }
}

impl<'g> Iterator for RangeStream<'g> {
    type Item = Cow<'g, LiteralValue>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we've passed the end bounds BEFORE processing
        if self.current_row > self.end_row {
            return None;
        }

        // Additional check: if we're beyond the column range, we're done
        if self.current_row == self.end_row && self.current_col > self.end_col {
            return None;
        }

        let addr = CellAddr::new(self.sheet.clone(), self.current_row, self.current_col);
        let value = self
            .graph
            .get_vertex_id_for_address(&addr)
            .and_then(|id| self.graph.get_vertex(*id))
            .map(|v| v.value())
            .unwrap_or(Cow::Owned(LiteralValue::Empty));

        // Debug: eprintln!("Processing cell {}:{} -> {:?}", self.current_row, self.current_col, value);

        // Advance position AFTER getting the value
        self.current_col += 1;
        if self.current_col > self.end_col {
            self.current_col = self.start_col;
            self.current_row += 1;
        }

        Some(value)
    }
}

/// A storage container for a range that can either be fully materialized (Owned)
/// for small ranges or lazily iterated (Stream) for large ranges.
#[derive(Debug)]
pub enum RangeStorage<'g> {
    /// For tiny ranges that are cheap to materialize on first use.
    Owned(Cow<'g, [Vec<LiteralValue>]>),

    /// For large ranges, providing a lazy, memory-efficient iterator.
    Stream(RangeStream<'g>),
}

impl<'g> RangeStorage<'g> {
    /// Provides a unified way to iterate over the range's values, consuming the storage.
    pub fn into_iter(self) -> impl Iterator<Item = Cow<'g, LiteralValue>> {
        match self {
            RangeStorage::Owned(owned_data) => {
                // This is inefficient as it requires cloning all data to flatten.
                // A better implementation would use a custom iterator.
                let flattened: Vec<LiteralValue> = owned_data.iter().flatten().cloned().collect();
                let owned_iterator = flattened.into_iter().map(Cow::Owned);
                Box::new(owned_iterator) as Box<dyn Iterator<Item = Cow<'g, LiteralValue>>>
            }
            RangeStorage::Stream(stream) => {
                Box::new(stream) as Box<dyn Iterator<Item = Cow<'g, LiteralValue>>>
            }
        }
    }
}
