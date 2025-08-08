use crate::SheetId;
use crate::args::CoercionPolicy;
use crate::reference::{CellRef, Coord};
use crate::stripes::{NumericChunk, ValidityMask};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use std::borrow::Cow;

use super::DependencyGraph;

/// A memory-efficient, streaming iterator over a large range in the dependency graph.
#[derive(Debug)]
pub struct RangeStream<'g> {
    graph: &'g DependencyGraph,
    sheet_id: SheetId,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
    // Current position
    current_row: u32,
    current_col: u32,
    finished: bool,
}

impl<'g> RangeStream<'g> {
    pub fn new(
        graph: &'g DependencyGraph,
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Self {
        Self {
            graph,
            sheet_id,
            start_row,
            start_col,
            end_row,
            end_col,
            current_row: start_row,
            current_col: start_col,
            finished: start_row > end_row || start_col > end_col,
        }
    }

    pub fn dimensions(&self) -> (u32, u32) {
        if self.end_row < self.start_row || self.end_col < self.start_col {
            (0, 0)
        } else {
            (
                self.end_row.saturating_sub(self.start_row) + 1,
                self.end_col.saturating_sub(self.start_col) + 1,
            )
        }
    }
}

impl<'g> Iterator for RangeStream<'g> {
    type Item = Cow<'g, LiteralValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let coord = Coord::new(self.current_row, self.current_col, true, true);
        let addr = CellRef::new(self.sheet_id, coord);
        let value = self
            .graph
            .get_vertex_id_for_address(&addr)
            .and_then(|id| self.graph.get_value(*id))
            .unwrap_or(LiteralValue::Empty);

        if self.current_row == self.end_row && self.current_col == self.end_col {
            self.finished = true;
        } else if self.current_col >= self.end_col {
            self.current_col = self.start_col;
            self.current_row += 1;
        } else {
            self.current_col += 1;
        }

        Some(Cow::Owned(value))
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
    /// Dimensions as (rows, cols)
    pub fn dims(&self) -> (usize, usize) {
        match self {
            RangeStorage::Owned(rows) => {
                let r = rows.len();
                let c = rows.first().map(|r| r.len()).unwrap_or(0);
                (r, c)
            }
            RangeStorage::Stream(stream) => {
                let (r, c) = stream.dimensions();
                (r as usize, c as usize)
            }
        }
    }

    pub fn is_owned(&self) -> bool {
        matches!(self, RangeStorage::Owned(_))
    }
    pub fn is_stream(&self) -> bool {
        matches!(self, RangeStorage::Stream(_))
    }

    /// Visit each cell in row-major order without materializing
    pub fn for_each_cell_flat(
        &mut self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        match self {
            RangeStorage::Owned(rows) => {
                for row in rows.iter() {
                    for cell in row.iter() {
                        f(cell)?;
                    }
                }
            }
            RangeStorage::Stream(stream) => {
                for cv in stream.by_ref() {
                    let v = cv.as_ref();
                    f(v)?;
                }
            }
        }
        Ok(())
    }

    /// Visit each row as a borrowed slice. For Stream, uses a reusable buffer per row.
    pub fn for_each_row(
        &mut self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        match self {
            RangeStorage::Owned(rows) => {
                for row in rows.iter() {
                    f(&row[..])?;
                }
            }
            RangeStorage::Stream(stream) => {
                let (rows, cols) = stream.dimensions();
                let mut buffer: Vec<LiteralValue> = Vec::with_capacity(cols as usize);
                for _ in 0..rows {
                    buffer.clear();
                    for _ in 0..cols {
                        let v = stream
                            .next()
                            .map(|c| c.into_owned())
                            .unwrap_or(LiteralValue::Empty);
                        buffer.push(v);
                    }
                    f(&buffer[..])?;
                }
            }
        }
        Ok(())
    }

    /// Visit each column as a contiguous slice. For Owned, clones a narrow temporary column buffer.
    /// For Stream, assembles the column in a temporary buffer per column.
    pub fn for_each_col(
        &mut self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        match self {
            RangeStorage::Owned(rows) => {
                let r = rows.len();
                let c = rows.first().map(|r| r.len()).unwrap_or(0);
                let mut col_buf: Vec<LiteralValue> = Vec::with_capacity(r);
                for j in 0..c {
                    col_buf.clear();
                    for i in 0..r {
                        // clone into a narrow buffer; avoids flattening whole range
                        col_buf.push(rows[i][j].clone());
                    }
                    f(&col_buf[..])?;
                }
            }
            RangeStorage::Stream(stream) => {
                // Build columns in one pass: O(R*C) time and memory
                let (rows, cols) = stream.dimensions();
                let rows_usize = rows as usize;
                let cols_usize = cols as usize;
                if rows_usize == 0 || cols_usize == 0 {
                    return Ok(());
                }

                // Preallocate per-column buffers
                let mut columns: Vec<Vec<LiteralValue>> = (0..cols_usize)
                    .map(|_| Vec::with_capacity(rows_usize))
                    .collect();

                // Consume the stream row-major and distribute values into column buffers
                for _r in 0..rows_usize {
                    for j in 0..cols_usize {
                        let v = stream
                            .next()
                            .map(|c| c.into_owned())
                            .unwrap_or(LiteralValue::Empty);
                        columns[j].push(v);
                    }
                }

                // Emit columns
                for j in 0..cols_usize {
                    f(&columns[j][..])?;
                }
            }
        }
        Ok(())
    }

    /// Pack numeric values into typed chunks and visit them.
    pub fn for_each_numeric_chunk(
        &mut self,
        policy: CoercionPolicy,
        min_chunk: usize,
        f: &mut dyn FnMut(NumericChunk) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let pack_value = |v: &LiteralValue| -> Result<Option<f64>, ExcelError> {
            match policy {
                CoercionPolicy::NumberLenientText => match v {
                    LiteralValue::Error(e) => Err(e.clone()),
                    LiteralValue::Empty => Ok(None), // skip empties for numeric stripes
                    other => Ok(crate::coercion::to_number_lenient(other).ok()),
                },
                CoercionPolicy::NumberStrict => match v {
                    LiteralValue::Error(e) => Err(e.clone()),
                    LiteralValue::Empty => Ok(None),
                    other => Ok(crate::coercion::to_number_strict(other).ok()),
                },
                _ => match v {
                    LiteralValue::Error(e) => Err(e.clone()),
                    _ => Ok(None),
                },
            }
        };

        let mut nums: Vec<f64> = Vec::with_capacity(min_chunk.max(1));
        let mut validity: Option<Vec<bool>> = None; // reserved for future use

        let mut flush =
            |nums: &mut Vec<f64>, validity: &mut Option<Vec<bool>>| -> Result<(), ExcelError> {
                if nums.is_empty() {
                    return Ok(());
                }
                let data_ptr: *const f64 = nums.as_ptr();
                let len = nums.len();
                // SAFETY: we only borrow for the duration of callback; no mutation occurs during borrow
                let data_slice = unsafe { std::slice::from_raw_parts(data_ptr, len) };
                let vm = validity.as_ref().map(|v| ValidityMask::Bools(&v[..]));
                let chunk = NumericChunk {
                    data: data_slice,
                    validity: vm,
                };
                f(chunk)?;
                nums.clear();
                if let Some(v) = validity.as_mut() {
                    v.clear();
                }
                Ok(())
            };

        match self {
            RangeStorage::Owned(rows) => {
                for row in rows.iter() {
                    for cell in row.iter() {
                        match pack_value(cell)? {
                            Some(n) => {
                                nums.push(n);
                                if nums.len() >= min_chunk {
                                    flush(&mut nums, &mut validity)?;
                                }
                            }
                            None => {
                                // mark invalid if we choose to track validity later
                            }
                        }
                    }
                }
                flush(&mut nums, &mut validity)?;
            }
            RangeStorage::Stream(stream) => {
                let cancel: Option<&std::sync::atomic::AtomicBool> = None; // placeholder until context wiring
                let chunk_cap = min_chunk;
                for cv in stream.by_ref() {
                    let v = cv.as_ref();
                    match pack_value(v)? {
                        Some(n) => {
                            nums.push(n);
                            if nums.len() >= chunk_cap {
                                flush(&mut nums, &mut validity)?;
                            }
                            if let Some(flag) = cancel {
                                if flag.load(std::sync::atomic::Ordering::Relaxed) {
                                    return Err(ExcelError::new(ExcelErrorKind::Cancelled));
                                }
                            }
                        }
                        None => { /* skip */ }
                    }
                }
                flush(&mut nums, &mut validity)?;
            }
        }

        Ok(())
    }

    /// Legacy flattening iterator retained as a slow compatibility path during migration.
    pub fn to_iterator(self) -> impl Iterator<Item = Cow<'g, LiteralValue>> {
        match self {
            RangeStorage::Owned(owned_data) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_rows_are_borrowed_no_clone() {
        // Build owned data and wrap as borrowed Cow
        let row0 = vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)];
        let row1 = vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)];
        let data: Vec<Vec<LiteralValue>> = vec![row0, row1];
        let rows_ptrs: Vec<*const LiteralValue> = data.iter().map(|r| r.as_ptr()).collect();

        let mut storage = RangeStorage::Owned(Cow::Borrowed(&data));
        let mut seen_ptrs: Vec<*const LiteralValue> = Vec::new();
        storage
            .for_each_row(&mut |row| {
                seen_ptrs.push(row.as_ptr());
                Ok(())
            })
            .unwrap();

        assert_eq!(rows_ptrs.len(), seen_ptrs.len());
        for (a, b) in rows_ptrs.iter().zip(seen_ptrs.iter()) {
            assert_eq!(*a, *b, "row slices should be borrowed, not cloned");
        }
    }

    #[test]
    fn numeric_chunking_sums_correctly() {
        let data: Vec<Vec<LiteralValue>> = vec![
            vec![
                LiteralValue::Number(1.0),
                LiteralValue::Text("x".into()),
                LiteralValue::Int(3),
            ],
            vec![
                LiteralValue::Boolean(true),
                LiteralValue::Empty,
                LiteralValue::Number(2.5),
            ],
        ];
        let mut storage = RangeStorage::Owned(Cow::Borrowed(&data));
        let mut sum = 0.0f64;
        storage
            .for_each_numeric_chunk(CoercionPolicy::NumberLenientText, 2, &mut |chunk| {
                for &n in chunk.data {
                    sum += n;
                }
                Ok(())
            })
            .unwrap();
        // Expected numbers: 1.0, 3, true->1.0, empty->0.0, 2.5 => 7.5
        assert!((sum - 7.5).abs() < 1e-9);
    }
}
