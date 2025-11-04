use crate::args::CoercionPolicy;
use crate::arrow_store;
use crate::stripes::NumericChunk;
use formualizer_common::{ExcelError, LiteralValue};

/// Unified view over a 2D range with efficient traversal utilities.
pub struct RangeView<'a> {
    backing: RangeBacking<'a>,
    rows: usize,
    cols: usize,
}

enum RangeBacking<'a> {
    /// Borrowed 2D rows without cloning (array literals, tests)
    Borrowed2D(&'a [Vec<LiteralValue>]),
    /// Arrow-backed range view (authoritative storage)
    Arrow(arrow_store::ArrowRangeView<'a>),
}

impl<'a> core::fmt::Debug for RangeView<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RangeView")
            .field("rows", &self.rows)
            .field("cols", &self.cols)
            .field("kind", &self.kind_probe())
            .finish()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RangeKind {
    Empty,
    NumericOnly,
    TextOnly,
    Mixed,
}

impl<'a> RangeView<'a> {
    pub fn from_borrowed(rows: &'a [Vec<LiteralValue>]) -> Self {
        let r = rows.len();
        let c = rows.first().map(|r| r.len()).unwrap_or(0);
        Self {
            backing: RangeBacking::Borrowed2D(rows),
            rows: r,
            cols: c,
        }
    }
    pub fn from_arrow(av: arrow_store::ArrowRangeView<'a>) -> Self {
        let (rows, cols) = av.dims();
        Self {
            backing: RangeBacking::Arrow(av),
            rows,
            cols,
        }
    }

    #[inline]
    pub fn dims(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows == 0 || self.cols == 0
    }

    pub fn kind_probe(&self) -> RangeKind {
        match &self.backing {
            RangeBacking::Borrowed2D(rows) => {
                if rows.is_empty() || self.is_empty() {
                    RangeKind::Empty
                } else {
                    // Quick scan with early exits
                    let mut has_num = false;
                    let mut has_text = false;
                    let mut has_other = false;
                    'outer: for r in rows.iter() {
                        for v in r.iter() {
                            match v {
                                LiteralValue::Number(_)
                                | LiteralValue::Int(_)
                                | LiteralValue::Boolean(_)
                                | LiteralValue::Empty => has_num = true,
                                LiteralValue::Text(_) => has_text = true,
                                _ => {
                                    has_other = true;
                                    break 'outer;
                                }
                            }
                        }
                    }
                    if has_other || (has_num && has_text) {
                        RangeKind::Mixed
                    } else if has_text {
                        RangeKind::TextOnly
                    } else if has_num {
                        RangeKind::NumericOnly
                    } else {
                        RangeKind::Empty
                    }
                }
            }
            RangeBacking::Arrow(_) => RangeKind::Mixed, // probe conservatively; mixed possible
        }
    }

    pub fn as_1x1(&self) -> Option<LiteralValue> {
        if self.rows == 0 || self.cols == 0 {
            return None;
        }
        if self.rows == 1 && self.cols == 1 {
            let mut out: Option<LiteralValue> = None;
            let _ = self.for_each_cell(&mut |v| {
                out = Some(v.clone());
                Ok(())
            });
            return out;
        }
        None
    }

    /// Get a specific cell by row and column index (0-based).
    /// Returns Empty for out-of-bounds access.
    pub fn get_cell(&self, row: usize, col: usize) -> LiteralValue {
        if row >= self.rows || col >= self.cols {
            return LiteralValue::Empty;
        }

        match &self.backing {
            RangeBacking::Borrowed2D(rows) => rows[row][col].clone(),
            RangeBacking::Arrow(av) => av.get_cell(row, col),
        }
    }

    /// Row-major cell traversal. For borrowable backings, passes borrowed slices/values.
    pub fn for_each_cell(
        &self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        match &self.backing {
            RangeBacking::Borrowed2D(rows) => {
                for r in rows.iter() {
                    for v in r.iter() {
                        f(v)?;
                    }
                }
            }
            RangeBacking::Arrow(av) => {
                for r in 0..self.rows {
                    for c in 0..self.cols {
                        let tmp = av.get_cell(r, c);
                        f(&tmp)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Visit each row as a borrowed slice when possible; otherwise uses a reusable buffer per row.
    pub fn for_each_row(
        &self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        match &self.backing {
            RangeBacking::Borrowed2D(rows) => {
                for r in rows.iter() {
                    f(&r[..])?;
                }
            }
            RangeBacking::Arrow(av) => {
                let mut buf: Vec<LiteralValue> = Vec::with_capacity(self.cols);
                for r in 0..self.rows {
                    buf.clear();
                    for c in 0..self.cols {
                        buf.push(av.get_cell(r, c));
                    }
                    f(&buf[..])?;
                }
            }
        }
        Ok(())
    }

    /// Visit each column as a contiguous slice (buffered when needed)
    pub fn for_each_col(
        &self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        match &self.backing {
            RangeBacking::Borrowed2D(rows) => {
                if self.cols == 0 {
                    return Ok(());
                }
                let mut col_buf: Vec<LiteralValue> = Vec::with_capacity(self.rows);
                for c in 0..self.cols {
                    col_buf.clear();
                    for r in 0..self.rows {
                        col_buf.push(rows[r][c].clone());
                    }
                    f(&col_buf[..])?;
                }
            }
            RangeBacking::Arrow(av) => {
                let mut col_buf: Vec<LiteralValue> = Vec::with_capacity(self.rows);
                for c in 0..self.cols {
                    col_buf.clear();
                    for r in 0..self.rows {
                        col_buf.push(av.get_cell(r, c));
                    }
                    f(&col_buf[..])?;
                }
            }
        }
        Ok(())
    }

    /// If Arrow-backed, return the underlying ArrowRangeView for vectorized fast paths.
    pub fn as_arrow(&self) -> Option<&arrow_store::ArrowRangeView<'a>> {
        match &self.backing {
            RangeBacking::Arrow(av) => Some(av),
            _ => None,
        }
    }

    /// Get a numeric value at a specific cell, with coercion.
    /// Returns None for empty cells or non-coercible values.
    pub fn get_cell_numeric(&self, row: usize, col: usize, policy: CoercionPolicy) -> Option<f64> {
        if row >= self.rows || col >= self.cols {
            return None;
        }

        let val = self.get_cell(row, col);
        pack_numeric(&val, policy).ok().flatten()
    }

    /// Numeric chunk iteration with coercion policy
    pub fn numbers_chunked(
        &self,
        policy: CoercionPolicy,
        min_chunk: usize,
        f: &mut dyn FnMut(NumericChunk) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let min_chunk = min_chunk.max(1);
        let mut buf: Vec<f64> = Vec::with_capacity(min_chunk);
        let mut flush = |buf: &mut Vec<f64>| -> Result<(), ExcelError> {
            if buf.is_empty() {
                return Ok(());
            }
            // SAFETY: read-only borrow for callback duration
            let ptr = buf.as_ptr();
            let len = buf.len();
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let chunk = NumericChunk {
                data: slice,
                validity: None,
            };
            f(chunk)?;
            buf.clear();
            Ok(())
        };

        match self.backing {
            RangeBacking::Borrowed2D(rows) => {
                for r in rows.iter() {
                    for v in r.iter() {
                        if let Some(n) = pack_numeric(v, policy)? {
                            buf.push(n);
                            if buf.len() >= min_chunk {
                                flush(&mut buf)?;
                            }
                        }
                    }
                }
                flush(&mut buf)?;
            }
            RangeBacking::Arrow(_) => {
                self.for_each_cell(&mut |v| {
                    if let Some(n) = pack_numeric(v, policy)? {
                        buf.push(n);
                        if buf.len() >= min_chunk {
                            flush(&mut buf)?;
                        }
                    }
                    Ok(())
                })?;
                flush(&mut buf)?;
            }
        }

        Ok(())
    }
}

#[inline]
fn pack_numeric(v: &LiteralValue, policy: CoercionPolicy) -> Result<Option<f64>, ExcelError> {
    match policy {
        CoercionPolicy::NumberLenientText => match v {
            LiteralValue::Error(e) => Err(e.clone()),
            LiteralValue::Empty => Ok(None),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed2d_rows_are_borrowed() {
        let row0 = vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)];
        let row1 = vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)];
        let data: Vec<Vec<LiteralValue>> = vec![row0, row1];
        let rows_ptrs: Vec<*const LiteralValue> = data.iter().map(|r| r.as_ptr()).collect();
        let view = RangeView::from_borrowed(&data);
        let mut seen_ptrs: Vec<*const LiteralValue> = Vec::new();
        view.for_each_row(&mut |row| {
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
    fn borrowed2d_numeric_chunking() {
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
        let view = RangeView::from_borrowed(&data);
        let mut sum = 0.0f64;
        view.numbers_chunked(CoercionPolicy::NumberLenientText, 2, &mut |chunk| {
            for &n in chunk.data {
                sum += n;
            }
            Ok(())
        })
        .unwrap();
        assert!((sum - 7.5).abs() < 1e-9);
    }

    #[test]
    fn flat_numeric_numbers_chunked_removed() {
        // Flats removed: ensure borrowed path still works for chunking
        let data: Vec<Vec<LiteralValue>> = vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ];
        let view = RangeView::from_borrowed(&data);
        assert_eq!(view.dims(), (2, 2));
        let mut collected = Vec::new();
        view.numbers_chunked(CoercionPolicy::NumberLenientText, 3, &mut |chunk| {
            collected.extend_from_slice(chunk.data);
            Ok(())
        })
        .unwrap();
        assert_eq!(collected, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn arrow_range_row_iteration_and_sum() {
        use crate::engine::{Engine, EvalConfig};
        use crate::test_workbook::TestWorkbook;
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let sheet = engine.default_sheet_name().to_string();
        engine
            .set_cell_value(&sheet, 1, 1, LiteralValue::Int(1))
            .unwrap();
        engine
            .set_cell_value(&sheet, 1, 2, LiteralValue::Int(2))
            .unwrap();
        engine
            .set_cell_value(&sheet, 2, 1, LiteralValue::Int(3))
            .unwrap();
        engine
            .set_cell_value(&sheet, 2, 2, LiteralValue::Int(4))
            .unwrap();

        let arrow_sheet = engine
            .sheet_store()
            .sheet(&sheet)
            .expect("default sheet present in Arrow store");
        let view = RangeView::from_arrow(arrow_sheet.range_view(0, 0, 1, 1));
        assert_eq!(view.dims(), (2, 2));
        let mut rows_sum = Vec::new();
        view.for_each_row(&mut |row| {
            let mut s = 0.0;
            for v in row {
                if let LiteralValue::Int(i) = v {
                    s += *i as f64
                } else if let LiteralValue::Number(n) = v {
                    s += *n
                }
            }
            rows_sum.push(s);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows_sum, vec![3.0, 7.0]);

        let mut sum = 0.0;
        RangeView::from_arrow(arrow_sheet.range_view(0, 0, 1, 1))
            .numbers_chunked(CoercionPolicy::NumberLenientText, 2, &mut |chunk| {
                for &n in chunk.data {
                    sum += n;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(sum, 10.0);
    }
}
