use crate::args::CoercionPolicy;
use crate::arrow_store;
use crate::arrow_store::IngestBuilder;
use crate::engine::DateSystem;
use crate::stripes::NumericChunk;
use formualizer_common::{ExcelError, LiteralValue};

enum RangeBacking<'a> {
    Borrowed(&'a arrow_store::ArrowSheet),
    Owned(Box<arrow_store::ArrowSheet>),
}

/// Unified view over a 2D range with efficient traversal utilities.
/// Phase 4: Arrow-only backing.
pub struct RangeView<'a> {
    backing: RangeBacking<'a>,
    sr: usize,
    sc: usize,
    er: usize,
    ec: usize,
    rows: usize,
    cols: usize,
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
    pub fn from_arrow(av: arrow_store::ArrowRangeView<'a>) -> Self {
        let (rows, cols) = av.dims();
        let sr = av.start_row();
        let sc = av.start_col();
        let er = av.end_row();
        let ec = av.end_col();
        let sheet = av.sheet();
        Self {
            backing: RangeBacking::Borrowed(sheet),
            sr,
            sc,
            er,
            ec,
            rows,
            cols,
        }
    }

    #[inline]
    fn av(&self) -> arrow_store::ArrowRangeView<'_> {
        match &self.backing {
            RangeBacking::Borrowed(sheet) => sheet.range_view(self.sr, self.sc, self.er, self.ec),
            RangeBacking::Owned(sheet) => sheet.range_view(self.sr, self.sc, self.er, self.ec),
        }
    }

    pub fn from_owned_rows(
        rows: Vec<Vec<LiteralValue>>,
        date_system: DateSystem,
    ) -> RangeView<'static> {
        let nrows = rows.len();
        let ncols = rows.iter().map(|r| r.len()).max().unwrap_or(0);

        let chunk_rows = 32 * 1024;
        let mut ib = IngestBuilder::new("__tmp", ncols, chunk_rows, date_system);

        for mut r in rows {
            r.resize(ncols, LiteralValue::Empty);
            ib.append_row(&r).expect("append_row for RangeView");
        }

        let sheet = Box::new(ib.finish());

        if nrows == 0 || ncols == 0 {
            return RangeView {
                backing: RangeBacking::Owned(sheet),
                sr: 1,
                sc: 1,
                er: 0,
                ec: 0,
                rows: 0,
                cols: 0,
            };
        }

        RangeView {
            backing: RangeBacking::Owned(sheet),
            sr: 0,
            sc: 0,
            er: nrows - 1,
            ec: ncols - 1,
            rows: nrows,
            cols: ncols,
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
        if self.is_empty() {
            return RangeKind::Empty;
        }

        let mut has_num = false;
        let mut has_text = false;
        let av = self.av();

        for r in 0..self.rows {
            for c in 0..self.cols {
                match av.get_cell(r, c) {
                    LiteralValue::Empty => {}
                    LiteralValue::Number(_) | LiteralValue::Int(_) => has_num = true,
                    LiteralValue::Text(_) => has_text = true,
                    _ => return RangeKind::Mixed,
                }
                if has_num && has_text {
                    return RangeKind::Mixed;
                }
            }
        }

        match (has_num, has_text) {
            (false, false) => RangeKind::Empty,
            (true, false) => RangeKind::NumericOnly,
            (false, true) => RangeKind::TextOnly,
            (true, true) => RangeKind::Mixed,
        }
    }

    pub fn as_1x1(&self) -> Option<LiteralValue> {
        if self.rows == 1 && self.cols == 1 {
            Some(self.av().get_cell(0, 0))
        } else {
            None
        }
    }

    /// Get a specific cell by row and column index (0-based).
    /// Returns Empty for out-of-bounds access.
    pub fn get_cell(&self, row: usize, col: usize) -> LiteralValue {
        self.av().get_cell(row, col)
    }

    /// Row-major cell traversal.
    pub fn for_each_cell(
        &self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let av = self.av();
        for r in 0..self.rows {
            for c in 0..self.cols {
                let tmp = av.get_cell(r, c);
                f(&tmp)?;
            }
        }
        Ok(())
    }

    /// Visit each row as a borrowed slice (buffered).
    pub fn for_each_row(
        &self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let av = self.av();
        let mut buf: Vec<LiteralValue> = Vec::with_capacity(self.cols);
        for r in 0..self.rows {
            buf.clear();
            for c in 0..self.cols {
                buf.push(av.get_cell(r, c));
            }
            f(&buf[..])?;
        }
        Ok(())
    }

    /// Visit each column as a contiguous slice (buffered).
    pub fn for_each_col(
        &self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let av = self.av();
        let mut col_buf: Vec<LiteralValue> = Vec::with_capacity(self.rows);
        for c in 0..self.cols {
            col_buf.clear();
            for r in 0..self.rows {
                col_buf.push(av.get_cell(r, c));
            }
            f(&col_buf[..])?;
        }
        Ok(())
    }

    pub fn as_arrow(&self) -> arrow_store::ArrowRangeView<'_> {
        self.av()
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

    /// Numeric chunk iteration with coercion policy.
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
    fn owned_rows_numeric_chunking() {
        let data: Vec<Vec<LiteralValue>> = vec![
            vec![
                LiteralValue::Number(1.0),
                LiteralValue::Text("x".into()),
                LiteralValue::Number(3.0),
            ],
            vec![
                LiteralValue::Boolean(true),
                LiteralValue::Empty,
                LiteralValue::Number(2.5),
            ],
        ];
        let view = RangeView::from_owned_rows(data, DateSystem::Excel1900);
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
    fn as_1x1_works() {
        let view = RangeView::from_owned_rows(
            vec![vec![LiteralValue::Number(7.0)]],
            DateSystem::Excel1900,
        );
        assert_eq!(view.as_1x1(), Some(LiteralValue::Number(7.0)));
    }
}
