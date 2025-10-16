// Note: keep imports minimal; coercion is centralized via crate::coercion
use crate::broadcast::{Shape2D, broadcast_shape, project_index};
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

/// Elementwise mapping context (minimal unary-numeric support for Milestone 3)
///
/// This initial version targets unary numeric builtins (e.g., SIN/COS).
/// It detects an array/range input, iterates elementwise in row-major order,
/// applies the provided unary function, and produces an output `LiteralValue`:
/// - Scalar input => caller should prefer scalar fallback; this context is
///   designed for array/range cases and returns an error if no array present.
pub struct SimpleMapCtx<'a, 'b> {
    args: &'a [ArgumentHandle<'a, 'b>],
    ctx: &'a dyn FunctionContext,
    shape: (usize, usize),
    output_rows: Vec<Vec<LiteralValue>>,
}

impl<'a, 'b> SimpleMapCtx<'a, 'b> {
    pub fn new(args: &'a [ArgumentHandle<'a, 'b>], ctx: &'a dyn FunctionContext) -> Self {
        // Determine broadcast shape across all inputs
        let mut shapes: Vec<Shape2D> = Vec::with_capacity(args.len().max(1));
        if args.is_empty() {
            shapes.push((1, 1));
        } else {
            for a in args.iter() {
                if let Ok(rv) = a.range_view() {
                    shapes.push(rv.dims());
                } else if let Ok(v) = a.value() {
                    if let LiteralValue::Array(arr) = v.as_ref() {
                        let rows = arr.len();
                        let cols = arr.first().map(|r| r.len()).unwrap_or(0);
                        shapes.push((rows, cols));
                    } else {
                        shapes.push((1, 1));
                    }
                } else {
                    shapes.push((1, 1));
                }
            }
        }
        let shape = broadcast_shape(&shapes).unwrap_or((1, 1));
        Self {
            args,
            ctx,
            shape,
            output_rows: Vec::new(),
        }
    }

    pub fn input_count(&self) -> usize {
        self.args.len()
    }

    pub fn broadcast_shape(&self) -> (usize, usize) {
        self.shape
    }

    pub fn is_array_context(&self) -> bool {
        self.shape != (1, 1)
    }

    /// Map a unary numeric function over the (broadcasted) input.
    /// Policy: NumberLenientText; non-coercible values yield #VALUE! per cell.
    pub fn map_unary_numeric<F>(&mut self, mut f: F) -> Result<(), ExcelError>
    where
        F: FnMut(f64) -> Result<LiteralValue, ExcelError>,
    {
        if self.args.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("No arguments provided to elementwise function"));
        }

        // Determine input as RangeView or Array or Scalar
        // Prefer range_view streaming path when available
        let first = &self.args[0];
        if let Ok(view) = first.range_view() {
            let (rows, _cols) = self.shape;
            let mut row_idx = 0usize;
            self.output_rows.clear();
            view.for_each_row(&mut |row| {
                let mut out_row: Vec<LiteralValue> = Vec::with_capacity(row.len());
                for cell in row.iter() {
                    let num_opt = match cell {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        other => crate::coercion::to_number_lenient_with_locale(
                            other,
                            &self.ctx.locale(),
                        )
                        .ok(),
                    };
                    match num_opt {
                        Some(n) => out_row.push(f(n)?),
                        None => out_row.push(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value)
                                .with_message("Element is not coercible to number"),
                        )),
                    }
                }
                self.output_rows.push(out_row);
                row_idx += 1;
                Ok(())
            })?;
            // In case of jagged/empty, normalize to intended rows
            if self.output_rows.is_empty() && rows == 0 {
                self.output_rows = Vec::new();
            }
            return Ok(());
        }

        // Fallback: literal array
        if let Ok(v) = first.value() {
            if let LiteralValue::Array(arr) = v.clone().into_owned() {
                self.output_rows.clear();
                for row in arr.into_iter() {
                    let mut out_row: Vec<LiteralValue> = Vec::with_capacity(row.len());
                    for cell in row.into_iter() {
                        let num_opt = match cell {
                            LiteralValue::Error(e) => return Err(e),
                            other => crate::coercion::to_number_lenient_with_locale(
                                &other,
                                &self.ctx.locale(),
                            )
                            .ok(),
                        };
                        match num_opt {
                            Some(n) => out_row.push(f(n)?),
                            None => out_row.push(LiteralValue::Error(
                                ExcelError::new(ExcelErrorKind::Value)
                                    .with_message("Element is not coercible to number"),
                            )),
                        }
                    }
                    self.output_rows.push(out_row);
                }
                return Ok(());
            }
            // Scalar: map single value
            match v.as_ref() {
                LiteralValue::Error(e) => return Err(e.clone()),
                other => {
                    let as_num =
                        crate::coercion::to_number_lenient_with_locale(other, &self.ctx.locale())
                            .ok();
                    let out = match as_num {
                        Some(n) => f(n)?,
                        None => LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value)
                                .with_message("Value is not coercible to number"),
                        ),
                    };
                    self.output_rows.clear();
                    self.output_rows.push(vec![out]);
                    self.shape = (1, 1);
                    return Ok(());
                }
            }
        }

        // If we reach here, there is no array/range; treat as #VALUE!
        Err(ExcelError::new(ExcelErrorKind::Value)
            .with_message("No array or scalar value provided for elementwise map"))
    }

    /// Binary numeric map with broadcasting across two inputs (args[0], args[1]).
    pub fn map_binary_numeric<F>(&mut self, mut f: F) -> Result<(), ExcelError>
    where
        F: FnMut(f64, f64) -> Result<LiteralValue, ExcelError>,
    {
        if self.args.len() < 2 {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Binary elementwise function requires two args"));
        }
        let a0 = &self.args[0];
        let a1 = &self.args[1];
        let target = self.shape;
        self.output_rows.clear();

        // Materialize both inputs as arrays of LiteralValue for now (future: streaming stripes)
        let to_array = |ah: &ArgumentHandle| -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
            if let Ok(rv) = ah.range_view() {
                let mut rows: Vec<Vec<LiteralValue>> = Vec::new();
                rv.for_each_row(&mut |row| {
                    rows.push(row.to_vec());
                    Ok(())
                })?;
                Ok(rows)
            } else {
                let v = ah.value()?;
                Ok(match v.as_ref() {
                    LiteralValue::Array(arr) => arr.clone(),
                    other => vec![vec![other.clone()]],
                })
            }
        };

        let arr0 = to_array(a0)?;
        let arr1 = to_array(a1)?;
        let shape0 = (arr0.len(), arr0.first().map(|r| r.len()).unwrap_or(0));
        let shape1 = (arr1.len(), arr1.first().map(|r| r.len()).unwrap_or(0));
        let _ = broadcast_shape(&[shape0, shape1])?; // validate compatible

        for r in 0..target.0 {
            let mut out_row = Vec::with_capacity(target.1);
            for c in 0..target.1 {
                let (r0, c0) = project_index((r, c), shape0);
                let (r1, c1) = project_index((r, c), shape1);
                let lv0 = arr0
                    .get(r0)
                    .and_then(|row| row.get(c0))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let lv1 = arr1
                    .get(r1)
                    .and_then(|row| row.get(c1))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);

                let n0 = match &lv0 {
                    LiteralValue::Number(n) => Some(*n),
                    LiteralValue::Int(i) => Some(*i as f64),
                    LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
                    LiteralValue::Empty => Some(0.0),
                    LiteralValue::Text(s) => s.trim().parse::<f64>().ok(),
                    LiteralValue::Error(e) => return Err(e.clone()),
                    _ => None,
                };
                let n1 = match &lv1 {
                    LiteralValue::Number(n) => Some(*n),
                    LiteralValue::Int(i) => Some(*i as f64),
                    LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
                    LiteralValue::Empty => Some(0.0),
                    LiteralValue::Text(s) => s.trim().parse::<f64>().ok(),
                    LiteralValue::Error(e) => return Err(e.clone()),
                    _ => None,
                };
                let out_cell = match (n0, n1) {
                    (Some(a), Some(b)) => f(a, b)?,
                    _ => LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value)
                            .with_message("Elements are not coercible to numbers"),
                    ),
                };
                out_row.push(out_cell);
            }
            self.output_rows.push(out_row);
        }
        Ok(())
    }

    pub fn take_output(self) -> LiteralValue {
        if self.shape == (1, 1) {
            if let Some(row) = self.output_rows.first()
                && let Some(cell) = row.first()
            {
                return cell.clone();
            }
            LiteralValue::Empty
        } else {
            LiteralValue::Array(self.output_rows)
        }
    }
}

impl<'a, 'b> crate::function::FnMapCtx for SimpleMapCtx<'a, 'b> {
    fn is_array_context(&self) -> bool {
        self.is_array_context()
    }

    fn map_unary_numeric(
        &mut self,
        f: &mut dyn FnMut(f64) -> Result<LiteralValue, ExcelError>,
    ) -> Result<(), ExcelError> {
        self.map_unary_numeric(f)
    }

    fn map_binary_numeric(
        &mut self,
        f: &mut dyn FnMut(f64, f64) -> Result<LiteralValue, ExcelError>,
    ) -> Result<(), ExcelError> {
        self.map_binary_numeric(f)
    }

    fn finalize(&mut self) -> LiteralValue {
        // Construct a shallow move by swapping out the buffer
        let rows = std::mem::take(&mut self.output_rows);
        if self.shape == (1, 1) {
            LiteralValue::Empty
        } else {
            LiteralValue::Array(rows)
        }
    }
}
