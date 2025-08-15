use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WindowAxis {
    Rows,
    Cols,
}

#[derive(Copy, Clone, Debug)]
pub struct WindowSpec {
    pub width: usize,
    pub step: usize,
    pub axis: WindowAxis,
    pub align_left: bool,
    pub padding: PaddingPolicy,
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PaddingPolicy {
    None,
    Empty,
    EdgeExtend,
}

/// Window evaluation context passed to windowed functions.
/// This is intentionally minimal; functions may downcast via `as_any()` to
/// access a concrete implementation with more helpers.
/// Simple window context that wraps raw argument handles and the function context.
pub struct SimpleWindowCtx<'a, 'b> {
    pub args: &'a [ArgumentHandle<'a, 'b>],
    pub fctx: &'a dyn FunctionContext,
    pub spec: WindowSpec,
}

impl<'a, 'b> SimpleWindowCtx<'a, 'b> {
    pub fn new(
        args: &'a [ArgumentHandle<'a, 'b>],
        fctx: &'a dyn FunctionContext,
        spec: WindowSpec,
    ) -> Self {
        Self { args, fctx, spec }
    }

    pub fn spec(&self) -> WindowSpec {
        self.spec
    }

    /// Iterate over aligned windows across all arguments in row-major order.
    /// For now, only supports width == 1 (single-cell windows) with axis Rows/Cols and step >= 1.
    /// The callback receives a slice of window cells (one per argument) at each position.
    pub fn for_each_window(
        &mut self,
        mut f: impl FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        if self.spec.width != 1 {
            return Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("window width>1 not yet supported"));
        }
        // First pass: determine maximum dimensions from any non-empty, non-1x1 range arg.
        // For Excel compatibility, we normalize all ranges to the maximum dimensions found,
        // padding shorter ranges with Empty values.
        let mut max_dims: Option<(usize, usize)> = None;
        let mut saw_empty = false;
        let mut range_dims: Vec<Option<(usize, usize)>> = Vec::with_capacity(self.args.len());

        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                match d {
                    (0, 0) => {
                        saw_empty = true;
                        range_dims.push(None);
                    }
                    (1, 1) => {
                        // scalar-like; will be broadcast
                        range_dims.push(Some((1, 1)));
                    }
                    other => {
                        // Track max dimensions across all non-scalar ranges
                        if let Some((max_r, max_c)) = max_dims {
                            // Check if dimensions are compatible (same number of columns for column vectors, etc.)
                            if (max_c == 1 && other.1 == 1)
                                || (max_r == 1 && other.0 == 1)
                                || (max_c == other.1 && max_r == other.0)
                            {
                                // Compatible dimensions - use the maximum
                                max_dims = Some((max_r.max(other.0), max_c.max(other.1)));
                            } else if max_c == other.1 {
                                // Same width, different height - use max height
                                max_dims = Some((max_r.max(other.0), max_c));
                            } else if max_r == other.0 {
                                // Same height, different width - use max width
                                max_dims = Some((max_r, max_c.max(other.1)));
                            } else {
                                // Incompatible dimensions - this is an actual error
                                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                                    format!(
                                        "incompatible range dimensions: {},{} vs {},{}",
                                        max_r, max_c, other.0, other.1
                                    ),
                                ));
                            }
                        } else {
                            max_dims = Some(other);
                        }
                        range_dims.push(Some(other));
                    }
                }
            } else {
                // Scalar argument
                range_dims.push(None);
            }
        }

        let total = if let Some((r, c)) = max_dims {
            r * c
        } else if saw_empty {
            0
        } else {
            1
        };
        // Build iterators for each argument with broadcasting and padding
        let mut iters: Vec<Box<dyn Iterator<Item = LiteralValue>>> =
            Vec::with_capacity(self.args.len());
        for (i, arg) in self.args.iter().enumerate() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                match d {
                    (0, 0) => {
                        // Empty range: broadcast empties to total (possibly 0)
                        iters.push(Box::new(std::iter::repeat_n(LiteralValue::Empty, total)));
                    }
                    (1, 1) => {
                        // Single cell: materialize one value and broadcast
                        let mut it = storage.to_iterator();
                        let v = it
                            .next()
                            .map(|c| c.into_owned())
                            .unwrap_or(LiteralValue::Empty);
                        iters.push(Box::new(std::iter::repeat_n(v, total)));
                    }
                    (rows, cols) => {
                        // For non-scalar ranges, pad to match max_dims if necessary
                        let range_total = rows * cols;
                        if range_total < total {
                            // Need to pad this range with Empty values
                            let base_iter = storage.to_iterator().map(|c| c.into_owned());
                            let padding =
                                std::iter::repeat(LiteralValue::Empty).take(total - range_total);
                            iters.push(Box::new(base_iter.chain(padding)));
                        } else {
                            // No padding needed
                            iters.push(Box::new(storage.to_iterator().map(|c| c.into_owned())));
                        }
                    }
                }
            } else if let Ok(v) = arg.value() {
                let vv = v.into_owned();
                iters.push(Box::new(std::iter::repeat_n(vv, total)));
            } else {
                iters.push(Box::new(std::iter::repeat_n(
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)),
                    total,
                )));
            }
        }
        // Create a vector to hold current window cells (one per arg)
        let mut window_cells: Vec<LiteralValue> = vec![LiteralValue::Empty; iters.len()];
        for _idx in 0..total {
            // cancellation
            if let Some(cancel) = self.fctx.cancellation_token() {
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    return Err(ExcelError::new(ExcelErrorKind::Cancelled));
                }
            }
            for (i, it) in iters.iter_mut().enumerate() {
                window_cells[i] = it.next().unwrap_or(LiteralValue::Empty);
            }
            f(&window_cells[..])?;
        }
        Ok(())
    }

    /// Multi-width window iteration. Produces, for each window position, a Vec per argument
    /// containing the window's cells in window order (k=0..width-1) along the selected axis.
    /// Padding behavior is controlled by `spec.padding`.
    pub fn for_each_window_multi(
        &mut self,
        mut f: impl FnMut(&[Vec<LiteralValue>]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let spec = self.spec;
        let width = spec.width.max(1);
        // Determine maximum dims from any non-empty, non-1x1 range arg for Excel compatibility
        let mut max_dims: Option<(usize, usize)> = None;
        let mut saw_empty = false;
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                match d {
                    (0, 0) => saw_empty = true,
                    (1, 1) => (),
                    other => {
                        if let Some((max_r, max_c)) = max_dims {
                            // Use maximum dimensions for compatibility
                            if (max_c == 1 && other.1 == 1)
                                || (max_r == 1 && other.0 == 1)
                                || (max_c == other.1 && max_r == other.0)
                            {
                                max_dims = Some((max_r.max(other.0), max_c.max(other.1)));
                            } else if max_c == other.1 {
                                max_dims = Some((max_r.max(other.0), max_c));
                            } else if max_r == other.0 {
                                max_dims = Some((max_r, max_c.max(other.1)));
                            } else {
                                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                                    format!(
                                        "incompatible range dimensions: {},{} vs {},{}",
                                        max_r, max_c, other.0, other.1
                                    ),
                                ));
                            }
                        } else {
                            max_dims = Some(other);
                        }
                    }
                }
            }
        }
        let (rows, cols) = if let Some(d) = max_dims {
            d
        } else if saw_empty {
            (0, 0)
        } else {
            (1, 1)
        };

        // Materialize/broadcast each argument into a flat row-major Vec for indexed access
        let total = rows * cols;
        let mut flats: Vec<Vec<LiteralValue>> = Vec::with_capacity(self.args.len());
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                match d {
                    (0, 0) => {
                        // Broadcast empties to total (may be 0)
                        flats.push(std::iter::repeat_n(LiteralValue::Empty, total).collect());
                    }
                    (1, 1) => {
                        let mut it = storage.to_iterator();
                        let v = it
                            .next()
                            .map(|c| c.into_owned())
                            .unwrap_or(LiteralValue::Empty);
                        flats.push(std::iter::repeat_n(v, total).collect());
                    }
                    (r, c) => {
                        // Collect values and pad if necessary
                        let mut values: Vec<LiteralValue> =
                            storage.to_iterator().map(|c| c.into_owned()).collect();
                        let range_total = r * c;
                        if range_total < total {
                            // Pad with Empty values to match total
                            values.extend(
                                std::iter::repeat(LiteralValue::Empty).take(total - range_total),
                            );
                        }
                        flats.push(values);
                    }
                }
            } else if let Ok(v) = arg.value() {
                flats.push(std::iter::repeat_n(v.into_owned(), total).collect());
            } else {
                flats.push(
                    std::iter::repeat_n(
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)),
                        total,
                    )
                    .collect(),
                );
            }
        }

        // Helper to index with padding
        let get_idx = |r: isize, c: isize| -> Option<usize> {
            if r >= 0 && (r as usize) < rows && c >= 0 && (c as usize) < cols {
                Some((r as usize) * cols + (c as usize))
            } else {
                match spec.padding {
                    PaddingPolicy::None => None,
                    PaddingPolicy::Empty => None, // signal as None; we'll push Empty
                    PaddingPolicy::EdgeExtend => {
                        let rr = r.clamp(0, rows as isize - 1) as usize;
                        let cc = c.clamp(0, cols as isize - 1) as usize;
                        Some(rr * cols + cc)
                    }
                }
            }
        };

        match spec.axis {
            WindowAxis::Rows => {
                for c in 0..cols {
                    let mut sr = 0usize;
                    while sr < rows {
                        // cancellation
                        if let Some(cancel) = self.fctx.cancellation_token() {
                            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                                return Err(ExcelError::new(ExcelErrorKind::Cancelled));
                            }
                        }
                        // Build window per argument
                        let mut windows: Vec<Vec<LiteralValue>> = Vec::with_capacity(flats.len());
                        let mut skip = false;
                        for flat in flats.iter() {
                            let mut win: Vec<LiteralValue> = Vec::with_capacity(width);
                            for k in 0..width {
                                let rr = sr as isize + k as isize;
                                match get_idx(rr, c as isize) {
                                    Some(idx) => win.push(flat[idx].clone()),
                                    None => {
                                        if spec.padding == PaddingPolicy::None {
                                            skip = true;
                                            break;
                                        } else {
                                            win.push(LiteralValue::Empty);
                                        }
                                    }
                                }
                            }
                            if skip {
                                break;
                            }
                            windows.push(win);
                        }
                        if !skip {
                            f(&windows[..])?;
                        }
                        sr = sr.saturating_add(spec.step.max(1));
                    }
                }
            }
            WindowAxis::Cols => {
                for r in 0..rows {
                    let mut sc = 0usize;
                    while sc < cols {
                        if let Some(cancel) = self.fctx.cancellation_token() {
                            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                                return Err(ExcelError::new(ExcelErrorKind::Cancelled));
                            }
                        }
                        let mut windows: Vec<Vec<LiteralValue>> = Vec::with_capacity(flats.len());
                        let mut skip = false;
                        for flat in flats.iter() {
                            let mut win: Vec<LiteralValue> = Vec::with_capacity(width);
                            for k in 0..width {
                                let cc = sc as isize + k as isize;
                                match get_idx(r as isize, cc) {
                                    Some(idx) => win.push(flat[idx].clone()),
                                    None => {
                                        if spec.padding == PaddingPolicy::None {
                                            skip = true;
                                            break;
                                        } else {
                                            win.push(LiteralValue::Empty);
                                        }
                                    }
                                }
                            }
                            if skip {
                                break;
                            }
                            windows.push(win);
                        }
                        if !skip {
                            f(&windows[..])?;
                        }
                        sc = sc.saturating_add(spec.step.max(1));
                    }
                }
            }
        }
        Ok(())
    }
}
