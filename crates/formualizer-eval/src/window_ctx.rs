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
        // First pass: determine dims from any range arg and validate consistency
        let mut dims: Option<(usize, usize)> = None;
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                if let Some(prev) = dims {
                    if prev != d {
                        return Err(ExcelError::new(ExcelErrorKind::Value)
                            .with_message("range dims mismatch"));
                    }
                } else {
                    dims = Some(d);
                }
            }
        }
        let total = dims.map(|(r, c)| r * c).unwrap_or(1);
        // Build iterators for each argument with broadcasting using computed dims
        let mut iters: Vec<Box<dyn Iterator<Item = LiteralValue>>> =
            Vec::with_capacity(self.args.len());
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                iters.push(Box::new(storage.to_iterator().map(|c| c.into_owned())));
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
        // Determine dims from any range arg
        let mut dims: Option<(usize, usize)> = None;
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                if let Some(prev) = dims {
                    if prev != d {
                        return Err(ExcelError::new(ExcelErrorKind::Value)
                            .with_message("range dims mismatch"));
                    }
                } else {
                    dims = Some(d);
                }
            }
        }
        let (rows, cols) = dims.unwrap_or((1, 1));

        // Materialize/broadcast each argument into a flat row-major Vec for indexed access
        let total = rows * cols;
        let mut flats: Vec<Vec<LiteralValue>> = Vec::with_capacity(self.args.len());
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                flats.push(storage.to_iterator().map(|c| c.into_owned()).collect());
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
