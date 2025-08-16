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

    /// Reduce over windows with optional parallel chunking. The reducer functions must be pure
    /// over their inputs and combine must be associative to ensure deterministic results.
    pub fn reduce_windows<T, FI, FF, FC>(
        &mut self,
        init: FI,
        fold: FF,
        combine: FC,
    ) -> Result<T, ExcelError>
    where
        T: Send,
        FI: Fn() -> T + Sync,
        FF: Fn(&[Vec<LiteralValue>], &mut T) -> Result<(), ExcelError> + Sync,
        FC: Fn(T, T) -> T + Sync,
    {
        // Prepare flattened, broadcasted argument data similar to for_each_window_multi
        let spec = self.spec;
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

        let total = rows * cols;
        let mut flats: Vec<Vec<LiteralValue>> = Vec::with_capacity(self.args.len());
        for arg in self.args.iter() {
            if let Ok(storage) = arg.range_storage() {
                let d = storage.dims();
                match d {
                    (0, 0) => flats.push(std::iter::repeat_n(LiteralValue::Empty, total).collect()),
                    (1, 1) => {
                        let mut it = storage.to_iterator();
                        let v = it
                            .next()
                            .map(|c| c.into_owned())
                            .unwrap_or(LiteralValue::Empty);
                        flats.push(std::iter::repeat_n(v, total).collect());
                    }
                    (r, c) => {
                        let mut values: Vec<LiteralValue> =
                            storage.to_iterator().map(|c| c.into_owned()).collect();
                        let range_total = r * c;
                        if range_total < total {
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

        // Helper for bounds/padding
        let get_idx = |r: isize, c: isize| -> Option<usize> {
            if r >= 0 && (r as usize) < rows && c >= 0 && (c as usize) < cols {
                Some((r as usize) * cols + (c as usize))
            } else {
                match spec.padding {
                    PaddingPolicy::None => None,
                    PaddingPolicy::Empty => None,
                    PaddingPolicy::EdgeExtend => {
                        let rr = r.clamp(0, rows as isize - 1) as usize;
                        let cc = c.clamp(0, cols as isize - 1) as usize;
                        Some(rr * cols + cc)
                    }
                }
            }
        };

        // Used-region clamp (data-driven): find leading/trailing all-empty rows/cols for large shapes
        let mut row_start = 0usize;
        let mut row_end = rows;
        let mut col_start = 0usize;
        let mut col_end = cols;
        if rows > 1000 && cols > 0 {
            'outer_top: for r in 0..rows {
                for flat in &flats {
                    let base = r * cols;
                    if flat[base..base + cols]
                        .iter()
                        .any(|v| !matches!(v, LiteralValue::Empty))
                    {
                        row_start = r;
                        break 'outer_top;
                    }
                }
            }
            'outer_bot: for r in (row_start..rows).rev() {
                for flat in &flats {
                    let base = r * cols;
                    if flat[base..base + cols]
                        .iter()
                        .any(|v| !matches!(v, LiteralValue::Empty))
                    {
                        row_end = r + 1; // exclusive
                        break 'outer_bot;
                    }
                }
            }
        }
        if cols > 1000 && rows > 0 {
            'outer_left: for c in 0..cols {
                for flat in &flats {
                    let mut any = false;
                    for r in row_start..row_end {
                        let idx = r * cols + c;
                        if !matches!(flat[idx], LiteralValue::Empty) {
                            any = true;
                            break;
                        }
                    }
                    if any {
                        col_start = c;
                        break 'outer_left;
                    }
                }
            }
            'outer_right: for c in (col_start..cols).rev() {
                for flat in &flats {
                    let mut any = false;
                    for r in row_start..row_end {
                        let idx = r * cols + c;
                        if !matches!(flat[idx], LiteralValue::Empty) {
                            any = true;
                            break;
                        }
                    }
                    if any {
                        col_end = c + 1; // exclusive
                        break 'outer_right;
                    }
                }
            }
        }
        let eff_rows = row_end.saturating_sub(row_start);
        let eff_cols = col_end.saturating_sub(col_start);
        let eff_total = eff_rows * eff_cols;

    // Heuristics for parallelism
    // Use context chunk_hint to decide when to switch to parallel; default (256x256)/4 = 16,384
    let hint = self.fctx.chunk_hint().unwrap_or(65_536);
    let min_cells: usize = if cfg!(test) { 2_000 } else { (hint / 4).max(8_192) };
    let can_parallel = self.fctx.thread_pool().is_some() && eff_total >= min_cells;

        // Local function to process a range of the major axis
        let flats_ref = &flats;
        let spec_copy = spec;
        let init_ref = &init;
        let process_range = move |sr: usize, er: usize| -> Result<T, ExcelError> {
            let mut acc = init_ref();
            match spec_copy.axis {
                WindowAxis::Rows => {
                    let step = spec_copy.step.max(1);
                    let width = spec_copy.width.max(1);
                    for r in (row_start + sr..row_start + er).step_by(step) {
                        for c in col_start..col_end {
                            // Build window vectors per argument of length width along rows
                            let mut windows: Vec<Vec<LiteralValue>> =
                                Vec::with_capacity(flats_ref.len());
                            let mut skip = false;
                            for flat in flats_ref.iter() {
                                let mut win: Vec<LiteralValue> = Vec::with_capacity(width);
                                for k in 0..width {
                                    let rr = r as isize + k as isize;
                                    match get_idx(rr, c as isize) {
                                        Some(idx) => win.push(flat[idx].clone()),
                                        None => {
                                            if spec_copy.padding == PaddingPolicy::None {
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
                                fold(&windows[..], &mut acc)?;
                            }
                        }
                    }
                }
                WindowAxis::Cols => {
                    let step = spec_copy.step.max(1);
                    let width = spec_copy.width.max(1);
                    for c in (col_start + sr..col_start + er).step_by(step) {
                        for r in row_start..row_end {
                            let mut windows: Vec<Vec<LiteralValue>> =
                                Vec::with_capacity(flats_ref.len());
                            let mut skip = false;
                            for flat in flats_ref.iter() {
                                let mut win: Vec<LiteralValue> = Vec::with_capacity(width);
                                for k in 0..width {
                                    let cc = c as isize + k as isize;
                                    match get_idx(r as isize, cc) {
                                        Some(idx) => win.push(flat[idx].clone()),
                                        None => {
                                            if spec_copy.padding == PaddingPolicy::None {
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
                                fold(&windows[..], &mut acc)?;
                            }
                        }
                    }
                }
            }
            Ok(acc)
        };

        if can_parallel {
            let pool = self.fctx.thread_pool().unwrap().clone();
            let threads = pool.current_num_threads().max(1);
            let (major_len, partitions) = match spec.axis {
                WindowAxis::Rows => (eff_rows.max(1), threads.min(eff_rows.max(1))),
                WindowAxis::Cols => (eff_cols.max(1), threads.min(eff_cols.max(1))),
            };
            let chunk = (major_len + partitions - 1) / partitions;
            use rayon::prelude::*;
            let result = pool.install(|| {
                (0..partitions)
                    .into_par_iter()
                    .map(|i| {
                        let start = i * chunk;
                        let end = ((i + 1) * chunk).min(major_len);
                        if start >= end {
                            // empty acc
                            return Ok(init_ref());
                        }
                        process_range(start, end)
                    })
                    .collect::<Result<Vec<T>, ExcelError>>()
            })?;
            // Deterministic combine in partition order
            let mut acc = init_ref();
            for part in result.into_iter() {
                acc = combine(acc, part);
            }
            Ok(acc)
        } else {
            let major_len = match spec.axis {
                WindowAxis::Rows => eff_rows,
                WindowAxis::Cols => eff_cols,
            };
            process_range(0, major_len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_core::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn reduce_rows_width3_step2_sum() {
        let wb = TestWorkbook::new();
        let ctx = interp(&wb);
        // Column vector 1..=6 (6x1)
        let col = lit(LiteralValue::Array(
            (1..=6).map(|n| vec![LiteralValue::Int(n)]).collect(),
        ));
        let args = vec![ArgumentHandle::new(&col, &ctx)];
        let fctx = ctx.function_context(None);
        let mut wctx = SimpleWindowCtx::new(
            &args,
            &fctx,
            WindowSpec {
                width: 3,
                step: 2,
                axis: WindowAxis::Rows,
                align_left: true,
                padding: PaddingPolicy::None,
            },
        );
        // Sum each window of 3 along rows with step 2; with padding None, last partial window is skipped
        let total = wctx
            .reduce_windows(
                || 0i64,
                |wins, acc| {
                    let sum_win: i64 = wins[0]
                        .iter()
                        .map(|v| match v {
                            LiteralValue::Int(i) => *i as i64,
                            LiteralValue::Number(n) => *n as i64,
                            _ => 0,
                        })
                        .sum();
                    *acc += sum_win;
                    Ok(())
                },
                |a, b| a + b,
            )
            .unwrap();
        // Windows at r=0 -> [1,2,3] sum 6; r=2 -> [3,4,5] sum 12; r=4 would be partial -> skipped => total 18
        assert_eq!(total, 18);
    }

    #[test]
    fn reduce_cols_width2_step3_sum() {
        let wb = TestWorkbook::new();
        let ctx = interp(&wb);
        // Row vector [1..=7] (1x7)
        let row = lit(LiteralValue::Array(vec![
            (1..=7).map(|n| LiteralValue::Int(n)).collect(),
        ]));
        let args = vec![ArgumentHandle::new(&row, &ctx)];
        let fctx = ctx.function_context(None);
        let mut wctx = SimpleWindowCtx::new(
            &args,
            &fctx,
            WindowSpec {
                width: 2,
                step: 3,
                axis: WindowAxis::Cols,
                align_left: true,
                padding: PaddingPolicy::None,
            },
        );
        let total = wctx
            .reduce_windows(
                || 0i64,
                |wins, acc| {
                    let sum_win: i64 = wins[0]
                        .iter()
                        .map(|v| match v {
                            LiteralValue::Int(i) => *i as i64,
                            LiteralValue::Number(n) => *n as i64,
                            _ => 0,
                        })
                        .sum();
                    *acc += sum_win;
                    Ok(())
                },
                |a, b| a + b,
            )
            .unwrap();
        // c=0 -> [1,2] sum 3; c=3 -> [4,5] sum 9; c=6 partial -> skipped; total 12
        assert_eq!(total, 12);
    }

    #[test]
    fn used_region_clamp_rows() {
        let wb = TestWorkbook::new();
        let ctx = interp(&wb);
        // 5000x1 column: 1000 empties, 3000 numbers 1..=3000, 1000 empties
        let mut data: Vec<Vec<LiteralValue>> = Vec::with_capacity(5000);
        for _ in 0..1000 {
            data.push(vec![LiteralValue::Empty]);
        }
        for n in 1..=3000 {
            data.push(vec![LiteralValue::Int(n)]);
        }
        for _ in 0..1000 {
            data.push(vec![LiteralValue::Empty]);
        }
        let col = lit(LiteralValue::Array(data));
        let args = vec![ArgumentHandle::new(&col, &ctx)];
        let fctx = ctx.function_context(None);
        let mut wctx = SimpleWindowCtx::new(
            &args,
            &fctx,
            WindowSpec {
                width: 1,
                step: 1,
                axis: WindowAxis::Rows,
                align_left: true,
                padding: PaddingPolicy::None,
            },
        );
        let counter = Arc::new(AtomicUsize::new(0));
        let ctr = counter.clone();
        let sum = wctx
            .reduce_windows(
                || 0i64,
                move |wins, acc| {
                    ctr.fetch_add(1, Ordering::Relaxed);
                    if let Some(v) = wins[0].last() {
                        if let LiteralValue::Int(i) = v {
                            *acc += *i as i64;
                        }
                    }
                    Ok(())
                },
                |a, b| a + b,
            )
            .unwrap();
        // Clamp should trim to 3000 effective rows
        assert_eq!(counter.load(Ordering::Relaxed), 3000);
        // Sum 1..=3000
        assert_eq!(sum, (3000i64 * 3001i64) / 2);
    }

    #[test]
    fn used_region_clamp_cols() {
        let wb = TestWorkbook::new();
        let ctx = interp(&wb);
        // 1x6000 row: 1000 empties, 4000 numbers 1..=4000, 1000 empties
        let mut row: Vec<LiteralValue> = Vec::with_capacity(6000);
        row.extend(std::iter::repeat(LiteralValue::Empty).take(1000));
        row.extend((1..=4000).map(LiteralValue::Int));
        row.extend(std::iter::repeat(LiteralValue::Empty).take(1000));
        let arr = lit(LiteralValue::Array(vec![row]));
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        let fctx = ctx.function_context(None);
        let mut wctx = SimpleWindowCtx::new(
            &args,
            &fctx,
            WindowSpec {
                width: 1,
                step: 1,
                axis: WindowAxis::Cols,
                align_left: true,
                padding: PaddingPolicy::None,
            },
        );
        let counter = Arc::new(AtomicUsize::new(0));
        let ctr = counter.clone();
        let sum = wctx
            .reduce_windows(
                || 0i64,
                move |wins, acc| {
                    ctr.fetch_add(1, Ordering::Relaxed);
                    if let Some(v) = wins[0].last() {
                        if let LiteralValue::Int(i) = v {
                            *acc += *i as i64;
                        }
                    }
                    Ok(())
                },
                |a, b| a + b,
            )
            .unwrap();
        // Clamp should trim to 4000 effective cols
        assert_eq!(counter.load(Ordering::Relaxed), 4000);
        // Sum 1..=4000
        assert_eq!(sum, (4000i64 * 4001i64) / 2);
    }
}
