//! Program 2 tier-3 range kernels (P2-M4): windowed aggregates over a
//! family run.
//!
//! A run whose template is `SUM`/`AVERAGE`/`MIN`/`MAX`/`COUNT` of cell or
//! range references (bounded, whole-row, whole-column, or open-ended;
//! relative or absolute) resolves each argument's rectangle once per member
//! (clamping open bounds to the Arrow sheet's populated extent), merges the
//! overlays of the run's union rows once per column chunk (decision 20.3),
//! and reduces each member's slice in the scalar builtin's exact order: per
//! argument, errors first (segment by chunk, then column, then row), then
//! numbers in row-major order into one seeded accumulator across all
//! arguments and chunks (no subtotals or prefix sums).
//!
//! Anything else declines (the tier-1 path evaluates it): names, 3-D or
//! external references, missing sheets, relocation out of the grid,
//! reversed explicit rectangles, forced materialization, or a function that
//! is not the built-in (an override keeps `family_kernel() == None`).

use super::exact::{LaneSlice, add_f64, lane_slice};
use super::*;
use crate::arrow_store::{ArrowSheet, OverlayCascade};
use crate::engine::arena::{AstNodeData, CompactRefType, DataStore, SheetKey};
use crate::function::FamilyKernel;
use arrow_array::{Array as _, Float64Array, UInt8Array};

/// One 1-based range endpoint: explicit coordinate (with `$` flag) or open
/// (`start == 0` -> sheet start; `end == u32::MAX` -> sheet end).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArgBound {
    At(u32, bool),
    Open,
}

impl ArgBound {
    fn from_start(v: u32, abs: bool) -> Self {
        if v == 0 { Self::Open } else { Self::At(v, abs) }
    }

    fn from_end(v: u32, abs: bool) -> Self {
        if v == u32::MAX {
            Self::Open
        } else {
            Self::At(v, abs)
        }
    }

    fn is_row_invariant(self) -> bool {
        matches!(self, Self::Open | Self::At(_, true))
    }
}

/// One template argument: a cell or range rectangle, 1-based as stored, with
/// per-bound absolute flags or open-axis markers.
#[derive(Clone, Copy, Debug)]
struct ArgRect {
    sheet: SheetId,
    rows: [ArgBound; 2],
    cols: [ArgBound; 2],
}

fn shift(value: u32, delta: i64, abs: bool) -> Option<u32> {
    if abs {
        return Some(value);
    }
    let v = i64::from(value) + delta;
    (1..=i64::from(u32::MAX)).contains(&v).then_some(v as u32)
}

fn shift_bound(bound: ArgBound, delta: i64) -> Option<Option<u32>> {
    match bound {
        ArgBound::Open => Some(None),
        ArgBound::At(v, abs) => Some(Some(shift(v, delta, abs)?)),
    }
}

/// Unclamped 1-based `(lo, hi)` along one axis at `delta`, or `None` when a
/// relative coordinate shifts out of `1..=u32::MAX` or both explicit bounds
/// cross (`lo > hi`).
fn axis_at(bounds: [ArgBound; 2], delta: i64) -> Option<(Option<u32>, Option<u32>)> {
    let lo = shift_bound(bounds[0], delta)?;
    let hi = shift_bound(bounds[1], delta)?;
    if let (Some(a), Some(b)) = (lo, hi)
        && a > b
    {
        return None;
    }
    Some((lo, hi))
}

/// Clamp 1-based `(lo, hi)` (where `None` is open) to 0-based inclusive
/// `[0, sheet_len - 1]`. Returns `None` when the clamped slice is empty.
fn clamp_axis(lo: Option<u32>, hi: Option<u32>, sheet_len: u32) -> Option<(u32, u32)> {
    if sheet_len == 0 {
        return None;
    }
    let lo0 = lo.map_or(0, |v| v - 1);
    let hi0 = hi.map_or(sheet_len - 1, |v| (v - 1).min(sheet_len - 1));
    (lo0 <= hi0).then_some((lo0, hi0))
}

impl ArgRect {
    fn is_row_invariant(&self) -> bool {
        self.rows[0].is_row_invariant() && self.rows[1].is_row_invariant()
    }

    /// 0-based inclusive `(r0, c0, r1, c1)` clamped to `sheet_nrows × sheet_ncols`.
    /// Outer `None` declines the kernel (out-of-grid shift or reversed explicit
    /// bounds); inner `None` is a valid empty slice (no populated cells).
    fn at(
        &self,
        row_delta: i64,
        col_delta: i64,
        sheet_nrows: u32,
        sheet_ncols: u32,
    ) -> Option<Option<(u32, u32, u32, u32)>> {
        let (r_lo, r_hi) = axis_at(self.rows, row_delta)?;
        let (c_lo, c_hi) = axis_at(self.cols, col_delta)?;
        let Some((r0, r1)) = clamp_axis(r_lo, r_hi, sheet_nrows) else {
            return Some(None);
        };
        let Some((c0, c1)) = clamp_axis(c_lo, c_hi, sheet_ncols) else {
            return Some(None);
        };
        Some(Some((r0, c0, r1, c1)))
    }

    /// Clamped union rectangle over the run `[first, last]` (with `first <= last`).
    /// Validates both endpoints before clamping so out-of-grid shifts or
    /// reversed explicit bounds anywhere in the run decline the kernel.
    fn union_clamped(
        &self,
        first: i64,
        last: i64,
        col_delta: i64,
        sheet_nrows: u32,
        sheet_ncols: u32,
    ) -> Option<Option<(u32, u32, u32, u32)>> {
        let (r_lo_a, _) = axis_at(self.rows, first)?;
        let (_, r_hi_b) = axis_at(self.rows, last)?;
        let (c_lo, c_hi) = axis_at(self.cols, col_delta)?;
        let Some((r0, r1)) = clamp_axis(r_lo_a, r_hi_b, sheet_nrows) else {
            return Some(None);
        };
        let Some((c0, c1)) = clamp_axis(c_lo, c_hi, sheet_ncols) else {
            return Some(None);
        };
        Some(Some((r0, c0, r1, c1)))
    }
}

/// Merged lanes of one column over one chunk segment of the union rows.
struct SegLanes {
    /// First absolute row covered.
    row: usize,
    numbers: Option<Arc<Float64Array>>,
    errors: Option<Arc<UInt8Array>>,
}

/// Merged lanes of one argument over the run's union rectangle.
struct ArgLanes<'s> {
    sheet: &'s ArrowSheet,
    c0: usize,
    /// Chunk index of the first union segment.
    first_chunk: usize,
    chunks: usize,
    /// `(col - c0) * chunks + (chunk - first_chunk)`.
    lanes: Vec<Option<SegLanes>>,
}

/// The row segments `[lo, hi]` of `[r0, r1]` by sheet chunk, as
/// `RangeView` iterates them (rows past the sheet end are dropped).
fn segments(
    sheet: &ArrowSheet,
    r0: usize,
    r1: usize,
    mut visit: impl FnMut(usize, usize, usize, usize),
) {
    let starts = &sheet.chunk_starts;
    let sheet_rows = sheet.nrows as usize;
    if sheet_rows == 0 || starts.is_empty() {
        return;
    }
    let row_end = r1.min(sheet_rows - 1);
    if r0 > row_end {
        return;
    }
    let first = starts.partition_point(|&s| s <= r0).saturating_sub(1);
    let end = starts.partition_point(|&s| s <= row_end);
    for ci in first..end {
        let start = starts[ci];
        let stop = starts.get(ci + 1).copied().unwrap_or(sheet_rows);
        let len = stop.saturating_sub(start);
        if len == 0 {
            continue;
        }
        let lo = start.max(r0);
        let hi = (start + len - 1).min(row_end);
        if lo > hi {
            continue;
        }
        visit(ci, start, lo, hi);
    }
}

impl<'s> ArgLanes<'s> {
    fn build(sheet: &'s ArrowSheet, r0: usize, c0: usize, r1: usize, c1: usize) -> Self {
        let mut segs: Vec<(usize, usize, usize, usize)> = Vec::new();
        segments(sheet, r0, r1, |ci, start, lo, hi| {
            segs.push((ci, start, lo, hi))
        });
        let first_chunk = segs.first().map_or(0, |s| s.0);
        let chunks = segs.last().map_or(0, |s| s.0 + 1 - first_chunk);
        let mut lanes: Vec<Option<SegLanes>> = (0..(c1 - c0 + 1) * chunks).map(|_| None).collect();
        for col in c0..=c1 {
            for &(ci, start, lo, hi) in &segs {
                let slot = (col - c0) * chunks + (ci - first_chunk);
                let Some(ch) = sheet.columns.get(col).and_then(|c| c.chunk(ci)) else {
                    lanes[slot] = Some(SegLanes {
                        row: lo,
                        numbers: None,
                        errors: None,
                    });
                    continue;
                };
                let range = (lo - start)..(hi - start + 1);
                let cascade = OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                let overlaid = cascade.has_any_in_range(range.clone());
                let base_n = ch.numbers_or_null().slice(range.start, range.len());
                let base_e = ch.errors_or_null().slice(range.start, range.len());
                let (numbers, errors) = if overlaid {
                    (
                        cascade.select_numbers(range.clone(), &base_n),
                        cascade.select_errors(range, &base_e),
                    )
                } else {
                    (Arc::new(base_n), Arc::new(base_e))
                };
                lanes[slot] = Some(SegLanes {
                    row: lo,
                    numbers: Some(numbers),
                    errors: Some(errors),
                });
            }
        }
        Self {
            sheet,
            c0,
            first_chunk,
            chunks,
            lanes,
        }
    }

    fn lane(&self, col: usize, ci: usize) -> Option<&SegLanes> {
        let k = ci.checked_sub(self.first_chunk)?;
        if k >= self.chunks {
            return None;
        }
        self.lanes
            .get((col - self.c0) * self.chunks + k)
            .and_then(Option::as_ref)
    }
}

enum Step {
    Error(ExcelErrorKind),
    Continue,
}

impl ArgLanes<'_> {
    /// One argument of one member: the first error, else the sum and count
    /// accumulated into `total`/`count` in the builtin's order.
    fn reduce(&self, rect: (u32, u32, u32, u32), total: &mut f64, count: &mut i64) -> Step {
        let (r0, c0, r1, c1) = (
            rect.0 as usize,
            rect.1 as usize,
            rect.2 as usize,
            rect.3 as usize,
        );
        // Fast path for single-row reads (e.g. `=SUM(1:1)`, `=AVERAGE(2:2)`):
        // at most one chunk segment, zero per-member heap allocation, and O(1)
        // validity checks per column.
        if r0 == r1 {
            let mut seg_ci = None;
            segments(self.sheet, r0, r0, |ci, _, _, _| seg_ci = Some(ci));
            let Some(ci) = seg_ci else {
                return Step::Continue;
            };
            for col in c0..=c1 {
                let Some(seg) = self.lane(col, ci) else {
                    continue;
                };
                if let Some(errors) = &seg.errors {
                    let off = r0 - seg.row;
                    if errors.is_valid(off) {
                        return Step::Error(crate::arrow_store::unmap_error_code(
                            errors.value(off),
                        ));
                    }
                }
            }
            for col in c0..=c1 {
                let Some(seg) = self.lane(col, ci) else {
                    continue;
                };
                if let Some(numbers) = &seg.numbers {
                    let off = r0 - seg.row;
                    if numbers.is_valid(off) {
                        *total += numbers.value(off);
                        *count += 1;
                    }
                }
            }
            return Step::Continue;
        }

        let mut segs: smallvec::SmallVec<[(usize, usize, usize); 4]> = smallvec::SmallVec::new();
        segments(self.sheet, r0, r1, |ci, _, lo, hi| segs.push((ci, lo, hi)));
        // Errors: every segment, every column, before any number.
        for &(ci, lo, hi) in &segs {
            for col in c0..=c1 {
                let Some(seg) = self.lane(col, ci) else {
                    continue;
                };
                if let Some(errors) = &seg.errors {
                    let slice = lane_slice(errors.as_ref(), lo - seg.row, hi - lo + 1);
                    if let Some(i) = slice.first_valid() {
                        return Step::Error(crate::arrow_store::unmap_error_code(slice.values[i]));
                    }
                }
            }
        }
        if c0 == c1 {
            for &(ci, lo, hi) in &segs {
                if let Some(seg) = self.lane(c0, ci)
                    && let Some(numbers) = &seg.numbers
                {
                    let slice = lane_slice(numbers.as_ref(), lo - seg.row, hi - lo + 1);
                    *count += (slice.len() - slice.null_count()) as i64;
                    add_f64(total, slice);
                }
            }
            return Step::Continue;
        }
        let mut slices: Vec<LaneSlice<'_, f64>> = Vec::with_capacity(c1 - c0 + 1);
        for &(ci, lo, hi) in &segs {
            slices.clear();
            for col in c0..=c1 {
                if let Some(seg) = self.lane(col, ci)
                    && let Some(numbers) = &seg.numbers
                {
                    let slice = lane_slice(numbers.as_ref(), lo - seg.row, hi - lo + 1);
                    *count += (slice.len() - slice.null_count()) as i64;
                    slices.push(slice);
                }
            }
            if let [slice] = slices.as_slice() {
                add_f64(total, *slice);
            } else if !slices.is_empty() {
                for row in 0..=hi - lo {
                    for slice in &slices {
                        *total += if slice.is_valid(row) {
                            slice.values.get(row).copied().unwrap_or(0.0)
                        } else {
                            0.0
                        };
                    }
                }
            }
        }
        Step::Continue
    }
}

impl ArgLanes<'_> {
    /// `MIN`/`MAX` of one argument of one member, as the builtin: the
    /// first error (every segment and column, before any number), then per
    /// segment and column Arrow's `min`/`max` of the number lane slice
    /// (the builtin's own kernel on the same values), replacing the running
    /// value on a strict `<` / `>`.
    fn extremum(&self, rect: (u32, u32, u32, u32), max: bool, acc: &mut Option<f64>) -> Step {
        let (r0, c0, r1, c1) = (
            rect.0 as usize,
            rect.1 as usize,
            rect.2 as usize,
            rect.3 as usize,
        );
        let mut segs: smallvec::SmallVec<[(usize, usize, usize); 4]> = smallvec::SmallVec::new();
        segments(self.sheet, r0, r1, |ci, _, lo, hi| segs.push((ci, lo, hi)));
        for &(ci, lo, hi) in &segs {
            for col in c0..=c1 {
                let Some(seg) = self.lane(col, ci) else {
                    continue;
                };
                if let Some(errors) = &seg.errors {
                    let slice = lane_slice(errors.as_ref(), lo - seg.row, hi - lo + 1);
                    if let Some(i) = slice.first_valid() {
                        return Step::Error(crate::arrow_store::unmap_error_code(slice.values[i]));
                    }
                }
            }
        }
        for &(ci, lo, hi) in &segs {
            for col in c0..=c1 {
                let Some(seg) = self.lane(col, ci) else {
                    continue;
                };
                let Some(numbers) = &seg.numbers else {
                    continue;
                };
                let slice = numbers.slice(lo - seg.row, hi - lo + 1);
                let n = if max {
                    arrow_arith::aggregate::max(&slice)
                } else {
                    arrow_arith::aggregate::min(&slice)
                };
                if let Some(n) = n
                    && acc.is_none_or(|current| if max { n > current } else { n < current })
                {
                    *acc = Some(n);
                }
            }
        }
        Step::Continue
    }

    /// `COUNT` of one argument of one member: the non-null numbers of every
    /// segment and column (the builtin does not look at errors).
    fn count(&self, rect: (u32, u32, u32, u32), count: &mut i64) {
        let (r0, c0, r1, c1) = (
            rect.0 as usize,
            rect.1 as usize,
            rect.2 as usize,
            rect.3 as usize,
        );
        segments(self.sheet, r0, r1, |ci, _, lo, hi| {
            for col in c0..=c1 {
                if let Some(seg) = self.lane(col, ci)
                    && let Some(numbers) = &seg.numbers
                {
                    let slice = lane_slice(numbers.as_ref(), lo - seg.row, hi - lo + 1);
                    *count += (slice.len() - slice.null_count()) as i64;
                }
            }
        });
    }
}

/// A planned aggregate kernel for one template.
pub(super) struct AggregateKernel {
    kind: FamilyKernel,
    args: smallvec::SmallVec<[ArgRect; 4]>,
}

impl AggregateKernel {
    /// Plan from a template root, or `None` if it is not a supported shape.
    pub(super) fn plan<R: EvaluationContext>(
        engine: &Engine<R>,
        ds: &DataStore,
        template: AstNodeId,
        current_sheet: SheetId,
    ) -> Option<Self> {
        if engine.force_materialize_range_views {
            return None;
        }
        let AstNodeData::Function { name_id, .. } = ds.get_node(template)? else {
            return None;
        };
        let fun = engine
            .resolved_functions
            .get_for_optimized_call(*name_id, engine.operation_diagnostic_collector.is_some())?;
        let kind = crate::external_callbacks::metadata(fun, || None, || fun.family_kernel())
            .filter(|k| {
                matches!(
                    k,
                    FamilyKernel::Sum
                        | FamilyKernel::Average
                        | FamilyKernel::Min
                        | FamilyKernel::Max
                        | FamilyKernel::Count
                )
            })?;
        let args = ds.get_args(template)?;
        if args.is_empty() {
            return None;
        }
        let mut out = smallvec::SmallVec::new();
        for &arg in args {
            let AstNodeData::Reference { ref_type, .. } = ds.get_node(arg)? else {
                return None;
            };
            let ref_type = ds.compact_ref(*ref_type);
            let sheet_of = |sheet: &Option<SheetKey>| -> Option<SheetId> {
                match sheet {
                    None => Some(current_sheet),
                    Some(SheetKey::Id(id)) => {
                        let name = engine.graph.sheet_reg().name(*id);
                        (!name.is_empty() && engine.graph.sheet_id(name) == Some(*id))
                            .then_some(*id)
                    }
                    Some(SheetKey::Name(_)) => None,
                }
            };
            let rect = match ref_type {
                CompactRefType::Cell {
                    sheet,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } => {
                    if *row == 0 || *col == 0 {
                        return None;
                    }
                    ArgRect {
                        sheet: sheet_of(sheet)?,
                        rows: [ArgBound::At(*row, *row_abs), ArgBound::At(*row, *row_abs)],
                        cols: [ArgBound::At(*col, *col_abs), ArgBound::At(*col, *col_abs)],
                    }
                }
                CompactRefType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                } => {
                    if *end_row == 0 || *end_col == 0 {
                        return None;
                    }
                    ArgRect {
                        sheet: sheet_of(sheet)?,
                        rows: [
                            ArgBound::from_start(*start_row, *start_row_abs),
                            ArgBound::from_end(*end_row, *end_row_abs),
                        ],
                        cols: [
                            ArgBound::from_start(*start_col, *start_col_abs),
                            ArgBound::from_end(*end_col, *end_col_abs),
                        ],
                    }
                }
                _ => return None,
            };
            // MIN/MAX/COUNT: a single explicit cell may resolve as a scalar
            // (numeric text counts, its format propagates); only windows and
            // open-axis ranges here.
            if !matches!(kind, FamilyKernel::Sum | FamilyKernel::Average)
                && rect.rows[0] == rect.rows[1]
                && rect.cols[0] == rect.cols[1]
                && matches!(rect.rows[0], ArgBound::At(..))
                && matches!(rect.cols[0], ArgBound::At(..))
            {
                return None;
            }
            out.push(rect);
        }
        Some(Self { kind, args: out })
    }

    pub(super) fn name(&self) -> &'static str {
        self.kind.name()
    }

    /// Values of the run's members (rows `row0..`, fixed column offset), or
    /// `None` to decline the whole run.
    pub(super) fn run<R: EvaluationContext>(
        &self,
        engine: &Engine<R>,
        anchor: (u32, u32),
        row0: u32,
        len: usize,
        col: u32,
    ) -> Option<Vec<LiteralValue>> {
        let col_delta = i64::from(col) - i64::from(anchor.1);
        let first = i64::from(row0) - i64::from(anchor.0);
        let last = first + len as i64 - 1;
        // Per argument: validate both run endpoints and build merged lanes
        // once over the clamped union rectangle (or `None` when empty).
        let mut arg_sheets: smallvec::SmallVec<[(u32, u32, Option<ArgLanes<'_>>); 4]> =
            smallvec::SmallVec::new();
        for arg in &self.args {
            let sheet_name = engine.graph.sheet_name(arg.sheet);
            let sheet = engine.sheet_store().sheet(sheet_name)?;
            let sheet_nrows = sheet.nrows;
            let sheet_ncols = sheet.columns.len() as u32;
            let lanes = arg
                .union_clamped(first, last, col_delta, sheet_nrows, sheet_ncols)?
                .map(|(r0, c0, r1, c1)| {
                    ArgLanes::build(sheet, r0 as usize, c0 as usize, r1 as usize, c1 as usize)
                });
            arg_sheets.push((sheet_nrows, sheet_ncols, lanes));
        }

        let eval_member = |row_delta: i64| -> Option<LiteralValue> {
            let mut total = 0.0f64;
            let mut count = 0i64;
            let mut error = None;
            let mut extremum: Option<f64> = None;
            for (arg, (sheet_nrows, sheet_ncols, lanes)) in self.args.iter().zip(&arg_sheets) {
                let Some(rect) = arg.at(row_delta, col_delta, *sheet_nrows, *sheet_ncols)? else {
                    continue;
                };
                let Some(lanes) = lanes.as_ref() else {
                    continue;
                };
                let step = match self.kind {
                    FamilyKernel::Min | FamilyKernel::Max => {
                        lanes.extremum(rect, self.kind == FamilyKernel::Max, &mut extremum)
                    }
                    FamilyKernel::Count => {
                        lanes.count(rect, &mut count);
                        Step::Continue
                    }
                    _ => lanes.reduce(rect, &mut total, &mut count),
                };
                if let Step::Error(kind) = step {
                    error = Some(kind);
                    break;
                }
            }
            let value = match (error, self.kind) {
                (None, FamilyKernel::Min | FamilyKernel::Max) => {
                    crate::builtins::utils::aggregate_result(extremum.unwrap_or(0.0))
                }
                (None, FamilyKernel::Count) => LiteralValue::Number(count as f64),
                (Some(kind), _) => LiteralValue::Error(ExcelError::new(kind)),
                (None, FamilyKernel::Sum) => crate::builtins::utils::aggregate_result(total),
                (None, FamilyKernel::Average) => {
                    if count == 0 {
                        LiteralValue::Error(ExcelError::new_div())
                    } else {
                        crate::builtins::utils::aggregate_result(total / (count as f64))
                    }
                }
                (None, _) => unreachable!("planned kernels are windowed aggregates"),
            };
            Some(crate::engine::result_finalization::finalize_formula_result(
                value,
            ))
        };

        // When every argument is row-invariant across the column run (e.g.
        // `=SUM($1:$1)`, `=SUM(A:A)`, `=SUM($3:$13)`), every member reads the
        // exact same rectangles and produces the exact same value.
        let panic_hook_inactive = {
            #[cfg(test)]
            {
                engine
                    .panic_kernel_after_member
                    .load(std::sync::atomic::Ordering::Relaxed)
                    >= len
            }
            #[cfg(not(test))]
            {
                true
            }
        };
        if len > 0 && panic_hook_inactive && self.args.iter().all(ArgRect::is_row_invariant) {
            let value = eval_member(first)?;
            return Some(vec![value; len]);
        }

        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            #[cfg(test)]
            if engine
                .panic_kernel_after_member
                .compare_exchange(
                    i,
                    usize::MAX,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                panic!("injected aggregate kernel panic");
            }
            out.push(eval_member(first + i as i64)?);
        }
        Some(out)
    }
}

#[cfg(test)]
mod sequential_tests {
    use crate::engine::{Engine, EvalConfig};
    use crate::test_workbook::TestWorkbook;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::parse;

    #[test]
    fn sequential_ranges_cross_arrow_chunks_and_family_paths() {
        for family in [false, true] {
            let mut engine = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    family_execution: family,
                    arrow_storage_enabled: true,
                    delta_overlay_enabled: true,
                    write_formula_overlay_enabled: true,
                    ..EvalConfig::default()
                },
            );
            let mut ingest = engine.begin_bulk_ingest_arrow();
            ingest.add_sheet("Sheet1", 2, 2);
            ingest
                .append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
            for value in [1e16, 1.0, -1e16, 1.0] {
                ingest
                    .append_row(
                        "Sheet1",
                        &[LiteralValue::Number(value), LiteralValue::Number(1.0)],
                    )
                    .unwrap();
            }
            ingest.finish().unwrap();
            let cases = [
                ("=SUM($A$1:$A$5)", 1.0),
                ("=SUM($A$1:$B$5)", 3.0),
                ("=SUM($A$2,$A$3:$A$5)", 1.0),
                ("=AVERAGE($A$1:$A$5)", 0.25),
                ("=AVERAGE($A$1:$B$5)", 0.375),
                ("=SUMIF($B$1:$B$5,\">0\",$A$1:$A$5)", 1.0),
                ("=SUMIFS($A$1:$A$5,$B$1:$B$5,\">0\")", 1.0),
                ("=AVERAGEIF($B$1:$B$5,\">0\",$A$1:$A$5)", 0.25),
                ("=AVERAGEIFS($A$1:$A$5,$B$1:$B$5,\">0\")", 0.25),
                ("=SUMIFS($A$1:$B$5,$A$1:$B$5,\"<>\")", 3.0),
            ];
            for (column, (formula, _)) in cases.iter().enumerate() {
                for row in 1..=40 {
                    engine
                        .set_cell_formula("Sheet1", row, column as u32 + 4, parse(formula).unwrap())
                        .unwrap();
                }
            }
            engine.evaluate_all().unwrap();
            for (column, (formula, expected)) in cases.iter().enumerate() {
                for row in 1..=40 {
                    assert_eq!(
                        engine.get_cell_value("Sheet1", row, column as u32 + 4),
                        Some(LiteralValue::Number(*expected)),
                        "family={family} row={row} {formula}"
                    );
                }
            }
            if family {
                assert!(engine.family_members_for_test() > 0);
            }
        }
    }
}
