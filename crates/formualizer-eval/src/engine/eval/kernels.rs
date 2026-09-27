//! Program 2 tier-3 range kernels (P2-M4): windowed aggregates over a
//! family run.
//!
//! A run whose template is `SUM`/`AVERAGE` of bounded references (cells or
//! ranges, relative or absolute) resolves each argument's rectangle once
//! per member, merges the overlays of the run's union rows once per column
//! chunk (decision 20.3), and reduces each member's slice in the scalar
//! builtin's exact order: per argument, errors first (segment by chunk,
//! then column, then row), then `total += sum(slice)` per segment per
//! column with Arrow's reduction replicated bit for bit (`exact::sum_f64`;
//! no reassociation, no prefix sums, decision 20.2).
//!
//! Anything else declines (the tier-1 path evaluates it): open ranges,
//! names, 3-D or external references, missing sheets, relocation out of
//! the grid, reversed rectangles, forced materialization, or a function
//! that is not the built-in (an override keeps `family_kernel() == None`).

use super::exact::{LaneSlice, lane_slice, sum_f64};
use super::*;
use crate::arrow_store::{ArrowSheet, OverlayCascade};
use crate::engine::arena::{AstNodeData, CompactRefType, DataStore, SheetKey};
use crate::function::FamilyKernel;
use arrow_array::{Float64Array, UInt8Array};

/// One template argument: a bounded rectangle, 1-based as stored, with
/// per-bound absolute flags.
#[derive(Clone, Copy, Debug)]
struct ArgRect {
    sheet: SheetId,
    rows: [(u32, bool); 2],
    cols: [(u32, bool); 2],
}

fn shift(value: u32, delta: i64, abs: bool) -> Option<u32> {
    if abs {
        return Some(value);
    }
    let v = i64::from(value) + delta;
    (1..=i64::from(u32::MAX)).contains(&v).then_some(v as u32)
}

impl ArgRect {
    /// 0-based inclusive `(r0, c0, r1, c1)` at an offset, or `None` where the
    /// per-cell path would not read a plain rectangle.
    fn at(&self, row_delta: i64, col_delta: i64) -> Option<(u32, u32, u32, u32)> {
        let r0 = shift(self.rows[0].0, row_delta, self.rows[0].1)?;
        let r1 = shift(self.rows[1].0, row_delta, self.rows[1].1)?;
        let c0 = shift(self.cols[0].0, col_delta, self.cols[0].1)?;
        let c1 = shift(self.cols[1].0, col_delta, self.cols[1].1)?;
        (r0 <= r1 && c0 <= c1).then(|| (r0 - 1, c0 - 1, r1 - 1, c1 - 1))
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
        for &(ci, lo, hi) in &segs {
            for col in c0..=c1 {
                let seg = self.lane(col, ci);
                match seg.and_then(|s| s.numbers.as_ref()) {
                    Some(numbers) => {
                        let seg = seg.expect("lane");
                        let slice: LaneSlice<'_, f64> =
                            lane_slice(numbers.as_ref(), lo - seg.row, hi - lo + 1);
                        *total += sum_f64(slice).unwrap_or(0.0);
                        *count += (slice.len() - slice.null_count()) as i64;
                    }
                    None => {
                        *total += 0.0;
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
                    arrow::compute::kernels::aggregate::max(&slice)
                } else {
                    arrow::compute::kernels::aggregate::min(&slice)
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
        let name = ds.resolve_ast_string(*name_id);
        let fun = crate::traits::FunctionProvider::get_function(engine, "", name)?;
        let kind = fun.family_kernel().filter(|k| {
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
                } => ArgRect {
                    sheet: sheet_of(sheet)?,
                    rows: [(*row, *row_abs), (*row, *row_abs)],
                    cols: [(*col, *col_abs), (*col, *col_abs)],
                },
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
                    if *start_row == 0
                        || *start_col == 0
                        || *end_row == u32::MAX
                        || *end_col == u32::MAX
                    {
                        return None;
                    }
                    ArgRect {
                        sheet: sheet_of(sheet)?,
                        rows: [(*start_row, *start_row_abs), (*end_row, *end_row_abs)],
                        cols: [(*start_col, *start_col_abs), (*end_col, *end_col_abs)],
                    }
                }
                _ => return None,
            };
            if rect.rows[0].0 == 0 || rect.cols[0].0 == 0 {
                return None;
            }
            // MIN/MAX/COUNT: a single cell may resolve as a scalar (numeric
            // text counts, its format propagates); only windows here.
            if !matches!(kind, FamilyKernel::Sum | FamilyKernel::Average)
                && rect.rows[0] == rect.rows[1]
                && rect.cols[0] == rect.cols[1]
            {
                return None;
            }
            out.push(rect);
        }
        Some(Self { kind, args: out })
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
        // Per argument: the union rectangle (rows vary monotonically with the
        // member offset, so the union of the first and last members' rects
        // covers every member), and the merged lanes over it.
        let mut lanes: smallvec::SmallVec<[ArgLanes<'_>; 4]> = smallvec::SmallVec::new();
        for arg in &self.args {
            let a = arg.at(first, col_delta)?;
            let b = arg.at(last, col_delta)?;
            let sheet_name = engine.graph.sheet_name(arg.sheet);
            let sheet = engine.sheet_store().sheet(sheet_name)?;
            lanes.push(ArgLanes::build(
                sheet,
                a.0.min(b.0) as usize,
                a.1 as usize,
                a.2.max(b.2) as usize,
                a.3 as usize,
            ));
        }
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let row_delta = first + i as i64;
            let mut total = 0.0f64;
            let mut count = 0i64;
            let mut error = None;
            let mut extremum: Option<f64> = None;
            for (arg, lanes) in self.args.iter().zip(&lanes) {
                let rect = arg.at(row_delta, col_delta)?;
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
            out.push(crate::engine::result_finalization::finalize_formula_result(
                value,
            ));
        }
        Some(out)
    }
}
