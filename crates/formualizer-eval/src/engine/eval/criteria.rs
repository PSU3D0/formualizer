//! Program 2 criteria kernels (P2-M4): a family run of `SUMIF(S)`,
//! `COUNTIF(S)` or `AVERAGEIF(S)` whose ranges are absolute (invariant)
//! indexes them once per run and reduces each member's matches.
//!
//! The scalar builtin (`eval_if_family`) builds, per criteria range, the
//! engine's criteria mask (`compute_criteria_mask`) and ANDs them: a row
//! matches when every mask is non-null true. For the predicates taken here
//! that mask is a pure function of one row's merged lanes:
//! - numeric (`>` `>=` `<` `<=`, `=`/`<>` a number): Arrow `cmp` of the
//!   row's number lane against the operand; a non-number row is null;
//! - `=`/`<>` a non-empty text: Arrow `ilike`/`nilike` of the row's
//!   lowered-text lane against the lowered operand; a non-text row is null
//!   for `=` and true for `<>`.
//!
//! The index groups each column's rows by distinct lane value, and a member
//! evaluates its predicate once per distinct value with the same Arrow
//! kernel, so every row gets the mask's own verdict. It then walks the
//! matching rows of its most selective criterion in row order, checks the
//! others by class, and reduces as the builtin does: a count, or per driver
//! row chunk the Arrow sum (`exact::sum_f64`) of the filtered target lane,
//! with the non-null count for averages.
//!
//! Whole columns (`$F:$F`) and open-ended columns (`$F$2:$F`) are views of
//! the used extent, as the builtin resolves them.
//!
//! Anything else declines (the memo or the walk evaluates it): other
//! predicates (empty text, wildcards, booleans, errors, blanks), a 1x1
//! range, numeric text equality over a column containing numbers,
//! ranges of different heights or past the sheet's last row,
//! a lane layout the mask path would read differently, a criterion whose
//! evaluation fails, and `COUNTIF` whose criterion matches an empty cell
//! (it counts logical cells past the data).

use super::exact::{LaneSlice, sum_f64};
use super::*;
use crate::args::CriteriaPredicate as P;
use crate::engine::arena::{AstNodeData, CompactRefType, DataStore};
use crate::engine::range_view::RangeView;
use crate::function::FamilyKernel;
use arrow_array::{Array as _, BooleanArray, Float64Array, StringArray};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Agg {
    Count,
    Sum,
    Average,
}

/// A run's criteria template: the aggregate, its target range, and
/// (criteria range, criterion argument) pairs.
pub(super) struct CriteriaPlan {
    agg: Agg,
    single: bool,
    target: Option<AstNodeId>,
    criteria: smallvec::SmallVec<[(AstNodeId, AstNodeId); 4]>,
}

/// Whether `arg` is an absolute single-column range, bounded (of at least
/// two rows) or open (`$F:$F`, `$F$2:$F`: its view is the used extent, the
/// same for every member).
fn invariant_column(ds: &DataStore, arg: AstNodeId) -> bool {
    match ds.get_node(arg) {
        Some(AstNodeData::Reference {
            ref_type:
                CompactRefType::Range {
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs: true,
                    end_row_abs,
                    end_col_abs: true,
                    ..
                },
            ..
        }) if *start_col > 0 && start_col == end_col => {
            let start_fixed = *start_row == 0 || (*start_row_abs && *start_row > 0);
            let end_fixed = *end_row == u32::MAX || (*end_row_abs && *end_row > *start_row);
            start_fixed && end_fixed && (*end_row == u32::MAX || *start_row > 0)
        }
        _ => false,
    }
}

impl CriteriaPlan {
    pub(super) fn plan(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        template: AstNodeId,
    ) -> Option<Self> {
        let AstNodeData::Function { name_id, .. } = ds.get_node(template)? else {
            return None;
        };
        let fun = functions.get_function("", ds.resolve_ast_string(*name_id))?;
        if fun.family_kernel() != Some(FamilyKernel::CriteriaAggregate) {
            return None;
        }
        let args: smallvec::SmallVec<[AstNodeId; 8]> =
            ds.get_args(template)?.iter().copied().collect();
        let (agg, single) = match fun.name() {
            "COUNTIF" => (Agg::Count, true),
            "SUMIF" => (Agg::Sum, true),
            "AVERAGEIF" => (Agg::Average, true),
            "COUNTIFS" => (Agg::Count, false),
            "SUMIFS" => (Agg::Sum, false),
            "AVERAGEIFS" => (Agg::Average, false),
            _ => return None,
        };
        let (target, pairs): (Option<AstNodeId>, &[AstNodeId]) = match (agg, single) {
            (Agg::Count, true) if args.len() == 2 => (None, &args[..]),
            // `SUMIF(range, criterion[, target])`: the target defaults to
            // the criteria range.
            (_, true) if args.len() == 2 => (Some(args[0]), &args[..]),
            (_, true) if args.len() == 3 => (Some(args[2]), &args[..2]),
            (Agg::Count, false) if args.len() >= 2 && args.len().is_multiple_of(2) => {
                (None, &args[..])
            }
            (_, false) if args.len() >= 3 && (args.len() - 1).is_multiple_of(2) => {
                (Some(args[0]), &args[1..])
            }
            _ => return None,
        };
        let mut criteria = smallvec::SmallVec::new();
        for pair in pairs.chunks(2) {
            if !invariant_column(ds, pair[0]) {
                return None;
            }
            // A criterion that is itself a range is not one value.
            if matches!(
                ds.get_node(pair[1]),
                Some(AstNodeData::Reference {
                    ref_type: CompactRefType::Range { .. },
                    ..
                })
            ) {
                return None;
            }
            criteria.push((pair[0], pair[1]));
        }
        if let Some(t) = target
            && !invariant_column(ds, t)
        {
            return None;
        }
        Some(Self {
            agg,
            single,
            target,
            criteria,
        })
    }
}

/// One criteria column grouped by distinct lane value. `u32::MAX` is the
/// null class (no number, or no text).
struct Classes<K> {
    of_row: Vec<u32>,
    values: Vec<K>,
    rows: Vec<Vec<u32>>,
    null_rows: Vec<u32>,
}

impl<K> Classes<K> {
    fn push(&mut self, row: u32, class: Option<u32>) {
        match class {
            Some(c) => {
                self.of_row.push(c);
                self.rows[c as usize].push(row);
            }
            None => {
                self.of_row.push(u32::MAX);
                self.null_rows.push(row);
            }
        }
    }
}

/// One criteria column's classes, each kind built on first use (a
/// column read only by text criteria never indexes its numbers).
struct ColumnIndex {
    range: AstNodeId,
    numbers: std::sync::OnceLock<Option<Classes<f64>>>,
    texts: std::sync::OnceLock<Option<Classes<String>>>,
}

impl ColumnIndex {
    fn numbers<R: EvaluationContext>(
        &self,
        engine: &Engine<R>,
        ds: &DataStore,
        sheet: &str,
        rows: usize,
    ) -> Option<&Classes<f64>> {
        self.numbers
            .get_or_init(|| index_numbers(&engine.criteria_view(ds, self.range, sheet)?, rows))
            .as_ref()
    }

    fn texts<R: EvaluationContext>(
        &self,
        engine: &Engine<R>,
        ds: &DataStore,
        sheet: &str,
        rows: usize,
    ) -> Option<&Classes<String>> {
        self.texts
            .get_or_init(|| index_texts(&engine.criteria_view(ds, self.range, sheet)?, rows))
            .as_ref()
    }
}

/// A run's index: every criteria column (numbers and lowered texts, as the
/// mask path reads them), the driver's row chunks and the target lane.
pub(super) struct CriteriaIndex {
    rows: usize,
    chunks: Vec<(usize, usize)>,
    columns: Vec<ColumnIndex>,
    target: Option<AstNodeId>,
    /// Target number lane per row (value, valid), on first use.
    target_lane: std::sync::OnceLock<Option<(Vec<f64>, Vec<bool>)>>,
    /// Rows grouped by their tuple of classes, per class-kind signature
    /// (text or number per criterion), on first use; `None` when there are
    /// too many tuples to scan per member.
    combos: std::sync::Mutex<Vec<ComboEntry>>,
    /// Whether members group rows by class tuple: the grouping's pass over
    /// the rows pays only across a long run (`COMBO_MIN_RUN`).
    group: bool,
}

/// A class-kind signature and its grouped rows (`None`: too many tuples).
type ComboEntry = (Vec<bool>, Option<std::sync::Arc<Combos>>);

/// Rows grouped by their class in every criterion (`u32::MAX`: null).
struct Combos {
    classes: Vec<smallvec::SmallVec<[u32; 4]>>,
    rows: Vec<Vec<u32>>,
}

/// More distinct class tuples than this: members walk their driver's rows.
const MAX_COMBOS: usize = 4096;
/// Runs shorter than this take the driver criterion's rows with per-row
/// checks instead of grouping rows by class tuple: a recalculation of a
/// few members over a large table (real_ops_model: ~20 `SUMIFS` per edit)
/// pays more for the grouping pass than it saves.
const COMBO_MIN_RUN: u32 = 32;

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn criteria_view<'c>(
        &'c self,
        ds: &DataStore,
        arg: AstNodeId,
        current_sheet: &str,
    ) -> Option<RangeView<'c>> {
        let AstNodeData::Reference { ref_type, .. } = ds.get_node(arg)? else {
            return None;
        };
        let reference = ds.reconstruct_reference_type_for_eval(ref_type, self.graph.sheet_reg());
        self.resolve_range_view(&reference, current_sheet).ok()
    }

    /// Build the run's index, or `None` where the plan declines.
    pub(super) fn build_criteria_index(
        &self,
        plan: &CriteriaPlan,
        ds: &DataStore,
        current_sheet: &str,
        run_len: u32,
    ) -> Option<CriteriaIndex> {
        let views: Vec<RangeView<'_>> = plan
            .criteria
            .iter()
            .map(|&(range, _)| self.criteria_view(ds, range, current_sheet))
            .collect::<Option<_>>()?;
        let target = match plan.target {
            Some(t) => Some(self.criteria_view(ds, t, current_sheet)?),
            None => None,
        };
        let (rows, cols) = views[0].dims();
        // Same heights, one column, every row on the sheet (no padding).
        for v in views.iter().chain(target.iter()) {
            if v.dims() != (rows, cols)
                || cols != 1
                || rows < 2
                || v.start_row() + rows > v.sheet().nrows as usize
            {
                return None;
            }
        }
        // The builtin drives chunks from the criteria range (single forms)
        // or the target, else the first criteria range.
        let driver = if plan.single {
            &views[0]
        } else {
            target.as_ref().unwrap_or(&views[0])
        };
        let mut chunks = Vec::new();
        let mut next = 0usize;
        for chunk in driver.iter_row_chunks() {
            let chunk = chunk.ok()?;
            if chunk.row_start != next {
                return None;
            }
            next += chunk.row_len;
            chunks.push((chunk.row_start, chunk.row_len));
        }
        if next != rows {
            return None;
        }
        let columns = plan
            .criteria
            .iter()
            .map(|&(range, _)| ColumnIndex {
                range,
                numbers: std::sync::OnceLock::new(),
                texts: std::sync::OnceLock::new(),
            })
            .collect();
        Some(CriteriaIndex {
            rows,
            chunks,
            columns,
            target: plan.target,
            target_lane: std::sync::OnceLock::new(),
            combos: std::sync::Mutex::new(Vec::new()),
            group: run_len >= COMBO_MIN_RUN,
        })
    }

    /// The run's class tuples for a kind signature (the columns' classes
    /// of those kinds are built already).
    fn combos(index: &CriteriaIndex, kinds: &[bool]) -> Option<std::sync::Arc<Combos>> {
        let mut cache = index.combos.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, c)) = cache.iter().find(|(k, _)| k.as_slice() == kinds) {
            return c.clone();
        }
        let of_row: Vec<&[u32]> = kinds
            .iter()
            .enumerate()
            .map(|(j, &text)| {
                let column = &index.columns[j];
                if text {
                    column
                        .texts
                        .get()
                        .and_then(Option::as_ref)
                        .map(|c| &c.of_row[..])
                } else {
                    column
                        .numbers
                        .get()
                        .and_then(Option::as_ref)
                        .map(|c| &c.of_row[..])
                }
            })
            .collect::<Option<_>>()?;
        let mut by_tuple: rustc_hash::FxHashMap<smallvec::SmallVec<[u32; 4]>, u32> =
            rustc_hash::FxHashMap::default();
        let mut combos = Combos {
            classes: Vec::new(),
            rows: Vec::new(),
        };
        let mut fits = true;
        for row in 0..index.rows {
            let tuple: smallvec::SmallVec<[u32; 4]> = of_row.iter().map(|c| c[row]).collect();
            let id = match by_tuple.get(&tuple) {
                Some(&id) => id,
                None => {
                    if combos.classes.len() == MAX_COMBOS {
                        fits = false;
                        break;
                    }
                    combos.classes.push(tuple.clone());
                    combos.rows.push(Vec::new());
                    let id = (combos.classes.len() - 1) as u32;
                    by_tuple.insert(tuple, id);
                    id
                }
            };
            combos.rows[id as usize].push(row as u32);
        }
        let entry = fits.then(|| std::sync::Arc::new(combos));
        cache.push((kinds.to_vec(), entry.clone()));
        entry
    }

    /// The target range's number lane over the driver's chunks.
    fn target_lane<'i>(
        &self,
        index: &'i CriteriaIndex,
        ds: &DataStore,
        sheet: &str,
    ) -> Option<&'i (Vec<f64>, Vec<bool>)> {
        index
            .target_lane
            .get_or_init(|| {
                let v = self.criteria_view(ds, index.target?, sheet)?;
                let mut values = Vec::with_capacity(index.rows);
                let mut valid = Vec::with_capacity(index.rows);
                for &(start, len) in &index.chunks {
                    let slices = v.slice_numbers(start, len);
                    match slices.first().and_then(|a| a.as_ref()) {
                        Some(a) if a.len() == len => {
                            for i in 0..len {
                                valid.push(a.is_valid(i));
                                values.push(if a.is_valid(i) { a.value(i) } else { 0.0 });
                            }
                        }
                        None => {
                            values.extend(std::iter::repeat_n(0.0, len));
                            valid.extend(std::iter::repeat_n(false, len));
                        }
                        Some(_) => return None,
                    }
                }
                Some((values, valid))
            })
            .as_ref()
    }

    /// One member through the index: its criteria evaluated at its offset,
    /// then matched and reduced. `None`: the member is left to the memo or
    /// the walk.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn criteria_member(
        &self,
        plan: &CriteriaPlan,
        index: &CriteriaIndex,
        interpreter: &crate::interpreter::Interpreter<'_>,
        ds: &DataStore,
        sheet: &str,
        row_delta: i64,
        col_delta: i64,
    ) -> Option<LiteralValue> {
        let reg = self.graph.sheet_reg();
        let rows = index.rows;
        // Per criterion: the verdict of every class, and the null class's.
        let mut verdicts: smallvec::SmallVec<[(Vec<bool>, bool, bool); 4]> =
            smallvec::SmallVec::new();
        for (j, &(_, arg)) in plan.criteria.iter().enumerate() {
            let value = interpreter
                .evaluate_arena_ast_with_offset(arg, row_delta, col_delta, ds, reg)
                .ok()?
                .into_literal();
            let pred = crate::args::parse_criteria(&value).ok()?;
            if plan.single
                && plan.agg == Agg::Count
                && crate::builtins::criteria_match(&pred, &LiteralValue::Empty)
            {
                return None;
            }
            let column = &index.columns[j];
            // Numeric text equality can match both text and numeric rows. A
            // single lane's classes cannot express that mask; use the walk.
            // Text-only columns can still use the indexed text verdicts.
            if is_numeric_text_equality(&pred)
                && !column.numbers(self, ds, sheet, rows)?.values.is_empty()
            {
                return None;
            }
            let verdict = match &pred {
                P::Gt(n) | P::Ge(n) | P::Lt(n) | P::Le(n) => {
                    number_verdicts(column.numbers(self, ds, sheet, rows)?, &pred, *n)?
                }
                P::Eq(LiteralValue::Number(x)) | P::Ne(LiteralValue::Number(x)) => {
                    number_verdicts(column.numbers(self, ds, sheet, rows)?, &pred, *x)?
                }
                P::Eq(LiteralValue::Int(i)) | P::Ne(LiteralValue::Int(i)) => {
                    number_verdicts(column.numbers(self, ds, sheet, rows)?, &pred, *i as f64)?
                }
                P::Eq(LiteralValue::Text(t)) | P::Ne(LiteralValue::Text(t)) if !t.is_empty() => {
                    text_verdicts(
                        column.texts(self, ds, sheet, rows)?,
                        matches!(pred, P::Ne(_)),
                        t,
                    )?
                }
                _ => return None,
            };
            verdicts.push(verdict);
        }
        // Rows grouped by class tuple: a member's matching rows are the
        // tuples every verdict admits (no per-row checks).
        let kinds: smallvec::SmallVec<[bool; 4]> = verdicts.iter().map(|v| v.2).collect();
        let grouped = if index.group {
            Self::combos(index, &kinds)
        } else {
            None
        };
        let exact_rows = grouped.map(|combos| {
            let admitted = |classes: &[u32]| {
                classes
                    .iter()
                    .zip(&verdicts)
                    .all(|(&class, (per_class, null_match, _))| {
                        if class == u32::MAX {
                            *null_match
                        } else {
                            per_class[class as usize]
                        }
                    })
            };
            let hits: smallvec::SmallVec<[usize; 4]> = (0..combos.classes.len())
                .filter(|&k| admitted(&combos.classes[k]))
                .collect();
            match hits.as_slice() {
                [only] => combos.rows[*only].clone(),
                _ => {
                    let mut rows: Vec<u32> = hits
                        .iter()
                        .flat_map(|&k| combos.rows[k].iter().copied())
                        .collect();
                    rows.sort_unstable();
                    rows
                }
            }
        });
        // The most selective criterion drives (fewest matching rows).
        let matching = |j: usize| -> usize {
            let (per_class, null_match, is_text) = &verdicts[j];
            let column = &index.columns[j];
            let (rows, null_rows): (&[Vec<u32>], &[u32]) = if *is_text {
                let c = column
                    .texts
                    .get()
                    .and_then(Option::as_ref)
                    .expect("text classes");
                (&c.rows, &c.null_rows)
            } else {
                let c = column
                    .numbers
                    .get()
                    .and_then(Option::as_ref)
                    .expect("number classes");
                (&c.rows, &c.null_rows)
            };
            per_class
                .iter()
                .zip(rows)
                .filter(|(m, _)| **m)
                .map(|(_, r)| r.len())
                .sum::<usize>()
                + if *null_match { null_rows.len() } else { 0 }
        };
        let exact = exact_rows.is_some();
        let driver = (0..verdicts.len()).min_by_key(|&j| matching(j))?;
        let mut rows: Vec<u32> = if let Some(rows) = exact_rows {
            rows
        } else {
            let (per_class, null_match, is_text) = &verdicts[driver];
            let column = &index.columns[driver];
            let (class_rows, null_rows): (&[Vec<u32>], &[u32]) = if *is_text {
                let c = column
                    .texts
                    .get()
                    .and_then(Option::as_ref)
                    .expect("text classes");
                (&c.rows, &c.null_rows)
            } else {
                let c = column
                    .numbers
                    .get()
                    .and_then(Option::as_ref)
                    .expect("number classes");
                (&c.rows, &c.null_rows)
            };
            let mut rows: Vec<u32> = per_class
                .iter()
                .zip(class_rows)
                .filter(|(m, _)| **m)
                .flat_map(|(_, r)| r.iter().copied())
                .collect();
            if *null_match {
                rows.extend_from_slice(null_rows);
            }
            rows
        };
        if !exact {
            rows.sort_unstable();
        }
        let passes = |row: u32| {
            exact
                || verdicts
                    .iter()
                    .enumerate()
                    .all(|(j, (per_class, null_match, is_text))| {
                        let column = &index.columns[j];
                        let class = if *is_text {
                            column
                                .texts
                                .get()
                                .and_then(Option::as_ref)
                                .expect("text classes")
                                .of_row[row as usize]
                        } else {
                            column
                                .numbers
                                .get()
                                .and_then(Option::as_ref)
                                .expect("number classes")
                                .of_row[row as usize]
                        };
                        if class == u32::MAX {
                            *null_match
                        } else {
                            per_class[class as usize]
                        }
                    })
        };
        match plan.agg {
            Agg::Count => {
                let count = rows.iter().filter(|&&r| passes(r)).count();
                Some(LiteralValue::Number(count as f64))
            }
            Agg::Sum | Agg::Average => {
                let (values, valid) = self.target_lane(index, ds, sheet)?;
                let mut total_sum = 0.0f64;
                let mut total_count = 0i64;
                let mut it = rows.iter().copied().filter(|&r| passes(r)).peekable();
                let mut buf: Vec<f64> = Vec::new();
                let mut bits: Vec<u8> = Vec::new();
                for &(start, len) in &index.chunks {
                    buf.clear();
                    bits.clear();
                    let end = (start + len) as u32;
                    let mut nulls = 0usize;
                    while let Some(&r) = it.peek() {
                        if r >= end {
                            break;
                        }
                        it.next();
                        let k = buf.len();
                        if k.is_multiple_of(8) {
                            bits.push(0);
                        }
                        if valid[r as usize] {
                            bits[k / 8] |= 1 << (k % 8);
                            buf.push(values[r as usize]);
                        } else {
                            nulls += 1;
                            buf.push(0.0);
                        }
                    }
                    if buf.is_empty() {
                        // `filter` of a chunk with no match: an empty
                        // array, whose sum is `None`.
                        continue;
                    }
                    let slice = LaneSlice {
                        values: &buf,
                        validity: (nulls > 0).then_some((&bits[..], 0)),
                    };
                    if let Some(s) = sum_f64(slice) {
                        total_sum += s;
                    }
                    total_count += (buf.len() - nulls) as i64;
                }
                debug_assert!(it.next().is_none(), "matched rows past the last chunk");
                Some(match plan.agg {
                    Agg::Sum => crate::builtins::utils::aggregate_result(total_sum),
                    _ if total_count == 0 => LiteralValue::Error(ExcelError::new_div()),
                    _ => crate::builtins::utils::aggregate_result(total_sum / total_count as f64),
                })
            }
        }
    }
}

/// Distinct numbers of a single-column view as `numbers_slices` yields
/// them (the numeric mask's input), or `None` if the segments do not tile
/// the rows.
fn index_numbers(view: &RangeView<'_>, rows: usize) -> Option<Classes<f64>> {
    let mut out = Classes {
        of_row: Vec::with_capacity(rows),
        values: Vec::new(),
        rows: Vec::new(),
        null_rows: Vec::new(),
    };
    let mut by_bits: rustc_hash::FxHashMap<u64, u32> = rustc_hash::FxHashMap::default();
    let mut next = 0usize;
    for seg in view.numbers_slices() {
        let (start, len, cols) = seg.ok()?;
        let lane = cols.first()?;
        if start != next || lane.len() != len {
            return None;
        }
        for i in 0..len {
            let row = (start + i) as u32;
            let class = lane.is_valid(i).then(|| {
                let x = lane.value(i);
                *by_bits.entry(x.to_bits()).or_insert_with(|| {
                    out.values.push(x);
                    out.rows.push(Vec::new());
                    (out.values.len() - 1) as u32
                })
            });
            out.push(row, class);
        }
        next += len;
    }
    (next == rows).then_some(out)
}

/// Distinct lowered texts of a single-column view as the text mask reads
/// them (`slice_lowered_text` per row chunk), or `None` if the chunks do
/// not tile the rows.
fn index_texts(view: &RangeView<'_>, rows: usize) -> Option<Classes<String>> {
    let mut out = Classes {
        of_row: Vec::with_capacity(rows),
        values: Vec::new(),
        rows: Vec::new(),
        null_rows: Vec::new(),
    };
    let mut by_text: rustc_hash::FxHashMap<String, u32> = rustc_hash::FxHashMap::default();
    let mut next = 0usize;
    for chunk in view.iter_row_chunks() {
        let chunk = chunk.ok()?;
        if chunk.row_start != next {
            return None;
        }
        let slices = view.slice_lowered_text(chunk.row_start, chunk.row_len);
        if slices.is_empty() {
            return None;
        }
        let lane = slices[0].as_ref();
        if lane.is_some_and(|l| l.len() != chunk.row_len) {
            return None;
        }
        for i in 0..chunk.row_len {
            let row = (chunk.row_start + i) as u32;
            let class = lane.filter(|l| l.is_valid(i)).map(|l| {
                let s = l.value(i);
                match by_text.get(s) {
                    Some(&c) => c,
                    None => {
                        out.values.push(s.to_string());
                        out.rows.push(Vec::new());
                        let c = (out.values.len() - 1) as u32;
                        by_text.insert(s.to_string(), c);
                        c
                    }
                }
            });
            out.push(row, class);
        }
        next += chunk.row_len;
    }
    (next == rows).then_some(out)
}

/// A numeric predicate's verdict per number class with Arrow's `cmp`
/// (the numeric mask's kernel); non-numbers are null, so no match.
fn number_verdicts(classes: &Classes<f64>, pred: &P, n: f64) -> Option<(Vec<bool>, bool, bool)> {
    use crate::compute_prelude::cmp;
    let values = Float64Array::from(classes.values.clone());
    let scalar = Float64Array::new_scalar(n);
    let mask = match pred {
        P::Gt(_) => cmp::gt(&values, &scalar),
        P::Ge(_) => cmp::gt_eq(&values, &scalar),
        P::Lt(_) => cmp::lt(&values, &scalar),
        P::Le(_) => cmp::lt_eq(&values, &scalar),
        P::Eq(_) => cmp::eq(&values, &scalar),
        P::Ne(_) => cmp::neq(&values, &scalar),
        _ => return None,
    }
    .ok()?;
    Some((mask_bools(&mask), false, false))
}

/// A text `=`/`<>` verdict per lowered-text class with Arrow's
/// `ilike`/`nilike` (the text mask's kernels); a non-text row is null for
/// `=` (no match) and true for `<>`.
fn text_verdicts(
    classes: &Classes<String>,
    ne: bool,
    text: &str,
) -> Option<(Vec<bool>, bool, bool)> {
    use arrow::compute::kernels::comparison::{ilike, nilike};
    let values = StringArray::from(classes.values.clone());
    let pattern = StringArray::new_scalar(text.to_lowercase());
    let mask = if ne {
        nilike(&values, &pattern)
    } else {
        ilike(&values, &pattern)
    }
    .ok()?;
    Some((mask_bools(&mask), ne, true))
}

/// Non-null true per element.
fn mask_bools(mask: &BooleanArray) -> Vec<bool> {
    (0..mask.len())
        .map(|i| mask.is_valid(i) && mask.value(i))
        .collect()
}
