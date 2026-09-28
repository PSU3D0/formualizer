//! Cells a legacy graph gave a vertex without a formula (decision 27 removed
//! those vertices): referenced cells (placeholders), value cells and spill
//! children. The graph's used extent (`used_row_bounds_for_columns`,
//! `used_col_bounds_for_rows`) counted them, and the evaluation-compat extent
//! of an open range falls back to it when a column (or row) has no value and
//! no formula. They are kept here as column runs, so that extent, and every
//! result that depends on it, stays what the legacy graph gave.
//!
//! Mutation takes `&mut` (no locking); queries run under `&self` from
//! evaluation threads and fold the pending cells in under the mutex.

use crate::SheetId;
use crate::engine::graph::editor::reference_adjuster::ShiftOperation;
use std::sync::Mutex;

/// Rows `r0..=r1` of one column: `sc` is `sheet << 16 | col` (0-based).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ExtentRun {
    sc: u32,
    r0: u32,
    r1: u32,
}

impl ExtentRun {
    fn sheet(self) -> SheetId {
        (self.sc >> 16) as SheetId
    }
    fn col(self) -> u32 {
        self.sc & 0xFFFF
    }
}

fn sc(sheet: SheetId, col: u32) -> u32 {
    (u32::from(sheet) << 16) | col.min(0xFFFF)
}

#[derive(Debug, Default)]
struct Inner {
    /// Disjoint runs sorted by `(sc, r0)`.
    runs: Vec<ExtentRun>,
    /// Cells not yet folded into `runs`: `sc << 32 | row`.
    pending: Vec<u64>,
}

impl Inner {
    fn fold(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        self.pending.sort_unstable();
        self.pending.dedup();
        let pending = std::mem::take(&mut self.pending);
        let old = std::mem::take(&mut self.runs);
        let mut merged = Vec::with_capacity(old.len() + 16);
        let mut a = old.into_iter().peekable();
        let mut b = pending
            .into_iter()
            .map(|k| ExtentRun {
                sc: (k >> 32) as u32,
                r0: k as u32,
                r1: k as u32,
            })
            .peekable();
        loop {
            let next = match (a.peek(), b.peek()) {
                (Some(x), Some(y)) => {
                    if x <= y {
                        a.next()
                    } else {
                        b.next()
                    }
                }
                (Some(_), None) => a.next(),
                (None, Some(_)) => b.next(),
                (None, None) => break,
            };
            push_coalesced(&mut merged, next.unwrap());
        }
        self.runs = merged;
    }

    fn normalize(&mut self) {
        self.runs.sort_unstable();
        let runs = std::mem::take(&mut self.runs);
        let mut out = Vec::with_capacity(runs.len());
        for r in runs {
            push_coalesced(&mut out, r);
        }
        self.runs = out;
    }

    fn sheet_span(&self, sheet: SheetId) -> std::ops::Range<usize> {
        let lo = sc(sheet, 0);
        let hi = sc(sheet, 0xFFFF);
        let a = self.runs.partition_point(|r| r.sc < lo);
        let b = self.runs.partition_point(|r| r.sc <= hi);
        a..b
    }
}

fn push_coalesced(out: &mut Vec<ExtentRun>, r: ExtentRun) {
    if let Some(last) = out.last_mut()
        && last.sc == r.sc
        && r.r0 <= last.r1.saturating_add(1)
    {
        last.r1 = last.r1.max(r.r1);
        return;
    }
    out.push(r);
}

#[derive(Debug, Default)]
pub(crate) struct ExtentRecord {
    inner: Mutex<Inner>,
}

impl ExtentRecord {
    fn inner(&mut self) -> &mut Inner {
        self.inner.get_mut().unwrap_or_else(|e| e.into_inner())
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Inner> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.fold();
        g
    }

    /// Note the cell at 0-based `(row, col)`.
    pub(crate) fn note(&mut self, sheet: SheetId, row: u32, col: u32) {
        let inner = self.inner();
        inner
            .pending
            .push((u64::from(sc(sheet, col)) << 32) | u64::from(row));
        if inner.pending.len() >= 4096 && inner.pending.len() >= inner.runs.len() {
            inner.fold();
        }
    }

    /// Fold the pending cells into the runs now (end of a load: the notes a
    /// load leaves pending would otherwise be sorted in by a later edit).
    pub(crate) fn fold_pending(&mut self) {
        self.inner().fold();
    }

    /// Forget the cells of `rows x cols` (0-based, inclusive). Returns the
    /// forgotten cells as `(col, r0, r1)` runs.
    pub(crate) fn forget_rect(
        &mut self,
        sheet: SheetId,
        rows: (u32, u32),
        cols: (u32, u32),
    ) -> Vec<(u32, u32, u32)> {
        let inner = self.inner();
        inner.fold();
        let span = inner.sheet_span(sheet);
        let mut forgotten = Vec::new();
        if span.is_empty() {
            return forgotten;
        }
        let mut changed = false;
        let mut out = Vec::with_capacity(inner.runs.len() + 1);
        out.extend_from_slice(&inner.runs[..span.start]);
        for &r in &inner.runs[span.clone()] {
            let col = r.col();
            if col < cols.0 || col > cols.1 || r.r1 < rows.0 || r.r0 > rows.1 {
                out.push(r);
                continue;
            }
            changed = true;
            forgotten.push((col, r.r0.max(rows.0), r.r1.min(rows.1)));
            if r.r0 < rows.0 {
                out.push(ExtentRun {
                    r1: rows.0 - 1,
                    ..r
                });
            }
            if r.r1 > rows.1 {
                out.push(ExtentRun {
                    r0: rows.1 + 1,
                    ..r
                });
            }
        }
        if changed {
            out.extend_from_slice(&inner.runs[span.end..]);
            inner.runs = out;
        }
        forgotten
    }

    /// Shift for a structural edit (pre-edit frame). Returns the runs a
    /// delete dropped (undo brings them back with [`Self::restore`]).
    pub(crate) fn shift(&mut self, op: &ShiftOperation) -> Vec<ExtentRun> {
        let (sheet, rows, start, count, insert) = match *op {
            ShiftOperation::InsertRows {
                sheet_id,
                before,
                count,
            } => (sheet_id, true, before, count, true),
            ShiftOperation::DeleteRows {
                sheet_id,
                start,
                count,
            } => (sheet_id, true, start, count, false),
            ShiftOperation::InsertColumns {
                sheet_id,
                before,
                count,
            } => (sheet_id, false, before, count, true),
            ShiftOperation::DeleteColumns {
                sheet_id,
                start,
                count,
            } => (sheet_id, false, start, count, false),
        };
        let inner = self.inner();
        inner.fold();
        let span = inner.sheet_span(sheet);
        let mut dropped = Vec::new();
        if span.is_empty() || count == 0 {
            return dropped;
        }
        let end = start.saturating_add(count);
        let mut moved = Vec::with_capacity(span.len() + 1);
        for &r in &inner.runs[span.clone()] {
            if rows {
                if r.r1 < start {
                    moved.push(r);
                } else if insert {
                    if r.r0 >= start {
                        moved.push(ExtentRun {
                            r0: r.r0.saturating_add(count),
                            r1: r.r1.saturating_add(count),
                            ..r
                        });
                    } else {
                        moved.push(ExtentRun { r1: start - 1, ..r });
                        moved.push(ExtentRun {
                            r0: end,
                            r1: r.r1.saturating_add(count),
                            ..r
                        });
                    }
                } else {
                    if r.r0 < start {
                        moved.push(ExtentRun { r1: start - 1, ..r });
                    }
                    let (d0, d1) = (r.r0.max(start), r.r1.min(end - 1));
                    if d0 <= d1 {
                        dropped.push(ExtentRun {
                            r0: d0,
                            r1: d1,
                            ..r
                        });
                    }
                    if r.r1 >= end {
                        moved.push(ExtentRun {
                            r0: r.r0.max(end) - count,
                            r1: r.r1 - count,
                            ..r
                        });
                    }
                }
            } else {
                let col = r.col();
                if col < start {
                    moved.push(r);
                } else if insert {
                    moved.push(ExtentRun {
                        sc: sc(r.sheet(), col.saturating_add(count)),
                        ..r
                    });
                } else if col < end {
                    dropped.push(r);
                } else {
                    moved.push(ExtentRun {
                        sc: sc(r.sheet(), col - count),
                        ..r
                    });
                }
            }
        }
        inner.runs.splice(span, moved);
        inner.normalize();
        dropped
    }

    /// Runs a delete dropped come back (after the band is inserted again).
    pub(crate) fn restore(&mut self, runs: Vec<ExtentRun>) {
        if runs.is_empty() {
            return;
        }
        let inner = self.inner();
        inner.fold();
        inner.runs.extend(runs);
        inner.normalize();
    }

    /// A sheet is removed.
    pub(crate) fn drop_sheet(&mut self, sheet: SheetId) {
        let inner = self.inner();
        inner.fold();
        let span = inner.sheet_span(sheet);
        inner.runs.drain(span);
    }

    /// Min/max 0-based row noted in columns `c0..=c1`.
    pub(crate) fn row_bounds_for_cols(
        &self,
        sheet: SheetId,
        c0: u32,
        c1: u32,
    ) -> Option<(u32, u32)> {
        let g = self.locked();
        if g.runs.is_empty() || c0 > c1 {
            return None;
        }
        let lo = sc(sheet, c0);
        let hi = sc(sheet, c1);
        let a = g.runs.partition_point(|r| r.sc < lo);
        let mut out: Option<(u32, u32)> = None;
        for r in g.runs[a..].iter().take_while(|r| r.sc <= hi) {
            out = Some(out.map_or((r.r0, r.r1), |(m, x)| (m.min(r.r0), x.max(r.r1))));
        }
        out
    }

    /// Min/max 0-based column noted in rows `r0..=r1`.
    pub(crate) fn col_bounds_for_rows(
        &self,
        sheet: SheetId,
        r0: u32,
        r1: u32,
    ) -> Option<(u32, u32)> {
        let g = self.locked();
        if g.runs.is_empty() {
            return None;
        }
        let span = g.sheet_span(sheet);
        let mut out: Option<(u32, u32)> = None;
        for r in &g.runs[span] {
            if r.r1 >= r0 && r.r0 <= r1 {
                let c = r.col();
                out = Some(out.map_or((c, c), |(m, x)| (m.min(c), x.max(c))));
            }
        }
        out
    }

    /// Whether the cell at 0-based `(row, col)` is noted.
    pub(crate) fn contains(&self, sheet: SheetId, row: u32, col: u32) -> bool {
        let g = self.locked();
        let key = sc(sheet, col);
        let i = g.runs.partition_point(|r| (r.sc, r.r0) <= (key, row));
        i > 0 && {
            let r = g.runs[i - 1];
            r.sc == key && r.r0 <= row && row <= r.r1
        }
    }

    /// Columns of `sheet` with a noted cell, ascending.
    pub(crate) fn columns(&self, sheet: SheetId) -> Vec<u32> {
        let g = self.locked();
        let mut out: Vec<u32> = g.runs[g.sheet_span(sheet)]
            .iter()
            .map(|r| r.col())
            .collect();
        out.dedup();
        out
    }

    /// Cells noted but not folded yet (tests).
    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending
            .len()
    }

    /// Runs held (after folding pending cells): tests, accounting.
    pub(crate) fn run_count(&self) -> usize {
        self.locked().runs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(rec: &ExtentRecord, sheet: SheetId) -> Vec<(u32, u32)> {
        let g = rec.locked();
        let mut out = Vec::new();
        for r in &g.runs[g.sheet_span(sheet)] {
            for row in r.r0..=r.r1 {
                out.push((row, r.col()));
            }
        }
        out.sort_unstable();
        out
    }

    #[test]
    fn notes_coalesce_into_runs_and_answer_bounds() {
        let mut rec = ExtentRecord::default();
        for row in (10..20).rev() {
            rec.note(1, row, 3);
        }
        rec.note(1, 15, 3);
        rec.note(1, 40, 3);
        rec.note(1, 5, 7);
        rec.note(2, 99, 3);
        assert_eq!(rec.run_count(), 4);
        assert_eq!(rec.row_bounds_for_cols(1, 3, 3), Some((10, 40)));
        assert_eq!(rec.row_bounds_for_cols(1, 0, 10), Some((5, 40)));
        assert_eq!(rec.row_bounds_for_cols(1, 4, 6), None);
        assert_eq!(rec.col_bounds_for_rows(1, 0, 9), Some((7, 7)));
        assert_eq!(rec.col_bounds_for_rows(1, 12, 12), Some((3, 3)));
        assert_eq!(rec.col_bounds_for_rows(1, 21, 39), None);
        assert_eq!(rec.col_bounds_for_rows(2, 0, 1000), Some((3, 3)));
        assert!(rec.contains(1, 10, 3) && rec.contains(1, 19, 3) && rec.contains(1, 40, 3));
        assert!(!rec.contains(1, 9, 3) && !rec.contains(1, 20, 3) && !rec.contains(1, 10, 4));
        assert!(!rec.contains(3, 10, 3));
    }

    #[test]
    fn row_delete_drops_the_band_and_undo_restores_it() {
        let mut rec = ExtentRecord::default();
        for row in 0..10 {
            rec.note(1, row, 2);
        }
        rec.note(1, 4, 5);
        let before = cells(&rec, 1);
        let op = ShiftOperation::DeleteRows {
            sheet_id: 1,
            start: 3,
            count: 4,
        };
        let dropped = rec.shift(&op);
        assert_eq!(
            cells(&rec, 1),
            vec![(0, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 2)]
        );
        rec.shift(&ShiftOperation::InsertRows {
            sheet_id: 1,
            before: 3,
            count: 4,
        });
        rec.restore(dropped);
        assert_eq!(cells(&rec, 1), before);
    }

    #[test]
    fn row_insert_splits_a_run_and_column_edits_move_whole_columns() {
        let mut rec = ExtentRecord::default();
        for row in 0..6 {
            rec.note(3, row, 1);
        }
        rec.note(3, 2, 4);
        rec.shift(&ShiftOperation::InsertRows {
            sheet_id: 3,
            before: 3,
            count: 2,
        });
        assert_eq!(
            cells(&rec, 3),
            vec![(0, 1), (1, 1), (2, 1), (2, 4), (5, 1), (6, 1), (7, 1)]
        );
        let dropped = rec.shift(&ShiftOperation::DeleteColumns {
            sheet_id: 3,
            start: 1,
            count: 2,
        });
        assert_eq!(dropped.len(), 2);
        assert_eq!(cells(&rec, 3), vec![(2, 2)]);
        rec.shift(&ShiftOperation::InsertColumns {
            sheet_id: 3,
            before: 0,
            count: 1,
        });
        assert_eq!(cells(&rec, 3), vec![(2, 3)]);
    }

    #[test]
    fn forget_rect_splits_runs_and_drop_sheet_clears_one_sheet() {
        let mut rec = ExtentRecord::default();
        for row in 0..10 {
            rec.note(1, row, 1);
            rec.note(2, row, 1);
        }
        assert_eq!(rec.forget_rect(1, (3, 4), (0, 5)), vec![(1, 3, 4)]);
        assert_eq!(rec.row_bounds_for_cols(1, 1, 1), Some((0, 9)));
        assert_eq!(rec.col_bounds_for_rows(1, 3, 4), None);
        rec.forget_rect(1, (0, 9), (1, 1));
        assert_eq!(rec.row_bounds_for_cols(1, 1, 1), None);
        rec.drop_sheet(2);
        assert_eq!(rec.run_count(), 0);
    }
}
