use formualizer_common::{
    ExcelError, ExcelErrorExtra, ExcelErrorKind, PackedSheetCell, ResourceExhaustionDetail,
    ResourceExhaustionReason, SheetId,
};

pub const TARGET_EVAL_DELTA_VERSION: u16 = 1;

/// Controls expansion of run-aware target deltas into the legacy per-cell shape.
///
/// The compatibility default is intentionally unlimited. Callers that need a hard
/// allocation boundary must opt into `CellLimit`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EvalDeltaCompatibilityPolicy {
    #[default]
    Unlimited,
    CellLimit(usize),
}

/// Opt-in control for evaluation delta collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaMode {
    /// Do not collect deltas (default).
    #[default]
    Off,
    /// Collect changed grid cell addresses (no values).
    Cells,
}

/// Engine-level evaluation deltas for a single evaluation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvalDelta {
    pub changed_cells: Vec<PackedSheetCell>,
}

impl EvalDelta {
    pub fn is_empty(&self) -> bool {
        self.changed_cells.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalDeltaRecord {
    Run {
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    },
    Region {
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    },
}

impl EvalDeltaRecord {
    pub fn sheet_id(&self) -> SheetId {
        match self {
            Self::Run { sheet_id, .. } | Self::Region { sheet_id, .. } => *sheet_id,
        }
    }

    pub fn bounds(&self) -> (u32, u32, u32, u32) {
        match self {
            Self::Run {
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            }
            | Self::Region {
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => (*start_row, *start_col, *end_row, *end_col),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEvalDelta {
    pub version: u16,
    pub records: Vec<EvalDeltaRecord>,
}

impl Default for TargetEvalDelta {
    fn default() -> Self {
        Self {
            version: TARGET_EVAL_DELTA_VERSION,
            records: Vec::new(),
        }
    }
}

impl TargetEvalDelta {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn compatibility_cells(&self, limit: usize) -> Result<EvalDelta, ExcelError> {
        self.compatibility_cells_with_policy(EvalDeltaCompatibilityPolicy::CellLimit(limit))
    }

    pub fn compatibility_cells_with_policy(
        &self,
        policy: EvalDeltaCompatibilityPolicy,
    ) -> Result<EvalDelta, ExcelError> {
        let mut observed = 0usize;
        for record in &self.records {
            let (start_row, start_col, end_row, end_col) = record.bounds();
            let cells = (end_row.saturating_sub(start_row) as usize + 1)
                .saturating_mul(end_col.saturating_sub(start_col) as usize + 1);
            observed = observed.saturating_add(cells);
            if let EvalDeltaCompatibilityPolicy::CellLimit(limit) = policy
                && observed > limit
            {
                return Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message(format!(
                        "target delta compatibility expansion exceeded {limit} cells"
                    ))
                    .with_extra(ExcelErrorExtra::Resource {
                        detail: Box::new(ResourceExhaustionDetail {
                            reason: ResourceExhaustionReason::WorkUnits,
                            limit: limit as u64,
                            observed: observed as u64,
                            request_id: None,
                        }),
                    }));
            }
        }

        let mut changed_cells = Vec::new();
        changed_cells.try_reserve(observed).map_err(|_| {
            ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("target delta compatibility expansion allocation failed")
                .with_extra(ExcelErrorExtra::Resource {
                    detail: Box::new(ResourceExhaustionDetail {
                        reason: ResourceExhaustionReason::ScratchMemory,
                        limit: observed as u64,
                        observed: observed as u64,
                        request_id: None,
                    }),
                })
        })?;
        for record in &self.records {
            let sheet_id = record.sheet_id();
            let (start_row, start_col, end_row, end_col) = record.bounds();
            for row in start_row..=end_row {
                for col in start_col..=end_col {
                    let packed = PackedSheetCell::try_new(sheet_id, row, col).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::NImpl)
                            .with_message("target delta cell exceeds packed compatibility bounds")
                            .with_extra(ExcelErrorExtra::Resource {
                                detail: Box::new(ResourceExhaustionDetail {
                                    reason: ResourceExhaustionReason::Admission,
                                    limit: u64::from(PackedSheetCell::MAX_ROW0)
                                        .saturating_mul(u64::from(PackedSheetCell::MAX_COL0)),
                                    observed: u64::from(row).saturating_mul(u64::from(col)),
                                    request_id: None,
                                }),
                            })
                    })?;
                    changed_cells.push(packed);
                }
            }
        }
        changed_cells.sort_unstable();
        changed_cells.dedup();
        Ok(EvalDelta { changed_cells })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DeltaRect {
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
}

pub(crate) struct DeltaCollector {
    pub(crate) mode: DeltaMode,
    changed: Vec<(SheetId, DeltaRect)>,
}

impl DeltaCollector {
    pub(crate) fn new(mode: DeltaMode) -> Self {
        Self {
            mode,
            changed: Vec::new(),
        }
    }

    #[inline]
    pub(crate) fn record_cell(&mut self, sheet_id: SheetId, row0: u32, col0: u32) {
        self.record_region(sheet_id, row0, col0, row0, col0);
    }

    #[inline]
    pub(crate) fn record_packed(&mut self, packed: PackedSheetCell) {
        if self.mode == DeltaMode::Off {
            return;
        }
        self.record_cell(packed.sheet_id(), packed.row0(), packed.col0());
    }

    pub(crate) fn record_region(
        &mut self,
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) {
        if self.mode == DeltaMode::Off || start_row > end_row || start_col > end_col {
            return;
        }
        self.changed.push((
            sheet_id,
            DeltaRect {
                start_row,
                start_col,
                end_row,
                end_col,
            },
        ));
    }

    pub(crate) fn finish_target(mut self) -> TargetEvalDelta {
        if self.mode == DeltaMode::Off {
            return TargetEvalDelta::default();
        }
        // Keep recording append-only. Two sort/sweep passes normalize horizontal
        // and vertical runs at finish, avoiding an all-prior-record scan per write.
        self.changed.sort_unstable_by_key(|(sheet_id, record)| {
            (
                *sheet_id,
                record.start_row,
                record.end_row,
                record.start_col,
                record.end_col,
            )
        });
        let mut horizontal: Vec<(SheetId, DeltaRect)> = Vec::with_capacity(self.changed.len());
        for (sheet_id, record) in self.changed {
            if let Some((previous_sheet, previous)) = horizontal.last_mut()
                && *previous_sheet == sheet_id
                && previous.start_row == record.start_row
                && previous.end_row == record.end_row
                && record.start_col <= previous.end_col.saturating_add(1)
            {
                previous.end_col = previous.end_col.max(record.end_col);
            } else {
                horizontal.push((sheet_id, record));
            }
        }
        horizontal.sort_unstable_by_key(|(sheet_id, record)| {
            (
                *sheet_id,
                record.start_col,
                record.end_col,
                record.start_row,
                record.end_row,
            )
        });
        let mut normalized: Vec<(SheetId, DeltaRect)> = Vec::with_capacity(horizontal.len());
        for (sheet_id, record) in horizontal {
            if let Some((previous_sheet, previous)) = normalized.last_mut()
                && *previous_sheet == sheet_id
                && previous.start_col == record.start_col
                && previous.end_col == record.end_col
                && record.start_row <= previous.end_row.saturating_add(1)
            {
                previous.end_row = previous.end_row.max(record.end_row);
            } else {
                normalized.push((sheet_id, record));
            }
        }
        normalized.sort_unstable_by_key(|(sheet_id, record)| {
            (
                *sheet_id,
                record.start_row,
                record.start_col,
                record.end_row,
                record.end_col,
            )
        });
        let records = normalized
            .into_iter()
            .map(|(sheet_id, record)| {
                if record.start_row == record.end_row || record.start_col == record.end_col {
                    EvalDeltaRecord::Run {
                        sheet_id,
                        start_row: record.start_row,
                        start_col: record.start_col,
                        end_row: record.end_row,
                        end_col: record.end_col,
                    }
                } else {
                    EvalDeltaRecord::Region {
                        sheet_id,
                        start_row: record.start_row,
                        start_col: record.start_col,
                        end_row: record.end_row,
                        end_col: record.end_col,
                    }
                }
            })
            .collect();
        TargetEvalDelta {
            version: TARGET_EVAL_DELTA_VERSION,
            records,
        }
    }

    pub(crate) fn finish(self) -> Result<EvalDelta, ExcelError> {
        self.finish_with_policy(EvalDeltaCompatibilityPolicy::Unlimited)
    }

    pub(crate) fn finish_with_policy(
        self,
        policy: EvalDeltaCompatibilityPolicy,
    ) -> Result<EvalDelta, ExcelError> {
        self.finish_target().compatibility_cells_with_policy(policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_coalesces_horizontal_and_vertical_runs() {
        let mut collector = DeltaCollector::new(DeltaMode::Cells);
        for col in 1..=1000 {
            collector.record_cell(0, 5, col);
        }
        for row in 10..=20 {
            collector.record_cell(0, row, 8);
        }
        let delta = collector.finish_target();
        assert_eq!(delta.version, TARGET_EVAL_DELTA_VERSION);
        assert_eq!(delta.records.len(), 2);
        assert!(
            delta
                .records
                .iter()
                .all(|record| matches!(record, EvalDeltaRecord::Run { .. }))
        );
    }

    #[test]
    fn legacy_default_expands_more_than_one_hundred_thousand_cells() {
        let mut collector = DeltaCollector::new(DeltaMode::Cells);
        collector.record_region(0, 0, 0, 6, 16_383);
        let delta = collector.finish().unwrap();
        assert_eq!(delta.changed_cells.len(), 114_688);
    }

    #[test]
    fn explicit_compatibility_cap_accepts_cap_and_rejects_cap_plus_one() {
        let mut at_cap = DeltaCollector::new(DeltaMode::Cells);
        at_cap.record_region(0, 0, 0, 0, 99);
        assert_eq!(
            at_cap
                .finish_with_policy(EvalDeltaCompatibilityPolicy::CellLimit(100))
                .unwrap()
                .changed_cells
                .len(),
            100
        );

        let mut over_cap = DeltaCollector::new(DeltaMode::Cells);
        over_cap.record_region(0, 0, 0, 0, 100);
        let error = over_cap
            .finish_with_policy(EvalDeltaCompatibilityPolicy::CellLimit(100))
            .unwrap_err();
        assert!(matches!(error.extra, ExcelErrorExtra::Resource { .. }));
    }

    #[test]
    fn compatibility_expansion_returns_typed_overflow_without_truncation() {
        let delta = TargetEvalDelta {
            version: TARGET_EVAL_DELTA_VERSION,
            records: vec![EvalDeltaRecord::Region {
                sheet_id: 0,
                start_row: 0,
                start_col: 0,
                end_row: 100,
                end_col: 100,
            }],
        };
        let error = delta.compatibility_cells(100).unwrap_err();
        assert!(matches!(error.extra, ExcelErrorExtra::Resource { .. }));
    }
}
