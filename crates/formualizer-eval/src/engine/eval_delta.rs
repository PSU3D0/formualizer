use formualizer_common::{PackedSheetCell, SheetId};

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

pub(crate) struct DeltaCollector {
    pub(crate) mode: DeltaMode,
    changed_cells: Vec<PackedSheetCell>,
}

impl DeltaCollector {
    pub(crate) fn new(mode: DeltaMode) -> Self {
        Self {
            mode,
            changed_cells: Vec::new(),
        }
    }

    #[inline]
    pub(crate) fn record_cell(&mut self, sheet_id: SheetId, row0: u32, col0: u32) {
        if self.mode == DeltaMode::Off {
            return;
        }
        if let Some(packed) = PackedSheetCell::try_new(sheet_id, row0, col0) {
            self.changed_cells.push(packed);
        }
    }

    #[inline]
    pub(crate) fn record_packed(&mut self, packed: PackedSheetCell) {
        if self.mode == DeltaMode::Off {
            return;
        }
        self.changed_cells.push(packed);
    }

    pub(crate) fn finish(mut self) -> EvalDelta {
        if self.mode == DeltaMode::Off || self.changed_cells.is_empty() {
            return EvalDelta::default();
        }
        self.changed_cells.sort_unstable();
        self.changed_cells.dedup();
        EvalDelta {
            changed_cells: self.changed_cells,
        }
    }
}
