use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use formualizer_common::RangeAddress;

use super::EvaluationBudgets;

pub type RequestId = u64;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum TargetPreparationFault {
    #[default]
    None,
    AfterDiscovery,
    FinalRevisionValidation,
    FinalGraphValidation,
    Admission,
    Reservation,
    BeforeFirstMutation,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EvaluationTarget {
    Cell {
        sheet: String,
        row: u32,
        col: u32,
    },
    Range(RangeAddress),
    Name {
        name: String,
        scope_sheet: Option<String>,
    },
    Table {
        name: String,
        selection: TableSelection,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum TableSelection {
    #[default]
    Whole,
    Headers,
    Data,
    Totals,
    Column(String),
    Columns {
        start: String,
        end: String,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OpaquePreparePolicy {
    #[default]
    Widen,
    Error,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum PrepareScope {
    #[default]
    Exact,
    Sheets(Vec<String>),
    Workbook,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OpaqueReason {
    DynamicReference,
    RuntimeTextReference,
    UnknownFunction,
    UnknownCustomFunction,
    UnresolvedCrossSheetBinding,
    UnresolvedName,
    UnresolvedTable,
    FormulaName,
    DeferredSourcePackage,
    UnsupportedSourceSemantics,
    UncertainDefaultSheetBinding,
}

#[derive(Clone, Debug)]
pub struct PrepareTargetsOptions<'a> {
    pub request_id: Option<RequestId>,
    pub cancel: Option<&'a AtomicBool>,
    pub deadline: Option<Instant>,
    pub budgets: Option<&'a EvaluationBudgets>,
    pub opaque_policy: OpaquePreparePolicy,
}

impl Default for PrepareTargetsOptions<'_> {
    fn default() -> Self {
        Self {
            request_id: None,
            cancel: None,
            deadline: None,
            budgets: None,
            opaque_policy: OpaquePreparePolicy::Widen,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PreparationRevision {
    pub graph: u64,
    /// Raw FormulaPlane epoch. Kept separate from each authority-index counter so
    /// unrelated component revisions cannot collide through arithmetic folding.
    pub authority: u64,
    pub authority_indexes: u64,
    pub authority_indexed_plane: u64,
    pub staged: u64,
    pub symbols: u64,
    pub semantic: u64,
    pub provider: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PreparationOutcome {
    #[default]
    Prepared,
    CompatibilityPrepared,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PreparedTargetGraphReport {
    pub request_id: RequestId,
    pub requested_targets: usize,
    pub normalized_regions: usize,
    pub normalized_target_list: Vec<EvaluationTarget>,
    pub selected_staged_cells: usize,
    pub selected_source_families: usize,
    pub retained_staged_cells: usize,
    pub selected_cells: Vec<RangeAddress>,
    pub retained_cells: Vec<RangeAddress>,
    pub widened_scope: PrepareScope,
    pub widening_reasons: Vec<OpaqueReason>,
    pub revisions: PreparationRevision,
    pub commit_window: Duration,
    pub estimated_scratch_bytes: u64,
    pub observed_scratch_bytes: u64,
    pub estimated_commit_work: u64,
    pub actual_commit_work: u64,
    pub outcome: PreparationOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StagedFormulaLease {
    pub(crate) row: u32,
    pub(crate) col: u32,
    pub(crate) generation: u64,
    pub(crate) insertion_order: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StagedFormulaPresence {
    generation: u64,
    insertion_order: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StagedFormulaIndex {
    revision: u64,
    next_generation: u64,
    next_insertion_order: u64,
    sheets: BTreeMap<String, BTreeMap<(u32, u32), StagedFormulaPresence>>,
    package_sheets: BTreeSet<String>,
}

impl StagedFormulaIndex {
    fn bump(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("staged formula index revision exhausted");
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn stage(&mut self, sheet: &str, row: u32, col: u32) {
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("staged formula generation exhausted");
        let entries = self.sheets.entry(sheet.to_string()).or_default();
        let insertion_order = entries.get(&(row, col)).map_or_else(
            || {
                let order = self.next_insertion_order;
                self.next_insertion_order = self
                    .next_insertion_order
                    .checked_add(1)
                    .expect("staged formula insertion order exhausted");
                order
            },
            |entry| entry.insertion_order,
        );
        entries.insert(
            (row, col),
            StagedFormulaPresence {
                generation,
                insertion_order,
            },
        );
        self.bump();
    }

    pub(crate) fn remove(&mut self, sheet: &str, row: u32, col: u32) -> bool {
        let removed = self
            .sheets
            .get_mut(sheet)
            .is_some_and(|entries| entries.remove(&(row, col)).is_some());
        if self.sheets.get(sheet).is_some_and(BTreeMap::is_empty) {
            self.sheets.remove(sheet);
        }
        if removed {
            self.bump();
        }
        removed
    }

    pub(crate) fn clear_sheet(&mut self, sheet: &str) {
        let changed = self.sheets.remove(sheet).is_some() | self.package_sheets.remove(sheet);
        if changed {
            self.bump();
        }
    }

    pub(crate) fn clear_all(&mut self) {
        if !self.sheets.is_empty() || !self.package_sheets.is_empty() {
            self.sheets.clear();
            self.package_sheets.clear();
            self.bump();
        }
    }

    pub(crate) fn set_package(&mut self, sheet: &str, present: bool) {
        let changed = if present {
            self.package_sheets.insert(sheet.to_string())
        } else {
            self.package_sheets.remove(sheet)
        };
        if changed {
            self.bump();
        }
    }

    pub(crate) fn touch_package(&mut self, sheet: &str) {
        if self.package_sheets.contains(sheet) {
            self.bump();
        }
    }

    pub(crate) fn has_packages(&self) -> bool {
        !self.package_sheets.is_empty()
    }

    pub(crate) fn package_sheets(&self) -> impl Iterator<Item = &str> {
        self.package_sheets.iter().map(String::as_str)
    }

    pub(crate) fn leases_in_region(
        &self,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Vec<StagedFormulaLease> {
        let mut leases = self
            .sheets
            .get(sheet)
            .into_iter()
            .flat_map(|entries| entries.range((start_row, 0)..=(end_row, u32::MAX)))
            .filter_map(|(&(row, col), entry)| {
                (col >= start_col && col <= end_col).then_some(StagedFormulaLease {
                    row,
                    col,
                    generation: entry.generation,
                    insertion_order: entry.insertion_order,
                })
            })
            .collect::<Vec<_>>();
        leases.sort_by_key(|lease| lease.insertion_order);
        leases
    }

    pub(crate) fn leases_for_sheet(&self, sheet: &str) -> Vec<StagedFormulaLease> {
        self.leases_in_region(sheet, 1, 1, u32::MAX, u32::MAX)
    }

    pub(crate) fn all_leases(&self) -> Vec<(String, StagedFormulaLease)> {
        let mut leases = self
            .sheets
            .iter()
            .flat_map(|(sheet, entries)| {
                entries.iter().map(move |(&(row, col), entry)| {
                    (
                        sheet.clone(),
                        StagedFormulaLease {
                            row,
                            col,
                            generation: entry.generation,
                            insertion_order: entry.insertion_order,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        leases.sort_by_key(|(_, lease)| lease.insertion_order);
        leases
    }

    pub(crate) fn lease_matches(&self, sheet: &str, lease: StagedFormulaLease) -> bool {
        self.sheets
            .get(sheet)
            .and_then(|entries| entries.get(&(lease.row, lease.col)))
            .is_some_and(|entry| {
                entry.generation == lease.generation
                    && entry.insertion_order == lease.insertion_order
            })
    }

    #[cfg(test)]
    pub(crate) fn ordinary_count(&self) -> usize {
        self.sheets.values().map(BTreeMap::len).sum()
    }
}
