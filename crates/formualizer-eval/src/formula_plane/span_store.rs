//! Passive FormulaPlane run store builder.
//!
//! The store is descriptive infrastructure for the FormulaPlane bridge. It does
//! not route evaluation, dirty propagation, scheduling, dependency graph writes,
//! or loader behavior.

use std::collections::{BTreeMap, BTreeSet};

use super::ids::{FormulaRunId, FormulaTemplateId};
use super::span_counters::{
    DEFAULT_CANDIDATE_ROW_BLOCK_SIZE, FormulaPlaneCandidateCell, SpanPartitionCounterOptions,
    compute_span_partition_counters,
};

pub const DEFAULT_GAP_SCAN_MAX_PER_AXIS_GROUP: u64 = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaTemplateDescriptor {
    pub id: FormulaTemplateId,
    pub source_template_id: String,
    pub formula_cell_count: u64,
    pub status: TemplateSupportStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TemplateSupportStatus {
    Supported,
    ParseError,
    Unsupported,
    Dynamic,
    Volatile,
    Mixed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FormulaRunShape {
    Row,
    Column,
    Singleton,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRunDescriptor {
    pub id: FormulaRunId,
    pub template_id: FormulaTemplateId,
    pub source_template_id: String,
    pub sheet: String,
    pub shape: FormulaRunShape,
    pub row_start: u32,
    pub col_start: u32,
    pub row_end: u32,
    pub col_end: u32,
    pub len: u64,
    pub row_block_start: u32,
    pub row_block_end: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpanGapDescriptor {
    pub template_id: FormulaTemplateId,
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub kind: SpanGapKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SpanGapKind {
    Hole,
    Exception {
        other_template_id: FormulaTemplateId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRejectedCell {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub source_template_id: String,
    pub reason: FormulaRejectReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FormulaRejectReason {
    ParseError,
    Unsupported,
    Dynamic,
    Volatile,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaTemplateArena {
    pub templates: Vec<FormulaTemplateDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRunStoreBuildReport {
    pub template_count: u64,
    pub formula_cell_count: u64,
    pub supported_formula_cell_count: u64,
    pub rejected_formula_cell_count: u64,
    pub parse_error_formula_count: u64,
    pub unsupported_formula_count: u64,
    pub dynamic_formula_count: u64,
    pub volatile_formula_count: u64,
    pub row_run_count: u64,
    pub column_run_count: u64,
    pub singleton_run_count: u64,
    pub formula_cells_represented_by_runs: u64,
    pub candidate_row_block_partition_count: u64,
    pub candidate_formula_run_to_partition_edge_estimate: u64,
    pub max_partitions_touched_by_run: u64,
    pub hole_count: u64,
    pub exception_count: u64,
    pub overlap_dropped_count: u64,
    pub rectangle_deferred_count: u64,
    pub gap_scan_truncated_count: u64,
    pub reconciliation: Fp2aReconciliation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fp2aReconciliation {
    pub matched: bool,
    pub deltas: Vec<Fp2aCounterDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fp2aCounterDelta {
    pub field: &'static str,
    pub fp2a_value: i64,
    pub span_store_value: i64,
    pub reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRunStore {
    pub row_block_size: u32,
    pub arena: FormulaTemplateArena,
    pub runs: Vec<FormulaRunDescriptor>,
    pub gaps: Vec<SpanGapDescriptor>,
    pub rejected_cells: Vec<FormulaRejectedCell>,
    pub report: FormulaRunStoreBuildReport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormulaRunStoreBuildOptions {
    pub row_block_size: u32,
    pub gap_scan_max_per_axis_group: u64,
}

impl Default for FormulaRunStoreBuildOptions {
    fn default() -> Self {
        Self {
            row_block_size: DEFAULT_CANDIDATE_ROW_BLOCK_SIZE,
            gap_scan_max_per_axis_group: DEFAULT_GAP_SCAN_MAX_PER_AXIS_GROUP,
        }
    }
}

impl FormulaRunStoreBuildOptions {
    fn normalized(self) -> Self {
        Self {
            row_block_size: self.row_block_size.max(1),
            gap_scan_max_per_axis_group: self.gap_scan_max_per_axis_group,
        }
    }
}

impl FormulaRunStore {
    pub fn build(cells: &[FormulaPlaneCandidateCell]) -> Self {
        build_formula_run_store(cells, FormulaRunStoreBuildOptions::default())
    }

    pub fn build_with_options(
        cells: &[FormulaPlaneCandidateCell],
        options: FormulaRunStoreBuildOptions,
    ) -> Self {
        build_formula_run_store(cells, options)
    }
}

pub fn build_formula_run_store(
    cells: &[FormulaPlaneCandidateCell],
    options: FormulaRunStoreBuildOptions,
) -> FormulaRunStore {
    let options = options.normalized();
    let template_id_by_source = assign_template_ids(cells);
    let arena = build_arena(cells, &template_id_by_source);
    let classified = classify_cells(cells, &template_id_by_source);

    let supported_by_template = group_supported_cells(&classified.supported_cells);
    let all_cell_templates = build_cell_template_index(&classified.all_cells);

    let candidate_runs = build_candidate_runs(&supported_by_template, options.row_block_size);
    let rectangle_deferred_count = count_deferred_rectangles(&supported_by_template);
    let (accepted_runs, overlap_dropped_count, represented_cells) =
        select_non_overlapping_runs(candidate_runs, options.row_block_size);
    let mut runs = materialize_runs(
        accepted_runs,
        &template_id_by_source,
        options.row_block_size,
    );

    add_singleton_runs(
        &mut runs,
        &classified.supported_cells,
        &represented_cells,
        options.row_block_size,
    );
    assign_run_ids(&mut runs);

    let (gaps, gap_scan_truncated_count) = scan_gaps(
        &supported_by_template,
        &all_cell_templates,
        options.gap_scan_max_per_axis_group,
    );

    let mut rejected_cells = classified.rejected_cells;
    rejected_cells.sort_by(|a, b| {
        (&a.sheet, a.row, a.col, &a.source_template_id, a.reason).cmp(&(
            &b.sheet,
            b.row,
            b.col,
            &b.source_template_id,
            b.reason,
        ))
    });

    let report = build_report(
        cells,
        options,
        &template_id_by_source,
        &runs,
        &gaps,
        rejected_cells.len() as u64,
        overlap_dropped_count,
        rectangle_deferred_count,
        gap_scan_truncated_count,
    );

    FormulaRunStore {
        row_block_size: options.row_block_size,
        arena,
        runs,
        gaps,
        rejected_cells,
        report,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CellKey {
    sheet: String,
    row: u32,
    col: u32,
}

#[derive(Clone, Debug)]
struct ClassifiedCell {
    source_template_id: String,
    template_id: FormulaTemplateId,
    key: CellKey,
}

#[derive(Clone, Debug)]
struct ClassifiedCells {
    supported_cells: Vec<ClassifiedCell>,
    rejected_cells: Vec<FormulaRejectedCell>,
    all_cells: Vec<ClassifiedCell>,
}

#[derive(Clone, Debug)]
struct PendingRun {
    template_id: FormulaTemplateId,
    source_template_id: String,
    sheet: String,
    shape: FormulaRunShape,
    row_start: u32,
    col_start: u32,
    row_end: u32,
    col_end: u32,
    len: u64,
}

impl PendingRun {
    fn cell_keys(&self) -> Vec<CellKey> {
        match self.shape {
            FormulaRunShape::Row => (self.col_start..=self.col_end)
                .map(|col| CellKey {
                    sheet: self.sheet.clone(),
                    row: self.row_start,
                    col,
                })
                .collect(),
            FormulaRunShape::Column => (self.row_start..=self.row_end)
                .map(|row| CellKey {
                    sheet: self.sheet.clone(),
                    row,
                    col: self.col_start,
                })
                .collect(),
            FormulaRunShape::Singleton => vec![CellKey {
                sheet: self.sheet.clone(),
                row: self.row_start,
                col: self.col_start,
            }],
        }
    }
}

fn assign_template_ids(cells: &[FormulaPlaneCandidateCell]) -> BTreeMap<String, FormulaTemplateId> {
    let sources = cells
        .iter()
        .map(|cell| cell.template_id.clone())
        .collect::<BTreeSet<_>>();
    sources
        .into_iter()
        .enumerate()
        .map(|(index, source)| (source, FormulaTemplateId(index as u32)))
        .collect()
}

fn build_arena(
    cells: &[FormulaPlaneCandidateCell],
    template_id_by_source: &BTreeMap<String, FormulaTemplateId>,
) -> FormulaTemplateArena {
    let mut stats = template_id_by_source
        .keys()
        .map(|source| (source.clone(), TemplateStats::default()))
        .collect::<BTreeMap<_, _>>();

    for cell in cells {
        let stat = stats.entry(cell.template_id.clone()).or_default();
        stat.formula_cell_count += 1;
        match reject_reason(cell) {
            Some(reason) => {
                stat.rejected_reasons.insert(reason);
            }
            None => stat.supported_count += 1,
        }
    }

    let templates = stats
        .into_iter()
        .map(|(source_template_id, stat)| FormulaTemplateDescriptor {
            id: template_id_by_source[&source_template_id],
            source_template_id,
            formula_cell_count: stat.formula_cell_count,
            status: stat.status(),
        })
        .collect();

    FormulaTemplateArena { templates }
}

#[derive(Clone, Debug, Default)]
struct TemplateStats {
    formula_cell_count: u64,
    supported_count: u64,
    rejected_reasons: BTreeSet<FormulaRejectReason>,
}

impl TemplateStats {
    fn status(&self) -> TemplateSupportStatus {
        if self.supported_count > 0 && !self.rejected_reasons.is_empty() {
            return TemplateSupportStatus::Mixed;
        }
        if self.supported_count > 0 {
            return TemplateSupportStatus::Supported;
        }
        match self.rejected_reasons.first().copied() {
            Some(FormulaRejectReason::ParseError) => TemplateSupportStatus::ParseError,
            Some(FormulaRejectReason::Unsupported) => TemplateSupportStatus::Unsupported,
            Some(FormulaRejectReason::Dynamic) => TemplateSupportStatus::Dynamic,
            Some(FormulaRejectReason::Volatile) => TemplateSupportStatus::Volatile,
            None => TemplateSupportStatus::Supported,
        }
    }
}

fn classify_cells(
    cells: &[FormulaPlaneCandidateCell],
    template_id_by_source: &BTreeMap<String, FormulaTemplateId>,
) -> ClassifiedCells {
    let mut supported_cells = Vec::new();
    let mut rejected_cells = Vec::new();
    let mut all_cells = Vec::new();

    for cell in cells {
        let template_id = template_id_by_source[&cell.template_id];
        let key = CellKey {
            sheet: cell.sheet.clone(),
            row: cell.row,
            col: cell.col,
        };
        all_cells.push(ClassifiedCell {
            source_template_id: cell.template_id.clone(),
            template_id,
            key: key.clone(),
        });
        if let Some(reason) = reject_reason(cell) {
            rejected_cells.push(FormulaRejectedCell {
                sheet: cell.sheet.clone(),
                row: cell.row,
                col: cell.col,
                source_template_id: cell.template_id.clone(),
                reason,
            });
        } else {
            supported_cells.push(ClassifiedCell {
                source_template_id: cell.template_id.clone(),
                template_id,
                key,
            });
        }
    }

    supported_cells.sort_by(|a, b| {
        (
            a.template_id,
            &a.source_template_id,
            &a.key.sheet,
            a.key.row,
            a.key.col,
        )
            .cmp(&(
                b.template_id,
                &b.source_template_id,
                &b.key.sheet,
                b.key.row,
                b.key.col,
            ))
    });
    all_cells.sort_by(|a, b| {
        (&a.key.sheet, a.key.row, a.key.col, a.template_id).cmp(&(
            &b.key.sheet,
            b.key.row,
            b.key.col,
            b.template_id,
        ))
    });

    ClassifiedCells {
        supported_cells,
        rejected_cells,
        all_cells,
    }
}

fn reject_reason(cell: &FormulaPlaneCandidateCell) -> Option<FormulaRejectReason> {
    if !cell.parse_ok {
        Some(FormulaRejectReason::ParseError)
    } else if cell.unsupported {
        Some(FormulaRejectReason::Unsupported)
    } else if cell.dynamic {
        Some(FormulaRejectReason::Dynamic)
    } else if cell.volatile {
        Some(FormulaRejectReason::Volatile)
    } else {
        None
    }
}

fn group_supported_cells(
    cells: &[ClassifiedCell],
) -> BTreeMap<FormulaTemplateId, Vec<ClassifiedCell>> {
    let mut by_template = BTreeMap::<FormulaTemplateId, Vec<ClassifiedCell>>::new();
    for cell in cells {
        by_template
            .entry(cell.template_id)
            .or_default()
            .push(cell.clone());
    }
    by_template
}

fn build_cell_template_index(cells: &[ClassifiedCell]) -> BTreeMap<CellKey, FormulaTemplateId> {
    let mut out = BTreeMap::new();
    for cell in cells {
        out.entry(cell.key.clone()).or_insert(cell.template_id);
    }
    out
}

fn build_candidate_runs(
    supported_by_template: &BTreeMap<FormulaTemplateId, Vec<ClassifiedCell>>,
    _row_block_size: u32,
) -> Vec<PendingRun> {
    let mut runs = Vec::new();
    for (template_id, cells) in supported_by_template {
        build_axis_runs(&mut runs, *template_id, cells, FormulaRunShape::Row);
        build_axis_runs(&mut runs, *template_id, cells, FormulaRunShape::Column);
    }
    runs.sort_by(compare_run_key);
    runs
}

fn build_axis_runs(
    out: &mut Vec<PendingRun>,
    template_id: FormulaTemplateId,
    cells: &[ClassifiedCell],
    shape: FormulaRunShape,
) {
    let mut groups: BTreeMap<(String, String, u32), Vec<u32>> = BTreeMap::new();
    for cell in cells {
        let fixed = match shape {
            FormulaRunShape::Row => cell.key.row,
            FormulaRunShape::Column => cell.key.col,
            FormulaRunShape::Singleton => unreachable!(),
        };
        let value = match shape {
            FormulaRunShape::Row => cell.key.col,
            FormulaRunShape::Column => cell.key.row,
            FormulaRunShape::Singleton => unreachable!(),
        };
        groups
            .entry((
                cell.source_template_id.clone(),
                cell.key.sheet.clone(),
                fixed,
            ))
            .or_default()
            .push(value);
    }

    for ((source_template_id, sheet, fixed), mut values) in groups {
        values.sort_unstable();
        values.dedup();
        let mut start = None::<u32>;
        let mut prev = None::<u32>;
        for value in values {
            match (start, prev) {
                (None, _) => {
                    start = Some(value);
                    prev = Some(value);
                }
                (Some(_), Some(previous)) if value == previous + 1 => prev = Some(value),
                (Some(run_start), Some(run_end)) => {
                    push_axis_run(
                        out,
                        template_id,
                        &source_template_id,
                        &sheet,
                        shape,
                        fixed,
                        run_start,
                        run_end,
                    );
                    start = Some(value);
                    prev = Some(value);
                }
                (Some(_), None) => unreachable!(),
            }
        }
        if let (Some(run_start), Some(run_end)) = (start, prev) {
            push_axis_run(
                out,
                template_id,
                &source_template_id,
                &sheet,
                shape,
                fixed,
                run_start,
                run_end,
            );
        }
    }
}

fn push_axis_run(
    out: &mut Vec<PendingRun>,
    template_id: FormulaTemplateId,
    source_template_id: &str,
    sheet: &str,
    shape: FormulaRunShape,
    fixed: u32,
    start: u32,
    end: u32,
) {
    if end <= start {
        return;
    }
    let (row_start, col_start, row_end, col_end) = match shape {
        FormulaRunShape::Row => (fixed, start, fixed, end),
        FormulaRunShape::Column => (start, fixed, end, fixed),
        FormulaRunShape::Singleton => unreachable!(),
    };
    out.push(PendingRun {
        template_id,
        source_template_id: source_template_id.to_string(),
        sheet: sheet.to_string(),
        shape,
        row_start,
        col_start,
        row_end,
        col_end,
        len: u64::from(end - start + 1),
    });
}

fn select_non_overlapping_runs(
    mut candidate_runs: Vec<PendingRun>,
    _row_block_size: u32,
) -> (Vec<PendingRun>, u64, BTreeSet<CellKey>) {
    candidate_runs.sort_by(|a, b| {
        b.len
            .cmp(&a.len)
            .then_with(|| shape_order(a.shape).cmp(&shape_order(b.shape)))
            .then_with(|| compare_run_key(a, b))
    });

    let mut accepted = Vec::new();
    let mut represented = BTreeSet::new();
    let mut overlap_dropped_count = 0;

    for run in candidate_runs {
        let cells = run.cell_keys();
        if cells.iter().any(|cell| represented.contains(cell)) {
            overlap_dropped_count += 1;
            continue;
        }
        for cell in cells {
            represented.insert(cell);
        }
        accepted.push(run);
    }

    (accepted, overlap_dropped_count, represented)
}

fn materialize_runs(
    pending_runs: Vec<PendingRun>,
    _template_id_by_source: &BTreeMap<String, FormulaTemplateId>,
    row_block_size: u32,
) -> Vec<FormulaRunDescriptor> {
    pending_runs
        .into_iter()
        .map(|run| descriptor_from_pending(run, row_block_size))
        .collect()
}

fn add_singleton_runs(
    runs: &mut Vec<FormulaRunDescriptor>,
    supported_cells: &[ClassifiedCell],
    represented_cells: &BTreeSet<CellKey>,
    row_block_size: u32,
) {
    let mut seen = BTreeSet::new();
    for cell in supported_cells {
        if represented_cells.contains(&cell.key) || !seen.insert(cell.key.clone()) {
            continue;
        }
        let block = row_block_index(cell.key.row, row_block_size);
        runs.push(FormulaRunDescriptor {
            id: FormulaRunId(0),
            template_id: cell.template_id,
            source_template_id: cell.source_template_id.clone(),
            sheet: cell.key.sheet.clone(),
            shape: FormulaRunShape::Singleton,
            row_start: cell.key.row,
            col_start: cell.key.col,
            row_end: cell.key.row,
            col_end: cell.key.col,
            len: 1,
            row_block_start: block,
            row_block_end: block,
        });
    }
}

fn descriptor_from_pending(run: PendingRun, row_block_size: u32) -> FormulaRunDescriptor {
    let row_block_start = row_block_index(run.row_start, row_block_size);
    let row_block_end = row_block_index(run.row_end, row_block_size);
    FormulaRunDescriptor {
        id: FormulaRunId(0),
        template_id: run.template_id,
        source_template_id: run.source_template_id,
        sheet: run.sheet,
        shape: run.shape,
        row_start: run.row_start,
        col_start: run.col_start,
        row_end: run.row_end,
        col_end: run.col_end,
        len: run.len,
        row_block_start,
        row_block_end,
    }
}

fn assign_run_ids(runs: &mut [FormulaRunDescriptor]) {
    runs.sort_by(|a, b| {
        (
            a.template_id,
            &a.sheet,
            shape_order(a.shape),
            a.row_start,
            a.col_start,
            a.row_end,
            a.col_end,
        )
            .cmp(&(
                b.template_id,
                &b.sheet,
                shape_order(b.shape),
                b.row_start,
                b.col_start,
                b.row_end,
                b.col_end,
            ))
    });
    for (index, run) in runs.iter_mut().enumerate() {
        run.id = FormulaRunId(index as u32);
    }
}

fn scan_gaps(
    supported_by_template: &BTreeMap<FormulaTemplateId, Vec<ClassifiedCell>>,
    all_cell_templates: &BTreeMap<CellKey, FormulaTemplateId>,
    gap_scan_max_per_axis_group: u64,
) -> (Vec<SpanGapDescriptor>, u64) {
    let mut gaps = BTreeSet::new();
    let mut truncated = 0;
    for (template_id, cells) in supported_by_template {
        truncated += scan_axis_gaps(
            &mut gaps,
            *template_id,
            cells,
            all_cell_templates,
            FormulaRunShape::Row,
            gap_scan_max_per_axis_group,
        );
        truncated += scan_axis_gaps(
            &mut gaps,
            *template_id,
            cells,
            all_cell_templates,
            FormulaRunShape::Column,
            gap_scan_max_per_axis_group,
        );
    }
    (gaps.into_iter().collect(), truncated)
}

fn scan_axis_gaps(
    gaps: &mut BTreeSet<SpanGapDescriptor>,
    template_id: FormulaTemplateId,
    cells: &[ClassifiedCell],
    all_cell_templates: &BTreeMap<CellKey, FormulaTemplateId>,
    shape: FormulaRunShape,
    gap_scan_max_per_axis_group: u64,
) -> u64 {
    let mut groups: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    for cell in cells {
        let fixed = match shape {
            FormulaRunShape::Row => cell.key.row,
            FormulaRunShape::Column => cell.key.col,
            FormulaRunShape::Singleton => unreachable!(),
        };
        let value = match shape {
            FormulaRunShape::Row => cell.key.col,
            FormulaRunShape::Column => cell.key.row,
            FormulaRunShape::Singleton => unreachable!(),
        };
        groups
            .entry((cell.key.sheet.clone(), fixed))
            .or_default()
            .push(value);
    }

    let mut truncated = 0;
    for ((sheet, fixed), mut values) in groups {
        values.sort_unstable();
        values.dedup();
        let Some(min) = values.first().copied() else {
            continue;
        };
        let Some(max) = values.last().copied() else {
            continue;
        };
        let width = u64::from(max - min + 1);
        if width > gap_scan_max_per_axis_group {
            truncated += 1;
            continue;
        }
        let present = values.into_iter().collect::<BTreeSet<_>>();
        for value in min..=max {
            if present.contains(&value) {
                continue;
            }
            let key = match shape {
                FormulaRunShape::Row => CellKey {
                    sheet: sheet.clone(),
                    row: fixed,
                    col: value,
                },
                FormulaRunShape::Column => CellKey {
                    sheet: sheet.clone(),
                    row: value,
                    col: fixed,
                },
                FormulaRunShape::Singleton => unreachable!(),
            };
            match all_cell_templates.get(&key) {
                Some(other_template_id) if *other_template_id != template_id => {
                    gaps.insert(SpanGapDescriptor {
                        template_id,
                        sheet: key.sheet,
                        row: key.row,
                        col: key.col,
                        kind: SpanGapKind::Exception {
                            other_template_id: *other_template_id,
                        },
                    });
                }
                Some(_) => {}
                None => {
                    gaps.insert(SpanGapDescriptor {
                        template_id,
                        sheet: key.sheet,
                        row: key.row,
                        col: key.col,
                        kind: SpanGapKind::Hole,
                    });
                }
            }
        }
    }
    truncated
}

fn count_deferred_rectangles(
    supported_by_template: &BTreeMap<FormulaTemplateId, Vec<ClassifiedCell>>,
) -> u64 {
    let mut count = 0;
    for cells in supported_by_template.values() {
        let mut by_sheet_row: BTreeMap<(String, u32), BTreeSet<u32>> = BTreeMap::new();
        for cell in cells {
            by_sheet_row
                .entry((cell.key.sheet.clone(), cell.key.row))
                .or_default()
                .insert(cell.key.col);
        }
        let mut rows_by_sheet: BTreeMap<String, Vec<BTreeSet<u32>>> = BTreeMap::new();
        for ((sheet, _row), cols) in by_sheet_row {
            if cols.len() >= 2 {
                rows_by_sheet.entry(sheet).or_default().push(cols);
            }
        }
        for rows in rows_by_sheet.values() {
            let mut found = false;
            for left_index in 0..rows.len() {
                for right in rows.iter().skip(left_index + 1) {
                    if rows[left_index].intersection(right).take(2).count() >= 2 {
                        count += 1;
                        found = true;
                        break;
                    }
                }
                if found {
                    break;
                }
            }
        }
    }
    count
}

fn build_report(
    cells: &[FormulaPlaneCandidateCell],
    options: FormulaRunStoreBuildOptions,
    template_id_by_source: &BTreeMap<String, FormulaTemplateId>,
    runs: &[FormulaRunDescriptor],
    gaps: &[SpanGapDescriptor],
    rejected_formula_cell_count: u64,
    overlap_dropped_count: u64,
    rectangle_deferred_count: u64,
    gap_scan_truncated_count: u64,
) -> FormulaRunStoreBuildReport {
    let row_run_count = runs
        .iter()
        .filter(|run| run.shape == FormulaRunShape::Row)
        .count() as u64;
    let column_run_count = runs
        .iter()
        .filter(|run| run.shape == FormulaRunShape::Column)
        .count() as u64;
    let singleton_run_count = runs
        .iter()
        .filter(|run| run.shape == FormulaRunShape::Singleton)
        .count() as u64;
    let formula_cells_represented_by_runs = runs.iter().map(|run| run.len).sum();
    let mut partitions = BTreeSet::new();
    let mut edge_estimate = 0;
    let mut max_partitions_touched = 0;
    for run in runs {
        let touched = u64::from(run.row_block_end - run.row_block_start + 1);
        edge_estimate += touched;
        max_partitions_touched = max_partitions_touched.max(touched);
        for block in run.row_block_start..=run.row_block_end {
            partitions.insert((run.sheet.clone(), block));
        }
    }
    let hole_count = gaps
        .iter()
        .filter(|gap| gap.kind == SpanGapKind::Hole)
        .count() as u64;
    let exception_count = gaps
        .iter()
        .filter(|gap| matches!(gap.kind, SpanGapKind::Exception { .. }))
        .count() as u64;
    let parse_error_formula_count = cells.iter().filter(|cell| !cell.parse_ok).count() as u64;
    let unsupported_formula_count = cells.iter().filter(|cell| cell.unsupported).count() as u64;
    let dynamic_formula_count = cells.iter().filter(|cell| cell.dynamic).count() as u64;
    let volatile_formula_count = cells.iter().filter(|cell| cell.volatile).count() as u64;

    let mut report = FormulaRunStoreBuildReport {
        template_count: template_id_by_source.len() as u64,
        formula_cell_count: cells.len() as u64,
        supported_formula_cell_count: cells.len() as u64 - rejected_formula_cell_count,
        rejected_formula_cell_count,
        parse_error_formula_count,
        unsupported_formula_count,
        dynamic_formula_count,
        volatile_formula_count,
        row_run_count,
        column_run_count,
        singleton_run_count,
        formula_cells_represented_by_runs,
        candidate_row_block_partition_count: partitions.len() as u64,
        candidate_formula_run_to_partition_edge_estimate: edge_estimate,
        max_partitions_touched_by_run: max_partitions_touched,
        hole_count,
        exception_count,
        overlap_dropped_count,
        rectangle_deferred_count,
        gap_scan_truncated_count,
        reconciliation: Fp2aReconciliation {
            matched: true,
            deltas: Vec::new(),
        },
    };
    report.reconciliation = reconcile_fp2a(cells, options, &report);
    report
}

fn reconcile_fp2a(
    cells: &[FormulaPlaneCandidateCell],
    options: FormulaRunStoreBuildOptions,
    report: &FormulaRunStoreBuildReport,
) -> Fp2aReconciliation {
    let fp2a = compute_span_partition_counters(
        cells,
        SpanPartitionCounterOptions {
            row_block_size: options.row_block_size,
        },
    );
    let mut deltas = Vec::new();
    push_delta(
        &mut deltas,
        "template_count",
        fp2a.template_count,
        report.template_count,
        "unexpected_delta",
    );
    push_delta(
        &mut deltas,
        "formula_cell_count",
        fp2a.formula_cell_count,
        report.formula_cell_count,
        "unexpected_delta",
    );
    push_delta(
        &mut deltas,
        "parse_error_formula_count",
        fp2a.parse_error_formula_count,
        report.parse_error_formula_count,
        "unexpected_delta",
    );
    push_delta(
        &mut deltas,
        "unsupported_formula_count",
        fp2a.unsupported_formula_count,
        report.unsupported_formula_count,
        "unexpected_delta",
    );
    push_delta(
        &mut deltas,
        "dynamic_formula_count",
        fp2a.dynamic_formula_count,
        report.dynamic_formula_count,
        "unexpected_delta",
    );
    push_delta(
        &mut deltas,
        "volatile_formula_count",
        fp2a.volatile_formula_count,
        report.volatile_formula_count,
        "unexpected_delta",
    );
    let run_reason = if report.overlap_dropped_count > 0 {
        "fp2b_overlap_deduplicates_cells"
    } else if report.rejected_formula_cell_count > 0 {
        "fp2b_excludes_rejected_cells_from_runs"
    } else if report.singleton_run_count > 0 {
        "fp2b_stores_supported_singletons_as_runs"
    } else {
        "unexpected_delta"
    };
    push_delta(
        &mut deltas,
        "row_run_count",
        fp2a.row_run_count,
        report.row_run_count,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "column_run_count",
        fp2a.column_run_count,
        report.column_run_count,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "candidate_formula_run_count",
        fp2a.candidate_formula_run_count,
        report.row_run_count + report.column_run_count,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "formula_cells_represented_by_runs",
        fp2a.formula_cells_represented_by_runs,
        report.formula_cells_represented_by_runs,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "singleton_formula_count",
        fp2a.singleton_formula_count,
        report.singleton_run_count,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "hole_count",
        fp2a.hole_count,
        report.hole_count,
        "fp2a_axis_gaps_vs_fp2b_coordinate_gaps",
    );
    push_delta(
        &mut deltas,
        "exception_count",
        fp2a.exception_count,
        report.exception_count,
        "fp2a_axis_gaps_vs_fp2b_coordinate_gaps",
    );
    push_delta(
        &mut deltas,
        "candidate_row_block_partition_count",
        fp2a.candidate_row_block_partition_count,
        report.candidate_row_block_partition_count,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "candidate_formula_run_to_partition_edge_estimate",
        fp2a.candidate_formula_run_to_partition_edge_estimate,
        report.candidate_formula_run_to_partition_edge_estimate,
        run_reason,
    );
    push_delta(
        &mut deltas,
        "max_partitions_touched_by_run",
        fp2a.max_partitions_touched_by_run,
        report.max_partitions_touched_by_run,
        run_reason,
    );

    Fp2aReconciliation {
        matched: deltas.is_empty(),
        deltas,
    }
}

fn push_delta(
    deltas: &mut Vec<Fp2aCounterDelta>,
    field: &'static str,
    fp2a_value: u64,
    span_store_value: u64,
    reason: &'static str,
) {
    if fp2a_value != span_store_value {
        deltas.push(Fp2aCounterDelta {
            field,
            fp2a_value: fp2a_value as i64,
            span_store_value: span_store_value as i64,
            reason,
        });
    }
}

fn compare_run_key(a: &PendingRun, b: &PendingRun) -> std::cmp::Ordering {
    (
        a.template_id,
        &a.sheet,
        shape_order(a.shape),
        a.row_start,
        a.col_start,
        a.row_end,
        a.col_end,
    )
        .cmp(&(
            b.template_id,
            &b.sheet,
            shape_order(b.shape),
            b.row_start,
            b.col_start,
            b.row_end,
            b.col_end,
        ))
}

fn shape_order(shape: FormulaRunShape) -> u8 {
    match shape {
        FormulaRunShape::Row => 0,
        FormulaRunShape::Column => 1,
        FormulaRunShape::Singleton => 2,
    }
}

fn row_block_index(row: u32, row_block_size: u32) -> u32 {
    row.saturating_sub(1) / row_block_size.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(sheet: &str, row: u32, col: u32, template_id: &str) -> FormulaPlaneCandidateCell {
        FormulaPlaneCandidateCell {
            sheet: sheet.to_string(),
            row,
            col,
            template_id: template_id.to_string(),
            parse_ok: true,
            volatile: false,
            dynamic: false,
            unsupported: false,
        }
    }

    fn default_cell(row: u32, col: u32, template_id: &str) -> FormulaPlaneCandidateCell {
        cell("Sheet1", row, col, template_id)
    }

    fn rejected(
        row: u32,
        col: u32,
        template_id: &str,
        parse_ok: bool,
        unsupported: bool,
        dynamic: bool,
        volatile: bool,
    ) -> FormulaPlaneCandidateCell {
        FormulaPlaneCandidateCell {
            parse_ok,
            unsupported,
            dynamic,
            volatile,
            ..default_cell(row, col, template_id)
        }
    }

    fn build(cells: Vec<FormulaPlaneCandidateCell>) -> FormulaRunStore {
        FormulaRunStore::build_with_options(
            &cells,
            FormulaRunStoreBuildOptions {
                row_block_size: 4,
                ..FormulaRunStoreBuildOptions::default()
            },
        )
    }

    fn shuffled(mut cells: Vec<FormulaPlaneCandidateCell>) -> Vec<FormulaPlaneCandidateCell> {
        let len = cells.len();
        if len <= 1 {
            return cells;
        }
        let mut out = Vec::with_capacity(len);
        for index in (1..len).step_by(2) {
            out.push(cells[index].clone());
        }
        for index in (0..len).rev().step_by(2) {
            out.push(cells[index].clone());
        }
        cells.clear();
        out
    }

    #[test]
    fn deterministic_template_ids() {
        let cells = vec![
            default_cell(1, 1, "b"),
            default_cell(1, 2, "a"),
            default_cell(1, 3, "c"),
        ];
        let reversed = cells.iter().cloned().rev().collect::<Vec<_>>();
        let shuffled = shuffled(cells.clone());

        for input in [cells, reversed, shuffled] {
            let store = build(input);
            let ids = store
                .arena
                .templates
                .iter()
                .map(|template| (template.source_template_id.as_str(), template.id.0))
                .collect::<Vec<_>>();
            assert_eq!(ids, vec![("a", 0), ("b", 1), ("c", 2)]);
        }
    }

    #[test]
    fn deterministic_run_ids_for_shuffled_input() {
        let cells = vec![
            default_cell(1, 1, "a"),
            default_cell(2, 1, "a"),
            default_cell(3, 1, "a"),
            default_cell(5, 2, "b"),
            default_cell(5, 3, "b"),
            default_cell(5, 4, "b"),
            default_cell(8, 8, "c"),
        ];
        let expected = build(cells.clone());
        assert_eq!(expected, build(cells.iter().cloned().rev().collect()));
        assert_eq!(expected, build(shuffled(cells)));
        assert_eq!(
            expected.runs.iter().map(|run| run.id.0).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn column_run_basic() {
        let store = build((1..=4).map(|row| default_cell(row, 2, "tpl")).collect());
        assert_eq!(store.runs.len(), 1);
        let run = &store.runs[0];
        assert_eq!(run.shape, FormulaRunShape::Column);
        assert_eq!(
            (run.row_start, run.col_start, run.row_end, run.col_end),
            (1, 2, 4, 2)
        );
        assert_eq!((run.row_block_start, run.row_block_end), (0, 0));
        assert!(store.gaps.is_empty());
    }

    #[test]
    fn row_run_basic() {
        let store = build((2..=5).map(|col| default_cell(3, col, "tpl")).collect());
        assert_eq!(store.runs.len(), 1);
        let run = &store.runs[0];
        assert_eq!(run.shape, FormulaRunShape::Row);
        assert_eq!(
            (run.row_start, run.col_start, run.row_end, run.col_end),
            (3, 2, 3, 5)
        );
        assert_eq!((run.row_block_start, run.row_block_end), (0, 0));
    }

    #[test]
    fn singleton_supported_cell() {
        let store = build(vec![default_cell(7, 9, "tpl")]);
        assert_eq!(store.runs.len(), 1);
        assert_eq!(store.runs[0].shape, FormulaRunShape::Singleton);
        assert_eq!(store.runs[0].len, 1);
        assert!(store.rejected_cells.is_empty());
    }

    #[test]
    fn hole_splits_run() {
        let store = build(vec![
            default_cell(1, 1, "tpl"),
            default_cell(2, 1, "tpl"),
            default_cell(4, 1, "tpl"),
            default_cell(5, 1, "tpl"),
        ]);
        assert_eq!(store.runs.len(), 2);
        assert!(
            store
                .runs
                .iter()
                .all(|run| run.shape == FormulaRunShape::Column)
        );
        assert_eq!(store.gaps.len(), 1);
        assert_eq!(
            store.gaps[0],
            SpanGapDescriptor {
                template_id: FormulaTemplateId(0),
                sheet: "Sheet1".to_string(),
                row: 3,
                col: 1,
                kind: SpanGapKind::Hole,
            }
        );
    }

    #[test]
    fn exception_splits_run() {
        let store = build(vec![
            default_cell(1, 1, "a"),
            default_cell(2, 1, "a"),
            default_cell(3, 1, "b"),
            default_cell(4, 1, "a"),
        ]);
        assert_eq!(store.runs.len(), 3);
        assert_eq!(store.gaps.len(), 1);
        assert_eq!(
            store.gaps[0].kind,
            SpanGapKind::Exception {
                other_template_id: FormulaTemplateId(1)
            }
        );
        assert_eq!((store.gaps[0].row, store.gaps[0].col), (3, 1));
    }

    #[test]
    fn rejected_parse_error() {
        let store = build(vec![rejected(1, 1, "tpl", false, false, false, false)]);
        assert!(store.runs.is_empty());
        assert_eq!(store.rejected_cells.len(), 1);
        assert_eq!(
            store.rejected_cells[0].reason,
            FormulaRejectReason::ParseError
        );
        assert_eq!(store.report.parse_error_formula_count, 1);
    }

    #[test]
    fn rejected_unsupported_dynamic_volatile_order() {
        let store = build(vec![
            rejected(1, 1, "parse", false, true, true, true),
            rejected(2, 1, "unsupported", true, true, true, true),
            rejected(3, 1, "dynamic", true, false, true, true),
            rejected(4, 1, "volatile", true, false, false, true),
        ]);
        let reasons = store
            .rejected_cells
            .iter()
            .map(|cell| cell.reason)
            .collect::<Vec<_>>();
        assert_eq!(
            reasons,
            vec![
                FormulaRejectReason::ParseError,
                FormulaRejectReason::Unsupported,
                FormulaRejectReason::Dynamic,
                FormulaRejectReason::Volatile,
            ]
        );
    }

    #[test]
    fn rejected_inside_supported_span() {
        let store = build(vec![
            default_cell(1, 1, "a"),
            rejected(2, 1, "b", false, false, false, false),
            default_cell(3, 1, "a"),
        ]);
        assert_eq!(store.rejected_cells.len(), 1);
        assert_eq!(store.gaps.len(), 1);
        assert_eq!(
            store.gaps[0].kind,
            SpanGapKind::Exception {
                other_template_id: FormulaTemplateId(1)
            }
        );
        assert_eq!(store.report.hole_count, 0);
        assert_eq!(store.report.exception_count, 1);
    }

    #[test]
    fn overlap_dedup_longer_run_wins() {
        let store = build(vec![
            default_cell(3, 1, "tpl"),
            default_cell(3, 2, "tpl"),
            default_cell(3, 3, "tpl"),
            default_cell(3, 4, "tpl"),
            default_cell(1, 2, "tpl"),
            default_cell(2, 2, "tpl"),
            default_cell(4, 2, "tpl"),
        ]);
        let row_runs = store
            .runs
            .iter()
            .filter(|run| run.shape == FormulaRunShape::Row)
            .count();
        assert_eq!(row_runs, 1);
        assert!(
            store
                .runs
                .iter()
                .any(|run| run.shape == FormulaRunShape::Row && run.len == 4)
        );
        assert_eq!(store.report.overlap_dropped_count, 1);
        assert_eq!(store.runs.iter().map(|run| run.len).sum::<u64>(), 7);
    }

    #[test]
    fn rectangle_deferred() {
        let cells = (1..=2)
            .flat_map(|row| (1..=3).map(move |col| default_cell(row, col, "tpl")))
            .collect::<Vec<_>>();
        let store = build(cells);
        assert_eq!(store.report.rectangle_deferred_count, 1);
        assert_eq!(store.report.row_run_count, 2);
        assert_eq!(store.report.column_run_count, 0);
        assert!(
            store
                .runs
                .iter()
                .all(|run| run.shape != FormulaRunShape::Singleton)
        );
    }

    #[test]
    fn fp2a_reconciliation_dense_vertical() {
        let cells = (1..=10)
            .map(|row| default_cell(row, 2, "tpl"))
            .collect::<Vec<_>>();
        let store = build(cells);
        assert_eq!(store.report.column_run_count, 1);
        assert_eq!(store.report.candidate_row_block_partition_count, 3);
        assert_eq!(
            store
                .report
                .candidate_formula_run_to_partition_edge_estimate,
            3
        );
        assert_eq!(store.report.max_partitions_touched_by_run, 3);
        assert!(store.report.reconciliation.matched);
        assert!(store.report.reconciliation.deltas.is_empty());
    }

    #[test]
    fn multi_sheet_determinism() {
        let cells = vec![
            cell("Alpha", 1, 1, "tpl"),
            cell("Alpha", 2, 1, "tpl"),
            cell("Beta", 4, 3, "tpl"),
            cell("Beta", 4, 4, "tpl"),
            rejected(9, 9, "bad", false, false, false, false),
        ];
        let mut beta_bad = cells[4].clone();
        beta_bad.sheet = "Beta".to_string();
        let mut input = cells[..4].to_vec();
        input.push(beta_bad);

        let expected = build(input.clone());
        assert_eq!(expected, build(input.iter().cloned().rev().collect()));
        assert_eq!(expected, build(shuffled(input)));
        assert_eq!(expected.arena.templates[1].source_template_id, "tpl");
        assert_eq!(expected.runs[0].sheet, "Alpha");
        assert_eq!(expected.runs[1].sheet, "Beta");
    }

    #[test]
    fn template_status_mixed_for_supported_and_rejected_same_source() {
        let store = build(vec![
            default_cell(1, 1, "tpl"),
            rejected(2, 1, "tpl", false, false, false, false),
        ]);
        assert_eq!(store.arena.templates.len(), 1);
        assert_eq!(
            store.arena.templates[0].status,
            TemplateSupportStatus::Mixed
        );
        assert_eq!(store.runs.len(), 1);
        assert_eq!(store.rejected_cells.len(), 1);
    }

    #[test]
    fn empty_input() {
        let store = build(Vec::new());
        assert!(store.arena.templates.is_empty());
        assert!(store.runs.is_empty());
        assert!(store.gaps.is_empty());
        assert!(store.rejected_cells.is_empty());
        assert_eq!(store.report.formula_cell_count, 0);
        assert!(store.report.reconciliation.matched);
    }

    #[test]
    fn single_unsupported_template_only() {
        let store = build(vec![rejected(1, 1, "tpl", true, true, false, false)]);
        assert_eq!(store.arena.templates.len(), 1);
        assert_eq!(
            store.arena.templates[0].status,
            TemplateSupportStatus::Unsupported
        );
        assert!(store.runs.is_empty());
        assert_eq!(store.rejected_cells.len(), 1);
    }

    #[test]
    fn row_block_size_normalization() {
        let cells = vec![default_cell(1, 1, "tpl"), default_cell(2, 1, "tpl")];
        let store = FormulaRunStore::build_with_options(
            &cells,
            FormulaRunStoreBuildOptions {
                row_block_size: 0,
                ..FormulaRunStoreBuildOptions::default()
            },
        );
        assert_eq!(store.row_block_size, 1);
        assert_eq!(store.runs[0].row_block_start, 0);
        assert_eq!(store.runs[0].row_block_end, 1);
    }
}
