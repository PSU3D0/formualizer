//! Passive FormulaPlane span and row-block partition counters.
//!
//! These counters are diagnostic only. They describe candidate dependent formula
//! placements and estimated row-block fanout; they do not route dirty
//! propagation, evaluation, or dependency graph construction.

use std::collections::{BTreeMap, BTreeSet};

/// Default diagnostic row block size used by FP2.A candidate partition counts.
pub const DEFAULT_CANDIDATE_ROW_BLOCK_SIZE: u32 = 4096;

/// Options for passive span/partition counter construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanPartitionCounterOptions {
    /// Fixed row-block height used for diagnostic candidate partitioning.
    pub row_block_size: u32,
}

impl Default for SpanPartitionCounterOptions {
    fn default() -> Self {
        Self {
            row_block_size: DEFAULT_CANDIDATE_ROW_BLOCK_SIZE,
        }
    }
}

impl SpanPartitionCounterOptions {
    fn normalized(self) -> Self {
        Self {
            row_block_size: self.row_block_size.max(1),
        }
    }
}

/// One parser-backed formula cell observed by a passive scanner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaPlaneCandidateCell {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub template_id: String,
    pub parse_ok: bool,
    pub volatile: bool,
    pub dynamic: bool,
    pub unsupported: bool,
}

/// Direction of a contiguous candidate dependent formula placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateRunOrientation {
    Row,
    Column,
}

/// Passive candidate run summary for a dependent formula placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CandidateFormulaRun {
    pub template_id: String,
    pub sheet: String,
    pub orientation: CandidateRunOrientation,
    /// Row for row runs, column for column runs.
    pub fixed_index: u32,
    /// Column start for row runs, row start for column runs.
    pub start_index: u32,
    /// Column end for row runs, row end for column runs.
    pub end_index: u32,
    pub len: u64,
    pub partitions_touched: u64,
}

/// Per-template passive FormulaPlane candidate counters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TemplateSpanPartitionCounters {
    pub template_id: String,
    pub formula_cells: u64,
    pub row_run_count: u64,
    pub column_run_count: u64,
    pub max_run_length: u64,
    pub formula_cells_represented_by_runs: u64,
    pub singleton_formula_count: u64,
    pub hole_count: u64,
    pub exception_count: u64,
    pub candidate_partition_count: u64,
    pub candidate_formula_run_to_partition_edge_estimate: u64,
    pub max_partitions_touched_by_run: u64,
    pub dense_run_coverage_percent: f64,
}

/// Workbook-level passive FormulaPlane candidate counters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpanPartitionCounters {
    pub row_block_size: u32,
    pub template_count: u64,
    pub repeated_template_count: u64,
    pub formula_cell_count: u64,
    pub parse_error_formula_count: u64,
    pub volatile_formula_count: u64,
    pub dynamic_formula_count: u64,
    pub unsupported_formula_count: u64,
    pub row_run_count: u64,
    pub column_run_count: u64,
    pub candidate_formula_run_count: u64,
    pub max_run_length: u64,
    pub formula_cells_represented_by_runs: u64,
    pub singleton_formula_count: u64,
    pub hole_count: u64,
    pub exception_count: u64,
    /// Estimate: formula cells in repeated candidate runs.
    pub estimated_materialization_avoidable_cell_count: u64,
    pub candidate_row_block_partition_count: u64,
    pub candidate_formula_run_to_partition_edge_estimate: u64,
    pub max_partitions_touched_by_run: u64,
    pub dense_run_coverage_percent: f64,
    pub template_counters: Vec<TemplateSpanPartitionCounters>,
    pub candidate_runs: Vec<CandidateFormulaRun>,
}

/// Build passive span and row-block partition counters from scanner cells.
///
/// The result is diagnostic only. Formula runs are candidate dependent formula
/// placements; candidate partitions are fixed row blocks of those placements.
/// No precedent region or result region authority is inferred here.
pub fn compute_span_partition_counters(
    cells: &[FormulaPlaneCandidateCell],
    options: SpanPartitionCounterOptions,
) -> SpanPartitionCounters {
    let options = options.normalized();
    let mut by_template: BTreeMap<String, Vec<&FormulaPlaneCandidateCell>> = BTreeMap::new();
    let mut cell_to_template: BTreeMap<(String, u32, u32), String> = BTreeMap::new();

    for cell in cells {
        cell_to_template.insert(
            (cell.sheet.clone(), cell.row, cell.col),
            cell.template_id.clone(),
        );
        by_template
            .entry(cell.template_id.clone())
            .or_default()
            .push(cell);
    }

    let mut out = SpanPartitionCounters {
        row_block_size: options.row_block_size,
        template_count: by_template.len() as u64,
        formula_cell_count: cells.len() as u64,
        parse_error_formula_count: cells.iter().filter(|cell| !cell.parse_ok).count() as u64,
        volatile_formula_count: cells.iter().filter(|cell| cell.volatile).count() as u64,
        dynamic_formula_count: cells.iter().filter(|cell| cell.dynamic).count() as u64,
        unsupported_formula_count: cells.iter().filter(|cell| cell.unsupported).count() as u64,
        ..SpanPartitionCounters::default()
    };

    let mut represented_cells: BTreeSet<(String, u32, u32)> = BTreeSet::new();
    let mut candidate_partitions: BTreeSet<(String, u32)> = BTreeSet::new();

    for (template_id, template_cells) in by_template {
        if template_cells.len() > 1 {
            out.repeated_template_count += 1;
        }

        let mut template_counter = TemplateSpanPartitionCounters {
            template_id: template_id.clone(),
            formula_cells: template_cells.len() as u64,
            ..TemplateSpanPartitionCounters::default()
        };
        let mut template_represented_cells: BTreeSet<(String, u32, u32)> = BTreeSet::new();
        let mut template_partitions: BTreeSet<(String, u32)> = BTreeSet::new();

        let row_runs = build_runs(
            &template_id,
            &template_cells,
            CandidateRunOrientation::Row,
            options.row_block_size,
        );
        let column_runs = build_runs(
            &template_id,
            &template_cells,
            CandidateRunOrientation::Column,
            options.row_block_size,
        );

        let (row_holes, row_exceptions) = count_axis_gaps(
            &template_id,
            &template_cells,
            &cell_to_template,
            CandidateRunOrientation::Row,
        );
        let (column_holes, column_exceptions) = count_axis_gaps(
            &template_id,
            &template_cells,
            &cell_to_template,
            CandidateRunOrientation::Column,
        );

        for run in row_runs.iter().chain(column_runs.iter()) {
            template_counter.max_run_length = template_counter.max_run_length.max(run.len);
            template_counter.candidate_formula_run_to_partition_edge_estimate +=
                run.partitions_touched;
            template_counter.max_partitions_touched_by_run = template_counter
                .max_partitions_touched_by_run
                .max(run.partitions_touched);

            for cell_key in cells_for_run(run) {
                represented_cells.insert(cell_key.clone());
                template_represented_cells.insert(cell_key);
            }
            for partition in partitions_for_run(run, options.row_block_size) {
                candidate_partitions.insert(partition.clone());
                template_partitions.insert(partition);
            }
        }

        template_counter.row_run_count = row_runs.len() as u64;
        template_counter.column_run_count = column_runs.len() as u64;
        template_counter.formula_cells_represented_by_runs =
            template_represented_cells.len() as u64;
        template_counter.singleton_formula_count = template_counter
            .formula_cells
            .saturating_sub(template_counter.formula_cells_represented_by_runs);
        template_counter.hole_count = row_holes + column_holes;
        template_counter.exception_count = row_exceptions + column_exceptions;
        template_counter.candidate_partition_count = template_partitions.len() as u64;
        template_counter.dense_run_coverage_percent = percent(
            template_counter.formula_cells_represented_by_runs,
            template_counter.formula_cells,
        );

        out.row_run_count += template_counter.row_run_count;
        out.column_run_count += template_counter.column_run_count;
        out.max_run_length = out.max_run_length.max(template_counter.max_run_length);
        out.hole_count += template_counter.hole_count;
        out.exception_count += template_counter.exception_count;
        out.candidate_formula_run_to_partition_edge_estimate +=
            template_counter.candidate_formula_run_to_partition_edge_estimate;
        out.max_partitions_touched_by_run = out
            .max_partitions_touched_by_run
            .max(template_counter.max_partitions_touched_by_run);
        out.candidate_runs.extend(row_runs);
        out.candidate_runs.extend(column_runs);
        out.template_counters.push(template_counter);
    }

    out.candidate_formula_run_count = out.row_run_count + out.column_run_count;
    out.formula_cells_represented_by_runs = represented_cells.len() as u64;
    out.singleton_formula_count = out
        .formula_cell_count
        .saturating_sub(out.formula_cells_represented_by_runs);
    out.estimated_materialization_avoidable_cell_count = out.formula_cells_represented_by_runs;
    out.candidate_row_block_partition_count = candidate_partitions.len() as u64;
    out.dense_run_coverage_percent = percent(
        out.formula_cells_represented_by_runs,
        out.formula_cell_count,
    );
    out.template_counters
        .sort_by(|a, b| a.template_id.cmp(&b.template_id));
    out.candidate_runs.sort_by(|a, b| {
        (
            &a.template_id,
            &a.sheet,
            orientation_key(a.orientation),
            a.fixed_index,
            a.start_index,
        )
            .cmp(&(
                &b.template_id,
                &b.sheet,
                orientation_key(b.orientation),
                b.fixed_index,
                b.start_index,
            ))
    });
    out
}

fn build_runs(
    template_id: &str,
    cells: &[&FormulaPlaneCandidateCell],
    orientation: CandidateRunOrientation,
    row_block_size: u32,
) -> Vec<CandidateFormulaRun> {
    let mut groups: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    for cell in cells {
        let key = match orientation {
            CandidateRunOrientation::Row => (cell.sheet.clone(), cell.row),
            CandidateRunOrientation::Column => (cell.sheet.clone(), cell.col),
        };
        let value = match orientation {
            CandidateRunOrientation::Row => cell.col,
            CandidateRunOrientation::Column => cell.row,
        };
        groups.entry(key).or_default().push(value);
    }

    let mut out = Vec::new();
    for ((sheet, fixed_index), mut values) in groups {
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
                (Some(_), Some(previous)) if value == previous + 1 => {
                    prev = Some(value);
                }
                (Some(run_start), Some(run_end)) => {
                    push_run(
                        &mut out,
                        template_id,
                        &sheet,
                        orientation,
                        fixed_index,
                        run_start,
                        run_end,
                        row_block_size,
                    );
                    start = Some(value);
                    prev = Some(value);
                }
                (Some(_), None) => unreachable!(),
            }
        }
        if let (Some(run_start), Some(run_end)) = (start, prev) {
            push_run(
                &mut out,
                template_id,
                &sheet,
                orientation,
                fixed_index,
                run_start,
                run_end,
                row_block_size,
            );
        }
    }
    out
}

fn push_run(
    out: &mut Vec<CandidateFormulaRun>,
    template_id: &str,
    sheet: &str,
    orientation: CandidateRunOrientation,
    fixed_index: u32,
    start_index: u32,
    end_index: u32,
    row_block_size: u32,
) {
    if end_index <= start_index {
        return;
    }
    let len = u64::from(end_index - start_index + 1);
    let partitions_touched = match orientation {
        CandidateRunOrientation::Row => 1,
        CandidateRunOrientation::Column => {
            let start_block = row_block_index(start_index, row_block_size);
            let end_block = row_block_index(end_index, row_block_size);
            u64::from(end_block - start_block + 1)
        }
    };
    out.push(CandidateFormulaRun {
        template_id: template_id.to_string(),
        sheet: sheet.to_string(),
        orientation,
        fixed_index,
        start_index,
        end_index,
        len,
        partitions_touched,
    });
}

fn count_axis_gaps(
    template_id: &str,
    cells: &[&FormulaPlaneCandidateCell],
    cell_to_template: &BTreeMap<(String, u32, u32), String>,
    orientation: CandidateRunOrientation,
) -> (u64, u64) {
    let mut groups: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    for cell in cells {
        let key = match orientation {
            CandidateRunOrientation::Row => (cell.sheet.clone(), cell.row),
            CandidateRunOrientation::Column => (cell.sheet.clone(), cell.col),
        };
        let value = match orientation {
            CandidateRunOrientation::Row => cell.col,
            CandidateRunOrientation::Column => cell.row,
        };
        groups.entry(key).or_default().push(value);
    }

    let mut holes = 0u64;
    let mut exceptions = 0u64;
    for ((sheet, fixed), mut values) in groups {
        values.sort_unstable();
        values.dedup();
        let Some(min) = values.first().copied() else {
            continue;
        };
        let Some(max) = values.last().copied() else {
            continue;
        };
        let present = values.into_iter().collect::<BTreeSet<_>>();
        for value in min..=max {
            if present.contains(&value) {
                continue;
            }
            let key = match orientation {
                CandidateRunOrientation::Row => (sheet.clone(), fixed, value),
                CandidateRunOrientation::Column => (sheet.clone(), value, fixed),
            };
            match cell_to_template.get(&key) {
                Some(other) if other != template_id => exceptions += 1,
                Some(_) => {}
                None => holes += 1,
            }
        }
    }
    (holes, exceptions)
}

fn cells_for_run(run: &CandidateFormulaRun) -> Vec<(String, u32, u32)> {
    (run.start_index..=run.end_index)
        .map(|index| match run.orientation {
            CandidateRunOrientation::Row => (run.sheet.clone(), run.fixed_index, index),
            CandidateRunOrientation::Column => (run.sheet.clone(), index, run.fixed_index),
        })
        .collect()
}

fn partitions_for_run(run: &CandidateFormulaRun, row_block_size: u32) -> Vec<(String, u32)> {
    match run.orientation {
        CandidateRunOrientation::Row => {
            vec![(
                run.sheet.clone(),
                row_block_index(run.fixed_index, row_block_size),
            )]
        }
        CandidateRunOrientation::Column => {
            let start_block = row_block_index(run.start_index, row_block_size);
            let end_block = row_block_index(run.end_index, row_block_size);
            (start_block..=end_block)
                .map(|block| (run.sheet.clone(), block))
                .collect()
        }
    }
}

fn row_block_index(row: u32, row_block_size: u32) -> u32 {
    row.saturating_sub(1) / row_block_size.max(1)
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64) * 100.0 / (denominator as f64)
    }
}

fn orientation_key(orientation: CandidateRunOrientation) -> u8 {
    match orientation {
        CandidateRunOrientation::Row => 0,
        CandidateRunOrientation::Column => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(row: u32, col: u32, template_id: &str) -> FormulaPlaneCandidateCell {
        FormulaPlaneCandidateCell {
            sheet: "Sheet1".to_string(),
            row,
            col,
            template_id: template_id.to_string(),
            parse_ok: true,
            volatile: false,
            dynamic: false,
            unsupported: false,
        }
    }

    #[test]
    fn counts_vertical_runs_and_row_block_partitions() {
        let cells = (1..=10)
            .map(|row| cell(row, 2, "tpl_a"))
            .collect::<Vec<_>>();
        let counters = compute_span_partition_counters(
            &cells,
            SpanPartitionCounterOptions { row_block_size: 4 },
        );

        assert_eq!(counters.template_count, 1);
        assert_eq!(counters.repeated_template_count, 1);
        assert_eq!(counters.column_run_count, 1);
        assert_eq!(counters.row_run_count, 0);
        assert_eq!(counters.max_run_length, 10);
        assert_eq!(counters.formula_cells_represented_by_runs, 10);
        assert_eq!(counters.singleton_formula_count, 0);
        assert_eq!(counters.candidate_row_block_partition_count, 3);
        assert_eq!(counters.candidate_formula_run_to_partition_edge_estimate, 3);
        assert_eq!(counters.max_partitions_touched_by_run, 3);
        assert_eq!(counters.estimated_materialization_avoidable_cell_count, 10);
        assert_eq!(counters.dense_run_coverage_percent, 100.0);
    }

    #[test]
    fn counts_holes_exceptions_and_singletons() {
        let cells = vec![
            cell(1, 1, "tpl_a"),
            cell(4, 1, "tpl_a"),
            cell(3, 1, "tpl_b"),
            FormulaPlaneCandidateCell {
                parse_ok: false,
                unsupported: true,
                ..cell(10, 1, "tpl_parse")
            },
        ];
        let counters = compute_span_partition_counters(
            &cells,
            SpanPartitionCounterOptions { row_block_size: 16 },
        );

        assert_eq!(counters.formula_cell_count, 4);
        assert_eq!(counters.parse_error_formula_count, 1);
        assert_eq!(counters.unsupported_formula_count, 1);
        assert_eq!(counters.column_run_count, 0);
        assert_eq!(counters.formula_cells_represented_by_runs, 0);
        assert_eq!(counters.singleton_formula_count, 4);
        assert_eq!(counters.hole_count, 1);
        assert_eq!(counters.exception_count, 1);
    }
}
