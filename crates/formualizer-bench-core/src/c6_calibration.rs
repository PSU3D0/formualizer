use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use formualizer_eval::engine::EvaluationTarget;
use formualizer_testkit::write_workbook;

pub mod families;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum FixtureFamily {
    Scalar,
    CrossSheet,
    Names,
    Layout,
    NativeTable,
    Dynamic,
}

impl FixtureFamily {
    pub const ALL: [Self; 6] = [
        Self::Scalar,
        Self::CrossSheet,
        Self::Names,
        Self::Layout,
        Self::NativeTable,
        Self::Dynamic,
    ];

    pub const BREADTH: [Self; 5] = [
        Self::CrossSheet,
        Self::Names,
        Self::Layout,
        Self::NativeTable,
        Self::Dynamic,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::CrossSheet => "cross_sheet",
            Self::Names => "names",
            Self::Layout => "layout",
            Self::NativeTable => "native_table",
            Self::Dynamic => "dynamic",
        }
    }

    pub const fn cli_label(self) -> &'static str {
        match self {
            Self::CrossSheet => "cross-sheet",
            Self::NativeTable => "native-table",
            _ => self.label(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum CalibrationPath {
    Full,
    Cells,
    Targets,
    Plan,
    Sheetport,
}

impl CalibrationPath {
    /// The original scalar matrix. Keep this stable for CLI/default compatibility.
    pub const SCALAR_V2: [Self; 4] = [Self::Full, Self::Cells, Self::Plan, Self::Sheetport];
    pub const ALL: [Self; 5] = [
        Self::Full,
        Self::Cells,
        Self::Targets,
        Self::Plan,
        Self::Sheetport,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Cells => "cells",
            Self::Targets => "targets",
            Self::Plan => "plan",
            Self::Sheetport => "sheetport",
        }
    }
}

/// The hard-coded Phase-A native family/path contract. Unsupported selectors are
/// deliberately absent instead of being emulated through a less specific API.
pub const fn path_supported(family: FixtureFamily, path: CalibrationPath) -> bool {
    match family {
        FixtureFamily::Scalar => matches!(
            path,
            CalibrationPath::Full
                | CalibrationPath::Cells
                | CalibrationPath::Plan
                | CalibrationPath::Sheetport
        ),
        FixtureFamily::CrossSheet | FixtureFamily::Dynamic => matches!(
            path,
            CalibrationPath::Full
                | CalibrationPath::Cells
                | CalibrationPath::Targets
                | CalibrationPath::Plan
                | CalibrationPath::Sheetport
        ),
        FixtureFamily::Names => matches!(
            path,
            CalibrationPath::Full
                | CalibrationPath::Targets
                | CalibrationPath::Plan
                | CalibrationPath::Sheetport
        ),
        FixtureFamily::Layout => {
            matches!(path, CalibrationPath::Full | CalibrationPath::Sheetport)
        }
        FixtureFamily::NativeTable => matches!(
            path,
            CalibrationPath::Full
                | CalibrationPath::Targets
                | CalibrationPath::Plan
                | CalibrationPath::Sheetport
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum TypedOracleValue {
    String(String),
    Integer(i64),
    Number(f64),
    Date(String),
    Boolean(bool),
    Empty,
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TargetScope {
    Tiny,
    Medium,
    Full,
}

impl TargetScope {
    pub const ALL: [Self; 3] = [Self::Tiny, Self::Medium, Self::Full];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Tiny => "tiny_lt_1pct",
            Self::Medium => "medium_approx_10pct",
            Self::Full => "full_100pct",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureShape {
    pub formulas: u32,
    pub tiny_formulas: u32,
    pub medium_formulas: u32,
    pub large_formulas: u32,
    pub dirty_formulas: u32,
}

impl FixtureShape {
    pub fn new(formulas: u32) -> Result<Self> {
        if formulas < 200 {
            bail!("--formulas must be at least 200");
        }
        let tiny_formulas = (formulas / 200).max(1); // 0.5%
        let medium_formulas = (formulas / 10).max(1); // 10%
        let dirty_formulas = 8;
        let large_formulas = formulas
            .checked_sub(tiny_formulas + medium_formulas + dirty_formulas)
            .ok_or_else(|| anyhow::anyhow!("fixture branch allocation overflow"))?;
        Ok(Self {
            formulas,
            tiny_formulas,
            medium_formulas,
            large_formulas,
            dirty_formulas,
        })
    }

    pub const fn oracle(self, scope: TargetScope) -> u32 {
        match scope {
            TargetScope::Tiny => self.tiny_formulas,
            TargetScope::Medium => self.medium_formulas,
            TargetScope::Full => self.formulas,
        }
    }

    pub const fn edit_sheet(self, scope: TargetScope) -> &'static str {
        match scope {
            TargetScope::Tiny | TargetScope::Full => "Tiny",
            TargetScope::Medium => "Medium",
        }
    }

    pub fn targets(self, scope: TargetScope) -> Vec<EvaluationTarget> {
        let cell = |sheet: &str, row| EvaluationTarget::Cell {
            sheet: sheet.to_string(),
            row,
            col: 2,
        };
        match scope {
            TargetScope::Tiny => vec![cell("Tiny", self.tiny_formulas)],
            TargetScope::Medium => vec![cell("Medium", self.medium_formulas)],
            TargetScope::Full => vec![
                cell("Tiny", self.tiny_formulas),
                cell("Medium", self.medium_formulas),
                cell("Large", self.large_formulas),
                cell("Dirty", self.dirty_formulas),
            ],
        }
    }

    pub fn cell_targets(self, scope: TargetScope) -> Vec<(&'static str, u32, u32)> {
        match scope {
            TargetScope::Tiny => vec![("Tiny", self.tiny_formulas, 2)],
            TargetScope::Medium => vec![("Medium", self.medium_formulas, 2)],
            TargetScope::Full => vec![
                ("Tiny", self.tiny_formulas, 2),
                ("Medium", self.medium_formulas, 2),
                ("Large", self.large_formulas, 2),
                ("Dirty", self.dirty_formulas, 2),
            ],
        }
    }

    pub fn output_cells(self, scope: TargetScope) -> Vec<(&'static str, u32, u32)> {
        self.cell_targets(scope)
    }
}

pub fn generate_fixture(path: &Path, formulas: u32) -> Result<FixtureShape> {
    let shape = FixtureShape::new(formulas)?;
    write_workbook(path, |book| {
        book.get_sheet_by_name_mut("Sheet1")
            .expect("default sheet")
            .set_name("Tiny");
        book.new_sheet("Medium").expect("Medium sheet");
        book.new_sheet("Large").expect("Large sheet");
        book.new_sheet("Dirty").expect("Dirty sheet");
        populate_finance_branch(book, "Tiny", shape.tiny_formulas, 1.0);
        populate_finance_branch(book, "Medium", shape.medium_formulas, 2.0);
        populate_finance_branch(book, "Large", shape.large_formulas, 3.0);
        populate_finance_branch(book, "Dirty", shape.dirty_formulas, 4.0);
    });
    Ok(shape)
}

fn populate_finance_branch(
    book: &mut umya_spreadsheet::Spreadsheet,
    sheet_name: &str,
    formulas: u32,
    seed: f64,
) {
    let sheet = book
        .get_sheet_by_name_mut(sheet_name)
        .expect("fixture sheet exists");
    sheet.get_cell_mut((1, 1)).set_value_number(seed);
    sheet.get_cell_mut((2, 1)).set_formula("=A1*1.015-0.25");
    for row in 2..=formulas {
        sheet
            .get_cell_mut((2, row))
            .set_formula(format!("=B{}*1.0001+$A$1*0.00001", row - 1));
    }
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub fn checksum_values(values: &[String]) -> String {
    let mut digest = Sha256::new();
    for value in values {
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

pub fn analytical_terminal(seed: f64, formulas: u32) -> f64 {
    let first = seed * 1.015 - 0.25;
    if formulas == 1 {
        return first;
    }
    let ratio = 1.0001_f64;
    let power = ratio.powi((formulas - 1) as i32);
    first * power + seed * 0.00001 * (power - 1.0) / (ratio - 1.0)
}

pub fn analytical_expected_outputs(
    shape: FixtureShape,
    scope: TargetScope,
    warm_repeats: usize,
) -> Vec<Vec<f64>> {
    (0..=warm_repeats)
        .map(|evaluation| {
            let repeat_seed = (evaluation > 0).then_some(1.0 + evaluation as f64);
            shape
                .output_cells(scope)
                .into_iter()
                .map(|(sheet, formulas, _)| {
                    let seed = match sheet {
                        "Tiny" => repeat_seed
                            .filter(|_| shape.edit_sheet(scope) == "Tiny")
                            .unwrap_or(1.0),
                        "Medium" => repeat_seed
                            .filter(|_| shape.edit_sheet(scope) == "Medium")
                            .unwrap_or(2.0),
                        "Large" => 3.0,
                        "Dirty" => 40.0,
                        _ => unreachable!("fixture output sheet"),
                    };
                    analytical_terminal(seed, formulas)
                })
                .collect()
        })
        .collect()
}

pub fn manifest_yaml(shape: FixtureShape, scope: TargetScope) -> String {
    let mut yaml = String::from(
        "spec: fio\nspec_version: \"0.3.0\"\nmanifest:\n  id: c6-target-locality\n  name: C6 Target Locality\n  workbook:\n    uri: fixture://c6.xlsx\n    locale: en-US\n    date_system: 1900\nports:\n",
    );
    yaml.push_str(&format!(
        "  - id: input\n    dir: in\n    shape: scalar\n    location: {{ a1: {}!A1 }}\n    schema: {{ type: number }}\n",
        shape.edit_sheet(scope)
    ));
    for (index, (sheet, row, _)) in shape.output_cells(scope).iter().enumerate() {
        yaml.push_str(&format!(
            "  - id: output_{index}\n    dir: out\n    shape: scalar\n    location: {{ a1: {sheet}!B{row} }}\n    schema: {{ type: number }}\n"
        ));
    }
    yaml
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimedPhase {
    pub milliseconds: f64,
    pub includes: Vec<String>,
}

impl TimedPhase {
    pub fn new(milliseconds: f64, includes: &[&str]) -> Self {
        Self {
            milliseconds,
            includes: includes.iter().map(|part| (*part).to_string()).collect(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PhaseTimings {
    pub load: Option<TimedPhase>,
    pub unrelated_dirty_setup: Option<TimedPhase>,
    #[serde(default)]
    pub selector_setup: Option<TimedPhase>,
    #[serde(default)]
    pub name_binding_probe: Option<TimedPhase>,
    pub bind_target_resolution: Option<TimedPhase>,
    pub preparation_plan_build: Option<TimedPhase>,
    pub first_evaluation: Option<TimedPhase>,
    pub output_read: Option<TimedPhase>,
    pub edit: Vec<TimedPhase>,
    pub warm_evaluation: Vec<TimedPhase>,
    pub warm_output_read: Vec<TimedPhase>,
    pub batch_restore: Option<TimedPhase>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineTelemetry {
    pub request_id: u64,
    pub request_kind: String,
    pub request_outcome: String,
    pub graph_vertices: usize,
    pub graph_formula_vertices: usize,
    pub graph_edges: usize,
    pub dirty_vertices: usize,
    pub staged_formulas: usize,
    pub active_spans: usize,
    pub target_requested: u64,
    pub target_normalized_regions: u64,
    pub staged_selected: u64,
    pub staged_retained: u64,
    pub preparation_scope_level: u8,
    pub widening_reason_bits: u64,
    pub target_commit_estimated_work: u64,
    pub target_commit_actual_work: u64,
    pub work_charged: u64,
    pub topology_strategy: String,
    pub topology_cache_outcome: String,
    pub topology_producers: u64,
    pub topology_candidates: u64,
    pub topology_edges: u64,
    pub topology_retained_bytes: u64,
    pub exact_pass_count: u64,
    pub native_topology_disk_bytes: u64,
    pub fallback_materialized_cells: u64,
    pub cycle_materialized_cells: u64,
    pub dirty_lease_outcome: String,
    pub retained_current: u64,
    pub retained_peak: u64,
    pub scratch_current: u64,
    pub scratch_peak: u64,
    pub graph_source_scratch_estimated: u64,
    pub graph_source_scratch_observed: u64,
    pub request_total_ms: f64,
    pub graph_prepare_ms: f64,
    pub topology_ms: f64,
    pub materialization_ms: f64,
    pub evaluation_ms: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub graph_vertices: usize,
    pub graph_formula_vertices: usize,
    pub graph_edges: usize,
    pub dirty_vertices: usize,
    pub evaluation_vertices: usize,
    pub staged_formulas: usize,
    pub active_spans: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildReport {
    pub schema_version: u32,
    #[serde(default = "default_scalar_family")]
    pub family: FixtureFamily,
    #[serde(default = "default_path_schema_version")]
    pub path_schema_version: u32,
    #[serde(default)]
    pub selector_set: String,
    pub path: CalibrationPath,
    pub scope: TargetScope,
    pub formulas: u32,
    pub reachable_oracle: u32,
    pub fixture_sha256: String,
    pub status: String,
    pub phases: PhaseTimings,
    pub outputs: Vec<Vec<String>>,
    pub analytical_expected_outputs: Vec<Vec<f64>>,
    pub analytical_output_oracle_passed: Option<bool>,
    #[serde(default)]
    pub typed_outputs: Vec<Vec<TypedOracleValue>>,
    #[serde(default)]
    pub typed_expected_outputs: Vec<Vec<TypedOracleValue>>,
    #[serde(default)]
    pub family_gates: BTreeMap<String, bool>,
    #[serde(default)]
    pub structural_oracles: BTreeMap<String, String>,
    #[serde(default)]
    pub plan_stale_reason: Option<String>,
    pub output_checksum: Option<String>,
    pub typed_error: Option<String>,
    pub telemetry: BTreeMap<String, EngineTelemetry>,
    pub graph_after_load: Option<GraphSnapshot>,
    pub graph_after_setup: Option<GraphSnapshot>,
    pub graph_after_run: Option<GraphSnapshot>,
    pub unrelated_staged_retained: Option<bool>,
    pub unrelated_dirty_retained: Option<bool>,
    pub oracle_within_one_percent: Option<bool>,
    pub exact_fixture_counts_passed: Option<bool>,
    pub exact_locality_counts_passed: Option<bool>,
    pub current_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Distribution {
    pub median: f64,
    pub p95: f64,
    pub mad: f64,
    pub max: f64,
}

const fn default_scalar_family() -> FixtureFamily {
    FixtureFamily::Scalar
}

const fn default_path_schema_version() -> u32 {
    2
}

pub fn distribution(values: &[f64]) -> Option<Distribution> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = percentile_sorted(&sorted, 0.5);
    let p95 = percentile_sorted(&sorted, 0.95);
    let mut deviations = sorted
        .iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    Some(Distribution {
        median,
        p95,
        mad: percentile_sorted(&deviations, 0.5),
        max: *sorted.last().expect("nonempty distribution"),
    })
}

fn percentile_sorted(sorted: &[f64], percentile: f64) -> f64 {
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

pub fn allowed_oracle(oracle: u32) -> u64 {
    u64::from(oracle).saturating_add(u64::from(oracle).div_ceil(100))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_uses_nearest_rank_and_median_absolute_deviation() {
        let stats = distribution(&[7.0, 1.0, 3.0, 2.0, 100.0, 5.0, 4.0]).unwrap();
        assert_eq!(stats.median, 4.0);
        assert_eq!(stats.p95, 100.0);
        assert_eq!(stats.mad, 2.0);
        assert_eq!(stats.max, 100.0);
    }

    #[test]
    fn target_scope_allocations_are_bounded() {
        let shape = FixtureShape::new(50_000).unwrap();
        assert!(shape.tiny_formulas < shape.formulas / 100);
        assert_eq!(shape.medium_formulas, shape.formulas / 10);
        assert_eq!(shape.oracle(TargetScope::Full), 50_000);
        assert_eq!(
            shape.tiny_formulas
                + shape.medium_formulas
                + shape.large_formulas
                + shape.dirty_formulas,
            shape.formulas
        );
    }

    #[test]
    fn supported_family_path_matrix_is_explicit() {
        let expected = [
            (
                FixtureFamily::Scalar,
                vec![
                    CalibrationPath::Full,
                    CalibrationPath::Cells,
                    CalibrationPath::Plan,
                    CalibrationPath::Sheetport,
                ],
            ),
            (
                FixtureFamily::CrossSheet,
                vec![
                    CalibrationPath::Full,
                    CalibrationPath::Cells,
                    CalibrationPath::Targets,
                    CalibrationPath::Plan,
                    CalibrationPath::Sheetport,
                ],
            ),
            (
                FixtureFamily::Names,
                vec![
                    CalibrationPath::Full,
                    CalibrationPath::Targets,
                    CalibrationPath::Plan,
                    CalibrationPath::Sheetport,
                ],
            ),
            (
                FixtureFamily::Layout,
                vec![CalibrationPath::Full, CalibrationPath::Sheetport],
            ),
            (
                FixtureFamily::NativeTable,
                vec![
                    CalibrationPath::Full,
                    CalibrationPath::Targets,
                    CalibrationPath::Plan,
                    CalibrationPath::Sheetport,
                ],
            ),
            (
                FixtureFamily::Dynamic,
                vec![
                    CalibrationPath::Full,
                    CalibrationPath::Cells,
                    CalibrationPath::Targets,
                    CalibrationPath::Plan,
                    CalibrationPath::Sheetport,
                ],
            ),
        ];
        for (family, supported) in expected {
            assert_eq!(
                CalibrationPath::ALL
                    .into_iter()
                    .filter(|path| path_supported(family, *path))
                    .collect::<Vec<_>>(),
                supported
            );
        }
    }

    #[test]
    fn analytical_oracle_covers_initial_and_repeated_edits() {
        let shape = FixtureShape::new(1_000).unwrap();
        let expected = analytical_expected_outputs(shape, TargetScope::Full, 2);
        assert_eq!(expected.len(), 3);
        assert_eq!(expected[0].len(), 4);
        assert_ne!(expected[0][0], expected[1][0]);
        assert_ne!(expected[1][0], expected[2][0]);
        assert_eq!(expected[0][1], expected[2][1]);
        assert_eq!(
            expected[0][3],
            analytical_terminal(40.0, shape.dirty_formulas)
        );
    }
}
