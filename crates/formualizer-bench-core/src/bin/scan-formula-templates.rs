use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use formualizer_bench_core::BenchmarkSuite;
use formualizer_eval::formula_plane::diagnostics::{
    FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic,
    FormulaPlaneDependencyComparisonDiagnostic, FormulaPlaneDependencyScanInput,
    FormulaPlaneDependencySummariesDiagnostic, FormulaPlaneTemplateDiagnostic,
    canonical_template_diagnostic, dependency_summaries_diagnostic,
};
use formualizer_eval::formula_plane::{
    CandidateRunOrientation, FormulaPlaneCandidateCell, FormulaRejectReason, FormulaRunShape,
    FormulaRunStore, FormulaRunStoreBuildReport, SpanGapKind, SpanPartitionCounterOptions,
    SpanPartitionCounters, TemplateSupportStatus, compute_span_partition_counters,
};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType, parse};
use serde::Serialize;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    workbook: Option<PathBuf>,
    #[arg(long, default_value = "benchmarks/scenarios.yaml")]
    scenarios: PathBuf,
    #[arg(long)]
    scenario: Option<String>,
    #[arg(long, default_value = ".")]
    root: PathBuf,
    #[arg(long, value_name = "PATH")]
    runner_json: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct RawFormula {
    sheet: String,
    cell: String,
    row: u32,
    col: u32,
    formula: String,
    shared: bool,
    shared_index: Option<String>,
    shared_ref: Option<String>,
}

#[derive(Debug, Clone)]
struct ScannedFormula {
    raw: RawFormula,
    template_id: String,
    canonical: String,
    labels: BTreeSet<String>,
    parse_ok: bool,
    ast: Option<ASTNode>,
    authority: Option<FormulaPlaneTemplateDiagnostic>,
}

#[derive(Debug, Serialize)]
struct AuthorityTemplateSummary {
    authority_template_key: String,
    authority_template_diagnostic_id: String,
    stable_hash_hex: String,
    formula_cell_count: u64,
    diagnostic_source_template_count: u64,
    representative_source_template_id: String,
    representative_cell: String,
    representative_formula: String,
    authority_supported: bool,
    flags: Vec<String>,
    reject_kinds: Vec<String>,
    reject_reasons: Vec<String>,
    representative_expression_debug: String,
}

#[derive(Debug, Serialize)]
struct AuthoritySourceTemplateMappingSummary {
    source_template_id: String,
    formula_cell_count: u64,
    unmapped_formula_cell_count: u64,
    authority_template_count: u64,
    authority_template_keys: Vec<String>,
    authority_template_diagnostic_ids: Vec<String>,
    ambiguous: bool,
}

#[derive(Debug, Serialize)]
struct AuthorityRunMappingSummary {
    run_id: u32,
    template_id: u32,
    source_template_id: String,
    authority_template_key: String,
    authority_template_diagnostic_id: String,
}

#[derive(Debug, Serialize)]
struct AuthorityRunUnmappedSummary {
    run_id: u32,
    template_id: u32,
    source_template_id: String,
    reason: &'static str,
    authority_template_keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct AuthorityTemplatesReport {
    authority_template_count: u64,
    diagnostic_source_template_count: u64,
    diagnostic_collision_count: u64,
    authority_supported_template_count: u64,
    authority_rejected_template_count: u64,
    parsed_formula_cell_count: u64,
    unmapped_formula_cell_count: u64,
    mapped_run_count: u64,
    ambiguous_run_count: u64,
    unmapped_run_count: u64,
    templates: Vec<AuthorityTemplateSummary>,
    source_template_mappings: Vec<AuthoritySourceTemplateMappingSummary>,
    run_mappings: Vec<AuthorityRunMappingSummary>,
    unmapped_runs_sample: Vec<AuthorityRunUnmappedSummary>,
}

#[derive(Debug, Serialize)]
struct TemplateSummary {
    template_id: String,
    canonical: String,
    cells: u64,
    first_cell: String,
    labels: Vec<String>,
    row_runs: u64,
    column_runs: u64,
    holes: u64,
    exceptions: u64,
    raw_formula_samples: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ScanTotals {
    formula_cells: u64,
    parse_ok: u64,
    parse_errors: u64,
    volatile_formula_cells: u64,
    dynamic_formula_cells: u64,
    unsupported_formula_cells: u64,
    shared_formula_tags: u64,
    shared_formula_anchor_tags: u64,
    shared_formula_indices: u64,
    templates: u64,
    repeated_templates: u64,
    repeated_template_cells: u64,
    row_runs: u64,
    column_runs: u64,
    holes: u64,
    exceptions: u64,
}

#[derive(Debug, Serialize)]
struct FormulaPlaneCandidateTemplateCounters {
    template_id: String,
    formula_cells: u64,
    row_run_count: u64,
    column_run_count: u64,
    max_run_length: u64,
    formula_cells_represented_by_runs: u64,
    singleton_formula_count: u64,
    hole_count: u64,
    exception_count: u64,
    candidate_partition_count: u64,
    candidate_formula_run_to_partition_edge_estimate: u64,
    max_partitions_touched_by_run: u64,
    dense_run_coverage_percent: f64,
}

#[derive(Debug, Serialize)]
struct FormulaPlaneCandidateRunSummary {
    template_id: String,
    sheet: String,
    orientation: &'static str,
    fixed_index: u32,
    start_index: u32,
    end_index: u32,
    len: u64,
    partitions_touched: u64,
}

#[derive(Debug, Serialize)]
struct FormulaPlaneCandidateCounters {
    row_block_size: u32,
    template_count: u64,
    repeated_template_count: u64,
    formula_cell_count: u64,
    parse_error_formula_count: u64,
    volatile_formula_count: u64,
    dynamic_formula_count: u64,
    unsupported_formula_count: u64,
    row_run_count: u64,
    column_run_count: u64,
    candidate_formula_run_count: u64,
    max_run_length: u64,
    formula_cells_represented_by_runs: u64,
    singleton_formula_count: u64,
    hole_count: u64,
    exception_count: u64,
    estimated_materialization_avoidable_cell_count: u64,
    candidate_row_block_partition_count: u64,
    candidate_formula_run_to_partition_edge_estimate: u64,
    max_partitions_touched_by_run: u64,
    dense_run_coverage_percent: f64,
    template_counters: Vec<FormulaPlaneCandidateTemplateCounters>,
    candidate_runs: Vec<FormulaPlaneCandidateRunSummary>,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreTemplateSummary {
    template_id: u32,
    source_template_id: String,
    formula_cell_count: u64,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreRunSummary {
    run_id: u32,
    template_id: u32,
    source_template_id: String,
    sheet: String,
    shape: &'static str,
    row_start: u32,
    col_start: u32,
    row_end: u32,
    col_end: u32,
    len: u64,
    row_block_start: u32,
    row_block_end: u32,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreGapSummary {
    template_id: u32,
    sheet: String,
    row: u32,
    col: u32,
    kind: &'static str,
    other_template_id: Option<u32>,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreRejectedCellSummary {
    sheet: String,
    row: u32,
    col: u32,
    source_template_id: String,
    reason: &'static str,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreReconciliationDeltaSummary {
    field: &'static str,
    fp2a_value: i64,
    span_store_value: i64,
    reason: &'static str,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreReconciliationSummary {
    matched: bool,
    deltas: Vec<FormulaRunStoreReconciliationDeltaSummary>,
}

#[derive(Debug, Serialize)]
struct FormulaRunStoreReportSummary {
    row_block_size: u32,
    template_count: u64,
    formula_cell_count: u64,
    supported_formula_cell_count: u64,
    rejected_formula_cell_count: u64,
    parse_error_formula_count: u64,
    unsupported_formula_count: u64,
    dynamic_formula_count: u64,
    volatile_formula_count: u64,
    run_count: u64,
    row_run_count: u64,
    column_run_count: u64,
    singleton_run_count: u64,
    formula_cells_represented_by_runs: u64,
    candidate_row_block_partition_count: u64,
    candidate_formula_run_to_partition_edge_estimate: u64,
    max_partitions_touched_by_run: u64,
    hole_count: u64,
    exception_count: u64,
    overlap_dropped_count: u64,
    rectangle_deferred_count: u64,
    gap_scan_truncated_count: u64,
    dense_run_coverage_percent: f64,
    compact_representation_denominator: u64,
    compact_representation_ratio: f64,
    reconciliation: FormulaRunStoreReconciliationSummary,
    templates: Vec<FormulaRunStoreTemplateSummary>,
    runs_sample: Vec<FormulaRunStoreRunSummary>,
    gaps_sample: Vec<FormulaRunStoreGapSummary>,
    rejected_cells_sample: Vec<FormulaRunStoreRejectedCellSummary>,
}

#[derive(Debug, Clone)]
struct GraphMaterializationStats {
    source: String,
    graph_formula_vertices: Option<u64>,
    formula_ast_roots: Option<u64>,
    formula_ast_nodes: Option<u64>,
    graph_edges: Option<u64>,
}

#[derive(Debug, Serialize)]
struct MaterializationAccounting {
    graph_stats_source: String,
    formula_cells: u64,
    graph_formula_vertices: Option<u64>,
    formula_ast_roots: Option<u64>,
    formula_ast_nodes: Option<u64>,
    graph_edges: Option<u64>,
    template_count: u64,
    run_count: u64,
    rejected_cell_count: u64,
    hole_count: u64,
    exception_count: u64,
    hole_exception_count: u64,
    dense_run_coverage_percent: f64,
    compact_representation_denominator: u64,
    compact_representation_ratio: f64,
    estimated_avoidable_formula_vertices: u64,
    estimated_avoidable_formula_vertices_basis: &'static str,
    estimated_avoidable_ast_roots: u64,
    estimated_avoidable_ast_roots_basis: &'static str,
    estimated_avoidable_graph_edges: Option<u64>,
    estimated_avoidable_graph_edges_basis: &'static str,
    runtime_win_claimed: bool,
}

#[derive(Debug, Serialize)]
struct DependencyCollectPolicyFingerprintSummary {
    expand_small_ranges: bool,
    range_expansion_limit: usize,
    include_names: bool,
}

#[derive(Debug, Serialize)]
struct DependencySummaryComparisonSummary {
    oracle_policy_name: &'static str,
    oracle_policy_fingerprint: DependencyCollectPolicyFingerprintSummary,
    requested_policy_fingerprint: DependencyCollectPolicyFingerprintSummary,
    exact_match_count: u64,
    over_approximation_count: u64,
    under_approximation_count: u64,
    rejection_count: u64,
    policy_drift_count: u64,
    fallback_reason_histogram: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
struct DependencySummariesReport {
    authority_template_count: u64,
    supported_template_count: u64,
    rejected_template_count: u64,
    run_summary_count: u64,
    precedent_region_count: u64,
    result_region_count: u64,
    reverse_summary_count: u64,
    comparison: DependencySummaryComparisonSummary,
    fallback_reasons: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
struct ScanOutput {
    workbook: String,
    totals: ScanTotals,
    formula_plane_candidates: FormulaPlaneCandidateCounters,
    formula_run_store: FormulaRunStoreReportSummary,
    authority_templates: AuthorityTemplatesReport,
    dependency_summaries: DependencySummariesReport,
    materialization_accounting: MaterializationAccounting,
    templates: Vec<TemplateSummary>,
}

impl From<SpanPartitionCounters> for FormulaPlaneCandidateCounters {
    fn from(counters: SpanPartitionCounters) -> Self {
        Self {
            row_block_size: counters.row_block_size,
            template_count: counters.template_count,
            repeated_template_count: counters.repeated_template_count,
            formula_cell_count: counters.formula_cell_count,
            parse_error_formula_count: counters.parse_error_formula_count,
            volatile_formula_count: counters.volatile_formula_count,
            dynamic_formula_count: counters.dynamic_formula_count,
            unsupported_formula_count: counters.unsupported_formula_count,
            row_run_count: counters.row_run_count,
            column_run_count: counters.column_run_count,
            candidate_formula_run_count: counters.candidate_formula_run_count,
            max_run_length: counters.max_run_length,
            formula_cells_represented_by_runs: counters.formula_cells_represented_by_runs,
            singleton_formula_count: counters.singleton_formula_count,
            hole_count: counters.hole_count,
            exception_count: counters.exception_count,
            estimated_materialization_avoidable_cell_count: counters
                .estimated_materialization_avoidable_cell_count,
            candidate_row_block_partition_count: counters.candidate_row_block_partition_count,
            candidate_formula_run_to_partition_edge_estimate: counters
                .candidate_formula_run_to_partition_edge_estimate,
            max_partitions_touched_by_run: counters.max_partitions_touched_by_run,
            dense_run_coverage_percent: counters.dense_run_coverage_percent,
            template_counters: counters
                .template_counters
                .into_iter()
                .map(|counter| FormulaPlaneCandidateTemplateCounters {
                    template_id: counter.template_id,
                    formula_cells: counter.formula_cells,
                    row_run_count: counter.row_run_count,
                    column_run_count: counter.column_run_count,
                    max_run_length: counter.max_run_length,
                    formula_cells_represented_by_runs: counter.formula_cells_represented_by_runs,
                    singleton_formula_count: counter.singleton_formula_count,
                    hole_count: counter.hole_count,
                    exception_count: counter.exception_count,
                    candidate_partition_count: counter.candidate_partition_count,
                    candidate_formula_run_to_partition_edge_estimate: counter
                        .candidate_formula_run_to_partition_edge_estimate,
                    max_partitions_touched_by_run: counter.max_partitions_touched_by_run,
                    dense_run_coverage_percent: counter.dense_run_coverage_percent,
                })
                .collect(),
            candidate_runs: counters
                .candidate_runs
                .into_iter()
                .map(|run| FormulaPlaneCandidateRunSummary {
                    template_id: run.template_id,
                    sheet: run.sheet,
                    orientation: match run.orientation {
                        CandidateRunOrientation::Row => "row",
                        CandidateRunOrientation::Column => "column",
                    },
                    fixed_index: run.fixed_index,
                    start_index: run.start_index,
                    end_index: run.end_index,
                    len: run.len,
                    partitions_touched: run.partitions_touched,
                })
                .collect(),
        }
    }
}

impl From<FormulaPlaneDependencySummariesDiagnostic> for DependencySummariesReport {
    fn from(diagnostic: FormulaPlaneDependencySummariesDiagnostic) -> Self {
        Self {
            authority_template_count: diagnostic.authority_template_count,
            supported_template_count: diagnostic.supported_template_count,
            rejected_template_count: diagnostic.rejected_template_count,
            run_summary_count: diagnostic.run_summary_count,
            precedent_region_count: diagnostic.precedent_region_count,
            result_region_count: diagnostic.result_region_count,
            reverse_summary_count: diagnostic.reverse_summary_count,
            comparison: diagnostic.comparison.into(),
            fallback_reasons: diagnostic.fallback_reasons,
        }
    }
}

impl From<FormulaPlaneDependencyComparisonDiagnostic> for DependencySummaryComparisonSummary {
    fn from(diagnostic: FormulaPlaneDependencyComparisonDiagnostic) -> Self {
        Self {
            oracle_policy_name: diagnostic.oracle_policy_name,
            oracle_policy_fingerprint: diagnostic.oracle_policy_fingerprint.into(),
            requested_policy_fingerprint: diagnostic.requested_policy_fingerprint.into(),
            exact_match_count: diagnostic.exact_match_count,
            over_approximation_count: diagnostic.over_approximation_count,
            under_approximation_count: diagnostic.under_approximation_count,
            rejection_count: diagnostic.rejection_count,
            policy_drift_count: diagnostic.policy_drift_count,
            fallback_reason_histogram: diagnostic.fallback_reason_histogram,
        }
    }
}

impl From<FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic>
    for DependencyCollectPolicyFingerprintSummary
{
    fn from(diagnostic: FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic) -> Self {
        Self {
            expand_small_ranges: diagnostic.expand_small_ranges,
            range_expansion_limit: diagnostic.range_expansion_limit,
            include_names: diagnostic.include_names,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let workbook = resolve_workbook(&cli)?;
    let raw = scan_ooxml_formulas(&workbook)?;
    let scanned = classify_formulas(raw);
    let graph_stats = match &cli.runner_json {
        Some(path) => Some(read_graph_materialization_stats(path)?),
        None => None,
    };
    let output = summarize(workbook, scanned, graph_stats)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn read_graph_materialization_stats(path: &Path) -> Result<GraphMaterializationStats> {
    let file = File::open(path).with_context(|| format!("open runner JSON {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_reader(file)
        .with_context(|| format!("parse runner JSON {}", path.display()))?;
    let extra = value
        .get("metrics")
        .and_then(|metrics| metrics.get("extra"))
        .unwrap_or(&serde_json::Value::Null);
    Ok(GraphMaterializationStats {
        source: path.display().to_string(),
        graph_formula_vertices: first_u64(
            extra,
            &[
                "load_graph_formula_vertex_count",
                "final_graph_formula_vertex_count",
            ],
        ),
        formula_ast_roots: first_u64(
            extra,
            &[
                "load_formula_ast_root_count",
                "final_formula_ast_root_count",
            ],
        ),
        formula_ast_nodes: first_u64(
            extra,
            &[
                "load_formula_ast_node_count",
                "final_formula_ast_node_count",
            ],
        ),
        graph_edges: first_u64(extra, &["load_graph_edge_count", "final_graph_edge_count"]),
    })
}

fn first_u64(value: &serde_json::Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_u64))
}

fn resolve_workbook(cli: &Cli) -> Result<PathBuf> {
    if let Some(path) = &cli.workbook {
        return Ok(path.clone());
    }
    let Some(scenario_id) = &cli.scenario else {
        bail!("provide --workbook <path> or --scenario <id>");
    };
    let suite = BenchmarkSuite::from_yaml_path(&cli.scenarios)
        .with_context(|| format!("load scenarios {}", cli.scenarios.display()))?;
    let scenario = suite
        .scenario(scenario_id)
        .with_context(|| format!("unknown scenario: {scenario_id}"))?;
    let p = PathBuf::from(&scenario.source.workbook_path);
    if p.is_absolute() {
        Ok(p)
    } else {
        Ok(cli.root.join(p))
    }
}

fn scan_ooxml_formulas(path: &Path) -> Result<Vec<RawFormula>> {
    let file = File::open(path).with_context(|| format!("open workbook {}", path.display()))?;
    let mut zip = zip::ZipArchive::new(file).context("open workbook zip")?;
    let sheet_names = workbook_sheet_names(&mut zip).unwrap_or_default();
    let rel_targets = workbook_relationship_targets(&mut zip).unwrap_or_default();
    let mut path_to_sheet = BTreeMap::new();
    for (rid, name) in sheet_names {
        if let Some(target) = rel_targets.get(&rid) {
            path_to_sheet.insert(normalize_target_path(target), name);
        }
    }

    let mut out = Vec::new();
    let mut entries = Vec::new();
    for i in 0..zip.len() {
        let name = zip.by_index(i)?.name().to_string();
        if name.starts_with("xl/worksheets/") && name.ends_with(".xml") {
            entries.push(name);
        }
    }
    entries.sort();

    for name in entries {
        let mut xml = String::new();
        zip.by_name(&name)?.read_to_string(&mut xml)?;
        let sheet = path_to_sheet.get(&name).cloned().unwrap_or(name.clone());
        scan_sheet_formulas(&sheet, &xml, &mut out);
    }
    Ok(out)
}

fn workbook_sheet_names<R: std::io::Read + std::io::Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<BTreeMap<String, String>> {
    let mut xml = String::new();
    zip.by_name("xl/workbook.xml")?.read_to_string(&mut xml)?;
    let mut out = BTreeMap::new();
    let mut pos = 0;
    while let Some(rel) = find_tag(&xml, "sheet", pos) {
        if let (Some(rid), Some(name)) = (attr(rel.tag, "r:id"), attr(rel.tag, "name")) {
            out.insert(rid, xml_unescape(&name));
        }
        pos = rel.end;
    }
    Ok(out)
}

fn workbook_relationship_targets<R: std::io::Read + std::io::Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<BTreeMap<String, String>> {
    let mut xml = String::new();
    zip.by_name("xl/_rels/workbook.xml.rels")?
        .read_to_string(&mut xml)?;
    let mut out = BTreeMap::new();
    let mut pos = 0;
    while let Some(rel) = find_tag(&xml, "Relationship", pos) {
        if let (Some(id), Some(target)) = (attr(rel.tag, "Id"), attr(rel.tag, "Target")) {
            out.insert(id, target);
        }
        pos = rel.end;
    }
    Ok(out)
}

fn normalize_target_path(target: &str) -> String {
    let target = target.trim_start_matches('/');
    if target.starts_with("xl/") {
        target.to_string()
    } else {
        format!("xl/{target}")
    }
}

struct TagRef<'a> {
    tag: &'a str,
    start: usize,
    end: usize,
}

fn find_tag<'a>(xml: &'a str, tag: &str, pos: usize) -> Option<TagRef<'a>> {
    let needle = format!("<{tag}");
    let rel_start = xml[pos..].find(&needle)?;
    let start = pos + rel_start;
    let after_name = start + needle.len();
    let next = xml.as_bytes().get(after_name).copied();
    if !matches!(next, Some(b' ') | Some(b'/') | Some(b'>')) {
        return find_tag(xml, tag, after_name);
    }
    let end = start + xml[start..].find('>')? + 1;
    Some(TagRef {
        tag: &xml[start..end],
        start,
        end,
    })
}

fn attr(tag: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=\"");
    if let Some(pos) = tag.find(&needle) {
        let start = pos + needle.len();
        let rest = &tag[start..];
        return rest.find('"').map(|end| rest[..end].to_string());
    }
    let needle = format!("{key}='");
    if let Some(pos) = tag.find(&needle) {
        let start = pos + needle.len();
        let rest = &tag[start..];
        return rest.find('\'').map(|end| rest[..end].to_string());
    }
    None
}

fn scan_sheet_formulas(sheet: &str, xml: &str, out: &mut Vec<RawFormula>) {
    let mut pos = 0;
    while let Some(f_tag) = find_tag(xml, "f", pos) {
        let content_start = f_tag.end;
        let Some(close_rel) = xml[content_start..].find("</f>") else {
            pos = f_tag.end;
            continue;
        };
        let content_end = content_start + close_rel;
        let formula = xml_unescape(&xml[content_start..content_end]);
        let cell_ref = preceding_cell_ref(xml, f_tag.start).unwrap_or_default();
        let (row, col) = parse_a1_cell(&cell_ref).unwrap_or((0, 0));
        let shared = attr(f_tag.tag, "t").as_deref() == Some("shared");
        out.push(RawFormula {
            sheet: sheet.to_string(),
            cell: cell_ref,
            row,
            col,
            formula,
            shared,
            shared_index: attr(f_tag.tag, "si"),
            shared_ref: attr(f_tag.tag, "ref"),
        });
        pos = content_end + 4;
    }
}

fn preceding_cell_ref(xml: &str, pos: usize) -> Option<String> {
    let cell_start = xml[..pos].rfind("<c ")?;
    let cell_end = cell_start + xml[cell_start..].find('>')? + 1;
    attr(&xml[cell_start..cell_end], "r")
}

fn xml_unescape(input: &str) -> String {
    input
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn parse_a1_cell(cell: &str) -> Option<(u32, u32)> {
    let mut col = 0u32;
    let mut row = 0u32;
    for ch in cell.chars() {
        if ch.is_ascii_alphabetic() {
            col = col * 26 + u32::from(ch.to_ascii_uppercase() as u8 - b'A' + 1);
        } else if ch.is_ascii_digit() {
            row = row * 10 + ch.to_digit(10)?;
        }
    }
    if row == 0 || col == 0 {
        None
    } else {
        Some((row, col))
    }
}

fn classify_formulas(raw: Vec<RawFormula>) -> Vec<ScannedFormula> {
    raw.into_iter()
        .map(|raw| {
            let mut labels = BTreeSet::new();
            if raw.shared {
                labels.insert("raw_ooxml_shared_formula".to_string());
                if raw.shared_ref.is_some() {
                    labels.insert("raw_ooxml_shared_anchor".to_string());
                }
            }
            let with_eq = if raw.formula.starts_with('=') {
                raw.formula.clone()
            } else {
                format!("={}", raw.formula)
            };
            match parse(&with_eq) {
                Ok(ast) => {
                    if ast.contains_volatile() {
                        labels.insert("volatile".to_string());
                    }
                    let authority = canonical_template_diagnostic(&ast, raw.row, raw.col);
                    let canonical = canonical_ast(&ast, raw.row, raw.col, &mut labels);
                    let template_id = stable_id(&canonical, &labels);
                    ScannedFormula {
                        raw,
                        template_id,
                        canonical,
                        labels,
                        parse_ok: true,
                        ast: Some(ast),
                        authority: Some(authority),
                    }
                }
                Err(err) => {
                    labels.insert("unsupported_parse_error".to_string());
                    let canonical = format!("PARSE_ERROR:{}", err.to_string().replace('\n', " "));
                    let template_id = stable_id(&canonical, &labels);
                    ScannedFormula {
                        raw,
                        template_id,
                        canonical,
                        labels,
                        parse_ok: false,
                        ast: None,
                        authority: None,
                    }
                }
            }
        })
        .collect()
}

fn canonical_ast(
    ast: &ASTNode,
    anchor_row: u32,
    anchor_col: u32,
    labels: &mut BTreeSet<String>,
) -> String {
    match &ast.node_type {
        ASTNodeType::Literal(value) => format!("LIT:{:?}", value_kind(value)),
        ASTNodeType::Reference { reference, .. } => {
            canonical_reference(reference, anchor_row, anchor_col, labels)
        }
        ASTNodeType::UnaryOp { op, expr } => {
            format!(
                "UNARY({op},{})",
                canonical_ast(expr, anchor_row, anchor_col, labels)
            )
        }
        ASTNodeType::BinaryOp { op, left, right } => format!(
            "BIN({op},{},{})",
            canonical_ast(left, anchor_row, anchor_col, labels),
            canonical_ast(right, anchor_row, anchor_col, labels)
        ),
        ASTNodeType::Function { name, args } => {
            let upper = name.to_ascii_uppercase();
            if matches!(upper.as_str(), "OFFSET" | "INDIRECT") {
                labels.insert("dynamic_reference".to_string());
            }
            if matches!(upper.as_str(), "NOW" | "TODAY" | "RAND" | "RANDBETWEEN") {
                labels.insert("volatile".to_string());
            }
            let args = args
                .iter()
                .map(|arg| canonical_ast(arg, anchor_row, anchor_col, labels))
                .collect::<Vec<_>>()
                .join(",");
            format!("FN({upper},{args})")
        }
        ASTNodeType::Call { callee, args } => {
            labels.insert("dynamic_call".to_string());
            let args = args
                .iter()
                .map(|arg| canonical_ast(arg, anchor_row, anchor_col, labels))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "CALL({},{args})",
                canonical_ast(callee, anchor_row, anchor_col, labels)
            )
        }
        ASTNodeType::Array(rows) => {
            let rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|arg| canonical_ast(arg, anchor_row, anchor_col, labels))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect::<Vec<_>>()
                .join(";");
            format!("ARRAY({rows})")
        }
    }
}

fn value_kind(value: &formualizer_common::LiteralValue) -> &'static str {
    match value {
        formualizer_common::LiteralValue::Number(_) => "number",
        formualizer_common::LiteralValue::Int(_) => "int",
        formualizer_common::LiteralValue::Text(_) => "text",
        formualizer_common::LiteralValue::Boolean(_) => "bool",
        formualizer_common::LiteralValue::Error(_) => "error",
        formualizer_common::LiteralValue::Empty => "empty",
        formualizer_common::LiteralValue::Array(_) => "array",
        formualizer_common::LiteralValue::Date(_) => "date",
        formualizer_common::LiteralValue::DateTime(_) => "datetime",
        formualizer_common::LiteralValue::Time(_) => "time",
        formualizer_common::LiteralValue::Duration(_) => "duration",
        formualizer_common::LiteralValue::Pending => "pending",
    }
}

fn canonical_reference(
    reference: &ReferenceType,
    anchor_row: u32,
    anchor_col: u32,
    labels: &mut BTreeSet<String>,
) -> String {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => format!(
            "REF({}{},{})",
            sheet_prefix(sheet.as_deref()),
            coord_part("R", *row, anchor_row, *row_abs),
            coord_part("C", *col, anchor_col, *col_abs)
        ),
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => format!(
            "RANGE({}{}:{};{}:{})",
            sheet_prefix(sheet.as_deref()),
            opt_coord_part("R", *start_row, anchor_row, *start_row_abs),
            opt_coord_part("C", *start_col, anchor_col, *start_col_abs),
            opt_coord_part("R", *end_row, anchor_row, *end_row_abs),
            opt_coord_part("C", *end_col, anchor_col, *end_col_abs)
        ),
        ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {
            labels.insert("unsupported_3d_reference".to_string());
            format!("UNSUPPORTED_REF:{reference:?}")
        }
        ReferenceType::External(_) => {
            labels.insert("unsupported_external_reference".to_string());
            format!("UNSUPPORTED_REF:{reference:?}")
        }
        ReferenceType::Table(_) => {
            labels.insert("unsupported_structured_reference".to_string());
            format!("UNSUPPORTED_REF:{reference:?}")
        }
        ReferenceType::NamedRange(name) => {
            labels.insert("named_reference".to_string());
            format!("NAME({})", name.to_ascii_uppercase())
        }
    }
}

fn sheet_prefix(sheet: Option<&str>) -> String {
    sheet
        .map(|s| format!("SHEET({})!", s.to_ascii_uppercase()))
        .unwrap_or_default()
}

fn coord_part(prefix: &str, value: u32, anchor: u32, absolute: bool) -> String {
    if absolute {
        format!("{prefix}${value}")
    } else {
        let delta = i64::from(value) - i64::from(anchor);
        format!("{prefix}{delta:+}")
    }
}

fn opt_coord_part(prefix: &str, value: Option<u32>, anchor: u32, absolute: bool) -> String {
    value
        .map(|v| coord_part(prefix, v, anchor, absolute))
        .unwrap_or_else(|| format!("{prefix}*"))
}

fn stable_id(canonical: &str, labels: &BTreeSet<String>) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in canonical
        .bytes()
        .chain(labels.iter().flat_map(|s| s.bytes()))
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("tpl_{hash:016x}")
}

fn summarize(
    workbook: PathBuf,
    scanned: Vec<ScannedFormula>,
    graph_stats: Option<GraphMaterializationStats>,
) -> Result<ScanOutput> {
    let mut by_template: BTreeMap<String, Vec<ScannedFormula>> = BTreeMap::new();
    let mut cell_to_template = BTreeMap::new();
    let mut candidate_cells = Vec::new();
    for formula in scanned {
        cell_to_template.insert(
            (formula.raw.sheet.clone(), formula.raw.row, formula.raw.col),
            formula.template_id.clone(),
        );
        candidate_cells.push(FormulaPlaneCandidateCell {
            sheet: formula.raw.sheet.clone(),
            row: formula.raw.row,
            col: formula.raw.col,
            template_id: formula.template_id.clone(),
            parse_ok: formula.parse_ok,
            volatile: formula.labels.contains("volatile"),
            dynamic: formula
                .labels
                .iter()
                .any(|label| label.starts_with("dynamic_")),
            unsupported: formula
                .labels
                .iter()
                .any(|label| label.starts_with("unsupported_")),
        });
        by_template
            .entry(formula.template_id.clone())
            .or_default()
            .push(formula);
    }
    let formula_plane_candidates =
        compute_span_partition_counters(&candidate_cells, SpanPartitionCounterOptions::default())
            .into();
    let formula_run_store_raw = FormulaRunStore::build(&candidate_cells);
    let formula_run_store = summarize_formula_run_store(&formula_run_store_raw);
    let authority_templates = summarize_authority_templates(&by_template, &formula_run_store_raw);
    let dependency_summaries =
        summarize_dependency_summaries(&by_template, &formula_run_store_raw)?;
    let materialization_accounting =
        summarize_materialization_accounting(&formula_run_store_raw, graph_stats.as_ref());

    let mut templates = Vec::new();
    let mut totals = ScanTotals {
        formula_cells: 0,
        parse_ok: 0,
        parse_errors: 0,
        volatile_formula_cells: 0,
        dynamic_formula_cells: 0,
        unsupported_formula_cells: 0,
        shared_formula_tags: 0,
        shared_formula_anchor_tags: 0,
        shared_formula_indices: 0,
        templates: by_template.len() as u64,
        repeated_templates: 0,
        repeated_template_cells: 0,
        row_runs: 0,
        column_runs: 0,
        holes: 0,
        exceptions: 0,
    };
    let mut shared_indices = BTreeSet::new();

    for (template_id, mut formulas) in by_template {
        formulas.sort_by(|a, b| {
            (&a.raw.sheet, a.raw.row, a.raw.col).cmp(&(&b.raw.sheet, b.raw.row, b.raw.col))
        });
        totals.formula_cells += formulas.len() as u64;
        totals.parse_ok += formulas.iter().filter(|f| f.parse_ok).count() as u64;
        totals.parse_errors += formulas.iter().filter(|f| !f.parse_ok).count() as u64;
        totals.volatile_formula_cells += formulas
            .iter()
            .filter(|f| f.labels.contains("volatile"))
            .count() as u64;
        totals.dynamic_formula_cells += formulas
            .iter()
            .filter(|f| f.labels.iter().any(|label| label.starts_with("dynamic_")))
            .count() as u64;
        totals.unsupported_formula_cells += formulas
            .iter()
            .filter(|f| {
                f.labels
                    .iter()
                    .any(|label| label.starts_with("unsupported_"))
            })
            .count() as u64;
        totals.shared_formula_tags += formulas.iter().filter(|f| f.raw.shared).count() as u64;
        totals.shared_formula_anchor_tags += formulas
            .iter()
            .filter(|f| f.raw.shared && f.raw.shared_ref.is_some())
            .count() as u64;
        for formula in &formulas {
            if let Some(index) = &formula.raw.shared_index {
                shared_indices.insert(index.clone());
            }
        }
        if formulas.len() > 1 {
            totals.repeated_templates += 1;
            totals.repeated_template_cells += formulas.len() as u64;
        }

        let labels = formulas
            .iter()
            .flat_map(|f| f.labels.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let canonical = formulas
            .first()
            .map(|f| f.canonical.clone())
            .unwrap_or_default();
        let first_cell = formulas
            .first()
            .map(|f| format!("{}!{}", f.raw.sheet, f.raw.cell))
            .unwrap_or_default();
        let raw_formula_samples = formulas
            .iter()
            .map(|f| f.raw.formula.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        let (row_runs, column_runs, holes, exceptions) =
            run_stats(&template_id, &formulas, &cell_to_template);
        totals.row_runs += row_runs;
        totals.column_runs += column_runs;
        totals.holes += holes;
        totals.exceptions += exceptions;
        templates.push(TemplateSummary {
            template_id,
            canonical,
            cells: formulas.len() as u64,
            first_cell,
            labels,
            row_runs,
            column_runs,
            holes,
            exceptions,
            raw_formula_samples,
        });
    }
    totals.shared_formula_indices = shared_indices.len() as u64;

    Ok(ScanOutput {
        workbook: workbook.display().to_string(),
        totals,
        formula_plane_candidates,
        formula_run_store,
        authority_templates,
        dependency_summaries,
        materialization_accounting,
        templates,
    })
}

fn summarize_dependency_summaries(
    by_template: &BTreeMap<String, Vec<ScannedFormula>>,
    store: &FormulaRunStore,
) -> Result<DependencySummariesReport> {
    let mut inputs = Vec::new();
    for (source_template_id, formulas) in by_template {
        for formula in formulas {
            let (Some(ast), Some(authority)) = (&formula.ast, &formula.authority) else {
                continue;
            };
            inputs.push(FormulaPlaneDependencyScanInput {
                source_template_id,
                authority_template_key: &authority.key_payload,
                sheet: &formula.raw.sheet,
                row: formula.raw.row,
                col: formula.raw.col,
                ast,
            });
        }
    }

    Ok(dependency_summaries_diagnostic(store, inputs)?.into())
}

#[derive(Debug, Clone)]
struct AuthorityTemplateAggregate {
    diagnostic: FormulaPlaneTemplateDiagnostic,
    formula_cell_count: u64,
    source_template_ids: BTreeSet<String>,
    representative_source_template_id: String,
    representative_cell: String,
    representative_formula: String,
}

fn summarize_authority_templates(
    by_template: &BTreeMap<String, Vec<ScannedFormula>>,
    store: &FormulaRunStore,
) -> AuthorityTemplatesReport {
    let mut by_authority_key = BTreeMap::<String, AuthorityTemplateAggregate>::new();
    let mut source_template_formula_counts = BTreeMap::<String, u64>::new();
    let mut source_template_unmapped_counts = BTreeMap::<String, u64>::new();
    let mut source_to_authority_keys = BTreeMap::<String, BTreeSet<String>>::new();
    let mut parsed_formula_cell_count = 0u64;
    let mut unmapped_formula_cell_count = 0u64;

    for (source_template_id, formulas) in by_template {
        let mut formulas = formulas.iter().collect::<Vec<_>>();
        formulas.sort_by(|a, b| compare_scanned_formula_cell(a, b));
        source_template_formula_counts.insert(source_template_id.clone(), formulas.len() as u64);

        for formula in formulas {
            let Some(authority) = &formula.authority else {
                unmapped_formula_cell_count += 1;
                *source_template_unmapped_counts
                    .entry(source_template_id.clone())
                    .or_default() += 1;
                continue;
            };

            parsed_formula_cell_count += 1;
            source_to_authority_keys
                .entry(source_template_id.clone())
                .or_default()
                .insert(authority.key_payload.clone());

            by_authority_key
                .entry(authority.key_payload.clone())
                .and_modify(|aggregate| {
                    aggregate.formula_cell_count += 1;
                    aggregate
                        .source_template_ids
                        .insert(source_template_id.clone());
                })
                .or_insert_with(|| {
                    let mut source_template_ids = BTreeSet::new();
                    source_template_ids.insert(source_template_id.clone());
                    AuthorityTemplateAggregate {
                        diagnostic: authority.clone(),
                        formula_cell_count: 1,
                        source_template_ids,
                        representative_source_template_id: source_template_id.clone(),
                        representative_cell: format!("{}!{}", formula.raw.sheet, formula.raw.cell),
                        representative_formula: formula.raw.formula.clone(),
                    }
                });
        }
    }

    let mut source_template_mappings = Vec::new();
    for (source_template_id, formula_cell_count) in source_template_formula_counts {
        let authority_template_keys = source_to_authority_keys
            .get(&source_template_id)
            .map(|keys| keys.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let authority_template_diagnostic_ids = authority_template_keys
            .iter()
            .filter_map(|key| by_authority_key.get(key))
            .map(|aggregate| aggregate.diagnostic.diagnostic_id.clone())
            .collect::<Vec<_>>();
        let authority_template_count = authority_template_keys.len() as u64;
        source_template_mappings.push(AuthoritySourceTemplateMappingSummary {
            source_template_id: source_template_id.clone(),
            formula_cell_count,
            unmapped_formula_cell_count: source_template_unmapped_counts
                .get(&source_template_id)
                .copied()
                .unwrap_or(0),
            authority_template_count,
            authority_template_keys,
            authority_template_diagnostic_ids,
            ambiguous: authority_template_count > 1,
        });
    }

    let diagnostic_collision_count = source_template_mappings
        .iter()
        .filter(|mapping| mapping.ambiguous)
        .count() as u64;
    let authority_supported_template_count = by_authority_key
        .values()
        .filter(|aggregate| aggregate.diagnostic.authority_supported)
        .count() as u64;
    let authority_rejected_template_count = by_authority_key
        .len()
        .saturating_sub(authority_supported_template_count as usize)
        as u64;

    let templates = by_authority_key
        .iter()
        .map(|(key, aggregate)| AuthorityTemplateSummary {
            authority_template_key: key.clone(),
            authority_template_diagnostic_id: aggregate.diagnostic.diagnostic_id.clone(),
            stable_hash_hex: stable_hash_hex(aggregate.diagnostic.stable_hash),
            formula_cell_count: aggregate.formula_cell_count,
            diagnostic_source_template_count: aggregate.source_template_ids.len() as u64,
            representative_source_template_id: aggregate.representative_source_template_id.clone(),
            representative_cell: aggregate.representative_cell.clone(),
            representative_formula: aggregate.representative_formula.clone(),
            authority_supported: aggregate.diagnostic.authority_supported,
            flags: aggregate.diagnostic.flags.clone(),
            reject_kinds: aggregate.diagnostic.reject_kinds.clone(),
            reject_reasons: aggregate.diagnostic.reject_reasons.clone(),
            representative_expression_debug: aggregate.diagnostic.expression_debug.clone(),
        })
        .collect::<Vec<_>>();

    let mut run_mappings = Vec::new();
    let mut ambiguous_run_count = 0u64;
    let mut unmapped_run_count = 0u64;
    let mut unmapped_runs_sample = Vec::new();

    for run in &store.runs {
        match source_to_authority_keys.get(&run.source_template_id) {
            Some(keys) if keys.len() == 1 => {
                let key = keys.iter().next().expect("one key present");
                if let Some(aggregate) = by_authority_key.get(key) {
                    run_mappings.push(AuthorityRunMappingSummary {
                        run_id: run.id.0,
                        template_id: run.template_id.0,
                        source_template_id: run.source_template_id.clone(),
                        authority_template_key: key.clone(),
                        authority_template_diagnostic_id: aggregate
                            .diagnostic
                            .diagnostic_id
                            .clone(),
                    });
                } else {
                    unmapped_run_count += 1;
                    push_unmapped_authority_run_sample(
                        &mut unmapped_runs_sample,
                        run,
                        "authority_template_missing",
                        Vec::new(),
                    );
                }
            }
            Some(keys) if keys.len() > 1 => {
                ambiguous_run_count += 1;
                push_unmapped_authority_run_sample(
                    &mut unmapped_runs_sample,
                    run,
                    "diagnostic_source_template_collision",
                    keys.iter().cloned().collect(),
                );
            }
            _ => {
                unmapped_run_count += 1;
                push_unmapped_authority_run_sample(
                    &mut unmapped_runs_sample,
                    run,
                    "no_authority_template_for_source",
                    Vec::new(),
                );
            }
        }
    }

    AuthorityTemplatesReport {
        authority_template_count: templates.len() as u64,
        diagnostic_source_template_count: by_template.len() as u64,
        diagnostic_collision_count,
        authority_supported_template_count,
        authority_rejected_template_count,
        parsed_formula_cell_count,
        unmapped_formula_cell_count,
        mapped_run_count: run_mappings.len() as u64,
        ambiguous_run_count,
        unmapped_run_count,
        templates,
        source_template_mappings,
        run_mappings,
        unmapped_runs_sample,
    }
}

fn compare_scanned_formula_cell(a: &ScannedFormula, b: &ScannedFormula) -> std::cmp::Ordering {
    (&a.raw.sheet, a.raw.row, a.raw.col, &a.raw.formula).cmp(&(
        &b.raw.sheet,
        b.raw.row,
        b.raw.col,
        &b.raw.formula,
    ))
}

fn push_unmapped_authority_run_sample(
    samples: &mut Vec<AuthorityRunUnmappedSummary>,
    run: &formualizer_eval::formula_plane::FormulaRunDescriptor,
    reason: &'static str,
    authority_template_keys: Vec<String>,
) {
    if samples.len() >= 20 {
        return;
    }
    samples.push(AuthorityRunUnmappedSummary {
        run_id: run.id.0,
        template_id: run.template_id.0,
        source_template_id: run.source_template_id.clone(),
        reason,
        authority_template_keys,
    });
}

fn stable_hash_hex(hash: u64) -> String {
    format!("{hash:016x}")
}

fn summarize_formula_run_store(store: &FormulaRunStore) -> FormulaRunStoreReportSummary {
    let report = &store.report;
    FormulaRunStoreReportSummary {
        row_block_size: store.row_block_size,
        template_count: report.template_count,
        formula_cell_count: report.formula_cell_count,
        supported_formula_cell_count: report.supported_formula_cell_count,
        rejected_formula_cell_count: report.rejected_formula_cell_count,
        parse_error_formula_count: report.parse_error_formula_count,
        unsupported_formula_count: report.unsupported_formula_count,
        dynamic_formula_count: report.dynamic_formula_count,
        volatile_formula_count: report.volatile_formula_count,
        run_count: store.runs.len() as u64,
        row_run_count: report.row_run_count,
        column_run_count: report.column_run_count,
        singleton_run_count: report.singleton_run_count,
        formula_cells_represented_by_runs: report.formula_cells_represented_by_runs,
        candidate_row_block_partition_count: report.candidate_row_block_partition_count,
        candidate_formula_run_to_partition_edge_estimate: report
            .candidate_formula_run_to_partition_edge_estimate,
        max_partitions_touched_by_run: report.max_partitions_touched_by_run,
        hole_count: report.hole_count,
        exception_count: report.exception_count,
        overlap_dropped_count: report.overlap_dropped_count,
        rectangle_deferred_count: report.rectangle_deferred_count,
        gap_scan_truncated_count: report.gap_scan_truncated_count,
        dense_run_coverage_percent: dense_run_coverage_percent(report),
        compact_representation_denominator: compact_representation_denominator(
            report,
            store.runs.len() as u64,
        ),
        compact_representation_ratio: compact_representation_ratio(report, store.runs.len() as u64),
        reconciliation: FormulaRunStoreReconciliationSummary {
            matched: report.reconciliation.matched,
            deltas: report
                .reconciliation
                .deltas
                .iter()
                .map(|delta| FormulaRunStoreReconciliationDeltaSummary {
                    field: delta.field,
                    fp2a_value: delta.fp2a_value,
                    span_store_value: delta.span_store_value,
                    reason: delta.reason,
                })
                .collect(),
        },
        templates: store
            .arena
            .templates
            .iter()
            .map(|template| FormulaRunStoreTemplateSummary {
                template_id: template.id.0,
                source_template_id: template.source_template_id.clone(),
                formula_cell_count: template.formula_cell_count,
                status: template_status_label(template.status),
            })
            .collect(),
        runs_sample: store
            .runs
            .iter()
            .take(20)
            .map(|run| FormulaRunStoreRunSummary {
                run_id: run.id.0,
                template_id: run.template_id.0,
                source_template_id: run.source_template_id.clone(),
                sheet: run.sheet.clone(),
                shape: run_shape_label(run.shape),
                row_start: run.row_start,
                col_start: run.col_start,
                row_end: run.row_end,
                col_end: run.col_end,
                len: run.len,
                row_block_start: run.row_block_start,
                row_block_end: run.row_block_end,
            })
            .collect(),
        gaps_sample: store
            .gaps
            .iter()
            .take(20)
            .map(|gap| match gap.kind {
                SpanGapKind::Hole => FormulaRunStoreGapSummary {
                    template_id: gap.template_id.0,
                    sheet: gap.sheet.clone(),
                    row: gap.row,
                    col: gap.col,
                    kind: "hole",
                    other_template_id: None,
                },
                SpanGapKind::Exception { other_template_id } => FormulaRunStoreGapSummary {
                    template_id: gap.template_id.0,
                    sheet: gap.sheet.clone(),
                    row: gap.row,
                    col: gap.col,
                    kind: "exception",
                    other_template_id: Some(other_template_id.0),
                },
            })
            .collect(),
        rejected_cells_sample: store
            .rejected_cells
            .iter()
            .take(20)
            .map(|cell| FormulaRunStoreRejectedCellSummary {
                sheet: cell.sheet.clone(),
                row: cell.row,
                col: cell.col,
                source_template_id: cell.source_template_id.clone(),
                reason: reject_reason_label(cell.reason),
            })
            .collect(),
    }
}

fn summarize_materialization_accounting(
    store: &FormulaRunStore,
    graph_stats: Option<&GraphMaterializationStats>,
) -> MaterializationAccounting {
    let report = &store.report;
    let run_count = store.runs.len() as u64;
    let compact_denominator = compact_representation_denominator(report, run_count);
    let compact_formula_vertex_proxy = run_count
        .saturating_add(report.exception_count)
        .saturating_add(report.rejected_formula_cell_count);
    let graph_formula_vertices = graph_stats.and_then(|stats| stats.graph_formula_vertices);
    let formula_ast_roots = graph_stats.and_then(|stats| stats.formula_ast_roots);
    let formula_ast_nodes = graph_stats.and_then(|stats| stats.formula_ast_nodes);
    let graph_edges = graph_stats.and_then(|stats| stats.graph_edges);
    let estimated_avoidable_formula_vertices = graph_formula_vertices
        .unwrap_or(report.formula_cell_count)
        .saturating_sub(compact_formula_vertex_proxy);
    let estimated_avoidable_ast_roots = formula_ast_roots
        .unwrap_or(report.formula_cell_count)
        .saturating_sub(
            report
                .template_count
                .saturating_add(report.rejected_formula_cell_count),
        );
    let estimated_avoidable_graph_edges = graph_edges.map(|edges| {
        edges.min(
            report
                .formula_cells_represented_by_runs
                .saturating_sub(run_count),
        )
    });

    MaterializationAccounting {
        graph_stats_source: graph_stats
            .map(|stats| stats.source.clone())
            .unwrap_or_else(|| "not_provided_by_scanner".to_string()),
        formula_cells: report.formula_cell_count,
        graph_formula_vertices,
        formula_ast_roots,
        formula_ast_nodes,
        graph_edges,
        template_count: report.template_count,
        run_count,
        rejected_cell_count: report.rejected_formula_cell_count,
        hole_count: report.hole_count,
        exception_count: report.exception_count,
        hole_exception_count: report.hole_count + report.exception_count,
        dense_run_coverage_percent: dense_run_coverage_percent(report),
        compact_representation_denominator: compact_denominator,
        compact_representation_ratio: compact_representation_ratio(report, run_count),
        estimated_avoidable_formula_vertices,
        estimated_avoidable_formula_vertices_basis: if graph_formula_vertices.is_some() {
            "runner_graph_formula_vertices_minus_compact_run_exception_rejected_proxy"
        } else {
            "scanner_formula_cells_minus_compact_run_exception_rejected_proxy"
        },
        estimated_avoidable_ast_roots,
        estimated_avoidable_ast_roots_basis: if formula_ast_roots.is_some() {
            "runner_ast_roots_minus_template_and_rejected_roots_proxy"
        } else {
            "scanner_formula_cells_minus_template_and_rejected_roots_proxy"
        },
        estimated_avoidable_graph_edges,
        estimated_avoidable_graph_edges_basis: if graph_edges.is_some() {
            "rough_min_runner_graph_edges_and_dense_run_cell_savings"
        } else {
            "not_estimated_without_runner_graph_edges"
        },
        runtime_win_claimed: false,
    }
}

fn dense_run_coverage_percent(report: &FormulaRunStoreBuildReport) -> f64 {
    if report.formula_cell_count == 0 {
        0.0
    } else {
        report
            .formula_cells_represented_by_runs
            .saturating_sub(report.singleton_run_count) as f64
            / report.formula_cell_count as f64
            * 100.0
    }
}

fn compact_representation_denominator(report: &FormulaRunStoreBuildReport, run_count: u64) -> u64 {
    run_count
        .saturating_add(report.template_count)
        .saturating_add(report.exception_count)
        .saturating_add(report.rejected_formula_cell_count)
        .max(1)
}

fn compact_representation_ratio(report: &FormulaRunStoreBuildReport, run_count: u64) -> f64 {
    report.formula_cell_count as f64 / compact_representation_denominator(report, run_count) as f64
}

fn template_status_label(status: TemplateSupportStatus) -> &'static str {
    match status {
        TemplateSupportStatus::Supported => "supported",
        TemplateSupportStatus::ParseError => "parse_error",
        TemplateSupportStatus::Unsupported => "unsupported",
        TemplateSupportStatus::Dynamic => "dynamic",
        TemplateSupportStatus::Volatile => "volatile",
        TemplateSupportStatus::Mixed => "mixed",
    }
}

fn run_shape_label(shape: FormulaRunShape) -> &'static str {
    match shape {
        FormulaRunShape::Row => "row",
        FormulaRunShape::Column => "column",
        FormulaRunShape::Singleton => "singleton",
    }
}

fn reject_reason_label(reason: FormulaRejectReason) -> &'static str {
    match reason {
        FormulaRejectReason::ParseError => "parse_error",
        FormulaRejectReason::Unsupported => "unsupported",
        FormulaRejectReason::Dynamic => "dynamic",
        FormulaRejectReason::Volatile => "volatile",
    }
}

fn run_stats(
    template_id: &str,
    formulas: &[ScannedFormula],
    cell_to_template: &BTreeMap<(String, u32, u32), String>,
) -> (u64, u64, u64, u64) {
    let mut by_row: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    let mut by_col: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    for formula in formulas {
        by_row
            .entry((formula.raw.sheet.clone(), formula.raw.row))
            .or_default()
            .push(formula.raw.col);
        by_col
            .entry((formula.raw.sheet.clone(), formula.raw.col))
            .or_default()
            .push(formula.raw.row);
    }
    let (row_runs, row_holes, row_exceptions) =
        count_runs(by_row, template_id, cell_to_template, true);
    let (col_runs, col_holes, col_exceptions) =
        count_runs(by_col, template_id, cell_to_template, false);
    (
        row_runs,
        col_runs,
        row_holes + col_holes,
        row_exceptions + col_exceptions,
    )
}

fn count_runs(
    groups: BTreeMap<(String, u32), Vec<u32>>,
    template_id: &str,
    cell_to_template: &BTreeMap<(String, u32, u32), String>,
    row_major: bool,
) -> (u64, u64, u64) {
    let mut runs = 0u64;
    let mut holes = 0u64;
    let mut exceptions = 0u64;
    for ((sheet, fixed), mut values) in groups {
        values.sort_unstable();
        values.dedup();
        let mut current_len = 0u64;
        let mut prev = None;
        for value in &values {
            if prev.is_none_or(|p| *value == p + 1) {
                current_len += 1;
            } else {
                if current_len > 1 {
                    runs += 1;
                }
                current_len = 1;
            }
            prev = Some(*value);
        }
        if current_len > 1 {
            runs += 1;
        }
        let Some(min) = values.first().copied() else {
            continue;
        };
        let Some(max) = values.last().copied() else {
            continue;
        };
        let present = values.into_iter().collect::<BTreeSet<_>>();
        for value in min..=max {
            let key = if row_major {
                (sheet.clone(), fixed, value)
            } else {
                (sheet.clone(), value, fixed)
            };
            if present.contains(&value) {
                continue;
            }
            match cell_to_template.get(&key) {
                Some(other) if other != template_id => exceptions += 1,
                Some(_) => {}
                None => holes += 1,
            }
        }
    }
    (runs, holes, exceptions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_formula(cell: &str, row: u32, col: u32, formula: &str) -> RawFormula {
        RawFormula {
            sheet: "Sheet1".to_string(),
            cell: cell.to_string(),
            row,
            col,
            formula: formula.to_string(),
            shared: false,
            shared_index: None,
            shared_ref: None,
        }
    }

    #[test]
    fn authority_sidecar_reports_diagnostic_source_collision() {
        let scanned = classify_formulas(vec![
            raw_formula("B1", 1, 2, "A1+1"),
            raw_formula("C1", 1, 3, "B1+2"),
        ]);
        assert_eq!(scanned[0].template_id, scanned[1].template_id);

        let output = summarize(PathBuf::from("collision.xlsx"), scanned, None)
            .expect("summarize collision workbook");

        assert_eq!(output.authority_templates.authority_template_count, 2);
        assert_eq!(
            output.authority_templates.diagnostic_source_template_count,
            1
        );
        assert_eq!(output.authority_templates.diagnostic_collision_count, 1);
        assert_eq!(output.authority_templates.source_template_mappings.len(), 1);
        assert_eq!(
            output.authority_templates.source_template_mappings[0].authority_template_count,
            2
        );
        assert!(output.authority_templates.source_template_mappings[0].ambiguous);
        assert_eq!(output.authority_templates.mapped_run_count, 0);
        assert_eq!(output.authority_templates.ambiguous_run_count, 1);
        assert_eq!(output.authority_templates.unmapped_run_count, 0);
        assert!(output.authority_templates.run_mappings.is_empty());
        assert_eq!(output.dependency_summaries.authority_template_count, 2);
        assert_eq!(output.dependency_summaries.supported_template_count, 2);
        assert_eq!(output.dependency_summaries.run_summary_count, 0);
        assert!(
            output
                .dependency_summaries
                .fallback_reasons
                .contains_key("diagnostic_source_template_collision")
        );
    }

    #[test]
    fn authority_sidecar_maps_runs_when_source_is_unambiguous() {
        let scanned = classify_formulas(vec![
            raw_formula("B1", 1, 2, "A1+1"),
            raw_formula("C1", 1, 3, "B1+1"),
        ]);
        assert_eq!(scanned[0].template_id, scanned[1].template_id);

        let output = summarize(PathBuf::from("unambiguous.xlsx"), scanned, None)
            .expect("summarize unambiguous workbook");

        assert_eq!(output.authority_templates.authority_template_count, 1);
        assert_eq!(output.authority_templates.diagnostic_collision_count, 0);
        assert_eq!(output.authority_templates.mapped_run_count, 1);
        assert_eq!(output.authority_templates.ambiguous_run_count, 0);
        assert_eq!(output.authority_templates.unmapped_run_count, 0);
        let run_mapping = &output.authority_templates.run_mappings[0];
        let source_mapping = &output.authority_templates.source_template_mappings[0];
        assert_eq!(run_mapping.run_id, 0);
        assert_eq!(source_mapping.authority_template_keys.len(), 1);
        assert_eq!(
            run_mapping.authority_template_key,
            source_mapping.authority_template_keys[0]
        );
        assert_eq!(output.dependency_summaries.authority_template_count, 1);
        assert_eq!(output.dependency_summaries.supported_template_count, 1);
        assert_eq!(output.dependency_summaries.rejected_template_count, 0);
        assert_eq!(output.dependency_summaries.run_summary_count, 1);
        assert_eq!(output.dependency_summaries.result_region_count, 1);
        assert_eq!(output.dependency_summaries.precedent_region_count, 1);
        assert_eq!(output.dependency_summaries.reverse_summary_count, 1);
        assert_eq!(output.dependency_summaries.comparison.exact_match_count, 2);
        assert_eq!(
            output
                .dependency_summaries
                .comparison
                .under_approximation_count,
            0
        );
    }

    #[test]
    fn dependency_summaries_reject_unsupported_templates_without_mapping() {
        let scanned = classify_formulas(vec![raw_formula("B1", 1, 2, "A1:A10")]);
        let output = summarize(PathBuf::from("unsupported.xlsx"), scanned, None)
            .expect("summarize unsupported workbook");

        assert_eq!(output.dependency_summaries.authority_template_count, 1);
        assert_eq!(output.dependency_summaries.supported_template_count, 0);
        assert_eq!(output.dependency_summaries.rejected_template_count, 1);
        assert_eq!(output.dependency_summaries.run_summary_count, 0);
        assert_eq!(output.dependency_summaries.comparison.rejection_count, 1);
        assert!(
            output
                .dependency_summaries
                .fallback_reasons
                .contains_key("finite_range_unsupported")
        );
    }

    #[test]
    fn scanner_json_keeps_fp1_fp3_sections_and_adds_authority_sidecar() {
        let scanned = classify_formulas(vec![raw_formula("B1", 1, 2, "A1+1")]);
        let output = summarize(PathBuf::from("sections.xlsx"), scanned, None)
            .expect("summarize section workbook");
        let value = serde_json::to_value(output).expect("serialize scan output");

        for section in [
            "totals",
            "formula_plane_candidates",
            "formula_run_store",
            "materialization_accounting",
            "templates",
            "authority_templates",
            "dependency_summaries",
        ] {
            assert!(value.get(section).is_some(), "missing {section}");
        }
    }
}
