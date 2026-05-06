//! Passive FormulaPlane dependency summaries for FP4.A.3 and FP4.A.4.
//!
//! This module is crate-internal and read-only. It classifies a narrow initial
//! scalar template subset and records explicit rejection reasons for everything
//! outside that subset; it does not change graph, scheduler, dirty, loader, or
//! evaluation behavior.

use std::collections::{BTreeMap, BTreeSet};

use crate::engine::graph::DependencyGraph;
use crate::engine::plan::{
    DependencyPlan, F_HAS_NAMES, F_HAS_TABLES, F_LIKELY_ARRAY, F_VOLATILE, RangeKey,
};
use formualizer_common::{Coord as AbsCoord, ExcelError};
use formualizer_parse::parser::{ASTNode, CollectPolicy};

use super::ids::{FormulaRunId, FormulaTemplateId};
use super::span_store::{
    FormulaRejectReason, FormulaRunDescriptor, FormulaRunShape, FormulaRunStore, SpanGapKind,
};
use super::template_canonical::{
    AxisRef, CanonicalExpr, CanonicalReference, CanonicalReferenceContext, CanonicalRejectReason,
    CanonicalTemplate, SheetBinding, UnsupportedReferenceKind, canonicalize_template,
    is_known_static_function,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FormulaClass {
    StaticPointwise,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum AnalyzerContext {
    Value,
    Reference,
    ByRefArg,
    CriteriaArg,
    ImplicitIntersection,
    LocalBinding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaDependencySummary {
    pub(crate) formula_class: FormulaClass,
    pub(crate) precedent_patterns: Vec<PrecedentPattern>,
    pub(crate) reject_reasons: Vec<DependencyRejectReason>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PrecedentPattern {
    Cell(AffineCellPattern),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AffineCellPattern {
    pub(crate) sheet: SheetBinding,
    pub(crate) row: AxisRef,
    pub(crate) col: AxisRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AffineRectPattern {
    pub(crate) sheet: SheetBinding,
    pub(crate) start_row: AxisRef,
    pub(crate) start_col: AxisRef,
    pub(crate) end_row: AxisRef,
    pub(crate) end_col: AxisRef,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DependencyRejectReason {
    OpenRangeUnsupported { context: AnalyzerContext },
    WholeAxisUnsupported { context: AnalyzerContext },
    FiniteRangeUnsupported { context: AnalyzerContext },
    NamedRangeUnsupported { context: AnalyzerContext },
    StructuredReferenceUnsupported { context: AnalyzerContext },
    ThreeDReferenceUnsupported { context: AnalyzerContext },
    ExternalReferenceUnsupported { context: AnalyzerContext },
    DynamicDependency { function: Option<String> },
    VolatileUnsupported { function: Option<String> },
    ReferenceReturningUnsupported { function: Option<String> },
    UnknownFunction { name: String },
    LocalEnvUnsupported { function: Option<String> },
    SpillUnsupported,
    ImplicitIntersectionUnsupported,
    FunctionUnsupported { name: String },
    UnsupportedAstNode { node: String },
}

pub(crate) const FP4A_FIXED_COLLECT_POLICY_NAME: &str =
    "fp4a_fixed_collect_v1_expand_small_ranges_false_limit_0_include_names_true";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DependencyCollectPolicyFingerprint {
    pub(crate) expand_small_ranges: bool,
    pub(crate) range_expansion_limit: usize,
    pub(crate) include_names: bool,
}

impl DependencyCollectPolicyFingerprint {
    fn from_policy(policy: &CollectPolicy) -> Self {
        Self {
            expand_small_ranges: policy.expand_small_ranges,
            range_expansion_limit: policy.range_expansion_limit,
            include_names: policy.include_names,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DependencySummaryComparisonInput<'a> {
    pub(crate) sheet: &'a str,
    pub(crate) row: u32,
    pub(crate) col: u32,
    pub(crate) ast: &'a ASTNode,
    pub(crate) summary: &'a FormulaDependencySummary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DependencySummaryComparisonReport {
    pub(crate) oracle_policy_name: &'static str,
    pub(crate) oracle_policy: DependencyCollectPolicyFingerprint,
    pub(crate) requested_policy: DependencyCollectPolicyFingerprint,
    pub(crate) exact_match_count: u64,
    pub(crate) over_approximation_count: u64,
    pub(crate) under_approximation_count: u64,
    pub(crate) rejection_count: u64,
    pub(crate) policy_drift_count: u64,
    pub(crate) fallback_reason_histogram: BTreeMap<String, u64>,
}

impl DependencySummaryComparisonReport {
    fn new(requested_policy: &CollectPolicy) -> Self {
        let oracle_policy = fp4a_fixed_collect_policy();
        Self {
            oracle_policy_name: FP4A_FIXED_COLLECT_POLICY_NAME,
            oracle_policy: DependencyCollectPolicyFingerprint::from_policy(&oracle_policy),
            requested_policy: DependencyCollectPolicyFingerprint::from_policy(requested_policy),
            exact_match_count: 0,
            over_approximation_count: 0,
            under_approximation_count: 0,
            rejection_count: 0,
            policy_drift_count: 0,
            fallback_reason_histogram: BTreeMap::new(),
        }
    }

    pub(crate) fn has_no_under_approximations(&self) -> bool {
        self.under_approximation_count == 0
    }

    fn record_fallback(&mut self, reason: impl Into<String>) {
        self.record_fallback_count(reason, 1);
    }

    fn record_fallback_count(&mut self, reason: impl Into<String>, count: u64) {
        if count == 0 {
            return;
        }
        *self
            .fallback_reason_histogram
            .entry(reason.into())
            .or_default() += count;
    }
}

pub(crate) fn fp4a_fixed_collect_policy() -> CollectPolicy {
    CollectPolicy {
        expand_small_ranges: false,
        range_expansion_limit: 0,
        include_names: true,
    }
}

pub(crate) fn compare_dependency_summaries_to_fixed_planner<'a, I>(
    graph: &mut DependencyGraph,
    inputs: I,
) -> Result<DependencySummaryComparisonReport, ExcelError>
where
    I: IntoIterator<Item = DependencySummaryComparisonInput<'a>>,
{
    let policy = fp4a_fixed_collect_policy();
    compare_dependency_summaries_to_planner_with_policy(graph, inputs, &policy)
}

pub(crate) fn compare_dependency_summaries_to_planner_with_policy<'a, I>(
    graph: &mut DependencyGraph,
    inputs: I,
    requested_policy: &CollectPolicy,
) -> Result<DependencySummaryComparisonReport, ExcelError>
where
    I: IntoIterator<Item = DependencySummaryComparisonInput<'a>>,
{
    let inputs = inputs.into_iter().collect::<Vec<_>>();
    let mut report = DependencySummaryComparisonReport::new(requested_policy);

    if report.requested_policy != report.oracle_policy {
        let drift_count = inputs.len() as u64;
        report.policy_drift_count = drift_count;
        report.record_fallback_count("collect_policy_drift", drift_count);
        return Ok(report);
    }

    let plan = graph.plan_dependencies(
        inputs
            .iter()
            .map(|input| (input.sheet, input.row, input.col, input.ast)),
        requested_policy,
        None,
    )?;

    compare_dependency_summaries_to_plan(graph, &inputs, &plan, &mut report);
    Ok(report)
}

fn compare_dependency_summaries_to_plan(
    graph: &DependencyGraph,
    inputs: &[DependencySummaryComparisonInput<'_>],
    plan: &DependencyPlan,
    report: &mut DependencySummaryComparisonReport,
) {
    for (index, input) in inputs.iter().enumerate() {
        if input.summary.formula_class != FormulaClass::StaticPointwise
            || !input.summary.reject_reasons.is_empty()
        {
            record_summary_rejection(report, input.summary);
            continue;
        }

        let planner_universe = normalize_planner_dependencies(graph, plan, index);
        let planner_fallbacks = planner_universe.fallback_reasons();
        if !planner_fallbacks.is_empty() {
            report.rejection_count += 1;
            for reason in planner_fallbacks {
                report.record_fallback(reason);
            }
            continue;
        }

        let summary_cells = match instantiate_summary_cells(input) {
            Ok(cells) => cells,
            Err(reason) => {
                report.rejection_count += 1;
                report.record_fallback(reason);
                continue;
            }
        };

        if summary_cells == planner_universe.cells {
            report.exact_match_count += 1;
        } else if planner_universe.cells.is_subset(&summary_cells) {
            report.over_approximation_count += 1;
        } else {
            report.under_approximation_count += 1;
        }
    }
}

fn record_summary_rejection(
    report: &mut DependencySummaryComparisonReport,
    summary: &FormulaDependencySummary,
) {
    report.rejection_count += 1;
    if summary.reject_reasons.is_empty() {
        report.record_fallback("summary_rejected_without_reason");
        return;
    }

    for reason in &summary.reject_reasons {
        report.record_fallback(dependency_reject_reason_key(reason));
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct NormalizedDependencyUniverse {
    cells: BTreeSet<FiniteCell>,
    ranges: BTreeSet<NormalizedRangeDependency>,
    names: BTreeSet<String>,
    tables: BTreeSet<String>,
    unsupported: BTreeSet<&'static str>,
}

impl NormalizedDependencyUniverse {
    fn fallback_reasons(&self) -> Vec<&'static str> {
        let mut reasons = BTreeSet::new();
        for range in &self.ranges {
            reasons.insert(match range {
                NormalizedRangeDependency::Finite(_) => "planner_finite_range_dependency",
                NormalizedRangeDependency::WholeRow { .. }
                | NormalizedRangeDependency::WholeCol { .. } => "planner_whole_axis_dependency",
                NormalizedRangeDependency::OpenRect { .. } => "planner_open_range_dependency",
            });
        }
        if !self.names.is_empty() {
            reasons.insert("planner_name_dependency");
        }
        if !self.tables.is_empty() {
            reasons.insert("planner_table_dependency");
        }
        reasons.extend(self.unsupported.iter().copied());
        reasons.into_iter().collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum NormalizedRangeDependency {
    Finite(FiniteRegion),
    WholeRow {
        sheet: String,
        row: u32,
    },
    WholeCol {
        sheet: String,
        col: u32,
    },
    OpenRect {
        sheet: String,
        start: Option<(u32, u32)>,
        end: Option<(u32, u32)>,
    },
}

fn normalize_planner_dependencies(
    graph: &DependencyGraph,
    plan: &DependencyPlan,
    index: usize,
) -> NormalizedDependencyUniverse {
    let mut universe = NormalizedDependencyUniverse::default();

    let Some(per_formula_cells) = plan.per_formula_cells.get(index) else {
        universe.unsupported.insert("planner_formula_index_missing");
        return universe;
    };
    for cell_index in per_formula_cells {
        let Some(&(sheet_id, coord)) = plan.global_cells.get(*cell_index as usize) else {
            universe.unsupported.insert("planner_cell_index_missing");
            continue;
        };
        let (row, col) = planner_coord_to_vc(coord);
        universe
            .cells
            .insert(FiniteCell::new(graph.sheet_name(sheet_id), row, col));
    }

    let Some(per_formula_ranges) = plan.per_formula_ranges.get(index) else {
        universe.unsupported.insert("planner_formula_index_missing");
        return universe;
    };
    for range in per_formula_ranges {
        universe
            .ranges
            .insert(normalize_planner_range(graph, range));
    }

    let Some(per_formula_names) = plan.per_formula_names.get(index) else {
        universe.unsupported.insert("planner_formula_index_missing");
        return universe;
    };
    universe.names.extend(per_formula_names.iter().cloned());

    let Some(per_formula_tables) = plan.per_formula_tables.get(index) else {
        universe.unsupported.insert("planner_formula_index_missing");
        return universe;
    };
    universe.tables.extend(per_formula_tables.iter().cloned());

    let flags = plan.per_formula_flags.get(index).copied().unwrap_or(0);
    if flags & F_VOLATILE != 0 {
        universe.unsupported.insert("planner_volatile_dependency");
    }
    if flags & F_HAS_NAMES != 0 && universe.names.is_empty() {
        universe.unsupported.insert("planner_name_dependency");
    }
    if flags & F_HAS_TABLES != 0 && universe.tables.is_empty() {
        universe.unsupported.insert("planner_table_dependency");
    }
    if flags & F_LIKELY_ARRAY != 0 {
        universe.unsupported.insert("planner_array_dependency");
    }

    universe
}

fn normalize_planner_range(graph: &DependencyGraph, range: &RangeKey) -> NormalizedRangeDependency {
    match range {
        RangeKey::Rect { sheet, start, end } => {
            let (row_start, col_start) = planner_coord_to_vc(*start);
            let (row_end, col_end) = planner_coord_to_vc(*end);
            NormalizedRangeDependency::Finite(FiniteRegion::new(
                graph.sheet_name(*sheet),
                row_start,
                col_start,
                row_end,
                col_end,
            ))
        }
        RangeKey::WholeRow { sheet, row } => NormalizedRangeDependency::WholeRow {
            sheet: graph.sheet_name(*sheet).to_string(),
            row: *row,
        },
        RangeKey::WholeCol { sheet, col } => NormalizedRangeDependency::WholeCol {
            sheet: graph.sheet_name(*sheet).to_string(),
            col: *col,
        },
        RangeKey::OpenRect { sheet, start, end } => NormalizedRangeDependency::OpenRect {
            sheet: graph.sheet_name(*sheet).to_string(),
            start: start.map(planner_coord_to_vc),
            end: end.map(planner_coord_to_vc),
        },
    }
}

fn planner_coord_to_vc(coord: AbsCoord) -> (u32, u32) {
    (coord.row().saturating_add(1), coord.col().saturating_add(1))
}

fn instantiate_summary_cells(
    input: &DependencySummaryComparisonInput<'_>,
) -> Result<BTreeSet<FiniteCell>, &'static str> {
    let mut cells = BTreeSet::new();
    for (pattern_index, pattern) in input.summary.precedent_patterns.iter().enumerate() {
        match pattern {
            PrecedentPattern::Cell(cell_pattern) => {
                let sheet = instantiate_sheet(&cell_pattern.sheet, input.sheet);
                let row = instantiate_axis_cell(
                    &cell_pattern.row,
                    input.row,
                    pattern_index,
                    PatternAxis::Row,
                )
                .map_err(|reason| summary_instantiation_fallback(&reason))?;
                let col = instantiate_axis_cell(
                    &cell_pattern.col,
                    input.col,
                    pattern_index,
                    PatternAxis::Col,
                )
                .map_err(|reason| summary_instantiation_fallback(&reason))?;
                cells.insert(FiniteCell::new(sheet, row, col));
            }
        }
    }
    Ok(cells)
}

fn summary_instantiation_fallback(reason: &RunSummaryRejectionReason) -> &'static str {
    match reason {
        RunSummaryRejectionReason::NonFiniteAxis { .. } => "summary_non_finite_axis",
        RunSummaryRejectionReason::InvalidAxisCoordinate { .. } => {
            "summary_invalid_axis_coordinate"
        }
        _ => "summary_pattern_instantiation_unsupported",
    }
}

pub(crate) fn dependency_reject_reason_key(reason: &DependencyRejectReason) -> String {
    match reason {
        DependencyRejectReason::OpenRangeUnsupported { .. } => "open_range_unsupported".to_string(),
        DependencyRejectReason::WholeAxisUnsupported { .. } => "whole_axis_unsupported".to_string(),
        DependencyRejectReason::FiniteRangeUnsupported { .. } => {
            "finite_range_unsupported".to_string()
        }
        DependencyRejectReason::NamedRangeUnsupported { .. } => {
            "named_range_unsupported".to_string()
        }
        DependencyRejectReason::StructuredReferenceUnsupported { .. } => {
            "structured_reference_unsupported".to_string()
        }
        DependencyRejectReason::ThreeDReferenceUnsupported { .. } => {
            "three_d_reference_unsupported".to_string()
        }
        DependencyRejectReason::ExternalReferenceUnsupported { .. } => {
            "external_reference_unsupported".to_string()
        }
        DependencyRejectReason::DynamicDependency { function } => {
            optional_function_key("dynamic_dependency", function)
        }
        DependencyRejectReason::VolatileUnsupported { function } => {
            optional_function_key("volatile_unsupported", function)
        }
        DependencyRejectReason::ReferenceReturningUnsupported { function } => {
            optional_function_key("reference_returning_unsupported", function)
        }
        DependencyRejectReason::UnknownFunction { name } => format!("unknown_function:{name}"),
        DependencyRejectReason::LocalEnvUnsupported { function } => {
            optional_function_key("local_env_unsupported", function)
        }
        DependencyRejectReason::SpillUnsupported => "spill_unsupported".to_string(),
        DependencyRejectReason::ImplicitIntersectionUnsupported => {
            "implicit_intersection_unsupported".to_string()
        }
        DependencyRejectReason::FunctionUnsupported { name } => {
            format!("function_unsupported:{name}")
        }
        DependencyRejectReason::UnsupportedAstNode { node } => {
            format!("unsupported_ast_node:{node}")
        }
    }
}

fn optional_function_key(prefix: &str, function: &Option<String>) -> String {
    match function {
        Some(function) => format!("{prefix}:{function}"),
        None => prefix.to_string(),
    }
}

pub(crate) fn summarize_dependencies(
    ast: &ASTNode,
    anchor_row: u32,
    anchor_col: u32,
) -> FormulaDependencySummary {
    let template = canonicalize_template(ast, anchor_row, anchor_col);
    summarize_canonical_template(&template)
}

pub(crate) fn summarize_canonical_template(
    template: &CanonicalTemplate,
) -> FormulaDependencySummary {
    let mut analyzer = SummaryAnalyzer::default();
    analyzer.add_canonical_reasons(template.labels.reject_reasons.iter());
    let expr_supported = analyzer.analyze_expr(&template.expr, AnalyzerContext::Value);
    let reject_reasons = analyzer.reasons.into_iter().collect::<Vec<_>>();
    let formula_class = if expr_supported && reject_reasons.is_empty() {
        FormulaClass::StaticPointwise
    } else {
        FormulaClass::Rejected
    };

    FormulaDependencySummary {
        formula_class,
        precedent_patterns: analyzer.precedents,
        reject_reasons,
    }
}

#[derive(Default)]
struct SummaryAnalyzer {
    precedents: Vec<PrecedentPattern>,
    reasons: BTreeSet<DependencyRejectReason>,
}

impl SummaryAnalyzer {
    fn add_canonical_reasons<'a>(
        &mut self,
        reasons: impl IntoIterator<Item = &'a CanonicalRejectReason>,
    ) {
        for reason in reasons {
            self.add_canonical_reason(reason);
        }
    }

    fn add_canonical_reason(&mut self, reason: &CanonicalRejectReason) {
        let dependency_reason = match reason {
            CanonicalRejectReason::InvalidPlacementAnchor { row, col } => {
                DependencyRejectReason::UnsupportedAstNode {
                    node: format!("invalid_placement_anchor:{row}:{col}"),
                }
            }
            CanonicalRejectReason::DynamicReferenceFunction { name } => {
                DependencyRejectReason::DynamicDependency {
                    function: Some(name.clone()),
                }
            }
            CanonicalRejectReason::UnknownOrCustomFunction { name } => {
                DependencyRejectReason::UnknownFunction { name: name.clone() }
            }
            CanonicalRejectReason::LocalEnvironmentFunction { name } => {
                DependencyRejectReason::LocalEnvUnsupported {
                    function: Some(name.clone()),
                }
            }
            CanonicalRejectReason::ParserVolatileFlag => {
                DependencyRejectReason::VolatileUnsupported { function: None }
            }
            CanonicalRejectReason::VolatileFunction { name } => {
                DependencyRejectReason::VolatileUnsupported {
                    function: Some(name.clone()),
                }
            }
            CanonicalRejectReason::ReferenceReturningFunction { name } => {
                DependencyRejectReason::ReferenceReturningUnsupported {
                    function: Some(name.clone()),
                }
            }
            CanonicalRejectReason::ArrayOrSpillFunction { .. }
            | CanonicalRejectReason::SpillReference { .. }
            | CanonicalRejectReason::SpillResultRegionOperator => {
                DependencyRejectReason::SpillUnsupported
            }
            CanonicalRejectReason::ArrayLiteral => DependencyRejectReason::UnsupportedAstNode {
                node: "array_literal".to_string(),
            },
            CanonicalRejectReason::ImplicitIntersectionOperator => {
                DependencyRejectReason::ImplicitIntersectionUnsupported
            }
            CanonicalRejectReason::CallExpression => DependencyRejectReason::UnsupportedAstNode {
                node: "call_expression".to_string(),
            },
            CanonicalRejectReason::NamedReference { .. }
            | CanonicalRejectReason::StructuredReference { .. }
            | CanonicalRejectReason::StructuredReferenceCurrentRow { .. }
            | CanonicalRejectReason::ThreeDReference { .. }
            | CanonicalRejectReason::ExternalReference { .. }
            | CanonicalRejectReason::OpenRangeReference { .. }
            | CanonicalRejectReason::WholeAxisReference { .. }
            | CanonicalRejectReason::UnsupportedReference { .. } => return,
        };
        self.reasons.insert(dependency_reason);
    }

    fn analyze_expr(&mut self, expr: &CanonicalExpr, context: AnalyzerContext) -> bool {
        match expr {
            CanonicalExpr::Literal(_) => matches!(context, AnalyzerContext::Value),
            CanonicalExpr::Reference {
                context: reference_context,
                reference,
            } => {
                let context = effective_context(context, reference_context);
                self.analyze_reference(reference, context)
            }
            CanonicalExpr::Unary { op, expr } => self.analyze_unary(op, expr, context),
            CanonicalExpr::Binary { op, left, right } => {
                self.analyze_binary(op, left, right, context)
            }
            CanonicalExpr::Function { id, args } => {
                let mut all_args_supported = true;
                for (arg_index, arg) in args.iter().enumerate() {
                    let arg_supported =
                        self.analyze_expr(arg, function_arg_context(&id.canonical_name, arg_index));
                    if !arg_supported {
                        all_args_supported = false;
                    }
                }

                if self.has_function_specific_rejection(&id.canonical_name) {
                    return false;
                }

                if is_known_static_function(&id.canonical_name) {
                    if all_args_supported && matches!(context, AnalyzerContext::Value) {
                        return true;
                    }
                    if !all_args_supported && !self.reasons.is_empty() {
                        return false;
                    }
                }

                self.reject_function(&id.canonical_name);
                false
            }
            CanonicalExpr::CallUnsupported { callee, args } => {
                self.analyze_expr(callee, AnalyzerContext::Value);
                for arg in args {
                    self.analyze_expr(arg, AnalyzerContext::Value);
                }
                self.reasons
                    .insert(DependencyRejectReason::UnsupportedAstNode {
                        node: "call_expression".to_string(),
                    });
                false
            }
            CanonicalExpr::ArrayUnsupported { rows } => {
                for row in rows {
                    for item in row {
                        self.analyze_expr(item, AnalyzerContext::Value);
                    }
                }
                self.reasons
                    .insert(DependencyRejectReason::UnsupportedAstNode {
                        node: "array_literal".to_string(),
                    });
                false
            }
        }
    }

    fn analyze_unary(&mut self, op: &str, expr: &CanonicalExpr, context: AnalyzerContext) -> bool {
        match op {
            "+" | "-" | "%" => self.analyze_expr(expr, context),
            "#" => {
                self.analyze_expr(expr, AnalyzerContext::Value);
                self.reasons
                    .insert(DependencyRejectReason::SpillUnsupported);
                false
            }
            "@" => {
                self.analyze_expr(expr, AnalyzerContext::ImplicitIntersection);
                self.reasons
                    .insert(DependencyRejectReason::ImplicitIntersectionUnsupported);
                false
            }
            _ => {
                self.analyze_expr(expr, context);
                self.reasons
                    .insert(DependencyRejectReason::UnsupportedAstNode {
                        node: format!("unary_operator:{op}"),
                    });
                false
            }
        }
    }

    fn analyze_binary(
        &mut self,
        op: &str,
        left: &CanonicalExpr,
        right: &CanonicalExpr,
        context: AnalyzerContext,
    ) -> bool {
        if is_supported_pointwise_binary_operator(op) {
            let left_supported = self.analyze_expr(left, context);
            let right_supported = self.analyze_expr(right, context);
            left_supported && right_supported
        } else if is_reference_returning_binary_operator(op) {
            self.analyze_expr(left, AnalyzerContext::Reference);
            self.analyze_expr(right, AnalyzerContext::Reference);
            self.reasons
                .insert(DependencyRejectReason::ReferenceReturningUnsupported { function: None });
            false
        } else {
            self.analyze_expr(left, context);
            self.analyze_expr(right, context);
            self.reasons
                .insert(DependencyRejectReason::UnsupportedAstNode {
                    node: format!("binary_operator:{op}"),
                });
            false
        }
    }

    fn analyze_reference(
        &mut self,
        reference: &CanonicalReference,
        context: AnalyzerContext,
    ) -> bool {
        match reference {
            CanonicalReference::Cell { sheet, row, col } => {
                if axis_is_finite_cell(row) && axis_is_finite_cell(col) {
                    self.push_precedent(PrecedentPattern::Cell(AffineCellPattern {
                        sheet: sheet.clone(),
                        row: row.clone(),
                        col: col.clone(),
                    }));
                    true
                } else {
                    self.reasons
                        .insert(DependencyRejectReason::UnsupportedAstNode {
                            node: "cell_reference_axis".to_string(),
                        });
                    false
                }
            }
            CanonicalReference::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                self.reject_range(context, [start_row, start_col, end_row, end_col]);
                false
            }
            CanonicalReference::Unsupported { kind, diagnostic } => {
                self.reject_unsupported_reference(kind, diagnostic, context);
                false
            }
        }
    }

    fn reject_range<'a>(
        &mut self,
        context: AnalyzerContext,
        axes: impl IntoIterator<Item = &'a AxisRef>,
    ) {
        let mut has_whole_axis = false;
        let mut has_open = false;
        let mut has_unsupported = false;
        for axis in axes {
            match axis {
                AxisRef::WholeAxis => has_whole_axis = true,
                AxisRef::OpenStart | AxisRef::OpenEnd => has_open = true,
                AxisRef::Unsupported => has_unsupported = true,
                AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. } => {}
            }
        }

        if has_whole_axis {
            self.reasons
                .insert(DependencyRejectReason::WholeAxisUnsupported { context });
        }
        if has_open {
            self.reasons
                .insert(DependencyRejectReason::OpenRangeUnsupported { context });
        }
        if has_unsupported {
            self.reasons
                .insert(DependencyRejectReason::UnsupportedAstNode {
                    node: "range_reference_axis".to_string(),
                });
        }
        if !has_whole_axis && !has_open && !has_unsupported {
            self.reasons
                .insert(DependencyRejectReason::FiniteRangeUnsupported { context });
        }
    }

    fn reject_unsupported_reference(
        &mut self,
        kind: &UnsupportedReferenceKind,
        diagnostic: &str,
        context: AnalyzerContext,
    ) {
        let reason = match kind {
            UnsupportedReferenceKind::NamedRange
                if matches!(context, AnalyzerContext::LocalBinding) =>
            {
                DependencyRejectReason::LocalEnvUnsupported { function: None }
            }
            UnsupportedReferenceKind::NamedRange => {
                DependencyRejectReason::NamedRangeUnsupported { context }
            }
            UnsupportedReferenceKind::StructuredReference => {
                DependencyRejectReason::StructuredReferenceUnsupported { context }
            }
            UnsupportedReferenceKind::ThreeDReference => {
                DependencyRejectReason::ThreeDReferenceUnsupported { context }
            }
            UnsupportedReferenceKind::ExternalReference => {
                DependencyRejectReason::ExternalReferenceUnsupported { context }
            }
            UnsupportedReferenceKind::SpillReference => DependencyRejectReason::SpillUnsupported,
            UnsupportedReferenceKind::Unknown => DependencyRejectReason::UnsupportedAstNode {
                node: format!("unsupported_reference:{diagnostic}"),
            },
        };
        self.reasons.insert(reason);
    }

    fn reject_function(&mut self, name: &str) {
        if self.has_function_specific_rejection(name) {
            return;
        }
        self.reasons
            .insert(DependencyRejectReason::FunctionUnsupported {
                name: name.to_string(),
            });
    }

    fn has_function_specific_rejection(&self, name: &str) -> bool {
        self.reasons.iter().any(|reason| match reason {
            DependencyRejectReason::DynamicDependency { function }
            | DependencyRejectReason::VolatileUnsupported { function }
            | DependencyRejectReason::ReferenceReturningUnsupported { function }
            | DependencyRejectReason::LocalEnvUnsupported { function } => {
                function.as_deref() == Some(name)
            }
            DependencyRejectReason::UnknownFunction { name: unknown_name } => unknown_name == name,
            DependencyRejectReason::SpillUnsupported => is_array_or_spill_function(name),
            _ => false,
        })
    }

    fn push_precedent(&mut self, pattern: PrecedentPattern) {
        if !self.precedents.contains(&pattern) {
            self.precedents.push(pattern);
        }
    }
}

fn effective_context(
    inherited: AnalyzerContext,
    canonical: &CanonicalReferenceContext,
) -> AnalyzerContext {
    if !matches!(inherited, AnalyzerContext::Value) {
        return inherited;
    }

    match canonical {
        CanonicalReferenceContext::Value => AnalyzerContext::Value,
        CanonicalReferenceContext::Reference => AnalyzerContext::Reference,
        CanonicalReferenceContext::FunctionArgument {
            function,
            arg_index,
        } => function_arg_context(function, *arg_index),
        CanonicalReferenceContext::CallArgument { .. } => AnalyzerContext::Value,
    }
}

fn function_arg_context(function: &str, arg_index: usize) -> AnalyzerContext {
    match function {
        "LET" | "LAMBDA" => AnalyzerContext::LocalBinding,
        "COUNTIF" | "SUMIF" if arg_index == 1 => AnalyzerContext::CriteriaArg,
        "COUNTIFS" | "SUMIFS" if arg_index % 2 == 1 => AnalyzerContext::CriteriaArg,
        "INDEX" | "OFFSET" => AnalyzerContext::ByRefArg,
        _ => AnalyzerContext::Value,
    }
}

fn axis_is_finite_cell(axis: &AxisRef) -> bool {
    matches!(
        axis,
        AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. }
    )
}

fn is_supported_pointwise_binary_operator(op: &str) -> bool {
    matches!(
        op,
        "+" | "-" | "*" | "/" | "^" | "&" | "=" | "<>" | "<" | "<=" | ">" | ">="
    )
}

fn is_reference_returning_binary_operator(op: &str) -> bool {
    matches!(op, ":" | "," | " ")
}

fn is_array_or_spill_function(name: &str) -> bool {
    matches!(
        name,
        "FILTER" | "RANDARRAY" | "SEQUENCE" | "SORT" | "SORTBY" | "TEXTSPLIT" | "UNIQUE"
    )
}

const DEFAULT_MAX_EXPLICIT_EXCLUDED_CELLS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FormulaRunDependencySummaryOptions {
    pub(crate) max_explicit_excluded_cells: usize,
}

impl Default for FormulaRunDependencySummaryOptions {
    fn default() -> Self {
        Self {
            max_explicit_excluded_cells: DEFAULT_MAX_EXPLICIT_EXCLUDED_CELLS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaRunDependencySummaryArena {
    pub(crate) row_block_size: u32,
    pub(crate) run_summaries: Vec<InstantiatedFormulaRunSummary>,
    pub(crate) rejected_runs: Vec<RunSummaryRejection>,
    pub(crate) counters: RunDependencySummaryCounters,
    pub(crate) reverse_counters: ReverseDependencyCounters,
    reverse_overage_samples: Vec<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RunDependencySummaryCounters {
    pub(crate) accepted_run_count: u64,
    pub(crate) supported_run_summary_count: u64,
    pub(crate) rejected_run_summary_count: u64,
    pub(crate) missing_template_summary_run_count: u64,
    pub(crate) rejected_template_summary_run_count: u64,
    pub(crate) demoted_run_summary_count: u64,
    pub(crate) result_region_count: u64,
    pub(crate) precedent_region_count: u64,
    pub(crate) row_block_partition_count: u64,
    pub(crate) result_excluded_cell_count: u64,
    pub(crate) precedent_excluded_cell_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReverseDependencyCounters {
    pub(crate) reverse_query_count: u64,
    pub(crate) reverse_exact_partition_count: u64,
    pub(crate) reverse_conservative_partition_count: u64,
    pub(crate) reverse_max_overage: u64,
    pub(crate) reverse_median_overage: u64,
    pub(crate) global_dirty_fallback_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InstantiatedFormulaRunSummary {
    pub(crate) run_id: FormulaRunId,
    pub(crate) template_id: FormulaTemplateId,
    pub(crate) source_template_id: String,
    pub(crate) shape: FormulaRunShape,
    pub(crate) result_region: RegionSummary,
    pub(crate) precedent_regions: Vec<InstantiatedPrecedentSummary>,
    pub(crate) partitions: Vec<RowBlockPartitionSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InstantiatedPrecedentSummary {
    pub(crate) pattern_index: usize,
    pub(crate) pattern: AffineCellPattern,
    pub(crate) region: RegionSummary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FormulaRunPartitionId {
    pub(crate) run_id: FormulaRunId,
    pub(crate) row_block_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowBlockPartitionSummary {
    pub(crate) id: FormulaRunPartitionId,
    pub(crate) result_region: RegionSummary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunSummaryRejection {
    pub(crate) run_id: FormulaRunId,
    pub(crate) source_template_id: String,
    pub(crate) reason: RunSummaryRejectionReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RunSummaryRejectionReason {
    MissingTemplateSummary,
    TemplateRejected {
        formula_class: FormulaClass,
        reject_reasons: Vec<DependencyRejectReason>,
    },
    InvalidRunRegion,
    NonFiniteAxis {
        pattern_index: usize,
        axis: PatternAxis,
    },
    InvalidAxisCoordinate {
        pattern_index: usize,
        axis: PatternAxis,
    },
    TooManyExcludedCells {
        count: usize,
        limit: usize,
    },
    EmptyResultAfterExclusions,
    ReverseGlobalFallbackRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PatternAxis {
    Row,
    Col,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RegionShape {
    Singleton,
    Row,
    Column,
    Rectangle,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FiniteCell {
    pub(crate) sheet: String,
    pub(crate) row: u32,
    pub(crate) col: u32,
}

impl FiniteCell {
    pub(crate) fn new(sheet: impl Into<String>, row: u32, col: u32) -> Self {
        Self {
            sheet: sheet.into(),
            row,
            col,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FiniteRegion {
    pub(crate) sheet: String,
    pub(crate) row_start: u32,
    pub(crate) col_start: u32,
    pub(crate) row_end: u32,
    pub(crate) col_end: u32,
}

impl FiniteRegion {
    pub(crate) fn new(
        sheet: impl Into<String>,
        row_start: u32,
        col_start: u32,
        row_end: u32,
        col_end: u32,
    ) -> Self {
        Self {
            sheet: sheet.into(),
            row_start: row_start.min(row_end),
            col_start: col_start.min(col_end),
            row_end: row_start.max(row_end),
            col_end: col_start.max(col_end),
        }
    }

    pub(crate) fn cell(sheet: impl Into<String>, row: u32, col: u32) -> Self {
        Self::new(sheet, row, col, row, col)
    }

    pub(crate) fn contains_cell(&self, cell: &FiniteCell) -> bool {
        self.sheet == cell.sheet
            && self.row_start <= cell.row
            && cell.row <= self.row_end
            && self.col_start <= cell.col
            && cell.col <= self.col_end
    }

    pub(crate) fn contains_coord(&self, sheet: &str, row: u32, col: u32) -> bool {
        self.sheet == sheet
            && self.row_start <= row
            && row <= self.row_end
            && self.col_start <= col
            && col <= self.col_end
    }

    pub(crate) fn intersects(&self, other: &FiniteRegion) -> bool {
        self.sheet == other.sheet
            && ranges_intersect(self.row_start, self.row_end, other.row_start, other.row_end)
            && ranges_intersect(self.col_start, self.col_end, other.col_start, other.col_end)
    }

    pub(crate) fn intersection(&self, other: &FiniteRegion) -> Option<FiniteRegion> {
        if !self.intersects(other) {
            return None;
        }
        Some(FiniteRegion::new(
            self.sheet.clone(),
            self.row_start.max(other.row_start),
            self.col_start.max(other.col_start),
            self.row_end.min(other.row_end),
            self.col_end.min(other.col_end),
        ))
    }

    pub(crate) fn cell_count(&self) -> u64 {
        let rows = u64::from(self.row_end - self.row_start + 1);
        let cols = u64::from(self.col_end - self.col_start + 1);
        rows.saturating_mul(cols)
    }

    pub(crate) fn shape(&self) -> RegionShape {
        match (
            self.row_start == self.row_end,
            self.col_start == self.col_end,
        ) {
            (true, true) => RegionShape::Singleton,
            (true, false) => RegionShape::Row,
            (false, true) => RegionShape::Column,
            (false, false) => RegionShape::Rectangle,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ExcludedCellSummary {
    pub(crate) cell: FiniteCell,
    pub(crate) kind: ExcludedCellKind,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ExcludedCellKind {
    Hole,
    Exception {
        other_template_id: FormulaTemplateId,
    },
    Rejected {
        reason: FormulaRejectReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegionSummary {
    pub(crate) region: FiniteRegion,
    pub(crate) shape: RegionShape,
    pub(crate) rectangle_cell_count: u64,
    pub(crate) excluded_cells: Vec<ExcludedCellSummary>,
    pub(crate) excluded_cell_count: u64,
    pub(crate) included_cell_count: u64,
}

impl RegionSummary {
    fn new(region: FiniteRegion, excluded_cells: Vec<ExcludedCellSummary>) -> Self {
        let mut excluded_by_cell = BTreeMap::<FiniteCell, ExcludedCellKind>::new();
        for excluded in excluded_cells {
            if region.contains_cell(&excluded.cell) {
                excluded_by_cell
                    .entry(excluded.cell)
                    .or_insert(excluded.kind);
            }
        }
        let excluded_cells = excluded_by_cell
            .into_iter()
            .map(|(cell, kind)| ExcludedCellSummary { cell, kind })
            .collect::<Vec<_>>();
        let rectangle_cell_count = region.cell_count();
        let excluded_cell_count = excluded_cells.len() as u64;
        let included_cell_count = rectangle_cell_count.saturating_sub(excluded_cell_count);
        let shape = region.shape();
        Self {
            region,
            shape,
            rectangle_cell_count,
            excluded_cells,
            excluded_cell_count,
            included_cell_count,
        }
    }

    pub(crate) fn contains_included_cell(&self, sheet: &str, row: u32, col: u32) -> bool {
        self.region.contains_coord(sheet, row, col)
            && !self.excluded_cells.iter().any(|excluded| {
                excluded.cell.sheet == sheet && excluded.cell.row == row && excluded.cell.col == col
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReverseQueryResult {
    pub(crate) changed_region: FiniteRegion,
    pub(crate) dependent_partitions: Vec<ReverseDependentPartitionSummary>,
    pub(crate) global_dirty_fallback: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReverseDependentPartitionSummary {
    pub(crate) partition_id: FormulaRunPartitionId,
    pub(crate) source_template_id: String,
    pub(crate) partition_result_region: RegionSummary,
    pub(crate) matched_result_regions: Vec<FiniteRegion>,
    pub(crate) exact_dependent_cell_count: u64,
    pub(crate) partition_cell_count: u64,
    pub(crate) overage_cell_count: u64,
    pub(crate) is_exact: bool,
}

pub(crate) fn instantiate_run_dependency_summaries(
    run_store: &FormulaRunStore,
    template_summaries: &BTreeMap<String, FormulaDependencySummary>,
) -> FormulaRunDependencySummaryArena {
    instantiate_run_dependency_summaries_with_options(
        run_store,
        template_summaries,
        FormulaRunDependencySummaryOptions::default(),
    )
}

pub(crate) fn instantiate_run_dependency_summaries_with_options(
    run_store: &FormulaRunStore,
    template_summaries: &BTreeMap<String, FormulaDependencySummary>,
    options: FormulaRunDependencySummaryOptions,
) -> FormulaRunDependencySummaryArena {
    let mut counters = RunDependencySummaryCounters {
        accepted_run_count: run_store.runs.len() as u64,
        ..RunDependencySummaryCounters::default()
    };
    let mut run_summaries = Vec::new();
    let mut rejected_runs = Vec::new();

    let mut runs = run_store.runs.iter().collect::<Vec<_>>();
    runs.sort_by(|a, b| a.id.cmp(&b.id));

    for run in runs {
        let Some(template_summary) = template_summaries.get(&run.source_template_id) else {
            counters.missing_template_summary_run_count += 1;
            rejected_runs.push(run_rejection(
                run,
                RunSummaryRejectionReason::MissingTemplateSummary,
            ));
            continue;
        };

        if template_summary.formula_class != FormulaClass::StaticPointwise
            || !template_summary.reject_reasons.is_empty()
        {
            counters.rejected_template_summary_run_count += 1;
            rejected_runs.push(run_rejection(
                run,
                RunSummaryRejectionReason::TemplateRejected {
                    formula_class: template_summary.formula_class,
                    reject_reasons: template_summary.reject_reasons.clone(),
                },
            ));
            continue;
        }

        match instantiate_one_run_summary(run, template_summary, run_store, options) {
            Ok(summary) => {
                counters.supported_run_summary_count += 1;
                counters.result_region_count += 1;
                counters.precedent_region_count += summary.precedent_regions.len() as u64;
                counters.row_block_partition_count += summary.partitions.len() as u64;
                counters.result_excluded_cell_count += summary.result_region.excluded_cell_count;
                counters.precedent_excluded_cell_count += summary
                    .precedent_regions
                    .iter()
                    .map(|precedent| precedent.region.excluded_cell_count)
                    .sum::<u64>();
                run_summaries.push(summary);
            }
            Err(reason) => {
                counters.demoted_run_summary_count += 1;
                rejected_runs.push(run_rejection(run, reason));
            }
        }
    }

    run_summaries.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    rejected_runs.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    counters.rejected_run_summary_count = rejected_runs.len() as u64;

    FormulaRunDependencySummaryArena {
        row_block_size: run_store.row_block_size.max(1),
        run_summaries,
        rejected_runs,
        counters,
        reverse_counters: ReverseDependencyCounters::default(),
        reverse_overage_samples: Vec::new(),
    }
}

impl FormulaRunDependencySummaryArena {
    pub(crate) fn query_changed_cell(
        &mut self,
        sheet: impl Into<String>,
        row: u32,
        col: u32,
    ) -> ReverseQueryResult {
        self.query_changed_region(&FiniteRegion::cell(sheet, row, col))
    }

    pub(crate) fn query_changed_region(
        &mut self,
        changed_region: &FiniteRegion,
    ) -> ReverseQueryResult {
        self.reverse_counters.reverse_query_count += 1;

        let mut pending = BTreeMap::<FormulaRunPartitionId, PendingReversePartition>::new();
        for run_summary in &self.run_summaries {
            for precedent in &run_summary.precedent_regions {
                if !precedent.region.region.intersects(changed_region) {
                    continue;
                }
                let Some(matched_result_region) =
                    inverse_changed_region_for_run(run_summary, precedent, changed_region)
                else {
                    continue;
                };
                for partition in &run_summary.partitions {
                    let Some(overlap) =
                        matched_result_region.intersection(&partition.result_region.region)
                    else {
                        continue;
                    };
                    let segment = segment_for_region(&overlap, run_summary.shape);
                    pending
                        .entry(partition.id)
                        .or_insert_with(|| PendingReversePartition {
                            partition: partition.clone(),
                            source_template_id: run_summary.source_template_id.clone(),
                            run_shape: run_summary.shape,
                            segments: Vec::new(),
                        })
                        .segments
                        .push(segment);
                }
            }
        }

        let mut dependent_partitions = pending
            .into_values()
            .filter_map(PendingReversePartition::finish)
            .collect::<Vec<_>>();
        dependent_partitions.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));

        for partition in &dependent_partitions {
            if partition.is_exact {
                self.reverse_counters.reverse_exact_partition_count += 1;
            } else {
                self.reverse_counters.reverse_conservative_partition_count += 1;
            }
            self.reverse_counters.reverse_max_overage = self
                .reverse_counters
                .reverse_max_overage
                .max(partition.overage_cell_count);
            self.reverse_overage_samples
                .push(partition.overage_cell_count);
        }
        self.reverse_counters.reverse_median_overage = median_u64(&self.reverse_overage_samples);

        ReverseQueryResult {
            changed_region: changed_region.clone(),
            dependent_partitions,
            global_dirty_fallback: false,
        }
    }
}

fn instantiate_one_run_summary(
    run: &FormulaRunDescriptor,
    template_summary: &FormulaDependencySummary,
    run_store: &FormulaRunStore,
    options: FormulaRunDependencySummaryOptions,
) -> Result<InstantiatedFormulaRunSummary, RunSummaryRejectionReason> {
    let result_region = result_region_for_run(run)?;
    let result_exclusions = collect_result_exclusions(run, run_store, options)?;
    let result_summary = RegionSummary::new(result_region, result_exclusions);
    if result_summary.included_cell_count == 0 {
        return Err(RunSummaryRejectionReason::EmptyResultAfterExclusions);
    }

    let partitions = build_row_block_partitions(run.id, &result_summary, run_store.row_block_size);
    if partitions.is_empty() {
        return Err(RunSummaryRejectionReason::ReverseGlobalFallbackRequired);
    }

    let mut precedent_regions = Vec::new();
    for (pattern_index, pattern) in template_summary.precedent_patterns.iter().enumerate() {
        match pattern {
            PrecedentPattern::Cell(cell_pattern) => {
                let region = instantiate_precedent_region(
                    run,
                    cell_pattern,
                    &result_summary,
                    pattern_index,
                )?;
                precedent_regions.push(InstantiatedPrecedentSummary {
                    pattern_index,
                    pattern: cell_pattern.clone(),
                    region,
                });
            }
        }
    }

    Ok(InstantiatedFormulaRunSummary {
        run_id: run.id,
        template_id: run.template_id,
        source_template_id: run.source_template_id.clone(),
        shape: run.shape,
        result_region: result_summary,
        precedent_regions,
        partitions,
    })
}

fn result_region_for_run(
    run: &FormulaRunDescriptor,
) -> Result<FiniteRegion, RunSummaryRejectionReason> {
    if run.row_start == 0
        || run.col_start == 0
        || run.row_end == 0
        || run.col_end == 0
        || run.row_start > run.row_end
        || run.col_start > run.col_end
    {
        return Err(RunSummaryRejectionReason::InvalidRunRegion);
    }
    Ok(FiniteRegion::new(
        run.sheet.clone(),
        run.row_start,
        run.col_start,
        run.row_end,
        run.col_end,
    ))
}

fn collect_result_exclusions(
    run: &FormulaRunDescriptor,
    run_store: &FormulaRunStore,
    options: FormulaRunDependencySummaryOptions,
) -> Result<Vec<ExcludedCellSummary>, RunSummaryRejectionReason> {
    let region = result_region_for_run(run)?;
    let mut excluded = BTreeMap::<FiniteCell, ExcludedCellKind>::new();

    for gap in &run_store.gaps {
        if gap.template_id != run.template_id
            || !region.contains_coord(&gap.sheet, gap.row, gap.col)
        {
            continue;
        }
        let kind = match gap.kind {
            SpanGapKind::Hole => ExcludedCellKind::Hole,
            SpanGapKind::Exception { other_template_id } => {
                ExcludedCellKind::Exception { other_template_id }
            }
        };
        excluded
            .entry(FiniteCell::new(gap.sheet.clone(), gap.row, gap.col))
            .or_insert(kind);
    }

    for rejected in &run_store.rejected_cells {
        if !region.contains_coord(&rejected.sheet, rejected.row, rejected.col) {
            continue;
        }
        excluded
            .entry(FiniteCell::new(
                rejected.sheet.clone(),
                rejected.row,
                rejected.col,
            ))
            .or_insert(ExcludedCellKind::Rejected {
                reason: rejected.reason,
            });
    }

    if excluded.len() > options.max_explicit_excluded_cells {
        return Err(RunSummaryRejectionReason::TooManyExcludedCells {
            count: excluded.len(),
            limit: options.max_explicit_excluded_cells,
        });
    }

    Ok(excluded
        .into_iter()
        .map(|(cell, kind)| ExcludedCellSummary { cell, kind })
        .collect())
}

fn instantiate_precedent_region(
    run: &FormulaRunDescriptor,
    pattern: &AffineCellPattern,
    result_summary: &RegionSummary,
    pattern_index: usize,
) -> Result<RegionSummary, RunSummaryRejectionReason> {
    let sheet = instantiate_sheet(&pattern.sheet, &run.sheet);
    let (row_start, row_end) = instantiate_axis_range(
        &pattern.row,
        result_summary.region.row_start,
        result_summary.region.row_end,
        pattern_index,
        PatternAxis::Row,
    )?;
    let (col_start, col_end) = instantiate_axis_range(
        &pattern.col,
        result_summary.region.col_start,
        result_summary.region.col_end,
        pattern_index,
        PatternAxis::Col,
    )?;
    let precedent_region = FiniteRegion::new(sheet, row_start, col_start, row_end, col_end);
    let exclusions =
        instantiate_precedent_exclusions(run.shape, pattern, result_summary, pattern_index)?;
    Ok(RegionSummary::new(precedent_region, exclusions))
}

fn instantiate_precedent_exclusions(
    run_shape: FormulaRunShape,
    pattern: &AffineCellPattern,
    result_summary: &RegionSummary,
    pattern_index: usize,
) -> Result<Vec<ExcludedCellSummary>, RunSummaryRejectionReason> {
    if !pattern_is_injective_for_run_shape(run_shape, pattern) {
        return Ok(Vec::new());
    }

    let mut excluded = BTreeMap::<FiniteCell, ExcludedCellKind>::new();
    let sheet = instantiate_sheet(&pattern.sheet, &result_summary.region.sheet);
    for result_exclusion in &result_summary.excluded_cells {
        let row = instantiate_axis_cell(
            &pattern.row,
            result_exclusion.cell.row,
            pattern_index,
            PatternAxis::Row,
        )?;
        let col = instantiate_axis_cell(
            &pattern.col,
            result_exclusion.cell.col,
            pattern_index,
            PatternAxis::Col,
        )?;
        excluded
            .entry(FiniteCell::new(sheet.clone(), row, col))
            .or_insert(result_exclusion.kind.clone());
    }

    Ok(excluded
        .into_iter()
        .map(|(cell, kind)| ExcludedCellSummary { cell, kind })
        .collect())
}

fn pattern_is_injective_for_run_shape(
    run_shape: FormulaRunShape,
    pattern: &AffineCellPattern,
) -> bool {
    match run_shape {
        FormulaRunShape::Row => matches!(pattern.col, AxisRef::RelativeToPlacement { .. }),
        FormulaRunShape::Column => matches!(pattern.row, AxisRef::RelativeToPlacement { .. }),
        FormulaRunShape::Singleton => true,
    }
}

fn instantiate_sheet(sheet: &SheetBinding, formula_sheet: &str) -> String {
    match sheet {
        SheetBinding::CurrentSheet => formula_sheet.to_string(),
        SheetBinding::ExplicitName { name } => name.clone(),
    }
}

fn instantiate_axis_range(
    axis: &AxisRef,
    placement_start: u32,
    placement_end: u32,
    pattern_index: usize,
    pattern_axis: PatternAxis,
) -> Result<(u32, u32), RunSummaryRejectionReason> {
    match axis {
        AxisRef::RelativeToPlacement { offset } => {
            let start = coordinate_with_offset(placement_start, *offset).ok_or(
                RunSummaryRejectionReason::InvalidAxisCoordinate {
                    pattern_index,
                    axis: pattern_axis,
                },
            )?;
            let end = coordinate_with_offset(placement_end, *offset).ok_or(
                RunSummaryRejectionReason::InvalidAxisCoordinate {
                    pattern_index,
                    axis: pattern_axis,
                },
            )?;
            Ok((start.min(end), start.max(end)))
        }
        AxisRef::AbsoluteVc { index } if *index > 0 => Ok((*index, *index)),
        AxisRef::AbsoluteVc { .. } => Err(RunSummaryRejectionReason::InvalidAxisCoordinate {
            pattern_index,
            axis: pattern_axis,
        }),
        AxisRef::OpenStart | AxisRef::OpenEnd | AxisRef::WholeAxis | AxisRef::Unsupported => {
            Err(RunSummaryRejectionReason::NonFiniteAxis {
                pattern_index,
                axis: pattern_axis,
            })
        }
    }
}

fn instantiate_axis_cell(
    axis: &AxisRef,
    placement: u32,
    pattern_index: usize,
    pattern_axis: PatternAxis,
) -> Result<u32, RunSummaryRejectionReason> {
    match axis {
        AxisRef::RelativeToPlacement { offset } => coordinate_with_offset(placement, *offset)
            .ok_or(RunSummaryRejectionReason::InvalidAxisCoordinate {
                pattern_index,
                axis: pattern_axis,
            }),
        AxisRef::AbsoluteVc { index } if *index > 0 => Ok(*index),
        AxisRef::AbsoluteVc { .. } => Err(RunSummaryRejectionReason::InvalidAxisCoordinate {
            pattern_index,
            axis: pattern_axis,
        }),
        AxisRef::OpenStart | AxisRef::OpenEnd | AxisRef::WholeAxis | AxisRef::Unsupported => {
            Err(RunSummaryRejectionReason::NonFiniteAxis {
                pattern_index,
                axis: pattern_axis,
            })
        }
    }
}

fn coordinate_with_offset(coordinate: u32, offset: i64) -> Option<u32> {
    let value = i128::from(coordinate) + i128::from(offset);
    if (1..=i128::from(u32::MAX)).contains(&value) {
        Some(value as u32)
    } else {
        None
    }
}

fn build_row_block_partitions(
    run_id: FormulaRunId,
    result_summary: &RegionSummary,
    row_block_size: u32,
) -> Vec<RowBlockPartitionSummary> {
    let row_block_size = row_block_size.max(1);
    let start_block = row_block_index(result_summary.region.row_start, row_block_size);
    let end_block = row_block_index(result_summary.region.row_end, row_block_size);
    let mut partitions = Vec::new();

    for block in start_block..=end_block {
        let (block_row_start, block_row_end) = row_block_bounds(block, row_block_size);
        let block_region = FiniteRegion::new(
            result_summary.region.sheet.clone(),
            block_row_start,
            result_summary.region.col_start,
            block_row_end,
            result_summary.region.col_end,
        );
        let Some(partition_region) = result_summary.region.intersection(&block_region) else {
            continue;
        };
        let partition_exclusions = result_summary
            .excluded_cells
            .iter()
            .filter(|excluded| partition_region.contains_cell(&excluded.cell))
            .cloned()
            .collect::<Vec<_>>();
        let partition_summary = RegionSummary::new(partition_region, partition_exclusions);
        if partition_summary.included_cell_count == 0 {
            continue;
        }
        partitions.push(RowBlockPartitionSummary {
            id: FormulaRunPartitionId {
                run_id,
                row_block_index: block,
            },
            result_region: partition_summary,
        });
    }

    partitions
}

fn row_block_bounds(block: u32, row_block_size: u32) -> (u32, u32) {
    let row_block_size = row_block_size.max(1);
    let start = block.saturating_mul(row_block_size).saturating_add(1);
    let end = block.saturating_add(1).saturating_mul(row_block_size);
    (start, end.max(start))
}

fn row_block_index(row: u32, row_block_size: u32) -> u32 {
    row.saturating_sub(1) / row_block_size.max(1)
}

fn inverse_changed_region_for_run(
    run_summary: &InstantiatedFormulaRunSummary,
    precedent: &InstantiatedPrecedentSummary,
    changed_region: &FiniteRegion,
) -> Option<FiniteRegion> {
    if changed_region.sheet != precedent.region.region.sheet {
        return None;
    }
    let (row_start, row_end) = inverse_axis_changed_range(
        &precedent.pattern.row,
        changed_region.row_start,
        changed_region.row_end,
    )?;
    let (col_start, col_end) = inverse_axis_changed_range(
        &precedent.pattern.col,
        changed_region.col_start,
        changed_region.col_end,
    )?;
    let candidate = FiniteRegion::new(
        run_summary.result_region.region.sheet.clone(),
        row_start,
        col_start,
        row_end,
        col_end,
    );
    run_summary.result_region.region.intersection(&candidate)
}

fn inverse_axis_changed_range(
    axis: &AxisRef,
    changed_start: u32,
    changed_end: u32,
) -> Option<(u32, u32)> {
    match axis {
        AxisRef::RelativeToPlacement { offset } => {
            let start = coordinate_with_offset(changed_start, -*offset)?;
            let end = coordinate_with_offset(changed_end, -*offset)?;
            Some((start.min(end), start.max(end)))
        }
        AxisRef::AbsoluteVc { index }
            if *index > 0 && changed_start <= *index && *index <= changed_end =>
        {
            Some((1, u32::MAX))
        }
        AxisRef::AbsoluteVc { .. } => None,
        AxisRef::OpenStart | AxisRef::OpenEnd | AxisRef::WholeAxis | AxisRef::Unsupported => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LineSegment {
    start: u32,
    end: u32,
}

struct PendingReversePartition {
    partition: RowBlockPartitionSummary,
    source_template_id: String,
    run_shape: FormulaRunShape,
    segments: Vec<LineSegment>,
}

impl PendingReversePartition {
    fn finish(mut self) -> Option<ReverseDependentPartitionSummary> {
        let merged_segments = merge_segments(&mut self.segments);
        let mut exact_dependent_cell_count = merged_segments
            .iter()
            .map(|segment| u64::from(segment.end - segment.start + 1))
            .sum::<u64>();

        for excluded in &self.partition.result_region.excluded_cells {
            let value = cell_axis_value(&excluded.cell, self.run_shape);
            if merged_segments
                .iter()
                .any(|segment| segment.contains(value))
            {
                exact_dependent_cell_count = exact_dependent_cell_count.saturating_sub(1);
            }
        }

        if exact_dependent_cell_count == 0 {
            return None;
        }

        let matched_result_regions = merged_segments
            .iter()
            .map(|segment| {
                segment_to_region(
                    segment,
                    &self.partition.result_region.region,
                    self.run_shape,
                )
            })
            .collect::<Vec<_>>();
        let partition_cell_count = self.partition.result_region.included_cell_count;
        let overage_cell_count = partition_cell_count.saturating_sub(exact_dependent_cell_count);

        Some(ReverseDependentPartitionSummary {
            partition_id: self.partition.id,
            source_template_id: self.source_template_id,
            partition_result_region: self.partition.result_region,
            matched_result_regions,
            exact_dependent_cell_count,
            partition_cell_count,
            overage_cell_count,
            is_exact: overage_cell_count == 0,
        })
    }
}

impl LineSegment {
    fn contains(&self, value: u32) -> bool {
        self.start <= value && value <= self.end
    }
}

fn segment_for_region(region: &FiniteRegion, run_shape: FormulaRunShape) -> LineSegment {
    match run_shape {
        FormulaRunShape::Row => LineSegment {
            start: region.col_start,
            end: region.col_end,
        },
        FormulaRunShape::Column => LineSegment {
            start: region.row_start,
            end: region.row_end,
        },
        FormulaRunShape::Singleton => LineSegment { start: 0, end: 0 },
    }
}

fn cell_axis_value(cell: &FiniteCell, run_shape: FormulaRunShape) -> u32 {
    match run_shape {
        FormulaRunShape::Row => cell.col,
        FormulaRunShape::Column => cell.row,
        FormulaRunShape::Singleton => 0,
    }
}

fn segment_to_region(
    segment: &LineSegment,
    partition_region: &FiniteRegion,
    run_shape: FormulaRunShape,
) -> FiniteRegion {
    match run_shape {
        FormulaRunShape::Row => FiniteRegion::new(
            partition_region.sheet.clone(),
            partition_region.row_start,
            segment.start,
            partition_region.row_end,
            segment.end,
        ),
        FormulaRunShape::Column => FiniteRegion::new(
            partition_region.sheet.clone(),
            segment.start,
            partition_region.col_start,
            segment.end,
            partition_region.col_end,
        ),
        FormulaRunShape::Singleton => partition_region.clone(),
    }
}

fn merge_segments(segments: &mut [LineSegment]) -> Vec<LineSegment> {
    segments.sort_by(|a, b| (a.start, a.end).cmp(&(b.start, b.end)));
    let mut merged = Vec::<LineSegment>::new();
    for segment in segments.iter().copied() {
        match merged.last_mut() {
            Some(last) if segment.start <= last.end.saturating_add(1) => {
                last.end = last.end.max(segment.end);
            }
            _ => merged.push(segment),
        }
    }
    merged
}

fn median_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn ranges_intersect(a_start: u32, a_end: u32, b_start: u32, b_end: u32) -> bool {
    a_start <= b_end && b_start <= a_end
}

fn run_rejection(
    run: &FormulaRunDescriptor,
    reason: RunSummaryRejectionReason,
) -> RunSummaryRejection {
    RunSummaryRejection {
        run_id: run.id,
        source_template_id: run.source_template_id.clone(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use formualizer_parse::parse;
    use formualizer_parse::parser::{ASTNode, CollectPolicy};

    use super::super::span_counters::FormulaPlaneCandidateCell;
    use super::super::span_store::{FormulaRunStore, FormulaRunStoreBuildOptions};
    use crate::engine::graph::DependencyGraph;

    fn ast(formula: &str) -> ASTNode {
        parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"))
    }

    fn summary(formula: &str, row: u32, col: u32) -> FormulaDependencySummary {
        let ast = ast(formula);
        summarize_dependencies(&ast, row, col)
    }

    fn cell(sheet: SheetBinding, row: AxisRef, col: AxisRef) -> PrecedentPattern {
        PrecedentPattern::Cell(AffineCellPattern { sheet, row, col })
    }

    fn has_reason(summary: &FormulaDependencySummary, reason: &DependencyRejectReason) -> bool {
        summary.reject_reasons.iter().any(|actual| actual == reason)
    }

    fn has_reason_kind(
        summary: &FormulaDependencySummary,
        matches: impl Fn(&DependencyRejectReason) -> bool,
    ) -> bool {
        summary.reject_reasons.iter().any(matches)
    }

    fn candidate_cell(row: u32, col: u32, template_id: &str) -> FormulaPlaneCandidateCell {
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

    fn run_store(cells: Vec<FormulaPlaneCandidateCell>, row_block_size: u32) -> FormulaRunStore {
        FormulaRunStore::build_with_options(
            &cells,
            FormulaRunStoreBuildOptions {
                row_block_size,
                ..FormulaRunStoreBuildOptions::default()
            },
        )
    }

    fn template_summary_map(
        entries: Vec<(&str, FormulaDependencySummary)>,
    ) -> BTreeMap<String, FormulaDependencySummary> {
        entries
            .into_iter()
            .map(|(source_template_id, summary)| (source_template_id.to_string(), summary))
            .collect()
    }

    fn compare_one(
        sheet: &str,
        row: u32,
        col: u32,
        ast: &ASTNode,
        summary: &FormulaDependencySummary,
    ) -> DependencySummaryComparisonReport {
        let mut graph = DependencyGraph::new();
        compare_dependency_summaries_to_fixed_planner(
            &mut graph,
            [DependencySummaryComparisonInput {
                sheet,
                row,
                col,
                ast,
                summary,
            }],
        )
        .expect("fixed planner comparison should build")
    }

    fn assert_single_exact_report(report: &DependencySummaryComparisonReport) {
        assert_eq!(report.exact_match_count, 1);
        assert_eq!(report.over_approximation_count, 0);
        assert_eq!(report.under_approximation_count, 0);
        assert_eq!(report.rejection_count, 0);
        assert_eq!(report.policy_drift_count, 0);
        assert!(report.fallback_reason_histogram.is_empty());
        assert!(report.has_no_under_approximations());
    }

    fn shuffled_candidates(
        mut cells: Vec<FormulaPlaneCandidateCell>,
    ) -> Vec<FormulaPlaneCandidateCell> {
        let len = cells.len();
        if len <= 1 {
            return cells;
        }
        let mut out = Vec::with_capacity(len);
        for index in (1..len).step_by(2) {
            out.push(cells[index].clone());
        }
        for index in (0..len).rev() {
            if index % 2 == 0 {
                out.push(cells[index].clone());
            }
        }
        cells.clear();
        out
    }

    #[test]
    fn formula_plane_dependency_summary_static_pointwise_addition_collects_cells() {
        let summary = summary("=A1+B1", 1, 3);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(
            summary.precedent_patterns,
            vec![
                cell(
                    SheetBinding::CurrentSheet,
                    AxisRef::RelativeToPlacement { offset: 0 },
                    AxisRef::RelativeToPlacement { offset: -2 }
                ),
                cell(
                    SheetBinding::CurrentSheet,
                    AxisRef::RelativeToPlacement { offset: 0 },
                    AxisRef::RelativeToPlacement { offset: -1 }
                )
            ]
        );
        assert!(summary.reject_reasons.is_empty());
    }

    #[test]
    fn formula_plane_dependency_summary_preserves_absolute_and_relative_axes() {
        let summary = summary("=$A$1+B2", 2, 3);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(
            summary.precedent_patterns,
            vec![
                cell(
                    SheetBinding::CurrentSheet,
                    AxisRef::AbsoluteVc { index: 1 },
                    AxisRef::AbsoluteVc { index: 1 }
                ),
                cell(
                    SheetBinding::CurrentSheet,
                    AxisRef::RelativeToPlacement { offset: 0 },
                    AxisRef::RelativeToPlacement { offset: -1 }
                )
            ]
        );
    }

    #[test]
    fn formula_plane_dependency_summary_preserves_static_cross_sheet_binding() {
        let summary = summary("=Sheet2!A1+B1", 1, 3);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(
            summary.precedent_patterns,
            vec![
                cell(
                    SheetBinding::ExplicitName {
                        name: "Sheet2".to_string(),
                    },
                    AxisRef::RelativeToPlacement { offset: 0 },
                    AxisRef::RelativeToPlacement { offset: -2 }
                ),
                cell(
                    SheetBinding::CurrentSheet,
                    AxisRef::RelativeToPlacement { offset: 0 },
                    AxisRef::RelativeToPlacement { offset: -1 }
                )
            ]
        );
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_exact_match_addition() {
        let formula_ast = ast("=A1+B1");
        let summary = summarize_dependencies(&formula_ast, 1, 3);

        let report = compare_one("Sheet1", 1, 3, &formula_ast, &summary);

        assert_eq!(report.oracle_policy_name, FP4A_FIXED_COLLECT_POLICY_NAME);
        assert_eq!(
            report.oracle_policy,
            DependencyCollectPolicyFingerprint {
                expand_small_ranges: false,
                range_expansion_limit: 0,
                include_names: true,
            }
        );
        assert_eq!(report.requested_policy, report.oracle_policy);
        assert_single_exact_report(&report);
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_instantiates_copied_relative_formula() {
        let template_ast = ast("=A1+B1");
        let copied_ast = ast("=A20+B20");
        let summary = summarize_dependencies(&template_ast, 1, 3);

        let report = compare_one("Sheet1", 20, 3, &copied_ast, &summary);

        assert_single_exact_report(&report);
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_handles_mixed_anchors_without_under_approx() {
        let template_ast = ast("=$A1+B$1");
        let copied_ast = ast("=$A20+B$1");
        let summary = summarize_dependencies(&template_ast, 1, 3);

        let report = compare_one("Sheet1", 20, 3, &copied_ast, &summary);

        assert_single_exact_report(&report);
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_matches_cross_sheet_static_cells() {
        let formula_ast = ast("=Sheet2!A1+B1");
        let summary = summarize_dependencies(&formula_ast, 1, 3);

        let report = compare_one("Sheet1", 1, 3, &formula_ast, &summary);

        assert_single_exact_report(&report);
    }

    #[test]
    fn formula_plane_dependency_summary_static_literals_and_unary_collects_cells() {
        let literal = summary("=42", 1, 1);
        let unary = summary("=-A1%", 1, 2);

        assert_eq!(literal.formula_class, FormulaClass::StaticPointwise);
        assert!(literal.precedent_patterns.is_empty());
        assert!(literal.reject_reasons.is_empty());
        assert_eq!(unary.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(
            unary.precedent_patterns,
            vec![cell(
                SheetBinding::CurrentSheet,
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::RelativeToPlacement { offset: -1 }
            )]
        );
        assert!(unary.reject_reasons.is_empty());
    }

    #[test]
    fn formula_plane_dependency_summary_static_pointwise_concatenation_collects_cells() {
        let summary = summary("=A1&\"x\"", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(
            summary.precedent_patterns,
            vec![cell(
                SheetBinding::CurrentSheet,
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::RelativeToPlacement { offset: -1 }
            )]
        );
        assert!(summary.reject_reasons.is_empty());
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_direct_finite_range_value() {
        let summary = summary("=A1:A10", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(!has_reason_kind(&summary, |reason| matches!(
            reason,
            DependencyRejectReason::FunctionUnsupported { .. }
        )));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_open_ended_and_open_axis_ranges() {
        // The parser accepts partially specified endpoints such as `A1:A`,
        // `A:A10`, `A1:10`, and `1:A10`, but not a fully omitted side.
        assert!(parse("=A1:").is_err());
        assert!(parse("=:A10").is_err());

        for formula in ["=A1:A", "=A:A10", "=A1:10", "=1:A10"] {
            let summary = summary(formula, 1, 2);

            assert_eq!(summary.formula_class, FormulaClass::Rejected);
            assert!(
                has_reason(
                    &summary,
                    &DependencyRejectReason::OpenRangeUnsupported {
                        context: AnalyzerContext::Value
                    }
                ),
                "expected open-range rejection for {formula}: {summary:?}"
            );
        }
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_named_structured_3d_and_external_references() {
        let named = summary("=MyName", 1, 1);
        let structured = summary("=Table1[Amount]", 1, 1);
        let three_d = summary("=Sheet1:Sheet3!A1", 1, 1);
        let external = summary("=[1]Sheet1!A1", 1, 1);

        assert_eq!(named.formula_class, FormulaClass::Rejected);
        assert_eq!(structured.formula_class, FormulaClass::Rejected);
        assert_eq!(three_d.formula_class, FormulaClass::Rejected);
        assert_eq!(external.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &named,
            &DependencyRejectReason::NamedRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(has_reason(
            &structured,
            &DependencyRejectReason::StructuredReferenceUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(has_reason(
            &three_d,
            &DependencyRejectReason::ThreeDReferenceUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(has_reason(
            &external,
            &DependencyRejectReason::ExternalReferenceUnsupported {
                context: AnalyzerContext::Value
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_reference_returning_operators() {
        let colon = summary("=(A1):(B1)", 1, 1);
        let union = summary("=(A1),(B1)", 1, 1);
        let intersection = summary("=A1:A3 B1:B3", 1, 1);

        for summary in [&colon, &union, &intersection] {
            assert_eq!(summary.formula_class, FormulaClass::Rejected);
            assert!(has_reason(
                summary,
                &DependencyRejectReason::ReferenceReturningUnsupported { function: None }
            ));
        }
        assert!(has_reason(
            &intersection,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Reference
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_reference_returning_functions() {
        let index = summary("=INDEX(A1:A3,1)", 1, 1);
        let choose = summary("=CHOOSE(1,A1,B1)", 1, 1);

        assert_eq!(index.formula_class, FormulaClass::Rejected);
        assert_eq!(choose.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &index,
            &DependencyRejectReason::ReferenceReturningUnsupported {
                function: Some("INDEX".to_string())
            }
        ));
        assert!(has_reason(
            &choose,
            &DependencyRejectReason::ReferenceReturningUnsupported {
                function: Some("CHOOSE".to_string())
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_pure_scalar_function_with_cell_arg() {
        let summary = summary("=ISNUMBER(A1)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(summary.precedent_patterns.len(), 1);
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_nested_pure_scalar_functions() {
        let summary = summary("=IF(ISNUMBER(A1), A1*2, 0)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(summary.precedent_patterns.len(), 1);
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_pure_scalar_function_with_no_refs() {
        let summary = summary("=ROUND(1.234, 2)", 1, 1);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert!(summary.precedent_patterns.is_empty());
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_abs_with_cell_arg() {
        let summary = summary("=ABS(A1)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_sum_range_not_pointwise_authority() {
        let summary = summary("=SUM(A1:A10)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(!has_reason(
            &summary,
            &DependencyRejectReason::FunctionUnsupported {
                name: "SUM".to_string()
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_whole_axis_references() {
        let direct_column = summary("=A:A", 1, 2);
        let direct_row = summary("=1:10", 1, 2);
        let function_arg = summary("=SUM(A:A)", 1, 2);

        assert_eq!(direct_column.formula_class, FormulaClass::Rejected);
        assert_eq!(direct_row.formula_class, FormulaClass::Rejected);
        assert_eq!(function_arg.formula_class, FormulaClass::Rejected);
        assert!(has_reason_kind(&direct_column, |reason| matches!(
            reason,
            DependencyRejectReason::WholeAxisUnsupported { .. }
        )));
        assert!(has_reason_kind(&direct_row, |reason| matches!(
            reason,
            DependencyRejectReason::WholeAxisUnsupported { .. }
        )));
        assert!(has_reason_kind(&function_arg, |reason| matches!(
            reason,
            DependencyRejectReason::WholeAxisUnsupported { .. }
        )));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_dynamic_dependencies() {
        let summary = summary("=INDIRECT(A1)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::DynamicDependency {
                function: Some("INDIRECT".to_string())
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_unknown_custom_functions() {
        let summary = summary("=CUSTOMFN(A1)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::UnknownFunction {
                name: "CUSTOMFN".to_string()
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_volatile_functions() {
        let summary = summary("=RAND()+A1", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::VolatileUnsupported {
                function: Some("RAND".to_string())
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_let_and_lambda_local_env() {
        let let_summary = summary("=LET(x,A1,x+1)", 1, 2);
        let lambda_summary = summary("=LAMBDA(x,x+1)", 1, 1);

        assert_eq!(let_summary.formula_class, FormulaClass::Rejected);
        assert_eq!(lambda_summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &let_summary,
            &DependencyRejectReason::LocalEnvUnsupported {
                function: Some("LET".to_string())
            }
        ));
        assert!(has_reason(
            &lambda_summary,
            &DependencyRejectReason::LocalEnvUnsupported {
                function: Some("LAMBDA".to_string())
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_spill_and_implicit_intersection() {
        let spill = summary("=A1#", 1, 1);
        let implicit = summary("=@A1", 1, 1);

        assert_eq!(spill.formula_class, FormulaClass::Rejected);
        assert_eq!(implicit.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &spill,
            &DependencyRejectReason::SpillUnsupported
        ));
        assert!(has_reason(
            &implicit,
            &DependencyRejectReason::ImplicitIntersectionUnsupported
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_rejects_unsupported_without_mismatch() {
        let range_ast = ast("=SUM(A1:A10)");
        let range_summary = summarize_dependencies(&range_ast, 1, 2);
        let named_ast = ast("=MyName");
        let named_summary = summarize_dependencies(&named_ast, 1, 1);
        let mut graph = DependencyGraph::new();

        let report = compare_dependency_summaries_to_fixed_planner(
            &mut graph,
            [
                DependencySummaryComparisonInput {
                    sheet: "Sheet1",
                    row: 1,
                    col: 2,
                    ast: &range_ast,
                    summary: &range_summary,
                },
                DependencySummaryComparisonInput {
                    sheet: "Sheet1",
                    row: 1,
                    col: 1,
                    ast: &named_ast,
                    summary: &named_summary,
                },
            ],
        )
        .expect("fixed planner comparison should build");

        assert_eq!(report.exact_match_count, 0);
        assert_eq!(report.over_approximation_count, 0);
        assert_eq!(report.under_approximation_count, 0);
        assert_eq!(report.rejection_count, 2);
        assert_eq!(report.policy_drift_count, 0);
        assert_eq!(
            report
                .fallback_reason_histogram
                .get("finite_range_unsupported"),
            Some(&1)
        );
        assert_eq!(
            report
                .fallback_reason_histogram
                .get("function_unsupported:SUM"),
            None
        );
        assert_eq!(
            report
                .fallback_reason_histogram
                .get("named_range_unsupported"),
            Some(&1)
        );
        assert!(report.has_no_under_approximations());
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_reports_policy_drift() {
        let formula_ast = ast("=A1+B1");
        let summary = summarize_dependencies(&formula_ast, 1, 3);
        let drift_policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit: 4,
            include_names: true,
        };
        let mut graph = DependencyGraph::new();

        let report = compare_dependency_summaries_to_planner_with_policy(
            &mut graph,
            [DependencySummaryComparisonInput {
                sheet: "Sheet1",
                row: 1,
                col: 3,
                ast: &formula_ast,
                summary: &summary,
            }],
            &drift_policy,
        )
        .expect("policy drift should be reported without planning");

        assert_eq!(report.exact_match_count, 0);
        assert_eq!(report.over_approximation_count, 0);
        assert_eq!(report.under_approximation_count, 0);
        assert_eq!(report.rejection_count, 0);
        assert_eq!(report.policy_drift_count, 1);
        assert_ne!(report.requested_policy, report.oracle_policy);
        assert_eq!(
            report.fallback_reason_histogram.get("collect_policy_drift"),
            Some(&1)
        );
    }

    #[test]
    fn formula_plane_dependency_summary_comparison_detects_under_approximation() {
        let formula_ast = ast("=A1+B1");
        let incomplete_summary = FormulaDependencySummary {
            formula_class: FormulaClass::StaticPointwise,
            precedent_patterns: vec![cell(
                SheetBinding::CurrentSheet,
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::RelativeToPlacement { offset: -2 },
            )],
            reject_reasons: Vec::new(),
        };

        let report = compare_one("Sheet1", 1, 3, &formula_ast, &incomplete_summary);

        assert_eq!(report.exact_match_count, 0);
        assert_eq!(report.over_approximation_count, 0);
        assert_eq!(report.under_approximation_count, 1);
        assert_eq!(report.rejection_count, 0);
        assert_eq!(report.policy_drift_count, 0);
        assert!(report.fallback_reason_histogram.is_empty());
        assert!(!report.has_no_under_approximations());
    }

    #[test]
    fn formula_plane_run_dependency_summary_instantiates_vertical_pointwise_regions() {
        let cells = (1..=100)
            .map(|row| candidate_cell(row, 3, "tpl_c_depends_a"))
            .collect::<Vec<_>>();
        let store = run_store(cells, 25);
        let summaries = template_summary_map(vec![("tpl_c_depends_a", summary("=A1", 1, 3))]);

        let arena = instantiate_run_dependency_summaries(&store, &summaries);

        assert_eq!(arena.counters.supported_run_summary_count, 1);
        assert_eq!(arena.counters.precedent_region_count, 1);
        assert_eq!(arena.counters.row_block_partition_count, 4);
        assert_eq!(arena.counters.result_excluded_cell_count, 0);
        assert_eq!(arena.run_summaries.len(), 1);

        let run_summary = &arena.run_summaries[0];
        assert_eq!(run_summary.shape, FormulaRunShape::Column);
        assert_eq!(
            run_summary.result_region.region,
            FiniteRegion::new("Sheet1", 1, 3, 100, 3)
        );
        assert_eq!(run_summary.result_region.shape, RegionShape::Column);
        assert_eq!(run_summary.result_region.included_cell_count, 100);
        assert_eq!(
            run_summary.precedent_regions[0].region.region,
            FiniteRegion::new("Sheet1", 1, 1, 100, 1)
        );
        assert_eq!(
            run_summary
                .partitions
                .iter()
                .map(|partition| partition.id.row_block_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            run_summary
                .partitions
                .iter()
                .map(|partition| partition.result_region.region.clone())
                .collect::<Vec<_>>(),
            vec![
                FiniteRegion::new("Sheet1", 1, 3, 25, 3),
                FiniteRegion::new("Sheet1", 26, 3, 50, 3),
                FiniteRegion::new("Sheet1", 51, 3, 75, 3),
                FiniteRegion::new("Sheet1", 76, 3, 100, 3),
            ]
        );
    }

    #[test]
    fn formula_plane_reverse_query_maps_changed_cell_to_dependent_partition() {
        let cells = (1..=100)
            .map(|row| candidate_cell(row, 3, "tpl_c_depends_a"))
            .collect::<Vec<_>>();
        let store = run_store(cells, 25);
        let summaries = template_summary_map(vec![("tpl_c_depends_a", summary("=A1", 1, 3))]);
        let mut arena = instantiate_run_dependency_summaries(&store, &summaries);

        let query = arena.query_changed_cell("Sheet1", 50, 1);

        assert!(!query.global_dirty_fallback);
        assert_eq!(query.dependent_partitions.len(), 1);
        let dependent = &query.dependent_partitions[0];
        assert_eq!(dependent.partition_id.row_block_index, 1);
        assert!(
            dependent
                .partition_result_region
                .contains_included_cell("Sheet1", 50, 3),
            "partition should contain C50"
        );
        assert_eq!(
            dependent.matched_result_regions,
            vec![FiniteRegion::cell("Sheet1", 50, 3)]
        );
        assert_eq!(dependent.exact_dependent_cell_count, 1);
        assert_eq!(dependent.partition_cell_count, 25);
        assert_eq!(dependent.overage_cell_count, 24);
        assert!(!dependent.is_exact);
        assert_eq!(arena.reverse_counters.reverse_query_count, 1);
        assert_eq!(arena.reverse_counters.reverse_exact_partition_count, 0);
        assert_eq!(
            arena.reverse_counters.reverse_conservative_partition_count,
            1
        );
        assert_eq!(arena.reverse_counters.reverse_max_overage, 24);
        assert_eq!(arena.reverse_counters.reverse_median_overage, 24);
        assert_eq!(arena.reverse_counters.global_dirty_fallback_count, 0);
    }

    #[test]
    fn formula_plane_run_dependency_summary_does_not_inherit_hole_cells() {
        let cells = vec![
            candidate_cell(1, 3, "tpl"),
            candidate_cell(2, 3, "tpl"),
            candidate_cell(4, 3, "tpl"),
            candidate_cell(5, 3, "tpl"),
        ];
        let store = run_store(cells, 10);
        let summaries = template_summary_map(vec![("tpl", summary("=A1", 1, 3))]);

        let arena = instantiate_run_dependency_summaries(&store, &summaries);

        assert_eq!(store.report.hole_count, 1);
        assert_eq!(arena.counters.supported_run_summary_count, 2);
        assert_eq!(arena.counters.result_excluded_cell_count, 0);
        assert_eq!(
            arena
                .run_summaries
                .iter()
                .map(|summary| summary.result_region.included_cell_count)
                .sum::<u64>(),
            4
        );
        assert!(
            arena
                .run_summaries
                .iter()
                .all(|summary| { !summary.result_region.contains_included_cell("Sheet1", 3, 3) })
        );
    }

    #[test]
    fn formula_plane_run_dependency_summary_does_not_inherit_exception_cells() {
        let cells = vec![
            candidate_cell(1, 3, "tpl"),
            candidate_cell(2, 3, "tpl"),
            candidate_cell(3, 3, "other"),
            candidate_cell(4, 3, "tpl"),
            candidate_cell(5, 3, "tpl"),
        ];
        let store = run_store(cells, 10);
        let summaries = template_summary_map(vec![("tpl", summary("=A1", 1, 3))]);

        let arena = instantiate_run_dependency_summaries(&store, &summaries);

        assert_eq!(store.report.exception_count, 1);
        assert_eq!(arena.counters.supported_run_summary_count, 2);
        assert_eq!(arena.counters.missing_template_summary_run_count, 1);
        assert_eq!(arena.rejected_runs.len(), 1);
        assert!(arena.run_summaries.iter().all(|summary| {
            summary.source_template_id != "tpl"
                || !summary.result_region.contains_included_cell("Sheet1", 3, 3)
        }));
    }

    #[test]
    fn formula_plane_run_dependency_summary_rejected_template_has_no_authority() {
        let cells = (1..=10)
            .map(|row| candidate_cell(row, 3, "bad_tpl"))
            .collect::<Vec<_>>();
        let store = run_store(cells, 10);
        let rejected_summary = summary("=SUM(A1:A10)", 1, 3);
        let summaries = template_summary_map(vec![("bad_tpl", rejected_summary)]);

        let arena = instantiate_run_dependency_summaries(&store, &summaries);

        assert!(arena.run_summaries.is_empty());
        assert_eq!(arena.counters.rejected_template_summary_run_count, 1);
        assert_eq!(arena.counters.rejected_run_summary_count, 1);
        assert!(matches!(
            arena.rejected_runs[0].reason,
            RunSummaryRejectionReason::TemplateRejected { .. }
        ));
    }

    #[test]
    fn formula_plane_run_dependency_summary_normalizes_row_block_partition_ids() {
        let cells = (1..=3)
            .map(|row| candidate_cell(row, 3, "tpl"))
            .collect::<Vec<_>>();
        let store = run_store(cells, 0);
        let summaries = template_summary_map(vec![("tpl", summary("=A1", 1, 3))]);

        let arena = instantiate_run_dependency_summaries(&store, &summaries);

        assert_eq!(store.row_block_size, 1);
        assert_eq!(arena.row_block_size, 1);
        let run_summary = &arena.run_summaries[0];
        assert_eq!(
            run_summary
                .partitions
                .iter()
                .map(|partition| (partition.id.run_id.0, partition.id.row_block_index))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1), (0, 2)]
        );
        let mut query_arena = arena.clone();
        let query = query_arena.query_changed_cell("Sheet1", 2, 1);
        assert_eq!(query.dependent_partitions.len(), 1);
        assert!(query.dependent_partitions[0].is_exact);
        assert_eq!(
            query_arena.reverse_counters.reverse_exact_partition_count,
            1
        );
    }

    #[test]
    fn formula_plane_run_dependency_summary_is_deterministic_for_shuffled_input() {
        let cells = (1..=20)
            .map(|row| candidate_cell(row, 3, "tpl"))
            .collect::<Vec<_>>();
        let summaries = template_summary_map(vec![("tpl", summary("=A1", 1, 3))]);

        let expected =
            instantiate_run_dependency_summaries(&run_store(cells.clone(), 7), &summaries);
        let reversed = instantiate_run_dependency_summaries(
            &run_store(cells.iter().cloned().rev().collect(), 7),
            &summaries,
        );
        let shuffled = instantiate_run_dependency_summaries(
            &run_store(shuffled_candidates(cells), 7),
            &summaries,
        );

        assert_eq!(expected, reversed);
        assert_eq!(expected, shuffled);
    }
}
