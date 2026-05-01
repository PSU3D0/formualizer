//! Unstable passive diagnostics for FormulaPlane scanner tooling.
//!
//! This module is only compiled with the non-default
//! `formula_plane_diagnostics` feature. It is not a runtime API, not a public
//! contract, and must remain a narrow sidecar over internal FormulaPlane
//! canonicalization.

use std::collections::{BTreeMap, BTreeSet};

use formualizer_common::ExcelError;
use formualizer_parse::parser::ASTNode;

use crate::engine::graph::DependencyGraph;

use super::dependency_summary::{
    DependencyCollectPolicyFingerprint, DependencySummaryComparisonInput,
    DependencySummaryComparisonReport, FormulaClass, FormulaDependencySummary,
    RunSummaryRejectionReason, compare_dependency_summaries_to_fixed_planner,
    dependency_reject_reason_key, instantiate_run_dependency_summaries, summarize_dependencies,
};
use super::span_store::FormulaRunStore;
use super::template_canonical::{
    CanonicalRejectKind, CanonicalRejectReason, CanonicalTemplateFlag, canonicalize_template,
};

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaPlaneTemplateDiagnostic {
    pub key_payload: String,
    pub stable_hash: u64,
    pub diagnostic_id: String,
    pub authority_supported: bool,
    pub flags: Vec<String>,
    pub reject_kinds: Vec<String>,
    pub reject_reasons: Vec<String>,
    pub expression_debug: String,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct FormulaPlaneDependencyScanInput<'a> {
    pub source_template_id: &'a str,
    pub authority_template_key: &'a str,
    pub sheet: &'a str,
    pub row: u32,
    pub col: u32,
    pub ast: &'a ASTNode,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaPlaneDependencySummariesDiagnostic {
    pub authority_template_count: u64,
    pub supported_template_count: u64,
    pub rejected_template_count: u64,
    pub run_summary_count: u64,
    pub precedent_region_count: u64,
    pub result_region_count: u64,
    pub reverse_summary_count: u64,
    pub comparison: FormulaPlaneDependencyComparisonDiagnostic,
    pub fallback_reasons: BTreeMap<String, u64>,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaPlaneDependencyComparisonDiagnostic {
    pub oracle_policy_name: &'static str,
    pub oracle_policy_fingerprint: FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic,
    pub requested_policy_fingerprint: FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic,
    pub exact_match_count: u64,
    pub over_approximation_count: u64,
    pub under_approximation_count: u64,
    pub rejection_count: u64,
    pub policy_drift_count: u64,
    pub fallback_reason_histogram: BTreeMap<String, u64>,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic {
    pub expand_small_ranges: bool,
    pub range_expansion_limit: usize,
    pub include_names: bool,
}

#[doc(hidden)]
pub fn canonical_template_diagnostic(
    ast: &ASTNode,
    anchor_row: u32,
    anchor_col: u32,
) -> FormulaPlaneTemplateDiagnostic {
    let template = canonicalize_template(ast, anchor_row, anchor_col);
    FormulaPlaneTemplateDiagnostic {
        key_payload: template.key.payload().to_string(),
        stable_hash: template.key.stable_hash(),
        diagnostic_id: template.key.diagnostic_id(),
        authority_supported: template.labels.is_authority_supported(),
        flags: template
            .labels
            .flags
            .iter()
            .map(template_flag_label)
            .map(str::to_string)
            .collect(),
        reject_kinds: template
            .labels
            .reject_reasons
            .iter()
            .map(|reason| reject_kind_label(reason.kind()))
            .map(str::to_string)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        reject_reasons: template
            .labels
            .reject_reasons
            .iter()
            .map(reject_reason_label)
            .collect(),
        expression_debug: format!("{:?}", template.expr),
    }
}

#[doc(hidden)]
pub fn dependency_summaries_diagnostic<'a, I>(
    run_store: &FormulaRunStore,
    inputs: I,
) -> Result<FormulaPlaneDependencySummariesDiagnostic, ExcelError>
where
    I: IntoIterator<Item = FormulaPlaneDependencyScanInput<'a>>,
{
    let inputs = inputs.into_iter().collect::<Vec<_>>();
    let mut summaries_by_authority_key = BTreeMap::<String, FormulaDependencySummary>::new();
    let mut source_to_authority_keys = BTreeMap::<String, BTreeSet<String>>::new();

    for input in &inputs {
        source_to_authority_keys
            .entry(input.source_template_id.to_string())
            .or_default()
            .insert(input.authority_template_key.to_string());
        summaries_by_authority_key
            .entry(input.authority_template_key.to_string())
            .or_insert_with(|| summarize_dependencies(input.ast, input.row, input.col));
    }

    let mut fallback_reasons = BTreeMap::<String, u64>::new();
    let supported_template_count = summaries_by_authority_key
        .values()
        .filter(|summary| dependency_summary_supported(summary))
        .count() as u64;
    for summary in summaries_by_authority_key.values() {
        if !dependency_summary_supported(summary) {
            record_summary_rejection(&mut fallback_reasons, summary, 1);
        }
    }

    let mut summaries_by_source = BTreeMap::<String, FormulaDependencySummary>::new();
    for (source_template_id, authority_keys) in &source_to_authority_keys {
        if authority_keys.len() == 1 {
            let authority_key = authority_keys.iter().next().expect("one authority key");
            if let Some(summary) = summaries_by_authority_key.get(authority_key) {
                summaries_by_source.insert(source_template_id.clone(), summary.clone());
            } else {
                record_count(&mut fallback_reasons, "authority_template_missing", 1);
            }
            continue;
        }

        let affected_runs = run_store
            .runs
            .iter()
            .filter(|run| run.source_template_id == *source_template_id)
            .count() as u64;
        record_count(
            &mut fallback_reasons,
            "diagnostic_source_template_collision",
            affected_runs.max(1),
        );
    }

    let run_arena = instantiate_run_dependency_summaries(run_store, &summaries_by_source);
    for rejection in &run_arena.rejected_runs {
        record_run_rejection(&mut fallback_reasons, &rejection.reason);
    }

    let mut comparison_inputs = Vec::new();
    for input in &inputs {
        let Some(authority_keys) = source_to_authority_keys.get(input.source_template_id) else {
            continue;
        };
        if authority_keys.len() != 1 {
            continue;
        }
        let authority_key = authority_keys.iter().next().expect("one authority key");
        let Some(summary) = summaries_by_authority_key.get(authority_key) else {
            continue;
        };
        comparison_inputs.push(DependencySummaryComparisonInput {
            sheet: input.sheet,
            row: input.row,
            col: input.col,
            ast: input.ast,
            summary,
        });
    }

    let mut graph = DependencyGraph::new();
    let comparison_report =
        compare_dependency_summaries_to_fixed_planner(&mut graph, comparison_inputs)?;

    Ok(FormulaPlaneDependencySummariesDiagnostic {
        authority_template_count: summaries_by_authority_key.len() as u64,
        supported_template_count,
        rejected_template_count: summaries_by_authority_key
            .len()
            .saturating_sub(supported_template_count as usize)
            as u64,
        run_summary_count: run_arena.counters.supported_run_summary_count,
        precedent_region_count: run_arena.counters.precedent_region_count,
        result_region_count: run_arena.counters.result_region_count,
        reverse_summary_count: run_arena.counters.row_block_partition_count,
        comparison: comparison_diagnostic(&comparison_report),
        fallback_reasons,
    })
}

fn comparison_diagnostic(
    report: &DependencySummaryComparisonReport,
) -> FormulaPlaneDependencyComparisonDiagnostic {
    FormulaPlaneDependencyComparisonDiagnostic {
        oracle_policy_name: report.oracle_policy_name,
        oracle_policy_fingerprint: policy_fingerprint_diagnostic(report.oracle_policy),
        requested_policy_fingerprint: policy_fingerprint_diagnostic(report.requested_policy),
        exact_match_count: report.exact_match_count,
        over_approximation_count: report.over_approximation_count,
        under_approximation_count: report.under_approximation_count,
        rejection_count: report.rejection_count,
        policy_drift_count: report.policy_drift_count,
        fallback_reason_histogram: report.fallback_reason_histogram.clone(),
    }
}

fn policy_fingerprint_diagnostic(
    fingerprint: DependencyCollectPolicyFingerprint,
) -> FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic {
    FormulaPlaneDependencyCollectPolicyFingerprintDiagnostic {
        expand_small_ranges: fingerprint.expand_small_ranges,
        range_expansion_limit: fingerprint.range_expansion_limit,
        include_names: fingerprint.include_names,
    }
}

fn dependency_summary_supported(summary: &FormulaDependencySummary) -> bool {
    summary.formula_class == FormulaClass::StaticPointwise && summary.reject_reasons.is_empty()
}

fn record_summary_rejection(
    fallback_reasons: &mut BTreeMap<String, u64>,
    summary: &FormulaDependencySummary,
    count: u64,
) {
    if summary.reject_reasons.is_empty() {
        record_count(fallback_reasons, "summary_rejected_without_reason", count);
        return;
    }

    for reason in &summary.reject_reasons {
        record_count(
            fallback_reasons,
            dependency_reject_reason_key(reason),
            count,
        );
    }
}

fn record_run_rejection(
    fallback_reasons: &mut BTreeMap<String, u64>,
    reason: &RunSummaryRejectionReason,
) {
    match reason {
        RunSummaryRejectionReason::MissingTemplateSummary => {
            record_count(fallback_reasons, "missing_template_summary", 1)
        }
        RunSummaryRejectionReason::TemplateRejected {
            formula_class: _,
            reject_reasons,
        } if reject_reasons.is_empty() => {
            record_count(fallback_reasons, "summary_rejected_without_reason", 1)
        }
        RunSummaryRejectionReason::TemplateRejected {
            formula_class: _,
            reject_reasons,
        } => {
            for reason in reject_reasons {
                record_count(fallback_reasons, dependency_reject_reason_key(reason), 1);
            }
        }
        RunSummaryRejectionReason::InvalidRunRegion => {
            record_count(fallback_reasons, "invalid_run_region", 1)
        }
        RunSummaryRejectionReason::NonFiniteAxis { .. } => {
            record_count(fallback_reasons, "summary_non_finite_axis", 1)
        }
        RunSummaryRejectionReason::InvalidAxisCoordinate { .. } => {
            record_count(fallback_reasons, "summary_invalid_axis_coordinate", 1)
        }
        RunSummaryRejectionReason::TooManyExcludedCells { .. } => {
            record_count(fallback_reasons, "too_many_excluded_cells", 1)
        }
        RunSummaryRejectionReason::EmptyResultAfterExclusions => {
            record_count(fallback_reasons, "empty_result_after_exclusions", 1)
        }
        RunSummaryRejectionReason::ReverseGlobalFallbackRequired => {
            record_count(fallback_reasons, "reverse_global_fallback_required", 1)
        }
    }
}

fn record_count(
    fallback_reasons: &mut BTreeMap<String, u64>,
    reason: impl Into<String>,
    count: u64,
) {
    if count == 0 {
        return;
    }
    *fallback_reasons.entry(reason.into()).or_default() += count;
}

fn template_flag_label(flag: &CanonicalTemplateFlag) -> &'static str {
    match flag {
        CanonicalTemplateFlag::ParserVolatileFlag => "parser_volatile",
        CanonicalTemplateFlag::FunctionCall => "function_call",
        CanonicalTemplateFlag::CurrentSheetBinding => "current_sheet_binding",
        CanonicalTemplateFlag::ExplicitSheetBinding => "explicit_sheet_binding",
        CanonicalTemplateFlag::RelativeReferenceAxis => "relative_reference_axis",
        CanonicalTemplateFlag::AbsoluteReferenceAxis => "absolute_reference_axis",
        CanonicalTemplateFlag::MixedAnchors => "mixed_anchors",
        CanonicalTemplateFlag::FiniteRangeReference => "finite_range_reference",
    }
}

fn reject_kind_label(kind: CanonicalRejectKind) -> &'static str {
    match kind {
        CanonicalRejectKind::InvalidPlacementAnchor => "invalid_placement_anchor",
        CanonicalRejectKind::DynamicReference => "dynamic_reference",
        CanonicalRejectKind::UnknownOrCustomFunction => "unknown_or_custom_function",
        CanonicalRejectKind::LocalEnvironment => "local_environment",
        CanonicalRejectKind::VolatileFunction => "volatile_function",
        CanonicalRejectKind::ReferenceReturningFunction => "reference_returning_function",
        CanonicalRejectKind::ArrayOrSpill => "array_or_spill",
        CanonicalRejectKind::ArrayLiteral => "array_literal",
        CanonicalRejectKind::SpillReference => "spill_reference",
        CanonicalRejectKind::ImplicitIntersection => "implicit_intersection",
        CanonicalRejectKind::CallExpression => "call_expression",
        CanonicalRejectKind::NamedReference => "named_reference",
        CanonicalRejectKind::StructuredReference => "structured_reference",
        CanonicalRejectKind::StructuredReferenceCurrentRow => "structured_reference_current_row",
        CanonicalRejectKind::ThreeDReference => "three_d_reference",
        CanonicalRejectKind::ExternalReference => "external_reference",
        CanonicalRejectKind::OpenRangeReference => "open_range_reference",
        CanonicalRejectKind::WholeAxisReference => "whole_axis_reference",
        CanonicalRejectKind::UnsupportedReference => "unsupported_reference",
    }
}

fn reject_reason_label(reason: &CanonicalRejectReason) -> String {
    match reason {
        CanonicalRejectReason::InvalidPlacementAnchor { row, col } => {
            format!("invalid_placement_anchor:row={row}:col={col}")
        }
        CanonicalRejectReason::DynamicReferenceFunction { name } => {
            format!("dynamic_reference_function:{name}")
        }
        CanonicalRejectReason::UnknownOrCustomFunction { name } => {
            format!("unknown_or_custom_function:{name}")
        }
        CanonicalRejectReason::LocalEnvironmentFunction { name } => {
            format!("local_environment_function:{name}")
        }
        CanonicalRejectReason::ParserVolatileFlag => "parser_volatile_flag".to_string(),
        CanonicalRejectReason::VolatileFunction { name } => format!("volatile_function:{name}"),
        CanonicalRejectReason::ReferenceReturningFunction { name } => {
            format!("reference_returning_function:{name}")
        }
        CanonicalRejectReason::ArrayOrSpillFunction { name } => {
            format!("array_or_spill_function:{name}")
        }
        CanonicalRejectReason::ArrayLiteral => "array_literal".to_string(),
        CanonicalRejectReason::SpillReference { original } => format!("spill_reference:{original}"),
        CanonicalRejectReason::SpillResultRegionOperator => {
            "spill_result_region_operator".to_string()
        }
        CanonicalRejectReason::ImplicitIntersectionOperator => {
            "implicit_intersection_operator".to_string()
        }
        CanonicalRejectReason::CallExpression => "call_expression".to_string(),
        CanonicalRejectReason::NamedReference { name } => format!("named_reference:{name}"),
        CanonicalRejectReason::StructuredReference { diagnostic } => {
            format!("structured_reference:{diagnostic}")
        }
        CanonicalRejectReason::StructuredReferenceCurrentRow { diagnostic } => {
            format!("structured_reference_current_row:{diagnostic}")
        }
        CanonicalRejectReason::ThreeDReference { diagnostic } => {
            format!("three_d_reference:{diagnostic}")
        }
        CanonicalRejectReason::ExternalReference { diagnostic } => {
            format!("external_reference:{diagnostic}")
        }
        CanonicalRejectReason::OpenRangeReference { original } => {
            format!("open_range_reference:{original}")
        }
        CanonicalRejectReason::WholeAxisReference { original } => {
            format!("whole_axis_reference:{original}")
        }
        CanonicalRejectReason::UnsupportedReference { diagnostic } => {
            format!("unsupported_reference:{diagnostic}")
        }
    }
}
