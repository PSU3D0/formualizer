//! Unstable passive diagnostics for FormulaPlane scanner tooling.
//!
//! This module is only compiled with the non-default
//! `formula_plane_diagnostics` feature. It is not a runtime API, not a public
//! contract, and must remain a narrow sidecar over internal FormulaPlane
//! canonicalization.

use formualizer_parse::parser::ASTNode;

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
