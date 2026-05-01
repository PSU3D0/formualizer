//! Passive FormulaPlane dependency summaries for FP4.A.3.
//!
//! This module is crate-internal and read-only. It classifies a narrow initial
//! scalar template subset and records explicit rejection reasons for everything
//! outside that subset; it does not change graph, scheduler, dirty, loader, or
//! evaluation behavior.

use std::collections::BTreeSet;

use formualizer_parse::parser::ASTNode;

use super::template_canonical::{
    AxisRef, CanonicalExpr, CanonicalReference, CanonicalReferenceContext, CanonicalRejectReason,
    CanonicalTemplate, SheetBinding, UnsupportedReferenceKind, canonicalize_template,
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
                for (arg_index, arg) in args.iter().enumerate() {
                    self.analyze_expr(arg, function_arg_context(&id.canonical_name, arg_index));
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
        "+" | "-" | "*" | "/" | "^" | "=" | "<>" | "<" | "<=" | ">" | ">="
    )
}

fn is_reference_returning_binary_operator(op: &str) -> bool {
    matches!(op, ":" | ",")
}

fn is_array_or_spill_function(name: &str) -> bool {
    matches!(
        name,
        "FILTER" | "RANDARRAY" | "SEQUENCE" | "SORT" | "SORTBY" | "TEXTSPLIT" | "UNIQUE"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_parse::parse;

    fn summary(formula: &str, row: u32, col: u32) -> FormulaDependencySummary {
        let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
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
    fn formula_plane_dependency_summary_rejects_sum_range_not_pointwise_authority() {
        let summary = summary("=SUM(A1:A10)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::FunctionUnsupported {
                name: "SUM".to_string()
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_whole_axis_references() {
        let direct = summary("=A:A", 1, 2);
        let function_arg = summary("=SUM(A:A)", 1, 2);

        assert_eq!(direct.formula_class, FormulaClass::Rejected);
        assert_eq!(function_arg.formula_class, FormulaClass::Rejected);
        assert!(has_reason_kind(&direct, |reason| matches!(
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
}
