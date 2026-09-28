//! Passive FormulaPlane dependency summaries for FP4.A.3 and FP4.A.4.
//!
//! This module is crate-internal and read-only. It classifies a narrow initial
//! scalar template subset and records explicit rejection reasons for everything
//! outside that subset; it does not change graph, scheduler, dirty, loader, or
//! evaluation behavior.

use std::collections::BTreeSet;

use super::canonical::{
    AxisRef, CanonicalExpr, CanonicalFunctionId, CanonicalReference, CanonicalReferenceContext,
    CanonicalRejectReason, CanonicalTemplate, SheetBinding, UnsupportedReferenceKind,
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
    CriteriaExpressionArg,
    CriteriaRangeArg,
    ImplicitIntersection,
    LocalBinding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaDependencySummary {
    pub(crate) formula_class: FormulaClass,
    pub(crate) precedent_patterns: Vec<PrecedentPattern>,
    pub(crate) reject_reasons: Vec<DependencyRejectReason>,
}

impl FormulaDependencySummary {
    /// True if every precedent axis is invariant with placement. Such formulas
    /// produce the same value at every placement.
    #[cfg(test)]
    pub(crate) fn is_constant_result(&self) -> bool {
        self.precedent_patterns.iter().all(|pattern| match pattern {
            PrecedentPattern::Cell(cell) => {
                axis_is_placement_invariant(&cell.row) && axis_is_placement_invariant(&cell.col)
            }
            PrecedentPattern::Range(rect) => {
                axis_is_placement_invariant(&rect.start_row)
                    && axis_is_placement_invariant(&rect.end_row)
                    && axis_is_placement_invariant(&rect.start_col)
                    && axis_is_placement_invariant(&rect.end_col)
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PrecedentPattern {
    Cell(AffineCellPattern),
    Range(AffineRectPattern),
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
    MixedAxisRangeUnsupported { context: AnalyzerContext },
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

pub(crate) fn summarize_canonical_template(
    template: &CanonicalTemplate,
) -> FormulaDependencySummary {
    let mut analyzer = SummaryAnalyzer::default();
    analyzer.add_canonical_reasons(template.labels.reject_reasons.iter());
    let expr_supported = analyzer.analyze_expr(&template.expr, AnalyzerContext::Value, false);
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
            CanonicalRejectReason::StructuredReference { .. }
            | CanonicalRejectReason::StructuredReferenceCurrentRow { .. }
            | CanonicalRejectReason::ThreeDReference { .. }
            | CanonicalRejectReason::ExternalReference { .. }
            | CanonicalRejectReason::OpenRangeReference { .. }
            | CanonicalRejectReason::WholeAxisReference { .. }
            | CanonicalRejectReason::UnsupportedReference { .. } => return,
            CanonicalRejectReason::FunctionContractUnsupported { name }
            | CanonicalRejectReason::ContextDependentFunction { name } => {
                DependencyRejectReason::FunctionUnsupported { name: name.clone() }
            }
        };
        self.reasons.insert(dependency_reason);
    }

    fn analyze_expr(
        &mut self,
        expr: &CanonicalExpr,
        context: AnalyzerContext,
        in_function_arg: bool,
    ) -> bool {
        match expr {
            CanonicalExpr::Literal(_) | CanonicalExpr::Omitted => matches!(
                context,
                AnalyzerContext::Value | AnalyzerContext::CriteriaExpressionArg
            ),
            CanonicalExpr::Reference {
                context: reference_context,
                reference,
            } => {
                let context = effective_context(context, reference_context);
                self.analyze_reference(reference, context, in_function_arg)
            }
            CanonicalExpr::Unary { op, expr } => {
                self.analyze_unary(op, expr, context, in_function_arg)
            }
            CanonicalExpr::Binary { op, left, right } => {
                self.analyze_binary(op, left, right, context, in_function_arg)
            }
            CanonicalExpr::Function { id, args } => {
                let mut all_args_supported = true;
                for (arg_index, arg) in args.iter().enumerate() {
                    if !self.analyze_expr(arg, function_argument_context(id, arg_index), true) {
                        all_args_supported = false;
                    }
                }
                if id.contract.is_none() || self.has_function_specific_rejection(&id.canonical_name)
                {
                    return false;
                }
                if all_args_supported && matches!(context, AnalyzerContext::Value) {
                    true
                } else {
                    if self.reasons.is_empty() {
                        self.reject_function(&id.canonical_name);
                    }
                    false
                }
            }
            CanonicalExpr::CallUnsupported { callee, args } => {
                self.analyze_expr(callee, AnalyzerContext::Value, false);
                for arg in args {
                    self.analyze_expr(arg, AnalyzerContext::Value, false);
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
                        self.analyze_expr(item, AnalyzerContext::Value, false);
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

    fn analyze_unary(
        &mut self,
        op: &str,
        expr: &CanonicalExpr,
        context: AnalyzerContext,
        in_function_arg: bool,
    ) -> bool {
        match op {
            "+" | "-" | "%" => self.analyze_expr(expr, context, in_function_arg),
            "#" => {
                self.analyze_expr(expr, AnalyzerContext::Value, in_function_arg);
                self.reasons
                    .insert(DependencyRejectReason::SpillUnsupported);
                false
            }
            "@" => {
                self.analyze_expr(expr, AnalyzerContext::ImplicitIntersection, in_function_arg);
                self.reasons
                    .insert(DependencyRejectReason::ImplicitIntersectionUnsupported);
                false
            }
            _ => {
                self.analyze_expr(expr, context, in_function_arg);
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
        in_function_arg: bool,
    ) -> bool {
        if is_supported_pointwise_binary_operator(op) {
            let left_supported = self.analyze_expr(left, context, in_function_arg);
            let right_supported = self.analyze_expr(right, context, in_function_arg);
            left_supported && right_supported
        } else if is_reference_returning_binary_operator(op) {
            self.analyze_expr(left, AnalyzerContext::Reference, in_function_arg);
            self.analyze_expr(right, AnalyzerContext::Reference, in_function_arg);
            self.reasons
                .insert(DependencyRejectReason::ReferenceReturningUnsupported { function: None });
            false
        } else {
            self.analyze_expr(left, context, in_function_arg);
            self.analyze_expr(right, context, in_function_arg);
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
        in_function_arg: bool,
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
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                if !self.reject_non_finite_range(context, [start_row, start_col, end_row, end_col])
                {
                    return false;
                }
                if !range_axis_kinds_supported(start_row, end_row, start_col, end_col) {
                    self.reasons
                        .insert(DependencyRejectReason::MixedAxisRangeUnsupported { context });
                    return false;
                }
                if in_function_arg
                    && matches!(
                        context,
                        AnalyzerContext::Value | AnalyzerContext::CriteriaRangeArg
                    )
                {
                    self.push_precedent(PrecedentPattern::Range(AffineRectPattern {
                        sheet: sheet.clone(),
                        start_row: start_row.clone(),
                        start_col: start_col.clone(),
                        end_row: end_row.clone(),
                        end_col: end_col.clone(),
                    }));
                    true
                } else {
                    self.reasons
                        .insert(DependencyRejectReason::FiniteRangeUnsupported { context });
                    false
                }
            }
            CanonicalReference::Named { .. } => {
                // The summary layer is passive classification: it has no
                // registry access and cannot resolve a defined name to a
                // concrete region, so it conservatively rejects here. The
                // authoritative accept decision for named formulas is driven
                // by the per-cell read projections computed at ingest, which
                // resolve names against the live registry.
                let reason = if matches!(context, AnalyzerContext::LocalBinding) {
                    DependencyRejectReason::LocalEnvUnsupported { function: None }
                } else {
                    DependencyRejectReason::NamedRangeUnsupported { context }
                };
                self.reasons.insert(reason);
                false
            }
            CanonicalReference::Unsupported { kind, diagnostic } => {
                self.reject_unsupported_reference(kind, diagnostic, context);
                false
            }
        }
    }

    fn reject_non_finite_range<'a>(
        &mut self,
        context: AnalyzerContext,
        axes: impl IntoIterator<Item = &'a AxisRef>,
    ) -> bool {
        let mut has_open = false;
        let mut has_unsupported = false;
        for axis in axes {
            match axis {
                AxisRef::OpenStart | AxisRef::OpenEnd => has_open = true,
                AxisRef::Unsupported => has_unsupported = true,
                AxisRef::WholeAxis
                | AxisRef::RelativeToPlacement { .. }
                | AxisRef::AbsoluteVc { .. } => {}
            }
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
        !has_open && !has_unsupported
    }

    fn reject_unsupported_reference(
        &mut self,
        kind: &UnsupportedReferenceKind,
        diagnostic: &str,
        context: AnalyzerContext,
    ) {
        let reason = match kind {
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
            DependencyRejectReason::SpillUnsupported => true,
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
        } => function_argument_context(function, *arg_index),
        CanonicalReferenceContext::CallArgument { .. } => AnalyzerContext::Value,
    }
}

pub(crate) fn function_argument_context(
    function: &CanonicalFunctionId,
    arg_index: usize,
) -> AnalyzerContext {
    use crate::function_contract::{
        CriteriaValueRange, FunctionArgumentDependencyContract as Arguments,
        FunctionArgumentDependencyRole as Role,
    };
    let Some(precision) = function.contract.and_then(|contract| contract.precision) else {
        return AnalyzerContext::Value;
    };
    match precision.arguments {
        Arguments::AllArgs(role) | Arguments::Variadic(role) => match role {
            Role::CriteriaRange => AnalyzerContext::CriteriaRangeArg,
            Role::CriteriaExpression => AnalyzerContext::CriteriaExpressionArg,
            Role::ByReference => AnalyzerContext::ByRefArg,
            Role::LocalBindingName | Role::LocalBindingValue | Role::LambdaBody => {
                AnalyzerContext::LocalBinding
            }
            _ => AnalyzerContext::Value,
        },
        Arguments::LocalBindingPairs | Arguments::LambdaParameters => AnalyzerContext::LocalBinding,
        Arguments::CriteriaPairs(criteria) => {
            let value_index = match criteria.value_range {
                CriteriaValueRange::Fixed(index) => Some(index),
                CriteriaValueRange::Optional { provided_index, .. } => Some(provided_index),
                CriteriaValueRange::None => None,
            };
            if value_index == Some(arg_index) || arg_index < criteria.first_criteria_pair {
                AnalyzerContext::Value
            } else if (arg_index - criteria.first_criteria_pair).is_multiple_of(2) {
                AnalyzerContext::CriteriaRangeArg
            } else {
                AnalyzerContext::CriteriaExpressionArg
            }
        }
    }
}

fn axis_is_finite_cell(axis: &AxisRef) -> bool {
    matches!(
        axis,
        AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. }
    )
}

/// Range bound combinations a precedent pattern can carry.
///
/// All-finite ranges may mix placement-relative and absolute bounds freely
/// within an axis: mixed-anchor ranges like `$A$2:$A{r}` (running total) and
/// `$A{r}:$A$N` (tail read) are affine in the placement index per bound, so
/// every downstream consumer (region instantiation, forward read-region
/// projection, inverse dirty projection) handles them per bound.
///
/// Ranges involving `WholeAxis` keep the stricter per-axis kind uniformity:
/// `A:A` is fine, but `A:$A` (whole-column with mixed column anchors) stays
/// rejected — the ingest read-projection path does not model it either, and
/// accepting it here only would make the two analyses disagree.
fn range_axis_kinds_supported(
    start_row: &AxisRef,
    end_row: &AxisRef,
    start_col: &AxisRef,
    end_col: &AxisRef,
) -> bool {
    let any_whole = [start_row, end_row, start_col, end_col]
        .iter()
        .any(|axis| matches!(axis, AxisRef::WholeAxis));
    if any_whole {
        axis_kinds_match(start_row, end_row) && axis_kinds_match(start_col, end_col)
    } else {
        // Open/unsupported bounds were rejected by `reject_non_finite_range`;
        // every remaining bound is RelativeToPlacement or AbsoluteVc and any
        // mixture is supported.
        true
    }
}

fn axis_kinds_match(left: &AxisRef, right: &AxisRef) -> bool {
    matches!(
        (left, right),
        (
            AxisRef::RelativeToPlacement { .. },
            AxisRef::RelativeToPlacement { .. }
        ) | (AxisRef::AbsoluteVc { .. }, AxisRef::AbsoluteVc { .. })
            | (AxisRef::WholeAxis, AxisRef::WholeAxis)
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

#[cfg(test)]
pub(crate) fn summarize_dependencies(
    ast: &formualizer_parse::parser::ASTNode,
    anchor_row: u32,
    anchor_col: u32,
) -> FormulaDependencySummary {
    let template = super::canonical::canonicalize_template(ast, anchor_row, anchor_col);
    summarize_canonical_template(&template)
}
#[cfg(test)]
pub(crate) fn dependency_reject_reason_key(reason: &DependencyRejectReason) -> String {
    match reason {
        DependencyRejectReason::OpenRangeUnsupported { .. } => "open_range_unsupported".to_string(),
        DependencyRejectReason::WholeAxisUnsupported { .. } => "whole_axis_unsupported".to_string(),
        DependencyRejectReason::FiniteRangeUnsupported { .. } => {
            "finite_range_unsupported".to_string()
        }
        DependencyRejectReason::MixedAxisRangeUnsupported { .. } => {
            "mixed_axis_range_unsupported".to_string()
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
#[cfg(test)]
fn optional_function_key(prefix: &str, function: &Option<String>) -> String {
    match function {
        Some(function) => format!("{prefix}:{function}"),
        None => prefix.to_string(),
    }
}

#[cfg(test)]
fn axis_is_placement_invariant(axis: &AxisRef) -> bool {
    matches!(axis, AxisRef::AbsoluteVc { .. } | AxisRef::WholeAxis)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use formualizer_parse::parse;
    use formualizer_parse::parser::ASTNode;

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

    fn range(
        sheet: SheetBinding,
        start_row: AxisRef,
        start_col: AxisRef,
        end_row: AxisRef,
        end_col: AxisRef,
    ) -> PrecedentPattern {
        PrecedentPattern::Range(AffineRectPattern {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        })
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

    fn template_summary_map(
        entries: Vec<(&str, FormulaDependencySummary)>,
    ) -> BTreeMap<String, FormulaDependencySummary> {
        entries
            .into_iter()
            .map(|(source_template_id, summary)| (source_template_id.to_string(), summary))
            .collect()
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
    fn formula_plane_dependency_summary_accepts_cross_sheet_relative_cell() {
        let summary = summary("=Sheet2!A1", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(
            summary.precedent_patterns,
            vec![cell(
                SheetBinding::ExplicitName {
                    name: "Sheet2".to_string(),
                },
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::RelativeToPlacement { offset: -1 }
            )]
        );
        assert!(summary.reject_reasons.is_empty());
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
    fn formula_plane_dependency_summary_accepts_cell_armed_reference_returning_functions() {
        for formula in ["=IF(A1,B1,C1)", "=IFS(A1,B1,A2,C1)", "=CHOOSE(A1,B1,C1)"] {
            let summary = summary(formula, 1, 4);
            assert_eq!(
                summary.formula_class,
                FormulaClass::StaticPointwise,
                "{formula}"
            );
            assert!(summary.reject_reasons.is_empty(), "{formula}: {summary:?}");
            assert!(
                summary.precedent_patterns.len() >= 3,
                "{formula}: {summary:?}"
            );
        }
    }

    #[test]
    fn reference_returning_admission_firewall_preserves_reject_reason_strings() {
        let cases: [(&str, &[&str]); 9] = [
            (
                "=IF(TRUE,A1:A3,0)",
                &["reference_returning_unsupported:IF", "spill_unsupported"],
            ),
            (
                "=CHOOSE(2,A1,B1:B3)",
                &[
                    "reference_returning_unsupported:CHOOSE",
                    "spill_unsupported",
                ],
            ),
            (
                "=IF(TRUE,NOW(),0)",
                &[
                    "volatile_unsupported:NOW",
                    "reference_returning_unsupported:IF",
                    "spill_unsupported",
                ],
            ),
            (
                "=IF(TRUE,OFFSET(A1,1,0),0)",
                &[
                    "dynamic_dependency:OFFSET",
                    "volatile_unsupported:OFFSET",
                    "reference_returning_unsupported:IF",
                    "reference_returning_unsupported:OFFSET",
                    "spill_unsupported",
                ],
            ),
            (
                "=IF(TRUE,INDIRECT(\"A1\"),0)",
                &[
                    "dynamic_dependency:INDIRECT",
                    "volatile_unsupported:INDIRECT",
                    "reference_returning_unsupported:IF",
                    "reference_returning_unsupported:INDIRECT",
                    "spill_unsupported",
                ],
            ),
            (
                "=IF(TRUE,INDEX(A1:A3,1),0)",
                &[
                    "reference_returning_unsupported:IF",
                    "reference_returning_unsupported:INDEX",
                    "spill_unsupported",
                ],
            ),
            (
                "=IF(TRUE,MyName,0)",
                &[
                    "named_range_unsupported",
                    "reference_returning_unsupported:IF",
                    "spill_unsupported",
                ],
            ),
            (
                "=IF(TRUE,SUM($A$1:$A),0)",
                &[
                    "open_range_unsupported",
                    "reference_returning_unsupported:IF",
                    "spill_unsupported",
                ],
            ),
            (
                "=(IF(TRUE,A1,B1)):(C1)",
                &["reference_returning_unsupported"],
            ),
        ];

        for (formula, expected) in cases {
            let summary = summary(formula, 4, 6);
            assert_eq!(summary.formula_class, FormulaClass::Rejected, "{formula}");
            let actual = summary
                .reject_reasons
                .iter()
                .map(dependency_reject_reason_key)
                .collect::<BTreeSet<_>>();
            for reason in expected {
                assert!(
                    actual.contains(*reason),
                    "{formula}: missing {reason}; {actual:?}"
                );
            }
        }
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_reference_capable_index() {
        let summary = summary("=INDEX(A1:A3,1)", 1, 1);

        assert_eq!(summary.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &summary,
            &DependencyRejectReason::ReferenceReturningUnsupported {
                function: Some("INDEX".to_string())
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
    fn formula_plane_dependency_summary_accepts_scalar_armed_short_circuit_function() {
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
    fn formula_plane_dependency_summary_accepts_sum_range_function_arg() {
        let summary = summary("=SUM(A1:A10)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(
            summary.precedent_patterns,
            vec![range(
                SheetBinding::CurrentSheet,
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::RelativeToPlacement { offset: -1 },
                AxisRef::RelativeToPlacement { offset: 9 },
                AxisRef::RelativeToPlacement { offset: -1 },
            )]
        );
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_absolute_sum_range_function_arg() {
        let summary = summary("=SUM($A$1:$A$10)", 20, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(
            summary.precedent_patterns,
            vec![range(
                SheetBinding::CurrentSheet,
                AxisRef::AbsoluteVc { index: 1 },
                AxisRef::AbsoluteVc { index: 1 },
                AxisRef::AbsoluteVc { index: 10 },
                AxisRef::AbsoluteVc { index: 1 },
            )]
        );
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_absolute_whole_column_sum() {
        let summary = summary("=SUM($A:$A)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(
            summary.precedent_patterns,
            vec![range(
                SheetBinding::CurrentSheet,
                AxisRef::WholeAxis,
                AxisRef::AbsoluteVc { index: 1 },
                AxisRef::WholeAxis,
                AxisRef::AbsoluteVc { index: 1 },
            )]
        );
        assert!(summary.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_classifies_whole_column_constant_by_axes() {
        let absolute_with_relative_cell = summary("=SUM($A:$A)-A1", 1, 2);
        assert_eq!(
            absolute_with_relative_cell.formula_class,
            FormulaClass::StaticPointwise
        );
        assert!(absolute_with_relative_cell.reject_reasons.is_empty());
        assert!(!absolute_with_relative_cell.is_constant_result());

        let relative_whole_column = summary("=SUM(A:A)", 1, 2);
        assert_eq!(
            relative_whole_column.formula_class,
            FormulaClass::StaticPointwise
        );
        assert!(relative_whole_column.reject_reasons.is_empty());
        assert_eq!(
            relative_whole_column.precedent_patterns,
            vec![range(
                SheetBinding::CurrentSheet,
                AxisRef::WholeAxis,
                AxisRef::RelativeToPlacement { offset: -1 },
                AxisRef::WholeAxis,
                AxisRef::RelativeToPlacement { offset: -1 },
            )]
        );
        assert!(!relative_whole_column.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_rejects_open_top_level_and_mixed_whole_column_ranges() {
        let open = summary("=SUM($A$1:$A)", 1, 2);
        assert_eq!(open.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &open,
            &DependencyRejectReason::OpenRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));

        let top_level = summary("=$A:$A", 1, 2);
        assert_eq!(top_level.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &top_level,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));

        let mixed = summary("=SUM(A:$A)", 1, 2);
        assert_eq!(mixed.formula_class, FormulaClass::Rejected);
        assert!(has_reason(
            &mixed,
            &DependencyRejectReason::MixedAxisRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_average_range_and_cell() {
        let summary = summary("=AVERAGE($A$1:$A$50) * B1", 1, 3);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(summary.precedent_patterns.len(), 2);
        assert!(matches!(
            summary.precedent_patterns[0],
            PrecedentPattern::Range(_)
        ));
        assert!(matches!(
            summary.precedent_patterns[1],
            PrecedentPattern::Cell(_)
        ));
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_sumifs_ranges_and_literal_criterion() {
        let summary = summary("=SUMIFS($B$1:$B$100, $A$1:$A$100, \"Type1\")", 1, 1);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(summary.precedent_patterns.len(), 2);
        assert!(
            summary
                .precedent_patterns
                .iter()
                .all(|precedent| matches!(precedent, PrecedentPattern::Range(_)))
        );
    }

    #[test]
    fn formula_plane_dependency_summary_detects_constant_result_family() {
        let summary = summary("=SUMIFS($B$1:$B$100, $A$1:$A$100, \"Type1\")", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_does_not_flag_relative_family_as_constant() {
        let summary = summary("=A1 * SUM($A$1:$A$10)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(!summary.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_flags_pure_literal_as_constant() {
        let summary = summary("=ROUND(1.5, 2)", 1, 1);

        assert!(summary.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_cross_sheet_sumifs_ranges() {
        let summary = summary(
            "=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, \"Type1\")",
            1,
            1,
        );

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(summary.precedent_patterns.len(), 2);
        assert!(summary.precedent_patterns.iter().all(|precedent| matches!(
            precedent,
            PrecedentPattern::Range(AffineRectPattern {
                sheet: SheetBinding::ExplicitName { .. },
                ..
            })
        )));
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_countif_and_countifs_range_args() {
        let countif = summary("=COUNTIF($A$1:$A$10, \"x\")", 1, 1);
        let countifs = summary("=COUNTIFS($A$1:$A$10, \"x\", $B$1:$B$10, \">5\")", 1, 1);

        assert_eq!(countif.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(countifs.formula_class, FormulaClass::StaticPointwise);
        assert_eq!(countif.precedent_patterns.len(), 1);
        assert_eq!(countifs.precedent_patterns.len(), 2);
        assert!(countif.reject_reasons.is_empty());
        assert!(countifs.reject_reasons.is_empty());
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_mixed_axis_running_total_range() {
        // `=SUM($A$1:$A1)` at row 1: absolute start row, placement-relative
        // end row (offset 0) — the expanding running-total idiom.
        let summary = summary("=SUM($A$1:$A1)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(
            summary.precedent_patterns,
            vec![range(
                SheetBinding::CurrentSheet,
                AxisRef::AbsoluteVc { index: 1 },
                AxisRef::AbsoluteVc { index: 1 },
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::AbsoluteVc { index: 1 },
            )]
        );
        assert!(!summary.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_mixed_axis_tail_read_range() {
        // `=SUM($A1:$A$100)` at row 1: placement-relative start row, absolute
        // end row — the shrinking tail-read idiom.
        let summary = summary("=SUM($A1:$A$100)", 1, 2);

        assert_eq!(summary.formula_class, FormulaClass::StaticPointwise);
        assert!(summary.reject_reasons.is_empty());
        assert_eq!(
            summary.precedent_patterns,
            vec![range(
                SheetBinding::CurrentSheet,
                AxisRef::RelativeToPlacement { offset: 0 },
                AxisRef::AbsoluteVc { index: 1 },
                AxisRef::AbsoluteVc { index: 100 },
                AxisRef::AbsoluteVc { index: 1 },
            )]
        );
        assert!(!summary.is_constant_result());
    }

    #[test]
    fn formula_plane_dependency_summary_accepts_function_arg_whole_column_but_rejects_direct_axis_ranges()
     {
        let direct_column = summary("=A:A", 1, 2);
        let direct_row = summary("=1:10", 1, 2);
        let function_arg = summary("=SUM(A:A)", 1, 2);

        assert_eq!(direct_column.formula_class, FormulaClass::Rejected);
        assert_eq!(direct_row.formula_class, FormulaClass::Rejected);
        assert_eq!(function_arg.formula_class, FormulaClass::StaticPointwise);
        assert!(has_reason(
            &direct_column,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(has_reason(
            &direct_row,
            &DependencyRejectReason::FiniteRangeUnsupported {
                context: AnalyzerContext::Value
            }
        ));
        assert!(function_arg.reject_reasons.is_empty());
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
