//! Authority-grade FormulaPlane template canonicalization for FP4.A.1.
//!
//! This module is passive and crate-internal. It walks an already parsed formula
//! AST and builds a deterministic template model for later dependency-summary
//! work; it does not change loader, graph, scheduler, dirty, materialization, or
//! evaluation behavior.
//!
//! Structured-reference boundary: the FP4.A scanner/passive path may
//! canonicalize raw parsed formulas. Production ingest fusion and any rewrite
//! that lowers structured references before canonicalization are not in FP4.A.1
//! scope. Current-row structured references are classified explicitly here so a
//! future ingest-side rewrite cannot silently give a different authority answer.

use std::collections::BTreeSet;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::{
    ASTNode, ASTNodeType, ReferenceType, SpecialItem, TableRowSpecifier, TableSpecifier,
};

/// Canonical template output for a single formula placement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalTemplate {
    pub(crate) key: FormulaTemplateKey,
    pub(crate) expr: CanonicalExpr,
    pub(crate) labels: CanonicalTemplateLabels,
}

/// Stable authority key payload for a canonical template.
///
/// Equality uses the full payload, not the diagnostic hash, so hash collisions
/// cannot merge template families.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaTemplateKey {
    payload: String,
    stable_hash: u64,
}

impl FormulaTemplateKey {
    fn new(expr: &CanonicalExpr, labels: &CanonicalTemplateLabels) -> Self {
        let mut payload = String::new();
        payload.push_str("fp4a1:");
        write_expr_key(expr, &mut payload);
        payload.push_str("|labels=");
        write_labels_key(labels, &mut payload);
        let stable_hash = stable_fnv1a64(payload.as_bytes());
        Self {
            payload,
            stable_hash,
        }
    }

    pub(crate) fn payload(&self) -> &str {
        &self.payload
    }

    pub(crate) fn stable_hash(&self) -> u64 {
        self.stable_hash
    }

    pub(crate) fn diagnostic_id(&self) -> String {
        format!("auth_tpl_{:016x}", self.stable_hash)
    }
}

/// Canonical expression tree used as the authority template body.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalExpr {
    Literal(CanonicalLiteral),
    Reference {
        context: CanonicalReferenceContext,
        reference: CanonicalReference,
    },
    Unary {
        op: String,
        expr: Box<CanonicalExpr>,
    },
    Binary {
        op: String,
        left: Box<CanonicalExpr>,
        right: Box<CanonicalExpr>,
    },
    Function {
        id: CanonicalFunctionId,
        args: Vec<CanonicalExpr>,
    },
    CallUnsupported {
        callee: Box<CanonicalExpr>,
        args: Vec<CanonicalExpr>,
    },
    ArrayUnsupported {
        rows: Vec<Vec<CanonicalExpr>>,
    },
}

/// Literal values are preserved by value, not only by kind.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalLiteral {
    Int(i64),
    NumberBits(u64),
    Text(String),
    Boolean(bool),
    Error(String),
    Array(Vec<Vec<CanonicalLiteral>>),
    Date(String),
    DateTime(String),
    Time(String),
    Duration(String),
    Empty,
    Pending,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalFunctionId {
    pub(crate) canonical_name: String,
}

/// Reference context is kept in the tree because by-ref/value semantics are
/// fallback-relevant even before FP4.A dependency summaries understand every
/// function contract.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalReferenceContext {
    Value,
    Reference,
    FunctionArgument { function: String, arg_index: usize },
    CallArgument { arg_index: usize },
}

/// Canonical reference model with affine axes relative to the formula placement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalReference {
    Cell {
        sheet: SheetBinding,
        row: AxisRef,
        col: AxisRef,
    },
    Range {
        sheet: SheetBinding,
        start_row: AxisRef,
        start_col: AxisRef,
        end_row: AxisRef,
        end_col: AxisRef,
    },
    Unsupported {
        kind: UnsupportedReferenceKind,
        diagnostic: String,
    },
}

/// Sheet binding mode observed in the parsed AST.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SheetBinding {
    CurrentSheet,
    /// Parser ASTs carry a sheet display name but no workbook-stable sheet id.
    /// The name is deterministic diagnostic input; later phases may replace it
    /// with registry-backed identity when available.
    ExplicitName {
        name: String,
    },
}

/// Per-axis affine reference endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AxisRef {
    RelativeToPlacement { offset: i64 },
    AbsoluteVc { index: u32 },
    OpenStart,
    OpenEnd,
    WholeAxis,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnsupportedReferenceKind {
    NamedRange,
    StructuredReference,
    ThreeDReference,
    ExternalReference,
    SpillReference,
    Unknown,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalTemplateLabels {
    pub(crate) reject_reasons: BTreeSet<CanonicalRejectReason>,
    pub(crate) flags: BTreeSet<CanonicalTemplateFlag>,
}

impl CanonicalTemplateLabels {
    pub(crate) fn is_authority_supported(&self) -> bool {
        self.reject_reasons.is_empty()
    }

    pub(crate) fn contains_reject_kind(&self, kind: CanonicalRejectKind) -> bool {
        self.reject_reasons
            .iter()
            .any(|reason| reason.kind() == kind)
    }

    fn reject(&mut self, reason: CanonicalRejectReason) {
        self.reject_reasons.insert(reason);
    }

    fn flag(&mut self, flag: CanonicalTemplateFlag) {
        self.flags.insert(flag);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CanonicalRejectKind {
    InvalidPlacementAnchor,
    DynamicReference,
    UnknownOrCustomFunction,
    LocalEnvironment,
    VolatileFunction,
    ReferenceReturningFunction,
    ArrayOrSpill,
    ArrayLiteral,
    SpillReference,
    CallExpression,
    NamedReference,
    StructuredReference,
    StructuredReferenceCurrentRow,
    ThreeDReference,
    ExternalReference,
    OpenRangeReference,
    WholeAxisReference,
    UnsupportedReference,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CanonicalRejectReason {
    InvalidPlacementAnchor { row: u32, col: u32 },
    DynamicReferenceFunction { name: String },
    UnknownOrCustomFunction { name: String },
    LocalEnvironmentFunction { name: String },
    ParserVolatileFlag,
    VolatileFunction { name: String },
    ReferenceReturningFunction { name: String },
    ArrayOrSpillFunction { name: String },
    ArrayLiteral,
    SpillReference { original: String },
    CallExpression,
    NamedReference { name: String },
    StructuredReference { diagnostic: String },
    StructuredReferenceCurrentRow { diagnostic: String },
    ThreeDReference { diagnostic: String },
    ExternalReference { diagnostic: String },
    OpenRangeReference { original: String },
    WholeAxisReference { original: String },
    UnsupportedReference { diagnostic: String },
}

impl CanonicalRejectReason {
    pub(crate) fn kind(&self) -> CanonicalRejectKind {
        match self {
            CanonicalRejectReason::InvalidPlacementAnchor { .. } => {
                CanonicalRejectKind::InvalidPlacementAnchor
            }
            CanonicalRejectReason::DynamicReferenceFunction { .. } => {
                CanonicalRejectKind::DynamicReference
            }
            CanonicalRejectReason::UnknownOrCustomFunction { .. } => {
                CanonicalRejectKind::UnknownOrCustomFunction
            }
            CanonicalRejectReason::LocalEnvironmentFunction { .. } => {
                CanonicalRejectKind::LocalEnvironment
            }
            CanonicalRejectReason::ParserVolatileFlag
            | CanonicalRejectReason::VolatileFunction { .. } => {
                CanonicalRejectKind::VolatileFunction
            }
            CanonicalRejectReason::ReferenceReturningFunction { .. } => {
                CanonicalRejectKind::ReferenceReturningFunction
            }
            CanonicalRejectReason::ArrayOrSpillFunction { .. } => CanonicalRejectKind::ArrayOrSpill,
            CanonicalRejectReason::ArrayLiteral => CanonicalRejectKind::ArrayLiteral,
            CanonicalRejectReason::SpillReference { .. } => CanonicalRejectKind::SpillReference,
            CanonicalRejectReason::CallExpression => CanonicalRejectKind::CallExpression,
            CanonicalRejectReason::NamedReference { .. } => CanonicalRejectKind::NamedReference,
            CanonicalRejectReason::StructuredReference { .. } => {
                CanonicalRejectKind::StructuredReference
            }
            CanonicalRejectReason::StructuredReferenceCurrentRow { .. } => {
                CanonicalRejectKind::StructuredReferenceCurrentRow
            }
            CanonicalRejectReason::ThreeDReference { .. } => CanonicalRejectKind::ThreeDReference,
            CanonicalRejectReason::ExternalReference { .. } => {
                CanonicalRejectKind::ExternalReference
            }
            CanonicalRejectReason::OpenRangeReference { .. } => {
                CanonicalRejectKind::OpenRangeReference
            }
            CanonicalRejectReason::WholeAxisReference { .. } => {
                CanonicalRejectKind::WholeAxisReference
            }
            CanonicalRejectReason::UnsupportedReference { .. } => {
                CanonicalRejectKind::UnsupportedReference
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CanonicalTemplateFlag {
    ParserVolatileFlag,
    FunctionCall,
    CurrentSheetBinding,
    ExplicitSheetBinding,
    RelativeReferenceAxis,
    AbsoluteReferenceAxis,
    MixedAnchors,
    FiniteRangeReference,
}

/// Canonicalize a parsed formula AST at a one-based formula placement anchor.
///
/// Relative reference axes normalize to deltas from `anchor_row` and
/// `anchor_col`; absolute axes remain literal user-visible coordinates.
pub(crate) fn canonicalize_template(
    ast: &ASTNode,
    anchor_row: u32,
    anchor_col: u32,
) -> CanonicalTemplate {
    let mut canonicalizer = Canonicalizer {
        anchor_row,
        anchor_col,
        labels: CanonicalTemplateLabels::default(),
    };

    if anchor_row == 0 || anchor_col == 0 {
        canonicalizer
            .labels
            .reject(CanonicalRejectReason::InvalidPlacementAnchor {
                row: anchor_row,
                col: anchor_col,
            });
    }
    if ast.contains_volatile() {
        canonicalizer
            .labels
            .flag(CanonicalTemplateFlag::ParserVolatileFlag);
        canonicalizer
            .labels
            .reject(CanonicalRejectReason::ParserVolatileFlag);
    }

    let expr = canonicalizer.canonicalize_expr(ast, CanonicalReferenceContext::Value);
    if canonicalizer
        .labels
        .flags
        .contains(&CanonicalTemplateFlag::AbsoluteReferenceAxis)
        && canonicalizer
            .labels
            .flags
            .contains(&CanonicalTemplateFlag::RelativeReferenceAxis)
    {
        canonicalizer
            .labels
            .flag(CanonicalTemplateFlag::MixedAnchors);
    }
    let labels = canonicalizer.labels;
    let key = FormulaTemplateKey::new(&expr, &labels);

    CanonicalTemplate { key, expr, labels }
}

struct Canonicalizer {
    anchor_row: u32,
    anchor_col: u32,
    labels: CanonicalTemplateLabels,
}

impl Canonicalizer {
    fn canonicalize_expr(
        &mut self,
        ast: &ASTNode,
        context: CanonicalReferenceContext,
    ) -> CanonicalExpr {
        match &ast.node_type {
            ASTNodeType::Literal(value) => CanonicalExpr::Literal(self.canonicalize_literal(value)),
            ASTNodeType::Reference {
                original,
                reference,
            } => CanonicalExpr::Reference {
                context,
                reference: self.canonicalize_reference(original, reference),
            },
            ASTNodeType::UnaryOp { op, expr } => CanonicalExpr::Unary {
                op: op.clone(),
                expr: Box::new(self.canonicalize_expr(expr, CanonicalReferenceContext::Value)),
            },
            ASTNodeType::BinaryOp { op, left, right } => {
                let child_context = if op == ":" {
                    CanonicalReferenceContext::Reference
                } else {
                    CanonicalReferenceContext::Value
                };
                CanonicalExpr::Binary {
                    op: op.clone(),
                    left: Box::new(self.canonicalize_expr(left, child_context.clone())),
                    right: Box::new(self.canonicalize_expr(right, child_context)),
                }
            }
            ASTNodeType::Function { name, args } => {
                let canonical_name = normalize_function_name(name);
                self.classify_function(&canonical_name);
                self.labels.flag(CanonicalTemplateFlag::FunctionCall);
                let canonical_args = args
                    .iter()
                    .enumerate()
                    .map(|(arg_index, arg)| {
                        self.canonicalize_expr(
                            arg,
                            CanonicalReferenceContext::FunctionArgument {
                                function: canonical_name.clone(),
                                arg_index,
                            },
                        )
                    })
                    .collect();

                CanonicalExpr::Function {
                    id: CanonicalFunctionId { canonical_name },
                    args: canonical_args,
                }
            }
            ASTNodeType::Call { callee, args } => {
                self.labels.reject(CanonicalRejectReason::CallExpression);
                let canonical_args = args
                    .iter()
                    .enumerate()
                    .map(|(arg_index, arg)| {
                        self.canonicalize_expr(
                            arg,
                            CanonicalReferenceContext::CallArgument { arg_index },
                        )
                    })
                    .collect();
                CanonicalExpr::CallUnsupported {
                    callee: Box::new(
                        self.canonicalize_expr(callee, CanonicalReferenceContext::Value),
                    ),
                    args: canonical_args,
                }
            }
            ASTNodeType::Array(rows) => {
                self.labels.reject(CanonicalRejectReason::ArrayLiteral);
                let rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                self.canonicalize_expr(expr, CanonicalReferenceContext::Value)
                            })
                            .collect()
                    })
                    .collect();
                CanonicalExpr::ArrayUnsupported { rows }
            }
        }
    }

    fn canonicalize_literal(&mut self, value: &LiteralValue) -> CanonicalLiteral {
        match value {
            LiteralValue::Int(value) => CanonicalLiteral::Int(*value),
            LiteralValue::Number(value) => CanonicalLiteral::NumberBits(value.to_bits()),
            LiteralValue::Text(value) => CanonicalLiteral::Text(value.clone()),
            LiteralValue::Boolean(value) => CanonicalLiteral::Boolean(*value),
            LiteralValue::Error(value) => CanonicalLiteral::Error(format!("{value:?}")),
            LiteralValue::Array(rows) => {
                self.labels.reject(CanonicalRejectReason::ArrayLiteral);
                CanonicalLiteral::Array(
                    rows.iter()
                        .map(|row| {
                            row.iter()
                                .map(|value| self.canonicalize_literal(value))
                                .collect()
                        })
                        .collect(),
                )
            }
            LiteralValue::Date(value) => CanonicalLiteral::Date(value.to_string()),
            LiteralValue::DateTime(value) => CanonicalLiteral::DateTime(value.to_string()),
            LiteralValue::Time(value) => CanonicalLiteral::Time(value.to_string()),
            LiteralValue::Duration(value) => CanonicalLiteral::Duration(format!("{value:?}")),
            LiteralValue::Empty => CanonicalLiteral::Empty,
            LiteralValue::Pending => CanonicalLiteral::Pending,
        }
    }

    fn canonicalize_reference(
        &mut self,
        original: &str,
        reference: &ReferenceType,
    ) -> CanonicalReference {
        match reference {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                self.classify_sheet_binding(sheet);
                if original.trim_end().ends_with('#') {
                    self.labels.reject(CanonicalRejectReason::SpillReference {
                        original: original.to_string(),
                    });
                    return CanonicalReference::Unsupported {
                        kind: UnsupportedReferenceKind::SpillReference,
                        diagnostic: original.to_string(),
                    };
                }

                let row = self.axis_from_value(*row, self.anchor_row, *row_abs);
                let col = self.axis_from_value(*col, self.anchor_col, *col_abs);
                self.flag_mixed_anchors(&[&row, &col]);
                CanonicalReference::Cell {
                    sheet: sheet_binding(sheet),
                    row,
                    col,
                }
            }
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
            } => {
                self.classify_sheet_binding(sheet);
                if original.trim_end().ends_with('#') {
                    self.labels.reject(CanonicalRejectReason::SpillReference {
                        original: original.to_string(),
                    });
                    return CanonicalReference::Unsupported {
                        kind: UnsupportedReferenceKind::SpillReference,
                        diagnostic: original.to_string(),
                    };
                }

                self.classify_range_bounds(original, *start_row, *end_row);
                self.classify_range_bounds(original, *start_col, *end_col);

                let (start_row, end_row) = self.axis_pair_from_range(
                    *start_row,
                    *end_row,
                    self.anchor_row,
                    *start_row_abs,
                    *end_row_abs,
                );
                let (start_col, end_col) = self.axis_pair_from_range(
                    *start_col,
                    *end_col,
                    self.anchor_col,
                    *start_col_abs,
                    *end_col_abs,
                );
                if !matches!(start_row, AxisRef::WholeAxis | AxisRef::OpenStart)
                    && !matches!(end_row, AxisRef::WholeAxis | AxisRef::OpenEnd)
                    && !matches!(start_col, AxisRef::WholeAxis | AxisRef::OpenStart)
                    && !matches!(end_col, AxisRef::WholeAxis | AxisRef::OpenEnd)
                {
                    self.labels
                        .flag(CanonicalTemplateFlag::FiniteRangeReference);
                }
                self.flag_mixed_anchors(&[&start_row, &start_col, &end_row, &end_col]);

                CanonicalReference::Range {
                    sheet: sheet_binding(sheet),
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                }
            }
            ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {
                let diagnostic = reference.to_string();
                self.labels.reject(CanonicalRejectReason::ThreeDReference {
                    diagnostic: diagnostic.clone(),
                });
                CanonicalReference::Unsupported {
                    kind: UnsupportedReferenceKind::ThreeDReference,
                    diagnostic,
                }
            }
            ReferenceType::External(_) => {
                let diagnostic = reference.to_string();
                self.labels
                    .reject(CanonicalRejectReason::ExternalReference {
                        diagnostic: diagnostic.clone(),
                    });
                CanonicalReference::Unsupported {
                    kind: UnsupportedReferenceKind::ExternalReference,
                    diagnostic,
                }
            }
            ReferenceType::Table(table) => {
                let diagnostic = reference.to_string();
                self.labels
                    .reject(CanonicalRejectReason::StructuredReference {
                        diagnostic: diagnostic.clone(),
                    });
                if table_has_current_row(table.specifier.as_ref()) {
                    self.labels
                        .reject(CanonicalRejectReason::StructuredReferenceCurrentRow {
                            diagnostic: diagnostic.clone(),
                        });
                }
                CanonicalReference::Unsupported {
                    kind: UnsupportedReferenceKind::StructuredReference,
                    diagnostic,
                }
            }
            ReferenceType::NamedRange(name) => {
                self.labels.reject(CanonicalRejectReason::NamedReference {
                    name: name.to_ascii_uppercase(),
                });
                CanonicalReference::Unsupported {
                    kind: UnsupportedReferenceKind::NamedRange,
                    diagnostic: name.to_ascii_uppercase(),
                }
            }
        }
    }

    fn classify_function(&mut self, canonical_name: &str) {
        let mut known_special = false;

        if is_dynamic_reference_function(canonical_name) {
            known_special = true;
            self.labels
                .reject(CanonicalRejectReason::DynamicReferenceFunction {
                    name: canonical_name.to_string(),
                });
        }
        if is_local_environment_function(canonical_name) {
            known_special = true;
            self.labels
                .reject(CanonicalRejectReason::LocalEnvironmentFunction {
                    name: canonical_name.to_string(),
                });
        }
        if is_volatile_function(canonical_name) {
            known_special = true;
            self.labels.reject(CanonicalRejectReason::VolatileFunction {
                name: canonical_name.to_string(),
            });
        }
        if is_reference_returning_function(canonical_name) {
            known_special = true;
            self.labels
                .reject(CanonicalRejectReason::ReferenceReturningFunction {
                    name: canonical_name.to_string(),
                });
        }
        if is_array_or_spill_function(canonical_name) {
            known_special = true;
            self.labels
                .reject(CanonicalRejectReason::ArrayOrSpillFunction {
                    name: canonical_name.to_string(),
                });
        }

        // The parser AST does not carry registry identity. Until FP4.B wires in
        // function contracts, FP4.A.1 treats only a small hard-coded built-in
        // set as known and labels every other function unknown/custom.
        if !known_special && !is_known_static_function(canonical_name) {
            self.labels
                .reject(CanonicalRejectReason::UnknownOrCustomFunction {
                    name: canonical_name.to_string(),
                });
        }
    }

    fn classify_sheet_binding(&mut self, sheet: &Option<String>) {
        if sheet.is_some() {
            self.labels
                .flag(CanonicalTemplateFlag::ExplicitSheetBinding);
        } else {
            self.labels.flag(CanonicalTemplateFlag::CurrentSheetBinding);
        }
    }

    fn classify_range_bounds(&mut self, original: &str, start: Option<u32>, end: Option<u32>) {
        match (start, end) {
            (None, None) => self
                .labels
                .reject(CanonicalRejectReason::WholeAxisReference {
                    original: original.to_string(),
                }),
            (None, Some(_)) | (Some(_), None) => {
                self.labels
                    .reject(CanonicalRejectReason::OpenRangeReference {
                        original: original.to_string(),
                    })
            }
            (Some(_), Some(_)) => {}
        }
    }

    fn axis_pair_from_range(
        &mut self,
        start: Option<u32>,
        end: Option<u32>,
        anchor: u32,
        start_abs: bool,
        end_abs: bool,
    ) -> (AxisRef, AxisRef) {
        match (start, end) {
            (Some(start), Some(end)) => (
                self.axis_from_value(start, anchor, start_abs),
                self.axis_from_value(end, anchor, end_abs),
            ),
            (None, None) => (AxisRef::WholeAxis, AxisRef::WholeAxis),
            (None, Some(end)) => (
                AxisRef::OpenStart,
                self.axis_from_value(end, anchor, end_abs),
            ),
            (Some(start), None) => (
                self.axis_from_value(start, anchor, start_abs),
                AxisRef::OpenEnd,
            ),
        }
    }

    fn axis_from_value(&mut self, value: u32, anchor: u32, absolute: bool) -> AxisRef {
        if absolute {
            self.labels
                .flag(CanonicalTemplateFlag::AbsoluteReferenceAxis);
            AxisRef::AbsoluteVc { index: value }
        } else {
            self.labels
                .flag(CanonicalTemplateFlag::RelativeReferenceAxis);
            AxisRef::RelativeToPlacement {
                offset: i64::from(value) - i64::from(anchor),
            }
        }
    }

    fn flag_mixed_anchors(&mut self, axes: &[&AxisRef]) {
        let has_absolute = axes
            .iter()
            .any(|axis| matches!(axis, AxisRef::AbsoluteVc { .. }));
        let has_relative = axes
            .iter()
            .any(|axis| matches!(axis, AxisRef::RelativeToPlacement { .. }));
        if has_absolute && has_relative {
            self.labels.flag(CanonicalTemplateFlag::MixedAnchors);
        }
    }
}

fn sheet_binding(sheet: &Option<String>) -> SheetBinding {
    match sheet {
        Some(name) => SheetBinding::ExplicitName { name: name.clone() },
        None => SheetBinding::CurrentSheet,
    }
}

fn normalize_function_name(name: &str) -> String {
    let mut normalized = name.trim().to_ascii_uppercase();
    loop {
        let stripped = ["_XLFN.", "_XLL.", "_XLWS."]
            .iter()
            .find_map(|prefix| normalized.strip_prefix(prefix).map(str::to_string));
        if let Some(stripped) = stripped {
            normalized = stripped;
        } else {
            return normalized;
        }
    }
}

fn is_dynamic_reference_function(name: &str) -> bool {
    matches!(name, "INDIRECT" | "OFFSET")
}

fn is_local_environment_function(name: &str) -> bool {
    matches!(name, "LET" | "LAMBDA")
}

fn is_volatile_function(name: &str) -> bool {
    matches!(name, "NOW" | "TODAY" | "RAND" | "RANDBETWEEN")
}

fn is_reference_returning_function(name: &str) -> bool {
    matches!(name, "CHOOSE" | "INDEX")
}

fn is_array_or_spill_function(name: &str) -> bool {
    matches!(
        name,
        "FILTER" | "RANDARRAY" | "SEQUENCE" | "SORT" | "SORTBY" | "TEXTSPLIT" | "UNIQUE"
    )
}

fn is_known_static_function(name: &str) -> bool {
    matches!(
        name,
        "ABS"
            | "ACOS"
            | "ACOSH"
            | "AND"
            | "ASIN"
            | "ASINH"
            | "ATAN"
            | "ATAN2"
            | "ATANH"
            | "AVERAGE"
            | "CEILING"
            | "CONCAT"
            | "CONCATENATE"
            | "COS"
            | "COSH"
            | "COUNT"
            | "COUNTA"
            | "COUNTBLANK"
            | "COUNTIF"
            | "COUNTIFS"
            | "DATE"
            | "DAY"
            | "ERROR.TYPE"
            | "EVEN"
            | "EXACT"
            | "EXP"
            | "FALSE"
            | "FIND"
            | "FLOOR"
            | "IF"
            | "IFERROR"
            | "IFNA"
            | "IFS"
            | "INT"
            | "ISBLANK"
            | "ISERR"
            | "ISERROR"
            | "ISEVEN"
            | "ISLOGICAL"
            | "ISNA"
            | "ISNONTEXT"
            | "ISNUMBER"
            | "ISODD"
            | "ISTEXT"
            | "LEFT"
            | "LEN"
            | "LN"
            | "LOG"
            | "LOG10"
            | "LOWER"
            | "MAX"
            | "MID"
            | "MIN"
            | "MOD"
            | "MONTH"
            | "NOT"
            | "ODD"
            | "OR"
            | "POWER"
            | "PRODUCT"
            | "PROPER"
            | "REPLACE"
            | "REPT"
            | "RIGHT"
            | "ROUND"
            | "ROUNDDOWN"
            | "ROUNDUP"
            | "SEARCH"
            | "SIN"
            | "SINH"
            | "SQRT"
            | "SUBSTITUTE"
            | "SUM"
            | "SUMIF"
            | "SUMIFS"
            | "SWITCH"
            | "TAN"
            | "TANH"
            | "TEXT"
            | "TEXTJOIN"
            | "TIME"
            | "TRIM"
            | "TRUE"
            | "TRUNC"
            | "UPPER"
            | "VALUE"
            | "YEAR"
    )
}

fn table_has_current_row(specifier: Option<&TableSpecifier>) -> bool {
    fn contains_current_row(specifier: &TableSpecifier) -> bool {
        match specifier {
            TableSpecifier::Row(TableRowSpecifier::Current)
            | TableSpecifier::SpecialItem(SpecialItem::ThisRow) => true,
            TableSpecifier::Combination(specifiers) => specifiers
                .iter()
                .any(|specifier| contains_current_row(specifier)),
            TableSpecifier::All
            | TableSpecifier::Data
            | TableSpecifier::Headers
            | TableSpecifier::Totals
            | TableSpecifier::Row(_)
            | TableSpecifier::Column(_)
            | TableSpecifier::ColumnRange(_, _)
            | TableSpecifier::SpecialItem(_) => false,
        }
    }

    specifier.is_some_and(contains_current_row)
}

fn write_expr_key(expr: &CanonicalExpr, out: &mut String) {
    match expr {
        CanonicalExpr::Literal(value) => {
            out.push_str("lit(");
            write_literal_key(value, out);
            out.push(')');
        }
        CanonicalExpr::Reference { context, reference } => {
            out.push_str("ref(");
            write_reference_context_key(context, out);
            out.push(';');
            write_reference_key(reference, out);
            out.push(')');
        }
        CanonicalExpr::Unary { op, expr } => {
            out.push_str("unary(");
            write_string_key(op, out);
            out.push(';');
            write_expr_key(expr, out);
            out.push(')');
        }
        CanonicalExpr::Binary { op, left, right } => {
            out.push_str("binary(");
            write_string_key(op, out);
            out.push(';');
            write_expr_key(left, out);
            out.push(';');
            write_expr_key(right, out);
            out.push(')');
        }
        CanonicalExpr::Function { id, args } => {
            out.push_str("fn(");
            write_string_key(&id.canonical_name, out);
            out.push(';');
            write_vec_key(args, out, write_expr_key);
            out.push(')');
        }
        CanonicalExpr::CallUnsupported { callee, args } => {
            out.push_str("call(");
            write_expr_key(callee, out);
            out.push(';');
            write_vec_key(args, out, write_expr_key);
            out.push(')');
        }
        CanonicalExpr::ArrayUnsupported { rows } => {
            out.push_str("array(");
            out.push_str(&rows.len().to_string());
            out.push(':');
            for row in rows {
                write_vec_key(row, out, write_expr_key);
                out.push(';');
            }
            out.push(')');
        }
    }
}

fn write_literal_key(value: &CanonicalLiteral, out: &mut String) {
    match value {
        CanonicalLiteral::Int(value) => {
            out.push_str("int:");
            out.push_str(&value.to_string());
        }
        CanonicalLiteral::NumberBits(bits) => {
            out.push_str("num_bits:");
            out.push_str(&format!("{bits:016x}"));
        }
        CanonicalLiteral::Text(value) => {
            out.push_str("text:");
            write_string_key(value, out);
        }
        CanonicalLiteral::Boolean(value) => {
            out.push_str(if *value { "bool:true" } else { "bool:false" });
        }
        CanonicalLiteral::Error(value) => {
            out.push_str("error:");
            write_string_key(value, out);
        }
        CanonicalLiteral::Array(rows) => {
            out.push_str("lit_array(");
            out.push_str(&rows.len().to_string());
            out.push(':');
            for row in rows {
                write_vec_key(row, out, write_literal_key);
                out.push(';');
            }
            out.push(')');
        }
        CanonicalLiteral::Date(value) => {
            out.push_str("date:");
            write_string_key(value, out);
        }
        CanonicalLiteral::DateTime(value) => {
            out.push_str("datetime:");
            write_string_key(value, out);
        }
        CanonicalLiteral::Time(value) => {
            out.push_str("time:");
            write_string_key(value, out);
        }
        CanonicalLiteral::Duration(value) => {
            out.push_str("duration:");
            write_string_key(value, out);
        }
        CanonicalLiteral::Empty => out.push_str("empty"),
        CanonicalLiteral::Pending => out.push_str("pending"),
    }
}

fn write_reference_context_key(context: &CanonicalReferenceContext, out: &mut String) {
    match context {
        CanonicalReferenceContext::Value => out.push_str("value"),
        CanonicalReferenceContext::Reference => out.push_str("reference"),
        CanonicalReferenceContext::FunctionArgument {
            function,
            arg_index,
        } => {
            out.push_str("fn_arg:");
            write_string_key(function, out);
            out.push(':');
            out.push_str(&arg_index.to_string());
        }
        CanonicalReferenceContext::CallArgument { arg_index } => {
            out.push_str("call_arg:");
            out.push_str(&arg_index.to_string());
        }
    }
}

fn write_reference_key(reference: &CanonicalReference, out: &mut String) {
    match reference {
        CanonicalReference::Cell { sheet, row, col } => {
            out.push_str("cell(");
            write_sheet_key(sheet, out);
            out.push(';');
            write_axis_key(row, out);
            out.push(';');
            write_axis_key(col, out);
            out.push(')');
        }
        CanonicalReference::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        } => {
            out.push_str("range(");
            write_sheet_key(sheet, out);
            out.push(';');
            write_axis_key(start_row, out);
            out.push(';');
            write_axis_key(start_col, out);
            out.push(';');
            write_axis_key(end_row, out);
            out.push(';');
            write_axis_key(end_col, out);
            out.push(')');
        }
        CanonicalReference::Unsupported { kind, diagnostic } => {
            out.push_str("unsupported_ref(");
            write_unsupported_reference_kind_key(kind, out);
            out.push(';');
            write_string_key(diagnostic, out);
            out.push(')');
        }
    }
}

fn write_sheet_key(sheet: &SheetBinding, out: &mut String) {
    match sheet {
        SheetBinding::CurrentSheet => out.push_str("sheet:current"),
        SheetBinding::ExplicitName { name } => {
            out.push_str("sheet:name:");
            write_string_key(name, out);
        }
    }
}

fn write_axis_key(axis: &AxisRef, out: &mut String) {
    match axis {
        AxisRef::RelativeToPlacement { offset } => {
            out.push_str("rel:");
            out.push_str(&offset.to_string());
        }
        AxisRef::AbsoluteVc { index } => {
            out.push_str("abs:");
            out.push_str(&index.to_string());
        }
        AxisRef::OpenStart => out.push_str("open_start"),
        AxisRef::OpenEnd => out.push_str("open_end"),
        AxisRef::WholeAxis => out.push_str("whole_axis"),
        AxisRef::Unsupported => out.push_str("unsupported"),
    }
}

fn write_unsupported_reference_kind_key(kind: &UnsupportedReferenceKind, out: &mut String) {
    out.push_str(match kind {
        UnsupportedReferenceKind::NamedRange => "named_range",
        UnsupportedReferenceKind::StructuredReference => "structured_reference",
        UnsupportedReferenceKind::ThreeDReference => "three_d_reference",
        UnsupportedReferenceKind::ExternalReference => "external_reference",
        UnsupportedReferenceKind::SpillReference => "spill_reference",
        UnsupportedReferenceKind::Unknown => "unknown",
    });
}

fn write_labels_key(labels: &CanonicalTemplateLabels, out: &mut String) {
    out.push_str("rejects[");
    out.push_str(&labels.reject_reasons.len().to_string());
    out.push(':');
    for reason in &labels.reject_reasons {
        write_reject_reason_key(reason, out);
        out.push(';');
    }
    out.push_str("]flags[");
    out.push_str(&labels.flags.len().to_string());
    out.push(':');
    for flag in &labels.flags {
        write_template_flag_key(flag, out);
        out.push(';');
    }
    out.push(']');
}

fn write_reject_reason_key(reason: &CanonicalRejectReason, out: &mut String) {
    match reason {
        CanonicalRejectReason::InvalidPlacementAnchor { row, col } => {
            out.push_str("invalid_anchor:");
            out.push_str(&row.to_string());
            out.push(':');
            out.push_str(&col.to_string());
        }
        CanonicalRejectReason::DynamicReferenceFunction { name } => {
            out.push_str("dynamic_fn:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::UnknownOrCustomFunction { name } => {
            out.push_str("unknown_fn:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::LocalEnvironmentFunction { name } => {
            out.push_str("local_env_fn:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::ParserVolatileFlag => out.push_str("parser_volatile"),
        CanonicalRejectReason::VolatileFunction { name } => {
            out.push_str("volatile_fn:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::ReferenceReturningFunction { name } => {
            out.push_str("reference_returning_fn:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::ArrayOrSpillFunction { name } => {
            out.push_str("array_or_spill_fn:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::ArrayLiteral => out.push_str("array_literal"),
        CanonicalRejectReason::SpillReference { original } => {
            out.push_str("spill_ref:");
            write_string_key(original, out);
        }
        CanonicalRejectReason::CallExpression => out.push_str("call_expression"),
        CanonicalRejectReason::NamedReference { name } => {
            out.push_str("name_ref:");
            write_string_key(name, out);
        }
        CanonicalRejectReason::StructuredReference { diagnostic } => {
            out.push_str("structured_ref:");
            write_string_key(diagnostic, out);
        }
        CanonicalRejectReason::StructuredReferenceCurrentRow { diagnostic } => {
            out.push_str("structured_ref_current_row:");
            write_string_key(diagnostic, out);
        }
        CanonicalRejectReason::ThreeDReference { diagnostic } => {
            out.push_str("three_d_ref:");
            write_string_key(diagnostic, out);
        }
        CanonicalRejectReason::ExternalReference { diagnostic } => {
            out.push_str("external_ref:");
            write_string_key(diagnostic, out);
        }
        CanonicalRejectReason::OpenRangeReference { original } => {
            out.push_str("open_range:");
            write_string_key(original, out);
        }
        CanonicalRejectReason::WholeAxisReference { original } => {
            out.push_str("whole_axis:");
            write_string_key(original, out);
        }
        CanonicalRejectReason::UnsupportedReference { diagnostic } => {
            out.push_str("unsupported_ref:");
            write_string_key(diagnostic, out);
        }
    }
}

fn write_template_flag_key(flag: &CanonicalTemplateFlag, out: &mut String) {
    out.push_str(match flag {
        CanonicalTemplateFlag::ParserVolatileFlag => "parser_volatile",
        CanonicalTemplateFlag::FunctionCall => "function_call",
        CanonicalTemplateFlag::CurrentSheetBinding => "current_sheet",
        CanonicalTemplateFlag::ExplicitSheetBinding => "explicit_sheet",
        CanonicalTemplateFlag::RelativeReferenceAxis => "relative_axis",
        CanonicalTemplateFlag::AbsoluteReferenceAxis => "absolute_axis",
        CanonicalTemplateFlag::MixedAnchors => "mixed_anchors",
        CanonicalTemplateFlag::FiniteRangeReference => "finite_range",
    });
}

fn write_vec_key<T>(values: &[T], out: &mut String, write_value: fn(&T, &mut String)) {
    out.push_str(&values.len().to_string());
    out.push(':');
    for value in values {
        write_value(value, out);
        out.push(',');
    }
}

fn write_string_key(value: &str, out: &mut String) {
    out.push_str(&value.len().to_string());
    out.push(':');
    out.push_str(value);
}

fn stable_fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_parse::parse;

    fn canonical(formula: &str, row: u32, col: u32) -> CanonicalTemplate {
        let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
        canonicalize_template(&ast, row, col)
    }

    fn only_reference(template: &CanonicalTemplate) -> &CanonicalReference {
        match &template.expr {
            CanonicalExpr::Reference { reference, .. } => reference,
            other => panic!("expected reference, got {other:?}"),
        }
    }

    #[test]
    fn formula_plane_literal_values_participate_in_authority_key() {
        let one = canonical("=A1+1", 1, 2);
        let two = canonical("=A1+2", 1, 2);

        assert_ne!(one.key, two.key);
        assert_ne!(one.key.payload(), two.key.payload());
        assert!(one.labels.is_authority_supported());
        assert!(two.labels.is_authority_supported());
    }

    #[test]
    fn formula_plane_copied_relative_references_share_key() {
        let templates = [
            canonical("=A2+B2", 2, 3),
            canonical("=A3+B3", 3, 3),
            canonical("=A4+B4", 4, 3),
        ];

        assert_eq!(templates[0].key, templates[1].key);
        assert_eq!(templates[1].key, templates[2].key);
    }

    #[test]
    fn formula_plane_absolute_axes_are_preserved_across_copies() {
        let templates = [
            canonical("=$A$1+B2", 2, 3),
            canonical("=$A$1+B3", 3, 3),
            canonical("=$A$1+B4", 4, 3),
        ];

        assert_eq!(templates[0].key, templates[1].key);
        assert_eq!(templates[1].key, templates[2].key);
        assert!(
            templates[0]
                .labels
                .flags
                .contains(&CanonicalTemplateFlag::MixedAnchors)
        );
    }

    #[test]
    fn formula_plane_mixed_axes_are_modeled_per_axis_and_endpoint() {
        let abs_col = canonical("=$A1", 5, 3);
        let abs_row = canonical("=A$1", 5, 3);
        let mixed_range = canonical("=$A1:B$2", 5, 3);

        assert_eq!(
            only_reference(&abs_col),
            &CanonicalReference::Cell {
                sheet: SheetBinding::CurrentSheet,
                row: AxisRef::RelativeToPlacement { offset: -4 },
                col: AxisRef::AbsoluteVc { index: 1 },
            }
        );
        assert_eq!(
            only_reference(&abs_row),
            &CanonicalReference::Cell {
                sheet: SheetBinding::CurrentSheet,
                row: AxisRef::AbsoluteVc { index: 1 },
                col: AxisRef::RelativeToPlacement { offset: -2 },
            }
        );
        assert_eq!(
            only_reference(&mixed_range),
            &CanonicalReference::Range {
                sheet: SheetBinding::CurrentSheet,
                start_row: AxisRef::RelativeToPlacement { offset: -4 },
                start_col: AxisRef::AbsoluteVc { index: 1 },
                end_row: AxisRef::AbsoluteVc { index: 2 },
                end_col: AxisRef::RelativeToPlacement { offset: -1 },
            }
        );
    }

    #[test]
    fn formula_plane_cross_sheet_binding_is_represented_deterministically() {
        let first = canonical("=Sheet2!A1", 1, 1);
        let second = canonical("=Sheet2!A1", 1, 1);

        assert_eq!(first.key, second.key);
        assert!(
            first
                .labels
                .flags
                .contains(&CanonicalTemplateFlag::ExplicitSheetBinding)
        );
        assert_eq!(
            only_reference(&first),
            &CanonicalReference::Cell {
                sheet: SheetBinding::ExplicitName {
                    name: "Sheet2".to_string(),
                },
                row: AxisRef::RelativeToPlacement { offset: 0 },
                col: AxisRef::RelativeToPlacement { offset: 0 },
            }
        );
    }

    #[test]
    fn formula_plane_dynamic_reference_functions_are_rejected_explicitly() {
        let template = canonical("=INDIRECT(\"A1\")", 1, 1);

        assert!(
            template
                .labels
                .contains_reject_kind(CanonicalRejectKind::DynamicReference)
        );
        assert!(!template.labels.is_authority_supported());
    }

    #[test]
    fn formula_plane_unknown_custom_functions_are_rejected_explicitly() {
        let template = canonical("=CUSTOMFN(A1)", 1, 1);

        assert!(
            template
                .labels
                .contains_reject_kind(CanonicalRejectKind::UnknownOrCustomFunction)
        );
        assert!(!template.labels.is_authority_supported());
    }

    #[test]
    fn formula_plane_let_lambda_local_environment_is_rejected_explicitly() {
        let template = canonical("=LET(x,A1,x+1)", 1, 2);

        assert!(
            template
                .labels
                .contains_reject_kind(CanonicalRejectKind::LocalEnvironment)
        );
        assert!(!template.labels.is_authority_supported());
    }

    #[test]
    fn formula_plane_keys_are_deterministic_independent_of_input_order() {
        let inputs = [
            ("=A2+B2", 2, 3),
            ("=$A$1+B2", 2, 3),
            ("=Sheet2!A1", 1, 1),
            ("=A1+1", 1, 2),
        ];
        let mut forward = inputs
            .iter()
            .map(|(formula, row, col)| canonical(formula, *row, *col).key.payload().to_string())
            .collect::<Vec<_>>();
        let mut reverse = inputs
            .iter()
            .rev()
            .map(|(formula, row, col)| canonical(formula, *row, *col).key.payload().to_string())
            .collect::<Vec<_>>();

        forward.sort();
        reverse.sort();
        assert_eq!(forward, reverse);
    }

    #[test]
    fn formula_plane_structured_current_row_refs_are_rejected_explicitly() {
        let template = canonical("=Table1[@Amount]", 4, 4);

        assert!(
            template
                .labels
                .contains_reject_kind(CanonicalRejectKind::StructuredReference)
        );
        assert!(
            template
                .labels
                .contains_reject_kind(CanonicalRejectKind::StructuredReferenceCurrentRow)
        );
    }
}
