//! Read-only, engine-native workbook introspection.
//!
//! Reports in this module are owned semantic snapshots. Construction reads the
//! existing formula/value/dependency authorities only: it never evaluates,
//! prepares dependency state, creates placeholder vertices, or marks cells
//! dirty.

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt;

use formualizer_common::{
    CellAddress, ExcelError, ExcelErrorKind, LiteralValue, RangeAddress, RangeArea, SheetId,
};
use formualizer_parse::parser::{
    ASTNode, ReferenceType, SpecialItem, TableReference, TableSpecifier,
};
use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::engine::named_range::NamedDefinition;
use crate::engine::refs;
use crate::engine::used_extent::{
    ExtentPolicy, OpenRangeBounds, ResolvedExtent, resolve_used_extent,
};
use crate::engine::{Engine, VertexId, VertexKind};
use crate::reference::{CellRef, Coord};
use crate::traits::EvaluationContext;

const DEFAULT_MAX_LINKS: u32 = 256;
const DEFAULT_MAX_WORK: u64 = 100_000;

/// Correlates a report with the engine mutation and recalculation state from
/// which it was copied.
#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct StateStamp {
    pub mutation_revision: u64,
    pub recalc_epoch: u64,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum Staleness {
    Current,
    Dirty,
    NeverEvaluated,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum SpillRole {
    Anchor { extent: RangeAddress },
    Member { anchor: CellAddress },
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CellSnapshot {
    pub address: CellAddress,
    pub formula: Option<String>,
    pub value: Option<LiteralValue>,
    pub value_included: bool,
    pub staleness: Staleness,
    pub volatile: bool,
    pub spill: Option<SpillRole>,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CellSnapshotReport {
    pub stamp: StateStamp,
    pub cell: CellSnapshot,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum NameResolution {
    Cell(CellAddress),
    Range {
        declared: RangeArea,
        resolved: Option<RangeAddress>,
    },
    Literal(LiteralValue),
    Formula {
        formula: String,
        value: Option<LiteralValue>,
    },
    Unresolved,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum SemanticReference {
    Cell(CellAddress),
    Range {
        declared: RangeArea,
        resolved: Option<RangeAddress>,
        cell_count: u64,
    },
    Name {
        name: String,
        resolution: NameResolution,
    },
    Table {
        name: String,
        specifier: String,
        resolved: RangeAddress,
    },
    External {
        raw: String,
    },
    Unsupported {
        text: String,
        reason: String,
    },
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum Provenance {
    Declared,
    Observed,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum TraceLinkKind {
    Formula { provenance: Provenance },
    SpillAnchor,
    SpillReader,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum LinkDisposition {
    Expanded,
    Convergent,
    Cycle,
    Elided,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum OmittedCount {
    Exact(u64),
    AtLeast(u64),
}

#[derive(Serialize, Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct TruncationReport {
    pub incomplete: bool,
    pub omitted: Option<OmittedCount>,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Precedent {
    pub reference: SemanticReference,
    pub provenance: Provenance,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct PrecedentReport {
    pub stamp: StateStamp,
    pub cell: CellAddress,
    pub precedents: Vec<Precedent>,
    pub truncation: TruncationReport,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Dependent {
    pub cell: CellAddress,
    /// Spill member addresses through which this reader was discovered. For a
    /// normal cell query this contains the queried cell itself.
    pub via: Vec<CellAddress>,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DependentsReport {
    pub stamp: StateStamp,
    pub cell: CellAddress,
    pub dependents: Vec<Dependent>,
    pub truncation: TruncationReport,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct TraceNodeId(pub u32);

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum TraceDirection {
    Precedents,
    Dependents,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct TraceLinkTarget {
    pub node: TraceNodeId,
    pub disposition: LinkDisposition,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TraceLink {
    pub reference: SemanticReference,
    pub kind: TraceLinkKind,
    pub targets: Vec<TraceLinkTarget>,
    pub omitted: Option<OmittedCount>,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TraceNode {
    pub id: TraceNodeId,
    pub cell: CellSnapshot,
    pub links: Vec<TraceLink>,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TraceGraph {
    pub stamp: StateStamp,
    pub direction: TraceDirection,
    pub roots: Vec<TraceNodeId>,
    pub nodes: Vec<TraceNode>,
    pub truncation: TruncationReport,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct RangePage {
    pub stamp: StateStamp,
    pub declared: RangeArea,
    pub resolved: Option<RangeAddress>,
    pub total: u64,
    pub offset: u64,
    pub items: Vec<CellSnapshot>,
    pub next_offset: Option<u64>,
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotOptions {
    pub include_value: bool,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self {
            include_value: true,
        }
    }
}

impl SnapshotOptions {
    pub fn with_include_value(mut self, include_value: bool) -> Self {
        self.include_value = include_value;
        self
    }
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrecedentOptions {
    pub max_links: u32,
    pub max_work: u64,
}

impl Default for PrecedentOptions {
    fn default() -> Self {
        Self {
            max_links: DEFAULT_MAX_LINKS,
            max_work: DEFAULT_MAX_WORK,
        }
    }
}

impl PrecedentOptions {
    pub fn with_max_links(mut self, max_links: u32) -> Self {
        self.max_links = max_links;
        self
    }

    pub fn with_max_work(mut self, max_work: u64) -> Self {
        self.max_work = max_work;
        self
    }
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependentsOptions {
    pub max_results: u32,
    pub max_work: u64,
}

impl Default for DependentsOptions {
    fn default() -> Self {
        Self {
            max_results: 256,
            max_work: DEFAULT_MAX_WORK,
        }
    }
}

impl DependentsOptions {
    pub fn with_max_results(mut self, max_results: u32) -> Self {
        self.max_results = max_results;
        self
    }

    pub fn with_max_work(mut self, max_work: u64) -> Self {
        self.max_work = max_work;
        self
    }
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceOptions {
    pub direction: TraceDirection,
    pub max_depth: u32,
    pub max_nodes: u32,
    pub max_links: u32,
    pub max_work: u64,
    pub range_member_budget: u32,
    pub include_values: bool,
}

impl Default for TraceOptions {
    fn default() -> Self {
        Self {
            direction: TraceDirection::Precedents,
            max_depth: 6,
            max_nodes: 512,
            max_links: 1_024,
            max_work: DEFAULT_MAX_WORK,
            range_member_budget: 256,
            include_values: true,
        }
    }
}

impl TraceOptions {
    pub fn with_direction(mut self, direction: TraceDirection) -> Self {
        self.direction = direction;
        self
    }

    pub fn with_max_depth(mut self, max_depth: u32) -> Self {
        self.max_depth = max_depth;
        self
    }

    pub fn with_max_nodes(mut self, max_nodes: u32) -> Self {
        self.max_nodes = max_nodes;
        self
    }

    pub fn with_max_links(mut self, max_links: u32) -> Self {
        self.max_links = max_links;
        self
    }

    pub fn with_max_work(mut self, max_work: u64) -> Self {
        self.max_work = max_work;
        self
    }

    pub fn with_range_member_budget(mut self, range_member_budget: u32) -> Self {
        self.range_member_budget = range_member_budget;
        self
    }

    pub fn with_include_values(mut self, include_values: bool) -> Self {
        self.include_values = include_values;
        self
    }
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangePageOptions {
    pub offset: u64,
    pub limit: u32,
    pub include_values: bool,
    pub expected_stamp: Option<StateStamp>,
}

impl Default for RangePageOptions {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 100,
            include_values: true,
            expected_stamp: None,
        }
    }
}

impl RangePageOptions {
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_include_values(mut self, include_values: bool) -> Self {
        self.include_values = include_values;
        self
    }

    pub fn with_expected_stamp(mut self, expected_stamp: StateStamp) -> Self {
        self.expected_stamp = Some(expected_stamp);
        self
    }
}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum InspectionUnavailableReason {
    DeferredDependencyGraph,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InspectError {
    SheetNotFound {
        sheet: String,
    },
    InvalidAddress {
        message: String,
    },
    InvalidOptions {
        message: String,
    },
    DependencyStateUnavailable {
        reason: InspectionUnavailableReason,
    },
    RevisionMismatch {
        expected: StateStamp,
        actual: StateStamp,
    },
    ResourceExhausted {
        resource: &'static str,
    },
}

impl fmt::Display for InspectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SheetNotFound { sheet } => write!(f, "sheet not found: {sheet}"),
            Self::InvalidAddress { message } => write!(f, "invalid address: {message}"),
            Self::InvalidOptions { message } => write!(f, "invalid inspection options: {message}"),
            Self::DependencyStateUnavailable { reason } => {
                write!(f, "dependency state unavailable: {reason:?}")
            }
            Self::RevisionMismatch { expected, actual } => write!(
                f,
                "inspection revision mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::ResourceExhausted { resource } => {
                write!(f, "inspection resource exhausted: {resource}")
            }
        }
    }
}

impl Error for InspectError {}

#[derive(Serialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct CellKey {
    sheet_id: SheetId,
    row0: u32,
    col0: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct FormulaView {
    ast: ASTNode,
    volatile: bool,
    dirty: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum InternalSpillRole {
    Anchor { extent: RangeAddress },
    Member { anchor: CellKey },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryCompleteness {
    Complete,
    Incomplete,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkBudget {
    remaining: u64,
}

impl WorkBudget {
    fn new(limit: u64) -> Self {
        Self { remaining: limit }
    }

    fn charge(&mut self) -> bool {
        if self.remaining == 0 {
            false
        } else {
            self.remaining -= 1;
            true
        }
    }
}

pub(crate) trait ReferenceVisitor {
    /// `false` requests an immediate, successful early stop.
    fn visit(&mut self, reference: refs::SemanticReference<'_>) -> bool;
}

pub(crate) trait DependentVisitor {
    /// `false` requests an immediate, successful early stop.
    fn visit(&mut self, dependent: CellKey) -> bool;
}

/// Formula-authority seam for introspection. The legacy implementation lands
/// here; another authority can implement the same address-semantic visitor
/// contract without moving DTO construction or traversal rules.
pub(crate) trait InspectSource {
    fn formula_at(&self, cell: CellKey) -> Result<Option<FormulaView>, InspectError>;
    fn visit_declared_references(
        &self,
        cell: CellKey,
        visitor: &mut dyn ReferenceVisitor,
    ) -> Result<(), InspectError>;
    fn visit_dependents_covering(
        &self,
        cell: CellKey,
        budget: &mut WorkBudget,
        visitor: &mut dyn DependentVisitor,
    ) -> Result<QueryCompleteness, InspectError>;
    fn spill_role(&self, cell: CellKey) -> Option<InternalSpillRole>;
}

struct LegacyInspectSource<'a, R> {
    engine: &'a Engine<R>,
}

impl<R: EvaluationContext> LegacyInspectSource<'_, R> {
    fn cell_ref(&self, key: CellKey) -> CellRef {
        CellRef::new(key.sheet_id, Coord::new(key.row0, key.col0, true, true))
    }
}

impl<R: EvaluationContext> InspectSource for LegacyInspectSource<'_, R> {
    fn formula_at(&self, cell: CellKey) -> Result<Option<FormulaView>, InspectError> {
        let sheet = self.engine.graph.sheet_name(cell.sheet_id);
        let row = cell.row0 + 1;
        let col = cell.col0 + 1;
        if let Some(text) = self.engine.get_staged_formula_text(sheet, row, col) {
            let ast =
                formualizer_parse::parse(&text).map_err(|error| InspectError::InvalidAddress {
                    message: format!("staged formula at {sheet}!R{row}C{col} is invalid: {error}"),
                })?;
            return Ok(Some(FormulaView {
                ast,
                volatile: false,
                dirty: true,
            }));
        }

        let cell_ref = self.cell_ref(cell);
        let Some(vertex) = self.engine.graph.get_vertex_for_cell(&cell_ref) else {
            return Ok(None);
        };
        let Some(ast) = self.engine.graph.get_formula(vertex) else {
            return Ok(None);
        };
        Ok(Some(FormulaView {
            ast,
            volatile: self.engine.graph.is_volatile(vertex),
            dirty: self.engine.graph.is_dirty(vertex),
        }))
    }

    fn visit_declared_references(
        &self,
        cell: CellKey,
        visitor: &mut dyn ReferenceVisitor,
    ) -> Result<(), InspectError> {
        let Some(formula) = self.formula_at(cell)? else {
            return Ok(());
        };

        struct Context<'a> {
            visitor: &'a mut dyn ReferenceVisitor,
            stopped: bool,
        }
        fn local_bindings(_: &Context<'_>, name: &str, _: usize) -> refs::LocalBindingStyle {
            match name
                .rsplit('.')
                .next()
                .unwrap_or(name)
                .to_ascii_uppercase()
                .as_str()
            {
                "LET" => refs::LocalBindingStyle::LocalBindingPairs,
                "LAMBDA" => refs::LocalBindingStyle::LambdaParameters,
                _ => refs::LocalBindingStyle::None,
            }
        }
        fn consume(
            context: &mut Context<'_>,
            reference: refs::SemanticReference<'_>,
        ) -> Result<(), ExcelError> {
            if context.visitor.visit(reference) {
                Ok(())
            } else {
                context.stopped = true;
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("inspection visitor requested stop"))
            }
        }

        let mut context = Context {
            visitor,
            stopped: false,
        };
        let result =
            refs::visit_tree_references(&formula.ast, &mut context, local_bindings, consume);
        if context.stopped {
            Ok(())
        } else {
            result.map_err(|error| InspectError::InvalidAddress {
                message: error.to_string(),
            })
        }
    }

    fn visit_dependents_covering(
        &self,
        cell: CellKey,
        budget: &mut WorkBudget,
        visitor: &mut dyn DependentVisitor,
    ) -> Result<QueryCompleteness, InspectError> {
        let complete = self.engine.graph.visit_range_dependents_covering_bounded(
            cell.sheet_id,
            cell.row0,
            cell.col0,
            &mut budget.remaining,
            &mut |vertex| {
                self.engine
                    .graph
                    .get_cell_ref(vertex)
                    .is_none_or(|cell_ref| {
                        visitor.visit(CellKey {
                            sheet_id: cell_ref.sheet_id,
                            row0: cell_ref.coord.row(),
                            col0: cell_ref.coord.col(),
                        })
                    })
            },
        );
        Ok(if complete {
            QueryCompleteness::Complete
        } else {
            QueryCompleteness::Incomplete
        })
    }

    fn spill_role(&self, cell: CellKey) -> Option<InternalSpillRole> {
        let cell_ref = self.cell_ref(cell);
        if let Some(vertex) = self.engine.graph.get_vertex_for_cell(&cell_ref)
            && let Some(cells) = self.engine.graph.spill_cells_for_anchor(vertex)
        {
            let mut bounds: Option<(u32, u32, u32, u32)> = None;
            for member in cells {
                bounds = Some(match bounds {
                    None => (
                        member.coord.row(),
                        member.coord.col(),
                        member.coord.row(),
                        member.coord.col(),
                    ),
                    Some((sr, sc, er, ec)) => (
                        sr.min(member.coord.row()),
                        sc.min(member.coord.col()),
                        er.max(member.coord.row()),
                        ec.max(member.coord.col()),
                    ),
                });
            }
            if let Some((sr, sc, er, ec)) = bounds {
                let sheet = self.engine.graph.sheet_name(cell.sheet_id).to_string();
                return Some(InternalSpillRole::Anchor {
                    extent: RangeAddress {
                        sheet,
                        start_row: sr + 1,
                        start_col: sc + 1,
                        end_row: er + 1,
                        end_col: ec + 1,
                    },
                });
            }
        }
        let anchor = self.engine.graph.spill_registry_anchor_for_cell(cell_ref)?;
        let anchor_ref = self.engine.graph.get_cell_ref(anchor)?;
        Some(InternalSpillRole::Member {
            anchor: CellKey {
                sheet_id: anchor_ref.sheet_id,
                row0: anchor_ref.coord.row(),
                col0: anchor_ref.coord.col(),
            },
        })
    }
}

fn merge_omitted(target: &mut Option<OmittedCount>, addition: OmittedCount) {
    *target = Some(match (target.take(), addition) {
        (None, value) => value,
        (Some(OmittedCount::Exact(a)), OmittedCount::Exact(b)) => {
            OmittedCount::Exact(a.saturating_add(b))
        }
        (Some(OmittedCount::Exact(a)), OmittedCount::AtLeast(b))
        | (Some(OmittedCount::AtLeast(a)), OmittedCount::Exact(b))
        | (Some(OmittedCount::AtLeast(a)), OmittedCount::AtLeast(b)) => {
            OmittedCount::AtLeast(a.saturating_add(b))
        }
    });
}

fn address_cmp(left: &CellAddress, right: &CellAddress) -> std::cmp::Ordering {
    left.sheet
        .cmp(&right.sheet)
        .then_with(|| left.row.cmp(&right.row))
        .then_with(|| left.column.cmp(&right.column))
}

impl<R: EvaluationContext> Engine<R> {
    fn inspect_stamp(&self) -> StateStamp {
        StateStamp {
            mutation_revision: self.inspection_mutation_revision(),
            recalc_epoch: self.recalc_epoch,
        }
    }

    fn inspect_source(&self) -> LegacyInspectSource<'_, R> {
        LegacyInspectSource { engine: self }
    }

    fn canonical_cell(
        &self,
        address: &CellAddress,
    ) -> Result<(CellKey, CellAddress), InspectError> {
        CellAddress::new(address.sheet.clone(), address.row, address.column).map_err(|error| {
            InspectError::InvalidAddress {
                message: error.to_string(),
            }
        })?;
        let Some(sheet_id) = self.graph.sheet_id(&address.sheet) else {
            return Err(InspectError::SheetNotFound {
                sheet: address.sheet.clone(),
            });
        };
        let canonical = CellAddress {
            sheet: self.graph.sheet_name(sheet_id).to_string(),
            row: address.row,
            column: address.column,
        };
        Ok((
            CellKey {
                sheet_id,
                row0: address.row - 1,
                col0: address.column - 1,
            },
            canonical,
        ))
    }

    fn canonical_area(&self, area: &RangeArea) -> Result<(SheetId, RangeArea), InspectError> {
        RangeArea::new(
            area.sheet.clone(),
            area.start_row,
            area.start_column,
            area.end_row,
            area.end_column,
        )
        .map_err(|error| InspectError::InvalidAddress {
            message: error.to_string(),
        })?;
        let Some(sheet_id) = self.graph.sheet_id(&area.sheet) else {
            return Err(InspectError::SheetNotFound {
                sheet: area.sheet.clone(),
            });
        };
        Ok((
            sheet_id,
            RangeArea {
                sheet: self.graph.sheet_name(sheet_id).to_string(),
                start_row: area.start_row,
                start_column: area.start_column,
                end_row: area.end_row,
                end_column: area.end_column,
            },
        ))
    }

    fn address_for_key(&self, key: CellKey) -> CellAddress {
        CellAddress {
            sheet: self.graph.sheet_name(key.sheet_id).to_string(),
            row: key.row0 + 1,
            column: key.col0 + 1,
        }
    }

    fn key_for_vertex(&self, vertex: VertexId) -> Option<CellKey> {
        if !matches!(
            self.graph.get_vertex_kind(vertex),
            VertexKind::FormulaScalar | VertexKind::FormulaArray
        ) {
            return None;
        }
        let cell = self.graph.get_cell_ref(vertex)?;
        Some(CellKey {
            sheet_id: cell.sheet_id,
            row0: cell.coord.row(),
            col0: cell.coord.col(),
        })
    }

    fn resolve_semantic_area(&self, area: &RangeArea) -> Option<RangeAddress> {
        let extent = resolve_used_extent(
            OpenRangeBounds {
                start_row: area.start_row,
                start_column: area.start_column,
                end_row: area.end_row,
                end_column: area.end_column,
            },
            ExtentPolicy::Semantic,
            |first, last| self.semantic_used_rows_for_columns(&area.sheet, first, last),
            |first, last| self.semantic_used_cols_for_rows(&area.sheet, first, last),
        )?;
        Some(Self::range_from_extent(&area.sheet, extent))
    }

    fn range_from_extent(sheet: &str, extent: ResolvedExtent) -> RangeAddress {
        RangeAddress {
            sheet: sheet.to_string(),
            start_row: extent.start_row,
            start_col: extent.start_column,
            end_row: extent.end_row,
            end_col: extent.end_column,
        }
    }

    fn snapshot_for_key(
        &self,
        key: CellKey,
        include_value: bool,
    ) -> Result<CellSnapshot, InspectError> {
        let source = self.inspect_source();
        let formula = source.formula_at(key)?;
        let address = self.address_for_key(key);
        let cached_value = self.read_cell_value(&address.sheet, address.row, address.column);
        let (canonical_formula, volatile, staleness) = match formula {
            Some(view) => {
                let staleness = if cached_value.is_none() {
                    Staleness::NeverEvaluated
                } else if view.dirty {
                    Staleness::Dirty
                } else {
                    Staleness::Current
                };
                (
                    Some(formualizer_parse::pretty::canonical_formula(&view.ast)),
                    view.volatile,
                    staleness,
                )
            }
            None => (None, false, Staleness::Current),
        };
        let spill = source.spill_role(key).map(|role| match role {
            InternalSpillRole::Anchor { extent } => SpillRole::Anchor { extent },
            InternalSpillRole::Member { anchor } => SpillRole::Member {
                anchor: self.address_for_key(anchor),
            },
        });
        Ok(CellSnapshot {
            address,
            formula: canonical_formula,
            value: include_value.then_some(cached_value).flatten(),
            value_included: include_value,
            staleness,
            volatile,
            spill,
        })
    }

    /// Inspect one cell without evaluating or preparing workbook state.
    pub fn inspect_cell(
        &self,
        cell: &CellAddress,
        options: &SnapshotOptions,
    ) -> Result<CellSnapshotReport, InspectError> {
        let (key, _) = self.canonical_cell(cell)?;
        Ok(CellSnapshotReport {
            stamp: self.inspect_stamp(),
            cell: self.snapshot_for_key(key, options.include_value)?,
        })
    }

    fn resolve_name(&self, key: CellKey, name: &str) -> NameResolution {
        let Some(named) = self.graph.resolve_name_entry(name, key.sheet_id) else {
            return NameResolution::Unresolved;
        };
        match &named.definition {
            NamedDefinition::Cell(cell) => NameResolution::Cell(CellAddress {
                sheet: self.graph.sheet_name(cell.sheet_id).to_string(),
                row: cell.coord.row() + 1,
                column: cell.coord.col() + 1,
            }),
            NamedDefinition::Range(range) => {
                let resolved = RangeAddress {
                    sheet: self.graph.sheet_name(range.start.sheet_id).to_string(),
                    start_row: range.start.coord.row() + 1,
                    start_col: range.start.coord.col() + 1,
                    end_row: range.end.coord.row() + 1,
                    end_col: range.end.coord.col() + 1,
                };
                NameResolution::Range {
                    declared: RangeArea::from_finite(&resolved),
                    resolved: Some(resolved),
                }
            }
            NamedDefinition::Literal(value) => NameResolution::Literal(value.clone()),
            NamedDefinition::Formula { ast, .. } => NameResolution::Formula {
                formula: formualizer_parse::pretty::canonical_formula(ast),
                value: self.graph.get_value(named.vertex),
            },
        }
    }

    fn resolve_table_area(
        &self,
        key: CellKey,
        table_ref: &TableReference,
    ) -> Option<(String, String, RangeAddress)> {
        let metadata = self.table_metadata(&table_ref.name)?;
        let canonical_name = metadata.name.clone();
        let specifier = table_ref
            .specifier
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let mut start_row = metadata.start_row;
        let mut end_row = metadata.end_row;
        let mut start_col = metadata.start_col;
        let mut end_col = metadata.end_col;
        let data_start = start_row + u32::from(metadata.header_row);
        let data_end = end_row.saturating_sub(u32::from(metadata.totals_row));

        fn col_index(headers: &[String], name: &str) -> Option<u32> {
            headers
                .iter()
                .position(|header| header.eq_ignore_ascii_case(name))
                .and_then(|index| u32::try_from(index).ok())
        }

        match table_ref.specifier.as_ref()? {
            TableSpecifier::All | TableSpecifier::SpecialItem(SpecialItem::All) => {}
            TableSpecifier::Data | TableSpecifier::SpecialItem(SpecialItem::Data) => {
                start_row = data_start;
                end_row = data_end;
            }
            TableSpecifier::Headers | TableSpecifier::SpecialItem(SpecialItem::Headers) => {
                if !metadata.header_row {
                    return None;
                }
                end_row = start_row;
            }
            TableSpecifier::Totals | TableSpecifier::SpecialItem(SpecialItem::Totals) => {
                if !metadata.totals_row {
                    return None;
                }
                start_row = end_row;
            }
            TableSpecifier::Column(name) => {
                let index = col_index(&metadata.headers, name)?;
                start_col += index;
                end_col = start_col;
                start_row = data_start;
                end_row = data_end;
            }
            TableSpecifier::ColumnRange(first, last) => {
                let mut first = col_index(&metadata.headers, first)?;
                let mut last = col_index(&metadata.headers, last)?;
                if first > last {
                    std::mem::swap(&mut first, &mut last);
                }
                start_col += first;
                end_col = metadata.start_col + last;
                start_row = data_start;
                end_row = data_end;
            }
            TableSpecifier::SpecialItem(SpecialItem::ThisRow)
            | TableSpecifier::Row(formualizer_parse::parser::TableRowSpecifier::Current) => {
                let row = key.row0 + 1;
                if row < data_start || row > data_end {
                    return None;
                }
                start_row = row;
                end_row = row;
            }
            TableSpecifier::Row(_) => return None,
            TableSpecifier::Combination(parts) => {
                let mut this_row = false;
                let mut selected_column: Option<(u32, u32)> = None;
                for part in parts {
                    match part.as_ref() {
                        TableSpecifier::SpecialItem(SpecialItem::ThisRow) => this_row = true,
                        TableSpecifier::Column(name) => {
                            let column = col_index(&metadata.headers, name)?;
                            selected_column = Some((column, column));
                        }
                        TableSpecifier::ColumnRange(first, last) => {
                            let mut first = col_index(&metadata.headers, first)?;
                            let mut last = col_index(&metadata.headers, last)?;
                            if first > last {
                                std::mem::swap(&mut first, &mut last);
                            }
                            selected_column = Some((first, last));
                        }
                        TableSpecifier::Data | TableSpecifier::SpecialItem(SpecialItem::Data) => {
                            start_row = data_start;
                            end_row = data_end;
                        }
                        TableSpecifier::Headers
                        | TableSpecifier::SpecialItem(SpecialItem::Headers) => {
                            if !metadata.header_row {
                                return None;
                            }
                            end_row = start_row;
                        }
                        TableSpecifier::Totals
                        | TableSpecifier::SpecialItem(SpecialItem::Totals) => {
                            if !metadata.totals_row {
                                return None;
                            }
                            start_row = end_row;
                        }
                        TableSpecifier::All | TableSpecifier::SpecialItem(SpecialItem::All) => {}
                        TableSpecifier::Row(_) | TableSpecifier::Combination(_) => return None,
                    }
                }
                if this_row {
                    let row = key.row0 + 1;
                    if row < data_start || row > data_end {
                        return None;
                    }
                    start_row = row;
                    end_row = row;
                }
                if let Some((first, last)) = selected_column {
                    start_col = metadata.start_col + first;
                    end_col = metadata.start_col + last;
                    if !this_row {
                        start_row = data_start;
                        end_row = data_end;
                    }
                }
            }
        }
        if start_row > end_row || start_col > end_col {
            return None;
        }
        Some((
            canonical_name,
            specifier,
            RangeAddress {
                sheet: metadata.sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            },
        ))
    }

    fn own_reference(
        &self,
        key: CellKey,
        reference: refs::SemanticReference<'_>,
    ) -> SemanticReference {
        match reference {
            refs::SemanticReference::Cell(cell) => {
                let sheet_id = cell
                    .sheet
                    .name()
                    .and_then(|name| self.graph.sheet_id(name))
                    .unwrap_or(key.sheet_id);
                SemanticReference::Cell(CellAddress {
                    sheet: self.graph.sheet_name(sheet_id).to_string(),
                    row: cell.row,
                    column: cell.col,
                })
            }
            refs::SemanticReference::FiniteRange(range)
            | refs::SemanticReference::OpenRange(range) => {
                let sheet_id = range
                    .sheet
                    .name()
                    .and_then(|name| self.graph.sheet_id(name))
                    .unwrap_or(key.sheet_id);
                let declared = RangeArea {
                    sheet: self.graph.sheet_name(sheet_id).to_string(),
                    start_row: range.start_row,
                    start_column: range.start_col,
                    end_row: range.end_row,
                    end_column: range.end_col,
                };
                let resolved = self.resolve_semantic_area(&declared);
                let cell_count = resolved.as_ref().map_or(0, |range| {
                    u64::from(range.width()) * u64::from(range.height())
                });
                SemanticReference::Range {
                    declared,
                    resolved,
                    cell_count,
                }
            }
            refs::SemanticReference::Name(name) => SemanticReference::Name {
                name: name.to_string(),
                resolution: self.resolve_name(key, name),
            },
            refs::SemanticReference::Table(table) => {
                if let Some((name, specifier, resolved)) = self.resolve_table_area(key, table) {
                    SemanticReference::Table {
                        name,
                        specifier,
                        resolved,
                    }
                } else {
                    SemanticReference::Unsupported {
                        text: ReferenceType::Table(table.clone()).to_string(),
                        reason: "structured reference could not be resolved at this placement"
                            .to_string(),
                    }
                }
            }
            refs::SemanticReference::ExternalSource(external) => SemanticReference::External {
                raw: external.raw.clone(),
            },
            refs::SemanticReference::ThreeDimensional(reference) => {
                SemanticReference::Unsupported {
                    text: reference.to_string(),
                    reason: "3D references are not supported by phase-1 introspection".to_string(),
                }
            }
            refs::SemanticReference::Unsupported(reference) => SemanticReference::Unsupported {
                text: reference.to_string(),
                reason: "reference form is unsupported by introspection".to_string(),
            },
        }
    }

    fn collect_precedents(
        &self,
        key: CellKey,
        max_links: u32,
        work: &mut WorkBudget,
    ) -> Result<(Vec<Precedent>, TruncationReport), InspectError> {
        struct Collector<'a, R> {
            engine: &'a Engine<R>,
            key: CellKey,
            max_links: usize,
            work: &'a mut WorkBudget,
            precedents: Vec<Precedent>,
            truncated: bool,
        }
        impl<R: EvaluationContext> ReferenceVisitor for Collector<'_, R> {
            fn visit(&mut self, reference: refs::SemanticReference<'_>) -> bool {
                if !self.work.charge() {
                    self.truncated = true;
                    return false;
                }
                let reference = self.engine.own_reference(self.key, reference);
                if self
                    .precedents
                    .iter()
                    .any(|existing| existing.reference == reference)
                {
                    return true;
                }
                if self.precedents.len() >= self.max_links {
                    self.truncated = true;
                    return false;
                }
                self.precedents.push(Precedent {
                    reference,
                    provenance: Provenance::Declared,
                });
                true
            }
        }

        let source = self.inspect_source();
        let mut collector = Collector {
            engine: self,
            key,
            max_links: max_links as usize,
            work,
            precedents: Vec::new(),
            truncated: false,
        };
        source.visit_declared_references(key, &mut collector)?;
        let truncation = if collector.truncated {
            TruncationReport {
                incomplete: true,
                omitted: Some(OmittedCount::AtLeast(1)),
            }
        } else {
            TruncationReport::default()
        };
        Ok((collector.precedents, truncation))
    }

    /// Return source-ordered, first-occurrence-deduplicated declared formula
    /// references for a cell.
    pub fn precedents(
        &self,
        cell: &CellAddress,
        options: &PrecedentOptions,
    ) -> Result<PrecedentReport, InspectError> {
        let (key, canonical) = self.canonical_cell(cell)?;
        let mut work = WorkBudget::new(options.max_work);
        let (precedents, truncation) =
            self.collect_precedents(key, options.max_links, &mut work)?;
        Ok(PrecedentReport {
            stamp: self.inspect_stamp(),
            cell: canonical,
            precedents,
            truncation,
        })
    }

    fn dependency_state_available(&self) -> Result<(), InspectError> {
        if self.has_staged_formulas() {
            Err(InspectError::DependencyStateUnavailable {
                reason: InspectionUnavailableReason::DeferredDependencyGraph,
            })
        } else {
            Ok(())
        }
    }

    fn spill_query_members(&self, key: CellKey) -> Vec<CellKey> {
        let source = self.inspect_source();
        let Some(InternalSpillRole::Anchor { .. }) = source.spill_role(key) else {
            return vec![key];
        };
        let cell_ref = CellRef::new(key.sheet_id, Coord::new(key.row0, key.col0, true, true));
        let Some(vertex) = self.graph.get_vertex_for_cell(&cell_ref) else {
            return vec![key];
        };
        self.graph
            .spill_cells_for_anchor(vertex)
            .unwrap_or(&[])
            .iter()
            .map(|member| CellKey {
                sheet_id: member.sheet_id,
                row0: member.coord.row(),
                col0: member.coord.col(),
            })
            .collect()
    }

    fn collect_dependents(
        &self,
        key: CellKey,
        max_results: u32,
        work: &mut WorkBudget,
    ) -> Result<(Vec<Dependent>, TruncationReport), InspectError> {
        self.dependency_state_available()?;
        let source = self.inspect_source();
        let mut found: FxHashMap<CellAddress, Vec<CellAddress>> = FxHashMap::default();
        let mut incomplete = false;
        let mut known_omitted_dependent = false;
        let max_results = max_results as usize;

        let members = self.spill_query_members(key);
        for member in members {
            if !work.charge() {
                incomplete = true;
                break;
            }
            let via = self.address_for_key(member);
            let mut record = |dependent_key: CellKey| {
                let address = self.address_for_key(dependent_key);
                if let Some(via_members) = found.get_mut(&address) {
                    if !via_members.contains(&via) {
                        via_members.push(via.clone());
                    }
                    return true;
                }
                if found.len() >= max_results {
                    incomplete = true;
                    known_omitted_dependent = true;
                    return false;
                }
                found.insert(address, vec![via.clone()]);
                true
            };

            let member_ref = CellRef::new(
                member.sheet_id,
                Coord::new(member.row0, member.col0, true, true),
            );
            if let Some(vertex) = self.graph.get_vertex_for_cell(&member_ref) {
                let complete = self.graph.visit_direct_dependents_bounded(
                    vertex,
                    &mut work.remaining,
                    &mut |dependent| self.key_for_vertex(dependent).is_none_or(&mut record),
                );
                if !complete {
                    incomplete = true;
                    break;
                }
            }

            struct Visitor<'a, F>(&'a mut F);
            impl<F: FnMut(CellKey) -> bool> DependentVisitor for Visitor<'_, F> {
                fn visit(&mut self, dependent: CellKey) -> bool {
                    (self.0)(dependent)
                }
            }
            let mut visitor = Visitor(&mut record);
            if source.visit_dependents_covering(member, work, &mut visitor)?
                == QueryCompleteness::Incomplete
            {
                incomplete = true;
                break;
            }
        }

        let mut dependents: Vec<_> = found
            .into_iter()
            .map(|(cell, mut via)| {
                via.sort_by(address_cmp);
                via.dedup();
                Dependent { cell, via }
            })
            .collect();
        dependents.sort_by(|left, right| address_cmp(&left.cell, &right.cell));
        Ok((
            dependents,
            if incomplete {
                TruncationReport {
                    incomplete: true,
                    omitted: Some(OmittedCount::AtLeast(u64::from(known_omitted_dependent))),
                }
            } else {
                TruncationReport::default()
            },
        ))
    }

    /// Return direct and compressed-range readers of a cell. Discovery is
    /// bounded before candidate materialization.
    pub fn dependents(
        &self,
        cell: &CellAddress,
        options: &DependentsOptions,
    ) -> Result<DependentsReport, InspectError> {
        let (key, canonical) = self.canonical_cell(cell)?;
        let mut work = WorkBudget::new(options.max_work);
        let (dependents, truncation) =
            self.collect_dependents(key, options.max_results, &mut work)?;
        Ok(DependentsReport {
            stamp: self.inspect_stamp(),
            cell: canonical,
            dependents,
            truncation,
        })
    }

    fn target_addresses(reference: &SemanticReference) -> Option<&RangeAddress> {
        match reference {
            SemanticReference::Range { resolved, .. } => resolved.as_ref(),
            SemanticReference::Name {
                resolution: NameResolution::Range { resolved, .. },
                ..
            } => resolved.as_ref(),
            SemanticReference::Table { resolved, .. } => Some(resolved),
            _ => None,
        }
    }

    fn target_cell(reference: &SemanticReference) -> Option<&CellAddress> {
        match reference {
            SemanticReference::Cell(cell) => Some(cell),
            SemanticReference::Name {
                resolution: NameResolution::Cell(cell),
                ..
            } => Some(cell),
            _ => None,
        }
    }

    fn is_ancestor(
        target: TraceNodeId,
        source: TraceNodeId,
        parents: &HashMap<TraceNodeId, Option<TraceNodeId>>,
    ) -> bool {
        let mut cursor = Some(source);
        while let Some(node) = cursor {
            if node == target {
                return true;
            }
            cursor = parents.get(&node).copied().flatten();
        }
        false
    }

    /// Build a bounded response-local BFS DAG.
    pub fn trace(
        &self,
        roots: &[CellAddress],
        options: &TraceOptions,
    ) -> Result<TraceGraph, InspectError> {
        if roots.is_empty() {
            return Err(InspectError::InvalidOptions {
                message: "trace requires at least one root".to_string(),
            });
        }
        if options.max_nodes == 0 {
            return Err(InspectError::InvalidOptions {
                message: "max_nodes must be at least one".to_string(),
            });
        }
        if options.direction == TraceDirection::Dependents {
            self.dependency_state_available()?;
        }

        let mut nodes = Vec::new();
        let mut node_by_address: FxHashMap<CellAddress, TraceNodeId> = FxHashMap::default();
        let mut root_ids = Vec::new();
        let mut queue = VecDeque::new();
        let mut parents: HashMap<TraceNodeId, Option<TraceNodeId>> = HashMap::new();
        let mut truncation = TruncationReport::default();

        for root in roots {
            let (key, canonical) = self.canonical_cell(root)?;
            if let Some(&id) = node_by_address.get(&canonical) {
                root_ids.push(id);
                continue;
            }
            if nodes.len() >= options.max_nodes as usize {
                truncation.incomplete = true;
                merge_omitted(&mut truncation.omitted, OmittedCount::AtLeast(1));
                continue;
            }
            let id = TraceNodeId(nodes.len() as u32);
            nodes.push(TraceNode {
                id,
                cell: self.snapshot_for_key(key, options.include_values)?,
                links: Vec::new(),
            });
            node_by_address.insert(canonical, id);
            root_ids.push(id);
            queue.push_back((id, key, 0u32));
            parents.insert(id, None);
        }

        let mut work = WorkBudget::new(options.max_work);
        let mut links_used = 0u32;
        let mut range_members_used = 0u32;

        while let Some((source_id, source_key, depth)) = queue.pop_front() {
            let can_follow = depth < options.max_depth;
            let mut links = Vec::new();
            if options.direction == TraceDirection::Precedents {
                if let Some(InternalSpillRole::Member { anchor }) =
                    self.inspect_source().spill_role(source_key)
                {
                    if links_used < options.max_links {
                        links_used += 1;
                        let anchor_address = self.address_for_key(anchor);
                        let mut link = TraceLink {
                            reference: SemanticReference::Cell(anchor_address.clone()),
                            kind: TraceLinkKind::SpillAnchor,
                            targets: Vec::new(),
                            omitted: None,
                        };
                        self.attach_cell_target(
                            anchor_address,
                            source_id,
                            anchor,
                            can_follow,
                            depth,
                            options,
                            &mut nodes,
                            &mut node_by_address,
                            &mut parents,
                            &mut queue,
                            &mut link,
                            &mut truncation,
                        )?;
                        links.push(link);
                    } else {
                        truncation.incomplete = true;
                        merge_omitted(&mut truncation.omitted, OmittedCount::AtLeast(1));
                    }
                } else {
                    let available_links = options.max_links.saturating_sub(links_used);
                    let (precedents, local_truncation) =
                        self.collect_precedents(source_key, available_links, &mut work)?;
                    if local_truncation.incomplete {
                        truncation.incomplete = true;
                        if let Some(omitted) = local_truncation.omitted {
                            merge_omitted(&mut truncation.omitted, omitted);
                        }
                    }
                    for precedent in precedents {
                        links_used += 1;
                        let mut link = TraceLink {
                            reference: precedent.reference,
                            kind: TraceLinkKind::Formula {
                                provenance: precedent.provenance,
                            },
                            targets: Vec::new(),
                            omitted: None,
                        };
                        if let Some(cell) = Self::target_cell(&link.reference).cloned() {
                            let (target_key, canonical) = self.canonical_cell(&cell)?;
                            self.attach_cell_target(
                                canonical,
                                source_id,
                                target_key,
                                can_follow,
                                depth,
                                options,
                                &mut nodes,
                                &mut node_by_address,
                                &mut parents,
                                &mut queue,
                                &mut link,
                                &mut truncation,
                            )?;
                        } else if let Some(range) = Self::target_addresses(&link.reference).cloned()
                        {
                            self.attach_range_targets(
                                &range,
                                source_id,
                                can_follow,
                                depth,
                                options,
                                &mut range_members_used,
                                &mut nodes,
                                &mut node_by_address,
                                &mut parents,
                                &mut queue,
                                &mut link,
                                &mut truncation,
                            )?;
                        }
                        links.push(link);
                    }
                }
            } else {
                let available = options.max_links.saturating_sub(links_used);
                let (dependents, local_truncation) =
                    self.collect_dependents(source_key, available, &mut work)?;
                if local_truncation.incomplete {
                    truncation.incomplete = true;
                    if let Some(omitted) = local_truncation.omitted {
                        merge_omitted(&mut truncation.omitted, omitted);
                    }
                }
                for dependent in dependents {
                    links_used += 1;
                    let spill_reader = dependent
                        .via
                        .iter()
                        .any(|via| via != &self.address_for_key(source_key));
                    let (target_key, canonical) = self.canonical_cell(&dependent.cell)?;
                    let mut link = TraceLink {
                        reference: SemanticReference::Cell(canonical.clone()),
                        kind: if spill_reader {
                            TraceLinkKind::SpillReader
                        } else {
                            TraceLinkKind::Formula {
                                provenance: Provenance::Declared,
                            }
                        },
                        targets: Vec::new(),
                        omitted: None,
                    };
                    self.attach_cell_target(
                        canonical,
                        source_id,
                        target_key,
                        can_follow,
                        depth,
                        options,
                        &mut nodes,
                        &mut node_by_address,
                        &mut parents,
                        &mut queue,
                        &mut link,
                        &mut truncation,
                    )?;
                    links.push(link);
                }
            }
            nodes[source_id.0 as usize].links = links;
        }

        Ok(TraceGraph {
            stamp: self.inspect_stamp(),
            direction: options.direction,
            roots: root_ids,
            nodes,
            truncation,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn attach_cell_target(
        &self,
        address: CellAddress,
        source_id: TraceNodeId,
        key: CellKey,
        can_follow: bool,
        depth: u32,
        options: &TraceOptions,
        nodes: &mut Vec<TraceNode>,
        node_by_address: &mut FxHashMap<CellAddress, TraceNodeId>,
        parents: &mut HashMap<TraceNodeId, Option<TraceNodeId>>,
        queue: &mut VecDeque<(TraceNodeId, CellKey, u32)>,
        link: &mut TraceLink,
        truncation: &mut TruncationReport,
    ) -> Result<(), InspectError> {
        if let Some(&target_id) = node_by_address.get(&address) {
            let disposition = if Self::is_ancestor(target_id, source_id, parents) {
                LinkDisposition::Cycle
            } else {
                LinkDisposition::Convergent
            };
            link.targets.push(TraceLinkTarget {
                node: target_id,
                disposition,
            });
            return Ok(());
        }
        if nodes.len() >= options.max_nodes as usize {
            link.omitted = Some(OmittedCount::AtLeast(1));
            truncation.incomplete = true;
            merge_omitted(&mut truncation.omitted, OmittedCount::AtLeast(1));
            return Ok(());
        }
        let target_id = TraceNodeId(nodes.len() as u32);
        nodes.push(TraceNode {
            id: target_id,
            cell: self.snapshot_for_key(key, options.include_values)?,
            links: Vec::new(),
        });
        node_by_address.insert(address, target_id);
        parents.insert(target_id, Some(source_id));
        link.targets.push(TraceLinkTarget {
            node: target_id,
            disposition: if can_follow {
                LinkDisposition::Expanded
            } else {
                LinkDisposition::Elided
            },
        });
        if can_follow {
            queue.push_back((target_id, key, depth + 1));
        } else {
            truncation.incomplete = true;
            merge_omitted(&mut truncation.omitted, OmittedCount::AtLeast(1));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn attach_range_targets(
        &self,
        range: &RangeAddress,
        source_id: TraceNodeId,
        can_follow: bool,
        depth: u32,
        options: &TraceOptions,
        range_members_used: &mut u32,
        nodes: &mut Vec<TraceNode>,
        node_by_address: &mut FxHashMap<CellAddress, TraceNodeId>,
        parents: &mut HashMap<TraceNodeId, Option<TraceNodeId>>,
        queue: &mut VecDeque<(TraceNodeId, CellKey, u32)>,
        link: &mut TraceLink,
        truncation: &mut TruncationReport,
    ) -> Result<(), InspectError> {
        let total = u64::from(range.width()) * u64::from(range.height());
        let mut attached = 0u64;
        let mut compressed_ancestors = Vec::new();
        let mut cursor = Some(source_id);
        while let Some(ancestor) = cursor {
            let address = &nodes[ancestor.0 as usize].cell.address;
            if range.sheet == address.sheet
                && range.start_row <= address.row
                && address.row <= range.end_row
                && range.start_col <= address.column
                && address.column <= range.end_col
            {
                compressed_ancestors.push((ancestor, address.row, address.column));
            }
            cursor = parents.get(&ancestor).copied().flatten();
        }

        // Preserve compressed cycle semantics even if the global member budget
        // is exhausted or an ancestor occurs late in a large row-major range.
        for (ancestor, _, _) in &compressed_ancestors {
            link.targets.push(TraceLinkTarget {
                node: *ancestor,
                disposition: LinkDisposition::Cycle,
            });
            attached += 1;
        }

        'rows: for row in range.start_row..=range.end_row {
            for column in range.start_col..=range.end_col {
                if compressed_ancestors
                    .iter()
                    .any(|(_, ancestor_row, ancestor_column)| {
                        row == *ancestor_row && column == *ancestor_column
                    })
                {
                    continue;
                }
                if *range_members_used >= options.range_member_budget {
                    break 'rows;
                }
                *range_members_used += 1;
                let address = CellAddress {
                    sheet: range.sheet.clone(),
                    row,
                    column,
                };
                let (key, canonical) = self.canonical_cell(&address)?;
                let before = link.targets.len();
                self.attach_cell_target(
                    canonical,
                    source_id,
                    key,
                    can_follow,
                    depth,
                    options,
                    nodes,
                    node_by_address,
                    parents,
                    queue,
                    link,
                    truncation,
                )?;
                if link.targets.len() > before {
                    attached += 1;
                } else if link.omitted.is_some() {
                    break 'rows;
                }
            }
        }
        if attached < total {
            let omitted = OmittedCount::Exact(total - attached);
            link.omitted = Some(omitted);
            truncation.incomplete = true;
            merge_omitted(&mut truncation.omitted, omitted);
        }
        Ok(())
    }

    /// Return one row-major owned page over the semantic finite extent.
    pub fn range_page(
        &self,
        area: &RangeArea,
        options: &RangePageOptions,
    ) -> Result<RangePage, InspectError> {
        if options.limit == 0 {
            return Err(InspectError::InvalidOptions {
                message: "range page limit must be at least one".to_string(),
            });
        }
        let stamp = self.inspect_stamp();
        if let Some(expected) = options.expected_stamp
            && expected != stamp
        {
            return Err(InspectError::RevisionMismatch {
                expected,
                actual: stamp,
            });
        }
        let (_, declared) = self.canonical_area(area)?;
        let resolved = self.resolve_semantic_area(&declared);
        let total = resolved.as_ref().map_or(0, |range| {
            u64::from(range.width()) * u64::from(range.height())
        });
        let start = options.offset.min(total);
        let end = start.saturating_add(u64::from(options.limit)).min(total);
        let mut items = Vec::new();
        items
            .try_reserve((end - start) as usize)
            .map_err(|_| InspectError::ResourceExhausted {
                resource: "range page items",
            })?;
        if let Some(range) = &resolved {
            let width = u64::from(range.width());
            for offset in start..end {
                let row = range.start_row + u32::try_from(offset / width).unwrap_or(u32::MAX);
                let column = range.start_col + u32::try_from(offset % width).unwrap_or(u32::MAX);
                let (key, _) = self.canonical_cell(&CellAddress {
                    sheet: range.sheet.clone(),
                    row,
                    column,
                })?;
                items.push(self.snapshot_for_key(key, options.include_values)?);
            }
        }
        Ok(RangePage {
            stamp,
            declared,
            resolved,
            total,
            offset: options.offset,
            items,
            next_offset: (end < total).then_some(end),
        })
    }
}
