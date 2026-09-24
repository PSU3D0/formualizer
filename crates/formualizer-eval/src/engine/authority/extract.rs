//! Engine-fed extraction: the authority's view of one formula cell, read
//! from the formula arena through the same reference visitor and the same
//! name/table/source resolution the legacy graph uses
//! (`graph/formula_analysis.rs::collect_graph_reference`), so the relation
//! the authority stores is the one legacy installs (decision 11).
//!
//! | reference (as classified by `refs::classify`) | edges | tag, origin |
//! |---|---|---|
//! | cell, finite range, open range (own or named sheet) | the rectangle, bounds relative/absolute as written | R1, text |
//! | name defined as a cell or finite range | the target, absolute | R1, symbol (LK) |
//! | name defined as a formula | the name formula's references, flattened through nested names | X, symbol (LK) |
//! | name defined as a literal; source scalar/table; external | none (symbol only) | — |
//! | table | the table's whole range, as legacy registers it | X, symbol (LK) |
//! | unresolved name, unknown sheet, 3-D, unsupported | none; formula marked opaque | — |

use super::geom::{MAX_COL, MAX_ROW};
use super::proj::{AxisMap, Bound, RefProj};
use super::store::{
    EdgeSpec, F_DYNAMIC, F_OPAQUE, F_VOLATILE, FormulaFacts, LkKey, OriginSpec, Tag,
};
use super::template::template_facts;
use crate::SheetId;
use crate::engine::arena::AstNodeId;
use crate::engine::graph::DependencyGraph;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::refs::{self, LocalBindingStyle, SemanticReference};
use formualizer_common::ExcelError;
use formualizer_parse::parser::ASTNode;

pub const LK_NAME: u8 = 1;
pub const LK_TABLE: u8 = 2;

struct Ctx<'a> {
    graph: &'a DependencyGraph,
    /// Sheet of the formula (or the name's scope when flattening a name).
    sheet: SheetId,
    row: u32,
    col: u32,
    /// Flattening a formula-defined name: every bound is absolute, every
    /// edge is `X` and owned by `symbol`.
    symbol: Option<LkKey>,
    depth: u32,
    edges: Vec<EdgeSpec>,
    flags: u16,
}

fn bound(v1: u32, abs: bool, placement: u32, fixed: bool) -> Bound {
    // `v1` is 1-based as written.
    if abs || fixed {
        Bound::Abs(v1 - 1)
    } else {
        Bound::Rel(i64::from(v1) as i32 - 1 - placement as i32)
    }
}

fn opt_bound(v1: Option<u32>, abs: bool, placement: u32, fixed: bool) -> Bound {
    match v1 {
        Some(v) if v >= 1 => bound(v, abs, placement, fixed),
        _ => Bound::Open,
    }
}

impl Ctx<'_> {
    fn resolve_sheet(&mut self, name: Option<&str>) -> Option<SheetId> {
        match name {
            None => Some(self.sheet),
            Some(n) => {
                let s = self.graph.sheet_id(n);
                if s.is_none() {
                    self.flags |= F_OPAQUE;
                }
                s
            }
        }
    }

    fn push(&mut self, proj: RefProj) {
        let (tag, origin) = match &self.symbol {
            None => (Tag::R1, OriginSpec::Text),
            Some(k) => (Tag::X, OriginSpec::Symbol(k.clone())),
        };
        self.edges.push(EdgeSpec { proj, tag, origin });
    }

    fn push_fixed(
        &mut self,
        sheet: SheetId,
        r0: u32,
        c0: u32,
        r1: u32,
        c1: u32,
        tag: Tag,
        lk: LkKey,
    ) {
        let (tag, lk) = match &self.symbol {
            // Inside a flattened name everything is X, owned by the outer LK.
            Some(outer) => (Tag::X, outer.clone()),
            None => (tag, lk),
        };
        if r0 > r1 || c0 > c1 || r1 > MAX_ROW || c1 > MAX_COL {
            self.flags |= F_OPAQUE;
            return;
        }
        self.edges.push(EdgeSpec {
            proj: RefProj {
                sheet,
                rows: AxisMap::fixed(r0, r1),
                cols: AxisMap::fixed(c0, c1),
            },
            tag,
            origin: OriginSpec::Symbol(lk),
        });
    }

    fn name_lk(&self, name: &str) -> LkKey {
        LkKey {
            ctx: self.sheet,
            kind: LK_NAME,
            name: self.graph.name_lookup_key(name).into_boxed_str(),
        }
    }

    fn flatten_name_formula(&mut self, ast: &ASTNode, scope: NameScope, lk: LkKey) {
        if self.depth > 32 {
            self.flags |= F_OPAQUE;
            return;
        }
        let scope_sheet = match scope {
            NameScope::Sheet(id) => id,
            NameScope::Workbook => self.graph.default_sheet_id(),
        };
        let mut inner = Ctx {
            graph: self.graph,
            sheet: scope_sheet,
            row: 0,
            col: 0,
            symbol: Some(self.symbol.clone().unwrap_or(lk)),
            depth: self.depth + 1,
            edges: Vec::new(),
            flags: 0,
        };
        let _ = refs::visit_tree_references(
            ast,
            &mut inner,
            |_, _, _| LocalBindingStyle::None,
            collect,
        );
        self.edges.append(&mut inner.edges);
        self.flags |= inner.flags;
    }
}

fn collect(ctx: &mut Ctx<'_>, r: SemanticReference<'_>) -> Result<(), ExcelError> {
    let fixed = ctx.symbol.is_some();
    match r {
        SemanticReference::Cell(c) => {
            if let Some(s) = ctx.resolve_sheet(c.sheet.name()) {
                let rows = AxisMap::point(bound(c.row, c.row_abs, ctx.row, fixed));
                let cols = AxisMap::point(bound(c.col, c.col_abs, ctx.col, fixed));
                ctx.push(RefProj {
                    sheet: s,
                    rows,
                    cols,
                });
            }
        }
        SemanticReference::FiniteRange(rg) | SemanticReference::OpenRange(rg) => {
            if rg.is_reversed() {
                // Legacy rejects reversed finite ranges at ingest.
                ctx.flags |= F_OPAQUE;
                return Ok(());
            }
            if let Some(s) = ctx.resolve_sheet(rg.sheet.name()) {
                let rows = AxisMap {
                    lo: opt_bound(rg.start_row, rg.start_row_abs, ctx.row, fixed),
                    hi: opt_bound(rg.end_row, rg.end_row_abs, ctx.row, fixed),
                };
                let cols = AxisMap {
                    lo: opt_bound(rg.start_col, rg.start_col_abs, ctx.col, fixed),
                    hi: opt_bound(rg.end_col, rg.end_col_abs, ctx.col, fixed),
                };
                ctx.push(RefProj {
                    sheet: s,
                    rows,
                    cols,
                });
            }
        }
        SemanticReference::Name(name) => {
            let lk = ctx.name_lk(name);
            match ctx.graph.resolve_name_entry(name, ctx.sheet) {
                Some(entry) => match &entry.definition {
                    NamedDefinition::Cell(cr) => {
                        let (r, c) = (cr.coord.row(), cr.coord.col());
                        ctx.push_fixed(cr.sheet_id, r, c, r, c, Tag::R1, lk);
                    }
                    NamedDefinition::Range(rr) => {
                        ctx.push_fixed(
                            rr.start.sheet_id,
                            rr.start.coord.row(),
                            rr.start.coord.col(),
                            rr.end.coord.row(),
                            rr.end.coord.col(),
                            Tag::R1,
                            lk,
                        );
                    }
                    NamedDefinition::Literal(_) => {}
                    NamedDefinition::Formula { ast, .. } => {
                        let (ast, scope) = (ast.clone(), entry.scope);
                        ctx.flatten_name_formula(&ast, scope, lk);
                    }
                },
                None => {
                    if ctx.graph.resolve_source_scalar_entry(name).is_none() {
                        ctx.flags |= F_OPAQUE;
                    }
                }
            }
        }
        SemanticReference::Table(t) => match ctx.graph.resolve_table_entry(&t.name) {
            Some(entry) => {
                let rr = entry.range;
                let lk = LkKey {
                    ctx: ctx.sheet,
                    kind: LK_TABLE,
                    name: entry.name.clone().into_boxed_str(),
                };
                ctx.push_fixed(
                    rr.start.sheet_id,
                    rr.start.coord.row(),
                    rr.start.coord.col(),
                    rr.end.coord.row(),
                    rr.end.coord.col(),
                    Tag::X,
                    lk,
                );
            }
            None => ctx.flags |= F_OPAQUE,
        },
        SemanticReference::ExternalSource(_)
        | SemanticReference::ThreeDimensional(_)
        | SemanticReference::Unsupported(_) => ctx.flags |= F_OPAQUE,
    }
    Ok(())
}

/// The authority's facts for the formula `ast` at 0-based `(row, col)` of
/// `sheet`. `volatile`/`dynamic` come from the vertex flags legacy computed.
pub fn extract_formula(
    graph: &DependencyGraph,
    sheet: SheetId,
    row: u32,
    col: u32,
    ast: AstNodeId,
    volatile: bool,
    dynamic: bool,
) -> FormulaFacts {
    let mut ctx = Ctx {
        graph,
        sheet,
        row,
        col,
        symbol: None,
        depth: 0,
        edges: Vec::new(),
        flags: 0,
    };
    let _ = refs::visit_arena_references(
        ast,
        &mut ctx,
        |c| c.graph.data_store(),
        |c| c.graph.sheet_reg(),
        collect,
    );
    // Keep only references that instantiate at the cell (all do for an
    // installed formula), then deduplicate: R is a set.
    ctx.edges.retain(|e| e.proj.instantiate(row, col).is_some());
    ctx.edges.sort_unstable();
    ctx.edges.dedup();
    let t = template_facts(graph.data_store(), ast, row, col);
    let mut flags = ctx.flags;
    if volatile {
        flags |= F_VOLATILE;
    }
    if dynamic {
        flags |= F_DYNAMIC;
    }
    FormulaFacts {
        edges: ctx.edges,
        ltokens: t.relocatable.then(|| t.tokens.into_boxed_slice()),
        template: ast,
        literals: t.literals,
        flags,
    }
}
