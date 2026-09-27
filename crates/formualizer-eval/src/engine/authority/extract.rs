//! Engine-fed extraction: the authority's view of one formula cell, read
//! from the formula arena through the same reference visitor and the same
//! name/table/source resolution the legacy graph uses
//! (`graph/formula_analysis.rs::collect_graph_reference`), so the relation
//! the authority stores is the one legacy installs (decision 11).
//!
//! | reference (as classified by `refs::classify`) | edges | tag, origin |
//! |---|---|---|
//! | cell, finite range, open range (own or named sheet) | the rectangle, bounds relative/absolute as written | R1, text |
//! | any defined name | the name's symbol node (`SYMBOL_SHEET`, its slot) | X, symbol (LK) |
//! | name defined as a cell or finite range | also the target, absolute | R1, symbol (LK) |
//! | source scalar/table; external | none (symbol only) | — |
//!
//! A name's own node (`extract_symbol`) has the name formula's references
//! (absolute, in the name's scope; nested names are edges to their nodes) or
//! its cell/range target as X precedents, owned by the name's LK.
//! | table | the table's whole range, as legacy registers it | X, symbol (LK) |
//! | unresolved name, unknown sheet, 3-D, unsupported | none; formula marked opaque | — |

use super::geom::{MAX_COL, MAX_ROW, SYMBOL_SHEET};
use super::proj::{AxisMap, Bound, RefProj};
use super::store::{
    EdgeSpec, F_DYNAMIC, F_OPAQUE, F_VOLATILE, FormulaFacts, LkKey, OriginSpec, Tag,
};
use super::template::template_facts;
use crate::SheetId;
use crate::engine::arena::AstNodeId;
use crate::engine::graph::DependencyGraph;
use crate::engine::named_range::{NameScope, NamedDefinition, NamedRange};
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
    /// The current reference's sheet as a registry id, when the arena
    /// stored it that way (the reference itself then carries no sheet).
    sheet_key: Option<SheetId>,
    /// Symbols the formula names that are missing or have no symbol node
    /// yet, by binding key (a name's lookup key; `DependencyGraph::unbound_symbol_key` for
    /// tables and sheets): the host
    /// re-derives a name's node when one of them appears.
    missing: Vec<Box<str>>,
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
        // An id-keyed reference resolves through its registry name, exactly
        // as the reconstructed name would.
        let name = match (name, self.sheet_key) {
            (None, Some(id)) => Some(self.graph.sheet_reg().name(id)),
            _ => name,
        };
        match name {
            None => Some(self.sheet),
            Some(n) => {
                // Inside a name's definition, a renamed sheet's old name
                // still reaches that sheet (legacy kept the name's edges).
                let s = self.graph.sheet_id(n).or_else(|| {
                    self.symbol
                        .as_ref()
                        .and_then(|_| self.graph.renamed_sheet_alias(n))
                });
                if s.is_none() {
                    // Not recorded as a binding miss: legacy never re-bound a
                    // name to a sheet added later (its edges stay dead), and
                    // the integration probe pins that (remove + re-add).
                    self.flags |= F_OPAQUE;
                }
                s
            }
        }
    }

    fn miss(&mut self, key: String) {
        self.missing.push(key.into_boxed_str());
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

    /// An X edge to the symbol node of name vertex `vertex`, if the host
    /// has placed one (it has for every live name once synced).
    fn push_symbol_node(&mut self, vertex: crate::engine::VertexId, lk: &LkKey) -> bool {
        let Some(slot) = self.graph.authority_host().symbols().slot(vertex) else {
            return false;
        };
        let lk = match &self.symbol {
            Some(outer) => outer.clone(),
            None => lk.clone(),
        };
        self.edges.push(EdgeSpec {
            proj: RefProj {
                sheet: SYMBOL_SHEET,
                rows: AxisMap::fixed(slot, slot),
                cols: AxisMap::fixed(0, 0),
            },
            tag: Tag::X,
            origin: OriginSpec::Symbol(lk),
        });
        true
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
            sheet_key: None,
            missing: Vec::new(),
        };
        let _ = refs::visit_tree_references(
            ast,
            &mut inner,
            |_, _, _| LocalBindingStyle::None,
            collect,
        );
        self.edges.append(&mut inner.edges);
        self.missing.append(&mut inner.missing);
        self.flags |= inner.flags;
    }
}

fn collect_keyed(
    ctx: &mut Ctx<'_>,
    r: SemanticReference<'_>,
    sheet_key: Option<SheetId>,
) -> Result<(), ExcelError> {
    ctx.sheet_key = sheet_key;
    let result = collect(ctx, r);
    ctx.sheet_key = None;
    result
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
                Some(entry) if ctx.push_symbol_node(entry.vertex, &lk) => match &entry.definition {
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
                    // The node carries the formula's precedents.
                    NamedDefinition::Literal(_) | NamedDefinition::Formula { .. } => {}
                },
                Some(entry) => {
                    // No symbol node yet: re-derive once it has one.
                    ctx.miss(lk.name.to_string());
                    match &entry.definition {
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
                    }
                }
                None => match ctx.graph.resolve_source_scalar_entry(name) {
                    // A source's row: invalidating the source dirties it.
                    Some(source) => {
                        if !ctx.push_symbol_node(source.vertex, &lk) {
                            ctx.miss(lk.name.to_string());
                        }
                    }
                    None => {
                        ctx.flags |= F_OPAQUE;
                        ctx.miss(lk.name.to_string());
                    }
                },
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
                // The table's row: redefining or dirtying the table reaches
                // its readers; the range edge orders them after the body.
                if !ctx.push_symbol_node(entry.vertex, &lk) {
                    ctx.miss(DependencyGraph::unbound_symbol_key("table", &t.name));
                }
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
            None => match ctx.graph.resolve_source_table_entry(&t.name) {
                Some(source) => {
                    let lk = LkKey {
                        ctx: ctx.sheet,
                        kind: LK_TABLE,
                        name: source.name.clone().into_boxed_str(),
                    };
                    if !ctx.push_symbol_node(source.vertex, &lk) {
                        ctx.flags |= F_OPAQUE;
                        ctx.miss(DependencyGraph::unbound_symbol_key("table", &t.name));
                    }
                }
                None => {
                    ctx.flags |= F_OPAQUE;
                    ctx.miss(DependencyGraph::unbound_symbol_key("table", &t.name));
                }
            },
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
        sheet_key: None,
        missing: Vec::new(),
    };
    let _ = refs::visit_arena_references_keyed(
        ast,
        &mut ctx,
        |c| c.graph.data_store(),
        |c| c.graph.sheet_reg(),
        collect_keyed,
    );
    // Keep only references that instantiate at the cell (all do for an
    // installed formula), then deduplicate: R is a set.
    ctx.edges.retain(|e| e.proj.instantiate(row, col).is_some());
    ctx.edges.sort_unstable();
    ctx.edges.dedup();
    let mut t = template_facts(graph.data_store(), ast, row, col);
    // A slot row holds at most 255 literals. A formula with more stays an
    // ungrouped singleton without a row: its own AST is its template.
    if t.literals.len() > super::slots::MAX_ARITY {
        t.relocatable = false;
        t.literals.clear();
    }
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
        template_anchor: None,
        literals: t.literals,
        flags,
    }
}

/// The facts of a name's symbol node at `(SYMBOL_SHEET, slot, 0)`: its
/// precedents in the name's scope, all absolute, X and owned by its LK. The
/// node is an ungrouped singleton whose template is never evaluated through
/// the arena (the executor evaluates the name vertex itself).
pub fn extract_symbol(
    graph: &DependencyGraph,
    name: &str,
    entry: &NamedRange,
    volatile: bool,
    dynamic: bool,
) -> FormulaFacts {
    extract_symbol_binding(graph, name, entry, volatile, dynamic).0
}

/// [`extract_symbol`], with the binding keys of the symbols the definition
/// names that are missing or have no node yet (sorted, deduplicated).
pub fn extract_symbol_binding(
    graph: &DependencyGraph,
    name: &str,
    entry: &NamedRange,
    volatile: bool,
    dynamic: bool,
) -> (FormulaFacts, Vec<Box<str>>) {
    let scope_sheet = match entry.scope {
        NameScope::Sheet(id) => id,
        NameScope::Workbook => graph.default_sheet_id(),
    };
    let lk = LkKey {
        ctx: scope_sheet,
        kind: LK_NAME,
        name: graph.name_lookup_key(name).into_boxed_str(),
    };
    let mut ctx = Ctx {
        graph,
        sheet: scope_sheet,
        row: 0,
        col: 0,
        symbol: Some(lk.clone()),
        depth: 1,
        edges: Vec::new(),
        flags: 0,
        sheet_key: None,
        missing: Vec::new(),
    };
    match &entry.definition {
        NamedDefinition::Cell(cr) => {
            let (r, c) = (cr.coord.row(), cr.coord.col());
            ctx.push_fixed(cr.sheet_id, r, c, r, c, Tag::X, lk);
        }
        NamedDefinition::Range(rr) => ctx.push_fixed(
            rr.start.sheet_id,
            rr.start.coord.row(),
            rr.start.coord.col(),
            rr.end.coord.row(),
            rr.end.coord.col(),
            Tag::X,
            lk,
        ),
        NamedDefinition::Literal(_) => {}
        NamedDefinition::Formula { ast, .. } => {
            let _ = refs::visit_tree_references(
                ast,
                &mut ctx,
                |_, _, _| LocalBindingStyle::None,
                collect,
            );
        }
    }
    ctx.edges.sort_unstable();
    ctx.edges.dedup();
    let mut flags = ctx.flags;
    if volatile {
        flags |= F_VOLATILE;
    }
    if dynamic {
        flags |= F_DYNAMIC;
    }
    let mut missing = ctx.missing;
    missing.sort_unstable();
    missing.dedup();
    (
        FormulaFacts {
            edges: ctx.edges,
            ltokens: None,
            template: AstNodeId::from_u32(u32::MAX),
            template_anchor: None,
            literals: Default::default(),
            flags,
        },
        missing,
    )
}
