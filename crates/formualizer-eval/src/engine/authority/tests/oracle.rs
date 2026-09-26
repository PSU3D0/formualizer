//! Independent naive oracles for R-1 and R-1X on engine fixtures.
//!
//! Every formula's references are read from the reconstructed parse tree
//! (`DependencyGraph::get_formula`, not the arena walk the authority uses),
//! resolved to absolute rectangles directly (no projections, inversion,
//! canon or indexes), and queries enumerate formulas brute force. Shared
//! trusted infrastructure: the parser's reference classification
//! (`refs::classify` via `visit_tree_references`) and the engine's own
//! name/table registries, which define the relation (R-1 §3 rows 7–9).

use super::super::geom::{Cell, MAX_COL, MAX_ROW};
use crate::engine::graph::DependencyGraph;
use crate::engine::named_range::NamedDefinition;
use crate::engine::refs::{self, LocalBindingStyle, SemanticReference};
use formualizer_common::ExcelError;
use formualizer_parse::parser::ASTNode;
use rustc_hash::FxHashSet;

/// Absolute rectangle `(sheet, r0, c0, r1, c1)` and whether it is R1.
pub type Read = (bool, u16, u32, u32, u32, u32);

pub struct Naive {
    pub formulas: Vec<(Cell, Vec<Read>)>,
}

struct Ctx<'a> {
    g: &'a DependencyGraph,
    sheet: u16,
    in_name: bool,
    depth: u32,
    out: Vec<Read>,
}

fn visit(ctx: &mut Ctx<'_>, r: SemanticReference<'_>) -> Result<(), ExcelError> {
    let r1 = !ctx.in_name;
    let sheet_of = |ctx: &Ctx<'_>, name: Option<&str>| match name {
        None => Some(ctx.sheet),
        Some(n) => ctx.g.sheet_id(n),
    };
    match r {
        SemanticReference::Cell(c) => {
            if let Some(s) = sheet_of(ctx, c.sheet.name()) {
                ctx.out
                    .push((r1, s, c.row - 1, c.col - 1, c.row - 1, c.col - 1));
            }
        }
        SemanticReference::FiniteRange(rg) | SemanticReference::OpenRange(rg) => {
            if rg.is_reversed() {
                return Ok(());
            }
            if let Some(s) = sheet_of(ctx, rg.sheet.name()) {
                let lo = |v: Option<u32>| v.filter(|&x| x >= 1).map_or(0, |x| x - 1);
                let hi = |v: Option<u32>, max: u32| v.filter(|&x| x >= 1).map_or(max, |x| x - 1);
                ctx.out.push((
                    r1,
                    s,
                    lo(rg.start_row),
                    lo(rg.start_col),
                    hi(rg.end_row, MAX_ROW),
                    hi(rg.end_col, MAX_COL),
                ));
            }
        }
        SemanticReference::Name(n) => {
            if let Some(e) = ctx.g.resolve_name_entry(n, ctx.sheet) {
                match &e.definition {
                    NamedDefinition::Cell(c) => {
                        let (r, col) = (c.coord.row(), c.coord.col());
                        ctx.out.push((r1, c.sheet_id, r, col, r, col));
                    }
                    NamedDefinition::Range(rr) => ctx.out.push((
                        r1,
                        rr.start.sheet_id,
                        rr.start.coord.row(),
                        rr.start.coord.col(),
                        rr.end.coord.row(),
                        rr.end.coord.col(),
                    )),
                    NamedDefinition::Literal(_) => {}
                    NamedDefinition::Formula { ast, .. } if ctx.depth < 32 => {
                        let scope_sheet = match e.scope {
                            crate::engine::named_range::NameScope::Sheet(s) => s,
                            crate::engine::named_range::NameScope::Workbook => {
                                ctx.g.default_sheet_id()
                            }
                        };
                        let ast = ast.clone();
                        let mut inner = Ctx {
                            g: ctx.g,
                            sheet: scope_sheet,
                            in_name: true,
                            depth: ctx.depth + 1,
                            out: Vec::new(),
                        };
                        let _ = refs::visit_tree_references(
                            &ast,
                            &mut inner,
                            |_, _, _| LocalBindingStyle::None,
                            visit,
                        );
                        for mut x in inner.out {
                            x.0 = false;
                            ctx.out.push(x);
                        }
                    }
                    NamedDefinition::Formula { .. } => {}
                }
            }
        }
        SemanticReference::Table(t) => {
            if let Some(e) = ctx.g.resolve_table_entry(&t.name) {
                let rr = &e.range;
                ctx.out.push((
                    false,
                    rr.start.sheet_id,
                    rr.start.coord.row(),
                    rr.start.coord.col(),
                    rr.end.coord.row(),
                    rr.end.coord.col(),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn hits(read: &Read, q: Cell) -> bool {
    read.1 == q.0 && read.2 <= q.1 && q.1 <= read.4 && read.3 <= q.2 && q.2 <= read.5
}

impl Naive {
    pub fn build(g: &DependencyGraph) -> Naive {
        let mut formulas = Vec::new();
        let vids: Vec<_> = g.vertices_with_formulas().collect();
        for v in vids {
            let Some(cr) = g.get_cell_ref(v) else {
                continue;
            };
            let Some(ast): Option<ASTNode> = g.get_formula(v) else {
                continue;
            };
            let mut ctx = Ctx {
                g,
                sheet: cr.sheet_id,
                in_name: false,
                depth: 0,
                out: Vec::new(),
            };
            let _ = refs::visit_tree_references(
                &ast,
                &mut ctx,
                |_, _, _| LocalBindingStyle::None,
                visit,
            );
            formulas.push(((cr.sheet_id, cr.coord.row(), cr.coord.col()), ctx.out));
        }
        formulas.sort_by_key(|f| f.0);
        Naive { formulas }
    }

    pub fn direct(&self, q: Cell, r1_only: bool) -> Vec<Cell> {
        let mut v: Vec<Cell> = self
            .formulas
            .iter()
            .filter(|(_, reads)| reads.iter().any(|r| (!r1_only || r.0) && hits(r, q)))
            .map(|(c, _)| *c)
            .collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    pub fn closure(&self, q: Cell, r1_only: bool) -> Vec<Cell> {
        let mut seen: FxHashSet<Cell> = FxHashSet::default();
        let mut queue = vec![q];
        while let Some(c) = queue.pop() {
            for d in self.direct(c, r1_only) {
                if seen.insert(d) {
                    queue.push(d);
                }
            }
        }
        let mut v: Vec<Cell> = seen.into_iter().collect();
        v.sort_unstable();
        v
    }

    /// The reads of one formula cell, R1 or all, as a sorted cell list
    /// clipped to `rows × cols` (small fixtures).
    pub fn precedent_cells(&self, cell: Cell, r1_only: bool, rows: u32, cols: u32) -> Vec<Cell> {
        let mut set: FxHashSet<Cell> = FxHashSet::default();
        if let Ok(i) = self.formulas.binary_search_by_key(&cell, |f| f.0) {
            for r in &self.formulas[i].1 {
                if r1_only && !r.0 {
                    continue;
                }
                for row in r.2..=r.4.min(rows) {
                    for col in r.3..=r.5.min(cols) {
                        set.insert((r.1, row, col));
                    }
                }
            }
        }
        let mut v: Vec<Cell> = set.into_iter().collect();
        v.sort_unstable();
        v
    }
}
