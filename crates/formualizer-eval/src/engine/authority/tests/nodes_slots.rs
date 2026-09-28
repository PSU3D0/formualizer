//! L-nodes and literal slots (design §4.2, SP-3 F1) on engine formulas:
//! filled-down families (literal-uniform and literal-distinct) become one
//! family node each, singletons stay singletons, non-relocatable templates
//! stay ungrouped, and every member's formula is reproduced exactly by its
//! `formula_view` (template relocated by the member's offset, with the
//! member's own literal row), before and after punch/refill/repartition.

use super::super::geom::{Cell, Rect};
use crate::engine::arena::ValueRef;
use crate::engine::graph::DependencyGraph;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

fn shift(v: &mut u32, abs: bool, d: i64) {
    if !abs {
        *v = (i64::from(*v) + d) as u32;
    }
}

fn shift_opt(v: &mut Option<u32>, abs: bool, d: i64) {
    if let Some(x) = v {
        shift(x, abs, d);
    }
}

/// Relocate `ast` by `(dr, dc)` and replace its literals, in pre-order,
/// by `lits` (the template walk's order).
fn instantiate(ast: &mut ASTNode, dr: i64, dc: i64, lits: &mut std::slice::Iter<'_, LiteralValue>) {
    match &mut ast.node_type {
        ASTNodeType::Literal(v) => {
            // A formula without a row keeps its template's literals (it is
            // its own template).
            if let Some(x) = lits.next() {
                *v = x.clone();
            }
        }
        ASTNodeType::Omitted => {}
        ASTNodeType::Reference { reference, .. } => match reference {
            ReferenceType::Cell {
                row,
                col,
                row_abs,
                col_abs,
                ..
            } => {
                shift(row, *row_abs, dr);
                shift(col, *col_abs, dc);
            }
            ReferenceType::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
                ..
            } => {
                shift_opt(start_row, *start_row_abs, dr);
                shift_opt(end_row, *end_row_abs, dr);
                shift_opt(start_col, *start_col_abs, dc);
                shift_opt(end_col, *end_col_abs, dc);
            }
            _ => {}
        },
        ASTNodeType::UnaryOp { expr, .. } => instantiate(expr, dr, dc, lits),
        ASTNodeType::BinaryOp { left, right, .. } => {
            instantiate(left, dr, dc, lits);
            instantiate(right, dr, dc, lits);
        }
        ASTNodeType::Function { args, .. } => {
            for a in args {
                instantiate(a, dr, dc, lits);
            }
        }
        ASTNodeType::Call { callee, args } => {
            instantiate(callee, dr, dc, lits);
            for a in args {
                instantiate(a, dr, dc, lits);
            }
        }
        ASTNodeType::Array(rows) => {
            for row in rows {
                for x in row {
                    instantiate(x, dr, dc, lits);
                }
            }
        }
    }
}

/// Drop display-only data (the original reference text, source tokens).
fn strip(ast: &mut ASTNode) {
    ast.source_token = None;
    match &mut ast.node_type {
        ASTNodeType::Reference { original, .. } => original.clear(),
        ASTNodeType::UnaryOp { expr, .. } => strip(expr),
        ASTNodeType::BinaryOp { left, right, .. } => {
            strip(left);
            strip(right);
        }
        ASTNodeType::Function { args, .. } => args.iter_mut().for_each(strip),
        ASTNodeType::Call { callee, args } => {
            strip(callee);
            args.iter_mut().for_each(strip);
        }
        ASTNodeType::Array(rows) => rows.iter_mut().flatten().for_each(strip),
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => {}
    }
}

/// Every formula cell's view reproduces its own formula.
fn check_views(g: &mut DependencyGraph) -> usize {
    g.authority().expect("ready");
    let vids: Vec<_> = g.vertices_with_formulas().collect();
    let mut n = 0;
    for v in vids {
        let Some(cr) = g.get_cell_ref(v) else {
            continue;
        };
        let cell: Cell = (cr.sheet_id, cr.coord.row(), cr.coord.col());
        let store = g.authority_host().store();
        let (template, _anchor, (dr, dc), row) =
            store.formula_view(cell).expect("formula cell has a view");
        let mut ast = g
            .data_store()
            .retrieve_ast(template, g.sheet_reg())
            .expect("template AST");
        let lits: Vec<LiteralValue> = row
            .unwrap_or(&[])
            .iter()
            .map(|&x: &ValueRef| g.data_store().retrieve_value(x))
            .collect();
        instantiate(&mut ast, dr, dc, &mut lits.iter());
        let mut own = g.get_formula(v).expect("own formula");
        strip(&mut ast);
        strip(&mut own);
        assert_eq!(
            format!("{:?}", ast.node_type),
            format!("{:?}", own.node_type),
            "view of {cell:?}"
        );
        n += 1;
    }
    n
}

fn engine() -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=40 {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
    }
    e.define_table(
        "T1",
        crate::reference::RangeRef::new(
            crate::reference::CellRef::new(0, crate::reference::Coord::new(0, 5, true, true)),
            crate::reference::CellRef::new(0, crate::reference::Coord::new(9, 6, true, true)),
        ),
        true,
        vec!["X".into(), "Y".into()],
        false,
    )
    .unwrap();
    e
}

#[test]
fn families_become_nodes_with_literal_rows() {
    let mut e = engine();
    // Literal-uniform family B1:B30, literal-distinct family C1:C30,
    // a two-column family D1:E20, singletons, a non-relocatable family.
    for r in 1..=30u32 {
        e.set_cell_formula("Sheet1", r, 2, parse(format!("=A{r}*2")).unwrap())
            .unwrap();
        e.set_cell_formula(
            "Sheet1",
            r,
            3,
            parse(format!("=A{r}*{r}+\"x{r}\"")).unwrap(),
        )
        .unwrap();
    }
    for r in 1..=20u32 {
        for (c, col) in [(4u32, "B"), (5, "C")] {
            e.set_cell_formula(
                "Sheet1",
                r,
                c,
                parse(format!("=SUM($A$1:$A{r})+{col}{r}")).unwrap(),
            )
            .unwrap();
        }
        e.set_cell_formula("Sheet1", r, 8, parse("=SUM(T1[X])").unwrap())
            .unwrap();
    }
    e.set_cell_formula("Sheet1", 35, 2, parse("=A1+A2").unwrap())
        .unwrap();
    let g = &mut e.graph;
    // Build from scratch (the load path).
    g.authority_host_mut().state = super::super::host::HostState::Unbuilt;
    g.authority().expect("ready");
    let store = g.authority_host().store();
    store.check().unwrap();
    let dom = |c: Cell| {
        let o = store.owner_at(c).unwrap();
        let (_, d, fam) = store.owner_dom(o);
        (d, fam)
    };
    assert_eq!(dom((0, 4, 1)), (Rect::new(0, 1, 29, 1), true));
    assert_eq!(
        dom((0, 4, 2)),
        (Rect::new(0, 2, 29, 2), true),
        "literal-distinct family is one node"
    );
    // D and E are one relative template (`+B{r}` from D and `+C{r}` from E
    // have the same column offset).
    assert_eq!(dom((0, 0, 3)), (Rect::new(0, 3, 19, 4), true));
    assert!(!dom((0, 34, 1)).1);
    // Table readers are not relocatable: ungrouped singletons.
    assert_eq!(dom((0, 3, 7)), (Rect::cell(3, 7), false));
    assert!(
        store
            .owner_group_key(store.owner_at((0, 3, 7)).unwrap())
            .is_none()
    );
    // Slot rows: every formula with literals has one (singletons included).
    let slot_rows = store.slots().rows();
    assert_eq!(slot_rows, 30 + 30, "B and C members carry literal rows");
    let n = check_views(g);
    assert!(n > 30 + 30 + 40 + 20);
}

#[test]
fn views_survive_punch_refill_and_repartition() {
    let mut e = engine();
    for r in 1..=30u32 {
        e.set_cell_formula("Sheet1", r, 3, parse(format!("=A{r}*{r}")).unwrap())
            .unwrap();
    }
    check_views(&mut e.graph);
    // Punch and refill with other literals; formula→formula edits.
    for r in (2..=29u32).step_by(3) {
        e.set_cell_value("Sheet1", r, 3, LiteralValue::Number(0.0))
            .unwrap();
    }
    check_views(&mut e.graph);
    for r in (2..=29u32).step_by(3) {
        e.set_cell_formula("Sheet1", r, 3, parse(format!("=A{r}*{}", r + 100)).unwrap())
            .unwrap();
    }
    for r in [5u32, 6, 7] {
        e.set_cell_formula("Sheet1", r, 3, parse(format!("=A{r}*{r}")).unwrap())
            .unwrap();
    }
    let n = check_views(&mut e.graph);
    assert_eq!(n, 30);
    let store = e.graph.authority_host().store();
    store.check().unwrap();
    assert!(store.stats.repartitions_committed > 0, "{:?}", store.stats);
    // Maintained == rebuild: the refilled column's canonical node set is
    // the rebuild's (one node), whatever fragments survive until the next
    // trigger.
    let rebuilt = super::fixtures::rebuild_from_graph(&e);
    assert_eq!(store.digest(), rebuilt.digest());
    let o = rebuilt.owner_at((0, 0, 2)).unwrap();
    assert_eq!(rebuilt.owner_dom(o).1, Rect::new(0, 2, 29, 2));
}

/// A formula with more literals than a slot row holds stays an ungrouped
/// singleton whose view is its own formula.
#[test]
fn formulas_over_the_slot_arity_stay_ungrouped() {
    let mut e = engine();
    let many: Vec<String> = (0..300).map(|k| k.to_string()).collect();
    for r in 1..=3u32 {
        let text = format!("=A{r}+SUM({})", many.join(","));
        e.set_cell_formula("Sheet1", r, 2, parse(text).unwrap())
            .unwrap();
    }
    let n = check_views(&mut e.graph);
    assert_eq!(n, 3);
    let store = e.graph.authority_host().store();
    for r in 0..3u32 {
        let o = store.owner_at((0, r, 1)).unwrap();
        assert!(store.owner_group_key(o).is_none());
    }
    assert_eq!(store.slots().rows(), 0);
}
