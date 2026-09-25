//! Engine fixtures shared by the build oracle (stage 3) and the engine
//! wiring tests (stage 6): two sheets of values, static, sheet-scoped and
//! formula-defined names, a table, and families and singletons drawn from
//! a formula menu covering every R-1/R-1X reference form.

use super::super::geom::{Cell, Cover, Rect};
use super::super::store::{Store, TagFilter};
use super::support::Rng;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

pub const ROWS: u32 = 30;
pub const COLS: u32 = 10;

/// Formula menu (`{r}` = this row, `{p}` = previous row, `{n}` = next row;
/// 1-based as written).
pub const MENU: &[&str] = &[
    "=A{r}*2",
    "=A{r}+B{p}",
    "=SUM($A$1:A{r})",
    "=SUM(A:A)",
    "=Data!A{r}+1",
    "=NCell+A{r}",
    "=SUM(NRange)",
    "=Loc*2",
    "=NF*3",
    "=SUM(T1[X])",
    "=SUM(3:3)",
    "=SUM(A{r}:B{n})",
    "=IF(A{r}>0,B{r},D{p})",
    "=INDIRECT(\"A1\")+A{r}",
    "=MissingName+1",
    "=SUM(Sheet1:Data!A1)",
    "=D{r}+E{p}",
    "=SUM($D$2:$D$6)*A{r}",
    "=B$4+$C{r}",
    "=Data!$B$2:$C$3",
    "=SUM(Data!B:B)+{r}",
];

pub fn text(t: &str, row1: u32) -> String {
    t.replace("{r}", &row1.to_string())
        .replace("{p}", &(row1.max(2) - 1).to_string())
        .replace("{n}", &(row1 + 1).to_string())
}

pub fn fixture_engine() -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.add_sheet("Data").unwrap();
    for sheet in ["Sheet1", "Data"] {
        for r in 1..=ROWS {
            e.set_cell_value(sheet, r, 1, LiteralValue::Number(f64::from(r)))
                .unwrap();
            e.set_cell_value(sheet, r, 2, LiteralValue::Number(f64::from(r) * 2.0))
                .unwrap();
        }
    }
    let s1 = e.graph.sheet_id("Sheet1").unwrap();
    let data = e.graph.sheet_id("Data").unwrap();
    let cell = |s, r, c| CellRef::new(s, Coord::new(r, c, true, true));
    e.define_name(
        "NCell",
        NamedDefinition::Cell(cell(s1, 4, 0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.define_name(
        "NRange",
        NamedDefinition::Range(RangeRef::new(cell(data, 1, 1), cell(data, 8, 2))),
        NameScope::Workbook,
    )
    .unwrap();
    e.define_name(
        "Loc",
        NamedDefinition::Cell(cell(data, 0, 0)),
        NameScope::Sheet(s1),
    )
    .unwrap();
    e.define_name(
        "NF",
        NamedDefinition::Formula {
            ast: parse("=Sheet1!A2+SUM(Data!B3:B4)+NCell").unwrap(),
            dependencies: vec![],
            range_deps: vec![],
        },
        NameScope::Workbook,
    )
    .unwrap();
    e.define_table(
        "T1",
        RangeRef::new(cell(data, 0, 4), cell(data, 9, 5)),
        true,
        vec!["X".into(), "Y".into()],
        false,
    )
    .unwrap();
    e
}

/// Fill families and singletons from the menu; rejected formulas are skipped.
pub fn populate(e: &mut Engine<TestWorkbook>, g: &mut Rng) {
    for (sheet, cols) in [("Sheet1", 3..=8u32), ("Data", 7..=9u32)] {
        for col in cols {
            let mut r = 1;
            while r <= ROWS {
                let t = MENU[g.below(MENU.len() as u32) as usize];
                let run = if g.chance(60) { g.below(12) + 2 } else { 1 };
                for rr in r..(r + run).min(ROWS + 1) {
                    if g.chance(85) {
                        let _ = e.set_cell_formula(sheet, rr, col, parse(text(t, rr)).unwrap());
                    }
                }
                r += run + g.below(3);
            }
        }
    }
}

pub fn query_cells(e: &Engine<TestWorkbook>) -> Vec<Cell> {
    let mut v = Vec::new();
    for sheet in ["Sheet1", "Data"] {
        let s = e.graph.sheet_id(sheet).unwrap();
        for r in 0..ROWS + 3 {
            for c in 0..COLS + 1 {
                v.push((s, r, c));
            }
        }
        v.push((s, 1_048_575, 0));
        v.push((s, 2, 16_383));
    }
    v
}

pub fn rebuild_from_graph(e: &Engine<TestWorkbook>) -> Store {
    // Reclassified (symbol nodes, design §4.1): the host's own build input,
    // which adds each name's symbol-plane node to the formula cells.
    Store::build(e.graph.authority_build_input())
}

// Reclassified (symbol nodes, design §4.1): names are symbol-plane nodes,
// so the grid relation the oracles define looks through them.
pub fn store_direct(s: &Store, q: Cell, f: TagFilter) -> Vec<Cell> {
    s.direct_grid_dependents(q.0, &Rect::cell(q.1, q.2), f)
        .cells()
}

pub fn store_closure(s: &Store, q: Cell, f: TagFilter) -> Vec<Cell> {
    s.dependents(&[(q.0, Rect::cell(q.1, q.2))], f)
        .0
        .cells()
        .into_iter()
        .filter(|c| c.0 != super::super::geom::SYMBOL_SHEET)
        .collect()
}

pub fn store_precedent_cells(s: &Store, cell: Cell, f: TagFilter) -> Vec<Cell> {
    let mut hits = Vec::new();
    s.direct_grid_precedents(cell, f, &mut hits);
    let mut cover = Cover::new();
    for (_, sh, r) in hits {
        if let Some(r) = r.intersect(&Rect::new(0, 0, ROWS, COLS)) {
            cover.insert_rect(sh, &r);
        }
    }
    cover.cells()
}
