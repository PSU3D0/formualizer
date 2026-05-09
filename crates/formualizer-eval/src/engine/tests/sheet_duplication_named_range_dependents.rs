use crate::SheetId;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{DependencyGraph, VertexId};
use crate::reference::{CellRef, Coord, RangeRef};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use rustc_hash::FxHashSet;

fn graph() -> DependencyGraph {
    super::common::graph_truth_graph()
}

fn cell_ref(sheet_id: SheetId, row: u32, col: u32) -> CellRef {
    CellRef::new(sheet_id, Coord::from_excel(row, col, true, true))
}

fn range_ref(
    sheet_id: SheetId,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
) -> RangeRef {
    RangeRef::new(
        cell_ref(sheet_id, start_row, start_col),
        cell_ref(sheet_id, end_row, end_col),
    )
}

fn vertex_for(graph: &DependencyGraph, sheet: &str, row: u32, col: u32) -> VertexId {
    *graph
        .get_vertex_id_for_address(&graph.make_cell_ref(sheet, row, col))
        .unwrap_or_else(|| panic!("missing vertex for {sheet}!R{row}C{col}"))
}

fn setup_sheet_scoped_named_range_graph() -> (DependencyGraph, SheetId, Vec<VertexId>) {
    let mut graph = graph();
    let source_id = graph.add_sheet("Source").unwrap();
    for row in 1..=10 {
        graph
            .set_cell_value("Source", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    graph
        .define_name(
            "MyRange",
            NamedDefinition::Range(range_ref(source_id, 1, 1, 10, 1)),
            NameScope::Sheet(source_id),
        )
        .unwrap();
    for row in 1..=5 {
        graph
            .set_cell_formula("Source", row, 2, parse("=SUM(MyRange)").unwrap())
            .unwrap();
    }
    let source_formulas = (1..=5)
        .map(|row| vertex_for(&graph, "Source", row, 2))
        .collect();
    (graph, source_id, source_formulas)
}

#[test]
fn duplicate_sheet_named_range_dependents_populated() {
    let (mut graph, source_id, _) = setup_sheet_scoped_named_range_graph();

    let copy_id = graph.duplicate_sheet(source_id, "Copy").unwrap();
    let copy_formulas: FxHashSet<VertexId> = (1..=5)
        .map(|row| vertex_for(&graph, "Copy", row, 2))
        .collect();

    let copy_name = graph
        .resolve_name_entry("MyRange", copy_id)
        .expect("copy sheet-scoped name");
    assert_eq!(copy_name.scope, NameScope::Sheet(copy_id));
    assert_eq!(copy_name.dependents, copy_formulas);
}

#[test]
fn duplicate_sheet_named_range_deletion_marks_dependents_dirty() {
    let (mut graph, source_id, source_formulas) = setup_sheet_scoped_named_range_graph();
    let copy_id = graph.duplicate_sheet(source_id, "Copy").unwrap();
    let copy_formulas: Vec<VertexId> = (1..=5)
        .map(|row| vertex_for(&graph, "Copy", row, 2))
        .collect();

    let mut formula_vertices = source_formulas.clone();
    formula_vertices.extend(copy_formulas.iter().copied());
    graph.clear_dirty_flags(&formula_vertices);

    graph
        .delete_name("MyRange", NameScope::Sheet(copy_id))
        .unwrap();

    for vertex in &copy_formulas {
        assert!(
            graph.is_dirty(*vertex),
            "copy formula {vertex:?} was not dirtied"
        );
    }
    for vertex in &source_formulas {
        assert!(
            !graph.is_dirty(*vertex),
            "source formula {vertex:?} was dirtied by copy name deletion"
        );
    }
}

#[test]
fn duplicate_sheet_cross_sheet_named_range_references_correct() {
    let mut graph = graph();
    let data_id = graph.add_sheet("Data").unwrap();
    let calc_id = graph.add_sheet("Calc").unwrap();
    for row in 1..=10 {
        graph
            .set_cell_value("Data", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    graph
        .define_name(
            "DataRange",
            NamedDefinition::Range(range_ref(data_id, 1, 1, 10, 1)),
            NameScope::Workbook,
        )
        .unwrap();
    for row in 1..=5 {
        graph
            .set_cell_formula("Calc", row, 1, parse("=SUM(DataRange)").unwrap())
            .unwrap();
    }

    let copy_id = graph.duplicate_sheet(calc_id, "CalcCopy").unwrap();
    let workbook_name = graph
        .resolve_name_entry("DataRange", copy_id)
        .expect("workbook-scoped DataRange");
    assert_eq!(workbook_name.scope, NameScope::Workbook);

    let expected_dependents: FxHashSet<VertexId> = (1..=5)
        .map(|row| vertex_for(&graph, "Calc", row, 1))
        .chain((1..=5).map(|row| vertex_for(&graph, "CalcCopy", row, 1)))
        .collect();
    assert_eq!(workbook_name.dependents, expected_dependents);
    assert!(
        graph
            .sheet_named_ranges_iter()
            .all(|((sheet_id, name), _)| *sheet_id != copy_id || name != "DataRange")
    );
}

#[test]
fn duplicate_sheet_with_no_named_ranges_unaffected() {
    let mut graph = graph();
    let source_id = graph.add_sheet("Source").unwrap();
    graph
        .set_cell_value("Source", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    graph
        .set_cell_value("Source", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    graph
        .set_cell_formula("Source", 3, 1, parse("=A1+A2").unwrap())
        .unwrap();

    graph.duplicate_sheet(source_id, "Copy").unwrap();

    let copy_formula = vertex_for(&graph, "Copy", 3, 1);
    let copy_a1 = vertex_for(&graph, "Copy", 1, 1);
    let copy_a2 = vertex_for(&graph, "Copy", 2, 1);
    let deps: FxHashSet<VertexId> = graph.get_dependencies(copy_formula).into_iter().collect();
    assert!(deps.contains(&copy_a1));
    assert!(deps.contains(&copy_a2));
    assert!(graph.get_formula(copy_formula).is_some());
}
