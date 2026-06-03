use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn evaluate_cell_walks_named_range_deps_for_dirty_upstreams() {
    // Reproduces the v0.5.x regression where build_demand_subgraph filtered
    // dep walks by VertexKind::FormulaScalar|FormulaArray, dropping the
    // pass-through NamedScalar/NamedArray vertices and never reaching the
    // formula cells underneath. A target like
    //   =SUM(direct_range, named_range_at_dirty_cells)
    // would evaluate using stale values for the named-range cells when
    // their upstream input was changed via set_value.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.graph.add_sheet("Inputs").unwrap();
    engine.graph.add_sheet("Model").unwrap();

    // Define the named range FIRST so the target formula's NamedRange ref
    // resolves at graph-build time.
    let model_sheet = engine.graph.sheet_id_mut("Model");
    let range_start = CellRef::new(model_sheet, Coord::from_excel(1, 2, true, true));
    let range_end = CellRef::new(model_sheet, Coord::from_excel(1, 3, true, true));
    engine
        .define_name(
            "RANGE_REF",
            NamedDefinition::Range(RangeRef::new(range_start, range_end)),
            NameScope::Workbook,
        )
        .unwrap();

    // Inputs sheet: one driver cell that all downstream formulas depend on.
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // Model sheet formula cells. Model!B1:C1 are the cells inside RANGE_REF.
    engine
        .set_cell_formula("Model", 1, 1, parse("=Inputs!A1*1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 1, 2, parse("=Inputs!A1*2").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 1, 3, parse("=Inputs!A1*3").unwrap())
        .unwrap();

    // Target = direct ref to A1 + sum over the defined-name range.
    // Baseline: 10 + (20 + 30) = 60.
    engine
        .set_cell_formula("Model", 2, 1, parse("=Model!A1+SUM(RANGE_REF)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    let baseline = engine.get_cell_value("Model", 2, 1).unwrap();
    assert!(
        matches!(baseline, LiteralValue::Number(n) if approx_eq(n, 60.0)),
        "baseline expected 60, got {baseline:?}",
    );

    // Change the upstream input. Evaluate ONLY the target cell.
    // The target's demand subgraph must transit through the NamedScalar
    // vertex into the underlying B1/C1 formula cells; otherwise those
    // cells stay at their baseline computed values (20, 30) and the
    // target evaluates as 100 + 20 + 30 = 150 instead of 100 + 200 + 300.
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(100.0))
        .unwrap();
    let target = engine.evaluate_cell("Model", 2, 1).unwrap().unwrap();
    assert!(
        matches!(target, LiteralValue::Number(n) if approx_eq(n, 600.0)),
        "after override, target should be 100 + 200 + 300 = 600; got {target:?}",
    );
}
