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
fn supermod_repro_evaluate_cell_with_dn_range_ref_after_set_value() {
    // Mirrors the supermod test_sensitivity_table failure shape:
    //   - "Inputs" sheet holds an init_revenue scalar cell
    //   - "Model" sheet has three groups of profit cells (base / upside / down)
    //     each computing init_revenue * factor
    //   - A workbook-scoped defined name "UP_PROFIT" covers the upside group
    //   - Target = SUM(base_cells, UP_PROFIT) — base via direct cell range,
    //     upside via the defined name
    //   - After set_value on init_revenue, evaluate_cell on the target must
    //     re-evaluate the upside cells (reached only through the NamedArray
    //     vertex) so the sum reflects the new input.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.graph.add_sheet("Inputs").unwrap();
    engine.graph.add_sheet("Model").unwrap();

    // Inputs!A1 — the scalar driver.
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // Model!E1, F1, G1 — base profit cells (referenced directly by target).
    engine
        .set_cell_formula("Model", 1, 5, parse("=Inputs!A1*1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 1, 6, parse("=Inputs!A1*1.1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 1, 7, parse("=Inputs!A1*1.21").unwrap())
        .unwrap();

    // Model!E8, F8, G8 — upside profit cells (referenced via defined name).
    engine
        .set_cell_formula("Model", 8, 5, parse("=Inputs!A1*1*1.2").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 8, 6, parse("=Inputs!A1*1.1*1.2").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 8, 7, parse("=Inputs!A1*1.21*1.2").unwrap())
        .unwrap();

    let model_sheet = engine.graph.sheet_id("Model").unwrap();
    let range_start = CellRef::new(model_sheet, Coord::from_excel(8, 5, true, true));
    let range_end = CellRef::new(model_sheet, Coord::from_excel(8, 7, true, true));
    engine
        .define_name(
            "UP_PROFIT",
            NamedDefinition::Range(RangeRef::new(range_start, range_end)),
            NameScope::Workbook,
        )
        .unwrap();

    // Target = SUM(direct base range, UP_PROFIT)
    engine
        .set_cell_formula("Model", 10, 1, parse("=SUM(E1:G1,UP_PROFIT)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    let baseline = engine.get_cell_value("Model", 10, 1).unwrap();
    let base_sum = 10.0 + 11.0 + 12.1;
    let up_sum = 12.0 + 13.2 + 14.52;
    let expected_baseline = base_sum + up_sum;
    assert!(
        matches!(baseline, LiteralValue::Number(n) if approx_eq(n, expected_baseline)),
        "baseline expected {expected_baseline}, got {baseline:?}",
    );

    // Change the input. Evaluate ONLY the target — the upside cells (Model!E8..G8)
    // must be re-evaluated through the UP_PROFIT NamedArray pass-through.
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(100.0))
        .unwrap();
    let target = engine.evaluate_cell("Model", 10, 1).unwrap().unwrap();
    let expected_after = (base_sum + up_sum) * 10.0;
    assert!(
        matches!(target, LiteralValue::Number(n) if approx_eq(n, expected_after)),
        "after override expected {expected_after}, got {target:?}",
    );
}
