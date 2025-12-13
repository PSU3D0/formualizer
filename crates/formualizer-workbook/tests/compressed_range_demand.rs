use formualizer_common::LiteralValue;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

// Reproduces the scheduling conditions from the user’s example at the IO layer,
// without relying on any specific backend. Ensures demand-driven eval enters
// whole-column compressed ranges and computes nested formula precedents.
#[test]
fn io_compressed_range_demand_driven() {
    let cfg = EvalConfig::default().with_range_expansion_limit(0); // keep infinite ranges compressed
    let wb = TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(wb, cfg);

    // Criteria in D3
    engine
        .set_cell_value("Sheet1", 3, 4, LiteralValue::Text("X".into()))
        .unwrap();
    // B2 feeds P2 via formula
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(5.0))
        .unwrap();

    // P2 = B2 (col 16)
    let p2 = parse("=B2").unwrap();
    engine.set_cell_formula("Sheet1", 2, 16, p2).unwrap();
    // S2 = D3 (col 19)
    let s2 = parse("=D3").unwrap();
    engine.set_cell_formula("Sheet1", 2, 19, s2).unwrap();
    // D7 = SUMIF(S:S, D3, P:P)
    let d7 = parse("=SUMIF(S:S, D3, P:P)").unwrap();
    engine.set_cell_formula("Sheet1", 7, 4, d7).unwrap();

    let v = engine
        .evaluate_cell("Sheet1", 7, 4)
        .unwrap()
        .unwrap_or(LiteralValue::Empty);
    assert!(matches!(v, LiteralValue::Number(n) if (n-5.0).abs() < 1e-9));
}
