use crate::engine::ingest_builder::BulkIngestSummary;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::ASTNode;
use formualizer_parse::parser::parse as parse_formula;

fn parse(formula: &str) -> ASTNode {
    parse_formula(formula).unwrap()
}

#[test]
fn bulk_ingest_then_eval_then_edit() {
    let cfg = EvalConfig::default();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // Ingest base values via Arrow store
    {
        let mut aib = engine.begin_bulk_ingest_arrow();
        aib.add_sheet("Sheet1", 3, 1024);
        aib.append_row(
            "Sheet1",
            &[
                LiteralValue::Number(10.0),
                LiteralValue::Empty,
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        aib.append_row(
            "Sheet1",
            &[
                LiteralValue::Number(20.0),
                LiteralValue::Empty,
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        aib.append_row(
            "Sheet1",
            &[
                LiteralValue::Number(30.0),
                LiteralValue::Empty,
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        aib.finish().unwrap();
    }

    // Stage formulas via graph bulk ingest
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");

    // Formulas: B1 = A1*2, B2 = A2 + A3, C1 = SUM(A1:A3)
    builder.add_formulas(
        sheet,
        vec![
            (1, 2, parse("=A1*2")),
            (2, 2, parse("=A2+A3")),
            (1, 3, parse("=SUM(A1:A3)")),
        ],
    );

    let summary: BulkIngestSummary = builder.finish().expect("bulk finish");
    assert!(summary.formulas >= 3);

    // Evaluate
    let _res = engine.evaluate_all().expect("eval");

    // Assert values
    use formualizer_common::LiteralValue::*;
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), Some(Number(20.0))); // B1
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), Some(Number(50.0))); // B2
    assert_eq!(engine.get_cell_value("Sheet1", 1, 3), Some(Number(60.0))); // C1

    // Edit a single value and re-evaluate
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(15.0))
        .expect("set value");
    let _res2 = engine.evaluate_all().expect("re-eval");

    // Check updated results
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), Some(Number(30.0))); // B1=15*2
    assert_eq!(engine.get_cell_value("Sheet1", 1, 3), Some(Number(65.0))); // C1=15+20+30
}
