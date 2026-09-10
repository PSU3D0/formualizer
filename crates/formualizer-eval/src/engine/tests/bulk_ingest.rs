use crate::engine::ingest_builder::BulkIngestSummary;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::ASTNode;
use formualizer_parse::parser::parse as parse_formula;

fn ingest(engine: &mut Engine<TestWorkbook>, sheet: &str, formulas: Vec<(u32, u32, ASTNode)>) {
    engine.sheet_id_mut(sheet);
    let mut builder = engine.begin_bulk_ingest();
    let sid = builder.add_sheet(sheet);
    builder.add_formulas(sid, formulas);
    builder.finish().unwrap();
}

fn chain(rows: u32, per_call: usize, values_first: bool) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    if values_first {
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .unwrap();
    }
    let formulas: Vec<_> = (if values_first { 2 } else { 1 }..=rows)
        .map(|r| {
            (
                r,
                1,
                parse(&if r == 1 {
                    "=1".into()
                } else {
                    format!("=A{}+1", r - 1)
                }),
            )
        })
        .collect();
    for chunk in formulas.chunks(per_call) {
        ingest(&mut engine, "Sheet1", chunk.to_vec());
    }
    engine
}

#[test]
fn bulk_ingest_split_chain_preserves_membership_and_incremental_results() {
    for (rows, per_call) in [(5, 1), (6, 2), (300, 100), (5000, 2048), (5000, 5000)] {
        for values_first in [false, true] {
            let mut engine = chain(rows, per_call, values_first);
            engine.evaluate_cells(&[("Sheet1", rows, 1)]).unwrap();
            assert_eq!(
                engine.get_cell_value("Sheet1", rows, 1),
                Some(LiteralValue::Number(rows as f64))
            );
            engine.evaluate_all().unwrap();
            for row in 1..=rows {
                assert_eq!(
                    engine.get_cell_value("Sheet1", row, 1),
                    Some(LiteralValue::Number(row as f64)),
                    "rows={rows} chunk={per_call} row={row}"
                );
            }
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
                .unwrap();
            engine.evaluate_all().unwrap();
            for row in 1..=rows {
                assert_eq!(
                    engine.get_cell_value("Sheet1", row, 1),
                    Some(LiteralValue::Number(row as f64 + 9.0))
                );
            }
        }
    }
}

#[test]
fn bulk_ingest_keeps_base_delta_names_and_replaces_dependencies() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord};
    let mut engine = chain(6, 2, true);
    let sid = engine.sheet_id_mut("Sheet1");
    let cell = |row, col| CellRef::new(sid, Coord::from_excel(row, col, true, true));
    engine
        .define_name(
            "Anchor",
            NamedDefinition::Cell(cell(6, 1)),
            NameScope::Workbook,
        )
        .unwrap();
    // A normal edit adds delta edges beside the bulk-built base.
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A6+1"))
        .unwrap();
    ingest(
        &mut engine,
        "Other",
        vec![(1, 1, parse("=Anchor+Sheet1!B1"))],
    );
    ingest(&mut engine, "Other", vec![(2, 1, parse("=A1+1"))]);
    ingest(&mut engine, "Sheet1", vec![(1, 3, parse("=B1+A6"))]);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Other", 2, 1),
        Some(LiteralValue::Number(14.0))
    );
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Other", 2, 1),
        Some(LiteralValue::Number(16.0))
    );
    let a6 = engine.graph.get_vertex_for_cell(&cell(6, 1)).unwrap();
    let b1 = engine.graph.get_vertex_for_cell(&cell(1, 2)).unwrap();
    assert!(engine.graph.get_dependencies(b1).contains(&a6));
    ingest(&mut engine, "Sheet1", vec![(1, 2, parse("=42"))]);
    ingest(&mut engine, "Other", vec![(3, 1, parse("=A2+1"))]);
    assert!(engine.graph.get_dependencies(b1).is_empty());
    assert!(!engine.graph.get_dependents(a6).contains(&b1));
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Other", 3, 1),
        Some(LiteralValue::Number(51.0))
    );
    // Empty ingests must leave membership and edges intact too.
    ingest(&mut engine, "Sheet1", vec![]);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(3.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Other", 3, 1),
        Some(LiteralValue::Number(52.0))
    );
}

#[test]
fn bulk_ingest_ids_and_admission_rejection_preserve_existing_graph() {
    use crate::engine::{AdmissionResourceBudget, EvaluationBudgets};
    let mut engine = chain(8, 2, true);
    let ast = engine.intern_formula_ast(&parse("=A8+1"));
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formula_ids(sheet, [(9, 1, ast)]);
    builder.finish().unwrap();
    let before = engine.baseline_stats();
    engine.set_evaluation_budgets_for_test(EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(before.graph_vertex_count),
            ..Default::default()
        },
        ..Default::default()
    });
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formulas(sheet, [(3, 1, parse("=Z100+1"))]);
    assert!(builder.finish().is_err());
    let after = engine.baseline_stats();
    assert_eq!(before.graph_vertex_count, after.graph_vertex_count);
    assert_eq!(before.graph_edge_count, after.graph_edge_count);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 9, 1),
        Some(LiteralValue::Number(9.0))
    );
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 9, 1),
        Some(LiteralValue::Number(13.0))
    );
}

#[test]
fn bulk_ingest_planned_sources_and_table_symbols_survive_rebuilds() {
    use crate::engine::ingest_pipeline::FormulaAstInput;
    use crate::reference::{CellRef, Coord, RangeRef};
    let mut engine = chain(6, 2, true);
    let sid = engine.sheet_id_mut("Sheet1");
    let cell = |row, col| CellRef::new(sid, Coord::from_excel(row, col, true, true));
    engine
        .define_source_scalar("ExternalScalar", Some(1))
        .unwrap();
    engine
        .define_table(
            "Sales",
            RangeRef::new(cell(1, 4), cell(3, 4)),
            true,
            vec!["Amount".into()],
            false,
        )
        .unwrap();
    let provider = TestWorkbook::default();
    let planned = engine
        .graph
        .ingest_pipeline(&provider)
        .ingest_batch([(
            FormulaAstInput::Tree(parse("=ExternalScalar+SUM(Sales[Amount])+A6")),
            cell(1, 2),
            None,
        )])
        .unwrap()
        .pop()
        .unwrap();
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");
    builder.add_formula_plans(sheet, [(1, 2, planned.ast_id, planned.dep_plan)]);
    builder.finish().unwrap();
    let b1 = engine.graph.get_vertex_for_cell(&cell(1, 2)).unwrap();
    let source = engine
        .graph
        .resolve_source_scalar_entry("ExternalScalar")
        .unwrap()
        .vertex;
    let table = engine.graph.resolve_table_entry("Sales").unwrap().vertex;
    let before = engine.graph.get_dependencies(b1);
    assert!(before.contains(&source));
    assert!(before.contains(&table));
    for row in 2..=4 {
        ingest(&mut engine, "Sheet1", vec![(row, 2, parse("=A6+1"))]);
        let after = engine.graph.get_dependencies(b1);
        assert_eq!(before.len(), after.len());
        for dep in &before {
            assert!(after.contains(dep));
        }
        assert!(engine.graph.get_dependents(source).contains(&b1));
        assert!(engine.graph.get_dependents(table).contains(&b1));
    }
}

#[test]
fn bulk_ingest_forward_diamonds_match_single_call() {
    let formulas = [
        (1, 1, parse("=B1+C1")),
        (1, 2, parse("=D1+1")),
        (1, 3, parse("=D1+2")),
        (1, 4, parse("=4")),
        (1, 5, parse("=F1*2")),
        (1, 6, parse("=7")),
    ];
    for chunk in [1, 2, 3, 6] {
        let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
        for batch in formulas.chunks(chunk) {
            ingest(&mut engine, "Sheet1", batch.to_vec());
        }
        engine
            .evaluate_cells(&[("Sheet1", 1, 1), ("Sheet1", 1, 5)])
            .unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(11.0))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 5),
            Some(LiteralValue::Number(14.0))
        );
        ingest(&mut engine, "Sheet1", vec![(1, 2, parse("=F1+1"))]);
        engine.evaluate_cells(&[("Sheet1", 1, 1)]).unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(14.0))
        );
        // Compare subsequent incremental evaluation with a fresh rebuild.
        engine
            .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(9.0))
            .unwrap();
        engine.evaluate_all().unwrap();
        let mut fresh = Engine::new(TestWorkbook::default(), EvalConfig::default());
        let mut replaced = formulas.to_vec();
        replaced[1].2 = parse("=F1+1");
        replaced[5].2 = parse("=9");
        ingest(&mut fresh, "Sheet1", replaced);
        fresh.evaluate_all().unwrap();
        for col in 1..=6 {
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, col),
                fresh.get_cell_value("Sheet1", 1, col)
            );
        }
    }
}

#[test]
#[ignore = "allocates 800001 vertices to exercise the production sparse threshold"]
fn bulk_ingest_sparse_delta_preserves_and_replaces_edges() {
    use crate::reference::{CellRef, Coord};
    use formualizer_common::Coord as AbsCoord;
    let mut engine = chain(6, 2, true);
    let sid = engine.sheet_id_mut("Sheet1");
    let coords: Vec<_> = (0..800001).map(|r| (sid, AbsCoord::new(r, 0))).collect();
    engine.graph.ensure_vertices_batch(&coords);
    drop(coords);
    assert!(engine.graph.vertex_count() > 800000);
    ingest(&mut engine, "Sheet1", vec![(7, 1, parse("=A6+1"))]);
    ingest(&mut engine, "Sheet1", vec![(8, 1, parse("=A7+1"))]);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 8, 1),
        Some(LiteralValue::Number(8.0))
    );
    ingest(&mut engine, "Sheet1", vec![(4, 1, parse("=40"))]);
    let a4 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sid, Coord::from_excel(4, 1, true, true)))
        .unwrap();
    assert!(engine.graph.get_dependencies(a4).is_empty());
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 8, 1),
        Some(LiteralValue::Number(44.0))
    );
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(12.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 8, 1),
        Some(LiteralValue::Number(44.0))
    );
}

#[test]
#[ignore = "synthetic ingest timing; run explicitly in release mode"]
fn bulk_ingest_membership_costs() {
    use std::time::Instant;
    for (rows, chunk, values_first) in [
        (20000, 20000, false),
        (20000, 20000, true),
        (20000, 2048, true),
        (20000, 100, true),
    ] {
        let start = Instant::now();
        let mut engine = chain(rows, chunk, values_first);
        let ingest = start.elapsed();
        engine.evaluate_all().unwrap();
        for row in 1..=rows {
            assert_eq!(
                engine.get_cell_value("Sheet1", row, 1),
                Some(LiteralValue::Number(row as f64))
            );
        }
        let vertices = engine.graph.vertex_count();
        let edges: usize = engine
            .graph
            .iter_vertex_ids()
            .map(|v| engine.graph.get_dependencies(v).len())
            .sum();
        eprintln!(
            "rows={rows} chunk={chunk} values_first={values_first} ingest_ms={} vertices={vertices} edges={edges} correct=true",
            ingest.as_millis()
        );
    }
}

#[test]
#[ignore = "synthetic multi-sheet/name-heavy timing; run explicitly in release mode"]
fn bulk_ingest_name_heavy_costs() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord};
    use std::time::Instant;
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    let sid = engine.sheet_id_mut("Inputs");
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(7.0))
        .unwrap();
    for i in 0..200 {
        engine
            .define_name(
                &format!("Anchor_{i}"),
                NamedDefinition::Cell(CellRef::new(sid, Coord::from_excel(1, 1, true, true))),
                NameScope::Workbook,
            )
            .unwrap();
    }
    let start = Instant::now();
    for sheet in ["Left", "Right"] {
        let formulas: Vec<_> = (1..=10000)
            .map(|row| (row, 1, parse(&format!("=Anchor_{}+1", row % 200))))
            .collect();
        for chunk in formulas.chunks(2048) {
            ingest(&mut engine, sheet, chunk.to_vec());
        }
    }
    let elapsed = start.elapsed();
    engine.evaluate_all().unwrap();
    for sheet in ["Left", "Right"] {
        for row in 1..=10000 {
            assert_eq!(
                engine.get_cell_value(sheet, row, 1),
                Some(LiteralValue::Number(8.0))
            );
        }
    }
    let edges: usize = engine
        .graph
        .iter_vertex_ids()
        .map(|v| engine.graph.get_dependencies(v).len())
        .sum();
    eprintln!(
        "name_heavy ingest_ms={} vertices={} edges={edges} correct=true",
        elapsed.as_millis(),
        engine.graph.vertex_count()
    );
    engine
        .set_cell_value("Inputs", 1, 1, LiteralValue::Number(8.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    for sheet in ["Left", "Right"] {
        for row in 1..=10000 {
            assert_eq!(
                engine.get_cell_value(sheet, row, 1),
                Some(LiteralValue::Number(9.0))
            );
        }
    }
}

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
