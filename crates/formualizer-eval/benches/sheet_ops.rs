use criterion::{Criterion, criterion_group, criterion_main};
use formualizer_eval::engine::EvalConfig;
use formualizer_eval::engine::eval::Engine;
// Use the wrapper that satisfies the EvaluationContext trait
use formualizer_eval::test_workbook::TestWorkbook;

fn setup_large_workbook(formula_count: usize) -> Engine<TestWorkbook> {
    let config = EvalConfig::default();
    // TestWorkbook likely implements Default or has a simple constructor
    let resolver = TestWorkbook::default();

    let mut engine = Engine::new(resolver, config);

    // Using engine methods to populate data
    engine.add_sheet("BaseSheet").unwrap();
    engine.add_sheet("Sheet1").unwrap();

    for i in 0..formula_count {
        let formula = format!("=BaseSheet!A{}", i + 1);
        let ast = formualizer_parse::parse(&formula).unwrap();
        let _ = engine.set_cell_formula("Sheet1", (i + 1) as u32, 1, ast);
    }

    engine.evaluate_all().unwrap();
    engine
}

fn bench_add_sheet_with_large_workbook(c: &mut Criterion) {
    let mut engine = setup_large_workbook(10_000);
    let mut iter_count = 0;

    c.bench_function("add_remove_sheet_metadata_overhead", |b| {
        b.iter(|| {
            iter_count += 1;
            let name = format!("BenchSheet-{}", iter_count);

            // Standard Engine API
            let sheet_id = engine.add_sheet(&name).unwrap();
            engine.remove_sheet(sheet_id).unwrap();
        })
    });
}

criterion_group!(benches, bench_add_sheet_with_large_workbook);
criterion_main!(benches);
