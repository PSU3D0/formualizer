// Integration test for Calamine backend; run with `--features calamine,umya`.
use crate::common::build_standard_grid;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};
use std::time::Instant; // generates grid via umya (col,row)

// This test is ignored by default due to generation cost; run with -- --ignored to include.
#[test]
#[ignore]
fn calamine_large_file_performance() {
    // Allow overriding size via env vars for experimentation
    let rows: u32 = std::env::var("FORMUALIZER_LARGE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000); // default ~100k cells with 20 cols
    let cols: u32 = std::env::var("FORMUALIZER_LARGE_COLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    // Spawn in thread with larger stack to mitigate potential deep recursion inside engine/calamine
    let handle = std::thread::Builder::new()
        .name("calamine_large_perf".into())
        .stack_size(32 * 1024 * 1024) // 32MB
        .spawn(move || {
            let path = build_standard_grid(rows, cols);
            let start = Instant::now();
            let mut backend = CalamineAdapter::open_path(&path).expect("open path");
            let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
            let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
            // Explicitly use FastBatch index mode for performance test
            engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::FastBatch);
            backend
                .stream_into_engine(&mut engine)
                .expect("load into engine");
            let elapsed = start.elapsed();

            eprintln!(
                "[calamine_large] rows={} cols={} elapsed={:?}",
                rows, cols, elapsed
            );
            assert!(elapsed.as_secs_f64() < 5.0, "Load exceeded 5s: {elapsed:?}");
        })
        .expect("spawn perf thread");

    handle.join().expect("perf thread panicked");
}
