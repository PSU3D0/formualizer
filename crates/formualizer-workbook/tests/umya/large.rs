// Integration test for Umya backend; run with `--features umya`.

use crate::common::build_standard_grid;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{SpreadsheetReader, UmyaAdapter};
use std::time::Instant;

#[test]
#[ignore]
fn umya_large_file_performance() {
    let rows: u32 = std::env::var("FORMUALIZER_LARGE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let cols: u32 = std::env::var("FORMUALIZER_LARGE_COLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    let handle = std::thread::Builder::new()
        .name("umya_large_perf".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let path = build_standard_grid(rows, cols);
            let start = Instant::now();
            let mut backend = UmyaAdapter::open_path(&path).expect("open path");
            let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
            let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
            engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::FastBatch);
            backend
                .stream_into_engine(&mut engine)
                .expect("load into engine");
            let elapsed = start.elapsed();
            eprintln!(
                "[umya_large] adapter=umya rows={} cols={} elapsed_ms={}",
                rows,
                cols,
                elapsed.as_millis()
            );
            assert!(
                elapsed.as_secs_f64() < 5.0,
                "Load exceeded 5s: {:?}",
                elapsed
            );
        })
        .expect("spawn perf thread");

    handle.join().expect("perf thread panicked");
}
