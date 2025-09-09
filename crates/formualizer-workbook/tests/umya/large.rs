// Integration test for Umya backend; run with `--features umya`.

use crate::common::build_standard_grid;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{LoadStrategy, SpreadsheetReader, UmyaAdapter, WorkbookLoader};
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

    let handle = std::thread::Builder::new().name("umya_large_perf".into()).stack_size(32*1024*1024).spawn(move || {
        let path = build_standard_grid(rows, cols);
        let start = Instant::now();
        let backend = UmyaAdapter::open_path(&path).expect("open path");
        let mut loader = WorkbookLoader::new(backend, LoadStrategy::EagerAll);
        let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
        let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
        engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::FastBatch);
        loader.load_into_engine(&mut engine).expect("load into engine");
        let elapsed = start.elapsed();
        eprintln!("[umya_large] adapter=umya rows={} cols={} cells_loaded={} elapsed_ms={} backend_read_ms={} engine_insert_ms={}",
            rows, cols, loader.stats().cells_loaded, elapsed.as_millis(), loader.stats().backend_read_time_ms, loader.stats().engine_insert_time_ms);
        assert!(elapsed.as_secs_f64() < 5.0, "Load exceeded 5s: {:?}", elapsed);
    }).expect("spawn perf thread");

    handle.join().expect("perf thread panicked");
}
