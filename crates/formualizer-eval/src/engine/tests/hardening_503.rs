use crate::engine::{eval::Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

#[test]
fn overlay_budget_keeps_computed_overlay_bounded_across_recalcs() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.max_overlay_memory_bytes = Some(2048);
    let mut engine = Engine::new(wb, cfg);

    // Many formula cells that would normally be mirrored into computed overlays.
    // Using a large enough count to exceed the tiny cap deterministically.
    for r in 1..=2000 {
        engine
            .set_cell_formula("Sheet1", r, 1, parse("=1").unwrap())
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A2000)").unwrap())
        .unwrap();

    // Repeated recalcs should not grow computed overlay memory without bound.
    for _ in 0..5 {
        let _ = engine.evaluate_all().unwrap();
        assert!(engine.overlay_memory_usage() <= 2048);
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(2000.0))
        );
    }
}

#[test]
fn bulk_spill_clear_dirties_dependents_without_delta_scan_fallback() {
    // This test exercises a large spill projection/clear. To avoid platform-specific
    // stack limits in the test harness, run the work on a larger-stack thread.
    std::thread::Builder::new()
        .name("hardening_503_bulk_spill_clear".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let wb = TestWorkbook::new();
            let mut engine = Engine::new(wb, EvalConfig::default());

            engine
                .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(5000,1)").unwrap())
                .unwrap();
            engine
                .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A5000)").unwrap())
                .unwrap();

            // Two-pass pattern: first pass materializes spill values,
            // second pass computes dependents that read the spilled region.
            let _ = engine.evaluate_all().unwrap();

            // Sanity: spill outputs should be mirrored into Arrow computed overlay.
            {
                let asheet = engine.sheet_store().sheet("Sheet1").expect("arrow sheet");
                let col0 = &asheet.columns[0];

                // Chunking-agnostic: locate the chunk for a given absolute row.
                let at_row = |row0: usize| -> Option<&crate::arrow_store::OverlayValue> {
                    let (ch_idx, in_off) = asheet.chunk_of_row(row0)?;
                    let ch = col0.chunk(ch_idx)?;
                    ch.computed_overlay.get(in_off)
                };

                match at_row(0) {
                    Some(crate::arrow_store::OverlayValue::Number(n)) => {
                        assert!((*n - 1.0).abs() < 1e-6)
                    }
                    other => panic!("expected computed overlay number at row0=0, got {other:?}"),
                }
                match at_row(9) {
                    Some(crate::arrow_store::OverlayValue::Number(n)) => {
                        assert!((*n - 10.0).abs() < 1e-6)
                    }
                    other => panic!("expected computed overlay number at row0=9, got {other:?}"),
                }
                match at_row(4999) {
                    Some(crate::arrow_store::OverlayValue::Number(n)) => {
                        assert!((*n - 5000.0).abs() < 1e-6)
                    }
                    other => {
                        panic!("expected computed overlay number at row0=4999, got {other:?}")
                    }
                }

                // Direct Arrow sum over the spilled column should see computed overlay.
                let av = asheet.range_view(0, 0, 4999, 0);
                let mut tot = 0.0;
                for res in av.numbers_slices() {
                    let (_, _, num_cols) = res.unwrap();
                    for col in num_cols {
                        tot += arrow::compute::kernels::aggregate::sum(col.as_ref()).unwrap_or(0.0);
                    }
                }
                assert!((tot - 12_502_500.0).abs() < 1e-6, "arrow sum saw {tot}");
            }

            let _ = engine.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 2),
                Some(LiteralValue::Number(12_502_500.0))
            );

            engine.graph.reset_instr();
            engine
                .set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
                .unwrap();
            let _ = engine.evaluate_all().unwrap();
            let _ = engine.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();

            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 2),
                Some(LiteralValue::Number(1.0))
            );

            let instr = engine.graph.instr();
            assert_eq!(instr.dependents_scan_fallback_calls, 0);
        })
        .unwrap()
        .join()
        .unwrap();
}
