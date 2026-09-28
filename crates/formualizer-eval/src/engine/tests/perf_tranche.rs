//! Correctness regressions retained from the historical tranche probe.

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn config(mode: FormulaPlaneMode, threads: usize) -> EvalConfig {
    EvalConfig {
        enable_parallel: threads > 1,
        max_threads: Some(threads),
        formula_plane_mode: mode,
        arrow_storage_enabled: true,
        delta_overlay_enabled: true,
        write_formula_overlay_enabled: true,
        ..EvalConfig::default()
    }
}

fn formula(e: &mut Engine<TestWorkbook>, row: u32, col: u32, text: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(text).unwrap())
        .unwrap();
}

fn dirty(e: &mut Engine<TestWorkbook>) {
    let ids: Vec<_> = e.graph.vertices_with_formulas().collect();
    for id in ids {
        e.graph.mark_vertex_dirty(id);
    }
}

fn assert_number(e: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    let v = e.get_cell_value("Sheet1", row, col).unwrap();
    let n = match v {
        LiteralValue::Number(n) => n,
        LiteralValue::Int(n) => n as f64,
        _ => panic!("{v:?}"),
    };
    assert_eq!(n, expected);
}

#[test]
fn criteria_whole_mask_work_is_independent_of_driver_chunks() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for chunks in [1, 8, 32] {
            for text in [false, true] {
                let mut e = Engine::new(TestWorkbook::new(), config(mode, 1));
                let mut ingest = e.begin_bulk_ingest_arrow();
                ingest.add_sheet("Sheet1", 2, 256 / chunks);
                for _ in 0..256 {
                    let key = if text {
                        LiteralValue::Text("alpha".into())
                    } else {
                        LiteralValue::Number(1.0)
                    };
                    ingest
                        .append_row("Sheet1", &[key, LiteralValue::Number(2.0)])
                        .unwrap();
                }
                ingest.finish().unwrap();
                let pred = if text { "\"a*\"" } else { "\">0\"" };
                for p in [1, 4] {
                    let pairs = std::iter::repeat_n(format!("A1:A256,{pred}"), p)
                        .collect::<Vec<_>>()
                        .join(",");
                    for (expression, expected, calls) in [
                        (format!("=SUMIFS(B1:B256,{pairs})"), 512.0, p),
                        (format!("=COUNTIFS({pairs})"), 256.0, p),
                        (format!("=AVERAGEIFS(B1:B256,{pairs})"), 2.0, p),
                        (format!("=SUMIF(A1:A256,{pred},B1:B256)"), 512.0, 1),
                        (format!("=SUMIFS(B1:B256,FALSE,TRUE,{pairs})"), 0.0, 0),
                    ] {
                        formula(&mut e, 1, 4, &expression);
                        super::super::eval::criteria_mask_test_hooks::take_mask_work();
                        e.evaluate_all().unwrap();
                        assert_number(&e, 1, 4, expected);
                        assert_eq!(
                            super::super::eval::criteria_mask_test_hooks::take_mask_work(),
                            (calls, calls * 256),
                            "{mode:?} chunks={chunks} {expression}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn lookup_mutation_boundaries_reclaim_and_readmit() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for mutation in 0..4 {
            let mut cfg = config(mode, 1);
            cfg.lookup_index_cache_max_bytes = 512_000;
            // Every member runs its own lookup: these tests count lookup
            // index builds and hits per member (the lift evaluates a
            // run-invariant MATCH once per run since Program 3).
            cfg.family_lift = false;
            let mut e = Engine::new(TestWorkbook::new(), cfg);
            for row in 1..=128 {
                e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                    .unwrap();
            }
            for row in 1..=12 {
                formula(&mut e, row, 3, "=MATCH(128,$A$1:$A$128,0)");
            }
            for cycle in 0..8 {
                dirty(&mut e);
                e.evaluate_all().unwrap();
                let report = e.last_lookup_index_cache_report();
                assert_eq!(report.builds, 1, "{mode:?} {mutation} {cycle} {report:?}");
                assert!(report.hits >= 8, "{report:?}");
                assert_eq!(report.entries_count, 1);
                assert_eq!(report.skipped_cap, 0);
                assert_number(&e, 1, 3, 128.0);
                dirty(&mut e);
                e.evaluate_all().unwrap();
                let warm = e.last_lookup_index_cache_report();
                assert_eq!(warm.builds, 0);
                assert_eq!(warm.hits, 12);
                assert_eq!(warm.bytes_in_cache, report.bytes_in_cache);
                assert_eq!(warm.entries_count, report.entries_count);
                match mutation {
                    0 => e.mark_data_edited(),
                    1 => e.mark_topology_edited(),
                    2 => e
                        .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(cycle as f64))
                        .unwrap(),
                    _ => e
                        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(-(cycle as f64)))
                        .unwrap(),
                }
                let report = e.last_lookup_index_cache_report();
                assert_eq!(report.entries_count, 0);
                assert_eq!(report.bytes_in_cache, 0);
            }
        }
    }
}

#[test]
fn lookup_axis_edits_change_answers_with_active_spans() {
    use crate::engine::{FormulaIngestBatch, FormulaIngestRecord};
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for text in [false, true] {
            let mut e = Engine::new(TestWorkbook::new(), config(mode, 1));
            let key = |i: u32| {
                if text {
                    LiteralValue::Text(format!("key-{i}"))
                } else {
                    LiteralValue::Number(i as f64)
                }
            };
            for row in 1..=128 {
                e.set_cell_value("Sheet1", row, 1, key(row)).unwrap();
                e.set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64 * 10.0))
                    .unwrap();
            }
            e.set_cell_value("Sheet1", 1, 6, key(128)).unwrap();
            let mut records = Vec::new();
            for row in 1..=24 {
                for (col, expr) in [
                    (3, "=MATCH($F$1,$A$1:$A$128,0)"),
                    (4, "=VLOOKUP($F$1,$A$1:$B$128,2,FALSE)"),
                    (5, "=XLOOKUP($F$1,$A$1:$A$128,$B$1:$B$128)"),
                    (7, "=ABS(ROUND(ABS(ROUND($B$1,2)),2))"),
                ] {
                    let ast = e.intern_formula_ast(&parse(expr).unwrap());
                    records.push(FormulaIngestRecord::new(
                        row,
                        col,
                        ast,
                        Some(std::sync::Arc::<str>::from(expr)),
                    ));
                }
            }
            e.ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
                .unwrap();
            for cycle in 0..4 {
                if cycle > 0 {
                    e.set_cell_value("Sheet1", 1, 1, key(if cycle % 2 == 1 { 128 } else { 1 }))
                        .unwrap();
                    e.set_cell_value("Sheet1", 128, 1, key(if cycle % 2 == 1 { 1 } else { 128 }))
                        .unwrap();
                }
                dirty(&mut e);
                e.evaluate_all().unwrap();
                for row in 1..=24 {
                    assert_number(&e, row, 3, if cycle % 2 == 1 { 1.0 } else { 128.0 });
                    for col in [4, 5] {
                        assert_number(&e, row, col, if cycle % 2 == 1 { 10.0 } else { 1280.0 });
                    }
                    assert_number(&e, row, 7, 10.0);
                }
                let report = e.last_lookup_index_cache_report();
                dirty(&mut e);
                e.evaluate_all().unwrap();
                let warm = e.last_lookup_index_cache_report();
                assert_eq!(warm.builds, 0);
                assert!(warm.hits > 0);
                assert_eq!(warm.bytes_in_cache, report.bytes_in_cache);
            }
        }
    }
}

#[test]
fn oversized_text_index_is_built_once_per_snapshot() {
    use crate::engine::lookup_index_cache::take_build_attempts;
    let mut cfg = config(FormulaPlaneMode::Off, 1);
    cfg.lookup_index_cache_max_bytes = 512_000;
    // Every member runs its own lookup: these tests count lookup
    // index builds and hits per member (the lift evaluates a
    // run-invariant MATCH once per run since Program 3).
    cfg.family_lift = false;
    let mut e = Engine::new(TestWorkbook::new(), cfg);
    for row in 1..=128 {
        e.set_cell_value(
            "Sheet1",
            row,
            1,
            LiteralValue::Text(format!("{row}-{}", "X".repeat(4096))),
        )
        .unwrap();
    }
    for row in 1..=16 {
        formula(&mut e, row, 3, "=MATCH($A$1,$A$1:$A$128,0)");
    }
    for _ in 0..3 {
        take_build_attempts();
        dirty(&mut e);
        e.evaluate_all().unwrap();
        assert_eq!(take_build_attempts(), 1);
        for row in 1..=16 {
            assert_number(&e, row, 3, 1.0);
        }
        let report = e.last_lookup_index_cache_report();
        assert_eq!(report.skipped_cap, 13);
        assert_eq!(report.bytes_in_cache, 0);
        dirty(&mut e);
        e.evaluate_all().unwrap();
        assert_eq!(take_build_attempts(), 0);
        assert_eq!(e.last_lookup_index_cache_report().skipped_cap, 16);
        e.mark_data_edited();
    }
}
