//! Standalone release probe: see benchmarks/perf-tranche-419-421.md.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::time::Instant;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

thread_local! {
    static ALLOCATIONS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}
struct ProbeAllocator;
#[global_allocator]
static ALLOCATOR: ProbeAllocator = ProbeAllocator;

fn allocation(bytes: usize) {
    let _ = ALLOCATIONS.try_with(|c| {
        if let Some((calls, total)) = c.get() {
            c.set(Some((calls + 1, total + bytes)));
        }
    });
}
unsafe impl GlobalAlloc for ProbeAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
fn measure(f: impl FnOnce()) -> (u128, usize, usize) {
    ALLOCATIONS.with(|c| c.set(Some((0, 0))));
    let start = Instant::now();
    f();
    let ns = start.elapsed().as_nanos();
    let (calls, bytes) = ALLOCATIONS.with(|c| c.replace(None).unwrap());
    (ns, calls, bytes)
}
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
    e.graph.mark_all_formula_spans_dirty(
        crate::engine::graph::WholeSpanDirtyReason::GlobalInvalidation,
    );
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
fn samples() -> usize {
    std::env::var("FZ_PERF_SAMPLES")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(7)
}

fn criteria() {
    const N: usize = 32768;
    for chunks in [1, 8, 32] {
        for predicates in [1, 4] {
            for text in [false, true] {
                for formulas in [1, 16] {
                    let mut e = Engine::new(TestWorkbook::new(), config(FormulaPlaneMode::Off, 1));
                    let setup = measure(|| {
                        let mut ingest = e.begin_bulk_ingest_arrow();
                        ingest.add_sheet("Sheet1", 2, N / chunks);
                        for i in 0..N {
                            let key = if text {
                                LiteralValue::Text(if i % 2 == 0 { "alpha" } else { "beta" }.into())
                            } else {
                                LiteralValue::Number((i % 2) as f64)
                            };
                            ingest
                                .append_row("Sheet1", &[key, LiteralValue::Number(2.0)])
                                .unwrap();
                        }
                        ingest.finish().unwrap();
                        let pred = if text { "\"alpha\"" } else { "\">0\"" };
                        let pairs = std::iter::repeat_n(format!("A1:A{N},{pred}"), predicates)
                            .collect::<Vec<_>>()
                            .join(",");
                        for row in 1..=formulas {
                            formula(&mut e, row, 4, &format!("=SUMIFS(B1:B{N},{pairs})"));
                        }
                    });
                    println!(
                        "criteria_setup chunks={chunks} p={predicates} text={text} formulas={formulas} ns={} allocs={} allocated_bytes={}",
                        setup.0, setup.1, setup.2
                    );
                    for sample in 0..samples() {
                        dirty(&mut e);
                        super::super::eval::criteria_mask_test_hooks::take_mask_work();
                        let m = measure(|| {
                            e.evaluate_all().unwrap();
                        });
                        let work = super::super::eval::criteria_mask_test_hooks::take_mask_work();
                        for row in 1..=formulas {
                            assert_number(&e, row, 4, N as f64);
                        }
                        println!(
                            "criteria chunks={chunks} p={predicates} text={text} formulas={formulas} sample={sample} ns={} allocs={} allocated_bytes={} mask_calls={} mask_logical_rows={}",
                            m.0, m.1, m.2, work.0, work.1
                        );
                    }
                    let edit = measure(|| {
                        e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(4.0))
                            .unwrap();
                    });
                    let recalc = measure(|| {
                        e.evaluate_all().unwrap();
                    });
                    for row in 1..=formulas {
                        assert_number(&e, row, 4, N as f64 + if text { 2.0 } else { 0.0 });
                    }
                    println!(
                        "criteria_edit chunks={chunks} p={predicates} text={text} formulas={formulas} edit_ns={} recalc_ns={} allocated_bytes={}",
                        edit.0, recalc.0, recalc.2
                    );
                }
            }
        }
    }
}

fn criteria_floors() {
    for whole_column in [false, true] {
        let mut e = Engine::new(TestWorkbook::new(), config(FormulaPlaneMode::Off, 1));
        let setup = measure(|| {
            let mut ingest = e.begin_bulk_ingest_arrow();
            ingest.add_sheet("Sheet1", 2, 2);
            for i in 0..8 {
                let value = if i == 0 || i == 7 {
                    LiteralValue::Number(1.0)
                } else {
                    LiteralValue::Empty
                };
                ingest
                    .append_row("Sheet1", &[value, LiteralValue::Number(2.0)])
                    .unwrap();
            }
            ingest.finish().unwrap();
            formula(
                &mut e,
                1,
                4,
                if whole_column {
                    "=SUMIFS(B:B,A:A,\">0\")"
                } else {
                    "=SUMIFS(B1:B8,A1:A8,\">0\")"
                },
            );
        });
        println!(
            "criteria_floor_setup whole_column={whole_column} ns={} allocated_bytes={}",
            setup.0, setup.2
        );
        for sample in 0..samples() {
            dirty(&mut e);
            super::super::eval::criteria_mask_test_hooks::take_mask_work();
            let m = measure(|| {
                e.evaluate_all().unwrap();
            });
            let work = super::super::eval::criteria_mask_test_hooks::take_mask_work();
            assert_number(&e, 1, 4, 4.0);
            println!(
                "criteria_floor whole_column={whole_column} sample={sample} ns={} allocs={} allocated_bytes={} mask_calls={} mask_logical_rows={}",
                m.0, m.1, m.2, work.0, work.1
            );
        }
    }
}

fn registry() {
    crate::builtins::load_builtins();
    for threads in [1, 4] {
        for sample in 0..samples() {
            let start = Instant::now();
            let counts: Vec<_> = std::thread::scope(|s| {
                (0..threads)
                    .map(|_| {
                        s.spawn(|| {
                            measure(|| {
                                for _ in 0..100_000 {
                                    black_box(crate::function_registry::get("", "ABS").unwrap());
                                    black_box(crate::function_registry::get("", "ROUND").unwrap());
                                }
                            })
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .collect()
            });
            println!(
                "registry threads={threads} sample={sample} ns={} lookups={} allocs={} allocated_bytes={}",
                start.elapsed().as_nanos(),
                threads * 200_000,
                counts.iter().map(|x| x.1).sum::<usize>(),
                counts.iter().map(|x| x.2).sum::<usize>()
            );
        }
        for mode in [
            FormulaPlaneMode::Off,
            FormulaPlaneMode::AuthoritativeExperimental,
        ] {
            for calls in [false, true] {
                let mut e = Engine::new(TestWorkbook::new(), config(mode, threads));
                let setup = measure(|| {
                    for row in 1..=2048 {
                        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(-2.0))
                            .unwrap();
                        let expr = if calls {
                            format!("=ABS(ROUND(ABS(ROUND(A{row},2)),2))")
                        } else {
                            format!("=((A{row}+2)*3-4)/2")
                        };
                        formula(&mut e, row, 2, &expr);
                    }
                });
                let spans = e.baseline_stats().formula_plane_active_span_count;
                assert_eq!(spans, 0);
                println!(
                    "registry_engine_setup threads={threads} mode={mode:?} calls={calls} ns={} active_spans={spans} execution=legacy",
                    setup.0
                );
                for sample in 0..samples() {
                    dirty(&mut e);
                    let m = measure(|| {
                        e.evaluate_all().unwrap();
                    });
                    for row in 1..=2048 {
                        assert_number(&e, row, 2, if calls { 2.0 } else { -2.0 });
                    }
                    println!(
                        "registry_engine threads={threads} mode={mode:?} calls={calls} formulas=2048 sample={sample} ns={} allocs_main_thread={} allocated_bytes_main_thread={}",
                        m.0, m.1, m.2
                    );
                }
            }
        }
    }
}

fn lookup() {
    const N: usize = 512;
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for text in [false, true] {
            for axis_edit in [false, true] {
                for sample in 0..samples() {
                    let mut cfg = config(mode, 1);
                    cfg.lookup_index_cache_max_bytes = 512_000;
                    let mut e = Engine::new(TestWorkbook::new(), cfg);
                    let key = |i: usize| {
                        if text {
                            LiteralValue::Text(format!("key-{i:05}-{}", "x".repeat(96)))
                        } else {
                            LiteralValue::Number(i as f64)
                        }
                    };
                    let setup = measure(|| {
                        let mut ingest = e.begin_bulk_ingest_arrow();
                        ingest.add_sheet("Sheet1", 2, 64);
                        for i in 0..N {
                            ingest
                                .append_row("Sheet1", &[key(i), LiteralValue::Number(i as f64)])
                                .unwrap();
                        }
                        ingest.finish().unwrap();
                        e.set_cell_value("Sheet1", 1, 3, key(N - 1)).unwrap();
                        for row in 1..=24 {
                            let expr = match row % 3 {
                                0 => format!("=VLOOKUP($C$1,$A$1:$B${N},2,FALSE)"),
                                1 => format!("=MATCH($C$1,$A$1:$A${N},0)-1"),
                                _ => format!("=XLOOKUP($C$1,$A$1:$A${N},$B$1:$B${N})"),
                            };
                            formula(&mut e, row, 4, &expr);
                        }
                    });
                    let spans = e.baseline_stats().formula_plane_active_span_count;
                    assert_eq!(spans, 0);
                    println!(
                        "lookup_setup mode={mode:?} text={text} axis_edit={axis_edit} sample={sample} ns={} active_spans={spans} execution=legacy",
                        setup.0
                    );
                    for cycle in 0..16 {
                        let edit = measure(|| {
                            if cycle > 0 {
                                if axis_edit {
                                    let moved = cycle % 2 == 1;
                                    e.set_cell_value(
                                        "Sheet1",
                                        1,
                                        1,
                                        key(if moved { N - 1 } else { 0 }),
                                    )
                                    .unwrap();
                                    e.set_cell_value(
                                        "Sheet1",
                                        N as u32,
                                        1,
                                        key(if moved { 0 } else { N - 1 }),
                                    )
                                    .unwrap();
                                } else {
                                    e.set_cell_value(
                                        "Sheet1",
                                        1,
                                        6,
                                        LiteralValue::Number(cycle as f64),
                                    )
                                    .unwrap();
                                }
                            }
                        });
                        dirty(&mut e);
                        let m = measure(|| {
                            e.evaluate_all().unwrap();
                        });
                        let expected = if axis_edit && cycle % 2 == 1 {
                            0.0
                        } else {
                            (N - 1) as f64
                        };
                        for row in 1..=24 {
                            assert_number(&e, row, 4, expected);
                        }
                        println!(
                            "lookup mode={mode:?} text={text} axis_edit={axis_edit} sample={sample} cycle={cycle} edit_ns={} ns={} allocs={} allocated_bytes={} report={:?}",
                            edit.0,
                            m.0,
                            m.1,
                            m.2,
                            e.last_lookup_index_cache_report()
                        );
                        dirty(&mut e);
                        let warm = measure(|| {
                            e.evaluate_all().unwrap();
                        });
                        for row in 1..=24 {
                            assert_number(&e, row, 4, expected);
                        }
                        println!(
                            "lookup_warm mode={mode:?} text={text} axis_edit={axis_edit} sample={sample} cycle={cycle} ns={} allocs={} allocated_bytes={} report={:?}",
                            warm.0,
                            warm.1,
                            warm.2,
                            e.last_lookup_index_cache_report()
                        );
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "release measurement; run alone with --nocapture --test-threads=1"]
fn release_probe() {
    match std::env::var("FZ_PERF_CASE").as_deref() {
        Ok("criteria") => {
            criteria();
            criteria_floors();
        }
        Ok("registry") => registry(),
        Ok("lookup") => lookup(),
        _ => {
            criteria();
            criteria_floors();
            registry();
            lookup();
        }
    }
}

// Regression tests (not part of the baseline measurement harness).
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
            let spans = e.baseline_stats().formula_plane_active_span_count;
            if mode == FormulaPlaneMode::AuthoritativeExperimental {
                assert_eq!(spans, 3);
                // MATCH, VLOOKUP and nested ABS/ROUND promote. XLOOKUP currently
                // retains its existing UnsupportedCanonicalTemplate fallback.
                assert_eq!(
                    e.last_formula_ingest_report()
                        .unwrap()
                        .graph_formula_cells_materialized,
                    24
                );
            } else {
                assert_eq!(spans, 0);
            }
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
                assert_eq!(
                    report.builds,
                    if mode == FormulaPlaneMode::Off { 2 } else { 1 },
                    "{mode:?} {report:?}"
                );
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
