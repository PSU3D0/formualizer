//! Counter-free counterparts of selected perf_ranges fixtures (library cfg(test) is off).
//! Build with --release --features test-support; --validate never calls Instant.
//! Raw timing output stays local. An exclusive measurement window is required.
use arrow_array::Array;
use formualizer_common::{DateSystem, LiteralValue};
use formualizer_eval::arrow_store::{ArrowSheet, IngestBuilder, OverlayValue};
use formualizer_eval::engine::range_view::RangeView;
use formualizer_eval::engine::{Engine, EvalConfig, FormulaPlaneMode};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use std::hint::black_box;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
enum Read {
    Numbers,
    First,
    Sum,
    Count,
}

fn read(view: &RangeView<'_>, mode: Read) -> f64 {
    if matches!(mode, Read::First) {
        return view
            .iter_row_chunks()
            .next()
            .map(|r| {
                let segment = r.unwrap();
                black_box(&segment.cols);
                segment.row_len as f64
            })
            .unwrap_or(0.0);
    }
    let mut total = 0.0;
    if matches!(mode, Read::Sum) {
        for segment in view.errors_slices() {
            for col in segment.unwrap().2 {
                total += (col.len() - col.null_count()) as f64;
            }
        }
    }
    for segment in view.numbers_slices() {
        for col in segment.unwrap().2 {
            total += if matches!(mode, Read::Count) {
                (col.len() - col.null_count()) as f64
            } else {
                arrow::compute::kernels::aggregate::sum(col.as_ref()).unwrap_or(0.0)
            };
        }
    }
    black_box(total)
}

fn sparse(chunk_rows: usize, chunks: usize) -> ArrowSheet {
    let mut ingest = IngestBuilder::new("Source", 2, chunk_rows, DateSystem::Excel1900);
    for _ in 0..chunk_rows {
        ingest
            .append_row(&[LiteralValue::Number(2.0), LiteralValue::Empty])
            .unwrap();
    }
    let mut sheet = ingest.finish();
    sheet.ensure_row_capacity(chunk_rows * chunks);
    let tail = sheet.nrows as usize - 8;
    for row in tail..tail + 8 {
        let (ci, off) = sheet.chunk_of_row(row).unwrap();
        sheet
            .ensure_column_chunk_mut(0, ci)
            .unwrap()
            .overlay
            .set(off, OverlayValue::Number(3.0));
    }
    sheet
}

fn dense(chunk_rows: usize) -> ArrowSheet {
    let mut ingest = IngestBuilder::new("Source", 8, chunk_rows, DateSystem::Excel1900);
    for _ in 0..32768 {
        ingest
            .append_row(&vec![LiteralValue::Number(2.0); 8])
            .unwrap();
    }
    ingest.finish()
}

fn oracle(view: &RangeView<'_>, mode: Read) -> f64 {
    let sheet = view.sheet();
    let mut total = 0.0;
    for (ci, &start) in sheet.chunk_starts.iter().enumerate() {
        let end = sheet
            .chunk_starts
            .get(ci + 1)
            .copied()
            .unwrap_or(sheet.nrows as usize);
        let lo = start.max(view.start_row());
        let hi = end.min(view.end_row().saturating_add(1));
        if lo >= hi {
            continue;
        }
        if matches!(mode, Read::First) {
            return (hi - lo) as f64;
        }
        for r in lo..hi {
            for c in 0..view.dims().1 {
                match view.get_cell(r - view.start_row(), c) {
                    LiteralValue::Number(_) if matches!(mode, Read::Count) => total += 1.0,
                    LiteralValue::Number(n) => total += n,
                    LiteralValue::Error(_) if matches!(mode, Read::Sum) => total += 1.0,
                    _ => {}
                }
            }
        }
    }
    total
}

fn observe(validate: bool, f: impl FnOnce()) -> u128 {
    if validate {
        f();
        0
    } else {
        let start = Instant::now();
        f();
        start.elapsed().as_nanos()
    }
}

fn direct_case(
    name: &str,
    make: impl Fn() -> ArrowSheet,
    bounds: impl Fn(&ArrowSheet) -> (usize, usize, usize, usize),
    mode: Read,
    expected: f64,
    validate: bool,
    samples: usize,
    repeats: usize,
) {
    for sample in 0..samples {
        let sheet = make();
        let (sr, sc, er, ec) = bounds(&sheet);
        let view = sheet.range_view(sr, sc, er, ec);
        assert_eq!(oracle(&view, mode), expected);
        for phase in ["cold", "warm"] {
            let iterations = if phase == "cold" || validate {
                1
            } else {
                repeats
            };
            let mut checksum = 0.0;
            let ns = observe(validate, || {
                for _ in 0..iterations {
                    checksum += read(black_box(&view), mode);
                }
            });
            assert_eq!(checksum, expected * iterations as f64, "{name}/{phase}");
            if !validate {
                println!(
                    "{}",
                    serde_json::json!({
                        "family":"direct", "case":name, "mode":format!("{mode:?}"), "phase":phase,
                        "sample":sample, "iterations":iterations, "ns":ns, "checksum":checksum,
                        "bounds":[sr,sc,er,ec], "sheet_rows":sheet.nrows, "chunks":sheet.chunk_starts.len(),
                    })
                );
            }
        }
    }
}

fn direct(validate: bool, samples: usize) {
    for (chunk_rows, chunks) in [(32768, 1), (32768, 32), (256, 4096)] {
        for tail in [false, true] {
            let position = if tail { "tail" } else { "head" };
            for mode in [Read::Numbers, Read::First] {
                let expected = if matches!(mode, Read::First) {
                    8.0
                } else if tail {
                    24.0
                } else {
                    16.0
                };
                direct_case(
                    &format!("locality-{chunk_rows}-{chunks}-{position}"),
                    || sparse(chunk_rows, chunks),
                    |sheet| {
                        let sr = if tail { sheet.nrows as usize - 8 } else { 0 };
                        (sr, 0, sr + 7, 0)
                    },
                    mode,
                    expected,
                    validate,
                    samples,
                    128,
                );
            }
        }
    }
    for chunk_rows in [32768, 1024] {
        for mode in [Read::Sum, Read::Count] {
            let expected = if matches!(mode, Read::Count) {
                32768.0 * 8.0
            } else {
                32768.0 * 16.0
            };
            direct_case(
                &format!("dense-{chunk_rows}-false"),
                || dense(chunk_rows),
                |_| (0, 0, 32767, 7),
                mode,
                expected,
                validate,
                samples,
                8,
            );
        }
    }
    direct_case(
        "sparse-gaps-true-wide-false",
        || sparse(256, 4096),
        |sheet| (0, 0, sheet.nrows as usize - 1, 0),
        Read::Sum,
        256.0 * 2.0 + 24.0,
        validate,
        samples,
        4,
    );
}

fn engine(validate: bool, samples: usize) {
    for chunk_rows in [32768, 256] {
        for sample in 0..samples {
            let mut engine = Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    enable_parallel: false,
                    max_threads: Some(1),
                    formula_plane_mode: FormulaPlaneMode::Off,
                    arrow_storage_enabled: true,
                    delta_overlay_enabled: true,
                    write_formula_overlay_enabled: true,
                    ..EvalConfig::default()
                },
            );
            let mut ingest = engine.begin_bulk_ingest_arrow();
            ingest.add_sheet("Source", 2, chunk_rows);
            for row in 1..=32768 {
                ingest
                    .append_row(
                        "Source",
                        &[LiteralValue::Number(row as f64), LiteralValue::Number(2.0)],
                    )
                    .unwrap();
            }
            ingest.finish().unwrap();
            for (row, formula) in ["=SUM(Source!A32761:A32768)", "=COUNT(Source!A32761:A32768)"]
                .iter()
                .enumerate()
            {
                engine
                    .set_cell_formula("Results", row as u32 + 1, 1, parse(formula).unwrap())
                    .unwrap();
            }
            assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
            let mut computed = 0;
            let ns = observe(validate, || {
                computed = engine.evaluate_all().unwrap().computed_vertices;
            });
            assert_eq!(computed, 2);
            let sum = (32761 + 32768) as f64 * 4.0;
            assert_eq!(
                engine.get_cell_value("Results", 1, 1),
                Some(LiteralValue::Number(sum))
            );
            assert_eq!(
                engine.get_cell_value("Results", 2, 1),
                Some(LiteralValue::Number(8.0))
            );
            if !validate {
                println!(
                    "{}",
                    serde_json::json!({
                        "family":"engine", "case":"sum-count", "phase":"cold", "chunk_rows":chunk_rows,
                        "sample":sample, "iterations":1, "ns":ns, "checksum":sum + 8.0, "computed":computed,
                    })
                );
            }
        }
    }
}

fn main() {
    assert!(
        !black_box(cfg!(test)),
        "production corroboration must not enable cfg(test)"
    );
    let validate = std::env::args().skip(1).any(|arg| arg == "--validate");
    let samples = if validate {
        1
    } else {
        std::env::var("FZ_RANGES_SAMPLES")
            .ok()
            .map(|s| s.parse().unwrap())
            .unwrap_or(7)
    };
    direct(validate, samples);
    engine(validate, samples);
    if validate {
        println!("range production fixtures: validated (no timing)");
    }
}
