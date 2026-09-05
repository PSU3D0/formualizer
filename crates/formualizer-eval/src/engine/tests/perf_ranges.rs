//! Shared baseline/candidate fixtures. Timing runs require an exclusive resource window.
use super::{config, dirty, measure};
use crate::arrow_store::{ArrowSheet, IngestBuilder, OverlayValue};
use crate::engine::range_view::{RangeView, range_work};
use crate::engine::{Engine, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use arrow_array::Array;
use formualizer_common::{DateSystem, LiteralValue};
use formualizer_parse::parser::parse;
use std::hint::black_box;

#[derive(Clone, Copy, Debug)]
enum Read {
    Generic,
    First,
    Numbers,
    Errors,
    Sum,
    Count,
}

fn read(view: &RangeView<'_>, mode: Read) -> f64 {
    let mut out = 0.0;
    match mode {
        Read::Generic | Read::First => {
            for item in view.iter_row_chunks() {
                let item = item.unwrap();
                out += item.row_len as f64;
                black_box(&item.cols);
                if matches!(mode, Read::First) {
                    break;
                }
            }
        }
        Read::Numbers | Read::Count | Read::Sum => {
            if matches!(mode, Read::Sum) {
                out += read(view, Read::Errors);
            }
            for item in view.numbers_slices() {
                let (_, _, cols) = item.unwrap();
                for col in cols {
                    out += if matches!(mode, Read::Count) {
                        (col.len() - col.null_count()) as f64
                    } else {
                        arrow::compute::kernels::aggregate::sum(col.as_ref()).unwrap_or(0.0)
                    };
                }
            }
        }
        Read::Errors => {
            for item in view.errors_slices() {
                let (_, _, cols) = item.unwrap();
                for col in cols {
                    out += (col.len() - col.null_count()) as f64;
                }
            }
        }
    }
    black_box(out)
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

fn dense(chunk_rows: usize, mixed: bool) -> ArrowSheet {
    let mut ingest = IngestBuilder::new("Source", 8, chunk_rows, DateSystem::Excel1900);
    for row in 0..32768 {
        let values: Vec<_> = (0..8)
            .map(|col| {
                if mixed && (row + col) % 3 == 0 {
                    LiteralValue::Text("mixed".into())
                } else {
                    LiteralValue::Number(2.0)
                }
            })
            .collect();
        ingest.append_row(&values).unwrap();
    }
    ingest.finish()
}

fn scalar_oracle(view: &RangeView<'_>, mode: Read) -> f64 {
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
        if matches!(mode, Read::Generic | Read::First) {
            total += (hi - lo) as f64;
            if matches!(mode, Read::First) {
                break;
            }
            continue;
        }
        for r in lo..hi {
            for c in 0..view.dims().1 {
                match view.get_cell(r - view.start_row(), c) {
                    LiteralValue::Number(n) if matches!(mode, Read::Numbers | Read::Sum) => {
                        total += n
                    }
                    LiteralValue::Number(_) if matches!(mode, Read::Count) => total += 1.0,
                    LiteralValue::Error(_) if matches!(mode, Read::Errors | Read::Sum) => {
                        total += 1.0
                    }
                    _ => {}
                }
            }
        }
    }
    total
}

fn observation(timed: bool, f: impl FnOnce()) -> (u128, usize, usize) {
    if timed {
        measure(f)
    } else {
        f();
        (0, 0, 0)
    }
}

fn direct_case(
    name: &str,
    make: impl Fn() -> ArrowSheet,
    bounds: impl Fn(&ArrowSheet) -> (usize, usize, usize, usize),
    mode: Read,
    timed: bool,
    samples: usize,
    repeats: usize,
) {
    for sample in 0..samples {
        let sheet = make();
        let (sr, sc, er, ec) = bounds(&sheet);
        let view = sheet.range_view(sr, sc, er, ec);
        let expected = scalar_oracle(&view, mode);
        for phase in ["cold", "warm"] {
            let iterations = if phase == "cold" { 1 } else { repeats };
            range_work::begin();
            let mut answer = 0.0;
            let measured = observation(timed, || {
                for _ in 0..iterations {
                    answer += read(black_box(&view), mode);
                }
            });
            let work = range_work::take();
            assert_eq!(
                answer,
                expected * iterations as f64,
                "{name}/{mode:?}/{phase}"
            );
            if timed {
                println!(
                    "{}",
                    serde_json::json!({
                        "family":"direct", "case":name, "mode":format!("{mode:?}"), "phase":phase,
                        "sample":sample, "iterations":iterations, "ns":measured.0,
                        "allocations":measured.1, "allocated_bytes":measured.2, "work":work,
                        "bounds":[sr,sc,er,ec], "sheet_rows":sheet.nrows, "chunks":sheet.chunk_starts.len(),
                        "physical_chunks":sheet.columns.iter().map(|c| c.total_chunk_count()).sum::<usize>()
                    })
                );
            }
        }
    }
}

fn direct(timed: bool, samples: usize) {
    for (chunk_rows, chunks) in [(32768, 1), (32768, 32), (256, 32), (256, 4096)] {
        for position in ["head", "tail", "oob", "cross"] {
            for mode in [Read::Generic, Read::First, Read::Numbers, Read::Errors] {
                direct_case(
                    &format!("locality-{chunk_rows}-{chunks}-{position}"),
                    || sparse(chunk_rows, chunks),
                    |sheet| {
                        let start = match position {
                            "head" => 0,
                            "tail" => sheet.nrows as usize - 8,
                            "cross" => chunk_rows - 4,
                            _ => sheet.nrows as usize + 8,
                        };
                        (start, 0, start + 7, 0)
                    },
                    mode,
                    timed,
                    samples,
                    if timed { 128 } else { 1 },
                );
            }
        }
    }
    for (chunk_rows, mixed) in [(32768, false), (1024, false), (1024, true)] {
        for mode in [Read::Numbers, Read::Sum, Read::Count] {
            direct_case(
                &format!("dense-{chunk_rows}-{mixed}"),
                || dense(chunk_rows, mixed),
                |_| (0, 0, 32767, 7),
                mode,
                timed,
                samples,
                if timed { 8 } else { 1 },
            );
        }
    }
    for gaps in [false, true] {
        for wide in [false, true] {
            direct_case(
                &format!("sparse-gaps-{gaps}-wide-{wide}"),
                || sparse(256, 4096),
                |sheet| {
                    (
                        if gaps { 0 } else { sheet.nrows as usize - 8 },
                        0,
                        sheet.nrows as usize - 1,
                        if wide { 7 } else { 0 },
                    )
                },
                Read::Sum,
                timed,
                samples,
                if timed { 4 } else { 1 },
            );
        }
    }
    for overlay in ["outside", "partial", "computed"] {
        direct_case(
            &format!("overlay-{overlay}"),
            || {
                let mut sheet = sparse(32768, 1);
                let ch = &mut sheet.columns[0].chunks[0];
                match overlay {
                    "outside" => {
                        ch.overlay.set(100, OverlayValue::Number(4.0));
                    }
                    "partial" => {
                        ch.computed_overlay.set(2, OverlayValue::Number(9.0));
                        ch.overlay.set(2, OverlayValue::Empty);
                        ch.overlay.set(
                            3,
                            OverlayValue::Error(crate::arrow_store::map_error_code(
                                formualizer_common::ExcelErrorKind::Div,
                            )),
                        );
                    }
                    _ => {
                        for off in 0..8 {
                            ch.computed_overlay.set(off, OverlayValue::Number(5.0));
                        }
                    }
                }
                sheet
            },
            |_| (0, 0, 7, 0),
            Read::Sum,
            timed,
            samples,
            if timed { 128 } else { 1 },
        );
    }
}

fn engine(timed: bool, samples: usize) {
    for family in ["sum-count", "lookup"] {
        for index_enabled in [false, true] {
            for chunk_rows in [32768, 256] {
                let mut cfg = config(FormulaPlaneMode::Off, 1);
                if !index_enabled {
                    cfg.lookup_index_cache_max_bytes = 0;
                }
                let mut engine = Engine::new(TestWorkbook::new(), cfg);
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
                let formulas: &[&str] = if family == "sum-count" {
                    &["=SUM(Source!A32761:A32768)", "=COUNT(Source!A32761:A32768)"]
                } else {
                    &[
                        "=VLOOKUP(32705,Source!A32705:B32768,2,FALSE)",
                        "=VLOOKUP(32768,Source!A32705:B32768,2,FALSE)",
                        "=VLOOKUP(99999,Source!A32705:B32768,2,FALSE)",
                    ]
                };
                for (row, formula) in formulas.iter().enumerate() {
                    engine
                        .set_cell_formula("Results", row as u32 + 1, 1, parse(formula).unwrap())
                        .unwrap();
                }
                for sample in 0..samples.max(4) {
                    dirty(&mut engine);
                    range_work::begin();
                    let measured = observation(timed, || {
                        engine.evaluate_all().unwrap();
                    });
                    let work = range_work::take();
                    if family == "sum-count" {
                        for (row, expected) in [(1, (32761 + 32768) as f64 * 4.0), (2, 8.0)] {
                            assert_eq!(
                                engine.get_cell_value("Results", row, 1),
                                Some(LiteralValue::Number(expected))
                            );
                        }
                    } else {
                        for row in 1..=2 {
                            assert_eq!(
                                engine.get_cell_value("Results", row, 1),
                                Some(LiteralValue::Number(2.0))
                            );
                        }
                        assert!(
                            matches!(engine.get_cell_value("Results", 3, 1), Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Na)
                        );
                        let report = engine.last_lookup_index_cache_report();
                        if !index_enabled {
                            assert_eq!(report.builds, 0);
                            assert_eq!(report.hits, 0);
                            assert!(work.segments >= 3);
                        } else if sample >= 3 {
                            assert!(report.hits >= 3);
                            assert_eq!(work.segments, 0);
                        }
                    }
                    if timed {
                        let report = engine.last_lookup_index_cache_report();
                        println!(
                            "{}",
                            serde_json::json!({
                                "family":"engine", "case":family, "index_enabled":index_enabled, "chunk_rows":chunk_rows,
                                "sample":sample, "ns":measured.0, "allocations":measured.1,
                                "allocated_bytes":measured.2, "work":work,
                                "lookup_builds":report.builds, "lookup_hits":report.hits,
                                "lookup_skipped_cap":report.skipped_cap,
                            })
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn range_probe_fixtures_validate() {
    direct(false, 1);
    engine(false, 1);
}

#[test]
#[ignore = "exclusive release measurement window required"]
fn range_release_probe() {
    let samples = std::env::var("FZ_RANGES_SAMPLES")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(7);
    let family = std::env::var("FZ_RANGES_FAMILY").unwrap_or_default();
    if family.is_empty() || family == "direct" {
        direct(true, samples);
    }
    if family.is_empty() || family == "engine" {
        engine(true, samples);
    }
}
