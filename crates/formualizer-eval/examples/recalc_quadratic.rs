//! Pure-Rust harness reproducing the O(N^2) recalc scaling reported for
//! `formualizer::workbook::recalculate_file` on row-local trivial formulas.
//!
//! Mimics the xlsx-fixture used by the original Python repro:
//!   cols A..E hold integer values, col F = SUM(A_R:E_R),
//!   col G = A_R*B_R+C_R-D_R.
//!
//! We skip xlsx I/O and drive the Engine directly via `begin_bulk_ingest_arrow()`
//! (base values) and `begin_bulk_ingest()` (formulas). That isolates the eval
//! hotspot from umya parse/write.
//!
//! Run with:
//!   cargo run --release -p formualizer-eval --example recalc_quadratic -- 1000 2000 5000 10000 20000
//!
//! Optional env knobs:
//!   FZ_EXPAND_RANGES=1    mirror the xlsx loader which sets range_expansion_limit=0
//!                         (default in this harness — matches recalculate_file).
//!   FZ_SKIP_RANGE_FORMULA=1  skip the SUM(A:E) formulas to test the hypothesis
//!                         that the O(N^2) cost is dominated by the range formula.

use std::time::Instant;

use formualizer_common::LiteralValue;
use formualizer_eval::engine::{Engine, EvalConfig, SheetIndexMode};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse as parse_formula;

fn build_engine(rows: u32, skip_range_formula: bool) -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default();
    let mut engine: Engine<TestWorkbook> = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();

    // Match the xlsx loader path in `UmyaAdapter::stream_into_engine`.
    engine.set_sheet_index_mode(SheetIndexMode::Lazy);
    engine.config.range_expansion_limit = 0;

    // 1) Bulk-ingest base values for columns A..E.
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Data", 7, 32 * 1024);
        for r in 0..rows {
            let base = r as f64;
            let row_vals = [
                LiteralValue::Number(base + 1.0),
                LiteralValue::Number(base + 2.0),
                LiteralValue::Number(base + 3.0),
                LiteralValue::Number(base + 4.0),
                LiteralValue::Number(base + 5.0),
                LiteralValue::Empty,
                LiteralValue::Empty,
            ];
            ab.append_row("Data", &row_vals).unwrap();
        }
        ab.finish().unwrap();
    }

    // 2) Stage formulas in row-major order.
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Data");
    let mut batch = Vec::with_capacity(rows as usize * 2);
    for r0 in 0..rows {
        let r = r0 + 1;
        if !skip_range_formula {
            let sum_text = format!("=SUM(A{r}:E{r})");
            batch.push((r, 6, parse_formula(&sum_text).unwrap()));
        }
        let arith_text = format!("=A{r}*B{r}+C{r}-D{r}");
        batch.push((r, 7, parse_formula(&arith_text).unwrap()));
    }
    builder.add_formulas(sheet, batch);
    builder.finish().unwrap();

    engine
}

fn main() {
    let ks: Vec<u32> = std::env::args()
        .skip(1)
        .map(|s| s.parse::<u32>().expect("row count arg"))
        .collect();
    let ks = if ks.is_empty() {
        vec![1000, 2000, 5000, 10000, 20000]
    } else {
        ks
    };

    let skip_range = std::env::var("FZ_SKIP_RANGE_FORMULA").ok().as_deref() == Some("1");
    if skip_range {
        eprintln!(
            "[harness] FZ_SKIP_RANGE_FORMULA=1 → skipping =SUM(A_R:E_R); only arithmetic formula kept"
        );
    }

    println!(
        "{:>8} {:>12} {:>14} {:>14} {:>14}",
        "rows", "build_ms", "eval_ms", "us_per_formula", "formulas"
    );
    for &k in &ks {
        let t_build = Instant::now();
        let mut engine = build_engine(k, skip_range);
        let build_ms = t_build.elapsed().as_secs_f64() * 1000.0;

        let t_eval = Instant::now();
        let (res, _delta) = engine.evaluate_all_with_delta().expect("evaluate_all");
        let eval_ms = t_eval.elapsed().as_secs_f64() * 1000.0;
        let n_formulas = res.computed_vertices.max(1) as f64;
        let us_per_formula = (eval_ms * 1000.0) / n_formulas;

        println!(
            "{:>8} {:>12.1} {:>14.1} {:>14.2} {:>14}",
            k, build_ms, eval_ms, us_per_formula, res.computed_vertices,
        );
    }
}
