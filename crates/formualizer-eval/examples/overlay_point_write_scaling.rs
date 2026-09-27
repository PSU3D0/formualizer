//! Scaling probe: single-cell recalc writes into a column whose first eval
//! wrote a computed-overlay fragment.
//!
//! Column A holds values, column B = A*2 (one family). First eval commits B
//! as run fragments. Then each round edits `k` scattered A cells and
//! recalculates, so each recalc writes `k` isolated B cells into the covering
//! fragment. Per-write cost must not grow with the fragment length.
//!
//! Run with:
//!   cargo run --release -p formualizer-eval --example overlay_point_write_scaling -- 1000 8000 32000 128000

use std::time::Instant;

use formualizer_common::LiteralValue;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse as parse_formula;

fn build(rows: u32) -> Engine<TestWorkbook> {
    let mut engine: Engine<TestWorkbook> =
        Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 2, 32 * 1024);
        for r in 0..rows {
            ab.append_row("S", &[LiteralValue::Number(r as f64), LiteralValue::Empty])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("S");
    let batch: Vec<_> = (1..=rows)
        .map(|r| (r, 2, parse_formula(format!("=A{r}*2")).unwrap()))
        .collect();
    builder.add_formulas(sheet, batch);
    builder.finish().unwrap();
    engine
}

fn main() {
    let sizes: Vec<u32> = std::env::args()
        .skip(1)
        .map(|s| s.parse().expect("row count"))
        .collect();
    let sizes = if sizes.is_empty() {
        vec![1000, 8000, 32000, 128000]
    } else {
        sizes
    };
    let rounds: u32 = std::env::var("ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let per_round: u32 = std::env::var("PER_ROUND")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    println!(
        "{:>8} {:>10} {:>8} {:>12} {:>14}  us_per_write by quarter",
        "rows", "first_ms", "writes", "recalc_ms", "us_per_write"
    );
    for &n in &sizes {
        let mut engine = build(n);
        let t = Instant::now();
        engine.evaluate_all().unwrap();
        let first_ms = t.elapsed().as_secs_f64() * 1e3;

        // Deterministic scatter over the whole column, stepping backwards so
        // each write lands inside the (shrinking) left piece of a split.
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut total = 0.0f64;
        let mut writes = 0u64;
        // Per-quarter recalc time: a session must not slow down as edits
        // accumulate (fragment count, point map).
        let mut quarters = [0.0f64; 4];
        for round in 0..rounds {
            for _ in 0..per_round {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let row = 1 + (state % n as u64) as u32;
                engine
                    .set_cell_value("S", row, 1, LiteralValue::Number(round as f64 + 0.5))
                    .unwrap();
                writes += 1;
            }
            let t = Instant::now();
            engine.evaluate_all().unwrap();
            let dt = t.elapsed().as_secs_f64();
            total += dt;
            quarters[(round * 4 / rounds) as usize] += dt;
        }
        let check = engine.get_cell_value("S", n, 2);
        assert!(matches!(check, Some(LiteralValue::Number(_))));
        let per_quarter = writes as f64 / 4.0;
        println!(
            "{:>8} {:>10.2} {:>8} {:>12.2} {:>14.2}  {:.2?}",
            n,
            first_ms,
            writes,
            total * 1e3,
            total * 1e6 / writes as f64,
            quarters.map(|q| q * 1e6 / per_quarter)
        );
    }
}
