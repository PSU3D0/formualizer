//! Repro for SUMIFS family variants. Surfaces:
//! - s013-shape (constant literal): broadcast works.
//! - s014-shape (varying literal): no fold (different canonical_hash per placement).
//! - relative-criterion: promotes but per-placement eval is slower than legacy.
//! - whole-col + relative: same regression amplified.
//! - whole-col + constant: broadcast works (after whole-axis promotion fix).
//!
//! Tests both `enable_parallel: true` (default; multi-threaded legacy)
//! and `enable_parallel: false` (single-threaded; matches WASM and constrained
//! environments). The serial-vs-parallel comparison isolates the inherent
//! per-placement cost from the parallel-vs-serial gap.

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("Need formualizer_runner feature");
}

#[cfg(feature = "formualizer_runner")]
fn main() {
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_workbook::{Workbook, WorkbookConfig};
    use std::time::Instant;

    // Reduced to 5k for faster A/B comparison across all 4 mode/parallel combos.
    // The legacy/Off path scales O(N·R) so 10k×10k = 100M cell reads per recalc;
    // 5k×5k = 25M which is enough to surface the regression at ~1/4 the wall time.
    const ROWS: u32 = 5_000;

    fn build(
        formula_template: impl Fn(u32) -> String,
        mode: FormulaPlaneMode,
        enable_parallel: bool,
    ) -> Workbook {
        let mut config = WorkbookConfig::interactive();
        config.eval.formula_plane_mode = mode;
        config.eval.enable_parallel = enable_parallel;
        let mut wb = Workbook::new_with_config(config);
        wb.add_sheet("Data").unwrap();
        for r in 1u32..=ROWS {
            let category = match r % 3 {
                0 => "Type1",
                1 => "Type2",
                _ => "Type3",
            };
            wb.set_value("Data", r, 1, LiteralValue::Text(category.to_string()))
                .unwrap();
            wb.set_value("Data", r, 2, LiteralValue::Number(r as f64))
                .unwrap();
        }
        for r in 1u32..=ROWS {
            wb.set_formula("Sheet1", r, 1, &formula_template(r))
                .unwrap();
            let key = match r % 3 {
                0 => "Type1",
                1 => "Type2",
                _ => "Type3",
            };
            wb.set_value("Sheet1", r, 2, LiteralValue::Text(key.to_string()))
                .unwrap();
        }
        wb
    }

    fn time_run(
        label: &str,
        mode: FormulaPlaneMode,
        enable_parallel: bool,
        builder: impl Fn(u32) -> String,
    ) {
        let mut wb = build(builder, mode, enable_parallel);
        let t0 = Instant::now();
        wb.evaluate_all().unwrap();
        let first = t0.elapsed();
        wb.set_value("Data", ROWS / 2, 2, LiteralValue::Number(999_999.0))
            .unwrap();
        let t1 = Instant::now();
        wb.evaluate_all().unwrap();
        let recalc = t1.elapsed();
        let sample = wb.get_value("Sheet1", 1, 1);
        let spans = wb.engine().baseline_stats().formula_plane_active_span_count;
        let par_label = if enable_parallel { "par" } else { "ser" };
        let mode_label = match mode {
            FormulaPlaneMode::Off => "Off",
            FormulaPlaneMode::AuthoritativeExperimental => "Auth",
            _ => "Other",
        };
        println!(
            "{:60} {:>4} {:>3}  first={:>9.2}ms  recalc={:>9.2}ms  spans={}  A1={:?}",
            label,
            mode_label,
            par_label,
            first.as_secs_f64() * 1000.0,
            recalc.as_secs_f64() * 1000.0,
            spans,
            sample
        );
    }

    fn run_all<F: Fn(u32) -> String + Copy>(label: &str, builder: F) {
        for &enable_parallel in &[true, false] {
            time_run(label, FormulaPlaneMode::Off, enable_parallel, builder);
            time_run(
                label,
                FormulaPlaneMode::AuthoritativeExperimental,
                enable_parallel,
                builder,
            );
        }
    }

    println!("ROWS = {ROWS}");
    println!("Format: <label> <mode> <par|ser>  first=<ms>  recalc=<ms>  spans=<n>  A1=<value>");
    println!();

    println!("--- Variant 1: SUMIFS constant criterion ---");
    let v1 = |_r: u32| format!("=SUMIFS(Data!$B$1:$B${ROWS}, Data!$A$1:$A${ROWS}, \"Type1\")");
    run_all("=SUMIFS(...,\"Type1\")  constant literal", v1);
    println!();

    println!("--- Variant 2: SUMIFS varying literal criterion (s014) ---");
    let v2 = |r: u32| {
        let cat = match r % 3 {
            0 => "Type1",
            1 => "Type2",
            _ => "Type3",
        };
        format!("=SUMIFS(Data!$B$1:$B${ROWS}, Data!$A$1:$A${ROWS}, \"{cat}\")")
    };
    run_all("=SUMIFS(...,\"<literal>\")  varying literal", v2);
    println!();

    println!("--- Variant 3: SUMIFS relative cell-ref criterion ---");
    let v3 = |r: u32| format!("=SUMIFS(Data!$B$1:$B${ROWS}, Data!$A$1:$A${ROWS}, B{r})");
    run_all("=SUMIFS(...,B{r})  relative cell-ref", v3);
    println!();

    println!("--- Variant 4: SUMIFS whole-col + relative criterion ---");
    let v4 = |r: u32| format!("=SUMIFS(Data!$B:$B, Data!$A:$A, B{r})");
    run_all("=SUMIFS($B:$B, $A:$A, B{r})  whole-col + relative", v4);
    println!();

    println!("--- Variant 5: SUMIFS whole-col + constant literal ---");
    let v5 = |_r: u32| "=SUMIFS(Data!$B:$B, Data!$A:$A, \"Type1\")".to_string();
    run_all("=SUMIFS($B:$B, $A:$A, \"Type1\")  whole-col + constant", v5);
}
