//! Program 1 must-fix 3: split the authority's full-build cost into
//! extraction and store build on one workbook (release, feature build).
//! `program1-buildprof --xlsx PATH [--reps N]`

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    use formualizer_workbook::{
        CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
    };
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str| {
        args.iter()
            .position(|a| a == k)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let path = get("--xlsx").expect("--xlsx");
    let reps: usize = get("--reps").and_then(|s| s.parse().ok()).unwrap_or(3);
    let backend = CalamineAdapter::open_path(&path)?;
    let mut wb =
        Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())?;
    let _ = formualizer_eval::engine::authority::probe::sync(wb.engine_mut());
    for _ in 0..reps {
        let (n, x, b) = formualizer_eval::engine::authority::probe::build_split(wb.engine());
        println!(
            "inputs={n} extract_ms={:.1} build_ms={:.1} extract_us_per={:.2} build_us_per={:.2}",
            x as f64 / 1e6,
            b as f64 / 1e6,
            x as f64 / 1e3 / n.max(1) as f64,
            b as f64 / 1e3 / n.max(1) as f64
        );
    }
    Ok(())
}

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("build with --features formualizer_runner");
}
