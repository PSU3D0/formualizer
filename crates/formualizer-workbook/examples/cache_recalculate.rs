//! Run with `--features xlsx-recalc --example cache_recalculate -- input.xlsx output.xlsx`.
//! The explicit destination prevents accidentally modifying the input workbook.
#[cfg(all(feature = "xlsx-recalc", not(target_arch = "wasm32")))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_file};
    use std::path::PathBuf;
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: cache_recalculate INPUT.xlsx OUTPUT.xlsx".into());
    }
    let input = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[1]);
    if input == output
        || (output.exists() && std::fs::canonicalize(&input)? == std::fs::canonicalize(&output)?)
    {
        return Err("the example requires a distinct output path".into());
    }
    let result = recalculate_xlsx_file(&input, Some(&output), XlsxRecalculateOptions::default())?;
    println!(
        "formulas={} evaluated={} errors={} changed={} parts={} output_bytes={}",
        result.formula_cells,
        result.summary.evaluated,
        result.summary.errors,
        result.cache_cells_changed,
        result.worksheet_parts_changed,
        result.bytes.len()
    );
    Ok(())
}
#[cfg(not(all(feature = "xlsx-recalc", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("this example requires native xlsx-recalc support");
}
