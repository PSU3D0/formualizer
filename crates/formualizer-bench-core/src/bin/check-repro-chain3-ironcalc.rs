#[cfg(not(feature = "ironcalc_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `ironcalc_runner`: cargo run -p formualizer-bench-core --features ironcalc_runner --bin check-repro-chain3-ironcalc -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "ironcalc_runner")]
fn main() -> anyhow::Result<()> {
    use ironcalc::import::load_from_xlsx;

    let path = "benchmarks/corpus/synthetic/repro_chain3.xlsx";
    let mut model = load_from_xlsx(path, "en", "UTC", "en")?;
    model.evaluate();
    let a1 = model
        .get_formatted_cell_value(0, 1, 1)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let a2 = model
        .get_formatted_cell_value(0, 2, 1)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let a3 = model
        .get_formatted_cell_value(0, 3, 1)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("ironcalc: A1={a1} A2={a2} A3={a3}");
    Ok(())
}
