use anyhow::{Result, anyhow};
use ironcalc::import::load_from_xlsx;

fn main() -> Result<()> {
    let path = "benchmarks/corpus/synthetic/repro_chain3.xlsx";
    let mut model = load_from_xlsx(path, "en", "UTC", "en")?;
    model.evaluate();
    let a1 = model
        .get_formatted_cell_value(0, 1, 1)
        .map_err(|e| anyhow!(e))?;
    let a2 = model
        .get_formatted_cell_value(0, 2, 1)
        .map_err(|e| anyhow!(e))?;
    let a3 = model
        .get_formatted_cell_value(0, 3, 1)
        .map_err(|e| anyhow!(e))?;
    println!("ironcalc: A1={a1} A2={a2} A3={a3}");
    Ok(())
}
