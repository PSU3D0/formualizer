use anyhow::Result;
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

fn main() -> Result<()> {
    let path = "benchmarks/corpus/synthetic/repro_chain3.xlsx";
    let backend = UmyaAdapter::open_path(path)?;
    let mut wb =
        Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())?;
    wb.evaluate_all()?;
    let a1 = wb.evaluate_cell("Sheet1", 1, 1)?;
    let a2 = wb.evaluate_cell("Sheet1", 2, 1)?;
    let a3 = wb.evaluate_cell("Sheet1", 3, 1)?;
    println!("formualizer: A1={a1:?} A2={a2:?} A3={a3:?}");
    Ok(())
}
