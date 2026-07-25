//! Measures layout preparation cost against the sheet's used-range extent.
//!
//! The layout preparation envelope is derived from `sheet_dimensions()`. This
//! probe answers whether that cost tracks the *extent* of the envelope or the
//! *populated cells* inside it, by holding the table size constant and pushing a
//! single far-away cell progressively further down the sheet.
use anyhow::Result;
use formualizer_sheetport::{EvalOptions, SheetPort};
use formualizer_testkit::write_workbook;
use formualizer_workbook::backends::CalamineAdapter;
use formualizer_workbook::traits::SpreadsheetReader;
use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};
use sheetport_spec::Manifest;
use std::time::Instant;

const MANIFEST: &str = r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: envelope-probe, name: Envelope Probe }
ports:
  - id: rows
    dir: out
    shape: table
    location:
      layout:
        sheet: Layout
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
    schema:
      kind: table
      columns:
        - { name: Label, type: string, col: A }
        - { name: Value, type: number, col: B }
"#;

fn main() -> Result<()> {
    let data_rows = 100u32;
    println!(
        "{:>12} {:>14} {:>12}",
        "far_row", "used_extent", "elapsed_ms"
    );
    for far_row in [200u32, 10_000, 100_000, 500_000, 1_000_000] {
        let dir = std::env::temp_dir().join(format!("fz-envelope-{far_row}"));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("envelope.xlsx");
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("sheet");
            sheet.set_name("Layout");
            let layout = book.get_sheet_by_name_mut("Layout").expect("Layout");
            layout.get_cell_mut((1u32, 1u32)).set_value("Label");
            layout.get_cell_mut((2u32, 1u32)).set_value("Value");
            for row in 2..=(data_rows + 1) {
                layout
                    .get_cell_mut((1u32, row))
                    .set_value(format!("r{row}"));
                layout
                    .get_cell_mut((2u32, row))
                    .set_formula(format!("={row}*2"));
            }
            // One populated cell far below the table inflates the used range
            // without adding work that belongs to the resolved output.
            layout.get_cell_mut((2u32, far_row)).set_value_number(1.0);
        });

        let reader = CalamineAdapter::open_path(&path)?;
        let mut workbook = Workbook::from_reader(
            reader,
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )?;
        let manifest = Manifest::from_yaml_str(MANIFEST)?;
        let started = Instant::now();
        let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
        // evaluate_once is the path that builds the preparation envelope.
        let outputs = sheetport.evaluate_once(EvalOptions::default())?;
        let elapsed = started.elapsed();
        assert!(!outputs.0.is_empty(), "probe must resolve the layout port");
        println!(
            "{far_row:>12} {:>14} {:>12.1}",
            far_row,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    // A contiguous table longer than the old 100_000-row default cap. The old
    // envelope stopped at the cap and the scan returned LayoutExhausted; the
    // derived bound resolves it.
    println!();
    println!(
        "{:>12} {:>14} {:>12}",
        "table_rows", "resolved_rows", "elapsed_ms"
    );
    for table_rows in [50_000u32, 150_000] {
        let dir = std::env::temp_dir().join(format!("fz-envelope-long-{table_rows}"));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("envelope.xlsx");
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("sheet");
            sheet.set_name("Layout");
            let layout = book.get_sheet_by_name_mut("Layout").expect("Layout");
            layout.get_cell_mut((1u32, 1u32)).set_value("Label");
            layout.get_cell_mut((2u32, 1u32)).set_value("Value");
            for row in 2..=(table_rows + 1) {
                layout
                    .get_cell_mut((1u32, row))
                    .set_value(format!("r{row}"));
                layout
                    .get_cell_mut((2u32, row))
                    .set_value_number(row as f64);
            }
        });
        let reader = CalamineAdapter::open_path(&path)?;
        let mut workbook = Workbook::from_reader(
            reader,
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )?;
        let manifest = Manifest::from_yaml_str(MANIFEST)?;
        let started = Instant::now();
        let mut sheetport = SheetPort::new(&mut workbook, manifest)?;
        let outcome = sheetport.evaluate_once(EvalOptions::default());
        let elapsed = started.elapsed();
        match outcome {
            Ok(_) => println!(
                "{table_rows:>12} {:>14} {:>12.1}",
                table_rows,
                elapsed.as_secs_f64() * 1000.0
            ),
            Err(err) => println!("{table_rows:>12} {:>14} {err}", "ERROR"),
        }
    }
    Ok(())
}
