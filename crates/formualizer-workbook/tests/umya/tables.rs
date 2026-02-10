// Integration test for native Excel tables; run with `--features umya`.

use crate::common::build_workbook;
use formualizer_common::LiteralValue;
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

fn rewrite_header_row_count(path: &std::path::Path, table_name: &str, header_row_count: u32) {
    use std::fs::File;
    use std::io::{Read, Write};

    let in_file = File::open(path).expect("open input xlsx");
    let mut zin = zip::ZipArchive::new(in_file).expect("zip open");

    let out_path = path.with_file_name("fixture-headerless.xlsx");
    let out_file = File::create(&out_path).expect("create output xlsx");
    let mut zout = zip::ZipWriter::new(out_file);
    let options = zip::write::FileOptions::default();

    for i in 0..zin.len() {
        let mut f = zin.by_index(i).expect("zip entry");
        let name = f.name().to_string();

        let mut data = Vec::new();
        f.read_to_end(&mut data).expect("read entry");

        // Only rewrite table xml matching the given table name.
        if name.starts_with("xl/tables/")
            && name.ends_with(".xml")
            && let Ok(mut xml) = String::from_utf8(std::mem::take(&mut data))
        {
            if let Some(start) = xml.find("<table")
                && let Some(end) = xml[start..].find('>')
            {
                let end = start + end;
                let tag = &xml[start..end];
                if tag.contains(&format!("name=\"{table_name}\""))
                    || tag.contains(&format!("displayName=\"{table_name}\""))
                {
                    let mut new_tag = tag.to_string();
                    if let Some(pos) = new_tag.find("headerRowCount=\"") {
                        let after = pos + "headerRowCount=\"".len();
                        if let Some(qend) = new_tag[after..].find('"') {
                            new_tag
                                .replace_range(after..after + qend, &header_row_count.to_string());
                        }
                    } else {
                        new_tag.push_str(&format!(" headerRowCount=\"{header_row_count}\""));
                    }
                    xml.replace_range(start..end, &new_tag);
                }
            }
            data = xml.into_bytes();
        }

        zout.start_file(name, options).expect("zip write start");
        zout.write_all(&data).expect("zip write");
    }

    zout.finish().expect("zip finish");

    // Replace original file in-place for the rest of the test.
    std::fs::copy(&out_path, path).expect("overwrite fixture");
}

#[test]
fn umya_loads_native_table_metadata_and_eval_structured_ref() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // Table region A1:B3: headers + 2 data rows.
        sh.get_cell_mut((1, 1)).set_value("Region");
        sh.get_cell_mut((2, 1)).set_value("Amount");
        sh.get_cell_mut((1, 2)).set_value("N");
        sh.get_cell_mut((2, 2)).set_value_number(10);
        sh.get_cell_mut((1, 3)).set_value("S");
        sh.get_cell_mut((2, 3)).set_value_number(20);

        // Formula in D1: SUM over the Amount column.
        sh.get_cell_mut((4, 1)).set_formula("SUM(Sales[Amount])");

        let mut table = umya_spreadsheet::structs::Table::new("Sales", ("A1", "B3"));
        table.add_column(umya_spreadsheet::structs::TableColumn::new("Region"));
        table.add_column(umya_spreadsheet::structs::TableColumn::new("Amount"));
        sh.add_table(table);
    });

    let backend = UmyaAdapter::open_path(&path).expect("open workbook");
    let mut wb = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load into engine workbook");

    let v = wb.evaluate_cell("Sheet1", 1, 4).unwrap();
    assert_eq!(v, LiteralValue::Number(30.0));
}

#[test]
fn umya_respects_header_row_count_from_table_xml_when_headerless() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // Headerless table region A1:B2: data rows only.
        sh.get_cell_mut((1, 1)).set_value("N");
        sh.get_cell_mut((2, 1)).set_value_number(10);
        sh.get_cell_mut((1, 2)).set_value("S");
        sh.get_cell_mut((2, 2)).set_value_number(20);

        // Formula in D1: SUM over the Amount column.
        sh.get_cell_mut((4, 1)).set_formula("SUM(Sales[Amount])");

        let mut table = umya_spreadsheet::structs::Table::new("Sales", ("A1", "B2"));
        table.add_column(umya_spreadsheet::structs::TableColumn::new("Region"));
        table.add_column(umya_spreadsheet::structs::TableColumn::new("Amount"));
        sh.add_table(table);
    });

    // Umya doesn't currently expose headerRowCount; patch the generated xlsx.
    rewrite_header_row_count(&path, "Sales", 0);

    let backend = UmyaAdapter::open_path(&path).expect("open workbook");
    let mut wb = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load into engine workbook");

    let v = wb.evaluate_cell("Sheet1", 1, 4).unwrap();
    assert_eq!(v, LiteralValue::Number(30.0));
}
