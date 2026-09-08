#![cfg(feature = "umya3")]
use formualizer_workbook::backends::umya3::{read_document, read_document_path};
use umya_spreadsheet3::{Border, Color, Workbook};

fn bytes(book: &Workbook) -> Vec<u8> {
    let mut result = Vec::new();
    umya_spreadsheet3::writer::xlsx::write_writer(book, &mut result).unwrap();
    result
}

#[test]
fn stock_loss_is_repaired_and_export_reopens_with_colour() {
    let mut book = umya_spreadsheet3::new_file();
    let sheet = book.sheet_by_name_mut("Sheet1").unwrap();
    let cell = sheet.cell_mut("A1");
    cell.set_formula("1+1").set_formula_result_number(2.0);
    cell.style_mut().font_mut().set_bold(true);
    let border = cell.style_mut().borders_mut().left_mut();
    border.set_border_style(Border::BORDER_THIN);
    border.set_color(Color::default().set_argb_str("FFC00000").clone());
    let source = bytes(&book);
    let stock =
        umya_spreadsheet3::reader::xlsx::read_reader(std::io::Cursor::new(&source), true).unwrap();
    assert!(
        stock
            .sheet(0)
            .unwrap()
            .cell("A1")
            .unwrap()
            .style()
            .borders()
            .unwrap()
            .left()
            .color()
            .is_none()
    );
    let repaired = read_document(&source).unwrap();
    let reopened =
        read_document(&formualizer_workbook::backends::umya3::write_document(&repaired).unwrap())
            .unwrap();
    for book in [&repaired, &reopened] {
        let cell = book.sheet(0).unwrap().cell("A1").unwrap();
        assert_eq!(
            cell.style()
                .borders()
                .unwrap()
                .left()
                .color()
                .unwrap()
                .argb_str(),
            "FFC00000"
        );
        assert_eq!(cell.formula(), "1+1");
        assert_eq!(cell.value_number(), Some(2.0));
        assert!(cell.style().font().unwrap().bold());
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.xlsx");
    std::fs::write(&path, &source).unwrap();
    assert_eq!(bytes(&read_document_path(&path).unwrap()), bytes(&repaired));
}

#[test]
fn rejects_non_zip_input() {
    assert!(read_document(b"not a workbook").is_err());
}

fn rewrite(source: &[u8], mut transform: impl FnMut(&str, String) -> String) -> Vec<u8> {
    use std::io::{Cursor, Read, Write};
    let mut input = zip::ZipArchive::new(Cursor::new(source)).unwrap();
    let mut output = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..input.len() {
        let mut entry = input.by_index(i).unwrap();
        let mut data = String::new();
        entry.read_to_string(&mut data).unwrap();
        output
            .start_file(entry.name(), zip::write::SimpleFileOptions::default())
            .unwrap();
        output
            .write_all(transform(entry.name(), data).as_bytes())
            .unwrap();
    }
    output.finish().unwrap().into_inner()
}

fn red_style() -> umya_spreadsheet3::Style {
    let mut style = umya_spreadsheet3::Style::default();
    let borders = style.borders_mut();
    borders.set_diagonal_up(true);
    borders.left_mut().set_border_style(Border::BORDER_THIN);
    borders
        .left_mut()
        .set_color(Color::default().set_argb_str("FFC00000").clone());
    for name in [
        "right",
        "top",
        "bottom",
        "diagonal",
        "vertical",
        "horizontal",
    ] {
        let border = match name {
            "right" => borders.right_mut(),
            "top" => borders.top_mut(),
            "bottom" => borders.bottom_mut(),
            "diagonal" => borders.diagonal_mut(),
            "vertical" => borders.vertical_mut(),
            _ => borders.horizontal_mut(),
        };
        border.set_border_style(Border::BORDER_THIN);
        border.set_color(Color::default().set_argb_str("FFC00000").clone());
    }
    style
}

#[test]
fn restores_all_sides_theme_indexed_and_tint_without_materializing_dimensions() {
    for attributes in [
        "rgb=\"00C00000\"",
        "theme=\"4\" tint=\"0.25\"",
        "indexed=\"10\" tint=\"-0.5\"",
    ] {
        let mut book = umya_spreadsheet3::new_file();
        book.sheet_mut(0)
            .unwrap()
            .cell_mut("C3")
            .set_style(red_style());
        let source = rewrite(&bytes(&book), |name, text| {
            if name == "xl/styles.xml" {
                text.replace("rgb=\"FFC00000\"", attributes)
            } else {
                text
            }
        });
        let stock =
            umya_spreadsheet3::reader::xlsx::read_reader(std::io::Cursor::new(&source), true)
                .unwrap();
        let repaired = read_document(&source).unwrap();
        assert_eq!(
            stock.sheet(0).unwrap().column_dimensions().len(),
            repaired.sheet(0).unwrap().column_dimensions().len()
        );
        let serialized = formualizer_workbook::backends::umya3::write_document(&repaired).unwrap();
        for book in [&repaired, &read_document(&serialized).unwrap()] {
            let borders = book
                .sheet(0)
                .unwrap()
                .cell("C3")
                .unwrap()
                .style()
                .borders()
                .unwrap();
            assert!(borders.diagonal_up());
            for (side_index, side) in [
                borders.left(),
                borders.right(),
                borders.top(),
                borders.bottom(),
                borders.diagonal(),
                borders.vertical(),
                borders.horizontal(),
            ]
            .into_iter()
            .enumerate()
            {
                let colour = side
                    .color()
                    .unwrap_or_else(|| panic!("missing side {side_index} for {attributes}"));
                if attributes.starts_with("rgb") {
                    assert_eq!(colour.argb_str(), "00C00000");
                } else if attributes.starts_with("theme") {
                    assert_eq!(colour.theme_index(), 4);
                    assert_eq!(
                        colour.tint(),
                        0.25,
                        "theme and tint identities survive export"
                    );
                } else {
                    assert_eq!(colour.indexed(), 10);
                    assert_eq!(colour.tint(), -0.5);
                }
            }
        }
    }
}

#[test]
fn restores_row_column_and_differential_styles_on_named_sheets() {
    use umya_spreadsheet3::{
        ConditionalFormatValues, ConditionalFormatting, ConditionalFormattingRule, Formula,
    };
    let mut book = umya_spreadsheet3::new_file();
    book.new_sheet("Other & quoted ' name").unwrap();
    let sheet = book.sheet_by_name_mut("Other & quoted ' name").unwrap();
    sheet
        .row_dimension_mut(7)
        .set_height(31.0)
        .set_style(red_style());
    sheet
        .column_dimension_by_number_mut(3)
        .set_width(23.0)
        .set_style(red_style());
    let mut rule = ConditionalFormattingRule::default();
    rule.set_type(ConditionalFormatValues::Expression)
        .set_priority(1)
        .set_style(red_style());
    let mut formula = Formula::default();
    formula.set_string_value("TRUE()");
    rule.set_formula(formula);
    let mut cf = ConditionalFormatting::default();
    cf.sequence_of_references_mut().set_sqref("A1:B5");
    cf.add_conditional_collection(rule);
    sheet.add_conditional_formatting_collection(cf);
    let source = bytes(&book);
    let imported = read_document(&source).unwrap();
    for book in [
        &imported,
        &read_document(&formualizer_workbook::backends::umya3::write_document(&imported).unwrap())
            .unwrap(),
    ] {
        let sheet = book.sheet_by_name("Other & quoted ' name").unwrap();
        let row = sheet.row_dimensions_to_hashmap().get(&7).unwrap();
        assert_eq!(row.height(), 31.0);
        let column = sheet
            .column_dimensions()
            .iter()
            .find(|c| c.col_num() == 3)
            .unwrap();
        assert_eq!(column.width(), 23.0);
        let rule = &sheet.conditional_formatting_collection()[0].conditional_collection()[0];
        for style in [row.style(), column.style(), rule.style().unwrap()] {
            assert_eq!(
                style.borders().unwrap().left().color().unwrap().argb_str(),
                "FFC00000"
            );
        }
    }
}

#[test]
fn invalid_style_colour_relationship_and_xml_are_errors() {
    let mut book = umya_spreadsheet3::new_file();
    book.sheet_mut(0)
        .unwrap()
        .cell_mut("A1")
        .set_style(red_style());
    let source = bytes(&book);
    for bad in [
        "rgb=\"broken\"",
        "theme=\"nope\"",
        "rgb=\"FFC00000\" tint=\"NaN\"",
    ] {
        let bytes = rewrite(&source, |name, text| {
            if name == "xl/styles.xml" {
                text.replace("rgb=\"FFC00000\"", bad)
            } else {
                text
            }
        });
        assert!(read_document(&bytes).is_err(), "{bad}");
    }
    let bad = rewrite(&source, |name, text| {
        if name == "xl/styles.xml" {
            text.replace("borderId=\"1\"", "borderId=\"99999\"")
        } else {
            text
        }
    });
    assert!(read_document(&bad).is_err());
    let bad = rewrite(&source, |name, text| {
        if name == "xl/styles.xml" {
            text.replace("<styleSheet", "<!DOCTYPE evil><styleSheet")
        } else {
            text
        }
    });
    assert!(read_document(&bad).is_err());
    let bad = rewrite(&source, |name, text| {
        if name == "_rels/.rels" {
            text.replace("xl/workbook.xml", "../../outside.xml")
        } else {
            text
        }
    });
    assert!(read_document(&bad).is_err());
}
