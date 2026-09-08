#![cfg(feature = "umya3")]
use formualizer_workbook::backends::umya3::{read_document, write_document};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use umya_spreadsheet3::{Color, Style};

type Attrs = BTreeMap<String, String>;
fn part(bytes: &[u8], name: &str) -> String {
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut xml = String::new();
    zip.by_name(name).unwrap().read_to_string(&mut xml).unwrap();
    xml
}
fn records(xml: &str) -> Vec<(Vec<String>, Attrs)> {
    use quick_xml::{Reader, events::Event};
    let mut reader = Reader::from_str(xml);
    let mut path = Vec::new();
    let mut result = Vec::new();
    loop {
        let event = reader.read_event().unwrap();
        match &event {
            Event::Start(e) | Event::Empty(e) => {
                path.push(String::from_utf8(e.local_name().as_ref().to_vec()).unwrap());
                let attrs = e
                    .attributes()
                    .map(|a| {
                        let a = a.unwrap();
                        (
                            String::from_utf8(a.key.as_ref().to_vec()).unwrap(),
                            a.decode_and_unescape_value(reader.decoder())
                                .unwrap()
                                .into_owned(),
                        )
                    })
                    .collect();
                result.push((path.clone(), attrs));
                if matches!(event, Event::Empty(_)) {
                    path.pop();
                }
            }
            Event::End(_) => {
                path.pop();
            }
            Event::Eof => break,
            _ => {}
        }
    }
    result
}
fn raw_colours(bytes: &[u8], cell: &str, table: &str, element: &str, id_attr: &str) -> Vec<Attrs> {
    let sheet = records(&part(bytes, "xl/worksheets/sheet1.xml"));
    let xf: usize = sheet
        .iter()
        .find(|(p, a)| p.last().is_some_and(|p| p == "c") && a.get("r").is_some_and(|r| r == cell))
        .unwrap()
        .1
        .get("s")
        .map(|s| s.parse().unwrap())
        .unwrap_or(0);
    let styles = records(&part(bytes, "xl/styles.xml"));
    let id: usize = styles
        .iter()
        .filter(|(p, _)| p == &["styleSheet", "cellXfs", "xf"])
        .nth(xf)
        .unwrap()
        .1
        .get(id_attr)
        .unwrap()
        .parse()
        .unwrap();
    let mut index = None;
    let mut result = Vec::new();
    for (path, attrs) in styles {
        if path == ["styleSheet", table, element] {
            index = Some(index.map_or(0, |i| i + 1));
        }
        if index == Some(id)
            && path.starts_with(&["styleSheet".into(), table.into(), element.into()])
            && path
                .last()
                .is_some_and(|n| ["color", "fgColor", "bgColor"].contains(&n.as_str()))
        {
            result.push(attrs);
        }
    }
    result
}
fn replace_styles(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut out = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        if entry.name() == "xl/styles.xml" {
            data = String::from_utf8(data)
                .unwrap()
                .replace(from, to)
                .into_bytes();
        }
        out.start_file(entry.name(), zip::write::SimpleFileOptions::default())
            .unwrap();
        out.write_all(&data).unwrap();
    }
    out.finish().unwrap().into_inner()
}

#[test]
fn raw_xml_preserves_palette_rgb_and_tint_selectors_at_referenced_ids() {
    for selector in [
        "rgb=\"FF000000\"",
        "rgb=\"FFFFFFFF\"",
        "rgb=\"FFC00000\" tint=\"0.5\"",
        "theme=\"1\" tint=\"0.25\"",
        "indexed=\"1\" tint=\"-0.25\"",
    ] {
        let mut book = umya_spreadsheet3::new_file();
        book.sheet_mut(0)
            .unwrap()
            .cell_mut("A1")
            .set_style(style(true, 1));
        let mut source = Vec::new();
        umya_spreadsheet3::writer::xlsx::write_writer(&book, &mut source).unwrap();
        let source = replace_styles(&source, "theme=\"1\"", selector);
        let imported = read_document(&source).unwrap();
        let exported = write_document(&imported).unwrap();
        let expected = records(&format!("<color {selector}/>"))[0].1.clone();
        assert_eq!(
            raw_colours(&exported, "A1", "fonts", "font", "fontId"),
            vec![expected.clone()]
        );
        assert_eq!(
            raw_colours(&exported, "A1", "borders", "border", "borderId"),
            vec![expected]
        );
    }
}

#[test]
fn pattern_and_gradient_colours_preserve_raw_identity_and_default_height() {
    let mut book = umya_spreadsheet3::new_file();
    let sheet = book.sheet_mut(0).unwrap();
    *sheet.sheet_format_properties_mut() = Default::default();
    for (cell, theme) in [("A1", true), ("B1", false)] {
        let fill = sheet
            .cell_mut(cell)
            .style_mut()
            .fill_mut()
            .pattern_fill_mut();
        fill.set_pattern_type(umya_spreadsheet3::PatternValues::Solid);
        if theme {
            fill.foreground_color_mut().set_theme_index(1);
        } else {
            fill.foreground_color_mut().set_indexed(1);
        }
        fill.background_color_mut()
            .set_argb_str("FFC00000")
            .set_tint(0.5);
    }
    let gradient = sheet
        .cell_mut("C1")
        .style_mut()
        .fill_mut()
        .gradient_fill_mut();
    for (position, theme) in [(0.0, true), (1.0, false)] {
        let mut stop = umya_spreadsheet3::GradientStop::default();
        stop.set_position(position);
        if theme {
            stop.color_mut().set_theme_index(1);
        } else {
            stop.color_mut().set_indexed(1);
        }
        gradient.gradient_stop_mut().push(stop);
    }
    let output = write_document(&book).unwrap();
    for (cell, selector) in [("A1", "theme"), ("B1", "indexed")] {
        let colours = raw_colours(&output, cell, "fills", "fill", "fillId");
        assert_eq!(colours[0], BTreeMap::from([(selector.into(), "1".into())]));
        assert_eq!(
            colours[1],
            BTreeMap::from([
                ("rgb".into(), "FFC00000".into()),
                ("tint".into(), "0.5".into())
            ])
        );
    }
    assert_eq!(
        raw_colours(&output, "C1", "fills", "fill", "fillId"),
        vec![
            BTreeMap::from([("theme".into(), "1".into())]),
            BTreeMap::from([("indexed".into(), "1".into())])
        ]
    );
    let sheet = records(&part(&output, "xl/worksheets/sheet1.xml"));
    assert_eq!(
        sheet
            .iter()
            .find(|(p, _)| p.last().is_some_and(|p| p == "sheetFormatPr"))
            .unwrap()
            .1["defaultRowHeight"],
        "15"
    );
    let reopened = read_document(&output).unwrap();
    assert_eq!(
        raw_colours(
            &write_document(&reopened).unwrap(),
            "C1",
            "fills",
            "fill",
            "fillId"
        ),
        raw_colours(&output, "C1", "fills", "fill", "fillId")
    );
}

fn style(theme: bool, index: u32) -> Style {
    let mut style = Style::default();
    style.font_mut().set_bold(true);
    let mut colour = Color::default();
    if theme {
        colour.set_theme_index(index);
    } else {
        colour.set_indexed(index);
    }
    *style.font_mut().color_mut() = colour.clone();
    style.borders_mut().left_mut().set_border_style("thin");
    style.borders_mut().left_mut().set_color(colour);
    style
}

#[test]
fn adapter_cache_bridge_writes_typed_results_without_clearing_formulas() {
    use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
    use formualizer_workbook::backends::FormulaCacheUpdate;
    use formualizer_workbook::{SpreadsheetReader, SpreadsheetWriter, Umya3Adapter};
    let mut book = umya_spreadsheet3::new_file();
    for (row, formula) in [
        (1, "1+1"),
        (2, "TRUE()"),
        (3, "\"007\""),
        (4, "NA()"),
        (5, "\"\""),
    ] {
        book.sheet_mut(0)
            .unwrap()
            .cell_mut((1, row))
            .set_formula(formula);
    }
    let mut adapter = Umya3Adapter::open_bytes(write_document(&book).unwrap()).unwrap();
    let values = [
        LiteralValue::Number(2.0),
        LiteralValue::Boolean(true),
        LiteralValue::Text("007".into()),
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)),
        LiteralValue::Empty,
    ];
    let updates: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(i, value)| FormulaCacheUpdate {
            sheet: "Sheet1".into(),
            row: i as u32 + 1,
            col: 1,
            value,
        })
        .collect();
    adapter
        .write_formula_caches_batch(&updates, formualizer_eval::engine::DateSystem::Excel1900)
        .unwrap();
    let output = adapter.save_to_bytes().unwrap();
    let xml = part(&output, "xl/worksheets/sheet1.xml");
    let cells: Vec<_> = records(&xml)
        .into_iter()
        .filter(|(p, _)| p.last().is_some_and(|p| p == "c"))
        .collect();
    assert_eq!(cells[0].1.get("t").map(String::as_str).unwrap_or("n"), "n");
    assert_eq!(cells[1].1["t"], "b");
    assert_eq!(cells[2].1["t"], "str");
    assert_eq!(cells[3].1["t"], "e");
    assert!(xml.contains("<v>007</v>"));
    assert!(xml.contains("<v>#N/A</v>"));
    let reopened = read_document(&output).unwrap();
    for row in 1..=5 {
        assert!(
            reopened
                .sheet(0)
                .unwrap()
                .cell((1, row))
                .unwrap()
                .is_formula()
        );
    }
}

#[test]
fn adapter_reader_stops_at_the_document_input_limit() {
    use formualizer_workbook::{SpreadsheetReader, Umya3Adapter};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Endless(Arc<AtomicUsize>);
    impl Read for Endless {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            buf.fill(0);
            self.0.fetch_add(buf.len(), Ordering::Relaxed);
            Ok(buf.len())
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let result = Umya3Adapter::open_reader(Box::new(Endless(count.clone())));
    let error = match result {
        Ok(_) => panic!("oversized input accepted"),
        Err(e) => e,
    };
    assert!(error.to_string().contains("input limit exceeded"));
    assert_eq!(count.load(Ordering::Relaxed), 256 * 1024 * 1024 + 1);
}

#[test]
fn distinct_colour_selectors_survive_colliding_upstream_style_hashes() {
    let mut book = umya_spreadsheet3::new_file();
    let sheet = book.sheet_mut(0).unwrap();
    sheet
        .cell_mut("A1")
        .set_value("theme black")
        .set_style(style(true, 1));
    sheet
        .cell_mut("B1")
        .set_value("indexed white")
        .set_style(style(false, 1));
    sheet
        .cell_mut("C1")
        .set_value("theme accent")
        .set_style(style(true, 4));
    let first = write_document(&book).unwrap();
    let read = read_document(&first).unwrap();
    let second = write_document(&read).unwrap();
    let rebound = read_document(&second).unwrap();
    for book in [&read, &rebound] {
        let sheet = book.sheet(0).unwrap();
        for (cell, theme, index) in [("A1", true, 1), ("B1", false, 1), ("C1", true, 4)] {
            let style = sheet.cell(cell).unwrap().style();
            for colour in [
                style.font().unwrap().color().clone(),
                style.borders().unwrap().left().color().unwrap(),
            ] {
                let expected = if theme {
                    Color::default().set_theme_index(index).clone()
                } else {
                    Color::default().set_indexed(index).clone()
                };
                assert_eq!(colour, expected, "{cell}");
            }
        }
    }
    // Export is a projection; it never rewrites the authoritative colour types.
    assert_eq!(
        book.sheet(0)
            .unwrap()
            .cell("A1")
            .unwrap()
            .style()
            .font()
            .unwrap()
            .color()
            .theme_index(),
        1
    );
}

#[test]
fn conditional_and_column_styles_do_not_alias_on_export() {
    use umya_spreadsheet3::{
        ConditionalFormatValues, ConditionalFormatting, ConditionalFormattingRule, Formula,
    };
    let mut book = umya_spreadsheet3::new_file();
    let sheet = book.sheet_mut(0).unwrap();
    sheet
        .column_dimension_by_number_mut(2)
        .set_width(21.0)
        .set_style(style(true, 1));
    sheet
        .column_dimension_by_number_mut(3)
        .set_width(21.0)
        .set_style(style(false, 1));
    for (priority, theme) in [(1, true), (2, false)] {
        let mut rule = ConditionalFormattingRule::default();
        rule.set_type(ConditionalFormatValues::Expression)
            .set_priority(priority)
            .set_style(style(theme, 1));
        let mut formula = Formula::default();
        formula.set_string_value("TRUE()");
        rule.set_formula(formula);
        let mut group = ConditionalFormatting::default();
        group.sequence_of_references_mut().set_sqref("A1:D5");
        group.add_conditional_collection(rule);
        sheet.add_conditional_formatting_collection(group);
    }
    let output = read_document(&write_document(&book).unwrap()).unwrap();
    let sheet = output.sheet(0).unwrap();
    for (column, theme) in [(2, true), (3, false)] {
        let column = sheet
            .column_dimensions()
            .iter()
            .find(|c| c.col_num() == column)
            .unwrap();
        assert_eq!(column.width(), 21.0);
        let colour = column.style().font().unwrap().color();
        assert_eq!(
            *colour,
            if theme {
                Color::default().set_theme_index(1).clone()
            } else {
                Color::default().set_indexed(1).clone()
            }
        );
    }
    for (i, group) in sheet.conditional_formatting_collection().iter().enumerate() {
        let colour = group.conditional_collection()[0]
            .style()
            .unwrap()
            .font()
            .unwrap()
            .color();
        assert_eq!(
            *colour,
            if i == 0 {
                Color::default().set_theme_index(1).clone()
            } else {
                Color::default().set_indexed(1).clone()
            }
        );
    }
}
