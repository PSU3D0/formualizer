//! Load-time formula families from source text: proven copies of a parsed
//! formula are staged as family members without being parsed. Every test
//! compares the engine against the same load with families off.
use formualizer_eval::engine::graph::editor::undo_engine::UndoEngine;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{
    ChangeLog, Engine, EvalConfig, SourceFamilyCounters, SourceFamilyMode,
};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};
use std::io::{Cursor, Read, Write};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

/// One worksheet cell of a fixture: a number, text, or a raw `<f>` element.
enum Cell {
    Num(f64),
    Text(&'static str),
    F(String),
}

/// Build an XLSX whose sheets hold exactly `sheets`' cells; formula cells
/// carry the given raw `<f>` element (shared anchors and descendants).
fn xlsx(sheets: &[(&str, Vec<(&str, Cell)>)]) -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let mut placeholders = Vec::new();
    for (index, (name, cells)) in sheets.iter().enumerate() {
        if index > 0 {
            book.new_sheet(*name).unwrap();
        }
        let sheet = book.get_sheet_by_name_mut(name).unwrap();
        for (address, cell) in cells {
            let target = sheet.get_cell_mut(*address);
            match cell {
                Cell::Num(n) => {
                    target.set_value_number(*n);
                }
                Cell::Text(t) => {
                    target.set_value_string(*t);
                }
                Cell::F(raw) => {
                    let placeholder = format!("PHX{}+0", placeholders.len() + 1);
                    target.set_formula(placeholder.clone());
                    placeholders.push((format!("<f>{placeholder}</f>"), raw.clone()));
                }
            }
        }
    }
    let mut original = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut original).unwrap();
    let mut input = ZipArchive::new(Cursor::new(original)).unwrap();
    let mut output = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for index in 0..input.len() {
        let mut entry = input.by_index(index).unwrap();
        let name = entry.name().to_string();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if name.starts_with("xl/worksheets/sheet") {
            let mut xml = String::from_utf8(bytes).unwrap();
            for (placeholder, raw) in &placeholders {
                xml = xml.replace(placeholder, raw);
            }
            bytes = xml.into_bytes();
        }
        output.start_file(name, options).unwrap();
        output.write_all(&bytes).unwrap();
    }
    output.finish().unwrap().into_inner()
}

fn anchor(si: u32, range: &str, text: &str) -> Cell {
    Cell::F(format!(
        "<f t=\"shared\" si=\"{si}\" ref=\"{range}\">{}</f>",
        text.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    ))
}

fn copy(si: u32) -> Cell {
    Cell::F(format!("<f t=\"shared\" si=\"{si}\"/>"))
}

fn ordinary(text: &str) -> Cell {
    Cell::F(format!(
        "<f>{}</f>",
        text.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    ))
}

fn col_name(col: u32) -> String {
    let mut col = col;
    let mut out = Vec::new();
    while col > 0 {
        out.push(b'A' + ((col - 1) % 26) as u8);
        col = (col - 1) / 26;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

fn addr(col: u32, row: u32) -> &'static str {
    Box::leak(format!("{}{row}", col_name(col)).into_boxed_str())
}

const ROWS: u32 = 30;

/// Shared families: vertical with mixed `$`, horizontal, a string family,
/// one interrupted by an ordinary formula, a family whose anchor is not a
/// template (lowercase reference), a sheet-qualified family, and one whose
/// copy would leave the grid.
fn families_xlsx() -> Vec<u8> {
    let mut main = Vec::new();
    for row in 1..=ROWS {
        main.push((addr(1, row), Cell::Num(f64::from(row))));
        main.push((addr(2, row), Cell::Num(f64::from(row * 3 % 7))));
        main.push((
            addr(3, row),
            Cell::Text(if row % 2 == 0 { "xa" } else { "yb" }),
        ));
    }
    let range = |col: u32| format!("{}1:{}{ROWS}", col_name(col), col_name(col));
    // D: A1+$B$1*B1, interrupted at D10 by an ordinary formula.
    main.push(("D1", anchor(0, &range(4), "A1+$B$1*B1")));
    // E: a string family.
    main.push((
        "E1",
        anchor(1, &range(5), "IF(LEFT(C1,1)=\"x\",\"B2\"&A1,\"\")"),
    ));
    // F: running sums with a range whose start is absolute.
    main.push(("F1", anchor(2, &range(6), "SUM($A$1:A1)/COUNT(B$1:B1)")));
    // G: lowercase anchor reference (Calamine upper-cases its copies).
    main.push(("G1", anchor(3, &range(7), "a1+1")));
    // H: sheet-qualified (not a lexical template).
    main.push(("H1", anchor(4, &range(8), "Other!A1*2")));
    for row in 2..=ROWS {
        if row == 10 {
            main.push(("D10", ordinary("A10*100")));
        } else {
            main.push((addr(4, row), copy(0)));
        }
        for (col, si) in [(5, 1), (6, 2), (7, 3), (8, 4)] {
            main.push((addr(col, row), copy(si)));
        }
    }
    // Row 32: a horizontal family I32:L32.
    main.push(("I32", anchor(5, "I32:L32", "I31-$A32+I$1")));
    for col in 10..=12 {
        main.push((addr(col, 32), copy(5)));
    }
    let mut other = Vec::new();
    for row in 1..=ROWS {
        other.push((addr(1, row), Cell::Num(f64::from(row * 10))));
    }
    // A family whose copy at the last column would leave the grid: Calamine
    // keeps the copy's text unshifted, which is not the template relocated.
    other.push(("XFC1", anchor(6, "XFC1:XFD1", "XFD2+A1")));
    other.push(("XFD1", copy(6)));
    other.push(("XFD2", Cell::Num(5.0)));
    xlsx(&[("Sheet1", main), ("Other", other)])
}

fn load(bytes: &[u8], deferred: bool, mode: SourceFamilyMode) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        defer_graph_building: deferred,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.set_source_family_mode(mode);
    let mut adapter = CalamineAdapter::open_bytes(bytes.to_vec()).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();
    engine
}

fn formula_text(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> Option<String> {
    engine.get_staged_formula_text(sheet, row, col).or_else(|| {
        engine
            .get_cell(sheet, row, col)
            .and_then(|(ast, _)| ast.map(|a| formualizer_parse::pretty::canonical_formula(&a)))
    })
}

fn snapshot(engine: &Engine<TestWorkbook>) -> Vec<(String, u32, u32, Option<String>, String)> {
    let mut out = Vec::new();
    for (sheet, cols) in [("Sheet1", 1..=14), ("Other", 16_380..=16_384)] {
        for row in 1..=ROWS + 4 {
            for col in cols.clone() {
                let value = match engine.get_cell_value(sheet, row, col) {
                    Some(LiteralValue::Number(n)) => format!("{:x}", n.to_bits()),
                    other => format!("{other:?}"),
                };
                out.push((
                    sheet.to_string(),
                    row,
                    col,
                    formula_text(engine, sheet, row, col),
                    value,
                ));
            }
        }
    }
    out
}

fn counters(engine: &Engine<TestWorkbook>) -> SourceFamilyCounters {
    engine.source_family_counters()
}

#[test]
fn proven_shared_copies_skip_parsing_and_match_the_parsed_load() {
    let bytes = families_xlsx();
    for deferred in [false, true] {
        let mut old = load(&bytes, deferred, SourceFamilyMode::Off);
        let mut new = load(&bytes, deferred, SourceFamilyMode::On);
        // Inspection before evaluation (deferred: before the graph exists).
        assert_eq!(snapshot(&new), snapshot(&old), "deferred={deferred}");
        old.evaluate_all().unwrap();
        new.evaluate_all().unwrap();
        assert_eq!(snapshot(&new), snapshot(&old), "deferred={deferred}");
        let (o, n) = (counters(&old), counters(&new));
        assert_eq!(o.formulas, n.formulas);
        assert_eq!(o.shared_members, 0);
        assert_eq!(o.parse_calls, o.formulas - o.parse_cache_hits);
        // D (28 copies), E, F (29 each), the horizontal family (3), and
        // G from its first (upper-cased, so certified) copy on (28).
        assert_eq!(n.shared_members, 28 + 29 + 29 + 3 + 28, "{n:?}");
        assert_eq!(
            n.parse_calls + n.shared_members + n.parse_cache_hits,
            n.formulas
        );
        // G's lowercase anchor and H's qualified anchor are not templates.
        assert!(n.templates_uncertified >= 2, "{n:?}");
        assert_eq!(n.fallback_off_grid, 1, "{n:?}");
        // A second evaluation is stable.
        new.evaluate_all().unwrap();
        old.evaluate_all().unwrap();
        assert_eq!(snapshot(&new), snapshot(&old));
    }
}

#[test]
fn off_grid_copy_keeps_calamine_text() {
    let bytes = families_xlsx();
    let engine = load(&bytes, false, SourceFamilyMode::On);
    // The copy at XFD1 keeps XFD2 (Calamine does not relocate off the grid).
    assert_eq!(
        formula_text(&engine, "Other", 1, 16_384).as_deref(),
        Some("=XFD2 + B1")
    );
}

#[test]
fn edits_after_load_and_structural_undo_redo_match_the_parsed_load() {
    let bytes = families_xlsx();
    for deferred in [false, true] {
        let mut engines = [
            load(&bytes, deferred, SourceFamilyMode::Off),
            load(&bytes, deferred, SourceFamilyMode::On),
        ];
        let mut logs = [ChangeLog::new(), ChangeLog::new()];
        let mut undos = [UndoEngine::new(), UndoEngine::new()];
        let mut snapshots: [Vec<_>; 2] = [Vec::new(), Vec::new()];
        for i in 0..2 {
            let (engine, log, undo) = (&mut engines[i], &mut logs[i], &mut undos[i]);
            let snaps = &mut snapshots[i];
            engine.evaluate_all().unwrap();
            engine
                .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(100.0))
                .unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine
                .action_with_logger(log, "insert", |a| a.insert_rows("Sheet1", 8, 2).map(|_| ()))
                .unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine
                .action_with_logger(log, "insert", |a| a.insert_rows("Sheet1", 3, 1).map(|_| ()))
                .unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine.undo_logged(undo, log).unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine.undo_logged(undo, log).unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine.redo_logged(undo, log).unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine
                .action_with_logger(log, "cols", |a| {
                    a.insert_columns("Sheet1", 2, 1).map(|_| ())
                })
                .unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine.undo_logged(undo, log).unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
            engine.delete_rows("Sheet1", 4, 3).unwrap();
            engine.delete_columns("Sheet1", 2, 1).unwrap();
            engine.evaluate_all().unwrap();
            snaps.push(snapshot(engine));
        }
        for (step, (old, new)) in snapshots[0].iter().zip(&snapshots[1]).enumerate() {
            assert_eq!(new, old, "deferred={deferred} step={step}");
        }
    }
}

#[test]
fn compression_off_parses_every_formula() {
    let bytes = families_xlsx();
    let config = EvalConfig {
        formula_compression: false,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.set_source_family_mode(SourceFamilyMode::On);
    let mut adapter = CalamineAdapter::open_bytes(bytes.clone()).unwrap();
    adapter.stream_into_engine(&mut engine).unwrap();
    let c = engine.source_family_counters();
    assert_eq!(c.shared_members + c.adjacent_members, 0);
    engine.evaluate_all().unwrap();
    let mut old = load(&bytes, false, SourceFamilyMode::Off);
    old.evaluate_all().unwrap();
    assert_eq!(snapshot(&engine), snapshot(&old));
}
