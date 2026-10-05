#![cfg(feature = "xlsx-recalc")]
//! Formulas at the default parser AST-height limit must be safe on the stack
//! the parser documentation states (1 MiB in release builds) through every
//! path a parsed formula takes: parse, clone, hash, print, drop, workbook
//! ingest and evaluation, Calamine XLSX load and XLSX cache recalculation.
//!
//! Left-associated chains (`A1+A2+...`) do not consume Pratt frames, so they
//! are what reaches the full default height; right-nested shapes are bounded
//! by the Pratt-frame limit first and are combined with a chain to reach it.
use formualizer_common::LiteralValue;
use formualizer_parse::{ParserLimits, parse};
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
    recalculate_xlsx_bytes,
};
use std::io::{Cursor, Write};

/// Stack for the measured thread. Release matches the documented requirement;
/// unoptimized builds keep much larger frames (about 5 MiB measured).
const STACK_BYTES: usize = if cfg!(debug_assertions) {
    8 * 1024 * 1024
} else {
    1024 * 1024
};

fn chain(op: &str, operands: usize) -> String {
    let mut text = String::from("=");
    for i in 0..operands {
        if i > 0 {
            text.push_str(op);
        }
        text.push_str(&format!("A{}", i % 100 + 1));
    }
    text
}

/// `open` repeated `levels` times around a left chain, closed with `close`.
fn nested(open: &str, close: &str, levels: usize, chain_operands: usize) -> String {
    format!(
        "={}{}{}",
        open.repeat(levels),
        &chain("+", chain_operands)[1..],
        close.repeat(levels)
    )
}

/// The deepest nesting of `open`/`close` around a chain that default limits
/// admit, topped up with chain operands to reach the default AST height.
fn deepest_default(open: &str, close: &str) -> String {
    let height = ParserLimits::default().ast_height();
    let mut levels = 0;
    while parse(nested(open, close, levels + 1, 2)).is_ok() {
        levels += 1;
    }
    let mut operands = 2;
    while parse(nested(open, close, levels, operands + 1)).is_ok() {
        operands += 1;
    }
    let text = nested(open, close, levels, operands);
    assert!(levels > 0, "{open}: no nesting admitted");
    assert!(
        parse(nested(open, close, levels, operands + 1))
            .unwrap_err()
            .message
            .contains("AST height"),
        "{open}: expected the AST-height limit at {height}"
    );
    text
}

fn default_height_formulas() -> Vec<(String, String)> {
    let height = ParserLimits::default().ast_height();
    let mut cases = vec![
        ("add".to_string(), chain("+", height)),
        // 128 cells joined by `&","&` give 255 operands; one more reaches 256.
        (
            "concat".to_string(),
            format!("{}&\",\"", chain("&\",\"&", height / 2)),
        ),
        (
            "power".to_string(),
            format!("=2{}", "^2".repeat(height - 1)),
        ),
    ];
    for (name, open, close) in [
        ("paren", "1+(", ")"),
        ("negate", "-(", ")"),
        ("if", "IF(A1>0,", ",0)"),
    ] {
        cases.push((name.to_string(), deepest_default(open, close)));
    }
    for (name, text) in &cases {
        parse(text).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            parse(format!("{text}&1"))
                .unwrap_err()
                .message
                .contains("AST height"),
            "{name}: expected to sit at the height limit"
        );
    }
    cases
}

fn xlsx(formula: &str) -> Vec<u8> {
    let f = formula[1..]
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;");
    let main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let rels = "http://schemas.openxmlformats.org/package/2006/relationships";
    let office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let parts = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_string()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{main}\" xmlns:r=\"{office}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{main}\"><sheetData><row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"C1\"><f>{f}</f><v>0</v></c></row></sheetData></worksheet>")),
    ];
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    for (name, body) in parts {
        zip.start_file(name, options).unwrap();
        zip.write_all(body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

fn config(parallel: bool) -> WorkbookConfig {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.enable_parallel = parallel;
    config
}

fn assert_computed(name: &str, value: Option<LiteralValue>) {
    match value {
        Some(LiteralValue::Number(_) | LiteralValue::Text(_)) => {}
        // 2^2^... overflows; the evaluator still had to walk the whole tree.
        Some(LiteralValue::Error(e)) if name == "power" => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Num)
        }
        other => panic!("{name}: unexpected value {other:?}"),
    }
}

fn exercise(name: &str, text: &str, packages: &[u8]) {
    let ast = parse(text).unwrap();
    let cloned = ast.clone();
    assert_eq!(cloned.fingerprint(), ast.fingerprint());
    let _ = cloned.calculate_hash();
    let _ = cloned.get_dependencies();
    let printed = formualizer_parse::pretty_print(&cloned);
    let canonical = formualizer_parse::canonical_formula(&cloned);
    drop(cloned);
    drop(ast);
    assert!(
        parse(&canonical).is_ok(),
        "{name}: canonical text re-parses"
    );
    drop(printed);

    for parallel in [false, true] {
        let mut wb = Workbook::new_with_config(config(parallel));
        wb.add_sheet("Sheet1").unwrap();
        wb.set_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .unwrap();
        wb.set_formula("Sheet1", 1, 3, text).unwrap();
        wb.evaluate_all().unwrap();
        assert_computed(name, wb.get_value("Sheet1", 1, 3));
        assert_computed(name, Some(wb.evaluate_cell("Sheet1", 1, 3).unwrap()));
    }

    let adapter = CalamineAdapter::open_bytes(packages.to_vec()).unwrap();
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config(true)).unwrap();
    wb.evaluate_all().unwrap();
    assert_computed(name, wb.get_value("Sheet1", 1, 3));
    drop(wb);

    let out = recalculate_xlsx_bytes(packages, Default::default())
        .unwrap_or_else(|e| panic!("{name}: recalculation failed: {e}"));
    assert_eq!(out.formula_cells, 1, "{name}");
}

#[test]
fn default_height_formulas_fit_the_documented_stack() {
    const CHILD: &str = "FORMUALIZER_FORMULA_STACK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        // Build inputs on this (large) thread; only the measured work runs on
        // the small one.
        let cases: Vec<_> = default_height_formulas()
            .into_iter()
            .map(|(name, text)| {
                let package = xlsx(&text);
                (name, text, package)
            })
            .collect();
        std::thread::Builder::new()
            .stack_size(STACK_BYTES)
            .spawn(move || {
                for (name, text, package) in &cases {
                    exercise(name, text, package);
                }
            })
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "default_height_formulas_fit_the_documented_stack",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(status.success(), "small-stack subprocess failed: {status}");
}
