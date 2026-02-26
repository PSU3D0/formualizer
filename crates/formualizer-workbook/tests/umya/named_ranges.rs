use formualizer_common::LiteralValue;
use formualizer_workbook::{
    CellData, LoadStrategy, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter, Workbook,
    WorkbookConfig,
    traits::{DefinedNameDefinition, DefinedNameScope, NamedRangeScope},
};

fn build_named_range_workbook() -> Workbook {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("named_range_eval_runtime.xlsx");

    let mut book = umya_spreadsheet::new_file();
    let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");
    sheet1.get_cell_mut((1, 1)).set_value_number(10.0);
    sheet1
        .add_defined_name("InputValue", "Sheet1!$A$1")
        .expect("add input name");
    sheet1.get_cell_mut((2, 1)).set_formula("InputValue*2");
    sheet1
        .add_defined_name("OutputValue", "Sheet1!$B$1")
        .expect("add output name");
    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write workbook");

    let backend = UmyaAdapter::open_path(&path).expect("open workbook for runtime evaluation");
    let mut workbook = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load workbook");
    workbook.evaluate_all().expect("initial evaluate");
    workbook
}

#[test]
fn umya_exposes_named_ranges_with_scope() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("named_ranges.xlsx");

    let mut book = umya_spreadsheet::new_file();
    // Ensure Sheet1 exists (new_file) and add Sheet2 for scope checks.
    let _ = book.new_sheet("Sheet2");

    {
        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");
        sheet1
            .add_defined_name("GlobalName", "Sheet1!$A$1")
            .expect("add global name");
        sheet1
            .add_defined_name("LocalName", "Sheet1!$B$2")
            .expect("add local name");
        // Mark last defined name as sheet-scoped.
        if let Some(last) = sheet1.get_defined_names_mut().last_mut() {
            last.set_local_sheet_id(0);
        }
    }

    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write workbook");

    let mut adapter = UmyaAdapter::open_path(&path).expect("open workbook");
    let sheet = adapter.read_sheet("Sheet1").expect("read sheet1");

    assert_eq!(sheet.named_ranges.len(), 2);

    let mut saw_global = false;
    let mut saw_local = false;

    for named in sheet.named_ranges {
        match named.name.as_str() {
            "GlobalName" => {
                saw_global = true;
                assert_eq!(named.scope, NamedRangeScope::Workbook);
                assert_eq!(named.address.sheet, "Sheet1");
                assert_eq!(named.address.start_row, 1);
                assert_eq!(named.address.start_col, 1);
                assert_eq!(named.address.end_row, 1);
                assert_eq!(named.address.end_col, 1);
            }
            "LocalName" => {
                saw_local = true;
                assert_eq!(named.scope, NamedRangeScope::Sheet);
                assert_eq!(named.address.sheet, "Sheet1");
                assert_eq!(named.address.start_row, 2);
                assert_eq!(named.address.start_col, 2);
                assert_eq!(named.address.end_row, 2);
                assert_eq!(named.address.end_col, 2);
            }
            other => panic!("unexpected named range {other}"),
        }
    }

    assert!(saw_global && saw_local);
}

#[test]
fn umya_named_range_loader_evaluates() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("named_range_eval.xlsx");

    // Build workbook with named input/output and dependent formula.
    let mut book = umya_spreadsheet::new_file();
    let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");
    sheet1.get_cell_mut((1, 1)).set_value_number(10.0);
    sheet1
        .add_defined_name("InputValue", "Sheet1!$A$1")
        .expect("add input name");
    sheet1.get_cell_mut((2, 1)).set_formula("InputValue*2");
    sheet1
        .add_defined_name("OutputValue", "Sheet1!$B$1")
        .expect("add output name");
    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write workbook");

    // Load through Workbook loader to ensure evaluation paths see the named ranges.
    let backend = UmyaAdapter::open_path(&path).expect("open workbook");
    let mut workbook = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load workbook");
    workbook.evaluate_all().expect("evaluate");

    let addr = workbook
        .named_range_address("InputValue")
        .expect("input named range");
    assert_eq!(addr.sheet, "Sheet1");
    assert_eq!(addr.start_row, 1);
    assert_eq!(addr.start_col, 1);
    let sheet_id = workbook.engine().sheet_id("Sheet1").unwrap();
    assert!(
        workbook
            .engine()
            .resolve_name_entry("InputValue", sheet_id)
            .is_some()
    );

    let output = workbook.get_value("Sheet1", 1, 2).expect("output present");
    assert!(matches!(output, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9));

    // Mutating the named range cell and reloading should propagate after evaluation.
    let mut adapter = UmyaAdapter::open_path(&path).expect("reopen workbook");
    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_value(15.0))
        .expect("write input value");
    adapter.save().expect("save workbook");

    let backend2 = UmyaAdapter::open_path(&path).expect("reopen updated workbook");
    let mut workbook2 = Workbook::from_reader(
        backend2,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("reload workbook");
    workbook2.evaluate_all().expect("re-evaluate");
    let updated = workbook2.get_value("Sheet1", 1, 2).expect("updated output");
    assert!(matches!(updated, LiteralValue::Number(n) if (n - 30.0).abs() < 1e-9));
}

#[test]
fn umya_imports_open_ended_column_named_ranges() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("named_range_open_ended.xlsx");

    let mut book = umya_spreadsheet::new_file();
    let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");

    // Lookup table in A:B
    sheet1.get_cell_mut((1, 1)).set_value("Professional");
    sheet1.get_cell_mut((2, 1)).set_value_number(123.0);

    // Workbook named range with open-ended rows.
    sheet1
        .add_defined_name("Split", "Sheet1!$A:$B")
        .expect("add split name");

    // Lookup formula that relies on named range import.
    sheet1
        .get_cell_mut((3, 1))
        .set_formula("=VLOOKUP(\"Professional\", Split, 2, FALSE())");

    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write workbook");

    let backend = UmyaAdapter::open_path(&path).expect("open workbook");
    let mut workbook = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load workbook");

    let split_addr = workbook
        .named_range_address("Split")
        .expect("split named range imported");
    assert_eq!(split_addr.sheet, "Sheet1");
    assert_eq!(split_addr.start_row, 1);
    assert_eq!(split_addr.start_col, 1);
    assert_eq!(split_addr.end_col, 2);
    assert_eq!(split_addr.end_row, 1_048_576);

    let sheet_id = workbook.engine().sheet_id("Sheet1").expect("sheet id");
    assert!(
        workbook
            .engine()
            .resolve_name_entry("Split", sheet_id)
            .is_some(),
        "engine should register open-ended named range"
    );

    let value = workbook
        .evaluate_cell("Sheet1", 1, 3)
        .expect("evaluate lookup");
    match value {
        LiteralValue::Number(n) => assert!((n - 123.0).abs() < 1e-9),
        LiteralValue::Int(i) => assert_eq!(i, 123),
        other => panic!("expected numeric lookup result, got {other:?}"),
    }
}

// Helper: write a umya Spreadsheet to bytes via the xlsx writer.
fn book_to_bytes(book: &umya_spreadsheet::Spreadsheet) -> Vec<u8> {
    let mut buf = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(book, &mut buf).expect("write to bytes");
    buf
}

/// Workbook-scoped names must remain Workbook-scoped in `defined_names()` regardless of
/// which worksheet object they were associated with when the file was built.
/// Before the fix, names without a localSheetId that came through the sheet-level
/// iteration path were incorrectly stamped with the iterating sheet as their scope.
#[test]
fn workbook_scoped_names_not_demoted_to_sheet_scope() {
    let mut book = umya_spreadsheet::new_file();
    let _ = book.new_sheet("Data");

    // Add workbook-scoped names to each sheet — no set_local_sheet_id call, so no localSheetId.
    {
        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        for r in 1..=4u32 {
            sheet1.get_cell_mut((r, 1)).set_value_number(r as f64);
        }
        sheet1
            .add_defined_name("RangeAlpha", "Sheet1!$A$1:$A$4")
            .expect("RangeAlpha");
    }
    {
        let data = book.get_sheet_by_name_mut("Data").expect("Data");
        for r in 1..=7u32 {
            data.get_cell_mut((r, 1)).set_value_number(r as f64);
        }
        data.add_defined_name("RangeBeta", "Data!$A$1:$A$7")
            .expect("RangeBeta");
    }

    let bytes = book_to_bytes(&book);
    let mut adapter = UmyaAdapter::open_bytes(bytes).expect("open from bytes");
    let names = adapter.defined_names().expect("defined_names");

    for name in ["RangeAlpha", "RangeBeta"] {
        let workbook_entries: Vec<_> = names
            .iter()
            .filter(|n| n.name == name && n.scope == DefinedNameScope::Workbook)
            .collect();
        assert_eq!(
            workbook_entries.len(),
            1,
            "{name} should appear exactly once as Workbook-scoped"
        );

        let sheet_entries: Vec<_> = names
            .iter()
            .filter(|n| n.name == name && n.scope == DefinedNameScope::Sheet)
            .collect();
        assert!(
            sheet_entries.is_empty(),
            "{name} must not be classified as Sheet-scoped; found: {sheet_entries:?}"
        );
    }
}

/// Workbook-scoped names declared on one sheet must be usable in formulas on a
/// different sheet.  Before the fix this produced #NAME? when the name was
/// incorrectly classified as sheet-scoped to its declaring sheet.
#[test]
fn workbook_scoped_name_resolves_cross_sheet() {
    let mut book = umya_spreadsheet::new_file();
    let _ = book.new_sheet("Lookup");

    // Lookup table on "Lookup": two-column key/value table.
    // Umya get_cell_mut takes (col, row), so row varies as the second argument.
    {
        let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup");
        let entries = [("Alpha", 10.0), ("Beta", 20.0), ("Gamma", 30.0)];
        for (i, (key, val)) in entries.iter().enumerate() {
            let row = (i + 1) as u32;
            lookup.get_cell_mut((1, row)).set_value(*key); // col A
            lookup.get_cell_mut((2, row)).set_value_number(*val); // col B
        }
        // Workbook-scoped name — no set_local_sheet_id, so no localSheetId.
        lookup
            .add_defined_name("LookupTable", "Lookup!$A$1:$B$3")
            .expect("LookupTable");
    }

    // Summary sheet uses the name in cross-sheet formulas placed in col A.
    {
        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1");
        sheet1
            .get_cell_mut((1, 1)) // col=1 row=1 → A1
            .set_formula("=ROWS(LookupTable)");
        sheet1
            .get_cell_mut((1, 2)) // col=1 row=2 → A2
            .set_formula("=VLOOKUP(\"Beta\", LookupTable, 2, FALSE())");
    }

    let bytes = book_to_bytes(&book);
    let backend = UmyaAdapter::open_bytes(bytes).expect("open from bytes");
    let mut workbook = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load workbook");
    workbook.evaluate_all().expect("evaluate");

    // formualizer get_value uses (row, col): A1=(row=1,col=1), A2=(row=2,col=1).
    let rows_val = workbook.get_value("Sheet1", 1, 1).expect("ROWS result");
    match rows_val {
        LiteralValue::Number(n) => {
            assert!(
                (n - 3.0).abs() < 1e-9,
                "ROWS(LookupTable) should be 3, got {n}"
            )
        }
        LiteralValue::Int(i) => assert_eq!(i, 3, "ROWS(LookupTable) should be 3"),
        other => panic!("expected number for ROWS, got {other:?}"),
    }

    let vlookup_val = workbook.get_value("Sheet1", 2, 1).expect("VLOOKUP result");
    match vlookup_val {
        LiteralValue::Number(n) => {
            assert!(
                (n - 20.0).abs() < 1e-9,
                "VLOOKUP Beta should be 20.0, got {n}"
            )
        }
        LiteralValue::Int(i) => assert_eq!(i, 20, "VLOOKUP Beta should be 20"),
        other => panic!("expected number for VLOOKUP, got {other:?}"),
    }
}

#[test]
fn umya_named_range_set_value_recalc() {
    let mut workbook = build_named_range_workbook();
    workbook.evaluate_all().expect("initial evaluate");

    let initial = workbook
        .get_value("Sheet1", 1, 2)
        .expect("initial output value");
    assert!(matches!(initial, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9));

    workbook
        .set_value("Sheet1", 1, 1, LiteralValue::Number(25.0))
        .expect("set named input");
    let sheet_id = workbook.engine().sheet_id("Sheet1").unwrap();
    assert_eq!(
        workbook.engine().get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(25.0)
    );

    let name_entry = workbook
        .engine()
        .resolve_name_entry("InputValue", sheet_id)
        .unwrap();
    let name_vertex = name_entry.vertex;

    let pending = workbook.engine().evaluation_vertices();
    assert!(
        pending.contains(&name_vertex),
        "named range vertex should be marked dirty after input mutation"
    );

    workbook.evaluate_all().expect("re-evaluate workbook");
    let name_val_after = workbook.engine().vertex_value(name_vertex);
    assert!(matches!(
        name_val_after,
        Some(LiteralValue::Number(n)) if (n - 25.0).abs() < 1e-9
    ));

    let updated = workbook
        .get_value("Sheet1", 1, 2)
        .expect("updated output value");
    assert!(
        matches!(updated, LiteralValue::Number(n) if (n - 50.0).abs() < 1e-9),
        "got {updated:?} instead"
    );
}

#[test]
fn umya_named_range_out_of_bounds_is_clamped_for_ingest() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("named_range_oob_plus_one.xlsx");

    let mut book = umya_spreadsheet::new_file();
    let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");
    sheet1.get_cell_mut((1, 1)).set_value_number(1.0); // A1
    sheet1
        .add_defined_name("TooFar", "Sheet1!$A$1:$A$1048577")
        .expect("add out-of-bounds named range");
    sheet1.get_cell_mut((2, 1)).set_formula("=SUM(TooFar)"); // B1
    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write workbook");

    let mut adapter = UmyaAdapter::open_path(&path).expect("open workbook");
    let names = adapter.defined_names().expect("read defined names");
    let toofar = names
        .into_iter()
        .find(|n| n.name == "TooFar")
        .expect("TooFar defined name imported");

    match toofar.definition {
        DefinedNameDefinition::Range { address } => {
            assert_eq!(address.start_row, 1);
            assert_eq!(address.end_row, 1_048_576);
            assert_eq!(address.start_col, 1);
            assert_eq!(address.end_col, 1);
        }
        other => panic!("expected range definition, got {other:?}"),
    }

    let backend = UmyaAdapter::open_path(&path).expect("open workbook for load");
    let mut workbook = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load workbook should not panic/fail");

    let addr = workbook
        .named_range_address("TooFar")
        .expect("TooFar resolved in workbook");
    assert_eq!(addr.end_row, 1_048_576);

    workbook.evaluate_all().expect("evaluate workbook");
    let value = workbook
        .get_value("Sheet1", 1, 2)
        .expect("formula value in B1");
    match value {
        LiteralValue::Number(n) => assert!((n - 1.0).abs() < 1e-9),
        LiteralValue::Int(i) => assert_eq!(i, 1),
        other => panic!("expected numeric value 1 for SUM(TooFar), got {other:?}"),
    }
}
