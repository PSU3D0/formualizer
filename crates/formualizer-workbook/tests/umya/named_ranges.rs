use formualizer_common::LiteralValue;
use formualizer_workbook::{
    CellData, LoadStrategy, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter, Workbook,
    WorkbookConfig, traits::NamedRangeScope,
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
