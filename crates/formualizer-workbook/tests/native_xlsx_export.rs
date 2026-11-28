//! Comprehensive round-trip and OOXML compliance tests for the native
//! `Workbook::to_xlsx_bytes()` serializer (zero `umya-spreadsheet` in production).

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use formualizer_common::{
    DateSystem, ExcelError, ExcelErrorKind, LiteralValue, date_to_serial_for,
    datetime_to_serial_for,
};
use formualizer_eval::engine::CycleConfig;
use formualizer_workbook::{
    LoadStrategy, NativeXlsxAdapter, SpreadsheetReader, Workbook, WorkbookConfig,
    XlsxRecalculateOptions, recalculate_xlsx_bytes,
};
use std::io::{Cursor, Read};

fn reopen_native(bytes: Vec<u8>) -> Workbook {
    reopen_native_with_config(bytes, WorkbookConfig::interactive())
}

fn reopen_native_with_config(bytes: Vec<u8>, config: WorkbookConfig) -> Workbook {
    let adapter = NativeXlsxAdapter::open_bytes(bytes).expect("open native xlsx bytes");
    Workbook::from_reader(adapter, LoadStrategy::EagerAll, config)
        .expect("load workbook from native xlsx bytes")
}

#[test]
fn roundtrip_sheet_order_special_names_strings_numbers_booleans_and_formulas() {
    let mut wb = Workbook::new();
    let sheet_names = [
        "Zeta & Alpha",
        "Quote \"One\" <Two>",
        "Unicode 🚀 Sheet",
        "EmptySheet",
    ];
    for name in &sheet_names {
        wb.add_sheet(name).unwrap();
    }
    if wb.sheet_names().iter().any(|s| s == "Sheet1") {
        wb.delete_sheet("Sheet1").unwrap();
    }

    // Sheet 1: strings (special XML chars, leading/trailing spaces, newlines, CRLF, emoji, _xHHHH_ literals)
    let s1 = sheet_names[0];
    let test_strings = [
        "plain",
        "  leading and trailing  ",
        "line1\nline2\nline3",
        "a & b < c > d \"e\" 'f'",
        "Unicode: café 日本語 🚀✨",
        "literal _x000A_ escape",
        "literal _x4E2D_ escape",
        "\tindented\t",
    ];
    for (i, text) in test_strings.iter().enumerate() {
        wb.set_value(s1, (i + 1) as u32, 1, LiteralValue::Text((*text).into()))
            .unwrap();
    }

    // Numbers: exact f64, -0.0, very large, very small, negative, int
    let test_numbers = [
        0.0_f64,
        -0.0_f64,
        42.0,
        -123456.789012345,
        1.2345678901234567e300,
        1.2345678901234567e-300,
        f64::MIN_POSITIVE,
    ];
    for (i, &num) in test_numbers.iter().enumerate() {
        wb.set_value(s1, (i + 1) as u32, 2, LiteralValue::Number(num))
            .unwrap();
    }
    wb.set_value(s1, 8, 2, LiteralValue::Int(9_007_199_254_740_991))
        .unwrap();

    // Booleans
    wb.set_value(s1, 1, 3, LiteralValue::Boolean(true)).unwrap();
    wb.set_value(s1, 2, 3, LiteralValue::Boolean(false))
        .unwrap();

    // Formulas + evaluated cached values (numeric, string, boolean, error)
    let s2 = sheet_names[1];
    wb.set_value(s2, 1, 1, LiteralValue::Number(10.0)).unwrap();
    wb.set_value(s2, 2, 1, LiteralValue::Number(32.5)).unwrap();
    wb.set_formula(s2, 1, 2, "=SUM(A1:A2)").unwrap();
    wb.set_formula(s2, 2, 2, "=\"  hi & <ok>  \"").unwrap();
    wb.set_formula(s2, 3, 2, "=A1<A2").unwrap();
    wb.set_formula(s2, 4, 2, "=1/0").unwrap();
    wb.evaluate_all().unwrap();

    let bytes = wb.to_xlsx_bytes().expect("serialize native xlsx");

    // 1. Reopen with our native loader and verify every sheet & value
    let mut reopened = reopen_native(bytes.clone());
    assert_eq!(reopened.sheet_names(), sheet_names);

    for (i, expected) in test_strings.iter().enumerate() {
        assert_eq!(
            reopened.get_value(s1, (i + 1) as u32, 1),
            Some(LiteralValue::Text((*expected).into())),
            "string mismatch at row {}",
            i + 1
        );
    }

    for (i, &expected) in test_numbers.iter().enumerate() {
        let actual = reopened.get_value(s1, (i + 1) as u32, 2);
        match actual {
            Some(LiteralValue::Number(n)) => {
                assert_eq!(
                    n.to_bits(),
                    expected.to_bits(),
                    "exact f64 bit mismatch at row {} ({n:?} vs {expected:?})",
                    i + 1
                );
            }
            other => panic!("expected Number at row {}, got {other:?}", i + 1),
        }
    }
    assert_eq!(
        reopened.get_value(s1, 8, 2),
        Some(LiteralValue::Number(9_007_199_254_740_991.0))
    );

    assert_eq!(
        reopened.get_value(s1, 1, 3),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        reopened.get_value(s1, 2, 3),
        Some(LiteralValue::Boolean(false))
    );

    assert_eq!(
        reopened.get_formula(s2, 1, 2).as_deref(),
        Some("SUM(A1:A2)")
    );
    assert_eq!(
        reopened.evaluate_cell(s2, 1, 2).unwrap(),
        LiteralValue::Number(42.5)
    );
    assert_eq!(
        reopened.evaluate_cell(s2, 2, 2).unwrap(),
        LiteralValue::Text("  hi & <ok>  ".into())
    );
    assert_eq!(
        reopened.evaluate_cell(s2, 3, 2).unwrap(),
        LiteralValue::Boolean(true)
    );
    assert!(matches!(
        reopened.evaluate_cell(s2, 4, 2).unwrap(),
        LiteralValue::Error(e) if e.kind == ExcelErrorKind::Div
    ));

    // EmptySheet stays empty (zero populated cells)
    assert_eq!(reopened.get_value("EmptySheet", 1, 1), None);
    let mut raw_check = NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap();
    assert!(raw_check.read_sheet("EmptySheet").unwrap().cells.is_empty());

    // 2. Cross-verify OOXML compliance by opening with umya-spreadsheet (dev-dependency only)
    let umya_book = umya_spreadsheet::reader::xlsx::read_reader(Cursor::new(&bytes), true)
        .expect("umya-spreadsheet must open native xlsx bytes cleanly");
    let umya_names: Vec<&str> = umya_book
        .get_sheet_collection()
        .iter()
        .map(|s| s.get_name())
        .collect();
    assert_eq!(umya_names, sheet_names);
    let umya_s1 = umya_book.get_sheet_by_name(s1).unwrap();
    assert_eq!(umya_s1.get_value((1, 1)), "plain");
    assert_eq!(umya_s1.get_value((1, 4)), "a & b < c > d \"e\" 'f'");
    assert_eq!(umya_s1.get_value((1, 5)), "Unicode: café 日本語 🚀✨");
    let umya_s2 = umya_book.get_sheet_by_name(s2).unwrap();
    assert!(umya_s2.get_cell((2, 1)).unwrap().is_formula());
    assert_eq!(
        umya_s2.get_cell((2, 1)).unwrap().get_formula(),
        "SUM(A1:A2)"
    );
    assert_eq!(umya_s2.get_cell((2, 1)).unwrap().get_value(), "42.5");
}

#[test]
fn roundtrip_all_13_excel_error_kinds_and_recalculate_xlsx_bytes() {
    let classic_kinds = [
        ExcelErrorKind::Null,
        ExcelErrorKind::Ref,
        ExcelErrorKind::Name,
        ExcelErrorKind::Value,
        ExcelErrorKind::Div,
        ExcelErrorKind::Na,
        ExcelErrorKind::Num,
    ];
    let non_classic_literal_kinds = [
        ExcelErrorKind::Spill,
        ExcelErrorKind::Calc,
        ExcelErrorKind::Error,
        ExcelErrorKind::NImpl,
        ExcelErrorKind::Circ,
        ExcelErrorKind::Cancelled,
    ];
    let all_kinds: Vec<ExcelErrorKind> = classic_kinds
        .iter()
        .chain(non_classic_literal_kinds.iter())
        .copied()
        .collect();
    assert_eq!(all_kinds.len(), 13);

    let mut wb = Workbook::new();
    wb.add_sheet("Errors").unwrap();
    if wb.sheet_names().iter().any(|s| s == "Sheet1") {
        wb.delete_sheet("Sheet1").unwrap();
    }

    // Column A: literal error values for all 13 error kinds
    for (i, &kind) in all_kinds.iter().enumerate() {
        wb.set_value(
            "Errors",
            (i + 1) as u32,
            1,
            LiteralValue::Error(ExcelError::new(kind)),
        )
        .unwrap();
    }

    // Column B: formula cells producing Excel errors and non-error results
    // plus a cyclic pair (C1/C2) and a spill-blocked formula (D1/D2) so
    // cached formula errors across classic, Excel 365 (#SPILL!), and engine-only (#CIRC!)
    // error kinds are exercised in the exported XML.
    wb.set_formula("Errors", 1, 2, "=1/0").unwrap(); // #DIV/0!
    wb.set_formula("Errors", 2, 2, "=NA()").unwrap(); // #N/A
    wb.set_formula("Errors", 3, 2, "=\"x\"+1").unwrap(); // #VALUE!
    wb.set_formula("Errors", 4, 2, "=SQRT(-1)").unwrap(); // #NUM!
    wb.set_formula("Errors", 5, 2, "=A1").unwrap(); // #NULL!
    wb.set_formula("Errors", 6, 2, "=A2").unwrap(); // #REF!
    wb.set_formula("Errors", 7, 2, "=A3").unwrap(); // #NAME?
    wb.set_formula("Errors", 8, 2, "=A8").unwrap(); // references literal "#SPILL!" text
    wb.set_formula("Errors", 9, 2, "=A9").unwrap(); // references literal "#CALC!" text
    wb.set_formula("Errors", 10, 2, "=A10").unwrap(); // references literal "#ERROR!" text
    wb.set_formula("Errors", 11, 2, "=A11").unwrap(); // references literal "#N/IMPL!" text
    wb.set_formula("Errors", 12, 2, "=A12").unwrap(); // references literal "#CIRC!" text
    wb.set_formula("Errors", 13, 2, "=A13").unwrap(); // references literal "#CANCELLED!" text
    // Real #CIRC! formula pair in C1/C2
    wb.set_formula("Errors", 1, 3, "=C2+1").unwrap();
    wb.set_formula("Errors", 2, 3, "=C1+1").unwrap();
    // Real #SPILL! formula in D1 (blocked by D2)
    wb.set_value("Errors", 2, 4, LiteralValue::Number(99.0))
        .unwrap();
    wb.set_formula("Errors", 1, 4, "=SEQUENCE(2,1)").unwrap();
    wb.evaluate_all().unwrap();

    let bytes = wb.to_xlsx_bytes().unwrap();

    // Inspect raw sheet XML to confirm OOXML error encoding rules:
    // - Classic 7 literal errors use t="e"
    // - Non-classic/engine-only literal errors use t="inlineStr"
    // - Formula cached #CIRC! uses t="str" (never t="e")
    // - Formula cached #SPILL! uses t="e"
    let mut archive = zip::ZipArchive::new(Cursor::new(&bytes)).unwrap();
    let mut sheet_xml = String::new();
    archive
        .by_name("xl/worksheets/sheet1.xml")
        .unwrap()
        .read_to_string(&mut sheet_xml)
        .unwrap();
    for kind in &classic_kinds {
        assert!(
            sheet_xml.contains(&format!(r#"t="e"><v>{kind}</v>"#)),
            "expected classic literal error {kind} as t=\"e\": {sheet_xml}"
        );
    }
    for kind in &non_classic_literal_kinds {
        assert!(
            sheet_xml.contains(&format!(r#"t="inlineStr"><is><t>{kind}</t></is>"#)),
            "expected non-classic literal error {kind} as inlineStr: {sheet_xml}"
        );
    }
    assert!(
        !sheet_xml.contains(r#"t="e"><v>#CIRC!</v>"#)
            && !sheet_xml.contains(r#"t="e"><f>C2 + 1</f><v>#CIRC!</v>"#),
        "engine-only #CIRC! must never be emitted with t=\"e\": {sheet_xml}"
    );
    assert!(
        sheet_xml.contains(r#"t="str"><f>C2 + 1</f><v>#CIRC!</v>"#),
        "expected cyclic formula cache #CIRC! as t=\"str\": {sheet_xml}"
    );
    assert!(
        sheet_xml.contains(r#"t="e"><f>SEQUENCE(2, 1)</f><v>#SPILL!</v>"#),
        "expected Excel 365 formula cache #SPILL! as t=\"e\": {sheet_xml}"
    );
    for (row, kind) in [
        (10, ExcelErrorKind::Error),
        (11, ExcelErrorKind::NImpl),
        (12, ExcelErrorKind::Circ),
        (13, ExcelErrorKind::Cancelled),
    ] {
        let expected_cell = format!(r#"<c r="B{row}" t="str"><f>A{row}</f><v>{kind}</v></c>"#);
        assert!(
            sheet_xml.contains(&expected_cell),
            "engine-only formula error {kind} must use t=\"str\" ({expected_cell}): {sheet_xml}"
        );
        assert!(
            !sheet_xml.contains(&format!(r#"t="e"><v>{kind}</v>"#)),
            "engine-only {kind} must never appear as t=\"e\": {sheet_xml}"
        );
    }

    // 1. Reopen with NativeXlsxAdapter
    let reopened = reopen_native(bytes.clone());
    for (i, &kind) in classic_kinds.iter().enumerate() {
        match reopened.get_value("Errors", (i + 1) as u32, 1) {
            Some(LiteralValue::Error(e)) => {
                assert_eq!(e.kind, kind, "classic error mismatch at row {}", i + 1)
            }
            other => panic!("expected error {kind} at row {}, got {other:?}", i + 1),
        }
    }
    for (offset, &kind) in non_classic_literal_kinds.iter().enumerate() {
        let row = (classic_kinds.len() + offset + 1) as u32;
        assert_eq!(
            reopened.get_value("Errors", row, 1),
            Some(LiteralValue::Text(kind.to_string())),
            "non-classic literal error at row {row} should reload as string"
        );
    }

    // 2. Round-trip through recalculate_xlsx_bytes (Workbook profile handles cycles cleanly)
    let recalced_workbook = recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::workbook())
        .expect(
            "to_xlsx_bytes -> recalculate_xlsx_bytes(workbook) must succeed for all 13 error kinds",
        );
    assert!(recalced_workbook.summary.evaluated >= 15);
    assert!(
        recalced_workbook
            .summary
            .error_summary
            .contains_key("#DIV/0!"),
        "expected #DIV/0! in recalc summary"
    );

    // 3. Also verify Strict profile succeeds on all 13 literal error kinds + non-cyclic formulas
    let mut strict_wb = Workbook::new();
    strict_wb.add_sheet("StrictErrors").unwrap();
    for (i, &kind) in all_kinds.iter().enumerate() {
        strict_wb
            .set_value(
                "StrictErrors",
                (i + 1) as u32,
                1,
                LiteralValue::Error(ExcelError::new(kind)),
            )
            .unwrap();
    }
    strict_wb.set_formula("StrictErrors", 1, 2, "=1/0").unwrap();
    strict_wb
        .set_formula("StrictErrors", 2, 2, "=LEN(A12)")
        .unwrap();
    strict_wb.evaluate_all().unwrap();
    let strict_bytes = strict_wb.to_xlsx_bytes().unwrap();
    let recalced_strict = recalculate_xlsx_bytes(&strict_bytes, XlsxRecalculateOptions::default())
        .expect("to_xlsx_bytes -> recalculate_xlsx_bytes(strict) must succeed for all 13 literal error kinds");
    assert_eq!(recalced_strict.summary.evaluated, 2);
    assert_eq!(recalced_strict.summary.errors, 1);

    // 4. Cross-verify umya-spreadsheet reads the archive cleanly
    let umya_book = umya_spreadsheet::reader::xlsx::read_reader(Cursor::new(&bytes), true)
        .expect("umya-spreadsheet reads error workbook");
    let ws = umya_book.get_sheet_by_name("Errors").unwrap();
    for (i, &kind) in all_kinds.iter().enumerate() {
        assert_eq!(ws.get_value((1, (i + 1) as u32)), kind.to_string());
    }
}

#[test]
fn rejects_illegal_xml_control_characters_in_sheet_names() {
    let mut wb = Workbook::new();
    wb.add_sheet("Bad\u{0001}Name").unwrap();
    let err = wb
        .to_xlsx_bytes()
        .expect_err("illegal XML 1.0 control char in sheet name must fail");
    assert!(
        err.to_string().contains("control character"),
        "unexpected error: {err}"
    );
}

#[test]
fn roundtrip_temporal_types_1900_and_1904_with_subseconds() {
    for date_system in [DateSystem::Excel1900, DateSystem::Excel1904] {
        let mut cfg = WorkbookConfig::interactive();
        cfg.eval.date_system = date_system;
        let mut wb = Workbook::new_with_config(cfg.clone());
        wb.add_sheet("Temporal").unwrap();

        let date = NaiveDate::from_ymd_opt(2026, 3, 15).unwrap();
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2026, 3, 15).unwrap(),
            NaiveTime::from_hms_milli_opt(14, 30, 45, 500).unwrap(),
        );
        let time = NaiveTime::from_hms_milli_opt(9, 15, 30, 250).unwrap();
        let duration = Duration::milliseconds(36 * 3600 * 1000 + 15 * 60 * 1000 + 500);

        wb.set_value("Temporal", 1, 1, LiteralValue::Date(date))
            .unwrap();
        wb.set_value("Temporal", 2, 1, LiteralValue::DateTime(datetime))
            .unwrap();
        wb.set_value("Temporal", 3, 1, LiteralValue::Time(time))
            .unwrap();
        wb.set_value("Temporal", 4, 1, LiteralValue::Duration(duration))
            .unwrap();
        wb.set_formula("Temporal", 1, 2, "=YEAR(A1)").unwrap();
        wb.set_formula("Temporal", 2, 2, "=MONTH(A1)").unwrap();
        wb.set_formula("Temporal", 3, 2, "=DAY(A1)").unwrap();
        wb.set_formula("Temporal", 4, 2, "=HOUR(A2)").unwrap();
        wb.set_formula("Temporal", 5, 2, "=MINUTE(A2)").unwrap();
        wb.set_formula("Temporal", 6, 2, "=SECOND(A2)").unwrap();

        let bytes = wb.to_xlsx_bytes().unwrap();

        // Inspect xl/workbook.xml for workbookPr date1904 flag
        let mut archive = zip::ZipArchive::new(Cursor::new(&bytes)).unwrap();
        let mut workbook_xml = String::new();
        archive
            .by_name("xl/workbook.xml")
            .unwrap()
            .read_to_string(&mut workbook_xml)
            .unwrap();
        assert_eq!(
            workbook_xml.contains(r#"date1904="1""#),
            date_system == DateSystem::Excel1904,
            "workbookPr date1904 mismatch for {date_system:?}: {workbook_xml}"
        );

        // Verify exact serial numbers reload via NativeXlsxAdapter
        let expected_date_serial = date_to_serial_for(date_system, &date);
        let expected_dt_serial = datetime_to_serial_for(date_system, &datetime);
        let expected_time_serial = (9.0 * 3600.0 + 15.0 * 60.0 + 30.25) / 86_400.0;
        let expected_dur_serial = (36.0 * 3600.0 + 15.0 * 60.0 + 0.5) / 86_400.0;

        let reopened = reopen_native_with_config(bytes.clone(), cfg);
        for (row, expected) in [
            (1, expected_date_serial),
            (2, expected_dt_serial),
            (3, expected_time_serial),
            (4, expected_dur_serial),
        ] {
            match reopened.get_value("Temporal", row, 1) {
                Some(LiteralValue::Number(actual)) => {
                    assert!(
                        (actual - expected).abs() < 1e-9,
                        "{date_system:?} row {row}: expected serial {expected}, got {actual}"
                    );
                }
                other => panic!("{date_system:?} row {row}: expected Number, got {other:?}"),
            }
        }

        // Verify native recalculate_xlsx_bytes detects date1904 from xl/workbook.xml automatically
        let recalced = recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::workbook())
            .expect("native recalc of exported temporal workbook");
        assert_eq!(
            recalced.calculation.as_ref().map(|c| c.date_system),
            Some(date_system)
        );
        let mut recalced_adapter = NativeXlsxAdapter::open_bytes(recalced.bytes).unwrap();
        for (row, expected) in [
            (1, 2026.0),
            (2, 3.0),
            (3, 15.0),
            (4, 14.0),
            (5, 30.0),
            (6, 46.0), // Excel SECOND() rounds 45.500s to 46
        ] {
            let cell = recalced_adapter
                .read_cell("Temporal", row, 2)
                .unwrap()
                .expect("formula cell");
            assert_eq!(
                cell.value,
                Some(LiteralValue::Number(expected)),
                "{date_system:?} recalc formula row {row}"
            );
        }

        // Cross-verify umya-spreadsheet reads it cleanly too
        let _umya = umya_spreadsheet::reader::xlsx::read_reader(Cursor::new(&bytes), true)
            .expect("umya reads temporal workbook");
    }
}

#[test]
fn roundtrip_calc_pr_and_empty_workbook_and_large_sheet() {
    // 1. Empty workbook emits valid Sheet1
    let empty_bytes = Workbook::new().to_xlsx_bytes().unwrap();
    let empty_reopened = reopen_native(empty_bytes.clone());
    assert_eq!(empty_reopened.sheet_names(), vec!["Sheet1"]);
    let _umya_empty = umya_spreadsheet::reader::xlsx::read_reader(Cursor::new(&empty_bytes), true)
        .expect("umya reads empty workbook");

    // 2. Iterative calcPr settings round-trip
    let mut cfg = WorkbookConfig::interactive();
    cfg.eval.cycle = CycleConfig::iterate(42, 0.0005);
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("Large").unwrap();

    // 3. Large sheet: 10,000+ cells (2,500 rows x 5 cols = 12,500 cells)
    let mut rows = Vec::with_capacity(2_500);
    for r in 0..2_500u32 {
        rows.push(vec![
            LiteralValue::Int(i64::from(r + 1)),
            LiteralValue::Number((r as f64) * 0.25),
            LiteralValue::Text(format!("row-{r}")),
            LiteralValue::Boolean(r % 2 == 0),
            LiteralValue::Number(-((r as f64) * 1.5)),
        ]);
    }
    wb.set_values("Large", 1, 1, &rows).unwrap();
    wb.set_formula("Large", 2_501, 1, "=SUM(A1:A2500)").unwrap();
    wb.evaluate_all().unwrap();

    let bytes = wb.to_xlsx_bytes().unwrap();
    let mut reopened = reopen_native(bytes);
    assert_eq!(
        reopened.engine().config.cycle,
        CycleConfig::iterate(42, 0.0005)
    );
    assert_eq!(reopened.sheet_dimensions("Large"), Some((2_501, 5)));
    assert_eq!(
        reopened.get_value("Large", 2_500, 3),
        Some(LiteralValue::Text("row-2499".into()))
    );
    assert_eq!(
        reopened.evaluate_cell("Large", 2_501, 1).unwrap(),
        LiteralValue::Number(3_126_250.0)
    );
}
