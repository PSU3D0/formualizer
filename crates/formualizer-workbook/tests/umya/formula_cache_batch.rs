use crate::common::build_workbook;
use formualizer_eval::engine::DateSystem;
use formualizer_workbook::{
    FormulaCacheUpdate, LiteralValue, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter,
};

fn assert_number_or_text_number(value: Option<LiteralValue>, expected: f64) {
    match value {
        Some(LiteralValue::Number(n)) => assert_eq!(n, expected),
        Some(LiteralValue::Text(s)) => {
            let parsed = s.parse::<f64>().expect("numeric text");
            assert_eq!(parsed, expected);
        }
        other => panic!("expected numeric cache {expected}, got {other:?}"),
    }
}

#[test]
fn umya_set_formula_cached_value_compat_still_works() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(1.0); // A1
        sh.get_cell_mut((2, 1)).set_formula("=A1+1"); // B1
    });

    let mut adapter = UmyaAdapter::open_path(&path).expect("open workbook");
    adapter
        .set_formula_cached_value(
            "Sheet1",
            1,
            2,
            &LiteralValue::Number(2.0),
            DateSystem::Excel1900,
        )
        .expect("single cache write");
    adapter.save().expect("save");

    let mut reopened = UmyaAdapter::open_path(&path).expect("reopen workbook");
    let sheet = reopened.read_sheet("Sheet1").expect("read sheet1");
    let b1 = sheet.cells.get(&(1, 2)).expect("B1");

    assert_eq!(b1.formula.as_deref(), Some("=A1+1"));
    assert_number_or_text_number(b1.value.clone(), 2.0);
}

#[test]
fn umya_formula_cache_batch_updates_multi_sheet_and_preserves_formula_text() {
    let path = build_workbook(|book| {
        let sh1 = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh1.get_cell_mut((1, 1)).set_value_number(1.0); // A1
        sh1.get_cell_mut((1, 2)).set_value_number(2.0); // A2
        sh1.get_cell_mut((2, 1)).set_formula("=SUM(A1:A2)"); // B1

        let _ = book.new_sheet("Sheet2");
        let sh2 = book.get_sheet_by_name_mut("Sheet2").unwrap();
        sh2.get_cell_mut((1, 1)).set_formula("=1+1"); // A1
    });

    let mut adapter = UmyaAdapter::open_path(&path).expect("open workbook");
    let updates = vec![
        FormulaCacheUpdate {
            sheet: "Sheet1".to_string(),
            row: 1,
            col: 2,
            value: LiteralValue::Number(3.0),
        },
        FormulaCacheUpdate {
            sheet: "Sheet2".to_string(),
            row: 1,
            col: 1,
            value: LiteralValue::Number(2.0),
        },
        // Non-formula cell should be ignored and remain 1.
        FormulaCacheUpdate {
            sheet: "Sheet1".to_string(),
            row: 1,
            col: 1,
            value: LiteralValue::Number(999.0),
        },
    ];

    adapter
        .write_formula_caches_batch(&updates, DateSystem::Excel1900)
        .expect("batch cache write");
    adapter.save().expect("save");

    let mut reopened = UmyaAdapter::open_path(&path).expect("reopen workbook");

    let s1 = reopened.read_sheet("Sheet1").expect("read sheet1");
    let s2 = reopened.read_sheet("Sheet2").expect("read sheet2");

    assert_eq!(
        s1.cells.get(&(1, 2)).and_then(|c| c.formula.as_deref()),
        Some("=SUM(A1:A2)")
    );
    assert_eq!(
        s2.cells.get(&(1, 1)).and_then(|c| c.formula.as_deref()),
        Some("=1+1")
    );

    assert_number_or_text_number(s1.cells.get(&(1, 2)).and_then(|c| c.value.clone()), 3.0);
    assert_number_or_text_number(s2.cells.get(&(1, 1)).and_then(|c| c.value.clone()), 2.0);

    // Non-formula cell A1 stays untouched.
    assert_number_or_text_number(s1.cells.get(&(1, 1)).and_then(|c| c.value.clone()), 1.0);
}
