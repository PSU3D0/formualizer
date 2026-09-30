#![cfg(target_arch = "wasm32")]

use formualizer_wasm::Workbook;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

fn render(value: JsValue) -> String {
    if let Some(n) = value.as_f64() {
        return format!("{n}");
    }
    value.as_string().unwrap_or_else(|| format!("{value:?}"))
}

#[wasm_bindgen_test]
fn iferror_and_ifna_replace_array_elements() {
    let wb = Workbook::new(None).unwrap();
    let sheet = wb.sheet("S".to_string()).unwrap();
    for (row, value) in [(1, 1.0), (2, 0.0), (3, 2.0)] {
        sheet.set_value(row, 1, JsValue::from_f64(value)).unwrap();
    }
    sheet.set_value(1, 3, JsValue::from_f64(1.0)).unwrap();
    sheet.set_formula(2, 3, "=NA()".to_string()).unwrap();
    sheet.set_formula(3, 3, "=1/0".to_string()).unwrap();
    sheet
        .set_formula(1, 5, "=IFERROR(1/A1:A3,-1)".to_string())
        .unwrap();
    sheet
        .set_formula(1, 6, "=IFNA(C1:C3,0)".to_string())
        .unwrap();
    sheet
        .set_formula(1, 7, "=SUM(IFERROR(1/A1:A3,0))".to_string())
        .unwrap();
    sheet
        .set_formula(5, 1, "=IFERROR(1/{0,4},8)".to_string())
        .unwrap();
    wb.evaluate_all().unwrap();

    let column = |col| {
        (1..=3)
            .map(|row| render(sheet.get_value(row, col).unwrap()))
            .collect::<Vec<_>>()
            .join(",")
    };
    assert_eq!(column(5), "1,-1,0.5");
    assert_eq!(column(6), "1,0,#DIV/0!");
    assert_eq!(render(sheet.get_value(1, 7).unwrap()), "1.5");
    assert_eq!(render(sheet.get_value(5, 1).unwrap()), "8");
    assert_eq!(render(sheet.get_value(5, 2).unwrap()), "0.25");
}
