#![cfg(target_arch = "wasm32")]

use formualizer_wasm::{Parser, Workbook, parse};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

#[wasm_bindgen_test]
fn ragged_arrays_return_parser_errors_not_traps() {
    for formula in ["={1,2;3}", "={1;2,3}", "=SUM({1,2;3})", "={{1,2;3}}"] {
        let error = match parse(formula, None) {
            Err(error) => error,
            Ok(_) => panic!("ragged array must fail parsing"),
        };
        assert!(
            error
                .as_string()
                .unwrap()
                .contains("Array rows must have equal length")
        );
        let mut parser = Parser::new(formula, None).unwrap();
        assert!(parser.parse().is_err());
    }
    assert!(parse("={1,2;3,4}", None).is_ok());
}

#[wasm_bindgen_test]
fn ragged_array_assignment_retains_sheet_state() {
    let wb = Workbook::new(None).unwrap();
    wb.add_sheet("S".to_string()).unwrap();
    let sheet = wb.sheet("S".to_string()).unwrap();
    sheet.set_formula(1, 1, "={1,2;3,4}".to_string()).unwrap();
    wb.evaluate_all().unwrap();
    let source = sheet.get_formula(1, 1).unwrap();
    for formula in ["={1,2;3}", "={1;2,3}", "={1;;2}"] {
        assert!(sheet.set_formula(1, 1, formula.to_string()).is_err());
        assert!(
            wb.set_formula("S".to_string(), 1, 1, formula.to_string())
                .is_err()
        );
        assert_eq!(sheet.get_formula(1, 1).unwrap(), source);
        assert_eq!(sheet.get_value(2, 2).unwrap().as_f64(), Some(4.0));
    }
    sheet.set_value(5, 5, JsValue::from_f64(9.0)).unwrap();
    assert!(sheet.set_formula(5, 5, "={1,2;3}".to_string()).is_err());
    assert_eq!(sheet.get_value(5, 5).unwrap().as_f64(), Some(9.0));
    sheet.set_formula(1, 1, "={5,6;7,8}".to_string()).unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(sheet.get_value(2, 2).unwrap().as_f64(), Some(8.0));
}
