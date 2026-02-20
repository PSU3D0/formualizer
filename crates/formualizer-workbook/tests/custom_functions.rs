use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_workbook::{CustomFnOptions, Workbook, WorkbookMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn workbook() -> Workbook {
    Workbook::new_with_mode(WorkbookMode::Ephemeral)
}

fn as_number(value: &LiteralValue) -> f64 {
    match value {
        LiteralValue::Number(n) => *n,
        LiteralValue::Int(i) => *i as f64,
        other => panic!("expected number, got {other:?}"),
    }
}

#[test]
fn custom_function_lifecycle_register_list_unregister() {
    let mut wb = workbook();

    wb.register_custom_function(
        "add_one",
        CustomFnOptions::default(),
        Arc::new(
            |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Ok(LiteralValue::Number(1.0))
            },
        ),
    )
    .unwrap();

    let listed = wb.list_custom_functions();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "ADD_ONE");

    wb.unregister_custom_function("Add_One").unwrap();
    assert!(wb.list_custom_functions().is_empty());

    let err = wb
        .unregister_custom_function("add_one")
        .expect_err("unregistering missing function should fail");
    assert_eq!(err.kind, ExcelErrorKind::Name);
}

#[test]
fn duplicate_registration_conflicts_case_insensitive() {
    let mut wb = workbook();

    wb.register_custom_function(
        "dupe",
        CustomFnOptions::default(),
        Arc::new(
            |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Ok(LiteralValue::Number(1.0))
            },
        ),
    )
    .unwrap();

    let err = wb
        .register_custom_function(
            "DUPE",
            CustomFnOptions::default(),
            Arc::new(
                |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                    Ok(LiteralValue::Number(2.0))
                },
            ),
        )
        .expect_err("duplicate function should fail");
    assert_eq!(err.kind, ExcelErrorKind::Name);
}

#[test]
fn case_insensitive_lookup_scalar_args_and_scalar_return() {
    let mut wb = workbook();

    wb.register_custom_function(
        "MiXeDcAsE",
        CustomFnOptions {
            min_args: 1,
            max_args: Some(1),
            ..Default::default()
        },
        Arc::new(
            |args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Ok(LiteralValue::Number(as_number(&args[0]) + 1.0))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=mixedcase(41)").unwrap();
    let value = wb.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(value, LiteralValue::Number(42.0));
}

#[test]
fn local_override_requires_explicit_allow_flag() {
    let mut wb = workbook();

    let err = wb
        .register_custom_function(
            "SUM",
            CustomFnOptions::default(),
            Arc::new(
                |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                    Ok(LiteralValue::Number(999.0))
                },
            ),
        )
        .expect_err("builtin override should be blocked by default");
    assert_eq!(err.kind, ExcelErrorKind::Name);

    wb.register_custom_function(
        "SUM",
        CustomFnOptions {
            allow_override_builtin: true,
            ..Default::default()
        },
        Arc::new(
            |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Ok(LiteralValue::Number(999.0))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=SUM(1,2)").unwrap();
    let value = wb.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(value, LiteralValue::Number(999.0));
}

#[test]
fn falls_back_to_global_builtin_when_local_absent() {
    let mut wb = workbook();
    wb.set_formula("Sheet1", 1, 1, "=SUM(1,2)").unwrap();
    let value = wb.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(value, LiteralValue::Number(3.0));
}

#[test]
fn materializes_range_args_by_value_and_supports_array_return() {
    let mut wb = workbook();
    wb.set_values(
        "Sheet1",
        1,
        1,
        &[
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ],
    )
    .unwrap();

    let seen = Arc::new(Mutex::new(None));
    let seen_for_handler = seen.clone();
    wb.register_custom_function(
        "RANGE_SUM",
        CustomFnOptions {
            min_args: 1,
            max_args: Some(1),
            ..Default::default()
        },
        Arc::new(
            move |args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                *seen_for_handler.lock().unwrap() = Some(args[0].clone());
                match &args[0] {
                    LiteralValue::Array(rows) => {
                        let total = rows
                            .iter()
                            .flatten()
                            .map(as_number)
                            .fold(0.0, |acc, n| acc + n);
                        Ok(LiteralValue::Number(total))
                    }
                    _ => Err(ExcelError::new(ExcelErrorKind::Value).with_message("expected array")),
                }
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 3, "=RANGE_SUM(A1:B2)").unwrap();
    assert_eq!(
        wb.evaluate_cell("Sheet1", 1, 3).unwrap(),
        LiteralValue::Number(10.0)
    );

    let seen_arg = seen
        .lock()
        .unwrap()
        .clone()
        .expect("handler should be called");
    assert_eq!(
        seen_arg,
        LiteralValue::Array(vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ])
    );

    wb.register_custom_function(
        "MAKE_GRID",
        CustomFnOptions::default(),
        Arc::new(
            |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Ok(LiteralValue::Array(vec![
                    vec![LiteralValue::Number(7.0), LiteralValue::Number(8.0)],
                    vec![LiteralValue::Number(9.0), LiteralValue::Number(10.0)],
                ]))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 4, 1, "=MAKE_GRID()").unwrap();
    wb.evaluate_all().unwrap();

    assert_eq!(
        wb.get_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(9.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn arity_validation_reports_value_error() {
    let mut wb = workbook();

    wb.register_custom_function(
        "TAKES_TWO",
        CustomFnOptions {
            min_args: 2,
            max_args: Some(2),
            ..Default::default()
        },
        Arc::new(
            |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Ok(LiteralValue::Number(0.0))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=TAKES_TWO(1)").unwrap();
    let value = wb.evaluate_cell("Sheet1", 1, 1).unwrap();
    match value {
        LiteralValue::Error(err) => assert_eq!(err.kind, ExcelErrorKind::Value),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn callback_error_is_propagated() {
    let mut wb = workbook();

    wb.register_custom_function(
        "FAILS",
        CustomFnOptions::default(),
        Arc::new(
            |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                Err(ExcelError::new(ExcelErrorKind::Num).with_message("bad input"))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=FAILS()").unwrap();
    let value = wb.evaluate_cell("Sheet1", 1, 1).unwrap();
    match value {
        LiteralValue::Error(err) => {
            assert_eq!(err.kind, ExcelErrorKind::Num);
        }
        other => panic!("expected propagated error, got {other:?}"),
    }
}

#[test]
fn volatile_custom_function_recalculates_each_pass() {
    let mut wb = workbook();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_handler = calls.clone();

    wb.register_custom_function(
        "VOL_COUNT",
        CustomFnOptions {
            volatile: true,
            ..Default::default()
        },
        Arc::new(
            move |_args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                let next = calls_for_handler.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(LiteralValue::Number(next as f64))
            },
        ),
    )
    .unwrap();

    wb.set_formula("Sheet1", 1, 1, "=VOL_COUNT()").unwrap();

    wb.evaluate_all().unwrap();
    let first = wb
        .get_value("Sheet1", 1, 1)
        .expect("value after first eval");

    wb.evaluate_all().unwrap();
    let second = wb
        .get_value("Sheet1", 1, 1)
        .expect("value after second eval");

    assert_ne!(first, second);
}
