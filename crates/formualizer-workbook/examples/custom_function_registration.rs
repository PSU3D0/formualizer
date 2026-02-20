use formualizer_common::LiteralValue;
use formualizer_common::error::{ExcelError, ExcelErrorKind};
use formualizer_workbook::{CustomFnOptions, Workbook};
use std::sync::Arc;

fn as_f64(value: &LiteralValue) -> f64 {
    match value {
        LiteralValue::Int(v) => *v as f64,
        LiteralValue::Number(v) => *v,
        _ => 0.0,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1")?;

    wb.set_values(
        "Sheet1",
        1,
        1,
        &[
            vec![LiteralValue::Number(10.0), LiteralValue::Number(20.0)],
            vec![LiteralValue::Number(30.0), LiteralValue::Number(40.0)],
        ],
    )?;

    wb.register_custom_function(
        "range_total",
        CustomFnOptions {
            min_args: 1,
            max_args: Some(1),
            ..Default::default()
        },
        Arc::new(
            |args: &[LiteralValue]| -> Result<LiteralValue, ExcelError> {
                let LiteralValue::Array(rows) = &args[0] else {
                    return Err(
                        ExcelError::new(ExcelErrorKind::Value).with_message("expected range input")
                    );
                };

                let total = rows.iter().flatten().map(as_f64).sum::<f64>();
                Ok(LiteralValue::Number(total))
            },
        ),
    )?;

    wb.set_formula("Sheet1", 1, 3, "=RANGE_TOTAL(A1:B2)")?;

    let value = wb.evaluate_cell("Sheet1", 1, 3)?;
    println!("RANGE_TOTAL(A1:B2) = {value:?}");

    println!("Registered functions: {:?}", wb.list_custom_functions());
    wb.unregister_custom_function("range_total")?;

    Ok(())
}
