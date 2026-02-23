use crate::{LiteralValue, Workbook};

/// Evaluate a formula in a minimal workbook and return the resulting scalar value.
///
/// This helper is intended for documentation examples to avoid repetitive setup.
///
/// # Example
///
/// ```rust
/// # use formualizer::doc_examples::eval_scalar;
/// let value = eval_scalar("=SUM(1,2,3)")?;
/// assert_eq!(value, formualizer::LiteralValue::Number(6.0));
/// # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
/// ```
pub fn eval_scalar(
    formula: &str,
) -> Result<LiteralValue, Box<dyn std::error::Error + Send + Sync>> {
    let mut workbook = Workbook::new();
    if !workbook.has_sheet("Sheet1") {
        workbook.add_sheet("Sheet1")?;
    }

    workbook.set_formula("Sheet1", 1, 1, formula)?;
    let value = workbook.evaluate_cell("Sheet1", 1, 1)?;
    Ok(value)
}
