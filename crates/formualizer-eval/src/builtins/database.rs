//! Database functions (D-functions)
//!
//! Excel D-functions perform aggregate operations on a database (range with header row)
//! filtering rows that match specified criteria.
//!
//! Implementations:
//! - DSUM(database, field, criteria) - Sums values in field column matching criteria
//! - DAVERAGE(database, field, criteria) - Averages values in field column matching criteria
//! - DCOUNT(database, field, criteria) - Counts numeric cells in field column matching criteria
//! - DMAX(database, field, criteria) - Maximum value in field column matching criteria
//! - DMIN(database, field, criteria) - Minimum value in field column matching criteria
//!
//! Database structure:
//! - First row contains column headers (field names)
//! - Subsequent rows contain data records
//!
//! Field argument:
//! - String matching a column header (case-insensitive)
//! - Number representing 1-based column index
//!
//! Criteria structure:
//! - First row contains column headers (subset of database headers)
//! - Subsequent rows contain criteria values (OR relationship between rows)
//! - Multiple columns in same row have AND relationship
//! - Supports comparison operators (>, <, >=, <=, <>), wildcards (*, ?)

use super::utils::{ARG_ANY_ONE, coerce_num, criteria_match};
use crate::args::{ArgSchema, CriteriaPredicate, parse_criteria};
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// Aggregation operation type for database functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DAggregate {
    Sum,
    Average,
    Count,
    Max,
    Min,
    Product,
}

/// Resolve the field argument to a 0-based column index within the database.
/// Field can be:
/// - A string matching a column header (case-insensitive)
/// - A number representing 1-based column index
fn resolve_field_index(
    field: &LiteralValue,
    headers: &[LiteralValue],
) -> Result<usize, ExcelError> {
    match field {
        LiteralValue::Text(name) => {
            let name_lower = name.to_ascii_lowercase();
            for (i, h) in headers.iter().enumerate() {
                if let LiteralValue::Text(hdr) = h
                    && hdr.to_ascii_lowercase() == name_lower
                {
                    return Ok(i);
                }
            }
            Err(ExcelError::new_value()
                .with_message(format!("Field '{}' not found in database headers", name)))
        }
        LiteralValue::Number(n) => {
            let idx = *n as i64;
            if idx < 1 || idx as usize > headers.len() {
                return Err(ExcelError::new_value().with_message(format!(
                    "Field index {} out of range (1-{})",
                    idx,
                    headers.len()
                )));
            }
            Ok((idx - 1) as usize)
        }
        LiteralValue::Int(i) => {
            if *i < 1 || *i as usize > headers.len() {
                return Err(ExcelError::new_value().with_message(format!(
                    "Field index {} out of range (1-{})",
                    i,
                    headers.len()
                )));
            }
            Ok((*i - 1) as usize)
        }
        _ => Err(ExcelError::new_value().with_message("Field must be text or number")),
    }
}

/// Parse criteria range into a list of criteria rows.
/// Each row is a vector of (column_index, predicate) pairs.
/// Multiple rows have OR relationship; columns within a row have AND relationship.
fn parse_criteria_range(
    criteria_view: &crate::engine::range_view::RangeView<'_>,
    db_headers: &[LiteralValue],
) -> Result<Vec<Vec<(usize, CriteriaPredicate)>>, ExcelError> {
    let (crit_rows, crit_cols) = criteria_view.dims();
    if crit_rows < 1 || crit_cols < 1 {
        return Ok(vec![]);
    }

    // First row is criteria headers - map to database column indices
    let mut crit_col_map: Vec<Option<usize>> = Vec::with_capacity(crit_cols);
    for c in 0..crit_cols {
        let crit_header = criteria_view.get_cell(0, c);
        if let LiteralValue::Text(name) = &crit_header {
            let name_lower = name.to_ascii_lowercase();
            let mut found = None;
            for (i, h) in db_headers.iter().enumerate() {
                if let LiteralValue::Text(hdr) = h
                    && hdr.to_ascii_lowercase() == name_lower
                {
                    found = Some(i);
                    break;
                }
            }
            crit_col_map.push(found);
        } else if matches!(crit_header, LiteralValue::Empty) {
            crit_col_map.push(None);
        } else {
            // Non-text, non-empty header - try to match as-is
            crit_col_map.push(None);
        }
    }

    // Parse criteria rows (starting from row 1)
    let mut criteria_rows = Vec::new();
    for r in 1..crit_rows {
        let mut row_criteria = Vec::new();
        let mut has_any_criteria = false;

        for (c, db_col) in crit_col_map.iter().enumerate() {
            let crit_val = criteria_view.get_cell(r, c);
            if matches!(crit_val, LiteralValue::Empty) {
                continue;
            }

            if let Some(db_col) = db_col {
                let pred = parse_criteria(&crit_val)?;
                row_criteria.push((*db_col, pred));
                has_any_criteria = true;
            }
        }

        if has_any_criteria {
            criteria_rows.push(row_criteria);
        }
    }

    Ok(criteria_rows)
}

/// Check if a database row matches any of the criteria rows (OR relationship).
/// Each criteria row is a list of (column_index, predicate) pairs (AND relationship).
fn row_matches_criteria(
    db_view: &crate::engine::range_view::RangeView<'_>,
    row: usize,
    criteria_rows: &[Vec<(usize, CriteriaPredicate)>],
) -> bool {
    // If no criteria, all rows match
    if criteria_rows.is_empty() {
        return true;
    }

    // OR relationship between criteria rows
    for crit_row in criteria_rows {
        let mut all_match = true;
        // AND relationship within a criteria row
        for (col_idx, pred) in crit_row {
            let cell_val = db_view.get_cell(row, *col_idx);
            if !criteria_match(pred, &cell_val) {
                all_match = false;
                break;
            }
        }
        if all_match {
            return true;
        }
    }

    false
}

/// Core evaluation function for all D-functions.
fn eval_d_function<'a, 'b>(
    args: &[ArgumentHandle<'a, 'b>],
    _ctx: &dyn FunctionContext<'b>,
    agg_type: DAggregate,
) -> Result<CalcValue<'b>, ExcelError> {
    if args.len() != 3 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value().with_message(format!(
                "D-function expects 3 arguments, got {}",
                args.len()
            )),
        )));
    }

    // Get database range
    let db_view = match args[0].range_view() {
        Ok(v) => v,
        Err(_) => {
            // Try to get as array literal
            let val = args[0].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Database must be a range or array"),
                )));
            }
        }
    };

    let (db_rows, db_cols) = db_view.dims();
    if db_rows < 2 || db_cols < 1 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value()
                .with_message("Database must have headers and at least one data row"),
        )));
    }

    // Get database headers (first row)
    let headers: Vec<LiteralValue> = (0..db_cols).map(|c| db_view.get_cell(0, c)).collect();

    // Get field argument and resolve to column index
    let field_val = args[1].value()?.into_literal();
    let field_idx = resolve_field_index(&field_val, &headers)?;

    // Get criteria range
    let crit_view = match args[2].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[2].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Criteria must be a range or array"),
                )));
            }
        }
    };

    // Parse criteria
    let criteria_rows = parse_criteria_range(&crit_view, &headers)?;

    // Collect matching values from the field column
    let mut values: Vec<f64> = Vec::new();

    // Iterate over data rows (starting from row 1, skipping header)
    for row in 1..db_rows {
        if row_matches_criteria(&db_view, row, &criteria_rows) {
            let cell_val = db_view.get_cell(row, field_idx);

            // For DCOUNT, only count numeric cells
            // For other functions, try to coerce to number
            match &cell_val {
                LiteralValue::Number(n) => values.push(*n),
                LiteralValue::Int(i) => values.push(*i as f64),
                LiteralValue::Boolean(b) => {
                    // Include booleans for DCOUNT only when explicitly numeric context
                    if agg_type != DAggregate::Count {
                        values.push(if *b { 1.0 } else { 0.0 });
                    }
                }
                LiteralValue::Empty => {
                    // Empty cells are skipped for all D-functions
                }
                LiteralValue::Text(s) => {
                    // Try numeric coercion for text
                    if let Ok(n) = coerce_num(&cell_val) {
                        values.push(n);
                    }
                    // Non-numeric text is skipped
                }
                LiteralValue::Error(e) => {
                    // Propagate errors
                    return Ok(CalcValue::Scalar(LiteralValue::Error(e.clone())));
                }
                _ => {}
            }
        }
    }

    // Compute aggregate result
    let result = match agg_type {
        DAggregate::Sum => {
            let sum: f64 = values.iter().sum();
            LiteralValue::Number(sum)
        }
        DAggregate::Average => {
            if values.is_empty() {
                LiteralValue::Error(ExcelError::new_div())
            } else {
                let sum: f64 = values.iter().sum();
                LiteralValue::Number(sum / values.len() as f64)
            }
        }
        DAggregate::Count => {
            // DCOUNT counts only numeric cells
            LiteralValue::Number(values.len() as f64)
        }
        DAggregate::Max => {
            if values.is_empty() {
                LiteralValue::Number(0.0)
            } else {
                let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                LiteralValue::Number(max)
            }
        }
        DAggregate::Min => {
            if values.is_empty() {
                LiteralValue::Number(0.0)
            } else {
                let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
                LiteralValue::Number(min)
            }
        }
        DAggregate::Product => {
            if values.is_empty() {
                LiteralValue::Number(0.0)
            } else {
                let product: f64 = values.iter().product();
                LiteralValue::Number(product)
            }
        }
    };

    Ok(CalcValue::Scalar(result))
}

/// Statistical operation type for database variance/stdev functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DStatOp {
    VarSample,   // DVAR - sample variance (n-1 denominator)
    VarPop,      // DVARP - population variance (n denominator)
    StdevSample, // DSTDEV - sample standard deviation (n-1 denominator)
    StdevPop,    // DSTDEVP - population standard deviation (n denominator)
}

/// Core evaluation function for database statistical functions (DVAR, DVARP, DSTDEV, DSTDEVP).
fn eval_d_stat_function<'a, 'b>(
    args: &[ArgumentHandle<'a, 'b>],
    _ctx: &dyn FunctionContext<'b>,
    stat_op: DStatOp,
) -> Result<CalcValue<'b>, ExcelError> {
    if args.len() != 3 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value().with_message(format!(
                "D-function expects 3 arguments, got {}",
                args.len()
            )),
        )));
    }

    // Get database range
    let db_view = match args[0].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[0].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Database must be a range or array"),
                )));
            }
        }
    };

    let (db_rows, db_cols) = db_view.dims();
    if db_rows < 2 || db_cols < 1 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value()
                .with_message("Database must have headers and at least one data row"),
        )));
    }

    // Get database headers (first row)
    let headers: Vec<LiteralValue> = (0..db_cols).map(|c| db_view.get_cell(0, c)).collect();

    // Get field argument and resolve to column index
    let field_val = args[1].value()?.into_literal();
    let field_idx = resolve_field_index(&field_val, &headers)?;

    // Get criteria range
    let crit_view = match args[2].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[2].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Criteria must be a range or array"),
                )));
            }
        }
    };

    // Parse criteria
    let criteria_rows = parse_criteria_range(&crit_view, &headers)?;

    // Collect matching numeric values from the field column
    let mut values: Vec<f64> = Vec::new();

    for row in 1..db_rows {
        if row_matches_criteria(&db_view, row, &criteria_rows) {
            let cell_val = db_view.get_cell(row, field_idx);

            match &cell_val {
                LiteralValue::Number(n) => values.push(*n),
                LiteralValue::Int(i) => values.push(*i as f64),
                LiteralValue::Boolean(b) => {
                    values.push(if *b { 1.0 } else { 0.0 });
                }
                LiteralValue::Text(s) => {
                    if let Ok(n) = coerce_num(&cell_val) {
                        values.push(n);
                    }
                }
                LiteralValue::Error(e) => {
                    return Ok(CalcValue::Scalar(LiteralValue::Error(e.clone())));
                }
                _ => {}
            }
        }
    }

    // Compute statistical result
    let result = match stat_op {
        DStatOp::VarSample | DStatOp::StdevSample => {
            // Sample variance/stdev requires at least 2 values
            if values.len() < 2 {
                LiteralValue::Error(ExcelError::new_div())
            } else {
                let n = values.len() as f64;
                let mean = values.iter().sum::<f64>() / n;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
                if matches!(stat_op, DStatOp::VarSample) {
                    LiteralValue::Number(variance)
                } else {
                    LiteralValue::Number(variance.sqrt())
                }
            }
        }
        DStatOp::VarPop | DStatOp::StdevPop => {
            // Population variance/stdev requires at least 1 value
            if values.is_empty() {
                LiteralValue::Error(ExcelError::new_div())
            } else {
                let n = values.len() as f64;
                let mean = values.iter().sum::<f64>() / n;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
                if matches!(stat_op, DStatOp::VarPop) {
                    LiteralValue::Number(variance)
                } else {
                    LiteralValue::Number(variance.sqrt())
                }
            }
        }
    };

    Ok(CalcValue::Scalar(result))
}

/// Core evaluation function for DGET - returns single value matching criteria.
fn eval_dget<'a, 'b>(
    args: &[ArgumentHandle<'a, 'b>],
    _ctx: &dyn FunctionContext<'b>,
) -> Result<CalcValue<'b>, ExcelError> {
    if args.len() != 3 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value()
                .with_message(format!("DGET expects 3 arguments, got {}", args.len())),
        )));
    }

    // Get database range
    let db_view = match args[0].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[0].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Database must be a range or array"),
                )));
            }
        }
    };

    let (db_rows, db_cols) = db_view.dims();
    if db_rows < 2 || db_cols < 1 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value()
                .with_message("Database must have headers and at least one data row"),
        )));
    }

    // Get database headers (first row)
    let headers: Vec<LiteralValue> = (0..db_cols).map(|c| db_view.get_cell(0, c)).collect();

    // Get field argument and resolve to column index
    let field_val = args[1].value()?.into_literal();
    let field_idx = resolve_field_index(&field_val, &headers)?;

    // Get criteria range
    let crit_view = match args[2].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[2].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Criteria must be a range or array"),
                )));
            }
        }
    };

    // Parse criteria
    let criteria_rows = parse_criteria_range(&crit_view, &headers)?;

    // Find matching values
    let mut matching_values: Vec<LiteralValue> = Vec::new();

    for row in 1..db_rows {
        if row_matches_criteria(&db_view, row, &criteria_rows) {
            matching_values.push(db_view.get_cell(row, field_idx));
        }
    }

    // DGET returns:
    // - #VALUE! if no match
    // - #NUM! if more than one match
    // - The single value if exactly one match
    let result = if matching_values.is_empty() {
        LiteralValue::Error(ExcelError::new_value().with_message("No record matches criteria"))
    } else if matching_values.len() > 1 {
        LiteralValue::Error(
            ExcelError::new_num().with_message("More than one record matches criteria"),
        )
    } else {
        matching_values.into_iter().next().unwrap()
    };

    Ok(CalcValue::Scalar(result))
}

/// Core evaluation function for DCOUNTA - counts non-blank cells matching criteria.
fn eval_dcounta<'a, 'b>(
    args: &[ArgumentHandle<'a, 'b>],
    _ctx: &dyn FunctionContext<'b>,
) -> Result<CalcValue<'b>, ExcelError> {
    if args.len() != 3 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value()
                .with_message(format!("DCOUNTA expects 3 arguments, got {}", args.len())),
        )));
    }

    // Get database range
    let db_view = match args[0].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[0].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Database must be a range or array"),
                )));
            }
        }
    };

    let (db_rows, db_cols) = db_view.dims();
    if db_rows < 2 || db_cols < 1 {
        return Ok(CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value()
                .with_message("Database must have headers and at least one data row"),
        )));
    }

    // Get database headers (first row)
    let headers: Vec<LiteralValue> = (0..db_cols).map(|c| db_view.get_cell(0, c)).collect();

    // Get field argument and resolve to column index
    let field_val = args[1].value()?.into_literal();
    let field_idx = resolve_field_index(&field_val, &headers)?;

    // Get criteria range
    let crit_view = match args[2].range_view() {
        Ok(v) => v,
        Err(_) => {
            let val = args[2].value()?.into_literal();
            if let LiteralValue::Array(arr) = val {
                crate::engine::range_view::RangeView::from_owned_rows(
                    arr,
                    crate::engine::DateSystem::Excel1900,
                )
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("Criteria must be a range or array"),
                )));
            }
        }
    };

    // Parse criteria
    let criteria_rows = parse_criteria_range(&crit_view, &headers)?;

    // Count non-blank cells in matching rows
    let mut count = 0;

    for row in 1..db_rows {
        if row_matches_criteria(&db_view, row, &criteria_rows) {
            let cell_val = db_view.get_cell(row, field_idx);

            // DCOUNTA counts all non-blank cells (unlike DCOUNT which only counts numbers)
            match &cell_val {
                LiteralValue::Empty => {
                    // Empty cells are NOT counted
                }
                LiteralValue::Text(s) if s.is_empty() => {
                    // Empty strings are treated as blank and NOT counted
                }
                LiteralValue::Error(e) => {
                    // Propagate errors
                    return Ok(CalcValue::Scalar(LiteralValue::Error(e.clone())));
                }
                _ => {
                    // All other values (numbers, non-empty text, booleans) are counted
                    count += 1;
                }
            }
        }
    }

    Ok(CalcValue::Scalar(LiteralValue::Number(count as f64)))
}

/* ─────────────────────────── DSUM ──────────────────────────── */
#[derive(Debug)]
pub struct DSumFn;

/// Sums values in a database field for records that match criteria.
///
/// `DSUM` filters database rows using a criteria range, then adds the selected field values.
///
/// # Remarks
/// - Criteria rows are evaluated with OR semantics; populated criteria columns within one row are ANDed.
/// - `field` resolves by case-insensitive header text or 1-based column index; unknown headers and out-of-range indexes return `#VALUE!`.
/// - Non-numeric values in the target field are ignored unless they coerce to numbers.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Sum revenue for East or West regions"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "West"
///   G3: "East"
/// formula: "=DSUM(A1:E7, \"Revenue\", G1:G3)"
/// expected: 415500
/// ```
///
/// ```yaml,sandbox
/// title: "Sum revenue by field index with numeric criteria"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Units"
///   G2: ">20"
/// formula: "=DSUM(A1:E7, 5, G1:G2)"
/// expected: 488500
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DSUM
/// Type: DSumFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DSUM(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DSumFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DSUM"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_function(args, ctx, DAggregate::Sum)
    }
}

/* ─────────────────────────── DAVERAGE ──────────────────────────── */
#[derive(Debug)]
pub struct DAverageFn;

/// Returns the arithmetic mean of values in a database field for matching records.
///
/// `DAVERAGE` applies criteria filtering first, then averages the numeric values in `field`.
///
/// # Remarks
/// - Criteria rows are OR conditions, while criteria columns in the same row are AND conditions.
/// - `field` can be a case-insensitive header name or a 1-based column index; invalid field resolution returns `#VALUE!`.
/// - If no numeric values match, the function returns `#DIV/0!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Average units for Gadget sales"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Gadget"
/// formula: "=DAVERAGE(A1:E7, \"Units\", G1:G2)"
/// expected: 29
/// ```
///
/// ```yaml,sandbox
/// title: "Average revenue for West or South regions"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "West"
///   G3: "South"
/// formula: "=DAVERAGE(A1:E7, 5, G1:G3)"
/// expected: 97000
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DAVERAGE
/// Type: DAverageFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DAVERAGE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DAverageFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DAVERAGE"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_function(args, ctx, DAggregate::Average)
    }
}

/* ─────────────────────────── DCOUNT ──────────────────────────── */
#[derive(Debug)]
pub struct DCountFn;

/// Counts numeric cells in a database field for records matching criteria.
///
/// `DCOUNT` ignores non-numeric values in the selected field even when the row itself matches.
///
/// # Remarks
/// - Criteria rows are ORed, and criteria columns inside a single row are ANDed.
/// - `field` header lookup is case-insensitive, and numeric `field` uses 1-based indexing; unresolved headers or invalid indexes return `#VALUE!`.
/// - Only numeric field values contribute to the count.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Count numeric revenue entries in East region"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
/// formula: "=DCOUNT(A1:E7, \"Revenue\", G1:G2)"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Count numeric units for Widget or Service products"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Widget"
///   G3: "Service"
/// formula: "=DCOUNT(A1:E7, 4, G1:G3)"
/// expected: 4
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DCOUNT
/// Type: DCountFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DCOUNT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DCountFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DCOUNT"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_function(args, ctx, DAggregate::Count)
    }
}

/* ─────────────────────────── DMAX ──────────────────────────── */
#[derive(Debug)]
pub struct DMaxFn;

/// Returns the largest value in a database field for records matching criteria.
///
/// `DMAX` scans the filtered records and returns the maximum numeric value found in `field`.
///
/// # Remarks
/// - Criteria rows are OR conditions; multiple non-empty criteria columns in one row are AND conditions.
/// - `field` can be a case-insensitive header string or a 1-based column index; failed resolution returns `#VALUE!`.
/// - If no numeric values are matched, this implementation returns `0`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Maximum revenue for West or South"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "West"
///   G3: "South"
/// formula: "=DMAX(A1:E7, \"Revenue\", G1:G3)"
/// expected: 126000
/// ```
///
/// ```yaml,sandbox
/// title: "Maximum units for Widget deals"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Widget"
/// formula: "=DMAX(A1:E7, 4, G1:G2)"
/// expected: 24
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DMAX
/// Type: DMaxFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DMAX(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DMaxFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DMAX"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_function(args, ctx, DAggregate::Max)
    }
}

/* ─────────────────────────── DMIN ──────────────────────────── */
#[derive(Debug)]
pub struct DMinFn;

/// Returns the smallest value in a database field for records matching criteria.
///
/// `DMIN` applies criteria filtering and then evaluates the minimum numeric value from `field`.
///
/// # Remarks
/// - Criteria rows are ORed together; criteria columns on the same row are ANDed.
/// - `field` resolves from a case-insensitive header label or 1-based index, and invalid resolution yields `#VALUE!`.
/// - If no numeric values are matched, this implementation returns `0`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Minimum revenue for East or West"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
///   G3: "West"
/// formula: "=DMIN(A1:E7, \"Revenue\", G1:G3)"
/// expected: 46000
/// ```
///
/// ```yaml,sandbox
/// title: "Minimum units where revenue exceeds 100000"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Revenue"
///   G2: ">100000"
/// formula: "=DMIN(A1:E7, 4, G1:G2)"
/// expected: 22
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DMIN
/// Type: DMinFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DMIN(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DMinFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DMIN"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_function(args, ctx, DAggregate::Min)
    }
}

/* ─────────────────────────── DPRODUCT ──────────────────────────── */
#[derive(Debug)]
pub struct DProductFn;

/// Multiplies values in a database field for records that satisfy criteria.
///
/// `DPRODUCT` filters the database first, then returns the product of numeric values in `field`.
///
/// # Remarks
/// - Criteria rows are evaluated as OR alternatives; criteria columns in one row are AND constraints.
/// - `field` resolves via case-insensitive header text or 1-based column index; unresolved field references return `#VALUE!`.
/// - If no numeric values match, this implementation returns `0`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Product of units in North or South"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "North"
///   G3: "South"
/// formula: "=DPRODUCT(A1:E7, \"Units\", G1:G3)"
/// expected: 486
/// ```
///
/// ```yaml,sandbox
/// title: "Product of units for East or West by index field"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
///   G3: "West"
/// formula: "=DPRODUCT(A1:E7, 4, G1:G3)"
/// expected: 196416
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DPRODUCT
/// Type: DProductFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DPRODUCT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DProductFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DPRODUCT"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_function(args, ctx, DAggregate::Product)
    }
}

/* ─────────────────────────── DSTDEV ──────────────────────────── */
#[derive(Debug)]
pub struct DStdevFn;

/// Returns the sample standard deviation of a database field for matching records.
///
/// `DSTDEV` computes standard deviation with the sample denominator (`n - 1`) after criteria filtering.
///
/// # Remarks
/// - Criteria rows represent OR branches; criteria columns in each row are combined with AND.
/// - `field` is resolved by case-insensitive header text or 1-based column index; invalid field resolution returns `#VALUE!`.
/// - At least two numeric values must match criteria, otherwise the result is `#DIV/0!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Sample stdev of units for East or West"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
///   G3: "West"
/// formula: "=DSTDEV(A1:E7, \"Units\", G1:G3)"
/// expected: 7.847504911329036
/// ```
///
/// ```yaml,sandbox
/// title: "Sample stdev of widget revenue"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Widget"
/// formula: "=DSTDEV(A1:E7, 5, G1:G2)"
/// expected: 19756.85535031659
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DSTDEV
/// Type: DStdevFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DSTDEV(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DStdevFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DSTDEV"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_stat_function(args, ctx, DStatOp::StdevSample)
    }
}

/* ─────────────────────────── DSTDEVP ──────────────────────────── */
#[derive(Debug)]
pub struct DStdevPFn;

/// Returns the population standard deviation of a database field for matching records.
///
/// `DSTDEVP` computes standard deviation with the population denominator (`n`) after criteria filtering.
///
/// # Remarks
/// - Criteria rows are OR branches, and each row's populated criteria columns are ANDed.
/// - `field` can be a case-insensitive header label or 1-based index; invalid lookup returns `#VALUE!`.
/// - At least one numeric value must match criteria, otherwise the result is `#DIV/0!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Population stdev of units for East or West"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
///   G3: "West"
/// formula: "=DSTDEVP(A1:E7, \"Units\", G1:G3)"
/// expected: 6.796138609534093
/// ```
///
/// ```yaml,sandbox
/// title: "Population stdev of widget revenue"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Widget"
/// formula: "=DSTDEVP(A1:E7, 5, G1:G2)"
/// expected: 16131.404843417147
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DSTDEVP
/// Type: DStdevPFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DSTDEVP(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DStdevPFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DSTDEVP"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_stat_function(args, ctx, DStatOp::StdevPop)
    }
}

/* ─────────────────────────── DVAR ──────────────────────────── */
#[derive(Debug)]
pub struct DVarFn;

/// Returns the sample variance of a database field for records matching criteria.
///
/// `DVAR` filters records first, then computes variance using the sample denominator (`n - 1`).
///
/// # Remarks
/// - Criteria rows are OR alternatives; criteria columns within each row are AND constraints.
/// - `field` can be resolved by case-insensitive header text or 1-based index; unresolved fields return `#VALUE!`.
/// - At least two numeric values must match criteria, otherwise the function returns `#DIV/0!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Sample variance of units for East or West"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
///   G3: "West"
/// formula: "=DVAR(A1:E7, \"Units\", G1:G3)"
/// expected: 61.583333333333336
/// ```
///
/// ```yaml,sandbox
/// title: "Sample variance of widget revenue"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Widget"
/// formula: "=DVAR(A1:E7, 5, G1:G2)"
/// expected: 390333333.3333333
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DVAR
/// Type: DVarFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DVAR(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DVarFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DVAR"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_stat_function(args, ctx, DStatOp::VarSample)
    }
}

/* ─────────────────────────── DVARP ──────────────────────────── */
#[derive(Debug)]
pub struct DVarPFn;

/// Returns the population variance of a database field for records matching criteria.
///
/// `DVARP` computes variance with the population denominator (`n`) over filtered records.
///
/// # Remarks
/// - Criteria rows are OR branches; populated criteria cells in the same row are combined with AND.
/// - `field` accepts case-insensitive header text or 1-based index; bad field/header resolution returns `#VALUE!`.
/// - At least one numeric value must match criteria, otherwise the function returns `#DIV/0!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Population variance of units for East or West"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
///   G3: "West"
/// formula: "=DVARP(A1:E7, \"Units\", G1:G3)"
/// expected: 46.1875
/// ```
///
/// ```yaml,sandbox
/// title: "Population variance of widget revenue"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Product"
///   G2: "Widget"
/// formula: "=DVARP(A1:E7, 5, G1:G2)"
/// expected: 260222222.2222222
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DVARP
/// Type: DVarPFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DVARP(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DVarPFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DVARP"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_d_stat_function(args, ctx, DStatOp::VarPop)
    }
}

/* ─────────────────────────── DGET ──────────────────────────── */
#[derive(Debug)]
pub struct DGetFn;

/// Returns a single field value from the only record that matches criteria.
///
/// `DGET` is useful for keyed lookups where criteria are expected to identify exactly one record.
///
/// # Remarks
/// - Criteria rows are OR alternatives; criteria columns inside one row are AND predicates.
/// - `field` resolves from a case-insensitive header name or 1-based index; unresolved field/header references return `#VALUE!`.
/// - Returns `#VALUE!` when no records match and `#NUM!` when multiple records match.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Get salesperson for a unique North Widget record"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   H1: "Product"
///   G2: "North"
///   H2: "Widget"
/// formula: "=DGET(A1:E7, \"Salesperson\", G1:H2)"
/// expected: "Kim"
/// ```
///
/// ```yaml,sandbox
/// title: "Multiple matches return NUM error"
/// grid:
///   A1: "Region"
///   B1: "Salesperson"
///   C1: "Product"
///   D1: "Units"
///   E1: "Revenue"
///   A2: "West"
///   B2: "Diaz"
///   C2: "Widget"
///   D2: 24
///   E2: 126000
///   A3: "East"
///   B3: "Patel"
///   C3: "Gadget"
///   D3: 31
///   E3: 142500
///   A4: "North"
///   B4: "Kim"
///   C4: "Widget"
///   D4: 18
///   E4: 87000
///   A5: "West"
///   B5: "Ramos"
///   C5: "Service"
///   D5: 12
///   E5: 46000
///   A6: "South"
///   B6: "Lee"
///   C6: "Gadget"
///   D6: 27
///   E6: 119000
///   A7: "East"
///   B7: "Noor"
///   C7: "Widget"
///   D7: 22
///   E7: 101000
///   G1: "Region"
///   G2: "East"
/// formula: "=DGET(A1:E7, 5, G1:G2)"
/// expected: "#NUM!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DGET
/// Type: DGetFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DGET(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DGetFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DGET"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_dget(args, ctx)
    }
}

/* ─────────────────────────── DCOUNTA ──────────────────────────── */
#[derive(Debug)]
pub struct DCountAFn;

/// [formualizer-docgen:schema:start]
/// Name: DCOUNTA
/// Type: DCountAFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: DCOUNTA(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for DCountAFn {
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "DCOUNTA"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        false
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        eval_dcounta(args, ctx)
    }
}

/// Register all database functions.
pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(DSumFn));
    crate::function_registry::register_function(Arc::new(DAverageFn));
    crate::function_registry::register_function(Arc::new(DCountFn));
    crate::function_registry::register_function(Arc::new(DMaxFn));
    crate::function_registry::register_function(Arc::new(DMinFn));
    crate::function_registry::register_function(Arc::new(DProductFn));
    crate::function_registry::register_function(Arc::new(DStdevFn));
    crate::function_registry::register_function(Arc::new(DStdevPFn));
    crate::function_registry::register_function(Arc::new(DVarFn));
    crate::function_registry::register_function(Arc::new(DVarPFn));
    crate::function_registry::register_function(Arc::new(DGetFn));
    crate::function_registry::register_function(Arc::new(DCountAFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    fn make_database() -> LiteralValue {
        // Simple database with headers: Name, Age, Salary
        LiteralValue::Array(vec![
            vec![
                LiteralValue::Text("Name".into()),
                LiteralValue::Text("Age".into()),
                LiteralValue::Text("Salary".into()),
            ],
            vec![
                LiteralValue::Text("Alice".into()),
                LiteralValue::Int(30),
                LiteralValue::Int(50000),
            ],
            vec![
                LiteralValue::Text("Bob".into()),
                LiteralValue::Int(25),
                LiteralValue::Int(45000),
            ],
            vec![
                LiteralValue::Text("Carol".into()),
                LiteralValue::Int(35),
                LiteralValue::Int(60000),
            ],
            vec![
                LiteralValue::Text("Dave".into()),
                LiteralValue::Int(30),
                LiteralValue::Int(55000),
            ],
        ])
    }

    fn make_criteria_all() -> LiteralValue {
        // Criteria that matches all (just header, no criteria values)
        LiteralValue::Array(vec![vec![LiteralValue::Text("Name".into())]])
    }

    fn make_criteria_age_30() -> LiteralValue {
        // Criteria: Age = 30
        LiteralValue::Array(vec![
            vec![LiteralValue::Text("Age".into())],
            vec![LiteralValue::Int(30)],
        ])
    }

    fn make_criteria_age_gt_25() -> LiteralValue {
        // Criteria: Age > 25
        LiteralValue::Array(vec![
            vec![LiteralValue::Text("Age".into())],
            vec![LiteralValue::Text(">25".into())],
        ])
    }

    #[test]
    fn dsum_all_salaries() {
        let wb = TestWorkbook::new().with_function(Arc::new(DSumFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Text("Salary".into()));
        let criteria = lit(make_criteria_all());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DSUM").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Sum of all salaries: 50000 + 45000 + 60000 + 55000 = 210000
        assert_eq!(result.into_literal(), LiteralValue::Number(210000.0));
    }

    #[test]
    fn dsum_age_30() {
        let wb = TestWorkbook::new().with_function(Arc::new(DSumFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Text("Salary".into()));
        let criteria = lit(make_criteria_age_30());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DSUM").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Sum of salaries where Age = 30: 50000 + 55000 = 105000
        assert_eq!(result.into_literal(), LiteralValue::Number(105000.0));
    }

    #[test]
    fn daverage_age_gt_25() {
        let wb = TestWorkbook::new().with_function(Arc::new(DAverageFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Text("Salary".into()));
        let criteria = lit(make_criteria_age_gt_25());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DAVERAGE").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Average of salaries where Age > 25: (50000 + 60000 + 55000) / 3 = 55000
        assert_eq!(result.into_literal(), LiteralValue::Number(55000.0));
    }

    #[test]
    fn dcount_age_30() {
        let wb = TestWorkbook::new().with_function(Arc::new(DCountFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Text("Salary".into()));
        let criteria = lit(make_criteria_age_30());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DCOUNT").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Count of numeric cells in Salary where Age = 30: 2
        assert_eq!(result.into_literal(), LiteralValue::Number(2.0));
    }

    #[test]
    fn dmax_all() {
        let wb = TestWorkbook::new().with_function(Arc::new(DMaxFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Text("Salary".into()));
        let criteria = lit(make_criteria_all());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DMAX").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Max salary: 60000
        assert_eq!(result.into_literal(), LiteralValue::Number(60000.0));
    }

    #[test]
    fn dmin_all() {
        let wb = TestWorkbook::new().with_function(Arc::new(DMinFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Text("Salary".into()));
        let criteria = lit(make_criteria_all());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DMIN").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Min salary: 45000
        assert_eq!(result.into_literal(), LiteralValue::Number(45000.0));
    }

    #[test]
    fn dsum_field_by_index() {
        let wb = TestWorkbook::new().with_function(Arc::new(DSumFn));
        let ctx = interp(&wb);

        let db = lit(make_database());
        let field = lit(LiteralValue::Int(3)); // Column 3 = Salary
        let criteria = lit(make_criteria_all());

        let args = vec![
            crate::traits::ArgumentHandle::new(&db, &ctx),
            crate::traits::ArgumentHandle::new(&field, &ctx),
            crate::traits::ArgumentHandle::new(&criteria, &ctx),
        ];

        let f = ctx.context.get_function("", "DSUM").unwrap();
        let result = f.dispatch(&args, &ctx.function_context(None)).unwrap();

        // Sum of all salaries: 210000
        assert_eq!(result.into_literal(), LiteralValue::Number(210000.0));
    }
}
