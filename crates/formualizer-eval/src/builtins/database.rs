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
                if let LiteralValue::Text(hdr) = h {
                    if hdr.to_ascii_lowercase() == name_lower {
                        return Ok(i);
                    }
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
                if let LiteralValue::Text(hdr) = h {
                    if hdr.to_ascii_lowercase() == name_lower {
                        found = Some(i);
                        break;
                    }
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

        for c in 0..crit_cols {
            let crit_val = criteria_view.get_cell(r, c);
            if matches!(crit_val, LiteralValue::Empty) {
                continue;
            }

            if let Some(db_col) = crit_col_map[c] {
                let pred = parse_criteria(&crit_val)?;
                row_criteria.push((db_col, pred));
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
