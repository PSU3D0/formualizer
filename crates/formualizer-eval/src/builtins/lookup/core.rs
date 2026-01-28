//! Classic lookup & reference essentials: MATCH, VLOOKUP, HLOOKUP (Sprint 4 subset)
//!
//! Implementation notes:
//! - MATCH supports match_type: 0 exact, 1 approximate (largest <= lookup), -1 approximate (smallest >= lookup)
//! - Approximate modes assume data sorted ascending (1) or descending (-1); unsorted leads to #N/A like Excel (we don't yet detect unsorted reliably, TODO)
//! - Binary search used for approximate modes for efficiency; linear scan for exact or when data small (<8 elements) to avoid overhead.
//! - VLOOKUP/HLOOKUP wrap MATCH logic; VLOOKUP: vertical first column; HLOOKUP: horizontal first row.
//! - Error propagation: if lookup_value is error -> propagate. If table/range contains errors in non-deciding positions, they don't matter unless selected.
//! - Type coercion: current simple: numbers vs numeric text coerced; text comparison case-insensitive? Excel is case-insensitive for MATCH (without wildcards). We implement case-insensitive for now.
//!   TODO(excel-nuance): refine boolean/text/number coercion differences.

use super::lookup_utils::{cmp_for_lookup, find_exact_index, is_sorted_ascending};
use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::ArgKind;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;
use formualizer_parse::parser::ReferenceType;

fn binary_search_match(slice: &[LiteralValue], needle: &LiteralValue, mode: i32) -> Option<usize> {
    if mode == 0 || slice.is_empty() {
        return None;
    }
    // Only ascending binary search currently (mode 1); descending path kept linear for now.
    if mode == 1 {
        // largest <= needle
        let mut lo = 0usize;
        let mut hi = slice.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match cmp_for_lookup(&slice[mid], needle) {
                Some(c) => {
                    if c > 0 {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                None => {
                    hi = mid;
                }
            }
        }
        if lo == 0 { None } else { Some(lo - 1) }
    } else {
        // -1 mode handled via linear fallback since semantics differ (smallest >=)
        let mut best: Option<usize> = None;
        for (i, v) in slice.iter().enumerate() {
            if let Some(c) = cmp_for_lookup(v, needle) {
                if c == 0 {
                    return Some(i);
                }
                if c >= 0 && best.is_none_or(|b| i < b) {
                    best = Some(i);
                }
            }
        }
        best
    }
}

#[derive(Debug)]
pub struct MatchFn;
impl Function for MatchFn {
    fn name(&self) -> &'static str {
        "MATCH"
    }
    fn min_args(&self) -> usize {
        2
    }
    func_caps!(PURE, LOOKUP);
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // lookup_value (any scalar)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // lookup_array (accepts both references and array literals)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // match_type (optional numeric, default 1)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Number(1.0)),
                },
            ]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            )));
        }
        let cv = args[0].value()?;
        let lookup_value = cv.into_literal();
        if let LiteralValue::Error(e) = lookup_value {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let mut match_type = 1.0; // default
        if args.len() >= 3 {
            let mt_val = args[2].value()?.into_literal();
            if let LiteralValue::Error(e) = mt_val {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            match mt_val {
                LiteralValue::Number(n) => match_type = n,
                LiteralValue::Int(i) => match_type = i as f64,
                LiteralValue::Text(s) => {
                    if let Ok(n) = s.parse::<f64>() {
                        match_type = n;
                    }
                }
                _ => {}
            }
        }
        let mt = if match_type > 0.0 {
            1
        } else if match_type < 0.0 {
            -1
        } else {
            0
        };
        let arr_ref = args[1].as_reference_or_eval().ok();
        if let Some(r) = arr_ref {
            let current_sheet = ctx.current_sheet();
            match ctx.resolve_range_view(&r, current_sheet) {
                Ok(rv) => {
                    if mt == 0 {
                        let wildcard_mode = matches!(lookup_value, LiteralValue::Text(ref s) if s.contains('*') || s.contains('?') || s.contains('~'));
                        if let Some(idx) = super::lookup_utils::find_exact_index_in_view(
                            &rv,
                            &lookup_value,
                            wildcard_mode,
                        )? {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                                (idx + 1) as i64,
                            )));
                        }
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Na),
                        )));
                    }

                    // Fallback for approximate match modes (handled via materialization for now)
                    let mut values: Vec<LiteralValue> = Vec::new();
                    if let Err(e) = rv.for_each_cell(&mut |v| {
                        values.push(v.clone());
                        Ok(())
                    }) {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }

                    // Lightweight unsorted detection for approximate modes
                    let is_sorted = if mt == 1 {
                        is_sorted_ascending(&values)
                    } else if mt == -1 {
                        values
                            .windows(2)
                            .all(|w| cmp_for_lookup(&w[0], &w[1]).is_some_and(|c| c >= 0))
                    } else {
                        true
                    };
                    if !is_sorted {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Na),
                        )));
                    }
                    let idx = if values.len() < 8 {
                        // linear small
                        let mut best: Option<(usize, &LiteralValue)> = None;
                        for (i, v) in values.iter().enumerate() {
                            if let Some(c) = cmp_for_lookup(v, &lookup_value) {
                                // compare candidate to needle
                                if mt == 1 {
                                    // v <= needle
                                    if (c == 0 || c == -1)
                                        && (best.is_none() || i > best.unwrap().0)
                                    {
                                        best = Some((i, v));
                                    }
                                } else {
                                    // -1, v >= needle
                                    if (c == 0 || c == 1) && (best.is_none() || i > best.unwrap().0)
                                    {
                                        best = Some((i, v));
                                    }
                                }
                            }
                        }
                        best.map(|(i, _)| i)
                    } else {
                        binary_search_match(&values, &lookup_value, mt)
                    };
                    match idx {
                        Some(i) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                            (i + 1) as i64,
                        ))),
                        None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Na),
                        ))),
                    }
                }
                Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            }
        } else {
            // Handle array literals and other non-reference values
            let v = args[1].value()?.into_literal();
            let values: Vec<LiteralValue> = match v {
                LiteralValue::Array(rows) => {
                    // Flatten the array (MATCH works on 1D, so take first row or column)
                    if rows.len() == 1 {
                        // Single row - use as-is
                        rows.into_iter().next().unwrap_or_default()
                    } else if rows.iter().all(|r| r.len() == 1) {
                        // Column vector - extract first element of each row
                        rows.into_iter()
                            .filter_map(|r| r.into_iter().next())
                            .collect()
                    } else {
                        // 2D array - flatten row by row
                        rows.into_iter().flatten().collect()
                    }
                }
                other => vec![other],
            };
            let idx = if mt == 0 {
                let wildcard_mode = matches!(lookup_value, LiteralValue::Text(ref s) if s.contains('*') || s.contains('?') || s.contains('~'));
                find_exact_index(&values, &lookup_value, wildcard_mode)
            } else {
                binary_search_match(&values, &lookup_value, mt)
            };
            match idx {
                Some(i) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    (i + 1) as i64,
                ))),
                None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                ))),
            }
        }
    }
}

#[derive(Debug)]
pub struct VLookupFn;
impl Function for VLookupFn {
    fn name(&self) -> &'static str {
        "VLOOKUP"
    }
    fn min_args(&self) -> usize {
        3
    }
    func_caps!(PURE, LOOKUP);
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // lookup_value
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // table_array (accepts both references and array literals)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // col_index_num (strict number)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberStrict,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // range_lookup (optional logical, default FALSE for safer exact default)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Logical],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::Logical,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Boolean(false)),
                },
            ]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            )));
        }
        let lookup_value = args[0].value()?.into_literal();

        // Try to get table as reference, fall back to array literal
        let table_ref_opt = args[1].as_reference_or_eval().ok();
        let col_index = match args[2].value()?.into_literal() {
            LiteralValue::Int(i) => i,
            LiteralValue::Number(n) => n as i64,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
        };
        if col_index < 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let approximate = if args.len() >= 4 {
            match args[3].value()?.into_literal() {
                LiteralValue::Boolean(b) => b,
                _ => true,
            }
        } else {
            false // engine chooses FALSE default (exact) rather than Excel's historical TRUE to avoid silent approximate matches
        };
        // Handle both cell references and array literals
        if let Some(table_ref) = table_ref_opt {
            let (sheet, sr, sc, er, ec) = match &table_ref {
                ReferenceType::Range {
                    sheet,
                    start_row: Some(sr),
                    start_col: Some(sc),
                    end_row: Some(er),
                    end_col: Some(ec),
                    ..
                } => (sheet.clone(), *sr, *sc, *er, *ec),
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Ref),
                    )));
                }
            };
            let current_sheet = ctx.current_sheet();
            let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
            let width = ec - sc + 1;
            if col_index as u32 > width {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }

            let rv = ctx.resolve_range_view(&table_ref, sheet_name)?;
            let rows = rv.dims().0;
            let first_col_view = rv.sub_view(0, 0, rows, 1);
            let row_idx_opt = if !approximate {
                let wildcard_mode = matches!(lookup_value, LiteralValue::Text(ref s) if s.contains('*') || s.contains('?') || s.contains('~'));
                super::lookup_utils::find_exact_index_in_view(
                    &first_col_view,
                    &lookup_value,
                    wildcard_mode,
                )?
            } else {
                // Fallback for approximate mode (requires materializing first column for now)
                let mut first_col: Vec<LiteralValue> = Vec::new();
                first_col_view.for_each_row(&mut |row| {
                    first_col.push(row[0].clone());
                    Ok(())
                })?;
                if first_col.is_empty() {
                    None
                } else {
                    binary_search_match(&first_col, &lookup_value, 1)
                }
            };

            match row_idx_opt {
                Some(i) => {
                    let target_col_idx = (col_index - 1) as usize;
                    Ok(crate::traits::CalcValue::Scalar(
                        rv.get_cell(i, target_col_idx),
                    ))
                }
                None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                ))),
            }
        } else {
            // Handle array literal
            let v = args[1].value()?.into_literal();
            let table: Vec<Vec<LiteralValue>> = match v {
                LiteralValue::Array(rows) => rows,
                other => vec![vec![other]],
            };
            if table.is_empty() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                )));
            }
            let width = table.first().map(|r| r.len()).unwrap_or(0);
            if col_index as usize > width {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }

            // First column values for lookup
            let first_col: Vec<LiteralValue> =
                table.iter().filter_map(|r| r.first().cloned()).collect();
            let row_idx_opt = if !approximate {
                let wildcard_mode = matches!(lookup_value, LiteralValue::Text(ref s) if s.contains('*') || s.contains('?') || s.contains('~'));
                find_exact_index(&first_col, &lookup_value, wildcard_mode)
            } else {
                binary_search_match(&first_col, &lookup_value, 1)
            };

            match row_idx_opt {
                Some(i) => {
                    let target_col_idx = (col_index - 1) as usize;
                    let val = table
                        .get(i)
                        .and_then(|r| r.get(target_col_idx))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                    Ok(crate::traits::CalcValue::Scalar(val))
                }
                None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                ))),
            }
        }
    }
}

#[derive(Debug)]
pub struct HLookupFn;
impl Function for HLookupFn {
    fn name(&self) -> &'static str {
        "HLOOKUP"
    }
    fn min_args(&self) -> usize {
        3
    }
    func_caps!(PURE, LOOKUP);
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // lookup_value
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // table_array (accepts both references and array literals)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // row_index_num (strict number)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberStrict,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // range_lookup (optional logical, default FALSE for safer exact default)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Logical],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::Logical,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Boolean(false)),
                },
            ]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            )));
        }
        let lookup_value = args[0].value()?.into_literal();

        // Try to get table as reference, fall back to array literal
        let table_ref_opt = args[1].as_reference_or_eval().ok();
        let row_index = match args[2].value()?.into_literal() {
            LiteralValue::Int(i) => i,
            LiteralValue::Number(n) => n as i64,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
        };
        if row_index < 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let approximate = if args.len() >= 4 {
            match args[3].value()?.into_literal() {
                LiteralValue::Boolean(b) => b,
                _ => true,
            }
        } else {
            false
        };
        // Handle both cell references and array literals
        if let Some(table_ref) = table_ref_opt {
            let (sheet, sr, sc, er, ec) = match &table_ref {
                ReferenceType::Range {
                    sheet,
                    start_row: Some(sr),
                    start_col: Some(sc),
                    end_row: Some(er),
                    end_col: Some(ec),
                    ..
                } => (sheet.clone(), *sr, *sc, *er, *ec),
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Ref),
                    )));
                }
            };
            let current_sheet = ctx.current_sheet();
            let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
            let height = er - sr + 1;
            let width = ec - sc + 1;
            if row_index as u32 > height {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }
            let rv = ctx.resolve_range_view(&table_ref, sheet_name)?;
            let cols = rv.dims().1;
            let first_row_view = rv.sub_view(0, 0, 1, cols);
            let col_idx_opt = if approximate {
                let mut first_row: Vec<LiteralValue> = Vec::with_capacity(width as usize);
                first_row_view.for_each_row(&mut |row| {
                    if first_row.is_empty() {
                        first_row.extend_from_slice(row);
                    }
                    Ok(())
                })?;
                binary_search_match(&first_row, &lookup_value, 1)
            } else {
                let wildcard_mode = matches!(lookup_value, LiteralValue::Text(ref s) if s.contains('*') || s.contains('?') || s.contains('~'));
                super::lookup_utils::find_exact_index_in_view(
                    &first_row_view,
                    &lookup_value,
                    wildcard_mode,
                )?
            };

            match col_idx_opt {
                Some(i) => {
                    let target_row_idx = (row_index - 1) as usize;
                    Ok(crate::traits::CalcValue::Scalar(
                        rv.get_cell(target_row_idx, i),
                    ))
                }
                None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                ))),
            }
        } else {
            // Handle array literal
            let v = args[1].value()?.into_literal();
            let table: Vec<Vec<LiteralValue>> = match v {
                LiteralValue::Array(rows) => rows,
                other => vec![vec![other]],
            };
            if table.is_empty() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                )));
            }
            let height = table.len();
            if row_index as usize > height {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }

            // First row values for lookup
            let first_row: Vec<LiteralValue> = table.first().cloned().unwrap_or_default();
            let col_idx_opt = if approximate {
                binary_search_match(&first_row, &lookup_value, 1)
            } else {
                let wildcard_mode = matches!(lookup_value, LiteralValue::Text(ref s) if s.contains('*') || s.contains('?') || s.contains('~'));
                find_exact_index(&first_row, &lookup_value, wildcard_mode)
            };

            match col_idx_opt {
                Some(i) => {
                    let target_row_idx = (row_index - 1) as usize;
                    let val = table
                        .get(target_row_idx)
                        .and_then(|r| r.get(i))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                    Ok(crate::traits::CalcValue::Scalar(val))
                }
                None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Na),
                ))),
            }
        }
    }
}

pub fn register_builtins() {
    use crate::function_registry::register_function;
    use std::sync::Arc;
    register_function(Arc::new(MatchFn));
    register_function(Arc::new(VLookupFn));
    register_function(Arc::new(HLookupFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    use std::sync::Arc;
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn match_wildcard_and_descending_and_unsorted() {
        // Wildcard: A1:A4 = "foo", "fob", "bar", "baz"
        let wb = TestWorkbook::new().with_function(Arc::new(MatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("foo".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("fob".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("bar".into()))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Text("baz".into()));
        let ctx = wb.interpreter();
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A4".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(4), Some(1)),
            },
            None,
        );
        let f = ctx.context.get_function("", "MATCH").unwrap();
        // Wildcard *o* matches "foo" (1) and "fob" (2), should return first match (1)
        let pat = lit(LiteralValue::Text("*o*".into()));
        let zero = lit(LiteralValue::Int(0));
        let args = vec![
            ArgumentHandle::new(&pat, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(1));
        // Wildcard b?z matches "baz" (4)
        let pat2 = lit(LiteralValue::Text("b?z".into()));
        let args2 = vec![
            ArgumentHandle::new(&pat2, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v2, LiteralValue::Int(4));
        // No match
        let pat3 = lit(LiteralValue::Text("z*".into()));
        let args3 = vec![
            ArgumentHandle::new(&pat3, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v3 = f
            .dispatch(&args3, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v3, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));

        // Descending approximate: 50,40,30,20,10; match_type = -1
        let wb2 = TestWorkbook::new()
            .with_function(Arc::new(MatchFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(50))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(40))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A5", LiteralValue::Int(10));
        let ctx2 = wb2.interpreter();
        let range2 = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A5".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(5), Some(1)),
            },
            None,
        );
        let minus1 = lit(LiteralValue::Int(-1));
        let thirty = lit(LiteralValue::Int(30));
        let args_desc = vec![
            ArgumentHandle::new(&thirty, &ctx2),
            ArgumentHandle::new(&range2, &ctx2),
            ArgumentHandle::new(&minus1, &ctx2),
        ];
        let v_desc = f
            .dispatch(&args_desc, &ctx2.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v_desc, LiteralValue::Int(3));
        // Descending, not found (needle > max)
        let sixty = lit(LiteralValue::Int(60));
        let args_desc2 = vec![
            ArgumentHandle::new(&sixty, &ctx2),
            ArgumentHandle::new(&range2, &ctx2),
            ArgumentHandle::new(&minus1, &ctx2),
        ];
        let v_desc2 = f
            .dispatch(&args_desc2, &ctx2.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v_desc2, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));

        // Unsorted detection: 10, 30, 20, 40, 50 (not sorted ascending)
        let wb3 = TestWorkbook::new()
            .with_function(Arc::new(MatchFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Int(40))
            .with_cell_a1("Sheet1", "A5", LiteralValue::Int(50));
        let ctx3 = wb3.interpreter();
        let range3 = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A5".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(5), Some(1)),
            },
            None,
        );
        let args_unsorted = vec![
            ArgumentHandle::new(&thirty, &ctx3),
            ArgumentHandle::new(&range3, &ctx3),
        ];
        let v_unsorted = f
            .dispatch(&args_unsorted, &ctx3.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v_unsorted, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
        // Unsorted detection descending: 50, 30, 40, 20, 10
        let wb4 = TestWorkbook::new()
            .with_function(Arc::new(MatchFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(50))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(40))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A5", LiteralValue::Int(10));
        let ctx4 = wb4.interpreter();
        let range4 = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A5".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(5), Some(1)),
            },
            None,
        );
        let args_unsorted_desc = vec![
            ArgumentHandle::new(&thirty, &ctx4),
            ArgumentHandle::new(&range4, &ctx4),
            ArgumentHandle::new(&minus1, &ctx4),
        ];
        let v_unsorted_desc = f
            .dispatch(&args_unsorted_desc, &ctx4.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v_unsorted_desc, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
    }

    #[test]
    fn match_exact_and_approx() {
        let wb = TestWorkbook::new().with_function(Arc::new(MatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Int(40))
            .with_cell_a1("Sheet1", "A5", LiteralValue::Int(50));
        let ctx = wb.interpreter();
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A5".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(5), Some(1)),
            },
            None,
        );
        let f = ctx.context.get_function("", "MATCH").unwrap();
        let thirty = lit(LiteralValue::Int(30));
        let zero = lit(LiteralValue::Int(0));
        let args = vec![
            ArgumentHandle::new(&thirty, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(3));
        let thirty_seven = lit(LiteralValue::Int(37));
        let args = vec![
            ArgumentHandle::new(&thirty_seven, &ctx),
            ArgumentHandle::new(&range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(3));
    }

    #[test]
    fn vlookup_basic() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(VLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("Key1".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Key2".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(100))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(200));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(2), Some(2)),
            },
            None,
        );
        let f = ctx.context.get_function("", "VLOOKUP").unwrap();
        let key2 = lit(LiteralValue::Text("Key2".into()));
        let two = lit(LiteralValue::Int(2));
        let false_lit = lit(LiteralValue::Boolean(false));
        let args = vec![
            ArgumentHandle::new(&key2, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&false_lit, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(200.0));
    }

    #[test]
    fn hlookup_basic() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(HLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("Key1".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("Key2".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(100))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(200));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(2), Some(2)),
            },
            None,
        );
        let f = ctx.context.get_function("", "HLOOKUP").unwrap();
        let key1 = lit(LiteralValue::Text("Key1".into()));
        let two = lit(LiteralValue::Int(2));
        let false_lit = lit(LiteralValue::Boolean(false));
        let args = vec![
            ArgumentHandle::new(&key1, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&false_lit, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(100.0));
    }
}
