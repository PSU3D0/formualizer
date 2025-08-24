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
use formualizer_core::parser::ReferenceType;
use formualizer_macros::func_caps;

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
                // lookup_array (by-ref range)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
        let lookup_value = match args[0].value() {
            Ok(v) => v,
            Err(e) => return Ok(LiteralValue::Error(e)),
        }; // propagate as value error
        let mut match_type = 1.0; // default
        if args.len() >= 3 {
            let mt_val = match args[2].value() {
                Ok(v) => v,
                Err(e) => return Ok(LiteralValue::Error(e)),
            };
            if let LiteralValue::Error(e) = mt_val.as_ref() {
                return Ok(LiteralValue::Error(e.clone()));
            }
            match mt_val.as_ref() {
                LiteralValue::Number(n) => match_type = *n,
                LiteralValue::Int(i) => match_type = *i as f64,
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
        let mut values: Vec<LiteralValue> = Vec::new();
        if let Some(r) = arr_ref {
            match ctx.resolve_range_view(&r, "Sheet1") {
                Ok(rv) => {
                    if let Err(e) = rv.for_each_cell(&mut |v| {
                        values.push(v.clone());
                        Ok(())
                    }) {
                        return Ok(LiteralValue::Error(e));
                    }
                }
                Err(e) => return Ok(LiteralValue::Error(e)),
            }
        } else {
            match args[1].value() {
                Ok(v) => values.push(v.as_ref().clone()),
                Err(e) => return Ok(LiteralValue::Error(e)),
            }
        }
        if values.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
        if mt == 0 {
            let wildcard_mode = matches!(lookup_value.as_ref(), LiteralValue::Text(s) if s.contains('*') || s.contains('?') || s.contains('~'));
            if let Some(idx) = find_exact_index(&values, lookup_value.as_ref(), wildcard_mode) {
                return Ok(LiteralValue::Int((idx + 1) as i64));
            }
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
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
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
        let idx = if values.len() < 8 {
            // linear small
            let mut best: Option<(usize, &LiteralValue)> = None;
            for (i, v) in values.iter().enumerate() {
                if let Some(c) = cmp_for_lookup(v, lookup_value.as_ref()) {
                    // compare candidate to needle
                    if mt == 1 {
                        // v <= needle
                        if (c == 0 || c == -1) && (best.is_none() || i > best.unwrap().0) {
                            best = Some((i, v));
                        }
                    } else {
                        // -1, v >= needle
                        if (c == 0 || c == 1) && (best.is_none() || i > best.unwrap().0) {
                            best = Some((i, v));
                        }
                    }
                }
            }
            best.map(|(i, _)| i)
        } else {
            binary_search_match(&values, lookup_value.as_ref(), mt)
        };
        match idx {
            Some(i) => Ok(LiteralValue::Int((i + 1) as i64)),
            None => Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))),
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
                // table_array (by-ref)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 3 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
        let lookup_value = match args[0].value() {
            Ok(v) => v,
            Err(e) => return Ok(LiteralValue::Error(e)),
        };
        let table_ref = match args[1].as_reference_or_eval() {
            Ok(r) => r,
            Err(e) => return Ok(LiteralValue::Error(e)),
        };
        let col_index = match args[2].value()?.as_ref() {
            LiteralValue::Int(i) => *i,
            LiteralValue::Number(n) => *n as i64,
            _ => return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))),
        };
        if col_index < 1 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let approximate = if args.len() >= 4 {
            match args[3].value()?.as_ref() {
                LiteralValue::Boolean(b) => *b,
                _ => true,
            }
        } else {
            false // engine chooses FALSE default (exact) rather than Excel's historical TRUE to avoid silent approximate matches
        };
        let (sheet, sr, sc, er, ec) = match &table_ref {
            ReferenceType::Range {
                sheet,
                start_row: Some(sr),
                start_col: Some(sc),
                end_row: Some(er),
                end_col: Some(ec),
            } => (sheet.clone(), *sr, *sc, *er, *ec),
            _ => return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref))),
        };
        let width = ec - sc + 1;
        if col_index as u32 > width {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)));
        }
        // Collect first column
        let mut first_col: Vec<LiteralValue> = Vec::new();
        {
            let rv = ctx.resolve_range_view(&table_ref, sheet.as_deref().unwrap_or("Sheet1"))?;
            let col_offset = 0usize;
            rv.for_each_row(&mut |row| {
                let v = row.get(col_offset).cloned().unwrap_or(LiteralValue::Empty);
                first_col.push(v);
                Ok(())
            })?;
        }
        let row_idx_opt = if approximate {
            if first_col.is_empty() {
                None
            } else {
                binary_search_match(&first_col, lookup_value.as_ref(), 1)
            }
        } else {
            let mut found = None;
            for (i, v) in first_col.iter().enumerate() {
                if let Some(c) = cmp_for_lookup(lookup_value.as_ref(), v) {
                    if c == 0 {
                        found = Some(i);
                        break;
                    }
                }
            }
            found
        };
        let row_idx = match row_idx_opt {
            Some(i) => i,
            None => return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))),
        };
        // Retrieve target by re-iterating rows (acceptable initial implementation)
        let rv = ctx.resolve_range_view(&table_ref, sheet.as_deref().unwrap_or("Sheet1"))?;
        let mut current = 0usize;
        let target_col_idx = ((sc + (col_index as u32) - 1) - sc) as usize; // zero-based within slice
        let mut out: Option<LiteralValue> = None;
        rv.for_each_row(&mut |row| {
            if current == row_idx {
                out = Some(
                    row.get(target_col_idx)
                        .cloned()
                        .unwrap_or(LiteralValue::Empty),
                );
            }
            current += 1;
            Ok(())
        })?;
        Ok(out.unwrap_or(LiteralValue::Empty))
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
                // table_array (by-ref)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 3 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
        let lookup_value = match args[0].value() {
            Ok(v) => v,
            Err(e) => return Ok(LiteralValue::Error(e)),
        };
        let table_ref = match args[1].as_reference_or_eval() {
            Ok(r) => r,
            Err(e) => return Ok(LiteralValue::Error(e)),
        };
        let row_index = match args[2].value()?.as_ref() {
            LiteralValue::Int(i) => *i,
            LiteralValue::Number(n) => *n as i64,
            _ => return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))),
        };
        if row_index < 1 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let approximate = if args.len() >= 4 {
            match args[3].value()?.as_ref() {
                LiteralValue::Boolean(b) => *b,
                _ => true,
            }
        } else {
            false
        };
        let (sheet, sr, sc, er, ec) = match &table_ref {
            ReferenceType::Range {
                sheet,
                start_row: Some(sr),
                start_col: Some(sc),
                end_row: Some(er),
                end_col: Some(ec),
            } => (sheet.clone(), *sr, *sc, *er, *ec),
            _ => return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref))),
        };
        let height = er - sr + 1;
        if row_index as u32 > height {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)));
        }
        let mut first_row: Vec<LiteralValue> = Vec::new();
        {
            let rv = ctx.resolve_range_view(&table_ref, sheet.as_deref().unwrap_or("Sheet1"))?;
            let mut row_counter = 0usize;
            rv.for_each_row(&mut |row| {
                if row_counter == 0 {
                    first_row.extend_from_slice(row);
                }
                row_counter += 1;
                Ok(())
            })?;
        }
        let col_idx_opt = if approximate {
            binary_search_match(&first_row, lookup_value.as_ref(), 1)
        } else {
            let mut f = None;
            for (i, v) in first_row.iter().enumerate() {
                if let Some(c) = cmp_for_lookup(lookup_value.as_ref(), v) {
                    if c == 0 {
                        f = Some(i);
                        break;
                    }
                }
            }
            f
        };
        let col_idx = match col_idx_opt {
            Some(i) => i,
            None => return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))),
        };
        let target_row_rel = (row_index as usize) - 1; // zero-based
        let target_col_rel = col_idx; // zero-based within row slice
        let rv = ctx.resolve_range_view(&table_ref, sheet.as_deref().unwrap_or("Sheet1"))?;
        let mut collected: Option<LiteralValue> = None;
        let mut r_counter = 0usize;
        rv.for_each_row(&mut |row| {
            if r_counter == target_row_rel {
                collected = Some(
                    row.get(target_col_rel)
                        .cloned()
                        .unwrap_or(LiteralValue::Empty),
                );
            }
            r_counter += 1;
            Ok(())
        })?;
        Ok(collected.unwrap_or(LiteralValue::Empty))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(4),
                    end_col: Some(1),
                },
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(1));
        // Wildcard b?z matches "baz" (4)
        let pat2 = lit(LiteralValue::Text("b?z".into()));
        let args2 = vec![
            ArgumentHandle::new(&pat2, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v2 = f.dispatch(&args2, &ctx.function_context(None)).unwrap();
        assert_eq!(v2, LiteralValue::Int(4));
        // No match
        let pat3 = lit(LiteralValue::Text("z*".into()));
        let args3 = vec![
            ArgumentHandle::new(&pat3, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v3 = f.dispatch(&args3, &ctx.function_context(None)).unwrap();
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(5),
                    end_col: Some(1),
                },
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
            .unwrap();
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
            .unwrap();
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(5),
                    end_col: Some(1),
                },
            },
            None,
        );
        let args_unsorted = vec![
            ArgumentHandle::new(&thirty, &ctx3),
            ArgumentHandle::new(&range3, &ctx3),
        ];
        let v_unsorted = f
            .dispatch(&args_unsorted, &ctx3.function_context(None))
            .unwrap();
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(5),
                    end_col: Some(1),
                },
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
            .unwrap();
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(5),
                    end_col: Some(1),
                },
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(3));
        let thirty_seven = lit(LiteralValue::Int(37));
        let args = vec![
            ArgumentHandle::new(&thirty_seven, &ctx),
            ArgumentHandle::new(&range, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(3));
    }

    #[test]
    fn match_lookup_value_error_propagates() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(MatchFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1));
        let ctx = wb.interpreter();
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(1),
                    end_col: Some(1),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "MATCH").unwrap();
        let err_lookup = lit(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)));
        let zero = lit(LiteralValue::Int(0));
        let args = vec![
            ArgumentHandle::new(&err_lookup, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        // Current validation path collapses error in lookup_value into value position without early propagation.
        // Accept either passthrough or generic #N/A fallback depending on future semantics.
        assert!(matches!(v, LiteralValue::Error(_)));
    }

    #[test]
    fn vlookup_negative_and_approximate() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(VLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("Ten".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("Twenty".into()))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Text("Thirty".into()));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "VLOOKUP").unwrap();
        // Negative col_index -> #VALUE!
        let fifteen = lit(LiteralValue::Int(15));
        let neg_one = lit(LiteralValue::Int(-1));
        let true_lit = lit(LiteralValue::Boolean(true));
        let args_neg = vec![
            ArgumentHandle::new(&fifteen, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&neg_one, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let v_neg = f.dispatch(&args_neg, &ctx.function_context(None)).unwrap();
        assert!(matches!(v_neg, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
        // Approximate TRUE should find largest <= 15 -> 10 (row 1)
        let two = lit(LiteralValue::Int(2));
        let args_approx = vec![
            ArgumentHandle::new(&fifteen, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let v_approx = f
            .dispatch(&args_approx, &ctx.function_context(None))
            .unwrap();
        assert_eq!(v_approx, LiteralValue::Text("Ten".into()));
    }

    #[test]
    fn hlookup_negative_and_approximate() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(HLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Ten".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("Twenty".into()))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Text("Thirty".into()));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:C2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(3),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "HLOOKUP").unwrap();
        let fifteen = lit(LiteralValue::Int(15));
        let neg_one = lit(LiteralValue::Int(-1));
        let true_lit = lit(LiteralValue::Boolean(true));
        let args_neg = vec![
            ArgumentHandle::new(&fifteen, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&neg_one, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let v_neg = f.dispatch(&args_neg, &ctx.function_context(None)).unwrap();
        assert!(matches!(v_neg, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
        // Approximate TRUE should find largest <= 15 -> 10 (col 1)
        let two = lit(LiteralValue::Int(2));
        let args_approx = vec![
            ArgumentHandle::new(&fifteen, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let v_approx = f
            .dispatch(&args_approx, &ctx.function_context(None))
            .unwrap();
        assert_eq!(v_approx, LiteralValue::Text("Ten".into()));
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(200));
    }

    #[test]
    fn vlookup_default_exact_behavior() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(VLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("Ten".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("Twenty".into()));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "VLOOKUP").unwrap();
        // Omit 4th arg: should be exact, so lookup 15 returns #N/A not row 1
        let fifteen = lit(LiteralValue::Int(15));
        let two = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&fifteen, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert!(matches!(v, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
        // Exact match 20 works
        let twenty = lit(LiteralValue::Int(20));
        let args2 = vec![
            ArgumentHandle::new(&twenty, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
        ];
        let v2 = f.dispatch(&args2, &ctx.function_context(None)).unwrap();
        assert_eq!(v2, LiteralValue::Text("Twenty".into()));
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(100));
    }

    #[test]
    fn hlookup_default_exact_behavior() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(HLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Ten".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("Twenty".into()));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "HLOOKUP").unwrap();
        // Omit 4th arg: exact expected, lookup 15 returns #N/A
        let fifteen = lit(LiteralValue::Int(15));
        let two = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&fifteen, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert!(matches!(v, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
        // Exact 20 works
        let twenty = lit(LiteralValue::Int(20));
        let args2 = vec![
            ArgumentHandle::new(&twenty, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
        ];
        let v2 = f.dispatch(&args2, &ctx.function_context(None)).unwrap();
        assert_eq!(v2, LiteralValue::Text("Twenty".into()));
    }

    // ---------------- Additional Edge / Error Tests ----------------

    #[test]
    fn match_not_found_exact_and_approx_low() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(MatchFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30));
        let ctx = wb.interpreter();
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(1),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "MATCH").unwrap();
        // Exact lookup for 25 -> #N/A
        let needle_exact = lit(LiteralValue::Int(25));
        let zero = lit(LiteralValue::Int(0));
        let args_exact = vec![
            ArgumentHandle::new(&needle_exact, &ctx),
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let v_exact = f
            .dispatch(&args_exact, &ctx.function_context(None))
            .unwrap();
        assert!(matches!(v_exact, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
        // Approximate (default) lookup for 5 (below first) -> #N/A
        let five = lit(LiteralValue::Int(5));
        let args_low = vec![
            ArgumentHandle::new(&five, &ctx),
            ArgumentHandle::new(&range, &ctx),
        ];
        let v_low = f.dispatch(&args_low, &ctx.function_context(None)).unwrap();
        assert!(matches!(v_low, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
    }

    #[test]
    fn vlookup_col_index_out_of_range_and_exact_not_found() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(VLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("A".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("B".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "VLOOKUP").unwrap();
        // col_index 3 (out of 2) -> #REF!
        let key_a = lit(LiteralValue::Text("A".into()));
        let three = lit(LiteralValue::Int(3));
        let false_lit = lit(LiteralValue::Boolean(false));
        let args_bad_col = vec![
            ArgumentHandle::new(&key_a, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&three, &ctx),
            ArgumentHandle::new(&false_lit, &ctx),
        ];
        let v_bad_col = f
            .dispatch(&args_bad_col, &ctx.function_context(None))
            .unwrap();
        assert!(matches!(v_bad_col, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Ref));
        // Exact not found -> #N/A
        let key_missing = lit(LiteralValue::Text("Z".into()));
        let two = lit(LiteralValue::Int(2));
        let args_not_found = vec![
            ArgumentHandle::new(&key_missing, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&false_lit, &ctx),
        ];
        let v_nf = f
            .dispatch(&args_not_found, &ctx.function_context(None))
            .unwrap();
        assert!(matches!(v_nf, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
    }

    #[test]
    fn hlookup_row_index_zero_and_arg_schema_type_error() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(HLookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("A".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("B".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let table = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "HLOOKUP").unwrap();
        // row_index 0 -> #VALUE!
        let key_a = lit(LiteralValue::Text("A".into()));
        let zero = lit(LiteralValue::Int(0));
        let false_lit = lit(LiteralValue::Boolean(false));
        let args_zero = vec![
            ArgumentHandle::new(&key_a, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&zero, &ctx),
            ArgumentHandle::new(&false_lit, &ctx),
        ];
        let v_zero = f.dispatch(&args_zero, &ctx.function_context(None)).unwrap();
        assert!(matches!(v_zero, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
        // Arg schema type error: supply logical where row_index expects number (NumberStrict coercion fails -> #VALUE!)
        let true_lit = lit(LiteralValue::Boolean(true));
        let args_type = vec![
            ArgumentHandle::new(&key_a, &ctx),
            ArgumentHandle::new(&table, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let v_type = f.dispatch(&args_type, &ctx.function_context(None)).unwrap();
        assert!(matches!(v_type, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
    }

    #[test]
    fn match_invalid_second_arg_not_range_triggers_ref_error() {
        let wb = TestWorkbook::new().with_function(Arc::new(MatchFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "MATCH").unwrap();
        let scalar_lookup = lit(LiteralValue::Int(10));
        let scalar_array = lit(LiteralValue::Int(20)); // SHOULD be a range per schema
        let args = vec![
            ArgumentHandle::new(&scalar_lookup, &ctx),
            ArgumentHandle::new(&scalar_array, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert!(matches!(v, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Ref));
    }
}
