//! Legacy LOOKUP function (vector and array forms).
//!
//! Excel's classic `LOOKUP` always performs approximate matching (largest value
//! less-than-or-equal to the lookup value) against data that must be sorted in
//! ascending order.  Two calling conventions exist:
//!
//! **Vector form** – `LOOKUP(lookup_value, lookup_vector, [result_vector])`
//!   Searches `lookup_vector` (a single row or column) and returns the
//!   corresponding element from `result_vector`.  If `result_vector` is
//!   omitted the match is returned from `lookup_vector` itself.
//!
//! **Array form** – `LOOKUP(lookup_value, array)`
//!   When given a 2-D array with no result vector:
//!   * If width > height  → search the first *row*, return from the last *row*.
//!   * Otherwise          → search the first *column*, return from the last *column*.

use super::lookup_utils::{LookupKey, SearchCells, excel_approximate_search, reference_extent};
use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;
use formualizer_parse::parser::ReferenceType;

/// Searches for a value and returns a corresponding value from another range.
///
/// `LOOKUP` always performs approximate matching against ascending-sorted data.
///
/// # Remarks
/// - **Vector form** `LOOKUP(value, lookup_vec, result_vec)`: searches `lookup_vec`,
///   returns corresponding position from `result_vec`.
/// - **Array form** `LOOKUP(value, array)`: if width > height searches first row
///   and returns from last row; otherwise searches first column and returns from
///   last column.
/// - Data must be sorted ascending; unsorted data may return incorrect results
///   (Excel does not guarantee #N/A for unsorted LOOKUP, but results are
///   undefined).
/// - Returns `#N/A` when the lookup value is smaller than every value in the
///   search range.
///
/// # Examples
/// ```excel
/// =LOOKUP(2,A1:A3,B1:B3)
/// ```
///
/// ```yaml,sandbox
/// title: "Vector form exact hit"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 3
///   B1: "a"
///   B2: "b"
///   B3: "c"
/// formula: '=LOOKUP(2,A1:A3,B1:B3)'
/// expected: "b"
/// ```
///
/// ```yaml,sandbox
/// title: "Vector form approximate"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 3
///   A4: 4
///   A5: 5
/// formula: '=LOOKUP(3.5,A1:A5)'
/// expected: 3
/// ```
///
/// ```yaml,docs
/// related:
///   - VLOOKUP
///   - HLOOKUP
///   - MATCH
/// faq:
///   - q: "Does LOOKUP support exact matching?"
///     a: "No. LOOKUP always performs approximate matching (largest <= lookup value)."
///   - q: "What happens with unsorted data?"
///     a: "Results are undefined. Unlike MATCH, LOOKUP does not guarantee #N/A for unsorted ranges."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: LOOKUP
/// Type: LookupFn
/// Min args: 2
/// Max args: 3
/// Variadic: false
/// Signature: LOOKUP(arg1: any@scalar, arg2: any@range, arg3?: any@range)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=any,required=false,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, LOOKUP
/// [formualizer-docgen:schema:end]
#[derive(Debug)]
pub struct LookupFn;
/// [formualizer-docgen:schema:start]
/// Name: LOOKUP
/// Type: LookupFn
/// Min args: 2
/// Max args: 3
/// Variadic: false
/// Signature: LOOKUP(arg1: any@scalar, arg2: any@range, arg3?: any@range)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=any,required=false,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, LOOKUP
/// [formualizer-docgen:schema:end]
impl Function for LookupFn {
    fn name(&self) -> &'static str {
        "LOOKUP"
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
                // lookup_vector / array (range)
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
                // result_vector (optional range)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new(
                ExcelErrorKind::Na,
            ))));
        }

        // An array (or multi-cell range) of lookup values lifts the call, as
        // in Excel: one LOOKUP per element, giving an array of results.
        let mut vectors = LookupVectors::default();
        match args[0].value()?.into_literal() {
            LiteralValue::Array(needles) => {
                super::lookup_utils::lift_lookup_values(ctx, needles, |needle| {
                    LookupFn::lookup_one(args, ctx, needle, &mut vectors)
                })
            }
            lookup_value => LookupFn::lookup_one(args, ctx, lookup_value, &mut vectors),
        }
    }
}

/// The lookup vector or array of one `LOOKUP` call and what it searches.
struct LookupVector {
    data: Vec<Vec<LiteralValue>>,
    /// The declared shape: a reference's own extent, not its trimmed view.
    rows: usize,
    cols: usize,
    cells: SearchCells,
    is_row_search: bool,
}

impl LookupVector {
    /// Both forms search the same way, on the declared shape: along the
    /// first row when it is wider than tall, down the first column otherwise
    /// (a single row or column is the vector form's usual case), over the
    /// declared length.
    fn read<'b>(
        arg: &ArgumentHandle<'_, 'b>,
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<Self, ExcelError> {
        let read = materialise_range(arg, ctx)?;
        let (rows, cols) = read.declared_shape();
        let is_row_search = cols > rows;
        let cells = if is_row_search {
            SearchCells::new(
                read.data.first().into_iter().flatten(),
                cols,
                ctx.date_system(),
            )
        } else {
            SearchCells::new(
                read.data.iter().filter_map(|r| r.first()),
                rows,
                ctx.date_system(),
            )
        };
        Ok(Self {
            data: read.data,
            rows,
            cols,
            cells,
            is_row_search,
        })
    }

    /// The array form's result: the matched position in the last row of a
    /// row search, or in the last column of a column search.
    fn result_at(&self, pos: usize) -> LiteralValue {
        let (row, col) = if self.is_row_search {
            (self.rows.saturating_sub(1), pos)
        } else {
            (pos, self.cols.saturating_sub(1))
        };
        cell_or_blank(&self.data, row, col)
    }
}

/// The result vector of the vector form, read as Excel reads it.
enum ResultVector {
    /// A reference with one row (one cell included) or one column: its cells
    /// from its first one along that axis. A cell past the data is blank.
    Reference(Vec<LiteralValue>),
    /// An array (a constant or a computed result) with one row or one
    /// column: `#N/A` past its end.
    Array(Vec<LiteralValue>),
    /// A reference or an array with several rows and columns: `#N/A`.
    TwoDimensional,
}

impl ResultVector {
    fn read<'b>(
        arg: &ArgumentHandle<'_, 'b>,
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<Self, ExcelError> {
        let read = materialise_range(arg, ctx)?;
        let (rows, cols) = read.declared_shape();
        let is_reference = read.reference.is_some();
        let values = if rows == 1 {
            read.data.into_iter().next().unwrap_or_default()
        } else if cols == 1 {
            read.data
                .into_iter()
                .filter_map(|r| r.into_iter().next())
                .collect()
        } else {
            return Ok(Self::TwoDimensional);
        };
        Ok(if is_reference {
            Self::Reference(values)
        } else {
            Self::Array(values)
        })
    }

    fn at(&self, pos: usize) -> LiteralValue {
        match self {
            Self::Reference(values) => {
                materialise_empty(values.get(pos).cloned().unwrap_or(LiteralValue::Empty))
            }
            Self::Array(values) => values
                .get(pos)
                .cloned()
                .map(materialise_empty)
                .unwrap_or_else(na),
            Self::TwoDimensional => na(),
        }
    }
}

/// The range arguments of one `LOOKUP` call, each read on first need and then
/// kept, so that an array of lookup values reads them once rather than once
/// per element, and a lookup value that needs one never reads the other.
#[derive(Default)]
struct LookupVectors {
    lookup: Option<Result<LookupVector, ExcelError>>,
    result: Option<Result<ResultVector, ExcelError>>,
}

impl LookupFn {
    /// One `LOOKUP` of a single lookup value (the arity is already checked).
    fn lookup_one<'b>(
        args: &[ArgumentHandle<'_, 'b>],
        ctx: &dyn FunctionContext<'b>,
        lookup_value: LiteralValue,
        vectors: &mut LookupVectors,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if let LiteralValue::Error(e) = lookup_value {
            return Ok(CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let Some(needle) = LookupKey::for_lookup_value(&lookup_value, ctx.date_system()) else {
            return Ok(CalcValue::Scalar(na()));
        };

        let lookup = vectors
            .lookup
            .get_or_insert_with(|| LookupVector::read(&args[1], ctx))
            .as_ref()
            .map_err(Clone::clone)?;

        let Some(pos) = excel_approximate_search(&lookup.cells, &needle) else {
            return Ok(CalcValue::Scalar(na()));
        };

        if args.len() >= 3 {
            let result = vectors
                .result
                .get_or_insert_with(|| ResultVector::read(&args[2], ctx))
                .as_ref()
                .map_err(Clone::clone)?;
            Ok(CalcValue::Scalar(result.at(pos)))
        } else {
            Ok(CalcValue::Scalar(materialise_empty(lookup.result_at(pos))))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A range argument read into a 2-D grid, with the reference it resolved to.
struct Materialised {
    data: Vec<Vec<LiteralValue>>,
    reference: Option<ReferenceType>,
}

impl Materialised {
    /// The shape the argument declares: the extent of its reference (a whole
    /// column spans every row, though its view is trimmed to the used rows),
    /// or the shape of its data for an array, a defined name or a table.
    fn declared_shape(&self) -> (usize, usize) {
        self.reference
            .as_ref()
            .and_then(reference_extent)
            .unwrap_or_else(|| dims(&self.data))
    }
}

/// Materialise a range argument into a 2-D Vec grid.
fn materialise_range<'a, 'b>(
    arg: &ArgumentHandle<'a, 'b>,
    ctx: &dyn FunctionContext<'b>,
) -> Result<Materialised, ExcelError> {
    if let Ok(r) = arg.as_reference_or_eval() {
        let current_sheet = ctx.current_sheet();
        let rv = ctx.resolve_range_view(&r, current_sheet)?;
        if let Some(error) = rv.materialization_error() {
            return Err(error);
        }
        let (rows, cols) = rv.dims();
        let mut data = Vec::with_capacity(rows);
        rv.for_each_row(&mut |row| {
            let mut owned = Vec::with_capacity(cols);
            owned.extend_from_slice(row);
            data.push(owned);
            Ok(())
        })?;
        Ok(Materialised {
            data,
            reference: Some(r),
        })
    } else {
        let data = match arg.value()?.into_literal() {
            LiteralValue::Array(rows) => rows,
            other => vec![vec![other]],
        };
        Ok(Materialised {
            data,
            reference: None,
        })
    }
}

fn dims(data: &[Vec<LiteralValue>]) -> (usize, usize) {
    let rows = data.len();
    let cols = data.first().map(|r| r.len()).unwrap_or(0);
    (rows, cols)
}

/// The cell at `(row, col)` of a grid, blank past its data (the trimmed tail
/// of a whole-column view, or an empty view).
fn cell_or_blank(data: &[Vec<LiteralValue>], row: usize, col: usize) -> LiteralValue {
    data.get(row)
        .and_then(|r| r.get(col))
        .cloned()
        .unwrap_or(LiteralValue::Empty)
}

fn na() -> LiteralValue {
    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))
}

/// Excel materialises empty lookup results as 0.
fn materialise_empty(v: LiteralValue) -> LiteralValue {
    match v {
        LiteralValue::Empty => LiteralValue::Number(0.0),
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

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

    // -- LOOKUP vector form (cell references) -----------------------------

    #[test]
    fn lookup_vector_exact_match() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(LookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("a".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("b".into()))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Text("c".into()));
        let ctx = wb.interpreter();

        let lookup_vec = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(3), Some(1)),
            },
            None,
        );
        let result_vec = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B3".into(),
                reference: ReferenceType::range(None, Some(1), Some(2), Some(3), Some(2)),
            },
            None,
        );

        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_vec, &ctx),
            ArgumentHandle::new(&result_vec, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Text("b".into()));
    }

    #[test]
    fn lookup_vector_approximate() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(LookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Int(4))
            .with_cell_a1("Sheet1", "A5", LiteralValue::Int(5));
        let ctx = wb.interpreter();

        let lookup_vec = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A5".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(5), Some(1)),
            },
            None,
        );

        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let needle = lit(LiteralValue::Number(3.5));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_vec, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        // Approximate: largest <= 3.5 is 3
        assert_eq!(v, LiteralValue::Number(3.0));
    }

    #[test]
    fn lookup_vector_below_min_returns_na() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(LookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20));
        let ctx = wb.interpreter();

        let lookup_vec = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A2".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(2), Some(1)),
            },
            None,
        );

        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(5));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_vec, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
    }

    // -- LOOKUP array form (array literals) --------------------------------

    #[test]
    fn lookup_array_column_search() {
        // 3 rows x 2 cols => searches first column, returns from last column
        let wb = TestWorkbook::new().with_function(Arc::new(LookupFn));
        let ctx = wb.interpreter();

        let arr = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1), LiteralValue::Text("a".into())],
            vec![LiteralValue::Int(2), LiteralValue::Text("b".into())],
            vec![LiteralValue::Int(3), LiteralValue::Text("c".into())],
        ]));

        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&arr, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Text("b".into()));
    }

    #[test]
    fn lookup_array_row_search() {
        // 2 rows x 3 cols => wider, searches first row, returns from last row
        let wb = TestWorkbook::new().with_function(Arc::new(LookupFn));
        let ctx = wb.interpreter();

        let arr = lit(LiteralValue::Array(vec![
            vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
                LiteralValue::Int(3),
            ],
            vec![
                LiteralValue::Text("x".into()),
                LiteralValue::Text("y".into()),
                LiteralValue::Text("z".into()),
            ],
        ]));

        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&arr, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Text("y".into()));
    }

    #[test]
    fn lookup_error_propagation() {
        let wb = TestWorkbook::new().with_function(Arc::new(LookupFn));
        let ctx = wb.interpreter();

        let arr = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
        ]]));
        let needle = lit(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&arr, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
    }

    #[test]
    fn lookup_single_element() {
        let wb = TestWorkbook::new().with_function(Arc::new(LookupFn));
        let ctx = wb.interpreter();

        let arr = lit(LiteralValue::Array(vec![vec![LiteralValue::Int(5)]]));
        let f = ctx.context.get_function("", "LOOKUP").unwrap();

        // Exact single-element match
        let needle = lit(LiteralValue::Int(5));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&arr, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(5));

        // Below single element → #N/A
        let needle_lo = lit(LiteralValue::Int(3));
        let args2 = vec![
            ArgumentHandle::new(&needle_lo, &ctx),
            ArgumentHandle::new(&arr, &ctx),
        ];
        let v2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(v2, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Na));
    }

    #[test]
    fn lookup_text_values() {
        // Text search in ascending order
        let wb = TestWorkbook::new()
            .with_function(Arc::new(LookupFn))
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("apple".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("banana".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("cherry".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(3));
        let ctx = wb.interpreter();

        let lookup_vec = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(3), Some(1)),
            },
            None,
        );
        let result_vec = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B3".into(),
                reference: ReferenceType::range(None, Some(1), Some(2), Some(3), Some(2)),
            },
            None,
        );

        let f = ctx.context.get_function("", "LOOKUP").unwrap();
        let needle = lit(LiteralValue::Text("banana".into()));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_vec, &ctx),
            ArgumentHandle::new(&result_vec, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(2.0));
    }
}
