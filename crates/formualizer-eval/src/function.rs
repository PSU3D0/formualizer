//! formualizer-eval/src/function.rs
// New home for the core `Function` trait and its capability flags.

use core::panic;

use crate::{
    args::ArgSchema,
    function_contract::{FunctionDependencyContract, FunctionSemanticContract},
    traits::ArgumentHandle,
};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::ReferenceType;

/// One-pass result for a function used where either a reference or a value is valid.
#[doc(hidden)]
#[derive(Clone)]
pub enum FunctionResolution<'a> {
    Reference(ReferenceType),
    ReferenceError(ExcelError),
    Value(crate::traits::CalcValue<'a>),
}

pub(crate) fn resolution_to_reference(
    result: Result<FunctionResolution<'_>, ExcelError>,
) -> Option<Result<ReferenceType, ExcelError>> {
    match result {
        Ok(FunctionResolution::Reference(reference)) => Some(Ok(reference)),
        Ok(FunctionResolution::ReferenceError(error)) | Err(error) => Some(Err(error)),
        Ok(FunctionResolution::Value(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            error,
        )))) => Some(Err(error)),
        Ok(FunctionResolution::Value(_)) => None,
    }
}

bitflags::bitflags! {
    /// Describes the capabilities and properties of a function.
    ///
    /// This allows the engine to select optimal evaluation paths (e.g., vectorized,
    /// parallel, GPU) and to enforce semantic contracts at compile time.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct FnCaps: u32 {
        // --- Semantics ---
        /// The function always produces the same output for the same input and has no
        /// side effects. This is the default for most functions.
        const PURE          = 0b0000_0000_0001;
        /// The function's output can change even with the same inputs (e.g., `RAND()`,
        /// `NOW()`). Volatile functions are re-evaluated on every sheet change.
        const VOLATILE      = 0b0000_0000_0010;

        // --- Shape / Evaluation Strategy ---
        /// The function reduces a range of inputs to a single value (e.g., `SUM`, `AVERAGE`).
        const REDUCTION     = 0b0000_0000_0100;
        /// The function operates on each element of its input ranges independently
        /// (e.g., `SIN`, `ABS`).
        const ELEMENTWISE   = 0b0000_0000_1000;
        /// The function operates on a sliding window over its input (e.g., `MOVING_AVERAGE`).
        const WINDOWED      = 0b0000_0001_0000;
        /// The function performs a lookup or search operation (e.g., `VLOOKUP`).
        const LOOKUP        = 0b0000_0010_0000;

        // --- Input Data Types ---
        /// The function primarily operates on numbers. The engine can prepare
        /// optimized numeric stripes (`&[f64]`) for it.
        const NUMERIC_ONLY  = 0b0000_0100_0000;
        /// The function primarily operates on booleans.
        const BOOL_ONLY     = 0b0000_1000_0000;

        // --- Backend Optimizations ---
        /// The function has an implementation suitable for SIMD vectorization.
        const SIMD_OK       = 0b0001_0000_0000;
        /// The function can process input as a stream, without materializing the
        /// entire range in memory.
        const STREAM_OK     = 0b0010_0000_0000;
        /// The function has a GPU-accelerated implementation.
        const GPU_OK        = 0b0100_0000_0000;

        // --- Reference semantics ---
        /// The function can return a reference (to a cell/range/table) when
        /// evaluated in a reference context. When used in a value context,
        /// engines may materialize the reference to a `LiteralValue`.
    const RETURNS_REFERENCE = 0b1000_0000_0000;

    // --- Planning / Interpreter parallelism hints ---
    /// The function enforces left-to-right evaluation and early-exit semantics.
    /// The planner must not evaluate arguments in parallel nor reorder them.
    const SHORT_CIRCUIT  = 0b0001_0000_0000_0000;
    /// It is safe and potentially profitable to evaluate arguments in parallel.
    /// The engine should still fold results in argument order for determinism.
    const PARALLEL_ARGS  = 0b0010_0000_0000_0000;
    /// It is safe to chunk and process input windows in parallel (e.g., SUMIFS).
    /// It is safe to chunk and process input windows in parallel (e.g., SUMIFS).
    const PARALLEL_CHUNKS= 0b0100_0000_0000_0000;
    /// Function has dynamic dependencies determined at runtime (e.g. INDIRECT, OFFSET).
    const DYNAMIC_DEPENDENCY = 0b1000_0000_0000_0000;
    /// Function establishes lexical bindings or evaluates a local call environment.
    const LOCAL_ENVIRONMENT = 0b0001_0000_0000_0000_0000;
    /// Function can produce a multi-cell dynamic-array result.
    const MAY_SPILL = 0b0010_0000_0000_0000_0000;
    }
}

/// Revised, object-safe trait for all Excel-style functions.
///
/// This trait uses a capability-based model (`FnCaps`) to declare function
/// properties, enabling the evaluation engine to select the most optimal
/// execution path (e.g., scalar, vectorized, parallel).
/// Engine-internal identity of a built-in function with a family kernel.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FamilyKernel {
    Sum,
    Average,
    /// `IF`: lifted element-wise (P2-M3), not a range kernel.
    If,
    /// `SUMIF(S)`, `COUNTIF(S)`, `AVERAGEIF(S)`: a family run memoizes by
    /// its varying arguments (the result is a function of them and of the
    /// absolute ranges).
    CriteriaAggregate,
    /// `VLOOKUP`, `HLOOKUP`, `MATCH`: memoized like `CriteriaAggregate`.
    Lookup,
    /// Scalar builtins the elementwise lift runs on typed lanes (P2-M3).
    Round,
    Abs,
    Min,
    Max,
    And,
    Or,
    IfError,
    /// `COUNT`: a windowed range kernel.
    Count,
    /// Program 3: type tests and date parts on typed lanes (a clean
    /// operand's result without the walk).
    IsNumber,
    IsText,
    IsLogical,
    IsBlank,
    IsError,
    IsErr,
    IsNa,
    Year,
    Month,
    Day,
    Weekday,
}

pub trait Function: Send + Sync + 'static {
    /// Capability flags for this function
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }

    /// Engine-internal: the family kernel that reproduces this function
    /// exactly over a family run. Only the built-in implementations return
    /// `Some`; an override registered under the same name keeps `None`, so
    /// the engine evaluates it through `eval`.
    #[doc(hidden)]
    fn family_kernel(&self) -> Option<FamilyKernel> {
        None
    }

    fn name(&self) -> &'static str;
    fn namespace(&self) -> &'static str {
        ""
    }
    fn min_args(&self) -> usize {
        0
    }
    fn variadic(&self) -> bool {
        false
    }
    fn volatile(&self) -> bool {
        self.caps().contains(FnCaps::VOLATILE)
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        if self.min_args() > 0 {
            panic!("Non-zero min_args must have a valid arg_schema");
        } else {
            &[]
        }
    }

    /// Optional list of additional alias names (case-insensitive) that should resolve to this
    /// function. Default: empty slice. Implementors can override to expose legacy names.
    /// Returned slice must have 'static lifetime (typically a static array reference).
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Optional dependency contract for passive planning/FormulaPlane analysis.
    ///
    /// The default is deliberately conservative: functions that do not opt in
    /// must not receive dependency-summary optimization. Implementations should
    /// return `Some` only for arities and argument roles they can describe
    /// without under-approximating dependencies.
    fn dependency_contract(&self, _arity: usize) -> Option<FunctionDependencyContract> {
        None
    }

    /// Explicit semantic classification for this call arity.
    ///
    /// The public default is intentionally untrusted. The registry supplies a
    /// sealed default only for crate-owned builtin registration.
    fn semantic_contract(&self, _arity: usize) -> Option<FunctionSemanticContract> {
        None
    }

    #[inline]
    fn function_salt(&self) -> u64 {
        // Stable hash of function name + namespace
        let full_name = if self.namespace().is_empty() {
            self.name().to_string()
        } else {
            format!("{}::{}", self.namespace(), self.name())
        };
        crate::rng::fnv1a64(full_name.as_bytes())
    }

    /// Derive an eval-internal scalar format annotation for this call.
    /// Functions drop annotations by default; selection and temporal constructors override it.
    fn propagate_format(
        &self,
        result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        let _ = result;
        None
    }

    fn apply_format_propagation<'a>(
        &self,
        result: crate::traits::CalcValue<'a>,
    ) -> crate::traits::CalcValue<'a> {
        let format = self.propagate_format(&result);
        result.with_format(format)
    }

    /// The unified evaluation path.
    ///
    /// This method replaces the separate scalar, fold, and map paths.
    /// Functions use the provided `ArgumentHandle`s to access inputs as either
    /// scalars or `RangeView`s (Arrow-backed virtual ranges).
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError>;

    /// Optional reference result path. Only called by the interpreter/engine
    /// when the callsite expects a reference (e.g., range combinators, by-ref
    /// argument positions, or spill sources).
    ///
    /// Default implementation returns `None`, indicating the function does not
    /// support returning references. Functions that set `RETURNS_REFERENCE`
    /// should override this.
    fn eval_reference<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Option<Result<formualizer_parse::parser::ReferenceType, ExcelError>> {
        None
    }

    /// Resolve a reference-capable function while preserving scalar-selector caching.
    ///
    /// The fallback deliberately re-enters the caller's ordinary value path. That
    /// preserves dispatch validation and array lifting for existing functions whose
    /// `eval_reference` declines a particular argument shape. Array-valued selectors
    /// may be evaluated once by each path.
    fn resolve_reference_or_value<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::FunctionContext<'b>,
        value_fallback: &dyn Fn() -> Result<crate::traits::CalcValue<'b>, ExcelError>,
    ) -> Result<FunctionResolution<'b>, ExcelError> {
        match self.eval_reference(args, ctx) {
            Some(Ok(reference)) => Ok(FunctionResolution::Reference(reference)),
            Some(Err(error)) => Ok(FunctionResolution::ReferenceError(error)),
            None => value_fallback().map(FunctionResolution::Value),
        }
    }

    /// Dispatch to the unified evaluation path with automatic argument validation.
    fn dispatch<'a, 'b, 'c>(
        &self,
        args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // Short-circuit functions (IF/IFS/CHOOSE/SWITCH/AND/OR, ...) evaluate
        // their arguments lazily inside `eval`; eagerly materializing every
        // argument here would execute reads in untaken branches (defeating the
        // documented short-circuit semantics) and double-evaluate taken ones.
        // Their schemas are Any-kind with no per-arg coercion, so per-argument
        // validation cannot fail; only the min-arity check is meaningful.
        // (LET/LAMBDA already bypass validation via `dispatch` overrides for
        // the same reason.)
        if self.caps().contains(FnCaps::SHORT_CIRCUIT) {
            if args.len() < self.min_args() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(formualizer_common::ExcelErrorKind::Value).with_message(
                        format!(
                            "Too few arguments: expected at least {}, got {}",
                            self.min_args(),
                            args.len()
                        ),
                    ),
                )));
            }
            return self
                .eval(args, ctx)
                .map(|result| self.apply_format_propagation(result));
        }

        // Central argument validation (includes min-arity check)
        {
            use crate::args::{ValidationOptions, validate_and_prepare_inline};
            let schema = self.arg_schema();
            if let Err(e) = validate_and_prepare_inline(
                args,
                schema,
                ValidationOptions {
                    warn_only: false,
                    min_args: self.min_args(),
                },
            ) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
        }

        self.eval(args, ctx)
            .map(|result| self.apply_format_propagation(result))
    }
}

#[cfg(test)]
mod argument_preparation_tests {
    use super::*;
    use crate::{test_workbook::TestWorkbook, traits::CalcValue};
    use formualizer_common::ExcelErrorKind;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Count(Arc<AtomicUsize>);
    impl Function for Count {
        fn name(&self) -> &'static str {
            "ARGCOUNT"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<CalcValue<'b>, ExcelError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(CalcValue::Scalar(LiteralValue::Number(-7.0)))
        }
    }

    #[test]
    fn nested_validation_memo_is_linear() -> Result<(), Box<dyn std::error::Error>> {
        crate::builtins::load_builtins();
        let calls = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new().with_function(Arc::new(Count(Arc::clone(&calls))));
        let interp = wb.interpreter();
        let formula = format!("={}ARGCOUNT(){}", "ABS(".repeat(12), ")".repeat(12));
        let ast = formualizer_parse::parser::parse(&formula)?;
        for expected in 1..=2 {
            assert_eq!(
                interp.evaluate_ast(&ast)?.into_literal(),
                LiteralValue::Number(7.0)
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                expected,
                "one leaf evaluation per recalc, not 2^depth"
            );
        }
        Ok(())
    }

    #[test]
    fn lazy_omitted_and_validation_errors_are_preserved() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::builtins::load_builtins();
        let calls = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new().with_function(Arc::new(Count(Arc::clone(&calls))));
        let interp = wb.interpreter();
        for (formula, expected) in [
            ("=IF(TRUE,7,ARGCOUNT())", LiteralValue::Number(7.0)),
            ("=IF(FALSE,ARGCOUNT(),7)", LiteralValue::Number(7.0)),
            ("=IFERROR(7,ARGCOUNT())", LiteralValue::Number(7.0)),
            ("=IFERROR(1/0,7)", LiteralValue::Number(7.0)),
            (
                "=CHOOSE(2,ARGCOUNT(),7,ARGCOUNT())",
                LiteralValue::Number(7.0),
            ),
            ("=IF(TRUE,,ARGCOUNT())", LiteralValue::Number(0.0)),
            ("=CHOOSE(2,ARGCOUNT(),)", LiteralValue::Number(0.0)),
        ] {
            let ast = formualizer_parse::parser::parse(formula)?;
            assert_eq!(
                interp.evaluate_ast(&ast)?.into_literal(),
                expected,
                "{formula}"
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "untaken branches stay lazy"
        );
        for (formula, kind) in [
            ("=ROUND(1/0,ARGCOUNT())", ExcelErrorKind::Div),
            ("=ROUND(NA(),1/0)", ExcelErrorKind::Na),
            ("=ROUND(NA())", ExcelErrorKind::Value),
            ("=ABS(\"not a number\")", ExcelErrorKind::Value),
        ] {
            let ast = formualizer_parse::parser::parse(formula)?;
            let result = interp.evaluate_ast(&ast)?.into_literal();
            assert!(
                matches!(result, LiteralValue::Error(ref e) if e.kind == kind),
                "{formula}: {result:?}"
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "validation stops at the first error"
        );
        Ok(())
    }
}
