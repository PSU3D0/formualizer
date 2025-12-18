//! formualizer-eval/src/function.rs
// New home for the core `Function` trait and its capability flags.

use core::panic;

use crate::{args::ArgSchema, traits::ArgumentHandle};
use formualizer_common::{ExcelError, LiteralValue};

bitflags::bitflags! {
    /// Describes the capabilities and properties of a function.
    ///
    /// This allows the engine to select optimal evaluation paths (e.g., vectorized,
    /// parallel, GPU) and to enforce semantic contracts at compile time.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct FnCaps: u16 {
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
    const PARALLEL_CHUNKS= 0b0100_0000_0000_0000;
    }
}

/// Revised, object-safe trait for all Excel-style functions.
///
/// This trait uses a capability-based model (`FnCaps`) to declare function
/// properties, enabling the evaluation engine to select the most optimal
/// execution path (e.g., scalar, vectorized, parallel).
pub trait Function: Send + Sync + 'static {
    /// Capability flags for this function
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
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

    /// Dispatch to the unified evaluation path with automatic argument validation.
    fn dispatch<'a, 'b, 'c>(
        &self,
        args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // Central argument validation
        {
            use crate::args::{ValidationOptions, validate_and_prepare};
            let schema = self.arg_schema();
            if let Err(e) =
                validate_and_prepare(args, schema, ValidationOptions { warn_only: false })
            {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
        }

        self.eval(args, ctx)
    }
}
