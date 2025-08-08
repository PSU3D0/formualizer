//! formualizer-eval/src/function.rs
// New home for the core `Function` trait and its capability flags.

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
        /// Can be implemented with `eval_fold`.
        const REDUCTION     = 0b0000_0000_0100;
        /// The function operates on each element of its input ranges independently
        /// (e.g., `SIN`, `ABS`). Can be implemented with `eval_map`.
        const ELEMENTWISE   = 0b0000_0000_1000;
        /// The function operates on a sliding window over its input (e.g., `MOVING_AVERAGE`).
        /// Can be implemented with `eval_window`.
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
    }
}

// --- Fast-Path Evaluation Contexts ---

use crate::traits::EvaluationContext;
use bumpalo::Bump;

/// A simple slice of homogeneous values for efficient iteration
pub struct SliceStripe<'a> {
    pub head: &'a [LiteralValue],
}

/// Context for `eval_fold` (Reduction operations).
/// Provides efficient iteration over input ranges for fold/reduce operations.
pub trait FnFoldCtx {
    /// Visit numeric chunks packed from all range arguments; no materialization required.
    fn for_each_numeric_chunk(
        &mut self,
        min_chunk: usize,
        f: &mut dyn FnMut(crate::stripes::NumericChunk) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError>;

    /// Visit cells (coerced via range visitors) in row-major order.
    fn for_each_cell(
        &mut self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError>;

    /// Return accumulated result (for two-pass folds like AVERAGE).
    fn write_result(&mut self, v: LiteralValue);
}

/// Concrete implementation of FnFoldCtx that works with current RangeStorage
pub struct SimpleFoldCtx<'a, 'b> {
    args: &'a [ArgumentHandle<'a, 'b>],
    _ctx: &'a dyn EvaluationContext,
    result: Option<LiteralValue>,
    /// Temporary arena for allocating iteration data
    arena: Bump,
}

impl<'a, 'b> SimpleFoldCtx<'a, 'b> {
    pub fn new(args: &'a [ArgumentHandle<'a, 'b>], ctx: &'a dyn EvaluationContext) -> Self {
        Self {
            args,
            _ctx: ctx,
            result: None,
            arena: Bump::new(),
        }
    }

    pub fn take_result(self) -> Option<LiteralValue> {
        self.result
    }
}

impl<'a, 'b> FnFoldCtx for SimpleFoldCtx<'a, 'b> {
    fn for_each_numeric_chunk(
        &mut self,
        min_chunk: usize,
        f: &mut dyn FnMut(crate::stripes::NumericChunk) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        for arg in self.args {
            if let Ok(mut storage) = arg.range_storage() {
                storage.for_each_numeric_chunk(
                    crate::args::CoercionPolicy::NumberLenientText,
                    min_chunk,
                    f,
                )?;
            } else if let Ok(value) = arg.value() {
                // Pack single scalar if numeric/coercible per policy
                let as_num = match value.as_ref() {
                    LiteralValue::Error(e) => {
                        return Err(e.clone());
                    }
                    LiteralValue::Number(n) => Some(*n),
                    LiteralValue::Int(i) => Some(*i as f64),
                    LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
                    LiteralValue::Empty => Some(0.0),
                    LiteralValue::Text(s) => s.trim().parse::<f64>().ok(),
                    _ => None,
                };
                if let Some(n) = as_num {
                    // provide a tiny chunk
                    let one = [n];
                    f(crate::stripes::NumericChunk {
                        data: &one,
                        validity: None,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn for_each_cell(
        &mut self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        for arg in self.args {
            if let Ok(mut storage) = arg.range_storage() {
                storage.for_each_cell_flat(f)?;
            } else if let Ok(value) = arg.value() {
                f(value.as_ref())?;
            }
        }
        Ok(())
    }

    fn write_result(&mut self, v: LiteralValue) {
        self.result = Some(v);
    }
}

/// Context for `eval_map` (Element-wise operations).
pub trait FnMapCtx {
    /// Whether inputs indicate an array/range context. If false, callers should fall back to scalar.
    fn is_array_context(&self) -> bool;

    /// Apply a unary numeric mapping over the broadcasted input. The closure should return the mapped cell.
    fn map_unary_numeric(
        &mut self,
        f: &mut dyn FnMut(f64) -> Result<LiteralValue, ExcelError>,
    ) -> Result<(), ExcelError>;

    /// Apply a binary numeric mapping over the broadcasted inputs (first two args).
    fn map_binary_numeric(
        &mut self,
        f: &mut dyn FnMut(f64, f64) -> Result<LiteralValue, ExcelError>,
    ) -> Result<(), ExcelError>;

    /// Finalize and retrieve the output value (typically an Array). Implementations may move out internal buffers.
    fn finalize(&mut self) -> LiteralValue;
}

/// Context for `eval_window` (Windowed operations).
pub trait FnWindowCtx {}

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
        &[]
    }

    /// The default, scalar evaluation path.
    ///
    /// This method is the fallback for all functions and the only required
    /// evaluation path. It processes arguments one by one.
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::EvaluationContext,
    ) -> Result<LiteralValue, ExcelError>;

    // --- Optional Fast Paths ---

    /// An optional, optimized path for reduction functions (e.g., `SUM`, `COUNT`).
    ///
    /// This method is called by the engine if the `REDUCTION` capability is set.
    /// It operates on a `FnFoldCtx` which provides efficient access to input data.
    fn eval_fold(&self, _f: &mut dyn FnFoldCtx) -> Option<Result<LiteralValue, ExcelError>> {
        None
    }

    /// An optional, optimized path for element-wise functions (e.g., `SIN`, `ABS`).
    ///
    /// This method is called by the engine if the `ELEMENTWISE` capability is set.
    /// It operates on a `FnMapCtx` which provides direct access to input/output
    /// data stripes for vectorized processing.
    fn eval_map(&self, _m: &mut dyn FnMapCtx) -> Option<Result<LiteralValue, ExcelError>> {
        None
    }

    /// An optional, optimized path for windowed functions (e.g., `MOVING_AVERAGE`).
    ///
    /// This method is called by the engine if the `WINDOWED` capability is set.
    fn eval_window(&self, _w: &mut dyn FnWindowCtx) -> Option<Result<(), ExcelError>> {
        None
    }

    /// Dispatch to the most optimal evaluation path based on capabilities.
    /// This default implementation checks caps and calls the appropriate eval method.
    fn dispatch<'a, 'b>(
        &self,
        args: &'a [crate::traits::ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let caps = self.caps();

        // Central argument validation (always on)
        {
            use crate::args::{ValidationOptions, validate_and_prepare};
            let schema = self.arg_schema();
            // Strict validation; convert errors to value errors to preserve interpreter Ok path
            if let Err(e) =
                validate_and_prepare(args, schema, ctx, ValidationOptions { warn_only: false })
            {
                return Ok(LiteralValue::Error(e));
            }
        }

        // Try fast paths based on capabilities
        if caps.contains(FnCaps::REDUCTION) {
            // Create fold context and try eval_fold
            let mut fold_ctx = SimpleFoldCtx::new(args, ctx);
            if let Some(result) = self.eval_fold(&mut fold_ctx) {
                return result;
            }
        }

        if caps.contains(FnCaps::ELEMENTWISE) {
            // Minimal unary elementwise path: construct a simple map ctx and call eval_map
            let mut m = crate::map_ctx::SimpleMapCtx::new(args, ctx);
            if FnMapCtx::is_array_context(&m) {
                let dyn_m: &mut dyn FnMapCtx = &mut m;
                if let Some(result) = self.eval_map(dyn_m) {
                    return result;
                }
            }
        }

        if caps.contains(FnCaps::WINDOWED) {
            // Try eval_window path (not implemented yet)
            // if let Some(result) = self.eval_window(...) {
            //     return result;
            // }
        }

        // Fallback to scalar evaluation
        self.eval_scalar(args, ctx)
    }
}
