# Custom Function Extension API v1

Status: Draft

## Why this exists

Formualizer users (Rust, Python, JS/WASM) should not need to fork the project to add missing or domain-specific functions.

v1 defines a single extension contract with three host surfaces:

1. Rust (native registration)
2. Python (register Python callables)
3. JS/WASM (register JS callables)

It also defines a compatible path for WASM-plugin-backed custom functions (e.g., Rust->WASM with `wasmi`) without changing formula syntax.

## Product goals

- **No-fork extensibility**: register custom functions at runtime.
- **Ergonomic host APIs**: Python/JS should be one-call registration.
- **Performance-aware**: cheap scalar path, optional batched path.
- **Deterministic semantics**: volatility/purity metadata explicit.
- **Cross-host compatibility**: same formula works in Rust/Python/JS bindings.

## Non-goals (v1)

- Dynamic dependency discovery from custom functions (INDIRECT/OFFSET-style virtual deps).
- By-ref argument passing and reference-returning custom functions.
- Async callbacks inside engine evaluation.
- Full parity with Excel JavaScript custom function runtime metadata model.

## Core model

## Function identity

- Name resolution is case-insensitive.
- Namespace remains optional (`""` by default).
- Workbook-local functions are preferred over global registry entries.

Resolution order:

1. Workbook-local custom function
2. Global function registry (builtins + globally registered customs)

## Registration descriptor

```rust
pub struct CustomFnOptions {
    pub min_args: usize,
    pub max_args: Option<usize>,   // None => variadic
    pub volatile: bool,
    pub thread_safe: bool,
    pub deterministic: bool,
    pub allow_override_builtin: bool,
}
```

Defaults:

- `min_args = 0`
- `max_args = None`
- `volatile = false`
- `thread_safe = false`
- `deterministic = true`
- `allow_override_builtin = false`

## Invocation contract (v1)

- Arguments are passed **by value** as `LiteralValue`.
- Ranges are materialized as `LiteralValue::Array` (row-major).
- Return types:
  - scalar `LiteralValue`
  - array `LiteralValue::Array`
  - error (`ExcelError`)

## Rust API surface (v1)

At workbook layer (`formualizer_workbook`):

```rust
pub fn register_custom_function(
    &mut self,
    name: &str,
    options: CustomFnOptions,
    handler: Arc<dyn CustomFnHandler>,
) -> Result<(), ExcelError>;

pub fn unregister_custom_function(&mut self, name: &str) -> Result<(), ExcelError>;
pub fn list_custom_functions(&self) -> Vec<CustomFnInfo>;
```

Handler trait:

```rust
pub trait CustomFnHandler: Send + Sync {
    fn call(&self, args: &[LiteralValue]) -> Result<LiteralValue, ExcelError>;

    // Optional fast-path for vector/range-heavy functions.
    fn call_batch(&self, _rows: &[Vec<LiteralValue>]) -> Option<Result<LiteralValue, ExcelError>> {
        None
    }
}
```

## Python API surface (v1)

```python
wb.register_function(
    name: str,
    callback: Callable[..., Any],
    *,
    min_args: int = 0,
    max_args: int | None = None,
    volatile: bool = False,
    thread_safe: bool = False,
    deterministic: bool = True,
    allow_override_builtin: bool = False,
) -> None

wb.unregister_function(name: str) -> None
wb.list_functions() -> list[dict]
```

Python value mapping:

- `None` -> Empty
- `bool/int/float/str` -> corresponding literal
- `datetime/date/time/timedelta` -> existing literal conversions
- nested lists -> `LiteralValue::Array`
- exceptions -> `ExcelError(#VALUE!)` with message

## JS/WASM API surface (v1)

In `bindings/wasm` TS wrapper:

```ts
workbook.registerFunction(
  name: string,
  callback: (...args: CellValue[]) => CellValue,
  options?: {
    minArgs?: number;
    maxArgs?: number | null;
    volatile?: boolean;
    threadSafe?: boolean;
    deterministic?: boolean;
    allowOverrideBuiltin?: boolean;
  }
): void;

workbook.unregisterFunction(name: string): void;
workbook.listFunctions(): RegisteredFunctionInfo[];
```

JS value mapping mirrors current literal conversions in `bindings/wasm/src/workbook.rs`.

## WASM plugin path (compatible with v1)

After host callbacks are stable, add plugin-backed handlers:

- inspect module bytes without side effects
- attach module to workbook context explicitly
- bind exported symbol to formula name
- handler implements same `CustomFnHandler`
- runtime backend selected per host (native: `wasmtime`; JS/WASM binding: JS host bridge; `wasmi` fallback)

Proposed API:

```rust
pub fn register_wasm_function(
    &mut self,
    name: &str,
    options: CustomFnOptions,
    plugin: WasmFunctionSpec,
) -> Result<(), ExcelError>;
```

Where `WasmFunctionSpec` contains module id, export name, and codec version.

## Error semantics

- Registration conflicts -> `#NAME?`-class configuration error (`ExcelErrorKind::Name`) with message.
- Callback panic/exception -> `#VALUE!` with safe error message.
- Callable returned but not invoked remains `#CALC!` semantics already introduced for LAMBDA.

## Security and sandboxing

- Python/JS callbacks are trusted host code.
- WASM plugins are sandboxed by runtime policy:
  - no host FS/network unless explicitly provided
  - instruction/memory limits configurable

## Performance expectations

- Native builtins remain fastest.
- Host callbacks prioritize ergonomics and correctness.
- WASM plugin handlers are the preferred high-performance extension lane.
- `call_batch` allows optimization without widening v1 semantic surface.

## Testing requirements (v1)

- Registration lifecycle: add/list/remove
- Argument arity enforcement
- Error propagation from callbacks
- Volatile function recalc behavior
- Python + JS mapping roundtrips
- Parity tests: same custom function behavior in Rust/Python/JS bindings

## Compatibility and migration

- Existing global `register_function_dynamic(...)` remains supported.
- New workbook-local API is additive and preferred.
- No formula syntax changes required.
