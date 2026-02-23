# WASM UDF Runtime Architecture (v1+)

Status: Draft

## Why this exists

Formualizer now supports workbook-local custom functions from Rust/Python/JS callbacks.
The next capability step is a **portable, high-performance plugin lane** where users can ship
compiled WASM modules instead of forking the project.

This document defines an end-to-end architecture for that lane.

## Goals

- No-fork custom-function extensibility across Rust, Python, and JS.
- One `.wasm` module can expose many spreadsheet functions.
- Stable ABI + manifest so modules are self-describing.
- Runtime portability:
  - native hosts can use `wasmtime` for throughput
  - constrained/portable hosts can use `wasmi`
- Deterministic error mapping and resource controls.

## Non-goals (initial runtime)

- Async UDF execution in recalculation loop.
- By-reference arguments / returning references.
- Dynamic dependency discovery from UDFs.
- Full WASI/system access by default.

## Design principles

1. **Invocation path stays callable-centric**
   - At eval time, a plugin UDF behaves like any other callable (`Function`/`CustomFnHandler`).
2. **Lifecycle is manager-centric**
   - Loading, compiling, caching, alias binding, and policy enforcement are handled by a plugin manager.
3. **ABI is runtime-neutral**
   - Same module works whether backend runtime is wasmtime or wasmi.
4. **Authoring must be ergonomic**
   - Extension authors should write Rust functions + metadata, not raw pointer plumbing.

---

## High-level architecture

## Components

1. **Workbook custom registry** (already exists)
   - stores workbook-local function bindings.

2. **WasmPluginManager** (new)
   - module store (bytes + compiled cache)
   - manifest index
   - function binding map (`formula_name -> module/function ref`)
   - loader utilities (bytes/file/directory)

3. **WasmRuntime abstraction** (new)
   - trait that defines module compile/instantiate/invoke + resource policy hooks
   - implementations:
     - `WasmtimeRuntime` (native targets)
     - `WasmiRuntime` (portable fallback, including wasm32 host scenarios)

4. **WasmUdfAdapter** (new)
   - implements `formualizer_eval::function::Function`
   - materializes args by value, invokes runtime, converts result back to `LiteralValue`

5. **Author SDK** (new crate)
   - `formualizer-udf-sdk` (macros + value helpers + manifest generation)

## Why both adapter and manager?

- The adapter handles per-call execution semantics.
- The manager handles everything else: module lifecycle, caching, manifest parsing, alias registration,
  and policy/resource governance.

Without a manager, runtime costs and complexity leak into every registration call.

---

## Public API surface (target)

## Workbook-facing API

```rust
// Effect-free inspection (no workbook mutation)
pub fn inspect_wasm_module_bytes(
    &self,
    wasm_bytes: &[u8],
) -> Result<WasmModuleInfo, ExcelError>;

// Workbook-local attachment (explicit, no implicit globals)
pub fn attach_wasm_module_bytes(
    &mut self,
    module_id: &str,
    wasm_bytes: &[u8],
) -> Result<WasmModuleInfo, ExcelError>;

pub fn register_wasm_module_file(
    &mut self,
    path: impl AsRef<std::path::Path>,
) -> Result<WasmModuleInfo, ExcelError>; // non-wasm32

pub fn register_wasm_modules_dir(
    &mut self,
    dir: impl AsRef<std::path::Path>,
) -> Result<Vec<WasmModuleInfo>, ExcelError>; // non-wasm32

// Workbook-local binding of formula name -> module export
pub fn bind_wasm_function(
    &mut self,
    formula_name: &str,
    options: CustomFnOptions,
    spec: WasmFunctionSpec,
) -> Result<(), ExcelError>;

pub fn unregister_wasm_module(&mut self, module_id: &str) -> Result<(), ExcelError>;
pub fn list_wasm_modules(&self) -> Vec<WasmModuleInfo>;
```

Notes:
- Rust path should remain workbook-local by default (explicit attach/bind).
- Process-global registration is intentionally not the default in Rust.

Notes:
- `register_wasm_function(...)` (existing seam) can be kept as compatibility alias to `bind_wasm_function(...)`.
- Module registration and function binding are intentionally separate.

## Binding APIs

Python:
- `register_wasm_module_bytes(module_id: str, wasm: bytes)`
- `register_wasm_module_file(path: str)`
- `register_wasm_modules_dir(path: str)`
- `bind_wasm_function(name: str, module_id: str, export_or_func: str, ...)`

JS/WASM:
- `registerWasmModuleBytes(moduleId: string, bytes: Uint8Array)`
- `bindWasmFunction(...)`
- For Node wrappers only: optional helper to load directory and call bytes API.

---

## Runtime backend strategy

## Trait

```rust
pub trait WasmUdfRuntime: Send + Sync {
    type ModuleHandle: Clone + Send + Sync + 'static;

    fn compile_module(&self, bytes: &[u8]) -> Result<Self::ModuleHandle, ExcelError>;

    fn read_manifest(&self, module: &Self::ModuleHandle) -> Result<Vec<u8>, ExcelError>;

    fn invoke(
        &self,
        module: &Self::ModuleHandle,
        invocation: WasmInvocation,
        limits: RuntimeLimits,
    ) -> Result<Vec<u8>, ExcelError>;
}
```

## Backend selection

- Native builds (`cfg(not(target_arch = "wasm32"))`): `wasmtime` is the primary backend.
- JS/WASM binding hosts (browser/Node/Workers): use JS-host WebAssembly engine bridge as primary.
- `wasmi` remains a portable fallback path for constrained/non-JS wasm host scenarios.
- Feature flags select compiled backends; runtime selection should be explicit at workbook setup time.

---

## ABI contract (v1)

The ABI is a **module-level contract** independent of formula names.
Formula names and aliases are provided by manifest metadata.

## Required exports

- `memory`
- `fz_abi_version() -> i32`  (must return `1`)
- `fz_manifest_ptr() -> i32`
- `fz_manifest_len() -> i32`
- `fz_alloc(len: i32) -> i32`
- `fz_free(ptr: i32, len: i32)`
- `fz_invoke(func_id: i32, req_ptr: i32, req_len: i32) -> i64`

`fz_invoke` returns packed `(ptr,len)` in `i64`:
- low 32 bits: `ptr`
- high 32 bits: `len`

## Value/request/response codec

- Initial codec: CBOR (stable schema IDs and strict field rules).
- `codec_version` in `WasmFunctionSpec` and manifest must match host-supported version.

Request payload (`InvokeRequestV1`):
- `function_id: u32`
- `args: [Value]`
- `call_id: u64` (diagnostics/tracing)

Response payload (`InvokeResponseV1`):
- `ok: bool`
- `value?: Value`
- `error?: { kind: ErrorKind, message: string }`

### Value mapping (v1)

Must map 1:1 with `LiteralValue` semantics:
- Empty
- Bool
- Int
- Number
- Text
- Date
- DateTime
- Time
- Duration
- Array (2D row-major)
- Error (`kind`, `message`)

---

## Manifest contract

Each module exposes a manifest blob via `fz_manifest_ptr/len`.

Example manifest:

```json
{
  "schema": "formualizer.udf.module/v1",
  "module": {
    "id": "com.acme.finance",
    "version": "1.2.0",
    "abi": 1,
    "codec": 1
  },
  "functions": [
    {
      "id": 1,
      "name": "XNPV_PLUS",
      "aliases": ["XNPVPLUS", "_xlfn.XNPV_PLUS"],
      "export": "fn_xnpv_plus",
      "min_args": 3,
      "max_args": null,
      "volatile": false,
      "deterministic": true,
      "thread_safe": true,
      "params": [
        { "name": "rate", "kinds": ["number"] },
        { "name": "cashflows", "kinds": ["array"] },
        { "name": "dates", "kinds": ["array"] }
      ],
      "returns": { "kinds": ["number", "error"] }
    }
  ]
}
```

Host behavior:
- canonicalize function names/aliases case-insensitively
- validate uniqueness across all registered workbook-local functions
- apply override policy (`allow_override_builtin`)

---

## Security/resource model

Defaults:
- No filesystem/network imports.
- Fuel/instruction limits configurable per function/module.
- Memory limits configurable per invocation/module.
- Trap -> `ExcelErrorKind::Value` with safe message.

Policy knobs:
- global runtime policy in workbook config
- optional per-function runtime hints (`WasmRuntimeHint`)

---

## Error model

- ABI/manifest incompatibility -> `ExcelErrorKind::NImpl` or `Value` (with actionable message).
- Runtime trap/panic -> `ExcelErrorKind::Value` (safe, bounded message).
- Function not found in module manifest -> `ExcelErrorKind::Name`.
- Arity mismatch -> `ExcelErrorKind::Value` (same style as host callbacks).

---

## Authoring model (Rust-first)

Authors should use SDK macros, not raw ABI.

Target author experience:

```rust
use formualizer_udf_sdk::{fz_module, fz_udf, Value, UdfError};

#[fz_udf(name = "XNPV_PLUS", aliases = ["XNPVPLUS"], min_args = 3)]
fn xnpv_plus(args: &[Value]) -> Result<Value, UdfError> {
    // domain logic
    Ok(Value::Number(42.0))
}

fz_module! {
    id: "com.acme.finance",
    version: "1.0.0",
    functions: [xnpv_plus]
}
```

SDK responsibilities:
- generate manifest
- generate dispatch table (`function_id -> handler`)
- implement ABI exports (`fz_*`)
- provide value/error conversion helpers

---

## Compatibility and migration

- Current callback registration APIs remain valid and first-class.
- WASM plugin UDFs are additive and use the same workbook-local lookup precedence.
- Existing `register_wasm_function(...)` seam is retained and wired into plugin manager implementation.

## Open decisions (to finalize before implementation)

1. CBOR vs MessagePack for v1 codec.
2. Whether to include optional `batch_invoke` ABI in v1 or v1.1.
3. Backend defaults by target (wasmtime-only on native by default vs dual backend feature set).
4. Manifest signature/integrity verification (out of initial scope, but design hook desirable).
