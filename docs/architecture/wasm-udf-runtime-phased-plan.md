# WASM UDF Runtime: Phased, Test-Driven Implementation Plan

Status: Draft

This plan executes the architecture in `docs/architecture/wasm-udf-runtime-architecture.md`
with incremental, reviewable phases.

## Execution principles

- Every phase is test-first where feasible (red -> green -> refactor).
- Keep commits phase-scoped and reversible.
- Preserve callback-based custom function behavior throughout.
- No large flag-day rewrites.

## Validation gates (per phase unless noted)

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- targeted tests for changed crates

Milestone gates (periodic full checks):
- `cargo test --workspace --exclude formualizer-python`
- `./scripts/dev-test.sh`
- `cd bindings/wasm && npm test`

---

## Phase 0: ABI + Manifest spec lock (docs + conformance fixtures)

Goal:
- Freeze v1 ABI and manifest schema before runtime coding.

Work:
1. Add schema docs + canonical examples under `docs/architecture/`.
2. Add conformance fixtures (valid and invalid manifests) under test fixtures.
3. Add parser/validator utilities in workbook crate (no runtime invocation yet).

Tests:
- manifest validator accepts canonical valid fixtures
- manifest validator rejects malformed/duplicate/unsupported codec fixtures
- case-insensitive alias uniqueness checks

Exit criteria:
- ABI and manifest parser behavior is explicit and tested.

---

## Phase 1: Plugin manager skeleton + module registry

Goal:
- Introduce lifecycle/control plane without call execution.

Work:
1. Add `WasmPluginManager` to workbook layer.
2. Add effect-free inspect API and workbook-local attach lifecycle APIs:
   - `inspect_wasm_module_bytes` (no mutation)
   - `attach_wasm_module_bytes`
   - `list_wasm_modules`
   - `unregister_wasm_module`
3. Store module metadata + raw bytes + parsed manifest.
4. Keep function bind path wired but NImpl until runtime bindability is enabled.

Tests:
- register/list/unregister module lifecycle
- duplicate module id rejection
- manifest parse errors surfaced clearly
- unregister removes function metadata bindings

Exit criteria:
- modules can be loaded/inspected safely; no invocation yet.

---

## Phase 2: Runtime abstraction and fake backend

Goal:
- Build stable runtime interface and host plumbing before real engines.

Work:
1. Introduce `WasmUdfRuntime` trait + invocation structs.
2. Implement in-memory/fake runtime for deterministic unit tests.
3. Wire plugin manager to runtime via dependency injection.
4. Convert `register_wasm_function` from seam-only to manager-backed binding step.

Tests:
- binding formula name -> module function id
- callback adapter path uses runtime abstraction
- arity enforcement parity with existing custom handlers
- runtime-returned error mapping parity tests

Exit criteria:
- end-to-end flow works against fake runtime.

---

## Phase 3: Native runtime backend (wasmtime)

Goal:
- Deliver high-performance plugin execution on native targets.

Work:
1. Add feature-gated `wasmtime` backend crate/module.
2. Implement:
   - module compile/cache
   - explicit runtime selection on workbook setup (`use_wasmtime_runtime`-style)
   - invocation path (typed export bridge first, ABI helpers where available)
3. Apply runtime limits (fuel/memory) from policy + optional hints.
4. Trap/error conversion to `ExcelError`.

Tests:
- integration tests with small test wasm module fixture
- scalar + array args/returns
- alias resolution and function id dispatch
- fuel/memory limit enforcement behavior
- invalid ABI exports fail with actionable messages

Exit criteria:
- native hosts can run real WASM UDF modules.

---

## Phase 4: Portable runtime backend (wasmi)

Goal:
- Provide runtime path for portability and wasm32-host scenarios.

Work:
1. Add feature-gated `wasmi` backend implementation of same trait.
2. Backend selection policy:
   - prefer wasmtime on native when available
   - fallback to wasmi otherwise
3. Ensure API behavior parity (errors, limits, value codec).

Tests:
- backend parity tests using shared fixture module
- explicit backend selection tests via config/feature gates

Exit criteria:
- both backends satisfy common runtime test suite.

---

## Phase 5: Workbook integration + function binding UX

Goal:
- Complete workbook-level APIs and make binding flow ergonomic.

Work:
1. Implement final API shape:
   - `register_wasm_module_bytes`
   - `register_wasm_module_file` (native)
   - `register_wasm_modules_dir` (native)
   - `bind_wasm_function` (or compatible alias)
2. Support auto-bind from manifest names/aliases with override policy controls.
3. Ensure workbook-local precedence over global registry remains consistent.

Tests:
- folder load with multiple modules and aliases
- collisions and override policy behavior
- unregister module removes all bound functions
- fallback to global registry when local binding absent

Exit criteria:
- Rust users can point to bytes/file/dir and use functions immediately.

---

## Phase 6: Python binding integration

Goal:
- Expose module registration and function binding in Python.

Work:
1. Add APIs in `bindings/python/src/workbook.rs`:
   - bytes/file/dir module registration
   - wasm function bind/list/unregister helpers
2. Add stub typing updates in `.pyi`.
3. Preserve existing callback registration behavior.

Tests (`bindings/python/tests/`):
- register module bytes and evaluate formula
- file/dir registration (where supported in test env)
- Python-visible error mapping from wasm traps
- coexistence of Python callback UDFs + WASM UDFs

Exit criteria:
- Python users can load wasm plugins without writing Rust.

---

## Phase 7: JS/WASM binding integration

Goal:
- Expose plugin UDF functionality to JS host APIs.

Work:
1. Add bytes-based registration in wasm bindings (`registerWasmModuleBytes`).
2. Add function bind/list APIs in TS wrapper.
3. Node helper utility in examples/docs for loading `.wasm` files/directories and passing bytes.

Tests (`bindings/wasm/tests/` + JS tests):
- bytes registration + bind + evaluate
- error mapping from thrown/trapped runtime errors
- coexistence with JS callback-registered functions

Exit criteria:
- JS users can consume plugin modules via bytes path.

---

## Phase 8: Author SDK (Rust-first)

Goal:
- Make authoring clean and safe.

Work:
1. Add new crate: `crates/formualizer-udf-sdk`.
2. Provide:
   - `#[fz_udf(...)]` proc macro
   - `fz_module!{...}` macro
   - value/error helpers
3. Generate ABI exports and manifest automatically.
4. Publish at least one end-to-end sample module crate.

Tests:
- compile-time macro tests
- runtime integration tests loading SDK-generated module
- manifest schema conformance tests

Exit criteria:
- extension authors can write ergonomic Rust UDF modules without ABI boilerplate.

---

## Phase 9: Hardening, docs, perf, and release readiness

Goal:
- Production confidence across correctness, UX, and performance.

Work:
1. Add comprehensive docs:
   - architecture overview
   - ABI reference
   - authoring guide
   - host loading guide (Rust/Python/JS)
2. Add benchmark suite:
   - native builtin vs host callback vs wasm plugin
3. Add stress/fuzz tests for malformed modules and payloads.
4. Add CI matrix for backends/features.

Tests/benchmarks:
- large-array UDF throughput
- repeated recalc with volatile and deterministic modules
- malformed module corpus

Exit criteria:
- documented, benchmarked, and release-ready plugin system.

---

## Recommended commit strategy

- One commit per phase where possible.
- If phase is too large, split by coherent vertical slices:
  - API + tests
  - runtime impl + tests
  - bindings + tests
- Keep docs/examples commit separate near end.

## Risks and mitigations

1. **ABI churn risk**
   - Mitigation: freeze ABI in Phase 0 with fixtures before runtime work.

2. **Backend divergence risk (wasmtime vs wasmi)**
   - Mitigation: shared runtime conformance suite run against both backends.

3. **Binding mismatch risk across Rust/Python/JS**
   - Mitigation: cross-host parity test matrix using same wasm fixture module.

4. **Security/resource exhaustion risk**
   - Mitigation: mandatory default limits, explicit opt-in policy loosening.

5. **Authoring friction risk**
   - Mitigation: SDK macros and templates before calling feature complete.

## Done definition (program-level)

All of the following are true:

- Rust/Python/JS users can register and invoke wasm plugin functions without forking.
- Same module manifest/function works consistently across hosts.
- Runtime backend selection is explicit and tested.
- Security/resource controls are enabled by default.
- Docs/examples are complete and validated in CI.
