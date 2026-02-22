# Builtin Function Docs Audit (`xtask docs-audit`)

Status: Draft / Implemented

This document defines the initial docs-quality guardrail for Formualizer builtins.

## Why this exists

We need a repeatable, automatable way to ensure that every registered builtin function has:

1. Documentation content
2. At least one inline **formula** example
3. At least one inline **Rust** example

This enables large parallel docs refinement passes with subagents and a deterministic CI gate.

## Tool entrypoint

A workspace task binary is provided via `xtask`:

```bash
cargo run -p xtask -- docs-audit
```

## How discovery works (v1)

The audit scans `crates/formualizer-eval/src/builtins/**/*.rs` and:

1. Finds registered builtins by parsing `register_function(Arc::new(TypeName))` calls.
2. Resolves `impl Function for TypeName` blocks.
3. Extracts function name from `fn name(&self) -> &'static str` string literal when available.
4. Extracts docs from Rust doc comments on:
   - `struct TypeName` (if present)
   - `impl Function for TypeName`

## Required docs checks (v1)

For each registered builtin, the checker reports issues for:

- `missing-function-impl`
- `missing-name-literal`
- `missing-doc-comment`
- `missing-rust-example`
- `missing-formula-example`

## Example doc-comment pattern

```rust
/// Return the sum of numeric arguments.
///
/// # Formula example
/// ```excel
/// =SUM(1,2,3)
/// ```
///
/// # Rust example
/// ```rust
/// // pseudo-code example for docs quality checks
/// let result = "=SUM(1,2,3)";
/// assert!(!result.is_empty());
/// ```
impl Function for SumFn {
    // ...
}
```

Notes:
- v1 checks for fenced blocks tagged `excel|formula|fx` and `rust|rs`.
- v1 does not yet execute snippet content; it is a structural quality gate.

## CLI options

```bash
cargo run -p xtask -- docs-audit \
  --paths 'crates/formualizer-eval/src/builtins/math/*.rs' \
  --functions SUM,AVERAGE \
  --json-out .tmp/docs-audit.json
```

Flags:

- `--paths <glob>`: constrain by builtin source files (repeatable)
- `--functions <A,B,...>`: constrain by function names
- `--json-out <path>`: emit machine-readable report for subagent orchestration
- `--strict`: non-zero exit on any finding

## Subagent workflow

1. Run full audit with JSON output.
2. Partition failures by file/category/function.
3. Dispatch subagents in parallel by shard.
4. Re-run audit and repeat until clean.

Example orchestration command seed:

```bash
cargo run -p xtask -- docs-audit --json-out .tmp/docs-audit.json
```

## Future extensions

- Parse and validate generated MDX function pages.
- Execute formula examples in a wasm eval harness.
- Compile-check rust examples via doctest-like harness.
- Add per-category quality thresholds.
- Add CI "changed-files strict mode" for incremental enforcement.
