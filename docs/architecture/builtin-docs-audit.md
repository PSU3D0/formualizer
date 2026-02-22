# Builtin Function Docs Audit (`xtask docs-audit`)

Status: Draft / Implemented

This document defines the initial docs-quality guardrail for Formualizer builtins.

## Why this exists

We need a repeatable, automatable way to ensure that every registered builtin function has:

1. Documentation content
2. At least one inline **formula** example

Rust examples are encouraged for high-value functions and guides, but are not globally required for every builtin.

This enables large parallel docs refinement passes with subagents and a deterministic CI gate.

## Tool entrypoints

A workspace task binary is provided via `xtask`:

```bash
cargo run -p xtask -- docs-audit
cargo run -p xtask -- docs-schema
```

- `docs-audit`: validates doc coverage quality checks.
- `docs-schema`: ensures generated schema sections in doc comments are present and up to date.

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
- `missing-formula-example`

## Example doc-comment pattern

Use the shared helper to keep Rust examples concise and doctest-friendly.

```rust
/// Return the sum of numeric arguments.
///
/// # Formula example
/// ```excel
/// # returns: 6
/// =SUM(1,2,3)
/// ```
///
/// # Rust example
/// ```rust,no_run
/// # use formualizer::doc_examples::eval_scalar;
/// let value = eval_scalar("=SUM(1,2,3)")?;
/// assert_eq!(value, formualizer::LiteralValue::Number(6.0));
/// # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
/// ```
impl Function for SumFn {
    // ...
}
```

Notes:
- Rust fences with modifiers (`rust,no_run`, `rust,ignore`) are recognized as Rust examples.
- Formula fences use `excel|formula|fx` and may include comment lines (`# ...`, `// ...`).
- Formula blocks must contain at least one non-comment line to count.
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

## docs-schema command

`docs-schema` writes or checks generated schema metadata blocks inside impl doc comments.

Markers:

- `[formualizer-docgen:schema:start]`
- `[formualizer-docgen:schema:end]`

Behavior:

- If markers exist, schema block is updated in place.
- If markers are missing, schema block is appended to existing doc comments.
- If no doc comment exists, schema block is inserted above `impl Function for ...`.

Usage:

```bash
# check mode (fails if stale/missing)
cargo run -p xtask -- docs-schema

# apply mode
cargo run -p xtask -- docs-schema --apply

# apply even when working tree is dirty
cargo run -p xtask -- docs-schema --apply --allow-dirty
```

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
