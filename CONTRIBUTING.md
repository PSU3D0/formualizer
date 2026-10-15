# Contributing to Formualizer

Thanks for contributing!

## Development setup

### Rust workspace

```bash
cargo build --workspace
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

### Rust tests

Prefer focused crate tests while iterating:

```bash
cargo test -p formualizer-eval
cargo test -p formualizer-workbook
```

### Builtin docs audit and schema generation

Use workspace tasks to keep builtin docs structured:

```bash
cargo run -p xtask -- audit
# strict CI-style mode:
cargo run -p xtask -- audit --strict

# check generated schema sections are up to date:
cargo run -p xtask -- schema
# apply updates in place:
cargo run -p xtask -- schema --apply --allow-dirty
```

For builtin doc comments:

- Use template: `docs/architecture/builtin-doc-comment-template.md`
- Prefer `formualizer::doc_examples::eval_scalar` in Rust snippets to keep examples concise.

Full (environment permitting):

```bash
cargo test --workspace
```

### Public Rust API snapshots

The published Rust crates have deterministic API snapshots under `public-api/`.
Canonical generation requires Linux `x86_64-unknown-linux-gnu`; use the
repository devcontainer or an equivalent Linux environment, not macOS or
Windows output. Rustup, Python 3, and standard Linux utilities including
`flock` are required.
Install the pinned isolated tools, then run the same drift check as CI:

```bash
scripts/public-api.sh setup
scripts/public-api.sh check
```

If a public API change is intentional, update all affected snapshots with
`scripts/public-api.sh update [crate ...]`, review the unified diff for semver
impact, and commit it with the source change. See `public-api/README.md` for the
crate scope, native feature profiles, exclusions, and review limitations.

### Python bindings

Use the helper script (creates/uses venv, builds wheel, runs tests):

```bash
./scripts/dev-test.sh
```

### WASM bindings

```bash
cd bindings/wasm
npm install
npm test
```

### Floating-point reductions

Every sum, mean, dot product or product over cell data must go through
`formualizer_eval::engine::eval::exact` (`CompensatedSum`,
`CompensatedProduct`, `compensated_sum`, `compensated_mean`,
`compensated_dot`, `compensated_sum_sq_dev`, `compensated_product`). A plain
`Iterator::sum`/`product` over `f64`, or Arrow's `sum` kernel, loses about an
ulp per element and drifts with the row count (`SUM` of 500 × 19.99 reads
9994.99999999998; Excel reads 9995). `engine::tests::compensated_summation_guard`
fails on such a site in `src/builtins` or `src/engine/eval`; if the term
count is genuinely bounded (a sum over regression coefficients, not rows),
put a `// bounded-sum: <why>` comment on or directly above the line.

## Pull request guidelines

- Keep PRs focused and reviewable.
- Add/adjust tests for behavior changes.
- Run fmt + clippy + relevant tests before opening PR.
- Update docs/examples when changing public APIs.
- Use conventional commit style when possible (e.g. `feat(...)`, `fix(...)`, `docs(...)`).

## Where to ask questions

- Use GitHub Discussions for usage/design questions.
- Use GitHub Issues for bugs and concrete feature requests.
