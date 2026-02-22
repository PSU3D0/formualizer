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
cargo run -p xtask -- docs-audit
# strict CI-style mode:
cargo run -p xtask -- docs-audit --strict

# check generated schema sections are up to date:
cargo run -p xtask -- docs-schema
# apply updates in place:
cargo run -p xtask -- docs-schema --apply --allow-dirty
```

For builtin doc comments:

- Use template: `docs/architecture/builtin-doc-comment-template.md`
- Prefer `formualizer::doc_examples::eval_scalar` in Rust snippets to keep examples concise.

Full (environment permitting):

```bash
cargo test --workspace
```

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

## Pull request guidelines

- Keep PRs focused and reviewable.
- Add/adjust tests for behavior changes.
- Run fmt + clippy + relevant tests before opening PR.
- Update docs/examples when changing public APIs.
- Use conventional commit style when possible (e.g. `feat(...)`, `fix(...)`, `docs(...)`).

## Where to ask questions

- Use GitHub Discussions for usage/design questions.
- Use GitHub Issues for bugs and concrete feature requests.
