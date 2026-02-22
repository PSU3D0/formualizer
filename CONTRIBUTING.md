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
