# Packaging, Versioning, Tagging, Releases

This repo publishes multiple artifacts (crates.io, PyPI, npm) from one monorepo. The goal is:

- **A single “product surface” version** shared across Rust (`crates/formualizer`), PyPI, and npm.
- **Independent release tracks** for the high-performance parser + shared types, and for the SheetPort spec.
- **Repeatable automation**: tags drive publishing; workflows fail fast if versions don’t match.

## What We Publish

### Rust (crates.io)

**SDK / parser track**
- `formualizer-common`: shared value/address/reference types.
- `formualizer-parse`: tokenizer/parser/pretty-print.

**Product track**
- `formualizer-eval`: evaluation engine.
- `formualizer-workbook`: workbook abstraction + loaders.
- `formualizer-sheetport`: SheetPort runtime over a workbook.
- `formualizer`: roll-up (“product surface”) crate; intended primary interface for bindings and most downstreams.

**Spec track**
- `sheetport-spec`: YAML/JSON schema + validation + CLI.

### Python (PyPI)

- `formualizer` (maturin / pyo3 extension): the product surface for Python.

### JS/WASM (npm)

- `formualizer` (wasm-pack output + TypeScript wrapper): the product surface for JS.

## Version Tracks

### 1) Product surface track (shared across Rust + PyPI + npm)

**Rule:** The following versions are always identical:

- `crates/formualizer/Cargo.toml` (`package.version`)
- `bindings/python/pyproject.toml` (`project.version`)
- `bindings/wasm/package.json` (`version`)

This is the public “Formualizer product version”.

### 2) Parser/SDK track (`formualizer-common` + `formualizer-parse`)

**Rule:** `formualizer-common` and `formualizer-parse` share one version and can ship independently of the product surface.

This allows the parser to evolve (and be consumed directly) without forcing a product/bindings release.

### 3) Spec track (`sheetport-spec`)

**Rule:** `sheetport-spec` is versioned and tagged independently.

Product releases may depend on a `sheetport-spec` version; if the product needs a newer spec, publish the spec first.

## Tagging Scheme

Tags encode *which track* is being released.

- **Product release:** `vX.Y.Z`
  - publishes: Rust product crates + PyPI + npm
- **Parser/SDK release:** `parse-vX.Y.Z`
  - publishes: `formualizer-common`, `formualizer-parse`
- **Spec release:** `sheetport-spec-vX.Y.Z`
  - publishes: `sheetport-spec` (and triggers mirror)

Multiple tags can point at the same commit if we want “synced” releases without forcing a single global version.

## Dependency + Pinning Rules

### Rust workspace dependencies

- Use workspace deps for internal development (`path = ...`).
- Published crates must also specify a **version requirement** for internal deps.

### Cross-track compatibility

- Product crates should depend on parser/SDK crates with semver ranges (not exact pins) once `parse/common` reach stability.
  - Example target end state: product crates depend on `formualizer-parse = "^1"` and `formualizer-common = "^1"`.
- While `0.x`, treat “minor” bumps as breaking; avoid frequent cross-track churn.

### Feature forwarding

`crates/formualizer` is the binding-facing surface. To let bindings depend only on `formualizer`, it must forward required features:

- Workbook backends (`calamine`, `umya`, `json`, etc.)
- Optional engine behavior toggles
- SheetPort integration toggles

Bindings should enable features on `formualizer` (not on individual subcrates).

## Publishing Order (Rust)

### Parser/SDK release (`parse-v*`)

1. `formualizer-common`
2. `formualizer-parse`

### Spec release (`sheetport-spec-v*`)

1. `sheetport-spec`

### Product release (`v*`)

Precondition: required `sheetport-spec` version already published.

Publish in dependency order:

1. `formualizer-macros` (if required by eval)
2. `formualizer-eval`
3. `formualizer-workbook`
4. `formualizer-sheetport`
5. `formualizer` (roll-up)

## GitHub Actions Release Principles

Release workflows should:

- Trigger on the correct tag pattern.
- Verify tag ↔ manifest version matches for that track.
- Run a `cargo publish --dry-run` (or equivalent check) before publishing.
- Publish without masking failures (no `|| true`).

For npm builds, ensure the wasm-pack target matches what we publish (bundler vs web target) and that the generated `pkg/` content matches what `package.json` expects.

## Version Bump Script

Use `scripts/bump-version.py` to update versions across all manifests for a given track:

```bash
# Product track (Rust product crates + Python + npm)
./scripts/bump-version.py --track product --version 0.4.0

# Parser/SDK track (formualizer-common + formualizer-parse)
./scripts/bump-version.py --track parse --version 1.1.0

# Spec track (sheetport-spec only)
./scripts/bump-version.py --track spec --version 0.4.0

# Preview changes without modifying files
./scripts/bump-version.py --track product --version 0.4.0 --dry-run

# Skip cargo check verification
./scripts/bump-version.py --track product --version 0.4.0 --no-verify
```

The script updates:
- **Package versions** in `Cargo.toml`, `pyproject.toml`, `package.json`
- **Workspace dependencies** in root `Cargo.toml`
- **Internal dependency versions** (e.g., `formualizer-eval = { path = "...", version = "X.Y.Z" }`)

After bumping, the script runs `cargo check` to verify the workspace compiles (use `--no-verify` to skip).

## Release Checklist (human)

1. Decide which track(s) you are releasing.
2. Run `./scripts/bump-version.py --track <track> --version <version>` (use `--dry-run` first to preview).
3. Ensure `CHANGELOG` entries exist where applicable.
4. Commit the version bump: `git commit -am "chore: bump <track> to <version>"`
5. Create the tag: `git tag v<version>` (or `parse-v<version>` / `sheetport-spec-v<version>`).
6. Push: `git push && git push --tags`
7. Verify GitHub Actions publishes successfully.
