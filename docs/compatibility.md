# Compatibility Corpus

Formualizer includes a small, curated compatibility corpus intended to catch semantic regressions
in the evaluator and workbook loader.

The corpus lives under `tests/corpus/` and is executed as part of the Rust test suite via the
harness in `crates/formualizer-workbook/tests/corpus.rs`.

## Fixture Layout

Each fixture is a directory:

- `tests/corpus/<category>/<fixture>/case.json`
- `tests/corpus/<category>/<fixture>/workbook.json` (JsonAdapter schema)
- `tests/corpus/<category>/<fixture>/expected.json`

`case.json` declares which cells to evaluate; `expected.json` is a stable snapshot of the
computed values (numbers, booleans, text, errors).

Fixtures can be skipped by setting `skip` in `case.json` (for dragons not implemented yet).

## Blessing Snapshots

To update snapshots locally:

```bash
FZ_CORPUS_BLESS=1 cargo test -p formualizer-workbook --test corpus
```

This rewrites each `expected.json` with the current engine output.

## Categories (M0)

- `cycles`
- `range-compression`
- `volatile-determinism`
- `named-ranges`
- `tables` (currently skipped)
- `dynamic-arrays` (currently skipped)

## Locale Contract (M0)

The evaluator uses an invariant locale:

- Numeric parsing uses `.` as the decimal separator and does not accept thousands separators.
- Locale-specific decimal formats like `"1.234,56"` are not supported and should yield `#VALUE!`
  when a number is required (e.g. `VALUE()`, `TEXT()` numeric formatting).
