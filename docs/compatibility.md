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

## Visibility Aggregates (Phase 1)

Supported now:

- `SUBTOTAL(function_num, ref1, [ref2], ...)` with `function_num` in `1..11` and `101..111`.
- `AGGREGATE(function_num, options, ref1, [ref2], ...)` with `function_num` in `1..11` and
  `options` in `0..3`.
- Hidden-row behavior is wired to workbook/engine row visibility masks:
  - `SUBTOTAL(1..11, ...)` includes hidden rows.
  - `SUBTOTAL(101..111, ...)` excludes manual + filter hidden rows.
  - `AGGREGATE` options `1`/`3` exclude manual + filter hidden rows.
- `AGGREGATE` options `2`/`3` ignore errors in aggregated refs.

Deferred in phase-1:

- Nested `SUBTOTAL`/`AGGREGATE` exclusion semantics (currently treated as ordinary scalar values).
- `AGGREGATE` `function_num` `12..19` and `options` `4..7` (return `#N/IMPL!`).
