# Preparation errors and spreadsheet guards

Decision for [#454](https://github.com/PSU3D0/formualizer/issues/454): **retain the existing preparation-exception policy**. This correctness tranche adds no reference-error mode and does not turn binding failures into catchable cell values.

## Two different boundaries

`IFERROR` and `IFNA` operate during formula evaluation. They cannot run before the formula and its dependencies have been prepared. Consequently, these are intentionally different:

| Formula | Existing behavior |
|---|---|
| `=IFERROR(#REF!,456)` | Returns 456 |
| `=IFNA(#REF!,456)` | Returns a `#REF!` cell error |
| `=IFNA(#N/A,456)` | Returns 456 |
| `=IFERROR(SUM(IFERROR(#REF!,0)),1)` | Returns 0 through the inner guard |
| `=IFERROR(NOSHEET!A1,456)` when NOSHEET does not exist | Reference preparation fails; the guard does not run |
| `=IFERROR(SUM(IFERROR(NOSHEET!A1,0)),1)` | Preparation fails; neither fallback is a substitute for evaluation |
| `=IFERROR(7,1/0)` | Returns 7 without evaluating the fallback |
| `=IFERROR(7,NOSHEET!A1)` | Can fail during dependency binding despite runtime branch laziness |
| `=IFERROR(MissingName,456)` | The current unresolved-name evaluation path returns 456 |
| `=IFNA(MissingName,456)` | Returns a `#NAME?` cell error |
| `=IFERROR(SUM(MissingTable[Amount]),456)` | Undefined-table preparation fails |
| `=IFERROR(UnknownFunction(1),456)` | The current unknown-function evaluation path returns 456 |
| `=IFERROR(A1,456)` in A1 | Returns a circularity cell error without evaluating the guard normally |
| Malformed `=IFERROR(1+,456)` imported under the default error-cell parse policy | The whole formula becomes a parse-error cell; no guard can be evaluated |

A missing-sheet binding failure and a literal `#REF!` may share an Excel error kind. Their provenance and lifecycle stage differ. Consumers must not identify catchability by kind alone, by error-message parsing, or by the presence of IFERROR in the source formula.

With immediate validation, assignment itself may fail. With deferred/imported formulas, preparation can fail on evaluation instead. The Rust workbook surface reports engine failures through `IoError::Engine`; Python exposes failures as exceptions. Fixes to source retention and targeted preparation do not alter this boundary.

## Request failures remain failures

Graph admission rejection and observed cancellation abort the request rather than yielding a guard's fallback. Existing resource provenance and `Cancelled` classification are preserved. Cooperative cancellation is checked at engine checkpoints; it is not parser preemption.

This decision does not impose a new universal policy on every parser or provider error. Existing formula-parse policies, unsupported-function behavior and cell-error production remain unchanged. A failure already represented as a spreadsheet value may be caught according to existing function semantics; an unsupported preparation/provider failure is not newly converted to a value by this tranche. Circularity, external bindings, missing names/tables and deleted-sheet handling also retain their existing phase-specific behavior.

In particular, there is no blanket `Err(_) => LiteralValue::Error(...)` conversion at the preparation boundary and no fallback to imported cached values presented as successful recalculation.

## Future semantic changes

Making unresolved references catchable would need a separate explicit API decision, not a getter fix or a broad exception handler. It would require:

- Oracle-backed classification for sheets, names, tables, external references, guards and error predicates.
- Typed distinction between semantic reference failures and operational/unsupported failures.
- Defined dependency invalidation when a formerly missing target becomes available, without unbounded placeholder allocation or workbook reconstruction.
- An explicit compatibility and release policy for callers that currently reject a workbook on preparation failure.

That implementation is deferred. This document resolves the policy decision for the current tranche; it does not claim new Excel reference-error parity.

## Regression coverage

`crates/formualizer-workbook/tests/preparation_error_policy.rs` covers direct/nested guards, IFNA specificity, runtime laziness versus binding, full/target preparation failures, graph admission and pre-signalled cancellation. These tests protect the boundary; they are not a comprehensive external-provider or circularity matrix.
