# Formula assignment and failure visibility

Logged workbook formula assignments now report binding/admission failures rather than silently retaining the previous value and returning success (#451). Python workbook and sheet setters surface the same failure as their unlogged paths. This is an observable bug fix: callers that relied on a successful return despite a rejected formula will now receive an error. No existing public Rust or Python method signature changes.

## Contracts retained

- **Immediate graph assignment:** binding/admission rejection is reported without clearing the previous cell formula/value, spill or history. Successful assignments retain the existing change-log and undo/redo behavior. This is not a new general rollback guarantee for arbitrary graph operations.
- **Deferred assignment:** text can be staged successfully before its references are validated. Failure may still occur during graph preparation/evaluation; assignment success is not a guarantee of evaluability. This fix does not add eager validation to fresh staged cells.
- **Graph-mode rectangular batches:** successful earlier assignments can remain committed when a later cell fails. These setters are not atomic batches; the existing prefix-commit policy is unchanged. Deferred batches continue to stage text.
- **Low-level Rust compatibility:** existing `VertexEditor::set_cell_formula` and `set_cell_formula_with_old_state` retain their infallible signatures and historical vertex-zero failure fallback. New code should use `try_set_cell_formula` or `try_set_cell_formula_with_old_state`, which return `Result<VertexId, ExcelError>`. Do not inspect an ID to infer success.

The fix does not disable logging, clone an entire workbook/graph, validate formulas twice, or widen preparation to the whole workbook. Spill snapshots already needed for successful undo are retained until assignment succeeds before any spill-clear event is published.

## Separate semantics

This does not change preparation exceptions into catchable Excel error values. Missing-sheet behavior inside `IFERROR`, preservation after failed full preparation, imported-target isolation and staged inspection text are tracked separately (#452–#455). Treating an unsupported or aborted computation as an Excel value or as a cached success requires an explicit policy decision, not an incidental setter fix.
