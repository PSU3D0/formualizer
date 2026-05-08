# Engine Contracts

This document defines the formal contracts that the formualizer engine
maintains regardless of `FormulaPlaneMode` configuration.

## Structural ops and value visibility

After any of the following operations, the engine clears computed
values for affected cells. Reads return `None` (i.e., absent) until
the next `evaluate_all` call recomputes them.

### Operations that trigger clear

- `Engine::insert_rows(sheet, before, count)` clears computed cells
  in `sheet` from row `before` onward.
- `Engine::delete_rows(sheet, start, count)` clears computed cells
  in `sheet` from row `start` onward.
- `Engine::insert_columns(sheet, before, count)` clears computed
  cells in `sheet` from column `before` onward.
- `Engine::delete_columns(sheet, start, count)` clears computed
  cells in `sheet` from column `start` onward.
- `Engine::add_sheet(name)` clears all computed cells in all sheets
  (because cross-sheet formulas may have had their references healed).
- `Engine::remove_sheet(sheet_id)` clears all computed cells in all
  remaining sheets (because cross-sheet formulas may have been
  tombstoned).

### Why

Structural ops can shift formula references such that previously-
computed values no longer correspond to the cells they're stored at.
Without clearing, reads would return values that don't match the
formulas at those positions. Clearing forces the user to call
`evaluate_all` before reads return reliable results.

### Both modes honor this contract

`FormulaPlaneMode::Off` and `FormulaPlaneMode::AuthoritativeExperimental`
implement this contract identically. Neither mode preserves stale
computed values across structural ops.

### Implications for users

Code that performs structural ops MUST call `evaluate_all` before
reading computed values. The pattern:

```rust
engine.insert_rows("Sheet1", 5, 10)?;
let value = engine.get_cell_value("Sheet1", 1, 2); // may be None
engine.evaluate_all()?;
let value = engine.get_cell_value("Sheet1", 1, 2); // now reliable
```

### Forward compatibility

Future versions may introduce lazy-read semantics where `get_cell_value`
auto-evaluates dirty cells on demand. That change would relax the
"call `evaluate_all` first" requirement but would NOT change the
underlying state model: computed cells are still cleared on structural
ops; lazy reads would just hide that detail.

## Other contracts

(This section to be expanded as additional contracts are formalized.)
