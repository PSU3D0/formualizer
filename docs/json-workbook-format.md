# JSON workbook format

The JSON workbook format is what `Workbook::from_json` (Rust), `Workbook.fromJson`
(JavaScript/WASM) and `Workbook.load_bytes(..., backend="json")` (Python) accept. It is a
convenient way to construct a workbook without an XLSX file.

Everything below is derived from the `json` backend in
`crates/formualizer-workbook/src/backends/json.rs`.

> **Tables do not require this format.** Since 0.8.0 a table can be defined at runtime over
> cells you have already written, with no serialise-and-reload round trip. See
> [Defining tables at runtime](#defining-tables-at-runtime).

## Top level

```jsonc
{
  "version": 1,              // optional, defaults to 1
  "compression": null,       // optional: null | "None" | "Lz4"
  "sources": [],             // optional, see Sources
  "defined_names": [],       // optional, see Defined names
  "sheets": {}               // map of sheet name -> Sheet
}
```

Every key is optional. `sheets` is a **map keyed by sheet name**, not an array.

Unknown keys at the top level are ignored, so a misplaced key is silently accepted and has no
effect. Check your nesting if something appears to load but does nothing.

## Sheet

```jsonc
{
  "cells": [],               // array of Cell, see below
  "dimensions": null,        // optional [rows, cols]
  "hidden": false,
  "date_system_1904": false,
  "merged_cells": [],
  "tables": [],              // array of Table, see below
  "named_ranges": [],
  "row_hidden_manual": [],
  "row_hidden_filter": []
}
```

`cells` is an **array**, not a map. Passing an object produces
`invalid type: map, expected a sequence`.

## Cell

```jsonc
{ "row": 1, "col": 1, "value": { "type": "Text", "value": "Nama" } }
{ "row": 2, "col": 2, "formula": "=SUM(A1:A10)" }
```

`row` and `col` are 1-based. `value` and `formula` are both optional; a cell may carry either.

### Cell values

Values are **adjacently tagged**: an object with a `type` discriminant and, for most variants, a
`value` payload. The tag is case-sensitive and capitalised.

| `type` | payload | example |
|---|---|---|
| `Int` | integer | `{ "type": "Int", "value": 42 }` |
| `Number` | float | `{ "type": "Number", "value": 3.5 }` |
| `Text` | string | `{ "type": "Text", "value": "Ani" }` |
| `Boolean` | bool | `{ "type": "Boolean", "value": true }` |
| `Empty` | none | `{ "type": "Empty" }` |
| `Date` | `YYYY-MM-DD` | `{ "type": "Date", "value": "2026-07-25" }` |
| `DateTime` | RFC 3339 | `{ "type": "DateTime", "value": "2026-07-25T09:30:00" }` |
| `Time` | `HH:MM:SS` | `{ "type": "Time", "value": "09:30:00" }` |
| `Duration` | integer | `{ "type": "Duration", "value": 3600 }` |
| `Array` | rows of values | `{ "type": "Array", "value": [[{ "type": "Int", "value": 1 }]] }` |
| `Error` | string | `{ "type": "Error", "value": "#DIV/0!" }` |
| `Pending` | none | `{ "type": "Pending" }` |

A bare scalar such as `"value": 5` is rejected: the tagged object is required.

`Int` is accepted on input but is normalised to a number when loaded, so a cell written as
`{ "type": "Int", "value": 42 }` reads back as the number `42`.

## Table

Tables live **inside a sheet**, under `sheets.<name>.tables`. A `tables` key at the top level is
ignored.

```jsonc
{
  "name": "Table1",
  "range": [1, 1, 4, 2],     // [firstRow, firstCol, lastRow, lastCol], 1-based, inclusive
  "headers": ["Nama", "Nilai"],
  "header_row": true,        // optional, defaults to true
  "totals_row": false        // required
}
```

- `range` is a **4-element array**, not an A1 string. It includes the header row when
  `header_row` is true.
- `headers` names the table's columns and must match the width of `range`.
- `header_row` defaults to `true`; `totals_row` has no default and must be supplied.

Tables are metadata over cells that already exist, so write the cells first. They do not
auto-expand: writing below or beside a table does not grow it.

### Worked example

```js
import { initializeWasm, Workbook } from 'formualizer';
await initializeWasm();

const T = (v) => ({ type: 'Text', value: v });
const N = (v) => ({ type: 'Number', value: v });

const doc = {
  sheets: {
    S: {
      cells: [
        { row: 1, col: 1, value: T('Nama') }, { row: 1, col: 2, value: T('Nilai') },
        { row: 2, col: 1, value: T('Ani') },  { row: 2, col: 2, value: N(10) },
        { row: 3, col: 1, value: T('Budi') }, { row: 3, col: 2, value: N(20) },
        { row: 4, col: 1, value: T('Cici') }, { row: 4, col: 2, value: N(30) },
      ],
      tables: [{
        name: 'Table1',
        range: [1, 1, 4, 2],
        headers: ['Nama', 'Nilai'],
        totals_row: false,
      }],
    },
  },
};

const wb = Workbook.fromJson(JSON.stringify(doc));
wb.setFormula('S', 10, 4, '=SUM(Table1[Nilai])');
wb.evaluateAll();
wb.evaluateCell('S', 10, 4); // 60
```

## Defining tables at runtime

Building a workbook in place and then defining a table avoids the JSON round trip entirely, and
is the right approach for an interactive editor where creating a table is a user action.

```js
const wb = new Workbook();
wb.addSheet('S');
wb.setValue('S', 1, 1, 'Nama');
wb.setValue('S', 1, 2, 'Nilai');
wb.setValue('S', 2, 1, 'Ani');
wb.setValue('S', 2, 2, 10);

wb.addTable({
  name: 'Table1',
  sheet: 'S',
  range: [1, 1, 2, 2],     // 1-based, inclusive, includes the header row
  headers: ['Nama', 'Nilai'],
  // headerRow defaults to true, totalsRow defaults to false
});

wb.setFormula('S', 4, 2, '=SUM(Table1[Nilai])');
wb.evaluateAll();

wb.getTables(); // [{ name, sheet, range, headers, headerRow, totalsRow }]
```

Note the binding uses camelCase (`headerRow`, `totalsRow`) while the JSON format uses snake_case
(`header_row`, `totals_row`). `addTable` rejects unknown keys rather than ignoring them.

Python:

```python
wb = fz.Workbook()
wb.add_sheet("S")
wb.set_value("S", 1, 1, "Nama")
wb.set_value("S", 1, 2, "Nilai")
wb.set_value("S", 2, 1, "Ani")
wb.set_value("S", 2, 2, 10)

wb.add_table("Table1", "S", (1, 1, 2, 2), ["Nama", "Nilai"])
wb.set_formula("S", 4, 2, "=SUM(Table1[Nilai])")
wb.evaluate_all()

wb.tables()  # [{"name": ..., "sheet": ..., "range": (1, 1, 2, 2), ...}]
```

Rust:

```rust
let mut wb = Workbook::new();
wb.add_sheet("S")?;
wb.set_value("S", 1, 1, LiteralValue::Text("Nama".into()))?;
wb.define_table("Table1", "S", (1, 1, 2, 2), vec!["Nama".into(), "Nilai".into()], true, false)?;
```

Updating, resizing and deleting a table at runtime are not yet exposed; a table's range is fixed
once defined.

## Defined names

```jsonc
{
  "name": "TaxRate",
  "scope": "workbook",              // or "sheet"
  "scope_sheet": null,              // required when scope is "sheet"
  "definition": { "type": "literal", "value": { "type": "Number", "value": 0.2 } }
}
```

`definition` is tagged by `type`, one of:

- `{ "type": "range", "address": "Sheet1!A1:B2" }`
- `{ "type": "literal", "value": <cell value> }`

## Sources

```jsonc
{ "type": "scalar", "name": "fx_rate", "version": 3 }
{ "type": "table",  "name": "trades",  "version": null }
```
