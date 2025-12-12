# FIO Manifest Specification (SheetPort)

Status: Draft

This document specifies the Formualizer I/O (FIO) manifest format, used by SheetPort to bind typed inputs/outputs to workbook locations.

Normative language: "MUST", "SHOULD", and "MAY" are used as defined in RFC 2119.

## 1. Manifest Overview

A manifest defines a set of "ports".

- An `in` port declares how clients provide values to the workbook.
- An `out` port declares how clients retrieve values from the workbook.

A conforming runtime binds the manifest to a workbook and provides deterministic I/O:

1. Validate manifest.
2. Bind each port selector to workbook coordinates.
3. Write `in` values.
4. Recalculate workbook.
5. Read `out` values.

## 2. Document Format

- The manifest MAY be represented as YAML or JSON.
- When represented as YAML, the document MUST deserialize into the canonical JSON model defined by the JSON Schema.
- Unknown fields MUST be rejected ("deny unknown fields").

Canonical JSON Schema: `schema/fio-0.3.json`.

## 3. Top-Level Fields

### 3.1 `spec`

- MUST be the string `fio`.

### 3.2 `spec_version`

- MUST be a semantic version string (e.g., `0.3.0`).
- A runtime MUST reject manifests with an incompatible major version.

### 3.3 `capabilities` (optional)

A manifest MAY declare conformance capabilities:

```yaml
capabilities:
  profile: core-v0
  features: ["..."]
```

- If omitted, `profile` MUST default to `core-v0`.
- `features` is reserved for future extensibility and MAY be ignored.

### 3.4 `manifest` (metadata)

The metadata block contains:

- `id` (string): stable identifier
- `name` (string): display name
- `description` (string, optional)
- `tags` (array of strings, optional)
- `workbook` (object, optional): advisory hints such as `uri`, `locale`, `date_system`, `timezone`
- `metadata` (object, optional): free-form JSON object

`workbook` fields are advisory and MAY be ignored unless a runtime explicitly documents support.

### 3.5 `ports`

- MUST be an array of port objects.
- Each `ports[*].id` MUST be unique within the manifest.

## 4. Profiles

Profiles gate optional/forward-looking features so runtimes can implement a stable subset.

### 4.1 `core-v0`

A `core-v0` manifest MUST NOT use:

- `struct_ref` selectors
- workbook `table` selectors

A `core-v0` manifest MAY use:

- `a1` selectors
- `name` selectors
- `layout` selectors

### 4.2 `full-v0` (reserved)

`full-v0` is reserved for future selector support:

- `struct_ref` selectors (Excel structured references)
- workbook `table` selectors (Excel tables)

Runtimes MAY reject `full-v0` manifests until implemented.

## 5. Ports

A port object has:

- `id` (string)
- `dir` ("in" | "out")
- `shape` ("scalar" | "record" | "range" | "table")
- `location` (selector)
- `schema` (type schema)

Optional fields:

- `required` (bool, default true)
- `description` (string)
- `constraints` (constraints)
- `units` (units metadata)
- `default` (JSON value, inputs only)
- `partition_key` (reserved hint; no effect in core runtimes)

A port with `dir: out` MUST NOT specify `default`.

### 5.1 Shapes and Selector Legality

Selector legality depends on the port `shape` and manifest `profile`.

`core-v0` legality:

| shape  | a1 | name | layout | struct_ref | table |
|--------|----|------|--------|------------|-------|
| scalar | ok | ok   | no     | no         | no    |
| record | ok | ok   | ok     | no         | no    |
| range  | ok | ok   | ok     | no         | no    |
| table  | no | no   | ok     | no         | no    |

`full-v0` additions (reserved):

- `struct_ref` MAY be used for scalar, record, and range.
- `table` MAY be used for table ports.

Record fields use a restricted selector set (`a1`, `name`, `struct_ref`).

## 6. Selectors

Selectors bind ports/fields to workbook regions.

### 6.1 `a1`

```yaml
location: { a1: Sheet1!A1:C10 }
```

The value MUST be an absolute A1-style reference string.

### 6.2 `name`

```yaml
location: { name: MyNamedRange }
```

The name refers to a workbook-defined named range.

### 6.3 `layout`

```yaml
location:
  layout:
    kind: header_contiguous_v1
    sheet: Inventory
    header_row: 1
    anchor_col: A
    terminate: first_blank_row
```

`layout.kind` selects a layout resolution algorithm.

#### 6.3.1 `header_contiguous_v1`

This layout resolves a rectangular region by:

- Using `sheet`, `header_row`, and `anchor_col` as the anchor.
- Determining relevant columns:
  - For `range` ports: discover contiguous columns starting at `anchor_col` by scanning header cells left-to-right and stopping at the first blank header cell.
  - For `table` ports: derive columns from the table schema:
    - if a column specifies `col`, it pins that column letter
    - otherwise columns are mapped sequentially starting at `anchor_col`

Termination (`terminate`) determines the last included row:

- `first_blank_row`: stop at the first row where all relevant columns are blank.
- `sheet_end`: stop at workbook sheet end (if sheet dimensions are available); otherwise fall back to `first_blank_row`.
- `until_marker`: stop when the anchor column contains `marker_text` (trimmed string match); blank row before marker also terminates.

Runtimes SHOULD bound scanning (e.g., max 100,000 rows) and MUST be deterministic.

### 6.4 `struct_ref` (reserved)

```yaml
location: { struct_ref: TblOrders[Qty] }
```

Reserved in `core-v0`. Requires `full-v0`.

### 6.5 `table` (reserved)

```yaml
location:
  table:
    name: TblOrders
    area: body
```

Reserved in `core-v0`. Requires `full-v0`.

## 7. Schemas

### 7.1 Scalar schema

```yaml
schema: { type: number }
```

`type` is one of:

- `string`, `number`, `integer`, `boolean`, `date`, `datetime`

### 7.2 Record schema

```yaml
schema:
  kind: record
  fields:
    field_a:
      type: string
      location: { a1: Sheet!B2 }
```

### 7.3 Range schema

```yaml
schema:
  cell_type: number
```

### 7.4 Table schema

```yaml
schema:
  kind: table
  columns:
    - { name: sku, type: string, col: A }
    - { name: qty, type: integer, col: B }
  keys: [sku]
```

## 8. Constraints

Constraints may appear on a port or on record fields.

- `min` / `max` apply to numeric types.
- `pattern` is a regex applied to the string representation of the value.
- `enum` is an explicit allow-list.
- `nullable` determines whether blank/null values are permitted.

### 8.1 Enum semantics (exact equality)

Enum entries are compared using exact JSON equality after converting the runtime value to JSON.

- Numbers are NOT normalized: `5` and `5.0` are different enum entries.
- Dates are represented as strings (`YYYY-MM-DD`).
- Datetimes are represented as strings (RFC 3339 recommended).

## 9. Validation Errors

A validator MUST return errors with:

- a `path` (JSON-path-like string) pointing to the failing field
- a human-readable `message`

Example paths:

- `ports[0].location`
- `ports[1].schema.fields.month.constraints.min`
- `capabilities.profile`
