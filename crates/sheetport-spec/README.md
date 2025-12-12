# sheetport-spec (FIO / SheetPort)

`sheetport-spec` defines the Formualizer I/O (FIO) manifest format used by SheetPort.

- Canonical JSON Schema: `schema/fio-0.3.json` (Draft 2019-09)
- Current spec version: `0.3.0` (`spec_version`)
- Conformance profiles: `core-v0` (default), `full-v0` (reserved)

## What Is FIO?

A FIO manifest binds typed input/output ports to workbook regions so a spreadsheet can be treated like a deterministic function:

- Inputs are written into declared locations
- The workbook is recalculated
- Outputs are read from declared locations

The manifest is a contract: it describes *what* data is expected, *where* it lives in the workbook, and *how* it is shaped.

## Profiles (Capability Gating)

Manifests may include:

```yaml
capabilities:
  profile: core-v0
```

`core-v0` limits selector usage to features expected to be broadly supported.

- `core-v0` supports: `a1`, `name`, `layout`
- `full-v0` is reserved for: `struct_ref`, workbook `table` selectors

## Validation

Rust:

```rust
use sheetport_spec::Manifest;

let yaml = std::fs::read_to_string("manifest.fio.yaml")?;
let manifest: Manifest = Manifest::from_yaml_str(&yaml)?;
manifest.validate()?;
```

CLI (from workspace root):

```bash
cargo run -p sheetport-spec --bin fio-lint -- tests/fixtures/supply_planning.yaml
cargo run -p sheetport-spec --bin fio-lint -- tests/fixtures/supply_planning.yaml --normalize
```

`--normalize` emits a canonicalized YAML form (sorted/deduped where applicable).

## Versioning

- `spec_version` in the manifest is the authoritative spec version.
- The crate version is expected to track the supported spec version.
