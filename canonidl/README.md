# CanonIDL — the minimal, machine-checkable spec for cross-lang parity

CanonIDL is a tiny, boring, **normative** schema that everything else hangs off of: bindings, tests, witness logs, Lean proofs, and receipts. Think “OpenAPI for libraries” with explicit semantics, determinism, and language mappings.

Below is the full shape: concepts, schema, canonicalization, and the toolchain that makes it actionable.

---

## 1) Goals & non-goals

**Goals**

* Single source of truth for API **surface + semantics** (MUST/SHALL).
* Machine-checkable drift detection across Rust / PyO3 / WASM / C-FFI.
* Determinism knobs (FP mode, seeds), **indexing discipline**, and **error model** captured as requirements with stable IDs (**CIDs**).
* Serve as an input to: codegen, conformance harness, witness checker, and Lean proof obligations.

**Non-goals**

* Replacing your human-readable spec; CanonIDL references it and anchors CIDs into it.
* Being a full formal language; it’s deliberately a **small JSON/YAML schema** you can lint and sign.

---

## 2) CanonIDL object model

* **spec**: identity & version of the bundle.
* **requirements\[]**: normative clauses with **CID**s (RFC-2119 terms).
* **surface**: classes, methods, fields, and **signatures**.
* **types**: discriminated unions/structs you expose (e.g., `LiteralValue`).
* **effects**: pre/postconditions, error channels, determinism flags, and **what witness events must fire**.
* **mappings**: language-specific rules (naming, int64, dates, ownership).
* **capabilities**: feature gates / negotiated flags (portable fallbacks).
* **tests** (optional pointers): goldens/property specs this CanonIDL expects.
* **attestation** (optional at authoring time): digest, signers; produced in CI.

---

## 3) Schema (concise, extensible)

> Author in YAML for humans; toolchain **canonicalizes to JSON** for hashing.

```yaml
# canonidl.yaml
spec:
  id: "formualizer/0.1.0-draft"
  version: "0.1.0"
  digest: null            # filled in by tool; sha256 over canonical JSON
  doc_anchors:
    - { cid: "FZ-INDEX-001", href: "spec/normative.md#indexing" }

requirements:
  - id: "FZ-INDEX-001"
    level: "MUST"         # MUST|SHOULD|MAY (RFC 2119)
    text: "All public APIs use 1-based row/col; 0 is invalid."
    rationale: "Interop with A1 notation; avoids off-by-one drift."
    tags: ["api","indexing"]

  - id: "FZ-FP-IEEE-003"
    level: "MUST"
    text: "IEEE-754 double, ties-to-even; FMA and FTZ/DAZ disabled."
    tags: ["determinism","numeric"]

types:
  - name: "LiteralValue"
    cid: "FZ-TYPE-LV-001"
    kind: "union"
    discriminants:
      - { tag: "Int",      ty: "i64" }
      - { tag: "Number",   ty: "f64" }
      - { tag: "Boolean",  ty: "bool" }
      - { tag: "Text",     ty: "string" }
      - { tag: "Empty" }
      - { tag: "Date",     ty: "date" }        # y/m/d (naive)
      - { tag: "Time",     ty: "time" }        # hh/mm/ss
      - { tag: "DateTime", ty: "datetime" }    # naive
      - { tag: "Duration", ty: { kind:"duration", unit:"second", nanos:true } }
      - { tag: "Array",    ty: { kind:"array2d", of:"LiteralValue" } }
      - { tag: "Error",    ty: "ExcelError" }
      - { tag: "Pending" }

  - name: "ExcelError"
    cid: "FZ-TYPE-ERR-001"
    kind: "struct"
    fields:
      - { name:"kind", ty:{ kind:"enum", cases:["Div0","Ref","Name","Value","Num","Null","Na","Spill","Calc","Circ","Cancelled","Custom"]}}
      - { name:"message", ty:"string?", doc:"optional human-readable" }

surface:
  classes:
    - name: "Engine"
      tier: 2
      methods:
        - name: "evaluate_cell"
          cid: "FZ-API-ENG-011"
          in:
            - { name:"sheet", ty:"string" }
            - { name:"row",   ty:{ kind:"int", min:1 }, cid:"FZ-INDEX-001" }
            - { name:"col",   ty:{ kind:"int", min:1 }, cid:"FZ-INDEX-001" }
          out: { ty:"LiteralValue?" }
          effects:
            requires: ["FZ-FP-IEEE-003"]
            domain_errors: ["#REF!","#VALUE!","#DIV/0!"]
            host_errors: ["INVALID_ARG","CANCELLED","TIMEOUT"]
            witness:
              - { event:"demand_eval", fields:["sheet","row","col"], cid:"FZ-WIT-DEM-001" }

        - name: "evaluate_all"
          cid: "FZ-API-ENG-010"
          in:  []
          out: { ty:"EvaluationResult" }
          effects:
            preserves: ["FZ-SPILL-ATOMIC-002"]
            witness:
              - { event:"spill_plan", cid:"FZ-WIT-SPL-001" }
              - { event:"commit",     cid:"FZ-WIT-SPL-002" }

mappings:
  - lang: "typescript"
    rules:
      - { cid:"FZ-MAP-TS-INT64-001", text:"Int(i64) maps to bigint; JSON serializes as decimal string." }
      - { cid:"FZ-MAP-TS-NAMES-001", text:"Classes PascalCase; methods camelCase." }
      - { cid:"FZ-MAP-TS-DATETIME-001", text:"Use structs, not JS Date; timezone-naive." }

  - lang: "c-ffi"
    rules:
      - { cid:"FZ-MAP-C-OWN-001", text:"All returned memory is freed via fz_free/fz_array_free." }
      - { cid:"FZ-MAP-C-ABI-001", text:"Structs #[repr(C)] with abi_size/abi_ver headers." }

capabilities:
  - id: "calc.parallel"
    level: "MAY"
    text: "Parallel scheduling permitted if `capabilities.calc.parallel=true`."
```

> **Meta-schema:** CanonIDL itself is validated with a JSON-Schema we ship (`canonidl.schema.json`). That keeps it tool-friendly.

---

## 4) Canonicalization & digest (so receipts are reproducible)

* **Serialization:** YAML → JSON with:

  * UTF-8 NFC,
  * keys sorted lexicographically,
  * numbers normalized (no `-0`, no `.0` unless integral, no exponent unless needed),
  * booleans lower-case.
* **Big ints:** serialized as **strings** with `/^[-]?[0-9]+$/`.
* **Digest:** `sha256` over canonical JSON bytes.
* **Spec digest** becomes `spec.digest`; it is referenced by:

  * binding manifests,
  * witness manifests,
  * Lean proof receipts.

---

## 5) CIDs (clause identifiers)

* Stable, human-readable: `FZ-<AREA>-<NNN>` (e.g., `FZ-INDEX-001`, `FZ-FP-IEEE-003`).
* **Don’t** reassign; deprecate with `superseded_by`.
* Reference from:

  * `requirements[].id`,
  * `surface.*.cid`,
  * `types.*.cid`,
  * `mappings.*.rules[].cid`,
  * witness events (`cid` field),
  * Lean obligations.

---

## 6) Types & the mini type algebra

Primitive atoms:

* `bool`, `string`, `i64`, `u64`, `f64`,
* `date`, `time`, `datetime`,
* `duration{ unit: second|millisecond, nanos: bool }`.

Combinators:

* `enum{cases[]}`, `struct{fields[]}`, `union{discriminants[]}`,
* `array{of}`, `array2d{of}`,
* `option` via `string?` shorthand,
* `int{min?,max?}` with `min:1` for **int1** (1-based index discipline).

**Why `array2d`?** Avoid nested lists ambiguity across langs; row-major transport is universal and easy to compare.

---

## 7) Effects, errors & determinism

Each method can declare:

* **requires / preserves**: CIDs it depends on or guarantees (e.g., FP mode, spill atomicity).
* **domain\_errors**: as **values** (`#REF!`, `#VALUE!`, …).
* **host\_errors**: as **statuses** (`INVALID_ARG`, `OOM`, `UNSUPPORTED`, `CANCELLED`, `TIMEOUT`, `ABI_MISMATCH`, `PLUGIN_ERROR`).
* **determinism**: `{ fp:"ieee754-binary64", seed:"u64", volatility:"OnRecalc|OnOpen|Always" }`.
* **witness obligations**: which events **must** appear during tests.

This lets the harness assert “value equality + same semantic path.”

---

## 8) Witness schema (test-only, but normative)

A single JSONL stream per run; each line:

```json
{
  "ts": null,                 // timestamps forbidden for reproducibility
  "event": "spill_plan",      // enumerated; defined in CanonIDL
  "cid": "FZ-WIT-SPL-001",    // points to a requirement
  "data": {
    "sheet": "Sheet1",
    "anchor": "A1",
    "rows": 3,
    "cols": 2
  }
}
```

**Rules**

* Key set and ordering are fixed.
* No nondeterministic fields.
* Engines **MUST** produce identical witness streams across languages for the same golden and seed (or CanonIDL must mark events “informative” if language-dependent).

---

## 9) Binding manifests (alignment contract per language)

Each binding includes `bindings/<lang>/manifest.yaml`:

```yaml
implements: "formualizer/0.1.0-draft"
spec_digest: "sha256:…"
lang: "python"
package: "formualizer"
symbols:
  - cid: "FZ-API-ENG-011"
    public: "Engine.evaluate_cell"
    signature: "(sheet:str,row:int,col:int)->Optional[LiteralValue]"
    maps:
      - "FZ-MAP-PY-NAMES-001"
      - "FZ-INDEX-001"
coverage:
  requirements:
    implemented: ["FZ-INDEX-001","FZ-FP-IEEE-003","FZ-SPILL-002"]
    unsupported: []
attestation:
  build:
    rustc: "1.81.0"
    python: "3.11.8"
    pyo3: "0.21"
    tool: "canonidl 0.2.1"
  signature: "sigstore:…"
```

**canonidl-check** compares `manifest.yaml` against CanonIDL:

* every `surface.*.cid` must be present with compatible signature,
* every **mapping rule** required by the spec is claimed,
* digest matches,
* optional: run smoke goldens.

---

## 10) Conformance: goldens & properties, defined *by reference*

CanonIDL points at tests:

```yaml
tests:
  goldens:
    - id: "G-SPL-001"
      cid: "FZ-SPILL-002"
      path: "goldens/spill/g001.json"
  properties:
    - id: "P-FP-ROUND-001"
      cid: "FZ-FP-IEEE-003"
      generator: "roundtrip://round-bankers"
      domain: { numbers: "edgecases-f64" }
      tolerance: "ulps=0"   # exact for these inputs
```

The **harness** loads CanonIDL, then:

* discovers referenced tests,
* executes them against each binding,
* compares **values + witness**,
* emits a **Verification Manifest** (for CertiSpec).

---

## 11) Lean obligations (selective, high-leverage)

CanonIDL lists obligations with Lean entry points:

```yaml
proofs:
  obligations:
    - cid: "FZ-SPILL-002"
      file: "proofs/spill_atomicity.lean"
      theorem: "Spill.atomic_commit_or_anchor_spill"
      witness_check: true   # there is a JSONL→Lean checker
```

A small Lean program checks either:

* **direct theorems** over an abstract step semantics, or
* **proof-carrying tests** where the witness contains a proof sketch that Lean replay-checks.

---

## 12) Versioning & evolution

* **Compat rules**

  * **Patch**: add clarifications; no signature or requirement changes.
  * **Minor**: additive surfaces/requirements with `level: MAY` or `SHOULD` defaults; `superseded_by` allowed.
  * **Major**: may remove or change semantics; new spec `id`.

* **Deprecation**

  * Mark requirement with `deprecated: true` and `superseded_by: "FZ-…"`; keep for ≥1 minor cycle.

* **Feature gates**

  * Put gated items under `capabilities` and reference the gate in method `effects.requires`.

---

## 13) Tooling

* `canonidl lint canonidl.yaml`
  Validates against the meta-schema, checks CIDs, RFC-2119 wording.

* `canonidl digest canonidl.yaml`
  Produces canonical JSON and writes `spec.digest`.

* `canonidl gen --lang ts|py|c --out …`
  Optional: stub code, `.d.ts`, C headers, and **binding manifest** skeletons.

* `canonidl check-binding bindings/<lang>/manifest.yaml`
  Ensures alignment with CanonIDL; outputs a machine-readable report (for CertiSpec).

* `canonidl run --langs rust,py,ts --emit-witness`
  Runs goldens/properties across languages; emits a **Verification Manifest** (signed).

---

## 14) Worked micro-example (end-to-end)

**CanonIDL excerpt**

```yaml
requirements:
  - id: "FZ-INDEX-001"
    level: "MUST"
    text: "1-based indices for row/col; 0 invalid (host INVALID_ARG)."

surface:
  classes:
    - name: "Sheet"
      methods:
        - name: "set_value"
          cid: "FZ-API-SHEET-003"
          in:
            - { name:"row", ty:{kind:"int",min:1}, cid:"FZ-INDEX-001"}
            - { name:"col", ty:{kind:"int",min:1}, cid:"FZ-INDEX-001"}
            - { name:"value", ty:"LiteralValue" }
          out: { ty:"void" }
          effects:
            host_errors: ["INVALID_ARG"]
```

**Golden**

```json
{
  "id": "G-INDEX-ROW0",
  "steps": [
    {"op":"sheet.set_value","row":0,"col":1,"value":{"type":"Number","value":1}}
  ],
  "expect": { "host_error": "INVALID_ARG" }
}
```

**Witness**

```json
{"event":"api_call","cid":"FZ-INDEX-001","data":{"method":"set_value","row":0,"col":1}}
{"event":"host_error","cid":"FZ-INDEX-001","data":{"code":"INVALID_ARG"}}
```

**Binding manifest claim (Python)**

```yaml
symbols:
  - cid: "FZ-API-SHEET-003"
    public: "Sheet.set_value"
    signature: "(row:int,col:int,value:LiteralValue)->None"
    maps: ["FZ-INDEX-001"]
```

The harness asserts: manifest aligns → golden fails with `INVALID_ARG` → witness shows `api_call` + `host_error` tied to `FZ-INDEX-001` → **PASS**.
