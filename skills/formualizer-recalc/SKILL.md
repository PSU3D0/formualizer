---
name: formualizer-recalc
description: Recalculate cached formula values after modifying .xlsx files, before reading computed values or handing workbooks off. Use with openpyxl or another existing workbook editor.
---

# XLSX edit → recalc → inspect → fix

openpyxl does not calculate formulas, and saving formula workbooks drops caches.
Cached-value readers can therefore see missing or stale results. Keep your editor;
run recalculation as the last writing step before reading or handing off the file.

## Get the native CLI

These distribution channels are planned, not yet published; use a source build
until a CLI-enabled release is available:

```sh
uvx formualizer recalc file.xlsx --json
# Or: pip install formualizer
python -m formualizer recalc file.xlsx --json
npx formualizer-cli recalc file.xlsx --json
# Or: cargo install formualizer-cli / cargo binstall formualizer-cli
# Or: download the native archive + SHA256SUMS from GitHub releases:
# https://github.com/psu3d0/formualizer/releases
# Source checkout:
cargo run --release -p formualizer-cli -- recalc file.xlsx --json
```

The installed command is `formualizer`. npm's `formualizer-cli` uses native
binaries, not the `formualizer` WASM library. No Pyodide CLI or npm WASM fallback.

## Workflow

1. Edit inputs/formulas with the existing tool and save the `.xlsx`.
2. Run `formualizer recalc file.xlsx --json`, capturing stdout and exit code.
3. Parse the single JSON object; branch on exit code and `status` below.
4. On successful computation, inspect `error_cells` and `errors` before accepting.
5. If errors are unexpected, fix inputs/formulas, save, then recalc again.
6. Read computed values with openpyxl `data_only=True` or another cached reader.
   Do not save that read-only values view.

Python invocation after saving (native formualizer wheel installed):

```python
import json
import subprocess
import sys

p = subprocess.run(
    [sys.executable, "-m", "formualizer", "recalc", "file.xlsx", "--json"],
    capture_output=True, text=True, check=False,
)
if not p.stdout.strip():
    raise RuntimeError(p.stderr or "CLI produced no JSON")
r = json.loads(p.stdout)
if p.returncode == 2:
    raise SystemExit(f"Refused; do not retry: {r['refusal']}")
if p.returncode != 0:
    raise SystemExit(f"Stop and diagnose: {p.returncode}: {r['message']}")
print(r["status"], r["error_cells"], r["errors"], r["errors_truncated"])
# Inspect errors before handing off; fix and repeat if unexpected.
```

Replace argv with `["formualizer", "recalc", "file.xlsx", "--json"]` to use
the standalone binary. Do not use `check=True`: nonzero codes need distinct actions.

| Exit | Status | Action |
| --- | --- | --- |
| 0 | `written` / `unchanged` / check `current` | Inspect formula errors; accept only appropriate results. |
| 1 | `error` | Diagnose I/O, input or engine failure; do not claim updated caches. |
| 2 | `refused` | Read `refusal.feature` / `context`. Do not retry the same workbook. Leave caches as-is or use another engine; do not claim recalculation. |
| 3 | check `stale` | Nothing written; run without `--check` to update. |
| 64 | `error` | Fix command-line arguments. |
| 130 | `interrupted` | Nothing written; resume only when intended. |

Formula errors such as `#DIV/0!` are computed results, not command failures.
`errors` entries have `sheet`, `cell`, `error`; `error_cells` is the total.
`errors_truncated` means locations were omitted; `--max-errors N` changes the
default 20-location cap, not the count. Branch on fields, not `message` text.

## Verify or preserve an input

```sh
formualizer recalc file.xlsx --check --json  # 0 current, 3 stale; never writes
formualizer recalc file.xlsx -o calculated.xlsx --json
```

Default writes atomically in place; a true no-op preserves bytes and mtime.
`-o` always publishes the destination, even with zero changes; the input is
untouched unless the output is that same path. Symlink destinations are refused.

## Boundaries and don'ts

- Only the strict supported XLSX subset is recalculated; no fallback engine.
  Data tables, connection-backed tables, missing table-managed cell formulas,
  external links and unsupported names/metadata are
  refused, as are unsupported package structures/results and resource bounds.
- New multi-cell spills and validated existing XLDAPR anchors are supported,
  including `A1#`. Fresh ordinary 1×1 results stay scalar (`A1#` is `#REF!`);
  existing anchors retain identity after collapse. Do not infer from function names.
- Formula text and untouched styles/drawings/package content are preserved;
  formula/generated caches and spill geometry/required metadata can change.
  Source-declared spill children are regenerated, not independent inputs.
- Recalc after every openpyxl save, which drops caches again. A re-save can
  strip dynamic metadata: the next recalc retains a fixed-size CSE array,
  without growth/shrink or new metadata. Scalars/errors broadcast, short
  results pad with `#N/A`, excess results truncate, and Empty members become
  zero. `A1#` readers of these anchors become `#REF!`; range readers work.
- Array-condition `IF` works elementwise, including `SUM(IF(...))` in CSE
  arrays. Singleton axes broadcast; incompatible shapes return `#VALUE!`.
- Do not run concurrent writers on the same file: atomic writes are not CAS.
- Do not claim full Excel equivalence or silently accept a refusal.

Excel tables retain their original XML and geometry. Supported tables require valid relationships, bounded nonoverlapping ranges and matching column headers. Calculated columns and totals must have worksheet formulas in every managed cell; write each new row formula before recalculation. Multi-cell spills into or from a table produce `#SPILL!`; scalar 1x1 results remain valid.

Volatile formulas are recomputed per run with one clock sample and the configured RNG policy. Rerun byte identity is conditional on unchanged samples; `--check` reports stale when they differ. Hidden-row intersections in `SUBTOTAL`/`AGGREGATE` are refused rather than guessed (zero-height and collapsed-outline rows count as hidden), including dynamic ranges, range operators over functions and LET/LAMBDA-bound arguments whose intersection cannot be proved. Iterative/stale results are still refused.
