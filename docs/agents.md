# Recalculate XLSX files after agent edits

Keep your existing editor. The loop is **edit → save → `formualizer recalc file.xlsx --json` → branch on exit code/status → inspect `errors` → fix and repeat**. Only hand off or read computed values after a successful recalculation.

openpyxl writes formulas but does not calculate them; saving a formula workbook removes its cached results. Readers such as openpyxl with `data_only=True`, pandas and workbook viewers rely on those caches and can see `None` or stale numbers when caches are absent or outdated. Formualizer fills supported formula caches without replacing your editing tool.

## Install or run

> **Not yet published:** these install channels go live with the first release that includes `formualizer recalc`. Until then, build from a source checkout with `cargo run --release -p formualizer-cli -- recalc file.xlsx --json`.

```sh
uvx formualizer recalc file.xlsx --json          # native Python wheel, no install step
pip install formualizer                        # then: formualizer recalc file.xlsx --json
python -m formualizer recalc file.xlsx --json    # same CLI through the Python module
npx @formualizer/cli recalc file.xlsx --json     # native npm launcher, not the WASM library
npm i -g @formualizer/cli                      # then: formualizer recalc file.xlsx --json
cargo binstall formualizer-cli                 # prebuilt release binary
cargo install formualizer-cli                  # build the formualizer command from crates.io
```

Or download the matching `formualizer-cli-v<version>-<target>` archive and `SHA256SUMS` from [GitHub releases](https://github.com/psu3d0/formualizer/releases), verify the checksum, extract it and put `formualizer` on PATH. Prebuilt binaries cover Linux x64/arm64 (glibc or static musl), macOS x64/arm64 and Windows x64. The CLI is not available in Pyodide, and npm has no WASM fallback: the unscoped `formualizer` npm package is the library, not the CLI.

## Python edit/recalc/read loop

After installing openpyxl and a CLI-enabled native formualizer wheel, run this in a directory where `file.xlsx` may be created or replaced:

```python
import json
import subprocess
import sys
from openpyxl import Workbook, load_workbook

# Edit and save with your usual tool. Repeat this phase to fix reported errors.
wb = Workbook()
ws = wb.active
for row, value in enumerate([10, 20, 30], start=1):
    ws.cell(row, 1, value)
ws["B1"] = "=SUM(A1:A3)"
ws["C1"] = "=SEQUENCE(3)"
wb.save("file.xlsx")
wb.close()

p = subprocess.run(
    [sys.executable, "-m", "formualizer", "recalc", "file.xlsx", "--json"],
    capture_output=True, text=True, check=False,
)
# Launcher/import failures may not produce CLI JSON. Never treat them as success.
if not p.stdout.strip():
    raise RuntimeError(p.stderr or "CLI produced no JSON")
result = json.loads(p.stdout)
print(p.returncode, result["status"])
if p.returncode == 2:
    raise SystemExit(f"Refused; do not retry: {result['refusal']}")
if p.returncode != 0:
    raise SystemExit(f"Stop and diagnose: {p.returncode}: {result['message']}")
assert result["status"] in {"written", "unchanged"}
for error in result["errors"]:
    print(error["sheet"], error["cell"], error["error"])
if result["error_cells"]:
    raise SystemExit("Inspect errors, fix inputs/formulas, save, then recalc again")

values = load_workbook("file.xlsx", data_only=True)
print(values.active["B1"].value)  # 60
print([values.active[f"C{r}"].value for r in range(1, 4)])  # [1, 2, 3]
values.close()  # read only: do not save this workbook
```

To use the standalone binary instead, replace the subprocess argv with `["formualizer", "recalc", "file.xlsx", "--json"]`. Do not use `check=True`: a refusal or stale check needs its own branch.

## Shell loop step

After your editor saves `file.xlsx`, use this POSIX shell step (requires `jq`). Repeat the edit/save phase only when inputs or formulas need correction.

```sh
rc=0
result=$(formualizer recalc file.xlsx --json) || rc=$?
printf '%s\n' "$result"
case "$rc" in
  0) printf '%s\n' "$result" | jq '{status, error_cells, errors, errors_truncated}'
     # Inspect errors; if unexpected, fix inputs/formulas, save and recalc again.
     ;;
  2) printf '%s\n' "$result" | jq '.refusal'
     echo 'Refused: do not retry or claim recalculation.' >&2; exit 2 ;;
  3) echo 'Check found stale caches: run without --check.' >&2; exit 3 ;;
  1|64|130) echo "Stop and diagnose CLI exit $rc." >&2; exit "$rc" ;;
  *) echo "Unexpected launcher/process exit $rc." >&2; exit "$rc" ;;
esac
```

## Branch on the outcome

| Exit | JSON status | Agent action |
| --- | --- | --- |
| 0 | `written`, `unchanged`; `current` with `--check` | Computation succeeded. Inspect `errors` before accepting results. |
| 1 | `error` | Diagnose I/O, invalid input or engine/internal failure; do not claim updated caches. |
| 2 | `refused` | Read `refusal.feature` and `refusal.context` ([what they mean](cache-only-xlsx.md#refusal-messages)). **Do not retry the same unsupported workbook.** Leave caches as-is or use another engine; never claim values were recalculated. |
| 3 | `stale` | `--check` only: nothing written. Run without `--check` to publish updates. |
| 64 | `error` | Correct command-line arguments. |
| 130 | `interrupted` | Cancellation before publication; nothing written. Resume only when intended. |

Excel table-bearing sheets are supported only within the validated subset described in [cache-only XLSX eligibility](cache-only-xlsx.md). When extending a calculated column, write a worksheet formula into every new row: table-level formula metadata alone is refused. Bare table names such as `SUM(Table1)` mean the data body. In table-bearing workbooks, `INDIRECT` needs literal text that names no table; cell-sourced `INDIRECT` text is refused there. `[#This Row]` is supported only in the data body. Shared formulas whose sheet qualifiers look like cell references (for example `'Q1'!` or `'FY2024'!`) are refused; write ordinary per-cell formulas instead. Table XML and geometry are never rewritten. There is no automatic fallback engine; branch on explicit refusal rather than publish stale values.

### Verify without writing

```sh
formualizer recalc file.xlsx --check --json
```

Exit 0 / `current` means caches and spill shape are current; exit 3 / `stale` means they would change. Neither writes a file, even with `-o`.

### Read formula errors

`errors` contains `{sheet, cell, error}` locations such as `Sheet1`, `B4`, `#DIV/0!`. These are **formula results, not tool failures**: representable error results can be written successfully with exit 0. Decide whether they are expected; otherwise fix inputs/formulas and repeat the whole edit/save/recalc loop. `error_cells` is the total count; `errors_truncated` means some locations were omitted. Use `--max-errors N` to raise the default 20-location cap (0 lists none); it does not change the total count.

## Spills, preservation and safety

- New multi-cell results such as `=SEQUENCE(3)` become dynamic-array anchors; validated existing XLDAPR anchors can grow, shrink or collapse. `A1#` and `_xlfn.ANCHORARRAY(A1)` read the current spill.
- **1×1 limitation:** a fresh ordinary formula returning one cell remains scalar: its `A1#` reader returns `#REF!`. An existing validated anchor retains spill identity after collapsing to one cell. Do not infer identity from function names.
- Source-declared spill children are generated caches, not independent inputs. Obsolete member caches are cleared while their styles/comments remain. Supported blocked anchors cache `#SPILL!`; unsupported spill publication is refused. This subset does not promise full Excel equivalence.
- The strict source-preserving path keeps formula text, styles, drawings, names and untouched package content. It changes formula/generated caches and spill geometry, including required dynamic metadata, relationships and worksheet dimensions. It does not rebuild the workbook through an editor.
- Default writes **in place**, atomically; a true in-place no-op leaves bytes and mtime untouched. Use `-o calculated.xlsx` to keep the input separate: the requested output is always published, even with zero changes. If output is the input path, it targets that input. Symlink destinations are refused.
- **Recalc after every openpyxl save**: saving drops formula caches again. Reading with `data_only=True` is fine, but do not save that values-only view. An openpyxl re-save can also remove dynamic metadata: the next recalc succeeds as a fixed-size CSE array, keeping its original extent. Shorter results pad with `#N/A`; larger results truncate; `A1#` readers become `#REF!`. Use ordinary range readers when this fixed-extent behavior is intended.
- Array-condition `IF` selects elementwise, including inside `SUM` and fixed CSE arrays. Scalar branches and singleton axes broadcast; incompatible shapes return `#VALUE!`.
- **Don't run concurrent writers on the same file.** Atomic replacement is not compare-and-swap protection against another editor.

See [cache-only XLSX eligibility, refusals and resource bounds](cache-only-xlsx.md) for the supported subset (including fixed-extent array policies and refusals for tables, external links and unsupported names/metadata), and [the authoritative CLI reference](cli.md) for JSON fields, counters and publication details. Limits are bounded by default and have no CLI tuning flags.

A copyable [agent skill](../skills/formualizer-recalc/SKILL.md) teaches the same workflow without requiring these docs alongside it.

Volatile formulas are recomputed with one request clock sample and the configured RNG policy on each source-recalc run. Their dependents must agree with that same evaluation; byte-identical reruns are conditional on unchanged sampled values, and `--check` reports stale when a sample differs. `SUBTOTAL`/`AGGREGATE` ranges intersecting stored hidden row or active-filter rows are refused because source row visibility is not hydrated; zero-height rows, rows grouped under a collapsed outline and `zeroHeight` sheets count as hidden. Dynamic reducer ranges, range operators over functions or names, and LET/LAMBDA-bound reducer arguments are refused when their hidden-row intersection cannot be proved.
