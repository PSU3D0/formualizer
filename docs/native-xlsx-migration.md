# Native XLSX reader migration (0.13)

XLSX loading now uses the native Compatible reader in Rust, Python, JavaScript/WASM and Pyodide. Ordinary loading and source-preserving recalculation share package/XML and engine-ingress primitives, but deliberately use different admission policies. Recalculation retains its strict worksheet scanner and whole-package integrity checks. This is not a new evaluator or a universal worksheet state machine.

## Python and JavaScript

Omitting `backend` selects `native` on every host. Explicit `backend="calamine"` (JavaScript: `{ backend: 'calamine' }`) remains an alias to native, not an independent reader. Existing keyword names, JavaScript camelCase methods, load configuration, path-source choices and date semantics remain supported. `umya` remains available where previously supported for rich document reading and writing; native loading does not replace rich export.

## Ordinary loading limits

Ordinary native loading accepts limits **before** source admission. Rust uses `XlsxLoadLimits`; Python uses keyword-only `load_limits`; JavaScript uses `loadLimits` inside its existing load options. The five fields are:

| Rust/Python | JavaScript | Default |
|---|---|---:|
| `max_input_bytes` | `maxInputBytes` | **127 MiB** (133,169,152 bytes) |
| `max_entries` | `maxEntries` | 10,000 ZIP entries |
| `max_expanded_bytes` | `maxExpandedBytes` | 256 MiB actual consumed output |
| `max_part_bytes` | `maxPartBytes` | 128 MiB per consumed part |
| `max_xml_depth` | `maxXmlDepth` | 128 |

These are independent bounds, not a compression-ratio heuristic. A legitimate 20–50 MiB XLSX can exceed the expanded defaults even without images. Raise the needed part/aggregate allowance explicitly on a suitably provisioned host; raising the input allowance alone does not address decoded data or formula-model memory.

Python (partial dictionaries inherit the Rust defaults):

```python
import formualizer as fz

wb = fz.load_workbook(
    "model.xlsx",
    load_limits={
        "max_part_bytes": 512 * 1024 * 1024,
        "max_expanded_bytes": 1024 * 1024 * 1024,
    },
)
```

The keyword also works on `load_workbook_bytes`, `Workbook.load_path`, `Workbook.from_path`, `Workbook.from_bytes` and `SheetPortSession.from_manifest_path`.

JavaScript/TypeScript (the public wrapper accepts `Uint8Array` or `ArrayBuffer`):

```typescript
import { Workbook } from 'formualizer';

const response = await fetch('/model.xlsx');
const bytes = new Uint8Array(await response.arrayBuffer());
const wb = Workbook.fromXlsxBytes(bytes, {
  loadLimits: {
    maxPartBytes: 512 * 1024 * 1024,
    maxExpandedBytes: 1024 * 1024 * 1024,
  },
});
```

The raw generated WASM loader uses `Workbook.fromXlsxBytesWithOptions(bytes, options)` for options. JSON loading and empty-workbook construction do not accept XLSX-specific overrides.

Rust:

```rust
use formualizer_workbook::{
    LoadStrategy, NativeXlsxAdapter, Workbook, WorkbookConfig,
    XlsxLoadLimits, XlsxPathSource,
};

fn main() -> Result<(), formualizer_workbook::IoError> {
    let limits = XlsxLoadLimits {
        max_part_bytes: 512 * 1024 * 1024,
        max_expanded_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let reader = NativeXlsxAdapter::open_path_with_source_and_limits(
        "model.xlsx", XlsxPathSource::SharedFile, limits,
    )?;
    let _workbook = Workbook::from_reader(
        reader, LoadStrategy::EagerAll, WorkbookConfig::ephemeral(),
    )?;
    Ok(())
}
```

Explicit-limits byte and reader constructors are `open_bytes_with_limits`, `open_bytes_cancellable_with_limits` and `open_reader_with_limits`. The reader constructor accepts the existing boxed `Read + Send + Sync` interface and starts at its current position; it does not rewind it or newly require seeking.

Unknown nested keys and invalid numeric values are rejected. Python requires integers other than bool; JavaScript requires safe non-negative integers representable on its host. Zero is zero, never unlimited. Explicit limits are native-XLSX-only options, not Umya/JSON settings. The Python SHARED_FILE route recognizes workbook content rather than requiring an `.xlsx` suffix, so valid packages under suffixless or `.bin` filenames accept overrides too. The existing explicit DIRECT_MMAP suffix restriction is unchanged. Non-XLSX content still fails native admission; configuring limits does not add another format. Omission preserves existing supported loading routes. Native/calamine aliases share identical limits.

The Python binding checks configuration and compressed byte length before making its additional Rust byte copy where feasible. Incoming JS bytes have already crossed into WASM before core admission. Neither API can undo caller-owned input memory. Engine geometry, cell/shared-range, formula-spool, address-space and model-memory limits remain independent; these options are not a hard process-RSS cap or a guarantee that an arbitrary large workbook fits a browser. The Python API shape also applies to Pyodide, but actual wheel/SDK execution is a separate CI gate.

## Selective loading versus whole-package integrity

Ordinary loading reads existing supported workbook metadata, worksheets, strings/styles and needed cached external parts. It does not decompress unread images, attachments, drawings, themes or shadowed members merely to validate their payloads. Their compressed bytes remain in the retained source; this is not a rich-document deletion/export operation.

Consequently, a bad CRC or huge expanded size in an unread payload no longer blocks ordinary value/formula loading. The existing Compatible ZIP structure/index checks still apply. Selected parts are checked for CRC at true decoder EOF, actual-versus-declared decoded length, per-part output and aggregate actual output. No ZIP size claim can grant unbounded allocation. Lazy external-cache reads share the same adapter-lifetime allowance; actual rereads consume it again, while moves/XML scans of already-owned buffers do not.

Required-part integrity/I/O errors, exhausted resource budgets and cancellation are fatal. They cannot silently become an unavailable external cache. Existing unsupported/malformed semantic link cases retain their best-effort refusal behavior. Fatal safety state prevents later data/bounds reads and retries from restarting with a fresh allowance. Cancellation is cooperative between bounded input/decompression chunks and parser events; a single XML token is not a hard wall-clock cancellation boundary.

Strict source-preserving recalc keeps its **existing** full-package CRC/actual-expansion checks, refusal order and separate `XlsxRecalculateLimits`, including its unchanged 64 MiB input default. Ordinary `load_limits` do not change recalc options. Skipping opaque ordinary inflation removes CPU work, but those bytes were already streamed through bounded scratch memory: it does not imply an equal peak-memory saving.

## Rust source changes

- `NativeXlsxAdapter` is the reader implementation and is available from the workbook crate root and `backends`.
- `CalamineAdapter` and `backends::calamine::CalamineAdapter` are compatibility type aliases to `NativeXlsxAdapter`, not an optional old parser. `XlsxPathSource` and its old module path remain available.
- The adapter's `SpreadsheetReader::Error` / `EngineLoadStream::Error` is now `IoError`, not `calamine::Error`. Callers explicitly annotating those associated types or matching Calamine errors must migrate.
- `IoError::Calamine(calamine::Error)` and `From<calamine::Error>` are removed. Match `IoError::XlsxRead(XlsxReadError)` for native value-reader errors. `XlsxReadError` and `XlsxError` are re-exported by the workbook crate. Existing parser-error display strings retain their historical `Calamine error:` prefix for compatibility; this does not indicate a dependency or fallback.
- `open_bytes_cancellable(bytes, cancel)` remains available with native `IoError` results. Path, bytes and reader constructors continue through `SpreadsheetReader`; reader input starts at its current position.
- The old adapter-only `external_link_target` accessor and `set_shadow_relocation_comparator_for_test` hook are removed. Raw link target inspection belongs to the source package relationship XML; cached external references retain the existing admission/refusal and evaluation behavior. There is no replacement public optimizer-injection hook.
- `calamine` and `calamine_integration` Cargo feature names are native-reader compatibility aliases. They do not enable the Calamine crate. `xlsx-recalc` enables native XLSX loading and source-preserving recalculation. `mmap` remains the existing no-op behavioral alias; mapping requires explicit `DirectMmap` selection.

The old adapter exposed XLSX, not Calamine's other input formats. CSV/JSON and supported Umya rich reader/writer capabilities are unchanged. Umya dependencies remain intentional; removing Calamine does not mean all spreadsheet dependencies are removed.

Minimal shared-formula/error routines adapted from Calamine retain its MIT notice in `crates/formualizer-workbook/src/xlsx/loader/LICENSE.calamine`. Historical differential evidence was obtained against the original independent reader before retirement; native-versus-native alias tests are compatibility tests, not new differential evidence.
