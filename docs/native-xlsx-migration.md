# Native XLSX reader migration (0.13)

XLSX loading now uses the native Compatible reader in Rust, Python, JavaScript/WASM and Pyodide. Source-preserving recalculation uses the same package admission and engine ingress, with its separate strict worksheet scanner. This is not a new evaluator or a universal worksheet state machine.

## Python and JavaScript

Omitting `backend` selects `native` on every host. Explicit `backend="calamine"` (JavaScript: `{ backend: 'calamine' }`) remains an alias to native, not an independent reader. Existing keyword names, JavaScript camelCase methods, load configuration, path-source choices and date semantics remain supported. `umya` remains available where previously supported for rich document reading and writing; native loading does not replace rich export.

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
