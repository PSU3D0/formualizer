# Umya 3 XLSX import

Enable `formualizer-workbook`'s `umya3` feature to use `Umya3Adapter`. The existing `umya` feature and `UmyaAdapter` remain on Umya 2; both features can coexist and share one adapter implementation.

For applications that own an Umya 3 document directly, use `formualizer_workbook::backends::umya3::{read_document, read_document_path, read_document_reader}`. These constructors return the ordinary authoritative `umya_spreadsheet::Workbook`. Use the paired `write_document`, `write_document_path`, or `write_document_writer` functions to export. `Umya3Adapter` uses these same constructors and writers internally. Calling upstream `umya_spreadsheet::{reader,writer}::xlsx` directly bypasses the workaround.

## Border-colour workaround and limitation

Published Umya 3.1.0 parses a border colour into a discarded clone. The compatibility reader restores original colour metadata at cold import, using XLSX relationships, cell-format border IDs, row/column styles and differential-format IDs. Other style fields remain Umya's values. No source XML, repair journal or parallel document is retained, and this adds no XLSX serialization to edit/evaluation loops.

Umya's retained stylesheet also uses colour hashes that collide across selectors (for example, theme 1 black and indexed 1 white), while border hashes omit theme identity. The paired cold writer corrects emitted font/fill/border colour definitions and their cell, row, column and conditional-format references from the authoritative document. It does not reconstruct unrelated style fields. **Theme, indexed and RGB selectors and tint remain intact; theme colours are not flattened.** The writer performs one Umya serialization followed by a bounded ZIP/XML projection correction, not a workbook reimport. All correction tables are temporary. Unspecified row height is projected as 15 points, consistent with the renderer, rather than Umya's conflicting export fallback.

The exporter preserves the selector present in the authoritative `Color`, not a caller's earlier spelling or intent. Some upstream programmatic setters normalize palette-equivalent RGB to indexed colours before export; the compatibility importer explicitly retains the RGB selector from source XML, including palette-equivalent RGB.

This is a targeted compatibility measure, not a claim of lossless preservation of every OOXML feature; it does not change Umya's existing handling of unsupported automatic-colour or extension metadata.

## Bounds and failures

The compatibility reader limits input and individual expanded package parts to 256 MiB, aggregate declared expanded size to 1 GiB, archive entries to 65,536, style-table entries to 100,000 per table, and XML nesting to 128. Duplicate parts, unsafe internal relationship targets, DTDs, invalid referenced style IDs and invalid colour metadata fail import. Path/reader constructors buffer one bounded snapshot so the document and repair observe identical bytes. No partially repaired workbook escapes an error.

Rendering APIs that accept an already constructed Umya workbook cannot recover information discarded before the call. Use these constructors at the document boundary; do not substitute regenerated rendering goldens for import fidelity tests.
