# XLSX ingestion boundaries

This crate-private tree separates immutable source parsing, evaluation input and source-preserving publication. `cache_recalculate` is the stable public facade: its public declarations retain their established API paths while implementation entrypoints are re-exported from this tree.

## Layers

- `options`, `error`: shared read bounds/cancellation and the reserved error boundary. Compatibility error rendering and refusal phase order remain unchanged.
- `xml`: bounded namespace-aware borrowed events and lexical helpers. Decoding does not mutate source bytes; lexical shortcuts do not establish semantic acceptance.
- `package`: member bounds, directory ownership, relationships and surgical ZIP publication. Provisional metadata discovery never replaces the authoritative ownership audit.
- `metadata`: strings, styles, name syntax, table geometry and dynamic-array declarations. Name import policy is supplied separately by ingestion.
- `worksheet`: one flat scanner, its path cursor, borrowed literal events, retained marks and shared-family checks. Marks refer to original bytes; optional indexes describe source geometry rather than evaluated spill output.
- `ingest`: value ingestion, formula staging/replay/evidence, calculation names and shared engine installation. Source-order identity survives grouping and spooling. The ordinary adapter and strict recalculator use these modules with their own admission policies.
- `recalculate`: strict admission, prior-footprint ownership, guards, evaluation-only transforms, projection, geometry, publication, timing and receipt accounting. The orchestration chooses strict policy; generic engine installation does not.

The ordinary Calamine adapter imports `ingest` and compatible metadata helpers. The strict path additionally consumes `package` and `worksheet`, validates source authority, then calls the shared ingestion machinery. Publication consumes evaluated results and source marks; it never serializes transformed evaluation formulas back into the workbook.

## Governing invariants

1. Original package bytes and formula marks remain authoritative. Evaluation text may differ without changing source formula bytes.
2. Deferred reader-shaped failures retain their ordering against earlier source, table and ownership refusals. Cancellation and size checks stay at their owning phases.
3. Only proven prior spill children may have old generated payloads ignored or removed. Geometry is bounded before output cells are generated.
4. Publication applies non-overlapping source-span patches and preserves unrelated XML/ZIP bytes. A failed or cancelled operation publishes nothing.
5. Temporary engine load configuration is restored on success and error. Exact replay and certification evidence never replace source identity.

Retained differential implementations and their intentional duplicate types are documented in `recalculate/tests/oracles/README.md`. They are test-only and are not production fallback readers.
