# Changelog

This changelog tracks spec and reference implementation changes.

## 0.3.1

- Reject layout `header_row` values outside Excel's 1–1,048,576 row bounds.
- Keep the manifest protocol and canonical schema at fio 0.3.0; this is a validator/packaging patch release.

## 0.3.0

- Add `capabilities.profile` for conformance gating (`core-v0` default, `full-v0` reserved).
- Define selector/shape legality rules in the spec validator.
- Add `layout.kind` (default `header_contiguous_v1`).
- Tighten constraint typing validation and document enum exact-equality semantics.
