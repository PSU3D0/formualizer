#!/usr/bin/env python3
"""Validate wasm artifact imports for Formualizer runtime profiles.

Usage:
    python3 scripts/check-wasm-artifact-imports.py portable <path/to/module.wasm>

Currently used for the portable profile to ensure the final emitted module does
not import wasm-bindgen/browser shims such as `__wbindgen_placeholder__` or
`__wbg_*` functions.
"""

from __future__ import annotations

import sys
from pathlib import Path

FORBIDDEN_MODULES_PORTABLE = {"__wbindgen_placeholder__"}
FORBIDDEN_FIELD_PREFIXES_PORTABLE = ("__wbg_", "__wbindgen_")
FORBIDDEN_FIELD_SUBSTRINGS_PORTABLE = (
    "getRandomValues",
    "randomFillSync",
    "performance",
)


def read_u32_leb(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        b = data[offset]
        offset += 1
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return value, offset
        shift += 7


def read_name(data: bytes, offset: int) -> tuple[str, int]:
    length, offset = read_u32_leb(data, offset)
    raw = data[offset : offset + length]
    offset += length
    return raw.decode("utf-8"), offset


def skip_limits(data: bytes, offset: int) -> int:
    flags, offset = read_u32_leb(data, offset)
    _, offset = read_u32_leb(data, offset)  # min
    if flags & 0x01:
        _, offset = read_u32_leb(data, offset)  # max
    return offset


def skip_import_desc(data: bytes, offset: int, kind: int) -> int:
    if kind == 0:  # func
        _, offset = read_u32_leb(data, offset)
        return offset
    if kind == 1:  # table
        offset += 1  # reftype
        return skip_limits(data, offset)
    if kind == 2:  # memory
        return skip_limits(data, offset)
    if kind == 3:  # global
        offset += 2  # valtype + mutability
        return offset
    if kind == 4:  # tag
        _, offset = read_u32_leb(data, offset)  # attribute
        _, offset = read_u32_leb(data, offset)  # type index
        return offset
    raise ValueError(f"unknown import kind: {kind}")


def iter_imports(wasm: bytes) -> list[tuple[str, str, int]]:
    if wasm[:4] != b"\0asm":
        raise ValueError("not a wasm binary (bad magic)")
    if wasm[4:8] != b"\x01\x00\x00\x00":
        raise ValueError("unsupported wasm version")

    imports: list[tuple[str, str, int]] = []
    offset = 8
    while offset < len(wasm):
        section_id = wasm[offset]
        offset += 1
        section_size, offset = read_u32_leb(wasm, offset)
        section_end = offset + section_size

        if section_id == 2:  # import section
            count, offset = read_u32_leb(wasm, offset)
            for _ in range(count):
                module, offset = read_name(wasm, offset)
                field, offset = read_name(wasm, offset)
                kind = wasm[offset]
                offset += 1
                offset = skip_import_desc(wasm, offset, kind)
                imports.append((module, field, kind))
            break

        offset = section_end

    return imports


def check_portable(imports: list[tuple[str, str, int]]) -> int:
    violations: list[str] = []
    for module, field, _kind in imports:
        if module in FORBIDDEN_MODULES_PORTABLE:
            violations.append(f"forbidden import module: {module}::{field}")
            continue
        if field.startswith(FORBIDDEN_FIELD_PREFIXES_PORTABLE):
            violations.append(f"forbidden wasm-bindgen import: {module}::{field}")
            continue
        if any(token in field for token in FORBIDDEN_FIELD_SUBSTRINGS_PORTABLE):
            violations.append(f"forbidden browser-ish import: {module}::{field}")

    if violations:
        print("[portable-wasm] forbidden imports found:", file=sys.stderr)
        for item in violations:
            print(f"  - {item}", file=sys.stderr)
        return 1

    print("[portable-wasm] artifact import surface is clean.")
    return 0


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Usage: check-wasm-artifact-imports.py <portable> <path/to/module.wasm>",
            file=sys.stderr,
        )
        return 2

    mode = sys.argv[1]
    path = Path(sys.argv[2])
    if mode != "portable":
        print(f"unsupported mode: {mode}", file=sys.stderr)
        return 2
    if not path.exists():
        print(f"wasm artifact not found: {path}", file=sys.stderr)
        return 2

    imports = iter_imports(path.read_bytes())
    print(f"[portable-wasm] inspected {len(imports)} imports from {path}")
    return check_portable(imports)


if __name__ == "__main__":
    raise SystemExit(main())
