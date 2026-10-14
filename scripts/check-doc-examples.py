#!/usr/bin/env python3
"""Check opt-in documentation fences against executable source files (stdlib only)."""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]


def source(reference):
    path, _, region = reference.partition("#")
    text = (ROOT / path).read_text()
    if region:
        lines = text.splitlines(keepends=True)
        start = next(i for i, line in enumerate(lines) if re.search(r"\bregion\s+" + re.escape(region) + r"\s*$", line))
        end = next(i for i in range(start + 1, len(lines)) if re.search(r"\bendregion\b", lines[i]))
        text = "".join(lines[start + 1:end])
    return text.rstrip("\n")


def main():
    paths = [ROOT / "README.md"]
    for pattern in ("bindings/*/README.md", "crates/*/README.md", "docs-site/content/**/*.mdx"):
        paths.extend(ROOT.glob(pattern))
    errors = []
    count = 0
    pattern = re.compile(r"<!--\s*example:\s*(\S+)\s*-->\s*```[^\n]*\n(.*?)\n```", re.S)
    for path in paths:
        for match in pattern.finditer(path.read_text()):
            count += 1
            try:
                expected = source(match[1])
                if match[2] != expected:
                    errors.append(f"{path.relative_to(ROOT)}: example {match[1]} differs")
            except (OSError, StopIteration) as exc:
                errors.append(f"{path.relative_to(ROOT)}: {match[1]}: {exc}")
    print(f"Checked {count} documentation examples")
    for error in errors:
        print(error, file=sys.stderr)
    return bool(errors)


if __name__ == "__main__":
    sys.exit(main())
