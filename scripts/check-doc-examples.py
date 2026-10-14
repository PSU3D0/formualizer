#!/usr/bin/env python3
"""Check opt-in documentation fences against executable source files (stdlib only)."""
from pathlib import Path
import re
import sys
import textwrap

ROOT = Path(__file__).resolve().parents[1]


def source(reference):
    path, _, region = reference.partition("#")
    text = (ROOT / path).read_text()
    if region:
        lines = text.splitlines(keepends=True)
        start = next(i for i, line in enumerate(lines) if re.search(r"\bregion\s+" + re.escape(region) + r"\s*$", line))
        end = next(i for i in range(start + 1, len(lines)) if re.search(r"\bendregion\b", lines[i]))
        # Regions inside a function body are shown dedented.
        text = textwrap.dedent("".join(lines[start + 1:end]))
    return text.rstrip("\n")


def main():
    paths = [ROOT / "README.md"]
    for pattern in ("bindings/*/README.md", "crates/*/README.md", "docs-site/content/**/*.mdx"):
        paths.extend(ROOT.glob(pattern))
    errors = []
    count = 0
    # Markdown uses `<!-- example: ref -->`; MDX, which rejects HTML comments, uses `{/* example: ref */}`.
    marker = r"(?:<!--\s*example:\s*(\S+)\s*-->|\{/\*\s*example:\s*(\S+)\s*\*/\})"
    pattern = re.compile(marker + r"[ \t]*\n[ \t]*\n?```[^\n]*\n(.*?)\n```", re.S)
    for path in (p for p in paths if p.is_file()):
        text = path.read_text()
        markers = len(re.findall(marker, text))
        matches = list(pattern.finditer(text))
        if markers != len(matches):
            errors.append(f"{path.relative_to(ROOT)}: example marker not followed by a code fence")
        for match in matches:
            ref, body = match[1] or match[2], match[3]
            count += 1
            try:
                if body != source(ref):
                    errors.append(f"{path.relative_to(ROOT)}: example {ref} differs")
            except (OSError, StopIteration) as exc:
                errors.append(f"{path.relative_to(ROOT)}: {ref}: {exc}")
    print(f"Checked {count} documentation examples")
    for error in errors:
        print(error, file=sys.stderr)
    return bool(errors)


if __name__ == "__main__":
    sys.exit(main())
