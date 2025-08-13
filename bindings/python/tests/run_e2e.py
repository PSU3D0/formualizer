#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pytest>=8",
#   "openpyxl>=3.1",
# ]
# ///

import sys
from pathlib import Path

import pytest  # type: ignore


def main() -> int:
    tests_dir = Path(__file__).parent
    args = [str(tests_dir)] + (sys.argv[1:] or ["-q"])
    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
