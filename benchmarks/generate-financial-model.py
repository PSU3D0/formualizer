# /// script
# requires-python = ">=3.11"
# dependencies = ["openpyxl==3.1.5"]
# ///
"""Preserved seed-7 headline workload: ten formulas per generated financial-model row.

Cell contents are reproducible; ZIP timestamps/producer metadata are not promised
byte-identical to the archived measured XLSX. No third-party workbook is needed.
"""
import argparse
import json
from pathlib import Path
import random

from openpyxl import Workbook


def generate(rows: int, output: Path) -> None:
    rng = random.Random(7)
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet("Model")
    sheet.append(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", 1.07])
    for row in range(2, rows + 2):
        a, b = rng.randint(1, 100), rng.randint(1, 100)
        rolling = f"=SUM(C{row-11}:C{row})" if row > 12 else f"=SUM($C$2:C{row})"
        sheet.append([
            a, b,
            f"=A{row}*B{row}",
            f"=C{row}+D{row-1}" if row > 2 else f"=C{row}",
            f"=IF(A{row}>50,C{row}*1.1,C{row}*0.9)",
            f"=ROUND(E{row}/12,2)", rolling,
            f"=MAX(E{row},F{row})", f"=D{row}-G{row}",
            f"=IFERROR(C{row}/B{row},0)", f"=H{row}*$M$1",
            f"=AND(A{row}>10,B{row}<90)",
        ])
    output.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.rows <= 1_048_575:
        parser.error("--rows must leave room for a header within Excel's row limit")
    generate(args.rows, args.output)
    print(json.dumps({"output": str(args.output), "seed": 7, "rows": args.rows, "formulas": args.rows * 10}))


if __name__ == "__main__":
    main()
