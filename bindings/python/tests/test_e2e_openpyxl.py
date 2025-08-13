import tempfile
from pathlib import Path

import pytest

try:
    import openpyxl  # type: ignore
except Exception:  # pragma: no cover - allow skipping if not present in dev env
    openpyxl = None

pytestmark = pytest.mark.skipif(openpyxl is None, reason="openpyxl not installed")


# The extension module name configured by maturin
import formualizer as fz


def make_wb(tmp: Path) -> Path:
    p = tmp / "e2e.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    # Values
    ws["A1"] = 1
    ws["A2"] = 2
    ws["A3"] = 3
    # Simple formula
    ws["B1"] = "=SUM(A1:A3)"
    # Conditionals
    ws["C1"] = "=IF(B1>3, B1*2, 0)"
    # SUMIFS-like (if supported) else basic SUM as placeholder
    ws["D1"] = "=SUM(A1:A3)"
    wb.save(p)
    return p


def test_openpyxl_roundtrip(tmp_path: Path):
    # Prepare XLSX via openpyxl
    xlsx_path = make_wb(tmp_path)

    # Construct engine-backed workbook and load XLSX using Calamine adapter
    cfg = fz.EvaluationConfig()
    wb = fz.Workbook(cfg)

    # Load using Rust IO calamine backend
    # NOTE: Python Workbook currently doesn’t expose load_path; use Rust loader through an FFI helper later
    # For now, emulate: create sheet, set values, set formulas, then evaluate
    s = wb.sheet("Sheet1")
    s.set_value(1, 1, fz.LiteralValue.int(1))
    s.set_value(2, 1, fz.LiteralValue.int(2))
    s.set_value(3, 1, fz.LiteralValue.int(3))
    s.set_formula(1, 2, "=SUM(A1:A3)")
    s.set_formula(1, 3, "=IF(B1>3, B1*2, 0)")
    s.set_formula(1, 4, "=SUM(A1:A3)")

    # Recompute
    eng = fz.Engine(cfg)
    # Share workbook engine if we expose later; evaluate via Workbook’s engine not separate Engine
    # For now use Sheet API only and then read results

    # Assert values
    assert s.get_cell(1, 2).value.as_int() == 6
    assert s.get_cell(1, 3).value.as_int() == 12
    assert s.get_cell(1, 4).value.as_int() == 6

    # Mutate cells and recompute
    s.set_value(3, 1, fz.LiteralValue.int(30))
    # Re-read expected changes (engine propagates)
    assert s.get_cell(1, 2).value.as_int() == 33
    assert s.get_cell(1, 3).value.as_int() == 66
    assert s.get_cell(1, 4).value.as_int() == 33


def test_batch_values_and_formulas(tmp_path: Path):
    wb = fz.Workbook()
    s = wb.sheet("Data")

    s.set_values_batch(1, 1, 2, 3, [
        [fz.LiteralValue.int(1), fz.LiteralValue.int(2), fz.LiteralValue.int(3)],
        [fz.LiteralValue.int(4), fz.LiteralValue.int(5), fz.LiteralValue.int(6)],
    ])

    s.set_formulas_batch(1, 4, 2, 1, [
        ["=SUM(A1:C1)"],
        ["=SUM(A2:C2)"],
    ])

    vals = s.get_values(fz.RangeAddress("Data", 1, 1, 2, 4))
    sums = [row[3].as_int() for row in vals]
    assert sums == [6, 15]

    forms = s.get_formulas(fz.RangeAddress("Data", 1, 4, 2, 4))
    assert forms == [["SUM(A1:C1)"], ["SUM(A2:C2)"]]
