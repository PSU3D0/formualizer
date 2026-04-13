import pytest

import formualizer as fz


def make_workbook() -> fz.Workbook:
    wb = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
    wb.add_sheet("Sheet1")
    wb.set_value("Sheet1", 1, 1, 123)
    wb.set_formula("Sheet1", 1, 2, "=A1")
    return wb


def test_workbook_cell_apis_reject_zero_based_coords():
    wb = make_workbook()

    with pytest.raises(ValueError, match="1-based"):
        wb.set_value("Sheet1", 0, 1, 99)
    with pytest.raises(ValueError, match="1-based"):
        wb.set_formula("Sheet1", 1, 0, "=1")
    with pytest.raises(ValueError, match="1-based"):
        wb.evaluate_cell("Sheet1", 0, 1)
    with pytest.raises(ValueError, match="1-based"):
        wb.evaluate_cells([("Sheet1", 1, 0)])
    with pytest.raises(ValueError, match="1-based"):
        wb.get_value("Sheet1", 0, 1)
    with pytest.raises(ValueError, match="1-based"):
        wb.get_formula("Sheet1", 1, 0)
    with pytest.raises(ValueError, match="1-based"):
        wb.set_values_batch("Sheet1", 0, 1, [[1]])
    with pytest.raises(ValueError, match="1-based"):
        wb.set_formulas_batch("Sheet1", 1, 0, [["=1"]])
