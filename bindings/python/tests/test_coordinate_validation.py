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


def test_workbook_cell_apis_reject_out_of_grid_coords():
    """Coordinates beyond Excel's grid must raise, not abort the interpreter.

    The engine packs coordinates into 20-bit row / 14-bit column fields and
    asserts on overflow, so these previously aborted the process.
    """
    wb = make_workbook()

    too_big_row = 1_048_577
    too_big_col = 16_385

    with pytest.raises(ValueError, match="exceeds the maximum"):
        wb.set_value("Sheet1", too_big_row, 1, 99)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        wb.set_formula("Sheet1", 1, too_big_col, "=1")
    with pytest.raises(ValueError, match="exceeds the maximum"):
        wb.get_value("Sheet1", too_big_row, 1)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        wb.evaluate_cell("Sheet1", too_big_row, 1)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        wb.set_values_batch("Sheet1", too_big_row, 1, [[1]])


def test_workbook_accepts_grid_boundary_coords():
    """The last valid cell (XFD1048576) must still be accepted."""
    wb = make_workbook()

    wb.set_value("Sheet1", 1_048_576, 16_384, 7)
    assert wb.get_value("Sheet1", 1_048_576, 16_384) == 7
