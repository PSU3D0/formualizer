import formualizer as fz


def test_edit_propagation_chain_calamine(xlsx_builder):
    def populate(wb):
        ws = wb["Sheet1"]
        ws["A1"] = 10
        ws["B1"] = "=A1"
        ws["C1"] = "=B1"

    path = xlsx_builder(populate)

    wb = fz.Workbook.from_path(str(path), backend="calamine")

    # Initial evaluation
    wb.evaluate_all()
    assert wb.evaluate_cell("Sheet1", 1, 2) == 10.0
    assert wb.evaluate_cell("Sheet1", 1, 3) == 10.0

    # Edit A1 to a formula and re-evaluate
    wb.set_formula("Sheet1", 1, 1, "=20")
    wb.evaluate_all()
    assert wb.evaluate_cell("Sheet1", 1, 2) == 20.0
    assert wb.evaluate_cell("Sheet1", 1, 3) == 20.0

    # Edit B1; only C1 (and B1) should change accordingly
    wb.set_formula("Sheet1", 1, 2, "=A1*3")
    wb.evaluate_all()
    assert wb.evaluate_cell("Sheet1", 1, 2) == 60.0
    assert wb.evaluate_cell("Sheet1", 1, 3) == 60.0


def test_demand_driven_without_evaluate_all(xlsx_builder):
    """After edits, evaluate_cell should recompute on-demand without evaluate_all."""

    def populate(wb):
        ws = wb["Sheet1"]
        ws["A1"] = 10
        ws["B1"] = "=A1"
        ws["C1"] = "=B1"

    path = xlsx_builder(populate)
    wb = fz.Workbook.from_path(str(path), backend="calamine")

    # Prime with a single cell eval to avoid full evaluate_all
    assert wb.evaluate_cell("Sheet1", 1, 3) == 10.0

    # Edit A1; do NOT call evaluate_all; directly evaluate C1 (demand-driven)
    wb.set_formula("Sheet1", 1, 1, "=20")
    assert wb.evaluate_cell("Sheet1", 1, 3) == 20.0

    # Edit B1; again, do NOT call evaluate_all; C1 demand should recompute through B1
    wb.set_formula("Sheet1", 1, 2, "=A1*3")
    assert wb.evaluate_cell("Sheet1", 1, 3) == 60.0
