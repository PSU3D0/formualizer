import pytest

import formualizer as fz


@pytest.mark.parametrize("logged", [False, True])
@pytest.mark.parametrize("through_sheet", [False, True])
@pytest.mark.parametrize("formula", ["=NOSHEET!A1", "=SUM(MissingTable[Amount])"])
def test_rejected_assignment_keeps_previous_state(logged, through_sheet, formula):
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_changelog_enabled(logged)
    wb.set_value("S", 1, 1, "sentinel")
    with pytest.raises(RuntimeError, match="Sheet not found|Undefined table"):
        if through_sheet:
            wb.sheet("S").set_formula(1, 1, formula)
        else:
            wb.set_formula("S", 1, 1, formula)
    assert wb.get_formula("S", 1, 1) is None
    assert wb.get_value("S", 1, 1) == "sentinel"
    wb.set_formula("S", 1, 1, "=1+2")
    assert wb.evaluate_cell("S", 1, 1) == 3


def test_rejected_spill_formula_does_not_clear_members():
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_formula("S", 1, 1, "=SEQUENCE(2,1)")
    wb.evaluate_all()
    before = wb.get_formula("S", 1, 1)
    with pytest.raises(RuntimeError, match="Sheet not found"):
        wb.set_formula("S", 1, 1, "=NOSHEET!A1")
    assert wb.get_formula("S", 1, 1) == before
    assert wb.get_value("S", 2, 1) == 2
