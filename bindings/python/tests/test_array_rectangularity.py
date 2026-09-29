import pytest

import formualizer as fz


@pytest.mark.parametrize(
    "formula", ["={1,2;3}", "={1;2,3}", "=SUM({1,2;3})", "={{1,2;3}}"]
)
def test_ragged_array_parse_error(formula):
    with pytest.raises(fz.ParserError, match="Array rows must have equal length"):
        fz.parse(formula)
    with pytest.raises(fz.ParserError, match="Array rows must have equal length"):
        fz.Parser().parse_string(formula)


@pytest.mark.parametrize("through_sheet", [False, True])
@pytest.mark.parametrize("logged", [False, True])
def test_ragged_array_assignment_is_atomic(through_sheet, logged):
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_changelog_enabled(logged)
    wb.set_formula("S", 1, 1, "={1,2;3,4}")
    wb.evaluate_all()
    source = wb.get_formula("S", 1, 1)
    for formula in ["={1,2;3}", "={1;2,3}", "={1;;2}"]:
        with pytest.raises(RuntimeError):
            if through_sheet:
                wb.sheet("S").set_formula(1, 1, formula)
            else:
                wb.set_formula("S", 1, 1, formula)
        assert wb.get_formula("S", 1, 1) == source
        assert wb.get_value("S", 2, 2) == 4
    wb.set_formula("S", 1, 1, "={5,6;7,8}")
    wb.evaluate_all()
    assert wb.get_value("S", 2, 2) == 8
    if logged:
        wb.undo()
        assert wb.get_formula("S", 1, 1) == source


def test_deferred_new_array_is_a_controlled_error():
    wb = fz.Workbook()
    wb.add_sheet("S")
    # New deferred cells keep the existing text-staging contract.
    wb.set_formula("S", 1, 1, "={1,2;3}")
    value = wb.evaluate_cell("S", 1, 1)
    assert value["type"] == "Error"
    assert value["kind"] == "Error"


def test_xlsx_ragged_array_uses_existing_parse_error_policy(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.Workbook()
    book.active["A1"] = "={1,2;3}"
    path = tmp_path / "ragged.xlsx"
    book.save(path)
    wb = fz.Workbook.load_path(str(path), strategy="eager_all")
    value = wb.evaluate_cell("Sheet", 1, 1)
    assert value["type"] == "Error"
    assert value["kind"] == "Error"
    wb.set_formula("Sheet", 1, 1, "={1,2;3,4}")
    wb.evaluate_all()
    assert wb.get_value("Sheet", 2, 2) == 4
