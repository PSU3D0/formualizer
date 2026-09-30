import math

import pytest

import formualizer as fz


@pytest.mark.parametrize("span_evaluation", [False, True])
@pytest.mark.parametrize("formula", ["POWER(1E200,2)", "EXP(1000)"])
def test_builtin_numeric_overflow_serializes_as_catchable_error(
    span_evaluation, formula
):
    book = fz.Workbook(span_evaluation=span_evaluation)
    sheet = book.sheet("S")
    sheet.set_formula(1, 1, formula)
    error = book.evaluate_cell("S", 1, 1)
    assert error["type"] == "Error"
    assert error["kind"] == "Num"
    assert book.get_value("S", 1, 1)["kind"] == "Num"
    sheet.set_formula(1, 2, f"IFERROR({formula},77)")
    assert book.evaluate_cell("S", 1, 2) == 77.0


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_builtin_numeric_underflow_and_finite_values(span_evaluation):
    book = fz.Workbook(span_evaluation=span_evaluation)
    sheet = book.sheet("S")
    for row, (formula, expected) in enumerate(
        [
            ("EXP(-1000)", 0.0),
            ("POWER(1E-200,2)", 0.0),
            ("POWER(2,10)", 1024.0),
            ("EXP(709)", math.exp(709)),
        ],
        start=1,
    ):
        sheet.set_formula(row, 1, formula)
        assert book.evaluate_cell("S", row, 1) == expected
