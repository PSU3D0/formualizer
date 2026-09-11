import math

import pytest

import formualizer as fz


@pytest.mark.parametrize("span_evaluation", [False, True])
@pytest.mark.parametrize(
    "formula,expected",
    [
        ("BESSELJ(0.06,100)", 5.522273948726346e-311),
        ("BESSELY(0.06,100)", -5.76410997427483e307),
        ("BESSELJ(1e-12,13)", 1.9603324996120135e-170),
        ("IFERROR(BESSELJ(3e9,2e9),42)", 42.0),
        ("IFERROR(BESSELY(3e9,2e9),42)", 42.0),
    ],
)
def test_bessel_finite_values_and_explicit_work_failure(
    span_evaluation, formula, expected
):
    book = fz.Workbook(span_evaluation=span_evaluation)
    book.sheet("S").set_formula(1, 1, formula)
    result = book.evaluate_cell("S", 1, 1)
    assert math.isfinite(result)
    assert math.isclose(result, expected, rel_tol=2e-12, abs_tol=0.0)
