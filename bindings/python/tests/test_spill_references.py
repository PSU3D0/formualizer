"""FORM214: spill-range references through the Python bindings.

`A1#` and `_xlfn.ANCHORARRAY(A1)` evaluate to the anchor's current spill
range. An anchor with no current spill (a value, an empty cell, a scalar or
fresh 1x1 result, a blocked spill) is `#REF!` by policy.
"""

import pytest

import formualizer as fz


def _error(kind):
    return {"type": "Error", "kind": kind}


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_both_spellings_sum_the_spill_range(span_evaluation):
    book = fz.Workbook(span_evaluation=span_evaluation)
    sheet = book.sheet("S")
    sheet.set_formula(1, 1, "=SEQUENCE(2)")
    sheet.set_formula(1, 2, "=SUM(A1#)")
    sheet.set_formula(1, 3, "=SUM(_xlfn.ANCHORARRAY(A1))")
    sheet.set_formula(1, 4, "=ROWS(A1#)")
    book.evaluate_all()
    assert book.get_value("S", 1, 2) == 3.0
    assert book.get_value("S", 1, 3) == 3.0
    assert book.get_value("S", 1, 4) == 2.0

    # Growing the anchor (same top-left value) updates both readers.
    sheet.set_formula(1, 1, "=SEQUENCE(4)")
    book.evaluate_all()
    assert book.get_value("S", 1, 2) == 10.0
    assert book.get_value("S", 1, 3) == 10.0
    assert book.get_value("S", 1, 4) == 4.0


def test_no_current_spill_is_ref_error():
    book = fz.Workbook()
    sheet = book.sheet("S")
    sheet.set_value(1, 1, 5)
    sheet.set_formula(1, 3, "=SEQUENCE(1)")
    sheet.set_formula(2, 1, "=SUM(A1#)")
    sheet.set_formula(2, 2, "=SUM(_xlfn.ANCHORARRAY(A1))")
    sheet.set_formula(2, 3, "=SUM(C1#)")
    book.evaluate_all()
    assert book.get_value("S", 2, 1) == _error("Ref")
    assert book.get_value("S", 2, 2) == _error("Ref")
    # A fresh 1x1 result is committed as a scalar (documented deferral).
    assert book.get_value("S", 2, 3) == _error("Ref")
