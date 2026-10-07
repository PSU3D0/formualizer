"""A dynamic-array spill blocked by a formula or by another spill.

The anchor becomes `#SPILL!` like a value-blocked anchor, the blocking
formula evaluates normally and evaluation completes (it used to raise). Once the spill fits again the
anchor re-spills on the next recalc.
"""

import pytest

import formualizer as fz


def _error(kind):
    return {"type": "Error", "kind": kind}


def _book(blocker):
    book = fz.Workbook()
    sheet = book.sheet("S")
    sheet.set_value(1, 5, 3)
    sheet.set_formula(1, 1, "=SEQUENCE(E1)")
    sheet.set_formula(1, 2, "=A1+0")
    sheet.set_formula(1, 3, "=SUM(A1#)")
    row, kind, payload = blocker
    if kind == "formula":
        sheet.set_formula(row, 1, payload)
    else:
        sheet.set_value(row, 1, payload)
    return book, sheet


@pytest.mark.parametrize(
    "blocker, blocker_value",
    [
        ((3, "formula", "=1+1"), 2.0),
        ((2, "formula", "=SEQUENCE(2)"), 1.0),
        ((3, "value", 99), 99.0),
    ],
    ids=["formula", "spill", "value"],
)
def test_blocked_anchor_is_spill_error_and_evaluation_completes(blocker, blocker_value):
    book, sheet = _book(blocker)
    book.evaluate_all()
    assert book.get_value("S", 1, 1) == _error("Spill")
    assert book.get_value("S", blocker[0], 1) == blocker_value
    assert book.get_value("S", 1, 2) == _error("Spill")
    assert book.get_value("S", 1, 3) == _error("Ref")

    # Shrinking the anchor so the spill fits: it spills on the next recalc
    # and the blocker keeps its value.
    sheet.set_value(1, 5, blocker[0] - 1)
    book.evaluate_all()
    fits = [float(r) for r in range(1, blocker[0])]
    assert [book.get_value("S", r, 1) for r in range(1, blocker[0])] == fits
    assert book.get_value("S", blocker[0], 1) == blocker_value
    assert book.get_value("S", 1, 2) == 1.0
    # A fresh 1x1 result is a scalar, so its spill reference is #REF!.
    expected_sum = sum(fits) if len(fits) > 1 else _error("Ref")
    assert book.get_value("S", 1, 3) == expected_sum


def test_formula_entered_into_a_live_spill_blocks_it():
    book = fz.Workbook()
    sheet = book.sheet("S")
    sheet.set_formula(1, 1, "=SEQUENCE(3)")
    book.evaluate_all()
    sheet.set_formula(3, 1, "=1+1")
    book.evaluate_all()
    assert book.get_value("S", 1, 1) == _error("Spill")
    assert book.get_value("S", 2, 1) is None
    assert book.get_value("S", 3, 1) == 2.0
