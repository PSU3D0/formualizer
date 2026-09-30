import pytest

import formualizer as fz


def _error(kind):
    return {"type": "Error", "kind": kind}


def _column(book, col, rows):
    return [book.get_value("S", row, col) for row in rows]


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_iferror_and_ifna_replace_array_elements(span_evaluation):
    book = fz.Workbook(span_evaluation=span_evaluation)
    sheet = book.sheet("S")
    for row, (a, b) in enumerate([(1, 10), (0, 20), (2, 30)], start=1):
        sheet.set_value(row, 1, a)
        sheet.set_value(row, 2, b)
    sheet.set_value(1, 3, 1)
    sheet.set_formula(2, 3, "=NA()")
    sheet.set_formula(3, 3, "=1/0")

    sheet.set_formula(1, 5, "=IFERROR(1/A1:A3,-1)")
    sheet.set_formula(1, 6, "=IFERROR(1/A1:A3,B1:B3)")
    sheet.set_formula(1, 7, "=IFERROR(C1:C3,0)")
    sheet.set_formula(1, 8, "=IFNA(C1:C3,0)")
    sheet.set_formula(1, 9, "=SUM(IFERROR(1/A1:A3,0))")
    sheet.set_formula(5, 1, "=IFERROR(1/{0,4},8)")
    sheet.set_formula(6, 1, "=IFERROR({1,2},SEQUENCE(1000000000))")
    book.evaluate_all()

    assert _column(book, 5, range(1, 4)) == [1.0, -1.0, 0.5]
    assert _column(book, 6, range(1, 4)) == [1.0, 20.0, 0.5]
    assert _column(book, 7, range(1, 4)) == [1.0, 0.0, 0.0]
    assert _column(book, 8, range(1, 4)) == [1.0, 0.0, _error("Div")]
    assert book.get_value("S", 1, 9) == 1.5
    assert [book.get_value("S", 5, c) for c in (1, 2)] == [8.0, 0.25]
    # A clean array passes through without evaluating the oversized fallback.
    assert [book.get_value("S", 6, c) for c in (1, 2)] == [1.0, 2.0]

    # Edits re-run the guards.
    sheet.set_value(2, 1, 4)
    sheet.set_formula(2, 3, "=5")
    book.evaluate_all()
    assert _column(book, 5, range(1, 4)) == [1.0, 0.25, 0.5]
    assert _column(book, 8, range(1, 4)) == [1.0, 5.0, _error("Div")]


def test_iferror_array_shape_rules():
    book = fz.Workbook()
    sheet = book.sheet("S")
    sheet.set_formula(1, 1, "=IFERROR({#N/A,2},{1;2})")
    sheet.set_formula(4, 1, "=IFERROR({#N/A,#N/A,#N/A},{8,9})")
    sheet.set_formula(5, 1, "=IFERROR(1/(SEQUENCE(4097)-1),SEQUENCE(1,4097))")
    book.evaluate_all()
    assert [book.get_value("S", 1, c) for c in (1, 2)] == [1.0, 2.0]
    assert [book.get_value("S", 2, c) for c in (1, 2)] == [2.0, 2.0]
    assert book.get_value("S", 4, 1) == _error("Value")
    assert book.get_value("S", 5, 1) == _error("Num")
