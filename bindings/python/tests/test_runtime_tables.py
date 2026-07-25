"""Runtime table definition (issue #212).

The reporter could only create a table by serialising a workbook to JSON and
reloading it. These build the table in place instead.
"""

import pytest

import formualizer as fz


def workbook_with_data() -> "fz.Workbook":
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_value("S", 1, 1, "Nama")
    wb.set_value("S", 1, 2, "Nilai")
    for row, (name, score) in enumerate(
        [("Ani", 10), ("Budi", 20), ("Cici", 30)], start=2
    ):
        wb.set_value("S", row, 1, name)
        wb.set_value("S", row, 2, score)
    return wb


def test_structured_references_resolve_after_add_table():
    wb = workbook_with_data()
    wb.add_table("Table1", "S", (1, 1, 4, 2), ["Nama", "Nilai"])

    formulas = [
        ("=SUM(Table1[Nilai])", 60),
        ("=COUNTA(Table1[#Headers])", 2),
        ("=COUNTA(Table1[#All])", 8),
        ('=SUMIF(Table1[Nama],"Budi",Table1[Nilai])', 20),
    ]
    for row, (formula, _expected) in enumerate(formulas, start=10):
        wb.set_formula("S", row, 4, formula)
    wb.evaluate_all()
    for row, (formula, expected) in enumerate(formulas, start=10):
        assert wb.get_value("S", row, 4) == expected, formula


def test_edits_inside_the_table_propagate():
    wb = workbook_with_data()
    wb.add_table("Table1", "S", (1, 1, 4, 2), ["Nama", "Nilai"])
    wb.set_formula("S", 10, 4, "=SUM(Table1[Nilai])")
    wb.evaluate_all()
    assert wb.get_value("S", 10, 4) == 60

    wb.set_value("S", 3, 2, 200)
    wb.evaluate_all()
    assert wb.get_value("S", 10, 4) == 240


def test_tables_lists_metadata():
    wb = workbook_with_data()
    wb.add_table("Table1", "S", (1, 1, 4, 2), ["Nama", "Nilai"], totals_row=False)
    tables = wb.tables()
    assert len(tables) == 1
    assert tables[0] == {
        "name": "Table1",
        "sheet": "S",
        "range": (1, 1, 4, 2),
        "headers": ["Nama", "Nilai"],
        "header_row": True,
        "totals_row": False,
    }


@pytest.mark.parametrize(
    "kwargs,fragment",
    [
        (
            {
                "name": "T",
                "sheet": "S",
                "cell_range": (1, 1, 4, 2),
                "headers": ["only-one"],
            },
            "2 column",
        ),
        (
            {
                "name": "T",
                "sheet": "Nope",
                "cell_range": (1, 1, 4, 2),
                "headers": ["a", "b"],
            },
            "Nope",
        ),
        (
            {
                "name": "T",
                "sheet": "S",
                "cell_range": (0, 1, 4, 2),
                "headers": ["a", "b"],
            },
            "1-based",
        ),
        (
            {
                "name": "T",
                "sheet": "S",
                "cell_range": (4, 1, 1, 2),
                "headers": ["a", "b"],
            },
            "inverted",
        ),
        (
            {
                "name": "T",
                "sheet": "S",
                "cell_range": (1, 1, 1, 2),
                "headers": ["a", "b"],
            },
            "no data rows",
        ),
    ],
)
def test_invalid_definitions_raise_actionable_errors(kwargs, fragment):
    wb = workbook_with_data()
    with pytest.raises(ValueError) as excinfo:
        wb.add_table(**kwargs)
    assert fragment in str(excinfo.value)
    assert wb.tables() == []
