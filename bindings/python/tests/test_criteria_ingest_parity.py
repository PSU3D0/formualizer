import pytest

import formualizer as fz


@pytest.mark.parametrize(
    ("formula", "expected", "after_clear"),
    [
        ('COUNTIF(A1:A6,"")', 1, 2),
        ('COUNTIF(A1:A6,"<>")', 5, 4),
        ('COUNTIF(A1:A20,"")', 15, 16),
        ('COUNTBLANK(A1:A20)', 15, 16),
        # Established scalar compatibility policy, not an Excel oracle claim.
        ('COUNTIF(A1:A6,"*")', 6, 6),
        ('COUNTIF(A1:A6,"1*")', 2, 1),
    ],
)
def test_loaded_constructed_criteria_parity(
    xlsx_builder, formula, expected, after_clear
):
    inputs = [1.0, 2.0, 3.0, "1", True]

    def populate(book):
        sheet = book.active
        for row, value in enumerate(inputs, 1):
            sheet.cell(row, 1, value)
        # Physically retain row six, independently of the criteria column.
        sheet.cell(6, 3, 10)
        sheet.cell(1, 2, "=" + formula)

    path = xlsx_builder(populate)
    loaded = fz.Workbook.from_bytes(path.read_bytes())
    built = fz.Workbook()
    built.add_sheet("Sheet1")
    for row, value in enumerate(inputs, 1):
        built.set_value("Sheet1", row, 1, value)
    built.set_value("Sheet1", 6, 3, 10)
    built.set_formula("Sheet1", 1, 2, formula)

    for workbook in (loaded, built):
        for _ in range(2):
            workbook.evaluate_all()
            assert workbook.get_value("Sheet1", 1, 2) == expected
        workbook.set_value("Sheet1", 1, 1, None)
        workbook.evaluate_all()
        assert workbook.get_value("Sheet1", 1, 2) == after_clear
