from io import BytesIO
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

import formualizer as fz


@pytest.mark.parametrize(
    ("formula", "expected", "after_clear"),
    [
        ('COUNTIF(A1:A6,"")', 1, 2),
        ('COUNTIF(A1:A6,"<>")', 5, 4),
        ('COUNTIF(A1:A20,"")', 15, 16),
        ("COUNTBLANK(A1:A20)", 15, 16),
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


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_blank_count_clipping_keeps_spilled_values_and_clear(span_evaluation):
    book = fz.Workbook(span_evaluation=span_evaluation)
    book.add_sheet("Data")
    book.add_sheet("Results")
    book.set_formula("Data", 10, 3, "SEQUENCE(2,3)")
    formulas = [
        'COUNTIF(Data!A1:F20,"")',
        "COUNTBLANK(Data!A1:F20)",
        'COUNTIF(Data!C:C,"")',
        "COUNTBLANK(Data!10:10)",
    ]
    for row, formula in enumerate(formulas, 1):
        book.set_formula("Results", row, 1, formula)
    book.evaluate_all()
    assert book.get_value("Data", 11, 5) == 6
    assert [book.get_value("Results", row, 1) for row in range(1, 5)] == [
        114,
        114,
        1_048_574,
        16_381,
    ]
    book.set_value("Data", 10, 3, 9)
    book.evaluate_all()
    assert [book.get_value("Results", row, 1) for row in range(1, 5)] == [
        119,
        119,
        1_048_575,
        16_383,
    ]


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_blank_count_clipping_keeps_uncached_shared_results(
    xlsx_builder, span_evaluation
):
    def populate(book):
        data = book.active
        data.title = "Data"
        for row in range(10, 13):
            data.cell(row, 3, 7)
        results = book.create_sheet("Results")
        for row, formula in enumerate(
            [
                'COUNTIF(Data!A1:E20,"")',
                "COUNTBLANK(Data!A1:E20)",
                'COUNTIF(Data!C:C,"")',
                "COUNTBLANK(Data!10:10)",
                "COUNTIF(Data!A1:E20,7)",
            ],
            1,
        ):
            results.cell(row, 1, "=" + formula)

    path = xlsx_builder(populate)
    # Genuine shared OOXML without cached values: counts must see calculated
    # authority, not merely the input value store's nonempty extent.
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    output = BytesIO()
    with ZipFile(path) as original, ZipFile(output, "w") as changed:
        for entry in original.infolist():
            content = original.read(entry.filename)
            if entry.filename == "xl/worksheets/sheet1.xml":
                sheet = ET.fromstring(content)
                for cell in sheet.iter(namespace + "c"):
                    coordinate = cell.get("r")
                    if coordinate in ("C10", "C11", "C12"):
                        cell.clear()
                        cell.set("r", coordinate)
                        formula = ET.SubElement(
                            cell, namespace + "f", {"t": "shared", "si": "1"}
                        )
                        if coordinate == "C10":
                            formula.set("ref", "C10:C12")
                            formula.text = "7"
                content = ET.tostring(sheet, encoding="utf-8")
            changed.writestr(entry, content)
    book = fz.Workbook.from_bytes(output.getvalue(), span_evaluation=span_evaluation)
    book.evaluate_all()
    assert [book.get_value("Data", row, 3) for row in range(10, 13)] == [7, 7, 7]
    assert [book.get_value("Results", row, 1) for row in range(1, 6)] == [
        97,
        97,
        1_048_573,
        16_383,
        3,
    ]
