from io import BytesIO
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

import formualizer as fz


def shared_bytes(path, records):
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    output = BytesIO()
    with ZipFile(path) as original, ZipFile(output, "w") as changed:
        for entry in original.infolist():
            content = original.read(entry.filename)
            if entry.filename == "xl/worksheets/sheet1.xml":
                sheet = ET.fromstring(content)
                for cell in sheet.iter(namespace + "c"):
                    coordinate = cell.get("r")
                    if coordinate in records:
                        index, domain, text = records[coordinate]
                        cell.clear()
                        cell.set("r", coordinate)
                        formula = ET.SubElement(
                            cell, namespace + "f", {"t": "shared", "si": str(index)}
                        )
                        if domain is not None:
                            formula.set("ref", domain)
                            formula.text = text
                content = ET.tostring(sheet, encoding="utf-8")
            changed.writestr(entry, content)
    return output.getvalue()


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_healthy_shared_member_does_not_demand_broken_master_precedent(
    xlsx_builder, span_evaluation
):
    def populate(book):
        sheet = book.active
        sheet.title = "S"
        sheet["A1"] = "=NOSHEET!A1"
        sheet["A2"] = 41
        sheet["B1"] = 0
        sheet["B2"] = 0

    raw = shared_bytes(
        xlsx_builder(populate),
        {"B1": (1, "B1:B2", "A1+1"), "B2": (1, None, None)},
    )
    book = fz.Workbook.from_bytes(raw, span_evaluation=span_evaluation)
    assert book.evaluate_cell("S", 2, 2) == 42
    with pytest.raises(fz.ExcelEvaluationError):
        book.evaluate_cell("S", 1, 2)
    with pytest.raises(fz.ExcelEvaluationError):
        book.evaluate_all()
    assert book.get_formula("S", 1, 1) is not None
    book.set_value("S", 2, 2, 123)
    book.set_value("S", 1, 1, 99)
    book.evaluate_all()
    assert book.get_value("S", 1, 2) == 100
    assert book.get_value("S", 2, 2) == 123
    assert book.get_formula("S", 2, 2) is None


@pytest.mark.parametrize("span_evaluation", [False, True])
def test_whole_shared_family_target_retains_unrelated_failure_and_edit_history(
    xlsx_builder, span_evaluation
):
    count = 200

    def populate(book):
        sheet = book.active
        sheet.title = "S"
        for row in range(1, count + 1):
            sheet.cell(row, 1, row)
            sheet.cell(row, 2, 0)
        sheet["D1"] = f"=SUM(B1:B{count})"
        sheet["D2"] = "=NOSHEET!A1"

    records = {f"B{row}": (1, None, None) for row in range(2, count + 1)}
    records["B1"] = (1, f"B1:B{count}", "A1+1")
    book = fz.Workbook.from_bytes(
        shared_bytes(xlsx_builder(populate), records),
        span_evaluation=span_evaluation,
    )
    expected = count * (count + 1) // 2 + count
    assert book.evaluate_cell("S", 1, 4) == expected
    assert book.get_formula("S", 2, 4) is not None
    book.set_value("S", 100, 1, 200)
    assert book.evaluate_cell("S", 1, 4) == expected + 100
    book.undo()
    assert book.evaluate_cell("S", 1, 4) == expected
    with pytest.raises(fz.ExcelEvaluationError):
        book.evaluate_all()
    book.set_value("S", 2, 4, 0)
    book.evaluate_all()
    assert book.get_value("S", 1, 4) == expected
