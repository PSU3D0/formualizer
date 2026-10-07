"""A recurrence filled down a column that reads a fixed cell in the same
column (``B4=+B3/12``, ``B9=+B8-(B8*$B$4)`` filled down) reads that cell at
every size, as Excel does: 17700, 17405, ..."""

from __future__ import annotations

from io import BytesIO
from xml.etree import ElementTree as ET
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

import formualizer as fz

SIZES = [31, 32, 33, 64, 1000]


def expected(n: int, rate: float = 0.2 / 12) -> list[float]:
    out, x = [], 18000.0
    for _ in range(n):
        x -= x * rate
        out.append(x)
    return out


def schedule(n: int, col: int, anchor_formula: bool) -> fz.Workbook:
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_value("S", 3, 2, 0.2)
    if anchor_formula:
        wb.set_formula("S", 4, 2, "=+B3/12")
    else:
        wb.set_value("S", 4, 2, 0.2 / 12)
    letter = "ABCDE"[col - 1]
    wb.set_value("S", 8, col, 18000)
    for r in range(9, 9 + n):
        wb.set_formula("S", r, col, f"=+{letter}{r - 1}-({letter}{r - 1}*$B$4)")
    return wb


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(("col", "anchor_formula"), [(2, True), (2, False), (5, True)])
def test_decay_schedule_reads_same_column_anchor(n, col, anchor_formula):
    wb = schedule(n, col, anchor_formula)
    wb.evaluate_all()
    got = [wb.get_value("S", r, col) for r in range(9, 9 + n)]
    assert got == expected(n)
    assert got[0] == 17700


def schedule_xlsx(n: int) -> bytes:
    rows = (
        '<row r="3"><c r="B3"><v>0.2</v></c></row>'
        f'<row r="4"><c r="B4"><f>+B3/12</f><v>{0.2 / 12!r}</v></c></row>'
        '<row r="8"><c r="B8"><v>18000</v></c></row>'
    )
    for r in range(9, 9 + n):
        f = (
            f'<f t="shared" ref="B9:B{8 + n}" si="0">+B8-(B8*$B$4)</f>'
            if r == 9
            else '<f t="shared" si="0"/>'
        )
        rows += f'<row r="{r}"><c r="B{r}">{f}<v>18000</v></c></row>'
    members = {
        "[Content_Types].xml": '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
        "_rels/.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        "xl/workbook.xml": '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Terrebonne" sheetId="1" r:id="rId1"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
        "xl/worksheets/sheet1.xml": '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        + rows
        + "</sheetData></worksheet>",
    }
    out = BytesIO()
    with ZipFile(out, "w", ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return out.getvalue()


@pytest.mark.parametrize("n", SIZES)
def test_xlsx_recalc_of_decay_schedule_matches_excel(n):
    result = fz.recalculate_xlsx_bytes(schedule_xlsx(n))
    with ZipFile(BytesIO(result["bytes"])) as archive:
        xml = archive.read("xl/worksheets/sheet1.xml").decode()
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(xml)
    got = [
        float(root.find(f'.//s:c[@r="B{r}"]', ns).findtext("s:v", namespaces=ns))
        for r in range(9, 9 + n)
    ]
    assert got == expected(n)
