from __future__ import annotations

from io import BytesIO
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

import formualizer as fz


def fixture_xlsx(*, formula: bool = True) -> bytes:
    worksheet = (
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData><row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>2</v></c>'
        + (
            '<c r="C1" t="str"><f>A1+B1</f><v>stale</v></c>'
            if formula
            else '<c r="C1"><v>3</v></c>'
        )
        + "</row></sheetData></worksheet>"
    )
    members = {
        "[Content_Types].xml": '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
        "_rels/.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        "xl/workbook.xml": '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
        "xl/worksheets/sheet1.xml": worksheet,
    }
    out = BytesIO()
    with ZipFile(out, "w", ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return out.getvalue()


def calculation_name_fixture(definitions: str, formula: str) -> bytes:
    out = BytesIO()
    with (
        ZipFile(BytesIO(fixture_xlsx())) as source,
        ZipFile(out, "w", ZIP_DEFLATED) as target,
    ):
        for name in source.namelist():
            body = source.read(name).decode()
            if name == "xl/workbook.xml":
                body = body.replace(
                    "</workbook>",
                    f"<definedNames>{definitions}</definedNames></workbook>",
                )
            elif name == "xl/worksheets/sheet1.xml":
                body = body.replace("A1+B1", formula)
            target.writestr(name, body)
    return out.getvalue()


@pytest.mark.parametrize(
    "definition,formula,expected,kind",
    [
        ("0.07", "Rate*2", "0.14", None),
        ("Sheet1!$A$1*2", "Rate", "2", None),
        ("TRUE", "Rate", "1", "b"),
        ("&quot;original text&quot;", "Rate", "original text", "str"),
        ("#N/A", "Rate", "#N/A", "e"),
    ],
)
def test_calculation_names_preserve_metadata_and_typed_results(
    tmp_path, definition, formula, expected, kind
):
    payload = calculation_name_fixture(
        f'<definedName name="Rate">{definition}</definedName>', formula
    )
    result = fz.recalculate_xlsx_bytes(payload)
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    cell = ET.fromstring(worksheet_xml(result["bytes"])).find('.//s:c[@r="C1"]', ns)
    assert cell.get("t") == kind
    assert cell.findtext("s:v", namespaces=ns) == expected
    with (
        ZipFile(BytesIO(payload)) as before,
        ZipFile(BytesIO(result["bytes"])) as after,
    ):
        assert before.read("xl/workbook.xml") == after.read("xl/workbook.xml")
    assert fz.recalculate_xlsx_bytes(result["bytes"])["bytes"] == result["bytes"]
    source, destination = tmp_path / "source.xlsx", tmp_path / "out.xlsx"
    source.write_bytes(payload)
    fz.recalculate_xlsx_file(str(source), output=str(destination))
    assert source.read_bytes() == payload
    assert destination.read_bytes() == result["bytes"]


@pytest.mark.parametrize(
    "definitions",
    [
        '<definedName name="Rate">Sheet1!$A$1,Sheet1!$B$1</definedName>',
        '<definedName name="Rate">Sheet1!A1*2</definedName>',
        '<definedName name="Rate">Rate</definedName>',
    ],
)
def test_unsupported_calculation_names_do_not_publish(tmp_path, definitions):
    payload = calculation_name_fixture(definitions, "Rate*2")
    with pytest.raises(OSError, match="recalculate XLSX failed"):
        fz.recalculate_xlsx_bytes(payload)
    source, destination = tmp_path / "source.xlsx", tmp_path / "out.xlsx"
    source.write_bytes(payload)
    destination.write_bytes(b"keep destination")
    with pytest.raises(OSError, match="recalculate XLSX failed"):
        fz.recalculate_xlsx_file(str(source), output=str(destination))
    assert source.read_bytes() == payload
    assert destination.read_bytes() == b"keep destination"


def worksheet_xml(payload: bytes) -> str:
    with ZipFile(BytesIO(payload)) as archive:
        return archive.read("xl/worksheets/sheet1.xml").decode()


def test_recalculate_xlsx_bytes_updates_typed_cache_and_returns_bytes():
    result = fz.recalculate_xlsx_bytes(fixture_xlsx())

    assert isinstance(result["bytes"], bytes)
    assert result["summary"]["status"] == "success"
    assert result["formula_cells"] == result["cache_cells_changed"] == 1
    assert result["worksheet_parts_changed"] == 1
    xml = worksheet_xml(result["bytes"])
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    cell = ET.fromstring(xml).find('.//s:c[@r="C1"]', ns)
    assert cell is not None and cell.get("t") is None
    assert cell.findtext("s:f", namespaces=ns) == "A1+B1"
    assert cell.findtext("s:v", namespaces=ns) == "3"
    repeated = fz.recalculate_xlsx_bytes(result["bytes"])
    assert repeated["bytes"] == result["bytes"]
    assert repeated["cache_cells_changed"] == 0


def test_recalculate_xlsx_bytes_no_formula_is_a_noop():
    payload = fixture_xlsx(formula=False)
    result = fz.recalculate_xlsx_bytes(payload)

    assert result["bytes"] == payload
    assert result["formula_cells"] == 0
    assert result["cache_cells_changed"] == 0
    assert result["worksheet_parts_changed"] == 0


def test_recalculate_xlsx_bytes_surfaces_core_rejection():
    with pytest.raises(OSError, match="recalculate XLSX failed"):
        fz.recalculate_xlsx_bytes(b"not an XLSX package")


def test_recalculate_xlsx_file_failure_leaves_existing_destination_unchanged(
    tmp_path: Path,
):
    source = tmp_path / "invalid.xlsx"
    destination = tmp_path / "destination.xlsx"
    source.write_bytes(b"not an XLSX package")
    destination.write_bytes(b"must remain unchanged")

    with pytest.raises(OSError, match="recalculate XLSX failed"):
        fz.recalculate_xlsx_file(str(source), output=str(destination))

    assert destination.read_bytes() == b"must remain unchanged"
