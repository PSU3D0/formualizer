"""Inputs that used to panic the extension (PanicException) or abort the
interpreter: duration values past chrono's range, non-finite numbers, and a
built-in defined name past the last grid row."""

import io
import math
import zipfile

import pytest

import formualizer as fz

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
OFFICE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
RELS = "http://schemas.openxmlformats.org/package/2006/relationships"


def xlsx(sheet_data: str, defined_names: str = "") -> bytes:
    names = f"<definedNames>{defined_names}</definedNames>" if defined_names else ""
    parts = {
        "[Content_Types].xml": (
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            "</Types>"
        ),
        "_rels/.rels": (
            f'<Relationships xmlns="{RELS}"><Relationship Id="rId1" '
            f'Type="{OFFICE}/officeDocument" Target="xl/workbook.xml"/></Relationships>'
        ),
        "xl/workbook.xml": (
            f'<workbook xmlns="{MAIN}" xmlns:r="{OFFICE}"><sheets>'
            f'<sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>{names}</workbook>'
        ),
        "xl/_rels/workbook.xml.rels": (
            f'<Relationships xmlns="{RELS}"><Relationship Id="rId1" '
            f'Type="{OFFICE}/worksheet" Target="worksheets/sheet1.xml"/></Relationships>'
        ),
        "xl/worksheets/sheet1.xml": (
            f'<worksheet xmlns="{MAIN}"><sheetData>{sheet_data}</sheetData></worksheet>'
        ),
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in parts.items():
            z.writestr(name, body)
    return out.getvalue()


def is_num_error(value) -> bool:
    return (
        isinstance(value, dict)
        and value.get("type") == "Error"
        and value.get("kind") == "Num"
    )


def test_out_of_range_duration_dict_is_a_value_error():
    wb = fz.Workbook()
    wb.add_sheet("Sheet1")
    with pytest.raises(ValueError, match="out of range"):
        wb.set_value("Sheet1", 1, 1, {"type": "duration", "seconds": 2**62})
    with pytest.raises(ValueError, match="out of range"):
        fz.LiteralValue.duration(2**62)
    # In range it still works, and the workbook is still usable.
    wb.set_value("Sheet1", 1, 1, {"type": "duration", "seconds": 90})
    wb.set_value("Sheet1", 2, 1, 5)
    assert wb.get_value("Sheet1", 2, 1) == 5


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_non_finite_set_value_is_stored_as_num_error(bad):
    wb = fz.Workbook()
    wb.add_sheet("Sheet1")
    wb.set_value("Sheet1", 1, 1, 1.0)
    wb.set_value("Sheet1", 2, 1, bad)
    wb.set_value("Sheet1", 3, 1, 3.0)
    wb.set_formula("Sheet1", 1, 2, "=MEDIAN(A1:A3)")
    wb.set_formula("Sheet1", 2, 2, "=ISERROR(A2)")
    wb.evaluate_all()
    assert is_num_error(wb.get_value("Sheet1", 2, 1))
    assert is_num_error(wb.get_value("Sheet1", 1, 2))
    assert wb.get_value("Sheet1", 2, 2) is True


def test_nan_cell_in_xlsx_loads_as_num_error():
    data = xlsx(
        '<row r="1"><c r="A1"><v>1</v></c><c r="B1"><f>MEDIAN(A1:A3)</f></c></row>'
        '<row r="2"><c r="A2"><v>NaN</v></c></row>'
        '<row r="3"><c r="A3"><v>3</v></c></row>'
    )
    wb = fz.Workbook.from_bytes(data)
    wb.evaluate_all()
    assert is_num_error(wb.get_value("Sheet1", 2, 1))
    assert is_num_error(wb.get_value("Sheet1", 1, 2))


def test_print_area_past_the_last_row_loads():
    data = xlsx(
        '<row r="1"><c r="A1"><v>2</v></c><c r="B1"><f>A1*2</f></c></row>',
        '<definedName name="_xlnm.Print_Area" localSheetId="0">'
        "Sheet1!$A$1:$B$1048577</definedName>",
    )
    wb = fz.Workbook.from_bytes(data)
    wb.evaluate_all()
    assert wb.get_value("Sheet1", 1, 2) == 4


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_non_finite_set_values_batch_is_stored_as_num_error(bad):
    wb = fz.Workbook()
    wb.add_sheet("Sheet1")
    wb.set_values_batch("Sheet1", 1, 1, [[1.0], [bad], [3.0]])
    wb.set_formula("Sheet1", 1, 2, "=MEDIAN(A1:A3)")
    wb.evaluate_all()
    assert is_num_error(wb.get_value("Sheet1", 2, 1))
    assert is_num_error(wb.get_value("Sheet1", 1, 2))
