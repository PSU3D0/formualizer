from io import BytesIO
from zipfile import ZipFile

import pytest
import formualizer as fz
from test_xlsx_cache_recalculate import calculation_name_fixture


@pytest.mark.parametrize("definition", ["Missing", "[0]!Macro", "Rate", "{1,2}", "OFFSET(#REF!,0,0,2,1)"])
def test_unused_name_does_not_prevent_load(tmp_path, definition):
    payload = calculation_name_fixture(
        f'<definedName name="Rate">{definition}</definedName>', "1+1"
    )
    path = tmp_path / "names.xlsx"
    path.write_bytes(payload)
    workbook = fz.load_workbook(str(path), strategy="eager_all")
    assert workbook.evaluate_cell("Sheet1", 1, 3) == 2
    diagnostic, = workbook.name_import_diagnostics
    assert diagnostic["name"] == "Rate"
    assert diagnostic["definition"] == definition
    assert diagnostic["message"]
    assert all(entry["name"] != "Rate" for entry in workbook.get_named_ranges())
    workbook.set_formula("Sheet1", 2, 3, "=Rate")
    assert workbook.evaluate_cell("Sheet1", 2, 3) == {"type": "Error", "kind": "Name"}


def test_scoped_omission_keeps_shadowing_and_is_not_exported_as_a_definition():
    payload = calculation_name_fixture(
        '<definedName name="Rate">7</definedName>'
        '<definedName name="Rate" localSheetId="0">Missing</definedName>',
        "1+1",
    )
    workbook = fz.load_workbook_bytes(payload, strategy="eager_all")
    diagnostic, = workbook.name_import_diagnostics
    assert diagnostic["scope_sheet"] == "Sheet1"
    assert diagnostic["local_sheet_id"] == 0
    workbook.set_formula("Sheet1", 2, 3, "=Rate")
    workbook.set_formula("Sheet1", 3, 3, '=INDIRECT("Rate")')
    for row in [2, 3]:
        assert workbook.evaluate_cell("Sheet1", row, 3) == {"type": "Error", "kind": "Name"}
    assert not any(entry["name"] == "Rate" and entry["scope"] == "sheet"
                   for entry in workbook.get_named_ranges())
    # This convenience writer does not serialize any defined names today.
    with ZipFile(BytesIO(workbook.to_xlsx_bytes())) as archive:
        assert b"definedName" not in archive.read("xl/workbook.xml")


@pytest.mark.parametrize("definition", ["Missing", "Rate", "OFFSET(#REF!,0,0,2,1)"])
def test_reading_an_omitted_name_produces_name_error(definition):
    payload = calculation_name_fixture(
        f'<definedName name="Rate">{definition}</definedName>', "Rate"
    )
    workbook = fz.load_workbook_bytes(payload, strategy="eager_all")
    assert workbook.evaluate_cell("Sheet1", 1, 3) == {"type": "Error", "kind": "Name"}
