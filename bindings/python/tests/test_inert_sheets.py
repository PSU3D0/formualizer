from io import BytesIO
from zipfile import ZipFile

import pytest
import formualizer as fz
from test_xlsx_cache_recalculate import calculation_name_fixture


def inert_fixture(kind, formula):
    source = calculation_name_fixture("", formula)
    with ZipFile(BytesIO(source)) as archive:
        parts = {name: archive.read(name) for name in archive.namelist()}
    office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rid = "" if kind == "module" else "rIdInert"
    parts["xl/workbook.xml"] = parts["xl/workbook.xml"].replace(
        b"</sheets>",
        f'<sheet name="Chart1" sheetId="2" r:id="{rid}"/></sheets>'.encode(),
    )
    if kind != "module":
        parts["xl/_rels/workbook.xml.rels"] = parts["xl/_rels/workbook.xml.rels"].replace(
            b"</Relationships>",
            f'<Relationship Id="rIdInert" Type="{office}/{kind}" Target="{kind}s/sheet2.xml"/></Relationships>'.encode(),
        )
        parts[f"xl/{kind}s/sheet2.xml"] = f'<{kind} xmlns="{main}"/>'.encode()
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        for name, body in parts.items():
            archive.writestr(name, body)
    return output.getvalue()


@pytest.mark.parametrize("kind", ["chartsheet", "dialogsheet", "module"])
@pytest.mark.parametrize("formula", ["Chart1!A1", "SUM(Chart1!A1:B3)", 'INDIRECT("Chart1!A1")'])
def test_inert_sheet_is_unknown_on_load_and_after_edit(tmp_path, kind, formula):
    payload = inert_fixture(kind, formula.replace('"', "&quot;"))
    path = tmp_path / "inert.xlsx"
    path.write_bytes(payload)
    workbook = fz.load_workbook(str(path), strategy="eager_all")
    assert workbook.sheet_names == ["Sheet1"]
    assert workbook.sheet_import_diagnostics == [{"name": "Chart1", "kind": kind}]
    assert workbook.evaluate_cell("Sheet1", 1, 3) == {"type": "Error", "kind": "Ref"}
    workbook.set_formula("Sheet1", 2, 3, "=" + formula)
    assert workbook.evaluate_cell("Sheet1", 2, 3) == {"type": "Error", "kind": "Ref"}
    with ZipFile(BytesIO(workbook.to_xlsx_bytes())) as archive:
        assert b"Chart1" not in archive.read("xl/workbook.xml")
    workbook.add_sheet("Chart1")
    assert "Chart1" in workbook.sheet_names
