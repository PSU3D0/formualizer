from __future__ import annotations

import pytest

import formualizer as fz


def build_workbook() -> fz.Workbook:
    wb = fz.Workbook()
    sheet = wb.sheet("Sheet1")
    sheet.set_value(1, 1, 10)
    sheet.set_value(2, 1, 20)
    sheet.set_value(2, 2, 5)
    sheet.set_formula(1, 2, "=A2+A1+SUM(A1:A2)")
    sheet.set_formula(1, 3, "=A1*2")
    sheet.set_formula(1, 4, "=A1*3")
    sheet.set_formula(1, 5, "=A1*4")
    sheet.set_formula(1, 6, "=G1+1")
    sheet.set_formula(1, 7, "=F1+1")
    sheet.set_formula(1, 8, "=SEQUENCE(3)")
    wb.evaluate_all()
    return wb


def test_all_inspection_entry_points_and_owned_reports() -> None:
    wb = build_workbook()

    snapshot = wb.inspect_cell("Sheet1!B1")
    assert snapshot.cell.address == "Sheet1!B1"
    assert snapshot.cell.value_included is True
    assert snapshot.cell.staleness is fz.Staleness.Current
    assert isinstance(snapshot.stamp.mutation_revision, int)
    assert isinstance(snapshot.stamp.recalc_epoch, int)
    assert snapshot.to_dict()["cell"]["address"] == "Sheet1!B1"

    precedents = wb.precedents("Sheet1!B1")
    assert [item.reference.kind for item in precedents.precedents] == [
        fz.ReferenceKind.Cell,
        fz.ReferenceKind.Cell,
        fz.ReferenceKind.Range,
    ]
    assert [item.reference.address for item in precedents.precedents[:2]] == [
        "Sheet1!A2",
        "Sheet1!A1",
    ]
    assert precedents.precedents[2].reference.declared == "Sheet1!A1:A2"
    assert precedents.precedents[2].reference.resolved == "Sheet1!A1:A2"

    dependents = wb.dependents("Sheet1!A1", max_results=2)
    assert [item.cell for item in dependents.dependents] == ["Sheet1!B1", "Sheet1!C1"]
    assert dependents.truncation.incomplete is True
    assert dependents.truncation.omitted.kind is fz.OmittedCountKind.AtLeast
    assert dependents.truncation.omitted.count == 1

    page = wb.range_page("Sheet1!A1:B2", limit=3)
    assert page.declared == "Sheet1!A1:B2"
    assert page.resolved == "Sheet1!A1:B2"
    assert page.total == 4
    assert [item.address for item in page.items] == [
        "Sheet1!A1",
        "Sheet1!B1",
        "Sheet1!A2",
    ]
    assert page.next_offset == 3
    final_page = wb.range_page(
        "Sheet1!A1:B2", offset=page.next_offset, limit=3, expected_stamp=page.stamp
    )
    assert [item.address for item in final_page.items] == ["Sheet1!B2"]
    assert final_page.next_offset is None

    trace = wb.trace(["Sheet1!B1"], max_depth=2, max_nodes=20)
    assert trace.roots[0].address == "Sheet1!B1"
    assert trace.nodes["Sheet1!A1"].address == "Sheet1!A1"
    assert list(trace.nodes)[0] == "Sheet1!B1"
    assert len(trace.nodes) == len(trace.to_dict()["nodes"])
    assert trace.links

    old = snapshot
    wb.set_formula("Sheet1", 1, 2, "=A1+1")
    dirty = wb.inspect_cell("Sheet1!B1")
    assert dirty.cell.staleness is fz.Staleness.Dirty
    assert old.cell.staleness is fz.Staleness.Current
    assert old.cell.formula != dirty.cell.formula


def test_cycle_spill_mapping_repr_and_exception() -> None:
    wb = build_workbook()
    cycle = wb.trace(["Sheet1!F1"], max_depth=5, max_nodes=20)
    dispositions = [
        target.disposition for link in cycle.links for target in link.targets
    ]
    assert fz.LinkDisposition.Cycle in dispositions
    assert cycle.nodes["Sheet1!F1"].address == "Sheet1!F1"

    anchor = wb.inspect_cell("Sheet1!H1")
    member = wb.inspect_cell("Sheet1!H2")
    assert anchor.cell.spill.kind is fz.SpillRoleKind.Anchor
    assert anchor.cell.spill.extent == "Sheet1!H1:H3"
    assert member.cell.spill.kind is fz.SpillRoleKind.Member
    assert member.cell.spill.anchor == "Sheet1!H1"

    large_sheet = wb.sheet("Large")
    for column in range(1, 81):
        large_sheet.set_value(2, column, column)
    large_sheet.set_formula(1, 1, "=SUM(A2:CB2)")
    wb.evaluate_all()
    large = wb.trace(
        ["Large!A1"],
        max_depth=2,
        max_nodes=100,
        max_links=100,
        range_member_budget=90,
    )
    assert len(repr(large)) < 160
    assert "nodes=" in repr(large)
    assert "=A2" not in repr(large)

    with pytest.raises(fz.SheetNotFoundError) as caught:
        wb.inspect_cell("Missing!A1")
    assert caught.value.code == "sheet_not_found"
    assert caught.value.sheet == "Missing"


def test_defined_name_is_reported_from_a_public_workbook_load(tmp_path) -> None:
    import openpyxl
    from openpyxl.workbook.defined_name import DefinedName

    path = tmp_path / "named.xlsx"
    source = openpyxl.Workbook()
    sheet = source.active
    sheet.title = "Sheet1"
    sheet["A1"] = 41
    sheet["B1"] = "=MyValue+1"
    source.defined_names.add(DefinedName("MyValue", attr_text="'Sheet1'!$A$1"))
    source.save(path)

    wb = fz.Workbook.load_path(str(path))
    wb.evaluate_all()
    report = wb.precedents("Sheet1!B1")
    assert len(report.precedents) == 1
    reference = report.precedents[0].reference
    assert reference.kind is fz.ReferenceKind.Name
    assert reference.name == "MyValue"
    assert reference.resolution.kind is fz.NameResolutionKind.Cell
    assert reference.resolution.address == "Sheet1!A1"


def test_bounds_are_plumbed_and_zero_budget_is_in_band() -> None:
    wb = build_workbook()
    assert len(wb.dependents("Sheet1!A1", max_results=1).dependents) == 1
    assert len(wb.precedents("Sheet1!B1", max_links=1).precedents) == 1
    no_work = wb.dependents("Sheet1!A1", max_results=10, max_work=0)
    assert no_work.truncation.incomplete is True
    assert no_work.truncation.omitted is None or (
        no_work.truncation.omitted.kind is fz.OmittedCountKind.AtLeast
    )
