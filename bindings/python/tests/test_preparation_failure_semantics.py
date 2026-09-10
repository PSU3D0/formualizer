from __future__ import annotations

from io import BytesIO

import openpyxl
import pytest

import formualizer as fz


def imported(formulas: dict[str, str]) -> fz.Workbook:
    source = openpyxl.Workbook()
    sheet = source.active
    assert sheet is not None
    sheet.title = "S"
    for address, formula in formulas.items():
        sheet[address] = formula
    output = BytesIO()
    source.save(output)
    return fz.Workbook.from_bytes(output.getvalue())


@pytest.mark.parametrize("targeted", [False, True])
def test_failed_import_preparation_retains_source_and_retries(targeted: bool) -> None:
    wb = imported({"A1": "=1+2", "B1": "=NOSHEET!A1", "C1": '="NOSHEET!A1"'})
    original = [wb.get_formula("S", 1, col) for col in range(1, 4)]
    for _ in range(2):
        with pytest.raises(fz.ExcelEvaluationError):
            if targeted:
                wb.evaluate_cell("S", 1, 2)
            else:
                wb.evaluate_all()
        assert [wb.get_formula("S", 1, col) for col in range(1, 4)] == original
        assert wb.inspect_cell("S!B1").cell.formula == "=NOSHEET!A1"
        assert wb.inspect_cell("S!C1").cell.formula == '="NOSHEET!A1"'
    wb.add_sheet("NOSHEET")
    wb.set_value("NOSHEET", 1, 1, 42)
    wb.evaluate_all()
    assert [wb.get_value("S", 1, col) for col in range(1, 4)] == [
        3,
        42,
        "NOSHEET!A1",
    ]


def test_imported_healthy_target_isolates_unrelated_but_not_required_failures() -> None:
    wb = imported({"A1": "=1+2", "B1": "=NOSHEET!A1", "C1": "=B1+1"})
    assert wb.evaluate_cell("S", 1, 1) == 3
    assert wb.get_formula("S", 1, 2) is not None
    with pytest.raises(fz.ExcelEvaluationError):
        wb.evaluate_cell("S", 1, 3)
    with pytest.raises(fz.ExcelEvaluationError):
        wb.evaluate_all()
    wb.add_sheet("NOSHEET")
    wb.set_value("NOSHEET", 1, 1, 42)
    wb.evaluate_all()
    assert wb.get_value("S", 1, 3) == 43


@pytest.mark.parametrize(
    "formula",
    [
        "=IFERROR(NOSHEET!A1,456)",
        "=IFNA(NOSHEET!A1,456)",
        "=IFERROR(SUM(IFERROR(NOSHEET!A1,0)),1)",
        "=IFERROR(7,NOSHEET!A1)",
        "=IFERROR(SUM(MissingTable[Amount]),456)",
        "=ISERROR(NOSHEET!A1)",
    ],
)
def test_reference_preparation_remains_an_exception(formula: str) -> None:
    wb = imported({"A1": formula})
    with pytest.raises(fz.ExcelEvaluationError):
        wb.evaluate_all()


@pytest.mark.parametrize(
    ("formula", "expected"),
    [
        ("=IFERROR(#REF!,456)", 456),
        ("=IFNA(#N/A,456)", 456),
        ("=IFERROR(SUM(IFERROR(#REF!,0)),1)", 0),
        ("=IFERROR(7,1/0)", 7),
        ("=IFERROR(MissingName,456)", 456),
        ("=IFERROR(UnknownFunction(1),456)", 456),
    ],
)
def test_cell_errors_still_reach_their_runtime_guards(
    formula: str, expected: int
) -> None:
    wb = imported({"A1": formula})
    wb.evaluate_all()
    assert wb.get_value("S", 1, 1) == expected


@pytest.mark.parametrize(
    ("formula", "kind"),
    [
        ("=IFNA(#REF!,456)", "Ref"),
        ("=IFNA(MissingName,456)", "Name"),
        ("=IFERROR(A1,456)", "Circ"),
        ("=IFERROR(1+,456)", "Error"),
    ],
)
def test_other_existing_error_phases_are_not_reclassified(
    formula: str, kind: str
) -> None:
    wb = imported({"A1": formula})
    wb.evaluate_all()
    assert wb.get_value("S", 1, 1) == {"type": "Error", "kind": kind}
