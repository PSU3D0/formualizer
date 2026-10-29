"""Native source-policy transport, verified against actual published XLSX caches."""

import io
from datetime import datetime, timezone

import openpyxl
import pytest

import formualizer as fz


def book(formulas, *, iterate=False, wide=False, date1904=False, precision=True):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "S"
    for address, formula in formulas.items():
        ws[address] = formula
    if wide:
        ws["XFD2"] = 3
    wb.calculation.fullPrecision = precision
    wb.calculation.iterate = iterate
    wb.calculation.iterateCount = 100
    wb.calculation.iterateDelta = 0.001
    if date1904:
        wb.epoch = openpyxl.utils.datetime.CALENDAR_MAC_1904
    stream = io.BytesIO()
    wb.save(stream)
    return stream.getvalue()


def reopen(result):
    data = result["bytes"]
    formulas = openpyxl.load_workbook(io.BytesIO(data), data_only=False).active
    values = openpyxl.load_workbook(io.BytesIO(data), data_only=True).active
    return formulas, values


def test_workbook_partial_sparse_invalidates_caught_gap_and_dependents():
    source = book(
        {"A1": "=IFERROR(NOT_IMPLEMENTED(2),7)", "B1": "=A1+1", "C1": "=2+2"}, wide=True
    )
    with pytest.raises(OSError):
        fz.recalculate_xlsx_bytes(source)
    result = fz.recalculate_xlsx_bytes(source, profile="workbook")
    assert result["coverage"] == "partial"
    assert result["coverage_scope"] == "conservative"
    assert result["caches_invalidated"] >= 2
    assert result["calculation_flags_changed"]
    diagnostics = result["operation_diagnostics"]
    assert (
        diagnostics["unavailableFunctions"]["NOT_IMPLEMENTED"]["cellReasonCount"] >= 1
    )
    assert diagnostics["logicalRootCellCount"] >= 1
    assert "unavailableResultCells" in diagnostics
    formulas, values = reopen(result)
    assert formulas["A1"].value == "=IFERROR(NOT_IMPLEMENTED(2),7)"
    assert values["A1"].value is None
    assert values["B1"].value is None
    assert values["C1"].value == 4
    with pytest.raises(fz.CalculationDeferred) as exc:
        fz.recalculate_xlsx_bytes(source, profile="workbook", require_complete=True)
    assert exc.value.reason == "KnownCoverageIncomplete"
    assert exc.value.context


def test_source_precision_deferral_preserves_new_upstream_reason():
    source = book({"A1": "=2+2"}, precision=False)
    strict = fz.recalculate_xlsx_bytes(source)
    assert strict["coverage"] == "partial"
    assert strict["operation_diagnostics"]["workbookFeatures"] == [
        "precision_as_displayed"
    ]
    with pytest.raises(fz.CalculationDeferred) as exc:
        fz.recalculate_xlsx_bytes(source, profile="workbook")
    assert exc.value.reason == "WorkbookPrecisionUnverified"
    assert exc.value.context


def test_legitimate_excel_errors_are_complete():
    result = fz.recalculate_xlsx_bytes(
        book({"A1": "=1/0", "B1": "=2+2"}), profile="workbook", require_complete=True
    )
    assert result["coverage"] == "complete"
    assert reopen(result)[1]["A1"].value == "#DIV/0!"
    assert reopen(result)[1]["B1"].value == 4


def test_verified_iteration_defers_and_explicit_cold_start_is_reported():
    source = book({"A1": "=(A1+1)/2"}, iterate=True)
    with pytest.raises(fz.CalculationDeferred) as exc:
        fz.recalculate_xlsx_bytes(source, profile="workbook")
    assert exc.value.reason == "IterativeInitialStateUnverified"
    result = fz.recalculate_xlsx_bytes(
        source, profile="workbook", calculation={"iterationInitialState": "cold_start"}
    )
    assert result["calculation"]["iterationInitialState"] == "cold_start"
    assert result["calculation"]["iterationOrigin"] == "workbook"
    assert result["calculation"]["maxIterations"] == 100
    assert result["calculation"]["maxChange"] == 0.001
    assert reopen(result)[1]["A1"].value > 0.99


def test_exact_seed_clock_source_date_system_and_legacy_merge():
    source = book({"A1": "=RAND()", "B1": "=NOW()"}, date1904=True)
    policy = {
        "workbookSeed": "18446744073709551615",
        "fixedNowUtc": "2024-01-02T03:04:05Z",
        "timezone": "utc",
    }
    result = fz.recalculate_xlsx_bytes(source, profile="workbook", calculation=policy)
    assert result["seed"] == 18446744073709551615
    assert result["clock"]["fixed"]
    assert result["calculation"]["workbookSeed"] == policy["workbookSeed"]
    assert result["calculation"]["dateSystem"] == "1904"
    assert result["calculation"]["fixedNowUtc"].startswith("2024-01-02T03:04:05")
    assert reopen(result)[1]["B1"].value == pytest.approx(43831.12783564815)
    again = fz.recalculate_xlsx_bytes(
        source, profile="workbook", calculation=policy, rng_seed=18446744073709551615
    )
    assert again["bytes"] == result["bytes"]
    legacy = fz.recalculate_xlsx_bytes(
        source,
        rng_seed=1,
        deterministic_timestamp_utc=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    assert legacy["seed"] == 1
    with pytest.raises(ValueError, match="conflict"):
        fz.recalculate_xlsx_bytes(source, calculation=policy, rng_seed=1)


@pytest.mark.parametrize(
    "policy",
    [
        {"unknown": 1},
        {"workbookSeed": 9007199254740992},
        {"workbookSeed": "-1"},
        {"iteration": {"mode": "iterate", "maxIterations": 0}},
        {"iteration": {"mode": "iterate", "maxChange": float("nan")}},
        {"timezone": {"offsetSeconds": 999999}},
        {"fixedNowUtc": "bad"},
    ],
)
def test_invalid_policy_precedes_package_admission(policy):
    with pytest.raises((ValueError, TypeError)):
        fz.recalculate_xlsx_bytes(b"invalid package", calculation=policy)


def test_policy_timezone_without_fixed_clock_is_effective():
    result = fz.recalculate_xlsx_bytes(
        book({"A1": "=NOW()"}), calculation={"timezone": "utc"}
    )
    assert result["calculation"]["timezone"] == "utc"
    assert result["clock"]["timezone"] == "UTC"
    assert result["clock"]["fixed"] is False


@pytest.mark.parametrize(
    "legacy",
    [
        {"deterministic_timestamp_utc": datetime(2025, 1, 1, tzinfo=timezone.utc)},
        {
            "deterministic_timestamp_utc": datetime(
                2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc
            ),
            "deterministic_timezone": "+02:00",
        },
    ],
)
def test_conflicting_legacy_clock_fields_are_rejected(legacy):
    with pytest.raises(ValueError, match="conflict"):
        fz.recalculate_xlsx_bytes(
            book({"A1": "=NOW()"}),
            calculation={"fixedNowUtc": "2024-01-02T03:04:05Z", "timezone": "utc"},
            **legacy,
        )


def test_file_options_and_failures_preserve_destination(tmp_path):
    source = tmp_path / "source.xlsx"
    out = tmp_path / "out.xlsx"
    source.write_bytes(book({"A1": "=2+2"}))
    result = fz.recalculate_xlsx_file(
        str(source), str(out), profile="workbook", require_complete=True
    )
    assert out.read_bytes() == result["bytes"]
    assert reopen(result)[1]["A1"].value == 4
    source.write_bytes(book({"A1": "=(A1+1)/2"}, iterate=True))
    before = out.read_bytes()
    with pytest.raises(fz.CalculationDeferred):
        fz.recalculate_xlsx_file(str(source), str(out), profile="workbook")
    assert out.read_bytes() == before
    source.write_bytes(book({"A1": "=2+2"}))
    with pytest.raises(OSError):
        fz.recalculate_xlsx_file(
            str(source), str(tmp_path / "missing" / "out.xlsx"), profile="workbook"
        )
    with pytest.raises(OSError) as exc:
        fz.recalculate_xlsx_bytes(b"corrupt", profile="workbook")
    assert not isinstance(exc.value, fz.CalculationDeferred)


def test_work_budget_is_failure_not_defer():
    with pytest.raises(fz.ExcelEvaluationError) as exc:
        fz.recalculate_xlsx_bytes(
            book({"A1": "=SUM(1,2)"}),
            profile="workbook",
            calculation={"maxWorkUnits": 0},
        )
    assert exc.value.resource_reason == "work_units"
    assert exc.value.limit == 0
    assert exc.value.observed > 0
    assert not isinstance(exc.value, fz.CalculationDeferred)
