"""Transport controls run against an installed wheel, with independent XLSX reopen."""

import inspect

import pytest
from test_source_calculation import book, reopen

import formualizer as fz

LIMIT_FIELDS = (
    "maxInputBytes",
    "maxEntries",
    "maxExpandedBytes",
    "maxWorksheetBytes",
    "maxFormulaCells",
    "maxOutputBytes",
    "maxXmlDepth",
    "maxCells",
    "maxColumns",
    "maxOperationRoots",
    "maxOperationTextBytes",
    "maxAffectedWork",
    "maxAffectedRegions",
    "maxAffectedScratchBytes",
)


@pytest.mark.parametrize("field", LIMIT_FIELDS)
@pytest.mark.parametrize(
    "value", [None, True, 0.5, -1, "0", [], float("nan"), float("inf")]
)
def test_invalid_limits_before_zip_and_file_destination(field, value, tmp_path):
    kwargs = {"limits": {field: value}, "profile": "workbook"}
    with pytest.raises((TypeError, ValueError)):
        fz.recalculate_xlsx_bytes(b"not a zip", **kwargs)
    out = tmp_path / "out.xlsx"
    out.write_bytes(b"keep destination")
    with pytest.raises((TypeError, ValueError)):
        fz.recalculate_xlsx_file(str(tmp_path / "absent.xlsx"), str(out), **kwargs)
    assert out.read_bytes() == b"keep destination"


@pytest.mark.parametrize(
    "limits",
    [
        [],
        [{}],
        True,
        "limits",
        {"unknown": 1},
        {"maxInputBytes": 2**32},
        {"maxAffectedWork": 2**53},
        {"maxColumns": 16385},
    ],
)
def test_invalid_limit_records_and_domains(limits):
    with pytest.raises((TypeError, ValueError)):
        fz.recalculate_xlsx_bytes(b"not a zip", limits=limits)
    assert fz.recalculate_xlsx_bytes(book({"A1": "=1+1"}))["coverage"] == "complete"


@pytest.mark.parametrize(
    "policy",
    [
        [],
        {"iteration": []},
        {"timezone": []},
        {"maxElapsedMs": 2**53},
        {"maxWorkUnits": 2**53},
        {"maxElapsedMs": True},
        {"maxWorkUnits": 0.5},
        {"maxElapsedMs": "17"},
        {"maxWorkUnits": "17"},
    ],
)
def test_policy_map_and_budget_validation_precedes_zip(policy):
    with pytest.raises((TypeError, ValueError)):
        fz.recalculate_xlsx_bytes(b"not a zip", calculation=policy)


def test_preset_then_explicit_limits_and_legacy_merge(tmp_path):
    source = book({"A1": "=SUM(1,2)"}, wide=True)
    for limits in [None, {}, {"maxAffectedWork": 2**53 - 1}]:
        result = fz.recalculate_xlsx_bytes(
            source,
            profile="workbook",
            limits=limits,
            rng_seed=7,
            error_location_limit=0,
        )
        assert result["seed"] == 7
        assert reopen(result)[1]["A1"].value == 3
    for require_complete in [False, True]:
        with pytest.raises(OSError):
            fz.recalculate_xlsx_bytes(
                source, require_complete=require_complete, calculation={}
            )
    # Explicit limits override the workbook preset rather than being discarded.
    with pytest.raises(OSError):
        fz.recalculate_xlsx_bytes(source, profile="workbook", limits={"maxColumns": 1})
    path, out = tmp_path / "in.xlsx", tmp_path / "out.xlsx"
    path.write_bytes(source)
    result = fz.recalculate_xlsx_file(
        str(path),
        str(out),
        profile="workbook",
        limits={"maxColumns": 16384},
        physical_telemetry=True,
    )
    assert out.read_bytes() == result["bytes"]
    assert result["operation_diagnostics"]["physicalFunctionDispatches"] > 0


@pytest.mark.parametrize(
    "formula,name,verification",
    [
        ('CELL("format",B1)', "CELL", "unavailable"),
        ('IFERROR(CELL("format",B1),7)', "CELL", "unavailable"),
        ("IF(TRUE,SUM,AVERAGE)", "IF", "unverified"),
        ("HSTACK(SUM,AVERAGE)", "HSTACK", "unverified"),
        ('PIVOTBY(A1:A2,B1:B2,C1:C2,"SUM",0,0,0,0,0,D1:D2)', "PIVOTBY", "unavailable"),
    ],
)
def test_executed_modes_prevent_false_complete(formula, name, verification, tmp_path):
    source = book(
        {
            "A1": 1,
            "A2": 2,
            "B1": 1,
            "B2": 2,
            "C1": 4,
            "C2": 5,
            "D1": True,
            "D2": False,
            "J1": "=" + formula,
            "K1": "=J1+1",
            "M1": "=2+2",
        }
    )
    result = fz.recalculate_xlsx_bytes(
        source, profile="workbook", limits={"maxOperationRoots": 1000000}
    )
    assert result["coverage"] == "partial"
    diagnostics = result["operation_diagnostics"]
    assert any(
        mode["name"] == name and mode["verification"] == verification
        for mode in diagnostics["functionModes"]
    )
    assert any(
        sample["operation"]["kind"]
        == verification.replace("unavailable", "unimplemented") + "_function_mode"
        for sample in diagnostics["samples"]
    )
    assert diagnostics["physicalFunctionDispatches"] is None
    unsampled = fz.recalculate_xlsx_bytes(
        source,
        profile="workbook",
        error_location_limit=0,
        limits={},
        physical_telemetry=True,
    )
    assert unsampled["operation_diagnostics"]["samples"] == []
    assert (
        unsampled["operation_diagnostics"]["functionModes"]
        == diagnostics["functionModes"]
    )
    assert unsampled["operation_diagnostics"]["physicalFunctionDispatches"] > 0
    assert reopen(result)[1]["J1"].value is None
    assert reopen(result)[1]["K1"].value is None
    assert reopen(result)[1]["M1"].value == 4
    evidence = diagnostics["dependencyEvidence"]
    assert evidence["semanticReadsUnattested"]
    assert evidence["sourceDomain"] == "admitted_workbook_and_fixed_cached_sources"
    assert (
        evidence["work"]
        == evidence["queryWork"] + evidence["publicationMembershipWorkBound"]
    )
    with pytest.raises(fz.CalculationDeferred):
        fz.recalculate_xlsx_bytes(source, profile="workbook", require_complete=True)
    path, out = tmp_path / "in.xlsx", tmp_path / "out.xlsx"
    path.write_bytes(source)
    out.write_bytes(b"unchanged")
    with pytest.raises(fz.CalculationDeferred):
        fz.recalculate_xlsx_file(
            str(path), str(out), profile="workbook", require_complete=True
        )
    assert out.read_bytes() == b"unchanged"


@pytest.mark.parametrize(
    "formula",
    [
        'CELL("not_a_mode",B1)',
        'CELL("row",B1)',
        "IF(FALSE,IF(TRUE,SUM,AVERAGE),7)",
        'IF("bad",SUM,AVERAGE)',
        "IF(TRUE,SUM(1,2),AVERAGE(1,2))",
        "IFERROR(1/0,7)",
        "1/0",
        'PIVOTBY(A1:A2,B1:B2,C1:C2,"SUM",9)',
        'PIVOTBY(A1:A2,B1:B2,C1:C2,"SUM",0,0,0,0,0)',
    ],
)
def test_invalid_inactive_supported_and_ordinary_errors_complete(formula):
    result = fz.recalculate_xlsx_bytes(
        book(
            {"A1": 1, "A2": 2, "B1": 1, "B2": 2, "C1": 4, "C2": 5, "J1": "=" + formula}
        ),
        profile="workbook",
        require_complete=True,
    )
    assert result["coverage"] == "complete"
    assert not result["operation_diagnostics"]["functionModes"]


@pytest.mark.parametrize(
    "field,reason",
    [
        ("maxOperationRoots", "operation_evidence_roots"),
        ("maxOperationTextBytes", "operation_evidence_text_bytes"),
        ("maxAffectedWork", "work_units"),
        ("maxAffectedRegions", "dependency_closure_output_items"),
        ("maxAffectedScratchBytes", "scratch_memory"),
    ],
)
def test_fatal_limits_sample_zero_atomic_destination_and_recovery(
    field, reason, tmp_path
):
    source = book({"A1": "=MISSING_FN(1)", "B1": "=A1+1", "C1": "=2+2"})
    path, out = tmp_path / "in.xlsx", tmp_path / "out.xlsx"
    path.write_bytes(source)
    out.write_bytes(b"keep destination")
    kwargs = {
        "profile": "workbook",
        "require_complete": False,
        "error_location_limit": 0,
        "limits": {field: 0},
    }
    for call in [
        lambda: fz.recalculate_xlsx_bytes(source, **kwargs),
        lambda: fz.recalculate_xlsx_file(str(path), str(out), **kwargs),
    ]:
        with pytest.raises(fz.ExcelEvaluationError) as exc:
            call()
        assert exc.value.resource_reason == reason
        assert exc.value.limit == 0
        assert exc.value.observed > 0
        assert not isinstance(exc.value, fz.CalculationDeferred)
        assert out.read_bytes() == b"keep destination"
        recovered = fz.recalculate_xlsx_bytes(source, profile="workbook")
        assert recovered["coverage"] == "partial"
        assert reopen(recovered)[1]["C1"].value == 4


def test_strict_unused_query_zero_and_optional_telemetry():
    no_query = dict.fromkeys(
        ["maxAffectedWork", "maxAffectedRegions", "maxAffectedScratchBytes"], 0
    )
    result = fz.recalculate_xlsx_bytes(book({"A1": "=1+1"}), limits=no_query)
    assert result["coverage"] == "complete"
    assert result["operation_diagnostics"]["dependencyEvidence"] is None
    strict_gap = book({"A1": "=MISSING_FN(1)"})
    partial = fz.recalculate_xlsx_bytes(strict_gap, limits=no_query)
    assert partial["coverage"] == "partial"
    assert partial["operation_diagnostics"]["dependencyEvidence"] is None
    with pytest.raises(fz.CalculationDeferred):
        fz.recalculate_xlsx_bytes(strict_gap, limits=no_query, require_complete=True)
    assert result["operation_diagnostics"]["physicalFunctionDispatches"] is None
    assert result["operation_diagnostics"]["physicalUnavailableObservations"] is None
    measured = fz.recalculate_xlsx_bytes(book({"A1": "=1+1"}), physical_telemetry=True)
    assert measured["operation_diagnostics"]["physicalFunctionDispatches"] == 0
    called = fz.recalculate_xlsx_bytes(
        book({"A1": "=SUM(1,2)"}), physical_telemetry=True
    )
    assert called["operation_diagnostics"]["physicalFunctionDispatches"] > 0


def test_independent_dynamic_scalar_spill_reopen():
    result = fz.recalculate_xlsx_bytes(
        book(
            {
                "A1": "=MISSING_FN(1)",
                "B1": '=INDIRECT("A1")',
                "C1": "=B1+1",
                "D1": 7,
                "D2": 9,
                "E1": '=INDIRECT("D1")',
                "F1": "=E1+1",
                "H1": '=INDIRECT("D1:D2")',
            }
        ),
        profile="workbook",
    )
    formulas, values = reopen(result)
    assert result["coverage"] == "partial"
    assert formulas["H1"].value.text == '=INDIRECT("D1:D2")'
    assert formulas["H1"].value.ref == "H1:H2"
    assert [values[cell].value for cell in ["A1", "B1", "C1"]] == [None, None, None]
    assert [values[cell].value for cell in ["E1", "F1", "H1", "H2"]] == [7, 8, 7, 9]


def test_query_work_bound_includes_publication_and_is_explicit():
    source = book({"A1": "=MISSING_FN(1)", "B1": "=2+2"})
    normal = fz.recalculate_xlsx_bytes(source, profile="workbook")
    evidence = normal["operation_diagnostics"]["dependencyEvidence"]
    # Query-only admission is insufficient for publishing this result.
    assert evidence["publicationMembershipWorkBound"] > 0
    with pytest.raises(fz.ExcelEvaluationError) as exc:
        fz.recalculate_xlsx_bytes(
            source,
            profile="workbook",
            limits={"maxAffectedWork": evidence["queryWork"]},
        )
    assert exc.value.resource_reason == "work_units"
    admitted = fz.recalculate_xlsx_bytes(
        source, profile="workbook", limits={"maxAffectedWork": evidence["work"]}
    )
    assert admitted["operation_diagnostics"]["dependencyEvidence"] == evidence


def test_public_source_types_signatures_and_exports():
    namespace = {}
    exec("from formualizer import *", namespace)
    for name in [
        "SourceRecalculateLimits",
        "SourceCalculationPolicy",
        "SourceRecalculateResult",
        "SourceOperationDiagnostics",
        "SourceUnavailableOperation",
        "SourceDependencyEvidence",
    ]:
        assert name in fz.__all__
        assert namespace[name] is getattr(fz, name)
    assert set(fz.SourceRecalculateLimits.__annotations__) == set(LIMIT_FIELDS)
    for function in [fz.recalculate_xlsx_bytes, fz.recalculate_xlsx_file]:
        signature = inspect.signature(function)
        assert signature.parameters["limits"].default is None
        assert signature.parameters["physical_telemetry"].default is False
