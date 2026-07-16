import textwrap

import pytest

from formualizer import (
    EvaluationConfig,
    ExcelEvaluationError,
    SheetPortSession,
    Workbook,
    WorkbookConfig,
)


SHEETPORT_MANIFEST = textwrap.dedent(
    """
    spec: fio
    spec_version: "0.3.0"
    manifest:
      id: resource-error-test
      name: Resource Error Test
      workbook:
        uri: memory://resource-error.xlsx
        locale: en-US
        date_system: 1900
    ports:
      - id: result
        dir: out
        shape: scalar
        location:
          a1: Outputs!A1
        schema:
          type: number
    """
)


def resource_workbook(*, max_work_units=None, max_eval_time_ms=None):
    eval_config = EvaluationConfig()
    eval_config.max_work_units = max_work_units
    eval_config.max_eval_time_ms = max_eval_time_ms
    workbook = Workbook(config=WorkbookConfig(eval_config=eval_config))
    workbook.add_sheet("Outputs")
    workbook.set_formula("Outputs", 1, 1, "=1+1")
    return workbook


def assert_resource_error(error, reason, limit):
    assert type(error) is ExcelEvaluationError
    assert error.kind == "NImpl"
    assert error.excel_kind == "NImpl"
    assert error.message.startswith("evaluation resource exhausted")
    assert error.context is None
    assert error.resource_reason == reason
    assert error.limit == limit
    assert error.observed >= error.limit
    assert isinstance(error.request_id, int)
    assert error.extra == {
        "resource_reason": reason,
        "limit": error.limit,
        "observed": error.observed,
        "request_id": error.request_id,
    }


def test_work_and_deadline_errors_keep_typed_resource_fields():
    with pytest.raises(ExcelEvaluationError) as work:
        resource_workbook(max_work_units=0).evaluate_all()
    assert_resource_error(work.value, "work_units", 0)

    with pytest.raises(ExcelEvaluationError) as deadline:
        resource_workbook(max_eval_time_ms=0).evaluate_all()
    assert_resource_error(deadline.value, "deadline", 0)


def test_sheetport_nested_workbook_engine_error_keeps_typed_fields():
    workbook = resource_workbook(max_work_units=0)
    session = SheetPortSession.from_manifest_yaml(SHEETPORT_MANIFEST, workbook)

    with pytest.raises(ExcelEvaluationError) as failure:
        session.evaluate_once()
    assert_resource_error(failure.value, "work_units", 0)
