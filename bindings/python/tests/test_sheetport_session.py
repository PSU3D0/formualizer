import textwrap

import pytest

from formualizer import (
    SheetPortConstraintError,
    SheetPortSession,
    Workbook,
)

MANIFEST_YAML = textwrap.dedent(
    """
    spec: fio
    spec_version: "0.3.0"
    manifest:
      id: python-sheetport-tests
      name: Python SheetPort Session Tests
      workbook:
        uri: memory://python-sheetport.xlsx
        locale: en-US
        date_system: 1900
    ports:
      - id: demand
        dir: in
        shape: scalar
        location:
          a1: Inputs!A1
        schema:
          type: number
      - id: mix
        dir: in
        shape: record
        location:
          a1: Inputs!B1:C1
        schema:
          kind: record
          fields:
            qty:
              type: integer
              location:
                a1: Inputs!B1
              constraints:
                min: 0
            label:
              type: string
              location:
                a1: Inputs!C1
        default:
          qty: 1
          label: seed
      - id: plan_output
        dir: out
        shape: scalar
        location:
          a1: Outputs!A1
        schema:
          type: number
    """
)


def build_workbook() -> Workbook:
    wb = Workbook()
    wb.add_sheet("Inputs")
    wb.add_sheet("Outputs")
    wb.set_value("Inputs", 1, 1, 120)
    wb.set_value("Inputs", 1, 2, 3)
    wb.set_value("Inputs", 1, 3, "seed")
    wb.set_value("Outputs", 1, 1, 42)
    return wb


def test_sheetport_session_read_write_roundtrip():
    wb = build_workbook()
    session = SheetPortSession.from_manifest_yaml(MANIFEST_YAML, wb)

    manifest = session.manifest
    assert manifest["spec"] == "fio"
    assert len(manifest["ports"]) == 3

    ports = session.describe_ports()
    assert {port["id"] for port in ports} == {"demand", "mix", "plan_output"}

    inputs = session.read_inputs()
    assert inputs["demand"] == pytest.approx(120)
    assert inputs["mix"]["qty"] == 3
    assert inputs["mix"]["label"] == "seed"

    outputs = session.read_outputs()
    assert outputs["plan_output"] == pytest.approx(42)

    session.write_inputs({"demand": 250.5, "mix": {"qty": 7}})
    refreshed = session.read_inputs()
    assert refreshed["demand"] == pytest.approx(250.5)
    assert refreshed["mix"]["qty"] == 7
    # Unspecified record field should remain unchanged
    assert refreshed["mix"]["label"] == "seed"

    # Workbook values are updated in place
    updated_value = wb.get_value("Inputs", 1, 1)
    assert updated_value == pytest.approx(250.5)

    evaluated = session.evaluate_once()
    assert evaluated["plan_output"] == pytest.approx(42)


def test_write_inputs_enforces_constraints():
    wb = build_workbook()
    session = SheetPortSession.from_manifest_yaml(MANIFEST_YAML, wb)

    with pytest.raises(SheetPortConstraintError) as excinfo:
        session.write_inputs({"mix": {"qty": -4}})

    details = excinfo.value.args[1]
    assert details[0]["port"] == "mix"
    assert "min" in details[0]["message"].lower()
