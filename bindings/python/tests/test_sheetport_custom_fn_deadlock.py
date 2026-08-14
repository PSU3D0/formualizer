"""SheetPort must release the GIL while the engine evaluates.

Regression for the same GIL deadlock #327 fixed on ``Workbook.evaluate_*``,
which ``SheetPortSession.evaluate_once`` still had: the calling thread held the
GIL while waiting on the engine's parallel layer, and the rayon workers
evaluating Python-backed custom functions blocked in ``PyGILState_Ensure``
waiting for that same GIL.

SheetPort only evaluates the cone reachable from the output ports, so the
custom-function cells must be *upstream of an output port* and numerous enough
to form a wide parallel layer — a workbook whose custom-function cells are not
reachable from a port evaluates fine even on the broken binding.
"""

import textwrap

import pytest

from formualizer import SheetPortSession, Workbook

_NUM_FORMULAS = 200

MANIFEST_YAML = textwrap.dedent(
    """
    spec: fio
    spec_version: "0.3.0"
    manifest:
      id: python-sheetport-custom-fn
      name: SheetPort custom function deadlock
      workbook:
        uri: memory://sheetport-custom-fn.xlsx
        locale: en-US
        date_system: 1900
    ports:
      - id: scale
        dir: in
        shape: scalar
        location:
          a1: Inputs!A1
        schema:
          type: number
      - id: total
        dir: out
        shape: scalar
        location:
          a1: Outputs!A1
        schema:
          type: number
    """
).strip()


def _triple(x):
    return float(x) * 3.0


def build_session() -> SheetPortSession:
    wb = Workbook()
    wb.add_sheet("Inputs")
    wb.add_sheet("Work")
    wb.add_sheet("Outputs")
    wb.set_value("Inputs", 1, 1, 2.0)

    # One wide layer of custom-function cells, all reachable from the output
    # port through the SUM below.
    for row in range(1, _NUM_FORMULAS + 1):
        wb.set_formula("Work", row, 1, "=PYTRIPLE(Inputs!$A$1)")
    wb.set_formula("Outputs", 1, 1, f"=SUM(Work!A1:A{_NUM_FORMULAS})")

    wb.register_function("pytriple", _triple, min_args=1, max_args=1)
    return SheetPortSession.from_manifest_yaml(MANIFEST_YAML, wb)


@pytest.mark.timeout(60)
def test_sheetport_evaluate_once_with_python_custom_function():
    session = build_session()
    out = session.evaluate_once()
    assert out["total"] == pytest.approx(2.0 * 3.0 * _NUM_FORMULAS)


@pytest.mark.timeout(60)
def test_sheetport_write_inputs_then_evaluate_once():
    session = build_session()
    session.write_inputs({"scale": 5.0})
    out = session.evaluate_once()
    assert out["total"] == pytest.approx(5.0 * 3.0 * _NUM_FORMULAS)

    # A second round-trip must keep working: `with_sheetport` now re-acquires
    # the workbook lock inside the detached region each call.
    session.write_inputs({"scale": 1.0})
    out = session.evaluate_once()
    assert out["total"] == pytest.approx(1.0 * 3.0 * _NUM_FORMULAS)


@pytest.mark.timeout(60)
def test_sheetport_reads_still_see_writes_through_the_detached_path():
    session = build_session()
    session.write_inputs({"scale": 7.0})
    assert session.read_inputs()["scale"] == pytest.approx(7.0)
    assert session.evaluate_once()["total"] == pytest.approx(7.0 * 3.0 * _NUM_FORMULAS)
    assert session.read_outputs()["total"] == pytest.approx(7.0 * 3.0 * _NUM_FORMULAS)
