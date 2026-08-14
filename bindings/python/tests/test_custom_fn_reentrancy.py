"""A custom-function callback must not be able to hang the interpreter.

`Workbook` keeps its engine state behind a non-reentrant `RwLock`, held for the
whole evaluation. Before this change, *any* workbook call made from inside a
Python custom function blocked forever on that lock — `get_value` on an
uncached cell, `set_value`, `sheet_names`, a nested `evaluate_*`, and so on.
The failure was also non-deterministic: `get_value` served from the legacy
compatibility cache returned normally, so the same code "worked" on an
in-memory workbook and hung on a loaded one.

Those calls now raise `RuntimeError` immediately. `cancel()` / `reset_cancel()`
stay usable because they only flip an atomic flag.

Every test here is timeout-bounded: without the guard these hang rather than
fail, and an unbounded deadlock test burns a CI runner instead of reporting.
"""

import pytest

import formualizer as fz

# Wide enough to push the callback onto rayon workers as well as the
# serial path, so the guard is exercised off-thread too.
_NUM_FORMULAS = 64


def build_workbook():
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_value("S", 1, 1, 1.0)
    wb.set_formula("S", 1, 2, "=B2+1")  # a formula cell, never in the cache
    for row in range(1, _NUM_FORMULAS + 1):
        wb.set_formula("S", row, 3, "=PYPROBE(A1)")
    return wb


def run_probe(operation):
    """Register `operation` as a custom function body and evaluate."""
    wb = build_workbook()
    captured = {}

    def probe(_value):
        if "result" not in captured:
            try:
                operation(wb)
                captured["result"] = None
            except BaseException as exc:
                captured["result"] = exc
        return 1.0

    wb.register_function("pyprobe", probe, min_args=1, max_args=1)
    wb.evaluate_all()
    return captured.get("result", KeyError("callback never ran"))


REENTRANT_OPERATIONS = {
    "get_value_formula_cell": lambda wb: wb.get_value("S", 1, 2),
    "set_value": lambda wb: wb.set_value("S", 50, 50, 1.0),
    "set_formula": lambda wb: wb.set_formula("S", 51, 51, "=1+1"),
    "sheet_names": lambda wb: wb.sheet_names,
    "evaluate_cell": lambda wb: wb.evaluate_cell("S", 1, 2),
    "evaluate_all": lambda wb: wb.evaluate_all(),
    "evaluate_cells": lambda wb: wb.evaluate_cells([("S", 1, 2)]),
    "list_functions": lambda wb: wb.list_functions(),
    "register_function": lambda wb: wb.register_function("other", lambda: 1.0),
    "tables": lambda wb: wb.tables(),
    "add_sheet": lambda wb: wb.add_sheet("Another"),
    "sheet_handle": lambda wb: wb.sheet("S"),
    "last_cycle_telemetry": lambda wb: wb.last_cycle_telemetry(),
    "get_eval_plan": lambda wb: wb.get_eval_plan([("S", 1, 2)]),
    "to_xlsx_bytes": lambda wb: wb.to_xlsx_bytes(),
}


@pytest.mark.timeout(60)
@pytest.mark.parametrize("name", sorted(REENTRANT_OPERATIONS))
def test_reentrant_workbook_access_raises_instead_of_hanging(name):
    result = run_probe(REENTRANT_OPERATIONS[name])
    assert isinstance(result, RuntimeError), (
        f"{name} from inside a custom function returned {result!r}; "
        "expected RuntimeError"
    )
    assert "custom function" in str(result)


@pytest.mark.timeout(60)
def test_reentrant_sheet_handle_access_raises():
    """A `Sheet` acquired *before* evaluation shares the same lock."""
    wb = build_workbook()
    sheet = wb.sheet("S")
    captured = {}

    def probe(_value):
        if "result" not in captured:
            try:
                captured["result"] = sheet.get_cell(1, 2)
            except BaseException as exc:
                captured["result"] = exc
        return 1.0

    wb.register_function("pyprobe", probe, min_args=1, max_args=1)
    wb.evaluate_all()
    assert isinstance(captured["result"], RuntimeError)


@pytest.mark.timeout(60)
def test_cancel_is_still_safe_from_a_callback():
    """`cancel`/`reset_cancel` touch only an atomic flag and must keep working."""
    wb = build_workbook()
    seen = []

    def probe(_value):
        wb.reset_cancel()
        wb.cancel()
        seen.append(True)
        return 1.0

    wb.register_function("pyprobe", probe, min_args=1, max_args=1)
    with pytest.raises(Exception) as excinfo:
        wb.evaluate_all()
    assert "CANCELLED" in str(excinfo.value).upper()
    assert seen


@pytest.mark.timeout(60)
def test_a_second_workbook_is_unaffected():
    """The guard is per workbook: a callback may drive a *different* workbook."""
    wb = build_workbook()
    other = fz.Workbook()
    other.add_sheet("T")
    other.set_value("T", 1, 1, 20.0)
    other.set_formula("T", 1, 2, "=A1*2")
    captured = {}

    def probe(_value):
        if "result" not in captured:
            captured["result"] = other.evaluate_cell("T", 1, 2)
        return 1.0

    wb.register_function("pyprobe", probe, min_args=1, max_args=1)
    wb.evaluate_all()
    assert captured["result"] == pytest.approx(40.0)


@pytest.mark.timeout(60)
def test_workbook_is_usable_again_after_the_callback_returns():
    """The marker must be cleared on the way out, including on exceptions."""
    wb = build_workbook()

    def probe(_value):
        try:
            _ = wb.sheet_names
        except RuntimeError:
            pass
        return 1.0

    wb.register_function("pyprobe", probe, min_args=1, max_args=1)
    wb.evaluate_all()
    assert "S" in wb.sheet_names
    assert wb.get_value("S", 1, 3) == pytest.approx(1.0)


@pytest.mark.timeout(60)
def test_raising_callback_still_clears_the_marker():
    wb = build_workbook()

    def probe(_value):
        raise ValueError("boom")

    wb.register_function("pyprobe", probe, min_args=1, max_args=1)
    wb.evaluate_all()
    assert "S" in wb.sheet_names
