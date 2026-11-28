"""The observable contract of `Workbook.cancel()` (#417).

A cancel request stops exactly one blocking evaluation: the one that is
running, or, if none is, the next one. That evaluation raises
`ExcelEvaluationError` with `kind == "Cancelled"`, cells that had not finished
keep their previous values, and the request is consumed when the evaluation
returns. `reset_cancel()` withdraws a pending request.

Every test fails if `cancel()` is a no-op.
"""

import threading
import time

import pytest

import formualizer as fz

# 22,500 lookups over 22,500 cells inside one MATCH call: many seconds
# uncancelled, so only an in-function stop lets the test finish promptly.
LONG_FORMULA = "=SUM(MATCH(SEQUENCE(150,150),SEQUENCE(150,150)))"
PROMPT_SECONDS = 30.0


def chain_workbook():
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(1, 1, 10)
    s.set_formula(1, 2, "=A1*2")
    s.set_formula(1, 3, "=B1+1")
    return wb


def assert_cancelled(excinfo):
    assert excinfo.value.kind == "Cancelled", excinfo.value


def test_pending_cancel_stops_the_next_evaluation_and_is_consumed_by_it():
    wb = chain_workbook()
    wb.evaluate_all()
    assert wb.get_value("S", 1, 3) == 21.0

    wb.set_value("S", 1, 1, 100)
    wb.cancel()
    with pytest.raises(fz.ExcelEvaluationError) as excinfo:
        wb.evaluate_all()
    assert_cancelled(excinfo)
    # Nothing was recomputed: the dependents keep their previous values.
    assert wb.get_value("S", 1, 2) == 20.0
    assert wb.get_value("S", 1, 3) == 21.0

    # The request was consumed; the next evaluation runs and catches up.
    wb.evaluate_all()
    assert wb.get_value("S", 1, 2) == 200.0
    assert wb.get_value("S", 1, 3) == 201.0


def test_reset_cancel_withdraws_a_pending_request():
    wb = chain_workbook()
    wb.cancel()
    wb.reset_cancel()
    wb.evaluate_all()
    assert wb.get_value("S", 1, 3) == 21.0


@pytest.mark.parametrize(
    "evaluate",
    [
        lambda wb: wb.evaluate_cell("S", 1, 3),
        lambda wb: wb.evaluate_cells([("S", 1, 3)]),
    ],
    ids=["evaluate_cell", "evaluate_cells"],
)
def test_targeted_evaluation_follows_the_same_contract(evaluate):
    wb = chain_workbook()
    wb.cancel()
    with pytest.raises(fz.ExcelEvaluationError) as excinfo:
        evaluate(wb)
    assert_cancelled(excinfo)
    assert wb.get_value("S", 1, 3) is None
    assert wb.evaluate_cell("S", 1, 3) == 21.0


def test_cancel_from_a_custom_function_stops_the_running_evaluation():
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(1, 1, 1)
    s.set_formula(1, 2, "=PYSTOP(A1)")
    # A chain downstream of the callback: none of it may be computed.
    for col in range(3, 40):
        s.set_formula(1, col, f"={fz_col(col - 1)}1+1")

    state = {"cancel": True, "calls": 0}

    def stop(value):
        state["calls"] += 1
        if state["cancel"]:
            wb.cancel()
        return value

    wb.register_function("pystop", stop, min_args=1, max_args=1)

    with pytest.raises(fz.ExcelEvaluationError) as excinfo:
        wb.evaluate_all()
    assert_cancelled(excinfo)
    assert state["calls"] == 1
    # The cell whose function saw the cancellation is not committed, and
    # nothing after it ran.
    assert wb.get_value("S", 1, 2) is None
    assert wb.get_value("S", 1, 39) is None

    # Consumed: the same workbook evaluates fully on the next call.
    state["cancel"] = False
    wb.evaluate_all()
    assert wb.get_value("S", 1, 2) == 1.0
    assert wb.get_value("S", 1, 39) == 38.0


def test_cancel_from_another_thread_stops_a_long_running_function():
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_formula(1, 1, LONG_FORMULA)
    s.set_formula(1, 2, "=1+1")

    timer = threading.Timer(0.2, wb.cancel)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(fz.ExcelEvaluationError) as excinfo:
            wb.evaluate_all()
    finally:
        timer.cancel()
        timer.join()
    elapsed = time.monotonic() - started
    assert_cancelled(excinfo)
    assert elapsed < PROMPT_SECONDS, f"cancel took {elapsed:.1f}s to stop MATCH"
    assert wb.get_value("S", 1, 1) is None

    # The workbook is usable afterwards and the request does not linger.
    s.set_formula(1, 1, "=40+2")
    wb.evaluate_all()
    assert wb.get_value("S", 1, 1) == 42.0


def test_cancelling_one_workbook_does_not_affect_another():
    wb1 = chain_workbook()
    wb2 = chain_workbook()
    wb1.cancel()
    wb2.evaluate_all()
    assert wb2.get_value("S", 1, 3) == 21.0
    with pytest.raises(fz.ExcelEvaluationError) as excinfo:
        wb1.evaluate_all()
    assert_cancelled(excinfo)


def fz_col(index):
    """1-based column index to letters (1 -> A, 27 -> AA)."""
    letters = ""
    while index:
        index, rem = divmod(index - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters
