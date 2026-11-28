"""A request stopped inside a function leaves nothing behind (#619, #621).

The engine-level behaviour is tested in Rust
(`engine::tests::in_function_limits`). These cover the paths an independent
review found leaking: an iterative cycle, an unmetered call after a failed
request, and a long DDB schedule.
"""

import pytest

import formualizer as fz

# 12 lookups over 100 cells: 1,200 units of in-function work.
OVER_BUDGET = "IFERROR(SUM(MATCH(SEQUENCE(12),SEQUENCE(100))),987)"


def budgeted_workbook(max_work_units, *, iterate=False):
    cfg = fz.EvaluationConfig()
    cfg.max_work_units = max_work_units
    if iterate:
        cfg.cycle_policy = "iterate"
    return fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))


def assert_work_exhausted(excinfo):
    assert "work_units" in str(excinfo.value), excinfo.value


@pytest.mark.parametrize("method", ["evaluate_all", "evaluate_cell", "step"])
def test_a_stopped_iterative_cycle_keeps_its_previous_value(method):
    wb = budgeted_workbook(150, iterate=True)
    s = wb.sheet("S")
    s.set_value(1, 2, 0)
    # A1 is a self-referential cycle member; with B1 = 0 it is cheap.
    s.set_formula(1, 1, f"=IF(B1,{OVER_BUDGET},42)+0*A1")
    wb.evaluate_all()
    assert wb.get_value("S", 1, 1) == 42.0

    s.set_value(1, 2, 1)
    with pytest.raises(Exception) as excinfo:
        if method == "evaluate_all":
            wb.evaluate_all()
        elif method == "evaluate_cell":
            wb.evaluate_cell("S", 1, 1)
        else:
            wb.begin_recalc().step()
    assert_work_exhausted(excinfo)
    # Neither the stopped function's #CANCELLED nor IFERROR's 987 is published.
    assert wb.get_value("S", 1, 1) == 42.0


def test_a_failed_request_does_not_fail_later_unmetered_calls():
    wb = budgeted_workbook(100)
    wb.sheet("S").set_formula(1, 1, "=SUM(MATCH(SEQUENCE(10),SEQUENCE(30)))")
    with pytest.raises(fz.ExcelEvaluationError) as excinfo:
        wb.evaluate_all()
    assert_work_exhausted(excinfo)
    # Planning is not a metered request and must not inherit the stop.
    assert wb.get_eval_plan([("S", 1, 1)], build_graph_if_needed=False) is not None
    assert wb.get_eval_plan([("S", 1, 1)], build_graph_if_needed=True) is not None


def test_ddb_long_schedule_does_not_underflow_before_scaling_by_cost():
    wb = fz.Workbook()
    wb.sheet("S").set_formula(1, 1, "=DDB(1E308,0,2000000,1048577,1600)")
    value = wb.evaluate_cell("S", 1, 1)
    # The per-period recurrence gives 2.784009088735201e-60.
    assert value == pytest.approx(2.784009088735201e-60, rel=1e-9)
