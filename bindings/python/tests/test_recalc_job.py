"""Behavioral regressions for the engine-owned Python job."""
import pytest
import formualizer as fz


def chain(n=30):
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_value("S", 1, 1, 1)
    for row in range(2, n + 1):
        wb.set_formula("S", row, 1, f"=A{row - 1}+1")
    return wb


def test_zero_budget_yields_installed_work_before_completion():
    wb = chain()
    job = wb.begin_recalc()
    reports = list(job.iter(budget_ms=0))
    assert len(reports) > 3
    assert any(r.phase == "Evaluating" for r in reports)
    assert reports[-1].phase == "Finished"
    assert wb.get_value("S", 30, 1) == 30


def test_precancelled_step_keeps_unfinished_work_dirty():
    wb = chain()
    job = wb.begin_recalc()
    token = fz.CancelToken()
    token.cancel()
    report = job.step(cancel=token)
    assert report.phase == "Cancelled"
    assert not job.active
    assert wb.has_pending_in_rect("S", 2, 1, 30, 1)
    wb.evaluate_all()
    assert wb.get_value("S", 30, 1) == 30
