"""Cycle telemetry surface on Workbook (RFC #113, spec §10).

`Workbook.last_cycle_telemetry()` mirrors the engine accessor of the same
name: per-recalc counters from runtime SCC / iterative-calculation
evaluation, reset at the start of every evaluation request.
"""

import pytest

import formualizer as fz


def _iterate_workbook(max_iterations=100, max_change=0.001):
    cfg = fz.EvaluationConfig()
    cfg.cycle_policy = "iterate"
    cfg.iterate_max_iterations = max_iterations
    cfg.iterate_max_change = max_change
    return fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))


def test_telemetry_zero_before_any_evaluation():
    wb = fz.Workbook()
    t = wb.last_cycle_telemetry()
    assert t.static_sccs == 0
    assert t.iterated_sccs == 0
    assert t.converged_sccs == 0
    assert t.capped_sccs == 0
    assert t.settle_passes_total == 0
    assert t.max_abs_delta_at_stop == 0.0
    assert t.nan_converged == 0


def test_convergent_pair_reports_converged_scc():
    # Convergent arithmetic cycle (spec §7.4): B1 = 0.5*A1 + 0.5*C1,
    # C1 = 0.5*B1 + 0.5*D1; A1=10, D1=20 -> B1=40/3, C1=50/3.
    wb = _iterate_workbook()
    wb.add_sheet("S")
    wb.set_value("S", 1, 1, 10)  # A1
    wb.set_value("S", 1, 4, 20)  # D1
    wb.set_formula("S", 1, 2, "=0.5*A1 + 0.5*C1")  # B1
    wb.set_formula("S", 1, 3, "=0.5*B1 + 0.5*D1")  # C1
    wb.evaluate_all()

    assert wb.get_value("S", 1, 2) == pytest.approx(40.0 / 3.0, abs=0.01)

    t = wb.last_cycle_telemetry()
    assert t.iterated_sccs == 1
    assert t.converged_sccs == 1
    assert t.capped_sccs == 0
    assert t.live_cycles_witnessed >= 1
    assert t.settle_passes_total >= 2
    assert t.max_passes_single_scc >= 2
    # The final-pass delta passed the convergence test.
    assert 0.0 <= t.max_abs_delta_at_stop < 0.001
    assert t.elapsed_ms >= 0


def test_divergent_accumulator_reports_capped_scc():
    # A1 = A1 + 1 never converges (delta is always 1): the SCC stops at the
    # Excel max_iterations cap, keeping the last value (NOT an error).
    wb = _iterate_workbook(max_iterations=25, max_change=0.001)
    wb.add_sheet("S")
    wb.set_formula("S", 1, 1, "=A1+1")
    wb.evaluate_all()

    assert wb.get_value("S", 1, 1) == pytest.approx(25.0)

    t = wb.last_cycle_telemetry()
    assert t.iterated_sccs == 1
    assert t.converged_sccs == 0
    assert t.capped_sccs == 1
    assert t.max_passes_single_scc == 25
    assert t.max_abs_delta_at_stop == pytest.approx(1.0)


def test_telemetry_resets_per_evaluation_request():
    wb = _iterate_workbook(max_iterations=25, max_change=0.001)
    wb.add_sheet("S")
    wb.set_formula("S", 1, 1, "=A1+1")
    wb.evaluate_all()
    assert wb.last_cycle_telemetry().capped_sccs == 1

    # An acyclic-only request leaves no cycle counters behind.
    wb.set_value("S", 1, 1, 0)
    wb.set_formula("S", 2, 1, "=A1+1")
    wb.evaluate_all()
    t = wb.last_cycle_telemetry()
    assert t.iterated_sccs == 0
    assert t.capped_sccs == 0


def test_telemetry_repr_is_informative():
    wb = fz.Workbook()
    r = repr(wb.last_cycle_telemetry())
    assert r.startswith("CycleTelemetry(")
    assert "converged_sccs=0" in r
