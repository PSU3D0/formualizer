"""Cycle / iterative-calculation config surface on EvaluationConfig (RFC #113)."""

import pytest

import formualizer as fz


def test_cycle_defaults():
    cfg = fz.EvaluationConfig()
    assert cfg.cycle_detection == "static"
    assert cfg.cycle_policy == "error"
    # Knob getters read Excel defaults even when not iterating.
    assert cfg.iterate_max_iterations == 100
    assert cfg.iterate_max_change == pytest.approx(0.001)


def test_setting_iterate_policy_promotes_detection_to_runtime():
    cfg = fz.EvaluationConfig()
    cfg.cycle_policy = "iterate"
    assert cfg.cycle_policy == "iterate"
    # spec §2: iteration requires runtime detection; the setter auto-promotes it.
    assert cfg.cycle_detection == "runtime"


def test_iterate_knobs_round_trip():
    cfg = fz.EvaluationConfig()
    cfg.iterate_max_iterations = 7
    cfg.iterate_max_change = 0.25
    assert cfg.cycle_policy == "iterate"
    assert cfg.cycle_detection == "runtime"
    assert cfg.iterate_max_iterations == 7
    assert cfg.iterate_max_change == pytest.approx(0.25)


def test_invalid_detection_raises():
    cfg = fz.EvaluationConfig()
    with pytest.raises(ValueError):
        cfg.cycle_detection = "bogus"


def test_invalid_iterate_knobs_raise():
    cfg = fz.EvaluationConfig()
    with pytest.raises(ValueError):
        cfg.iterate_max_iterations = 0
    with pytest.raises(ValueError):
        cfg.iterate_max_change = -1.0


def test_static_with_iterate_policy_rejected():
    cfg = fz.EvaluationConfig()
    cfg.cycle_policy = "iterate"
    with pytest.raises(ValueError):
        cfg.cycle_detection = "static"


def test_iterative_calculation_end_to_end():
    # Convergent arithmetic cycle (spec §7.4): B1 = 0.5*A1 + 0.5*C1,
    # C1 = 0.5*B1 + 0.5*D1; A1=10, D1=20 -> B1=40/3, C1=50/3.
    cfg = fz.EvaluationConfig()
    cfg.cycle_policy = "iterate"
    cfg.iterate_max_iterations = 100
    cfg.iterate_max_change = 0.001

    wb = fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))
    wb.add_sheet("S")
    wb.set_value("S", 1, 1, 10)  # A1
    wb.set_value("S", 1, 4, 20)  # D1
    wb.set_formula("S", 1, 2, "=0.5*A1 + 0.5*C1")  # B1
    wb.set_formula("S", 1, 3, "=0.5*B1 + 0.5*D1")  # C1
    wb.evaluate_all()

    assert wb.get_value("S", 1, 2) == pytest.approx(40.0 / 3.0, abs=0.01)
    assert wb.get_value("S", 1, 3) == pytest.approx(50.0 / 3.0, abs=0.01)
