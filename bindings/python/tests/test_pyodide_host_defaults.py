import sys

import formualizer as fz


def _plan_parallel_enabled(workbook: fz.Workbook) -> bool:
    workbook.add_sheet("Sheet1")
    workbook.set_value("Sheet1", 1, 1, 1)
    workbook.set_value("Sheet1", 2, 1, 2)
    workbook.set_formula("Sheet1", 1, 2, "=SUM(A1:A2)")
    workbook.evaluate_cell("Sheet1", 1, 2)
    plan = workbook.get_eval_plan([("Sheet1", 1, 2)])
    return plan.parallel_enabled


def test_evaluation_config_default_matches_host():
    cfg = fz.EvaluationConfig()
    assert cfg.enable_parallel is (sys.platform != "emscripten")


def test_workbook_constructor_applies_host_parallel_policy():
    workbook = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
    assert _plan_parallel_enabled(workbook) is (sys.platform != "emscripten")


def test_explicit_parallel_override_is_writable():
    cfg = fz.EvaluationConfig()
    cfg.enable_parallel = not cfg.enable_parallel
    assert cfg.enable_parallel is (sys.platform == "emscripten")
