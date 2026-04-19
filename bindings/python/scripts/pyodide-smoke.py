import json
import sys

import formualizer as fz

assert sys.platform == "emscripten", sys.platform

ast = fz.parse("=SUM(A1:A2)")
assert "SUM" in ast.to_formula()

cfg = fz.EvaluationConfig()
assert cfg.enable_parallel is False
cfg.enable_parallel = True
assert cfg.enable_parallel is True

wb_plan = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
wb_plan.add_sheet("Sheet1")
wb_plan.set_value("Sheet1", 1, 1, 20)
wb_plan.set_value("Sheet1", 2, 1, 22)
wb_plan.set_formula("Sheet1", 1, 2, "=SUM(A1:A2)")
default_plan = wb_plan.get_eval_plan([("Sheet1", 1, 2)])
assert default_plan.parallel_enabled is False

wb = fz.Workbook()
wb.add_sheet("Sheet1")
wb.set_value("Sheet1", 1, 1, 20)
wb.set_value("Sheet1", 2, 1, 22)
wb.set_formula("Sheet1", 1, 2, "=SUM(A1:A2)")
assert wb.evaluate_cell("Sheet1", 1, 2) == 42.0

wb.register_function("py_add", lambda a, b: a + b, min_args=2, max_args=2)
wb.set_formula("Sheet1", 2, 2, "=PY_ADD(A1,A2)")
assert wb.evaluate_cell("Sheet1", 2, 2) == 42

wb_override = fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))
wb_override.add_sheet("Sheet1")
wb_override.set_value("Sheet1", 1, 1, 1)
wb_override.set_value("Sheet1", 2, 1, 2)
wb_override.set_formula("Sheet1", 1, 2, "=SUM(A1:A2)")
assert wb_override.evaluate_cell("Sheet1", 1, 2) == 3.0

xlsx_bytes = wb.to_xlsx_bytes()
assert isinstance(xlsx_bytes, bytes)
assert len(xlsx_bytes) > 100

from_bytes = fz.Workbook.from_bytes(xlsx_bytes, backend="umya")
assert from_bytes.evaluate_cell("Sheet1", 1, 2) == 42.0

from_top_level = fz.load_workbook_bytes(xlsx_bytes)
assert from_top_level.evaluate_cell("Sheet1", 1, 2) == 42.0

summary = {
    "ast_formula": ast.to_formula(),
    "default_parallel": default_plan.parallel_enabled,
    "install_method": globals().get("FORMUALIZER_INSTALL_METHOD", "unknown"),
    "platform": sys.platform,
    "wheel_bytes": len(xlsx_bytes),
}

json.dumps(summary, sort_keys=True)
