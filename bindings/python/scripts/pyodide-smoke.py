import json
import sys

import formualizer as fz

assert sys.platform == "emscripten", sys.platform

ast = fz.parse("=SUM(A1:A2)")
assert "SUM" in ast.to_formula()

DEPTH_ERROR = "Formula nesting too deep (max 72)"


def accepted_formulas():
    return [
        ("parentheses", f"={'(' * 64}1{')' * 64}"),
        ("sum", f"={'SUM(' * 64}1{')' * 64}"),
        ("if", f"={'IF(A1>0,' * 64}1{',0)' * 64}"),
    ]


def hostile_formulas():
    return [
        ("parentheses", f"={'(' * 5000}1{')' * 5000}"),
        ("unary", f"={'-' * 5000}1"),
        ("sum", f"={'SUM(' * 5000}1{')' * 5000}"),
        ("right-infix", f"={'1+(' * 5000}1{')' * 5000}"),
        ("if", f"={'IF(A1>0,' * 5000}1{',0)' * 5000}"),
        ("arrays", f"={'{' * 5000}1{'}' * 5000}"),
        ("power", f"={'1^' * 5000}1"),
    ]


def assert_depth_error(parse_call):
    try:
        parse_call()
    except fz.ParserError as error:
        assert DEPTH_ERROR in str(error), str(error)
    else:
        raise AssertionError("deep formula must raise ParserError")


def exercise_ast(value):
    assert value.to_formula().startswith("=")
    assert value.pretty()
    assert value.to_dict()
    del value


for _shape, formula in accepted_formulas():
    exercise_ast(fz.parse(formula))

for _shape, formula in hostile_formulas():
    assert_depth_error(lambda formula=formula: fz.parse(formula))

exercise_ast(fz.parse("=A1+1"))

parser = fz.Parser()
for _shape, formula in accepted_formulas():
    exercise_ast(parser.parse_string(formula))
for _shape, formula in hostile_formulas():
    assert_depth_error(lambda formula=formula: parser.parse_string(formula))
exercise_ast(parser.parse_string("=A1+1"))

token_parser = fz.Parser()
accepted_tokens = fz.tokenize(accepted_formulas()[1][1])
exercise_ast(token_parser.parse_tokens(accepted_tokens))
for _shape, formula in hostile_formulas():
    assert_depth_error(
        lambda formula=formula: token_parser.parse_tokens(fz.tokenize(formula))
    )
exercise_ast(token_parser.parse_tokens(fz.tokenize("=A1+1")))

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

from_bytes = fz.Workbook.from_bytes(xlsx_bytes)
assert from_bytes.evaluate_cell("Sheet1", 1, 2) == 42.0

from_top_level = fz.load_workbook_bytes(xlsx_bytes, backend="umya")
assert from_top_level.evaluate_cell("Sheet1", 1, 2) == 42.0

try:
    fz.Workbook.from_bytes(xlsx_bytes, backend="calamine")
except NotImplementedError:
    pass
else:
    raise AssertionError("Pyodide must reject unavailable backend='calamine'")

summary = {
    "ast_formula": ast.to_formula(),
    "default_parallel": default_plan.parallel_enabled,
    "install_method": globals().get("FORMUALIZER_INSTALL_METHOD", "unknown"),
    "platform": sys.platform,
    "wheel_bytes": len(xlsx_bytes),
}

json.dumps(summary, sort_keys=True)
