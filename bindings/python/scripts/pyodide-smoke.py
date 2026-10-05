import json
import sys

import formualizer as fz

assert sys.platform == "emscripten", sys.platform

ast = fz.parse("=SUM(A1:A2)")
assert "SUM" in ast.to_formula()


def expected_error(shape):
    if shape in {"power", "arithmetic", "postfix"}:
        return "Formula AST height limit exceeded"
    return "Formula nesting too deep (max 72)"


def accepted_formulas():
    return [
        ("parentheses", f"={'(' * 64}1{')' * 64}"),
        ("sum", f"={'SUM(' * 64}1{')' * 64}"),
        ("flat-height", f"={'A1+' * 255}A1"),
        ("postfix-height", f"=1{'%' * 255}"),
        ("power-height", f"={'1^' * 255}1"),
        ("if", f"={'IF(A1>0,' * 64}1{',0)' * 64}"),
    ]


def hostile_formulas():
    return [
        ("parentheses", f"={'(' * 1000}1{')' * 1000}"),
        ("unary", f"={'-' * 1000}1"),
        ("sum", f"={'SUM(' * 1000}1{')' * 1000}"),
        ("right-infix", f"={'1+(' * 1000}1{')' * 1000}"),
        ("if", f"={'IF(A1>0,' * 1000}1{',0)' * 1000}"),
        ("arrays", f"={'{' * 1000}1{'}' * 1000}"),
        ("power", f"={'1^' * 1000}1"),
        ("arithmetic", f"={'1+' * 1000}1"),
        ("postfix", f"=1{'%' * 1000}"),
    ]


def assert_depth_error(parse_call, shape):
    try:
        parse_call()
    except fz.ParserError as error:
        assert expected_error(shape) in str(error), str(error)
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
    assert_depth_error(lambda formula=formula: fz.parse(formula), _shape)

exercise_ast(fz.parse("=A1+1"))

parser = fz.Parser()
for _shape, formula in accepted_formulas():
    exercise_ast(parser.parse_string(formula))
for _shape, formula in hostile_formulas():
    assert_depth_error(lambda formula=formula: parser.parse_string(formula), _shape)
exercise_ast(parser.parse_string("=A1+1"))

token_parser = fz.Parser()
accepted_tokens = fz.tokenize(accepted_formulas()[1][1])
exercise_ast(token_parser.parse_tokens(accepted_tokens))
for _shape, formula in hostile_formulas():
    assert_depth_error(
        lambda formula=formula: token_parser.parse_tokens(fz.tokenize(formula)), _shape
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

from_calamine = fz.Workbook.from_bytes(xlsx_bytes, backend="calamine")
assert from_calamine.evaluate_cell("Sheet1", 1, 2) == 42.0


def source_xlsx(formula, cache):
    """A minimal source package with A1=20, A2=22 and B1=<formula>."""
    import io
    import zipfile

    main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rels = "http://schemas.openxmlformats.org/package/2006/relationships"
    office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    parts = {
        "[Content_Types].xml": (
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            "</Types>"
        ),
        "_rels/.rels": (
            f'<Relationships xmlns="{rels}"><Relationship Id="rId1" '
            f'Type="{office}/officeDocument" Target="xl/workbook.xml"/></Relationships>'
        ),
        "xl/workbook.xml": (
            f'<workbook xmlns="{main}" xmlns:r="{office}"><sheets>'
            '<sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'
        ),
        "xl/_rels/workbook.xml.rels": (
            f'<Relationships xmlns="{rels}"><Relationship Id="rId1" '
            f'Type="{office}/worksheet" Target="worksheets/sheet1.xml"/></Relationships>'
        ),
        "xl/worksheets/sheet1.xml": (
            f'<worksheet xmlns="{main}"><sheetData>'
            f'<row r="1"><c r="A1"><v>20</v></c><c r="B1"><f>{formula}</f><v>{cache}</v></c></row>'
            '<row r="2"><c r="A2"><v>22</v></c></row>'
            "</sheetData></worksheet>"
        ),
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name, body in parts.items():
            archive.writestr(name, body)
    return out.getvalue()


def cached_b1(data):
    import io
    import re
    import zipfile

    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        xml = archive.read("xl/worksheets/sheet1.xml").decode()
    return re.search(r'<c r="B1"[^>]*><f>[^<]*</f><v>([^<]*)</v>', xml).group(1)


recalc = fz.recalculate_xlsx_bytes(source_xlsx("SUM(A1:A2)", 0))
assert cached_b1(recalc["bytes"]) == "42", cached_b1(recalc["bytes"])
assert recalc["cache_cells_changed"] == 1
assert recalc["clock"]["now"] is None
current = fz.recalculate_xlsx_bytes(recalc["bytes"])
assert current["bytes"] == recalc["bytes"] and current["cache_cells_changed"] == 0

unknown = fz.recalculate_xlsx_bytes(source_xlsx("SPDVOL(A1)", 0))
assert unknown["summary"]["unknown_functions"] == [{"name": "SPDVOL", "cells": 1}]
assert unknown["summary"]["error_summary"]["#NAME?"]["messages"] == [
    "Unknown function: SPDVOL"
]

# No system clock in Pyodide: TODAY()/NOW() need a fixed instant.
try:
    fz.recalculate_xlsx_bytes(source_xlsx("TODAY()", 0))
except OSError as error:
    assert "TODAY/NOW need a wall clock" in str(error), str(error)
else:
    raise AssertionError("Pyodide must refuse TODAY() without a fixed timestamp")
import datetime

fixed = fz.recalculate_xlsx_bytes(
    source_xlsx("TODAY()", 0),
    deterministic_timestamp_utc=datetime.datetime(2026, 1, 31, 9, tzinfo=datetime.timezone.utc),
)
assert cached_b1(fixed["bytes"]) == "46053", cached_b1(fixed["bytes"])
assert fixed["clock"]["fixed"] is True

summary = {
    "ast_formula": ast.to_formula(),
    "default_parallel": default_plan.parallel_enabled,
    "install_method": globals().get("FORMUALIZER_INSTALL_METHOD", "unknown"),
    "platform": sys.platform,
    "recalc_b1": cached_b1(recalc["bytes"]),
    "wheel_bytes": len(xlsx_bytes),
}

json.dumps(summary, sort_keys=True)
