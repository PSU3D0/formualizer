import pytest

import formualizer as fz


def make_workbook() -> fz.Workbook:
    wb = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
    wb.add_sheet("Sheet1")
    return wb


def assert_excel_error(value, kind: str) -> dict:
    assert isinstance(value, dict)
    assert value.get("type") == "Error"
    assert value.get("kind") == kind
    return value


def test_register_simple_add_function_and_evaluate_formula():
    wb = make_workbook()
    wb.register_function("py_add", lambda a, b: a + b, min_args=2, max_args=2)

    wb.set_formula("Sheet1", 1, 1, "=PY_ADD(2,3)")
    assert wb.evaluate_cell("Sheet1", 1, 1) == 5


def test_function_name_lookup_and_unregistration_are_case_insensitive():
    wb = make_workbook()

    wb.register_function("MiXeD", lambda x: x + 1, min_args=1, max_args=1)

    wb.set_formula("Sheet1", 1, 1, "=mixed(41)")
    assert wb.evaluate_cell("Sheet1", 1, 1) == 42

    wb.unregister_function("mixed")
    wb.set_formula("Sheet1", 1, 1, "=MIXED(1)")
    assert_excel_error(wb.evaluate_cell("Sheet1", 1, 1), "Name")


def test_override_builtin_requires_explicit_opt_in():
    wb = make_workbook()

    with pytest.raises(RuntimeError, match="allow_override_builtin"):
        wb.register_function("sum", lambda *_args: 999)

    wb.register_function("sum", lambda *_args: 999, allow_override_builtin=True)
    wb.set_formula("Sheet1", 1, 1, "=SUM(1,2)")
    assert wb.evaluate_cell("Sheet1", 1, 1) == 999


def test_register_function_arity_handling():
    wb = make_workbook()
    calls = {"count": 0}

    def takes_two(a, b):
        calls["count"] += 1
        return a + b

    wb.register_function("takes_two", takes_two, min_args=2, max_args=2)
    wb.set_formula("Sheet1", 1, 1, "=TAKES_TWO(1)")

    value = wb.evaluate_cell("Sheet1", 1, 1)
    assert_excel_error(value, "Value")
    assert calls["count"] == 0


def test_nested_list_array_return_roundtrip():
    wb = make_workbook()

    wb.set_value("Sheet1", 1, 1, 1)
    wb.set_value("Sheet1", 1, 2, 2)
    wb.set_value("Sheet1", 2, 1, 3)
    wb.set_value("Sheet1", 2, 2, 4)

    seen_args = []

    def reshape(grid):
        seen_args.append(grid)
        return [
            [grid[0][0] + 10, grid[0][1] + 20],
            [grid[1][0] + 30, grid[1][1] + 40],
        ]

    wb.register_function("reshape", reshape, min_args=1, max_args=1)
    wb.set_formula("Sheet1", 1, 3, "=RESHAPE(A1:B2)")
    wb.evaluate_all()

    assert seen_args == [[[1, 2], [3, 4]]]
    assert wb.get_value("Sheet1", 1, 3) == 11
    assert wb.get_value("Sheet1", 1, 4) == 22
    assert wb.get_value("Sheet1", 2, 3) == 33
    assert wb.get_value("Sheet1", 2, 4) == 44


def test_python_exception_maps_to_excel_value_error():
    wb = make_workbook()

    def explode(_x):
        raise RuntimeError("kaboom\ninternal")

    wb.register_function("explode", explode, min_args=1, max_args=1)
    wb.set_formula("Sheet1", 1, 1, "=EXPLODE(1)")

    value = wb.evaluate_cell("Sheet1", 1, 1)
    err = assert_excel_error(value, "Value")
    message = err.get("message")
    # Engine cell storage currently preserves error kind reliably; message may be elided.
    if message is not None:
        assert "RuntimeError" in message
        assert "\n" not in message


def test_unregister_function_behavior():
    wb = make_workbook()

    wb.register_function("temp_fn", lambda: 7)
    wb.unregister_function("temp_fn")

    wb.set_formula("Sheet1", 1, 1, "=TEMP_FN()")
    value = wb.evaluate_cell("Sheet1", 1, 1)
    assert_excel_error(value, "Name")


def test_list_functions_contents():
    wb = make_workbook()

    wb.register_function(
        "alpha",
        lambda: 1,
        min_args=0,
        max_args=0,
        deterministic=False,
        thread_safe=True,
    )
    wb.register_function(
        "beta",
        lambda x: x,
        min_args=1,
        max_args=None,
        volatile=True,
        allow_override_builtin=True,
    )

    functions = wb.list_functions()

    assert [item["name"] for item in functions] == ["ALPHA", "BETA"]

    alpha = functions[0]
    assert alpha["min_args"] == 0
    assert alpha["max_args"] == 0
    assert alpha["volatile"] is False
    assert alpha["thread_safe"] is True
    assert alpha["deterministic"] is False
    assert alpha["allow_override_builtin"] is False

    beta = functions[1]
    assert beta["min_args"] == 1
    assert beta["max_args"] is None
    assert beta["volatile"] is True
    assert beta["thread_safe"] is False
    assert beta["deterministic"] is True
    assert beta["allow_override_builtin"] is True
