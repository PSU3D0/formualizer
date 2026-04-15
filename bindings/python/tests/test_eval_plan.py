import pytest

import formualizer as fz


def build_default_workbook() -> fz.Workbook:
    wb = fz.Workbook()
    wb.add_sheet("Sheet1")
    wb.set_value("Sheet1", 1, 1, 123)
    wb.set_formula("Sheet1", 1, 2, "=A1")
    return wb


def test_get_eval_plan_builds_graph_by_default_for_deferred_workbooks():
    wb = build_default_workbook()

    plan = wb.get_eval_plan([("Sheet1", 1, 2)])

    assert plan.total_vertices_to_evaluate >= 1
    assert plan.target_cells == ["Sheet1!B1"]


def test_get_eval_plan_can_disable_implicit_graph_build():
    wb = build_default_workbook()

    with pytest.raises(RuntimeError, match="deferred graph"):
        wb.get_eval_plan([("Sheet1", 1, 2)], build_graph_if_needed=False)
