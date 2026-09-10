"""Tests for undo/redo, begin/end action, and cancel APIs."""

import pytest

import formualizer as fz


class TestUndoRedo:
    """Basic undo/redo on single operations."""

    def test_undo_set_value(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 42)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 42.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) is None

    def test_redo_set_value(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 42)
        wb.evaluate_all()

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) is None

        wb.redo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 42.0

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="#412: formula undo leaves computed value",
    )
    def test_undo_formula_on_fresh_cell(self):
        """Undo formula creation clears C1 without changing its inputs."""
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        s.set_value(1, 2, 20)
        wb.evaluate_all()

        # Set a formula on the previously empty C1.
        s.set_formula(1, 3, "=A1+B1")
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 3) == 30.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 3) is None
        assert wb.get_formula("S1", 1, 3) is None
        assert wb.get_value("S1", 1, 1) == 10.0
        assert wb.get_value("S1", 1, 2) == 20.0

    def test_undo_overwrite_value(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0

        s.set_value(1, 1, 99)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 99.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0

    def test_undo_propagation(self):
        """Undo a value change should also undo dependent recalculations."""
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        s.set_formula(1, 2, "=A1*2")
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 2) == 20.0

        s.set_value(1, 1, 50)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 2) == 100.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0
        assert wb.get_value("S1", 1, 2) == 20.0

    def test_multiple_undos(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 1)
        s.set_value(1, 2, 2)
        s.set_value(1, 3, 3)
        wb.evaluate_all()

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 3) is None
        assert wb.get_value("S1", 1, 1) == 1.0
        assert wb.get_value("S1", 1, 2) == 2.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 2) is None

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) is None

    def test_redo_chain(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        wb.evaluate_all()

        s.set_value(1, 1, 20)
        wb.evaluate_all()
        s.set_value(1, 1, 30)
        wb.evaluate_all()

        wb.undo()
        wb.evaluate_all()
        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0

        wb.redo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 20.0

        wb.redo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 30.0


class TestCompoundActions:
    """Tests for begin_action/end_action grouping."""

    def test_begin_end_groups_into_one_undo(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        wb.begin_action("set two values")
        s.set_value(1, 1, 10)
        s.set_value(1, 2, 20)
        wb.end_action()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0
        assert wb.get_value("S1", 1, 2) == 20.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) is None
        assert wb.get_value("S1", 1, 2) is None

    def test_redo_compound_action(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        wb.begin_action("batch edit")
        s.set_value(1, 1, 100)
        s.set_value(1, 2, 200)
        wb.end_action()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 100.0
        assert wb.get_value("S1", 1, 2) == 200.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) is None
        assert wb.get_value("S1", 1, 2) is None

        wb.redo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 100.0
        assert wb.get_value("S1", 1, 2) == 200.0

    def test_compound_action_with_evaluation(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 1)
        s.set_value(1, 2, 2)
        s.set_formula(1, 3, "=A1+B1")
        wb.evaluate_all()
        wb.begin_action("value batch with dependent formula")
        s.set_value(1, 1, 5)
        s.set_value(1, 2, 10)
        wb.end_action()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 3) == 15.0
        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 1.0
        assert wb.get_value("S1", 1, 2) == 2.0
        assert wb.get_value("S1", 1, 3) == 3.0
        wb.redo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 3) == 15.0

    def test_nested_begin_end_is_single_group(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        wb.begin_action("outer")
        s.set_value(1, 1, 1)
        wb.begin_action("inner")
        s.set_value(1, 2, 2)
        wb.end_action()
        s.set_value(1, 3, 3)
        wb.end_action()
        wb.evaluate_all()

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) is None
        assert wb.get_value("S1", 1, 2) is None
        assert wb.get_value("S1", 1, 3) is None


class TestCancel:
    """Tests for cooperative cancellation."""

    # In-flight cancellation needs the separate #417 contract; calling cancel()
    # synchronously before evaluate_all() does not test cancellation during work.

    def test_cancel_then_reset(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 42)
        s.set_formula(1, 2, "=A1*2")

        wb.cancel()
        wb.reset_cancel()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 2) == 84.0

    def test_other_workbook_evaluates_after_cancel_request(self):
        wb1 = fz.Workbook()
        wb2 = fz.Workbook()
        s1 = wb1.sheet("S1")
        s2 = wb2.sheet("S1")

        s1.set_value(1, 1, 10)
        s2.set_value(1, 1, 20)

        wb1.cancel()
        wb2.evaluate_all()
        assert wb2.get_value("S1", 1, 1) == 20.0
        assert wb1.get_value("S1", 1, 1) == 10.0
        wb1.reset_cancel()
        wb1.evaluate_all()
        assert wb1.get_value("S1", 1, 1) == 10.0


class TestEdgeCases:
    """Edge cases for undo/redo and changelog."""

    def test_undo_without_changes_is_noop(self):
        wb = fz.Workbook()
        names = wb.sheet_names
        assert wb.undo() is None
        assert wb.sheet_names == names

    def test_redo_without_undo_is_noop(self):
        wb = fz.Workbook()
        names = wb.sheet_names
        assert wb.redo() is None
        assert wb.sheet_names == names

    def test_metadata_setters_allow_subsequent_edit(self):
        wb = fz.Workbook()
        wb.set_actor_id("user-123")
        wb.set_correlation_id("corr-456")
        wb.set_reason("user edit")
        s = wb.sheet("S1")
        s.set_value(1, 1, 42)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 42.0

    def test_changelog_toggle_controls_recording(self):
        wb = fz.Workbook()
        wb.set_changelog_enabled(False)
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0

        wb.set_changelog_enabled(True)
        s.set_value(1, 1, 20)
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 20.0
        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0
        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 1) == 10.0

    def test_undo_after_sheet_add_delete(self):
        wb = fz.Workbook()
        wb.add_sheet("Temp")
        s = wb.sheet("Temp")
        s.set_value(1, 1, 99)
        wb.evaluate_all()
        assert wb.get_value("Temp", 1, 1) == 99.0

        wb.undo()
        wb.evaluate_all()
        assert wb.get_value("Temp", 1, 1) is None
        assert "Temp" in wb.sheet_names

    def test_evaluate_cells_after_undo(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        s.set_formula(1, 2, "=A1*3")
        wb.evaluate_all()
        assert wb.get_value("S1", 1, 2) == 30.0

        s.set_value(1, 1, 20)
        assert wb.evaluate_cells([("S1", 1, 2)]) == [60.0]
        wb.undo()
        assert wb.evaluate_cells([("S1", 1, 2)]) == [30.0]
        assert wb.get_value("S1", 1, 1) == 10.0
        assert wb.get_value("S1", 1, 2) == 30.0


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="#301: rewritten empty precedent loses dependency",
)
def test_rewrite_previously_empty_precedent_after_undo():
    wb = fz.Workbook()
    sheet = wb.sheet("S1")
    sheet.set_formula(4, 4, "=C6+1")
    assert wb.evaluate_cell("S1", 4, 4) == 1.0
    sheet.set_value(6, 3, 10)
    assert wb.evaluate_cell("S1", 4, 4) == 11.0
    wb.undo()
    assert wb.evaluate_cell("S1", 4, 4) == 1.0
    sheet.set_value(6, 3, 20)
    assert wb.evaluate_cell("S1", 4, 4) == 21.0


class TestSheetLevelUndoRedo:
    """Undo/redo through the Sheet API."""

    def test_sheet_set_value_undo(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 42)
        wb.evaluate_all()
        assert s.get_cell(1, 1).value == 42.0

        wb.undo()
        wb.evaluate_all()
        assert s.get_cell(1, 1).value is None

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="#412: formula undo leaves computed value",
    )
    def test_sheet_set_formula_undo(self):
        """Sheet-level undo clears the created formula and its computed value."""
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_value(1, 1, 10)
        s.set_value(1, 2, 20)
        s.set_formula(1, 3, "=A1+B1")
        wb.evaluate_all()
        assert s.get_cell(1, 3).value == 30.0

        wb.undo()  # undo formula staging
        wb.evaluate_all()
        assert s.get_cell(1, 1).value == 10.0
        assert s.get_cell(1, 2).value == 20.0
        assert s.get_cell(1, 3).value is None
        assert wb.get_formula("S1", 1, 3) is None

    def test_sheet_batch_set_undo(self):
        wb = fz.Workbook()
        s = wb.sheet("S1")
        s.set_values_batch(1, 1, 2, 2, [[1, 2], [3, 4]])
        wb.evaluate_all()
        assert s.get_cell(1, 1).value == 1.0
        assert s.get_cell(2, 2).value == 4.0

        wb.undo()
        wb.evaluate_all()
        assert s.get_cell(1, 1).value is None
        assert s.get_cell(2, 2).value is None
