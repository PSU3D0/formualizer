import formualizer as fz


def workbook_with_overwritten_value():
    workbook = fz.Workbook(config=fz.WorkbookConfig(enable_changelog=True))
    sheet = workbook.sheet("Sheet1")
    sheet.set_value(1, 1, "before")
    sheet.set_value(1, 1, "after")
    return workbook, sheet


def test_undo_updates_all_public_value_readers():
    workbook, sheet = workbook_with_overwritten_value()

    assert workbook.get_value("Sheet1", 1, 1) == "after"
    assert sheet.get_cell(1, 1).value == "after"

    workbook.undo()

    assert workbook.get_value("Sheet1", 1, 1) == "before"
    assert sheet.get_cell(1, 1).value == "before"


def test_redo_updates_all_public_value_readers():
    workbook, sheet = workbook_with_overwritten_value()
    workbook.undo()

    assert workbook.get_value("Sheet1", 1, 1) == "before"
    assert sheet.get_cell(1, 1).value == "before"

    workbook.redo()

    assert workbook.get_value("Sheet1", 1, 1) == "after"
    assert sheet.get_cell(1, 1).value == "after"


def test_undo_redo_are_noops_when_changelog_disabled():
    workbook = fz.Workbook(config=fz.WorkbookConfig(enable_changelog=False))
    sheet = workbook.sheet("Sheet1")
    sheet.set_value(1, 1, "value")

    workbook.undo()
    assert workbook.get_value("Sheet1", 1, 1) == "value"

    workbook.redo()
    assert workbook.get_value("Sheet1", 1, 1) == "value"
