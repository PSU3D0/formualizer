import formualizer as fz

wb = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
wb.add_sheet("Sheet1")

wb.set_value("Sheet1", 1, 1, 1)
wb.set_value("Sheet1", 1, 2, 2)
wb.set_value("Sheet1", 2, 1, 3)
wb.set_value("Sheet1", 2, 2, 4)


def sum_range(grid):
    return sum(cell for row in grid for cell in row)


wb.register_function("py_sum_range", sum_range, min_args=1, max_args=1)
wb.set_formula("Sheet1", 1, 3, "=PY_SUM_RANGE(A1:B2)")

print(wb.evaluate_cell("Sheet1", 1, 3))
print(wb.list_functions())

wb.unregister_function("py_sum_range")
