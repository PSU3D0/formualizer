import formualizer as fz


def test_parse_to_formula_quotes_text():
    ast = fz.parse('=SUMIFS(A:A,B:B,"*Parking*")')
    assert ast.to_formula() == '=SUMIFS(A:A, B:B, "*Parking*")'


def test_engine_get_cell_formula_quotes_text():
    # Build a minimal workbook with one sheet and one formula cell
    wb = fz.Workbook()
    wb.add_sheet("S")
    eng = fz.Engine.from_workbook(wb)
    eng.set_formula("S", 1, 1, '=SUMIFS(A:A,B:B,"*Parking*")')
    cell = eng.get_cell("S", 1, 1)
    assert cell.formula == '=SUMIFS(A:A, B:B, "*Parking*")'
