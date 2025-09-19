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


def test_openformula_tokenize_and_parse():
    tokenizer = fz.tokenize('=SUM([.A1];[.A2])', dialect=fz.FormulaDialect.OpenFormula)
    assert tokenizer.render() == '=SUM([.A1];[.A2])'
    assert str(tokenizer.dialect) == "OpenFormula"

    tokens = tokenizer.tokens()
    assert tokens[2].value == ';'
    assert str(tokens[2].subtype) == "Arg"

    ast = fz.parse('=SUM([Sheet One.A1:.B2])', dialect=fz.FormulaDialect.OpenFormula)
    refs = list(ast.walk_refs())
    assert len(refs) == 1
    ref = refs[0]
    # RangeRef exposes sheet/start/end attributes
    assert ref.sheet == 'Sheet One'
    assert ref.start.row == 1
    assert ref.end.col == 2
