import formualizer as fz


def test_parse_to_formula_quotes_text():
    ast = fz.parse('=SUMIFS(A:A,B:B,"*Parking*")')
    assert ast.to_formula() == '=SUMIFS(A:A, B:B, "*Parking*")'


def test_parse_projects_omitted_argument_distinctly():
    ast = fz.parse("=IFERROR(A1,)")
    assert ast.children()[1].node_type() == "Omitted"
    assert ast.children()[1].get_literal_value() is None
    assert ast.to_formula() == "=IFERROR(A1,)"


def test_workbook_evaluates_omitted_iferror_fallback_as_zero():
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_formula("S", 1, 1, "=IFERROR(1/0,)")
    wb.evaluate_all()
    assert wb.get_value("S", 1, 1) == 0


def test_workbook_get_formula_roundtrips_text():
    wb = fz.Workbook()
    wb.add_sheet("S")
    wb.set_formula("S", 1, 1, '=SUMIFS(A:A,B:B,"*Parking*")')
    assert wb.get_formula("S", 1, 1) == '=SUMIFS(A:A,B:B,"*Parking*")'


def test_openformula_tokenize_and_parse():
    tokenizer = fz.tokenize("=SUM([.A1];[.A2])", dialect=fz.FormulaDialect.OpenFormula)
    assert tokenizer.render() == "=SUM([.A1];[.A2])"
    assert str(tokenizer.dialect) == "OpenFormula"

    tokens = tokenizer.tokens()
    assert tokens[2].value == ";"
    assert str(tokens[2].subtype) == "Arg"

    ast = fz.parse("=SUM([Sheet One.A1:.B2])", dialect=fz.FormulaDialect.OpenFormula)
    refs = list(ast.walk_refs())
    assert len(refs) == 1
    ref = refs[0]
    # RangeRef exposes sheet/start/end attributes
    assert ref.sheet == "Sheet One"
    assert ref.start.row == 1
    assert ref.end.col == 2
