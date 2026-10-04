import pytest

import formualizer as fz


def expected_error(shape: str) -> str:
    if shape in {"power", "arithmetic", "postfix"}:
        return "Formula AST height limit exceeded"
    return "Formula nesting too deep (max 72)"


def accepted_formulas() -> list[tuple[str, str]]:
    return [
        ("parentheses", f"={'(' * 64}1{')' * 64}"),
        ("sum", f"={'SUM(' * 64}1{')' * 64}"),
        ("flat-height", f"={'A1+' * 255}A1"),
        ("postfix-height", f"=1{'%' * 255}"),
        ("power-height", f"={'1^' * 255}1"),
        ("if", f"={'IF(A1>0,' * 64}1{',0)' * 64}"),
    ]


def hostile_formulas() -> list[tuple[str, str]]:
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


def assert_depth_error(parse_call, shape: str = "parentheses") -> None:
    with pytest.raises(fz.ParserError) as caught:
        parse_call()
    assert expected_error(shape) in str(caught.value)


@pytest.mark.parametrize("shape, formula", accepted_formulas())
def test_top_level_parse_accepts_boundary_and_converts_ast(
    shape: str, formula: str
) -> None:
    ast = fz.parse(formula)
    assert ast.to_formula().startswith("=")
    assert ast.pretty()
    assert ast.to_dict()
    del ast


@pytest.mark.parametrize("shape, formula", hostile_formulas())
def test_top_level_parse_rejects_depth_with_parser_error(
    shape: str, formula: str
) -> None:
    assert_depth_error(lambda: fz.parse(formula), shape)


def test_top_level_parse_succeeds_after_rejected_input() -> None:
    assert_depth_error(lambda: fz.parse(hostile_formulas()[0][1]))
    ast = fz.parse("=A1+1")
    assert ast.to_formula().replace(" ", "") == "=A1+1"
    del ast


def test_parser_parse_string_accepts_boundary_and_reuses_after_failure() -> None:
    parser = fz.Parser()
    for _shape, formula in accepted_formulas():
        ast = parser.parse_string(formula)
        assert ast.to_formula().startswith("=")
        assert ast.pretty()
        del ast

    for _shape, formula in hostile_formulas():
        assert_depth_error(lambda formula=formula: parser.parse_string(formula), _shape)

    ast = parser.parse_string("=A1+1")
    assert ast.to_formula().replace(" ", "") == "=A1+1"
    del ast


def test_parser_parse_tokens_accepts_and_rejects_depth() -> None:
    parser = fz.Parser()
    accepted = fz.tokenize(accepted_formulas()[1][1])
    ast = parser.parse_tokens(accepted)
    assert "SUM" in ast.to_formula()
    assert ast.pretty()
    del ast
    del accepted

    for _shape, formula in hostile_formulas():
        assert_depth_error(
            lambda formula=formula: parser.parse_tokens(fz.tokenize(formula)), _shape
        )

    ast = parser.parse_tokens(fz.tokenize("=A1+1"))
    assert ast.to_formula().replace(" ", "") == "=A1+1"
    del ast
