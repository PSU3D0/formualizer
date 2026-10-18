import pytest

import formualizer as fz


@pytest.mark.parametrize(
    "operator",
    ["+", "-", "*", "/", "^", "&", "=", "<>", "<", ">", "<=", ">=", ":", ",", " "],
)
def test_binary_operators_remain_strings(operator: str) -> None:
    # Plain cell ranges are folded to references; keep a dynamic range node.
    formula = "=A1:OFFSET(B1,0,0)" if operator == ":" else f"=A1{operator}B2"
    ast = fz.parse(formula)
    assert ast.get_operator() == operator
    assert ast.to_dict()["operator"] == operator
    assert operator in ast.pretty()
    assert ast.to_formula().startswith("=")


@pytest.mark.parametrize("operator", ["+", "-", "%", "#", "@"])
def test_unary_operators_remain_strings(operator: str) -> None:
    formula = f"=A1{operator}" if operator in {"%", "#"} else f"={operator}A1"
    ast = fz.parse(formula)
    assert ast.get_operator() == operator
    assert ast.to_dict()["operator"] == operator
    assert operator in ast.pretty()
    assert ast.to_formula().startswith("=")
