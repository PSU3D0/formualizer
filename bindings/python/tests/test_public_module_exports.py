import formualizer
import formualizer.formualizer_py as native


def test_public_reexports_match_native_module() -> None:
    expected = {
        "ASTNode",
        "Cell",
        "CellRef",
        "LiteralValue",
        "Sheet",
        "SheetPortSession",
        "Token",
        "Tokenizer",
        "Workbook",
        "parse",
        "tokenize",
    }

    exceptions = {
        "ExcelEvaluationError",
        "FormualizerHostError",
        "ParserError",
        "SheetPortConstraintError",
        "SheetPortError",
        "SheetPortManifestError",
        "SheetPortWorkbookError",
        "TokenizerError",
    }

    assert expected | exceptions <= set(native.__all__)
    assert set(formualizer.__all__) == set(native.__all__) | {
        "ReferenceLike",
        "visitor",
    }
    for name in expected | exceptions:
        assert getattr(formualizer, name) is getattr(native, name)

    assert formualizer.Cell.__module__ == "formualizer.formualizer_py"
    assert formualizer.Workbook.__module__ == "formualizer.formualizer_py"


def test_legacy_aliases_are_star_exported() -> None:
    aliases = {
        "PyFormulaDialect": "FormulaDialect",
        "PyRefWalker": "RefWalker",
        "PyToken": "Token",
        "PyTokenSubType": "TokenSubType",
        "PyTokenType": "TokenType",
        "PyTokenizer": "Tokenizer",
        "PyTokenizerIter": "TokenizerIter",
    }

    namespace: dict[str, object] = {}
    exec("from formualizer import *", namespace)

    for alias, canonical in aliases.items():
        assert alias in native.__all__
        assert namespace[alias] is getattr(formualizer, canonical)

    assert namespace["ReferenceLike"] is formualizer.ReferenceLike
    assert namespace["visitor"] is formualizer.visitor
