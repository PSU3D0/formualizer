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
        "SourceCalculationPolicy",
        "SourceRecalculateLimits",
        "SourceRecalculateResult",
        "SourceOperationDiagnostics",
        "SourceUnavailableOperation",
        "SourceDependencyEvidence",
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


def test_installed_stubs_match_public_exports_and_source_signatures() -> None:
    import ast
    from pathlib import Path

    package = Path(formualizer.__file__).parent
    for name, exports in [
        ("__init__.pyi", formualizer.__all__),
        ("formualizer_py.pyi", native.__all__),
    ]:
        module = ast.parse((package / name).read_text())
        all_assignment = next(
            node
            for node in module.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__all__"
                for target in node.targets
            )
        )
        # Private CLI helpers are registered outside stub-gen inventory.
        # Compare the public surface without changing those runtime exports.
        public_exports = {name for name in exports if not name.startswith("_")}
        assert set(ast.literal_eval(all_assignment.value)) == public_exports
        if name == "formualizer_py.pyi":
            functions = {
                node.name: node
                for node in module.body
                if isinstance(node, ast.FunctionDef)
            }
            for function in ["recalculate_xlsx_bytes", "recalculate_xlsx_file"]:
                signature = functions[function]
                assert ast.unparse(signature.returns) == "SourceRecalculateResult"
                keywords = {
                    arg.arg: ast.unparse(arg.annotation)
                    for arg in signature.args.kwonlyargs
                }
                assert keywords["limits"] == "SourceRecalculateLimits | None"
                assert keywords["calculation"] == "SourceCalculationPolicy | None"
