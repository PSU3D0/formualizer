import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


class DocExamplesTest(unittest.TestCase):
    def test_fences_and_regions(self):
        script = Path(__file__).resolve().parents[1] / "check-doc-examples.py"
        spec = importlib.util.spec_from_file_location("doc_examples", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "example.py").write_text("# region demo\nprint('hello')\n# endregion\n")
            readme = root / "README.md"
            readme.write_text("<!-- example: example.py#demo -->\n```python\nprint('hello')\n```\n")
            with patch.object(module, "ROOT", root):
                self.assertFalse(module.main())
                readme.write_text("<!-- example: example.py#demo -->\n```python\nprint('wrong')\n```\n")
                self.assertTrue(module.main())
                readme.write_text("<!-- example: missing.py -->\n```python\npass\n```\n")
                self.assertTrue(module.main())
                readme.write_text("<!-- example: example.py#demo -->\n\nNo fence follows.\n")
                self.assertTrue(module.main())
                (root / "nested.py").write_text("def f():\n    # region body\n    x = 1\n    # endregion\n")
                readme.write_text("<!-- example: nested.py#body -->\n```python\nx = 1\n```\n")
                self.assertFalse(module.main())
                readme.unlink()
                docs = root / "docs-site/content/docs"
                docs.mkdir(parents=True)
                page = docs / "page.mdx"
                page.write_text("{/* example: example.py#demo */}\n```python\nprint('hello')\n```\n")
                self.assertFalse(module.main())
                page.write_text("{/* example: example.py#demo */}\n```python\nprint('wrong')\n```\n")
                self.assertTrue(module.main())


if __name__ == "__main__":
    unittest.main()
