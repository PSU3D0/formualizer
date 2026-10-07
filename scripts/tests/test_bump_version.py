"""CLI membership in the product version track and dependency bump policy."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "bump-version.py"
spec = importlib.util.spec_from_file_location("bump_version", SCRIPT)
bump_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bump_version)


class CliVersionTests(unittest.TestCase):
    def test_cli_package_and_dependency_bump(self):
        manifest = "crates/formualizer-cli/Cargo.toml"
        self.assertIn((manifest, "toml", ["package", "version"]),
                      bump_version.PRODUCT_PACKAGE_VERSION_FILES)
        self.assertIn((manifest, "formualizer-workbook"),
                      bump_version.PRODUCT_INTERNAL_DEPS)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / manifest
            path.parent.mkdir(parents=True)
            path.write_text((bump_version.REPO_ROOT / manifest).read_text())
            with patch.object(bump_version, "REPO_ROOT", root):
                bump_version.update_internal_deps([(manifest, "formualizer-workbook")],
                                                  "0.11.0", False)
            self.assertIn('version = "0.11.0", default-features = false', path.read_text())


if __name__ == "__main__":
    unittest.main()
