from __future__ import annotations

import importlib.util
import io
import os
import sys
import tarfile
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "release-preflight.py"
SPEC = importlib.util.spec_from_file_location("release_preflight", SCRIPT)
assert SPEC and SPEC.loader
release_preflight = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release_preflight
SPEC.loader.exec_module(release_preflight)


def crate_archive(
    path: Path, files: dict[str, bytes], *, symlink: str | None = None
) -> Path:
    root = path.name.removesuffix(".crate")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        for relative, contents in files.items():
            info = tarfile.TarInfo(f"{root}/{relative}")
            info.size = len(contents)
            archive.addfile(info, io.BytesIO(contents))
        if symlink is not None:
            info = tarfile.TarInfo(f"{root}/unsafe-link")
            info.type = tarfile.SYMTYPE
            info.linkname = symlink
            archive.addfile(info)
    return path


class ReleasePreflightTests(unittest.TestCase):
    def test_subprocess_environment_drops_release_credentials(self) -> None:
        source = {
            "PATH": "/bin",
            "CARGO_REGISTRY_TOKEN": "secret",
            "CARGO_REGISTRIES_PRIVATE_TOKEN": "private",
            "GH_TOKEN": "gh-secret",
            "GITHUB_TOKEN": "actions-secret",
        }
        with mock.patch.dict(os.environ, source, clear=True):
            env = release_preflight.credential_free_environment()
        self.assertEqual(env["PATH"], "/bin")
        self.assertEqual(env["GIT_TERMINAL_PROMPT"], "0")
        self.assertFalse(any(key.endswith("TOKEN") for key in env))

    def test_registry_index_paths_follow_cargo_layout(self) -> None:
        registry = Path("/registry")
        self.assertEqual(
            release_preflight.registry_index_path(registry, "formualizer-common"),
            registry / "index/fo/rm/formualizer-common",
        )
        self.assertEqual(
            release_preflight.registry_index_path(registry, "abc"),
            registry / "index/3/a/abc",
        )
        self.assertEqual(
            release_preflight.registry_index_path(registry, "ab"),
            registry / "index/2/ab",
        )

    def test_index_record_uses_packaged_manifest_and_checksum(self) -> None:
        manifest = b"""[package]\nname = "demo-crate"\nversion = "1.2.3"\nrust-version = "1.80"\n\n[features]\ndefault = []\nserde = ["dep:serde", "common/serde"]\n\n[dependencies.common]\nversion = "3.0.0"\n\n[dependencies.serde]\nversion = "1"\noptional = true\nfeatures = ["derive"]\n\n[target.'cfg(target_arch = "wasm32")'.dependencies.getrandom02]\nversion = "0.2"\npackage = "getrandom"\nfeatures = ["js"]\n"""
        with tempfile.TemporaryDirectory() as temp:
            archive = crate_archive(
                Path(temp) / "demo-crate-1.2.3.crate",
                {
                    "Cargo.toml": manifest,
                    "Cargo.toml.orig": manifest,
                    "src/lib.rs": b"",
                },
            )
            record = release_preflight.index_record_from_archive(archive)

        self.assertEqual(record["name"], "demo-crate")
        self.assertEqual(record["vers"], "1.2.3")
        self.assertEqual(record["rust_version"], "1.80")
        self.assertEqual(record["features"], {"default": []})
        self.assertEqual(record["features2"], {"serde": ["dep:serde", "common/serde"]})
        self.assertEqual(record["v"], 2)
        renamed = next(dep for dep in record["deps"] if dep["name"] == "getrandom02")
        self.assertEqual(renamed["package"], "getrandom")
        self.assertEqual(renamed["target"], 'cfg(target_arch = "wasm32")')
        self.assertEqual(renamed["kind"], "normal")

    def test_index_record_requires_packaged_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            archive = crate_archive(
                Path(temp) / "demo-1.0.0.crate", {"src/lib.rs": b""}
            )
            with self.assertRaisesRegex(RuntimeError, "archive is missing"):
                release_preflight.index_record_from_archive(archive)

    def test_payload_ignores_generated_metadata_but_not_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            first = crate_archive(
                Path(temp) / "demo-1.0.0.crate",
                {
                    ".cargo_vcs_info.json": b"commit-a",
                    "Cargo.toml": b"generated-a",
                    "Cargo.lock": b"lock-a",
                    "Cargo.toml.orig": b"version = '1.0.0'",
                    "src/lib.rs": b"pub fn value() -> u8 { 1 }",
                },
            )
            payload = release_preflight.archive_payload(first)
            self.assertEqual(set(payload), {"Cargo.toml.orig", "src/lib.rs"})

            second = crate_archive(
                Path(temp) / "other" / "demo-1.0.0.crate",
                {
                    ".cargo_vcs_info.json": b"commit-b",
                    "Cargo.toml": b"generated-b",
                    "Cargo.lock": b"lock-b",
                    "Cargo.toml.orig": b"version = '1.0.0'",
                    "src/lib.rs": b"pub fn value() -> u8 { 2 }",
                },
            )
            self.assertNotEqual(payload, release_preflight.archive_payload(second))

    def test_payload_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            archive = crate_archive(
                Path(temp) / "demo-1.0.0.crate",
                {"Cargo.toml.orig": b"[package]"},
                symlink="../../outside",
            )
            with self.assertRaisesRegex(RuntimeError, "unsafe archive member type"):
                release_preflight.archive_payload(archive)

    def test_persistent_state_rejects_nested_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "real").mkdir()
            (root / "real" / "link").symlink_to(root / "outside")
            with self.assertRaisesRegex(
                RuntimeError, "symlink in persistent preflight state"
            ):
                release_preflight.reject_tree_symlinks(root)

    def test_binding_manifests_have_exact_non_publishable_policy(self) -> None:
        for manifest, expected_name in release_preflight.BINDING_PACKAGE_POLICY:
            data = tomllib.loads(
                (release_preflight.ROOT / manifest).read_text(encoding="utf-8")
            )
            self.assertEqual(data["package"]["name"], expected_name)
            self.assertIs(data["package"]["publish"], False)
        release_preflight.validate_binding_package_policy()

    def test_binding_policy_reports_each_manifest_mutation(self) -> None:
        for manifest, expected_name in release_preflight.BINDING_PACKAGE_POLICY:
            source = (release_preflight.ROOT / manifest).read_text(encoding="utf-8")
            for mutation in ("missing-publish", "changed-publish", "changed-name"):
                with self.subTest(manifest=manifest, mutation=mutation):
                    contents = source
                    if mutation == "missing-publish":
                        contents = contents.replace("publish = false\n", "", 1)
                    elif mutation == "changed-publish":
                        contents = contents.replace("publish = false", "publish = true", 1)
                    else:
                        contents = contents.replace(
                            f'name = "{expected_name}"',
                            'name = "unexpected-binding-name"',
                            1,
                        )
                    with tempfile.TemporaryDirectory() as temp:
                        root = Path(temp)
                        for candidate, _ in release_preflight.BINDING_PACKAGE_POLICY:
                            destination = root / candidate
                            destination.parent.mkdir(parents=True, exist_ok=True)
                            destination.write_text(
                                (release_preflight.ROOT / candidate).read_text(
                                    encoding="utf-8"
                                ),
                                encoding="utf-8",
                            )
                        (root / manifest).write_text(contents, encoding="utf-8")
                        with self.assertRaisesRegex(
                            RuntimeError, rf"{manifest}.*{expected_name}"
                        ):
                            release_preflight.validate_binding_package_policy(root)

    def test_binding_packages_are_disjoint_from_every_release_track(self) -> None:
        track_names = {
            package.name
            for packages in release_preflight.TRACKS.values()
            for package in packages
        }
        for _, name in release_preflight.BINDING_PACKAGE_POLICY:
            self.assertNotIn(name, track_names)
        release_preflight.validate_binding_package_policy()

    def test_track_order_matches_publish_dependencies(self) -> None:
        self.assertEqual(
            [package.name for package in release_preflight.TRACKS["parse"]],
            ["formualizer-common", "formualizer-parse"],
        )
        self.assertEqual(
            [package.name for package in release_preflight.TRACKS["product"]],
            [
                "formualizer-common",
                "formualizer-parse",
                "sheetport-spec",
                "formualizer-macros",
                "formualizer-eval",
                "formualizer-workbook",
                "formualizer-sheetport",
                "formualizer",
            ],
        )


if __name__ == "__main__":
    unittest.main()
