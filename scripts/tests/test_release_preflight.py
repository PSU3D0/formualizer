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


POLICY_FIXTURE_FILES = (
    "Cargo.toml",
    "crates/formualizer/Cargo.toml",
    "crates/formualizer-workbook/Cargo.toml",
    "crates/formualizer-cffi/Cargo.toml",
    "bindings/python/Cargo.toml",
    "bindings/wasm/Cargo.toml",
)

WORKBOOK_WORKSPACE_VERSION = str(
    tomllib.loads((release_preflight.ROOT / "Cargo.toml").read_text(encoding="utf-8"))[
        "workspace"
    ]["dependencies"]["formualizer-workbook"]["version"]
)


def workbook_workspace_dependency(extra: str = "") -> str:
    return (
        "formualizer-workbook = { version = "
        f'"{WORKBOOK_WORKSPACE_VERSION}", path = "crates/formualizer-workbook"'
        f"{extra} }}"
    )


def copy_policy_fixture(destination: Path) -> None:
    for relative in POLICY_FIXTURE_FILES:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            (release_preflight.ROOT / relative).read_text(encoding="utf-8"),
            encoding="utf-8",
        )


def mutate_file(root: Path, relative: str, old: str, new: str) -> None:
    path = root / relative
    contents = path.read_text(encoding="utf-8")
    if old not in contents:
        raise AssertionError(
            f"fixture mutation source not found in {relative}: {old!r}"
        )
    path.write_text(contents.replace(old, new, 1), encoding="utf-8")


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

    def assert_policy_mutations_fail(
        self, pattern: str, *mutations: tuple[str, str, str]
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            copy_policy_fixture(root)
            for mutation in mutations:
                mutate_file(root, *mutation)
            with self.assertRaisesRegex(RuntimeError, pattern):
                release_preflight.validate_binding_value_feature_policy(root)

    def test_binding_value_feature_policy_matches_fixed_inventory(self) -> None:
        coverage = release_preflight.validate_binding_value_feature_policy()
        self.assertEqual(
            set(coverage),
            {"cffi-native", "python-native", "python-pyodide", "wasm-browser"},
        )
        for profile in ("cffi-native", "python-native", "wasm-browser"):
            self.assertEqual(coverage[profile], {"system-clock": "enabled"})
        self.assertRegex(
            coverage["python-pyodide"]["system-clock"], r"^opt-out: Pyodide"
        )

    def test_binding_value_feature_policy_rejects_native_python_omission(self) -> None:
        self.assert_policy_mutations_fail(
            r"python-native.*system-clock.*uncovered",
            (
                "bindings/python/Cargo.toml",
                ', "umya", "xlsx-recalc", "system-clock"] }',
                ', "umya", "xlsx-recalc"] }',
            ),
        )

    def test_binding_value_feature_policy_expands_only_source_aliases(self) -> None:
        self.assert_policy_mutations_fail(
            r"wasm-browser.*system-clock.*uncovered",
            (
                "crates/formualizer/Cargo.toml",
                'wasm-js = ["portable-wasm", "system-clock", "js-runtime"]',
                'wasm-js = ["portable-wasm", "js-runtime"]',
            ),
        )

    def test_binding_value_feature_policy_honors_dependency_defaults(self) -> None:
        self.assert_policy_mutations_fail(
            r"cffi-native.*system-clock.*uncovered",
            (
                "crates/formualizer-workbook/Cargo.toml",
                'default = ["json", "csv", "system-clock", "xlsx-recalc"]',
                'default = ["json", "csv", "xlsx-recalc"]',
            ),
        )

    def test_binding_value_feature_policy_honors_workspace_default_disable(
        self,
    ) -> None:
        self.assert_policy_mutations_fail(
            r"cffi-native.*system-clock.*uncovered",
            (
                "Cargo.toml",
                workbook_workspace_dependency(),
                workbook_workspace_dependency(", default-features = false"),
            ),
        )

    def test_binding_value_feature_policy_unions_inherited_explicit_features(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            copy_policy_fixture(root)
            mutate_file(
                root,
                "Cargo.toml",
                workbook_workspace_dependency(),
                workbook_workspace_dependency(
                    ', default-features = false, features = ["system-clock"]'
                ),
            )
            release_preflight.validate_binding_value_feature_policy(root)

    def test_binding_value_feature_policy_rejects_member_default_override(self) -> None:
        self.assert_policy_mutations_fail(
            r"member default-features override",
            (
                "Cargo.toml",
                workbook_workspace_dependency(),
                workbook_workspace_dependency(", default-features = false"),
            ),
            (
                "crates/formualizer-cffi/Cargo.toml",
                'formualizer-workbook = { workspace = true, features = ["umya"] }',
                'formualizer-workbook = { workspace = true, default-features = true, features = ["umya"] }',
            ),
        )

    def test_binding_value_feature_policy_rejects_unsupported_workspace_inheritance(
        self,
    ) -> None:
        self.assert_policy_mutations_fail(
            r"unsupported workspace member inheritance",
            (
                "crates/formualizer-cffi/Cargo.toml",
                'formualizer-workbook = { workspace = true, features = ["umya"] }',
                'formualizer-workbook = { workspace = true, path = "../formualizer-workbook", features = ["umya"] }',
            ),
        )

    def test_binding_value_feature_policy_requires_pyodide_opt_out(self) -> None:
        self.assert_policy_mutations_fail(
            r"python-pyodide.*system-clock.*uncovered",
            (
                "bindings/python/Cargo.toml",
                '[package.metadata.formualizer-release.value-feature-opt-outs.python-pyodide]\nsystem-clock = "Pyodide/Emscripten deliberately uses a fixed or caller-injected clock instead of ambient wall-clock time."\n\n',
                "",
            ),
        )

    def test_binding_value_feature_policy_rejects_vacuous_review_claim(self) -> None:
        self.assert_policy_mutations_fail(
            r"substantive rationale",
            (
                "bindings/python/Cargo.toml",
                "Pyodide/Emscripten deliberately uses a fixed or caller-injected clock instead of ambient wall-clock time.",
                "Reviewed ",
            ),
        )

    def test_binding_value_feature_policy_rejects_generic_target_overlap(self) -> None:
        self.assert_policy_mutations_fail(
            r"alternate semantic dependency 'formualizer'|dependency 'formualizer' edges",
            (
                "bindings/python/Cargo.toml",
                'formualizer = { path = "../../crates/formualizer", default-features = false, features = ["eval", "workbook", "sheetport", "parse", "calamine", "umya", "xlsx-recalc", "system-clock"] }',
                "",
            ),
            (
                "bindings/python/Cargo.toml",
                "serde_json = { workspace = true }",
                'serde_json = { workspace = true }\nformualizer = { path = "../../crates/formualizer", default-features = false, features = ["system-clock"] }',
            ),
        )

    def test_binding_value_feature_policy_inventory_survives_profile_and_edge_removal(
        self,
    ) -> None:
        self.assert_policy_mutations_fail(
            r"edges.*emscripten",
            (
                "bindings/python/Cargo.toml",
                '[package.metadata.formualizer-release.value-feature-opt-outs.python-pyodide]\nsystem-clock = "Pyodide/Emscripten deliberately uses a fixed or caller-injected clock instead of ambient wall-clock time."\n\n',
                "",
            ),
            (
                "bindings/python/Cargo.toml",
                '[target.\'cfg(target_os = "emscripten")\'.dependencies]\nformualizer = { path = "../../crates/formualizer", default-features = false, features = ["eval", "workbook", "sheetport", "parse", "calamine", "umya", "xlsx-recalc"] }',
                "",
            ),
        )

    def test_binding_value_feature_policy_rejects_profile_and_target_rewrite(
        self,
    ) -> None:
        self.assert_policy_mutations_fail(
            r"unknown opt-out profile|alternate semantic dependency.*x86_64|edges.*x86_64",
            (
                "bindings/python/Cargo.toml",
                "value-feature-opt-outs.python-pyodide",
                "value-feature-opt-outs.python-other",
            ),
            (
                "bindings/python/Cargo.toml",
                "[target.'cfg(not(target_os = \"emscripten\"))'.dependencies]",
                "[target.'cfg(target_arch = \"x86_64\")'.dependencies]",
            ),
        )

    def test_binding_value_feature_policy_rejects_optional_policy_edge(self) -> None:
        self.assert_policy_mutations_fail(
            r"optional policy dependency",
            (
                "bindings/wasm/Cargo.toml",
                'formualizer = { path = "../../crates/formualizer", default-features = false,',
                'formualizer = { path = "../../crates/formualizer", optional = true, default-features = false,',
            ),
        )

    def test_binding_value_feature_policy_rejects_optional_weak_activation_model(self) -> None:
        self.assert_policy_mutations_fail(
            r"weak forwarding|optional policy dependency",
            (
                "bindings/wasm/Cargo.toml",
                'formualizer = { path = "../../crates/formualizer", default-features = false, features = ["wasm-js", "sheetport"] }',
                'formualizer = { path = "../../crates/formualizer", optional = true, default-features = false, features = ["portable-wasm", "sheetport"] }',
            ),
            (
                "bindings/wasm/Cargo.toml",
                'default = ["console_panic", "json", "calamine", "xlsx-recalc"]',
                'default = ["console_panic", "json", "calamine", "xlsx-recalc", "clock"]\nclock = ["formualizer?/system-clock"]',
            ),
        )

    def test_binding_value_feature_policy_rejects_binding_clock_forwarding(self) -> None:
        self.assert_policy_mutations_fail(
            r"forwarding enables value-affecting features",
            (
                "bindings/wasm/Cargo.toml",
                'default = ["console_panic", "json", "calamine", "xlsx-recalc"]',
                'default = ["console_panic", "json", "calamine", "xlsx-recalc", "clock"]\nclock = ["formualizer/system-clock"]',
            ),
        )

    def test_binding_value_feature_policy_rejects_forwarded_source_alias(self) -> None:
        self.assert_policy_mutations_fail(
            r"forwarding enables value-affecting features.*system-clock",
            (
                "bindings/python/Cargo.toml",
                'default = ["allocator-jemalloc"]',
                'default = ["allocator-jemalloc", "formualizer/wasm-js"]',
            ),
        )

    def test_binding_value_feature_policy_rejects_direct_semantic_dependency(self) -> None:
        self.assert_policy_mutations_fail(
            r"alternate semantic dependency 'formualizer-workbook'",
            (
                "bindings/python/Cargo.toml",
                "serde_json = { workspace = true }",
                "serde_json = { workspace = true }\nformualizer-workbook = { workspace = true }",
            ),
        )

    def test_binding_value_feature_policy_rejects_renamed_target_semantic_dependency(self) -> None:
        self.assert_policy_mutations_fail(
            r"alternate semantic dependency 'clock-workbook'.*formualizer-workbook",
            (
                "bindings/python/Cargo.toml",
                '[target.\'cfg(target_os = "emscripten")\'.dependencies]',
                '[target.\'cfg(target_os = "emscripten")\'.dependencies]\nclock-workbook = { package = "formualizer-workbook", path = "../../crates/formualizer-workbook" }',
            ),
        )

    def test_binding_value_feature_policy_rejects_workspace_alias_semantic_dependency(self) -> None:
        self.assert_policy_mutations_fail(
            r"alternate semantic dependency 'clock-workbook'.*formualizer-workbook",
            (
                "Cargo.toml",
                workbook_workspace_dependency(),
                workbook_workspace_dependency()
                + "\nclock-workbook = { package = \"formualizer-workbook\", version = "
                + f'"{WORKBOOK_WORKSPACE_VERSION}", path = "crates/formualizer-workbook" }}',
            ),
            (
                "bindings/python/Cargo.toml",
                "serde_json = { workspace = true }",
                "serde_json = { workspace = true }\nclock-workbook = { workspace = true }",
            ),
        )

    def test_new_value_affecting_feature_requires_real_activation(self) -> None:
        self.assert_policy_mutations_fail(
            r"semantic-switch.*uncovered",
            (
                "crates/formualizer/Cargo.toml",
                'value-affecting-features = { system-clock = "Selects ambient wall-clock evaluation for TODAY() and NOW(); omitting it changes computed values to the fixed-clock fallback." }',
                'value-affecting-features = { system-clock = "Selects ambient wall-clock evaluation for TODAY() and NOW(); omitting it changes computed values to the fixed-clock fallback.", semantic-switch = "Test-only semantic switch." }',
            ),
            (
                "crates/formualizer/Cargo.toml",
                "[features]\n",
                "[features]\nsemantic-switch = []\n",
            ),
        )

    def test_new_value_affecting_feature_cannot_self_approve_opt_out(self) -> None:
        self.assert_policy_mutations_fail(
            r"unapproved opt-out.*semantic-switch",
            (
                "crates/formualizer/Cargo.toml",
                'value-affecting-features = { system-clock = "Selects ambient wall-clock evaluation for TODAY() and NOW(); omitting it changes computed values to the fixed-clock fallback." }',
                'value-affecting-features = { system-clock = "Selects ambient wall-clock evaluation for TODAY() and NOW(); omitting it changes computed values to the fixed-clock fallback.", semantic-switch = "Test-only semantic switch." }',
            ),
            (
                "crates/formualizer/Cargo.toml",
                "[features]\n",
                "[features]\nsemantic-switch = []\n",
            ),
            (
                "bindings/python/Cargo.toml",
                'system-clock = "Pyodide/Emscripten deliberately uses a fixed or caller-injected clock instead of ambient wall-clock time."',
                'system-clock = "Pyodide/Emscripten deliberately uses a fixed or caller-injected clock instead of ambient wall-clock time."\nsemantic-switch = "A newly written rationale cannot authorize a policy exception without changing the independent checker allowlist."',
            ),
        )

    def test_binding_value_feature_policy_rejects_stale_opt_out(self) -> None:
        self.assert_policy_mutations_fail(
            r"python-pyodide.*stale opt-out",
            (
                "bindings/python/Cargo.toml",
                'features = ["eval", "workbook", "sheetport", "parse", "calamine", "umya", "xlsx-recalc"] }\n\n#',
                'features = ["eval", "workbook", "sheetport", "parse", "calamine", "umya", "xlsx-recalc", "system-clock"] }\n\n#',
            ),
        )

    def test_binding_value_feature_policy_rejects_unknown_profile_metadata(
        self,
    ) -> None:
        self.assert_policy_mutations_fail(
            r"unknown opt-out profile",
            (
                "bindings/python/Cargo.toml",
                "[lib]",
                '[package.metadata.formualizer-release.value-feature-opt-outs.other]\nsystem-clock = "A long but unauthorized profile rationale that must never redefine the fixed release inventory."\n\n[lib]',
            ),
        )

    def test_binding_value_feature_policy_rejects_metadata_schema_drift(self) -> None:
        self.assert_policy_mutations_fail(
            r"unapproved opt-out.*approximate-cargo",
            (
                "bindings/python/Cargo.toml",
                "[package.metadata.formualizer-release.value-feature-opt-outs.python-pyodide]",
                "[package.metadata.formualizer-release.value-feature-opt-outs.python-pyodide]\napproximate-cargo = true",
            ),
        )

    def test_binding_value_feature_policy_requires_directory_source_path(self) -> None:
        self.assert_policy_mutations_fail(
            r"source path must be a directory",
            (
                "bindings/wasm/Cargo.toml",
                'path = "../../crates/formualizer"',
                'path = "../../crates/formualizer/Cargo.toml"',
            ),
        )

    def test_binding_value_feature_policy_rejects_source_outside_checkout(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temp,
            tempfile.TemporaryDirectory() as outside,
        ):
            root = Path(temp)
            copy_policy_fixture(root)
            mutate_file(
                root,
                "bindings/wasm/Cargo.toml",
                'path = "../../crates/formualizer"',
                f'path = "{outside}"',
            )
            with self.assertRaisesRegex(RuntimeError, r"source leaves checkout"):
                release_preflight.validate_binding_value_feature_policy(root)

    def test_binding_value_feature_policy_rejects_symlinked_source_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp, tempfile.TemporaryDirectory() as outside:
            root = Path(temp)
            copy_policy_fixture(root)
            source = root / "crates/formualizer-workbook"
            (source / "Cargo.toml").unlink()
            source.rmdir()
            source.symlink_to(Path(outside), target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, r"symlinked source path"):
                release_preflight.validate_binding_value_feature_policy(root)

    def test_binding_value_feature_policy_rejects_symlinked_source_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp, tempfile.TemporaryDirectory() as outside:
            root = Path(temp)
            copy_policy_fixture(root)
            source = root / "crates/formualizer-workbook/Cargo.toml"
            external = Path(outside) / "Cargo.toml"
            external.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
            source.unlink()
            source.symlink_to(external)
            with self.assertRaisesRegex(RuntimeError, r"symlinked source Cargo.toml"):
                release_preflight.validate_binding_value_feature_policy(root)

    def test_product_preflight_checks_features_before_registry_lookup(self) -> None:
        calls: list[str] = []

        def record(name: str):
            def check(*_args, **_kwargs) -> None:
                calls.append(name)
                if name == "parser":
                    raise RuntimeError("stop before registry lookup")

            return check

        with (
            mock.patch.object(
                release_preflight, "validate_binding_package_policy", record("package")
            ),
            mock.patch.object(
                release_preflight,
                "validate_binding_value_feature_policy",
                record("features"),
            ),
            mock.patch.object(
                release_preflight, "validate_no_prerelease_wording", record("wording")
            ),
            mock.patch.object(
                release_preflight, "validate_parser_track_lockstep", record("parser")
            ),
            mock.patch("builtins.print"),
            self.assertRaisesRegex(RuntimeError, "stop before registry lookup"),
        ):
            release_preflight.preflight("product", False)
        self.assertEqual(calls, ["package", "features", "wording", "parser"])

    def test_parser_track_versions_are_in_lockstep_in_tree(self) -> None:
        self.assertEqual(
            release_preflight.COMMON.version(), release_preflight.PARSE.version()
        )
        release_preflight.validate_parser_track_lockstep("parse")
        release_preflight.validate_parser_track_lockstep("spec")

    def test_parser_track_lockstep_rejects_version_drift(self) -> None:
        with mock.patch.object(
            release_preflight.Package, "version", autospec=True
        ) as version:
            version.side_effect = lambda package: (
                "3.1.0" if package.name == "formualizer-common" else "3.0.0"
            )
            for track in ("parse", "product"):
                with self.subTest(track=track):
                    with self.assertRaisesRegex(
                        RuntimeError, r"lockstep.*formualizer-common is 3\.1\.0"
                    ):
                        release_preflight.validate_parser_track_lockstep(track)

    def test_product_track_requires_published_parser_crates(self) -> None:
        looked_up: list[tuple[str, str]] = []

        def missing(name: str, version: str) -> None:
            looked_up.append((name, version))
            return None

        with self.assertRaisesRegex(
            RuntimeError, r"formualizer-common .* is not published"
        ):
            release_preflight.validate_parser_track_lockstep("product", lookup=missing)
        self.assertEqual(
            looked_up,
            [("formualizer-common", release_preflight.COMMON.version())],
        )

        looked_up.clear()
        release_preflight.validate_parser_track_lockstep("parse", lookup=missing)
        self.assertEqual(looked_up, [])

        def published(name: str, version: str) -> dict[str, str]:
            looked_up.append((name, version))
            return {"num": version}

        release_preflight.validate_parser_track_lockstep("product", lookup=published)
        self.assertEqual(
            [name for name, _ in looked_up],
            ["formualizer-common", "formualizer-parse"],
        )

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
                "formualizer-cli",
            ],
        )


class CliReleaseWorkflowTests(unittest.TestCase):
    """Keep the native CLI release wiring aligned with preflight and safe."""

    workflows = release_preflight.ROOT / ".github" / "workflows"

    def job_block(self, text: str, job: str) -> str:
        start = text.index(f"\n  {job}:\n")
        lines = text[start + 1 :].splitlines()
        block = [lines[0]]
        for line in lines[1:]:
            # The next two-space-indented key starts the next job.
            if line.startswith("  ") and not line.startswith("   ") and line.strip():
                break
            block.append(line)
        return "\n".join(block)

    def test_product_crate_publication_follows_preflight_order(self) -> None:
        text = (self.workflows / "release.yml").read_text(encoding="utf-8")
        published = [
            line.split()[1]
            for line in self.job_block(text, "publish-product-crates").splitlines()
            if line.strip().startswith("publish_with_retry ")
        ]
        product = [package.name for package in release_preflight.TRACKS["product"]]
        self.assertEqual(published, [name for name in product if name in published])
        self.assertEqual(published[-2:], ["formualizer", "formualizer-cli"])

    def test_cli_build_workflow_cannot_publish(self) -> None:
        text = (self.workflows / "cli-build.yml").read_text(encoding="utf-8")
        for forbidden in (
            "secrets.",
            "npm publish",
            "cargo publish",
            "id-token",
            "contents: write",
            "action-gh-release",
            "gh release",
        ):
            self.assertNotIn(forbidden, text)
        self.assertIn("permissions:\n  contents: read\n", text)

    def test_cli_publish_jobs_only_run_for_product_tags(self) -> None:
        text = (self.workflows / "release.yml").read_text(encoding="utf-8")
        push = "github.event_name == 'push' && startsWith(github.ref_name, 'v')"
        build = self.job_block(text, "build-cli")
        self.assertIn(push + " && needs.verify-product.result == 'success'", build)
        self.assertIn(
            "github.event_name == 'workflow_dispatch' && startsWith(inputs.resume_cli_tag, 'v')",
            build,
        )
        publish = self.job_block(text, "publish-npm-cli")
        self.assertIn("needs.build-cli.result == 'success'", publish)
        self.assertIn(push + " && needs.publish-product-crates.result == 'success'", publish)
        # A resume dispatch never reaches the crate or wheel jobs.
        for job in ("publish-product-crates", "publish-pypi", "publish-npm", "verify-product"):
            self.assertNotIn("workflow_dispatch", self.job_block(text, job))
        self.assertIn('"./npm-tarballs/${file}"', publish)
        self.assertIn("--provenance", publish)
        self.assertIn("files: cli-dist/*", self.job_block(text, "github-release"))

    def test_binstall_metadata_matches_release_archive_names(self) -> None:
        manifest = tomllib.loads(
            (release_preflight.ROOT / "crates/formualizer-cli/Cargo.toml").read_text(encoding="utf-8")
        )
        binstall = manifest["package"]["metadata"]["binstall"]
        stem = "formualizer-cli-v{ version }-{ target }"
        self.assertTrue(binstall["pkg-url"].endswith(f"/releases/download/v{{ version }}/{stem}.tar.gz"))
        self.assertEqual(binstall["bin-dir"], f"{stem}/{{ bin }}{{ binary-ext }}")
        windows = binstall["overrides"]["x86_64-pc-windows-msvc"]
        self.assertTrue(windows["pkg-url"].endswith(f"{stem}.zip"))
        workflow = (self.workflows / "cli-build.yml").read_text(encoding="utf-8")
        self.assertIn('name="formualizer-cli-v${version}-${target}"', workflow)
        self.assertIn('"release-assets/${name}.tar.gz"', workflow)
        self.assertIn('"../release-assets/${name}.zip"', workflow)


class PrereleaseWordingTests(unittest.TestCase):
    NOTICE = "> **Not yet published:** these install channels go live soon.\n"

    def write(self, root: Path, relative: str, text: str) -> None:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_clean_shipped_docs_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write(root, "README.md", "# Formualizer\n\nInstall with pip.\n")
            self.write(root, "docs/cli.md", "Planned work lives elsewhere.\n")
            release_preflight.validate_no_prerelease_wording(root)

    def test_every_shipped_location_is_reported(self) -> None:
        shipped = (
            "README.md",
            "CHANGELOG.md",
            "bindings/python/README.md",
            "crates/formualizer-cli/README.md",
            "npm/formualizer-cli/README.md",
            "docs/agents.md",
            "docs/nested/page.md",
            "skills/formualizer-recalc/SKILL.md",
            "docs-site/content/docs/recalc-cli/index.mdx",
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for relative in shipped:
                self.write(root, relative, "intro\n" + self.NOTICE)
            hits = release_preflight.find_prerelease_wording(root)
        self.assertEqual(
            sorted(hit.split(":", 1)[0] for hit in hits), sorted(shipped)
        )
        self.assertTrue(all(":2: > **Not yet published:**" in hit for hit in hits))

    def test_all_phrasings_and_mdx_callouts_are_caught(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write(
                root,
                "docs-site/content/docs/a.mdx",
                '<Callout type="warn" title="Not yet published">soon</Callout>\n'
                "Channels are planned (publication pending).\n"
                "Download the archive once published.\n",
            )
            hits = release_preflight.find_prerelease_wording(root)
        self.assertEqual([hit.split(":")[1] for hit in hits], ["1", "2", "3"])

    def test_unshipped_files_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write(root, "docs-site/node_modules/pkg/README.md", self.NOTICE)
            self.write(root, "scripts/notes.md", self.NOTICE)
            self.write(root, "crates/formualizer-cli/src/lib.rs", "// not yet published\n")
            self.assertEqual(release_preflight.find_prerelease_wording(root), [])

    def test_validation_lists_each_location(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write(root, "docs/cli.md", "x\n" + self.NOTICE)
            with self.assertRaisesRegex(RuntimeError, r"docs/cli\.md:2: > \*\*Not yet published"):
                release_preflight.validate_no_prerelease_wording(root)

    def test_cli_flag_runs_only_the_wording_check(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["release-preflight.py", "--check-prerelease-wording"]),
            mock.patch.object(release_preflight, "validate_no_prerelease_wording") as check,
            mock.patch.object(release_preflight, "preflight") as preflight,
            mock.patch("builtins.print"),
        ):
            self.assertEqual(release_preflight.main(), 0)
        check.assert_called_once_with()
        preflight.assert_not_called()


if __name__ == "__main__":
    unittest.main()
