#!/usr/bin/env python3
"""Verify Formualizer release archives before a tag can publish them.

The preflight builds crates in publish order against a temporary local Cargo
registry, so downstream archives resolve the exact prospective upstream
archives rather than workspace path dependencies or older crates.io releases.
It also rejects source drift when a package version already exists on crates.io.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "target"
LOCK_PATH = TARGET / "release-preflight.lock"
TOOL_VERSION = "0.2.12"
TOOLCHAIN = "1.93.0"
TOOL_ROOT = TARGET / "release-preflight-tools" / f"cargo-local-registry-{TOOL_VERSION}"
TOOL_BIN = TOOL_ROOT / "bin" / "cargo-local-registry"
TOOL_CARGO_HOME = TARGET / "release-preflight-cargo-home"
USER_AGENT = "formualizer-release-preflight/1 (https://github.com/psu3d0/formualizer)"
DIRECT_SECRET_ENV = frozenset({"CARGO_REGISTRY_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"})


@dataclass(frozen=True)
class Package:
    name: str
    manifest: str

    def version(self) -> str:
        data = tomllib.loads((ROOT / self.manifest).read_text(encoding="utf-8"))
        return str(data["package"]["version"])

    def archive(self) -> Path:
        return TARGET / "package" / f"{self.name}-{self.version()}.crate"


COMMON = Package("formualizer-common", "crates/formualizer-common/Cargo.toml")
PARSE = Package("formualizer-parse", "crates/formualizer-parse/Cargo.toml")
SPEC = Package("sheetport-spec", "crates/sheetport-spec/Cargo.toml")
MACROS = Package("formualizer-macros", "crates/formualizer-macros/Cargo.toml")
EVAL = Package("formualizer-eval", "crates/formualizer-eval/Cargo.toml")
WORKBOOK = Package("formualizer-workbook", "crates/formualizer-workbook/Cargo.toml")
SHEETPORT = Package("formualizer-sheetport", "crates/formualizer-sheetport/Cargo.toml")
FORMUALIZER = Package("formualizer", "crates/formualizer/Cargo.toml")

TRACKS: dict[str, tuple[Package, ...]] = {
    "parse": (COMMON, PARSE),
    "spec": (SPEC,),
    "product": (COMMON, PARSE, SPEC, MACROS, EVAL, WORKBOOK, SHEETPORT, FORMUALIZER),
}

# Cargo-generated metadata varies with the packaging Cargo version or source
# commit. Cargo.toml.orig and every shipped source/data/doc file remain in the
# comparison, so dependency requirements and payload behavior are still covered.
DRIFT_EXCLUDES = frozenset({".cargo_vcs_info.json", "Cargo.toml", "Cargo.lock"})


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def command_output(command: list[str], *, env: dict[str, str] | None = None) -> str:
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True).strip()


def credential_free_environment() -> dict[str, str]:
    env = os.environ.copy()
    for key in list(env):
        if key in DIRECT_SECRET_ENV or (
            key.startswith("CARGO_REGISTRIES_") and key.endswith("_TOKEN")
        ):
            env.pop(key)
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def assert_safe_project_path(path: Path) -> None:
    """Refuse symlinks for persistent preflight state below the repo target."""

    target = TARGET
    if target.exists() and target.is_symlink():
        raise RuntimeError(f"refusing symlinked target directory: {target}")
    try:
        relative = path.relative_to(target)
    except ValueError as exc:
        raise RuntimeError(f"persistent path is outside target: {path}") from exc
    cursor = target
    for part in relative.parts:
        cursor /= part
        if cursor.exists() and cursor.is_symlink():
            raise RuntimeError(f"refusing symlinked preflight path: {cursor}")


def reject_tree_symlinks(root: Path) -> None:
    if not root.exists():
        return
    for directory, names, files in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in [*names, *files]:
            candidate = directory_path / name
            if candidate.is_symlink():
                raise RuntimeError(
                    f"refusing symlink in persistent preflight state: {candidate}"
                )


def ensure_clean(allow_dirty: bool) -> None:
    status = command_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"]
    )
    if status and not allow_dirty:
        raise RuntimeError(
            "release preflight requires a clean checkout; commit/revert changes or pass "
            "--allow-dirty for development-only validation\n" + status
        )


@contextlib.contextmanager
def exclusive_lock() -> Iterator[None]:
    assert_safe_project_path(LOCK_PATH)
    TARGET.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a+", encoding="utf-8") as lock_file:
        print("Waiting for exclusive release-preflight lock...", flush=True)
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        print("Acquired release-preflight lock.", flush=True)
        yield


def ensure_local_registry_tool() -> Path:
    assert_safe_project_path(TOOL_BIN)
    assert_safe_project_path(TOOL_CARGO_HOME)
    reject_tree_symlinks(TOOL_ROOT)
    reject_tree_symlinks(TOOL_CARGO_HOME)
    if TOOL_BIN.exists():
        actual = command_output([str(TOOL_BIN), "--version"])
        if actual == f"cargo-local-registry {TOOL_VERSION}":
            return TOOL_BIN
        raise RuntimeError(f"unexpected tool at {TOOL_BIN}: {actual}")

    TOOL_ROOT.parent.mkdir(parents=True, exist_ok=True)
    TOOL_CARGO_HOME.mkdir(parents=True, exist_ok=True)
    install_root = Path(
        tempfile.mkdtemp(prefix="cargo-local-registry-install-", dir=TOOL_ROOT.parent)
    )
    try:
        env = credential_free_environment()
        env["CARGO_HOME"] = str(TOOL_CARGO_HOME)
        run(
            [
                "cargo",
                f"+{TOOLCHAIN}",
                "install",
                "cargo-local-registry",
                "--version",
                TOOL_VERSION,
                "--locked",
                "--root",
                str(install_root),
            ],
            env=env,
        )
        candidate = install_root / "bin" / "cargo-local-registry"
        actual = command_output([str(candidate), "--version"])
        if actual != f"cargo-local-registry {TOOL_VERSION}":
            raise RuntimeError(f"installed unexpected cargo-local-registry: {actual}")
        if TOOL_ROOT.exists():
            raise RuntimeError(f"tool destination appeared concurrently: {TOOL_ROOT}")
        install_root.rename(TOOL_ROOT)
    finally:
        if install_root.exists():
            shutil.rmtree(install_root)
    return TOOL_BIN


def registry_index_path(registry: Path, name: str) -> Path:
    name = name.lower()
    if len(name) == 1:
        relative = Path("1") / name
    elif len(name) == 2:
        relative = Path("2") / name
    elif len(name) == 3:
        relative = Path("3") / name[0] / name
    else:
        relative = Path(name[:2]) / name[2:4] / name
    return registry / "index" / relative


def read_archive_file(archive: Path, relative: str) -> bytes:
    root = f"{archive.name.removesuffix('.crate')}"
    member_name = f"{root}/{relative}"
    with tarfile.open(archive, "r:gz") as tf:
        try:
            member = tf.getmember(member_name)
        except KeyError as exc:
            raise RuntimeError(f"archive is missing {member_name}") from exc
        fileobj = tf.extractfile(member)
        if fileobj is None:
            raise RuntimeError(f"archive member is not a file: {member_name}")
        return fileobj.read()


def dependency_records(
    table: dict[str, Any], kind: str, target: str | None
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for alias, raw in sorted(table.items()):
        data = raw if isinstance(raw, dict) else {"version": raw}
        item: dict[str, Any] = {
            "name": alias,
            "req": str(data.get("version", "*")),
            "features": list(data.get("features", [])),
            "optional": bool(data.get("optional", False)),
            "default_features": bool(data.get("default-features", True)),
            "target": target,
            "kind": kind,
        }
        if "package" in data:
            item["package"] = str(data["package"])
        if "registry" in data:
            item["registry"] = str(data["registry"])
        records.append(item)
    return records


def index_record_from_archive(archive: Path) -> dict[str, Any]:
    manifest = tomllib.loads(read_archive_file(archive, "Cargo.toml").decode("utf-8"))
    package = manifest["package"]
    dependencies: list[dict[str, Any]] = []
    dependencies.extend(
        dependency_records(manifest.get("dependencies", {}), "normal", None)
    )
    dependencies.extend(
        dependency_records(manifest.get("dev-dependencies", {}), "dev", None)
    )
    dependencies.extend(
        dependency_records(manifest.get("build-dependencies", {}), "build", None)
    )
    for target, tables in sorted(manifest.get("target", {}).items()):
        dependencies.extend(
            dependency_records(tables.get("dependencies", {}), "normal", target)
        )
        dependencies.extend(
            dependency_records(tables.get("dev-dependencies", {}), "dev", target)
        )
        dependencies.extend(
            dependency_records(tables.get("build-dependencies", {}), "build", target)
        )

    ordinary_features: dict[str, list[str]] = {}
    namespaced_features: dict[str, list[str]] = {}
    for feature, values in sorted(manifest.get("features", {}).items()):
        values = list(values)
        if any(value.startswith("dep:") or "?/" in value for value in values):
            namespaced_features[feature] = values
        else:
            ordinary_features[feature] = values

    record: dict[str, Any] = {
        "name": str(package["name"]),
        "vers": str(package["version"]),
        "deps": dependencies,
        "cksum": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "features": ordinary_features,
        "yanked": False,
    }
    if namespaced_features:
        record["features2"] = namespaced_features
        record["v"] = 2
    if package.get("links"):
        record["links"] = str(package["links"])
    if package.get("rust-version"):
        record["rust_version"] = str(package["rust-version"])
    return record


def registry_has_version(registry: Path, name: str, version: str) -> bool:
    index_path = registry_index_path(registry, name)
    if not index_path.exists():
        return False
    return any(
        json.loads(line).get("vers") == version
        for line in index_path.read_text(encoding="utf-8").splitlines()
        if line
    )


def add_archive_to_registry(registry: Path, archive: Path) -> dict[str, Any]:
    record = index_record_from_archive(archive)
    destination = registry / archive.name
    shutil.copyfile(archive, destination)
    index_path = registry_index_path(registry, record["name"])
    index_path.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict[str, Any]] = []
    if index_path.exists():
        existing = [
            json.loads(line) for line in index_path.read_text().splitlines() if line
        ]
    existing = [item for item in existing if item.get("vers") != record["vers"]]
    existing.append(record)
    existing.sort(key=lambda item: item["vers"])
    index_path.write_text(
        "".join(
            json.dumps(item, separators=(",", ":"), sort_keys=True) + "\n"
            for item in existing
        ),
        encoding="utf-8",
    )
    print(f"Staged {record['name']} {record['vers']} ({record['cksum']})", flush=True)
    return record


def archive_payload(archive: Path) -> dict[str, str]:
    """Return stable shipped-file hashes and reject unsafe/ambiguous members."""

    expected_root = archive.name.removesuffix(".crate")
    payload: dict[str, str] = {}
    with tarfile.open(archive, "r:gz") as tf:
        for member in tf.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise RuntimeError(f"unsafe archive path: {member.name}")
            if not path.parts or path.parts[0] != expected_root:
                raise RuntimeError(f"unexpected archive root: {member.name}")
            if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                raise RuntimeError(f"unsafe archive member type: {member.name}")
            if member.isdir():
                continue
            if not member.isfile():
                raise RuntimeError(f"unsupported archive member type: {member.name}")
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            if relative in DRIFT_EXCLUDES:
                continue
            if relative in payload:
                raise RuntimeError(f"duplicate archive member: {relative}")
            fileobj = tf.extractfile(member)
            if fileobj is None:
                raise RuntimeError(f"could not read archive member: {member.name}")
            payload[relative] = hashlib.sha256(fileobj.read()).hexdigest()
    return payload


def crates_io_version(name: str, version: str) -> dict[str, Any] | None:
    url = f"https://crates.io/api/v1/crates/{name}/{version}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)["version"]
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def download_registry_archive(
    name: str, version: str, destination: Path, expected: str
) -> None:
    url = f"https://crates.io/api/v1/crates/{name}/{version}/download"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with (
        urllib.request.urlopen(request, timeout=60) as response,
        destination.open("wb") as output,
    ):
        shutil.copyfileobj(response, output)
    actual = hashlib.sha256(destination.read_bytes()).hexdigest()
    if actual != expected:
        raise RuntimeError(
            f"registry checksum mismatch for {name} {version}: expected {expected}, got {actual}"
        )


def check_same_version_drift(
    package: Package, local_archive: Path, downloads: Path
) -> str:
    version = package.version()
    metadata = crates_io_version(package.name, version)
    if metadata is None:
        print(f"No crates.io collision for {package.name} {version}.", flush=True)
        return "new"

    registry_archive = downloads / f"{package.name}-{version}.crate"
    download_registry_archive(
        package.name, version, registry_archive, str(metadata["checksum"])
    )
    local_payload = archive_payload(local_archive)
    registry_payload = archive_payload(registry_archive)
    if local_payload != registry_payload:
        local_names = set(local_payload)
        registry_names = set(registry_payload)
        added = sorted(local_names - registry_names)
        removed = sorted(registry_names - local_names)
        changed = sorted(
            name
            for name in local_names & registry_names
            if local_payload[name] != registry_payload[name]
        )
        details = [
            f"same-version source drift for {package.name} {version}",
            f"  added: {added[:20]}",
            f"  removed: {removed[:20]}",
            f"  changed: {changed[:20]}",
            "bump the package version or restore the published payload before release",
        ]
        raise RuntimeError("\n".join(details))
    print(f"Published payload matches {package.name} {version}.", flush=True)
    return "matching"


def staging_environment(cargo_home: Path, registry: Path) -> dict[str, str]:
    cargo_home.mkdir(parents=True)
    config = f"""[source.crates-io]\nreplace-with = "formualizer-preflight"\n\n[source.formualizer-preflight]\nlocal-registry = {json.dumps(str(registry))}\n\n[net]\ngit-fetch-with-cli = true\n"""
    (cargo_home / "config.toml").write_text(config, encoding="utf-8")
    env = credential_free_environment()
    env["CARGO_HOME"] = str(cargo_home)
    return env


def package_one(
    package: Package, env: dict[str, str] | None, *, allow_dirty: bool
) -> Path:
    archive = package.archive()
    archive.parent.mkdir(parents=True, exist_ok=True)
    if archive.exists():
        archive.unlink()
    command = ["cargo", "package", "-p", package.name, "--locked"]
    if allow_dirty:
        command.append("--allow-dirty")
    run(command, env=env)
    if not archive.is_file():
        raise RuntimeError(f"cargo package did not produce {archive}")
    archive_payload(archive)
    record = index_record_from_archive(archive)
    if record["name"] != package.name or record["vers"] != package.version():
        raise RuntimeError(f"archive identity mismatch for {package.name}: {record}")
    return archive


def seed_patched_registry_dependencies(
    tool: Path,
    registry: Path,
    env: dict[str, str],
    packages: tuple[Package, ...],
) -> None:
    """Add crates whose registry lock entry was replaced by a git/path patch."""

    metadata = json.loads(
        command_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            env=credential_free_environment(),
        )
    )
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
    locked: dict[str, set[str]] = {}
    for package in lock.get("package", []):
        locked.setdefault(str(package["name"]), set()).add(str(package["version"]))

    requirements: dict[str, set[str]] = {}
    package_names = {package.name for package in packages}
    for metadata_package in metadata["packages"]:
        if metadata_package["name"] not in package_names:
            continue
        for dependency in metadata_package["dependencies"]:
            source = dependency.get("source") or ""
            if not source.startswith("registry+"):
                continue
            requirements.setdefault(str(dependency["name"]), set()).add(
                str(dependency["req"])
            )

    for name, reqs in sorted(requirements.items()):
        candidates = locked.get(name, set())
        exact = {req[1:] for req in reqs if req.startswith("=")}
        versions = sorted(candidates & exact if exact else candidates)
        for version in versions:
            if registry_has_version(registry, name, version):
                continue
            print(
                f"Seeding registry fallback for patched dependency {name} {version}.",
                flush=True,
            )
            run(
                [str(tool), "add", name, "--version", version, str(registry)],
                env=env,
            )


def prepare_local_registry(
    temp_root: Path, packages: tuple[Package, ...]
) -> tuple[Path, dict[str, str]]:
    tool = ensure_local_registry_tool()
    registry = temp_root / "registry"
    cargo_home = temp_root / "cargo-home"
    registry.mkdir()
    env = credential_free_environment()
    env["CARGO_HOME"] = str(TOOL_CARGO_HOME)
    run([str(tool), "sync", str(ROOT / "Cargo.lock"), str(registry)], env=env)
    seed_patched_registry_dependencies(tool, registry, env, packages)
    return registry, staging_environment(cargo_home, registry)


def preflight(track: str, allow_dirty: bool) -> None:
    ensure_clean(allow_dirty)
    packages = TRACKS[track]
    with (
        exclusive_lock(),
        tempfile.TemporaryDirectory(
            prefix=f"formualizer-{track}-preflight-"
        ) as temp_name,
    ):
        temp_root = Path(temp_name)
        downloads = temp_root / "downloads"
        downloads.mkdir()
        registry: Path | None = None
        package_env = credential_free_environment()
        if len(packages) > 1:
            registry, package_env = prepare_local_registry(temp_root, packages)

        results: list[dict[str, str]] = []
        for package in packages:
            archive = package_one(package, package_env, allow_dirty=allow_dirty)
            collision = check_same_version_drift(package, archive, downloads)
            if registry is not None:
                add_archive_to_registry(registry, archive)
            results.append(
                {
                    "name": package.name,
                    "version": package.version(),
                    "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                    "registry_collision": collision,
                }
            )

        print(json.dumps({"track": track, "packages": results}, indent=2), flush=True)
        print(f"Release preflight passed for {track}.", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--track", required=True, choices=sorted(TRACKS))
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="permit uncommitted source for development validation; never use for a release tag",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        preflight(args.track, args.allow_dirty)
    except (
        OSError,
        RuntimeError,
        ValueError,
        subprocess.CalledProcessError,
        tarfile.TarError,
    ) as exc:
        print(f"release preflight failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
