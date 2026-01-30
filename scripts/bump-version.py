#!/usr/bin/env python3
"""
Version bump script for Formualizer monorepo.

Handles three release tracks per docs/packaging-and-releases.md:
  - product: Rust product crates + Python + npm (must all match)
  - parse:   formualizer-common + formualizer-parse (SDK track)
  - spec:    sheetport-spec (independent spec track)

Usage:
    ./scripts/bump-version.py --track product --version 0.4.0
    ./scripts/bump-version.py --track parse --version 1.1.0
    ./scripts/bump-version.py --track spec --version 0.4.0
    ./scripts/bump-version.py --track product --version 0.4.0 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable

# Repository root (script lives in scripts/)
REPO_ROOT = Path(__file__).resolve().parent.parent

# =============================================================================
# Track definitions
# =============================================================================

# Product track: roll-up crate + bindings + internal product crates
PRODUCT_PACKAGE_VERSION_FILES = [
    # Primary version locations (must all match)
    ("crates/formualizer/Cargo.toml", "toml", ["package", "version"]),
    ("bindings/python/pyproject.toml", "toml", ["project", "version"]),
    ("bindings/wasm/package.json", "json", ["version"]),
    # WASM binding crate
    ("bindings/wasm/Cargo.toml", "toml", ["package", "version"]),
]

# Workspace dependency versions that track product version
PRODUCT_WORKSPACE_DEPS = []

# Internal dependency references in product crates (dep name -> new version)
# These are dependencies with explicit version = "X.Y.Z" alongside path = "..."
PRODUCT_INTERNAL_DEPS = []

# Parser/SDK track
PARSE_PACKAGE_VERSION_FILES = [
    ("crates/formualizer-common/Cargo.toml", "toml", ["package", "version"]),
    ("crates/formualizer-parse/Cargo.toml", "toml", ["package", "version"]),
]

PARSE_WORKSPACE_DEPS = [
    ("Cargo.toml", "formualizer-common"),
    ("Cargo.toml", "formualizer-parse"),
]

# Internal dependency references for parse track
PARSE_INTERNAL_DEPS = [
    # crates/formualizer/Cargo.toml references to common/parse
    ("crates/formualizer/Cargo.toml", "formualizer-common"),
    ("crates/formualizer/Cargo.toml", "formualizer-parse"),
    # crates/formualizer-sheetport/Cargo.toml references
    ("crates/formualizer-sheetport/Cargo.toml", "formualizer-common"),
    ("crates/formualizer-sheetport/Cargo.toml", "formualizer-parse"),
]

# Spec track (standalone)
SPEC_PACKAGE_VERSION_FILES = [
    ("crates/sheetport-spec/Cargo.toml", "toml", ["package", "version"]),
]


# =============================================================================
# File manipulation helpers
# =============================================================================


def read_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def validate_semver(version: str) -> bool:
    """Validate version string is valid semver (X.Y.Z with optional prerelease)."""
    pattern = r"^\d+\.\d+\.\d+(-[a-zA-Z0-9]+(\.[a-zA-Z0-9]+)*)?$"
    return bool(re.match(pattern, version))


def parse_semver(version: str) -> tuple[int, int, int, str]:
    """Parse semver into (major, minor, patch, prerelease)."""
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)(-.*)?$", version)
    if not match:
        raise ValueError(f"Invalid semver: {version}")
    major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))
    prerelease = match.group(4) or ""
    return major, minor, patch, prerelease


def compare_versions(old: str, new: str) -> int:
    """Compare two semver versions. Returns -1 if old < new, 0 if equal, 1 if old > new."""
    old_parts = parse_semver(old)
    new_parts = parse_semver(new)

    # Compare major.minor.patch
    for o, n in zip(old_parts[:3], new_parts[:3]):
        if o < n:
            return -1
        if o > n:
            return 1

    # Same major.minor.patch - compare prerelease
    # No prerelease > prerelease (e.g., 1.0.0 > 1.0.0-rc.1)
    old_pre, new_pre = old_parts[3], new_parts[3]
    if old_pre and not new_pre:
        return -1  # old is prerelease, new is release -> upgrade
    if not old_pre and new_pre:
        return 1  # old is release, new is prerelease -> downgrade
    if old_pre < new_pre:
        return -1
    if old_pre > new_pre:
        return 1
    return 0


# =============================================================================
# TOML manipulation (regex-based to preserve formatting)
# =============================================================================


def update_toml_package_version(content: str, new_version: str) -> str:
    """Update [package] version = "..." in a Cargo.toml."""
    # Match version = "..." after [package] section
    pattern = r'(\[package\][^\[]*?version\s*=\s*)"[^"]*"'
    replacement = rf'\1"{new_version}"'
    new_content, count = re.subn(
        pattern, replacement, content, count=1, flags=re.DOTALL
    )
    if count == 0:
        raise ValueError("Could not find [package] version in TOML")
    return new_content


def update_toml_project_version(content: str, new_version: str) -> str:
    """Update [project] version = "..." in a pyproject.toml."""
    pattern = r'(\[project\][^\[]*?version\s*=\s*)"[^"]*"'
    replacement = rf'\1"{new_version}"'
    new_content, count = re.subn(
        pattern, replacement, content, count=1, flags=re.DOTALL
    )
    if count == 0:
        raise ValueError("Could not find [project] version in TOML")
    return new_content


def update_workspace_dep_version(content: str, dep_name: str, new_version: str) -> str:
    """Update a workspace dependency version in [workspace.dependencies]."""
    # Match: dep_name = { version = "X.Y.Z", ... }
    pattern = rf'({re.escape(dep_name)}\s*=\s*\{{\s*version\s*=\s*)"[^"]*"'
    replacement = rf'\1"{new_version}"'
    new_content, count = re.subn(pattern, replacement, content, count=1)
    if count == 0:
        raise ValueError(f"Could not find workspace dep {dep_name}")
    return new_content


def update_internal_dep_version(content: str, dep_name: str, new_version: str) -> str:
    """Update an internal dependency with path + version."""
    # Match: dep_name = { path = "...", version = "X.Y.Z", ... }
    # or: dep_name = { version = "X.Y.Z", path = "...", ... }
    # The version field can appear before or after path
    pattern = rf'({re.escape(dep_name)}\s*=\s*\{{[^}}]*?version\s*=\s*)"[^"]*"'
    replacement = rf'\1"{new_version}"'
    new_content, count = re.subn(pattern, replacement, content, count=1)
    if count == 0:
        # Dependency might not have explicit version (workspace = true)
        return content
    return new_content


# =============================================================================
# JSON manipulation
# =============================================================================


def update_json_version(content: str, new_version: str) -> str:
    """Update top-level "version" in a JSON file, preserving formatting."""
    data = json.loads(content)
    data["version"] = new_version
    # Preserve 2-space indent typical for package.json
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


# =============================================================================
# Version update orchestration
# =============================================================================


def get_current_version(rel_path: str, fmt: str, keys: list[str]) -> str:
    """Read current version from a manifest file."""
    path = REPO_ROOT / rel_path
    content = read_file(path)

    if fmt == "json":
        data = json.loads(content)
        for key in keys:
            data = data[key]
        return str(data)
    elif fmt == "toml":
        # Simple regex extraction for [section] version = "X.Y.Z"
        section = keys[0]
        pattern = rf'\[{section}\][^\[]*?version\s*=\s*"([^"]*)"'
        match = re.search(pattern, content, re.DOTALL)
        if match:
            return match.group(1)
        raise ValueError(f"Could not find [{section}] version in {rel_path}")
    else:
        raise ValueError(f"Unknown format: {fmt}")


def update_package_version(
    rel_path: str, fmt: str, keys: list[str], new_version: str, dry_run: bool
) -> tuple[str, str]:
    """Update package version in a manifest file. Returns (old_version, new_version)."""
    path = REPO_ROOT / rel_path
    content = read_file(path)
    old_version = get_current_version(rel_path, fmt, keys)

    if fmt == "json":
        new_content = update_json_version(content, new_version)
    elif fmt == "toml":
        section = keys[0]
        if section == "package":
            new_content = update_toml_package_version(content, new_version)
        elif section == "project":
            new_content = update_toml_project_version(content, new_version)
        else:
            raise ValueError(f"Unknown TOML section: {section}")
    else:
        raise ValueError(f"Unknown format: {fmt}")

    if not dry_run:
        write_file(path, new_content)

    return old_version, new_version


def update_workspace_deps(
    deps: list[tuple[str, str]], new_version: str, dry_run: bool
) -> list[str]:
    """Update workspace dependency versions. Returns list of updated deps."""
    updated = []
    files_content: dict[str, str] = {}

    for rel_path, dep_name in deps:
        path = REPO_ROOT / rel_path
        if rel_path not in files_content:
            files_content[rel_path] = read_file(path)

        try:
            files_content[rel_path] = update_workspace_dep_version(
                files_content[rel_path], dep_name, new_version
            )
            updated.append(f"{rel_path}: {dep_name}")
        except ValueError:
            pass  # Dep not found with explicit version

    if not dry_run:
        for rel_path, content in files_content.items():
            write_file(REPO_ROOT / rel_path, content)

    return updated


def update_internal_deps(
    deps: list[tuple[str, str]], new_version: str, dry_run: bool
) -> list[str]:
    """Update internal dependency versions (path + version). Returns list of updated deps."""
    updated = []
    files_content: dict[str, str] = {}

    for rel_path, dep_name in deps:
        path = REPO_ROOT / rel_path
        if rel_path not in files_content:
            files_content[rel_path] = read_file(path)

        old_content = files_content[rel_path]
        files_content[rel_path] = update_internal_dep_version(
            files_content[rel_path], dep_name, new_version
        )
        if files_content[rel_path] != old_content:
            updated.append(f"{rel_path}: {dep_name}")

    if not dry_run:
        for rel_path, content in files_content.items():
            write_file(REPO_ROOT / rel_path, content)

    return updated


# =============================================================================
# Track handlers
# =============================================================================


def check_version_regression(
    track_files: list[tuple[str, str, list[str]]], new_version: str, force: bool
) -> bool:
    """Check if new version is a regression from current. Returns True if OK to proceed."""
    # Get current version from first file in track
    rel_path, fmt, keys = track_files[0]
    current = get_current_version(rel_path, fmt, keys)

    cmp = compare_versions(current, new_version)
    if cmp > 0:
        # Regression detected
        print(f"ERROR: Version regression detected: {current} -> {new_version}")
        if force:
            print("WARNING: Proceeding anyway due to --force flag")
            return True
        print("Use --force to override this check")
        return False
    if cmp == 0:
        print(f"WARNING: Version unchanged: {current} -> {new_version}")
    return True


def bump_product(version: str, dry_run: bool, verify: bool, force: bool) -> bool:
    """Bump product track versions."""
    # Check for version regression
    if not check_version_regression(PRODUCT_PACKAGE_VERSION_FILES, version, force):
        return False

    print(f"{'[DRY RUN] ' if dry_run else ''}Bumping PRODUCT track to {version}")
    print("=" * 60)

    # 1. Update package versions
    print("\nPackage versions:")
    for rel_path, fmt, keys in PRODUCT_PACKAGE_VERSION_FILES:
        old, new = update_package_version(rel_path, fmt, keys, version, dry_run)
        status = "OK" if old != new else "unchanged"
        print(f"  {rel_path}: {old} -> {new} ({status})")

    # 2. Update workspace dependencies
    print("\nWorkspace dependencies:")
    updated_ws = update_workspace_deps(PRODUCT_WORKSPACE_DEPS, version, dry_run)
    for item in updated_ws:
        print(f"  {item} -> {version}")
    if not updated_ws:
        print("  (none)")

    # 3. Update internal dependencies
    print("\nInternal dependencies:")
    updated_int = update_internal_deps(PRODUCT_INTERNAL_DEPS, version, dry_run)
    for item in updated_int:
        print(f"  {item} -> {version}")
    if not updated_int:
        print("  (none)")

    # 4. Verify with cargo check
    if verify and not dry_run:
        print("\nRunning cargo check...")
        result = subprocess.run(
            ["cargo", "check", "--workspace"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("ERROR: cargo check failed:")
            print(result.stderr)
            return False
        print("  cargo check passed")

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Product track bump complete.")
    print(f"Next: git commit and tag with 'v{version}'")
    return True


def bump_parse(version: str, dry_run: bool, verify: bool, force: bool) -> bool:
    """Bump parser/SDK track versions."""
    # Check for version regression
    if not check_version_regression(PARSE_PACKAGE_VERSION_FILES, version, force):
        return False

    print(f"{'[DRY RUN] ' if dry_run else ''}Bumping PARSE track to {version}")
    print("=" * 60)

    # 1. Update package versions
    print("\nPackage versions:")
    for rel_path, fmt, keys in PARSE_PACKAGE_VERSION_FILES:
        old, new = update_package_version(rel_path, fmt, keys, version, dry_run)
        status = "OK" if old != new else "unchanged"
        print(f"  {rel_path}: {old} -> {new} ({status})")

    # 2. Update workspace dependencies
    print("\nWorkspace dependencies:")
    updated_ws = update_workspace_deps(PARSE_WORKSPACE_DEPS, version, dry_run)
    for item in updated_ws:
        print(f"  {item} -> {version}")
    if not updated_ws:
        print("  (none)")

    # 3. Update internal dependencies in downstream crates
    print("\nInternal dependencies (downstream crates):")
    updated_int = update_internal_deps(PARSE_INTERNAL_DEPS, version, dry_run)
    for item in updated_int:
        print(f"  {item} -> {version}")
    if not updated_int:
        print("  (none)")

    # 4. Verify with cargo check
    if verify and not dry_run:
        print("\nRunning cargo check...")
        result = subprocess.run(
            ["cargo", "check", "--workspace"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("ERROR: cargo check failed:")
            print(result.stderr)
            return False
        print("  cargo check passed")

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Parse track bump complete.")
    print(f"Next: git commit and tag with 'parse-v{version}'")
    return True


def bump_spec(version: str, dry_run: bool, verify: bool, force: bool) -> bool:
    """Bump spec track version."""
    # Check for version regression
    if not check_version_regression(SPEC_PACKAGE_VERSION_FILES, version, force):
        return False

    print(f"{'[DRY RUN] ' if dry_run else ''}Bumping SPEC track to {version}")
    print("=" * 60)

    # 1. Update package version
    print("\nPackage versions:")
    for rel_path, fmt, keys in SPEC_PACKAGE_VERSION_FILES:
        old, new = update_package_version(rel_path, fmt, keys, version, dry_run)
        status = "OK" if old != new else "unchanged"
        print(f"  {rel_path}: {old} -> {new} ({status})")

    # Note: sheetport-spec is also bumped in product track, but if doing
    # a standalone spec release, only bump the spec crate itself.
    # Downstream deps (formualizer-sheetport, formualizer) would be updated
    # in a subsequent product release.

    # 2. Verify with cargo check
    if verify and not dry_run:
        print("\nRunning cargo check on sheetport-spec...")
        result = subprocess.run(
            ["cargo", "check", "-p", "sheetport-spec"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("ERROR: cargo check failed:")
            print(result.stderr)
            return False
        print("  cargo check passed")

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Spec track bump complete.")
    print(f"Next: git commit and tag with 'sheetport-spec-v{version}'")
    return True


# =============================================================================
# CLI
# =============================================================================


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bump versions for Formualizer release tracks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --track product --version 0.4.0
  %(prog)s --track parse --version 1.1.0 --dry-run
  %(prog)s --track spec --version 0.4.0 --no-verify
  %(prog)s --track product --version 0.1.0 --force  # allow downgrade

Tracks:
  product  Rust product crates + Python + npm bindings
  parse    formualizer-common + formualizer-parse (SDK)
  spec     sheetport-spec (standalone)
""",
    )
    parser.add_argument(
        "--track",
        "-t",
        required=True,
        choices=["product", "parse", "spec"],
        help="Release track to bump",
    )
    parser.add_argument(
        "--version",
        "-v",
        required=True,
        help="New version (semver, e.g., 0.4.0 or 1.0.0-rc.1)",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Show what would be changed without modifying files",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip cargo check verification after bumping",
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Allow version regression (downgrade)",
    )

    args = parser.parse_args()

    # Validate version format
    if not validate_semver(args.version):
        print(f"ERROR: Invalid semver: {args.version}", file=sys.stderr)
        print("Expected format: X.Y.Z or X.Y.Z-prerelease", file=sys.stderr)
        return 1

    # Dispatch to track handler
    handlers: dict[str, Callable[[str, bool, bool, bool], bool]] = {
        "product": bump_product,
        "parse": bump_parse,
        "spec": bump_spec,
    }

    success = handlers[args.track](
        args.version,
        dry_run=args.dry_run,
        verify=not args.no_verify,
        force=args.force,
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
