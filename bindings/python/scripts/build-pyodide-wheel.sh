#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PYODIDE_CMD=(uvx --from pyodide-cli --with pyodide-build pyodide)

# Target a specific Pyodide runtime version. The `pyodide_<abi>_wasm32` tag
# and downstream config values all derive from this pin. Override at invocation
# time if you need to retarget: `PYODIDE_XBUILDENV_VERSION=0.30.0 ./scripts/...`.
PYODIDE_XBUILDENV_VERSION="${PYODIDE_XBUILDENV_VERSION:-0.29.3}"

printf 'Preparing Pyodide wheel build\n'
printf '  xbuildenv:   %s\n' "$PYODIDE_XBUILDENV_VERSION"

# Install xbuildenv first — subsequent `pyodide config get ...` reads its values
# from the xbuildenv directory, so this step is load-bearing for the queries
# below and must not be reordered. `--force` is needed because pyodide-build
# 0.34.x reports every xbuildenv version as "incompatible" in its search table,
# even though installation works correctly.
"${PYODIDE_CMD[@]}" xbuildenv install --force "$PYODIDE_XBUILDENV_VERSION"

PYTHON_VERSION="$(${PYODIDE_CMD[@]} config get python_version)"
PYODIDE_ABI_VERSION="$(${PYODIDE_CMD[@]} config get pyodide_abi_version)"
EMSCRIPTEN_VERSION="$(${PYODIDE_CMD[@]} config get emscripten_version)"
RUST_TOOLCHAIN="$(${PYODIDE_CMD[@]} config get rust_toolchain)"
RUST_EMSCRIPTEN_TARGET_URL="$(${PYODIDE_CMD[@]} config get rust_emscripten_target_url)"
PYODIDE_RUSTFLAGS="$(${PYODIDE_CMD[@]} config get rustflags)"
PYODIDE_CFLAGS="$(${PYODIDE_CMD[@]} config get cflags)"
PYODIDE_CXXFLAGS="$(${PYODIDE_CMD[@]} config get cxxflags)"
PYODIDE_LDFLAGS="$(${PYODIDE_CMD[@]} config get ldflags)"

printf '  python:      %s\n' "$PYTHON_VERSION"
printf '  abi:         pyodide_%s\n' "$PYODIDE_ABI_VERSION"
printf '  emscripten:  %s\n' "$EMSCRIPTEN_VERSION"
printf '  rust:        %s\n' "$RUST_TOOLCHAIN"
printf '  eh-sysroot:  %s\n' "$RUST_EMSCRIPTEN_TARGET_URL"

if ! command -v rustup >/dev/null 2>&1; then
    echo 'rustup is required to install the Pyodide wasm-EH rust sysroot.' >&2
    exit 1
fi

if ! rustup toolchain list | grep -Fq "$RUST_TOOLCHAIN"; then
    rustup toolchain install "$RUST_TOOLCHAIN" --profile minimal
fi
export RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN"

# Replace the stock wasm32-unknown-emscripten sysroot with Pyodide's wasm-EH
# sysroot. Stock rustup ships a std built with JS-trampoline exceptions
# (invoke_*), which fails to import under Pyodide 0.29+ (expects wasm EH).
RUSTC_SYSROOT="$(rustc --print sysroot)"
RUSTLIB_DIR="$RUSTC_SYSROOT/lib/rustlib"
EH_TARGET_DIR="$RUSTLIB_DIR/wasm32-unknown-emscripten"
EH_MARKER="$EH_TARGET_DIR/.pyodide-wasm-eh.sentinel"
EH_EXPECTED_TAG="$(basename "$RUST_EMSCRIPTEN_TARGET_URL" .tar.bz2)"

if [[ ! -f "$EH_MARKER" || "$(cat "$EH_MARKER" 2>/dev/null)" != "$EH_EXPECTED_TAG" ]]; then
    printf 'Installing Pyodide wasm-EH rust sysroot (%s)\n' "$EH_EXPECTED_TAG"
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' RETURN
    tarball="$tmpdir/sysroot.tar.bz2"
    curl -fL --retry 3 -o "$tarball" "$RUST_EMSCRIPTEN_TARGET_URL"
    mkdir -p "$RUSTLIB_DIR"
    rm -rf "$EH_TARGET_DIR"
    tar -xjf "$tarball" -C "$RUSTLIB_DIR"
    printf '%s' "$EH_EXPECTED_TAG" > "$EH_MARKER"
fi

rm -rf dist/pyodide
mkdir -p dist/pyodide

# Purge any previously cached wasm build — cargo will otherwise reuse
# artifacts compiled with the stock (non-wasm-EH) sysroot and flags.
WORKSPACE_ROOT="$(cargo locate-project --workspace --message-format plain | xargs dirname)"
rm -rf "$WORKSPACE_ROOT/target/wasm32-unknown-emscripten"

# Export the canonical Pyodide toolchain flags so `pyodide build` (which,
# unlike `pyodide build-recipes`, doesn't inject them automatically) emits
# wasm-EH-compatible output.
export RUSTFLAGS="$PYODIDE_RUSTFLAGS"
export CFLAGS="$PYODIDE_CFLAGS"
export CXXFLAGS="$PYODIDE_CXXFLAGS"
export LDFLAGS="$PYODIDE_LDFLAGS"

printf 'RUSTFLAGS: %s\n' "$RUSTFLAGS"
printf 'CFLAGS:    %s\n' "$CFLAGS"
printf 'LDFLAGS:   %s\n' "$LDFLAGS"

"${PYODIDE_CMD[@]}" build --outdir dist/pyodide

wheel_path="$(find dist/pyodide -maxdepth 1 -name '*.whl' | sort | head -n 1)"
if [[ -z "$wheel_path" ]]; then
    echo 'Pyodide build did not produce a wheel.' >&2
    exit 1
fi

# pyodide-build 0.34 repacks wheels with the forward-looking
# `pyemscripten_2025_0_wasm32` tag, which the micropip shipped with
# Pyodide 0.29.x parses as a bogus "Emscripten v pyemscripten.2025.0"
# string and rejects. The actual tag Pyodide 0.29.x expects (matching
# its own lockfile) is `pyodide_2025_0_wasm32`. Retag to that so
# `micropip.install(...)` accepts the wheel without falling back to
# zip extraction.
CANONICAL_PLATFORM_TAG="pyodide_${PYODIDE_ABI_VERSION}_wasm32"
current_plat_tag="$(basename "$wheel_path" .whl | awk -F- '{print $NF}')"
if [[ "$current_plat_tag" != "$CANONICAL_PLATFORM_TAG" ]]; then
    printf 'Retagging wheel platform tag: %s -> %s\n' "$current_plat_tag" "$CANONICAL_PLATFORM_TAG"
    uvx --from wheel -- wheel tags --remove --platform-tag "$CANONICAL_PLATFORM_TAG" "$wheel_path" >/dev/null
    wheel_path="$(find dist/pyodide -maxdepth 1 -name '*.whl' | sort | head -n 1)"
fi

printf 'Built wheel: %s\n' "$wheel_path"
