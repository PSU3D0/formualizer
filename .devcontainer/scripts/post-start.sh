#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if command -v mise >/dev/null 2>&1; then
  # Ensure mise-managed tools are available for the rest of the session.
  eval "$(mise hook-env -s bash)"

  if [ -f "${WORKSPACE_DIR}/mise.toml" ]; then
    (cd "${WORKSPACE_DIR}" && mise install)
  fi

  mise use --global github:BurntSushi/ripgrep >/dev/null 2>&1 || true
  mise use --global npm:@anthropic-ai/claude-code >/dev/null 2>&1 || true
  mise use --global npm:@openai/codex >/dev/null 2>&1 || true
fi

tmux start-server
if ! tmux has-session -t dev 2>/dev/null; then
  tmux new -d -s dev
fi
