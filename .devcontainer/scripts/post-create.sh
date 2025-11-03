#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEVCONTAINER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$DEVCONTAINER_DIR/.." && pwd)"
TMUX_CONF_SOURCE="${DEVCONTAINER_DIR}/tmux.conf"
ZSH_SNIPPET_SOURCE="${DEVCONTAINER_DIR}/zshrc.snippet"

sudo apt-get update
sudo apt-get install -y \
  tmux \
  curl \
  zsh \
  fzf \
  unzip
sudo apt-get clean

export PATH="${HOME}/.local/bin:${PATH}"

mkdir -p "${HOME}/.cache" "${HOME}/.local/share" "${HOME}/.local/state"
chown -R vscode:vscode "${HOME}/.cache" "${HOME}/.local/share" "${HOME}/.local/state"

if command -v mise >/dev/null 2>&1 && [ -f "${WORKSPACE_DIR}/mise.toml" ]; then
  mise trust "${WORKSPACE_DIR}/mise.toml"
fi

mise install

ZSHRC="${HOME}/.zshrc"

# Tmux configuration and plugins
mkdir -p "${HOME}/.tmux/plugins"

if [ -f "${TMUX_CONF_SOURCE}" ]; then
  if ! grep -q "formualizer-devcontainer managed tmux config" "${HOME}/.tmux.conf" 2>/dev/null; then
    cp "${TMUX_CONF_SOURCE}" "${HOME}/.tmux.conf"
  fi
fi
mkdir -p "${HOME}/.tmux/resurrect"

# Oh My Zsh installation
if [ ! -d "${HOME}/.oh-my-zsh" ]; then
  RUNZSH=no CHSH=no KEEP_ZSHRC=yes sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
fi

ZSHRC="${HOME}/.zshrc"

if [ ! -f "${ZSHRC}" ]; then
  cp "${HOME}/.oh-my-zsh/templates/zshrc.zsh-template" "${ZSHRC}"
fi

uv run python <<'PY'
import os
import pathlib
import re

zshrc_path = pathlib.Path(os.environ.get("ZSHRC", os.path.expanduser("~/.zshrc")))
content = zshrc_path.read_text()

if re.search(r'^ZSH_THEME=.*$', content, flags=re.MULTILINE):
    content = re.sub(
        r'^ZSH_THEME=.*$',
        'ZSH_THEME="robbyrussell"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
else:
    content += '\nZSH_THEME="robbyrussell"\n'

if re.search(r'^plugins=.*$', content, flags=re.MULTILINE):
    content = re.sub(
        r'^plugins=.*$',
        'plugins=(git z tmux ssh-agent fzf)',
        content,
        count=1,
        flags=re.MULTILINE,
    )
else:
    content += '\nplugins=(git z tmux ssh-agent)\n'

zshrc_path.write_text(content)
PY

if [ -f "${ZSH_SNIPPET_SOURCE}" ] && ! grep -q "formualizer-devcontainer oh-my-zsh snippet" "${ZSHRC}" 2>/dev/null; then
  cat "${ZSH_SNIPPET_SOURCE}" >> "${ZSHRC}"
fi

sudo chsh -s "$(command -v zsh)" vscode
