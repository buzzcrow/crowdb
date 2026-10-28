#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

git config core.hooksPath .githooks
bashrc="${HOME:?}/.bashrc"
if [[ ! -f "$bashrc" ]] || ! grep -Eq '^[[:space:]]*(source|\.)[[:space:]]+<\(pixi completion (--shell[ =]|-s[ =])bash\)' "$bashrc"; then
    cat >> "$bashrc" <<'EOF'

# Complete Pixi commands and workspace task names.
if command -v pixi >/dev/null 2>&1; then
  source <(pixi completion --shell bash)
fi
EOF
    echo "[install-deps] enabled Pixi Bash completion in $bashrc; open a new terminal to use it"
fi
if [[ $(uname -sm) == 'Linux x86_64' ]]; then
    pixi run --manifest-path container/oci-image/pixi.toml python container/oci-image/preflight.py --setup-apparmor
fi
if [[ ! -d app/crowdb-web/ui/node_modules ]]; then
    (cd app/crowdb-web/ui && npm ci)
fi
cargo install cargo-tarpaulin || echo "Warning: cargo-tarpaulin install failed (expected on macOS)"
cargo install --locked samply || echo "Warning: samply install failed"
cargo install --locked inferno || echo "Warning: inferno install failed"
