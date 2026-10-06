#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

# Stop all CROWDB services owned by this user, including unrecorded children.
# Match executable names so a shell or editor mentioning CROWDB is not killed.
crowdb_pids() {
    ps -u "$(id -u)" -o pid=,comm=,stat= |
        awk '$2 ~ /^crowdb-/ && $3 !~ /^Z/ { print $1 }'
}

collect_service_pids() {
    service_pids=()
    while IFS= read -r pid; do
        [ -n "$pid" ] && service_pids+=("$pid")
    done < <(crowdb_pids)
}

echo "[clean] stop CROWDB services"
collect_service_pids
if [ "${#service_pids[@]}" -gt 0 ]; then
    kill -TERM "${service_pids[@]}" 2>/dev/null || true
    sleep 0.2
fi
for _ in {1..20}; do
    collect_service_pids
    [ "${#service_pids[@]}" -gt 0 ] || break
    kill -KILL "${service_pids[@]}" 2>/dev/null || true
    sleep 0.1
done
collect_service_pids
if [ "${#service_pids[@]}" -gt 0 ]; then
    echo "[clean] CROWDB processes still alive: ${service_pids[*]}" >&2
    exit 1
fi

echo "[clean] complete runtime tree"
rm -rf -- "$PIXI_PROJECT_ROOT/.crowdb-runtime"

# ── Rust build artifacts ──
echo "[clean] cargo clean"
cargo clean
echo "[clean] tarpaulin report"
rm -f tarpaulin-report.html

# ── C++ build artifacts (crowdb-tree, crowdb-rpc, crowdb-diskio) ──
echo "[clean] C++ build dirs"
rm -rf lib/crowdb-tree/build lib/crowdb-tree/build-*
rm -rf lib/crowdb-tree/Testing lib/crowdb-tree/.cache
rm -rf lib/crowdb-rpc/build lib/crowdb-rpc/build-* lib/crowdb-rpc/.cache
rm -rf app/crowdb-diskio/build app/crowdb-diskio/build-* app/crowdb-diskio/.cache

# ── Frontend build artifacts (keep node_modules) ──
echo "[clean] frontend build artifacts"
rm -rf app/crowdb-web/ui/dist app/crowdb-web/ui/.vite app/crowdb-web/ui/test-results
rm -f app/crowdb-web/ui/*.tsbuildinfo

# ── find-based cleanup (run last, after rm steps shrink the tree) ──
# Prune heavy non-source subtrees (.pixi, node_modules, target, .git) so find
# only walks the source tree instead of 60k+ vendored/compiled entries.
echo "[clean] mutants.out"
find . \( -name .pixi -o -name node_modules -o -name target -o -name .git \) -prune \
  -o -type d -name mutants.out -print -exec rm -rf {} \; 2>/dev/null || true

# ── OS junk ──
# Avoid -delete (implies -depth on BSD/macOS, breaks -prune → full traversal).
echo "[clean] .DS_Store"
find . \( -name .pixi -o -name node_modules -o -name target -o -name .git \) -prune \
  -o -name .DS_Store -print0 2>/dev/null | xargs -0 rm -f 2>/dev/null || true

echo "[clean] done"
