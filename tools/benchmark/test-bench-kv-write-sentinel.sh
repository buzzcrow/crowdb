#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
exec pixi run cargo test -p crowdb-cli --test kv_bench_test snapshot_sentinel
