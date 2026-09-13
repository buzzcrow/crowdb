#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source tools/bench-kv-write-sentinel.sh

validate_largeval_result 0 0 0 1 0
for fixture in \
    "1 0 0 1 0" \
    "0 1 0 1 0" \
    "0 0 1 1 0" \
    "0 0 0 0 0" \
    "0 0 0 1 1"
do
    if validate_largeval_result $fixture; then
        echo "invalid fixture passed: $fixture" >&2
        exit 1
    fi
done
echo "large-value sentinel fixtures passed"
