#!/usr/bin/env bash
# Shared validation contract for the large-value snapshot sentinel.

validate_largeval_result() {
    local errors="$1" correctness_errors="$2" election_delta="$3"
    local snapshot_completed="$4" snapshot_failed="$5"
    [ "$errors" -eq 0 ] \
        && [ "$correctness_errors" -eq 0 ] \
        && [ "$election_delta" -eq 0 ] \
        && [ "$snapshot_completed" -gt 0 ] \
        && [ "$snapshot_failed" -eq 0 ]
}
