<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R52 — Reverse Scan

## Summary

The chunk-KV tree/FFI path has a bounded native descending scan and predecessor
seek. The legacy async tree, RPC, and client scan surfaces remain forward-only,
so reverse direction still needs completion across those layers.

## Problem

Some workloads need descending-key iteration — e.g. "newest first" when
keys are timestamp-ordered, or tail-of-keyspace pagination. Today the
only way to get reverse order is to scan forward and sort client-side,
which is O(N log N) and defeats the O(limit) scan pushdown.

## Scope

- **Engine** (`crowdb-tree`): `LeafChainCursor` supports reverse positioning,
  and chunk-KV uses predecessor descent across leaves while fixing one page's
  memtable/root/GC view. The remaining legacy async surface needs backward
  traversal integration:
  - `seek(start_before)` targets the leaf containing `start_before`
    and positions at the last entry < `start_before`.
  - `advance()` moves to the previous entry in key order (prev slot
    in the leaf, or the last entry of the previous leaf).
  - The merge loop walks L0 + L1 cursors backward, selecting the
    max-key entry (vs min-key forward), highest-slot-wins on collision.
- **L0 cursor** (R50): `cursor_reverse` uses tower predecessor searches without
  adding `prev` pointers or increasing node memory. The legacy scan API can
  reuse this path.
- **FFI** (`ct_scan_async`): add a `direction` parameter
  (`CT_SCAN_FORWARD = 0`, `CT_SCAN_REVERSE = 1`).
- **RPC** (`KvScanRequest`): add a `direction` field. The
  S3-style pagination uses the first key of each page as the next
  `start_before` (vs the last key as `start_after` in forward mode).
- **Client** (`CrowdbClient::scan`): add a `direction` parameter;
  pagination state tracks `start_before` instead of `start_after`.

## Cost Shape

Reverse scans have different cache behavior than forward scans:
backward leaf traversal touches pages in reverse allocation order,
which may have worse prefetch/sequential-read characteristics. Needs
its own scan perf baseline — add reverse-scan configs to
`tools/bench-kv-scan-regression.sh`.

## Dependencies

- R48 (lazy `LeafChainCursor`) — the cursor infrastructure exists;
  reverse mode adds a new traversal direction to it.
- R50 (epoch-protected MemTable) — the skip-list cursor needs prev
  links or a reverse traversal path.

## Complexity

Medium. The engine cursor work is the hardest part; the FFI/RPC/client
plumbing is straightforward (one new field per layer).
