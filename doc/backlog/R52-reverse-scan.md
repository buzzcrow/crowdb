<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R52: scan / crowdb-tree — Reverse KV Scan

**Problem**: The chunk-backed ordered KV path already exposes forward and
reverse scans end to end: `ScanDirection` is on its wire contract,
`crowdb-chunk-kv-server` calls the native `scan_reverse`, and the composed
client paginates in descending order. The ordinary KV path remains
forward-only even though crowdb-tree now has synchronous `ct_seek_reverse` and
`ct_scan_reverse` entry points with native predecessor descent.

The remaining gap spans the ordinary KV stack. `KVEngine::scan`,
`ct_scan_async`, `FBKvScanRequest`, `KvRpcService`, and
`CrowdbKvClient::scan` all accept only `start_after` and always return ascending
keys. Operators therefore cannot request newest-first prefix pages from the
ordinary KV API without fetching and sorting the full range client-side. The
cost becomes O(N) transfer and O(N log N) client work instead of an engine-side
O(page size) traversal.

The current native reverse surface is not itself sufficient for the KV path:
it is synchronous, does not expose `keys_only` or deadline behavior, and is not
integrated with cold-page completion through `AsyncCrowdbtree`. Merely adding a
wire flag would either block an async handler on cold storage or silently lose
existing scan options. The root scan and storage contracts are
`doc/design/kv/design-crowdb-kv.md`,
`doc/design/tree/design-crowdb-tree-engine.md`, and
`doc/design/tree/design-crowdb-tree-storage.md`.

Concrete scenarios are descending timestamp-key pagination, fetching the last
N keys under a metadata prefix, and retrying a reverse page after a leader
redirect without duplicating its boundary key.

**Solution**: Complete reverse direction on the existing ordinary KV scan
operation while preserving the forward API and wire default.

1. Extend crowdb-tree's packed scan implementation and async C ABI with a
   direction. Reverse mode positions at the greatest live key below the
   exclusive continuation bound, walks predecessor leaves, merges L0 and L1 by
   greatest key, and keeps highest-slot-wins and tombstone suppression exactly
   symmetric with forward mode. It must retain the current fixed-view retry
   behavior for cold pages, byte budget, `keys_only`, deadline, and truncated
   result contract. Existing synchronous reverse seek/scan wrappers remain
   available for chunk-KV.
2. Add a KV-specific direction enum to `crowdb-protocol` and append it to
   `FBKvScanRequest`; the FlatBuffer default is forward so an omitted field
   preserves existing clients. Forwarding nodes copy the field unchanged.
   The existing wire `start_after` bytes remain the exclusive continuation key
   for compatibility: forward returns keys greater than it and reverse returns
   keys less than it. In reverse mode an empty continuation begins below the
   existing exclusive `end_key`, or below the prefix successor when `end_key`
   is empty. The prefix start is the lower range bound.
3. Thread direction through `KVEngine`, `CrowdbTreeEngine`, the in-memory test
   engine, store scan handling, and `KvRpcService`. Read mode, bounded cutoff,
   count-only, byte-budget, timeout, redirect, and structured error semantics
   do not change. Count-only accepts either direction and returns the same
   direction-independent count.
4. Keep `CrowdbKvClient::scan`, `scan_bounded`, and `scan_bounded_at` as
   forward-compatible wrappers. Add explicit reverse entry points whose public
   arguments use the name `start_before`. The shared pagination loop advances
   from the last returned key according to direction, including after
   transport retry or `NotLeader` redirection, and rejects a non-monotonic or
   repeated server page instead of looping.
5. Add reverse cases to the tree, FFI, protocol, KV core, server, and client
   tests. Cover L0/L1 collisions, tombstones, cross-leaf cold reads, prefix and
   `end_key` clipping, empty and one-item ranges, entry and byte truncation,
   bounded cutoffs, deadlines, and retry pagination. Add reverse configurations
   to `tools/bench-kv-scan-regression.sh` and document their reference results
   in `doc/design/kv/kv-scan-flow-analysis.md`.

Chunk-KV protocol/client changes, reverse journal scans, reverse snapshot-handle
scans, and adding previous pointers to memtable or leaf nodes are not part of
this requirement.

**Dependencies**:

- The landed crowdb-tree predecessor implementation and synchronous
  `ct_seek_reverse` / `ct_scan_reverse` are the native baseline.
- The landed chunk-KV directional scan is a semantic reference, not an
  implementation dependency; ordinary KV keeps its own protocol types.
- The existing bounded-scan cutoff, byte-budget pagination, and async cold-page
  completion remain mandatory. If the native reverse merge cannot reuse the
  forward async retry machinery, implementation must first factor a shared
  directional packed-scan core rather than add a blocking fallback.

**Acceptance**:

- Setup a tree with overlapping L0/L1 versions, tombstones, and enough durable
  leaves to force cold loads; run async reverse pages with entry and byte
  limits; assert strictly descending live keys, highest-slot-wins values, no
  repeated boundary, correct truncation, and completion through the async
  reactor. Invariant: reverse scan is the order-dual of forward scan without
  blocking cold I/O. Integration test.
- Setup old/default and explicit-forward KV scan requests; encode, decode, and
  forward them; assert both decode as forward and preserve all existing fields.
  Invariant: the wire addition is backward compatible. Unit test.
- Setup a prefix range with lower and upper clipping; issue reverse server
  scans using empty and non-empty continuation keys; assert every key is inside
  the range, ordered descending, and strictly below the continuation. Invariant:
  reverse bounds are exclusive and prefix-safe. Integration test.
- Setup a paginated reverse client scan and inject a transport failure and
  leader redirect after a non-empty page; resume it; assert the final sequence
  has no gaps or duplicates and bounded scans retain one cutoff. Invariant:
  retry state advances in the requested direction. Integration test.
- Setup reverse `keys_only`, `count_only`, and an expired deadline request;
  assert empty values for keys-only, the same count as forward count-only, and
  a partial truncated timeout rather than an unbounded scan. Invariant: scan
  options are direction-independent. Integration test.
- Setup the reverse benchmark cases on the documented reference platform; run
  them and record throughput, latency, and error counts beside the forward
  baselines. Invariant: reverse performance has a maintained regression
  sentinel. E2E test.

Verification commands:

- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run test-protocol`
- `pixi run test-kv-core`
- `pixi run test-kv-client`
- `pixi run test-kv-server`
- `pixi run -- bash tools/bench-kv-scan-regression.sh`
- `pixi run -- cargo fmt --all --check`
- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run rs-lint`
