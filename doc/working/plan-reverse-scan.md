<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Reverse KV Scan Plan

Upstream requirement: `doc/backlog/R52-reverse-scan.md`

Root designs: `doc/design/kv/design-crowdb-kv.md`,
`doc/design/tree/design-crowdb-tree-engine.md`, and
`doc/design/tree/design-crowdb-tree-storage.md`

Goal: expose bounded, retry-safe descending scans through the ordinary KV API
by completing the existing native reverse traversal rather than duplicating it.

## Phase 1 — Native Directional Scan

- [ ] **Unify packed scan options**: introduce one internal direction/options
  shape covering continuation, prefix/end bounds, limit, byte budget,
  keys-only, deadline, and tombstone policy; route the existing forward and
  reverse entry points through it. Files: `lib/crowdb-tree/include/crowdb-tree/btree/scan_packed.h`,
  `lib/crowdb-tree/src/btree/`, `lib/crowdb-tree/src/c_api.cpp`.
- [ ] **Complete reverse async traversal**: make predecessor descent and
  previous-leaf loading return the same pending/retry signal as forward cold
  loads while retaining one fixed memtable/root/epoch view per completed
  attempt. Symbols: `Crowdbtree::scan_async`, `ct_scan_async`,
  `ct_scan_reverse`. Files: `lib/crowdb-tree/include/crowdb-tree/`,
  `lib/crowdb-tree/src/`.
- [ ] **Extend the C ABI and Rust adapter**: append a forward-default direction
  argument, update `sys.rs`, and add `scan_directional` / `try_scan_directional`
  while retaining forward wrappers. Files: `lib/crowdb-tree/include/crowdb-tree/c_api.h`,
  `lib/crowdb-tree/src/c_api.cpp`, `lib/crowdb-tree/ffi/src/{sys,async_tree,scan}.rs`.
- [ ] **Cover native edge cases**: test L0/L1 collisions, tombstones,
  inclusive native seek versus exclusive KV continuation, cold cross-leaf
  pages, byte truncation, keys-only, and deadline stops. Files:
  `lib/crowdb-tree/tests/integration/async_scan_test.cpp`,
  `lib/crowdb-tree/ffi/tests/ffi_test.rs`.

## Phase 2 — KV Protocol and Engine

- [ ] **Add the KV direction contract**: define `KvScanDirection`, append a
  forward-default FlatBuffer enum field, regenerate bindings, and round-trip
  omitted/forward/reverse values. Files: `lib/crowdb-protocol/src/fbs/kv_client.fbs`,
  `lib/crowdb-protocol/src/types/kv_client.rs`, protocol encode/decode wrappers,
  `lib/crowdb-protocol/tests/`.
- [ ] **Thread direction through engine scans**: add direction to
  `KVEngine::scan`, map it in `CrowdbTreeEngine`, and update all in-memory test
  engines without changing count-only results. Files:
  `lib/crowdb-kv/src/kv/{kv_engine,crowdb_tree_engine}.rs`,
  `lib/crowdb-kv/tests/common/`, `lib/crowdb-kv/tests/kv_test/`.
- [ ] **Serve and forward reverse requests**: decode and validate direction,
  preserve it through leader forwarding, and pass it into `kv_scan` for local,
  linearizable, MinSlot, and bounded paths. Files:
  `lib/crowdb-kv/src/rpc/kv_rpc_service.rs`, KV store scan modules, server tests.

## Phase 3 — Client Pagination

- [ ] **Encode direction in transport**: extend `send_scan` and its test
  transport, preserving forward-default compatibility. Files:
  `lib/crowdb-kv-client/src/transport/rpc_transport.rs`,
  `lib/crowdb-kv-client/tests/`, `app/crowdb-kv-server/tests/common/`.
- [ ] **Refactor the shared pagination loop**: track an exclusive continuation
  independent of its legacy wire name; validate ascending/descending page
  monotonicity and advance from the last item after success, redirect, or
  transport retry. Symbols: `CrowdbKvClient::scan_impl`. Files:
  `lib/crowdb-kv-client/src/client/core.rs`.
- [ ] **Expose reverse wrappers**: add reverse equivalents for ordinary and
  bounded scans with `start_before` naming; keep existing public methods as
  forward wrappers. Files: `lib/crowdb-kv-client/src/client/core.rs`,
  `lib/crowdb-kv-client/src/lib.rs` if re-exports change.
- [ ] **Test retries and bounds**: cover byte-paged reverse results, prefix and
  end clipping, empty ranges, one-item pages, deadline, count-only, bounded
  cutoff, transport failure, and `NotLeader` resume. Files:
  `lib/crowdb-kv-client/tests/`, `lib/crowdb-kv/tests/store_test/`,
  `app/crowdb-kv-server/tests/`.

## Phase 4 — Evidence and Documentation

- [ ] **Add reverse benchmark cases**: mirror representative forward bounded,
  deep-pagination, and multi-thread cases with direction-specific labels and
  output. Files: `tools/bench-kv-scan-regression.sh`.
- [ ] **Record performance shape**: capture same-host forward/reverse
  throughput, latency, errors, and cold-page observations. Files:
  `doc/design/kv/kv-scan-flow-analysis.md`.
- [ ] **Reconcile permanent contracts**: document ordinary KV direction,
  continuation semantics, and forward wire default. Files:
  `doc/design/kv/design-crowdb-kv.md`, relevant tree design sections.
- [ ] **Run focused gates**: run all commands listed by R52 and fix ordinary
  failures before closure. Files: affected workspace.

## Consolidated File List

- `lib/crowdb-tree/include/crowdb-tree/{c_api.h,btree/}`
- `lib/crowdb-tree/src/{c_api.cpp,btree/}`
- `lib/crowdb-tree/ffi/src/{async_tree,scan,sys}.rs`
- `lib/crowdb-tree/{tests,ffi/tests}/`
- `lib/crowdb-protocol/src/{fbs,types,fb_wrappers}/`
- `lib/crowdb-protocol/tests/`
- `lib/crowdb-kv/src/{kv,rpc}/` and `lib/crowdb-kv/tests/`
- `lib/crowdb-kv-client/src/{client,transport}/` and its tests
- `app/crowdb-kv-server/tests/`
- `tools/bench-kv-scan-regression.sh`
- `doc/design/kv/kv-scan-flow-analysis.md` and relevant root designs

## Tests

Unit tests:

- Direction default/encode/decode/forwarding.
- Directional bound and page-monotonicity helpers.
- In-memory engine forward/reverse/count equivalence.

Integration tests:

- Native and FFI reverse merge across cold leaves.
- KV server reverse scans for every read/bounded option.
- Client pagination across byte limits, redirect, and transport retry.

E2E tests:

- Reverse scan regression cases beside their forward baselines.
