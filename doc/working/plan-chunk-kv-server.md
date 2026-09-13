<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Server Plan

Source: [`design-chunk-kv-server.md`](design-chunk-kv-server.md) and
[`../backlog/R143-chunk-kv-server.md`](../backlog/R143-chunk-kv-server.md).

Goal: implement the standalone, discoverable, lease-fenced service that hosts
R142 partitions and publishes one complete group-0 range catalog.

## Phase 1: Protocol and Catalog Core

- [x] Rename public catalog symbols to the explicit
  `ChunkKvRangeCatalog*` vocabulary across protocol, server, and routed client.
- [x] Add monitor descriptor, instance observation, assignment, artifact,
  transition, range-page/head, serving-grant, and typed outcome models.
- [x] Validate complete binary-keyspace coverage, page/head checksums,
  generation monotonicity, exact adjacency, and epoch non-regression.
- [x] Implement an injected immutable catalog store with page-before-head
  publication and ambiguous-head reread resolution.
- [x] Back catalog pages, the generation head, and monitor descriptors with
  revision-checked group-0 KV operations and reconciliation reads.

## Phase 2: Monitor and Lease Authority

- [x] Extract shared `KvGroupOperations`, add a group-0 control-plane facade,
  and migrate the supervised domain-monitor runtime off KV-client loopback RPC
  while preserving read-failure containment and leader-tenure fencing.
- [x] Persist monitor descriptors from chunk-KV, chunkdb, and diskdb before
  readiness; run compiled chunk-KV/chunkdb drivers and an operator-only diskdb
  driver on every group-0 replica, with publication gated by leader tenure.
- [x] Add raw expired-instance observation and fake-clock health transitions.
- [x] Issue aggregate assignment-digest grants and enforce conservative local
  self-fencing and replacement exclusion deadlines.

## Phase 3: Server and RPC

- [x] Add `crowdb-chunk-kv-server` config, logging, metrics, health,
  management, graceful shutdown, and zero/many partition hosting.
- [x] Reopen each locally assigned latest tree root and stream manifest as
  `Prepared`, replay from the root's WAL offset through the durable tail, and
  activate only under a matching serving grant.
- [x] Reconcile refreshed catalogs with the exact hosted-partition snapshot,
  recovering incoming assignments before atomically replacing local handles.
- [x] Add direct point RPC types and handlers for R142 operations, typed errors,
  request identity, journal positions, deadlines, and stale-owner redirects.
- [x] Bind ordered seek and scan requests to direction, partition, epoch, and map
  revision and return refresh-required after topology changes.

## Phase 4: Transfer, Split, and Balance

- [x] Persist and resume idempotent transfer/split transitions with prepared
  target readiness and exact R142 proof resolution.
- [x] Implement dead-owner detection, deterministic transfer planning, lease
  exclusion, graceful fencing, target recovery, and no-copy catalog cutover.
- [x] Add median split selection, count-first placement, weighted improvement,
  cooldown, and transition concurrency limits.

## Phase 5: Gates and Documentation

- [x] Add protocol, catalog, lease, monitor, RPC, lifecycle, and fixture tests.
- [x] Run every required Rust and server gate through `pixi run`.
- [x] Fold stable behavior into a permanent server design and index entry.

## Gate Results

- `cargo fmt --all -- --check`: passed.
- `rs-lint`: passed for the full workspace.
- `crowdb-kv-client`, `crowdb-chunk-kv`, `crowdb-chunk-kv-server`,
  `crowdb-chunkdb`, and `crowdb-diskdb` all-target test gates: passed.
- Clean aggregate `test-server`: passed KV server, diskdb, diskdb-client,
  chunkdb, chunk-client, and diskio-client stages.
- The real three-owner MemDisk production regression passed with 30,000
  acknowledged writes and no errors. Automatic count and 5 MiB size splitting
  converged to 13 partitions distributed 5/4/4, recorded 12 split fences and
  12 commits, and recovered five assigned partitions plus an acknowledged key
  after restart in 1,050 ms.

## Open Issues

- Review decision: ordinary legacy KV puts retain their existing
  Paxos-chosen response point because callers and async-apply tests depend on
  it. `KvGroupOperations` owns the read apply fence, and conditional group-0
  control writes wait through apply before returning; R143 monitor writes use
  only the conditional path.
- Review decision: `EnsureDomainMonitor` is implemented as a revision-checked
  group-0 client operation rather than a new kv-server RPC. The compiled
  supervisor rejects unsupported persisted descriptors and the existing
  registry contract exposes `UnsupportedMonitorDomain`; adding a separate RPC
  solely for capability negotiation would reintroduce the self-RPC coupling
  removed from monitor execution.
