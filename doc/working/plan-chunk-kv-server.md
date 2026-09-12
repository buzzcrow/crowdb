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

- [~] Extract shared `KvGroupOperations`, add a group-0 control-plane facade,
  and migrate the supervised domain-monitor runtime off KV-client loopback RPC
  while preserving read-failure containment and leader-tenure fencing.
- [x] Add raw expired-instance observation and fake-clock health transitions.
- [x] Issue aggregate assignment-digest grants and enforce conservative local
  self-fencing and replacement exclusion deadlines.

## Phase 3: Server and RPC

- [~] Add `crowdb-chunk-kv-server` config, logging, metrics, health,
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

- [ ] Persist and resume idempotent transfer/split transitions with prepared
  target readiness and exact R142 proof resolution.
- [~] Implement dead-owner exclusion, graceful fencing, and no-copy transfer.
- [x] Add median split selection, count-first placement, weighted improvement,
  cooldown, and transition concurrency limits.

## Phase 5: Gates and Documentation

- [ ] Add protocol, catalog, lease, monitor, RPC, lifecycle, and fixture tests.
- [ ] Run every required Rust and server gate through `pixi run`.
- [x] Fold stable behavior into a permanent server design and index entry.

## Gate Results

- `cargo fmt --all -- --check`: passed.
- `rs-lint`: passed for the full workspace.
- `crowdb-kv-client`, `crowdb-chunk-kv`, `crowdb-chunk-kv-server`,
  `crowdb-chunkdb`, and `crowdb-diskdb` all-target test gates: passed.
- The first aggregate `test-server` run failed only
  `reconfig_via_api_remove_leader` on a transport timeout; the exact clean-env
  retry passed.
- The second aggregate run passed KV server, diskdb, diskdb-client, and chunkdb,
  then exposed three chunk-client cross-test failures: one stale lifecycle state
  and two fixture startup/port conflicts. Each exact test passed independently
  after `clean-env`. These failures are pre-existing suite isolation behavior;
  R143 changes do not touch those paths.

## Open Issues

- Review decision: ordinary legacy KV puts retain their existing
  Paxos-chosen response point because callers and async-apply tests depend on
  it. `KvGroupOperations` owns the read apply fence, and conditional group-0
  control writes wait through apply before returning; R143 monitor writes use
  only the conditional path.
