<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Server Plan

Source: [`design-chunk-kv-server.md`](design-chunk-kv-server.md) and
[`../backlog/R143-chunk-kv-server.md`](../backlog/R143-chunk-kv-server.md).

Goal: implement the standalone, discoverable, lease-fenced service that hosts
R142 partitions and publishes one complete group-0 range catalog.

## Phase 1: Protocol and Catalog Core

- [~] Add monitor descriptor, instance observation, assignment, artifact,
  transition, range-page/head, serving-grant, and typed outcome models.
- [x] Validate complete binary-keyspace coverage, page/head checksums,
  generation monotonicity, exact adjacency, and epoch non-regression.
- [x] Implement an injected immutable catalog store with page-before-head
  publication and ambiguous-head reread resolution.

## Phase 2: Monitor and Lease Authority

- [~] Implement idempotent domain-monitor registration and a supervised,
  leader-fenced driver runtime with read-failure containment.
- [x] Add raw expired-instance observation and fake-clock health transitions.
- [x] Issue aggregate assignment-digest grants and enforce conservative local
  self-fencing and replacement exclusion deadlines.

## Phase 3: Server and RPC

- [~] Add `crowdb-chunk-kv-server` config, logging, metrics, health,
  management, graceful shutdown, and zero/many partition hosting.
- [~] Add direct RPC types and handlers for R142 operations, typed errors,
  request identity, journal positions, deadlines, and stale-owner redirects.
- [~] Bind scan continuation tokens to direction, partition, epoch, and map
  revision and return refresh-required after topology changes.

## Phase 4: Transfer, Split, and Balance

- [~] Persist and resume idempotent transfer/split transitions with prepared
  target readiness and exact R142 proof resolution.
- [~] Implement dead-owner exclusion, graceful fencing, and no-copy transfer.
- [x] Add median split selection, count-first placement, weighted improvement,
  cooldown, and transition concurrency limits.

## Phase 5: Gates and Documentation

- [~] Add protocol, catalog, lease, monitor, RPC, lifecycle, and fixture tests.
- [ ] Run every required Rust and server gate through `pixi run`.
- [x] Fold stable behavior into a permanent server design and index entry.

## Open Issues

- Production R142 constructors and ordered operations constrain full data-plane
  and real-process coverage.
- Group-0 page/head persistence and monitor supervision need integration with
  the existing KV client/server boundaries.
- R145 owns routed multi-partition composition and end-to-end client coverage.
- Data RPC request/response, transition-detail, and balance-policy wire models
  now cover the point-operation foundation; crowdb-rpc FlatBuffers transport,
  ordered seek/scan execution, and management endpoints remain. Scan interval
  clipping and topology-bound continuation validation are complete.
- Transfer records and the reducer preserve no-copy artifact identity and old-
  owner exclusion, but the group-0 transition store, target recovery worker,
  split orchestration, and catalog cutover adapter remain.
- Config defaults, reserved ports, lock-free counters, health snapshots, and
  drain-time admission closure are implemented; process logging, HTTP
  management, heartbeat publication, RPC listener startup, and bounded
  checkpoint drain remain.
