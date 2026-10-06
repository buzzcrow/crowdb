<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB dynamic service ownership Plan

Contract: [R213](../backlog/R213-chunkdb-dynamic-service-ownership.md).
Architecture: [slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md).
Goal: safely redistribute ChunkDB execution authority without relocating storage.

## Protocol and KV admission

- [~] **Slot fence contract**: introduce typed slot owner identities and canonical
  fence keys; extend reserved-key protection, owner-write validation and drain
  admission without adding a lock. Files: `lib/crowdb-protocol/src/`,
  `lib/crowdb-kv/src/cluster/group_owner_fence.rs`, KV RPC handlers,
  `lib/crowdb-kv-client/src/client/core/owned.rs`, integration tests.
- [ ] **Persistent transition schema**: define incarnations, per-slot authority,
  phase state and complete generation publication independently of the storage
  map. Files: protocol chunk-slot/key modules, KV-client binding modules/tests.

## ChunkDB authority

- [ ] **Captured write authority**: route every chunk/task/reservation mutation
  through read-only slot owner admission with its original assignment identity.
  Preserve record CAS and retained per-chunk mutex. Files:
  `app/crowdb-chunkdb/src/storage*`, `task/store*`, `range_guard.rs`.
- [ ] **Incarnation and activation**: register a process incarnation, prepare and
  activate only after data-group fences; recover durable work and revoke old
  tasks. Files: ChunkDB startup/RPC/task runtimes and scope tests.

## Monitor and clients

- [ ] **Resumable monitor**: implement revision-CAS transitions, fencing, failure
  grace, stable slot-count balancing, hysteresis and movement budgets. Keep fixed
  policy tests. Files: `app/crowdb-kv-server/src/background/domain_monitor/chunkdb*`
  and `tests/domain_monitor_test*`.
- [ ] **Snapshot routing**: accept complete newer service generations; reject
  stale authority and preserve unknown-outcome semantics. Files: KV-client
  binding modules, ChunkDB client/server routing, native tree resolver tests.
- [ ] **Console and readiness**: expose transition/assignment state and explicit
  dynamic-policy setup; extend the owned fresh-cluster UI flow. Files: Console
  deployment/status/UI and `e2e/flows/92-three-node-data.spec.ts`.

## Verification and cleanup

- [ ] **Fault coverage**: verify each transition interruption, stale mutation,
  task revocation, process reincarnation, leader change and unknown outcome.
- [ ] **Gates and permanent design**: run focused unit/integration/E2E cases,
  separate fmt and clippy gates; document final architecture and measured limits.
- [ ] **Final cleanup**: remove completed requirement, its index entry and this
  plan; point audit references at permanent architecture.

## Verification groups

- Unit: protocol map/key validation and deterministic policy planning.
- Integration: KV owner admission/drain; slot publication; monitor transitions;
  ChunkDB scoped storage/tasks/reservations; client routing and native resolver.
- E2E: fresh three-node cluster, diskless expansion, actual KV/S3/Iceberg data.

## Existing work

The checkout already contains validated readiness/group-creation/error-selection
fixes, the diskless UI regression and `tools/load-tpch.py`. Preserve them and
stage only coherent requirement changes. Persistent console services must not
be stopped by the ephemeral test cleanup.
