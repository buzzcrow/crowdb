<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB dynamic service ownership Plan

Contract: [R221](../backlog/R221-chunkdb-dynamic-service-ownership.md).
Architecture: [slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md).
Goal: safely redistribute ChunkDB execution authority without relocating storage.

## Protocol and KV admission

- [x] **Slot fence contract**: introduce typed slot owner identities and canonical
  fence keys; extend reserved-key protection, owner-write validation and drain
  admission without adding a lock. Files: `lib/crowdb-protocol/src/`,
  `lib/crowdb-kv/src/cluster/group_owner_fence.rs`, KV RPC handlers,
  `lib/crowdb-kv-client/src/client/core/owned.rs`, integration tests.
- [~] **Persistent transition schema**: define incarnations, per-slot authority,
  phase state and complete generation publication independently of the storage
  map. Files: protocol chunk-slot/key modules, KV-client binding modules/tests.
  Keep per-slot authority epochs independent of service-map publication epochs.
  Persist a single handoff cohort before data-group side effects; fence receipts
  must cover every moved slot before the complete map can be published. Read
  routing and authority as one validated generation. An initialized fixed
  runtime must not silently acquire dynamic authority.

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

## Verified protocol and KV foundation

- `ChunkServiceIncarnation` uses nonzero OS-generated 128-bit process identity.
  `ChunkSlotAuthority` binds it to a nonzero instance and per-slot generation;
  its canonical read-only fence comparison value is 33 bytes.
- `ChunkSlotFenceKey` covers canonical decimal slots 0..1023 in the selected
  nonzero data group; group zero is rejected by both client and RPC validation.
  Malformed keys remain reserved from ordinary writes. Conditional
  Put/Batch validate new authority, forbid slot-fence deletion and cross-fence
  mutation, and retain the existing DiskDB contract.
- Owner changes share the existing atomic admission/drain and tenure recovery
  barriers. No new lock or unsafe exception was introduced. Ordinary writes
  preserve the fence revision and optional business-record CAS.
- Protocol: `pixi run cargo test -p crowdb-protocol --test chunk_slot_authority_test
  --test chunk_slot_test` — 9 passed.
- Admission: `pixi run clean-env && pixi run cargo test -p crowdb-kv --test
  group_test owner_fence` — 12 passed across both namespaces; caller cancellation,
  unknown proposals, topology replacement and new leader tenure included.
- RPC: `pixi run clean-env && pixi run cargo test -p crowdb-kv-client --test
  chunk_slot_owner_fence_test --test conditional_retry_test` — 8 passed,
  including same-ID reincarnation, group-zero rejection,
  independent slots, record CAS and malformed/raw RPC rejection.
- Workspace `pixi run rs-lint` passed after fixing documentation markup and
  positive conditional branch order. `pixi run rs-fmt-check` passed.
- Runtime activation, persistent handoff, balancing and dynamic Console policy
  remain unimplemented; the fixed deployment behavior remains the active policy.
