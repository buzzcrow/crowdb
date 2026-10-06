<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB dynamic service ownership Plan

Contract: [R221](../backlog/R221-chunkdb-dynamic-service-ownership.md).
Architecture: [slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md).
Goal: safely redistribute ChunkDB execution authority without relocating storage.

## Submission epoch decision

- Admission compares the captured slot owner and durable epoch, not incarnation.
  Same-ID restart and every regrant advance that slot's epoch before admitting
  work. A global map update for another slot does not reject an unchanged slot.
- Recheck immediately before every client KV submission. Submitted work may
  finish; accept the final-check/submission race. No handoff draining, new
  hot-path lock, or independent incarnation gate is required.
- Pre-submission rejection triggers internal routing refresh and reexecution
  with the original request ID/deadline, possibly on this same process with a
  new epoch. Never replace an old execution's captured epoch in place. Retain
  business CAS, completed-step deduplication and unknown-outcome reconciliation.
- Existing incarnation-bearing wire types and KV draining primitives still
  implement the earlier contract. Their presence is not the new admission
  policy. Preserve DiskDB fence semantics; dynamic ChunkDB uses the local
  submission check and group-zero epoch publication instead.

## Protocol and KV admission

- [x] **Slot fence contract**: introduce typed slot owner identities and canonical
  fence keys; extend reserved-key protection, owner-write validation and drain
  admission without adding a lock. Files: `lib/crowdb-protocol/src/`,
  `lib/crowdb-kv/src/cluster/group_owner_fence.rs`, KV RPC handlers,
  `lib/crowdb-kv-client/src/client/core/owned.rs`, integration tests.
- [x] **Persistent epoch publication**: persist per-slot owner/epoch and complete
  generation publication independently of the storage
  map. Files: protocol chunk-slot/key modules, KV-client binding modules/tests.
  Keep per-slot authority epochs independent of service-map publication epochs.
  Publish routing/epoch bindings and both heads in one service-head CAS batch.
  Reject epoch rollback and owner changes without epoch advancement; retry
  reconciles the same complete snapshot. Read routing and authority as one
  validated generation. An initialized fixed
  runtime must not silently acquire dynamic authority.
  Implemented pending verification: `publish_service_epochs`, complete authority
  bitmap keys and paired snapshot reads. Dynamic publication does not call the
  earlier handoff fence APIs or perform data-group receipt reads.

## ChunkDB authority

- [x] **Captured write authority**: route every chunk/task/reservation mutation
  through local submission checks with the original owner/epoch.
  Preserve record CAS and retained per-chunk mutex. Files:
  `app/crowdb-chunkdb/src/storage*`, `task/store*`, `range_guard.rs`.
- [x] **Restart and activation**: durably advance owned slot epochs at restart;
  prepare targets and recover durable work; stop stale
  tasks. Files: ChunkDB startup/RPC/task runtimes and scope tests.

## Monitor and clients

- [x] **Resumable monitor**: implement atomic revision-CAS publication, failure
  grace, stable slot-count balancing, hysteresis and movement budgets. Keep fixed
  policy tests. Files: `app/crowdb-kv-server/src/background/domain_monitor/chunkdb*`
  and `tests/domain_monitor_test*`.
- [x] **Snapshot routing**: accept complete newer service generations; reject
  stale authority and preserve unknown-outcome semantics. Files: KV-client
  binding modules, ChunkDB client/server routing, native tree resolver tests.
- [x] **Console and readiness**: expose transition/assignment state and explicit
  dynamic-policy setup; extend the owned fresh-cluster UI flow. Files: Console
  deployment/status/UI and `e2e/flows/92-three-node-data.spec.ts`.

## Verification and cleanup

- [x] **Fault coverage**: verify each transition interruption, stale mutation,
  task revocation, process reincarnation, leader change and unknown outcome.
- [x] **Gates and permanent design**: run focused unit/integration/E2E cases,
  separate fmt and clippy gates; document final architecture and measured limits.
- [x] **Final cleanup**: remove completed requirement, its index entry and this
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
  are not wired into the monitor; the fixed deployment remains the active policy.

## Verified durable handoff foundation

- Protocol: `pixi run cargo test -p crowdb-protocol --test chunk_slot_handoff_test`
  — 3 passed: receipts, phase ordering, canonical recovery and schema rejection.
- Persistence: `pixi run clean-env && pixi run cargo test -p crowdb-kv-client
  --test chunk_slot_handoff_test --test chunk_slot_map_test` — 10 passed.
  Tests use real group-0 and data-group RPCs. A replacement controller resumes
  partial fences, retries do not rewrite the fence, forged receipts are rejected,
  competing controllers reserve one cohort, stale updates cannot remove another
  controller's receipts, and the fixed routing map remains unchanged.
- Standalone Publish is rejected until complete routing/authority publication is
  implemented. Activation/recovery and the dynamic policy remain pending.
- Workspace `pixi run rs-lint`, `pixi run rs-fmt-check` and diff whitespace checks
  passed for this stage.

## Current verification

- Atomic epoch publication and competing publishers: 2 integration tests passed;
  complete coverage, rollback/reuse rejection, same-ID epoch advancement,
  partial authority rejection, unchanged storage and legacy data fences.
- Dynamic and fixed monitors: 6 integration tests passed. A fourth owner balances
  at 256 slots each with a 64-slot per-tick budget; failure grace survives
  controller replacement and resets after recovery; corruption leaves maps intact.
- Local and real store submission checks passed across both maintenance domains.
  Native runtime routing and task scope/restart tests passed.
- Fresh Console baseline: 49.6s. KV error-text selection spec passed. First
  dynamic run failed while waiting for DiskIO readiness; teardown masked the
  first assertion. A standalone rerun hit the WebServer startup timeout while
  competing with a cold lint build. Retry after builds complete, without changing
  assertion limits or adding test retries.
- Added real RPC tests for cached-owner rerouting and same-process epoch regrant
  while an execution waits on the existing per-chunk mutex. Added accepted-write
  gate test: Group 0 publication must not drain a held data-group write.
- ChunkDB transport distinguishes unknown submitted results from explicit server
  admission rejections. Mutations do not blindly replay transport failures;
  read-only calls may retry. Ownership retry retains its request ID and uses
  the original bounded operation deadline with no ownership backoff.

## Final acceptance

- Dynamic fresh-cluster UI flow passed in 59.7s against the measured 49.6s
  baseline: 36 ready services, six balanced owners, unchanged storage map,
  diskless KV replicas, exact S3 and Iceberg round trips. Slot balance checking
  now follows data validation and uses the default three-second assertion bound.
- Workspace Rust fmt and clippy passed. UI TypeScript lint and all 16
  service-plan tests passed. Error text selection E2E passed.
- Final expanded integration regression is running, including task revocation
  in both domains and replacement Group 0 leader tenure.
- Hot-path review: local immutable owner/epoch comparison only; no new locks,
  draining, RPC fence checks or ownership-retry sleeps. Submitted writes and
  the final-check/submission race retain the documented acceptance boundary.

- Final integration results: full stack 38, real partial EC bytes 1, task scope
  3, KV group publication 4, slot maps 8, epoch publication 2, dynamic monitor
  4, fixed monitor 3, protocol 9 and storage readiness 1 passed.
- A concurrent identical epoch publication exposed preparation-phase CAS
  rejection without reconciliation. The publication wrapper now reconciles
  both preparation and submitted-CAS conflicts against the exact complete map;
  competing distinct maps still choose one winner. Affected regression passed.
- One UI run concurrent with the full-stack suite timed out on diskless Node 5
  DiskDB deployment. Trace shows Node 4 deployment took 2096ms and Node 5
  remained in flight at the assertion deadline. Final verification runs UI alone.

- UI startup root cause: early Chunk-KV journal bootstrap could await an
  unstarted published ChunkDB owner and block the serial default deployment
  plan. Default plans now finish the selected ChunkDB cohort before starting
  Chunk-KV. A held-deployment regression proves bootstrap cannot start early;
  all 17 service-plan tests and TypeScript lint pass.
- E2E observes every automatic deployment HTTP response (201), then separately
  checks durable progress and DOM readiness with unchanged three-second bounds.
  Initial PKV readiness is observed in the sidebar before opening its selection
  dialog. Teardown preserves and prints the primary failure.

- Diskless DiskDB discovery now receives the cluster's KV management seeds,
  including established Group 0 voters. A broader node-health probe no longer
  delays an already-ready DiskDB deployment response; the existing background
  cache refresh retains responsibility for health updates.
- Final ChunkDB focused regression: 16 passed. DiskDB/web lifecycle regression:
  cancellation 5, auto-start 1, routes 8 and service lifecycle 7 passed.
  Workspace fmt/clippy and UI TypeScript lint passed after the final changes.

- Final exact UI spec passed in 46.1s, below the measured 49.6s baseline,
  including all 36 deployment responses, six registered DiskDB instances,
  diskless ready CDB/CKV, group replicas on nodes 3/4/5, exact KV/S3/Iceberg
  reads, six balanced service owners and unchanged storage generation/coverage.
  Default assertion/action limits remain unchanged.
- All implementation and acceptance work is complete. Final cleanup removes
  the requirement detail, backlog entry and this plan; permanent slot routing
  architecture retains the owner/epoch, incarnation and submission decisions.
