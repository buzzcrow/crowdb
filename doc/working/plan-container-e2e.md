<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: establish centralized container fixtures and migrate the first KV real-server scenario; other layers remain pending.

## First batch

- [x] **Inventory**: classify kv-server and shared harness launch sites; preserve component logic tests. Files: `container/crowdb-e2e/README.md`.
- [x] **KV fixture**: allocate labeled private bridge networks and per-node volumes, start packaged KV servers with the existing block/block-device/e2e profile, expose loopback management ports (container management `7000`, RPC `7001..7011`), wire real RPC topology, collect sanitized diagnostics and teardown owned resources. Files: `container/crowdb-e2e/fixture.py`.
- [x] **Protocol migration**: move `e2e_three_node_cluster_kv_put_batch_delete` without losing assertions into a Rust test client executed as a sidecar on the fixture network. Record image ID/source labels and client source. Files: `container/crowdb-e2e/Cargo.toml`, `tests/kv_test.rs`, `tests/common.rs`, application source test.
- [x] **Recovery and isolation**: crash/restart persisted servers, verify exact data/tombstones, run two clusters concurrently and terminate one before verifying the survivor. Enforce concurrency/CPU/memory budgets and interruption cleanup. Files: component runner and fixture tests.
- [x] **CI**: consume the existing once-built OCI artifact in a KV job; run the identical local entry point and upload diagnostics. Keep release publication dependent on its accepted digest. Files: Pixi tasks and OCI workflows.
- [x] **Verification**: fixture unit/failure tests, KV sidecar E2E, missing image rejection, Rust fmt/clippy, Python and workflow lint. Commit the coherent first batch; retain this plan/backlog for subsequent migration.

## Files

- `container/crowdb-e2e/`: lifecycle, runner, Rust client tests, unit tests, documentation.
- `app/crowdb-kv-server/tests/cluster_e2e_test.rs`: remove only the migrated scenario.
- `Cargo.toml`, `Cargo.lock`, `pixi.toml`, CI component inventory/workflows.

## Tests

- Unit/integration: budget rejection, partial startup failure, owned cleanup, original-error preservation, diagnostics redaction.
- E2E: original three-voter CRUD/batch/delete assertions, retained WAL-backed values after SIGKILL/restart, two isolated networks with identical ports/store/replica IDs and different values; survivor remains usable after other fixture teardown.
- Gates: `pixi run test-container-e2e-fixture`, `pixi run test-container-e2e --layer kv --concurrency 2`, `pixi run rs-fmt-check`, `pixi run rs-lint`.

## First-batch verification

- Shared runtime rebuilt/imported; tested config ID `sha256:47411b55b98365a5de3e0b85684d68eeb9901a61ac94d9a93b7674842282219c`, OCI manifest `sha256:c4ef05c71f45771314b09263412bb538bb6009ab02d177fab6d5dbffc9e6a33c`.
- Required two-slot container acceptance passed: both original CRUD cases, one three-server SIGKILL/restart and persisted-data check, full fixture retirement, survivor exact-value/tombstone check.
- Fixture tests passed, including partial allocations, interruption, original-error preservation, resource budgets, redaction, endpoint refresh and fault targeting. The 12 OCI policy/artifact tests passed.
- Remaining five kv-server cluster cases passed; missing image fails before allocation.
- Workspace Rust fmt/clippy, changed Python/workflow lint and CI task reachability passed. Remote workflow execution remains unverified.
- User-requested `pixi run clean` completed after gates, releasing roughly 659 GB. A root-owned sandbox cache initially blocked Cargo cleanup; its generated cache ownership was repaired and the full clean rerun succeeded. Runtime/build/test artifacts were removed; the local Docker image remains available.

## Listener rule for all later migrations

- Follow [TCP listener ownership](../design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart): set/check `SO_REUSEADDR` before bind on every RPC/HTTP listener; fixed endpoints survive restarts and `TIME_WAIT`.
- Wait for the old listener/process to exit. A live owner causes explicit `EADDRINUSE`; no automatic port increment and no default `SO_REUSEPORT`.
- Audit each server/framework as its tests migrate; include same-port restart and concurrent-live-bind rejection coverage. The C++ RPC server already requests `SO_REUSEADDR`; check option-error handling and HTTP listener equivalence during the service migration.

## Remaining migration

- Fixed four-digit service defaults and progressive retirement of legacy port allocators after their callers migrate.
- Snapshot join, reconfiguration, other kv-server process scenarios; mixed tests retain internal checks.
- Diskdb, Diskio, Chunk/storage, access, console and existing deployment suites.
- Runtime adapters beyond system Docker, host/hardware profiles, and full inventory outside the first batch.
