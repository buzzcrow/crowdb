<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Test Task Backlog

<!-- DO NOT DELETE THIS FILE — it is a persistent backlog, not a per-task draft. -->

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/kv/design-crowdb-kv-test.md`](../design/kv/design-crowdb-kv-test.md).

## Current CI Test Design

Regular CI uses nine parallel jobs, grouped by runtime requirements. Local tests are
organized by component tasks in `pixi.toml`; each component script owns its package
list and execution order. GitHub Actions calls the same component tasks. See
[tools/README.md](../../tools/README.md) for the tooling map.

| Job           | Component task(s)                       | Coverage                                         |
| ------------- | ---------------------------------------- | ------------------------------------------------ |
| Lint          | `check-ci-test-tasks`, fmt, clippy        | Package assignments and reachable CI tasks       |
| CppTests      | `test-cpp`                               | C++ and Rust FFI                                 |
| UnitTests     | `test-core`                              | Core Rust libraries                              |
| ServerTests   | `test-storage`, `test-access`             | Native services, streams, access and monitor     |
| S3E2E         | `-e s3-e2e test-boto3-e2e`              | Access S3, access server and 17 boto3 cases      |
| IcebergE2E    | `-e iceberg-e2e test-iceberg-e2e`       | PyIceberg, native storage, GC and crash recovery |
| IcebergSDK    | `-e iceberg-e2e test-iceberg-sdk`       | Official Java SDK and pinned Apache RCK          |
| ConsoleTests  | `test-console`                          | Shared operations, CLI and Web                   |
| UITests       | `test-console-ui`                       | Vitest and real-backend Playwright               |

Subprocess suites run sequentially inside each job and clean disposable runtime
state. Iceberg jobs use the pinned `iceberg-e2e` Pixi environment for Python,
Maven and Java; Rust/native builds use the default environment. The RCK task
fetches and verifies its exact Apache Iceberg source revision. Test-only child
listener functions remain ignored and are invoked by their parent crash tests.

The `test-storage` component builds the KV, DiskDB and ChunkDB binaries before
running stream acceptance tests. The `test-console` component builds the
runtime binaries needed for deployment and restart coverage. Components must
work without service binaries left by another job.

`test-suite` runs the host groups, including both Iceberg groups and the Rust
SDK task. The Rust SDK task is available through
`pixi run -e iceberg-e2e test-rust-iceberg-e2e` and the manual-only
`IcebergRustSDK` workflow. It does not run on regular pushes or pull requests.
DockerPreview is a manual-only workflow that runs
`pixi run test-single-node-container` on a Linux amd64 Docker host. The release
workflow also runs this image test before publication.

IcebergE2E uses the release profile for PyIceberg and native acceptance. The
access-server component suite runs in ServerTests, so IcebergE2E does not run it
again.

### CI test-task check

`pixi run check-ci-test-tasks` validates every workspace package against
`COMPONENT_PACKAGES` in `tools/ci-checks/check-ci-test-tasks.py`, including the
test harness's own runtime-namespace tests.
The guard follows Pixi group calls and checked-in shell scripts from CI, so a
required component task disconnected from its job fails validation. It also
checks that DockerPreview and IcebergRustSDK are reachable from their manual
workflows and absent from regular CI.

### Adding tests

1. Add package tests under the owning crate's `tests/`; existing component tasks
   discover ordinary targets through `--all-targets`.
2. For a new package, add a component task and its `TASK_PACKAGES` assignment.
3. Add the component to the group script matching its runtime requirements.
4. Feature-gated or ignored tests require explicit task selectors. Do not count
   compiling an ignored test as executing it; exclude subprocess helper entries.
5. Run `pixi run check-ci-test-tasks`, the affected suites, and workflow validation.
6. Measure changed suites with `pixi run bash tools/test-metrics/measure.sh TASK...`
   and update the timing table. Environment selection is automatic.

## Suite Timing

Each platform section keeps one row per test package and only the latest run.
Leave unexecuted packages in place with `—`; record elapsed time in seconds.

### macOS

| Test package       | Date       | Tests                                    | Seconds | Status |
| ------------------ | ---------- | ---------------------------------------- | ------- | ------ |
| `test-cpp`         | —          | —                                        | —       | ⏳     |
| `test-core`        | —          | —                                        | —       | ⏳     |
| `test-storage`     | —          | —                                        | —       | ⏳     |
| `test-access`      | —          | —                                        | —       | ⏳     |
| `test-console`     | —          | —                                        | —       | ⏳     |
| `test-console-ui`  | 2026-10-07 | 63 (55 passed, 4 failed, 4 did not run) | 336     | ❌     |
| `test-boto3-e2e`   | —          | —                                        | —       | ⏳     |
| `test-iceberg-e2e` | —          | —                                        | —       | ⏳     |
| `test-iceberg-sdk` | —          | —                                        | —       | ⏳     |

### intel7960

| Test package       | Date | Tests | Seconds | Status |
| ------------------ | ---- | ----- | ------- | ------ |
| `test-cpp`         | —    | —     | —       | ⏳     |
| `test-core`        | —    | —     | —       | ⏳     |
| `test-storage`     | —    | —     | —       | ⏳     |
| `test-access`      | —    | —     | —       | ⏳     |
| `test-console`     | —    | —     | —       | ⏳     |
| `test-console-ui`  | —    | —     | —       | ⏳     |
| `test-boto3-e2e`   | —    | —     | —       | ⏳     |
| `test-iceberg-e2e` | —    | —     | —       | ⏳     |
| `test-iceberg-sdk` | —    | —     | —       | ⏳     |

### amd5950

| Test package       | Date | Tests | Seconds | Status |
| ------------------ | ---- | ----- | ------- | ------ |
| `test-cpp`         | —    | —     | —       | ⏳     |
| `test-core`        | —    | —     | —       | ⏳     |
| `test-storage`     | —    | —     | —       | ⏳     |
| `test-access`      | —    | —     | —       | ⏳     |
| `test-console`     | —    | —     | —       | ⏳     |
| `test-console-ui`  | —    | —     | —       | ⏳     |
| `test-boto3-e2e`   | —    | —     | —       | ⏳     |
| `test-iceberg-e2e` | —    | —     | —       | ⏳     |
| `test-iceberg-sdk` | —    | —     | —       | ⏳     |

The measurement helper writes logs and aggregate results below
`.crowdb-runtime/artifacts/measure-tests/`. Keep slow individual-test notes next
to the component measurement that produced them.

Action items from the macOS full-suite runs:

- [ ] `22-kv-topology`: eliminate live KV registration races under full-suite load.
- [ ] `11-cluster-server-lifecycle`: keep the Deploy action enabled while the UI dialog initializes.
- [ ] `12-cluster-node-inspect`: prevent the full-suite timeout and SVG `NaN` geometry errors.
- [ ] `90-flow-full-chain`: prevent SVG `NaN` console errors under full-suite load.

The lifecycle, reconfiguration, topology, and three-node data tests pass when
rerun individually after the macOS readiness and port fixes. The package row
remains failed until the full suite is green.

---

## WAL Subsystem

Source: `lib/crowdb-kv/src/wal/`. Tests: 12 files, ~92 tests.

- [ ] **WAL disk-loss recovery (full fail-out)**: the full fail-out procedure
  (step-out RPC + reconfiguration, `design-crowdb-kv-wal.md` §8.1) is not yet
  implemented. The test should verify the node fails out of the group and
  rejoins via snapshot install after the disk is replaced. **Blocked** on the
  fail-out feature landing.

## Store

Source: `lib/crowdb-kv/src/store/`. Tests: 8 files, 26 tests.

- [ ] **Per-group WAL disk isolation**: `WalConfig.wal_disks` is per-`WalEngine`,
  not per-group within a store — the server startup path
  (`create_group_with_wal`) derives `wal_disks` from the store-level config, so
  groups cannot be assigned different physical disks. **Blocked** on a
  store-level config change to support per-group `wal_disks` override.

## Deployment

Source: `app/crowdb-kv-server/`. Tests: 9 files.

- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.
