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

CI uses ten parallel jobs, grouped by runtime requirements. Component tasks in
`pixi.toml` select library, binary, and integration test targets with
`--tests`; benchmark targets are excluded. Group scripts under
`tools/pixi-tasks/` define execution order. GitHub Actions calls those group
tasks. See [tools/README.md](../../tools/README.md) for the tooling map.

| Job           | Group task                              | Coverage                                         |
| ------------- | --------------------------------------- | ------------------------------------------------ |
| Lint          | `test-task-coverage`, fmt, clippy       | Package assignments and reachable CI tasks       |
| CppTests      | `test-cpp`                              | C++ and Rust FFI                                 |
| UnitTests     | `test-unit`                             | Rust libraries, including `test-access-iceberg`  |
| ServerTests   | `test-server`                           | Native services, access server and monitor       |
| S3E2E         | `-e s3-e2e test-boto3-e2e`              | Access S3, access server and 17 boto3 cases      |
| IcebergE2E    | `-e iceberg-e2e test-iceberg-e2e`       | PyIceberg, native storage, GC and crash recovery |
| IcebergSDK    | `-e iceberg-e2e test-iceberg-sdk`       | Official Java/Rust SDKs and pinned Apache RCK    |
| ConsoleTests  | `test-console`                          | Shared operations, CLI and Web                   |
| UITests       | `test-console-ui`                       | Vitest and real-backend Playwright               |
| DockerPreview | `test-single-node-container`            | Linux amd64 image smoke and container E2E        |

Subprocess suites run sequentially inside each job and clean disposable runtime
state. Iceberg jobs use the pinned `iceberg-e2e` Pixi environment for Python,
Maven and Java; Rust/native builds use the default environment. The RCK task
fetches and verifies its exact Apache Iceberg source revision. Test-only child
listener functions remain ignored and are invoked by their parent crash tests.

`test-suite` runs the host groups, including both Iceberg groups. Docker is a
separate explicit `test-single-node-container` task requiring a Linux amd64
Docker host. It is always included in the DockerPreview CI job.

### Coverage guard

`pixi run test-task-coverage` validates every workspace package against
`TASK_PACKAGES` in `tools/ci-checks/check-test-task-coverage.py`, including the
test harness's own runtime-namespace tests.
The guard follows Pixi group calls and checked-in shell scripts from CI, so an
existing component task disconnected from its job fails validation. It also
requires explicit CI reachability for the container and client acceptance tasks.

### Adding tests

1. Add package tests under the owning crate's `tests/`; existing component tasks
   discover ordinary targets through `--all-targets`.
2. For a new package, add a component task and its `TASK_PACKAGES` assignment.
3. Add the component to the group script matching its runtime requirements.
4. Feature-gated or ignored tests require explicit task selectors. Do not count
   compiling an ignored test as executing it; exclude subprocess helper entries.
5. Run `pixi run test-task-coverage`, the affected suites, and workflow validation.
6. Measure changed suites with `pixi run bash tools/test-metrics/measure.sh TASK...`
   and update the timing table. Environment selection is automatic.

## Suite Timing

Keep baseline timings alongside new measurements to identify runtime regressions.
The machine columns retain independent runs; their dates appear below the header.
The m5pro measurement date was not recorded. New measurements, exact commands,
reported test counts and exit codes are saved under
`.crowdb-runtime/artifacts/measure-tests/`. Timing includes incremental builds
and subprocess startup/shutdown, so feature changes and cold builds affect it.
Counts are runner-reported cases, not assertions; ignored cases are excluded.
Native Iceberg and Java/Rust/RCK SDK acceptance use release binaries, matching
the published container profile. Component suites retain their default test
profile. The focused debug native 100 MiB multipart upload, completion,
restart, replay, and full read passed on 2026-09-29 in 54.40 s. Its previous
10 s completion deadline failure did not recur after the streaming I/O changes.

Status icons: ✅ = PASS, ⚠️ = PASS with ignored tests, ❌ = FAIL,
⏳ = measurement pending.
A dash means timing was not recorded, not a skipped test. Container scenarios
are checked by scripts and do not report a Rust-style test count.

| Suite                          | Tests | m5pro   | 5950-24.04 | 7960-24.04 | Status |
| ------------------------------ | ----- | ------- | ---------- | ---------- | ------ |
| Test date                      | —     | —       | 2026-09-10 | 2026-09-28 | —      |
| `test-tree-ct`                 | 568   | 20.1 s  | 52.75 s    | —          | ✅      |
| `test-common-ct`               | 28    | —       | 0.66 s     | —          | ✅      |
| `test-tree-ffi`                | 31    | 13.5 s  | 2.89 s     | —          | ✅      |
| `test-rpc-ct`                  | 67    | —       | 4.32 s     | —          | ✅      |
| `test-rpc-ffi`                 | 15    | —       | 10.43 s    | —          | ✅      |
| `test-diskio-ct`               | 121   | —       | 8.12 s     | —          | ✅      |
| `test-common`                  | 77    | 21.9 s  | 20.56 s    | —          | ✅      |
| `test-harness`                 | 2     | —       | —          | —          | ✅      |
| `test-protocol`                | 135   | 12.2 s  | 3.75 s     | —          | ✅      |
| `test-kv-core`                 | 572   | 43.2 s  | 72.87 s    | —          | ✅      |
| `test-kv-client`               | 58    | 23.4 s  | 27.55 s    | —          | ✅      |
| `test-chunkdb-client`          | 10    | 13.8 s  | 7.65 s     | —          | ✅      |
| `test-chunk-kv`                | 19    | —       | 5.32 s     | —          | ✅      |
| `test-chunk-stream`            | 15    | —       | 1.56 s     | —          | ✅      |
| `test-chunk-kv-client`         | 12    | —       | 0.37 s     | —          | ✅      |
| `test-chunk-kv-server`         | 24    | —       | 6.57 s     | —          | ✅      |
| `test-kv-server`               | 89    | 53.0 s  | 53.94 s    | —          | ✅      |
| `test-diskdb`                  | 141   | 42.8 s  | 36.88 s    | —          | ✅      |
| `test-diskdb-client`           | 7     | 13.9 s  | 25.44 s    | —          | ✅      |
| `test-chunkdb`                 | 102   | 27.8 s  | 41.61 s    | —          | ✅      |
| `test-chunk-client`            | 105   | —       | 57.98 s    | —          | ✅      |
| `test-diskio-client`           | 4     | —       | 10.33 s    | —          | ✅      |
| `test-access-s3`               | 59    | —       | 5.02 s     | —          | ✅      |
| `test-console-shared`          | 115   | 39.2 s  | 81.29 s    | —          | ✅      |
| `test-console-cli`             | 15    | 69.4 s  | 8.54 s     | —          | ✅      |
| `test-console-server`          | 82    | 50.7 s  | 79.91 s    | —          | ✅      |
| `test-console-ui`              | 142   | 165.7 s | 252.99 s   | —          | ✅      |
| `test-boto3-e2e`               | 162   | —       | 162 s      | —          | ✅      |
| `test-access-iceberg`          | 692   | —       | —          | 110.69 s   | ✅      |
| `test-access-server`           | 85    | —       | —          | 60.33 s    | ✅      |
| `test-monitor`                 | 56    | —       | —          | 45.86 s    | ✅      |
| `test-pyiceberg-e2e`           | 87    | —       | —          | —          | ✅      |
| `test-iceberg-native`          | 9     | —       | —          | 1024.67 s  | ✅      |
| `test-java-iceberg-e2e`        | 10    | —       | —          | 551.31 s   | ✅      |
| `test-java-iceberg-fileio-e2e` | 3     | —       | —          | —          | ✅      |
| `test-rust-iceberg-e2e`        | 5     | —       | —          | 1457.31 s  | ✅      |
| `test-iceberg-rck`             | 1     | —       | —          | 307.34 s   | ✅      |
| `test-single-node-container`   | —     | —       | —          | —          | ✅      |

The Java group includes the FileIO task; its row is not an additional run.
PyIceberg, S3, UI and container acceptance passed before timing collection;
their task wall-clock durations were not recorded. The completed UI rerun has
86 component and 56 browser cases (browser runner time: 4.7 minutes).

---

## Slowest Tests (2026-09-10)

All individual tests or test binaries with wall-clock time >= 7 s.

| Suite                 | Time    | Test / binary                                                               |
| --------------------- | ------- | --------------------------------------------------------------------------- |
| `test-kv-core`        | 38.76 s | `group_test` — Paxos group election, reconfiguration, recovery (99 tests)   |
| `test-console-ui`     | 21.7 s  | `13-todo-ui-behavior:29` — deploy 3 nodes, disjoint DiskDB listeners        |
| `test-chunk-client`   | 18.33 s | `small_object_writer_e2e` — small-write E2E with real ChunkDB + DiskIO (14) |
| `test-console-server` | 18.07 s | `cluster_deployer_test` — deployer lifecycle (3 tests)                      |
| `test-console-shared` | 15.12 s | `lifecycle_e2e_test` — lifecycle E2E (1 test)                               |
| `test-console-server` | 13.53 s | `rolling_upgrade_test` — rolling upgrade (1 test)                           |
| `test-console-ui`     | 10.8 s  | `50-chunk-capacity-disk-group:428` — assign disk-group to diskdb via UI     |
| `test-chunk-client`   | 10.39 s | `chunk_reader_e2e` — chunk reader E2E with failure injection (6 tests)      |
| `test-console-server` | 9.93 s  | `cluster_restart_incremental_test` — restart cycles (5 tests)               |
| `test-console-ui`     | 8.9 s   | `21-kv-reconfig:254` — stop non-leader, stop leader triggers reelection     |
| `test-chunkdb`        | 8.42 s  | `full_stack_test` — full stack E2E (20 tests)                               |
| `test-console-ui`     | 8.0 s   | `13-todo-ui-behavior:269` — close dialog, preserve KV on DiskDB fail        |
| `test-console-server` | 7.97 s  | `replica_leader_removal_test` — leader removal (2 tests)                    |
| `test-kv-server`      | 7.75 s  | `cluster_e2e_test` — cluster E2E with kv-server subprocess spawns (6)       |
| `test-console-ui`     | 7.7 s   | `31-kv-ops-advanced:98` — prefix/selected/inline delete + copy, load more   |

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
