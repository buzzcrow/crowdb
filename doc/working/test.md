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

CI uses six parallel jobs. The jobs are grouped by execution environment and
process isolation, not one job per Rust package. Each job pays a fixed setup
overhead for checkout, system packages, Pixi, and the Cargo cache, so grouping
compatible tests keeps wall-clock time low without mixing incompatible runtime
requirements.

The component task is the source of truth for running a package's tests. The
group task is the source of truth for assigning component tasks to a CI job.
GitHub Actions runs only the group tasks; developers can run either level
locally.

| Job | Group task | Component tasks | Environment |
| --- | --- | --- | --- |
| **Lint** | `test-task-coverage` | `cargo fmt`, `cargo clippy` | Formatting, linting, and package-to-task coverage validation |
| **CppTests** | `test-cpp` | `test-tree-ct`, `test-common-ct`, `test-rpc-ct`, `test-diskio-ct`, `test-tree-ffi`, `test-rpc-ffi` | CMake-built C++ tests and Rust FFI tests |
| **UnitTests** | `test-unit` | `test-common`, `test-protocol`, `test-kv-core`, `test-kv-client`, `test-chunkdb-client` | Pure Rust tests without subprocess dependencies |
| **ServerTests** | `test-server` | `test-kv-server`, `test-diskdb`, `test-diskdb-client`, `test-chunkdb`, `test-chunk-client`, `test-diskio-client` | Tests that spawn KV, DiskDB, or DiskIO processes |
| **ConsoleTests** | `test-console` | `test-console-shared`, `test-console-cli`, `test-console-server` | Console and lifecycle tests that spawn KV servers |
| **UITests** | `test-console-ui` | Frontend Vitest and Playwright E2E | Real backend subprocesses and system browser |

All test tasks live in the `# ── Test ──` section of `pixi.toml`. Group
tasks invoke the component tasks in a fixed order and use `set -e`, so a
component failure stops the group. Subprocess groups clean the environment
before execution, and the ServerTests, ConsoleTests, and UITests jobs perform
an `always()` cleanup after their test and artifact steps.

### Coverage guard

`pixi run test-task-coverage` runs
`tools/check-test-task-coverage.py`. It reads Cargo workspace metadata and
requires every workspace package to be assigned to a component test task. The
Lint job runs this guard before the other test jobs, preventing a new Rust
package from silently disappearing from CI.

The only allowlisted support packages are:

- `crowdb-test-harness`: support library covered through dependent package tests.
- `crowdb-port-alloc`: E2E support binary exercised by `test-console-ui`.

A new test folder inside an assigned package needs no CI mapping change because
the component task uses `cargo test -p <package> --all-targets`. A new workspace
package must be added to `TASK_PACKAGES` in the coverage script and assigned to
the appropriate component task.

### Adding tests

1. Add or update the component task in `pixi.toml`.
2. Choose the group by runtime requirements:
   - CMake-built C++ tests → `test-cpp`.
   - Rust tests without subprocesses → `test-unit`.
   - Server or storage subprocesses → `test-server`.
   - Console lifecycle or CLI subprocesses → `test-console`.
   - Browser E2E → `test-console-ui`.
3. Add a new workspace package to `TASK_PACKAGES` when applicable.
4. Run `pixi run test-task-coverage` and the affected group task locally.
5. Update `.github/workflows/ci.yml` only when adding a new CI job or changing
   the group-to-job mapping.

## Suite Timing

The historical measurements below were taken on 2026-08-28 on Linux
(post-task-fb pull `81d56124` + KV client dead-connection fix) and macOS.
The latest Linux run was performed on 2026-09-05 by running each component
Pixi task independently, in table order. Latest times are wall-clock task
times including incremental build and subprocess startup/shutdown. C++ ctest
suites report their test count from ctest; Rust and UI suites report the
runner's test results. A timeout is recorded when the task exceeded the
300-second per-suite limit; it is not counted as an assertion failure.

Status icons: ✅ = PASS (0 failures), ⚠️ = PASS with ignored tests, ❌ = TIMEOUT or failures.

| Suite                 | Tests | macOS    | Linux (08-28) | Linux (09-06) | Status |
| --------------------- | ----- | -------- | ------------- | ------------- | ------ |
| `test-tree-ct`        | 444   | 20.1 s   | 15.78 s       | 42.2 s        | ✅     |
| `test-common-ct`      | 26    | —        | 17.78 s       | 0.60 s        | ✅     |
| `test-tree-ffi`       | 31    | 13.5 s   | 0.54 s        | 0.84 s        | ✅     |
| `test-rpc-ct`         | 63    | —        | 2.59 s        | 4.14 s        | ✅     |
| `test-rpc-ffi`        | 15    | —        | 0.68 s        | 0.75 s        | ✅     |
| `test-diskio-ct`      | 99    | —        | 4.46 s        | 6.93 s        | ✅     |
| `test-common`         | 74    | 21.9 s   | 9.78 s        | 9.99 s        | ✅     |
| `test-protocol`       | 127   | 12.2 s   | 0.70 s        | 0.35 s        | ✅     |
| `test-kv-core`        | 570   | 43.2 s   | 63.20 s       | 57.06 s       | ✅     |
| `test-kv-client`      | 58    | 23.4 s   | 4.70 s        | 6.34 s        | ✅     |
| `test-chunkdb-client` | 10    | 13.8 s   | 2.00 s        | 3.30 s        | ✅     |
| `test-kv-server`      | 87    | 53.0 s   | 43.52 s       | 44.06 s       | ✅     |
| `test-diskdb`         | 138   | 42.8 s   | 25.76 s       | 26.30 s       | ✅     |
| `test-diskdb-client`  | 7     | 13.9 s   | 16.81 s       | 23.08 s       | ✅     |
| `test-chunkdb`        | 79    | 27.8 s   | 19.75 s       | 39.32 s       | ✅     |
| `test-chunk-client`   | 56    | —        | 12.11 s       | 10.78 s       | ✅     |
| `test-diskio-client`  | 4     | —        | 43.22 s       | 19.31 s       | ✅     |
| `test-console-shared` | 114   | 39.2 s   | 9.36 s        | 59.91 s       | ✅     |
| `test-console-cli`    | 13    | 69.4 s   | 44.09 s       | 10.44 s       | ✅     |
| `test-console-server` | 81    | 50.7 s   | 42.72 s       | 69.67 s       | ✅     |
| `test-console-ui`     | 138   | 165.7 s  | 270.0 s       | 254.98 s      | ✅     |

---

## Slowest Tests (2026-09-06)

All individual tests or test binaries with wall-clock time >= 7 s.

| Suite                 | Time    | Test / binary                                                              |
| --------------------- | ------- | -------------------------------------------------------------------------- |
| `test-kv-core`        | 41.64 s | `group_test` — Paxos group election, reconfiguration, recovery (99 tests)  |
| `test-console-shared` | 15.13 s | `lifecycle_e2e_test` — lifecycle E2E (1 test)                              |
| `test-console-server` | 14.58 s | `replica_leader_removal_test` — leader removal (2 tests)                   |
| `test-console-server` | 13.50 s | `rolling_upgrade_test` — rolling upgrade (1 test)                          |
| `test-diskio-client`  | 11.15 s | `disk_io_full_test` — full disk IO E2E (1 test)                            |
| `test-console-server` | 10.35 s | `cluster_restart_incremental_test` — restart cycles (5 tests)              |
| `test-console-ui`     | 10.7 s  | `50-chunk-capacity-disk-group:428` — assign disk-group to diskdb via UI    |
| `test-kv-server`      | 9.39 s  | `cluster_e2e_test` — cluster E2E with kv-server subprocess spawns (6)      |
| `test-console-ui`     | 8.8 s   | `21-kv-reconfig:254` — stop non-leader, stop leader triggers reelection    |
| `test-console-ui`     | 7.9 s   | `13-todo-ui-behavior:269` — close dialog, preserve KV on DiskDB fail       |
| `test-console-ui`     | 7.7 s   | `31-kv-ops-advanced:98` — prefix/selected/inline delete + copy, load more  |
| `test-chunkdb`        | 7.39 s  | `full_stack_test` — full stack E2E (7 tests)                               |
| `test-console-server` | 7.37 s  | `cluster_deployer_test` — deployer lifecycle (3 tests)                     |
| `test-common`         | 7.01 s  | `config_test` — config watcher tests (8 tests)                             |

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
