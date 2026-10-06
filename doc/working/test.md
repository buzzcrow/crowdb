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
