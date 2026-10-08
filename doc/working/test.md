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

| Test package       | Date       | Tests   | Seconds | Status                                  |
| ------------------ | ---------- | ------- | ------- | --------------------------------------- |
| `test-cpp`         | 2026-10-07 | 860/860 | 55      | ✓                                       |
| `test-core`        | 2026-10-07 | 977/978 | 368     | ✓ 1 ignored                            |
| `test-storage`     | 2026-10-07 | 811/815 | 1064    | ✓ 4 ignored                            |
| `test-access`      | 2026-10-07 | 906/906 | —       | ✓                                       |
| `test-console`     | 2026-10-07 | 77/79   | 61      | ✓ 2 ignored                            |
| `test-console-ui`  | 2026-10-07 | 63/63   | 192     | ✓                                       |
| `test-boto3-e2e`   | 2026-10-07 | 32/32   | 295     | ✓                                       |
| `test-iceberg-e2e` | 2026-10-07 | —       | 338     | ✓                                       |
| `test-iceberg-sdk` | 2026-10-07 | —       | —       | X Maven dependency resolution stalled  |

### intel7960

| Test package       | Date       | Tests     | Seconds | Status                   |
| ------------------ | ---------- | --------- | ------- | ------------------------ |
| `test-cpp`         | 2026-10-08 | 937/937   | 140.22  | ✓                        |
| `test-core`        | 2026-10-08 | 983/983   | 140.55  | ✓                        |
| `test-storage`     | 2026-10-08 | 820/820   | 686.02  | ✓                        |
| `test-access`      | 2026-10-08 | 1036/1037 | 385.12  | ✓ 1 separately scheduled |
| `test-console`     | 2026-10-08 | 307/308   | 885.01  | X                        |
| `test-console-ui`  | 2026-10-08 | 225/225   | 339.75  | ✓                        |
| `test-boto3-e2e`   | 2026-10-08 | 255/255   | 413.49  | ✓                        |
| `test-iceberg-e2e` | 2026-10-08 | 29/32     | 740.87  | ✓ 3 separately scheduled |
| `test-iceberg-sdk` | 2026-10-08 | 11/11     | 611.95  | ✓                        |

Measurement notes for this host:

- Final measurements are in progress after enabling ordinary native tests on
  Linux and fixing the failures they exposed. Pending rows are not results
  from the earlier code or the smaller test selection.
- The remaining Console inspection failure reproduces a Chunk-KV source
  restart rejected by a tree owner epoch ahead of its catalog assignment.
  Cutover recovery is tracked separately against
  [the ownership design](../design/chunkds/design-crowdb-chunk-kv-server.md#7-child-balance-state-machine); independent fixes do
  not resolve that failure or make the Console row a pass.
- Seconds cover each complete Pixi task, including prerequisite builds.
  Each Rust case is counted once per test binary; a later explicit run resolves
  its earlier ignored entry. SDK subprocess checks use their Rust harness
  cases. The child listener helper is invoked by its parent and is not counted
  as a separate acceptance case.
- Linux executes ordinary cluster, deployment and protocol tests. The macOS
  exceptions remain. Dedicated Iceberg native workloads run explicitly;
  Java and Boto3 SDK cases run in their respective environment tasks.
- Access's one separately scheduled case is
  `official_boto3_recognizes_a_copy_error_after_http_200_and_keepalives`; it
  passed in the complete Boto3 task. Iceberg E2E's three separately scheduled
  cases are the official Java catalog/snapshot, opaque metadata/data/delete
  reads and S3 FileIO checks; all passed in `test-iceberg-sdk`. These are
  environment-specific SDK harnesses, not skipped ordinary Linux tests.
- SIGSEGV was reproduced under GDB and ASAN. Concurrent RPC connection
  destruction freed its close callback while the I/O worker still executed it.
  Retaining the connection through both callbacks fixed the use-after-free.
  The C++ disconnect regressions passed 100 ASAN repetitions, and both actual
  leader-removal tests passed 40 GDB repetitions after the fix.
- Core, original binary, stacks and ASAN report are retained under
  `/tmp/crowdb-core-investigation/`; see its `README.md`. Earlier copied-binary
  trials without the required KV binary skipped tests and are not evidence.
- ChunkKV recovery now distinguishes materialized split halves from remaining
  overlays. Startup discards obsolete recovery only after a newer authoritative
  catalog generation is proven; unchanged-generation errors remain failures.
- The Console inspection fixture reached five partitions before DiskDB could
  no longer reserve a 256-MiB block on its small secondary disks. The fixture
  now provisions its secondary disks with the production-policy acceptance
  capacity while preserving the first disk's 80-zone browser contract;
  the twelve-partition assertion and its original deadline remain unchanged.
  Native failure logs are retained in
  `.crowdb-runtime/artifacts/native-restart-failure-1208503/`.
- Large values could exhaust the bounded split-sampling page after one key.
  Sampling now reads at most one extra live key using an exclusive continuation.
  The regression failed before the fix and passes with the existing sample and
  heartbeat bounds.
- The official Rust SDK fixture's lock entry still had version `0.2.2` while
  its manifest had `0.3.0`, so `--locked` rejected the build. The fixture lock
  now matches without changing third-party dependencies; the version gate also
  rejects a stale fixture lock entry. The locked build passed.
- The additional `test-rust-iceberg-e2e` task passed all 5 cases with no ignored
  tests in 1373.56 seconds, including full native retirement grace, namespace
  and table lifecycle, lost replies across listeners and native storage restart.
  This separate Rust SDK task is not included in the Java SDK row above.
  Its final logs are in
  `.crowdb-runtime/artifacts/measure-tests/20261008T150225.242092Z/`.
- Disposable CLI clusters inherit their outer test's port ownership. Process
  records survive data cleanup, and cleanup removes nested test claims while
  preserving operator namespaces. Earlier test claims exhausted the port range;
  removing confirmed test records and fixing their lifecycle restored all 983
  Core cases. The original claims are backed up in
  `.crowdb-runtime/artifacts/claims-before-owned-test-cleanup.json`.
- Complete measurement logs and results are retained under
  `.crowdb-runtime/artifacts/measure-tests/20261008T111850.953337Z/` and
  `.crowdb-runtime/artifacts/measure-tests/20261008T114852.396832Z/`.
  The final Core and subsequent tasks continue under
  `.crowdb-runtime/artifacts/measure-tests/20261008T121934.101621Z/`.
  The later full Console failure is recorded under
  `.crowdb-runtime/artifacts/measure-tests/20261008T131808.656006Z/`.
  Final independent-fix verification continues under
  `.crowdb-runtime/artifacts/measure-tests/20261008T144104.319807Z/`.
  Boto3's focused batch-delete case and complete affected-suite rerun passed
  without relaxed limits after removing old test processes.

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

Remaining action item from the macOS full-suite runs:

- [ ] `test-iceberg-sdk`: Maven `dependency:go-offline` stayed idle for more
  than eight minutes on macOS; rerun when the pinned dependency cache is
  available.

The complete macOS UI package now passes all 63 tests, including the 23-node
topology setup and the three-node KV/S3/Iceberg data flow.

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
