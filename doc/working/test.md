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
| `test-cpp`         | 2026-10-09 | 942/942   | 131.26  | ✓                        |
| `test-core`        | 2026-10-09 | 987/987   | 126.92  | ✓                        |
| `test-storage`     | 2026-10-09 | 839/839   | 828.45  | ✓                        |
| `test-access`      | 2026-10-09 | 1036/1037 | 412.60  | ✓ 1 separately scheduled |
| `test-console`     | 2026-10-09 | 307/308   | 877.17  | X partial                |
| `test-console-ui`  | 2026-10-09 | 225/225   | 306.98  | ✓                        |
| `test-boto3-e2e`   | 2026-10-09 | 255/255   | 428.89  | ✓                        |
| `test-iceberg-e2e` | 2026-10-09 | 29/32     | 789.37  | ✓ 3 separately scheduled |
| `test-iceberg-sdk` | 2026-10-09 | 11/11     | 571.23  | ✓                        |

Measurement notes for this host:

- Full UI passes 225/225 in 306.98 seconds after its topology preparation
  checks current complete membership and matching terms instead of caching an
  earlier leader seen while a new replica is unknown. The entire affected
  five-case spec also passes. Original six-service recovery and real three-node
  KV/S3/Iceberg data assertions pass in both complete UI attempts.
- Console remains failing. Serial Console-shared acceptance passes the S3
  cluster cases; the full attempt still fails real Page observation with a
  changed catalog generation before any browser lifecycle mutation. Ordering
  alone does not establish a stable catalog. A stronger focused preparation
  check requires one normal policy cooldown at the same complete generation;
  it fails within the unchanged ten-minute preparation deadline because new
  weighted transfers continue after reaching 4/4/4. Those unsuccessful
  experiments are withdrawn and archived pending fixture-versus-convergence
  review. The focused run takes 672.13 seconds including setup and teardown.
  Logs are retained at
  `.crowdb-runtime/artifacts/native-stable-catalog-focused.out` and
  `.crowdb-runtime/artifacts/native-restart-failure-2545228/`.
- Simultaneous S3 cluster startup previously failed during tree bootstrap.
  Block WAL sync reached 2.13 seconds and leadership changed. The exact test
  passes alone (31.12 seconds); all eight S3 cluster cases pass serially
  (176.24 seconds). Console-shared acceptance now runs sequentially with every
  case and durable sync retained.
- Eight of the nine Linux tasks pass, with the additional Rust SDK task also
  passing. Console remains incomplete at native catalog observation. Its latest
  full attempt used the subsequently withdrawn ordering experiment; no complete
  pass of the restored selection is claimed. The three later fixture phases
  were not reached in that failed attempt. Final independent measurements are
  retained in `.crowdb-runtime/artifacts/measure-tests/20261009T022131.804646Z/`.
- The original Console source/root epoch failure has passing focused recovery
  regressions and native split/transfer data checks. An earlier focused native
  browser selection with block defaults passes journal owner interruption,
  Iceberg metadata inspection and S3 multipart pagination. Its enclosing
  native case passed in 398.17 seconds. This is not a full Console pass. Complete Storage
  subsequently passes after the additional handoff lost-response and abort
  regressions.
- Seconds cover each complete Pixi task, including prerequisite builds.
  An earlier Storage attempt stopped at the real-service restart case: direct
  block WAL reads returned EINVAL and replay skipped the segment. Aligned reads
  and narrowly scoped empty-tail cleanup fix recovery; all 118 group tests,
  109 WAL tests and the complete 839-case Storage task now pass.
  Each Rust case is counted once per test binary; a later explicit run resolves
  its earlier ignored entry. SDK subprocess checks use their Rust harness
  cases. The child listener helper is invoked by its parent and is not counted
  as a separate acceptance case.
- Cluster processes explicitly use block tree storage and block-device WAL;
  these are also the system defaults. Durable synchronization remains enabled.
  The user accepted occasional slow sync through file-backed device simulation
  on 2026-10-09. Native API preparation requests use a separate ten-second
  budget and cases allow three minutes; page actions and UI assertions retain
  their three-second budgets. Ordinary UI tests retain their existing budgets.
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
  tests in 1376.40 seconds, including full native retirement grace, namespace
  and table lifecycle, lost replies across listeners and native storage restart.
  This separate Rust SDK task is not included in the Java SDK row above.
  Its final logs are in
  `.crowdb-runtime/artifacts/measure-tests/20261009T022131.804646Z/`.
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
