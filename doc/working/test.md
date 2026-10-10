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
| `test-core`        | 2026-10-09 | 994/994   | ~140.23 | ✓                        |
| `test-storage`     | 2026-10-09 | 841/841   | 916.53  | ✓                        |
| `test-access`      | 2026-10-09 | 1036/1037 | 412.60  | Pending: coverage gap    |
| `test-console`     | 2026-10-09 | 350/350   | ~1588.1 | ✓                        |
| `test-console-ui`  | 2026-10-09 | 229/229   | 307.40  | ✓                        |
| `test-boto3-e2e`   | 2026-10-09 | 255/255   | 428.89  | ✓                        |
| `test-iceberg-e2e` | 2026-10-09 | 29/32     | 789.37  | Pending: coverage gap    |
| `test-iceberg-sdk` | 2026-10-09 | 11/11     | 571.23  | ✓                        |

Measurement notes for this host:

- Access and Iceberg rows retain their earlier partial-run counts/times, but
  are not complete task acceptance. The parent tasks must dispatch their SDK
  cases automatically in one invocation; see
  [task completeness plan](plan-test-task-completeness.md). Prior passing SDK
  runs do not close those parent-task coverage gaps.

- Latest complete Console gate passes 350 Rust test executions (including
  three dedicated phase wrapper reruns), zero failed/ignored. All 23 native
  browser cases pass: 20 in the main fixture, plus one in each dedicated
  prerequisite-plan, journal interruption, and production split phase.
  Dedicated wrapper times: 17.90s, 50.09s, 308.15s; final counts are 4/4/4
  with complete acknowledged-data checks. The task interval is approximately
  1588.13s from log creation to final write, not an independent wall timer.
  Start samples across two fixtures: ChunkDB 215/255ms, DiskIO 366/466ms,
  ChunkKV 789/3800ms, Access 178/148ms. Native Start response budget is 5s;
  process/DOM assertions remain 3s. Core recovery is unchanged; fsync has
  not been established as the delay source. Full log:
  `.crowdb-runtime/artifacts/balance-policy-20261009/console-start-budget-final.out`.

- Latest complete UI task passes 166 unit and all 63 browser cases (229/229),
  measured independently at 307.40 seconds. Reload cancellation can no longer
  publish failed progress from an old page; serial deployment completions and
  bucket creation are asserted before their durable/DOM checks. Budgets and
  real KV/S3/Iceberg data assertions are retained. Log:
  `.crowdb-runtime/artifacts/balance-policy-20261009/ui-reload-acceptance-final.out`.
- The three separately scheduled native phases now pass explicitly, zero
  ignored: prerequisite-plan resumption 19.32s, journal interruption 54.26s,
  production split/count convergence and complete data checks 350.85s (4/4/4).
  Their logs are `native-plan-response-order.out`, `native-journal-final.out`,
  and `native-count-final.out` in the balance-policy artifact directory.
  These focused successes do not replace complete Console acceptance; native
  Start timing follow-up passes: ChunkDB 215ms, DiskIO 366ms, ChunkKV 789ms,
  Access 178ms (browser 9.2s, owned fixture 375.59s). The earlier recovery
  reached its listener after 3.15s. User-authorized native Start response
  budget is now 5s; process/DOM assertions remain 3s. Full Console rerun now passes; R228/R229 remain deferred. Timing log:
  `.crowdb-runtime/artifacts/balance-policy-20261009/native-lifecycle-start-distribution.out`.

- Previous full Console gate: 307 completed Rust cases pass, one native wrapper
  fails (zero ignored Rust cases). Its browser phase passes 19, fails the
  lifecycle case and schedules three cases separately. The gate stops before
  those independent phases. S3 passes 8/8 again (159.10s). First lifecycle
  divergence: Start is still restoring trees during the immediate 3s PID poll;
  cleanup sends duplicate restart and reports 409. Test correction awaits and
  asserts that original response before the unchanged process/DOM assertion.
  Focused lifecycle acceptance also fails the unchanged 3s response budget
  (345.63s fixture); core startup is unchanged and fsync is not yet established
  as the cause. `~1111s` (Console) and `~371s` (UI) are
  task-log creation-to-final-write intervals, not independent wall timers.
  Complete log: `.crowdb-runtime/artifacts/balance-policy-20261009/count-only-console.out`.

- Sequential same-group additions fix the topology fixture: its complete
  spec passes 5/5 in 33.1s; original isolated case failed 2/3 ready groups.
  Cross-UI/server conflict protection remains proposed in R229, not implemented.
- Earlier count-only UI attempt: 164 unit tests and 62/63 browser tests pass.
  Multi-rack topology leader election observes 2/3 ready groups within its
  unchanged 10s budget; three-node real-data flow passes. Full task log:
  `.crowdb-runtime/artifacts/balance-policy-20261009/count-only-ui.out`.
- Approved allocation routing change passes both allocator regressions:
  pre-send failure reroutes once; discarded post-allocation reply does not
  replay (durable busy bytes checked). Complete serial S3 mini-cluster file
  passes 8/8, zero ignored, in 183.98s after rebuilding services. Full Console
  acceptance is pending.

- Current count-only Core passes 994/994 with zero ignored. Its ~140.23s is
  the complete task log's creation-to-final-write interval, not a separately
  captured wall-clock timer. Log:
  `.crowdb-runtime/artifacts/balance-policy-20261009/count-only-core.out`.
  Actual native Page/hidden-Weight acceptance passes (browser 2.3s, owned
  cluster 345.98s); data-weight placement is deferred to R228, not marked passed.
- Current S3 outage reproduction: exact node-3 case passes in 36.20s; full
  serial mini-cluster file fails 7/8 in 193.41s on node-1. Group 301 allocation
  contacts removed endpoint 11200 after ownership recovery. This is a stale
  DiskDB routing cache, not a crash/fsync timeout. Original runtime was
  `.crowdb-runtime/ephemeral/s3-mini-protected-outage-1-2995274-5/`; full-gate
  cleanup removed this disposable runtime. First-divergence details are retained.
  Full Console still lacks final acceptance; no retry/readiness workaround added.
- Latest balance implementation: Core 993/993, Storage 841/841 and full UI 227/227 pass, with
  zero ignored cases. UI includes all 63 browser cases, including the original
  six-service recovery and three-node real KV/S3/Iceberg flow. Logs are under
  `.crowdb-runtime/artifacts/measure-tests/20261009T053116.399322Z/`.
- Latest consistent Console attempt stops at the node-3 S3 outage test: large
  object allocation contacts the stopped DiskDB endpoint and returns 503.
  Its 158/159 count covers only the stages reached, not the full suite. Logs:
  `.crowdb-runtime/artifacts/measure-tests/20261009T052410.043897Z/`;
  service logs are preserved under
  `.crowdb-runtime/artifacts/balance-policy-20261009/outage-3/`.
  The preceding attempt mixed old/new diagnostic binaries during a field
  addition and is not final acceptance evidence; it also observed an independent
  empty multipart ListParts response after successful uploads. Both remain
  investigation items, with no deadline increases or caller retries.
- Isolated normal native restart/multipart acceptance passes in 24.03 seconds.
  Dedicated weighted acceptance fails in 968.43 seconds: new acknowledged
  writes do not update the opened manifest's retained-pack estimate without a
  checkpoint, so the unified policy continues to observe tolerance rather than
  the intended new imbalance. Its real split inherited/current Journal browser
  case passes in 18.7 seconds. This is incomplete acceptance, not a Linux skip;
  logs are in `.crowdb-runtime/artifacts/balance-policy-20261009/` and the
  statistics boundary is recorded for review in `plan-chunk-kv-cutover.md`.
- The following earlier measurements describe the pre-balance baseline.
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
