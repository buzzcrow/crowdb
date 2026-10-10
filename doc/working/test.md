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

## Target test classification and timing

This is the target classification for [R230](../backlog/R230-test-container-layer-e2e.md).
Rows are scope identifiers, not implemented Pixi commands. The existing KV batch
covers only part of these flows; pending rows do not imply successful migration.
Production servers use the release container image, optionally retaining debug
information. Clients execute on the host against container IPs/fixed ports.
Each node has a separately mounted host data/log/crash root.

Preserve distinct component UT flow/edge/fault coverage, not every old case.
Review overlap across modules and E2E; merge/delete redundant checks only after
mapping assertions to a verified owner. Retain finer internal state/race/crash-cut
tests locally. Once migrated server E2E passes, delete its old case/unused launcher;
split mixed tests first. Track missing important flows and add focused coverage.
Test counts describe executions; they are not a coverage improvement metric.

`tools/benchmark/bench-chunk-kv-regression.sh` and its selections are explicitly
deferred: preserve their current behavior during migration and decide their
classification/consolidation only after the remaining work is complete.

### Classification

CI groups below are proposed larger jobs. A job can dispatch several independently
selected flow groups; measurements and results remain at the finer scope below.
Component scopes are split further by crate/test binary when measured. SDK rows
are split by actual SDK client, browser rows by spec, and crash/restart matrices
by fault target/cut point. Keep a separate case row for unexpectedly slow tests.
Do not require one CI job for each measurement row.

| Scope                       | Coverage                                            | Proposed CI group |
| --------------------------- | --------------------------------------------------- | ----------------- |
| `component.cpp`             | C++ / FFI unit and integration                      | Component         |
| `component.kv`              | KV / Paxos / WAL / client deterministic flows       | Component         |
| `component.diskdb`          | Diskdb allocators / ownership / journal transitions | Component         |
| `component.diskio`          | Diskio / native I/O / client logic                  | Component         |
| `component.chunkdb`         | Chunkdb placement / lifecycle / tasks / EC          | Component         |
| `component.chunk-kv`        | Chunk-KV split / handoff / client logic             | Component         |
| `component.chunk-stream`    | Chunk client / stream deterministic flows           | Component         |
| `component.access-s3`       | S3 / multipart repository and protocol logic        | Component         |
| `component.access-iceberg`  | Iceberg commit / FileIO / GC deterministic flows    | Component         |
| `component.access-dataset`  | Dataset core / adapter / transport / retention      | Component         |
| `component.console`         | Console / CLI / web logic and stub routes           | Component         |
| `component.monitor`         | Monitor profile / bootstrap / supervisor logic      | Component         |
| `component.ui`              | Frontend presentation and unit tests                | Component         |
| `fixture`                   | Runner / ownership / cleanup / budgets              | Fixture           |
| `e2e.kv.management`         | Store / group / replica APIs and errors             | Metadata          |
| `e2e.kv.routing`            | Topology / follower hints / client retries          | Metadata          |
| `e2e.kv.membership`         | Membership changes / epoch fencing                  | Metadata          |
| `e2e.kv.recovery`           | Restore / snapshot join / WAL tail / restart        | Metadata          |
| `e2e.kv.group0`             | Group-0 initialization / discovery / wipe isolation | Metadata          |
| `e2e.diskdb.allocation`     | Allocate / free / reuse / capacity / routing        | Storage           |
| `e2e.diskdb.recovery`       | Ownership fencing / persisted journal recovery      | Storage           |
| `e2e.diskio.io`             | Exact range bytes / registry sync / RPC errors      | Storage           |
| `e2e.diskio.durability`     | Sync / restart / bounded live I/O failure           | Storage           |
| `e2e.chunkdb.lifecycle`     | Allocate / reserve / append / seal / delete         | Storage           |
| `e2e.chunkdb.repair`        | Degraded repair / mirror-EC conversion              | Storage           |
| `e2e.chunkdb.handoff`       | Task / journal recovery / ownership handoff         | Storage           |
| `e2e.chunk-stream.objects`  | Small / large / range data / durable publication    | Storage           |
| `e2e.chunk-stream.recovery` | Takeover / service restart / Diskio errors          | Storage           |
| `e2e.chunk-kv.protocol`     | Read / write / scan / catalog routing / fencing     | Storage           |
| `e2e.chunk-kv.recovery`     | Split / transfer / restart / exact records          | Storage           |
| `e2e.s3.protocol`           | Object / copy / list / multipart / policy           | AccessS3          |
| `e2e.s3.recovery`           | Service restart / response loss / durable data      | AccessS3          |
| `e2e.s3.sdk`                | boto3 / AWS CLI / rclone / Java / JS / Go           | AccessS3          |
| `e2e.iceberg.catalog`       | Namespace / table / commit / auth                   | AccessIceberg     |
| `e2e.iceberg.fileio`        | FileIO / listing / formats / exact data             | AccessIceberg     |
| `e2e.iceberg.recovery`      | Commit / file crash matrix / GC / capacity          | AccessIceberg     |
| `e2e.iceberg.sdk`           | PyIceberg / Java / Rust / RCK compatibility         | AccessIceberg     |
| `e2e.dataset`               | Audited existing Dataset server flows               | AccessDataset     |
| `e2e.console.api-cli`       | Real console / CLI operations and authority         | ConsoleDeploy     |
| `e2e.console.browser`       | Browser provisioning / lifecycle / real data        | ConsoleDeploy     |
| `e2e.deployment.bootstrap`  | Manual / automatic / full-set / subset startup      | ConsoleDeploy     |
| `e2e.deployment.recovery`   | Supervision / identity / logs / crash / mounts      | ConsoleDeploy     |
| `e2e.isolation`             | Same-host mixed-domain concurrency and teardown     | Isolation         |
| `special.containerd`        | Release image runtime adapter acceptance            | Special           |
| `special.host-hardware`     | Host networking / real devices / hardware           | Special           |
| `benchmark`                 | Opt-in throughput / latency benchmarks              | Manual            |

### Measurement rules

- Use monotonic wall-clock measurements. Record host/platform, run date, scope,
  executed/passed/failed/skipped counts, result and artifact link. Preserve failed
  run timings too; an early failure is not a complete-suite measurement.
- For E2E record build/preparation, server startup/readiness, test execution and
  diagnostics/teardown separately, plus total elapsed seconds. Shared image build
  is measured once per run; do not charge it to every flow or hide it in test time.
- Keep per-case durations from Rust/client/browser reports and fixture startup
  timings, so a slow scenario can be distinguished from a slow bootstrap. Add
  per-SDK, per-spec and per-fault rows as those selectors are implemented.
- Record concurrency and CPU/memory budgets with run/image/source identity.
  Parallel job wall time differs from the sum of its flow durations; record both
  distinctly and do not add simultaneous durations as elapsed job time.
- Keep the latest measurement per host and finest measured scope in these tables;
  retain full reports in artifacts. Show slowest cases and setup phases in each
  run report, ranked by measured duration. Do not relax an assertion timeout to
  make a slow-test report disappear.
- Use `—` for unavailable phase measurements; never derive fine-grained timings
  from the historical aggregate suite tables. Unexecuted/unmigrated scopes stay
  pending. CI group names and final Pixi selectors are planned until wired.

### intel7960: target scopes

New rows start unmeasured. The fixture row measures the complete Pixi invocation
without an image build or server startup; the current test report executes twelve cases.
Future detailed component and E2E rows replace their unmeasured scope placeholders.

| Scope                       | Date       | Tests | Build s | Start s  | Test s   | Collect/stop s | Total s  | Status  |
| --------------------------- | ---------- | ----- | ------- | -------- | -------- | -------------- | -------- | ------- |
| `component.cpp`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.kv`              | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.diskdb`          | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.diskio`          | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.chunkdb`         | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.chunk-kv`        | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.chunk-stream`    | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.access-s3`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.access-iceberg`  | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.access-dataset`  | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.console`         | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.monitor`         | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.ui`              | —          | —     | —       | —        | —        | —              | —        | Pending |
| `fixture`                   | 2026-10-11 | 19/19 | —       | —        | —        | —              | 0.474    | PASS    |
| `e2e.kv.management`         | 2026-10-11 | 5/5   | shared  | per flow | per flow | per flow       | per flow | Partial |
| `e2e.kv.routing`            | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.kv.membership`         | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.kv.recovery`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.kv.group0`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.diskdb.allocation`     | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.diskdb.recovery`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.diskio.io`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.diskio.durability`     | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunkdb.lifecycle`     | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunkdb.repair`        | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunkdb.handoff`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunk-stream.objects`  | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunk-stream.recovery` | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunk-kv.protocol`     | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunk-kv.recovery`     | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.s3.protocol`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.s3.recovery`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.s3.sdk`                | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.catalog`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.fileio`        | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.recovery`      | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.sdk`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.dataset`               | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.console.api-cli`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.console.browser`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.deployment.bootstrap`  | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.deployment.recovery`   | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.isolation`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `special.containerd`        | —          | —     | —       | —        | —        | —              | —        | Pending |
| `special.host-hardware`     | —          | —     | —       | —        | —        | —              | —        | Pending |
| `benchmark`                 | —          | —     | —       | —        | —        | —              | —        | Pending |

## Migrated KV flow timing

Linux, 2026-10-11; all rows PASS. Seconds measured against the same retained-debug
release image. These are verified flows within still incomplete parent scopes.
Two fixture slots ran concurrently with a 6 CPU/6144 MiB budget: warm client
build 0.413s, complete scenario wall 32.816s. Client sums include write
and verify phases where applicable; survivor wall includes coordination with the
crashed fixture. Rows rank fixture wall time. `results.json` also ranks the
slowest individual client phases.

| Flow                                                              | Nodes | Start | Client | Restart | Collect | Stop  | Wall  |
| ----------------------------------------------------------------- | ----- | ----- | ------ | ------- | ------- | ----- | ----- |
| quorum_expands_and_shrinks_without_losing_writes (kv.membership)  | 5     | 2.344 | 3.906  | 0.000   | 0.364   | 1.829 | 8.444 |
| e2e_three_node_cluster_kv_put_batch_delete (kv.isolation)         | 3     | 1.426 | 0.851  | 0.000   | 0.222   | 1.156 | 7.365 |
| dynamic_groups_isolate_data (kv.routing)                          | 3     | 1.707 | 2.456  | 0.000   | 0.229   | 1.394 | 5.787 |
| e2e_three_node_cluster_kv_put_batch_delete (kv.recovery)          | 3     | 1.585 | 0.857  | 1.907   | 0.215   | 1.191 | 5.757 |
| multi_store_isolation_and_recovery (kv.routing)                   | 3     | 1.794 | 0.133  | 2.226   | 0.217   | 1.274 | 5.646 |
| async_and_sync_leader_lifecycle (kv.management)                   | 3     | 1.708 | 2.138  | 0.000   | 0.213   | 1.094 | 5.153 |
| topology_wiring_and_progressive_setup (kv.management)             | 2     | 1.234 | 2.397  | 0.000   | 0.152   | 1.068 | 4.852 |
| snapshot_import_precedes_wiring_and_catches_up_tail (kv.recovery) | 3     | 1.464 | 1.524  | 0.000   | 0.222   | 1.168 | 4.379 |
| topology_and_follower_hint (kv.routing)                           | 3     | 1.492 | 0.917  | 0.000   | 0.216   | 1.217 | 3.843 |
| flush_and_wipe_preserve_group_and_resume_writes (kv.management)   | 1     | 0.840 | 0.877  | 0.000   | 0.072   | 0.633 | 2.424 |
| management_lifecycle (kv.management)                              | 1     | 0.781 | 0.392  | 0.000   | 0.070   | 0.589 | 1.833 |
| synchronous_step_down_fence (kv.management)                       | 1     | 0.744 | 0.343  | 0.000   | 0.073   | 0.586 | 1.747 |
| epoch_replay_and_authority_bootstrap (kv.membership)              | 1     | 0.734 | 0.302  | 0.000   | 0.075   | 0.620 | 1.732 |
| replica_lifecycle (kv.membership)                                 | 1     | 0.762 | 0.111  | 0.000   | 0.074   | 0.622 | 1.570 |

## Suite Timing

These existing aggregate rows retain the historical task measurements. New
classification measurements above use finer scopes and are not inferred from
these totals. Keep one latest row per package/platform; use `—` when unmeasured.

### macOS

| Test package       | Date       | Tests   | Seconds | Status                                  |
| ------------------ | ---------- | ------- | ------- | --------------------------------------- |
| `test-cpp`         | 2026-10-07 | 860/860 | 55      | ✓                                       |
| `test-core`        | 2026-10-07 | 977/978 | 368     | ✓ 1 ignored                             |
| `test-storage`     | 2026-10-07 | 811/815 | 1064    | ✓ 4 ignored                             |
| `test-access`      | 2026-10-07 | 906/906 | —       | ✓                                       |
| `test-console`     | 2026-10-07 | 77/79   | 61      | ✓ 2 ignored                             |
| `test-console-ui`  | 2026-10-07 | 63/63   | 192     | ✓                                       |
| `test-boto3-e2e`   | 2026-10-07 | 32/32   | 295     | ✓                                       |
| `test-iceberg-e2e` | 2026-10-07 | —       | 338     | ✓                                       |
| `test-iceberg-sdk` | 2026-10-07 | —       | —       | X Maven dependency resolution stalled   |

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

- Container migration acceptance (2026-10-11): host clients, retained-debug
  release image, fixed ports, no port publishing. Artifacts:
  `.crowdb-runtime/artifacts/container-e2e/run-c2d57c0480f946c2828ebab5b0f35d8f/`.
  Warm client build: 0.619s; scenario wall: 9.069s. Two concurrent three-node
  startups: 1.456/1.550s; CRUD cases: 0.623/0.626s; recovery/survivor checks:
  0.224/0.236s. Collect: 0.211/0.218s; stop: 1.162/1.182s. Recovery wall also
  includes SIGKILL/restart and coordination; the client duration is not its
  complete flow duration. Scope rows remain pending for unmigrated assertions.

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
  budget is now 5s; process/DOM assertions remain 3s. Full Console rerun now passes; R229 remains deferred. Timing log:
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
  cluster 345.98s); data-weight placement is covered by the completed tree metrics work.
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
  statistics boundary is recorded with the completed tree metrics work.
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
| `test-cpp`         | —    | —     | —       | ⏳      |
| `test-core`        | —    | —     | —       | ⏳      |
| `test-storage`     | —    | —     | —       | ⏳      |
| `test-access`      | —    | —     | —       | ⏳      |
| `test-console`     | —    | —     | —       | ⏳      |
| `test-console-ui`  | —    | —     | —       | ⏳      |
| `test-boto3-e2e`   | —    | —     | —       | ⏳      |
| `test-iceberg-e2e` | —    | —     | —       | ⏳      |
| `test-iceberg-sdk` | —    | —     | —       | ⏳      |

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
