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

| Scope                           | Coverage                                                          | Proposed CI group |
| ------------------------------- | ----------------------------------------------------------------- | ----------------- |
| `component.cpp`                 | C++ / FFI unit and integration                                    | Component         |
| `component.kv`                  | KV / Paxos / WAL / client deterministic flows                     | Component         |
| `component.diskdb`              | Diskdb allocators / ownership / journal transitions               | Component         |
| `component.diskio`              | Diskio / native I/O / client logic                                | Component         |
| `component.chunkdb`             | Chunkdb placement / lifecycle / tasks / EC                        | Component         |
| `component.chunk-kv`            | Chunk-KV split / handoff / client logic                           | Component         |
| `component.chunk-stream`        | Chunk client / stream deterministic flows                         | Component         |
| `component.access-s3`           | S3 / multipart repository and protocol logic                      | Component         |
| `component.access-iceberg`      | Iceberg commit / FileIO / GC deterministic flows                  | Component         |
| `component.access-dataset`      | Dataset core / adapter / transport / retention                    | Component         |
| `component.console`             | Console / CLI / web logic and stub routes                         | Component         |
| `component.monitor`             | Monitor profile / bootstrap / supervisor logic                    | Component         |
| `component.ui`                  | Frontend presentation and unit tests                              | Component         |
| `fixture`                       | Runner / ownership / cleanup / budgets                            | Fixture           |
| `e2e.kv.management`             | Store / group / replica APIs and errors                           | Metadata          |
| `e2e.kv.routing`                | Topology / follower hints / client retries                        | Metadata          |
| `e2e.kv.membership`             | Membership changes / epoch fencing                                | Metadata          |
| `e2e.kv.recovery`               | Restore / snapshot join / WAL tail / restart                      | Metadata          |
| `e2e.kv.group0`                 | Group-0 initialization / discovery / wipe isolation               | Metadata          |
| `e2e.kv.client`                 | Discovery / CRUD / CAS / scan / domain monitor                    | Metadata          |
| `e2e.kv.metadata`               | Slot map / handoff / authority publication                        | Metadata          |
| `e2e.diskdb.allocation`         | Allocate / commit / free / capacity / concurrent routing          | Storage           |
| `e2e.diskdb.ownership`          | Wrong-owner facts / compact rejection / registry routes           | Storage           |
| `e2e.diskdb.reuse`              | Exhaustion / exact reuse / delayed incarnation retries            | Storage           |
| `e2e.diskdb.recovery`           | Ownership fencing / persisted journal recovery                    | Storage           |
| `e2e.diskio.io.mem`             | Exact MemDisk ranges / overwrite / concurrent bytes               | Storage           |
| `e2e.diskio.io.null`            | Exact NullDisk patterns / boundary sizes / concurrency            | Storage           |
| `e2e.diskio.discovery`          | Group-0 heartbeat / owner rejection / disk reconciliation         | Storage           |
| `e2e.diskio.errors`             | Missing disk/zone / missing creation-time rejection               | Storage           |
| `e2e.diskio.faults`             | Injected write/read/fsync I/O failure                             | Storage           |
| `e2e.diskio.semantic`           | Views / ordering / admission / route-generation recovery          | Storage           |
| `e2e.diskio.direct`             | O_DIRECT fsync / unaligned ranges / crash recovery                | Storage           |
| `e2e.diskio.durability`         | Sync / restart / bounded live I/O failure                         | Storage           |
| `e2e.chunkdb.lifecycle`         | Allocate / reserve / append / seal / delete                       | Storage           |
| `e2e.chunkdb.allocation`        | Unsent reroute / ambiguous reply / exact allocation count         | Storage           |
| `e2e.chunkdb.placement`         | Public mirror / EC geometry and physical protection               | Storage           |
| `e2e.chunkdb.recovery`          | Exact chunk metadata / revision across dependency crash           | Storage           |
| `e2e.chunkdb.io`                | Exact bytes across every Diskio owner / disk                      | Storage           |
| `e2e.chunkdb.conversion`        | Partial mirror-EC / parity / expired claim / dirty tail           | Storage           |
| `e2e.chunkdb.repair`            | Missing data/parity repair / durable completed task               | Storage           |
| `e2e.chunkdb.slots`             | Three service owners / purpose bands / storage failover           | Storage           |
| `e2e.chunkdb.handoff`           | Task / journal recovery / ownership handoff                       | Storage           |
| `e2e.chunk-client.small`        | Shared frames / cancellation / strip and chunk rotation           | Storage           |
| `e2e.chunk-client.large`        | Data / parity / mirror / partial tails / range streaming          | Storage           |
| `e2e.chunk-client.reader`       | Range recovery / corruption / transient faults / coalesced repair | Storage           |
| `e2e.chunk-client.large_repair` | Replica replacement / ordered buffers / exhaustion / abort        | Storage           |
| `e2e.chunk-client.small_flow`   | Conversion / takeover / repair / queue pressure / scheduling      | Storage           |
| `e2e.chunk-client.benchmark`    | Functional benchmark frame generation and exact readback          | Storage           |
| `e2e.chunk-stream.objects`      | Idle writer takeover / durable stream publication                 | Storage           |
| `e2e.chunk-stream.recovery`     | Complete metadata and file-backed service restart                 | Storage           |
| `e2e.chunk-stream.errors`       | Live Diskio errors / whole storage outage                         | Storage           |
| `e2e.chunk-kv.protocol`         | Read / write / scan / catalog routing / fencing                   | Storage           |
| `e2e.chunk-kv.recovery`         | Split / transfer / restart / exact records                        | Storage           |
| `e2e.s3.protocol`               | Object / copy / list / multipart / policy                         | AccessS3          |
| `e2e.s3.recovery`               | Service restart / response loss / durable data                    | AccessS3          |
| `e2e.s3.sdk`                    | boto3 / AWS CLI / rclone / Java / JS / Go                         | AccessS3          |
| `e2e.iceberg.catalog`           | Namespace / table / commit / auth                                 | AccessIceberg     |
| `e2e.iceberg.fileio`            | FileIO / listing / formats / exact data                           | AccessIceberg     |
| `e2e.iceberg.recovery`          | Commit / file crash matrix / GC / capacity                        | AccessIceberg     |
| `e2e.iceberg.sdk`               | PyIceberg / Java / Rust / RCK compatibility                       | AccessIceberg     |
| `e2e.dataset`                   | Audited existing Dataset server flows                             | AccessDataset     |
| `e2e.console.api-cli`           | Real console / CLI operations and authority                       | ConsoleDeploy     |
| `e2e.console.browser`           | Browser provisioning / lifecycle / real data                      | ConsoleDeploy     |
| `e2e.deployment.selection`      | Default services / selected services / same-image restart         | Deployment        |
| `e2e.deployment.bootstrap`      | Manual / automatic / full-set / subset startup                    | ConsoleDeploy     |
| `e2e.deployment.recovery`       | Supervision / identity / logs / crash / mounts                    | ConsoleDeploy     |
| `e2e.isolation`                 | Same-host mixed-domain concurrency and teardown                   | Isolation         |
| `special.containerd`            | Release image runtime adapter acceptance                          | Special           |
| `special.host-hardware`         | Host networking / real devices / hardware                         | Special           |
| `benchmark`                     | Opt-in throughput / latency benchmarks                            | Manual            |

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
without an image build or server startup; the latest fixture gate executes thirty-four cases.
Future detailed component and E2E rows replace their unmeasured scope placeholders.

| Scope                           | Date       | Tests | Build s | Start s  | Test s   | Collect/stop s | Total s  | Status  |
| ------------------------------- | ---------- | ----- | ------- | -------- | -------- | -------------- | -------- | ------- |
| `component.cpp`                 | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.kv`                  | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.diskdb`              | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.diskio`              | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.chunkdb`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.chunk-kv`            | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.chunk-stream`        | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.access-s3`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.access-iceberg`      | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.access-dataset`      | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.console`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.monitor`             | —          | —     | —       | —        | —        | —              | —        | Pending |
| `component.ui`                  | —          | —     | —       | —        | —        | —              | —        | Pending |
| `fixture`                       | 2026-10-11 | 34/34 | —       | —        | —        | —              | 1.047    | PASS    |
| `e2e.kv.management`             | 2026-10-11 | 5/5   | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.kv.routing`                | 2026-10-11 | 3/3   | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.kv.membership`             | 2026-10-11 | 8/8   | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.kv.recovery`               | 2026-10-11 | 3/3   | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.kv.group0`                 | 2026-10-11 | 8/8   | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.kv.client`                 | 2026-10-11 | 6/6   | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.kv.metadata`               | 2026-10-11 | 14/14 | shared  | per flow | per flow | per flow       | per flow | PASS    |
| `e2e.diskdb.allocation`         | 2026-10-11 | 1/1   | shared  | 2.173    | 1.893    | 1.041          | 5.129    | PASS    |
| `e2e.diskdb.reuse`              | 2026-10-11 | 1/1   | shared  | 1.743    | 1.421    | 1.020          | 4.188    | PASS    |
| `e2e.diskdb.ownership`          | 2026-10-11 | 1/1   | shared  | 2.082    | 0.131    | 1.010          | 3.226    | PASS    |
| `e2e.diskdb.recovery`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.diskio.io.mem`             | 2026-10-11 | 1/1   | shared  | 2.027    | 0.564    | 1.029          | 3.622    | PASS    |
| `e2e.diskio.io.null`            | 2026-10-11 | 1/1   | shared  | 1.855    | 0.856    | 1.086          | 3.801    | PASS    |
| `e2e.diskio.discovery`          | 2026-10-11 | 1/1   | shared  | 1.907    | 0.926    | 1.102          | 3.940    | PASS    |
| `e2e.diskio.errors`             | 2026-10-11 | 1/1   | shared  | 1.838    | 0.225    | 1.052          | 3.117    | PASS    |
| `e2e.diskio.faults`             | 2026-10-11 | 1/1   | shared  | 1.889    | 0.233    | 1.007          | 3.131    | PASS    |
| `e2e.diskio.semantic`           | 2026-10-11 | 1/1   | shared  | 2.046    | 8.051    | 1.408          | 11.510   | PASS    |
| `e2e.diskio.direct`             | 2026-10-11 | 1/1   | shared  | 0.691    | 0.059    | 0.640          | 1.957    | PASS    |
| `e2e.diskio.durability`         | 2026-10-11 | 1/1   | shared  | 0.709    | 0.059    | 0.801          | 2.173    | PASS    |
| `e2e.chunkdb.lifecycle`         | 2026-10-11 | 1/1   | shared  | 3.257    | 0.181    | 2.118          | 5.559    | PASS    |
| `e2e.chunkdb.allocation`        | 2026-10-11 | 1/1   | shared  | 1.957    | 0.092    | 0.995          | 3.047    | PASS    |
| `e2e.chunkdb.placement`         | 2026-10-11 | 1/1   | shared  | 3.198    | 0.215    | 2.233          | 5.651    | PASS    |
| `e2e.chunkdb.recovery`          | 2026-10-11 | 1/1   | shared  | 3.509    | 0.327    | 2.429          | 9.752    | PASS    |
| `e2e.chunkdb.io`                | 2026-10-11 | 1/1   | shared  | 6.364    | 0.136    | 4.386          | 10.888   | PASS    |
| `e2e.chunkdb.conversion`        | 2026-10-11 | 2/2   | shared  | 12.889   | 6.616    | 8.645          | 28.157   | PASS    |
| `e2e.chunkdb.repair`            | 2026-10-11 | 2/2   | shared  | 12.102   | 8.571    | 8.657          | 29.337   | PASS    |
| `e2e.chunkdb.slots`             | 2026-10-11 | 1/1   | shared  | 4.801    | 18.223   | 7.645          | 30.673   | PASS    |
| `e2e.chunkdb.handoff`           | 2026-10-11 | 1/1   | shared  | 6.124    | 1.726    | 4.364          | 12.216   | PASS    |
| `e2e.chunk-client.large`        | 2026-10-11 | 5/5   | shared  | 31.366   | 4.278    | 22.830         | 58.491   | PASS    |
| `e2e.chunk-client.small`        | 2026-10-11 | 3/3   | shared  | 18.639   | 1.198    | 13.673         | 33.518   | PASS    |
| `e2e.chunk-client.reader`       | 2026-10-11 | 7/7   | shared  | 44.014   | 10.351   | 31.382         | 85.768   | PASS    |
| `e2e.chunk-client.large_repair` | 2026-10-11 | 8/8   | shared  | 46.431   | 5.494    | 32.228         | 84.182   | PASS    |
| `e2e.chunk-client.small_flow`   | 2026-10-11 | 14/15 | shared  | 103.924  | 64.727   | 64.684         | 233.387  | FAIL    |
| `e2e.chunk-client.benchmark`    | 2026-10-11 | 2/2   | shared  | 12.359   | 2.358    | 9.125          | 23.847   | PASS    |
| `e2e.chunk-stream.objects`      | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.chunk-stream.recovery`     | 2026-10-11 | 1/1   | shared  | 6.130    | 0.437    | 4.216          | 18.390   | PASS    |
| `e2e.chunk-kv.protocol`         | 2026-10-11 | 1/1   | shared  | 7.127    | 0.032    | 4.541          | 11.704   | PASS    |
| `e2e.chunk-kv.recovery`         | 2026-10-11 | 1/1   | shared  | 9.124    | 0.234    | 5.676          | 22.874   | PASS    |
| `e2e.s3.protocol`               | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.s3.recovery`               | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.s3.sdk`                    | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.catalog`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.fileio`            | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.recovery`          | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.iceberg.sdk`               | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.dataset`                   | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.console.api-cli`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.console.browser`           | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.deployment.selection`      | 2026-10-11 | 3/3   | 3.418   | per flow | per flow | per flow       | 20.153   | PASS    |
| `e2e.deployment.bootstrap`      | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.deployment.recovery`       | —          | —     | —       | —        | —        | —              | —        | Pending |
| `e2e.isolation`                 | —          | —     | —       | —        | —        | —              | —        | Pending |
| `special.containerd`            | —          | —     | —       | —        | —        | —              | —        | Pending |
| `special.host-hardware`         | —          | —     | —       | —        | —        | —              | —        | Pending |
| `benchmark`                     | —          | —     | —       | —        | —        | —              | —        | Pending |

## Migrated object and Chunk-KV per-case timing

- intel7960, 2026-10-11. All servers use image config
  `sha256:c9d68aac2556f0ed855e9c20a9852c1aab87a80b8605df05a123e9efe79d674e`,
  source `94aa283499d77d641b2fcd7d9a7846cb12455b8f`, with release optimization
  and retained debug info. Clients execute on the host.
- Object run `run-21df8676b48245858979b6c373e04da8`: 40 cases, 263.815 s wall,
  5.140 s build, two slots, 12 CPU / 12288 MiB. Thirty-eight passed; the outage
  reader and original one-copy benchmark counter assumptions failed. Corrected
  benchmark run `run-3f5767fedbc54bb6834424cd4510d0d2`: both pass, 12.061 s wall,
  3.700 s build. Rows below use these latest benchmark measurements.
- Chunk-KV run `run-a563226b470342ab91474403ad22de27`: two cases / three phases
  pass, 34.589 s wall, 9.479 s build, one slot, 7 CPU / 7168 MiB. Its independently
  owned fixtures overlap the last object fixtures. Restart covers all twelve
  services and a new client verifies native values, tombstones and ordered reads.
- The 15-second outage reader bound remains unresolved; retain its failed timing
  and source proof. A full object/all-domain pass is not claimed. Client timings
  exclude seed/readiness but include test-controlled service reconfiguration or
  fault actions. Summed fixture totals are work across concurrent slots, not job
  elapsed time. Restart timing is separate from client execution.

| Scope                     | Case                                                                           | Client s | Start s | Restart s | Collect/stop s | Total s | Status |
| ------------------------- | ------------------------------------------------------------------------------ | -------- | ------- | --------- | -------------- | ------- | ------ |
| chunk-client.benchmark    | mixed_read_benchmark_prepares_and_verifies_real_frames                         | 1.235    | 6.145   | 0.000     | 4.670          | 12.054  | PASS   |
| chunk-client.benchmark    | small_write_benchmark_uses_real_metadata_and_null_disk                         | 1.123    | 6.214   | 0.000     | 4.455          | 11.794  | PASS   |
| chunk-client.large        | large_raw_buffers_submit_complete_mirror_strips_concurrently                   | 0.379    | 6.212   | 0.000     | 4.261          | 10.854  | PASS   |
| chunk-client.large        | large_two_copy_mirror_reads_across_strips                                      | 0.806    | 6.268   | 0.000     | 4.400          | 11.476  | PASS   |
| chunk-client.large        | large_write_multi_strip_persists_data_metadata_and_parity                      | 1.176    | 6.581   | 0.000     | 4.295          | 12.055  | PASS   |
| chunk-client.large        | large_write_rotates_chunks_without_losing_data                                 | 1.369    | 5.731   | 0.000     | 4.434          | 11.535  | PASS   |
| chunk-client.large        | large_write_unknown_size_partial_tail_is_durable                               | 0.634    | 6.357   | 0.000     | 4.352          | 11.347  | PASS   |
| chunk-client.large_repair | large_write_replaces_failed_data_and_parity_segments_end_to_end                | 0.886    | 5.515   | 0.000     | 3.964          | 10.368  | PASS   |
| chunk-client.large_repair | large_write_repair_exhaustion_deletes_unsealed_chunk                           | 0.460    | 6.001   | 0.000     | 3.820          | 10.285  | PASS   |
| chunk-client.large_repair | large_mirror_abort_drains_delayed_submitted_write_before_delete                | 0.509    | 6.116   | 0.000     | 3.926          | 10.555  | PASS   |
| chunk-client.large_repair | large_mirror_replaces_failed_replica_before_ordered_completion                 | 1.238    | 5.570   | 0.000     | 4.427          | 11.240  | PASS   |
| chunk-client.large_repair | large_mirror_rotates_failed_partial_final_strip                                | 0.377    | 6.191   | 0.000     | 4.086          | 10.657  | PASS   |
| chunk-client.large_repair | large_mirror_repair_exhaustion_deletes_unsealed_chunk                          | 0.772    | 5.386   | 0.000     | 3.850          | 10.010  | PASS   |
| chunk-client.large_repair | large_mirror_retains_later_framed_buffers_until_failed_first_strip_is_repaired | 0.442    | 5.819   | 0.000     | 4.114          | 10.378  | PASS   |
| chunk-client.large_repair | large_mirror_rotates_after_repair_exhaustion_and_replays_ordered_data          | 0.810    | 5.833   | 0.000     | 4.043          | 10.689  | PASS   |
| chunk-client.reader       | ec_range_read_recovers_only_the_requested_bytes                                | 0.948    | 5.705   | 0.000     | 4.440          | 11.095  | PASS   |
| chunk-client.reader       | ec_read_reports_data_loss_beyond_parity_tolerance                              | 0.583    | 6.213   | 0.000     | 4.528          | 11.328  | PASS   |
| chunk-client.reader       | mirror_read_succeeds_and_reports_replica_loss                                  | 2.435    | 6.254   | 0.000     | 4.547          | 13.239  | PASS   |
| chunk-client.reader       | partial_read_preserves_healthy_ec_shard_ranges_around_data_loss                | 1.379    | 5.922   | 0.000     | 4.357          | 11.661  | PASS   |
| chunk-client.reader       | chunkdb_restart_admits_verified_corruption_and_repairs_full_shard              | 2.443    | 5.939   | 0.000     | 4.646          | 13.032  | PASS   |
| chunk-client.reader       | routed_full_fragment_requests_share_one_durable_repair                         | 1.567    | 7.041   | 0.000     | 4.316          | 12.926  | PASS   |
| chunk-client.reader       | transient_mirror_read_failure_does_not_mark_segment_unavailable                | 0.996    | 6.939   | 0.000     | 4.549          | 12.486  | PASS   |
| chunk-client.small        | cancelled_shared_object_keeps_pipeline_available                               | 0.225    | 6.188   | 0.000     | 4.462          | 10.880  | PASS   |
| chunk-client.small        | shared_object_spans_mirror_strips_with_one_location                            | 0.366    | 6.082   | 0.000     | 4.676          | 11.127  | PASS   |
| chunk-client.small        | small_write_rotates_strips_and_chunks_without_splitting_objects                | 0.790    | 6.053   | 0.000     | 4.513          | 11.359  | PASS   |
| chunk-client.small_flow   | automatic_chunkdb_scan_converts_three_groups_and_preserves_tail                | 18.315   | 7.038   | 0.000     | 4.365          | 29.723  | PASS   |
| chunk-client.small_flow   | chunkdb_restart_recovers_an_inflight_conversion_claim                          | 11.110   | 6.662   | 0.000     | 4.442          | 22.216  | PASS   |
| chunk-client.small_flow   | eight_closed_mirror_strips_become_one_durable_ec_strip_without_reread          | 3.064    | 6.841   | 0.000     | 4.037          | 13.946  | PASS   |
| chunk-client.small_flow   | small_write_batches_concurrent_objects_and_reads_them_back                     | 1.276    | 5.610   | 0.000     | 4.361          | 11.251  | PASS   |
| chunk-client.small_flow   | small_write_scales_out_on_queued_bytes_then_scales_in_when_empty               | 0.564    | 6.435   | 0.000     | 4.448          | 11.449  | PASS   |
| chunk-client.small_flow   | chunkdb_takes_over_durable_conversion_task_after_client_io_failure             | 4.839    | 10.563  | 0.000     | 4.639          | 20.046  | PASS   |
| chunk-client.small_flow   | consecutive_conversion_groups_keep_one_mibibyte_shard_geometry                 | 2.576    | 5.573   | 0.000     | 3.963          | 12.116  | PASS   |
| chunk-client.small_flow   | manual_chunkdb_trigger_converts_closed_active_range_end_to_end                 | 2.654    | 10.589  | 0.000     | 4.487          | 17.733  | PASS   |
| chunk-client.small_flow   | small_write_carry_does_not_underflow_queue_pressure                            | 0.559    | 6.121   | 0.000     | 4.428          | 11.110  | PASS   |
| chunk-client.small_flow   | small_write_scales_out_on_queued_object_count                                  | 0.510    | 6.390   | 0.000     | 4.506          | 11.409  | PASS   |
| chunk-client.small_flow   | small_write_new_chunk_repair_exhaustion_preserves_sealed_predecessor           | 0.493    | 6.107   | 0.000     | 4.709          | 11.312  | PASS   |
| chunk-client.small_flow   | small_write_repair_preserves_acknowledged_prefix_in_open_block                 | 0.445    | 6.306   | 0.000     | 4.344          | 11.097  | PASS   |
| chunk-client.small_flow   | protected_read_reports_diskio_process_failure                                  | 17.417   | 6.246   | 0.000     | 3.087          | 26.755  | FAILED |
| chunk-client.small_flow   | protected_small_write_reports_real_diskio_write_failure                        | 0.303    | 6.818   | 0.000     | 4.253          | 11.378  | PASS   |
| chunk-client.small_flow   | small_write_repairs_failed_replica_through_real_chunkdb_and_diskio             | 0.601    | 6.625   | 0.000     | 4.615          | 11.846  | PASS   |
| chunk-kv.protocol         | production_transport_calls_direct_owner_rpc                                    | 0.032    | 7.127   | 0.000     | 4.541          | 11.704  | PASS   |
| chunk-kv.recovery         | routed_native_records_and_tombstones_survive_complete_service_restart          | 0.234    | 9.124   | 7.836     | 5.676          | 22.874  | PASS   |

## Migrated Diskdb and monitor selection timing

- Host intel7960, 2026-10-11, Linux Docker bridge, concurrency 2, declared
  6 CPU / 6144 MiB. Server source `d8bf780c351bf3afec87624db74956410e8ced5f`;
  image config `sha256:10254caf607d21cb102e0b1fd636d55a65625eb06d54895bad0d01cf6bc3be9a`,
  OCI manifest `sha256:743539d20444ad687f00b771d37d257a5e30803f2b84ade5e40934dc99bfbdd4`,
  release optimization with debug info retained.
- Diskdb run `run-62923a58b6674ed58b9943b84b81dc21`: 5 flows / 12 host phases
  passed; build 4.322 s, scenario wall 12.726 s. Start includes metadata seeding;
  Client excludes seed. Recovery kills Diskdb alone or Diskdb plus KV, preserving
  exact acknowledged records and revisions, then compacts freed space and reuses it.
- All 173 Diskdb application tests passed in 13.433 s including build. Internal
  keepalive, ownership fences, journal transitions and scanner proofs remain
  component tests with pinned in-memory metadata; their host KV process launcher
  and binary-availability skips are removed. Six deterministic client UT remain.
- Monitor selection run `run-baa850f79d5e442e8d9153309e9bf1f2`: full seven-service
  profile plus KV-only and KV+Diskdb, 6 host write/verify phases and 5 invalid
  startup checks passed; build 3.418 s, scenario wall 20.153 s. SIGKILL followed
  by container recreation preserves each node's mounted data and inspected IP.
  Full deployment/browser/SSH failure matrix remains pending.

| Flow                                                                | Server containers | Start s | Client s | Restart s | Collect/stop s | Wall s |
| ------------------------------------------------------------------- | ----------------- | ------- | -------- | --------- | -------------- | ------ |
| `diskdb::allocation::allocate_commit_and_free_through_client`       | 2                 | 1.856   | 2.780    | 0.000     | 1.013          | 5.653  |
| `diskdb::space_reuse::exhaust_free_compact_and_reuse_exact_space`   | 2                 | 1.898   | 1.392    | 0.000     | 1.047          | 4.341  |
| `diskdb::ownership::wrong_owner_free_is_ignored_by_compaction`      | 2                 | 1.792   | 0.082    | 0.000     | 0.982          | 2.859  |
| `diskdb::recovery::metadata_restart_preserves_acknowledged_records` | 2                 | 2.045   | 0.317    | 2.171     | 0.972          | 5.511  |
| `diskdb::recovery::diskdb_restart_preserves_acknowledged_records`   | 2                 | 2.072   | 0.315    | 0.695     | 1.032          | 4.123  |
| `deployment::complete_release_services_recover_acknowledged_data`   | 1                 | 5.891   | 0.179    | 4.302     | 0.832          | 11.210 |
| `deployment::selected_storage_services_recover_acknowledged_data`   | 1                 | 2.853   | 0.184    | 3.405     | 0.792          | 7.239  |
| `deployment::selected_kv_services_recover_acknowledged_data`        | 1                 | 2.348   | 0.188    | 3.274     | 0.796          | 6.612  |

## Migrated KV flow timing

Linux, 2026-10-11; all 48 fixtures PASS, 47 unique Rust cases and 54
client phases, plus three release startup rejection checks. These cover the
currently migrated KV scopes; other server domains remain pending.
Host clients consumed the formal retained-debug image, source `6c5bc476`,
config `sha256:f1338467ef62b70542339def054629a745ce7ff58dcc1c4002940edd0c66e5ff`.
Two fixture slots, 6 CPUs/6144 MiB. Warm client build 0.440s;
scenario wall 85.987s. Client sums include write/verify
phases; concurrent fixture walls must not be added as job elapsed time.
Startup, control actions, collection and teardown are retained in
[results.json](../../.crowdb-runtime/artifacts/container-e2e/run-5d914d0b899045439e600b7510c5f044/results.json).
Rows rank measured fixture wall; the report also ranks individual client phases.

| Flow (scope)                                                                                    | Nodes | Start | Client | Restart | Collect | Stop  | Wall   |
| ----------------------------------------------------------------------------------------------- | ----- | ----- | ------ | ------- | ------- | ----- | ------ |
| graceful_leader_shutdown (kv.recovery)                                                          | 3     | 1.693 | 4.721  | 0.000   | 5.227   | 0.962 | 12.608 |
| unavailable_voter_and_follower_removal (kv.membership)                                          | 4     | 2.045 | 3.203  | 0.000   | 5.297   | 1.012 | 11.561 |
| remove_leader_and_continue (kv.membership)                                                      | 3     | 1.681 | 2.961  | 0.000   | 5.259   | 0.997 | 10.901 |
| quorum_expands_and_shrinks_without_losing_writes (kv.membership)                                | 5     | 2.337 | 3.738  | 0.000   | 0.372   | 1.831 | 8.280  |
| e2e_three_node_cluster_kv_put_batch_delete (kv.isolation)                                       | 3     | 1.707 | 0.914  | 0.000   | 0.222   | 1.168 | 7.523  |
| e2e_three_node_cluster_kv_put_batch_delete (kv.recovery)                                        | 3     | 1.541 | 0.888  | 1.903   | 0.216   | 1.172 | 5.884  |
| dynamic_groups_isolate_data (kv.routing)                                                        | 3     | 1.736 | 2.507  | 0.000   | 0.209   | 1.406 | 5.862  |
| multi_store_isolation_and_recovery (kv.routing)                                                 | 3     | 1.815 | 0.160  | 2.142   | 0.214   | 1.159 | 5.496  |
| async_and_sync_leader_lifecycle (kv.management)                                                 | 3     | 1.608 | 1.748  | 0.000   | 0.205   | 1.139 | 4.704  |
| topology_wiring_and_progressive_setup (kv.management)                                           | 2     | 1.117 | 2.238  | 0.000   | 0.146   | 0.913 | 4.417  |
| snapshot_import_precedes_wiring_and_catches_up_tail (kv.recovery)                               | 3     | 1.549 | 1.303  | 0.000   | 0.209   | 1.236 | 4.299  |
| topology_and_follower_hint (kv.routing)                                                         | 3     | 1.531 | 0.966  | 0.000   | 0.223   | 1.280 | 4.004  |
| paused_bootstrap_admits_no_registration_writes (kv.group0)                                      | 1     | 0.865 | 2.353  | 0.000   | 0.070   | 0.644 | 3.937  |
| nonmember_recovers_discovery_and_registration_identity (kv.group0)                              | 2     | 0.956 | 1.887  | 0.000   | 0.148   | 0.907 | 3.901  |
| cleanup_fences_delayed_requests (kv.group0)                                                     | 1     | 0.530 | 0.701  | 0.840   | 0.067   | 0.753 | 2.897  |
| competing_cohorts_use_the_service_head_revision_and_reject_stale_maps (kv.metadata)             | 1     | 1.022 | 0.686  | 0.000   | 0.073   | 0.854 | 2.640  |
| competing_publications_choose_one_complete_epoch_map_without_changing_data_fences (kv.metadata) | 1     | 1.050 | 0.693  | 0.000   | 0.078   | 0.761 | 2.587  |
| flush_and_wipe_preserve_group_and_resume_writes (kv.management)                                 | 1     | 0.655 | 0.980  | 0.000   | 0.074   | 0.786 | 2.500  |
| missing_cached_endpoint_refreshes_dynamic_owner_before_routing (kv.metadata)                    | 1     | 1.149 | 0.585  | 0.000   | 0.070   | 0.621 | 2.427  |
| stale_epoch_publications_are_conflicts_and_concurrent_regrants_preserve_all_slots (kv.metadata) | 1     | 0.916 | 0.654  | 0.000   | 0.078   | 0.746 | 2.398  |
| a_replacement_controller_resumes_partial_fences_without_replacing_the_cohort (kv.metadata)      | 1     | 0.899 | 0.775  | 0.000   | 0.073   | 0.636 | 2.385  |
| complete_epoch_publication_is_atomic_idempotent_and_storage_independent (kv.metadata)           | 1     | 1.108 | 0.569  | 0.000   | 0.071   | 0.609 | 2.358  |
| prepared_owner_survives_restart (kv.group0)                                                     | 1     | 0.622 | 0.340  | 0.667   | 0.071   | 0.600 | 2.308  |
| service_client_needs_no_storage_map_and_refreshes_same_owner_endpoints (kv.metadata)            | 1     | 0.915 | 0.520  | 0.000   | 0.075   | 0.787 | 2.301  |
| orphan_binding_is_preserved_without_creating_a_head (kv.metadata)                               | 1     | 0.837 | 0.643  | 0.000   | 0.080   | 0.734 | 2.298  |
| paginated_owner_records_preserve_empty_instances (kv.metadata)                                  | 1     | 0.967 | 0.616  | 0.000   | 0.073   | 0.632 | 2.289  |
| scan_deadline_returns_partial_result (kv.client)                                                | 1     | 0.935 | 0.622  | 0.000   | 0.074   | 0.616 | 2.249  |
| initialize_and_restore (kv.group0)                                                              | 1     | 0.612 | 0.254  | 0.723   | 0.071   | 0.571 | 2.235  |
| legacy_records_are_never_overwritten (kv.metadata)                                              | 1     | 1.011 | 0.527  | 0.000   | 0.073   | 0.612 | 2.226  |
| legacy_chunk_state_without_range_bindings_is_not_reinterpreted (kv.metadata)                    | 1     | 0.870 | 0.561  | 0.000   | 0.061   | 0.728 | 2.224  |
| initialize_is_atomic_idempotent_and_layer_independent (kv.metadata)                             | 1     | 0.913 | 0.612  | 0.000   | 0.072   | 0.618 | 2.219  |
| independent_clients_cannot_submit_two_changes_or_skip_installation (kv.group0)                  | 1     | 0.945 | 0.571  | 0.000   | 0.075   | 0.603 | 2.197  |
| ordinary_and_malformed_writes_cannot_change_slot_authority (kv.membership)                      | 1     | 1.069 | 0.382  | 0.000   | 0.071   | 0.633 | 2.157  |
| raw_owner_rpc_rejects_malformed_authority_and_cross_namespace_mutation (kv.membership)          | 1     | 0.813 | 0.462  | 0.000   | 0.073   | 0.787 | 2.137  |
| concurrent_domain_monitor_ensure_reconciles_cas_busy (kv.client)                                | 1     | 0.794 | 0.386  | 0.000   | 0.073   | 0.848 | 2.104  |
| corrupt_owner_and_partial_generation_fail_closed (kv.metadata)                                  | 1     | 0.946 | 0.478  | 0.000   | 0.076   | 0.572 | 2.075  |
| put_get_delete_round_trip_via_topology_discovery (kv.client)                                    | 1     | 0.864 | 0.507  | 0.000   | 0.071   | 0.583 | 2.028  |
| revision_cas_and_conditional_batch_round_trip (kv.client)                                       | 1     | 0.849 | 0.443  | 0.000   | 0.073   | 0.653 | 2.023  |
| incarnation_change_revokes_old_writes_and_preserves_record_cas (kv.membership)                  | 1     | 0.773 | 0.315  | 0.000   | 0.068   | 0.850 | 2.010  |
| concurrent_initializers_cannot_combine_different_assignments (kv.metadata)                      | 1     | 0.836 | 0.405  | 0.000   | 0.071   | 0.681 | 1.997  |
| scan_keys_only_and_scan_count (kv.client)                                                       | 1     | 0.667 | 0.567  | 0.000   | 0.073   | 0.656 | 1.968  |
| management_lifecycle (kv.management)                                                            | 1     | 0.787 | 0.415  | 0.000   | 0.071   | 0.586 | 1.863  |
| synchronous_step_down_fence (kv.management)                                                     | 1     | 0.772 | 0.393  | 0.000   | 0.071   | 0.610 | 1.848  |
| batch_write_and_scan (kv.client)                                                                | 1     | 0.852 | 0.356  | 0.000   | 0.065   | 0.565 | 1.840  |
| epoch_replay_and_authority_bootstrap (kv.membership)                                            | 1     | 0.779 | 0.306  | 0.000   | 0.071   | 0.570 | 1.731  |
| replica_lifecycle (kv.membership)                                                               | 1     | 0.852 | 0.062  | 0.000   | 0.066   | 0.580 | 1.563  |
| custom_replica (kv.group0)                                                                      | 1     | 0.611 | 0.213  | 0.000   | 0.073   | 0.590 | 1.489  |
| competing_prepares (kv.group0)                                                                  | 1     | 0.625 | 0.065  | 0.000   | 0.072   | 0.597 | 1.362  |

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

## Diskio migration measurements

- Exact formal release image/source remains the shared Diskdb/selection image above.
- `run-4760a5f986824b02b404acfa4828c361`: eight owned fixtures, fourteen host-client phases,
  two slots, 6 CPUs/6144 MiB. Client build 6.165 s;
  scenario wall 20.641 s. Every flow and cleanup passed.
- Discovery seed time is included in Start, excluded from Client. Durability
  Client combines write/verify; per-fixture timing.json retains the separate
  restart phase. Semantic control responses retain each kill/start duration.
- File-backed tests passed through the supported engine fallback on this host;
  real block-device/io_uring hardware acceptance remains separate.

## Chunkdb first public boundary timing

- Run `run-6308a1cf82014b9cb72cb3840e65f8c6`: four cases, nine host phases
  (including four seeds), concurrency two, 12.181 s scenario wall. Host-client
  build 13.615 s includes shared Cargo lock wait; client columns exclude seed.
- Three lifecycle/placement/recovery profiles each own three KV voters plus
  Diskdb and Chunkdb; allocation transport owns KV plus Diskdb. All use formal
  release source `6f17c8f690d1bfeb594aa010d209be72b8ce1e0b`, OCI manifest
  `sha256:16911413347a2c73c237aa1513151f125609704bd9c1cf8645cb2452d0a62125`,
  config `sha256:6cd7f00266bee11cc6c2820a3e302c1607f1fa31567151ab1ebb3b2fcc31960c`,
  retained debug info and release optimization.
- Original allocation/lifecycle public cases retired after acceptance. Thirty-four
  internal flows, mixed geometry, single-rack placement and allocator snapshot UT
  remain and pass (37 tests). The 34 internal flows take 6.39 s; geometry 0.15 s,
  single-rack 0.83 s and allocator snapshot 0.08 s, excluding build.
- Chunkdb public migration now includes degraded topology repair and journal handoff; the final eleven-case acceptance below supersedes this initial four-case measurement.
- Recovery total includes a separately recorded 3.800 s dependency restart.
  Collection/stop columns sum only diagnostic collection and teardown.

## Chunkdb storage and multislot timing

- `run-9801d52258a74d099b0e77138b3c9e91`: nine cases pass, 54.618 s wall, 0.574 s client build; concurrency two with 12 CPUs / 12288 MiB. Scope conversion totals include two independently owned fixtures, not concurrent wall time.
- Formal source `94aa2834`, Docker config `sha256:c9d68aac2556f0ed855e9c20a9852c1aab87a80b8605df05a123e9efe79d674e`; release optimized with debug info retained. KV full regression passed concurrently on the same image.
- Longest case is multislot fault recovery: 31.443 s fixture / 18.703 s host client. Logs retain seed discovery against the stopped voter and failover to surviving voters for future latency review.
- Source retirement follows exact bytes, completed tasks, dirty tail, atomic revisions and all purpose routing assertions. Deterministic component claim, fencing and placement tests remain.

- Final Chunkdb batch `run-6ae19b8e9e724a3fb3dc2b6edf863481`: eleven cases pass, 67.887 s wall / 0.647 s client build, concurrency two, 12 CPUs / 12288 MiB. Repair owns two cases (four-shard byte repair and degraded topology restoration). Journal handoff owns one exact copy/publication/source-free case. Retained component full-stack tests: 36 pass in 20.30 s; all other Chunkdb component targets also pass.

## Chunk client and stream accepted boundary timing

- Objects `run-d3773353e00943cc8704485116f3a8b8`: eight cases pass, 46.542 s scenario wall, 4.534 s client build. Two independent eleven-container profiles run concurrently. Five large-object cases include complete EC bytes/parity/metadata, protected two-copy mirror ranges, concurrent complete mirror strips, chunk rotation and range streaming, and unknown-size partial tails. Three small-object cases include exact shared-frame ranges across strips, cancellation followed by a durable survivor, and IcebergTable type-preserving chunk rotation.
- Restart `run-b30240af4fa64db0aed6c0ab4c0a913d`: 18.397 s scenario wall / 4.630 s build (shared Cargo lock), complete eleven-service restart, 7.603 s restart phase. Eighteen file devices each expose one physical 128-MiB zone with hexadecimal CLI IDs matching public hardware metadata. Exact prefix and suffix survive; the successor chunk differs from the abandoned writer chunk.
- Formal image remains source `94aa2834`, config `sha256:c9d68aac2556f0ed855e9c20a9852c1aab87a80b8605df05a123e9efe79d674e`. Component writer/reader/pressure, stream fencing/recovery/publication and corruption proofs remain. The complete client/stream migration is still active; exited-Diskio failure-budget behavior is unresolved and receives no PASS claim.

## Access S3 container migration measurements

- 2026-10-11: 36 named owners passed against formal image
  `sha256:c9d68aac2556f0ed855e9c20a9852c1aab87a80b8605df05a123e9efe79d674e`,
  source `94aa283499d77d641b2fcd7d9a7846cb12455b8f`. Servers stayed unchanged;
  host clients used protected two-copy storage across six failure domains.
- Protocol budget: two independent 14-node fixtures, 16 CPU / 16384 MiB.
  Other batches: one fixture, 8 CPU / 8192 MiB. SDK prepare time is included
  in host client totals; seed/setup and diagnostic/cleanup are fixture time.
- The default boto3 native metrics were separately checked with all original
  counters and zero added large-writer payload copies. The retired S3 full
  stack range-delete assertion moved to the existing Chunkdb lifecycle owner.
- This is the S3 batch result; unfinished domains and the two exited-storage
  timing decisions prevent claiming the complete migration gate passed.

| Scope          | Case                                                                           | Host client seconds | Fixture seconds | Status |
| -------------- | ------------------------------------------------------------------------------ | ------------------- | --------------- | ------ |
| s3.benchmark   | access_s3::benchmark::direct_chunk_path                                        | 0.341               | 16.866          | PASS   |
| s3.benchmark   | s3.benchmark::s3_request_path                                                  | 1.042               | 16.316          | PASS   |
| s3.credentials | s3.credentials::release_cli_issue_ensure_lookup                                | 1.393               | 17.015          | PASS   |
| s3.protocol    | s3.protocol::test_aws_chunked_trailers_are_verified_before_publication         | 0.874               | 16.599          | PASS   |
| s3.protocol    | s3.protocol::test_aws_cli_workflow                                             | 12.488              | 26.153          | PASS   |
| s3.protocol    | s3.protocol::test_basic_bucket_object_matrix                                   | 6.714               | 20.290          | PASS   |
| s3.protocol    | s3.protocol::test_batch_delete_preserves_exact_keys_and_quiet                  | 1.035               | 14.617          | PASS   |
| s3.protocol    | s3.protocol::test_batch_delete_rejects_entire_invalid_request                  | 1.293               | 16.713          | PASS   |
| s3.protocol    | s3.protocol::test_batch_delete_thousand_keys_and_unversioned_retry             | 74.530              | 89.070          | PASS   |
| s3.protocol    | s3.protocol::test_concurrent_overwrite_delete_and_get_are_portable             | 1.635               | 17.186          | PASS   |
| s3.protocol    | s3.protocol::test_copy_captures_source_before_overwrite_and_delete             | 6.756               | 22.044          | PASS   |
| s3.protocol    | s3.protocol::test_default_boto3_checksums_and_multipart                        | 10.718              | 26.536          | PASS   |
| s3.protocol    | s3.protocol::test_fragmentation_and_storage_boundaries                         | 2.316               | 17.724          | PASS   |
| s3.protocol    | s3.protocol::test_independent_frontends_share_one_namespace                    | 0.917               | 15.488          | PASS   |
| s3.protocol    | s3.protocol::test_invalid_user_metadata_preserves_objects_and_sessions         | 1.002               | 14.865          | PASS   |
| s3.protocol    | s3.protocol::test_multipart_copy_selects_ranges_and_replaces_parts             | 11.064              | 27.880          | PASS   |
| s3.protocol    | s3.protocol::test_multipart_replaces_parts_and_publishes_selected_bytes        | 2.235               | 15.675          | PASS   |
| s3.protocol    | s3.protocol::test_ordinary_put_size_matrix                                     | 5.869               | 21.707          | PASS   |
| s3.protocol    | s3.protocol::test_presigned_transfers_tamper_and_expiry                        | 2.725               | 16.628          | PASS   |
| s3.protocol    | s3.protocol::test_rclone_workflow                                              | 5.376               | 20.891          | PASS   |
| s3.protocol    | s3.protocol::test_server_side_copy_preserves_bytes_and_supported_metadata      | 1.268               | 16.898          | PASS   |
| s3.protocol    | s3.protocol::test_signed_raw_http_wire_contract                                | 0.734               | 14.178          | PASS   |
| s3.protocol    | s3.protocol::test_slow_response_reader_keeps_full_object_consistent            | 1.102               | 14.715          | PASS   |
| s3.protocol    | s3.protocol::test_slow_signed_upload_releases_native_buffers                   | 4.723               | 19.190          | PASS   |
| s3.protocol    | s3.protocol::test_truncated_signed_upload_does_not_publish_and_releases_credit | 1.988               | 18.161          | PASS   |
| s3.protocol    | s3.protocol::test_user_metadata_publication_copy_and_multipart                 | 1.350               | 16.803          | PASS   |
| s3.recovery    | s3.recovery::access                                                            | 3.398               | 18.801          | PASS   |
| s3.recovery    | s3.recovery::chunk-kv                                                          | 2.559               | 19.759          | PASS   |
| s3.recovery    | s3.recovery::chunkdb                                                           | 2.607               | 19.270          | PASS   |
| s3.recovery    | s3.recovery::diskdb                                                            | 2.625               | 18.990          | PASS   |
| s3.recovery    | s3.recovery::diskio                                                            | 3.198               | 22.846          | PASS   |
| s3.recovery    | s3.recovery::group0                                                            | 2.932               | 24.626          | PASS   |
| s3.responses   | s3.responses::lost_put_and_multipart_replies_are_idempotent                    | 1.607               | 16.893          | PASS   |
| s3.sdk         | s3.sdk::go                                                                     | 4.132               | 19.547          | PASS   |
| s3.sdk         | s3.sdk::java                                                                   | 11.173              | 26.218          | PASS   |
| s3.sdk         | s3.sdk::js                                                                     | 7.949               | 21.682          | PASS   |

- Exact artifact runs (wall / client build seconds):
  - `run-e0fce6afb33a449dab0ebfb5f29f947c`: 256.930 / 0.516.
  - `run-141601d6ca444108b8175cc9981898d4`: 124.304 / 0.621.
  - `run-ed7be660027b43758f71633caf9c4f2f`: 67.454 / 0.661.
  - `run-f6e3d5aac18d443cac222bce62abeea4`: 16.898 / 0.579.
  - `run-85d94889f34e487f84f6c3716cf6fb8b`: 16.323 / 0.672.
  - `run-748762abb1394f3587ab28712b4b83f3`: 16.876 / 9.971.
  - `run-648f9a14aaf347608d9d5ae271308e92`: 17.022 / 0.501.

### S3 CI CPU quota and cross-protocol acceptance

- 2026-10-11: the 23 protocol cases and all three language SDKs passed with
  0.25 CPU per service container. Two protocol fixtures reserve 8 CPU /
  16384 MiB; a single language fixture reserves 4 CPU / 8192 MiB. Client
  timeouts and production retry policies stayed unchanged. Every ordinary
  HTTP operation asserts zero added large payload copies; the two large copy
  owners assert their copy path ran.
- `run-54794303700e412cb5ab69ed2e004695`: 23 passing owners, wall 314.083s, client build 14.281s.
- `run-237237925570436daf13a89bfce17622`: 3 passing owners, wall 79.713s, client build 13.254s.
- `run-6747308f1f01457294232abd53ee56b4`: 1 passing owners, wall 23.541s, client build 14.012s.
  - `access_s3::policy::s3_and_iceberg_keep_distinct_policies_on_protected_storage`: client 2.356s; fixture 23.533s.
- `run-943681fa038d4e449ad2307c84571fd4`: 1 passing owners, wall 21.860s, client build 70.868s.
  - `access_s3::protocol_http::combined_http_listeners_keep_protocol_chunk_policies_separate`: client 3.593s; fixture 21.853s.
- `run-889988f5ffdd44c7806750d816f4f0df`: 1 passing owners, wall 5.379s, client build 3.747s.
- Retained S3 library UT: 113 passing cases. Focused Access config/XML/copy/
  metadata-inspection/service UT: 16 passing cases, plus the pinned boto3
  embedded-error compatibility proof. Three lifecycle UTs cover terminal
  storage failure and coordinated shutdown of two or three protocol services.
- The original protected exited-Diskio 15-second budget comparison remains
  an explicit legacy E2E source. Full migration acceptance is still pending.

### Iceberg protected storage first migration batch

- 2026-10-11, `run-4db6ca739f2f472f9687867bc6c8dd6a`: all three named owners pass;
  72.123 s scenario wall, 13.310 s host-client build. Two independent fixtures
  run concurrently, 14 release containers each, 0.25 CPU/container, 8 CPU /
  16384 MiB aggregate reservation. Clients and PyIceberg stay on the host.
- Formal image config `sha256:d207b46eca74df4362610b01d70d783e910523920fb9a5f9fe9acee6f971e7f4`,
  source `b299f93bbe1fa86af759e59fd41538dc30cfa2eb`, retained debug information.

| Scope               | Named flow                                                               | Client (s) | Fixture (s) |
| ------------------- | ------------------------------------------------------------------------ | ---------- | ----------- |
| `iceberg.metadata`  | `metadata_create_and_commit_stay_within_operation_budget`                  | 0.821      | 17.865      |
| `iceberg.namespace` | `namespace_functional_crud_survives_native_storage_and_listener_restart`   | 36.228     | 54.250      |
| `iceberg.files`     | `native_file_tree_publication_and_ranges_survive_catalog_storage_restart`  | 16.426     | 33.690      |

- Metadata create/commit retains the original two-second limits and exact
  get/CAS budgets. Native files retain three checkpoint intents, tree height,
  exact full/range bytes, bounded frames, publication replay and multipart
  completion recovery over two Chunk-KV crashes. A normally configured release
  GC worker releases expired-session credits while preserving selected parts.
- PyIceberg retains configuration, bearer/warehouse rejection and namespace
  CRUD errors, cross-frontend visibility and persisted properties after restart.
  The complete production profile advertises 14 routes, checked exactly.
  The catalog lifecycle profile has a short delegation lease and checks exactly
  the original six namespace routes. Both retain all namespace assertions.
- Catalog lifecycle, further FileIO/SDK/GC/crash suites and Dataset remain pending.

- `run-4e9a903dd7b74cfd9d1e3a00b04384c5`: 1 passing owners; 92.741 s scenario wall; 0.586 s build.

- `run-e6d70f2565cf473a8003fb6914e660be`: 3 passing owners; 78.644 s scenario wall; 0.722 s build.

- `run-6c94205d10954458a2cc548f495aa897`: 1 passing owners; 24.903 s scenario wall; 8.328 s build.

| Scope           | Named flow                                            | Client (s) | Fixture (s) |
| --------------- | ----------------------------------------------------- | ---------- | ----------- |
| iceberg.catalog | catalog_recovery_survives_real_chunk_kv_restart       | 76.693     | 92.734      |
| iceberg.gc      | authenticated_gc_controls_survive_separate_processes  | 39.202     | 54.529      |
| iceberg.gc      | enabled_scheduler_admits_durable_purge_markers_once   | 29.180     | 45.512      |
| iceberg.gc      | enabled_scheduler_admits_and_advances_completed_clear | 15.681     | 33.090      |
| iceberg.gc      | official_sdk_foreground_progresses_under_gc_backlog   | 9.482      | 24.898      |

- Catalog retains initialize/rename/clear replay, published grace recovery,
  retired namespace isolation, epoch increments, conditional name recreation,
  interrupted request journals and exact retry-ledger scans after native restart.
- GC retains CLI privilege rejection, idempotent task admission, pause/resume,
  stalled-state inspection/retry, durable purge markers and completed-clear
  scheduling with one-item pages. The pinned foreground SDK check is mandatory.
- Concurrent foreground proof retains four official SDK workers, signed metadata
  reads and refreshed credentials, overlapping GC progress and all 64 writes;
  normal rack-separated two-copy native protection replaces old one-copy mode.
- S3 full-image low-quota run `run-e9f2655309a5497ebdf1038e163855ab`: 37/38 pass;
  thousand-key batch deletion reaches the migration runner's cumulative 120 s process limit at
  0.25 CPU/container. At default 0.5 CPU the same exact case passes in
  112.320 s (`run-e645f383561d4808b6924bcae9ef6847`). This is a slow flow requiring
  adequate gate resources; its three batch operations and assertions stay intact.
  The cumulative limit also affected the 1 CPU/container run. Source audit
  confirms the original Python subprocess had no cumulative deadline. The
  migrated owner now records initial/retry/new-publication as three sequential
  client phases on its same fixture, preserving all three operations and SDK
  request deadlines; each phase remains bounded by 120 s. Fresh acceptance
  of this phase split remains pending.

### Iceberg SDK and signed FileIO acceptance

- `run-b5aa23e83362458ea5272ca331bb292a`: 2 owners, pass; 1218.948 s scenario wall.
- `run-41cfc3ee4b15416c90f70e958a12b9fe`: 1 owners, pass; 21.253 s scenario wall.
- `run-8fb3396aafc445b5a9d3bcf62c2bbe76`: 4 owners, pass; 42.522 s scenario wall.
- `run-90c1aaa3f8f44a95b788a8a790fdd50b`: 1 owners, pass; 138.657 s scenario wall.

| Scope         | Named flow | Client (s) | Fixture (s) |
| ------------- | ---------- | ---------- | ----------- |
| iceberg.sdk   | official_rust_client_rejects_retired_native_catalog_after_full_grace             |   1206.136 |    1218.922 |
| iceberg.sdk   | official_rust_client_lost_reply_survives_native_storage_restart                  |     26.084 |      38.767 |
| iceberg.sdk   | official_rust_client_namespace_and_table_lifecycle                               |      5.523 |      21.220 |
| iceberg.files | native_file_tree_publication_and_ranges_survive_catalog_storage_restart          |     14.128 |      26.622 |
| iceberg.files | explicit_cleanup_rejects_a_retained_metadata_file                                |      1.980 |      16.374 |
| iceberg.files | concurrent_multipart_creates_use_independent_sessions                            |      1.388 |      15.886 |
| iceberg.files | explicit_cleanup_supports_single_batch_and_same_path_reupload                    |      1.650 |      14.261 |

- Retirement keeps the complete 1200-second production grace and verifies stale catalog rejection and same-name recreation through the pinned SDK. Lost-reply recovery retains acknowledgement loss, cross-frontend visibility and storage restart. Original Rust SDK server-launching sources retire only after these and the complete namespace/table lifecycle owner pass.
- Component Iceberg tests: 708 tests in 142 binaries pass. The serial doctest gate passes; its first concurrent run had transitive artifact resolution errors while another Cargo feature build replaced shared outputs. No test assertion changed. Access-server default component gate: 109 tests pass.
- Host client sources and SDK locks are copied per run, alongside the immutable Rust binaries; source digests include Java, JavaScript, Go and shell inputs. Per-run SDK outputs do not mutate the repository client source.

### Fresh complete S3 and FileIO gates

- `run-095737a638d44969b4514582ee277b44`: 38 passing owners; 408.388 s scenario wall, 5.667 s client build.
- `run-0280cfaaed50430780f313188cff296b`: 7 passing owners; 73.303 s scenario wall, 1.292 s client build.

| Scope         | Named flow | Client (s) | Fixture (s) |
| ------------- | ---------- | ---------- | ----------- |
| iceberg.files | ordinary_put_size_matrix_streams_and_reads_ranges                                |     15.619 |      30.666 |
| iceberg.files | native_file_tree_publication_and_ranges_survive_catalog_storage_restart          |     15.289 |      27.745 |
| iceberg.files | small_routing_is_strict_at_the_strip_threshold                                   |      1.931 |      15.812 |
| iceberg.files | explicit_cleanup_supports_single_batch_and_same_path_reupload                    |      1.920 |      14.877 |
| iceberg.files | concurrent_multipart_creates_use_independent_sessions                            |      1.515 |      14.489 |
| iceberg.files | slow_socket_resumes_upload_after_writer_drains                                   |      1.864 |      14.414 |
| iceberg.files | explicit_cleanup_rejects_a_retained_metadata_file                                |      1.891 |      14.398 |

- The complete S3 set includes all 23 protocol owners, six service-recovery owners, three SDK owners and six specialized owners. The thousand-key owner retains its initial/retry/new-publication operations and exact assertions as three measured phases on one fixture.
- FileIO keeps original cleanup privilege/reference protection, independent concurrent multipart sessions, 10 KiB/1 MiB/12 MiB/100 MiB complete hashes and ranges, protected large-copy geometry, slow-body socket progress and exact threshold routing metrics. Old equivalent bodies retire after this gate. The first range owner registration omitted its test attribute; zero matching tests correctly failed the runner, and the corrected exact owner and complete seven-owner gate pass.

### Protected large FileIO and Java SDK timing

- `run-06f8475d48c7489db24b39ff91714689`: 12 passing owners; 128.592 s scenario wall.
- `run-5cb37e34da204394a290259f7fa62b05`: 1 passing owners; 20.778 s scenario wall.
- `run-25b9084327ce45a3b07814c62c2cb425`: 1 passing owners; 50.412 s scenario wall.
- `run-b10e359aeef04fdbacd7ca85ac47ec0d`: 1 passing owners; 22.854 s scenario wall.
- `run-677b374720884908bf44c3b1c14b2088`: 1 passing owners; 37.460 s scenario wall.

| Scope         | Named flow | Client (s) | Fixture (s) |
| ------------- | ---------- | ---------- | ----------- |
| iceberg.files | multipart_100_mib_survives_restart_and_complete_replay                                     |     22.411 |      35.902 |
| iceberg.files | repeated_100_mib_multipart_upload_profile                                                  |      4.762 |      19.314 |
| iceberg.files | repeated_100_mib_put_streams_without_client_payload_copy                                   |      5.455 |      19.022 |
| iceberg.files | native_file_credentials_refresh_expire_and_follow_lifecycle_fences                         |      6.101 |      18.231 |
| iceberg.files | signed_standard_put_get_and_multipart_publish_unbound_files                                |      2.609 |      13.780 |
| iceberg.sdk   | official_java_s3_fileio_uploads_and_reads_native_files                                     |      8.367 |      20.770 |
| iceberg.sdk   | official_java_catalog_commits_native_parquet_snapshots_and_staged_tables                   |     37.323 |      50.404 |
| iceberg.sdk   | official_java_opaque_metadata_publication_preserves_valid_data_and_delete_reads            |     10.115 |      22.846 |
| iceberg.files | thirty_two_concurrent_direct_puts                                                          |     24.237 |      37.452 |

- Exact image config `sha256:70b0b5d97171be60db67a17bca295d60bdd7a59dae2413e6275648913da7db73`, source `547fe8fd4005077772f94d61939019539acd85cb`; optimized release binaries retain debug information. Only Console UI changed from the earlier accepted server sources.
- Native large profiles preserve original attempt/completion/logical-byte metrics, complete multipart/ranged-byte proofs, MD5 rejection without publication, exact two-part composite ETags, bounded 1.2 MiB operation get/CAS/time budgets, complete 100 MiB restart replay and same-name credential fencing. Concurrent direct writes retain the 33-party barrier and all 32 successful 8 MiB requests.
- Java SDKs preserve normal S3FileIO reads/writes, real Parquet snapshots and staged table restart validation, and selected data/delete-file read proofs. Maven builds once in the run-specific pinned child project; each client keeps its original external deadline. New fine scopes are `iceberg.sdk.java` and `iceberg.sdk.rust`; `iceberg.sdk` combines them.
- Managed Console is **not accepted**. The restored monitor summary passes component/build gates but the original combined browser proof now exposes current Docker readonly policy: `/api/racks` returns 403, not its older 503 expectation, and user-data writes are forbidden. The authority-outage proof sees disconnected preview requests while supervision recovers dependencies. Original assertions and sources remain until the product-mode disposition is resolved.

### Combined SDK, service selection and Dataset acceptance

- `run-7910db0eaa964d75a517464ea77df80f`: 3 passing owners; 50.102 s scenario wall.
  - `iceberg.sdk.java` / `official_java_catalog_commits_native_parquet_snapshots_and_staged_tables`: client 35.671 s; fixture 50.095 s.
  - `iceberg.sdk.java` / `official_java_opaque_metadata_publication_preserves_valid_data_and_delete_reads`: client 10.148 s; fixture 24.012 s.
  - `iceberg.sdk.java` / `official_java_s3_fileio_uploads_and_reads_native_files`: client 10.054 s; fixture 21.988 s.
- `run-5e9246c309ad4701ab00414e2df8bb08`: 3 passing owners; 17.852 s scenario wall.
  - `deployment.selection` / `complete_release_services_recover_acknowledged_data`: client 0.173 s; fixture 9.082 s.
  - `deployment.selection` / `selected_storage_services_recover_acknowledged_data`: client 0.169 s; fixture 7.052 s.
  - `deployment.selection` / `selected_kv_services_recover_acknowledged_data`: client 0.189 s; fixture 6.102 s.
- `run-5569f38c94964c71b2d50abf3789e4c6`: 2 passing owners; 16.746 s scenario wall.
  - `dataset.http` / `publish_persist_restart_and_scan_round_trip`: client 2.724 s; fixture 16.737 s.
  - `dataset.http` / `listener_authenticates_and_serves_create_open`: client 1.028 s; fixture 14.811 s.

- Dataset component tests remain: 30 core, 3 retention, 2 adapter and 2 transport tests passed. Two inline HTTP listener owners now run against formal release containers. The publish owner retains its original snapshot/manifest/scan assertions, adds exact payload bytes and restarts Dataset plus Chunk-KV before reading. Publication allocates the authoritative snapshot ID; expected metadata uses the returned ID, preserving original behavior.

### Container FileIO profile acceptance

- `run-087b7c04f0bc47b68b603d3b27fc1f03`: both owners pass; scenario wall 68.166 s.
- Source `be5757bb7db1089ec18c59e18fd3944518a27a52`; image config `sha256:6331286a54f51737387e610183a899c5d8dade8284f1a6ab5734bfcbc04b94e9`.

| Scope             | Named flow          | Client (s) | Fixture (s) |
| ----------------- | ------------------- | ---------- | ----------- |
| iceberg.benchmark | small_file_profiles |     37.125 |      49.716 |
| iceberg.benchmark | five_mib_profile    |      4.524 |      18.435 |

- Three small-file sizes retain 128 requests each at concurrency 1 and 32, all preparation/allocation/feed/frame counters, and complete GET bytes. The 5 MiB owner retains direct/multipart operation budgets, exact path metrics, chunk geometry, composite ETag and complete bytes. Both now use persistent protected release storage; old native NullDisk profile sources retire after this gate.

### Fresh Console operations and FileIO acceptance
- Formal image source `712bafe63bcc0bd1f63a574f5350e900da2321c0`; config `sha256:c6c054e56522efcdde98366cabd114571e13e3253cf4cc1c9e650819dd75faf6`.

- `run-f1e60806f97f447283bc24ce904c1cf4`: 68 passing owners.

| Scope                     | Named flow                                                                                     | Client (s) | Fixture (s) |
| ------------------------- | ---------------------------------------------------------------------------------------------- | ---------- | ----------- |
| console.bootstrap         | changed_bootstrap_topology_is_rejected_before_group_zero_mutation                              |      0.095 |       4.054 |
| console.bootstrap         | committed_group_zero_is_verified_before_stale_intent_is_removed                                |      0.229 |       3.908 |
| console.bootstrap         | interrupted_bootstrap_restores_identity_then_clears_verified_intent                            |      0.195 |       3.600 |
| console.bootstrap         | bootstrap_preflights_logical_conflicts_before_publishing_missing_hardware                      |      0.255 |       4.274 |
| console.bootstrap         | bootstrap_publishes_shared_hardware_labels_and_credential_reference                            |      0.366 |       4.595 |
| console.bootstrap         | bootstrap_rejects_a_different_rack_name_even_when_membership_matches                           |      0.179 |       4.219 |
| console.bootstrap         | bootstrap_rejects_conflicting_hardware_without_overwriting_authority                           |      0.172 |       4.086 |
| console.bootstrap         | bootstrap_resumes_missing_records_and_preserves_committed_revisions                            |      0.390 |       3.897 |
| console.bootstrap         | bootstrap_retry_preserves_known_rack_membership_and_node_runtime_fields                        |      0.197 |       4.147 |
| console.bootstrap         | clean_requires_confirmed_group_replicas                                                        |      0.261 |       4.262 |
| console.cleanup           | failed_group_deletion_preserves_all_authority_records                                          |      0.251 |       4.010 |
| console.cleanup           | failed_store_deletion_preserves_all_authority_records                                          |      0.189 |       4.021 |
| console.cleanup           | group_deletion_cleans_replicas_and_preserves_siblings                                          |      0.245 |       4.262 |
| console.cleanup           | group_deletion_reconciles_a_lost_metadata_response                                             |      0.345 |       4.187 |
| console.cleanup           | store_deletion_cleans_descendants_and_reaches_later_replica_hosts                              |      0.227 |       3.969 |
| console.cleanup           | store_deletion_reconciles_a_lost_metadata_response                                             |      0.395 |       4.439 |
| console.cleanup           | missing_peer_endpoint_rolls_back_without_publishing_membership                                 |      0.325 |       4.374 |
| console.cleanup           | peer_wiring_failure_rolls_back_without_publishing_membership                                   |      0.257 |       3.900 |
| console.cleanup           | replica_creation_requires_every_existing_peer_endpoint                                         |      0.182 |       4.089 |
| console.cleanup           | replica_creation_requires_every_existing_peer_registration                                     |      0.139 |       3.783 |
| console.cleanup           | replica_creation_rolls_back_when_new_endpoint_is_missing                                       |      0.209 |       3.623 |
| console.cleanup           | automatic_replica_identity_exhaustion_is_validation_error                                      |      0.125 |       3.735 |
| console.cleanup           | existing_replica_host_is_rejected_before_local_mutation                                        |      0.131 |       3.934 |
| console.cleanup           | failed_group_creation_removes_new_store                                                        |      0.188 |       4.061 |
| console.cleanup           | failed_rollback_is_reported                                                                    |      0.205 |       4.073 |
| console.cli               | kv_put_get_delete_round_trip                                                                   |      0.447 |       4.308 |
| console.cli               | cluster_node_list_ignores_legacy_local_state                                                   |      0.204 |       4.069 |
| console.cli               | cluster_rack_list_ignores_legacy_local_state                                                   |      0.197 |       4.154 |
| console.cli               | cluster_status_via_direct_group0                                                               |      0.133 |       4.017 |
| console.cli               | destroy_reads_group_zero_and_requires_launch_registry                                          |      0.482 |       4.352 |
| console.cli               | incremental_local_deploy_reads_group_zero_instead_of_legacy_file                               |      0.168 |       4.301 |
| console.cli               | kv_server_controls_require_launch_registry                                                     |      0.144 |       3.780 |
| console.cli               | rack_node_lifecycle                                                                            |      0.456 |       4.161 |
| console.cli               | store_group_replica_round_trip                                                                 |      1.840 |       5.556 |
| console.cli               | cluster_status_topology_via_direct_group0                                                      |      0.355 |       4.240 |
| console.cli               | cli_uses_launch_registry_and_runtime_identity_without_legacy_state                             |      0.748 |       4.551 |
| console.cli               | registry_bootstrap_uses_sealed_intent_without_legacy_topology_file                             |      0.341 |       4.250 |
| console.cli               | registry_hardware_uses_group_zero_across_cli_invocations                                       |      0.594 |       4.549 |
| console.clients           | put_get_delete_cycle                                                                           |      0.782 |       2.394 |
| console.clients           | full_store_group_remote_cycle                                                                  |      1.505 |       2.927 |
| console.deployment        | authenticated_node_update_preserves_ids_and_converges_from_another_console                     |      0.190 |       4.182 |
| console.deployment        | concurrent_admissions_allocate_unique_stable_ids_through_group_zero                            |      0.495 |       4.196 |
| console.deployment        | service_retries_retain_identity_and_other_nodes_cannot_claim_the_same_service                  |      0.192 |       3.911 |
| console.discovery         | cache_hit_within_ttl                                                                           |      0.195 |       4.656 |
| console.discovery         | chunk_kv_raw_observation_preserves_load_and_partition_state                                    |      0.161 |       4.618 |
| console.discovery         | discover_by_endpoint_found                                                                     |      0.146 |       3.959 |
| console.discovery         | discover_by_endpoint_not_found                                                                 |      0.106 |       3.499 |
| console.discovery         | discover_registered_diskdb_instance                                                            |      0.170 |       4.376 |
| console.discovery         | empty_registry_returns_error                                                                   |      0.058 |       3.155 |
| console.discovery         | expired_instance_filtered                                                                      |      0.126 |       3.401 |
| console.discovery         | invalidate_forces_refresh                                                                      |      0.230 |       4.189 |
| console.discovery         | round_robin_selection                                                                          |      0.066 |       3.616 |
| console.discovery         | zero_ttl_disables_caching                                                                      |      0.137 |       3.405 |
| console.hardware          | committed_disk_group_survives_a_lost_response                                                  |      0.381 |       3.736 |
| console.hardware          | committed_rack_survives_a_lost_conditional_write_response                                      |      0.115 |       3.814 |
| console.hardware          | concurrent_node_creates_preserve_both_rack_memberships                                         |      0.202 |       3.651 |
| console.hardware          | separate_consoles_confirm_matching_hardware_and_reject_conflicts                               |      0.255 |       3.621 |
| console.hardware          | two_consoles_share_disk_groups_and_disks_without_local_topology                                |      0.442 |       3.966 |
| console.logical           | group_and_initial_replica_share_one_committed_revision                                         |      0.157 |       3.751 |
| console.logical           | group_creation_prepares_a_local_store_on_new_nodes                                             |      0.154 |       3.729 |
| console.logical           | lost_batch_response_confirms_complete_group_membership                                         |      0.245 |       3.927 |
| console.logical           | orphan_membership_is_not_overwritten_by_group_creation                                         |      0.264 |       4.043 |
| console.logical           | committed_store_is_confirmed_after_its_write_response_is_lost                                  |      0.094 |       4.071 |
| console.logical           | concurrent_conflicting_store_publication_is_preserved                                          |      0.268 |       4.237 |
| console.logical           | concurrent_matching_store_publication_is_confirmed                                             |      0.165 |       4.031 |
| console.web.authority     | bare_metal_hardware_routes_share_confirmed_group_zero_state                                    |      0.510 |       6.121 |
| console.web.authority     | bare_metal_snapshot_requires_live_authority_without_a_docker_monitor                           |     13.110 |      33.317 |
| console.web.authority     | snapshot_validates_later_replica_hosts_as_well_as_original_store_hosts                         |      0.177 |       6.148 |

- `run-b4c543cc563746e389617b8a680f20b6`: 2 passing owners.

| Scope                     | Named flow                                                                                     | Client (s) | Fixture (s) |
| ------------------------- | ---------------------------------------------------------------------------------------------- | ---------- | ----------- |
| iceberg.benchmark         | five_mib_profile                                                                               |      4.269 |      18.827 |
| iceberg.benchmark         | small_file_profiles                                                                            |     40.945 |      56.008 |

- `run-d48cca26df0c4eaca6c4d2bb71538ceb`: 1 passing owner; `iceberg.sdk.rck` / `apache_rest_compatibility_kit_supported_catalog_surface`: client 38.065 s, fixture 54.222 s.

- `run-d3d0d1e6d3cf4b638c7022066237e3df`: 1 passing owner; `console.web.slots` / `owner_windows_keep_disjoint_slots_generations_and_exact_ids`: client 0.256 s, fixture 6.067 s.

### Monitor, deployment and Web acceptance
- Same formal image config `sha256:c6c054e56522efcdde98366cabd114571e13e3253cf4cc1c9e650819dd75faf6`; each fault scenario owns its full release container.

- `run-06d28b770ff44e5391f7ec4b0d21e477`: 3 passing owners.

| Scope                  | Named flow                                                                 | Client (s) | Fixture (s) |
| ---------------------- | -------------------------------------------------------------------------- | ---------- | ----------- |
| console.clients        | formal_server_appears_healthy_in_aggregate_snapshot                        |      0.047 |       1.466 |
| console.clients        | put_get_delete_cycle                                                       |      0.838 |       2.304 |
| console.clients        | full_store_group_remote_cycle                                              |      1.529 |       3.033 |

- `run-ff753234d94e4d838c45d582b657bf7b`: 1 passing owners.

| Scope                  | Named flow                                                                 | Client (s) | Fixture (s) |
| ---------------------- | -------------------------------------------------------------------------- | ---------- | ----------- |
| console.web.catalog    | published_owner_change_invalidates_saved_catalog_and_runtime_cursors       |      0.278 |       5.881 |

- `run-63921b227f644bd2a162643e3891d318`: 14 passing owners.

| Scope                  | Named flow                                                                 | Client (s) | Fixture (s) |
| ---------------------- | -------------------------------------------------------------------------- | ---------- | ----------- |
| deployment.supervision | access_crash                                                               |      3.610 |       8.338 |
| deployment.supervision | access_hang                                                                |     23.586 |      28.020 |
| deployment.supervision | chunk_kv_crash                                                             |      3.698 |       8.311 |
| deployment.supervision | chunk_kv_hang                                                              |     23.918 |      28.201 |
| deployment.supervision | chunkdb_crash                                                              |      3.509 |       8.122 |
| deployment.supervision | chunkdb_hang                                                               |     23.665 |      28.241 |
| deployment.supervision | diskdb_crash                                                               |      7.586 |      12.402 |
| deployment.supervision | diskdb_hang                                                                |     26.982 |      31.613 |
| deployment.supervision | diskio_crash                                                               |      7.760 |      13.139 |
| deployment.supervision | diskio_hang                                                                |     26.489 |      30.995 |
| deployment.supervision | kv_crash                                                                   |     29.106 |      33.783 |
| deployment.supervision | kv_hang                                                                    |     48.110 |      52.699 |
| deployment.supervision | web_crash                                                                  |      3.688 |       8.282 |
| deployment.supervision | web_hang                                                                   |     23.855 |      28.432 |

- `run-f16aa9b5eff7493787b86398330f1b1b`: 2 passing owners.

| Scope                  | Named flow                                                                 | Client (s) | Fixture (s) |
| ---------------------- | -------------------------------------------------------------------------- | ---------- | ----------- |
| deployment.hardware    | conflicting_group_zero_record_rejects_without_creating_hardware            |      0.130 |       4.024 |
| deployment.hardware    | writes_and_validates_hardware_with_real_group_zero                         |      0.271 |       3.787 |

- `run-d649f54bbf36450fabd09b7fa5d90d5c`: 1 passing owners.

| Scope                  | Named flow                                                                 | Client (s) | Fixture (s) |
| ---------------------- | -------------------------------------------------------------------------- | ---------- | ----------- |
| deployment.bootstrap   | interrupted_bootstrap_preserves_completed_steps                            |      0.090 |      13.073 |

### Fresh release deployment measurements

- Formal release source `7a47012129be3e3015c463db5f17726593c6e8df`, config `sha256:bccfcd41f739d0370c4687ffc96bcbc18c45ed0edc0dc64e496e0886cee65649`, optimized with debug info retained. Server quota: 0.25 CPU for selected KV, 2 CPU for the complete service set; host-client/resource reservations enforced. The full 25-owner run has 23 initial passes; corrected storage/validation scopes and the additional SDK/runtime owner pass independently on the identical image.

| Scope                  | Named flow                                                                        | Client (s) | Fixture (s) | Accepted run                         |
| ---------------------- | --------------------------------------------------------------------------------- | ---------- | ----------- | ------------------------------------ |
| deployment.bootstrap   | interrupted_bootstrap_preserves_completed_steps                                   |      0.103 |      14.896 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.clients     | official_clients_and_protocol_layout_survive_restart                              |     17.956 |      28.969 | run-704a072312a34ea5816676437a12acef |
| deployment.selection   | complete_release_services_recover_acknowledged_data                               |      0.187 |      10.083 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.hardware    | conflicting_group_zero_record_rejects_without_creating_hardware                   |      0.120 |       3.649 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.hardware    | writes_and_validates_hardware_with_real_group_zero                                |      0.181 |       3.967 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.kv          | monitor_creates_real_kv_groups_then_validates_after_restart                       |      0.390 |      12.525 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.selection   | selected_kv_services_recover_acknowledged_data                                    |      0.197 |      11.344 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.selection   | selected_storage_services_recover_acknowledged_data                               |      0.175 |      13.220 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.storage     | chunk_services_validate_live_registration_and_logical_topology                    |      0.870 |      10.836 | run-c208b8a204e54cec953c8a67413a8b1b |
| deployment.storage     | real_iceberg_catalog_and_listener_survive_restart                                 |      0.453 |      10.091 | run-c208b8a204e54cec953c8a67413a8b1b |
| deployment.supervision | access_crash                                                                      |      3.073 |       8.251 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | access_hang                                                                       |     23.246 |      28.458 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | chunk_kv_crash                                                                    |      3.170 |       8.333 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | chunk_kv_hang                                                                     |     28.375 |      33.430 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | chunkdb_crash                                                                     |      6.754 |      13.961 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | chunkdb_hang                                                                      |     23.663 |      28.869 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | diskdb_crash                                                                      |      8.072 |      23.367 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | diskdb_hang                                                                       |     27.154 |      32.653 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | diskio_crash                                                                      |      8.075 |      13.355 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | diskio_hang                                                                       |     27.438 |      34.985 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | kv_crash                                                                          |     28.499 |      34.082 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | kv_hang                                                                           |     49.114 |      54.550 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | web_crash                                                                         |      3.249 |       8.359 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.supervision | web_hang                                                                          |     23.385 |      28.491 | run-f1261241061e4e0faae2fea7ff06c2f2 |
| deployment.validation  | invalid_manifest_is_rejected                                                      |      0.022 |       5.696 | run-18c32de4733a4cad9a1ec1929cb46a39 |
| deployment.validation  | invalid_profile_is_rejected                                                       |      0.023 |       4.969 | run-18c32de4733a4cad9a1ec1929cb46a39 |

### Terminal lifecycle and default-volume measurements

- Same source/config as the fresh release deployment measurements above. Each named flow owns its release container and persistent state.

| Scope                  | Named flow                                                                    | Client (s) | Fixture (s) | Accepted run                         |
| ---------------------- | ----------------------------------------------------------------------------- | ---------- | ----------- | ------------------------------------ |
| deployment.failures    | restart_exhaustion_exits_the_container                                        |     60.274 |      68.204 | run-c560ffe7c4c54c4fb47ac83461767904 |
| deployment.failures    | recovery_rejects_changed_durable_credentials                                  |      2.252 |      10.238 | run-c560ffe7c4c54c4fb47ac83461767904 |
| deployment.anonymous   | default_anonymous_volume_is_ready_and_monitor_death_exits_container           |      0.366 |       4.721 | run-d919293a24e64f03a09b125f8087676e |

### Shipped readonly Web browser

- Baseline unchanged script: 6.089 s. Accepted run `run-604b717d7494423f9a4ec4232c6fb7ee`, same retained-debug release config as above. Original 3-second DOM and 10-second response bounds retained; installed host browser only.

| Scope              | Named flow                                                      | Browser (s) | Dependency preparation (s) | Fixture (s) |
| ------------------ | --------------------------------------------------------------- | ----------- | -------------------------- | ----------- |
| deployment.browser | readonly_release_ui_preserves_domains_and_live_authority         |       6.181 |                      3.319 |      14.637 |

### Listener failures and single-node balance policy

- Same formal release config as above. Listener cases reserve 3 CPU/3072 MiB including the extra server. Balance case uses an explicitly selected host data parent; original 12-second absence window is retained.

| Scope                | Named flow                                                 | Client (s) | Fixture (s) | Accepted run                         |
| -------------------- | ---------------------------------------------------------- | ---------- | ----------- | ------------------------------------ |
| deployment.listeners | occupied_s3_listener_terminates_combined_access            |      0.026 |       6.086 | run-6360a52e1976425abf4c1dcb7900c180 |
| deployment.listeners | occupied_iceberg_listener_terminates_combined_access       |      0.026 |       6.078 | run-6360a52e1976425abf4c1dcb7900c180 |
| deployment.balance   | single_node_disables_local_chunk_kv_split_planning         |     12.227 |      17.164 | run-17106e704fb44b28971fecac49f07723 |

### Retained Web handler component proofs

- Seven original tests retain their assertions and use pinned in-memory metadata and a topology peer. These measurements exclude compilation. Packaged public authority/catalog/slot and managed deployment flows remain in container acceptance.

| Component target       | Cases | Previous host-server fixture (s) | Pinned component fixture (s) |
| ---------------------- | ----- | -------------------------------- | ---------------------------- |
| storage_readiness_test |     1 |                             0.91 |                         0.12 |
| chunk_kv_catalog_test  |     1 |                             2.24 |                         0.76 |
| managed_mode_test      |     5 |                             6.01 |                         5.20 |
