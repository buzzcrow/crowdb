<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R196: benchmark — Refine benchmark tests

#### Problem

Service benchmarks currently mix native regression fixtures, standalone Rust
bench binaries, and `crowdb-cli bench` commands. Result parsing, phase checks,
CPU accounting, and reports are repeated in shell/Python drivers. The existing
S3 CLI already sends authenticated HTTP requests to a selected cluster, but
does not observe server resource usage. Iceberg lacks an equivalent reusable
upload regression. These differences make storage and access measurements hard
to reproduce or relate to one another.

The current Stream measurements demonstrate why deployment and CPU conditions
belong with every result: a client and services sharing CPUs 0–3 behave very
differently from the same binaries allowed on CPUs 0–31. Chunk-KV uses Stream
for its WAL, so Stream waits must also be observable in the KV workload. Object
performance needs its own measurements above ChunkIO. Neither an isolated
Stream microbenchmark nor an HTTP upload result describes all these layers.

This requirement unifies the service benchmark regressions under
`tools/benchmark/`, including RPC, KV, DiskDB, ChunkDB, ChunkIO/object,
Chunk-Stream, Chunk-KV, S3, and Iceberg. Relevant contracts are the
[OCI cluster lifecycle](../../container/oci-image/README.md),
[Stream design](../design/chunkds/design-crowdb-chunk-stream.md),
[Chunk-KV design](../design/chunkds/design-crowdb-chunk-kv.md),
[object writer design](../design/chunkio/design-crowdb-chunkio-small-object-writer.md),
and [upload flow](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md).

#### Solution

The default regression deployment is a **three-node cluster with one node per
container**. A host-side `crowdb-cli bench` process is the external benchmark
tool. Publish the test servers' required ports and advertise endpoints reachable
by that client. All service regression drivers adopt this deployment and CLI
path; the current native Stream fixture remains only until this work lands.

Responsibilities are explicit:

- Existing OCI/cluster lifecycle code builds and starts the three containers,
  prepares storage, checks full application readiness, exposes endpoints, and
  stops only the invocation's owned resources.
- `crowdb-cli bench` owns workload execution, sampling, resource/metrics
  collection, accounting, validation, and the common structured report. Reuse
  common-library system counters and existing management/status APIs.
- Each driving script selects cases and deployment/CPU profiles, invokes the
  external CLI, retains its exit status and artifacts, and records measured
  results in that same script's comments. Do not maintain separate tracked
  result documents or baseline JSON files, or duplicate collectors in scripts.

The following invariants define the benchmark:

- **I1 — Actual service path.** The external CLI reaches the selected cluster
  over published ports. Storage workloads use the deployed services; S3 and
  Iceberg use authenticated HTTP. The CLI does not create an in-process cluster
  as a substitute. A successful write means the layer's configured durability
  and publication contract is satisfied.
- **I2 — Controlled input and timing.** Record payload seed, size, concurrency,
  count/duration, warmup, read-cache policy, and media/durability profile. Bound
  client payload memory independently of large object size. Separate cluster
  setup, dataset preparation, warmup, measurement, drain, verification, and
  cleanup. A duration-limited case cannot silently finish early due to a small
  operation limit. Timed 4-KiB single-/32-producer write cases run at least
  60 seconds by default; short pipeline smoke tests are labelled separately.
- **I3 — Complete accounting.** Track admitted, completed, failed, and incomplete
  operations, drain admitted work, and verify deterministic bytes outside the
  timed interval. A timeout, missing result/required metric, failed cleanup,
  or unverified write fails the case. Missing cases cannot disappear from a
  passing summary. Bounded memory and queue limits remain regression gates.
- **I4 — Attributable measurements.** Identify client, all three nodes, service
  instances, and measurement windows. Counts and sums are interval deltas;
  interval percentiles come from matching samples/histogram buckets, never
  subtraction of cumulative percentiles. Do not average node percentiles into
  a cluster percentile. Concurrent phases overlap and cannot be summed as if
  they partitioned request latency. Counter resets or replaced processes are
  explicit. Optional unavailable counters are `unsupported`, never zero.
- **I5 — Comparable conditions.** A result includes exact commands and software,
  image, topology, CPU, runtime, storage, and durability identities. Compare
  only matching profiles with documented tolerances. Default placement and a
  constrained cpuset are separate profiles; native MemDisk and container disk
  results are also distinct. The accepted implementation is the baseline;
  the runner does not build or run an older binary to produce a comparison.
- **I6 — Owned three-node deployment.** Readiness requires all three members
  and usable storage. Port conflicts, missing peers, or unreachable advertised
  endpoints fail before measurement, without singleton/native fallback.
  Failure/interruption retains diagnostics and cleans only resources owned by
  this invocation. Image builds, compilation, tests, and other workloads run
  outside performance measurement windows.
- **I7 — Results live with the driver.** Record the dated measured summary and
  its conditions directly in each driving script. The CLI can emit JSON/TSV
  and raw artifacts into ignored runtime directories and generate a comment
  block for the driver. Retain useful previous measurements there with their
  profile/revision so comparisons have context. Do not create an additional
  tracked result file; optional raw traces do not replace the script summary.

Work items:

1. Reuse `container/oci-image/` and the existing CLI cluster lifecycle to run
   the owned three-container cluster. Publish HTTP and the management/RPC/data
   endpoints needed by each selected workload; verify client reachability of
   every address returned by discovery, routing, and redirects. Supply the
   accepted public origins when creating the deployment. The CLI runs outside
   all node containers, using explicit target bindings and private credentials.
   Record exact container/image identities and port bindings. Do not implement
   another bootstrap or silently attach to an unrelated running cluster.
2. Refine the commands under `app/crowdb-cli/src/commands/bench/`, extending its
   existing `metrics` and `result` responsibilities and reusing workload code
   in `lib/crowdb-console-shared/src/ops/` and owning storage libraries. Add
   Stream and Chunk-KV entry points where absent; move the reusable Stream
   workload behind the CLI. Preserve S3's existing real HTTP path and extend
   it for bounded large uploads and multipart. Add the Iceberg FileIO adapter.
   All workload families share accounting, collector lifetimes, output schema,
   and failure handling instead of parsing human-readable metrics in scripts.
3. Implement one CLI collection lifecycle around preparation, measurement,
   drain, and teardown. Take service snapshots before/after the measured window
   and flush final client metrics. Use bounded histograms and existing periodic
   system metrics; make per-operation traces optional. Report collection cost
   and window boundaries, with no request-path file/process polling. Retrieve
   server process/cgroup evidence through the owned deployment or management
   APIs, mapping container identities explicitly; never interpret a container
   PID as a host PID or report CLI-only CPU as cluster CPU.
4. Capture the report contents and workload matrix below. Reuse shared system
   counters for CPU and C6; do not add another hardware counter implementation
   to the scripts or CLI. Permission failures leave optional fields explicitly
   `unsupported`; default benchmarks need no extra perf privileges or power
   policy changes. A missing required workload/phase field still fails.
5. Convert every service regression driver under `tools/benchmark/` to the
   three-node container plus external CLI path, retaining case filters, bounded
   timeouts, and complete failure output. The scripts carry named case/gate
   configuration and measured result comments; collection, latency math, and
   report validation belong in the CLI. Preserve correctness gates during the
   conversion. Recapture results for the new container profile rather than
   treating native measurements as directly comparable. Support an explicitly
   selected remote cluster through the same CLI, with unavailable local-host
   measurements marked `unsupported` and no remote deployment cleanup.

Required report contents:

- **Conditions:** UTC date; CLI command; source revision and dirty status;
  client/service binary hashes and image digest; host CPU model, kernel,
  physical/logical cores, SMT/NUMA topology; three node identities, placement,
  ports, mirror/EC policy, media type/device, KV/WAL backend, fsync/durability,
  dataset/cache policy, and runtime/RPC worker counts. Retained MemDisk, null
  data sources, sparse files, and real disks must be named accurately.
- **CPU placement and usage:** requested and effective cpusets for client,
  containers, and each service's worker threads; cgroup CPU quota/period and
  throttling deltas; client and per-node/service user/system CPU seconds,
  average CPU %, RSS/peak/growth, context switches and migrations where
  available. State that 100% means one logical CPU. Separate timed-window
  usage from whole-command usage that includes dataset preparation.
- **System evidence:** disk and network bytes/operations and resource pressure
  when available; per-core C6 and per-package C6 residency with units, elapsed
  interval and availability. Counter support/permission/reset states stay
  distinct. Optional detailed perf profiles have explicit collection settings
  and their own comparable profile; no per-operation privileged probes.
- **Workload outcome:** logical/physical bytes, operations/s and MiB/s, actual
  duration, admission/drain counts, errors/retries/timeouts, error classes,
  count/mean/p50/p95/p99/max latency and a mergeable histogram. State sample
  counts and resolution; omit unsupported percentiles with a reason. Separate
  client payload generation/signing/hash work from transfer/server stages.
- **Latency attribution:** queue depth/bytes and wait; RPC round-trip and
  response scheduling; append batch/mirror waits; asynchronous capacity and
  candidate preparation versus foreground wait; seal/rollover, StreamChunkRecord
  publication/build/get/scan/CAS, checkpoint and replay; cache/physical reads
  and reclaimed bytes. Chunk-KV adds WAL append/commit, tree read/write,
  flush/snapshot/compaction, and read-cache evidence. Object adds admission,
  batching, reservation/preparation, payload write, parity, and metadata
  publication. Use the actual available stage counters and identify required
  fields by workload; capture count, sum, distribution, and max separately.
- **Protocol stages:** S3/Iceberg direct PUT, UploadPart, multipart completion,
  and optional table commit are separate operations. A successful part does
  not count as a published object. Upload completion includes protocol metadata
  publication; FileIO latency does not include an unrelated snapshot commit.

Minimum workload matrix:

- **Existing RPC/KV/DiskDB/ChunkDB cases:** retain their meaningful workload and
  correctness coverage while moving startup and measurement to the common path.
- **Stream:** preserve the current seven cases: 4-KiB append at 1/32 producers,
  1-MiB append at 8, 4-MiB rollover, random 4-KiB read at 32, 320-MiB replay,
  and 576-MiB GC. Ordinary append must not checkpoint the cursor; metadata
  publication scales with chunk transitions, not historical record count.
- **Chunk-KV:** 4-KiB values at 1/32 clients for put/get, plus batch and ordered
  scan; record key/value sizes, batch size, read/write mix and cache state.
  Include a run crossing WAL chunk rollover and a bounded restart/replay case.
  A sustained case must expose background flush/snapshot/compaction work and
  cannot end before queued durable work drains. Correlate end-to-end KV latency
  with its Stream WAL metrics in the same report.
- **Object/ChunkIO:** small and large write/read plus mixed reads through the
  existing object writers/readers; include 4-KiB at 1/32 clients, 1-MiB at 8,
  and the configured small/large boundary. Record physical write amplification,
  batching, placement and verification rather than inferring performance from
  Stream or KV alone.
- **S3/Iceberg:** small-path and threshold-boundary uploads, direct 100-MiB PUT,
  and multipart 100-MiB upload, at single and concurrent clients. Reuse bounded
  deterministic payload blocks and precompute expected integrity data outside
  timing; record the integrity mode and part size. Preserve S3 read/range/list
  and mix coverage through the same collector. Final bounded byte/range checks
  occur outside upload timing.

#### Dependencies

- Existing three-node OCI cluster lifecycle, readiness, target bindings, and
  port/public-origin configuration. Add missing external RPC/data exposure in
  that lifecycle; an unreachable route blocks the affected benchmark rather
  than falling back to native services.
- Existing storage benchmark workloads, Stream/Chunk-KV APIs, object writers
  and readers, and S3/Iceberg authentication/publication paths. Reuse these
  contracts and existing native results as recorded evidence; this requirement
  owns their CLI/container regression conversion and new profile measurements.
- Shared metrics/system-status counters and management snapshots provide
  collection. Hardware-dependent counters remain optional if unavailable;
  required stage coverage is implemented or its case fails explicitly.
- Official-client compatibility remains in the existing E2E suites. A
  client-inclusive profile can use the common result schema without replacing
  the controlled CLI workload or mixing their results.

#### Acceptance

- Given an owned three-container cluster, run every converted driver with the
  host CLI; assert three ready members, published reachable endpoints and real
  service requests, with no native/in-process replacement (I1, I6). E2E test.
- Given a port collision, absent third member, or an unreachable advertised
  RPC/HTTP address, start a case; assert failure before timing, useful
  diagnostics, and no singleton fallback (I1, I6). E2E test.
- Given an existing unrelated cluster and an interrupted/failed benchmark,
  stop the run; assert only owned containers/data are cleaned under the declared
  retention policy and all failure logs remain available (I3, I6). E2E test.
- Given each minimum workload and fixed deterministic inputs, execute through
  the CLI; assert complete operation/byte accounting, bounded memory/queues,
  real stored-byte verification, and a common report (I1–I4). E2E test.
- Given S3 and Iceberg multipart uploads, complete both; assert distinct part,
  completion and optional table-commit measurements, correct integrity checks,
  and no part counted as a final object (I1, I3, I4). E2E test.
- Given a 60-second 4-KiB case and a small operation cap, execute the duration
  profile; assert it cannot receive a passing full-duration verdict early and
  that setup/warmup/drain have distinct windows (I2, I3). Integration test.
- Given permission-denied hardware counters and remote endpoints, collect
  metrics; assert per-core and package C6/local-resource availability is
  `unsupported` where appropriate, with no fabricated zeros or default sudo
  requirement (I4). Integration test.
- Given independent client/container cpusets and a throttled node, run a case;
  assert effective per-service/thread placement, quota, throttling, CPU time,
  RSS and timing windows are attributed correctly (I4, I5). E2E test.
- Given overlapping stage histograms, low sample counts, process replacement
  and counter reset, build the report; assert interval aggregation is valid,
  unsupported percentiles are identified, and neither percentile subtraction
  nor exclusive summation of overlapping phases occurs (I4). Unit test.
- Given Chunk-KV WAL rollover/replay and object small/large-boundary workloads,
  run the CLI cases; assert their own verified outcomes, latency distributions,
  Stream WAL/object stages, and drained durable work (I1–I4). E2E test.
- Given a failed request, missing phase/result/case, malformed report, timeout
  or cleanup failure, summarize the run; assert nonzero status and retained
  diagnostic evidence instead of a partial passing result (I3, I4). Integration test.
- Given matching and mismatched measurement profiles, assess recorded results;
  assert only matching profiles receive a tolerance verdict and that the current
  baseline does not require running an older binary (I5). Unit test.
- Given a completed run, generate its driver comment block; assert it includes
  results, date, exact command, image/revision, cpusets, CPU usage and artifact
  references, and can update the driving script without another tracked result
  file. Assert scripts delegate collection/validation to the CLI (I7). Integration test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`,
`pixi run cargo test -p crowdb-console-shared`,
`pixi run cargo test -p crowdb-cli`, and
`pixi run test-container-e2e-fixture`.
Run each converted service regression serially through
`pixi run bash tools/benchmark/bench-chunk-stream-regression.sh`,
`pixi run bash tools/benchmark/bench-chunk-kv-regression.sh`,
`pixi run bash tools/benchmark/bench-chunkio-write-regression.sh`,
`pixi run bash tools/benchmark/bench-chunkio-small-write-regression.sh`,
`pixi run bash tools/benchmark/bench-chunkio-read-regression.sh`,
`pixi run bash tools/benchmark/bench-rpc-regression.sh`,
`pixi run bash tools/benchmark/bench-kv-write-regression.sh`,
`pixi run bash tools/benchmark/bench-kv-read-regression.sh`,
`pixi run bash tools/benchmark/bench-kv-scan-regression.sh`,
`pixi run bash tools/benchmark/bench-diskdb-regression.sh`, and
`pixi run bash tools/benchmark/bench-chunkdb-regression.sh`.
Verify the S3/Iceberg adapters with `pixi run test-e2e-s3` and
`pixi run test-e2e-iceberg`, including their new CLI regression cases.
