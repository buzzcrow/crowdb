<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Stream Performance Plan

Upstream: [R244](../backlog/R244-chunk-stream-incremental-directory-publication.md)
and [Chunk-Stream design](../design/chunkds/design-crowdb-chunk-stream.md).

Goal: make synchronized mirror writes complete ordinary append without Stream
KV work, with compact locations, incremental directories, and bounded recovery.

## Status and scope

- Deployment requirement R243 is complete at `c3d7d64d`; R244 is in progress.
- T01/T02/T03/T05–T07 foundations and T20a's measured hot-path changes are
  verified. T20b capacity preparation is verified; T18 is active. Ordinary
  append now completes at synchronized mirrors with local owner checks; cursor
  persistence occurs only at checkpoints. Production recovery scans complete
  grouped frames past the checkpoint after process exclusion and ordered drains.
  The affected suite, strict lint and unchanged two-case performance gates
  pass, including a second measurement of the exact final source.
  T13's indexed retention primitives are committed; expiry/read-IO grace and
  runtime/consumer wiring remain. Directory, shared-codec, ordinary writer and local-progress
  foundations exist; the main Stream worker still needs indexed wiring,
  safe ownership/recovery, bounded retention and production acceptance.
- User instruction: this product has never shipped. Support only the chosen
  current format and matched service contract. Do not add old-format migration,
  old-service fallback or capability negotiation. Remove compatibility code and
  its fixtures. Existing development runtime artifacts remain preserved; a test
  fixture creates data in its current format.
- The matched retained-MemDisk baseline and intermediate rerun at `377870bb`
  fail six performance gates. The final hot-path run passes four of seven gates;
  the three remaining failures are concurrent small-append p99 and large-append
  throughput. All cases have zero errors/watchdogs, and directory publication,
  queue and RSS bounds pass. Performance acceptance is still incomplete.
- The two pre-existing unrelated user edits stay outside this requirement.

- The main worker now uses current grouped frames and one location per append.
  No revision-1 Stream codec, raw-write mode or frame-version negotiation remains.
  Indexed directory wiring and the remaining ownership/recovery acceptance
  work remain separate implementation tasks. Per-batch cursor work is removed
  by the current integration, pending the gates recorded below.

## Synchronized-mirror latency investigation

- The 4 KiB / one-in-flight benchmark uses retained `MemDisk`: Linux
  `memfd_create`, byte-addressed writes, and the real io_uring submission and
  completion path. It does not write user data to a physical SSD. The two
  required mirrors run concurrently, with one write-and-sync RPC per replica.
  Group-0 `sync_interval_ms` does not gate data synchronization.
- Added `diskio.uring.{read,write,fsync}.{queue,completion}.lh`: queue spans
  callback allocation through SQ publication; completion spans publication
  through CQ dispatch and includes submission, kernel work and scheduling.
  These are not physical-device service-time measurements. Restored observation
  of `rpc.response.schedule.lh` using the existing callback timestamp; this
  includes callback buffer conversion and the wait until the Rust future resumes.
  The production benchmark now attaches existing ChunkIO metrics and prints
  per-replica write, RPC round-trip and response-scheduling histograms.
- Matched source/binary measurements are retained under
  `/home/cj/.cache/crowdb-r244-mirror-latency-results/`:
  `20261023T142534Z` uses the default CPU placement;
  `20261023T143143Z` runs the same fixture and benchmark under
  `pixi run taskset -c 0-3 bash .../measure.sh`. Binary manifests match and all
  fixture cleanup receipts report success. Each workload lasts ten seconds;
  both default-setting repetitions give the same rounded mean in each run.

| Measurement                         | Default CPU placement | CPUs 0–3 only       |
| ----------------------------------- | --------------------- | ------------------- |
| Append operations/s                 | 1,449 / 1,450         | 6,477 / 6,468       |
| Append mean                         | 688 / 688 us          | 153 / 153 us        |
| Append p50                          | 698 / 698 us          | 145 / 145 us        |
| Append p99                          | 991 / 989 us          | 295 / 317 us        |
| Mirrored append mean, first run      | 648.70 us             | 144.71 us           |
| RPC round-trip mean, first run       | 458.47 us             | 81.18 us            |
| Response scheduling mean, first run  | 98.61 us              | 22.68 us            |
| Server write mean, steady windows    | 158.25 us             | 25.80 us            |
| Server fsync mean, steady windows    | 31.42 us              | 11.21 us            |

- Server means are count-weighted across all nodes, excluding the first two
  and last one seconds of each case; client histograms also include background
  initialization and a small number of metadata RPCs. Replica means, mirrored
  maximums, and different populations must not be added as exclusive phases.
  The default server write includes 53.88 us waiting for SQ publication and
  99.74 us through CQ dispatch; the corresponding constrained values are
  7.53 and 17.12 us. All ten Stream cases have zero errors/watchdogs and zero
  cursor-checkpoint samples. CPU placement is a diagnostic variable, not a
  change to the accepted benchmark defaults or a product throughput claim.
- A direct memfd probe with real readback measured fresh-page `pwrite` at
  2.56 us and `fsync` at 0.79 us on average (Python call overhead included).
  The same shared io_uring engine, without RPC, measured 29.09 us for continuous
  write-plus-sync. Inserting 500 us of idle time **before** each timed operation
  increased that to 309.70 us; the idle gap is excluded from latency. Confining
  that probe to one CPU reduced the idle case to 99.39 us. Probe source and
  output are retained alongside the Stream runs.
- This host has 16 physical / 32 logical CPUs, `powersave`, and `intel_idle`;
  the reported C6 exit latency is 92 us with 276 us target residency. The
  controlled experiments establish a large CPU-placement/idle-wakeup effect.
  They do not separately identify how much comes from C-states, CPU frequency,
  migration, cache locality, or scheduler dispatch. System power policy was
  not modified. Merely reducing Tokio workers to two yields 657 us; reducing
  connections to one yields 673 us against adjacent 700/702 us controls, and
  also reducing RPC workers to one yields 694 us.
- Follow-up implementation should reduce avoidable thread handoffs in the
  RPC/IO completion path and measure both low-in-flight and saturated loads.
  Preserve the single-publisher SQ ordering and write/fsync completion proof.
  Any polling/affinity policy needs explicit CPU-cost measurements. Keep the
  existing histogram underflow/rank issues visible: below 65.536 us its current
  percentile bucket is not precise; exact sum/count means and raw benchmark
  end-to-end percentiles above remain usable.
- Verification: 14 shared io_uring tests, all 85 DiskIO tests and all 25 Rust
  RPC FFI tests pass. Strict release all-target clippy passes for RPC FFI and
  Chunk-Stream; workspace Rust format, changed C++ format, changed-file
  `tree-lint` and diff whitespace checks pass. The final C++ lint cleanup only
  changes aggregate initialization spelling and the SQE pointer conversion;
  both CPU-placement measurements used the identical pre-cleanup binaries.

## Cursor-free append integration (current)

- Ordinary append performs grouped encoding, synchronized required-mirror IO,
  owner validation, and local mapping/cursor publication. It makes no ChunkDB
  advance or Stream directory RPC. `stream.checkpoint.cursor.lh` replaces the
  old append cursor metric; a focused test asserts zero samples after append,
  then one after an ordered checkpoint. Checkpoint batches all pending bytes
  into one cursor advance before publishing its matching directory. Seal and
  rollover persist the synchronized local boundary directly.
- Fixed Group-0 writer assignments reference independently renewed per-boot
  process leases. Admission and acknowledgment validate the local authority;
  physical submissions retain their original conservative deadline through
  the metrics and routing decorators. Assignment nonces distinguish concurrent
  same-process claims and prevent an old release from releasing its successor.
  Explicit WAL transfer drains admitted writes, checkpoints, and releases the
  assignment. The service supplies its existing process authority, retaining
  its existing exact serving-context admission/response checks.
- Recovery excludes an unreleased prior process using its persisted process
  and grant expiry plus shared clock/request-age bounds, then drains current
  mirrored segments. It scans grouped frames beyond persisted progress,
  selects the shortest healthy complete prefix, verifies/replays it, checks the
  layout again, seals, and starts a fresh chunk. Live frame reads use the local
  synchronized bound without changing cached or persisted chunk boundaries.
- Tests cover a 2 MiB multi-frame acknowledged write recovered with zero cursor
  commits, incomplete groups, corruption, unequal replica tails, fenced delayed
  replies, renewal during blocked data IO, exclusive claims and release ABA,
  release-based immediate takeover, checkpoint aggregation, and WAL transfer.
  Logs live outside the project in
  `/home/cj/.cache/crowdb-r244-cursor-results/`.
- Current failures found during integration were old per-append cursor/metric
  assertions, finite checkpoint access after drained transfer, an incomplete
  fixture cut inside a checksummed frame instead of between grouped frames,
  and trim accounting sampled before rollover persisted the final append.
  Exact failing commands/output remain in the logs. A decorator inspection
  found the new IO deadline was not forwarded by metrics; the forwarding and
  actual gated TCP deadline test were added before measurement.
- Verification: all 450 affected Stream, Chunk-client, Chunk-KV and server
  tests pass, plus three protocol owner-record tests. Strict five-crate
  all-target clippy and workspace fmt pass. After the final cold-path fix,
  the 42 mirror/recovery/owner tests and 32 partition tests pass again. Repeated
  completed releases do not touch a successor's owner row; a stalled write
  cannot produce a clean release proof. Logs: `affected-tests-3.log`,
  `protocol-tests.log`, `release-tests.log`, `partition-final.log`, and
  `clippy-final.log` in the evidence directory above.
- First unchanged two-case retained-MemDisk measurement, after rebuilding all
  service and benchmark artifacts: `20261023T135751Z/`. Both gates pass with
  zero errors/watchdogs and source-unchanged receipt. Single producer rises
  295.63 -> 1639.63 ops/s, average 3.381 -> 0.608 ms, p99 3.997 -> 0.984 ms;
  32 producers rise 3706.15 -> 14776.94 ops/s, average 8.631 -> 2.164 ms,
  p99 13.927 -> 4.448 ms. Cursor checkpoint count is zero in both timed cases.
  The final release-idempotency checks were added afterward and are verified
  separately; the second run captures the exact final source.
- Final-source rerun: `20261023T140634Z/`, exit 0, source unchanged,
  zero errors/watchdogs, both original gates pass. The runner uses the original
  script's build-skip switch for unchanged service binaries; `cargo bench`
  rebuilds the changed benchmark before timing. All seven non-benchmark binary
  hashes match the full rebuild above. Each workload runs for 10 seconds on
  the same three-node retained-MemDisk fixture; no competing build/test runs
  during timing. Source snapshots/hashes, eight binary identities, comparisons,
  raw phase logs and successful fixture-stop receipts are retained.
- Final 4 KiB/1T: 295.63 -> 1462.28 ops/s (4.95x), average
  3.381 -> 0.682 ms, p99 3.997 -> 0.988 ms. Mirror synchronization averages
  0.643 ms; queue dispatch 3.74 us. Only initial attachment waits for capacity.
  14624 appends cause zero cursor checkpoints and one directory publication.
- Final 4 KiB/32T: 3706.15 -> 14776.94 ops/s (3.99x), average
  8.631 -> 2.163 ms, p99 13.927 -> 4.083 ms. Mirror synchronization averages
  0.907 ms per batch and queue wait 1.006 ms per request. 147847 appends,
  8697 batches and two rollovers cause zero cursor checkpoints and three
  directory publications. Rare capacity/rollover waits remain: 21 samples,
  maximum 367 ms. Higher throughput now reaches rollover during this workload;
  do not infer that p99 removes this remaining tail. Keep that optimization
  separate from the verified removal of ordinary cursor RPCs.
- T18 remains active: the exact serving assignment still needs propagation
  into the WAL IO context and assignment-specific unreachable-owner exclusion.
  The current process-level exclusion is conservative and can wait while the
  prior process remains live; it does not treat serving-grant expiry alone as
  permission to recover. Failed-open retry within the same live process also
  remains fail-closed. Recovery currently requires every current replica's
  drain; safe exclusion of an unreachable allocation remains future work.
  These limits and indexed directory/retention work prevent declaring R244
  complete; they do not reintroduce per-append RPCs.

## Direct hot-path integration

- [x] **T20b — Prepare strip capacity ahead of writes**: overlap bounded
  allocation and single-attempt replica initialization with appends in the
  already initialized prefix. A worker owns at most one preparation task;
  consume only completed capacity and drain preparation before seal/rotation
  or worker shutdown. Preserve current batching policy. Separate queue dispatch,
  foreground capacity waits, and background preparation in perf histograms.
  Permit allocation publication across cursor-only metadata progress while
  retaining exact layout/owner fences; reconcile definite cursor CAS conflicts
  caused by that allocation without replaying data. Verify blocked preparation,
  uncertain initialization, close/drain, growth/cursor races, and unchanged
  4 KiB single-producer/32-producer benchmarks. Files:
  `lib/crowdb-chunk-stream/src/stream/{capacity,preparation,worker}.rs`,
  `lib/crowdb-chunk-stream/src/production_chunk{,/growth,/cursor}.rs`,
  `lib/crowdb-chunk-stream/src/perf.rs`,
  `lib/crowdb-chunk-client/src/chunk/mirror_flow/repair.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler/append.rs`.
- The selected final design has no per-batch ChunkDB cursor update. Retaining
  it during this capacity change is unfinished integration, not a new contract.
  Queue dispatch and capacity waits now have separate histograms.

- The user requests actual measured performance gains after the intermediate
  rerun. Split directory removal from cursor removal: the existing synchronized
  write plus fenced cursor still protects append, so local mappings and lifecycle
  publication can remove the per-batch directory wait before T18 is complete.
  This is an implementation sequence, not a second format or weaker final contract.
- `stream/view.rs` retains append-only lock-free mappings since the last published
  checkpoint. One release/acquire boundary exposes each completed batch; no append
  clones the historical manifest, active extent vector or completed pages.
  Checkpoint/rollover installs a new read view. In-flight readers retain the old
  view until completion.
- An ordered `Capture` command publishes the current synchronized directory once
  and returns a finite handle. `StreamViewSource` distinguishes a live owner,
  owner checkpoint and persisted fallback. A runtime with the local owner routes
  reads through that barrier. Remote persisted fallback can lag; WAL checkpoints
  and split cutovers explicitly flush before publishing their recovery proof.
- Read planning validates each immutable extent page on cache insertion instead
  of rescanning all entries for every frame. The first same-task buffered-read
  implementation regressed replay; restore bounded multi-task execution for
  multi-frame reads, retaining a direct path for single-frame reads. Frame CRC,
  role and chunk identity checks remain on every data read.
- Add perf distributions for local mapping publication, owner checkpoint, read
  planning and physical reads. Retain original performance workloads and gates.
- Verification: 276 tests across Stream, Chunk-KV and Chunk-KV server pass with
  no failures/ignored cases (`live-directory-tests-final-2.log`); all-target strict
  clippy for those three crates passes (`live-directory-clippy.log`). New cases
  cover 2048 directory-free appends and recovery, finite ordered capture, explicit
  lagging fallback, stale capture and production runtime owner routing.
- The expanded test link initially exhausted `/nv`; the preceding successful
  suite's 33 debug executables and 39 earlier KV-client/server test executables
  were moved with receipts to
  `/home/cj/.cache/crowdb-r244-live-directory-debug-archive/`, freeing 16.39 GiB.
  No source, persistent fixture or release baseline was removed. The complete
  rerun passed after restoring free space.
- After the successful gates and commit, the entire project runtime/build
  directories disappeared during the session. This work issued no deletion of
  those directories; the cause is unestablished. Earlier raw artifacts are no
  longer available at their recorded paths. Their numbers below are historical
  records, not re-created raw evidence. New logs and receipts are saved outside
  the project at `/home/cj/.cache/crowdb-r244-live-directory-results/`.
- The flat directory still rebuilds at lifecycle checkpoints; lazy indexed runtime wiring and
  process-authorized content recovery remain unfinished. Do not mark R244 complete.

### First hot-path measurement and follow-up

- Full rebuild and unchanged seven-case regression at `fc6ae4e4`: artifacts in
  `/home/cj/.cache/crowdb-r244-live-directory-results/20261023T095340Z/`, including
  `intent.json`, `run.log`, `receipt.json`, original `stream/results.tsv`, phase
  logs and successful fixture-stop receipt. Previous measurements are explicitly
  transcribed from the recorded history in `previous-measurements.json`.
- Compared with `377870bb`: 4 KiB/1T 156.68 -> 294.51 ops/s, average
  6.381 -> 3.394 ms, p99 7.461 -> 3.933 ms; 4 KiB/32T 1894.28 -> 3451.04 ops/s,
  p99 39.568 -> 22.394 ms. 1 MiB/8T 46.74 -> 53.80 MiB/s; 4 MiB rollover
  94.23 -> 93.50 MiB/s; random 4 KiB 22019.79 -> 21761.24 ops/s;
  replay 159.29 -> 88.41 MiB/s. GC's single sample is 22.693 ms.
- All workloads have zero errors/watchdogs; queue/RSS bounds pass. Directory
  publications are attachment-only: 1 for 2946 single-producer batches, 1 for
  2045 concurrent batches, 3 for 545 large batches, and 2 for 65 rollover batches.
  The script exits 1 with five performance gate failures; this is not acceptance.
- Follow-up targets the measured causes: large-append capacity handling averages
  11.151 ms, and replay's physical-read phase accounts for 87.673 of 87.723 ms
  per 8 MiB window. Add bounded 8 MiB growth windows with at most eight parallel
  strip initializations; expose capacity only after all required replica syncs.
  Record allocator growth and strip initialization separately in perf histograms.
- Cache up to 64 verified-frame layouts under their original server deadline.
  Reuse requires a valid snapshot covering the requested durable prefix;
  growth, expiry, physical failures and checksum failures force refresh. Reader
  clones share invalidation. No payload integrity check or required replica
  synchronization is removed. Focused tests cover growth amortization/hard caps,
  uncertain initialization drain, cache deadline, prefix growth and corruption.
- The corrupt-replica regression also exposed unnecessary cache invalidation
  when a read skipped an already recorded unavailable segment. Invalidate only
  newly observed failures; the refreshed healthy fallback layout remains reusable
  under its original grant. The test verifies both exact bytes and query counts.
- Follow-up verification: all 431 tests in the four affected crates (Chunk
  client, Stream, Chunk-KV, Chunk-KV server) pass with zero failures/ignored cases.
  Full logs and the receipt are in
  `/home/cj/.cache/crowdb-r244-live-directory-results/checks-20261023T101920Z/tests-verified.log`.
  The preceding compile failure was a borrowed stream-future `Send` inference
  limitation, replaced by an explicitly bounded `FuturesUnordered` queue.
  Growth expectations now assert the exact 8 MiB window, and corruption query
  counts include the existing confirmed-observation metadata refresh.
- Strict all-target clippy for the same four crates and workspace fmt pass.
  After the MSRV-compatible `Option::map_or` spelling and checked test conversion,
  all 42 reader/production-adapter tests pass again. Logs are
  `clippy-verified.log`, `focused-verified.log` and `fmt-verified.log` in that
  checks directory. No lint suppression, timeout extension or gate change was used.

### Final measured hot-path result

- At `05076149`, the layout cache and restored read task parallelism raise random
  reads to 40767.95 ops/s and replay to 540.01 MiB/s. Large append rises to
  86.99 MiB/s, but the fixed 8 MiB growth window increases concurrent small-write
  p99 to 38.833 ms. Full evidence is in
  `/home/cj/.cache/crowdb-r244-live-directory-results/20261023T103745Z/`.
- The final follow-up sizes the window from four waiting append lengths, rounded
  to a power of two and bounded to 2–8 MiB. It retains the hard chunk limit and
  full zero/sync initialization. Small appends avoid the largest initialization
  burst; large appends retain the allocation amortization. All 57 affected
  production-adapter/growth/Stream tests, strict four-crate all-target clippy and
  fmt pass (`adaptive-tests-2.log`, `adaptive-clippy.log`, `adaptive-fmt.log`).
- Rerun every original workload/gate after rebuilding, with the same 10-second
  timed workloads, 180-second watchdog, three-node native retained-MemDisk and
  mem-block KV/WAL fixture. No container/CLI conversion, null-disk substitution,
  threshold change or competing test/build ran during measurement.
- Final artifacts:
  `/home/cj/.cache/crowdb-r244-live-directory-results/20261023T104812Z/`.
  `intent.json` and `source.patch` identify the exact follow-up on `05076149`;
  `binaries.json` records release identities. `run.log`, original
  `stream/results.tsv`, `comparison.tsv`/`comparison.json`, `phases.tsv` and
  successful fixture-stop receipt retain the results. The earlier baseline's
  raw-log loss remains disclosed above; comparisons use its recorded values.

| Workload                | Before `377870bb` | Final     | Unit  | Throughput gain |
| ----------------------- | ----------------- | --------- | ----- | --------------- |
| Append 4 KiB, 1 producer | 156.68            | 326.75    | ops/s | 108.55%         |
| Append 4 KiB, 32         | 1894.28           | 3512.56   | ops/s | 85.43%          |
| Append 1 MiB, 8          | 46.74             | 86.40     | MiB/s | 84.85%          |
| Rollover 4 MiB           | 94.23             | 123.48    | MiB/s | 31.04%          |
| Random 4 KiB, 32         | 22019.79          | 38564.78  | ops/s | 75.14%          |
| Replay 320 MiB, 1        | 159.29            | 496.66    | MiB/s | 211.80%         |

- Small single-producer average latency falls 6.381 -> 3.059 ms; p99 falls
  7.461 -> 3.931 ms. Concurrent small-write p99 falls 39.568 -> 25.288 ms
  (from 38.833 ms in the fixed-window attempt), but still exceeds 20 ms.
  Large-write p99 falls 235.470 -> 111.738 ms. GC passes at 19.638 ms;
  this is one operation, so do not infer a stable throughput improvement.
- The script exits 1: small concurrent p99 25.288 > 20 ms; 1 MiB throughput
  86.40 < 125 MiB/s; rollover throughput 123.48 < 200 MiB/s. It is not a passed
  performance regression or R244 completion. The single-producer, random-read,
  replay and GC gates pass. All seven cases have zero errors/watchdogs.
- Publication counts are 1/3268 single-producer batches, 1/2152 concurrent
  batches, 4/872 large batches and 2/65 rollover batches: only chunk attachments.
  The small single-producer stages average 0.666 ms mirror, 2.320 ms cursor and
  0.003 ms local view; cursor work remains about 76% of total latency. Large
  append averages 4.099 ms capacity, 0.751 ms encode, 3.023 ms mirror and
  3.665 ms cursor. Nested stages/parallel initialization percentiles are not
  additive. Replay physical reads average 13.626 ms per 8 MiB window, compared
  with 87.673 ms in the rejected same-task read implementation.
- T20a is complete as the measured removal of directory waits and read/growth
  amplification. The later cursor-free integration is recorded above; the
  indexed directory, bounded historical checkpoints and consumer root pins
  remain unfinished. The unchanged final performance gates remain T26 work.

### Capacity preparation verification

- The worker starts at most one bounded preparation while writing the initialized
  prefix. Ready results change only capacity, preserving the worker's newer
  cursors. Close, rollover and worker exit drain initialization before changing
  the chunk lifetime. Uncertain initialization never retries zeroing; temporary
  allocation unavailability permits the normal capacity slow path to retry.
- Allocation publication accepts monotonic cursor/lease-only changes against
  the exact same owner and layout. The production adapter reads the resulting
  complete chunk record in the background: constructing its new revision from
  an older local cursor could hide a delayed cursor response. Repair publication
  likewise rebases only while epoch and the exact replaced strip range match,
  reusing its initialized replacement and stable operation ID.
- Fault tests cover delayed allocation, delayed initialization, exhaustion of
  ready capacity, close/drain, initialization failure, temporary allocation
  recovery, cursor publication races, and repair publication races. The latter
  initially failed with `StaleWriter`; `repair-race-red-exact.log` preserves
  the exact failure. A first test invocation selected zero cases and is not
  counted as evidence. The transient-allocation test initially exposed a generic
  `Internal` error classification; only allocator availability now maps to the
  retryable preparation outcome. The exact failed output remains retained.
- Before the final race fixes, all 186 Stream/WAL tests and four ChunkDB fence/
  append integration tests passed. The final focused suite passes 36 tests,
  all 154 Chunk-client tests pass, strict all-target clippy for Chunk-client,
  Stream, Chunk-KV and ChunkDB passes, and workspace fmt passes. Evidence is in
  `/home/cj/.cache/crowdb-r244-capacity-results/`, including
  `stream-wal-tests.log`, `allocation-fence-test.log`,
  `append-integration-tests.log`, `focused-tests-4.log`, `client-tests.log` and
  `clippy-3.log`. A test-only cloned-slice lint was corrected without suppression.
- The first unchanged two-case measurement is retained in
  `/home/cj/.cache/crowdb-r244-capacity-results/20261023T124047Z/`.
  Concurrent append increases 3512.56 -> 3726.08 ops/s and p99 decreases
  25.288 -> 13.077 ms. All 19 growth preparations finish in the background;
  the only foreground capacity wait is initial attachment. Single-producer
  throughput is 291.64 ops/s versus the older 326.75; cursor latency increases
  2.320 -> 2.641 ms, while actual queue dispatch averages 3.03 us. Do not claim
  a single-producer throughput gain from this capacity change.
- Final unchanged two-case rerun, including the repair race fix:
  `/home/cj/.cache/crowdb-r244-capacity-results/20261023T124827Z/`.
  Receipt confirms exit 0 and unchanged measured sources; binary hashes and
  source copies are retained. 1T: 295.63 ops/s, average 3.381 ms, p99 3.997 ms.
  32T: 3706.15 ops/s, average 8.631 ms, p99 13.927 ms. Both cases have zero
  errors/watchdogs and pass their unchanged gates. All 19 concurrent growths
  finish in the background; the sole foreground wait is initial attachment.
  This is verification of two cases only, not the complete seven-case gate.

## Gap assessment

The gap is medium-to-large: the existing transport, mirror repair, framing,
bounded queue, CAS adapter, and instrumentation are reusable, but the Stream
state machine and persisted directory model need coordinated changes. Deleting
two calls in `write_batch` cannot produce a correct working implementation.

- **Compact locations — medium, D01/A01.** `ChunkLocation`,
  `framed_physical_length`, and `physical_range_for_subrange` already exist.
  However, `write_batch`, `Extent`, `StreamExtentPage::frame_lengths`,
  `resolve_extent`, and `PhysicalRead` all assume one physical frame per extent.
  Encoding, storage format, recovery, and reads must change together.
- **Common physical format — large, D01/A15.** The selected codec must serve
  Rust and C++, user-data writers/readers, native buffers, Stream, and tree
  chunk/page storage. The current `u16` payload size and 64 KiB limit prevent a
  single large append frame; the selected bounded groups retain that maximum
  and add Stream-only roles/completeness fields to the common format. The
  existing Rust/C++ CRC input ranges differ; define
  canonical bytes with shared golden vectors.
  User data keeps ordinary whole-object publication and streaming without a
  known total length. Do not add Stream grouping or whole-object buffering.
- **Combined write/sync RPC — medium, D02/A02.** DiskIO already has ordered
  writes and async flush. The missing work crosses `FBDiskWriteRequest`, wire
  constructors, client durability handling, server completion, `DiskWriter`,
  and both ordinary and replacement writes in `MirrorStripFlow`. Passing
  `Durability::Fsync` today still performs two requests. A new boolean alone is
  unsafe with an old server that ignores it and returns ordinary write success.
- **In-memory progress — medium, D03/D04/A03/A04.**
  `production_chunk::update_state` copies persisted
  `acknowledged_cursor` into `ChunkStateView::cursor`. With deferred checkpoints,
  a growth/repair/query refresh can rewind local synchronized progress. The next
  write and `seal` also check that same atomic cursor. Separate logical progress
  ownership from layout refresh before removing cursor commits.
- **Read availability and tail reconstruction — large, D06/A06/A07.**
  `recover_active_frames` is bounded by a supplied persisted cursor and issues
  a header read plus verified-frame read per frame. `ChunkReader` derives
  `durable_bytes` from `acknowledged_cursor`; `StripReader` returns
  `NotYetAvailable` beyond that bound. Fresh acknowledged data cannot simply
  bypass these limits through the existing reader. Verified-frame reads also
  query layout per call, so frame-by-frame recovery creates avoidable metadata
  traffic. A bounded scan/read contract and batched parsing are needed.
- **Finalization and ownership — large, D05/D09/A05/A10.** A content scanner
  already exists: `FinalizeChunkTaskHandler` calls `scan_complete_frame_boundary`,
  and `seal_chunk_bytes` can persist an exact final boundary. Reuse this work,
  but the scanner currently selects one available mirror and reads per frame;
  it does not implement the selected Q1 group boundary or Q2 rule. The legacy
  `seal_expired_writer_chunks` seals at the persisted cursor, while the task
  handler treats an already sealed chunk as complete. Deferred cursors must not
  let expiry finalize a stale length or delete data after a failed scan. Idle
  liveness checks do not implement the selected Group-0 lease protocol. The
  server already has serving grants, monotonic deadlines, and durable transfer
  release proofs to compose with Stream ownership.
- **Segmented directory — large, D07/D08/A08/A09.**
  `StreamManifest::extent_pages` is flat; page identity includes head epoch and
  generation. `publish_state` reconstructs all extents, and `open` loads every
  page and collects all extents. Revision reuse and eight-way writes can stay,
  but page identity, root/index, tail builder, lookup, and open must be replaced.
- **Retention and cache — large, D11/A12.** Reclamation deletes pages by
  generation, and `ExtentPageCache` copies a growing map and invalidates it by
  generation. Neither supports history shared across roots. Persisted root
  references, consumer pins, candidate-page protection, and bounded cache
  accounting must be explicit before shared pages become reclaimable.
- **Live views and WAL integration — large, D10/A11/A13.** `open_read_only`
  deliberately stops at the published manifest boundary. Chunk-KV checkpoints,
  split artifacts, replay, and reclamation carry Stream generation pins. With
  an older checkpoint and newer in-memory tail, implement the selected owner
  flush/checkpoint capture or explicitly lagging persisted fallback. Associate
  those pins with independently versioned roots; exact split/transfer cutovers
  cannot silently use an older fallback.
- **Observation and performance evidence — medium, D12/A14.** Existing phase
  histograms and MemDisk/mem-block fixtures are usable. `observe_metadata` is a
  synchronous slice of the flat fence list, so it needs bounded indexed
  pagination rather than eagerly loading history. Add counters for write RPCs,
  local publication, index reuse, root publications, recovery, and actual WAL
  latency. Historical 7.44-ms measurements predate the first optimization and
  are not a fresh implementation baseline.

The two relatively isolated first gains are combined write/sync RPCs and compact
location arithmetic. The largest dependent work is the directory/retention
model and replacing the cursor authority across write, read, recovery, and
ownership. Basic parsing exists; its boundary, replica, and IO behavior need
adaptation rather than a new transport or storage engine.

## Decisions and execution dependencies

- Q1 selects verifiable head/follower groups with a 64 KiB total frame maximum.
  One append is one validity unit and one extent. Magic retains purpose/version;
  the common header has ordinary/head/follower roles. Heads declare length/count,
  followers identify head offset and ordinal, and complete groups validate count,
  order, lengths, CRC, and chunk identity. Single-frame appends are count-one
  heads. No partial append is exposed and no commit frame/RPC is added. Rust/C++
  share bytes and CRC coverage; only Stream emits grouped roles. User-data/tree
  frames remain ordinary under the common codec. No design blocker is identified;
  implementation must verify native buffers, geometry, and cross-language parity.
- Q2 selects the shortest complete-append prefix among healthy available
  mirrors after owner exclusion, within the original IO budget. Failed or
  corrupt observations are not artificial short candidates; conflicts fail
  explicitly, absence of a completed healthy candidate yields unavailable, and
  no arbitration is awaited.
- Q3 selects Group-0 authority with conservative local lease deadlines and
  admission/ACK checks. Explicit drained, durable release permits immediate
  new-grant activation; an unreachable owner requires safe expiry and old-IO
  exclusion. Renewal is background work, not per-append KV.
- Q4 selects owner-ordered flush/checkpoint when reachable, otherwise an
  explicitly lagging consistent persisted prefix. Capture returns its bound
  and source; a newer exact cutover cannot use a stale fallback. This fallback
  is not an upper bound for writer failover recovery.
- Versioned directory decoding and synchronized-wire rollout must be explicit.
  Adding a FlatBuffers field does not itself establish old-server support.
  Current-format data survives reopen and faults; no old-format conversion is required.
- T01 and T05–T07 can precede the remaining ownership work.
  Common codec T08 and directory work T09–T13 follow format cleanup T04;
  T14–T19 establish the state, recovery, lifecycle, and view contracts.
  T20 integrates the new completion
  boundary only after these dependencies exist. T21–T26 provide consumer and
  production acceptance. No intermediate speed sample substitutes for them.

## Matched baseline evidence

- HEAD `fceee28e` code baseline; ten executable/library SHA256 identities in
  `r244-chunk-stream/baseline/binaries.json`. Service, test-util fixture and bench
  builds are outside measurement. Three logical native nodes use mem-block KV/
  WAL and retained MemDisk; original mirror counts, queue/batch geometry and
  regression gates are unchanged. All five deployment KVM guests are gracefully
  stopped before measurement, with disks preserved.
- Stream: all seven workloads complete with zero data errors/watchdogs. Six
  original performance checks fail, as expected for the design gap. 4 KiB/1T:
  147.47 ops/s, avg 6780 us, p99 7865 us; mirror avg 1222 us, cursor avg 2503 us,
  directory publication avg 2969 us, directory build avg 11.79 us. 4 KiB/32T:
  1841.59 ops/s, avg 17371 us, p99 36948 us. Large 1 MiB: 42.01 MiB/s;
  rollover 4 MiB: 97.88 MiB/s; random 4 KiB: 22101.41 ops/s; replay: 159.42
  MiB/s; GC: 11110.75 MiB/s. Complete counts/distributions/RSS/queues and fixture
  data/logs are in `baseline/stream/` and `baseline/stream.log`.
- Actual Chunk-KV: unchanged regression passes 30000 operations, 32 producers,
  512-byte values, 25% reads; 2836 ops/s, avg 11015 us, p99 32363 us, zero errors.
  Split/owner redistribution and retained replay pass. `baseline/chunk-kv/`
  contains server phase perf logs, configs, RSS and results. Both exact fixture
  shutdown receipts have `ok=true,error=null`. These are baseline results,
  not completion of the new append/ownership/format contract.

## Intermediate performance rerun

- At user request, reran the unchanged original regression script against
  `377870bb714a8129810eb138d01ad2b917249d91`. Services, native DiskIO, benchmark
  and fixture were rebuilt from that source before measurement. All seven
  original cases, durations, thresholds, mirror configuration and retained
  MemDisk/mem-block fixture settings remain unchanged. Container/CLI conversion
  is deferred at the user's request; this run uses the existing native fixture.
- Receipts are under
  `r244-chunk-stream/comparison-377870bb-20261023T090139Z/`: `intent.json`,
  eight binary identities in `binaries.json`, complete `run.log`, original
  `stream/results.tsv`, computed `comparison.tsv`/`comparison.json`, and
  count/avg/p50/p99/max in `phase-comparison.tsv`. The measured code baseline
  identity comes from the original binary manifest at `fceee28e`, whose changes
  from R243's final commit are documentation only.
- Throughput compared with baseline:
  - 4 KiB/1T: 147.47 -> 156.68 ops/s (+6.25%); average 6.780 -> 6.381 ms,
    p99 7.865 -> 7.461 ms.
  - 4 KiB/32T: 1841.59 -> 1894.28 ops/s (+2.86%); average 17.371 ->
    16.879 ms, p99 36.948 -> 39.568 ms (+7.09%).
  - 1 MiB/8T: 42.01 -> 46.74 MiB/s (+11.26%); average 188.807 ->
    169.752 ms, p99 214.455 -> 235.470 ms (+9.80%).
  - 4 MiB rollover: 97.88 -> 94.23 MiB/s (-3.73%); p99 344.329 -> 362.054 ms.
  - Random 4 KiB/32T: 22101.41 -> 22019.79 ops/s (-0.37%);
    p99 2.240 -> 2.196 ms.
  - Sequential replay of 320 MiB: 159.42 -> 159.29 MiB/s (-0.08%);
    five completed replays, p99 2039.324 -> 1998.867 ms. This measures sequential
    read replay, not cold failover recovery.
  - GC: one operation, 22.690 -> 21.543 ms. Its reported 11110.75 ->
    11704.42 MiB/s is a single-sample metadata/reclamation measurement.
- Every case completes with zero operational errors/watchdogs, but the script
  exits 1: all four append cases still violate the hot-path metadata publication
  bound, random reads remain below 30000 ops/s, and replay remains below
  300 MiB/s. GC passes. Do not report this run as a passed regression or completed
  performance acceptance. Queue and RSS growth checks pass. Fixture cleanup
  confirms `ok=true,error=null`; no test service remains running.
- The single-producer mirror stage improves from 1.222 to 0.747 ms on average.
  Cursor persistence remains 2.531 ms and Stream publication remains 3.009 ms,
  together about 87% of the 6.381-ms append latency. There are 1567 append
  batches and 1568 Stream publications: ordinary append still publishes once per
  batch. These measurements confirm that the principal latency bottleneck has
  not been removed. Grouped framing and growth-publication cleanup reduce
  amplification, but do not replace T18/T20's remaining runtime work.

## Synchronized write implementation evidence

- Request/response append-only schema slots, missing synchronization proof and flush
  failure values pass 16 protocol tests. The Rust and C++ schemas are generated
  by their ordinary build paths, with no generated-file edits.
- The semantic client propagates synchronization through contiguous and bounded
  view requests. Matching peers require one write request/reply; missing
  synchronization proof fails without a fallback request. Five actual TCP cases
  cover no early completion, missing-proof errors, buffered behavior, ordered view splitting and typed write/flush
  failures without extra requests.
- The actual C++ write handler retains disk/connection/payload through ordered
  write and flush. Nine new delayed/failure/lifetime/ordering tests pass; all
  165 DiskIO/build-tree CTests pass. Invalid control/payload/zone input performs
  no IO. Full tree-lint exits 0 (existing warnings remain); the two new C++
  files' focused lint has no diagnostics after ordinary style cleanup.
- Mirror and replacement paths now use synchronized seams. Metrics forwarding
  delegates those seams directly; it cannot reintroduce two requests. Two
  production adapter-chain TCP tests pass for three required replicas and a
  delayed replacement, with exactly one synchronized request per copy and no
  explicit fsync. The full two-client suite passes 141 tests.
- Final gates pass: full `test-cpp` has 709 native tests plus 85 Rust FFI/tool
  tests, zero ignored; receipt `unit/cpp-abe7f7e1cc1f42b2b83021beea62b0ba/`
  `results.json`. Three affected crates' all-target clippy, Access-server and
  E2E-test compilation, Rust fmt and changed C++ format pass. One ordinary
  clippy failure was redundant default updates in the new test allocator;
  removed them and repeated the focused test/gate successfully. Logs are
  `synchronized-{full-cpp,client-suite,rust-gate2,fmt-final,cpp-fmt}.log` under
  `r244-chunk-stream/`. The two-client suite includes purpose, mirror repair,
  reads and native-buffer forwarding. New bytes/recovery/lifecycle contracts
  still require the later production acceptance.
- This changes RPC synchronization only. Append still commits ChunkDB cursors
  and Stream directories until the ownership/framing/recovery dependencies are
  implemented; the six failing baseline performance gates remain unmet.

## Format execution notes (T04/T08 implementation target)

- Only canonical revision-2 framing is the implementation target. User-data/
  tree use ordinary roles; Stream uses verifiable complete append groups.
- Share CRC32C input, footer chunk-ID validation, checked role capacities and
  bounded parser memory in Rust/C++. Reject unsupported revisions and roles.
- No old-format conversion, rollout ordering or capability negotiation is needed.
  The predecessor Stream worker and its codec are removed with grouped wiring.
- Preserve current-format crash/reopen, corruption, full-append and cross-language
  tests. Keep ordinary user-data publication atomic for whole objects.

## Shared codec implementation evidence (partial T04/T08)

- `348f300e` introduced shared revision-2 roles/geometry, a bounded complete-group
  validator and independent Rust/C++ vectors. Header/payload CRC32C and separate
  expected chunk-ID validation agree across languages.
- Group encoding preserves Bytes payload owners, checks canonical full interior
  frames, association/ordinal/count/total length and constant-state validation.
- These foundations passed protocol/native gates recorded in `shared-frame-*`
  artifacts. Old-format support from that milestone is being deleted under T04.
  Current remaining group integration is not claimed complete.

## Object reader framing evidence (partial T08)

- Ordinary object reads validate one canonical revision-2 geometry from compact
  locations and actual verified headers. Bounded read windows and credits retain
  original payload owners, including frames crossing physical buffers.
- Current cleanup removes revision-1 geometry and fixtures. Focused tests retain
  exact cross-frame/subrange bytes, pointer ownership and CRC mirror failover.
- Stream last-chunk reads still need their separate authorized content-derived
  boundary; this does not widen arbitrary ordinary object reads.

## Ordinary producer execution notes (T08)

- Small/large/async/native user-data writers emit ordinary revision-2 frames.
  Native regions reserve exact role-dependent overhead and preserve owners.
- Tree packs use the same ordinary format. Compact references carry plain logical
  lengths with no format tag or revision byte. Manifest encode/decode accepts one
  current version. Checksums cover the complete current record.
- Streaming uploads retain whole-object publication and bounded memory; no Stream
  head/follower roles or whole-object buffering are added to ordinary producers.

## Ordinary producer implementation evidence (partial T08)

- All audited ordinary Rust producers now request revision 2, including both
  object writers, shared small batches, S3 native buffers/streaming and
  Access-server S3/Iceberg frame selection. Region offsets, capacities and
  exact-size owner accounting use the common V2 geometry. Native finalization
  checks revision before touching reserved regions and regenerates a prepared
  header if its requested ordinary purpose changes.
- Tree's new format-7 references carry explicit per-pack frame revision through
  the existing length field's reserved high bit; old format-6 references decode
  as V1. Checksums cover the revision tag. New ordinary BtreePage/PageIndex
  frames and current geometry are used by synchronous, asynchronous and
  ownership-materialization paths. Reused locations retain their revision.
  Extracted catalog encoding/checksums and verified pack reads into private
  owning modules, rather than extending the large store implementation.
- Three focused native cases pass for compact reference serialization, unknown
  or mislabeled manifest versions, actual retained V1 bytes/catalog reads and
  a subsequent V2 publication without altering the old bytes. Initial native
  build repairs were test target include propagation, checksum visibility during
  extraction, and a missed private-header declaration; no product test assertion
  has failed in this group. Rust all-target clippy for all four affected crates
  passes. The four-crate Rust run passes 1132 cases; the two existing external
  Python SDK cases remain explicitly ignored by the default command. Full native
  testing found two stale 34-byte/68-byte V1 expectations. Auditing that family
  also found the production Config default still used a 65,502-byte pack; change
  it to 65,501 so one default pack stays within one V2 frame. Added a dedicated
  default-boundary assertion. All six focused cases pass. The repeated full
  native/FFI gate passes 719 native tests and 85 Rust FFI/tool tests, zero ignored,
  and covers the corrected default through the Rust FFI. Receipt:
  `unit/cpp-91a82bf210354f3abb1c9d72b9a33b78/results.json`. Full tree-lint exits
  zero with existing warnings; the final focused private codec/read/revision and
  header lint has no diagnostics. Logs: `frame-consumer-tests2.log`,
  `frame-consumer-full-cpp2.log`, `frame-tree-focused-final.log`,
  `frame-consumer-tree-lint.log` and `frame-tree-lint-final.log`.
- This completes ordinary producer migration, not Stream group integration,
  indexed directories, owner leases or content-derived recovery. T08 remains
  unchecked until those consumers are wired and verified.

## Directory execution notes (T04/T09–T13 implementation target)

- Only independent immutable indexed records are supported. Keep fixed bounded
  heads, compact grouped append locations, SHA-addressed pages and path copying.
- Root publication protects candidates before page writes and resolves a guarded
  head CAS. Retention control revisions fence concurrent publication/pins/GC.
- No frozen old schema, source snapshot, migration marker or conversion cursor.
- Reader pins, sealed chunk spans, bounded root/page/data collection and restart
  cleanup still need implementation before ordinary append can omit checkpoints.

## Indexed record implementation evidence (partial T09)

- `5c9b4bcc` defines bounded heads, SHA-addressed independent leaf/branch records
  and checked scope, height, fences, counts, logical/physical continuity.
- T04 removes old-format discrimination: one grouped append location contains
  logical start and ChunkLocation, without a per-entry frame-format enum.
- Independent page/root/publication-guard/control namespaces survive owner and
  generation changes. No migration namespace or format dispatcher remains.
- Indexed storage foundations exist. The main worker, retention/GC and new append
  completion remain unfinished and must be integrated before acceptance.

## Incremental index implementation evidence (partial T10–T12)

- Added bounded immutable DirectoryIndex traversal and prepared mutations. Append
  loads the rightmost path, retains completed leaf/branch identities and encodes
  only changed tail paths/new segments. Trim copies the left path and keeps a
  location crossing the watermark whole. Preparation does not write KV state or
  authorize chunk release; roots remain independently readable.
- Lookup loads at most height + 1 records. Range scans bound both record IO and
  returned locations, with continuation offsets tied to the captured immutable
  root. A page budget below the root traversal depth fails before IO, preventing
  a continuation that cannot make progress. One checkpoint accepts at most 65536
  locations; excess work backpressures instead of materializing unlimited history.
- Production KV implements identity-addressed page reads. Every loaded record is
  verified for content identity, purpose and reference fences before traversal.
- Seven integration cases cover 1/16/256 historical segments, full/unfinished
  tail reuse, height growth, bounded paginated ranges, compact 4 MiB groups,
  complete grouped locations, cross-leaf continuity, trim and missing/corrupt records.
  All 70 Stream tests, all-target clippy and fmt pass. Receipts:
  `directory-index-tests.log`, `directory-index-full-stream.log`,
  `directory-index-clippy4.log`. These APIs are not yet the active append/read
  state machine. Guarded KV publication, root retention, bounded
  active-tail snapshots/cache and append completion remain unfinished.

## Guarded KV publication evidence (partial T11/T13)

- Added an identity/revision-bearing indexed-head snapshot and production KV
  publication of prepared DirectoryMutation values. Candidate scope, base root,
  monotonic generation/epoch/trim/tail and active original/checkpoint locations
  are validated before writes. New pages and immutable root records use
  create-if-absent CAS and exact-byte conflict/unknown-outcome reconciliation.
- A bounded durable guard contains the candidate, base head and at most 128
  new page identities. It precedes page storage. Head replacement and guard
  removal are one conditional KV batch; failures retain the old head and guard.
  The per-stream retention-control revision advances atomically on guard creation
  and head replacement, fencing GC against both new guards and same-ID orphan
  resurrection. Existing and future pin/retirement/cleanup transitions must
  compose with this same revision; a plain check followed by deletion is unsafe.
- Fixed a concurrent first-publication observation: a control read can precede
  another publisher's atomic control/guard creation. A subsequently found guard
  with that older absent-control observation returns backpressure, not corruption.
  Concurrent identical candidates across 16 fresh stream scopes exercise this.
- Two protocol and seven real-KV cases pass, including unknown record shapes,
  scope/identity/allocation bounds, retained roots, page conflicts/protected retry,
  stale owner CAS, active checkpoint disagreement, unknown-schema rejection and
  both GC revision races. Full Protocol/Stream suites pass 349 cases, zero
  ignored; all-target clippy and fmt pass. Logs:
  `directory-publication-{protocol,tests,clippy4,full-suite2,fmt2}.log`.
- The predecessor Stream worker is not switched yet:
  root pins/GC, owner authority, content recovery and the new finite-view/ACK
  contract must be wired together. No performance acceptance is claimed here.

## Independent page cache evidence (partial T12)

- KV directory reads now use a fixed 64-slot, directly mapped ArcSwap cache.
  Page IDs remain reusable across root epochs/generations; no historical map is
  cloned or retained. Cold insert verifies the bounded record's SHA identity and
  copies only its bytes, avoiding retention of a larger transport allocation.
  Collisions replace one slot; there is no new lock or unsafe scope.
- DirectoryCacheSnapshot exposes hits, misses, replacements and resident
  pages/bytes/slot limit. Resident page bytes are bounded by 64 * 64 KiB,
  independently of retained history; caller-owned in-flight buffers follow their
  existing admission bounds. Cache residency does not replace root/data pins.
- Three additional real-KV cases verify reuse across publication generations,
  512 historical pages under the fixed slot/byte bound, and refusal to cache
  corrupt/oversized records. Full Protocol/Stream suites pass 352 cases, zero
  ignored; all-target clippy and fmt pass. Logs:
  `directory-cache-{clippy4,full-suite,fmt}.log`. The predecessor worker remains until the indexed state machine is wired.

## Shared metadata backend evidence (T09–T12 wiring foundation)

- `9e911373` shares one conditional-record state machine between KV and the
  deterministic Memory backend. The memory adapter uses its existing test lock;
  no production lock or new unsafe scope was added.
- The factory binds metadata group and scope. Both backends use the same immutable
  page writes, publication guard/control ordering and revision-fenced head CAS.
- Retained roots and failed-publication protection tests remain. Compatibility
  conversion tests and their source/marker/progress machinery were removed.
- The main worker still needs to consume this factory; custom deterministic
  wrappers must forward it when that integration replaces the predecessor API.

## Local synchronized progress evidence (partial T14)

- ProductionStreamChunkStore now tracks persisted and synchronized physical
  cursors separately. Stream V2 chunks admit subsequent grouped writes from
  synchronized progress; layout/query/repair refreshes only advance, never rewind,
  that local position. Growth descriptors preserve it. Liveness renewal continues
  to use the persisted checkpoint and does not silently publish a newer cursor.
- The first grouped write consumes a locally fresh allocation token; a cold
  adapter cannot overwrite a prior process's group even when its persisted
  cursor is zero. A failed first attempt cannot turn an unknown old range into
  a fresh allocation. Grouped chunks reject legacy-frame writes. Legacy V1/raw
  test seams retain their cursor-authority behavior until grouped wiring.
- Only successful synchronized completion of all strip writes promotes local
  progress. Explicit seal accepts the synchronized grouped boundary and persists
  it through the existing chunk-layer seal. MemoryStreamStore models the same
  grouped progress/seal contract under its existing test-only mutex. The mock
  production allocator's seal response now matches actual ChunkDB by returning
  the exact persisted seal_bytes cursor.
- Three new production-adapter cases cover stale checkpoint queries, renewal,
  growth, replica repair/full images, further append and final seal, mixed
  revisions and cold overwrite refusal. One memory case covers multiple groups
  with zero cursor advances before explicit seal. Focused 13 cases and clippy
  pass. All 91 Stream cases and fmt pass, zero ignored. Logs:
  `local-progress-{clippy4,tests3,full-stream,fmt}.log`.
- The existing worker still encodes V1 and waits for cursor/directory publication.
  Removing growth-triggered Stream publication and using these local positions
  in the grouped worker remain wiring tasks. No lease, recovery, finite-view or
  performance acceptance is claimed by this adapter preparation.

## Current capacity isolation evidence (partial T14)

- StreamChunkStore reports current chunk-layer capacity. Production uses its
  authoritative layout cache and Memory reports backing capacity. This is now
  required of test adapters too; no missing-query publication branch remains.
  Growth never creates a Stream head/directory publication solely for capacity.
- The predecessor writer/read-only open refresh capacity from the chunk layer after its
  durable query rather than rejecting newer valid storage against a cached
  manifest capacity. The read-only view still stops at its captured root; it
  never combines a newer physical cursor with an older logical directory.
- Layout cache replacement now compares modify_ts through ArcSwap RCU. Delayed
  old replies cannot replace a newer layout; cursor/checkpoint counters and the
  sealed flag do not move backward. This adds no lock. The allocator mock's
  seal timestamp/cursor now match actual ChunkDB transitions.
- Two growth cases prove unchanged attachment/append publication counts without
  growth-only writes, and a persisted 1.5 MiB legacy cursor with a still-1 MiB
  manifest hint. Cold read-only capture retains EOF zero; writer reopen recovers
  exact retained bytes. Production-adapter coverage injects an older layout
  reply after growth and verifies capacity/progress remain current. Focused
  14 cases and clippy pass. All 92 Stream cases and fmt pass, zero ignored. Logs:
  `capacity-isolation-{clippy3,tests,full-stream,fmt}.log`.
- The seal boundary correction below rejects stale shortening. T16 must still
  align expiry/finalization: a grouped owner's synchronized position can exceed
  its persisted cursor. Do not enable deferred append completion until owner
  exclusion and content-derived finalization protect that progress.

## Persisted cursor seal boundary evidence (partial T16)

- Raw Stream/Wal sealing rejects an exact byte boundary below an already
  persisted acknowledged cursor before touching reservations, strips, task state
  or cleanup. Larger synchronized boundaries remain legal. The check runs under
  existing lifecycle sequencing; no lock or unsafe scope was added.
- Two actual pinned KV/DiskDB/ChunkDB lifecycle cases cover both Stream and Wal,
  rejection at byte 16 after cursor 17 with complete state/version equality,
  exact-byte seal, and sealing a newer boundary from an older zero checkpoint.
  Full ChunkDB tests, all-target clippy and fmt pass; counts/logs are retained in
  `stream-seal-{tests,clippy2,full-chunkdb,fmt}.log`.
- This prevents regression of a persisted cursor only. Expiry/finalizer still
  require allocation format authority, grouped content scans and owner/old-IO
  exclusion before deferred-cursor ACK can be enabled. In particular zero in
  old metadata cannot prove grouped data absence.

## Unpublished-format cleanup execution

- User confirms no released old format/service must remain supported. Removed
  frozen Chunk bodies and reservation fallbacks, all Stream source/marker/
  progress/conversion modules and keys, and their dedicated fixtures. The
  compact indexed extent now has no per-entry format enum.
- Chunk metadata accepts only its explicit current envelope. The journal test
  now uses the actual current encoder; its CAS/flush/reopen assertions remain.
- Indexed head decode returns the current bounded head directly. No migration
  head dispatcher or legacy-to-indexed path remains. Raw Stream metadata decode
  was removed; the predecessor worker's own metadata API still exists only
  until the indexed worker replaces it.
- Missing synchronized-write completion proof returns a durability error with
  no fallback fsync or repeated write. Removed nested fallback accounting.
- Ordinary object reads have one revision-2 geometry. C++ tree manifests accept
  only the current version, references carry plain logical lengths, and reads/
  checksums no longer branch on per-reference frame revisions or old manifests.
- The shared revision-1 frame codec and old Stream worker remain to be removed
  together with grouped recovery/finalizer and indexed-worker wiring; they are
  unfinished implementation, not an old-format support requirement. No claim of
  completed T04/T08 or new append/performance acceptance is made here.
- Initial affected suite failed in `journal_handoff_test` because its fixture
  bypassed the production metadata encoder and wrote a raw body. Fixed that
  fixture at its creation point, without restoring any compatibility decoder.
  The focused 20 cases and all 652 cases across Protocol, Stream, ChunkDB,
  DiskIO-client and Chunk-client pass with zero ignored tests. Strict all-target
  clippy and Rust/C++ format pass. Full tree-lint exits zero; changed-scope lint
  is clean after removing redundant parentheses (the unrelated existing
  make_shared warning in the pipeline remains). Full `test-cpp` passes 719
  native cases and 85 Rust FFI/tool cases with zero ignored tests; receipt
  `unit/cpp-e2ca7816ffc74dc18ef850d7f4d540f0/results.json`. Complete logs are
  `no-compat-{focused2,full-suite,clippy2,tree-lint,tree-lint-focused,full-cpp,
  fmt-final,cpp-fmt}.log` under `r244-chunk-stream/`.

## Canonical ordinary-frame cleanup

- Removed ordinary revision-1 magic variants in Rust/C++, both retained CRC
  golden vectors and the alternate footer-inclusive CRC acceptance. The one
  supported CRC covers header/group fields/payload; expected chunk identity is
  separately verified. C++ now has only revision 2, without old-revision header
  helpers. Current ordinary encoders/readers and tree packs use revision 2. Negative tests reject removed magic values and noncanonical CRC.
- Native-body tests check wrong-purpose rejection before touching framing bytes.
  The actual ChunkDB finalization test now writes current PageIndex frames.
  These consumer tests plus frame tests pass 33 cases, zero ignored.
- Product E2E small/large/repair/read fixtures still used old magic, the 65,502-
  byte payload cap or 14-byte ordinary headers. Updated them to current 65,501-
  byte payloads and 15-byte headers while preserving physical bytes, mirror
  repair and subrange assertions. Object location arithmetic uses current
  ordinary geometry; predecessor Stream accounting is private to that worker. Compile them through all-target
  clippy. Actual OCI E2E needs the final matched current image; do not run an old
  deployment image against the new strict client and call that acceptance.
- The predecessor Rust Stream worker still emits revision 1; remove that last
  Stream codec alongside grouped recovery/finalizer and indexed-worker wiring.
  This is current implementation being replaced, with no old-format read or
  migration commitment. The final shared codec supports only revision 2.

- Final canonical-frame gates pass: all 481 Protocol/Stream/Chunk-client tests,
  33 focused framing/native-body/finalizer cases, and two real TCP mirror-chain
  cases; zero ignored. All-target clippy also compiles affected Access, ChunkDB,
  Stream and product E2E consumers. Full `test-cpp` passes 720 native cases and
  85 Rust FFI/tool cases, zero ignored; receipt
  `unit/cpp-d0bbea06f8ee4685b2684386f3ef71da/results.json`. Full tree-lint exits
  zero with existing warnings and no diagnostics in the changed codec/tests.
  Rust/C++ format pass. Logs: `canonical-frame-{full-suite,consumers,mirror-chain,
  clippy2,tree-lint,full-cpp,fmt,cpp-fmt}.log`. Actual product OCI E2E remains T26.

## Bounded retention inventory foundation (partial T13)

- `DirectoryRecords::scan_keys` reads only bounded keys/revisions through KV's
  bounded scan. Typed root/page/publication inventory pages enforce a maximum of
  128 keys, strict ordering/scope/shape, exclusive continuation and explicit
  incomplete zero-progress errors. No record bodies are loaded for enumeration.
- Memory uses its existing test lock and an ordered metadata map, selecting one
  bounded lexicographic range without sorting or cloning all metadata. Production
  gets no new lock or unsafe scope.
- Two actual shared-backend tests pass: five root generations/pages paginate
  exactly, scopes/groups do not leak, scans do not publish heads, invalid limits
  fail, a malformed 128 KiB body is not decoded by key enumeration, and a damaged
  key fails closed. Full Stream/clippy/fmt gates pass with the atomic-root tests
  below.
- This is an inventory primitive, not completed collection. Root retirement,
  chunk spans, reachability and control-revision-conditional deletion still need
  implementation. Enumeration never authorizes deletion on its own.

## Atomic retained-root publication (partial T11/T13)

- Retained root records are now written in the same head-revision CAS batch as
  the stable head, retention control and guard removal. Page writes remain
  protected and precede that batch. A rejected head CAS cannot leave a root
  record that looks published. No separate publication receipt is needed.
- Existing root bytes are checked for conflicts before the batch. Competing
  normal publishers cannot overwrite a retained root: they consume the same
  expected head revision, so only one batch can win.
- Deterministic and actual KV cases pass: injected failure/revision conflict
  leaves pages guarded but no new root; retry keeps page identities and commits
  head/root with identical bytes and the same KV revision. Existing earlier
  roots survive. All nine indexed KV/cache tests also pass.
- This establishes a published-root identity for later pins/retirement, not
  completed pin/GC/consumer integration. Full Stream passes 82 tests, zero ignored;
  all-target clippy and Rust fmt pass. Logs are
  `directory-{root-atomic-tests,retention-full-stream,retention-clippy,
  retention-fmt}.log` under `r244-chunk-stream/`.

## Exact root pins (partial T13/T19)

- Current-format pin records contain a full bounded directory head and fixed
  needed/captured interval, not an old-generation conversion or compatibility
  descriptor. Keys use tag `0x0019`; records are at most 1 KiB with 512 identities
  per stream. Key scans remain bounded to 128 entries. No new production lock.
- New identities require an exact retained published root. Existing identical
  pins and live reader renewal survive retirement of the root-index row;
  retained page roots remain in the pin descriptor. Durable consumers have no
  expiry; requested reader leases are at most 60 seconds and cannot renew after
  expiry. Stale snapshots cannot release or renew superseding pin revisions,
  including identity reuse. Lifecycle writes cannot recreate missing control.
- Pin creation/renewal/release mutate retention control in the same CAS batch.
  Control is captured before root validation/inventory, so concurrent root
  retirement or new protection invalidates a stale cleanup CAS. Missing roots,
  expired pins, conflicting identities and full budgets fail explicitly.
- Three protocol and six shared Memory/actual-KV tests pass, including root
  retirement, rejected stale cleanup, exact old EOF, revision/ABA, expiry,
  bounded admission and missing-control fencing. An initial retirement fixture
  incorrectly built a successor from an empty root; it now extends its actual
  base. Full Protocol/Stream: 361 passed, zero ignored; strict all-target clippy
  and Rust fmt pass. Logs: `root-pin-{tests2,full-suite,clippy}.log`.
- This is metadata state, not a read IO lease or completed reclamation. T19 must
  enforce local conservative deadlines and background renewal. T13 must add
  expiry/read-IO grace, retirement policy, sealed spans, page/data reachability
  and bounded conditional cleanup before enabling deletion.

## Bounded root-index retirement (partial T13)

- `retire_directory_roots_before` examines at most 128 oldest retained root
  identities and atomically deletes only those below the supplied generation
  horizon. The horizon cannot exceed the observed head. It captures control
  before revalidating the exact head and conditionally mutates control with the
  deletions. Conflicts retry; unknown outcomes reconcile absence.
- Current head and exact pins survive; new identities cannot acquire retired
  roots. This removes no pages or physical bytes. Repeating bounded passes
  continues from the remaining oldest keys and stops idempotently at the
  horizon. No migration, tombstone or compatibility representation is added.
- Shared Memory/actual-KV tests retire four of five roots in two-record passes,
  reject stale heads and invalid budgets/horizons, keep the current head and
  all pages unchanged, and read the exact pinned old view afterward. Actual KV
  leased-reader renewal after retirement now uses this API as well.
- Full Stream: 90 tests, zero ignored; focused pin tests, strict all-target
  clippy and Rust fmt pass. Logs: `root-retirement-{tests,full-stream,pin-tests,
  clippy-final,fmt}.log`. Main worker/consumer wiring and page/data collection
  remain unfinished.

## Required current adapters (partial T04/T14)

- `StreamChunkStore::current_capacity` is required and returns a capacity, with
  no unsupported/`None` branch. Open reads current chunk geometry directly.
  Growth updates local capacity without a legacy-store publication fallback.
- Indexed directory binding and liveness renewal are required adapter methods.
  Test wrappers explicitly forward both capabilities to their real backend;
  missing implementations cannot silently compile with an unsupported binding
  or successful no-op renewal.
- Existing successor-growth/stale-capacity, local synchronized-progress,
  publication ordering, liveness, reads and reopen tests cover the current
  contract. Full Stream passes 88 tests, zero ignored. Strict all-target clippy
  and Rust fmt gate this cleanup; logs are `adapter-cleanup-{tests,clippy,fmt}.log`.

## Bounded directory-page collection (partial T13)

- An opaque bounded proof cursor captures retention control before validating
  the exact current head and independently addressed candidate bytes. One
  continuation allocation is reused across passes; no historical mark set or
  full directory is materialized. Each pass observes at most 128 protections.
- Current and retained roots, every exact pin, and every publication guard are
  checked. Root fences select one path with at most directory-height page IO;
  guards protect explicit new pages and the complete base subtree even when
  the candidate root is not readable or its base root-index row has retired.
  Expired pins remain conservative protection pending safe expiry cleanup.
- Control changes invalidate partial proofs. Only a complete proof permits
  deletion in a control-revision CAS that also advances control. Uncertain
  writes reconcile absence; malformed/missing proof records fail closed. The
  bounded local cache evicts confirmed deletions. Restart simply begins a fresh
  proof against current protection, without trusting incomplete old marks.
- Six Memory/actual-KV tests cover shared and changed pages, retired roots with
  exact pins, ten retained generations with one-observation continuation,
  pin/publication invalidation, unpublished pages/base subtrees, invalid budgets
  and corrupt candidates. Current reads/head remain unchanged. A protocol test
  verifies ID-only record decoding derives checked fences and rejects unknown
  envelopes, trailing bytes and wrong scope. This is the current format only.
- Full Protocol/Stream: 370 passed, zero ignored; final six focused tests,
  strict all-target clippy and Rust fmt pass. Lint repairs moved a fixture test
  into its existing target, boxed the large continuation once, and replaced two
  redundant closures. Logs: `page-collection-{tests-final,full-suite,
  clippy-final,fmt}.log`. Whole-chunk spans/data collection, expiry grace,
  abandoned-guard cleanup and runtime wiring remain unfinished.

## Resolved publication-guard retirement (partial T13)

- Added bounded typed guard loading and exclusive-continuation retirement. A
  guard at/below the exact revalidated stable head generation is resolved or
  superseded: strictly increasing head generations prevent its original CAS
  from winning again. A guard newer than the head remains protected. No timeout
  or old-format fallback is used as authority.
- Cleanup captures control before head/guard observations, validates records,
  and atomically advances control with eligible deletes. It reuses the root
  retirement conditional-delete/reconciliation boundary and does not delete
  pages. A guard with an excluded owner but still-next generation needs the
  later ownership proof; this path does not guess exclusion from elapsed time.
- Two additional tests cover five guards in single-record pages: four resolved
  or losing candidates retire, the live successor remains, and its retry still
  publishes correctly. The old unshared base subtree becomes collectable only
  after retirement. Actual KV malformed guards block both retirement and page
  deletion. All 98 Stream tests, strict all-target clippy and Rust fmt pass.
  Logs: `guard-retirement-{tests,full-stream,clippy,fmt}.log`.

## Exact sealed chunk spans (partial T13)

- One immutable current-format span records chunk identity/purpose, original
  logical base, complete final logical end and physical end. The independent
  key uses tag `0x001a`; bounded key-only scans enumerate spans without decoding
  bodies. Fixed record bounds reject wrong scope/geometry and unknown envelopes.
- Ending an active attachment now requires exactly one matching span in the
  prepared mutation. It must match its original attachment, successor base,
  prior checkpoint and complete final append. A same-chunk checkpoint cannot
  invent a sealed span. The stable head/root, span, control and guard removal
  commit in one CAS; conflicting immutable span bytes fail.
- Protocol and Memory/actual-KV tests cover invalid/missing/partial spans, failed
  head publication with no span residue, retry, equal head/span KV revisions,
  cold loading, empty chunks and partial trim retaining the original full span.
  Full Protocol/Stream: 379 passed, zero ignored; strict all-target clippy and
  Rust fmt pass. Logs: `sealed-span-{tests-final,full-suite-final,clippy-final,
  fmt}.log`. Whole-chunk reachability/release and main-worker wiring remain.

## Predecessor whole-chunk trim correction (partial T13)

- The existing trim loop released whole chunks one extent at a time. Two focused
  cases failed before the fix: trimming one completed frame of a sealed chunk
  with a retained suffix, and trimming one record while its chunk remained
  active. Both incorrectly released 100,068 bytes instead of zero.
- Cleanup now groups contiguous extents by chunk, checks the complete final
  logical end and excludes the current active chunk. One release removes the
  whole eligible group. The byte budget stops between whole chunks, while one
  eligible chunk can exceed that budget to ensure progress. Trim still publishes
  its logical watermark before any release.
- All three regression cases pass after the correction, including later append
  to the trimmed active chunk, partial suffix reads, rollover, whole final release
  and a one-byte GC budget. Full Stream: 106 passed, zero ignored; strict
  all-target clippy and Rust fmt pass. Logs:
  `whole-chunk-trim-{before,after,full-stream,clippy,fmt}.log`.
- This fixes the currently running worker. It does not claim new indexed
  root/pin retention or the no-KV append completion boundary is wired.

## Whole-chunk retention proof and retryable release (partial T13)

- Page and sealed-chunk collection share the bounded protection walker and an
  opaque `DirectoryCollection` continuation; the unpublished page-only type/API
  was replaced without forwarding aliases. Each pass observes at most 128
  records, with no growing history set or new production lock.
- Complete span end must not exceed current trim. Current/retained heads protect
  visible intervals plus their whole active chunk, pins protect fixed captured
  intervals, and guards protect both candidate/base data. Partial overlap keeps
  the whole chunk. Expired pins remain conservative until safe expiry cleanup.
- A successful proof CAS advances control and retickets the unchanged span by
  its KV row revision. The span itself remains durable work until physical
  release succeeds; no extra tombstone/job namespace is needed. A newer ticket
  invalidates the old one before IO. Completion removes the span under a fresh
  control fence; interrupted physical or metadata completion can restart/reprove.
- `release_trimmed` now requires the exact physical end in every adapter. The
  production adapter queries current ChunkDB state, rejects active/mismatched
  seals and treats deleted/absent IDs idempotently. Memory enforces the same
  seal boundary. The predecessor worker supplies its checked whole-chunk end.
- Five additional tests with real grouped payloads cover current trim, retained
  roots, one-byte exact pins, late empty views, interruption before/after data
  release, idempotent retry, rejected active physical chunks and superseded
  tickets. Shared actual-KV/Memory cases keep the active successor untouched.
  Production-adapter tests also reject active/short boundaries and repeat a
  completed release as zero bytes. Full Stream: 111 passed, zero ignored; final
  ten span tests, strict all-target clippy and Rust fmt pass. Logs:
  `chunk-collection-{check,tests,full-stream,tests-final,clippy-final,fmt}.log`.
- T13 still needs the runtime driver/budgets and consumer wiring, safe automatic
  pin expiry/read-IO grace and ownership-proven cleanup of next-generation
  abandoned candidates. Move to T15/T16 recovery now; do not enable no-KV append
  ACKs before ownership, finalization and finite-view dependencies are complete.

## Remaining bounded-publication wiring

- New indexed retention still requires runtime whole-span collection and
  safe expiry integration. Preserve the predecessor whole-chunk trim
  regression cases when replacing that worker.
- An active 256 MiB chunk can hold more than 65,536 tiny append locations.
  `MAX_DIRECTORY_MUTATION_EXTENTS` and the 128-page publication guard cannot be
  raised to accommodate a whole chunk in one allocation. During rollover,
  stage bounded path-copy batches under durable candidate guards; retain each
  staged root until the final bounded head CAS publishes the complete root.
  Intermediate staging must not publish stable heads or immutable roots under
  the final epoch/generation identity. Ordinary append still performs no KV
  work when optional checkpoints are off, and rollover performs one head CAS.
- Staging restart/retry and orphan cleanup must use retention-control revisions
  and ownership fences. This is new-format publication construction, not an old
  directory migration. Add tiny-payload history coverage beyond one mutation
  budget and crash between stages before marking indexed-worker wiring done.
- All new sealed chunks have an exact original logical base in their attachment.
  Persist exact sealed chunk spans for whole-chunk retention. No unknown-base
  compatibility fallback or old-consumer generation conversion is needed.

## Bounded complete-prefix parser (partial T15)

- `FramePrefixScanner` accepts only selected revision-2 ordinary/grouped bytes,
  keeps one at-most-64-KiB frame plus constant validator/hash state, accepts at
  most 1 MiB per push and emits at most 1,024 complete extents per batch. Stream
  emits one location per complete append; ordinary frames retain their mode.
- Only a complete group advances physical/logical progress and its SHA-256
  prefix digest. Observed bytes/parsed frames are separate counters. Missing
  followers cannot expose interior frames or skip holes. Exact-capacity end
  needs no next-header input. Nonzero start checkpoints preserve original bases.
- Wrong CRC/identity/role/revision, oversized declarations, arithmetic errors,
  short observations and interior grouped checkpoints fail explicitly. A full
  header overrunning capacity is invalid, not a shorter healthy candidate. Any
  parser/observation error poisons the scanner; finishing it later cannot revive
  an apparently healthy prefix. Incomplete data never contaminates the digest.
- Twelve focused cases pass, covering byte-sized fragments, frame/window splits,
  3,000 tiny groups with bounded batches, incomplete capacity suffix, ordinary
  mode, required checkpoints, limits and all error classes. Full Protocol: 288
  passed, zero ignored; strict all-target clippy and Rust fmt pass. The only lint
  repair replaced newer Option helpers with MSRV-supported expressions. Logs:
  `frame-prefix-{tests-final,full-protocol,clippy-final,fmt}.log`.
- This is shared parsing, not completed mirror observation/finalizer/runtime
  integration. Continue with bounded physical-window IO and Q2 comparison.

## Shared physical-window observation (partial T15)

- Added a chunk-client physical-window seam with exact-length reads, at most
  1 MiB windows, scanner-bounded output and explicit caller deadlines. Existing
  ordinary object read bounds are unchanged. No IO failure becomes zero-filled
  data or an empty candidate. Owner exclusion and stable layout authority remain
  constructor/caller prerequisites for the later backend adapter.
- Up to five configured replica observations run concurrently within the same absolute
  deadline. Failed, short, corrupt and timed-out observations are excluded whole.
  The shortest healthy complete prefix wins; equal prefixes compare their full
  digest, longer replicas are reobserved only through that selected boundary.
  Conflicting valid bytes fail explicitly, with no arbitration operation.
- Bounded mapping replay uses the selected prefix and its original deadline,
  checks its digest/identity/end again, and fails changed data or sink errors.
  Partial sink work is staging only and cannot publish a stable root. It does
  not retain a full mapping vector per replica. Success exposes reads/bytes and
  scanner progress for later recovery metrics.
- Nine shared-IO cases pass: unequal valid ends, corruption and mid-read failure,
  equal/unequal conflicting contents, no healthy source, short read, one shared
  concurrent timeout, changed replay, sink deadline/error, bounded 3,000-location
  replay and no extra exact-capacity read. Complete Client/Stream tests, strict
  all-target clippy and Rust fmt pass; logs:
  `frame-observation-{check,tests,full-suite,clippy-final,fmt}.log`.
- This is the shared observation/replay engine. Current-layout physical readers,
  and finalizer primitives now exist. Owner exclusion and main-worker recovery
  remain unfinished; no production performance result is claimed.

## Pinned mirrored physical layout (partial T15)

- `MirroredFrameReaders` owns one immutable current Chunk snapshot and explicit
  local deadline, validates Stream/Wal purpose/owner/epoch/state and contiguous
  allocated geometry, and constructs at most five replica readers, matching the
  current adapter's supported configuration. Allocation
  identity/capacity and independent replica disk identities must be present;
  missing current fields are rejected without compatibility fallback.
- Reads use binary strip lookup and split bounded windows at actual strip
  boundaries. A contiguous result retains its IO buffer; cross-strip assembly
  stays within one window. The adapter makes no allocator query per frame/window
  and never widens ordinary ChunkReader reads. Only this explicit excluded-owner
  tail context can inspect allocation beyond an older checkpoint.
- Every subread enforces the captured monotonic deadline, including in-flight
  timeout and clones retained after expiry. Known unavailable copies and short
  physical reads fail instead of producing synthetic bytes. Layout captures do
  not mutate replica health or adopt writer state.
- Six physical-adapter cases use real grouped bytes through the DiskWriter
  seam: 100 KiB across frames/13 strips with checkpoint zero, pinned snapshots,
  unavailable replicas, scope/short-read rejection, expired clones/in-flight IO
  and invalid owner/layout/allocation identity before IO. The five-copy boundary
  is also exercised. All 15 observation cases pass. Full Client/Stream: 259 passed,
  zero ignored; strict all-target
  clippy and Rust fmt pass. Checked integer conversions fixed test-fixture lint.
  Logs: `mirrored-frame-{check,tests,full-suite,clippy-final,fmt}.log`.
- Runtime owner exclusion, zero-header emission/initialization, sealed-EC tail
  adaptation and finalizer/main-worker wiring still remain. Do not activate
  deferred ACKs yet. Preserve Q4 read-only capture semantics separately.

## Grouped main-worker integration (partial T04/T08/T10)

- Main Stream append now encodes one verifiable head/follower group per request
  and creates one location for its entire payload. Large append framing and
  capacity accounting use checked grouped geometry. Logical reads resolve only
  intersecting complete frames, verify their head/follower association against
  the published location and retain Bytes slices after validation.
- Removed the last Stream revision-1 magic, revision dispatch, alternate header
  helpers, raw/legacy Memory mode and Production write fallback. Production
  accepts only grouped heads on this owner's fresh allocation; all local writes
  thereafter use synchronized progress. Cold adapters cannot resume an old chunk.
  Attributed Stream identity is checked before data IO and checkpoint mutation.
- Cursor metadata absence no longer proves synchronized data absent. While the
  checkpoint remains on the current path, retry that metadata operation only;
  never resubmit the already synchronized payload to a successor. Removed the
  misleading append-absence result/error and corresponding WAL mapping.
- Checkpoint-bounded recovery now uses the shared scanner in at-most-1-MiB
  windows and emits complete groups, not individually valid followers. An
  older checkpoint cannot authorize shortening locally known synchronized data:
  the conservative failure preserves its bytes and layout until excluded-owner
  content recovery is integrated. This is not A06 completion.
- Focused Stream tests cover mixed sizes through 4 MiB, one location per append,
  exact physical cursors, head/middle/tail and cross-frame bytes, and physical
  reads proportional to intersecting frames. Unsupported old magic is rejected
  by both contiguous/view protocol parsers. Current rollover/repair/read/trim
  fixtures use actual grouped bytes and canonical capacities.
- Remaining work: the main worker keeps the flat directory and waits for
  per-batch cursor/head metadata. Root-index/tail wiring,
  owner leases, content recovery, finite-view pins and performance acceptance
  are not implied by the grouped-format switch.
- Verification: 855 tests across 137 Protocol, Chunk-client, Stream, Chunk-KV,
  Chunk-KV-server and ChunkDB targets pass, zero failed/ignored. All-target
  clippy for those six packages and workspace fmt pass. Receipts:
  `grouped-worker-{full,clippy-final,fmt}.log`. Initial test failures retained
  in `grouped-worker-{initial,suite}.log` identify stale fixture geometry and
  the now-rejected attempt to discard synchronized bytes using an older cursor.

## Initialized unwritten suffix and immutable retries (partial T15)

- Fresh allocation validates segment ownership/geometry and zeroes each entire
  1 MiB strip, synchronized on every required mirror before attachment. Growth
  initializes only its newly allocated strips. A separate local initialized
  capacity prevents metadata refresh from authorizing an uninitialized range.
- Initialization is a single semantic write attempt. An uncertain result cannot
  open that capacity or trigger another initialization. Stream replacement writes
  its retained prefix and zero suffix once before publishing the replacement;
  uncertain initialization does not immediately discard/reuse that segment.
- Ordinary append images contain only complete groups. No per-append zero marker
  or rewrite permission remains in MirrorShadow. This is necessary because an
  earlier request can execute after a successful retry and a later append. The
  reserved 14-byte unwritten header below the hard limit remains naturally zero
  inside initialized capacity. Exact hard-capacity ends need no header.
- The prior `unwritten-tail-*` receipt passed 858 tests but did not cover delayed
  duplicates. Its per-append marker implementation was unsafe and is superseded;
  it is not evidence for retry safety. New cases replay actual captured earlier
  append requests after two successful appends and check both retained payloads.
  Recycled-media, failed initialization, uncertain growth, strip-edge, growth and
  exact-capacity cases exercise the new contract.
- DiskIO retries retain the original creation time and reject an expired operation
  before submission. Three new native regressions first failed on the original
  partial-block path: delayed duplicate after cache reset, two uncached boundaries,
  and cached head with an uncached tail. Reads now preserve each missing boundary;
  failed/short boundary reads submit no write. All twelve alignment cases pass.
- Matched protocol cleanup removes the retired allocation slot and response code;
  current schema fields are contiguous. Malformed synchronization proof still
  fails directly, with no fallback request or capability negotiation.
- The remaining KiB-only ChunkDB seal request and server fallback are removed;
  see the exact-byte sealing evidence below. Coarse chunk geometry remains
  distinct from the exact effective data boundary.
- Verification: 875 tests across 144 targets in Protocol, DiskIO-client,
  Chunk-client, Stream, ChunkDB, Chunk-KV and Chunk-KV-server pass with zero
  failures/ignored. Full native tests pass (724 C++ cases plus 85 Rust FFI cases).
  All-target clippy, changed-file tree-lint and format gates pass. The final
  focused rerun follows a payload-submission helper extraction and checked size
  conversions. Receipts: `initialized-suffix-{full-two,native-full,clippy-final-two,
  native-lint-final,final-focused,fmt-final}.log` and
  `append-range-native-{red,green}.log`.
- Indexed worker wiring, excluded-owner cold recovery, IO drains and lease
  integration remain. Timing defaults alone do not prove that an already submitted
  kernel write has finished; ordered completion/flush barriers are required before
  observing an excluded owner or reusing its allocation.

## Ordered segment drain and queued request expiry (partial T15/T16/T18)

- Immutable-range and strict matched-wire cleanup is committed at `2fb1ff81`.
- Two new native regressions first failed: byte-addressed writes bypassed the
  segment queue, and an empty ordered request issued a second media write instead
  of waiting as a barrier. All writes now retain their segment queue position
  until backend completion; a zero-size request completes without media IO.
- The server rejects missing, stale, overflowing and unbounded-future timestamps.
  It samples monotonic time before wall time, derives the fixed conservative
  request deadline, and rechecks it at dequeue and after boundary reads. Queued
  expiry returns `OldRequest`, not synchronization proof. Existing kernel writes
  retain their active queue position until completion regardless of elapsed age.
- Admission age/skew cannot exceed the shared protocol ceilings (30 seconds and
  one second). The previous parser fixture used 40 seconds/two seconds despite
  the finalizer assuming 30 seconds/one second. Startup now rejects that unsafe
  mismatch; shorter configured windows remain supported. The constants name their
  upper-bound role and keep no obsolete aliases. Owner expiry still must compose
  conservative clock bounds with ordered drains, not a fixed sleep alone.
- DiskioClient has an explicit drain operation sharing the existing synchronized
  wire contract. Empty data writes still fail. DiskWriter routing and metrics
  forward the same segment identity, absolute deadline and completion proof.
  No independent-fsync fallback, capability field or ordinary-append extra RPC
  is introduced.
- Focused native cases cover byte-write ordering, empty barriers, expired queued
  requests, expiration during boundary reads, stale/future admission and a fresh
  drain after a blocked write. Rust wire tests distinguish empty drains from
  invalid empty data writes; the real routing/metrics seam waits for drain proof.
- The finalizer now invokes this primitive before tail observation, as recorded
  below. This does not yet provide caller-proven owner exclusion. Integrate it
  with owner/late-request exclusion and with every
  affected physical release before reuse. Sealing currently retires excess strips
  and finalization can delete an empty chunk; those paths also require safe drain
  or deferred reclamation. A healthy-prefix scan cannot authorize freeing an
  unavailable replica's allocation. Resolve these together with T16/T18/T19.
- Verification: 878 Rust tests across 145 targets pass with zero failures or
  ignored cases; native tests pass (732 C++ cases plus 85 Rust FFI cases).
  The gated drain/empty-data test passes after its final response-lifetime update.
  All-target clippy, changed-file tree-lint and Rust/C++ format gates pass.
  Receipts: `ordered-drain-{full,native-full,final-focused,clippy,native-lint-final,
  fmt}.log`; the original two failing cases remain in `ordered-drain-red.log`.

## Finalizer drains and current metadata shape (partial T04/T15/T16)

- A consumed reservation without a planned cursor previously returned Reconciled.
  The regression now requires an explicit format error with the durable record
  and blocks untouched. Removed the legacy branch rather than reclaiming it.
  Required placement policy/repair fields and reservation planned vectors no
  longer deserialize through old missing-field defaults. Two protocol regressions
  first failed on omitted policy/planned-cursor fields; optional values remain
  optional under the current schema.
- The finalizer drains every captured mirror/EC segment with bounded concurrency
  before any content scan, including ordinary empty chunks. Missing synchronization
  proof retries with no scan, seal or free. Drain has at most 15 seconds inside
  the original 30-second drain/scan budget; scan/comparison do not reset it.
- Sealing an observed prefix compares the complete captured metadata while holding
  the existing lifecycle guard. Changed revision/layout means retry. Empty tails
  use that same seal fence before deletion; a crash between seal and deletion
  resumes without accessing physical identities already retired. Deleted cleanup
  can also resume through the idempotent lifecycle path.
- The TCP regression first returned Complete without any drain proof. It now
  checks empty S3 and nonempty Wal retention, zero reads on proof failure, then
  successful recovery when proof returns. Snapshot conflict and seal/deletion
  restart cases cover the additional lifecycle fence. Verification passes: 900
  Rust cases across 149 targets, eleven-package all-target clippy and formatting.
  A final 29-case seal rerun passes after extracting reservation cancellation;
  the full suite includes all twelve finalizer cases. Receipts use
  `finalizer-drain-*`, `reservation-cursor-red.log` and `metadata-shape-red.log`.
- This is partial integration. Group-0 process/session continuity, captured IO
  deadlines and all other physical release paths still need T18/T19 wiring.
  The finalizer conservatively retries if any allocation lacks drain proof;
  completing healthy-prefix recovery while deferring failed-copy reclamation
  requires that wiring. Do not infer producer exclusion from elapsed time alone.

## Exact-byte ChunkDB sealing (partial T04/T08)

- A native RPC regression first failed: sealing four bytes sent zero in the exact
  byte field and only a rounded KiB count. The legacy server branch interpreted
  that as 1024 bytes. Native seal responses also lacked exact-boundary validation.
- Removed the request's KiB field from the current FBS and typed Rust schema,
  the zero-byte legacy branch and the LifecycleHandler KiB wrapper. Zero always
  means an exact empty seal. The serde form requires an explicit byte count.
  No retired slot, alternate request form or forwarding alias remains.
- Rust ordinary/Stream writers, routing, CLI and native tree RPC now send exact
  bytes. Existing byte cursors and coarse physical geometry stay distinct.
  Test allocators preserve exact bytes rather than rounding their read bounds.
- Native sealing validates returned chunk identity, owner, exact cursor and sealed
  state before caching success. A sealed empty chunk with zero capacity legitimately
  has no strips; an active/nonempty chunk without a layout remains malformed.
- Focused tests cover canonical wire zero/4/1025-byte counts, explicit JSON bytes,
  exact empty lifecycle state, native empty/partial seals and inconsistent replies.
  Verification passes: 892 Rust tests across 148 targets in eight core packages,
  the Iceberg cross-listener recovery case, CLI all-target check, and 734 C++ plus
  85 Rust FFI cases. All-target clippy in ten packages, changed-file tree-lint
  and format checks pass. The final small-object rerun passes all 40 cases.
  Receipts use `exact-seal-*`, including the original `exact-seal-native-red.log`
  failure. Main indexed worker/lease wiring remains.

## Single fenced strip replacement interface (partial T04)

- Audited `UpdateChunkStrip`: production only forwarded the old request to
  `replace_chunk_strip_range`; the sole real caller was E2E repair fault injection.
  Removed its typed/FBS messages, message IDs, response wrapper, dispatch,
  lifecycle wrapper, client methods, allocator trait forwarding and test stubs.
- Deleted the empty former tonic service module and its deprecated type, removed
  obsolete mixed-rollout comments and the transport-wide dead-code allowance,
  and deleted the unused error adapter exposed by that allowance.
- Repair injection now supplies the exact old range, expected revision and stable
  operation identity through the canonical replacement RPC. Replacement,
  conversion and relocation metrics use the current `layout_mutation` family.
- Updated the RPC design to describe mandatory current transport and fenced
  mutations, removing obsolete optional-transport and fixed-request-count claims.
  Neighboring designs and the pending recovery requirement use the same API.
- Verification: 895 Rust tests across 148 targets pass. All-target clippy passes
  in eleven packages, including Access-server, CLI and E2E. Four E2E seal fixtures
  now initialize the complete exact-byte request directly. Full native verification
  passes (734 C++ cases and 85 Rust FFI cases); format checks pass. Receipts use
  `strip-wrapper-*`. The indexed worker still replaces
  the old flat directory together with its runtime wiring.

## Immutable view and bounded repair-image wiring (partial T14/T20/T22)

- Removed the duplicate whole-batch byte buffer. Frame header/payload/footer
  owners now reach MirrorShadow as immutable views, and only bounded strip
  repair images assemble their bytes. The data view is a slice of that same
  repair image. Single-buffer append admission keeps its Bytes owner; multiple
  caller buffers normalize once. There is no per-frame payload allocation.
- Replaced the redundant global-data-plus-images API with StreamMirrorWrite:
  physical start, complete data end and ordered strip images. Production checks
  complete coverage and image ranges before acknowledging; the current data
  boundary is exact. Production and Memory share this contract; test fixtures
  stage actual encoded bytes into valid strip images.
- The repair shadow retains only the strip containing the next write boundary,
  rather than a whole active tail. Its explicit complete boundary also prevents
  a writer from rewinding within retained zero-valued payload bytes.
- Physical append accounting now uses submitted image lengths and the backend's
  required mirror count, replacing the fixed multiplier of three. A production
  test compares it with bytes retained on all its mirror disks; the compact
  append test checks canonical physical bytes with its one-media seam.
  Repair/failed-attempt traffic still needs its separate T22 counters.
- Stream full targets, all-target clippy and workspace fmt pass. Receipts:
  `view-images-{full,clippy,fmt}.log`. This changes byte preparation and IO
  ownership; it does not remove per-batch cursor or root publication.

## Single production Stream write path cleanup (partial T04/T14)

- Removed the unused raw-byte `MirrorChunkWriter` and its public exports. Its
  append/grow/seal path had no production callers; ProductionStreamChunkStore
  only borrowed allocation from it. Allocation now directly requests the typed
  Stream/Wal chunk and validates fresh identity, owner, epoch, state, zero
  checkpoint and initial geometry before installing the local writer token.
  Physical Stream constants belong to the chunk facade. There is no forwarding
  alias or alternative unframed Stream write path.
- The removed writer also accepted two copies for a three/five-copy request.
  A new regression reproduced Production allocation returning success with that
  incomplete set. Fresh allocation now requires every configured copy. Existing
  write/repair admission retains the same requirement after attachment.
- Removed three tests of the obsolete raw writer; production grouped-byte
  tests cover actual writes, growth, synchronized repair and exact release.
  A stale audit link now points to current typed allocation and preserves the
  system-Wal/business-Stream separation.
- All 260 Client/Stream tests across 40 targets pass, zero failed/ignored;
  affected all-target clippy and workspace fmt pass. Receipts:
  `direct-allocation-{before,full,clippy,fmt}.log`.

## Finalizer shared-prefix integration (partial T16)

- Removed the unused table-scanning expiry API and its cursor-only seal path,
  plus its now-unused reservation cleanup helper. Durable liveness tasks are
  the only expiry finalizer. Exact seal still performs its reservation cleanup
  and strip closure under the existing lifecycle serialization.
- The finalizer now uses shared complete-prefix observation, at-most-1-MiB
  physical windows across strips, concurrent mirror candidates, shortest healthy
  complete group selection and overlap comparison within one 30-second budget.
  Stream/Wal inspect allocated bytes beyond an old checkpoint; ordinary objects
  validate their already persisted publication cursor. Required checkpoints
  remain a lower bound. A task for another writer generation cannot mutate data.
- Removed the test-util branch fabricating zero reads when ConversionDiskIo
  has no connected client. Missing IO is now an explicit error in every build.
  A zero Stream checkpoint with failed IO cannot authorize deletion.
- Real current-format bytes over the component TCP DiskIO seam cover Stream
  and Wal, an incomplete 180-KiB append after a complete 100-KiB append, unequal
  valid tails, corrupt first replica, conflicting valid contents, no connected
  IO, an interior-group checkpoint and a stale task generation. Ordinary tree
  frame finalization remains covered. Full ChunkDB targets pass; focused and
  lint receipts are `finalizer-{integration,full,clippy-final,fmt}.log`.
- This does not claim the main worker or new owner recovery is integrated.
  Owner authority/drain and last referenced sealed-chunk recovery remain
  part of T15–T19; fresh-strip initialization is covered above.

## Required mirror count evidence (partial T14)

- A regression reproduced a three-copy configuration returning `Ok(())` after
  synchronizing only two copies. The old admission range accepted any count
  between two and the configured count. The adapter now requires the exact
  configured count both before IO and after mirror repair, before promoting
  synchronized progress. Missing protection returns unavailable without changing
  replica health or persisting a cursor.
- Three- and five-copy regressions reject a two-copy layout before any write.
  All 13 production-adapter cases pass, including retained strip repair, growth,
  checkpoint refresh and the production runtime. Full Client/Stream targets,
  including synchronized TCP-chain coverage, clippy and fmt pass. Receipts:
  `required-copies-{before,after,full,clippy,fmt}.log`.

## Recovery IO execution notes (T15/T16 target)

- Shared parsing uses only current revision-2 ordinary/grouped frames. Separate
  tentative frame progress from committed complete-append progress, keep one
  frame buffer and bounded output batches, reject corrupt/failed observations,
  and hash only the complete encoded prefix for cross-replica comparison.
- A full nonzero header whose declared frame exceeds allocated capacity is an
  invalid observation, not a healthy short prefix. A genuinely partial header at
  exact capacity can retain the prior complete boundary. Short IO before the
  allocated bound is unavailable. Never synthesize zero bytes on IO failure.
- Observe replica prefixes within one original IO budget. Compare exact complete
  encoded prefixes; longer observations may need a bounded second pass up to
  the selected shortest boundary. Rebuild mappings in bounded batches from the
  chosen validated source rather than retaining a full location vector per copy.
- `select_frame_prefix` reserves half the original remaining budget for initial
  observations and ends comparisons at three quarters, retaining the last quarter
  for mapping replay. No phase resets or extends the original deadline. A paused
  regression first failed with a comparison timeout after short/long healthy
  mirrors and a hung third mirror; it now completes comparison and mapping replay.
  Failed/timed-out comparison sources are excluded and reported; valid conflicting
  overlap remains fatal. Expired selection submits no physical reads. Focused
  coverage passes (18 cases). All 441 tests across 69 targets in Chunk-client,
  Stream and ChunkDB pass, as do all-target clippy and format checks. Receipts:
  `recovery-budget-{red,focused,full,clippy}.log`.
- Before enabling deferred completion, establish an authoritative unwritten
  suffix on recycled units. Initialize each fresh or newly grown strip once on
  every required mirror before admitting its first data. Replacement images
  include the retained prefix and a zero suffix with no automatic retries.
  An uncertain initialization cannot be repeated, published or immediately
  reclaimed. Finish the independent old-IO drain/reuse proof before runtime GC.
- Ordinary writes cover only complete groups. They contain no trailing zero
  marker, so a delayed immutable duplicate does not overlap the next append.
  Block alignment reads and preserves all missing partial boundary blocks.
  Initialization and repair bytes remain separate from append data accounting.
- Capacity preparation reserves 14 unwritten bytes beyond the next complete end
  while below the hard chunk limit. A crash after metadata growth but before
  initializing new strips must still encounter a full zero header inside the
  previous initialized capacity. Exact hard-capacity ends need no marker.
- StreamMirrorWrite image coverage equals its complete end. MirrorShadow retains
  only the unfinished strip prefix and grants no tail truncation/rewrite exception.
  Do not interpret stale/corrupt bytes or IO errors as EOF. Ordinary object
  producers retain persisted cursor authority; Stream/Wal use complete groups.

## Captured serving-context implementation (partial T18)

- The first regression proved `clear()` discarded sequence history and allowed
  reinstalling the identical old grant. A second blocked actual Stream-backed
  WAL IO, cleared authority and installed a replacement grant; the old request
  incorrectly returned success with a journal position.
- `ServingAuthority` now retains sequence high-water state across clear and tracks
  continuity per exact assignment. Continuous live renewal preserves a captured
  context; gaps, assignment removal and clear never revive one. Installation and
  request checks use one owner monotonic clock, including time spent paused while
  installing a grant. Captured IO deadlines do not extend with renewal.
- Normal point execution captures authority before WAL/tree work and validates
  it before successful acknowledgment. Revocation returns LeaseExpired and no
  journal position; the gate test checks durable data is retained and a stable-ID
  retry succeeds. Catching-up targets keep their explicit transfer-artifact path.
  Existing grants remain the authority; no per-request KV operation or lock is
  added. Fixture setup moved into `tests/common/serving_lease.rs`.
- Verification passes: 905 Rust cases across 149 targets, eleven-package
  all-target clippy and formatting. Two context regressions, continuous renewal
  and the blocked WAL acknowledgment case also pass after retaining grants by
  immutable Arc ownership. Red/green receipts use `lease-continuity-*`.
  Process-boot binding, Stream
  admission/IO context and independent renewal remain before deferred completion.

## Grouped request authority implementation (partial T18)

- After point-context fencing, a blocked batch regression still reproduced a
  resurrection: its first member returned LeaseExpired after clear/replacement,
  but its second member captured the new grant and appended successfully.
- Multi-get and batch mutation now capture all resolved serving-assignment
  contexts before IO and retain them across their complete execution. Every
  member checks the captured set against one immutable grant snapshot before
  admission and before success. Revocation prevents later WAL admission while
  preserving durable earlier data and stable-ID retry. A separate paused batch
  verifies continuous renewal still permits both writes.
- Elapsed group time advances the original wall/monotonic deadline; member
  execution does not restart its budget. Catching-up targets retain explicit
  transfer-artifact validation. This adds no metadata operation or lock.
- Group execution moved out of the large server root into `server/group.rs`
  and `server/group/authority.rs`; token capture/validation lives under
  `serving/lease/context.rs`. Test request/gate helpers stay in tests/common.
- Verification passes: 907 Rust cases across 149 targets, eleven-package
  all-target clippy and formatting. The original failure is `group-lease-red.log`;
  focused and full evidence use `group-lease-*` receipts.

## Process-lease metadata implementation (partial T18)

- Added one fixed Group-0 `StreamProcessLeaseKey` per purpose/owner instance.
  Its current JSON record carries a boot nonce, monotonically increasing process
  epoch, expiry, prior exclusion boundary and retained WAL grant fences. Stream
  and Wal owners have separate keys; records reject unknown/missing fields and
  impossible successor/grant state. Ordinary expiry never deletes the epoch fence.
- `StreamProcessLeaseClient` captures one predecessor and reuses one boot nonce
  through CAS reconciliation. A competing/replaced process cannot recapture a
  new predecessor implicitly. Expired own claims and renewals fail; a new boot
  advances the epoch and retains the latest prior process/grant expiry. Renewal
  preserves grant high-water fields changed by the control plane.
- Six focused cases pass using a real pinned Group-0 KV dependency and current
  protocol records: idempotence, simultaneous claimants, stale boots, expiry,
  grant retention, key-purpose isolation and malformed metadata. Protocol tests,
  two-package all-target clippy and fmt pass in `process-lease-*` receipts.
- Local projection and conditional boot/heartbeat/grant wiring are implemented
  below. Startup exclusion and Stream IO binding remain. A claim alone is not an old-IO drain
  or permission to adopt predecessor data. Normal WAL authority still requires
  the existing exact serving grant; preparation IO uses the process scope.

## Local process-authority implementation (partial T18)

- A lock-free local projection captures the fixed process identity and an
  absolute monotonic IO deadline. Admission and completion checks read local
  state without Group-0 requests. Continuous renewal preserves ownership but
  cannot extend an already admitted IO deadline.
- Expiry, a renewal gap, explicit fencing and confirmed replacement cannot be
  undone in place. Out-of-order older observations cannot shorten a newer live
  lease; malformed observations do not revoke accepted authority.
- Seven local clock/context tests and five Group-0 lease tests pass, including
  a confirmed replacement invalidating an admitted local context. Strict
  protocol/client all-target clippy and workspace fmt pass. Receipts:
  `process-authority-final-focused.log`, `process-authority-clippy-final.log`,
  and `process-authority-fmt.log` under the requirement artifact directory.
- Production boot/renewal and grant publication now use it as described below.
  Stream IO still needs the captured deadline; this does not complete T18.

## Process-bound service authority implementation (partial T18)

- Chunk-KV claims one fixed WAL process lease before initial partition recovery.
  A dedicated task renews it independently of recovery, observation sampling and
  request IO. Expiry/replacement permanently fences it and wakes service shutdown.
  Health exposes the accepted record, remaining duration and effective fence.
- Serving grants and service observations require the process epoch. A boot
  rejects another boot's grant; capture/completion checks include process scope.
  The explicit catching-up transfer path also checks process scope around point
  execution. Idle readiness cannot outlive the process lease.
- Monitor publication atomically updates the grant and retained grant sequence/
  greatest expiry under the process-record revision. Process replacement and
  renewal use the same key, preventing a concurrent grant extension from being
  lost during replacement. A stale epoch's observation receives no new grant.
- Heartbeat and withdrawal condition on exact boot identity and the same lease
  revision. The unconditional Chunk-KV registry wrappers are removed; withdrawal
  preserves the process fence and cannot delete a successor's observation.
- Required process/load fields have no missing-field defaults. The former old
  registration compatibility test now checks the complete current record and
  rejects omissions. Current fixtures explicitly carry their boot epoch.
- Four affected crates pass 592 tests across 106 targets, with no ignored cases.
  `process-boot-tests-final.log` records the full run. The initial full run found
  one idle-owner fixture still using a zero default process epoch; fixing that
  fixture preserved the strict production contract.
- Strict all-target clippy passes for protocol, KV client/server, Chunk-KV
  server, Web and E2E; workspace fmt passes. Receipts:
  `process-boot-clippy-complete.log` and `process-boot-fmt.log`. The small
  let/else and renewal-loop lint refactors pass the final 36-case focused flow
  rerun in `process-boot-final-flow-tests.log`.
- Startup predecessor exclusion, absolute IO-deadline propagation, background
  preparation IO and ordered physical drain/release remain. This does not yet
  complete the writer-ownership acceptance or enable ordinary append without KV.

## Current split metadata cleanup (partial T04)

- Removed the replacement-parent split variant and its extra retained-parent
  overlay from protocol records, readiness construction and state reduction.
  Every split phase retains the original parent tree/WAL identity; only the
  child carries the range-bounded overlay used by the current design.
- Split/readiness JSON rejects unknown fields, including the removed overlay;
  the planned timestamp is required. Tests exercise current-record roundtrip,
  omitted timestamps, retired fields and replaced parent identity. Fixtures now
  use the same current parent identity instead of manufacturing old records.
- Recovery tests retain exact mismatch/phase checks. Independent plain recovery
  applies to the retained parent; overlay-proof checks apply to the child.
  The initial focused failure was an old fixture expecting both halves to have
  overlays, not a production failure. Its setup/assertions now model the actual
  current artifacts and preserve the mismatch checks.
- The 31 split-filter cases pass in `split-current-format-focused-2.log`.
  All four affected crates pass 592 cases across 106 targets with no ignored
  cases in `split-current-format-tests.log`. Six-package strict all-target
  clippy and workspace fmt pass in `split-current-format-clippy.log` and
  `split-current-format-fmt.log`.

## Ownership execution notes (T18/T19 target)

- Grants and conditional heartbeat now bind one fixed per-boot process lease.
  A new boot must respect the prior grant's safe exclusion boundary
  before observing the abandoned tail; a cached instance ID cannot prove that.
- Captured assignment continuity, sequence high-water fences and the fixed
  process identity now fence admitted work across clear, expiry and replacement.
- Point execution and grouped RPCs now revalidate captured contexts around their
  asynchronous work. Bind
  Stream IO to the minimum captured Group-0/liveness absolute deadline; checking
  only before creating an async future permits a paused producer to resume with
  a fresh request timestamp. Renewal must run independently of blocked data IO.
- Keep one process worker per Stream owner session. The production runtime
  currently opens new handles without a shared worker registry; independent
  same-epoch workers must not acknowledge competing tails once per-batch KV
  fencing is removed. Wire this together with the indexed worker and finite-view
  flush path, then remove the flat predecessor.
- Ordered drains prove completion only after future producers are excluded.
  Apply that ordering to cold recovery, excess-strip release, uncertain
  initialization/replacement, consumed reservations and whole-chunk GC. A missing
  drain proof preserves durable cleanup work and physical allocation.

## Phase 0 — Establish evidence and settle contracts

- [x] **T01 — Capture a matched implementation baseline**: retain release
  identities, exact three-node fixture parameters, mirror count, checkpoints,
  queue/batch geometry, RSS, phase distributions, head/page counts, errors, and
  cleanup receipts for Stream and actual Chunk-KV. Use retained MemDisk and
  mem-block; do not compare against historical NullDisk figures as if matched.
  Reuse existing metrics before adding new ones. Maps to D12/A14.
  Files: `tools/benchmark/bench-chunk-stream-regression.sh`,
  `tools/benchmark/bench-chunk-kv-regression.sh`,
  `lib/crowdb-chunk-stream/benches/production_bounds/`.
- [x] **T02 — Record Q1/Q2 and framing failure assertions**: R244 and the design
  select verifiable bounded frame groups, whole-append validity, one extent per
  group, and the shortest healthy available complete-append prefix. A07/A15/A16
  require missing/reordered/truncated followers, bad CRC/identity, interrupted
  large appends at every boundary, failed observations, and conflicting mirrors
  to preserve the previous complete-append end or return an explicit error.
  Codec, recovery, fault tests, and performance remain implementation tasks;
  this check records decisions only. Files:
  `doc/backlog/R244-chunk-stream-incremental-directory-publication.md`,
  `doc/design/chunkds/design-crowdb-chunk-stream.md`.
- [x] **T03 — Record Q3/Q4 and capture/expiry ordering**: the design and R244
  select Group-0 lease/release/expiry, local admission/ACK checks, old-IO
  exclusion, owner flush/checkpoint capture, and explicitly lagging fallback.
  A10/A11/A13 record their assertions. Implementation remains in T18/T19/T21;
  this check does not claim tests or code are complete. Files:
  `doc/backlog/R244-chunk-stream-incremental-directory-publication.md`,
  `doc/design/chunkds/design-crowdb-chunk-stream.md`.
- [ ] **T04 — Remove unpublished format/service compatibility**: the user
  confirms no product release or supported persisted old format exists. Remove
  old raw Chunk/Stream decoders, frozen schema copies, directory conversion,
  migration guards/progress/source keys, old-server sync fallback and revision-1
  reader/tree support. Keep one canonical frame format and current directory
  schema; malformed/unknown records and missing sync proof fail explicitly.
  Wire the grouped/indexed worker and delete its predecessor together. Retain
  new-format reopen/fault tests and unchanged performance gates. No format
  negotiation or per-chunk capability fields are needed for matched services.
  Files: protocol frame/metadata codecs, Stream directory/worker, object readers,
  DiskIO semantic writes, native tree catalog/frame codecs and affected tests.

## Phase 1 — Combine write and synchronization

- [x] **T05 — Add the synchronized wire contract**: define the sync option in
  the current compact schema; update contiguous and view wire constructors
  and protocol tests. Exercise both generated Rust and C++ schemas through
  their existing generators; do not hand-edit generated files. Apply T04's
  compatibility rule. Maps to D02/A02. Files:
  `lib/crowdb-protocol/src/fbs/diskio.fbs`,
  `lib/crowdb-protocol/build.rs`, `lib/crowdb-rpc/CMakeLists.txt`,
  `lib/crowdb-diskio-client/src/client.rs`,
  `lib/crowdb-protocol/tests/diskio_wire_test.rs` (new).
- [x] **T06 — Complete write/flush through one server response**: make
  `DiskioServer::handle_write` await ordered write and, when requested, flush
  before its sole reply. Retain buffer/disk/connection lifetimes through both
  callbacks. Short/failed writes and failed flushes cannot produce success or
  double completion. Keep buffered behavior and independent fsync. Maps to
  D02/A02. Files: `app/crowdb-diskio/src/rpc/dio_server.cpp`,
  `app/crowdb-diskio/src/rpc/dio_server.h`,
  `app/crowdb-diskio/tests/dio_server_write_test.cpp` (new),
  `app/crowdb-diskio/CMakeLists.txt`.
- [x] **T07 — Propagate synchronization through mirror writes and repair**:
  replace the client write-reply/fsync-reply sequence for supported peers.
  Preserve bounded view splitting and same-strip ordering; flush coverage must
  include preceding fragments before append completion. Extend the `DiskWriter`
  seam and update ordinary and replacement writes in `MirrorStripFlow`, not
  just the nonfailure path. Audit every `DiskWriter` implementation, especially
  `MetricsDiskWriter`, so an adapter cannot drop the sync option or restore the
  second RPC. Update affected test seams if trait signatures change. Assert one
  synchronized RPC per replica through the production adapter chain for an
  unsplit single-strip write. Maps to D02/A02/A03. Files:
  `lib/crowdb-diskio-client/src/semantic.rs`,
  `lib/crowdb-diskio-client/src/semantic/write_views.rs`,
  `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`,
  `lib/crowdb-chunk-client/src/disk_io/routing.rs`,
  `lib/crowdb-chunk-client/src/chunk/mirror_flow.rs`,
  `lib/crowdb-chunk-client/src/chunk/mirror_flow/repair.rs`,
  `lib/crowdb-diskio-client/tests/synchronized_write_test.rs` (new),
  `lib/crowdb-chunk-client/tests/mirror_strip_writer_test.rs`,
  `lib/crowdb-chunk-client/tests/common/mod.rs`, and affected `DiskWriter`
  test implementations found by repository search.

## Phase 2 — Unify framing and build compact directories

- [ ] **T08 — Implement the selected common frame revision**: update the Rust
  and C++ physical codecs together, with shared golden vectors, identical CRC
  coverage, strict current-format handling, checked sizes, and geometry helpers.
  Implement T02's Stream head/follower roles, head length/count, follower head
  offset/ordinal, and group validation; preserve count-one heads. CRC covers
  header/group fields/payload, with separate expected chunk-ID validation.
  User-data and tree consumers emit ordinary frames; retain whole-object
  publication and unknown-length streaming. Preserve native receive-buffer
  ownership, incremental CRC, bounded IO, and scatter/gather;
  do not require an allocation of the entire large append. Update every common
  frame consumer and native payload capacity. Tree scope is its common chunk
  codec, not unrelated internal tree-node layouts. Maps to D01/A01/A07/A15.
  Files: `lib/crowdb-protocol/src/frame.rs`,
  `lib/crowdb-protocol/cpp/include/crowdb-protocol/frame.h`,
  `lib/crowdb-protocol/tests/frame_test.rs`,
  `lib/crowdb-chunk-client/src/writer/large_object.rs`,
  `lib/crowdb-chunk-client/src/writer/large_async_object.rs`,
  `lib/crowdb-chunk-client/src/writer/small_pipeline/batch.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-access-s3/src/native_buffer.rs`,
  `lib/crowdb-access-s3/src/native_buffer/allocator.rs`,
  `lib/crowdb-access-s3/src/streaming.rs`,
  `lib/crowdb-access-iceberg/src/file/blocks.rs`,
  `lib/crowdb-tree/src/backend/chunk/chunk_pack_pipeline.cpp`,
  `lib/crowdb-tree/src/backend/chunk/chunk_page_store.cpp`, and their existing
  frame/native-buffer/object/tree test suites.

- [ ] **T09 — Introduce versioned directory records**: replace the flat head
  fence list with a bounded root and independent page references. Represent
  multi-frame extents with logical intervals and `ChunkLocation` geometry;
  keep page identity independent of owner epoch/head generation. Implement
  T04's single-format rules and purpose/identity checks. Maps to D01/D07,
  A01/A08/A09. Files: `lib/crowdb-protocol/src/chunk_stream.rs`,
  `lib/crowdb-protocol/src/key/chunk_stream.rs`,
  `lib/crowdb-protocol/tests/frame_test.rs`,
  `lib/crowdb-protocol/tests/stream_directory_test.rs` (new).
- [ ] **T10 — Build bounded tail locations**: replace one `Extent` per frame
  and the worker's whole-history builder with tail pages and necessary paths.
  Reuse canonical framing arithmetic, retain short-frame boundaries, and never
  clone the full active/history list to publish an in-memory append view. Maps
  to D01/D07, A01/A08. Files: `lib/crowdb-chunk-stream/src/metadata.rs`,
  `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/stream/append.rs`,
  `lib/crowdb-chunk-stream/src/directory.rs` (new),
  `lib/crowdb-chunk-stream/tests/metadata_test.rs`.
- [ ] **T11 — Publish only changed directory paths**: replace
  `build_extent_pages` over all retained extents with an incremental publication
  batch. Persist new pages before root CAS, retain the prior root on failure,
  reuse CAS revisions, and reconcile unknown outcomes using the same candidate
  identities. Protect in-flight candidate pages. A large seal may flush several
  new segments, without retagging completed history. Maps to D07/D08/A08/A09.
  Files: `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/kv.rs`,
  `lib/crowdb-chunk-stream/src/kv/pages.rs`,
  `lib/crowdb-chunk-stream/src/stream/publication.rs`,
  `lib/crowdb-chunk-stream/tests/kv_metadata_test.rs`.
- [ ] **T12 — Load and read the index lazily**: remove eager
  `load_extent_pages`/`collect_extents` from open. Traverse only required paths,
  cache pages by independent identity within an explicit budget, and expand
  compact locations into bounded verified frame ranges. Batch adjacent reads
  and reuse a validated layout view rather than querying metadata per frame.
  Maps to D01/D07/D10, A01/A08/A11. Files:
  `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/stream/read.rs`,
  `lib/crowdb-chunk-stream/src/extent_cache.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-stream/tests/read_publication_test.rs`.
- [ ] **T13 — Reclaim by roots and whole-chunk retention**: associate consumer
  generation horizons with retained root references; protect current, pinned,
  and in-flight roots/pages. Replace generation-only page deletion. Trim cannot
  free a chunk while another extent or consumer still needs it. Bound cleanup
  scans/deletes and make restart retryable. Maps to D11/A12. Files:
  `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/kv.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/memory.rs`,
  `lib/crowdb-chunk-stream/tests/stream_test.rs`,
  `lib/crowdb-chunk-stream/tests/kv_metadata_test.rs`.

## Phase 3 — Establish content-derived progress before changing completion

- [ ] **T14 — Separate synchronized progress from `chunkinfo` refresh**: give
  worker/local write progress an explicit owner independent of the latest
  persisted cursor. Growth, repair, query, and liveness refresh must preserve
  synchronized local progress and repair images. Remove Stream publication
  caused solely by internal layout growth. Keep other chunk consumers' cursor
  APIs intact where they retain their own contract. Maps to D03/D04/A03/A04.
  Files: `lib/crowdb-chunk-stream/src/production_chunk.rs`,
  `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/stream/capacity.rs`,
  `lib/crowdb-chunk-stream/src/memory.rs`,
  `lib/crowdb-chunk-stream/tests/production_chunk_test.rs`,
  `lib/crowdb-chunk-stream/tests/rollover_growth_test.rs`.
- [ ] **T15 — Implement bounded tail observation and scanning**: establish a
  chunk-layer read mode/bound that can inspect the last allocated chunk beyond
  an older checkpoint without globally widening ordinary object reads. Read
  bounded windows, parse multiple frames locally, and carry frames across read
  and strip boundaries. Track tentative frame progress separately from committed
  group progress; never expose a partial append or skip its hole. Apply T02's
  complete-append boundary and Q2's shortest
  healthy available prefix; exclude mid-scan failures and corruption rather
  than inventing a short cursor. Bound the entire concurrent observation by
  one IO budget, reject conflicting overlap, and return unavailable with no
  candidate. Cover unwritten tail and exact capacity end; return physical
  cursor, logical length, and compact locations. Share suitable scan primitives
  with the finalizer without making
  the Stream library depend on the ChunkDB application. Maps to D06/A06/A07.
  Files: `lib/crowdb-chunk-stream/src/stream/recovery.rs`,
  `lib/crowdb-chunk-stream/src/production_chunk.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-client/src/chunk/strip_reader.rs`,
  `lib/crowdb-chunk-client/src/chunk/frame_scan.rs` (new if shared),
  `lib/crowdb-chunk-stream/tests/durable_cursor_recovery_test.rs`.
- [ ] **T16 — Align abandoned-chunk finalization with recovery**: audit both
  `FinalizeChunkTaskHandler` and `seal_expired_writer_chunks`. Expiry must not
  finalize an old checkpoint as the complete length or make a later scanner
  skip valid data. Apply the agreed ownership/write-age boundary before scan
  and seal; do not delete a Stream chunk merely because its persisted cursor
  is zero or the first replica's frame is unreadable. Preserve chunk-layer
  isolation and existing non-Stream consumers. Maps to D05/D06/D09, A05/A07/A10.
  Files: `app/crowdb-chunkdb/src/finalize.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler/seal.rs`,
  `app/crowdb-chunkdb/src/task/store.rs`,
  `app/crowdb-chunkdb/tests/finalization_test.rs`.
- [ ] **T17 — Make lifecycle transitions restartable**: attach new allocations
  before data admission; at rollover finalize the current physical/logical
  bounds and directory tail before successor writes. Update writer open,
  close, and externally sealed rotation to recover only the last referenced
  chunk and tolerate each allocation/seal/head-CAS crash window. Maps to
  D05/D06/D08, A05/A06/A09. Files: `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/stream/capacity.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/stream/publication.rs`,
  `lib/crowdb-chunk-stream/tests/rollover_growth_test.rs`,
  `lib/crowdb-chunk-stream/tests/stream_test.rs`.
- [~] **T18 — Enforce the selected Group-0 writer lease**: implement Q3 for busy
  workers, long IO, paused/resumed owners, renewal failure, delayed old writes,
  and takeover. Acknowledgment requires current authority even when all writes
  succeeded; no per-append KV fallback. Compose serving grants and Stream owner
  identity/epoch with conservative monotonic deadlines and background renewal.
  Supply an owner-authority snapshot through the injected runtime for standalone
  streams as well as Chunk-KV; do not make the library depend on server code.
  Explicit transfer stops admission, drains writes, checkpoints, and durably
  releases authority before the new grant starts immediately. Unreachable
  owners require safe expiry and old-write exclusion before tail scan. Reuse
  existing lifecycle protections without adding a hot-path lock or an unapproved
  unsafe scope. Maps to
  D09/A10. Files: `lib/crowdb-chunk-stream/src/stream/liveness.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/production_chunk.rs`,
  `lib/crowdb-chunk-stream/tests/idle_authority_test.rs`,
  `app/crowdb-chunk-kv-server/src/serving/lease.rs`,
  `app/crowdb-chunk-kv-server/src/serving/worker.rs`,
  `app/crowdb-chunk-kv-server/src/serving/transfer.rs`,
  `app/crowdb-chunk-kv-server/src/control_store.rs`.
- [ ] **T19 — Capture owner-flushed or checkpoint fallback views**: implement
  Q4 for exact-epoch `open_read_only` and current-epoch opens. A reachable owner
  handles an ordered barrier, persists a complete-group ChunkDB cursor, then
  publishes the matching directory checkpoint before returning epoch/chunk/end.
  If publication fails after the cursor persists, retain the prior published
  checkpoint; do not combine mismatched roots/epochs/cursors. If unreachable,
  capture the consistent persisted ChunkDB prefix with its matching root,
  explicit source, and potentially older end. Reject a fallback below an exact
  requested cutover. Capture once, pin required root/data state, and preserve
  EOF through writes/rollover. Readers never adopt, seal, allocate, or publish
  writer state themselves; owner-triggered checkpoint is allowed. Maps to
  D10/A11. Files:
  `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/production.rs`,
  `lib/crowdb-chunk-stream/src/stream/read.rs`,
  `lib/crowdb-chunk-stream/tests/read_publication_test.rs`,
  `app/crowdb-chunk-kv-server/src/storage.rs`.
- [x] **T20a — Remove the per-batch directory wait and read amplification**:
  publish lock-free local append mappings, add an ordered checkpoint/finite view,
  flush WAL recovery proofs, validate cached directory pages once, reuse bounded
  valid chunk layouts and preserve parallel multi-frame reads. Amortize physical
  growth and initialize new strips concurrently with bounded memory. Cursor
  fencing was retained in this earlier step; the current integration replaces
  it with process authority and complete-frame recovery. Verify affected Stream/WAL/server tests and
  repeat the unchanged seven-case retained-MemDisk benchmark.
  Files: `lib/crowdb-chunk-stream/src/stream/view.rs`,
  `lib/crowdb-chunk-stream/src/stream/{append,read,worker,publication}.rs`,
  `lib/crowdb-chunk-stream/src/{metadata,production,perf}.rs`,
  `lib/crowdb-chunk-stream/src/production_chunk/{allocation,growth}.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader{,/layout_cache}.rs`,
  `lib/crowdb-chunk-kv/src/partition/{journal,split}.rs`.
- [ ] **T20 — Finish mirror-completion acceptance**: ordinary append now uses
  synchronized-replica completion and local publication, with the cursor and
  directory waits removed. Complete remaining T05–T19 integration and acceptance
  (particularly indexed bounded publication and exact WAL assignment IO). Keep
  admission, exact ranges, queue accounting, ambiguous outcomes, and watchdog
  ownership intact. Optional checkpoints use time/byte budgets and are disabled
  for the strict append sentinel. Maps to D03/D08, A03/A04/A05/A06. Files:
  `lib/crowdb-chunk-stream/src/stream/append.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/metrics.rs`,
  `lib/crowdb-chunk-stream/tests/stream_test.rs`.

## Phase 4 — Integrate consumers, observations, and faults

- [ ] **T21 — Preserve actual WAL/retry/transfer semantics**: adapt
  `StreamPartitionJournal`, checkpoint generation/root retention, replay,
  checkpoint trim, split, and transfer to T02/T03 and independent page roots.
  Exact final split/transfer cutovers cannot silently accept a lagging fallback.
  Keep consumer framing/retry results separate from physical Stream framing.
  Generation no longer implies a complete new copy of all pages. Maps to
  D10/D11/A11/A12/A13. Files:
  `lib/crowdb-chunk-kv/src/partition/journal.rs`,
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `lib/crowdb-chunk-kv/src/partition/split/`,
  `lib/crowdb-chunk-kv/src/partition/transfer.rs`,
  `app/crowdb-chunk-kv-server/src/storage.rs`,
  `app/crowdb-chunk-kv-server/src/storage/split_recovery.rs`.
- [ ] **T22 — Keep inspection bounded under the index**: replace synchronous
  flat-list slicing in `observe_metadata` with the selected bounded indexed
  observation/pagination contract and consistent generation/root tokens. Update
  journal/server adapters without preloading all history to retain an old
  synchronous signature. Preserve exact numeric encoding and finite results.
  Maps to D07/D12/A08. Files:
  `lib/crowdb-chunk-stream/src/stream/observation.rs`,
  `lib/crowdb-chunk-kv/src/partition/observation.rs`,
  `lib/crowdb-chunk-kv/src/partition/journal.rs`,
  `app/crowdb-chunk-kv-server/src/server/observation/storage.rs`,
  `lib/crowdb-chunk-stream/tests/observation_test.rs`,
  `app/crowdb-chunk-kv-server/tests/partition_observation_test.rs`.
- [ ] **T23 — Attribute the new path**: retain periodic and cumulative
  count/avg/p50/p99/max, and add synchronized-RPC, local-publication, root/page
  reuse/write, scan/query, replica repair, retention, and cleanup counters.
  Separate `chunkinfo` changes from Stream publications and actual WAL/client
  latency from standalone append. Replace the fixed three-copy physical-byte
  multiplier with configured/actual replica accounting and separate repair
  amplification. Maps to D12/A14. Files:
  `lib/crowdb-chunk-stream/src/perf.rs`,
  `lib/crowdb-chunk-stream/src/metrics.rs`,
  `app/crowdb-chunk-kv-server/src/metrics.rs`,
  `lib/crowdb-chunk-stream/benches/production_bounds/result.rs`,
  `lib/crowdb-chunk-stream/tests/perf_metrics_test.rs`,
  `app/crowdb-chunk-kv-server/tests/perf_log_test.rs`.
- [ ] **T24 — Complete the deterministic failure matrix**: cover A01–A16 with
  exact expected outcomes from T02/T03. Replace old cursor-authority assertions
  rather than deleting their crash coverage. Exercise 1/16/256 directory
  segments, frame/read-window/strip boundaries, one retained extent in a trimmed
  chunk, shared roots, concurrent GC/publication, stale checkpoints, and process
  takeover. Run real production adapters for cursor/read/finalizer coupling;
  an in-memory Stream seam alone cannot prove those cases. Files: Stream,
  chunk-client, DiskIO-client, ChunkDB, and server test files listed below.

## Phase 5 — Verify production behavior and performance

- [ ] **T25 — Run retained-byte production acceptance**: verify service
  restart, mirror failure, late old-writer IO, read-only cutovers, real WAL replay,
  retry preservation, checkpoint/trim, split, and transfer. Retain failed case
  names and complete service logs; use `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` when
  debugging. MemDisk process tests retain its media; media-loss tests require
  backing that survives the tested fault. Maps to A05/A10/A11/A12/A13. Files:
  `e2e-test/tests/chunk_stream.rs`, `e2e-test/tests/chunk_kv.rs`,
  `e2e-test/tests/chunk_kv/recovery.rs` and existing split/transfer suites.
  Run affected ordinary user-data/native-buffer/tree paths for A15 as well.
- [ ] **T26 — Measure matched performance and growth**: repeat T01 fixtures
  with identical counts and release configuration. Enforce A14's existing gates
  plus zero ordinary append KV work, one synchronized RPC per replica for an
  unsplit single-strip batch, compact large-payload metadata, bounded index/open
  work, and one-chunk recovery. Sweep retained history and separate cold recovery
  from steady-state append; report phase distributions and actual WAL/client
  latency. Keep compilation outside measurement and verified cleanup receipts.
  Files: `tools/benchmark/bench-chunk-stream-regression.sh`,
  `tools/benchmark/bench-chunk-kv-regression.sh`,
  `lib/crowdb-chunk-stream/benches/production_bounds/`.
- [ ] **T27 — Complete gates and reconcile documentation**: run affected
  tests, Rust fmt/clippy, and changed C++ format/tree-lint; update Stream,
  DiskIO, and chunk-IO documentation to the verified result. Record evidence
  against every R244 acceptance case. Requirement/plan/backlog cleanup happens
  only after all required behavior and performance acceptance pass, following
  `/implement-requirement`; planning itself creates no implementation commit.
  Files: upstream design/requirement, this plan, `doc/backlog/backlog.md`,
  `doc/design/diskio/design-crowdb-diskio.md`,
  `doc/design/chunkio/design-crowdb-chunkio.md`.

## Consolidated file scope

The task entries above are the authoritative per-hunk list. Consolidated areas:

- Protocol: `lib/crowdb-protocol/src/{chunk_stream.rs,frame.rs}`,
  `src/key/chunk_stream.rs`, `src/fbs/diskio.fbs`, `build.rs`, and frame/directory/
  wire tests; the shared C++ frame header and common tree chunk consumers are
  included. C++ schema generation remains in `lib/crowdb-rpc/CMakeLists.txt`.
- DiskIO: `app/crowdb-diskio/src/rpc/dio_server.{cpp,h}`, its test registration,
  and ordered-write/flush server tests; `lib/crowdb-diskio-client/src/client.rs`,
  `semantic.rs`, and `semantic/write_views.rs`, plus synchronized-wire tests.
- Chunk IO: `lib/crowdb-chunk-client/src/disk_io/{disk_writer,routing}.rs`,
  `src/client.rs` (`MetricsDiskWriter` forwarding),
  `src/chunk/{mirror_flow,chunk_reader,strip_reader}.rs`,
  `src/chunk/mirror_flow/repair.rs`, and a shared scan helper only if extraction
  is useful; existing mirror/reader tests remain covered.
- Stream: `lib/crowdb-chunk-stream/src/{storage,metadata,kv,memory,production,
  production_chunk,extent_cache,metrics,perf}.rs`, `src/kv/pages.rs`,
  `src/stream.rs`, `src/stream/`, the new directory module, integration tests,
  and production benchmark modules. Update `src/lib.rs` exports as APIs change.
- ChunkDB lifecycle: `app/crowdb-chunkdb/src/{finalize.rs,lifecycle/handler.rs}`,
  `src/lifecycle/handler/seal.rs`, `src/task/store.rs`, and finalization tests.
- Consumers: `lib/crowdb-chunk-kv/src/partition{.rs,/journal.rs,/observation.rs,
  /split/,/transfer.rs}`, server storage/observation/metrics adapters and their
  tests, production E2E files, and the two regression scripts.
- Common framing consumers: user-data small/large and async-native writers,
  S3 native receive buffers and streaming, Iceberg block capacity, common chunk
  reads, and C++ tree chunk/page storage as enumerated in T08. Stream-only
  completeness semantics do not change whole-object publication behavior.
- Owner authority: server `serving/{lease,worker,transfer}.rs` and
  `control_store.rs`, composed with Stream liveness and completion.
- New files named in tasks are proposed locations, not files claimed to exist.
  Exact byte packing may refine the scope; record a concrete blocker in R244
  before changing the selected grouped framing contract.

## Verification groups and completion evidence

- Unit/pure-contract coverage: canonical frame/location arithmetic, independent
  page identities, version rejection, wire sync encoding, checked limits, and
  directory lookup/build transitions. Use crate `tests/` for new Rust tests;
  primary files are protocol `frame_test.rs`, proposed `diskio_wire_test.rs`
  and `stream_directory_test.rs`, and Stream `metadata_test.rs`.
  Shared-format golden vectors cross Rust/C++ and every purpose, including
  CRC coverage, incomplete/corrupt input, unsupported revisions, and length bounds.
- Integration coverage: synchronized server response lifetime/errors, mirror
  repair, production local-cursor refresh, bounded scans, CAS ambiguity,
  lazy directory/cache, retention/GC, ownership, and finite views. Keep Stream's
  self `test-util` dev-dependency and equivalent crate-local test seams.
- E2E coverage: actual synchronized RPCs, retained bytes over restarts, real
  Chunk-KV retry/replay/split/transfer, and matched memory-backed performance.
- Store implementation receipts under an owned
  `.crowdb-runtime/artifacts/r244-chunk-stream/` run directory. Each receipt names
  binary/config identities, command, exit status, case errors, metrics, and
  cleanup. No new runtime artifacts have been created for this planning audit.
- Ordinary commands use a 60-second shell timeout. Start long/hang-prone build,
  E2E, and benchmark commands in the background, retain complete output, and
  poll; command completion and cleanup, not a timeout extension, establish pass.

Commands for implementation, not commands executed by this documentation update:

```bash
pixi run clean-env
pixi run cargo test -p crowdb-protocol -p crowdb-chunk-stream -p crowdb-chunk-client -p crowdb-diskio-client -p crowdb-chunkdb -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets
pixi run cargo test -p crowdb-common --lib metrics::
pixi run build-cpp
pixi run ctest --test-dir app/crowdb-diskio/build --output-on-failure
pixi run test-e2e-chunk-stream
pixi run test-e2e-chunk-kv
pixi run clean-env
pixi run bash tools/benchmark/bench-chunk-stream-regression.sh
pixi run clean-env
pixi run bash tools/benchmark/bench-chunk-kv-regression.sh
pixi run rs-fmt-check
pixi run clang-format --dry-run --Werror app/crowdb-diskio/src/rpc/dio_server.cpp app/crowdb-diskio/src/rpc/dio_server.h
pixi run tree-lint
pixi run cargo clippy -p crowdb-protocol -p crowdb-common -p crowdb-chunk-stream -p crowdb-chunk-client -p crowdb-diskio-client -p crowdb-chunkdb -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets -- -D warnings
```

Run focused groups per changed task before the final combined gates. Discover
the exact available tasks before implementation, and do not run `clean-env` or
shared service teardown during this read-only planning audit.
If `DiskWriter` signature changes affect the access-server test adapter, also
run `pixi run cargo check -p crowdb-access-server --tests` before declaring the
seam integration complete.

The common frame revision also requires consumer/native-buffer and C++ codec
coverage. Include every changed C++ frame consumer in format checks:

```bash
pixi run cargo test -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-tree-ffi --all-targets
pixi run ctest --test-dir lib/crowdb-tree/build --output-on-failure
pixi run test-e2e-s3
pixi run test-e2e-iceberg
pixi run clang-format --dry-run --Werror lib/crowdb-protocol/cpp/include/crowdb-protocol/frame.h lib/crowdb-tree/src/backend/chunk/chunk_pack_pipeline.cpp lib/crowdb-tree/src/backend/chunk/chunk_page_store.cpp
pixi run cargo clippy -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-tree-ffi --all-targets -- -D warnings
```
