<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R196: access server — S3 and Iceberg upload benchmark regression

#### Problem

The current `crowdb-cli bench s3` measures an invocation-owned memory-backed
cluster, not an HTTP request through a durable access server. The S3 E2E Python
benchmark captures request samples inside a full-stack test but has no reusable
regression command or failure sentinel. Iceberg has focused upload timings, but
no equivalent repeatable benchmark. The chunk IO regression script exercises
the writer below HTTP decoding, request integrity, and protocol publication.
Consequently, a change can improve chunk IO while slowing S3 or Iceberg PUT,
multipart part upload, or metadata publication without a comparable measurement.
See the [access server design](../design/access-server/design-crowdb-access-server.md),
[Iceberg upload-flow analysis](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md),
and [implemented upload-flow analysis](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md).

#### Solution

Extend `crowdb-cli bench` with one real HTTP upload workload driver that uses
the same measurement, payload generation, concurrency, and result schema for
S3 and Iceberg. Keep the existing memory-backed `bench s3` contract unchanged.
Protocol adapters perform authentication, target setup, direct PUT or multipart
requests, response validation, and cleanup. A timed sample starts immediately
before request submission and ends only when the server acknowledges the
durable write and the protocol's corresponding object, file, or part record is
published. Multipart completion is a separate timed phase; an Iceberg table
snapshot commit is not included in FileIO upload latency and is reported
separately if exercised.

The workload reuses a prepared, deterministic 1-MiB payload block for large
objects and computes expected integrity values before timing. Streaming does
not allocate or copy an object-sized client buffer, hash the full object in the
timed loop, or read the object back after every successful upload. A bounded
sample outside the timed interval verifies stored bytes and range boundaries.
The CLI reports client preparation and transfer timings separately so client
work is not mistaken for server time.

The regression scripts own a reproducible local single-node storage and
access-server deployment by default, using the project's existing deployment
and configuration facilities. They retain service logs, metrics snapshots,
machine-readable per-operation samples, and a summary under one run directory.
An explicit remote-endpoint mode uses the same workload against a supplied
server without claiming local process or storage metrics. The scripts compare
only like-for-like topology and configuration profiles.

The following invariants define the benchmark:

- **I1 — Real protocol path.** Every timed S3 and Iceberg sample crosses the
  HTTP access server, request integrity checks, chunk writer, and the relevant
  protocol publication. The report distinguishes direct PUT, multipart part,
  multipart completion, and optional table commit.
- **I2 — Controlled input.** Size, concurrency, operation count or duration,
  warmup, payload seed, request integrity mode, and deployment profile are
  recorded. Client payload memory is bounded independently of object size.
  Preparation, namespace/table/session setup, final verification, and cleanup
  are outside the timed upload samples.
- **I3 — Honest completion.** The driver records admitted, completed, failed,
  and incomplete operations, drains admitted work before exiting, validates
  success responses and ETags/digests, and exits nonzero for any failed,
  missing, or unverified operation. A timeout or metrics-collection failure
  remains visible in the retained result.
- **I4 — Comparable measurements.** Both protocols emit the same JSON and TSV
  fields for object size, concurrency, request count, logical bytes, elapsed
  time, throughput, average, p50, p95, p99 when sample counts support them,
  and error classes. Percentiles with too few samples are absent, not inferred.
  The report includes client CPU/RSS and, for local runs, access-server
  CPU/RSS and before/after server metric deltas. The shared write-flow stage
  counters from R195 are included when available, with concurrent stage
  durations reported separately from wall-clock latency.
- **I5 — Regression decision.** The default sentinel gates correctness,
  completion, timeout, artifact presence, and required metric availability.
  Throughput and latency comparisons use an explicitly selected baseline for
  the same profile and a documented tolerance; they do not use one fixed
  hardware-dependent number. The result states whether a case was measured,
  comparable, regressed, or invalid, and names the failing condition.

Work items:

1. Add a real HTTP upload verb and shared result model under
   `app/crowdb-cli/src/commands/bench/` and its workload implementation under
   `lib/crowdb-console-shared/src/ops/`. The CLI accepts protocol, endpoint,
   direct or multipart mode, workload bounds, output path, and credentials via
   existing environment/config conventions. Reuse the same input producer and
   percentile/accounting logic for both protocol adapters. Do not route this
   workload through the memory-backed `bench s3` engine.
2. Implement S3 and Iceberg upload adapters. For S3, cover direct PUT and
   UploadPart plus CompleteMultipart. For Iceberg, cover native FileIO direct
   PUT and multipart part plus completion against an authorized table/file
   scope. Preserve each protocol's signing, integrity, ETag, and publication
   rules; never count a successful part as a published final object. Report
   client-side signing or hashing work separately if the selected integrity
   mode requires it.
3. Add `tools/benchmark/` regression scripts modeled on
   `bench-chunkio-write-regression.sh`. Use a named local deployment profile,
   build required binaries through `pixi run`, start and clean up the stack,
   execute isolated S3 and Iceberg cases, retain full output and service
   metrics, and print a compact per-case summary. Provide bounded timeouts
   and a case filter for focused runs. Support explicit remote endpoints
   without trying to destroy a remote deployment.
4. Include a small-path case below the configured threshold, a boundary
   case, direct 100-MiB PUT, and multipart 100-MiB upload for each protocol.
   Run at least one single-client and one concurrent-client case. Record the
   exact threshold, part size, mirror/EC policy, durability setting, and
   software revision with each run so changed profiles are not silently
   compared. Keep setup and verification outside timed samples.
5. Save a baseline artifact and comparison contract for each supported local
   profile. Accept an optional reference result and tolerance in the runner;
   reject comparisons across incompatible profiles. Preserve raw samples and
   metrics for diagnosis even when a case fails.

#### Dependencies

- The existing local deployment facilities, access-server S3 and Iceberg
  HTTP routes, protocol credentials, and chunk IO benchmark artifact
  conventions are the baseline. The shared upload flow is implemented and its current performance is accepted;
  this requirement owns reproducible repeated measurements and regression gates.
- The implemented shared upload flow supplies finer write-flow stage metrics.
  The local sentinel requires those fields for both protocols and reports any
  missing field explicitly rather than fabricating stage measurements.
- Official client compatibility remains covered by the existing E2E suites;
  this benchmark's common producer isolates server upload performance from
  PyArrow or boto3 buffering. A separate client-inclusive profile may reuse
  the same result schema without replacing the controlled regression profile.

#### Acceptance

- Given a local single-node deployment and the same prepared payload, run S3
  and Iceberg direct PUT cases; assert each traverses the HTTP server,
  validates integrity, publishes the correct object or file record, and emits
  comparable latency and byte fields (I1, I2, I4). E2E test.
- Given S3 and Iceberg multipart workloads, upload parts and complete them;
  assert part and completion latencies and outcomes are distinct, and a part
  alone does not count as a published final object (I1, I3). E2E test.
- Given small, threshold-boundary, and 100-MiB cases at one and multiple
  clients, run the CLI with a fixed seed; assert the recorded case profile,
  bounded client payload memory, complete drain, verified bytes, and retained
  JSON/TSV samples match the submitted operations (I2–I4). E2E test.
- Given a rejected digest, failed publication, timeout, or interrupted client,
  run a case; assert the command exits nonzero, records the correct failure or
  incomplete count, and retains logs and metrics for diagnosis (I3, I5).
  Integration test.
- Given a matching baseline and a mismatched profile, compare both with the
  same result; assert the matching comparison applies its configured tolerance
  and reports a regression when exceeded, while the mismatched comparison is
  rejected without a performance verdict (I4, I5). Unit test.
- Given R195 metrics on S3 and Iceberg servers, run the local scripts; assert
  before/after deltas include the shared stage counts and waits, have no
  object-derived labels, and keep overlapping durations separate from total
  request latency (I4, I5). E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-console-shared`, `pixi run cargo test -p crowdb-cli`, and the focused S3 and Iceberg benchmark regression scripts through `pixi run` for the implemented scope.
