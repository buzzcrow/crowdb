<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R195: access server — TPC Iceberg object upload performance

#### Problem

The TPC loader writes Parquet objects through Iceberg FileIO. A measured
100-MiB upload to the single-node container took about 5 seconds, mostly
while closing its multipart output stream. A focused direct PUT in the
small-cluster test took about 1.36 seconds. These are different client paths,
but the gap requires tracing the real TPC route. The Iceberg HTTP loop awaits
each `ChunkIoWriter::on_framed_data` call before polling the next body buffer;
the mirror writer can await DiskIO before accepting more data. Receive,
digest, and durable writing therefore overlap poorly. Multipart session and
publication work may add further latency. See the [Iceberg upload-flow
analysis](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md),
[access server design](../design/access-server/design-crowdb-access-server.md),
and [chunk IO design](../design/chunkio/design-crowdb-chunkio.md).

#### Solution

Implement the object-scoped, bounded producer/consumer write flow first, then
measure the complete 100-MiB TPC FileIO upload, including multipart part
transfers and CompleteMultipart, and reduce its dominant costs. The goal is
the fastest practical upload on the documented single-node profile without
changing durability, integrity, or publication semantics. There is no fixed
seconds threshold: compare before and after results on the same host and
profile, retain stage evidence, and stop when further changes add complexity
without a measured benefit. Parquet generation and the later Iceberg table
snapshot commit are reported separately from FileIO upload latency.

One object-scoped Iceberg write owner retains the parsed request context,
body bounds, digest state, writer, and terminal result for each large file PUT
or multipart part. Large requests may overlap receive, digest, and
chunk writes under bounded write-flow backpressure. The chunk writer keeps
exclusive mutable ownership of its state; do not introduce a hot-path lock,
per-frame virtual dispatch, or a kernel wake for every buffer. The digest
consumes zero-copy logical payload views; SHA-256 is computed only when the
request requires it. The write owner waits for body validation, digest, and
durable writer completion before Iceberg publishes a part or file record.

Implement the flow regardless of the baseline timing. Apply further
optimizations only where supported by the measured stage breakdown.
S3 and Iceberg PUT and multipart UploadPart use one shared producer/consumer
driver below protocol publication policy. The same writer handoff applies to
small objects after body receive; their distinct shared small-write pipeline
remains responsible for durable chunk placement. Multi-node EC throughput and
small-write performance parity are outside this work.

The following invariants define the work:

- **I1 — Bounded overlap.** Only one task fetches an object's socket body.
  It offers each completed owner to the write flow and immediately drives an
  idle writer in the same upload task. Four completed owners may be held while
  up to four independent mirror-strip writes are in flight by default; both
  limits are separately configurable. The writer's next dequeue resumes paused fetch
  without timer polling or a wake on every frame. The global native buffer
  budget bounds retained receive memory, including digest references.
- **I2 — Correct bytes.** The fetch layer prepares frame headers and CRC32C
  over placement-independent bytes. The writer fills the actual chunk ID
  after placement; chunk ID is excluded from CRC32C. The digest sees ordered
  logical payload, never
  frame headers or footers. Partial frames and chunk rotation remain valid
  without an object-sized copy.
- **I3 — Durable publication.** Accepted buffers stay owned until writer
  and digest views finish. A completed strip keeps its buffer until every
  preceding strip commits in order. A failed mirror segment is replaced and
  replayed before later results can commit. A part or file becomes visible only after decoded
  body length, digest, all required mirror/EC writes, fsyncs, seals, and
  metadata preconditions succeed. Failed or ambiguous publication follows
  the existing authoritative recovery rules.
- **I4 — Measured costs.** Record attempts, completions, errors, bytes,
  current and peak owners and writes in flight, plus counts and cumulative
  wait time for socket input, write-flow pause, digest capacity, writer capacity,
  DiskIO completion, and metadata publication. Record end-to-end latency
  separately because concurrent stage times overlap. Use fixed metric
  dimensions and avoid one shared atomic update per 64-KiB frame. Preserve
  failure and cancellation measurements without routine buffer logs.

Work items:

1. Consolidate Iceberg `file_http` request write state and completion into an
   object-scoped owner. Share the body handoff, digest worker, and writer
   scheduling between S3 and Iceberg PUT and UploadPart, while retaining
   separate publication and authorization rules.
2. Add bounded receive/digest/write overlap and ordered concurrent mirror
   strip completion. Use the same write-consumer handoff for small objects,
   retaining their separate shared small-write pipeline. Maintain buffer
   lifetime, frame integrity, and failure fencing.
3. Use the real TPC loader/FileIO route and R196's benchmark to compare
   client preparation, UploadPart, CompleteMultipart, digest, chunk writes,
   and metadata publication. Record part size, concurrency, topology,
   durability settings, software revision, and host with every result.
4. Expose fixed-stage metrics through the Iceberg metrics surface and retain
   before/after snapshots with raw samples. Explain any remaining dominant
   cost when the chosen implementation stops improving.

#### Dependencies

- R196 provides a reusable HTTP benchmark and regression result format. A
  focused existing FileIO test may be used while R196 is implemented, but
  R195 completion requires a reproducible measurement of the real TPC route.
- The current native receive provider, `FramedWriteBuffer`, Iceberg
  multipart authority, and chunk writer are the baseline. Include only the
  bounded digest behavior needed here if its current implementation is
  unmerged.
- S3 and multi-node EC performance remain observable through R196 and their
  existing tests, but are not completion gates for R195.

#### Acceptance

- Given a prepared 100-MiB TPC Parquet object and one documented single-node
  profile, run repeated warm FileIO uploads before and after the change;
  assert zero failed or incomplete operations, correct publication, retained
  raw samples and stage breakdown, and a material improvement in the
  dominant measured stage without a regression in total upload time
  (I1–I4). E2E test.
- Given delayed digest and DiskIO completions during a large part upload,
  continue receiving while offer returns continue; assert receive and write
  overlap, memory stays within the native budget, actual waits have counts
  and durations, and no
  referenced buffer is freed early (I1, I4). Integration test.
- Given a delayed first mirror failure after later writes complete, retain
  later buffers and their order, replay the failed segment into a replacement,
  then seal and read back the exact object (I1–I3). Integration test.
- Given S3 and Iceberg multipart parts and ordinary PUTs, upload equal payloads
  through the shared handoff, validate their MD5 and optional SHA-256, and
  assert both protocols publish only their own completed locations (I1–I3).
  Integration test.
- Given a small S3 or Iceberg object, feed the same write consumer and finish
  through its shared small-write pipeline without changing publication or
  digest behavior (I1–I3). Integration test.
- Given a wrong digest, truncated body, failed write, or ambiguous part
  publication, stop or drain the upload; assert no invalid part or file
  becomes visible, authoritative metadata is checked before cleanup, and
  metrics retain the failed stage and elapsed time (I2–I4). Integration test.
- Given a completed 100-MiB upload, read a bounded first, middle, and final
  range after timing; assert bytes match the source and no per-upload
  readback was included in latency (I2, I3). E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-access-server --features iceberg-e2e`, and the focused R196 Iceberg upload regression through `pixi run` for the implemented scope.
