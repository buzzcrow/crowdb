<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Shared Access Streaming I/O Plan

Implements [R190](../backlog/R190-access-iceberg-shared-streaming-io.md).
Goal: share S3's whole-object write/read path with Iceberg, preserving authority
and crash recovery while eliminating per-frame catalog operations.

Status: Bounded S3 and Iceberg reads, access configuration, conditional S3
SHA-256, shared HTTP receive plumbing, Iceberg pin removal, location descriptors,
whole-object uploads and metadata-only MPU completion pass focused tests.
Fault coverage, operation-count measurements and final gates remain.

The access processes now have a shared typed TOML startup schema. The single-node
container renders it separately for S3 and Iceberg; it exposes read slots,
window size, retained bytes, recovery memory, small-write memory and pipeline
limits, S3 resources and Iceberg GC limits
while keeping secrets in the environment.

## Execution

- [~] **Complete flow review and counters**: map read/write/delete/GC, ownership, buffer
  lifetime, publication and physical reuse for S3 and Iceberg. Review the whole
  flow before implementation; include cancellation and crash boundaries. Record
  read-window, location-normalization and Chunk layout-query counts through
  CROWDB metrics, then compare with perf counters before changing read flow.
- [~] **Remove legacy request-level metadata amplification**: removed the
  file-request, file-publication, delegated-credential and table-load GC pin writes;
  removed the pin record, protocol schema, GC handling and manual commands.
  No old Iceberg pin data exists, so no compatibility path is needed.
  UploadPart still updates its session through multiple serialized CAS
  operations, and the old sealer still rereads complete non-JSON files.
  Treat new-path bytes as opaque and keep part publication independent across
  part numbers. Measure catalog operations per request.
- [x] **Bounded ordered read pipeline**: replace the 64 MiB fetch window with
  configurable per-stream slots (default three, at most 1 MiB physical data
  each). Schedule strip reads concurrently, verify complete frames, retain
  out-of-order results within those slots, and hand owner-backed payload views
  to HTTP in order. Add a global retained-byte budget across streams and a
  separate recovery scratch budget. Hold credits until the HTTP body releases the buffers;
  cancel outstanding work when the response ends. Remove copies in normal EC
  reads and in the RPC-to-DiskIO response handoff. Before retaining pooled RPC
  buffers in HTTP, prove or extend pool lifetime through their final release.
- [x] **Remove repeated read-stream location work**: normalize a descriptor
  once per stream, select only locations overlapping each window, and export
  lock-free normalization, range-scan, window and layout-query counters from
  Chunk I/O through S3 `/metrics`. Files: `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-client/src/metrics.rs`, `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-access-s3/src/metrics.rs`. Focused Chunk reader and S3 metrics
  tests pass; perf attribution remains under the active review task.
- [x] **Conditional S3 payload SHA-256**: maintain MD5 for the existing ETag,
  and initialize payload SHA-256 only when a request declares one. Both generic
  and native-owner upload paths select the digest before receiving body bytes.
- [x] **Shared receive plumbing**: extract deferred native HTTP receive-provider
  installation from the S3 facade into a protocol-neutral access-server module;
  install bounded native owners for admitted Iceberg PUT and UploadPart requests.
  Keep S3 receive-provider behavior covered and verify Iceberg signed-body
  decoding before handing native owners directly to the new Chunk writer.
- [x] **Complete stream descriptors and ownership**: define bounded durable
  locations for complete files/parts. Preserve old SHA-256 descriptors and their verification; new descriptors
  default to frame CRC and carry optional requested checksums separately from
  the HTTP ETag. Published references remain protected through the GC grace period; the
  user assigned unpublished orphan discovery to R92's future ChunkDB scanner,
  so do not add upload-owner or per-chunk intent writes to the data path.
- [x] **Whole-object uploads**: reuse prepared Chunk writers, 1 MiB native owners
  and 64 KiB frames; remove per-leaf catalog intent/durable-completion waits.
  Reject Iceberg PUT/UploadPart lacking Content-MD5, an S3 checksum or a signed
  payload before reading the body. Publish only after declared transfer checksum
  and durable storage completion; leave client file formats opaque.
- [x] **Shared reads**: use Chunk read streams and owner-backed Bytes for full
  GET and ranges; preserve integrity, the GC grace period and cancellation.
- [~] **Multipart completion**: compose validated completed parts without the
  old serial per-leaf rewrite/commit path; retain recovery and terminal credits.
  Save part MD5 values at upload and produce the final ETag from those values.
  Compute SHA-256 for a part only when explicitly requested; preserve any
  requested composite checksum as separate metadata without rereading parts.
- [ ] **Faults and measurements**: test cancellation, lost replies, crash points,
  ownership reclamation, stale grants and existing records; compare identical
  5 MiB baseline plus ordinary 10 KiB/1 MiB/12 MiB/100 MiB PUTs and a
  100 MiB multipart upload (twenty 5 MiB parts), with matching build profiles.
- [ ] **Final gates and cleanup**: run affected S3/Iceberg suites, fmt/clippy,
  update current architecture, close this requirement after the full reviewed flow passes.

## Evidence

- Existing native Iceberg writer uses 65,502-byte leaves. A 5 MiB upload has
  81 leaves plus a directory block. Each registers ownership with six reads
  and one conditional write, then forces readable-cursor completion.
- Existing S3 selects the prepared large writer for this size and publishes
  object metadata after `on_finish`; receive owners are configured at 1 MiB.
- Confirmed write receive cadence: an object below 1 MiB is received in full
  before one push to the selected pipeline; larger objects hand off each ready
  1 MiB owner as reception continues, plus a final partial owner. This is
  separate from both the 0.9 × strip-capacity routing threshold and 64 KiB
  frame validation. Current S3 native owner handoff applies to known-length
  large writers; R190 must extend the shared path accordingly.
- Release/null-DiskIO measurements: ordinary PUT 2791/2682/2752 ms; UploadPart
  2091/2184/2159 ms. All HTTP 200. Temporary probe was removed.
- Debug multipart intermittently exceeds its existing 10 s deadline even after
  streaming MD5 verification. This remains unresolved; do not hide it by only
  changing test profiles or raising the timeout.
- Confirmed 8+2 partial EC with two 1 MiB data shards: incremental parity matches
  the zero-filled reference and recovers two data failures or mixed data/code
  failures. The focused EC test is in `lib/crowdb-common/rust/tests/ec_test.rs`.
- Chunk read stream normalizes locations once per stream, schedules up to three
  concurrent physical reads of at most 1 MiB each, and caches valid Chunk layouts
  across windows. Credits remain reserved while HTTP holds payload views.
- The user confirmed 64 KiB write frames for both S3 and Iceberg for now.
  Revisit a larger Iceberg frame only with evidence about actual read units
  and end-to-end integrity coverage.
- Legacy Iceberg `FileHttp::execute` persisted one `GcPin` per file request,
  `FileRepository::publish` added a publication pin, and the table loader and
  credential endpoint also created pins; those writes have been removed.
  `MultipartRepository::part` still reloads the session before and after
  reading a part. `reserve_part` and `settle_part` serialize part uploads through
  session revisions. `FileSealer::seal` reads complete non-JSON files before
  format validation. These are separate from the per-leaf write intents and
  must be removed or amortized in the R190 path.
- Frame decode previously copied each physical frame and payload, then copied
  the growing output again for every adjacent range (quadratic total copying).
  Extraction and payload slices now reuse `Bytes` ownership. All Chunk read
  APIs return verified buffer arrays; streams send each buffer without joining.
  `frame_decode_wait_ns` measures the decoder and `frame_parse_wait_ns`
  measures parsing plus CRC. The removed range-merging counter previously
  measured 5.9–7.5 ms on a 4 MiB release run; that merge no longer executes.
- The original bitwise CRC32C and byte-table replacement were too slow on the
  4 MiB mixed-read release run: `frame_parse_wait_ns` summed to 17.5 ms with
  the table implementation. Hardware-dispatched CRC32C retains the seed-zero,
  no-final-XOR wire checksum and reduced that sum to 1.49 ms. The independent
  bitwise CRC reference and existing cross-language frame vector pass. On two
  otherwise identical four-request release runs, measured read throughput was
  94.9 then 165.2 MiB/s; RPC and host noise remain, so this is directional.
- After removing payload range merging, the same four-request release fixture
  passed at 173.8 MiB/s with 2.27 ms cumulative frame parse time. This is a
  small concurrency-2 null-DiskIO sample, not a throughput target. Whole-object
  and range reads now return arrays of verified `Bytes`; HTTP streams emit each
  frame buffer without an object-sized copy. Callers that require contiguous
  bytes, such as legacy Iceberg block parsing, materialize them explicitly.
- The normal TCP RPC receive path now detaches the buffer owner from its pool,
  so DiskIO and the ordered S3 body retain the original payload allocation.
  Split frames are CRC checked across buffer views without joining the payload.
  The RDMA/non-system-pool fallback still copies once and needs a separate review.
- Legacy Iceberg tree records still use their per-leaf reader and full-file
  digest. New location descriptors use the shared Chunk read stream and emit
  verified owner-backed buffers without joining the object.
- The bounded Chunk read tests pass all 11 focused cases, the Access Server
  target suite passes, and the S3 full-stack suite passes all 17 cases after
  configuration wiring. These checks establish correctness of this slice;
  they do not establish an Iceberg throughput improvement or complete R190.
- S3 single-part integrity now initializes payload SHA-256 only when a signed
  payload digest is declared. Focused integrity and streaming tests, S3
  Clippy, Rust formatting, and all 17 S3 full-stack cases passed before the
  streamed-writer change; the full stack is being rerun. The old Iceberg tree
  descriptor retains its whole-file SHA-256, while new location descriptors
  rely on frame checksums and verified request integrity.
- Shared objects larger than one frame enter the pipeline with the first 1 MiB
  receive buffer. A bounded two-buffer channel feeds subsequent data while the
  pipeline writes the current buffer. Cancellation seals the abandoned chunk
  and opens another chunk for queued objects. S3 and Iceberg wait for the
  readable cursor before publishing even a sub-64 KiB object. The focused
  shared-object and cancellation E2E cases pass; the full suite is running.
- After rebasing onto `origin/main` at `a59d24fb`, the Java acceptance setup
  prefetches Maven runtime dependencies before its offline SDK invocations.
  Offline `exec:java` reached the Java program with all dependencies present.
  The ChunkDB restart fixture now waits for a usable registry read instead of
  sleeping for three seconds; its focused and full reader E2E suites pass.
- The Iceberg UploadPart response can use the part value just successfully
  settled; reloading the session and part added four catalog reads. A separate
  upload lookup now performs one part read and leaves the session CAS to reject
  stale snapshots, removing two more catalog reads. The reserve CAS now
  returns its pending session to settlement, removing another reload. The
  focused repository test confirms exactly one read for the part lookup.
  The remaining session
  reservation/settlement CAS operations still serialize different part numbers;
  removing them needs a completion snapshot that keeps selected overwritten
  parts reachable when UploadPart races Complete.
- A native 100 MiB upload with twenty 5 MiB parts completed into one immutable
  descriptor with twenty locations and the expected composite MD5 ETag. A new
  Iceberg listener accepted Complete replay and returned the full verified
  object. The focused end-to-end test passed with a 128 MiB request grant;
  the ordinary 16 MiB test grant correctly rejects a 100 MiB full GET.
- The native publication crash matrix now uses the streamed Chunk client through
  its fault-injection wrapper. Every PUT and MPU catalog write boundary passed
  before/after listener loss and replay. Immutable PUT retries compare the
  saved MD5 ETag and length, so a retry that allocated a different chunk still
  resolves to the first published file; different ETags remain conflicts.
- New streamed Complete selections now persist each selected part's length,
  ETag and exact location bytes in the existing selection payload. Streamed
  publication consumes that snapshot without rereading part records. A focused
  test replaces a selected part record after freeze and still publishes the
  originally selected locations and composite ETag; the 100 MiB replay test
  passes with the new selection format. The old selection format remains
  readable for legacy multipart recovery.
- Streamed UploadPart now performs one CAS on its own part key and no session
  reservation/settlement CAS. Distinct part numbers commit independently; a
  lost CAS reply is resolved by reading only that part record. Create caps
  `max_parts` by the staged-byte reservation divided by the per-part limit,
  so concurrent parts cannot exceed reserved capacity without a shared
  counter. Complete validates its snapshot against the per-session maxima
  rather than mutable staged counters. The focused concurrency, lost-reply,
  limits, ordinary HTTP MPU and 100 MiB restart/replay tests pass.
  The 100 MiB E2E now sends each pair of 5 MiB parts concurrently; its
  Complete, listener restart, replay and full GET still pass. The native
  publication crash matrix also passes after the direct-CAS change.

## Files

- Access server: shared body receive module, S3 dispatcher/operations, Iceberg
  HTTP, body decoding, uploads, reads and runtime wiring.
- Access libraries: native buffers, streaming, file descriptors, multipart,
  file validation, GC and record codecs.
- Chunk client/protocol: existing large writer, read streams and durable
  allocation ownership; extend only where the shared path requires it.

## Tests

- Unit: checksums/framing, descriptor validation, bounded reads and lifecycle.
- Integration: native receive-provider tests, S3/Iceberg upload/read tests,
  counted catalog calls and backwards-compatible descriptors.
- E2E: native file/multipart crash tests, official SDKs, container acceptance.
