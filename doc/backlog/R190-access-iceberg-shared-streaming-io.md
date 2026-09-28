<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R190: access — Shared S3 and Iceberg streaming data path

Status: Ready after R187 completion, at the user's request. Begin with a
complete read/write/delete/GC flow review before implementation.

## Problem

Iceberg FileIO treats each roughly 64 KiB leaf as a separately durable small
object. Each leaf registers physical ownership through six catalog reads and
one conditional write, then waits for the Chunk readable cursor. Receiving the
next leaf waits for that entire chain. Reads fetch individual leaves and copy
returned bytes. S3 already receives into 1 MiB owners, frames at 64 KiB, and uses
whole-object Chunk writers and lazy read streams.

A measured 5 MiB release-build upload on the native null-DiskIO stack took
2.68–2.79 seconds for ordinary PUT and 2.09–2.18 seconds for UploadPart, excluding
multipart completion. These are API measurements, not NVMe throughput.

Root designs: [Iceberg](../design/access-server/iceberge/design-crowdb-iceberg.md),
[S3](../design/access-server/s3/design-crowdb-access-s3.md), and
[Chunk I/O](../design/chunkio/design-crowdb-chunkio.md).

## Solution

The user-selected data path is shared streaming infrastructure with independent
S3 and Iceberg metadata semantics:

    HTTP receive owner (1 MiB) -> Chunk writer (64 KiB frames) -> DiskIO
    durable complete locations -> one atomic file/part publication point
    published locations -> Chunk read stream -> owner-backed HTTP response

1. Share deferred HTTP receive-provider installation and bounded native owner
   allocation. Authenticate and admit before reading bodies. For objects under
   1 MiB, receive the complete body and submit it to the selected write pipeline
   once. For larger bodies, submit each ready 1 MiB receive owner to the write
   pipeline while reception continues, then submit the final partial owner.
   This receive cadence is independent of the small-object routing threshold
   and the 64 KiB integrity frame. Preserve signed AWS-chunked decoding,
   checksums and unknown-length bounded streaming.
2. Write a whole file or multipart part through the Chunk writer selected by
   object size. Do not register catalog intents or await a durable cursor per
   frame. A frame is transport/integrity granularity, not a catalog transaction.
   Select the shared small-object pipeline when logical bytes are strictly below
   a configurable ratio, initially 0.9, of one strip's data capacity. Larger
   objects and uploads whose total length is unknown at admission own their
   chunks. Unknown-length uploads stream into that dedicated writer without
   buffering to the small-object threshold. EC tails encode only present data
   shards; absent data positions contribute zero without writing empty blocks
   to DiskIO.
3. Publish complete immutable file or part metadata only after data completion,
   length validation and HTTP transfer checksum verification. Require at least
   one verified request integrity declaration for Iceberg PUT and UploadPart:
   `Content-MD5`, an S3 checksum header/trailer, or a signed payload. Reject a
   request with none before reading its body. Treat
   content as opaque bytes; do not parse JSON, Avro, Parquet, ORC or Puffin on
   the file IO path. A client that supplied a format validates it after reading;
   CROWDB modules that generate format files validate their own output. Keep
   fencing, conflicting-path rejection and ambiguous-result resolution.
   Readers cannot observe partial data.
4. Stream GET and Range through the same Chunk read machinery as S3, retaining
   owner-backed buffers and bounded backpressure. Read complete overlapping
   64 KiB frames and verify their CRC before exposing requested bytes. Keep
   Iceberg credentials, generation checks and GC retention protection.
   Use the same 64 KiB frame for Iceberg in this requirement. A possible larger
   Iceberg frame remains a later measured choice; do not assume Parquet or
   Iceberg checksums replace Chunk frame validation.
   Schedule selected strip blocks as an ordered producer-consumer pipeline.
   Each read stream has a configurable number of slots, initially three; each
   slot holds at most 1 MiB of physical read data and its verified frame views.
   Read slots concurrently, but deliver frames in logical order. If the first
   slot waits for recovery, completed later slots wait within the fixed budget;
   do not schedule a replacement until the consumer releases a slot. Keep the
   slot charged while its owner-backed payload remains in the HTTP body, so
   socket backpressure bounds retained RPC buffers as well as queued reads.
   Also bound total retained read bytes across concurrent streams; a per-stream
   slot count alone cannot bound server memory. Account for recovery scratch
   space separately from the normal read slots.
   Verify each complete 64 KiB frame before exposing its requested payload.
   Normal reads must not copy payload in the RPC, DiskIO, EC, Chunk or HTTP
   layers; recovery may allocate the reconstructed bytes. Ensure the RPC pool
   outlives every buffer retained by a response, including cancellation.
5. Make multipart completion concatenate the selected parts' chunk-location
   arrays with adjusted logical offsets. It does not read part bytes or restore
   the per-leaf write/commit path. Save each part's raw MD5 on upload and compute
   the multipart ETag from the MD5 of the ordered raw MD5 values, followed by
   `-<part-count>`. Share protocol-neutral session, part and completion logic
   with the later S3 MPU implementation, while keeping API-specific publication.
   Preserve ordering, replay and atomic final-file visibility.
   New file records do not require a whole-file SHA-256. Preserve the old
   SHA-256 record variant and its full-read verification for existing files;
   use a versioned descriptor for new files and verify complete 64 KiB frames
   on reads. Calculate a payload SHA-256 only when the request explicitly
   declares one, including signed payload verification or an optional SHA-256
   checksum. Multipart SHA-256, when requested, is a composite of part checksums,
   not the SHA-256 of concatenated object bytes. Do not reread parts at Complete.
   Keep the HTTP ETag separate from storage integrity metadata.
6. Publish one complete descriptor only after durable data completion. A crash
   before publication may leave orphan chunks or shared ranges; do not add
   per-chunk catalog intents or upload-owner records to the write hot path.
   R92's future ChunkDB scanner will identify and reclaim these orphans after
   proving they are absent from every published access descriptor and active
   writer. Current delete/GC must never free published data before the
   configured grace period, and
   submitted writes must drain before any physical reuse.
7. Review delete and GC end to end alongside reads and writes: logical
   invisibility, reference discovery, grace periods, cancelled writes,
   shared ranges, compaction and physical reuse must form one coherent model.
8. Keep existing stored file descriptors readable, or implement an explicit
   migration within this work; do not silently invalidate persisted volumes.

## Dependencies

- Existing native receive owners, prepared Chunk writers and Chunk read streams.
- Existing Iceberg file, multipart and GC contracts remain acceptance obligations.
- R92 will handle scanner discovery and reclamation of allocations abandoned
  before publication; R168/R169/R147 contain other deferred shared-storage
  reclamation work. R190 may leave unpublished allocations as orphans until
  those requirements are implemented, but must preserve all published data.
- R188 remains a separate console-authority follow-up.

## Acceptance

- Given ordinary PUT bodies of 10 KiB, 1 MiB, 12 MiB and 100 MiB, upload
  through S3 and Iceberg -> both use bounded
  1 MiB owners and 64 KiB frames; no catalog operation is issued per frame.
  **Bounded shared ingress. Integration test.**
- Given a configured 0.9 ratio and 8+2 or mirror strip geometry, upload bodies
  immediately below, at and above the derived data-capacity threshold -> only
  below-threshold objects use a shared chunk. **Configurable routing. Integration test.**
- Given an 8+2 tail containing two data shards, seal and lose two real data
  segments or one data and one parity segment -> recover exact bytes without
  writing six empty data segments. **Partial EC recovery. Integration test.**
- Given signed chunks, corrupted signatures/checksums, short bodies and cancelled
  requests, upload -> reject without publishing metadata; release owner credits
  and drain in-flight writes. **No partial visibility. Integration test.**
- Given completed bytes, publish with a conflicting path or a lost reply -> keep
  one complete authoritative outcome without overwriting different content.
  **Atomic publication. Integration test.**
- Given full, cross-frame and cross-chunk ranges, read -> exact bytes, bounded
  retained buffers, integrity checks and cancellation propagation.
  **Shared bounded reads. Integration test.**
- Given a 100 MiB multipart upload (twenty 5 MiB parts), complete/replay/restart
  -> one correct immutable file, no per-leaf rewrite/commit loop and no
  premature part reclamation.
  **Multipart correctness. E2E test.**
- Given saved MD5 values for selected MPU parts, Complete in a reordered subset
  -> return the MD5 of their ordered raw digests with the selected part-count
  suffix without a part read. **Metadata-only ETag. Integration test.**
- Given an unsigned upload without a requested SHA-256 checksum, write without
  hashing the payload with SHA-256; given a declared signed payload or SHA-256
  checksum, validate it before publication. Existing SHA-256 file records remain
  readable. **Conditional SHA-256. Integration test.**
- Given process loss before/after data completion and metadata publication,
  recover -> no partial file is visible; unpublished allocations may remain
  orphaned for R92; published data remain readable through the grace period.
  **Crash publication boundary. E2E test.**
- Given published and unpublished data, delete and run GC across restart
  -> logical deletion precedes physical reclamation; retained reads remain valid;
  freed ranges are not reused before writes drain; unpublished orphan discovery
  is deferred to R92.
  **Delete/GC consistency. E2E test.**
- Given existing file descriptors, restart and read -> preserve bytes and ranges.
  **Persisted-data readability. Integration test.**
- Given the same 5 MiB fixtures and build/storage profile, measure PUT, UploadPart
  and GET -> record elapsed time and dependency-operation counts against the
  baseline without raising deadlines or weakening assertions.
  **Measured operation reduction. E2E test.**

Commands:

```sh
pixi run test-access-iceberg
pixi run test-access-s3
pixi run test-access-server
pixi run -e s3-e2e test-boto3-e2e
pixi run -e iceberg-e2e test-iceberg-native
pixi run -e iceberg-e2e test-iceberg-sdk
pixi run rs-fmt-check
pixi run rs-lint
```
