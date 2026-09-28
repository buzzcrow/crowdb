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
   allocation. Authenticate and admit before reading bodies. Preserve signed
   AWS-chunked decoding, checksums and unknown-length bounded streaming.
2. Write a whole file or multipart part through the Chunk writer selected by
   object size. Do not register catalog intents or await a durable cursor per
   frame. A frame is transport/integrity granularity, not a catalog transaction.
3. Publish complete immutable file or part metadata only after data completion
   and validation. Keep fencing, conflicting-path rejection and ambiguous-result
   resolution. Readers cannot observe partial data.
4. Stream GET and Range through the same Chunk read machinery as S3, retaining
   owner-backed buffers and bounded backpressure. Keep Iceberg credentials,
   generation checks, format validation, full-file integrity and GC protection.
5. Make multipart completion consume complete part references without restoring
   the per-leaf write/commit path. Preserve ordering, replay, format validation
   and atomic final-file visibility.
6. Retain crash-safe allocation ownership and reclamation below the per-frame
   catalog path. Use durable Chunk allocation/lifecycle ownership rather than
   deleting protection and assuming S3 already implements all orphan GC.
   Drain submitted writes before reclaim; never free published or pinned data.
7. Review delete and GC end to end alongside reads and writes: logical
   invisibility, reader pins, owner discovery, grace periods, cancelled writes,
   shared ranges, compaction and physical reuse must form one coherent model.
8. Keep existing stored file descriptors readable, or implement an explicit
   migration within this work; do not silently invalidate persisted volumes.

## Dependencies

- Existing native receive owners, prepared Chunk writers and Chunk read streams.
- Existing Iceberg file, multipart and GC contracts remain acceptance obligations.
- R168/R169/R147 contain deferred shared-storage reclamation work. Do not claim
  those are implemented or weaken Iceberg recovery to bypass them; implement
  any ownership support required for this path within this requirement.
- R188 remains a separate console-authority follow-up.

## Acceptance

- Given ordinary PUT bodies of 10 KiB, 1 MiB, 12 MiB and 100 MiB, upload
  through S3 and Iceberg -> both use bounded
  1 MiB owners and 64 KiB frames; no catalog operation is issued per frame.
  **Bounded shared ingress. Integration test.**
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
  -> one correct immutable file,
  no per-leaf rewrite/commit loop and no premature part reclamation.
  **Multipart correctness. E2E test.**
- Given process loss before/after data completion and metadata publication,
  recover -> unpublished allocations remain discoverable and eventually reclaim;
  published/pinned data remain readable. **Crash-safe ownership. E2E test.**
- Given published, pinned and unpublished data, delete and run GC across restart
  -> logical deletion precedes physical reclamation; retained reads remain valid;
  ownership is discoverable and freed ranges are not reused before writes drain.
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
