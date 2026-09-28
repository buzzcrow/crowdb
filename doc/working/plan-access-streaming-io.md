<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Shared Access Streaming I/O Plan

Implements [R190](../backlog/R190-access-iceberg-shared-streaming-io.md).
Goal: share S3's whole-object write/read path with Iceberg, preserving authority
and crash recovery while eliminating per-frame catalog operations.

Status: Ready after R187 completion. No production refactor has started.

## Execution

- [ ] **Complete flow review**: map read/write/delete/GC, ownership, buffer
  lifetime, publication and physical reuse for S3 and Iceberg. Review the whole
  flow before implementation; include cancellation and crash boundaries.
- [ ] **Shared receive plumbing**: extract deferred native HTTP receive-provider
  installation from the S3 facade into a protocol-neutral access-server module;
  keep S3 behavior covered by existing receive-provider tests. Inspect native
  owner handoff and Iceberg signed-body decoding before wiring the fast path.
- [ ] **Complete stream descriptors and ownership**: define bounded durable
  locations for complete files/parts and allocation ownership for unpublished
  data. Preserve old descriptors; integrate GC with the new owner references.
- [ ] **Whole-object uploads**: reuse prepared Chunk writers, 1 MiB native owners
  and 64 KiB frames; remove per-leaf catalog intent/durable-completion waits.
  Publish only after checksum, format and storage completion.
- [ ] **Shared reads**: use Chunk read streams and owner-backed Bytes for full
  GET and ranges; preserve integrity, pins and cancellation.
- [ ] **Multipart completion**: compose validated completed parts without the
  old serial per-leaf rewrite/commit path; retain recovery and terminal credits.
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
- Release/null-DiskIO measurements: ordinary PUT 2791/2682/2752 ms; UploadPart
  2091/2184/2159 ms. All HTTP 200. Temporary probe was removed.
- Debug multipart intermittently exceeds its existing 10 s deadline even after
  streaming MD5 verification. This remains unresolved; do not hide it by only
  changing test profiles or raising the timeout.

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
