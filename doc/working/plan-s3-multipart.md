<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Multipart Plan

Upstream: [R167](../backlog/R167-s3-multipart-upload.md).

Goal: add durable S3 multipart uploads while sharing protocol-neutral part
composition and state transitions with Iceberg.

Status: active. The basic S3 milestone is complete; the protocol-neutral R190
core referenced by R167 is absent, so extraction begins from the working
Iceberg multipart path.

## Shared foundation

- [x] **Metadata-only composition**: extract offset adjustment and composite
  MD5 ETag from Iceberg `MultipartRepository::prepare_stream_publication` into
  `lib/crowdb-access-multipart`, preserving the existing selected-part fences
  in Iceberg. Include overflow and malformed location tests. Files: new crate,
  Iceberg publication, workspace manifests.
- [x] **Durable transition core**: isolate session/part states, replacement
  generations, completion selection, abort and recovery transitions from
  Iceberg catalog-specific keys and records. Keep store CAS and namespace
  adaptation in each protocol. Shared phase vocabulary, admission bounds,
  selection validation, accounting and location composition are now used by
  both adapters; storage CAS and durable record layouts remain protocol-specific.
  Files: shared
  multipart crate, Iceberg file repository, S3 metadata store. Both adapters
  use the same inclusive/exclusive lifetime decision and checked session/part
  revision advancement. Iceberg's byte assembly checkpoints and S3's
  metadata-only object publication remain in their adapters because their
  durable evidence and work units differ.

## S3 adapter and HTTP

- [x] **Durable S3 records**: add upload and part keys/records with raw 16-byte
  MD5 and selected revision under one upload prefix. Use bucket identity and
  object key as namespace scope; preserve immutable part data after replacement.
  Versioned session/part records and ordered, binary-safe keys are in place;
  CAS-backed begin, phase transition and part replacement now use exact-value
  confirmation after lost replies. An identical part record retry returns the
  existing revision; a new location remains a replacement. Completion
  snapshots and a predecessor-fenced metadata-only object publication path are
  in place. The session, current part and immutable generations now share one
  upload prefix for R95; an immutable object-key/upload-ID index preserves
  bounded ListMultipartUploads ordering. HTTP wiring remains.
- [x] **S3 routes and wire**: classify create/upload/list/complete/abort/list
  uploads, parse bounded completion XML, emit compatible responses and errors.
  Preserve SigV4 authentication and existing basic routes. The repository now
  provides bounded, ordered ListParts pagination over current generations;
  multipart query shapes now enter the authenticated dispatcher with distinct
  metrics. Bounded
  upload listing now
  paginates active sessions by key and upload ID, skipping terminal/expired
  records and failing on scan-budget exhaustion; HTTP dispatch remains pending.
  Upload IDs now sort by initiation millisecond. S3-compatible multipart error
  codes and the create, complete, ListParts, and ListMultipartUploads XML
  response builders have focused tests. The bounded completion
  XML parser now has one implementation in access-server and is exposed by both
  the Iceberg and S3 protocol modules. The six S3 HTTP operations are wired.
  Duplicate or descending completion parts now map to S3 `InvalidPartOrder`;
  malformed XML remains `InvalidRequest`.
  Full boto3 stack acceptance passes Create, UploadPart, ListParts, Complete,
  Abort and ListUploads, including replay, replacement and invalid ETag cases.
- [x] **Part ingestion**: reuse the bounded streaming writer and admission
  budget, persist part location/integrity before success, reconcile lost replies.
  Production UploadPart now uses the basic streaming writer and saves raw MD5
  with locations. A byte-identical retry keeps the selected generation even
  when a new write produced different chunk locations; R95 can reclaim those
  unreachable chunks. Full stack ingestion passes 5 MiB and small parts, a
  dropped UploadPart success response, and retries after service restart.
- [x] **Atomic completion**: fence selected part generations, validate order,
  count, size and checksum, compose locations through the shared core, and
  publish one immutable object generation without reading part bytes. The S3
  adapter now freezes selection under session CAS, validates it again before
  object-key CAS, and confirms exact publication after response loss. An
  immutable generation records preserve selected bytes across a concurrent
  part-number replacement. Metadata-only completion and byte-exact GET pass
  the full boto3 stack, including a repeated Complete request.
- [x] **Part publication fence**: reserve each changed current-part pointer
  under session CAS, persist the immutable generation, settle the pointer and
  clear the reservation. Complete and Abort must not pass an unresolved
  reservation. A helper can finish a committed reservation after reply loss or
  restart. Test a replacement racing with freeze, then test recovery at every
  durable boundary. Keep this CAS-based path lock-free and retain orphan
  generations for R95. A focused test freezes an interrupted reservation only
  after recovery, and a changed pointer prevents object publication.
- [x] **Abort and expiry**: make terminal states idempotent and preserve the
  part generations that R95's chunk-centered scanner needs for reference checks.
  The S3 adapter now has an idempotent, response-loss-safe logical abort and a
  bounded hourly expiry sweep over the upload listing index. Session and part
  records remain under one upload prefix after terminal transition. Focused
  expiry pagination and evidence tests pass. Requests encountering an expired
  open session terminate it before the hourly sweep. R95 owns physical cleanup.

## Acceptance and cleanup

- [x] **Focused and E2E tests**: known MD5 vectors, out-of-order/replaced parts,
  invalid completion, response loss and restart, abort/expiry metadata, and
  ordinary single-part compatibility. The complete S3 library and access-server
  suites plus 19 full-stack boto3/restart cases pass. The new cases drop
  UploadPart, Complete and Abort success replies, replay them, preserve an
  incomplete session across six service restarts, then complete it. Hourly
  expiry has a focused metadata test; broader concurrency and error-matrix
  acceptance remains.
- [x] **Gates and docs**: run both access crate suites, access-server E2E,
  Rust fmt and clippy separately; update S3 design and remove R167 plus this
  plan only after all acceptance criteria pass.

## Files

- `Cargo.toml`, `Cargo.lock`, `lib/crowdb-access-multipart/**`
- `lib/crowdb-access-iceberg/src/file/multipart_repository/**`
- `lib/crowdb-access-s3/src/{metadata,route,integrity,wire}.rs` and children
- `app/crowdb-access-server/src/s3/**`
- `lib/crowdb-access-s3/tests/**`, `app/crowdb-access-server/tests/**`

## Tests

- Unit: shared composition MD5, location offsets and bounds.
- Integration: durable S3 session/part/complete/abort transitions and Iceberg
  publication regression.
- E2E: official S3 client multipart flows, response loss and restart.
