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
- [~] **Durable transition core**: isolate session/part states, replacement
  generations, completion selection, abort and recovery transitions from
  Iceberg catalog-specific keys and records. Keep store CAS and namespace
  adaptation in each protocol. Shared phase vocabulary, admission bounds,
  selection validation, accounting and location composition are now used by
  both adapters; storage CAS and durable record layouts remain protocol-specific.
  Files: shared
  multipart crate, Iceberg file repository, S3 metadata store.

## S3 adapter and HTTP

- [ ] **Durable S3 records**: add upload and part keys/records with raw 16-byte
  MD5, selected revision and cleanup state. Use bucket identity and object key
  as namespace scope; preserve immutable part data after replacement.
  Versioned session/part records and ordered, binary-safe keys are in place;
  CAS-backed begin, phase transition and part replacement now use exact-value
  confirmation after lost replies. Completion snapshots and a predecessor-fenced
  metadata-only object publication path are in place. HTTP wiring and cleanup
  state remain.
- [ ] **S3 routes and wire**: classify create/upload/list/complete/abort/list
  uploads, parse bounded completion XML, emit compatible responses and errors.
  Preserve SigV4 authentication and existing basic routes. The repository now
  provides bounded, ordered ListParts pagination over current generations;
  multipart query shapes are parsed separately. Bounded upload listing now
  paginates active sessions by key and upload ID, skipping terminal/expired
  records and failing on scan-budget exhaustion; HTTP dispatch remains pending.
  Upload IDs now sort by initiation millisecond. S3-compatible multipart error
  codes and the create, complete, ListParts, and ListMultipartUploads XML
  response builders have focused tests. The bounded completion
  XML parser now has one implementation in access-server and is exposed by both
  the Iceberg and S3 protocol modules. The S3 HTTP path still needs wiring.
- [ ] **Part ingestion**: reuse the bounded streaming writer and admission
  budget, persist part location/integrity before success, reconcile lost replies.
- [ ] **Atomic completion**: fence selected part generations, validate order,
  count, size and checksum, compose locations through the shared core, and
  publish one immutable object generation without reading part bytes. The S3
  adapter now freezes selection under session CAS, validates it again before
  object-key CAS, and confirms exact publication after response loss. An
  immutable generation records preserve selected bytes across a concurrent
  part-number replacement. The HTTP path and end-to-end publication test remain.
- [ ] **Abort and expiry**: make terminal states idempotent, queue unreachable
  private part data for bounded cleanup, and protect active/read-pinned data.
  The S3 adapter now has an idempotent, response-loss-safe logical abort; the
  durable cleanup queue, expiry scan and read-pin protection remain.

## Acceptance and cleanup

- [ ] **Focused and E2E tests**: known MD5 vectors, out-of-order/replaced parts,
  invalid completion, response loss and restart, abort/expiry cleanup, and
  ordinary single-part compatibility.
- [ ] **Gates and docs**: run both access crate suites, access-server E2E,
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
