<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R167: access server / S3 — Multipart upload

## Status

**Deferred until the R152–R166 basic S3 milestone is complete.**
It is unblocked when single-request streaming, publication recovery, ETag,
listing, deletion, and compatibility E2E behavior are stable.

## Problem

Multipart upload adds durable upload/part listing, independent part retries,
completion ordering, abort cleanup, and multipart ETag semantics. Adding it
before the basic publication and cleanup paths stabilize would duplicate
unsettled recovery rules and delay the deliberately limited first service.

The scope boundary is
`doc/design/access-server/s3/design-crowdb-access-s3.md` §1.

## Solution

1. Add create, upload-part, list-parts, complete, abort, and required upload
   listing operations through S3-owned API and namespace adapters over the
   protocol-neutral multipart session, part, and completion core from R190.
2. Store immutable part identities and integrity records durably; a retried
   part number replaces only that part's selected generation and schedules old
   private data for cleanup.
3. Complete with one fenced metadata transaction that validates ordered part
   identities, sizes, checksums, and expected upload state before publishing
   one immutable object generation. Compose the selected parts' chunk-location
   arrays with adjusted logical offsets. Complete does not read part data or
   concatenate it through access-server memory.
4. Persist each uploaded part's raw 16-byte MD5. The multipart ETag is the
   lowercase hexadecimal MD5 of the selected parts' raw MD5 bytes in order,
   followed by `-<part-count>`. Keep the basic single-part ETag rule unchanged.
   Abort and expiry create bounded, idempotent cleanup records.
5. Preserve the basic admission bounds for parallel part traffic and wire
   compatibility for all retry and conflict outcomes.

## Dependencies

- Depends on R152–R166.
- Reuses the basic milestone's publication/recovery, streaming input, logical
  deletion, and integrity contracts.
- Reuses R190's protocol-neutral multipart core; S3 retains its own
  authorization, namespace, wire errors, ETag response, and object publication.
- R170 owns any accelerated multipart transfer and additionally depends on this
  requirement before enabling that operation.

## Acceptance

- Given parts uploaded out of order with part retries, when completion names a
  valid order, assert exact concatenated bytes become visible through one
  generation without gateway concatenation or part reads. Invariant: completion
  is metadata-only, atomic and storage-backed. E2E test.
- Given completed parts with known MD5 values, when completion selects and
  reorders them, assert the ETag uses only the selected raw part MD5 values in
  completion order and the part count suffix. Invariant: multipart ETag matches
  the S3-compatible composite algorithm. Unit test.
- Given missing, duplicated, undersized, checksum-mismatched, or concurrently
  replaced parts, when completion runs, assert no object publishes and exact
  errors are stable. Invariant: only the validated part set can publish.
  Integration test.
- Given response loss during part upload, complete, and abort, when identities
  retry after restart, assert the durable outcome is returned without duplicate
  generations or cleanup. Invariant: every multipart transition is idempotent.
  E2E test.
- Given abort/expiry with readers or cleanup failures, when reconciliation
  runs, assert no active upload is reclaimed and unreachable part data is
  eventually queued within bounds. Invariant: cleanup follows durable upload
  state. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`

## Open Issues

- The referenced R190 requirement is no longer present in the backlog. Both
  adapters now use shared phase names, selected-part validation, accounting and
  metadata-only location composition. Iceberg's remaining session and part
  recovery is bound to its catalog identity and store. Extract the remaining
  protocol-neutral transition decisions while keeping keys, authorization and
  responses in the protocol adapters.
- S3 preserves immutable part generations so completion can publish a selected
  generation across concurrent part-number replacement. An identical durable
  part record retry keeps its revision, but an HTTP retry may stream the same
  bytes to a new location. Losing replacement candidates, replayed stream
  locations and old generations can remain unreachable. Abort, expiry and
  replacement cleanup need durable bounded records and reader-pin protection
  before the HTTP path is enabled.
