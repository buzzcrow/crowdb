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
   listing operations as a separate S3-owned state machine.
2. Store immutable part identities and integrity records durably; a retried
   part number replaces only that part's selected generation and schedules old
   private data for cleanup.
3. Complete with one fenced metadata transaction that validates ordered part
   identities, sizes, checksums, and expected upload state before publishing
   one immutable object generation. No concatenation through access-server
   memory is allowed.
4. Define multipart-specific ETag/checksum behavior without changing the basic
   single-part generation rules. Abort and expiry create bounded,
   idempotent cleanup records.
5. Preserve the basic admission bounds for parallel part traffic and wire
   compatibility for all retry and conflict outcomes.

## Dependencies

- Depends on R152–R166.
- Reuses the basic milestone's publication/recovery, streaming input, logical
  deletion, and integrity contracts.
- R170 owns any accelerated multipart transfer and additionally depends on this
  requirement before enabling that operation.

## Acceptance

- Given parts uploaded out of order with part retries, when completion names a
  valid order, assert exact concatenated bytes become visible through one
  generation without gateway concatenation. Invariant: completion is atomic
  and storage-backed. E2E test.
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
