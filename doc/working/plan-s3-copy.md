<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Server-Side Copy Plan

Upstream: [copy requirement](../backlog/R198-s3-server-side-copy.md).

Goal: copy one captured object generation through bounded storage readers/writers and publish atomically.

## Tasks

- [x] **Copy selection**: explicit CopyObject/UploadPartCopy routes, strict source addresses, ranges, directives and conditions are covered by passing focused tests; unsupported headers fail before mutation. Files: lib/crowdb-access-s3/src/copy.rs, route.rs; copy and route tests.
- [x] **Storage orchestration**: source/destination use the configured tenant, captured immutable locations stream with writer capacity, and publication reuses object/multipart fences; response/cancellation tests pass. Files: app/crowdb-access-server/src/s3/operations/copy.rs, multipart.rs; s3/copy_body.rs.
- [x] **Protocol acceptance**: boto3 copies, metadata/self-copy, encoded keys, conditions, ranges, part replacement, disconnect, response loss and restart passed; stable generation and embedded-error behavior passed. Files: app/crowdb-access-server/tests/s3_e2e/; Rust integration tests.
- [x] **Permanent contract and gates**: supported metadata, size limits, streaming and response timing are documented; fmt/clippy, full library/server and real-client gates passed. Files: doc/design/access-server/s3/design-crowdb-access-s3.md.
- [x] **Cleanup**: commit verified implementation, then remove completed requirement, index entry and this plan.

## Verification

- Unit/integration: pixi run test-access-s3; pixi run test-access-server.
- E2E: pixi run clean-env; pixi run -e s3-e2e test-boto3-e2e.
- Gates: pixi run rs-fmt-check; pixi run rs-lint.

## Preserved work

- The named stash retains copy and batch-delete drafts. Only copy files are extracted for this requirement; batch deletion remains untouched until copy is complete.

## Evidence

- Focused copy/route/response tests passed, including size limits, signed headers, token syntax, timeout and body-drop cancellation.
- Clippy passed after normal lint repairs. The metrics fixture was updated from 15 to 17 operations; its namespace/credential cardinality checks still pass.
- Full client acceptance passed: 21 cases and one existing MemTable stress skip. It includes the production embedded-error response parsed by boto3, source replacement/deletion, disconnect/retry, object and part copy response-loss retry, and copied-object/part recovery across six service restarts.
- The first full client run passed all three copy scenarios and the original HTTP matrix, then exposed a test-scope mismatch: cumulative large-write copy counts include intentional storage-to-storage copies. Acceptance now records their baseline and asserts HTTP uploads add zero copies, before the copy response-loss scenario. No upload assertion is relaxed.
