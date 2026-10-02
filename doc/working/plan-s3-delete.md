<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Batch Delete Plan

Upstream: [batch deletion requirement](../backlog/R199-s3-batch-delete.md).

Goal: validate a complete bounded request, then apply independent logical deletes and truthful per-key results.

## Tasks

- [x] **Selection and integrity**: add a finite POST bucket delete selector, strict bounded XML, MD5 and observed boto3 CRC32 integrity validation. Reject extensions before mutation. Files: lib/crowdb-access-s3/src/delete.rs, route.rs; tests/delete_test.rs.
- [x] **Orchestration and results**: reuse logical deletion sequentially, retain duplicate input order, collect independent errors and honor Quiet. Add a generic bounded executor for partial-failure and cancellation tests. Files: app/crowdb-access-server/src/s3/operations/delete.rs; S3 wire/metrics.
- [x] **Acceptance**: test 1,000 escaped/unicode keys, duplicates, retries/new publication, malformed/version/checksum request atomic rejection, missing buckets, partial failure, disconnect/restart. Files: library integration tests; app/crowdb-access-server/tests/s3_e2e/ and full-stack harness.
- [x] **Documentation and gates**: document integrity compatibility extension, duplicate ordering and unversioned retry risks; run library/server, real-client, fmt/clippy gates before commit. Files: permanent S3 design.
- [ ] **Cleanup**: remove completed detail/index/plan after acceptance.

## Verification

- Unit/integration: pixi run test-access-s3; pixi run test-access-server.
- E2E: pixi run clean-env; pixi run -e s3-e2e test-boto3-e2e.
- Gates: pixi run rs-fmt-check; pixi run rs-lint.

## Evidence

- The pinned boto3 DeleteObjects serializer sends x-amz-checksum-crc32 and x-amz-sdk-checksum-algorithm=CRC32 even with when_required settings. The finite compatibility surface accepts this verified checksum in place of MD5, while also verifying every supplied MD5/payload SHA256.
- Original copy and batch drafts remain preserved in the named stash; copy is already completed independently.
- Eight focused Rust tests pass, including injected per-key failure and cancellation of a 1,000-key batch with only one operation in flight. Formatting and workspace clippy pass.
- test-boto3-e2e passes the full library/server suites, official embedded-copy-error parsing, and real storage stack: 24 passed, one pre-existing MemTable stress skip. This includes three new batch cases, dropped committed batch response/retry, and persisted absence across access/group0/chunkdb/diskdb/diskio/chunk-KV restarts.
- Authentication currently drops principal identity and selects the listener's configured tenant. The real authority-model decision is recorded separately in R203; batch deletion preserves the existing realm and rejects invalid signatures but cannot certify per-user ACLs.
