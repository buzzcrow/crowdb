<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R204: access-s3 — Client user metadata

#### Problem

rclone 1.75.1 always writes x-amz-meta-mtime, even with no_system_metadata and
use-server-modtime enabled. Ordinary and multipart uploads fail with
NotImplemented. Ignoring the header would discard requested metadata. The
finite [S3 design](../design/access-server/s3/design-crowdb-access-s3.md)
currently rejects user metadata; rclone requires its durable round-trip.

#### Solution

1. Define bounded normalized user metadata in ObjectRecord.attributes, published
   atomically with one generation. PUT/HEAD/GET round-trip values; invalid,
   duplicate and oversized metadata fail before mutation. Empty attributes
   remain valid for existing records.
2. Persist initiation metadata in MultipartSessionRecord through replacement
   and recovery, applying it only on completion. Define an explicit migration
   policy before changing its bincode schema. CopyObject COPY/REPLACE selects
   captured source metadata or a completely validated replacement.
3. Run retained rclone transfer/copy/sync/cleanup gates. Gate container release
   with rclone only after its entire declared recipe passes.

#### Dependencies

- Use the current shared listener namespace. R206 is deferred pending necessity
  review and is not a prerequisite for metadata persistence.
- R200 retains pinned scripts and verified integrity. AWS CLI and boto3 gates
  progress independently; rclone is not accepted yet.
- Reuse multipart generation fences and immutable publication; no global
  hot-path lock or new physical reclamation policy.

#### Acceptance

- Given bounded mixed-case metadata, PUT/HEAD/GET/copy/overwrite; assert exact
  normalization and generation isolation, and zero mutation for malformed or
  oversized values. Integration test.
- Given old session bytes and new metadata-bearing sessions, restart and
  complete; assert migration preserves uploads and initiated metadata.
  Integration test.
- Given pinned rclone, run discovery, ordinary/multipart transfer, prefix,
  copy, sync and cleanup; assert names/bytes/metadata without dropping headers
  or disabling server integrity. E2E test.

Run pixi run test-access-s3, pixi run test-access-server,
pixi run -e s3-e2e test-rclone-e2e,
pixi run test-single-node-container, pixi run rs-fmt-check, and pixi run rs-lint.

#### Open Questions

- Upgrade existing multipart sessions lazily with a versioned legacy decoder,
  or require uploads to drain first? Lazy decoding preserves active uploads but
  requires explicit legacy schema coverage; draining changes deployment rules.
