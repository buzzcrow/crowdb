<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R204: access-s3 — Client metadata and mounted-file contract

#### Problem

rclone 1.75.1 always writes x-amz-meta-mtime, even with no_system_metadata and
use-server-modtime enabled. Ordinary and multipart uploads fail with
NotImplemented. Ignoring the header would discard requested metadata. s3fs
also requires directory/file attributes and private ACLs absent from the finite
[S3 design](../design/access-server/s3/design-crowdb-access-s3.md).
An actual FUSE mount reaches directory creation, where x-amz-meta-atime returns
NotImplemented and mkdir reports EOPNOTSUPP. This is an API gap, not a missing
host prerequisite. The bounded mount gate retains this reproduction.

#### Solution

1. Define bounded normalized user metadata in ObjectRecord.attributes, published
   atomically with one generation. PUT/HEAD/GET round-trip values; invalid,
   duplicate and oversized metadata fail before mutation. Empty attributes
   remain valid for existing records.
2. Persist initiation metadata in MultipartSessionRecord through replacement
   and recovery, applying it only on completion. Define an explicit migration
   policy before changing its bincode schema. CopyObject COPY/REPLACE selects
   captured source metadata or a completely validated replacement.
3. Trace actual s3fs 1.97 mounts. Define directory markers, mode/uid/gid/mtime
   headers and copy-based rename. Implement required finite bucket/location
   selectors; define private ACL behavior against the selected authority model
   rather than silently accepting ACLs.
4. Run retained rclone transfer/copy/sync/cleanup and s3fs
   create/read/overwrite/list/rename/unlink/remount gates. Gate container release
   with rclone only after its entire declared recipe passes.

#### Dependencies

- R206 selects realm/principal authority; attributes alone cannot certify ACLs.
- R200 retains pinned scripts and verified integrity. AWS CLI and boto3 gates
  progress independently; rclone and s3fs are not accepted yet.
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
- Given FUSE-enabled Linux, run actual file/directory operations and remount;
  assert persisted bytes and documented POSIX limitations. E2E test.
- Given the selected authority model and private ACLs, execute allowed/denied
  requests; assert no privilege changes or silent ACL acceptance. Integration test.

Run pixi run test-access-s3, pixi run test-access-server,
pixi run -e s3-e2e test-rclone-e2e, pixi run -e s3-e2e test-s3fs-required,
pixi run test-single-node-container, pixi run rs-fmt-check, and pixi run rs-lint.

#### Open Questions

- Upgrade existing multipart sessions lazily with a versioned legacy decoder,
  or require uploads to drain first? Lazy decoding preserves active uploads but
  requires explicit legacy schema coverage; draining changes deployment rules.
- Which private ACL and POSIX attributes are valid under R206's authority model?
