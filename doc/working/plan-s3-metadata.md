<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 User Metadata Plan

Upstream: [client user metadata](../backlog/R204-s3-client-metadata.md).

Goal: preserve bounded client metadata across object publication, copy and multipart recovery, then accept the configured rclone workflow.

## Tasks

- [x] **Metadata contract and storage**: add lowercase named, printable ASCII values with a 2 KiB combined key/value limit; reject duplicate names and unsupported operation placement. Encode in ObjectRecord.attributes and a new-version MultipartSessionRecord field. No legacy compatibility. Files: lib/crowdb-access-s3/src/{metadata/user.rs,metadata/multipart.rs,route.rs}.
- [x] **HTTP operations**: validate before writing payload or publishing a session; return metadata on HEAD/GET/ranged GET; COPY retains source attributes and REPLACE selects the supplied map. Multipart publication retains initiation attributes. Files: app/crowdb-access-server/src/s3/operations*, lib/crowdb-access-s3/src/{retrieval.rs,metadata/multipart_repository/publication.rs}.
- [x] **Focused verification**: add normalization, bounds, rejection, generation/copy and durable-session regressions. Run library/server tests, real SDK metadata and rclone recipes, and new-format restart checks. Files: lib/crowdb-access-s3/tests/*, app/crowdb-access-server/tests/s3_e2e/*, s3_full_stack_test.rs.
- [x] **Container acceptance and documentation**: add verified rclone workflow to container release acceptance; document finite ASCII metadata contract and pinned recipe. Run container gate, fmt/clippy and shell/Python checks. Files: container/single-node-container/tests/*, README.md, doc/design/access-server/s3/design-crowdb-access-s3.md.
- [~] **Cleanup**: commit verified implementation; remove completed requirement and this plan; update parent client plan/index. Accumulated storage failure remains independently tracked under R205.

## Verification

- Unit/integration: pixi run test-access-s3; pixi run test-access-server.
- E2E: pixi run clean-env && pixi run -e s3-e2e test-rclone-e2e; focused metadata and restart cases via s3_full_stack_test.
- Container: pixi run test-single-node-container.
- Quality: pixi run rs-fmt-check; pixi run rs-lint.

## Evidence

- Library tests pass, including bounded metadata codec, signed copy selectors and operation placement. Real-stack SDK publication/copy/MPU and invalid-metadata preservation cases pass independently. Fmt/clippy pass.
- Canonical rclone task exposed an environment-selection error in the wrapper: clean-env belongs to default, not the optional client environment. Explicitly select default; resume the same task.
- Canonical rclone run passes all library/server gates and actual ordinary/multipart uploads, then CopyObject fails on x-amz-storage-class. Listings already emit STANDARD; the pinned client's copy implementation retains that source class. Accept only explicit STANDARD on publication operations, retain rejection of other/duplicate selectors and require signed copy selectors. Focused route/copy tests pass; the next rclone run reaches a separate copy validation rejection. Add header/query-name-only diagnostics to identify its first divergence without retaining credentials.
- Copy diagnostics identify the SDK x-id query marker as the remaining rejection. Validate its exact operation and uniqueness in the shared copy parser. Full rclone discovery, ordinary/multipart transfer, listing, server copy, sync/delete, exact downloads and mtime now pass. Compare timestamps as instants because the client emits its local timezone. No checksum suppression or server-modtime workaround remains. Final fmt/clippy pass; restart and container acceptance remain.
- Focused real-stack restart gate passes all six service restarts (group0, access server, ChunkDB, DiskDB, DiskIO and chunk-kv). Ordinary/copied metadata survives each restart; an upload initiated beforehand completes afterward with the original metadata and selected part bytes. Canonical rclone task is being rerun on final source before container acceptance.
- Final canonical rclone task passes, including all library/server tests, official boto3 embedded-copy-error recognition and the real rclone workflow. Container acceptance is running against the rebuilt local image.
- Full pixi run test-single-node-container passes: rebuilt release image, publication policy, runtime linkage, browser acceptance, boto3/AWS CLI/rclone/Iceberg writes and reads, all seven service crash/hang recoveries, persisted-volume restart, restart exhaustion, invalid durable-state rejection and monitor lifecycle. Rclone exact bytes and mtime survive recovery/restart. Image remains local; no publication performed.
