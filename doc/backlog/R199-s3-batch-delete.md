<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R199: access-s3 — Multi-object deletion

#### Problem

The general S3 service implements DeleteObject but has no DeleteObjects route.
Clients that batch cleanup or synchronization cannot use POST bucket `?delete`;
falling back to individual requests increases round trips and may require
client changes. The [S3 design](../design/access-server/s3/design-crowdb-access-s3.md)
requires logical deletion before reclamation and explicit unsupported errors.

#### Solution

DeleteObjects composes the existing logical deletion operation. A batch is
not a transaction: each key has its own result. Whole-request validation occurs
before deletion, while valid requests may have per-key failures.

1. Extend `lib/crowdb-access-s3/src/route.rs` and the S3 wire/error layer for
   POST bucket `?delete`, with an explicit operation rather than ordinary PUT.
   Parse XML with bounded body size, depth, key length, and at most 1,000 keys.
   Honor Quiet and require verified integrity for the supported general bucket
   surface. Accept Content-MD5 and the CRC32 header emitted by the pinned boto3
   serializer; CRC32 in place of MD5 is an explicit compatibility extension.
   Verify every declared checksum and the signed payload hash. Reject malformed XML, bad checksums, excessive keys, and
   unsupported version-ID requests before any key is deleted.
2. Add batch orchestration using `lib/crowdb-access-s3/src/object.rs` and
   `app/crowdb-access-server/src/s3/operations.rs`. Authenticate the bucket and
   apply the same per-key authorization as DeleteObject. Use bounded concurrency
   and the same ordered unconditional point mutations as DeleteObject, without
   a new global lock or a preliminary per-object read/CAS.
3. Return S3 DeleteResult XML with escaped keys and per-key Deleted/Error entries.
   Missing objects are successful deletes. Quiet suppresses successes but
   retains errors. A request-wide authentication/bucket/parse failure remains
   a request-level S3 error, not a successful empty batch.
4. Define duplicate-key results according to the supported AWS contract and
   make request retries safe for absent keys. Document that retries after a
   concurrent new PUT can delete that new object, as with ordinary unversioned
   deletion; do not claim exactly-once deletion across response loss.

#### Dependencies

- R203 defines per-principal namespace/grant authority. Batch deletion preserves
  the current configured-listener realm and does not add or certify user ACLs.

- Existing DeleteObject publication and cleanup behavior is reused; this does
  not implement versions, delete markers, or new physical reclamation rules.
- R168/R169 continue to own shared physical reclamation. Batch logical deletion
  can land independently using the same fallback as single-object deletion.
- R200 exercises client cleanup; boto3 DeleteObjects is the independent baseline.

#### Acceptance

- Given existing and absent keys, issue a valid batch; assert matching per-key
  successes, subsequent absence, and unrelated objects retained. Logical deletion.
  E2E test.
- Given Quiet true and a mixed-success batch with an injected per-key storage
  failure, delete; assert only errors are returned and successful keys remain
  deleted without rollback. Partial-result fidelity. Integration test.
- Given empty/malformed XML, 1,001 keys, exceeded parser bounds, invalid integrity
  headers, or version IDs, delete; assert request errors and zero mutations.
  Validate before mutation. Integration test.
- Given exactly 1,000 keys and escaped/unicode names, delete and parse the
  response with boto3; assert the full supported limit and exact key identity.
  Wire fidelity. E2E test.
- Given an invalid signature and a missing bucket, issue a batch; assert
  request-level AccessDenied/NoSuchBucket errors and no mutation. Admission.
  E2E test.
- Multi-principal bucket authorization is deferred to R203 because the existing
  authentication interface does not propagate that identity to operations.
- Given duplicate keys, a repeated request, and a concurrent PUT, execute
  deletion; assert documented duplicate and retry results and whole-generation
  visibility matching ordinary DeleteObject semantics. Publication ordering.
  Integration test.
- Given a slow 1,000-key batch and a disconnect/restart after some deletions,
  observe work limits and retry; assert bounded concurrency and convergence
  for keys without intervening writes. Bounded recovery. Integration test.

Run `pixi run test-access-s3`, `pixi run test-access-server`,
`pixi run -e s3-e2e test-boto3-e2e`, `pixi run rs-fmt-check`, and
`pixi run rs-lint` for the implemented scope.
