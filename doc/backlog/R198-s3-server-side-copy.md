<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R198: access-s3 — Server-side object and multipart copy

#### Problem

The general S3 route enum has no `CopyObject` or `UploadPartCopy` operation.
A PUT selected by `x-amz-copy-source` must not fall through to ordinary upload
handling. S3 applications cannot request server-side duplication or assemble
large objects by copying existing object ranges. A rename workflow needs copy
followed by a separate delete; copy itself must never delete the source.

The [S3 design](../design/access-server/s3/design-crowdb-access-s3.md)
requires atomic publication, stable reads, bounded streaming, and independent
S3 authority. Copy must preserve those properties under overwrite and recovery.

#### Solution

Use the existing authenticated S3 listener and storage publication paths.
Capture one immutable source generation and retain its read authority until
copy finishes. Initially stream source bytes through bounded existing readers
and destination writers. This avoids introducing shared-reference lifetime or
GC assumptions. Metadata-only copies may be a later optimization after their
reference and reclamation contract is proven.

1. Extend `lib/crowdb-access-s3/src/route.rs` and `route/multipart.rs` to
   distinguish copy headers from ordinary uploads. Validate percent-encoded
   source bucket/key, source conditions, range syntax, and destination scope.
   Reject unsupported version IDs and extension headers before mutation.
2. Add source selection and copy semantics in `lib/crowdb-access-s3/src/object.rs`,
   `retrieval.rs`, and `publication.rs`. Authorize both source read and
   destination write. Concurrent source overwrite/delete cannot mix generations.
   Implement AWS-compatible COPY/REPLACE handling for the metadata supported
   by ordinary PUT; reject unsupported directives. Handle same-key copy using
   AWS rules, including rejection of a no-change self-copy.
3. Extend `app/crowdb-access-server/src/s3/operations.rs` and
   `s3/operations/multipart.rs` to return copy XML, ETags, timestamps, and
   protocol errors. Range copy is for UploadPartCopy; CopyObject does not
   silently accept a partial range. CopyObject enforces the AWS single-copy
   size limit; larger objects use multipart copy.
4. Reuse durable multipart part replacement and completion. An interrupted
   copy publishes no partial object or part. Bound buffers and concurrency
   independently of object size; losing candidates follow existing reclamation.
   For failures after response headers are sent, emit the S3 embedded error
   form rather than reporting apparent successful publication.

#### Dependencies

- Existing general S3 authentication, retrieval, publication, multipart, and
  boto3 real-storage tests are the baseline; Iceberg FileIO is a separate scope.
- R168/R169 shared reclamation are not prerequisites for a streamed copy.
  Do not add metadata-only sharing while those reference lifetimes are unresolved.
- R200 supplies real-client copy workflows; use boto3 directly before it lands.

#### Acceptance

- Given source objects in the same and another authorized bucket, CopyObject
  to new and existing destinations; assert exact bytes, COPY/REPLACE metadata,
  correct result XML, and an unchanged source. Atomic publication. E2E test.
- Given encoded keys and source conditions, copy matching and mismatching
  sources; assert correct decoding and condition failures without destination
  mutation. Request fidelity. Integration test.
- Given another principal's source or destination, attempt copy; assert neither
  read nor publication is permitted across authorization scope. Isolation. E2E test.
- Given source overwrite/delete during a bounded slow copy, finish the copy;
  assert one complete captured generation or a pre-selection failure, never
  mixed bytes. Stable read. Integration test.
- Given a self-copy, perform both an unchanged COPY and supported metadata
  REPLACE; assert AWS-compatible rejection/success with unchanged payload.
  Metadata semantics. E2E test.
- Given CopyObject over its size limit, invalid ranges, version selectors, and
  unsupported directives, submit requests; assert explicit errors and no new
  destination generation. Fail closed. Integration test.
- Given an existing multipart session, copy full and ranged source bytes into
  parts, replace a part, then complete; assert selected byte ordering and ETags.
  Multipart selection fence. E2E test.
- Given missing source/upload IDs or invalid part/range bounds, UploadPartCopy;
  assert protocol errors without changing the current part. Fail closed. E2E test.
- Given a copy interrupted before publication or a response lost after commit,
  restart/retry; assert no partial visibility, correct final bytes, and settled
  multipart state. Recovery safety. E2E test.
- Given large copies and slow storage, observe buffer credits and inject a
  failure after headers; assert bounded ownership and an embedded error that
  the SDK recognizes. Bounded progress and truthful completion. Integration test.

Run `pixi run test-access-s3`, `pixi run test-access-server`,
`pixi run -e s3-e2e test-boto3-e2e`, `pixi run rs-fmt-check`, and
`pixi run rs-lint` for the implemented scope.
