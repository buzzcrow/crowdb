<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R158: access server / S3 — ListObjectsV2 ordering and continuation tokens

## Problem

Object metadata is ordered in Chunk-KV, but one bucket scan can cross routed
partitions and partition ownership can change between pages. An opaque token
that captures gateway-local state would break stateless scale-out. Claiming a
global snapshot would require a read-fence mechanism that Chunk-KV does not
provide.

The listing contract is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §5.

## Solution

1. Implement `ListObjectsV2` as a bounded ordered Chunk-KV scan over R153's
   tenant and bucket interval, with binary-safe prefix, delimiter, start-after,
   maximum-key, and encoding behavior.
2. Return a signed, versioned, opaque continuation token containing bucket
   identity, normalized request parameters, last emitted key/common prefix,
   and enough routed-scan position to resume. Do not store token state in an
   access-server instance.
3. Make listing explicitly non-snapshot. Keys that remain visible and unchanged
   for the complete traversal are neither duplicated nor skipped. Concurrent
   creates, overwrites, and deletes may appear or be absent according to the
   page on which they become visible.
4. Re-resolve partition ownership after split, transfer, or stale-route errors
   and resume strictly after the last emitted item. Reject expired, malformed,
   wrong-bucket, and parameter-mismatched tokens before scanning.
5. Bound scanned records, response bytes, metadata retries, and retained key
   memory independently of bucket size.

## Dependencies

- Depends on R152 and R153.
- Uses the routed ordered-scan and retry contract in
  `crowdb-chunk-kv-client`.
- R160 provides stateless frontend routing; listing itself must remain
  stateless without it.

## Acceptance

- Given binary keys, prefixes, and delimiters spanning several Chunk-KV
  partitions, when all pages are consumed, assert strict byte ordering and the
  exact expected objects/common prefixes. Invariant: protocol pagination
  preserves metadata key order. E2E test.
- Given a partition split or ownership transfer between pages, when a token is
  resumed through another access server, assert every unchanged key appears
  once with no gateway-local state. Invariant: routing changes do not break
  stable-key continuation. E2E test.
- Given creates and deletes between pages, when listing resumes, assert the
  documented non-snapshot outcomes and never claim a snapshot version.
  Invariant: concurrency semantics are explicit and internally consistent.
  Integration test.
- Given malformed, expired, cross-bucket, or parameter-mismatched tokens, when
  listing starts, assert rejection occurs before a metadata scan. Invariant:
  tokens cannot escape their authorized scan interval. Unit test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
