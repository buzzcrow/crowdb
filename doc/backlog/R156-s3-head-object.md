<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R156: access server / S3 — HeadObject and object attribute contract

## Problem

Clients need a cheap, consistent way to discover object length, ETag,
modification time, content type, and supported checksum attributes without
opening chunk data. Returning attributes assembled from mutable or physical
layout state could disagree with a concurrent GET or overwrite.

The generation contract is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §§2 and 5.

## Solution

1. Resolve bucket and binary key to one immutable visible generation through
   `crowdb-chunk-kv-client`; do not contact chunk storage for a normal HEAD.
2. Map only attributes persisted in that generation to the R152 compatibility
   surface. Content length, ETag, supported checksum, last-modified, content
   type, and generation-derived condition evaluation come from one record.
3. Evaluate supported conditional headers against that generation. A
   concurrent overwrite after resolution cannot change the response being
   constructed.
4. Return the same not-found, authorization masking, malformed condition, and
   internal metadata errors defined by R162/R163. Never expose physical chunk,
   disk, EC, node, or rack information.
5. Bound metadata deadlines and retries. Reconcile ambiguous routing changes
   through the routed Chunk-KV client rather than falling back to data reads.

## Dependencies

- Depends on R152 and R153.
- Uses R163 error mapping and R164 ETag/checksum definitions when available.
- Does not depend on R155 data streaming or chunk availability for a valid
  metadata record.

## Acceptance

- Given a published generation, when HEAD runs, assert returned length, ETag,
  checksum, timestamps, and content type exactly match that generation and no
  chunk read occurs. Invariant: HEAD is metadata-only. Integration test.
- Given an overwrite races after generation resolution, when HEAD completes,
  assert all headers come from either the old or new generation, never a mix.
  Invariant: attributes have one-generation consistency. Integration test.
- Given missing, tombstoned, unauthorized, and malformed conditional requests,
  when HEAD runs, assert stable status/headers and no storage topology leaks.
  Invariant: HEAD preserves namespace and authorization boundaries. Integration
  test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
