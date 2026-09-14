<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R163: access server / S3 — Error mapping and HTTP response compatibility

## Problem

CROWDB metadata, chunk, routing, timeout, and admission failures do not map
directly to S3 status codes. Ad hoc handler mappings produce incompatible XML,
leak topology, or retry unsafe operations after an ambiguous outcome.

The compatibility boundary is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §1.

## Solution

1. Define one S3-local error taxonomy that preserves operation phase,
   retryability, request identity, and whether metadata/chunk outcome is known.
   Convert lower-layer errors once at the library boundary.
2. Serialize stable S3 XML error code, message, resource, request ID, host ID,
   status, and required headers. HEAD errors follow HTTP/S3 header-only rules.
3. Map malformed/unsupported requests, missing bucket/key, range failure,
   precondition failure, conflict, throttling, timeout, unavailable storage,
   and internal corruption without exposing chunk IDs, partitions, nodes,
   racks, disks, paths, or native error text.
4. Reconcile ambiguous mutating outcomes by R154/R159 identities before
   choosing success or failure. Never label an unknown committed outcome as a
   safe fresh retry.
5. Keep request IDs stable through logs/traces while bounding and escaping all
   client-derived error content.

## Dependencies

- Depends on R152's supported surface.
- Consumed by R155–R162 and R164.
- Uses lower-layer typed errors; it does not require Catalog or Dataset to
  share S3 error semantics.

## Acceptance

- Given one representative of every S3-local error class, when serialized for
  object, bucket, range, and HEAD operations, assert exact status, headers, XML
  shape, escaping, and retry classification. Invariant: equivalent failures
  have one stable wire representation. Unit test.
- Given lower-layer errors containing topology and paths, when mapped, assert
  the response exposes none of them while internal logs retain a request-ID
  correlation. Invariant: public errors do not reveal storage topology.
  Integration test.
- Given ambiguous PUT or DELETE completion, when error mapping is requested,
  assert reconciliation runs first and returns the durable outcome. Invariant:
  wire errors cannot undermine mutation idempotency. Integration test.
- Given oversized or malformed client values, when included in an error path,
  assert response size remains bounded and XML is valid. Invariant: error
  handling is not an amplification or injection path. Unit test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
