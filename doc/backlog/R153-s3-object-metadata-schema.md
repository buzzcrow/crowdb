<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R153: access server / S3 — Bucket namespace and object metadata schema

## Problem

S3 operations need a durable namespace that supports binary object keys, point
lookup, prefix listing, overwrite generations, and continuation across
Chunk-KV partitions. Chunk storage does not define bucket authority or S3
attributes, while storing protocol metadata in chunk layouts would couple S3
to physical placement.

The root architecture is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §2.

## Solution

1. Define versioned FlatBuffer records and key builders under
   `lib/crowdb-access-s3` for tenant-owned bucket identity and state, object
   identity, immutable object generation, current-generation visibility, and
   upload identity.
2. Encode keys as a schema/version prefix, tenant ID, stable bucket ID, record
   kind, and length-delimited binary object key. Preserve bytewise ordering so
   one bucket and prefix map to a bounded Chunk-KV scan interval without using
   UTF-8 or delimiter assumptions.
3. Store logical length, checksum/ETag data, creation/modification time,
   content type, user-visible supported attributes, publication generation,
   upload ID, and an opaque chunk-client data reference. Physical segment,
   disk, node, rack, and EC layout remain chunk-client responsibilities.
4. Make every overwrite a new immutable generation. The visibility record
   selects exactly one generation or an absent tombstone; readers retain the
   selected generation for their whole operation.
5. Reject malformed, unsupported-version, cross-tenant, oversized-key, and
   inconsistent-length records before storage access. Unknown optional fields
   round-trip when the schema permits forward compatibility.
6. Define S3 bucket create, head, tenant-scoped list, and delete transitions.
   A stable bucket ID backs each unique name; create is idempotent for its
   owner, and delete uses a fenced emptiness check against the object-key
   interval so a concurrent PUT cannot publish into a deleted bucket. Bucket
   rename is outside initial scope.

## Dependencies

- Depends on R152 for the S3 crate and compatibility boundary.
- Uses Chunk-KV ordered keys and routed point/scan operations.
- R154, R156, R158, and R159 consume the schema.
- Does not depend on R92 or R95; physical object deletion is defined by R159.

## Acceptance

- Given binary keys containing zeroes, slashes, non-UTF-8 bytes, and maximum
  supported length, when encoded and decoded, assert identity and ordering are
  preserved. Invariant: object identity is byte-exact. Unit test.
- Given several tenants, buckets, and prefixes, when metadata keys are sorted,
  assert each bucket-prefix interval is contiguous and cannot include another
  tenant or bucket. Invariant: listing bounds preserve namespace isolation.
  Unit test.
- Given two overwrites of one object, when both generation records exist,
  assert the visibility record names one immutable generation and a reader of
  the old generation retains its original data reference. Invariant: overwrite
  never mutates a generation being read. Integration test.
- Given an unknown schema version or inconsistent logical length/data
  reference, when decoded, assert the request fails before a chunk read or
  mutation. Invariant: corrupt metadata cannot become storage authority.
  Unit test.
- Given bucket creation retries and concurrent final-object publication versus
  bucket deletion, when metadata compares commit, assert one stable bucket ID
  is returned and either the object wins in a live bucket or empty deletion
  wins with publication rejected. Invariant: no visible object belongs to a
  deleted bucket. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
