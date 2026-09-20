<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R180: access server / Iceberg — Native immutable files and FileIO

## Problem

Iceberg metadata, manifest lists, manifests, data files, delete files, deletion
vectors, and statistics files are immutable objects with format-specific range-read
patterns. General S3 authority permits overwrite and delete that Iceberg cannot
permit. Storing complete files, chunk vectors, footers, or metadata graphs in
Chunk-KV would also make records and reads unbounded.

R177 requires native file authority, an Iceberg-owned S3-shaped surface, durable
multipart in the first writable milestone, and generation-local metadata
projections. This requirement implements that foreground storage contract. R183
owns physical reclamation.

## Solution

- **FILE-I1 — Byte immutability:** a published location resolves forever to one
  FileId, length, digest, and byte sequence.
- **FILE-I2 — Bounded records:** a file record contains bounded inline bytes or one
  bounded chunk root plus fixed-size hints, never a growing chunk vector or footer.
- **FILE-I3 — Streaming scale:** upload, download, range read, multipart completion,
  Avro decode, and format probing use bounded windows independent of file length.
- **FILE-I4 — Native authority:** the Iceberg FileIO surface authorizes a table
  prefix and immutable files; it never reads or writes general S3 metadata.
- **FILE-I5 — Canonical fallback:** projections and format hints may avoid work but
  canonical bytes are the only file authority.

1. Add `file/id.rs`, `key.rs`, `record.rs`, `repository.rs`, `writer.rs`,
   `reader.rs`, `location.rs`, `multipart.rs`, and `s3_compat.rs`. A table location
   is `s3://iceberg-<catalog-id-base32>/t/<table-id-hex>/`; an exact client-created
   relative key below that prefix maps once to a server FileId. Names and namespace
   paths never enter the location. Reject bucket, table, or path escape and never
   normalize two different S3 keys into one identity.
2. Store metadata JSON, manifest lists, and manifests with an inline-or-chunk
   variant. Stored inline payload is at most 16 KiB; only original input at most
   64 KiB may be tested for LZ4 compression. Data, position/equality delete,
   deletion-vector, and statistics files always use chunk storage regardless of
   size.
3. Publish only after complete bytes, digest, length, file kind, content format,
   and fixed-size format hint are verified. A retry of the same location with the
   same digest returns the existing result; different bytes return conflict.
   Published overwrite is impossible.
4. Implement immutable PUT, HEAD, and GET with one contiguous range. PUT streams
   directly into bounded chunk writers; GET retains one FileRecord and applies
   response credits so slow clients bound prefetch. Unsupported S3 operations
   return stable S3-shaped errors without mutation.
5. Implement durable create, upload-part, list-parts, complete, and abort multipart
   state. Bound sessions, parts per session, part bytes, aggregate staged bytes,
   TTL, reconciliation pages, completion work, and retries. Complete publishes one
   immutable file or returns the prior result; abandoned parts are R183 candidates.
6. Issue short-lived delegated credentials restricted to CatalogId, TableId, exact
   operation set, table prefix, byte limits, expiry, and nonce. File DELETE is not
   delegated and has no public S3 route; only R183 can authorize physical removal.
7. Add `metadata_projection/` with generation-local root, bounded pages, and child
   JSON objects qualified by TableId, metadata generation, JSON digest, and
   projection version. Missing, partial, corrupt, or oversized projections fall
   back to byte-identical metadata JSON and never block publication.
8. Stream manifest lists and manifests by Avro blocks. Validate v1/v2/v3 inheritance
   rules, sequence and row-ID fields, data/delete content, deletion-vector
   descriptors, and metrics without building an unbounded entry vector.
9. Store only fixed-size Parquet, ORC, Avro, and Puffin location hints verified at
   seal time. Invalid hints trigger bounded probing of canonical bytes. Parsed
   footers, stripe directories, block directories, and pages are memory-only R185
   cache entries until a separate measured requirement approves persistence.

## Dependencies

- Depends on R177 and R178 for identity, capability, key/value, active context,
  authentication, and chunk clients.
- Supplies immutable file identities, canonical locations, metadata projection,
  and delegated-access contracts to R181, R182, R183, and R184.
- R183 owns staged, orphan, expired, and unreachable physical cleanup. Before R183,
  such data may leak but can never become visible through a published location.
- R185 owns decoded caches. All reads remain correct when every cache is disabled.

## Acceptance

- Given inline boundaries at 16 KiB and compression-input boundaries at 64 KiB,
  when compressible and incompressible metadata, manifest, data, delete, deletion
  vector, and statistics files are written, assert the required variant is selected
  and every read returns identical bytes. Invariants: FILE-I1 and FILE-I2. Unit test.
- Given files spanning chunk boundaries and clients with arbitrary backpressure,
  when full and one-range GETs run, assert returned bytes and status are correct and
  retained memory and prefetch remain within configured windows. Invariant:
  FILE-I3. Integration test.
- Given two PUTs to one location with equal or different bytes plus response loss,
  when they retry across Access Servers, assert equal content returns one FileId and
  different content conflicts without overwrite. Invariant: FILE-I1. E2E test.
- Given multipart upload crash points, duplicate parts, completion retries, abort,
  and TTL expiry, when recovery resumes, assert at most one immutable file publishes
  and all state and work stay within independent limits. Invariants: FILE-I1 and
  FILE-I3. Integration test.
- Given valid, missing, partial, corrupt, wrong-version, and wrong-digest metadata
  projections, when load requests need selected and complete metadata, assert valid
  pages avoid full decode and every invalid case falls back to byte-identical JSON
  without changing authority. Invariant: FILE-I5. Integration test.
- Given v1, v2, and v3 manifests containing sequence, row-lineage, position/equality
  delete, and deletion-vector cases, when blocks stream across chunk boundaries,
  assert entries follow the version rules and memory does not grow with total
  entries. Invariant: FILE-I3. Integration test.
- Given official S3 FileIO behavior, when allowed operations, bucket CRUD,
  overwrite, path escape, tagging, lifecycle, and DELETE are attempted, assert only
  the declared table-prefix operations succeed and general S3 objects remain
  isolated. Invariant: FILE-I4. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
