<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R234: access server / Iceberg — Native storage path and SDK-compatible file access

## Problem

Iceberg table files currently use a mixed location model: table identity is
encoded in one location form while file authority and parquet data can be
resolved through unrelated keys or directories. This makes the KV layout hard
to inspect, leaves rename and cross-namespace move invariants implicit, and
adds avoidable metadata work before a native file read. The root design is
[`design-crowdb-iceberg.md`](../design/access-server/iceberge/design-crowdb-iceberg.md)
and the execution details are in
[`plan-iceberg-storage-path.md`](../working/plan-iceberg-storage-path.md).

The public contract cannot add a second chunk-location field: Iceberg clients
consume one `data_file.file_path` URL and third-party SDKs cannot be changed.
The URL must remain S3-shaped for compatibility while the bytes remain owned
by native `ChunkType::IcebergTable` storage. A client or server must also not
be able to list the whole Iceberg bucket by omitting a prefix.

## Solution

The requirement establishes these invariants:

- **ICEBERG-PATH-1 — Immutable identity path:** every table-owned path starts
  with `<catalog-id>/<storage-namespace-id>/<table-id>/`. The namespace ID is
  captured at table creation. Rename and cross-namespace move update the live
  name mapping only; they do not move file bytes or rewrite file URLs.
- **ICEBERG-PATH-2 — Fixed compatibility bucket:** every public URI uses the
  fixed virtual bucket `iceberg`; the three IDs are object-key segments. A
  table ID is never used as a bucket.
- **ICEBERG-PATH-3 — One public file URL:** Iceberg `file_path` is the only
  SDK-visible file location. It uses the fixed bucket and immutable table
  prefix. Native injection can issue a direct locator after durable upload,
  before constructing manifests. Unmodified SDK writers keep their chosen
  logical URL and resolve its native file record. Native injection determines
  the final URL before manifest construction; readers need no additional field.
- **ICEBERG-PATH-4 — One file record:** each published file has exactly one
  `file/<public-relative-filename>` record under the immutable table prefix.
  The filename is also the public URL suffix and list key suffix. Native
  injection can use `f/v1/<readable-stem>.<locator>.parquet`; the authenticated
  locator includes a unique file identity, so repeated readable stems do not
  collide. The value contains file ID, length, integrity metadata, format,
  native descriptor and lifecycle state. No path/public/id secondary file
  indexes are created. List, inspection, recovery and GC use this same record.
- **ICEBERG-PATH-5 — Table-scoped list:** ListObjectsV2 accepts only a complete
  table prefix `<catalog-id>/<storage-namespace-id>/<table-id>/` belonging to
  the authorized request. Empty, catalog-only, namespace-only, foreign-table,
  and bucket-wide prefixes are rejected. `prefix`, `delimiter`, `max-keys`,
  continuation, start-after, URL encoding, common prefixes, and truncation
  fields retain S3 behavior.
- **ICEBERG-PATH-6 — Native lifecycle:** file publication binds one immutable
  file identity to one canonical public URL. Logical deletion removes the file
  from the selected index/list and schedules delayed GC. A holder of its direct
  URL with a valid grant may still read retained bytes until physical reclamation.
  Direct reads do not query a file tombstone or location mapping.
  Retained chunk IDs and offsets remain stable until deletion and reclamation.
- **ICEBERG-PATH-7 — SDK write compatibility:** ordinary SDK location providers
  choose names locally before PUT; PUT admission cannot change that URL to
  contain locations allocated later. Preserve the original URL for these
  files. An internal injection operation may return one final direct URL after
  publication; consumers put that URL into manifests using standard fields.
- **ICEBERG-PATH-8 — Locator classes:** logical URLs use the public-key file
  index; direct URLs carry a versioned authenticated native descriptor or a
  chunk-resident directory root. A token resolved through another KV mapping
  is not a direct locator. Both classes bind the full table identity. Locator
  authenticity is separate from access authorization: the persistent URL has
  no credential expiry, while each HTTP request requires a valid grant.
- **ICEBERG-PATH-9 — No hidden rewrite:** metadata and manifest references are
  kept byte-for-byte at the SDK-supplied URL. Commit publication does not scan
  or rewrite manifests. Direct URLs are chosen before the first manifest is
  constructed. Existing data-file names, position-delete paths, deletion-vector
  references and historical snapshots are never silently converted to aliases.
- **ICEBERG-PATH-10 — Versioned migration:** the new slash-component layout is
  encoded as a new key-layout version. Existing v1 binary keys remain readable
  by the legacy decoder and are never reinterpreted as v2. New catalogs use v2;
  existing catalogs retain v1 reads, writes and URLs. In-place conversion is
  outside this requirement; activation must reject attempts to relabel a v1
  catalog as v2. Never rewrite historical metadata just to change its keys.
- **ICEBERG-PATH-11 — Control-plane isolation:** `iceberg` is one virtual
  bucket, not a general bucket authority. HeadBucket and region/endpoint
  discovery are satisfied by the Iceberg listener and table grant; bucket
  creation/deletion and unscoped ListBuckets/ListObjects are rejected. Object
  operations require a complete table prefix and a grant for that immutable
  storage path.

The model is defined in the permanent Iceberg design; the working plan tracks
implementation and verification. Documentation agreement is not evidence that
the current runtime already implements the new layout or direct-read path.

The request path is:

```text
Iceberg file_path URL
    -> fixed bucket and three-ID table scope
    -> validated native file locator
    -> chunk read or table-scoped KV index operation
    -> IcebergTable chunk storage
```

1. Add one canonical identity/path codec for catalog ID, storage namespace ID,
   table ID, and safe relative file keys. Use it for table metadata, file
   authorities, file-by-path indexes, multipart records, GC records, grants,
   and continuation tokens. Reject traversal, malformed encodings, aliases,
   and identity mismatches.
2. Assign the immutable storage path during normal and staged table creation.
   Persist the fixed path in the table head and initial metadata. Keep the
   live `(namespace, name)` mapping separate so rename, move, drop, and
   recreate cannot alias retained files.
3. Move all table-owned KV records below the canonical identity prefix. Publish
   one `file/<public-relative-filename>` record with a create-only CAS after
   native bytes are durable. Preserve the selected record on collision;
   replay the same upload identity/result, and reject conflicting uploads.
   Carry the canonical filename in lifecycle tasks rather than adding an ID
   index. Bound every scan to the table prefix.
4. Keep the native Iceberg chunk type and `FileBlockStore` as the byte owner.
   In native injection, finalize bytes and a bounded read descriptor, durably
   publish the file, then return its one direct URL. Ordinary PUT/multipart
   publishes a descriptor under its original URL. Implement exact GET, HEAD,
   Range GET and cleanup for both classes without silently switching names.
5. Validate the fixed bucket and complete table prefix at the HTTP/FileIO
   boundary. Do not expose bucket-wide `ListBuckets` or unscoped object list
   results. For a missing exact object, support the SDK fallback list of
   `<object-key>/` without widening the authorization scope.
6. Return list results from the native file index with public `file_path` keys,
   not by scanning chunk placement. Preserve Arrow/PyArrow directory behavior:
   a `FileSelector` base directory becomes a key prefix with one trailing `/`,
   while an exact file first uses HEAD and may list `<key>/` only as a directory
   fallback. Arrow's S3 implementation derives the bucket from the URI host
   and the key from the URI path; it does not infer catalog or namespace from
   the bucket.
7. Align the loader and container injection flow with the returned canonical
   URI. Any optional general-S3 mirror remains a separate demo artifact and is
   not an Iceberg file authority.
8. Reconcile the permanent Iceberg design and upload-flow documents with this
   contract. In particular, replace the older bucket/table-prefix examples,
   state that commit does not rewrite manifest paths, and define the direct
   chunk-read API as separate from Iceberg file-record lookup. Do not mark R234
   complete while those documents disagree with the implemented contract.

Defined edge behavior:

- Catalog/name mappings and table head are sufficient to find the complete
  storage identity; no table-ID reverse index is required. One file-record
  type covers metadata JSON, manifest-list, manifest, data/delete and statistics
  files. Snapshots and refs remain standard JSON fields, including the v1
  direct-manifest compatibility branch. No extra snapshot authority is added.
- Large metadata is a native chunk-backed object. Head and file values stay
  bounded; the full snapshot array belongs in the canonical metadata JSON.
  Existing validated generation projections may accelerate REFS reads but are
  disposable. ALL responses and format-version requirements stay intact.
- Public `metadata/` and `statistics/` prefixes map directly to the equivalent
  bounded `file/` KV ranges. List pages never decode those object payloads,
  accumulate the complete table or scan unrelated file kinds. Snapshot
  enumeration inside JSON is separate from file listing. Metadata/parser/
  response budgets reject oversize input explicitly, never truncate history.
- Head publication follows durable standard files and validates the prior
  generation. Retained snapshots, branches and tags protect shared files from
  GC. Removing an entry from the current snapshot alone proves no deletion right.

- The three-ID path is the payload inside Iceberg storage; omit a redundant
  `iceberg/` component. Catalog/namespace/name control records use separate
  scopes. Live name lookup returns both table ID and storage namespace ID.
  Rename may update live identity/epoch fields, but never the head key or file
  locations. Removing an old namespace cannot delete a moved table's files.
- Keys retain the existing 1,024-byte bound and relative UTF-8 rules; HTTP
  percent decoding occurs only at the transport boundary. Reserved `f/` paths
  are issued only by native publication. Direct directory roots handle files
  with many extents without an unbounded URL. Locator signatures retain their
  verification keys for the file lifetime and never substitute for HTTP grants.
- List enumerates selected immutable public objects, including files awaiting
  commit, not snapshot reachability. It excludes unfinished/losing/deleted
  records and never emits an extra alias for a direct file. Return object keys,
  not full URLs. Retain existing bounded pages, live forward traversal and
  grant-bound continuation tokens; a refreshed credential starts a new scan.
- Preserve conditional HEAD/GET, single-range reads and create-only PUT.
  Identical retry requires content identity, not merely equal length. Multipart
  completion selects one ordered part set and replays after response loss;
  abort cannot remove a published file. Explicit cleanup grants and retained
  reference checks govern single/batch deletion. Snapshot expiration alone
  does not delete physical bytes. Copy and bucket mutations remain unsupported.
- Existing table-bound grants survive rename/move until normal expiry or
  revocation. New grants resolve the live name. Branches, tags, time travel,
  rollback and compaction preserve existing file URLs and retained native
  extents. Recreated names and reused physical offsets cannot revive old URLs.

## Dependencies

- Depends on the existing native Iceberg table, FileIO, chunk, and KV
  authority designs listed in the working plan, plus a chunk read operation
  that accepts a validated complete native locator without resolving a file
  record through the Iceberg KV server.
- Changes direct-file deletion semantics explicitly: metadata/list visibility
  ends at logical deletion, while retained bytes remain addressable until GC.
  Existing logical-URL resolution may still reject its tombstone. Direct GET,
  HEAD and Range GET must not use that resolver or add per-file state queries.
  Grant signature/scope/expiry validation remains independent of file lookup.
  The direct path skips Iceberg file-location/lifecycle KV queries and retains
  native chunk-layer layout resolution for cold reads, repair and migration.
  GC and storage reads must prevent reclaimed extents from aliasing new
  data, including after restart and physical space reuse.
- Uses the standard Iceberg URI requirement and Arrow S3 behavior as
  compatibility references, not as a requirement to introduce an external S3
  store: [Iceberg specification](https://iceberg.apache.org/spec/),
  [Arrow S3 filesystem](https://raw.githubusercontent.com/apache/arrow/apache-arrow-25.0.0/cpp/src/arrow/filesystem/s3fs.cc).

## Acceptance

- Given a table created in the default namespace and a named namespace, when
  their table heads and file indexes are inspected, assert every table-owned
  key starts with the catalog, storage namespace, and table IDs in that order.
  Invariant: ICEBERG-PATH-1. Unit test.
- Given a table URL such as
  `s3://iceberg/cat/ns/table/f/file.parquet`, when Arrow parses it, assert the
  bucket is `iceberg` and the object key starts with `cat/ns/table/`. Assert a
  table ID is never parsed as the bucket. Invariants: ICEBERG-PATH-2 and
  ICEBERG-PATH-3. Integration test.
- Given a file published before a same-namespace rename or cross-namespace
  move, when the file is loaded through the new table name, assert its single
  `file_path` and native chunk ownership are unchanged. Invariants:
  ICEBERG-PATH-1 and ICEBERG-PATH-3. E2E test.
- Given a published parquet record, when a client performs HEAD, full GET, and
  footer Range GET using only `file_path`, assert the server can serve the
  request without requiring a second public location field and routes bytes to
  native chunk storage. Invariants: ICEBERG-PATH-3 and ICEBERG-PATH-6.
  Integration test.
- Given a third-party SDK PUT to its locally chosen URL and a native injection,
  when both files are published, assert the SDK file keeps its original URL
  while injection returns one direct URL before manifest construction. Neither
  requires a second field or subsequent manifest rewrite. Invariants:
  ICEBERG-PATH-7 and ICEBERG-PATH-9. Integration test.
- Given a direct locator token, when its version, table ID, file ID, length,
  or chunk-root binding is changed, or its HTTP grant expires, assert the
  request is rejected before chunk access. Given a valid token, assert direct
  reads perform neither file-location nor file-lifecycle KV queries.
  Invariants: ICEBERG-PATH-8 and ICEBERG-PATH-6. Integration test.
- Given a file key and its native file index record, when metadata and recovery
  inspect the record, assert its key suffix is the public filename and its
  value contains length, integrity and native locator metadata, with no
  additional path/public/id file records. Invariant:
  ICEBERG-PATH-4. Unit test.
- Given an authorized table list with recursive and non-recursive selectors,
  when the server receives ListObjectsV2 requests, assert the prefix is the
  complete table prefix, delimiter and pagination are honored, and public
  object keys are returned. Invariant: ICEBERG-PATH-5. Integration test.
- Given an empty, catalog-only, namespace-only, foreign-table, or bucket-wide
  list prefix, when a caller requests ListObjectsV2, assert the request is
  rejected without revealing keys from another table. Invariant:
  ICEBERG-PATH-5. E2E test.
- Given an exact missing file request, when Arrow performs HEAD followed by its
  directory fallback list, assert only `<object-key>/` is considered and no
  broader prefix is queried. Invariant: ICEBERG-PATH-5. Integration test.
- Given a logical file delete followed by delayed physical GC, when the old URL
  is read with a valid grant, assert list excludes the file while its direct URL
  still reads retained bytes. After GC, the URL fails and cannot read replacement
  bytes at a reused extent. Retained locators never change. Invariant:
  ICEBERG-PATH-6. E2E test.
- Given a dropped table and a recreated table with the same name, when both
  paths are inspected, assert the recreated table receives a new table ID and
  cannot read the old table's records or chunks. Invariants: ICEBERG-PATH-1 and
  ICEBERG-PATH-6. E2E test.
- Given a v1 catalog, when v2 layout activation is requested, assert activation
  is rejected while legacy reads and writes remain available. Given a new v2
  catalog, assert v1 keys cannot decode as v2 and scans never merge layouts.
  Invariant: ICEBERG-PATH-10. Integration test.
- Given HeadBucket, ListBuckets, CreateBucket, DeleteBucket, and a list without
  a complete table prefix, assert the fixed virtual bucket returns only the
  documented compatibility response and never reveals another table's keys.
  Invariant: ICEBERG-PATH-11. Integration test.
- Given logical names and public keys with different ordering, when paginating
  a table list with delimiters, assert ordered public keys, no aliases and
  correct continuation through empty pages. Invariants: ICEBERG-PATH-4 and
  ICEBERG-PATH-5. Integration test.
- Given a crash before or after the single file-record CAS, when recovery retries,
  assert only selected records become visible and GC preserves the selected
  bytes. Invariants: ICEBERG-PATH-4 and ICEBERG-PATH-6. Integration test.
- Given two injections with the same readable name and a retry of one upload,
  when publication finishes, assert distinct files have distinct locator-bearing
  filenames, the retry returns its original filename, and a conflicting exact
  key never overwrites the winner. Invariant: ICEBERG-PATH-4. Integration test.
- Given a retained snapshot, new HTTP credentials and rotated signing keys,
  when reading its direct URL, assert the URL is unchanged and still readable;
  an expired grant remains rejected. Invariant: ICEBERG-PATH-8. Integration test.
- Given a moved table and its old namespace, when that namespace is dropped
  and recreated, assert live lookup still returns the frozen storage path and
  the files survive. Invariant: ICEBERG-PATH-1. E2E test.
- Given files with multiple extents, when reading footer and boundary ranges,
  assert correct bytes, bounded URLs and zero Iceberg file-location and
  file-deletion-state KV RPCs. Chunk layout resolution remains supported.
  Invariants: ICEBERG-PATH-3 and
  ICEBERG-PATH-8. Integration test.
- Given concurrent multipart completion/abort, same-length conflicting PUTs
  and conditional/range reads, when requests retry after response loss, assert
  immutable selected bytes, one completion result and documented HTTP errors.
  Invariants: ICEBERG-PATH-6 and ICEBERG-PATH-7. Integration test.
- Given branches, tags, position deletes and historical snapshots, when moving,
  compacting and rolling back the table, assert original paths and delete
  applicability remain intact. Invariants: ICEBERG-PATH-1 and ICEBERG-PATH-9.
  E2E test.


- Given metadata exceeding the inline threshold with many snapshots, when
  publishing and reloading it through ALL/REFS, assert chunk-backed canonical
  bytes, bounded head/file values and identical required table semantics with
  projections present, missing or corrupt. Invariants: ICEBERG-PATH-4 and
  ICEBERG-PATH-9; permanent ICE-I2 and ICE-I13. Integration test.
- Given many metadata/manifest files mixed with data files, when listing the
  table, metadata directory and a filename prefix over multiple pages, assert
  correct public keys, bounded per-page work, no payload reads and no scans
  outside the chosen prefix. Invariant: ICEBERG-PATH-5. Integration test.
- Given v1/v2/v3 fixtures and two concurrent commits, when clients reload the
  selected head and historical snapshots, assert the standard reference graph,
  version-specific fields and a single complete winning generation; losing
  candidates remain invisible to table load. Invariants: ICEBERG-PATH-9 and
  permanent ICE-I3/ICE-I7. E2E test.

Required implementation gates (documentation editing does not run these):

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo clippy -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-chunk-client --all-targets -- -D warnings`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
