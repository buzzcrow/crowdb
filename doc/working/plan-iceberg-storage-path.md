<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Iceberg Storage Path Plan

Requirement: [R234](../backlog/R234-access-iceberg-native-storage-layout.md)

Upstream references:

- [Iceberg table specification](../design/access-server/iceberge/iceberg-table-spec-1.11.0.md)
- [CROWDB Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md)
- [CROWDB Iceberg upload flow](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md)

Goal: make catalog, namespace, and table identities explicit in the Iceberg
KV and FileIO path model, keep file bytes in the native CROWDB Iceberg storage,
and preserve file locations across table rename and cross-namespace move.

Status: implementation in progress. Native catalog layout selection, default
namespace creation, table/file/projection paths, direct URL reads, upload
selection/replay and the loader's returned-location handoff are implemented.
The auxiliary-key/recovery audit is complete. Remaining work is native
SDK/container acceptance and final design/backlog cleanup. Direct reads skip
Iceberg
file-location/state KV and retain the chunk layer's normal layout resolution,
as confirmed in the chat.

Latest verified behavior:

- Native catalog authorities persist layout 2 in a record envelope that older
  servers reject; omitted legacy layout fields remain version 1. Rename and
  capability activation preserve layout. New initialization/clear chooses 2.
  The authority also persists the reserved version-1 locator key; access-token
  rotation cannot invalidate file URLs. Configured later signing versions retain
  that catalog key automatically. Both credential and locator key rotation pass
  the native HTTP read test.
- Native default namespace creation uses normal durable namespace publication
  before table metadata writes. Native creation and replay retain one frozen
  namespace, one head and one filename authority.
- Native GC discovers v2 file records through bounded v2 scans, resolves standard
  graph references by canonical filename, deletes that authority and removes
  native projection pages at catalog retirement. Restart-at-every-step tests pass.
- Injection PUT selects an operation receipt before file publication. Both lost
  receipt and lost publication replies replay the same FileId, root and URL;
  changed digest/principal/identity and deleted-file replays fail closed.
- Direct locators require an owner-bound directory root even for one leaf.
  An identical replacement at a reused root extent cannot revive the old URL.
  HTTP probes root ownership for HEAD/GET/conditions and reuses the preflight
  directory in its pull reader, without file-location/state KV.
- Native HTTP integration forbids all file KV GETs and verifies cold restart,
  locator-key rotation, range reads, ETag conditions, scoped listing, logical
  delete retention, and HEAD/GET failure after physical root removal.
- Credential refresh includes the frozen namespace in the existing endpoint URL.
  Moving a table, dropping/recreating its default namespace and refreshing through
  the old endpoint retains its old prefix without any table-ID reverse index.
- The TPC loader journals its UUIDv7 upload URI before transfer and the selected
  returned URL before manifest registration. Python verification: 226 passed,
  7 explicitly skipped integrations; Ruff check/format passed. Existing unrelated
  loader edits are preserved. A pre-existing region-only assertion was corrected
  to validate the actual table name for the nation case as well.

Verified foundation gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets` passed,
  including seven native storage-path cases and existing legacy/lifecycle tests.
- `pixi run -- cargo fmt --all -- --check` passed.
- `pixi run rs-lint` passed, including workspace all-target Clippy.

Verified runtime work:

- `TableLocation`/`FileLocation` preserve native and legacy URLs; one native
  `canonical_file_key` binds a record to the full path and filename.
- `FileRepository` publishes/resolves one native record and recovers publication
  response loss. Prefix listing uses scanned records without per-file GETs;
  a 1,200-object mixed-prefix fixture verifies ordered bounded pages.
- Grant v2, directory v2, digest checkpoint v2, multipart/write-intent owner
  codecs and name mappings preserve storage namespace. Mapping/head bindings
  retain it when the live namespace changes.
- `storage_head_key` is used by table selection, commits, creation publication,
  rename/drop, namespace drop probes and GC fences. A native lookup/move test
  verifies the head key and metadata URL remain unchanged across a live move.
- `DirectFileLocations` supplies authenticated, bounded versioned parquet
  locators with a verification key ring. HTTP reads reconstruct their file
  descriptor directly; tests cover multi-extent ranges, tampering, old-key
  retention, publication response loss, logical deletion and physical removal
  with no Iceberg file KV reads. `finalize_native_parquet` requires the caller's
  durably selected owner/tree; injection now selects a durable receipt before
  publishing the filename authority.
- `FileGrantIssuer` retains at most 64 validated credentials in an ArcSwap
  cache, rechecking context and time on hits. Expired grants and changed catalog
  epochs fail closed. Requests still verify SigV4 and file scope individually.
- HTTP exact/list parsing supports three IDs and rejects broad list prefixes;
  ordinary SDK PUT/multipart creation cannot occupy the reserved `f/` directory.
- Iceberg all-target tests, focused snapshot/multipart/mapping/direct tests,
  HTTP request/admission tests, workspace Clippy and fmt passed. Credential
  issuance now derives namespace from selected table/draft metadata. Fixed
  bucket HeadBucket/region probes are wired; bucket parser tests pass. Native
  catalog activation, lifecycle and GC unit/integration tests pass;
  native container SDK flows are being verified.

## Target structure

Iceberg storage owns the logical directory. Native chunk placement remains
opaque and is still selected by the Iceberg chunk type, allocator, and
`FileBlockStore`; the directory below is the authority path and the stable
name used to resolve a file. The namespace component is the storage namespace
ID captured when the table is created:

```text
<catalog-id>/<storage-namespace-id>/<table-id>/
├── metadata/
│   ├── <table-metadata>.metadata.json
│   ├── <manifest-list>.avro
│   └── <manifest>.avro
├── data/
│   ├── <partition-directories>/data-<id>.parquet
│   ├── <partition-directories>/delete-<id>.parquet
│   └── <partition-directories>/delete-vector-<id>.puffin
└── statistics/
    └── <id>.puffin
```

`metadata/`, `data/`, and `statistics/` are conventional defaults, not a new
Iceberg semantic requirement. The server must accept any safe relative key
below the immutable table prefix so ordinary Iceberg clients can choose their
own file names and subdirectories. Every metadata, manifest, data, delete,
Puffin, and statistics reference generated by CROWDB must remain under this
prefix. Opaque client manifests are not recursively validated by normal commit;
FileIO enforces scope whenever an actual reference is accessed. Importing
external files requires explicit native ingestion, not an external authority.

This is the logical Iceberg file namespace. The single public `file_path` may
render a native locator such as `f/<opaque-locator>.parquet` under the same
table prefix. The readable stem may be included in the public filename itself;
there is no second internal filename index or second public location field.

Native injection returns a versioned locator file component after bytes and
their descriptor are durable, before creating any referencing manifest:
`s3://iceberg/<catalog-id>/<storage-namespace-id>/<table-id>/f/v1/<readable-stem>.<locator>.parquet`.
Ordinary SDK writes retain their chosen `data/...` or other safe relative key;
their location provider runs in the client and cannot be assumed to call a
CROWDB reservation API. Both forms have exactly one public URL per file. The
fixed `iceberg` compatibility bucket is always the URI bucket; the
object key carries the three ID segments. A table ID is never promoted to a
bucket. The `s3://` wrapper exists for SDK compatibility; the object is still
read from native Iceberg authority and chunk storage.

The KV layout uses the same identity tuple as a slash-separated logical path;
the serialized key must still version and validate each fixed ID segment:

```text
<catalog-id>/
├── head
├── namespace/name/<encoded-namespace> -> namespace-id
└── <namespace-id>/
    ├── head
    ├── name/<encoded-table-name> -> (storage-namespace-id, table-id)
    └── <table-id>/
        ├── head
        ├── file/<public-relative-filename>
        ├── projection/<metadata-generation>/<page-or-receipt>
        └── operation/<operation-id>/injection
```

The namespace ID in a table's directory is always its storage namespace ID.
The name mapping may live in a different namespace after a move. There is no
table-ID reverse index: callers and recovery tasks carry the full identity.
`projection/` represents the existing optional, rebuildable metadata read
acceleration, not another file record or authoritative snapshot store.

The first three path segments are parsed by fixed identity encodings and
positions. The native canonical encodings are lower-case fixed-width hexadecimal for all
three 128-bit IDs (32 bytes of ASCII each). Legacy buckets retain their base32
catalog component;
labels such as `namespace` and `table` are rejected rather than accepted as
aliases.

The displayed paths are inside Iceberg storage. Neither the bucket name nor
`iceberg/` is repeated in the KV payload; a binary storage-family/version header
may precede it. Catalog/namespace control indexes are not table-owned records
and therefore use their own typed scopes. In a table scope the payload is
exactly the three IDs followed by a typed suffix. Name indexes include the
storage namespace so a moved table can be found without a catalog-wide scan.

`file/<public-relative-filename>` is the only persistent file record. It holds
the complete selected descriptor and provides logical GET resolution, ordered
S3 listing, inspection and GC. Publish it with one create-only CAS after bytes
are durable. There are no separate path/public/id indexes. Lifecycle tasks
carry the canonical filename; file ID remains a value field and locator binding.
Multipart/operation/GC state is temporary workflow state, not another file index.

The native filename is `f/v1/<readable-stem>.<locator>.parquet`; use the last dot
before the extension to delimit the URL-safe, dot-free locator. Authenticate
the exact filename as well as the native descriptor to prevent alternate-name
aliases. The locator contains a unique file ID, so identical readable stems
remain distinct. Retry the same injection operation with the original file ID
and return the selected URL; a conflicting exact key fails rather than
overwriting. Ordinary SDK filenames retain their exact chosen key and the same
create-only collision rule. Include the readable stem in the total key limit.

Rename/move changes name mappings and the head's live name/epoch fields using
the existing lifecycle state machine. The head's key, storage identity,
metadata generation, file URLs and chunk ownership remain unchanged. Dropping
the old namespace cannot cascade into files of a moved table; namespace ID
reuse is forbidden and cleanup follows table ownership rather than names.

## Standard metadata, large objects and prefix listing

The supported format profile remains the repository's pinned Iceberg v1/v2/v3
specification. The live upstream spec may describe newer versions; this layout
change does not advertise those versions. References:
[table metadata](https://iceberg.apache.org/spec/#table-metadata),
[snapshots](https://iceberg.apache.org/spec/#snapshots), and
[manifests](https://iceberg.apache.org/spec/#manifests).

```text
table/head -> current metadata JSON file
                 snapshots[] -> manifest-list -> manifests -> data/delete files
                 refs, snapshot-log, metadata-log, schemas, partition specs
```

- Snapshots and refs retain their standard JSON representation. They do not
  require individual authoritative KV keys. Preserve the v1 direct `manifests`
  alternative when reading old snapshots; v2/v3 use `manifest-list`.
- Metadata JSON, manifest lists, manifests, data/delete and statistics files
  all use the one `file/<public-relative-filename>` record model. File kind
  does not introduce another index. Standard metadata/Avro fields, sequence
  inheritance, schemas and exact path-based delete references remain intact.
- A large metadata JSON is a native object whose bytes are written into
  IcebergTable chunks. Its KV value contains a bounded native descriptor/root,
  canonical length and integrity metadata, not the complete JSON or snapshots
  array. Preserve the existing small-file inline threshold; crossing it selects
  native chunk storage without changing FileIO behavior. Large extent vectors
  likewise use a bounded chunk directory rather than growing the KV value.
- `head` stays bounded: selected metadata pointer, generation and existing
  identity/commit state. Publish immutable files durably before the head CAS.
  Concurrent commits still validate their base; no head can select partial
  bytes. A commit retry may create a new manifest list/metadata generation;
  it does not overwrite already published files to retain the old filename.
- CROWDB-generated metadata names preserve useful public prefixes:
  `metadata/<name>.metadata.json`, `metadata/<name>.avro` and
  `statistics/<name>.puffin`. Existing clients may choose other safe paths.
  Native parquet injection retains the direct `f/v1/...` filename. Metadata
  objects may resolve their single file record; direct parquet reads remain
  independent of metadata/list queries. Large metadata storage does not force
  all objects into the direct parquet naming convention.
- Table listing maps the public prefix `<table-path>/metadata/` to the bounded
  KV prefix `<table-path>/file/metadata/`. A more specific filename prefix maps
  identically, without scanning `data/`, `f/`, statistics, operations or GC.
  Full table list scans only `<table-path>/file/`. Prefix upper bounds and
  continuation cursors are constructed from the canonical encoded key.
- Apply the existing list page record/byte/output limits to metadata listing
  as well. Build each page incrementally; never collect or sort the full file
  set. Delimiter rollups and tombstone filtering advance a bounded cursor, and
  return a continuation when the scan budget ends even if the page is sparse.
  List reads file descriptors only, never opens metadata JSON or manifests.
- Object listing and snapshot enumeration are different operations. A prefix
  list shows retained metadata files, not the snapshots inside one JSON file;
  no `snapshot/<id>` objects or private SDK pagination fields are synthesized.
  Use the existing generation-bound validated projections for REFS loads;
  preserve ALL response semantics. Projections remain optional, paged and
  discardable, and cannot participate in commit authority or GC reachability
  proof. Missing/corrupt projections use the bounded canonical parser.
- Chunk-backed storage alone does not make parsing unbounded metadata safe.
  Keep explicit metadata-byte, collection, parser-memory and response budgets;
  reject oversized unsupported requests clearly without truncating valid
  snapshots or refs. Reuse the existing streaming/bounded read paths where
  available; do not claim that the existing commit parser is fully streaming.
- Physical GC follows retained snapshot/branch/tag and metadata retention
  rules, not filename order, list age or absence from the current snapshot.
  Snapshot expiration is a metadata update; files shared by another retained
  snapshot remain readable. Historical JSON files follow their own retention
  policy rather than being immediately deleted on head replacement.

## File URL and native read boundary

The Iceberg `file_path` remains a stable URL. Published CROWDB chunk
locators (`ChunkId` plus offset and logical span) are stable while the file is
retained; they become invalid only after the file is deleted and its physical
storage is reclaimed. This makes a native read locator a viable optimization,
but it still cannot be assumed to be one scalar location: a streamed file can
have multiple locations and a tree-backed file can require its directory root.

The preferred read fast path is therefore:

```text
file_path URL -> immutable file authority or native read locator
            -> ChunkRoot/Locations -> ChunkIoClient range stream
            -> native chunk storage
```

There is only one public URL: the URL stored as Iceberg `file_path`. If
ordinary SDK GET and range GET are to bypass the file-location lookup, this URL
must be a native read locator, for example a compact signed file-root/location
token under the table prefix. The server validates the catalog/table grant,
file length, integrity metadata and locator binding before
reading. Large location vectors use a bounded chunk-resident directory root;
a token requiring a KV mapping would reintroduce the location lookup.

An SDK chooses its path before PUT. Reserving a target when PUT arrives cannot
put the newly allocated location into the already chosen URL. Such files retain
their logical URL and use `record_resolution`. Native injection owns the upload
and manifest-building sequence: finalize bytes and descriptor, publish the
file, return one direct URL, then build manifests with that URL. An internal
operation ID supports retry without publishing another file. This boundary is
the agreed native injection boundary; ordinary SDK writers keep logical URLs.

The direct-read locator is versioned and authenticated. It binds catalog,
storage namespace, table, file ID, length, integrity metadata, and a compact
chunk root or complete location descriptor. It has a bounded URI
size and carries no arbitrary client-controlled chunk ID. A valid direct read
calls the chunk API with the complete validated locator and skips the Iceberg
file repository; a fallback read is explicit in the file record and is never
presented as direct. Persistent locators have no credential expiry or credential
nonce: refreshed HTTP grants must continue reading retained snapshots. Include
a signing-key version and retain verification keys while their files survive;
the locator signature grants no access by itself. Inline/compressed metadata
may use the logical path; direct mode requires random-readable native bytes.

The current chunk reader calls `query_layout`/`query_chunk`; removing
`FileRepository::resolve` does not by itself remove chunk metadata RPCs.
Measure Iceberg file-location resolution, file-state checks, chunk-layout
resolution and data reads separately, including cold cache. The first two
must cause zero KV RPCs on the direct path. Native chunk layout resolution is
retained for cold routing, repair and migration.
Stable chunk IDs/offsets do
not imply immutable replica addresses. A direct chunk API must preserve normal
repair/failover behavior and never embed transient host placement in file URLs.

Logical deletion affects metadata/list and schedules physical reclamation; it
does not promise immediate revocation of an already known direct URL. A valid
grant and locator can read retained bytes until GC. GET/HEAD/Range GET perform
no file tombstone lookup, and need no per-file revocation cache. List, deletion,
GC and recovery may use KV normally. Physical storage must reject reclaimed
chunk identities/extents and prevent old locators reading newly allocated data;
check this at the storage boundary without an Iceberg file-record lookup.

The upload state machine is `Staging -> Published -> Deleted -> Reclaimed`,
or `Staging -> Aborted -> Reclaimed`. Incomplete or aborted records never
appear in ListObjectsV2. Complete PUT and multipart retries are idempotent;
conflicting content or overwrite attempts fail without changing a published
record. Equal length alone never establishes an identical retry. Logical
deletion removes list visibility; direct reads remain possible during retention.
GC/read concurrency follows native storage lifetime rules: return retained bytes
or a storage error, never bytes from a replacement allocation.

The native metadata record uses the public relative filename as its map key
and stores file ID, length, integrity metadata, format, root and native
locations as its value. The public URL is derived from table identity and
filename. This preserves inspection, listing, GC, and
recovery clarity. A file's canonical URL and ETag never change during its
lifetime, including rename, credential rotation and snapshot rollback.

Arrow/PyArrow derives the bucket from the URI host and the object key from the
URI path. For a `FileSelector`, Arrow removes a trailing slash from the base
directory and sends `ListObjectsV2` with `Prefix = key + "/"`; non-recursive
selectors also send `Delimiter = "/"`. An exact file operation normally
becomes HEAD/GET (and Range for reads); if HEAD misses, Arrow may test an
implicit directory with `Prefix = object-key + "/"`. The native FileIO surface
must therefore keep table-scoped listing, even though Iceberg manifests
normally provide explicit file paths and do not require a full table scan for
reads.

The server enforces this scope instead of trusting the client: every list
request must name the complete `<catalog-id>/<storage-namespace-id>/<table-id>/`
prefix authorized for the request. Empty, catalog-only, namespace-only,
foreign-table, and bucket-wide prefixes are rejected; bucket-wide bucket
listing is not exposed. The missing-file fallback is allowed only for the
exact `<object-key>/` child prefix and cannot widen the table grant. This is a
security and isolation rule, not an Arrow workaround.

The list compatibility contract includes `prefix`, `delimiter`, `max-keys`,
`continuation-token`, `start-after`, `encoding-type=url`, common prefixes,
`IsTruncated`, and `NextContinuationToken`. Exact-object existence fallbacks
may list `<object-key>/` to distinguish a missing object from an implicit
directory. Listing remains table-scoped and never grants catalog-wide
discovery.

The fixed virtual bucket has narrow control-plane behavior. HeadBucket succeeds
only when the Iceberg endpoint and table grant are valid; the configured
endpoint supplies region information, so Arrow does not need AWS bucket
discovery. ListBuckets returns `403 AccessDenied`; authenticated GetBucketLocation
returns the configured region. CreateBucket and DeleteBucket are rejected. No operation creates a
bucket per table, and every object operation still requires the immutable
three-ID prefix and a matching grant.

## Request and lifecycle constraints

- Canonical object keys are at most 1,024 UTF-8 bytes, including the three IDs
  and locator. Preserve relative-key case, literal percent/plus and internal
  repeated slashes as in the existing FileIO contract. Reject leading slash,
  dot traversal, backslash, controls, query/fragment delimiters and malformed
  ID encodings. Decode HTTP percent escapes once at the transport boundary;
  neither stored URLs nor KV keys are repeatedly decoded or normalized.
  Namespace/name index components use an unambiguous length/escape codec.
- The complete table prefix ends in `/`. Authorization compares parsed IDs and
  component boundaries, not textual `starts_with(table-id)`. Descendant list
  prefixes may end inside a filename; S3 prefix matching need not end in `/`.
  Arrow's exact-missing-file `<key>/` probe needs no remembered prior HEAD.
- List selected published objects, including files awaiting a snapshot commit;
  exclude incomplete uploads, losing candidates and tombstones. List is not a
  snapshot reachability query. Return public object keys (not full URIs), size,
  ETag and the existing fixed epoch LastModified. No internal logical alias is
  additionally advertised for a direct file. Listing `data/` need not return
  direct objects whose actual public keys are under `f/`.
- Order and paginate `file/` by public filename. Support delimiter `/`, URL
  encoding, start-after and common prefixes, with the existing 256-record/
  4-MiB scan and 2-MiB XML limits, at most 1,000 returned entries. Max-keys zero
  returns an empty result. Continuations bind principal, grant nonce/expiry,
  catalog epoch, full storage identity, prefix, delimiter and encoding;
  foreign/expired tokens fail before scanning. Advance through zero-result
  pages and common-prefix subtrees. Pagination remains a forward live traversal.
- GET/HEAD retain S3 content length, stable ETag and conditional request
  semantics. Support closed, open-ended and suffix single byte ranges; clamp
  an excessive end, return 206/Content-Range for a valid range and 416 for an
  unsatisfiable range. Reject multiple ranges explicitly. Never interpret a
  requested offset relative to the underlying chunk rather than the file, or
  read neighbouring shared-chunk data. ETag is not necessarily a content MD5.
- Ordinary PUT and multipart keys cannot enter reserved `f/`. Injection issues
  those URLs only after a selected durable publication; unknown or forged
  tokens fail closed. Unknown length and multipart SDK uploads stay on their
  original logical key. Completing multipart validates the ordered part set,
  freezes it once, and replays the same outcome after response loss. Abort or
  TTL expiry reclaims only unselected allocations, never a published winner.
- Writer/reader grants include table-scoped list; cleanup-only grants do not.
  DELETE/DeleteObjects require explicit cleanup authority and retained-reference
  checks. Failed per-object batch deletions return individual errors. Physical
  GC waits for admitted-read retention and durable proof; shared chunks cannot
  be reclaimed while another live extent references them. Snapshot expiration
  alone is not file deletion. Keep locator file IDs non-reusable to prevent
  old URLs reading later allocations at reused offsets.
- Rename/move leaves existing table-bound grants valid until their normal
  expiry/revocation; issuance via the old name fails. Drop prevents new
  writes/credentials; direct URLs with still-valid grants can read retained
  bytes until physical reclamation. Purge
  reclaims only the old table's identity; recreate cannot acquire its grants.
- Unsupported CopyObject, UploadPartCopy and bucket administration return an
  explicit S3 error without mutation. A filesystem directory has no separate
  native record; list synthesizes common prefixes. Do not claim broad SDK
  compatibility for directory creation or copy/rename helpers until tested.
- Branch/tag references and historical snapshots keep their file URLs through
  rollback, compaction and snapshot expiration. Compaction creates new immutable
  files; it never overwrites old locator targets. Commit retains existing CAS,
  requirement validation and response-loss behavior. Table `set-location` may
  accept the existing canonical base with a trailing slash but cannot relocate
  the immutable identity; unsupported location-provider overrides fail clearly.

## Decisions

- [x] **Freeze the physical table path at table creation**: use an immutable
  ID path containing the catalog ID, the namespace ID at creation, and the
  table ID. A rename changes the name index and table metadata name, while a
  cross-namespace move changes the live namespace mapping without moving file
  bytes or rewriting every manifest reference. The path's namespace component
  is therefore a storage namespace ID, not necessarily the table's current
  namespace after a move. Files use a canonical prefix such as
  `s3://iceberg/<catalog-id>/<storage-namespace-id>/<table-id>/`.
- [x] **Represent the default namespace explicitly**: create or recover one
  durable default `NamespaceId` for the catalog and use it in the same path and
  KV key rules as every named namespace. No special implicit path is needed.
- [x] **Keep S3-shaped URIs as the compatibility surface**: clients use the
  S3 SDK and SigV4 over the native Iceberg listener, but the URI does not imply
  a general S3 metadata bucket or an external S3 data store. The existing
  `ChunkType::IcebergTable` and `FileBlockStore` remain the byte owner.
- [x] **Use a fixed compatibility bucket**: use `iceberg` as the virtual S3
  bucket for every Iceberg file URL. Keep catalog, storage namespace, and table
  IDs in the object key. Do not create one bucket per table; enforce table
  scope in request validation.
- [x] **Reject unscoped listing**: require a complete table prefix for every
  ListObjectsV2 request, reject empty or broader prefixes, and preserve the
  Arrow trailing-slash directory fallback only within that table scope.
- [x] **Expose one file URL**: the Iceberg `file_path` is the only public file
  location. Keep clear parquet keys and native locator metadata inside the KV
  value; do not add a second SDK-visible location field.
- **Proposed rollout**: new catalogs use v2. Existing v1 catalogs retain v1
  reads, writes, grants and URLs; reject in-place version relabeling. Migration
  is outside this requirement. Dispatch by catalog layout version and never
  merge v1/v2 table scans or reinterpret a v1 suffix as a v2 path.
- [x] **Choose the relative file layout**: the SDK may choose any safe logical
  relative name below the table prefix. Native injection uses the reserved
  `f/v1/<readable-stem>.<locator>.parquet` form; ordinary SDK writes retain the supplied key.
  Both use one `file/<public-relative-filename>` record. The TPC loader passes only its file name to the provider and
  does not add a second table directory.

## Phase 1: identity and canonical path model

- [x] **Reconcile the permanent design contract**: update
  `doc/design/access-server/iceberge/design-crowdb-iceberg.md` and the upload
  flow to the fixed `iceberg` bucket, three-ID key, injection-before-manifest
  behavior, no manifest rewrite, and direct-vs-record read modes. This must
  land before implementation is considered complete.

- [x] **Define the immutable storage-path codec**: `StorageTablePath` contains
  `CatalogId`, storage `NamespaceId` and `TableId`; its lower-hex path is
  independent of live names. `NativeFileLocation` uses the fixed virtual bucket
  and preserves the exact filename. Boundary, UTF-8, alias and legacy isolation
  tests pass in `native_storage_path_test.rs`, `file_location_test.rs`,
  and `key_test.rs`.
  Files: `lib/crowdb-access-iceberg/src/key/storage_path.rs`,
  `lib/crowdb-access-iceberg/src/file/native_location.rs`.
- [x] **Carry immutable storage identity through runtime records**: use
  `StorageTablePath` in table heads/mappings, chunk-directory owners,
  credentials and creation metadata; add layout dispatch without changing the
  v1 catalog contract. The standalone codec does not activate new catalogs.
  Files: `lib/crowdb-access-iceberg/src/file/location.rs`,
  `lib/crowdb-access-iceberg/src/file/directory.rs`,
  `lib/crowdb-access-iceberg/src/table/record.rs`.
- [x] **Define canonical URI encoding and validation**: encode IDs in fixed
  lower-case forms, reserve `f/` for server-issued locators, preserve relative
  UTF-8 keys, and reject aliases, traversal, authority changes, and malformed
  prefixes. Round trips must recover all three IDs and the relative file key.
  Files: `lib/crowdb-access-iceberg/src/file/location.rs`,
  `lib/crowdb-access-iceberg/tests/file_location_test.rs`.
- [x] **Assign the storage path once** during table creation, including staged
  creation, and bind it into the table head and initial metadata. Rename and
  cross-namespace move must retain it; a newly created table gets a new table
  identity and path.
  Files: `lib/crowdb-access-iceberg/src/commit/create/publisher/preparation.rs`,
  `lib/crowdb-access-iceberg/src/table/lifecycle/`,
  `lib/crowdb-access-iceberg/src/commit/create.rs`.

## Phase 2: KV key directory redesign

- [x] **Remove redundant planned indexes**: name lookup returns the complete
  storage identity; carry it through callers, multipart, GC and recovery so
  no table-ID reverse index is introduced. Store metadata JSON, Avro manifests
  and parquet through the same single file record. Keep existing bounded
  metadata projection pages under the owning table/generation as derived data.
  Files: `lib/crowdb-access-iceberg/src/table/`,
  `lib/crowdb-access-iceberg/src/metadata_projection/`,
  `lib/crowdb-access-iceberg/src/key/codec.rs`.

- [x] **Introduce one table-path prefix encoder** for catalog, storage
  namespace, and table IDs. All table-owned file keys must use it rather than
  assembling ad-hoc suffixes.
  Files: `lib/crowdb-access-iceberg/src/key.rs`,
  `lib/crowdb-access-iceberg/src/key/codec.rs`,
  `lib/crowdb-access-iceberg/src/file/key.rs`.
- [x] **Move the single file authority under the prefix**:
  `table-path/file/<public-relative-filename>` is the complete selected record.
  Do not introduce separate file name, public-key or file-ID indexes. Preserve bounded
  ordered scans and reject a range
  whose catalog, namespace, or table prefix differs from the request.
  Files: `lib/crowdb-access-iceberg/src/file/key.rs`,
  `lib/crowdb-access-iceberg/src/file/listing.rs`,
  `lib/crowdb-access-iceberg/src/file/listing/repository.rs`,
  `lib/crowdb-access-iceberg/src/catalog/storage.rs`.
- [x] **Audit every table-owned auxiliary key**: multipart records, write
  intents/fences, GC candidates/nodes/pending state, and recovery scans must
  either carry the same table-path prefix or explicitly document why an
  operation-scoped key is independent. Update range bounds and foreign-record
  checks together.
  Files: `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `lib/crowdb-access-iceberg/src/record/`,
  `lib/crowdb-access-iceberg/src/key/codec.rs`.
  Head/file/projection/injection records use the frozen native prefix. The
  permanent design explicitly lists independent catalog service journals:
  unpublished write intents/fences, upload sessions/parts, catalog admission,
  operation/payload journals and GC task/claim/proof workspace. They recover
  work without a published table and coordinate catalog-wide reclamation;
  they never resolve or list a published file. Their values bind full storage
  identity. GC layout dispatch and claim adoption, inactive protection and
  proof traversal now compare full identity rather than only table ID.
- [x] **Version the table key envelope and routed-store dispatch**:
  `TableStorageKey` uses a v2 envelope followed by the
  slash-separated immutable table path and scope. File keys contain only the
  public filename; `file_range` bounds metadata/filename scans directly.
  `StorageKey` selects the decoder by version without fallback. Routed point
  operations accept either validated envelope, while scan bounds reject mixed
  versions and out-of-range returned keys. Legacy decoding and cross-table/
  namespace range isolation are tested.
  Files: `lib/crowdb-access-iceberg/src/key/table_storage.rs`,
  `lib/crowdb-access-iceberg/src/key/storage.rs`,
  `lib/crowdb-access-iceberg/src/catalog/storage.rs`,
  `lib/crowdb-access-iceberg/tests/native_storage_path_test.rs`.
- [x] **Dispatch catalogs and recovery by persisted layout**: bind the selected
  layout in catalog authority before creating new native paths. Retain v1
  reads/writes/grants for v1 catalogs; reject relabeling and never merge layouts
  in recovery scans. Repository and lifecycle wiring must use the selected
  layout together; key codec support alone is not activation.
  Files: `lib/crowdb-access-iceberg/src/catalog/`,
  `lib/crowdb-access-iceberg/src/record/authority.rs`,
  `lib/crowdb-protocol/src/fbs/iceberg.fbs`,
  `lib/crowdb-access-iceberg/tests/*recovery*test.rs`.

## Phase 3: FileIO and S3-compatible access

- [x] **Implement native injection finalization**: the internal ingestion entry
  accepts the full storage identity, readable stem, upload identity and bytes;
  finish durable native writes, bind the exact filename to a signed descriptor,
  CAS one file record, then return its one canonical URL. Persist/reuse the
  existing operation identity for uncertain outcomes; no separate permanent
  filename/public/file-ID indexes. SDK PUT continues to return its normal S3
  response and does not pretend to rename the client's path.
  Files: `lib/crowdb-access-iceberg/src/file/`,
  `app/crowdb-access-server/src/iceberg/`, `tools/load-tpch.py`.
- [x] **Enforce the direct chunk boundary**: introduce/extend the native range
  read interface accepting the authenticated full descriptor/root; instrument
  Iceberg file-location, file-state and chunk layout queries separately.
  Retain `query_chunk` for native layout resolution, including cache misses.
  Exercise cold routing, multi-extent reads, repair, GC races and offset reuse.
  Files: `lib/crowdb-access-iceberg/src/file/blocks.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`.
- [x] **Reuse bounded permission validation**: define permission-cache entries
  by principal, catalog epoch, full storage identity and operations; bound
  validity by grants and applicable authority leases. Misses validate authority,
  expired entries fail closed, and direct reads never load per-file state.
  Keep permission cache accounting separate from native location resolution.
  Use existing lock-free cache mechanisms; do not introduce a lock as a shortcut.
  Files: `lib/crowdb-access-iceberg/src/file/credentials.rs`,
  `app/crowdb-access-server/src/iceberg/file_admission.rs`.

- [x] **Bind grants and credential prefixes to the full storage path** so a
  credential issued for one table cannot address another namespace/table path.
  Update refresh responses and staged-table credentials consistently.
  Files: `lib/crowdb-access-iceberg/src/file/credentials.rs`,
  `lib/crowdb-access-iceberg/src/wire/credentials.rs`,
  `app/crowdb-access-server/src/iceberg/table_credentials.rs`.
- [x] **Parse and route the new URI prefix** in exact object, list, multipart,
  cleanup, and continuation-token flows. A table rename must continue to use
  the same URI and file records after the new name is loaded from the catalog.
  Files: `app/crowdb-access-server/src/iceberg/file_request.rs`,
  `app/crowdb-access-server/src/iceberg/file_http.rs`,
  `app/crowdb-access-server/src/iceberg/file_list_request.rs`,
  `app/crowdb-access-server/src/iceberg/file_admission.rs`.
- [x] **Keep physical bytes native**: no filesystem or general S3 object
  authority is introduced. Uploads continue through native chunk writes and
  publication remains a KV authority CAS. A direct read receives a complete
  validated chunk locator without `FileRepository::resolve`; a
  `record_resolution` file uses the existing file record path.
  Files: `app/crowdb-access-server/src/iceberg/file_http/stream.rs`,
  `lib/crowdb-access-iceberg/src/file/blocks.rs`,
  `lib/crowdb-access-iceberg/src/file/repository.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`.
- [x] **Constrain catalog location-provider output**: reject a provider result
  outside the canonical table prefix before upload. This prevents the loader
  from recording a URI that the native grant cannot serve.
  Files: `/cjdata/cpp/crowdb-tpc-loader/src/crowdb_tpc_loader/backend.py`,
  `/cjdata/cpp/crowdb-tpc-loader/src/crowdb_tpc_loader/loader.py`.
- [x] **Use one SDK-visible file URL**: use its public filename as the single
  native KV file key, retain the native chunk descriptor and read
  mode in its value, and make the single URL written to Iceberg manifests the
  bound location. Implement native injection finalization before manifest
  creation. Ordinary PUT retains its logical URL and `record_resolution`;
  neither path rewrites existing manifests or requires a new SDK field.
  Files: `lib/crowdb-access-iceberg/src/file/record.rs`,
  `lib/crowdb-access-iceberg/src/file/location.rs`,
  `app/crowdb-access-server/src/iceberg/file_http/`,
  `lib/crowdb-access-iceberg/src/wire/credentials.rs`.
- [x] **Add direct locator validation**: bind locator version, table identity,
  file identity, length, integrity metadata and chunk root; authenticate the
  current HTTP grant independently of the durable locator signature;
  reject tampering before chunk access and enforce one contiguous range.
  Files: `lib/crowdb-access-iceberg/src/file/location.rs`,
  `lib/crowdb-access-iceberg/src/file/record.rs`,
  `app/crowdb-access-server/src/iceberg/file_http.rs`.

## Phase 4: metadata, rename, and lifecycle invariants

- [x] **Keep large metadata as native objects**: preserve the inline threshold,
  route larger metadata JSON/Avro into native chunk storage, bound descriptors
  with directory roots and keep snapshot arrays out of TableHead. Exercise
  canonical bytes through FileIO and ALL/REFS, including missing/corrupt
  generation projections and explicit parser/response limit errors.
  Files: `lib/crowdb-access-iceberg/src/file/record.rs`,
  `lib/crowdb-access-iceberg/src/table/metadata/`,
  `lib/crowdb-access-iceberg/src/metadata_projection/`,
  `lib/crowdb-access-iceberg/src/commit/files.rs`.
- [x] **Implement bounded metadata prefix list**: map the public metadata or
  filename prefix to the exact `file/` range; stream ordered records into a
  bounded page and persist an authenticated continuation at scan-budget
  exhaustion. Handle tombstones, sparse pages, delimiter common prefixes and
  zero max-keys without payload reads, table-wide sorting or unrelated scans.
  Files: `lib/crowdb-access-iceberg/src/file/listing/`,
  `app/crowdb-access-server/src/iceberg/file_list_request.rs`.
- [x] **Preserve standard metadata semantics**: cover snapshots/refs/logs and
  v1 direct manifests, v2/v3 sequence inheritance and existing v3 lineage;
  preserve exact delete-file references and immutable manifest bytes. Commit
  retries may write new manifest lists; atomic head selection and retained
  reference GC remain authoritative. Add no private snapshot KV authority.
  Files: `lib/crowdb-access-iceberg/src/commit/`,
  `lib/crowdb-access-iceberg/src/table/metadata/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/tests/iceberg_table_lifecycle_test.rs`.

- [x] **Validate table metadata location against the immutable storage path**;
  `set-location` may normalize a trailing slash but cannot change the path
  identity. Metadata generations and manifest references remain below that
  path.
  Files: `lib/crowdb-access-iceberg/src/table/metadata/root.rs`,
  `lib/crowdb-access-iceberg/src/commit/evaluator/initial.rs`,
  `lib/crowdb-access-iceberg/src/commit/evaluator/scalar.rs`,
  `lib/crowdb-access-iceberg/src/commit/files.rs`.
- [x] **Preserve rename behavior**: same-namespace rename and cross-namespace
  move update only name/mapping authority; old names disappear, the storage
  path remains readable through the new table identity, and a recreated name
  receives a new path.
  Files: `lib/crowdb-access-iceberg/src/table/lifecycle/`,
  `app/crowdb-access-server/tests/iceberg_table_lifecycle_test.rs`,
  `e2e-test/clients/iceberg-java/src/main/java/TestIcebergCatalogWrites.java`.
- [x] **Define grant and URL behavior across moves**: existing immutable URLs
  continue to bind to the storage namespace ID while the table is live; new
  credentials resolve the current name mapping and old-name resolution fails.
  Drop/recreate receives a new table ID, and direct and fallback locators reject
  the old table after purge.
  Files: `lib/crowdb-access-iceberg/src/file/credentials.rs`,
  `app/crowdb-access-server/src/iceberg/file_admission.rs`.
- [x] **Recheck GC and recovery ownership** against the immutable storage path
  before reclaiming files or chunk roots. Logical drop and physical purge must
  not confuse a recreated table with a retained old path.
  Files: `lib/crowdb-access-iceberg/src/gc/`,
  `lib/crowdb-access-iceberg/src/file/multipart_recovery.rs`,
  `lib/crowdb-access-iceberg/src/record/`.

## Phase 5: loader and container injection

- [x] **Align the TPC loader with the canonical table path**: use stable data
  names below the provider's `data/` directory, verify the returned URI is
  table-scoped, and keep the run ID in recovery/report metadata rather than in
  the object path.
  Files: `/cjdata/cpp/crowdb-tpc-loader/src/crowdb_tpc_loader/backend.py`,
  `/cjdata/cpp/crowdb-tpc-loader/src/crowdb_tpc_loader/loader.py`,
  `/cjdata/cpp/crowdb-tpc-loader/tests/unit/test_commits.py`.
- [x] **Separate or remove the optional S3 mirror** in
  `tools/load-tpch.py`. If retained for the container demo, document its
  bucket/key as an independent S3 object copy and do not treat it as the
  Iceberg table file layout.
  Files: `tools/load-tpch.py`,
  `container/single-node-container/README.md`.

## Native container verification

- Fresh image source verification and the two scoped listing cases pass with
  PyIceberg 0.11.1 and Arrow 25.0.1. The production loader upload helper selects
  one URL on retry; standard manifest registration, Arrow read and SQL aggregate
  pass before/after a cross-namespace move and catalog storage restart.
- First complete core partition: 23 passed, two failures. The GC CLI fixture
  still supplied a table ID; it now supplies the complete frozen path. Tree
  multipart found a real decoder gap: native digest checkpoints are 205 bytes,
  but completion decoding only accepted legacy 189-byte checkpoints. Both bounded
  shapes now decode, with full owner/checksum validation unchanged; the new
  partial native checkpoint round-trip and existing recovery/completion tests pass.
- Core partition rerun: all 25 cases pass, including native tree multipart
  reload, real chunk-KV restart, GC controls and large streamed/multipart IO.
- Apache Iceberg 1.11.0 Java catalog snapshot/staged creation and opaque
  data/delete reads pass. S3FileIO also passes after its foreign-table fixture
  was corrected to mutate a table ID within the valid three-ID prefix, retaining
  the 403 AccessDenied assertion.
- Official Rust lifecycle and lost-response/native restart cases pass. The
  retirement test is waiting through its full delegated-access grace period.
- PyIceberg 0.10.0 matches the loader adapter's declared range. Its complete
  production dataflow creates a fixture table, uploads through native injection,
  persists the selected URL in a real load report, commits the standard manifest,
  then reloads the report/table and executes SQL over native reads after restart.
  This focused fixture verifies ingestion; it does not claim an SF1 benchmark.
- The loader feature is committed separately with its 226-test/Ruff gate and
  fresh native container read/SQL verification. Pre-existing version, docs and
  ordinary filename edits remain unstaged in that repository.

## Acceptance tests

Unit:

- [ ] Canonical storage path and URI round trips include catalog, storage
  namespace, table, and relative file key.
- [ ] New and old key layouts cannot collide; table-path scans exclude foreign
  namespace/table records.
- [ ] File grants, list prefixes, continuation tokens, multipart sessions, GC
  keys, and chunk-directory owners reject mismatched identities.

Integration:

- [ ] Store metadata above the inline threshold with many snapshots; assert
  bounded head/record size, chunk-backed bytes, correct ALL/REFS and projection
  fallback. Exceed each configured limit and assert an explicit error with no
  truncated success or partially published head.
- [ ] Generate many metadata and manifest files with interleaved other file
  prefixes. Page full-table, `metadata/` and filename-prefix lists; record scan
  bounds, peak page memory and payload-read counts. Assert no unrelated scans,
  no payload reads and complete results on an unchanged eligible set.
- [ ] Race metadata commits and retry their lost responses; reload every
  selected version with official clients and verify snapshot/manifest references,
  retained branches/tags and protected shared-file GC behavior.

- [ ] Publish native injection, load its direct URL through unmodified readers,
  and publish a separate SDK-written logical URL. Assert one URL per file and
  correct position deletes, deletion-vector references and historical reads.
- [ ] Crash before and after the single file-record CAS, restart and
  replay; assert no ghost list entries, no lost selected file and no GC of a
  losing operation's shared winner. Retry equal-length different bytes.
- [ ] Renew credentials and rotate locator signing keys; retained snapshot URLs
  stay readable. Delete concurrently with direct read and restart: list omits
  the deleted file, but a valid direct URL reads bytes until reclamation. After
  reclamation/reuse, the old URL must never return another file's bytes.
- [ ] Count location, deletion-state and chunk-layout RPCs independently for
  cold/warm HEAD and footer ranges. Include multi-location files, directory-root
  boundary reads, chunk repair/failover and URI size limits. No zero-KV claim
  may be inferred from merely skipping `FileRepository::resolve`; assert zero
  Iceberg file-location and file-state KV RPCs for direct reads. Normal chunk
  layout resolution remains supported and is measured independently.
- [ ] Test public-key order and delimiter grouping when internal logical names
  sort differently; test max-keys zero, empty pages and credential refresh.
- [ ] Test suffix/open-ended/out-of-bounds ranges, ETag conditions, unknown
  length upload, multipart abort-vs-complete races and response-loss retries.
- [ ] Move a table, drop/recreate its old namespace, then resolve and read via
  the new name; assert no cascade deletion or storage namespace ID reuse.

- [ ] Create a table in the default namespace and an explicit namespace;
  inspect metadata, KV keys, and native file locations.
- [ ] PUT/GET/HEAD/list/multipart a parquet file through an S3 SDK and verify
  the object is below the canonical table prefix, bytes remain in native
  Iceberg chunk storage, the URL is unchanged by commit, and direct versus
  fallback reads are observable.
- [ ] Exercise Arrow HeadBucket, exact HEAD miss directory fallback, recursive
  and non-recursive ListObjectsV2, continuation expiry, and rejection of empty,
  catalog-only, namespace-only, foreign-table, and bucket-wide prefixes.
- [ ] Rename within a namespace and move across namespaces; reload metadata and
  read the original file through the new name without moving or duplicating it.
- [ ] Drop and recreate the same name; verify the new table cannot see the old
  path and GC/recovery retain the correct ownership.

E2E:

- [ ] Run PyIceberg, Java Iceberg, and Rust Iceberg lifecycle flows with native
  FileIO credentials and the new URI prefix.
- [ ] Run the container injection flow and verify the Iceberg report's
  `remote_uri` values separately from any optional general-S3 mirror report.
- [ ] Exercise response loss, restart, multipart recovery, and retired-table
  grace paths with the new key ranges.
- [ ] Exercise v1 legacy reads/writes, rejected in-place activation, v1/v2 collision checks,
  direct locator tampering, foreign-table use, checksum/length conflicts, and
  the no-manifest-rewrite guarantee.

## Consolidated file list

- Identity and URI: `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/table/record.rs`.
- KV keys and scans: `lib/crowdb-access-iceberg/src/key/`,
  `lib/crowdb-access-iceberg/src/file/key.rs`,
  `lib/crowdb-access-iceberg/src/catalog/storage.rs`.
- Metadata scale: `lib/crowdb-access-iceberg/src/metadata_projection/`,
  `lib/crowdb-access-iceberg/src/table/metadata/`.
- Native direct reads: `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`.
- HTTP/FileIO: `app/crowdb-access-server/src/iceberg/`,
  `lib/crowdb-access-iceberg/src/wire/credentials.rs`.
- Lifecycle and GC: `lib/crowdb-access-iceberg/src/commit/`,
  `lib/crowdb-access-iceberg/src/table/lifecycle/`,
  `lib/crowdb-access-iceberg/src/gc/`.
- Loader/container: `/cjdata/cpp/crowdb-tpc-loader/src/crowdb_tpc_loader/`,
  `/cjdata/cpp/crowdb/tools/load-tpch.py`,
  `container/single-node-container/README.md`.
