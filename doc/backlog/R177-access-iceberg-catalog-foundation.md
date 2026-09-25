<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R177: access server / Iceberg — Native Iceberg storage blueprint

## Problem

CROWDB has Chunk-KV, chunk storage, and an S3 protocol, but it does not yet have an
Iceberg authority. Treating Iceberg as ordinary S3 objects would lose the catalog
name hierarchy, atomic table commits, immutable metadata and data files, snapshot
reachability, and spec-defined conflict behavior. It would also allow general S3
overwrite and delete rules to violate the Iceberg table specification.

The implementation needs one program contract before catalog, namespace, table,
file, reclamation, REST, and cache work can proceed independently. This requirement
owns that contract and every decision shared by R178 through R185. The permanent
architecture is [Native Iceberg Storage](../design/access-server/iceberge/design-crowdb-iceberg.md),
and the backed-up Apache specifications under `doc/design/access-server/iceberge/` are the
normative protocol and format references.

## Solution

### 1. Core milestone

The first milestone implements one active catalog, multipart namespaces, table
CRUD and rename, Iceberg format v1, v2, and v3 metadata and files, optimistic table
commits, snapshots and references, native immutable file storage, and an
Iceberg-owned S3-shaped FileIO surface. Each format version has separate
parse/read/create/write capabilities. The implementation supports the spec-defined
v1-to-v2 and v2-to-v3 upgrades only after validating every intermediate invariant.

The format profile includes schema, partition-spec, and sort-order evolution;
sequence numbers and row-level deletes; row lineage, deletion vectors, default
values, and v3 types and encodings; snapshot references and retention metadata;
statistics and partition statistics; and the Avro, Parquet, ORC, and Puffin rules
needed by those features. Optional behavior is capability-gated where the table
spec permits it. A server must not advertise write support for a version while
ignoring a mandatory field, inheritance rule, validation, or file encoding.

The initial functional checkpoint selects Parquet data/delete files and Puffin
deletion vectors. Selected ORC validation is explicitly deferred to R186 by user
decision; ORC byte storage is not a selected-file validation capability. ORC is
not a prerequisite for this checkpoint, and unsupported selected formats fail
explicitly. This narrows the initial checkpoint, not the eventual format profile.

The milestone does not advertise views, multi-table transactions, register-table,
server-side scan planning, multiple active catalogs, tenants, or warehouses.
Unsupported endpoints and optional features return the precise standard
unsupported response and perform no mutation.

The user approved a foreground functional checkpoint before reclamation. R179
through R182 are complete; continue foreground R184 conformance.
Implement R183 afterward
and finish the remaining R184 gates. This does not remove R183 or complete the
original correctness milestone early. Before reclamation, unreachable storage is
retained, physical file/chunk deletion remains disabled, and logical purge records
a durable pending proof task without claiming that space has been reclaimed.
Ownership and recovery evidence must survive until later candidate discovery.
The functional checkpoint uses existing provisioned disk capacity: insufficient
eligible space prevents new chunk allocation. It requires no separate Iceberg
quota or pre-full write-stop policy. R183 owns full-capacity failure/recovery
acceptance and later reclamation; per-request bounds do not bound retained
storage. No mandatory semantics of an advertised version are deferred.

### 2. Authority hierarchy

```text
system root -> active CatalogId/activation epoch
  -> catalog authority
     -> namespace name index -> NamespaceId -> namespace authority
        -> table name index -> TableId -> TableHead/current metadata generation
           -> immutable metadata, manifest, data, delete, and statistics files
```

- `CatalogId`, `NamespaceId`, `TableId`, and `FileId` are random, non-zero,
  non-reused 128-bit CROWDB identities. Iceberg's `table-uuid` remains a distinct
  spec field in table metadata.
- Name mappings are ordered lookup indexes. Stable-ID records are authoritative;
  list and load filter mappings whose ID, lifecycle, or name epoch is stale.
- All Iceberg keys use a versioned `ICE\0` protocol prefix. Resource authorities,
  indexes, and operation payloads live below their CatalogId. A separate bounded
  system scope holds the active root, management operations/audit, and REST
  idempotency bindings that must survive catalog replacement. System records never
  provide a resource lookup path into a retired catalog.
- Chunk-KV stores bounded authorities, mappings, heads, operation state, and file
  records. Chunk storage owns all non-inline file bytes. Disk, EC, placement, and
  node identities never enter Iceberg metadata or locations.
- The immutable standard table metadata JSON plus the `TableHead` that selects it
  are the recoverable table-state authority. Binary projections are disposable,
  generation-qualified accelerators.

### 3. Program invariants

- **ICE-I1 — Stable identity:** rename never changes a CatalogId, NamespaceId,
  TableId, FileId, metadata location, or committed bytes.
- **ICE-I2 — One authority:** an index, cache, projection, or notification cannot
  publish or repair catalog state; it must validate against its stable authority.
- **ICE-I3 — Atomic generation:** one successful commit performs one `TableHead`
  compare-exchange that selects one complete immutable metadata generation.
- **ICE-I4 — Immutable files:** a published canonical location always resolves to
  the same length, digest, and bytes and cannot be overwritten.
- **ICE-I5 — Bounded work:** no authority value contains unbounded children; every
  request, scan page, stream window, projection, operation, retry, and GC batch has
  independent byte, item, and concurrency limits.
- **ICE-I6 — Recoverable mutation:** a durable request identity, request digest,
  phase, and result make response-loss retry safe on another Access Server. Reuse
  of one identity with different input fails.
- **ICE-I7 — Domain clear:** after clear completes, no new request, cache entry,
  credential, location, or resource retry can expose retired catalog resources.
  Authorized management status, audit, and result replay may identify the retired
  CatalogId without granting access to its resources.
- **ICE-I8 — Spec honesty:** only implemented endpoints and format capabilities are
  advertised; unknown, disabled, or lossy requirements and updates fail closed.
- **ICE-I9 — Protocol ownership:** the Iceberg FileIO surface shares low-level
  storage clients with S3 but never uses general S3 bucket/object authority.
- **ICE-I10 — Lock-free hot path:** implementations add no catalog-wide or global
  cache lock to lookup, load, commit, or file streaming.

### 4. Requirement decomposition and order

1. R178 establishes the library, active catalog domain, management safety, stable
   key/value envelope, server lifecycle, and `/v1/config` baseline.
2. R179 is complete: namespace authority, bounded standard REST operations,
   child/drop fencing, official-client boundary acceptance and native restart.
   Its contract is retained in [Native Iceberg Storage](../design/access-server/iceberge/design-crowdb-iceberg.md).
3. R180 is complete: native immutable files, bounded streaming/range FileIO,
   durable multipart, delegated credentials and validated generation-local
   metadata projections, with native fault/restart and official SDK acceptance.
4. R181 is complete: table identity, v1/v2/v3 metadata validation, lifecycle,
   load/list/exists, rename/drop and fault/replay acceptance on native files.
5. R182 is complete: atomic create/staged-create and update commits, requirements,
   format upgrades, idempotency, conflict classification and native fault recovery.
6. R183 implements snapshot-aware purge, orphan cleanup, retired catalog cleanup,
   and bounded reclamation after R180 through R182 define reachability.
7. R184 completes public REST integration, authentication, endpoint discovery,
   standard errors, and official-client conformance for the core profile.
8. R185 adds bounded caches and cross-server invalidation after all identities,
   epochs, digests, and reclamation fences are stable.

R178 through R184 form the correctness milestone. R185 is a later performance
milestone and cannot be required for correctness.

### 5. Resolved open issues

All open issues from the former R177-A through R177-D drafts and the former R178
cache draft are answered here. Child requirements must reference these decisions
and must not carry independent open questions.

1. **Clear boundary:** a root CAS enters durable maintenance and stops fresh
   authoritative admission and lease renewal. Existing root leases may still admit
   old-context work until their fixed expiry. A later root CAS selects the new
   catalog while maintenance remains active. Clear completes and new-catalog
   admission opens only after the persisted old-lease, request, and delegated-access
   deadlines have passed. Lease validity starts before the authoritative root read,
   never on receipt of a delayed response; delegation cannot extend the bound.
   Restart and reconciliation preserve those deadlines and configured clock-skew
   allowance. No instance-registry acknowledgement is required. Reclamation also
   waits for durable reader and operator pins. R178 owns this state machine.
2. **Tenant:** the first milestone stores no default tenant and puts no fixed
   TenantId in hot keys. A later tenant root may map to an active CatalogId without
   changing catalog-scoped keys.
3. **Warehouse:** absent or empty `warehouse` selects the sole catalog. Any non-empty
   value returns the spec-defined `NoSuchWarehouse` response; it is never ignored
   or created implicitly.
4. **Retired-catalog safety:** a configurable minimum retention period, clear grace,
   durable reader pins, and explicit operator pins all fence physical GC.
5. **Catalog management:** R178 owns authenticated initialize/status/rename/clear
   management commands. Clear requires a dedicated privilege, explicit destructive
   confirmation, request identity, and durable audit record; it is not an Iceberg
   REST endpoint. System-scoped management records preserve the original result
   across later clears; authentication and digest validation precede replay, and
   replay precedes checking the current epoch for a new mutation.
6. **Namespaces:** arbitrary multipart identifiers are supported within configured
   maximum levels and encoded bytes. Parent listing is complete. Namespace rename
   is not implemented because it is non-standard; table rename may move across
   namespaces.
7. **Namespace drop:** child creation and rename-in first durably reserve their
   parent/name index entry, then validate the parent through a `Ready` CAS before
   publication. Drop CASes the parent to `Dropping` before probing those same index
   ranges. Unresolved reservations prevent an empty proof; recovery settles them
   before removal. A published child restores `Ready` and returns not-empty;
   tombstoning requires a complete empty proof. The namespace layer implements
   bounded recovery and single-key CAS; there is no cross-key transaction or process lock.
8. **Namespace listing:** scan ordered mappings with bounded over-fetch, validate
   targets in bounded batches, and bind the opaque continuation token to catalog,
   parent, parameters, and last scanned key. Stale mappings are omitted. An absent
   `pageToken` requires one complete response with a null next token; an empty
   `pageToken` starts pagination. The namespace layer implements bounded spooling
   and a pre-response 503 on resource exhaustion, never a successful truncated listing.
9. **Namespace properties:** at most 256 entries; keys and values are UTF-8 without
   NUL, at most 1 KiB and 8 KiB respectively; the encoded authority is at most
   64 KiB. Duplicate remove/update keys return the standard 422 response. Mapping
   name epochs, property revisions, and admission fences are distinct; property
   updates and failed drops never invalidate an otherwise current name mapping.
10. **Metadata projections:** metadata JSON gets a durable, disposable,
    generation-local root/page/child projection. Other parsed format structures
    stay in R185's memory cache until measurements justify a later requirement.
11. **Register table:** it is deferred and not advertised because external
    locations could bypass native CROWDB file authority.
12. **Table identity:** CROWDB TableId and Iceberg `table-uuid` are distinct and
    both validated. Neither is derived from a mutable name.
13. **Rename and drop:** durable operation records reserve destinations and drive
    a recoverable state machine. `TableHead` name epoch/lifecycle decides validity;
    stale source or target mappings are filtered. An old name never remains an
    alias after rename.
14. **Snapshot loading:** both `ALL` and `REFS` are supported for declared endpoints;
    each is generated from the same selected metadata generation.
15. **Format profile:** v1, v2, and v3 each support parse, read, create, and write.
    Mandatory version-specific semantics are implemented, and v1-to-v2 and
    v2-to-v3 upgrades are supported. Optional spec features remain separately
    capability-gated and cannot be silently discarded.
16. **Canonical location:** use the table prefix
    `s3://iceberg-<catalog-id-base32>/t/<table-id-hex>/`. An exact client-created
    relative key beneath it maps once to a server FileId. The reserved bucket and
    prefix are decoded by the Iceberg-owned FileIO service; catalog, namespace, and
    table names never participate in a location.
17. **FileIO operations:** the first writable milestone includes immutable PUT,
    HEAD, one-range GET, create/upload/list/complete/abort multipart, and delegated
    credentials. Bucket CRUD, overwrite, tagging, lifecycle, and unrestricted
    DELETE are unsupported.
    Standard FileIO PUT supplies a location and bytes without Iceberg content
    type. Native file authority records the verified physical format and may keep
    semantic kind unbound. A selected manifest or metadata reference supplies
    semantic usage; load and commit admission validate that usage against canonical
    bytes before publishing a table head. Filenames and Parquet schemas never
    decide data versus equality-delete kind.
18. **Multipart:** multipart is required for the first writable milestone; all
    sessions, parts, bytes, TTLs, completion, and abort work are durable and bounded.
19. **Reclamation:** R183 uses generation-indexed candidates plus traversal from
    retained snapshot roots and metadata logs. It never relies on racing per-file
    reference counts.
20. **Projection retention:** metadata projections are generation-local without
    cross-generation content deduplication in the first milestone. Simple bounded
    GC is preferred over reference-count and write-amplification complexity.
21. **Cache clear fencing:** R185 uses the lease-plus-grace clear boundary in item
    1; notification remains only a latency optimization.
22. **Cache limits:** R185 defines separate configurable hard caps for every class,
    queue, batch, fill, and fanout dimension. Initial defaults come from its focused
    benchmark gate and configuration tests; no class inherits an unbounded or
    universal one-size value.
23. **REST retries:** R178 supplies optional UUIDv7 `Idempotency-Key` handling and
    an advertised retention window before namespace endpoints land. Durable system
    bindings fix the principal, operation, digest, CatalogId, and activation epoch;
    retired bindings reject resource replay and cannot initiate work in the new
    catalog. Final successes and deterministic terminal 4xx are replayed; 5xx do
    not finalize the operation. Requests without a key have internal recovery
    identities but no cross-request exactly-once guarantee.

### Confirmed Compatibility Decisions

- **Storage capacity boundary (OI-3, confirmed 2026-09-24):** use the existing
  disk provisioning/allocation flow, including configured capacity limits for
  the current file-backed simulated disks. When managed disk capacity cannot satisfy
  a new chunk, allocation fails naturally; do not add an Iceberg-layer quota or
  pre-full stop threshold for the current checkpoint. R183 records full-capacity
  failure safety, later GC and recovery requirements. This settles the capacity
  policy, not an assertion that unimplemented GC or full-disk tests have passed.

- **Engine testing deferred (OI-2, confirmed 2026-09-24):** do not run Spark,
  Flink or Trino acceptance in the current implementation phase. Record this in
  the execution plan's Next section; the user will establish a separate testing
  project later and select its engine/version/deployment matrix there. This
  removes the immediate selection decision, not the outstanding conformance
  obligation. Do not advertise untested engine compatibility or close full R184
  acceptance on existing SDK evidence alone.

- **Functional/performance acceptance split (OI-1, confirmed 2026-09-24):**
  functional correctness uses a bounded runtime profile independently of a
  subsecond latency target. Preserve the original 500-ms clear/restart timing
  coverage; move full namespace CRUD assertions to a separate functional test,
  not out of the suite. A larger functional deadline is not a performance
  improvement or latency guarantee. Fix only obvious performance bugs with
  demonstrated root causes and correctness regression tests. Record broader
  optimization candidates for a later consolidated performance backlog.
  Do not add test-side retries, suppress failures, weaken assertions, bypass
  durability/authorization, or change concurrency/clear semantics to fabricate
  a performance result.
  A native fault-matrix diagnostic returned one unconfirmed five-second
  `Store(Client(Deadline))` from the independent verification client after HTTP
  replay succeeded. Subsequent complete acceptance passed without changing that
  timeout or adding retries. Track the observation and future routing/transport
  capture in the [performance follow-up](../working/plan-iceberg-functional-catalog.md#performance-work-to-consolidate-later);
  do not claim its root cause is fixed or turn it into a new human design choice.

- **Name-mapping interoperability profile (confirmed 2026-09-24):** selected-use
  admission uses the pinned Java 1.11.0 SDK-safe intersection. Reject colliding
  dotted paths and multiple ID-less mapping nodes; preserve segmented paths and
  literal dots for accepted mappings. This is an input-profile restriction, not
  a claim that the table specification bans those cases. The pinned SDK's
  [MappingUtil](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/mapping/MappingUtil.java)
  flattens nested paths with dots into unique map keys, and its ID index treats
  repeated null IDs as duplicates. Thus a literal `a.b` alongside child `b` of
  `a`, or multiple ID-less imported fields, can fail SDK indexing even when
  structurally valid under the table format. Structural parsing remains separate
  from selected-use compatibility validation; never flatten an ambiguous path
  into a different field binding.

- **Direct format upgrades (confirmed 2026-09-24):** allow explicit v1-to-v3,
  applying both intermediate version rules internally. The pinned official
  [TableMetadata.Builder](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/TableMetadata.java)
  `upgradeFormatVersion` rejects downgrades and unsupported targets but does not
  reject skipped versions. The evaluator expands the request into adjacent
  internal steps; transition checking consumes that expanded trace. This does
  not relax downgrade, unsupported-version or semantic-preservation checks.

## Dependencies

- Depends on routed Chunk-KV compare-exchange and scans, chunk streaming and range
  reads, stable request identities, Group-0 service discovery, and `crowdb-rpc`.
- The Apache Iceberg REST OpenAPI and table specification in
  `doc/design/access-server/iceberge/` are normative. Apache Java, `iceberg-rust`, official
  clients, and the REST Compatibility Kit are test oracles, not production
  authorities.
- R178 through R185 depend on this requirement. A material decision change must
  first update R177 and the affected acceptance contracts.
- General S3 requirements do not gate Iceberg correctness. Shared chunk and
  transport improvements may be reused only below the protocol-authority boundary.

## Acceptance

- Given the eight child requirements, when their scopes and dependencies are
  inspected, assert every core Catalog, Namespace, Table, commit, FileIO,
  reclamation, REST, and cache concern has exactly one owner and R185 is not on the
  correctness path. Invariant: ICE-I2 one authority. Integration test.
- Given every declared v1, v2, and v3 capability and upgrade, when metadata and
  files are compared with the backed-up table spec and reference implementations,
  assert mandatory semantics are preserved and unknown or disabled optional
  features fail before mutation. Invariant: ICE-I8 spec honesty. Integration test.
- Given any supported or unsupported REST endpoint, when `/v1/config` and an
  operation are compared with the backed-up OpenAPI, assert advertised behavior is
  implemented and unadvertised behavior fails without mutation. Invariant: ICE-I8
  spec honesty. E2E test.
- Given rename, commit, clear, response loss, and crash injection, when another
  Access Server resumes the operation, assert stable identities, one selected
  generation, retry digest equality, and the clear boundary remain true.
  Invariants: ICE-I1, ICE-I3, ICE-I6, and ICE-I7. Integration test.
- Given metadata from bytes to hundreds of MiB and data files to TiB scale, when
  load, commit, range read, and GC execute, assert no KV value, request allocation,
  stream window, page, or background batch grows with the complete table or file.
  Invariant: ICE-I5 bounded work. Integration test.
- Given the canonical S3-shaped location and a general S3 object with a similar
  textual key, when each is accessed, assert only the Iceberg authority can publish,
  overwrite, authorize deletion, or reclaim the Iceberg file. Invariants: ICE-I4
  and ICE-I9. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`

## Open Questions

OI-1 separates functional/performance acceptance; OI-2 defers engine testing to
the user's later independent project;
OI-3 uses the existing disk/chunk allocation capacity boundary, with remaining
GC and exhaustion-recovery requirements recorded in R183.

- **OI-4 — Partition-statistics historical fields (confirmed):**
  the backed-up specification's Partition Statistics File section describes a
  union of all historical partition fields. Pinned Java 1.11.0
  [Partitioning.partitionType / allActiveFieldIds](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/Partitioning.java)
  instead filters out fields whose source columns are absent from the current
  schema. The user confirmed compatibility with this SDK projection: accept
  this explicit omission case while validating retained
  fields, types, row ordering and counts; do not silently treat omitted partition
  values as known or broaden omissions to arbitrary fields. Statistics publication
  now validates canonical rows and selected manifest counts; accepted immutable
  references retain their writer semantics across evolution. This decision is resolved.

- **OI-5 — Ordinary delete-rewrite equivalence responsibility (confirmed):**
  distinguish valid file/metadata structure from proving that a rewrite preserves
  the logical set of live rows. Java 1.11.0
  [RewriteFiles](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/api/src/main/java/org/apache/iceberg/RewriteFiles.java)
  requires the caller's replacement data/delete records to preserve logical
  equivalence. Its
  [REST CatalogHandlers.commit](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java)
  validates requirements, applies metadata updates and delegates publication;
  that handler does not scan rows to prove equivalence.
  - User decision: keep this computation the writer/engine's responsibility,
    preserving CROWDB's implemented authorization, immutable-file authority,
    schema/sequence/partition validation, position bounds, DV merge checks and
    atomic publication. Add explicit compatibility tests and document that
    ordinary equality/position-delete rewrites are not a server-side row-set
    equivalence proof. This is not permission to bypass existing checks.
  - Do not implement server-side row-set equivalence evaluation or make it a
    catalog completion prerequisite. This assigns responsibility; it does not
    assert that every engine independently recomputes and verifies its output.
    Filename retention, matching counts or rejecting every removed delete are
    not substitutes for equivalence and must not restrict legal compaction.
  - This decision does not change the confirmed partition-statistics omission,
    GC, ORC or engine-test deferrals. No human decision remains pending here.

Unfinished implementation and unexecuted acceptance remain in the working plans.
R179–R182 are closed by their acceptance gates, not by these decisions.
R183–R184 remain open; this does not imply engine/GC conformance.

- **OI-6 — Legacy zero format capability bits (resolved):** existing catalog
  authorities persist zero even though installed table routes currently accept
  v1/v2/v3 operations. Startup, REST, FileIO and credential refresh reject any
  nonzero bits; config therefore advertises false while table operations work.
  R184 must establish a durable version policy before changing these checks.
  The operator explicitly activates a validated supported profile with an
  authenticated, CAS-backed management operation. Zero remains literal and is
  never silently widened on startup. The operation preserves catalog identity,
  activation epoch, names, bounds, tables and prior retry records, and advances
  only configuration generation. A pre-activation config request must not
  advertise unsupported values as an operational profile; existing table data
  remains intact while the operator rolls out activation. Clear creates a new
  zero-profile catalog and therefore requires explicit activation again.
