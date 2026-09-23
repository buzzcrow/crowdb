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

The milestone does not advertise views, multi-table transactions, register-table,
server-side scan planning, multiple active catalogs, tenants, or warehouses.
Unsupported endpoints and optional features return the precise standard
unsupported response and perform no mutation.

The user approved an earlier functional checkpoint in this order: finish R179,
then R180, R181, R182, and foreground R184 conformance; implement R183 afterward
and finish the remaining R184 gates. This does not remove R183 or complete the
original correctness milestone early. Before reclamation, unreachable storage is
retained, physical file/chunk deletion remains disabled, and logical purge records
a durable pending proof task without claiming that space has been reclaimed.
Ownership and recovery evidence must survive until later candidate discovery.
The functional checkpoint requires capacity monitoring and write admission that
fails before storage exhaustion; per-request bounds alone do not bound retained
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
2. R179 implements namespace authority and standard namespace operations.
3. R180 implements native immutable files, streaming/range FileIO, multipart, and
   generation-local metadata projections. It can proceed after R178 in parallel
   with R179.
4. R181 implements table identity, v1/v2/v3 metadata validation, lifecycle, load,
   list, rename, and drop on R179 and R180.
5. R182 implements atomic create/staged-create and update commits, requirements,
   updates, format upgrades, idempotency, conflict classification, and recovery.
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
   tombstoning requires a complete empty proof. R179 owns the bounded recovery and
   single-key-CAS protocol; there is no cross-key transaction or process lock.
8. **Namespace listing:** scan ordered mappings with bounded over-fetch, validate
   targets in bounded batches, and bind the opaque continuation token to catalog,
   parent, parameters, and last scanned key. Stale mappings are omitted. An absent
   `pageToken` requires one complete response with a null next token; an empty
   `pageToken` starts pagination. R179 defines bounded spooling and a pre-response
   503 on resource exhaustion, never a successful truncated listing.
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

All unresolved human decisions for R179 through R184 are collected here. Continue
independent implementation while awaiting confirmation; settled contracts and
ordinary implementation tasks are not open questions.

- **Namespace latency acceptance:** should every uncontended native namespace
  mutation complete within the existing real-stack fixture's 500-ms admission
  bound, or should functional CRUD use a separate bounded deployment profile
  while retaining that fixture for fast clear/restart testing? The current durable
  journal and HTTP retry ledger sometimes exhaust 500 ms; responses remain
  retryable and publication recoverable. Keeping 500 ms requires further critical
  path/batching work; a separate realistic profile distinguishes semantic
  conformance from a subsecond latency target. Do not enlarge existing timeouts or
  add test-side retries without confirmation. Five diagnostic/fix runs and the
  exact outstanding failure are recorded in the R179 execution plan. Continue
  independent work, but do not claim R179 E2E acceptance or completion.

- **File kind at standard PUT:** may native FileRecord classify verified physical
  format/storage family while the selected manifest owns semantic data/equality-
  delete usage? Standard FileIO supplies a location and bytes, not an Iceberg
  content-kind header. Equality-delete files use ordinary table column IDs and
  the manifest supplies `content` and `equality_ids`, so their bytes alone cannot
  always distinguish them from data files. Recommended: retain immutable physical
  authority and validate semantic usage at manifest/commit admission. Alternative:
  require a per-file upload intent identifying kind, which needs an extension or
  client adaptation. R180 currently requires verified kind before publication;
  do not guess from filenames or silently weaken that contract. Continue bounded
  storage, credentials and format parsing, but defer HTTP kind binding until this
  contract is confirmed.

- **Release engine profiles:** which Spark, Flink, and Trino versions and
  deployment profiles must gate the first functional release? Testing all three
  immediately provides broader interoperability evidence but increases fixture
  and environment work; selecting one initial release profile accelerates the
  checkpoint while the other profiles remain pending R184 acceptance. Implement
  the common harness and specification fixtures without waiting for this choice;
  do not silently claim untested engine support.
- **No-GC trial capacity:** what deployment storage budget and reserved free-space
  margin should apply until R183 lands? A fixed byte budget is predictable for a
  dedicated trial; a backend-capacity-based threshold accommodates shared storage
  but needs reliable capacity accounting. Bounded request/session implementation
  is independent of this choice. Do not enable unattended sustained writes or
  invent a production capacity guarantee before the deployment policy is set.
