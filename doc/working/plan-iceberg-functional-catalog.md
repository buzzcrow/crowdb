# Iceberg Functional Catalog Plan

Upstream: [R177 blueprint](../backlog/R177-access-iceberg-catalog-foundation.md),
[R179 namespaces](../backlog/R179-access-iceberg-namespace.md),
[R180 FileIO](../backlog/R180-access-iceberg-fileio.md),
[R181 lifecycle](../backlog/R181-access-iceberg-table-lifecycle.md),
[R182 commits](../backlog/R182-access-iceberg-table-commit.md),
[R183 reclamation](../backlog/R183-access-iceberg-reclamation.md),
[R184 conformance](../backlog/R184-access-iceberg-rest-conformance.md).

Goal: implement a usable native catalog in dependency order without presenting
deferred storage reclamation as completed correctness work.

Persistent-plan exception: this file coordinates multiple requirements. Remove
completed tasks and their obsolete upstream links; retain the plan until the
program finishes. Each requirement keeps its own detailed execution plan.

Status: the user approved this ordering and implementation of independent work.
Collect unresolved human decisions in R177 for confirmation when the user returns;
do not stop unrelated tasks. No user-guide tasks.
Continue independently while the user is away. The active foreground scope is
R177 through R184 excluding R183 physical GC; ORC belongs to deferred R186.
Keep human choices in R177, implementation gaps here, and commit verified slices.

Handover checkpoint (2026-09-23): contextual manifest decoding now includes
historical schema/spec binding, partition tuples, typed bounds/equality fields and
a list-bound reader with EOF totals and cancellation poisoning. Generation-local
metadata projection pages and canonical streaming fallback are also implemented;
multipart part LastModified, S3-shaped response serialization and intersected
grant/service/session byte limits are implemented as separate components;
native FileIO routing and physical sealing are connected, and a pinned Apache
Iceberg 1.11.0 / AWS SDK 2.44.4 FileIO baseline now passes with default signed
checksum trailers and streamed Complete responses;
partition summaries, bounded Variant bounds and a scoped streaming DV cross-file
validator are now implemented. Candidate snapshot enumeration/admission and
table load/commit wiring remains pending. Resume instructions, exact next implementation slices,
landed APIs, remaining integration gaps and test commands are in
`plan-iceberg-fileio.md` under `Handover — 2026-09-23`. Do not interpret this
pause as R179/R180 completion. R181/R182/R183 and full R184 are still pending.

## Remaining Complexity Review

- **Highest: atomic commits and creation (R182)**. Requirement/update evaluation,
  immutable candidate metadata, namespace admission, one head-CAS publisher,
  lost-response replay and v1/v2/v3 evolution must agree on a single generation.
  This depends on unfinished FileIO and table state, so do not implement it as an
  isolated HTTP handler or advertise write support from partial coverage.
- **Highest: reclamation safety (R183)**. Cross-snapshot reachability, catalog/table
  generations, pins, reader/delegation leases, retained multipart evidence and
  crash-safe deletion proofs are coupled. Keep the approved deferral and physical
  deletion disabled; this is not a cleanup job that can safely use only TTL.
- **High: remaining FileIO semantics (R180)**. Collection schemas, equality IDs,
  partition/metric validation and snapshot-wide DV/row-lineage checks require
  bounded traversal plus table context. The scalar bridge and equality-ID lists are landed:
  typed IDs/paths, v1/v2/v3 inheritance, atomic failure behavior and cross-block
  state have focused tests. Six metric maps now have bounded decoding and structural/count
  validation before inheritance. Historical schema/spec context, partition tuples,
  typed scalar/geospatial bounds and a list-bound streaming reader are now implemented.
  Partition summary decoding and reader containment/EOF validation are implemented.
  Variant bound objects also have bounded decoding and typed ordering checks.
  The DV validator binds Puffin bytes to live manifest/data records, checks row
  range, partition/sequence applicability and uniqueness over sorted inputs.
  Remaining complex work includes complete candidate snapshot enumeration,
  prior-delete replacement and actual data-file semantics. Use these components;
  do not conflate them with full seal or commit acceptance.
- **High: multipart/HTTP composition (R180)**. Durable credits, parts, completion,
  publication and recovery primitives are wired into native HTTP routes. A pinned
  Java FileIO test covers default signed checksum trailers, Complete, reads and
  embedded errors. Wider client profiles, optional multipart checksum metadata,
  table credential vending and selected-use semantics remain.
  Invalid frozen selections and uncertain publication must not acquire a second
  HTTP-only state machine. Standard PUT retains ambiguous kinds as unbound until
  selected metadata supplies the declared use, as already approved.
- **High: namespace/table races (R179/R181)**. Create/rename-in versus namespace
  drop needs shared admission and crash recovery; bounded table heads, logical
  drop and purge intent are still prerequisites. Preserve the separate namespace
  latency blocker instead of weakening its acceptance fixture.
- **Medium, good bounded follow-ups**: additional negative format fixtures,
  multipart request-body decoding and selected metadata projection consumers.
  Take one small verified slice per commit. None alone completes a catalog server.
- **Broad integration cost (R184)**: official FileIO/REST clients, cancellation,
  native restarts and engine/version matrices. Start foreground vertical slices
  as lifecycle/commit features land; GC-dependent acceptance remains last. Release
  engine profiles and no-GC capacity policy remain human choices in R177.

## Review checkpoint

- R178 supplies catalog management, authentication, recovery, and config. The
  current HTTP dispatcher accepts authenticated config and namespace CRUD.
- R179 has identifiers, properties, authority/mapping records, bounded scans,
  conditional deletion, separate writer credentials, payload pages, and durable
  create/property/drop drivers, shared helping, periodic repair, listing and REST.
  Official CRUD acceptance awaits the R177 latency decision; future table
  create/rename-in admission remains pending.
- R180 through R184 have no corresponding completed feature implementations.
  Shared infrastructure is reusable, but is not acceptance of these requirements.
- A listening config service already works. A namespace catalog needs R179.
  A native catalog that clients can create tables in, write to, and read from
  needs R180, R181, R182, and the relevant R184 integration and client tests.
- R177's full correctness milestone includes R183 and all R184 acceptance.
  An earlier functional checkpoint must not be labelled that full milestone.

## Approved reclamation deferral

- Defer R183 execution, not its backlog or safety contract. R180 explicitly
  allows unreachable staged/orphan data to leak before reclamation; R181 permits
  logical drop without cleanup; R182 keeps losing candidates unreachable.
- Keep physical deletion of Iceberg-owned files and chunks disabled, including
  implicit cleanup by upload expiry, multipart abort, table drop, and catalog
  clear. Logical expiration, bounded recovery, and publication fencing still run.
- Preserve ownership, generations, durable operation outcomes, upload state, and
  purge intent needed for later candidate discovery. Do not remove the last
  evidence of retained storage while recycling bounded foreground state.
- For `purgeRequested=true`, persist a pending proof task before reporting the
  logical drop complete, as R181 requires. Do not report physical purge complete
  or expose a public file DELETE route. Worker status/control remains unavailable
  until implemented and verified.
- Without reclamation, cumulative retained storage is not bounded by per-request
  or session limits. Use a capacity-limited trial with monitored free capacity;
  stop admitting writes before exhaustion. This is not a sustainable long-running
  production storage policy.
- Retention, reader/credential leases, and operator pins must be enforced before
  any future deleter is enabled. Deferral is not permission to replace positive
  reachability proof with TTL-only deletion.
- Run R184's foreground integration early, but leave its reclamation-dependent
  acceptance and original completion status pending. Update upstream milestone
  wording reflects the approved split while retaining the full milestone.

## Dependency-ordered execution

### Remaining tasks from the current five-task batch

The first task, the pinned official FileIO baseline, is verified. Details and
commands are in `plan-iceberg-fileio.md`, official Java checkpoint.

- [ ] **Credential vending**: implement the standard REST storage-credential
  response and refresh contract from the pinned OpenAPI and official SDK. Reuse
  `FileGrantIssuer`; derive operations from the authenticated read/write role.
  Its live endpoint depends on the selected table identity/lifecycle below;
  implement the wire/issuer slice first, then attach it with table loads.
  Wire slice: `wire/credentials.rs` serializes one exact table prefix and the
  SDK's access key, secret, session token and decimal millisecond expiry. Keep
  secrets out of Debug. Issuance binds authenticated principal, fresh nonce and
  server byte/TTL limits; only the independent writer receives mutations.
  Test all four roles, refresh rotation, expiry/overflow and cross-table denial.
  Standard evidence: pinned OpenAPI `StorageCredential`/`LoadCredentialsResponse`
  and Apache Iceberg 1.11.0 `VendedCredentialsProvider` (requires the expiry
  property, refreshes five minutes before expiry, accepts exactly one S3 grant).
  SDK factory activation uses `client.refresh-credentials-endpoint`, not the
  provider-internal `credentials.uri`; verified against pinned
  [AwsClientProperties](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/aws/src/main/java/org/apache/iceberg/aws/AwsClientProperties.java)
  and [VendedCredentialsProvider](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/aws/src/main/java/org/apache/iceberg/aws/s3/VendedCredentialsProvider.java).
  Wire/issuer slice verified: library all-target tests, fmt, workspace lint and
  explicit server `iceberg-e2e` lint pass. Official Java FileIO fetched the Rust
  response from a test HTTP endpoint, cached it, and completed real native PUT,
  multipart, HEAD, GET, seek and embedded-error checks (178.19 s). This is not
  a production catalog credentials endpoint or a timed refresh acceptance test.
  Maven reports the existing SDK daemon-thread cleanup warnings with exit 0.
- [~] **Selected-use validation**: complete format semantics and validate
  canonical unbound files against trusted metadata/manifest declarations. Do not
  infer use from names, headers or upload container bytes.
  First slice: `ManifestReader` binds unbound canonical uploads only to the
  selected manifest declaration; `ManifestListReader` streams the selected list
  with one bounded decoded block, exact location/kind checks and cancellation
  poisoning. Both require EOF before claiming completion. `bind_kind` must
  validate the original authority before constructing any derived view.
  Keep historical writer-version selection, snapshot enumeration completeness,
  cross-manifest invariants and Parquet/ORC data/delete semantic checks separate;
  this streaming slice does not establish a publishable table generation.
  Verified the streaming slice with library all-target tests and focused
  canonical-corruption/invalid-authority tests, fmt and workspace lint. No table
  capability is advertised by these helpers; production credential endpoints
  remain dependent on live table authority, not arbitrary caller TableIds.
  Verified selection slice: `ManifestListSelection` and `open_selected` bind the
  list to trusted historical snapshot ID, parent, sequence and v3 row-ID range.
  Validate optional OCF linkage against that selection, reject future manifest
  sequences and require newly added manifests to use the snapshot sequence.
  Preserve compatibility with writers that omit these non-required OCF keys;
  never substitute current table format version for the historical writer.
  Standard evidence: pinned specification, Snapshots and Manifest Lists, and
  [official ManifestListWriter](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/ManifestListWriter.java)
  (including literal `null` parent metadata). The official
  [ManifestLists reader](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/ManifestLists.java)
  projects fields rather than requiring the writer's optional OCF linkage.
  This slice does not prove row-ID assignment intervals or validate data/delete
  bytes. Tests cover optional/official-style
  headers, empty-list mismatches, scope overflow, reused/new manifest sequences,
  and poisoned cursors after selection failure.
  Enumeration slice: `SnapshotManifestReader` owns a fresh selected list and
  sequentially resolves each canonical manifest plus trusted historical context.
  It cannot skip missing/corrupt manifests or bypass EOF totals. Retain one list
  block, one manifest reader and a separately budgeted identity index; cap manifests, entries and aggregate
  manifest bytes. `finish` exposes counts only after the list and every manifest
  reached verified EOF. Cancellation during authority resolution or inner reads
  poisons the outer cursor. `SnapshotManifestSource` implementations must fence
  the candidate generation; none is wired to production table authority yet.
  Enumeration completion is not data/delete byte validation, DV bitmap/data-row
  binding, historical row-ID preservation, or publication proof.
  Verification: 12 added selection/enumeration tests pass; library all-target
  tests, workspace fmt check and workspace clippy pass. Fixture chunk copies
  preserve owner binding by writing fresh trees rather than relabeling FileIds.
  Historical-read correction: snapshot JSON does not carry a writer format
  version. Follow the specification's Writer Requirements read-compatibility
  matrix instead of inferring an exact historical version. `ManifestListSelection`
  now carries current `table_version`; `ManifestListProjection::for_read` defaults
  missing content/sequences and retains unknown optional counts. The strict `new`
  projection remains available for validating a known writer's output; commit
  integration must enforce new-file writer requirements separately.
  Upgraded v3 tables accept old snapshots with no row lineage, while malformed
  present values and inconsistent optional OCF linkage still fail. Canonical
  list tests cover v1/v2 snapshots in v2/v3 tables with and without writer headers.
  Evidence: pinned specification Writer Requirements and Row Lineage upgrade
  rules; official
  [SnapshotParser](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/SnapshotParser.java)
  preserves historical absent sequence/lineage, and
  [GenericManifestFile](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/GenericManifestFile.java)
  applies field-based defaults without guessing a historical writer version.

  Historical-read gates: 24 focused list/snapshot tests, workspace fmt and
  workspace clippy pass. No new format capability or production route is enabled.

  Remaining execution slices from the ten-task batch, in dependency order:
  2. Cross-manifest identity/descriptor consistency and row-ID assignment ranges.
     Row-ID slice implemented: keep only the snapshot allocation and current/next
     manifest cursor; use actual inherited counts, not row-count estimates.
     Reject missing assignments, overlapping newly assigned intervals, new ranges
     escaping `first-row-id + added-rows`, and reused ranges crossing into the new
     allocation. Gaps and unused allocation remain valid. Scope checks do not
     replace comparison against prior metadata to prove preservation of old IDs.
     Exact identity slice: `SnapshotIdentityIndex` rejects repeated manifests and
     live ordinary paths, checks shared Puffin physical lengths, disjoint DV spans
     and unique DV targets. Deleted entries do not count as live references.
     Distinct DVs in the same Puffin file are valid (specification Row-level Deletes).
     Integrate checks before entries escape `SnapshotManifestReader`; EOF summary
     is unavailable after index failure. Independent node and retained-key-byte
     limits bound transient memory (hard ceilings: one million keys and 64 MiB of
     key bytes); no eviction, probabilistic membership or unbounded collection.
     Larger snapshots currently fail the configured budget rather than spilling;
     future external-memory optimization must retain exactness and orphan evidence.
     These limits are not service-wide admission until production wiring lands.
     Verified 17 identity/snapshot tests plus workspace fmt/clippy; the earlier
     row-ID slice passed 11 snapshot tests. Remaining historical-preservation and
     physical-file checks require prior selected metadata and canonical readers.
  3. Canonical Parquet schema/field-ID/row-count and selected data/delete checks.
     Footer slice: `file/parquet/` decodes bounded Thrift Compact metadata from
     canonical footer ranges, never stored hints. Independent footer/value/depth/
     schema/row-group limits bound input and decoded structures. Validate required
     field types, duplicate Thrift fields, schema preorder/IDs, row-group column
     counts and physical types, column byte spans, and aggregate rows/byte counts.
     Reject encrypted/external metadata explicitly. This is not page decoding or
     complete Iceberg logical-type/equality/position-delete validation. Typed
     logical annotations now retain decimal parameters, integer width/signedness,
     time units/UTC flags, Variant version and spatial CRS/algorithm. Validate
     required annotation wire types and unions; unknown annotation IDs remain
     explicit. Physical/logical compatibility and legacy annotation agreement
     still require separate validation.
     Evidence: Apache
     [Parquet 2.10 IDL](https://github.com/apache/parquet-format/blob/apache-parquet-format-2.10.0/src/main/thrift/parquet.thrift)
     and [Compact protocol](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md).
     `parquet_official_footer.rs` embeds the 730-byte footer from Apache
     [alltypes_plain.parquet](https://github.com/apache/parquet-testing/blob/master/data/alltypes_plain.parquet)
     (original file length 1851, footer offset 1113), exercising real delta headers.
     The test supplies placeholder body bytes and tests only footer interpretation,
     not those data pages or official Iceberg writer acceptance.
     Footer checkpoint: library all-target tests, seven focused footer tests,
     workspace fmt and clippy pass. Collections grow only as decoded values arrive;
     nested advertised sizes cannot multiply speculative vector reservations.
     Annotation checkpoint: three annotation tests and seven footer tests plus
     workspace fmt/clippy pass. Current Parquet IDL supplies Variant/spatial
     annotations; Iceberg's pinned mapping and Java `TypeToMessageType` remain
     the selected-use compatibility contract, not generic Parquet permissiveness.
     Column checkpoint: match every ordered column path to its schema leaf,
     excluding the root and retaining nested/repeated ancestry. Non-repeated
     column value counts (including nulls) must equal row-group row counts;
     repeated columns are not incorrectly constrained to that equality. Four
     column tests cover swapped same-type siblings, paths, counts and ancestry;
     all 14 focused Parquet tests and workspace fmt/clippy pass.
     Selected footer binding: `manifest::read_selected_parquet_metadata` checks
     table/location/format/length and live entry status before storage access,
     binds content kind from the manifest without changing the upload, and compares
     canonical footer rows with manifest `record_count`. Its result is metadata,
     not a schema/delete/page-validation proof. Three tests cover all content kinds,
     incompatible prebound kinds, no-I/O descriptor rejection and false row counts.
     Selected schema/delete slice now implemented through `read_parquet_selection`,
     `validate_parquet_schema` and `validate_parquet_position_deletes`:
     - Field IDs bind to retained historical fields and logical parents; explicit
       bounded name mappings normalize collection paths. No-ID files use the SDK's
       top-level ordinal fallback. Missing required fields accept non-null initial
       defaults; default value interpretation remains the table/read planner's job.
     - Validate primitive/logical mappings, numeric and decimal promotions,
       v3 date promotion, nested/legacy LIST, MAP key/value identity, and Variant
       unshredded/shredded schema layouts. Variant payloads are not decoded here.
     - Equality-delete IDs must be present, unique, eligible primitive fields
       outside collections. Position-delete reserved columns and optional row
       projection bind separately; optional row payloads are not decoded here.
     - Canonical position-delete pages validate every path/position pair, sorted
       order, referenced-file binding and applicable target row bounds. Duplicate
       pairs are permitted. The caller resolves selected-scope applicability;
       old delete files may reference removed data files. Results publish only
       after exact EOF; these functions change no catalog authority.
     - Page V1/V2, PLAIN, dictionary RLE/bitpacking, delta integer/string and
       byte-stream-split encodings are bounded independently by page bytes,
       decoded values, page count and total rows. Supported codecs: uncompressed,
       Snappy, Gzip, Zstd and LZ4_RAW. Unsupported codecs/encodings fail explicitly;
       this is not an unrestricted Parquet reader. Maximum decoded page 8 MiB;
       Zstd windows also capped at 8 MiB. CRC is checked when present.
     - `parquet_iceberg_fixture.rs` contains complete files produced by Iceberg
       Java 1.11.0 / parquet-mr 1.17.1, Zstd, both page versions, 100 deletes each.
       Tests read their actual body bytes, including corruption rejection.
       Reproduction source and pinned Maven dependencies are in
       `tests/common/parquet_java/`; run Maven through `pixi run -e iceberg-e2e`
       with `JAVA_HOME="$CONDA_PREFIX/lib/jvm"`, goal `compile exec:java` and
       `-Dexec.args="s3://iceberg-aeaqcaibaeaqcaibaeaqcaibae/t/02020202020202020202020202020202/data/target.parquet"`.
       The normal Rust gate uses embedded fixtures and does not require Maven.
     - Added safe Snappy/Zstd dependencies; lockfile pins jobserver 0.1.32 instead
       of 0.1.35 to preserve the workspace's Rust 1.75 compatibility floor.
       No new local unsafe exception or production lock is introduced.
     Schema/delete checkpoint: 22 new tests pass, including complete official SDK
     page fixtures and corrupt-body rejection. Library `--all-targets`, workspace
     `rs-fmt-check` and `rs-lint` pass. Compatibility decisions follow Iceberg
     1.11.0 `ParquetSchemaUtil`, `TypeToMessageType`, `ApplyNameMapping` and
     `BaseParquetReaders`, plus Parquet encoding and Variant shredding contracts.
     Combined checkpoint: `pixi run -- cargo test -p crowdb-access-iceberg
     --all-targets`, `pixi run rs-fmt-check` and `pixi run rs-lint` pass. These are
     library gates; no production table API or new server acceptance is claimed.
     Remaining integration/coverage within this task:
     - Supply trusted historical contexts, default values and name mappings from
       selected table metadata. Unknown-to-concrete promotion remains explicitly
       unsupported; full default-value validation belongs to metadata validation.
       Expand official SDK fixtures for nested data, Variant and spatial fields;
       current schema cases use synthetic structures, not official body readers.
     - Equality values and optional position-delete row payloads are not scanned;
       general data-page validation is outside this reserved-column decoder.
     - Connect the resulting validation to full selected snapshot traversal;
       a standalone metadata-returning function must not become a commit proof.
  4. Deferred to R186: canonical ORC equivalent checks with bounded decoding.
     Consult the current
     [ORC protobuf](https://github.com/apache/orc-format/blob/main/src/main/proto/orc/proto/orc_proto.proto)
     alongside pinned Iceberg ORC mapping and official writer/reader code. The
     specification website's Footer field 11 differs from the current protobuf
     (`calendar`); do not copy that example as the wire authority. Bound encoded
     bytes, decoded bytes, protobuf work, type depth/count and stripe count
     independently. Compression framing uses independent three-byte chunks;
     codec/column encryption support must be explicit, not silently ignored.
  5. Bind complete snapshot enumeration, actual file row counts and DV validation.
     Current user-approved sequence skips deferred ORC: finish this integration,
     then items 6, 7 and 8. Reject unsupported selected formats explicitly.
     Library orchestration implemented as `manifest::validate_snapshot_files`:
     three complete bounded enumerations validate data first, DVs second and
     remaining deletes last. Canonical Parquet footer rows/schema populate an
     independently count/byte-bounded index; retained historical contexts are
     shared per manifest and charged to that index. No metrics maps are retained.
     Position deletes apply only to selected data with matching spec/partition
     and a data sequence no greater than the delete sequence, unless superseded
     by an applicable DV. Removed or otherwise inapplicable targets do not borrow
     another file's row count. Every DV still validates its canonical descriptor
     and bitmap; applicable DVs additionally check the canonical data row bound.
     Independent aggregate delete-row, DV-count/blob-byte and index budgets fail
     closed; errors/cancellation return no completion result or authority mutation.
     Six integration tests cover actual SDK position-delete pages, manifest
     ordering, stale targets, false footer counts, missing authorities, deferred
     ORC, index limits, aggregate equality-delete work, DV supersession and DV row overflow. Canonical data footer
     fixtures intentionally do not claim general data-page scanning.
     Remaining: generation-trusted source wiring from item 7, prior-snapshot
     delete/DV preservation in commit validation, and production table publication.
     The summary is not a full commit proof or equality-value scan.
  6. Bounded TableHead/name mappings and generation-qualified repository.
     Library core implemented in `src/table/`: separate bounded head and name
     mapping records, append-only FlatBuffers union tags, strict key binding,
     stable TableId versus optional v1 Iceberg UUID, namespace/name epoch,
     lifecycle, metadata generation/location/FileId/digest and operation fences.
     `TableRepository::select` resolves only published head-qualified names and
     binds one immutable JSON record without rereading a newer head midway.
     `ensure_current` compares the complete head and active catalog context; it
     is explicitly a read check, never a replacement for publication CAS.
     Four tests cover record bounds/key/version checks, stale reservations/names,
     tombstones, metadata corruption, catalog retirement and generation changes.
     Tests install fixture records only; no alternate production publisher or
     new unsafe exception/lock is introduced. Full namespace/REST composition,
     JSON validation and ALL/REFS responses remain in items 7 and 8.
     Checkpoint gates: both `crowdb-access-iceberg` and `crowdb-protocol`
     `--all-targets` tests pass; the final added equality-work case also passes
     its focused gate. Workspace `rs-fmt-check` and `rs-lint` pass. No server
     endpoint or complete table metadata acceptance is claimed by this checkpoint.
  7. Full v1/v2/v3 table metadata validation, preserving original JSON.
     Implemented library checkpoint: `TableMetadataDocument` preserves original
     bytes, verifies selected head/digest, and decodes duplicate-free JSON under
     independent byte/value/string/depth/collection limits. Canonical reads bind
     the immutable file identity and verify the entire file before parsing.
     Version envelopes, UUID/native location binding, independent schema
     structure/identifier checks and last-column bounds are validated. Snapshot
     graphs, retained parent sequencing, refs/main, row allocation ranges, logs,
     statistics and encryption-key structures are checked without treating these
     structures as proof of their files. Upgraded sequence-zero history and
     missing historical row lineage remain readable; log ordering follows the
     official SDK's 60-second clock-skew tolerance.
     Pinned Java 1.11.0 `TableMetadataParser` v1/v2/v3 empty-table fixtures are
     generated by `TestMetadataFixtures` in the existing Maven harness and
     checked for byte-preserving reads. Thirteen new tests cover this checkpoint;
     SDK fixtures do not yet cover evolved schemas or nonempty snapshots.
     Follow-up implemented: typed initial/write defaults (including recursive
     collection values, typed map-key uniqueness, decimal scale/precision,
     temporal precision/range and empty struct defaults); bounded embedded name
     mapping; partition field identities/source/transform checks and sort order
     direction/null-order checks. Default layouts bind current schema; historical
     layouts retain dropped sources instead of binding every old spec to current
     columns. Independent schemas remain readable without inventing a linear
     evolution history from their array order. Legacy v1 schema IDs are retained.
     Java fixtures now include v2/v3 dropped partition/sort source columns and v3
     decimal/nanosecond defaults, produced and round-tripped by the pinned SDK.
     Remaining: prior/candidate schema and partition/sort evolution validation,
     including immutable initial defaults, ID reuse and upgrades; trusted
     manifest contexts and complete file-validation wiring. These checks need
     selected prior-generation authority; do not mistake document parsing for
     commit admission. Expand nonempty official snapshot fixtures before closure.
     Implemented checkpoint: `commit::validate_metadata_transition` binds prior/candidate
     identities, the immediate successor generation, an explicit ordered upgrade
     trace, monotone allocation counters and retained/new snapshot distinctions.
     Never infer update order from schema-array ordering: the official builder
     supports selecting an older retained schema before further updates. The
     ordered evaluator must validate each actual schema/layout update at its
     application point; this transition helper is deliberately not a full commit
     proof. Preserve allocations from intermediate snapshots removed in the same
     transaction and require lineage on newly added v3 snapshots only.
     The closed `TableRequirement` union covers all eight pinned table requirement
     variants, including required-but-nullable ref snapshot IDs, implicit v1 main,
     legacy partition high-water inference and distinct invalid/budget/conflict
     results. Evaluation is pure over one selected document; wire request bounds,
     ordered updates and CAS/recovery are still separate unfinished phases.
     Eight transition/requirement tests plus metadata/load/list regression tests
     pass, with workspace fmt/clippy gates. Do not advertise commit support yet.
     Next wire checkpoint: `CommitRequest::decode` bounds complete JSON and both
     union counts before returning typed requirements and all 23 table update
     variants. Unknown/view actions, duplicate keys at any depth, malformed
     payload shapes and route/body identifier mismatch fail closed. Nested schema,
     layout, snapshot and auxiliary payloads retain original raw JSON separately
     from their decoded fields so future optional numbers are not rounded during
     candidate construction. Four request tests cover this layer; decoding is
     not update evaluation or semantic admission. Direct v1-to-v3 upgrade policy
     conflicts with the pinned SDK and is now a human decision in R177; other
     work continues without exposing that unsupported path.
     Scalar admission now rejects malformed UUIDs, unsupported target versions,
     invalid schema/spec/order selectors and invalid branch/tag retention values.
     The `-1` last-added selector remains legal; actual existence, source-version
     transitions and native location authority belong to ordered evaluation.
     Reference retention follows the pinned Java `SnapshotRef.Builder`: positive
     values only, with branch-only minimum-count and snapshot-age settings.
     Six request tests pass, including null retention, integer boundaries and
     duplicate removal IDs. Deprecated last-column/statistics snapshot fields
     must not become authority: the pinned `MetadataUpdateParser` derives these
     from nested payloads instead. Nested payload semantics remain unfinished.
     Retained-definition checkpoint: transitions now reject mutation of an
     existing schema/spec/order ID, including changed field names, transforms,
     sort direction and defaults. Definition comparison charges every nested JSON
     value before cloning, under the shared transition work limit. Legacy v1
     schema/spec envelopes and implicit partition IDs normalize to modern forms;
     empty identifier-ID sets and their ordering do not invent a change.
     Removed history and new definition IDs remain legal at this layer. New-ID
     schema evolution still needs ordered validation and is not inferred from the
     final current schema. Seven transition tests cover this checkpoint. The
     legacy counter fixture now retains spec 0 instead of changing its meaning
     during an upgrade. No commit endpoint or publication path is enabled.
     Generation-context checkpoint: `TableMetadataDocument::manifest_context`
     selects retained schema/spec IDs from that document, binds the actual pair,
     and optionally attaches bounded retained schema history for dropped-column
     metrics. Lookup and nested reconstruction share an explicit work budget.
     Current schema is never substituted for a requested historical schema;
     missing history fails closed rather than trusting uploaded Avro headers.
     Three tests cover historical partition sources, incompatible pairs, v1
     implicit IDs, missing history and count/work limits. This is the context
     factory only: canonical file resolution, reused-manifest provenance after
     schema expiration, complete snapshot validation and publication fencing
     remain to be composed by the evaluator/source layer.
     Nonempty pinned SDK fixtures now cover all three table versions. The Java
     generator `TestSnapshotMetadataFixtures` adds two snapshots, moves main,
     tags the first snapshot and round-trips the canonical JSON through Iceberg
     1.11.0. Rust verifies original bytes, parents, v1 sequence-zero inheritance,
     refs and v3 row allocations against the generated documents. This fixture
     validates metadata interoperability, not the referenced Avro files or REST
     E2E. Regenerate with the existing Maven harness using
     `-Dexec.mainClass=TestSnapshotMetadataFixtures` and the native test table URI.
     Verification: the Maven generator succeeds (existing SLF4J provider warnings
     are nonfatal); complete library `--all-targets`, workspace fmt and clippy
     pass after these transition/context/SDK-fixture checkpoints.
     Keep `TableMetadataDocument` explicitly documented as a partial validation
     result, not a publishable generation or a REST capability. No endpoint is
     advertised by this checkpoint. Files: `src/table/metadata.rs`, its children,
     and `tests/table_metadata_*_test.rs` in `crowdb-access-iceberg`.
     Checkpoint gates: library `--all-targets` tests pass; final focused metadata
     tests (13), workspace `rs-fmt-check` and `rs-lint` pass. No user-guide changes.
     Cross-check pinned spec and official SDK fixtures before accepting historical
     schema/spec combinations; do not treat manifest header claims as trusted
     table metadata or assume all historical schemas remain in current metadata.
  8. Generation-consistent load/list/exists, ALL/REFS, ETags and fallback.
     Implemented library read slice: `TableLoader` resolves live namespace and
     table identities, reads one selected canonical file, then rechecks head and
     namespace identity/name epoch. Concurrent changes fail with conflict rather
     than mixing metadata generations. ALL returns original bytes; REFS selects
     branch/tag target snapshots and preserves other raw JSON values. ETags bind
     catalog/TableId/generation/digest and loading mode, as required by the REST
     specification; a matching conditional request cannot bypass corruption or
     lifecycle checks. Canonical-only loading works without projections; no
     projection fast path is enabled before an equivalent validation proof exists.
     `TableLister` qualifies each mapping against its head, signs namespace/name
     epoch/context/page-size-bound tokens and counts stale entries toward work.
     Absent tokens collect the complete bounded result; empty tokens start paging.
     Work and retained-name byte exhaustion fail before any result is returned.
     Files: `src/table/load.rs`, `list.rs`, `list/token.rs`; table load/list tests.
     Access Server adapter checkpoint: `iceberg/table_read.rs` composes GET load,
     HEAD exists and complete/paged table listing after shared bearer/catalog
     admission. It preserves raw metadata inside the standard response envelope,
     uses mode-specific ETags/304, checks query/header bounds and single path
     decoding, and retains one of four lock-free spool permits through response
     delivery. Complete and paged lists share admission; output bytes are capped
     before any response is sent. Failed requests release admission.
     Five fixture-backed TCP tests cover ALL/REFS, conditional/HEAD, large unknown
     numeric values, escaped names, token binding, all credential roles, missing
     objects, corruption and resource admission. Setup is exclusively through
     `with_table_reads_for_tests` behind `test-util`; production runtime and
     advertised capabilities remain unchanged. No second publisher is introduced.
     Remaining: production activation with credential vending and final metadata
     validation; official-client ALL/REFS/conditional and complete/paged list E2E.
     Run server tests and clippy with `--features iceberg`: default server feature
     selection skips these tests entirely and is not evidence of validation.
     Keep table capabilities unadvertised until these integration gates pass.
     Adapter gates pass: server `--features iceberg --all-targets` tests and
     clippy, production-only `--no-default-features --features iceberg` library
     check, workspace fmt and `rs-lint`. The native/full-stack E2E feature is
     intentionally separate and is not claimed by this checkpoint.
     Official Java RESTCatalog read acceptance now passes against the same TCP
     fixture service: page-size-one listing, HEAD existence/missing table, ALL
     and REFS loads, repeated conditional loads, tag/main state, escaped names
     and REFS-to-ALL snapshot hydration. A test FileIO throws on every file
     operation, proving hydration uses REST rather than hidden file access.
     Fixture-installed handlers advertise exactly the three implemented read
     endpoints so the pinned SDK endpoint checks run normally. Production cannot
     install them yet; its config remains unchanged and disabled table routes
     return the standard unsupported response. This is official-client protocol
     acceptance over fixture authority, not native-backend or commit E2E.
     Files: `tests/iceberg_table_sdk_test.rs` and
     `tests/common/iceberg_java/src/main/java/TestIcebergCatalogReads.java`.
     Run explicitly (the Maven-dependent test is ignored by ordinary suites):
     `pixi run -e iceberg-e2e -- bash -c 'export JAVA_HOME="$CONDA_PREFIX/lib/jvm" CROWDB_ICEBERG_E2E_MVN="$CONDA_PREFIX/bin/mvn"; pixi run -e default -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_table_sdk_test -- --ignored --nocapture'`.
     The explicit inner `-e default` is required: the Java environment has no
     Cargo, and an unqualified nested `pixi run` inherits that environment.
     Gates for this read/default slice: library all-target tests and final focused
     metadata/load/list tests pass, as do workspace fmt/clippy; pinned Java fixture
     generation succeeds (nonfatal existing SLF4J binding warnings only).
  9. Production credentials with live table authorization and timed SDK refresh.
  10. Durable rename/drop and namespace races/recovery, retaining purge intent.
- [ ] **Selected table metadata**: implement bounded table heads/mappings,
  metadata version validation and generation-consistent load/projection fallback.
  Wire credential vending only after table authorization and lifecycle checks.
- [ ] **Table lifecycle**: implement durable rename/drop, destination admission,
  namespace races and restart recovery; retain purge intent for deferred GC.

### Requirement milestones

- [ ] **Finish namespace acceptance**: resolve the recorded 500-ms real-stack
  CRUD latency decision, then verify official-client CRUD/restarts and the future
  table create/rename-in admission contract. Close R179 only
  after its full gates. Files: namespace/wire modules, server Iceberg modules,
  library/server tests and namespace execution plan.
- [ ] **Implement immutable file authority**: canonical locations, bounded file
  records, inline/chunk selection, seal validation, immutable publication,
  streaming PUT/HEAD/range GET, and delegated table-prefix credentials. Files:
  library `src/file/`, record/schema extensions, server FileIO routes, tests.
- [ ] **Complete FileIO contract**: durable bounded multipart and recovery,
  projection fallback, streaming manifest validation, and verified format hints.
  Preserve abandoned-state discovery without physical cleanup. Run official
  FileIO tests before closing R180. Files: file and metadata projection modules,
  server integration and tests; a new per-requirement FileIO plan.
- [ ] **Implement selected table metadata**: bounded heads and mappings,
  v1/v2/v3 validation, canonical-byte preservation, selected-generation ALL/REFS
  loads, ETags, exists and listing. Use test fixture heads only; do not invent a
  second production create publisher. Files: library `src/table/`, tests.
- [ ] **Complete table lifecycle**: fenced same/cross-namespace rename, logical
  drop, durable pending purge intent, retry/recovery and REST integration. Verify
  rename-in versus namespace drop before closing R181. Files: table lifecycle,
  server handlers, tests; a new per-requirement lifecycle plan.
- [ ] **Implement table creation**: immediate/staged create, immutable initial
  metadata, namespace admission, one initial head publisher, and expiration
  recovery. Connect native FileIO; verify an official-client create/load/write
  vertical slice as capabilities become available. Files: library `src/commit/`,
  table/FileIO integration, server handlers and tests.
- [ ] **Complete atomic commits**: bounded requirement/update evaluation against
  one generation, complete declared v1/v2/v3 semantics, upgrades, head CAS,
  terminal replay and orphan evidence. Verify conflicts and crash points before
  closing R182. Files: commit modules, wire models and tests; a new commit plan.
- [ ] **Gate the functional checkpoint**: complete common REST composition,
  discovery, credentials, errors, metrics, cancellation and admission. Run the
  pinned compatibility kit, Java/Rust clients and supported engine profiles;
  publish executable version/capability results. Explicitly record pending GC
  coverage rather than closing R184. Files: library `src/rest/`, server runtime,
  conformance fixtures, test environment and a per-requirement REST plan.
- [ ] **Implement reclamation later**: durable candidates, bounded reachability,
  retention/pins, deletion proofs, isolated worker budgets and operator controls.
  Reconcile data retained during the functional checkpoint. Close R183 only after
  deletion safety and restart gates. Files: library `src/gc/`, server operator
  integration and tests; a new reclamation plan.
- [ ] **Close full conformance**: run remaining reclamation-dependent and complete
  cross-feature acceptance, then close R184 and the original correctness
  milestone. R185 cache optimization remains outside this plan. Files: client
  fixtures, affected permanent design, requirement/index and execution plans.

## Consolidated files and verification

- Production: `lib/crowdb-access-iceberg/src/`, scoped additions to
  `lib/crowdb-protocol/src/fbs/iceberg.fbs`, and
  `app/crowdb-access-server/src/iceberg/`.
- Tests: `lib/crowdb-access-iceberg/tests/`,
  `app/crowdb-access-server/tests/`, protocol tests when schema changes, and
  pinned conformance environments. All Rust tests stay outside production files.
- Unit: encoding/size boundaries, identifier and metadata validation, every
  supported requirement/update variant, format and upgrade fixtures.
- Integration: real Chunk-KV/chunk storage, competing publishers, every durable
  crash boundary, uncertain CAS replies, bounded streams/scans and restart replay.
- E2E: namespace CRUD first; then FileIO/multipart, table lifecycle and commits;
  finally compatibility kit/client/engine and full format matrices. Carry these
  incrementally rather than waiting until R184 to expose integration failures.
- Per requirement: affected library/protocol/server tests, existing
  `pixi run -e iceberg-e2e test-pyiceberg-e2e`, separately
  `pixi run -- cargo fmt --all -- --check` and `pixi run rs-lint`, plus relevant
  feature-enabled gates. Prefix server-spawning tests with `pixi run clean-env &&`.
  Passing config-only client tests does not count as full catalog acceptance.
- Keep coherent verified commits and truthful checkpoints. A seven-hour absence
  is not a delivery estimate for six requirements; start with remaining R179
  execution/recovery and continue in the approved order, bypassing only tasks that
  depend on unresolved human decisions recorded in R177.
