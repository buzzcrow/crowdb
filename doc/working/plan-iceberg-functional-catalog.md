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
  4. Canonical ORC equivalent checks with bounded decoding.
  5. Bind complete snapshot enumeration, actual file row counts and DV validation.
  6. Bounded TableHead/name mappings and generation-qualified repository.
  7. Full v1/v2/v3 table metadata validation, preserving original JSON.
  8. Generation-consistent load/list/exists, ALL/REFS, ETags and fallback.
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
