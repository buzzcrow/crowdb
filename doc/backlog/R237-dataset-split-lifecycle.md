<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R237: dataset — Named Split Lifecycle, HTTP API and SDK Support

**Status**: In progress; v2 discovery/head inspection and foundational Split
records are implemented; mutation lifecycle and acceptance remain pending. Execute
[the backend/SDK plan](../working/plan-dataset-split-lifecycle.md) before R236.
Full Split support, including API, SDK and UI, is required by the user.
R237 owns all shared Dataset backend/API/SDK prerequisites; R236 owns Console UI.
The user confirmed independent Split revisions and creating a child Snapshot
from a Split. Data Snapshot identity and Split revision identity are separate.

**Problem**:

- Dataset architecture describes a Split as a logical subset of a Snapshot, but
  `lib/crowdb-access-dataset/src/manifest.rs`, Access HTTP models and public SDK
  have no persisted Split identity, definition, membership or lifecycle. Users
  cannot publish reproducible named train/validation/test subsets, discover them
  in another client, or resume a read bound to one named subset.
- Read-time Selection already supports prefix/range/IDs/keyword/field match and
  boolean composition. Its current matcher inspects explicit current-Manifest
  inline fields, not effective inherited values. Reusing it unchanged for Split
  construction or inheritance filters would omit matching inherited fields.
- A temporary read filter, a named Split and worker/rank partitioning serve
  different purposes. Split is logical data membership, never a storage chunk,
  Manifest partition, mutable latest reference or training worker allocation.
- Root designs: [Dataset architecture](../design/access-server/dataset/design-crowdb-access-dataset.md),
  [Dataset specification](../design/access-server/dataset/spec-crowdb-dataset.md),
  [HTTP v1](../design/access-server/dataset/openapi-crowdb-dataset-v1.yaml),
  [SDK](../../sdk/README.md), and
  [Dataset Console requirement](R236-console-dataset-workspace.md).
- Scenarios: publish train/validation together with uploaded samples; read train
  identically in Rust/Python; create a child from a historical Snapshot while
  retaining Split membership; filter parent data by Split; recover an interrupted
  preparation without changing assignment or leaking metadata/payload ownership.

**Solution**:

Planning defaults adopted to make the handoff executable (not additional user
decisions): disjoint Splits, unassigned Samples allowed, inherited membership
frozen until explicit reevaluation, deterministic approximate weighted partition,
and parentless roots allowed on existing Datasets. These are explicit settings
where applicable, never hidden behavior. One parent per Snapshot still excludes
merge DAGs. Local text/hex preview and public versioned inspection APIs serve R236.

1. **Define authoritative Snapshot-scoped Splits.** Extend
   `lib/crowdb-access-dataset` publication, key/store and record contracts.
   - Identity is Dataset identity + immutable Snapshot ID + immutable Split-set
     revision + validated Split name. Each Snapshot has a current Split revision
     reference; a revision versions the whole named Split set atomically.
     Names are nonempty UTF-8, at most 255 encoded bytes, unique within a Snapshot,
     compared exactly without Unicode normalization. Treat them as opaque names,
     percent-encoded as one API path segment, never filesystem paths.
   - Create/edit/rename/remove operates on a Split draft. Publish Split revision
     freezes definitions/membership/policies as a new revision and advances only
     that Snapshot's current Split revision. It does not create a data Snapshot,
     change its Manifest, or advance Dataset latest/stable. Existing revision
     identities and readers remain unchanged. Data changes create a child Snapshot.
   - Each member is an existing Sample ID in that Snapshot; one ID appears at
     most once per Split. Empty Splits are valid. Splits share Snapshot Schema
     and field values, with no independent payload copy or Schema override.
   - A Dataset/Snapshot without Splits remains valid. All samples is a UI/query
     scope, not a fabricated named Split or a reserved name in stored metadata.
     Exact counts are available after complete resolution; partial evaluation
     counts are labelled scanned/matched and never passed off as totals.

2. **Support definition, preview and deterministic membership resolution.**
   Extend authority selection/planning without reading chunk payloads.
   - Definitions support explicit IDs and existing Selection forms: prefix,
     half-open range, inline keyword/field equality, And/Or/Not. Not complements
     against the chosen Snapshot's complete sample universe. IDs remain bytes;
     HTTP/SDK representations use the existing canonical base64 convention.
   - Reject unknown explicit IDs, duplicate IDs/names, unknown predicate fields,
     malformed rules and scope mismatches. Keyword searches effective inline
     fields in the selected Schema; field equality uses exact inline bytes.
     Resolve ancestry and tombstones before evaluation. Missing/tombstoned and
     chunk-backed values do not match these inline predicates; no full-text or
     semantic decoding is implied. Missing fields are not silently empty bytes.
   - Freeze the resulting membership during Split preparation, along with definition and
     evaluator version. Preview uses the same semantics against an immutable
     parent or a revision-bound draft. Editing a draft invalidates its preview;
     Prepare cannot silently accept membership computed for an older revision.
   - Train/validation/test convenience: a multi-Split partition operation
     accepts named integer weights and an explicit seed, assigning each Sample
     by a versioned stable hash of seed + exact Sample ID into disjoint intervals.
     Use algorithm `sample-id-sha256-v1`: SHA-256 over ASCII
     `crowdb-split-v1` followed by a zero byte, unsigned 64-bit big-endian seed,
     unsigned 32-bit big-endian ID byte length, and exact ID bytes. Interpret the
     first eight digest bytes as unsigned big-endian x and choose bucket
     floor(x * sum(weights) / 2^64) with a 128-bit intermediate. Apply cumulative
     intervals in the explicitly persisted Split order. Positive integer weights
     sum to at most 2^32-1. Include cross-language fixed vectors. No Snapshot ID,
     revision ID, runtime-random value or language-default hash participates.
     Assignment is deterministic and stable for unchanged IDs; weights describe
     expected proportions, not exact small-dataset counts. Display actual counts.
   - Declare overlap policy for the revision's named Split set: default
     disjoint, with explicit allow-overlap for intentional reuse. Coverage is a
     separate require-complete/allow-unassigned choice, default allow-unassigned.
     Partition convenience creates complete disjoint coverage. Other definitions
     must pass the selected policy before publication, reporting bounded example
     violations and authoritative counts only after a complete check.
   - No recursive Split references or automatic train/test names. A parent Split
     may be an explicit input to child composition, resolved before child Split
     publication; it does not create a live dependency on a mutable name.

3. **Integrate child creation and inheritance filtering.** Extend Dataset
   composition/publication APIs used by both SDKs and the R236 wizard.
   - Default child creation preserves all parent Sample data and captures the
     parent's current Split revision once for inheritance. It must compose the
     full Manifest required by current authority;
     an empty submitted Sample list is not shorthand for inheritance.
   - Optional inheritance input is All, a parent Split, a Selection, or a parent
     Split intersected with a Selection. Source identity always includes parent
     Snapshot, Split revision and name; a current-revision shortcut is resolved
     once when composition starts. Resolve against the exact chosen parent,
     then apply explicit additions/removals/field changes. No partial result can
     become a complete child membership after timeout or limit exhaustion.
     For source S1/r2/train, child S2 has parent S1 and complete Sample membership
     drawn from that frozen Split, followed by explicit edits. The left Snapshot
     tree remains S1 -> S2; Split is provenance, not a second parent or tree level.
     Later r3 changes never alter S2. Cross-Dataset or multi-parent merges are not
     implied. An empty source cannot publish an empty Snapshot under the existing
     contract; require additions or show the validation error.
   - Default Split inheritance is parent membership intersected with
     surviving child Sample IDs. Field edits do not silently reassign samples,
     and new Sample IDs remain unassigned unless the user explicitly assigns them
     or reevaluates a Split definition. If complete coverage was requested, such
     unassigned additions block Prepare until resolved or policy explicitly changes.
   - Retain inherited definitions as provenance, while identifying membership as
     inherited/frozen, not claiming a rule was reevaluated. Offer explicit Reevaluate
     against the child effective data, edit/rename/remove inherited definitions,
     and create new Splits. Review resulting membership/overlap/coverage before
     Prepare. Inherited Splits form the child's own initial revision, with source
     Snapshot/revision/name recorded as provenance. They do not share a mutable
     current-revision reference with the parent. Parent readers remain unchanged.
   - Missing parent Split, unavailable parent Snapshot and changed draft revision
     are explicit errors. Reclaim fences and retention protect source reads and
     preparation; ancestry validation stays in Dataset authority.

4. **Publish and recover revisions atomically.** Extend authority publication,
   storage, retention and reclaim; reuse Dataset upload ownership.
   - Split preparation validates complete membership and policies, persists bounded
     pages and their integrity binding, and records a recoverable operation ID.
     Split publication CAS-advances the current revision against an explicit
     expected revision (including no revision). Concurrent editors conflict,
     retaining their draft/candidate; never silently overwrite another edit.
   - One revision publishes the whole set, including unchanged Split definitions,
     so readers never see train from r1 with validation from r2. Incomplete pages
     remain invisible. Removing the last Split publishes an empty set revision;
     historical definitions remain addressable.
   - An initial revision can be prepared alongside a new data Snapshot. If supplied,
     data Snapshot Publish exposes its complete Samples and initial Split revision
     together and advances Dataset latest; it never reports full success with
     missing requested Splits. A Snapshot without an initial revision is valid and
     can acquire one later. Split-only publication never advances Dataset latest.
     R237 implements arbitrary historical parents and head concurrency separation
     as shared backend prerequisites; R236 exposes them in its wizard.
   - Operation identity binds owning Snapshot, expected revision, Split names,
     definitions, frozen membership and policies. Equivalent retries return the
     same revision/result; divergent content conflicts. Historical retries never
     rewind current Split revision or Dataset latest and never rerun assignment.
   - Retain all published Split revisions for the owning Snapshot's lifetime in
     the first release; no independent revision-pruning API or automatic age-based
     deletion. This supports reproducible reads and source provenance. Cancelled
     drafts/prepared revisions can be explicitly abandoned and reclaimed after
     ownership checks, with recoverable pending cleanup.
   - Reads and child composition protect the exact revision and owning Snapshot
     with scoped leases. Fence new revision publication during Snapshot reclaim.
     Reclaim eligible Snapshot-owned revisions and pages together without deleting
     other Snapshots' shared payloads. Removing a Split in a newer revision deletes
     no Sample bytes and invalidates no historical reader.

5. **Expose the complete versioned HTTP contract.** Extend
   `app/crowdb-access-server/src/dataset` and its OpenAPI with typed public DTOs.
   - Data Snapshot draft input includes optional initial Split definitions, explicit inheritance choices
     and overlap/coverage policies. Provide revision-bound preview/composition
     operations with bounded membership/validation pages and recoverable status
     for work continuing across requests. Prepare/Publish and operation inspection
     include authoritative Split preparation status, never only a client estimate.
   - Add prepare/publish/status/abandon operations for independent Split revisions,
     expected-current-revision checks, current revision resolution and paged revision
     history. A no-change data Snapshot is not required for editing its Splits.
   - Snapshot/revision-scoped ListSplits, GetSplit and ListSplitMembers expose identity,
     definition/provenance, policy, complete counts and opaque continuation.
     Default page 20, maximum 100, at most 1 MiB response. Splits and member lists
     have independent cursors bound to Dataset/Snapshot/Split/query/version.
   - Read plans can select a named Split, optionally intersected with a Selection,
     plus existing projection/order/shuffle/batch/worker settings. Resolve latest
     or stable to an immutable Snapshot once, resolve current Split revision once,
     and bind the full Split identity in plan and cursor identity. Same Split name
     in another revision or Snapshot
     cannot resume an existing cursor.
   - Apply Split and Selection before ordering/shuffle and worker partitioning,
     then fetch only admitted payloads. Existing fetch/acknowledge/progress/resume/
     close, integrity and lease contracts remain intact. Empty Split reads end
     successfully; unknown Split is not-found, not an empty successful read.
   - Reuse authentication and logical error classes. Semantic conflicts are not
     blanket-retried; invalid definitions are client-invalid, changed revisions
     conflict, and incomplete/over-budget work returns explicit progress or a
     limit error. Console readonly mode blocks mutation routes server-side.
   - Bound predicate depth/nodes using existing 16/1024 limits. Keep JSON within
     8 MiB and read plans within 100000 admitted IDs unless a separately reviewed
     API version changes these contracts. Bound scan/decode/ancestry work per
     request and page retained membership; do not fetch an entire Manifest then
     slice a response. A resumable evaluator must release resources on cancel.
     Complete Split membership may exceed read-plan admission: enumeration remains
     paged and an oversized plan fails explicitly rather than truncating members.

6. **Deliver Rust and Python SDK parity.** Extend
   `sdk/crates/crowdb-sdk-dataset`, `sdk/crates/crowdb-sdk-python`, Python package
   declarations/documentation, and shared contract fixtures.
   - Typed models/helpers cover Split definition/policy, child inheritance,
     deterministic partition requests, preview/status, publication, paged discovery
     and member enumeration, independent revision edits/history and CAS conflicts.
     Iterators fetch lazily and expose failures without
     silently returning partial results as complete collections.
   - Start/read/acknowledge/resume/cancel works with a named Split plus optional
     filter and existing projection/shuffle/worker controls. Python delegates
     to Rust transport/evaluation contracts; it does not implement a second matcher
     or hash algorithm. Preserve bounded buffering, GIL release and fork rules.
   - Examples cover upload -> define train/validation -> preview -> Prepare ->
     Publish -> iterate train -> acknowledge/resume -> create a child from train
     -> adjust inherited Splits -> publish. Include failure/retry/cancel examples
     and exact Snapshot/revision/operation identity in recoverable errors.
   - Non-local sources are SDK-side ingestion: callers use source clients to read
     S3/Iceberg/other inputs and feed bounded streams/files plus explicit Sample/
     Field mappings into Dataset uploads. Show a source-adapter example and test
     stream cancellation/errors; Dataset owns uploaded bytes, not external source
     credentials or deletion authority. No Console source browser is required.
     Native managed connectors/catalog discovery are not implied by stream ingest.

7. **Evolve storage and clients without silent semantic changes.** Update
   Dataset specification, design and OpenAPI, SDK release contract and fixtures.
   - Old persisted Snapshots load as having no Splits. Do not append fields to
     unversioned bincode layouts and assume compatibility; use explicit record
     versioning/decoding and rollout rules. Unknown required versions fail clearly.
   - Legacy publication inputs remain supported with explicit documented omitted-
     Split behavior. Legacy behavior is no declared Splits, while new
     child-composition helpers explicitly request inheritance. This prevents an
     old client from unknowingly opting into new Split policy validation.
   - Split-capable requests must fail clearly on a server lacking the capability;
     an older server ignoring unknown JSON fields must not silently publish a
     Snapshot without requested Splits. Negotiate capability/version before write.
   - Version effective-value predicate semantics rather than silently changing
     old persisted Read Plan identities or resuming an old plan with a new matcher.
     Legacy selection behavior and its limits stay explicit during migration.

8. **Supply shared backend prerequisites for R236.** Own bounded Dataset discovery,
   Snapshot root/child/inventory enumeration, metadata-only Schema/Sample/Field
   inspection, historical-parent publication and child-composition recovery here.
   The exact user-visible contracts remain in R236; its frontend does not implement
   a separate authority or download full Manifests to emulate them.
   - Allow any live published same-Dataset parent or an explicit parentless root.
     Separate parent from the expected head generation captured at preparation.
     A conflicting Publish preserves the candidate. Explicit retry after review
     uses a new commit-attempt ID and expected generation for the same frozen
     candidate; it does not reparent or mutate its operation content. Identical
     attempts reconcile; completed publications can never advance latest again.
   - Record durable successful-publication receipts independent of latest ancestry.
     Once branches exist, ancestry reachability is not proof of past success.
     Recovery must distinguish an uncommitted Published record from a completed
     historical publication, including a crash after head CAS before receipt save.
   - Initial revision binding is durable before a new Snapshot is externally
     readable. Snapshot visibility is the single gate for its initial revision;
     Split current cannot be independently edited until the owning Snapshot is
     committed. A data Publish response confirms both complete data and initial
     Split visibility, not two unrelated best-effort requests.
   - New writes maintain bounded metadata indexes/projections. Old records remain
     readable through legacy APIs; bounded inspection uses explicit resumable
     metadata preparation with progress/limits when no bounded projection exists.
     Do not silently scan/rewrite all old records at startup or on a list request.
     Preparation is idempotent, metadata-only and non-destructive. Unavailable or
     oversized legacy metadata is a capability/limit state, never an empty list.
     The plan must implement discovery of old Dataset identities, not rely solely
     on an index populated by new writers.

**Dependencies**:

- Existing Dataset durable publication, HTTP uploads/reads and SDK are the base.
  No direct-client/RDMA/local-cache requirement is a prerequisite.
- R236 consumes this contract for the required tree, matrix and wizard. R237 owns
  shared backend/API/SDK; R236 owns Console integration only. Complete R237 API/SDK
  acceptance first, then R236 browser acceptance. Browser cross-surface scenarios
  below are owned by R236 and do not block R237's independent backend completion.
  On R237 cleanup, move its final contract to permanent Dataset documents and
  update R236/its plan links; do not leave references to deleted requirements.
- Shared native integration fixtures and `sdk/contract-fixtures/dataset-v1` verify
  equivalent behavior. New contract fixtures must identify their required API
  capability/version; do not overwrite legacy interpretation.

**Acceptance**:

- Two edits from r1 -> publish r2 then attempt the competing publish -> conflict
  without creating a data Snapshot or moving Dataset latest/stable; all Split names
  switch together, an r1 reader continues, and r2 is restart-durable. Invariant:
  independent atomic revision publication. Integration test.
- Parent S1/r2/train -> compose child while parent advances to r3 -> S2 retains
  parent S1, frozen source membership/provenance and its independent initial Split
  revision; empty-source publication requires valid additions. Invariant: pinned
  Split-source inheritance. Integration test.
- Source-adapter stream with interruptions -> upload through Rust/Python ingestion
  and publish -> mapped Sample/Field bytes match the source, cancellation releases
  resources and no external source is mutated. Invariant: SDK-source ingestion.
  Integration test.
- Snapshot with named Splits -> publish and inspect after restart -> exact scoped
  identities, definitions and membership persist; duplicate/invalid names fail,
  and no-Split/empty-Split states differ from not-found. Invariant: durable Split
  identity. Integration test.
- Explicit IDs, prefix/range, compound predicates, inherited inline values,
  tombstones and chunk-backed fields -> preview then Prepare -> frozen membership
  matches the effective-value contract with no chunk payload reads; malformed,
  duplicate and unknown references fail. Invariant: resolved bounded semantics.
  Integration test.
- Identical samples/seed/weights in different insertion orders and both SDKs ->
  partition -> stable, disjoint complete assignment and actual counts, including
  empty small-set partitions; changed draft invalidates preview. Invariant:
  deterministic partitioning. Integration test.
- Overlapping or uncovered memberships -> prepare under each policy -> allowed
  combinations succeed and forbidden combinations fail with bounded diagnostics.
  Invariant: independent overlap/coverage enforcement. Integration test.
- Parent Splits plus additions/deletions/field edits -> default child composition,
  filtered inheritance and explicit reevaluation -> membership follows each chosen
  policy, new IDs are not silently assigned, parent remains unchanged, and partial
  evaluation blocks publication. Invariant: explicit inheritance. Integration test.
- Prepared Split pages and concurrent publication/failure/restart -> retry identical
  operation, then divergent operation -> complete atomic visibility and stable
  candidate identity, divergent retry conflicts, historical retry never rewinds
  latest. Invariant: atomic idempotent publication. Integration test.
- Split read interrupted before/after acknowledgement -> resume with same versus
  changed Snapshot/revision/Split/filter -> correct replay/progress for the same plan and
  rejection for changed identity; latest movement cannot alter membership.
  Invariant: plan binding. Integration test.
- Named Split, projection/shuffle and multiple workers -> read through Rust and
  Python -> same logical data and worker coverage, empty Split ends cleanly,
  unknown Split fails, and payloads outside selection are not fetched. Invariant:
  surface parity and selection-before-delivery. Integration test.
- Many Splits/members, deep ancestry, oversized rules and cancelled preview ->
  list/evaluate/read -> bounded memory/work/response, scoped cursors and explicit
  limits; no unbounded background collection or truncated-success membership.
  Invariant: bounded operation. Integration test.
- Retained revisions/shared payloads/active reads and abandoned candidates -> remove
  Split in a new revision or reclaim eligible Snapshot -> historical definitions
  and protected data stay readable until eligible owning-Snapshot reclaim,
  Split metadata cleanup is recoverable and no foreign source bytes are deleted.
  Invariant: ownership-safe lifecycle. Integration test.
- Legacy persisted records, old clients and servers without Split capability ->
  read/publish/resume -> documented no-Split compatibility or explicit capability/
  version error, never silent loss of requested Split definitions or changed plan
  semantics. Invariant: versioned compatibility. Integration test.
- HTTP credentials, readonly Console and malformed cross-Snapshot cursors ->
  inspect/mutate -> authorization and scope checks hold, with equivalent SDK error
  classes. Invariant: boundary enforcement. Integration test.
- Browser using real Dataset service -> upload, define/preview Splits, publish,
  browse Split members, create filtered child and adjust Splits -> required R236
  UI flows reflect authoritative API state and SDK reads agree. Invariant:
  complete cross-surface lifecycle. E2E test.

Implementation verification commands (not required for this design-only edit):

```sh
pixi run test-access
pixi run test-dataset-sdk
pixi run test-dataset-sdk-python
pixi run check-dataset-sdk-boundary
pixi run test-console
pixi run test-console-ui
pixi run test-e2e-console
pixi run rs-fmt-check
pixi run rs-lint
pixi run sdk-fmt-check
pixi run sdk-lint
pixi run ts-lint
```
