<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Dataset Split Lifecycle Plan

Upstream: [R237](../backlog/R237-dataset-split-lifecycle.md),
[R236](../backlog/R236-console-dataset-workspace.md),
[Dataset contract](../design/access-server/dataset/spec-crowdb-dataset.md).
Goal: deliver the complete shared Dataset backend, independent Split revisions,
and Rust/Python SDK contract before Console integration.

## Handoff and ownership

- Implementation authorized and started on 2026-10-15. Paths labelled new remain
  intended additions until their tasks are verified. Evidence below records actual
  checks; unchecked tasks do not imply available APIs.
- Execute this plan before [Console plan](plan-console-dataset-workspace.md).
  R237 owns metadata store/indexes, publication/branching, composition, Splits,
  HTTP, SDK and native service acceptance. R236 owns listener exposure in Console,
  proxy/mode enforcement, UI and browser acceptance. No task owns the other's
  work as a fallback, and no frontend implementation is needed to finish R237.
- Preserve unrelated work, especially concurrent Cluster lifecycle changes.
  Run `git status --short` and inspect touched files before each phase. Do not
  restore/stash/commit unrelated files. Implementation follows
  implement-requirement's coherent gated commits.
- Use one active `[~]` task while implementing; mark `[x]` only after its focused
  checks pass. Record command, case, result and artifact path in Evidence. Never
  mark a phase passed because its data was rendered with mocks.

## Fixed scope and defaults

- Snapshot fixes Schema/Samples/field values. Split revision fixes the entire set
  of named Split definitions/members/policies for one Snapshot. Publishing a data
  Snapshot advances Dataset latest; publishing a Split revision does not.
- Source `S1/r2/train` creates a child with parent S1 and frozen source membership.
  Initial child Splits are copied/intersected against survivors, not dynamically
  linked. All-data inheritance is default. Arbitrary published parents and new
  parentless roots are supported. Empty data Snapshots remain invalid.
- Adopt backlog defaults: disjoint, allow-unassigned, frozen inheritance, explicit
  reevaluation, versioned SHA-256 approximate partitioning. Retain all committed
  Split revisions until owning-Snapshot reclaim. No revision-pruning feature.
- Not in scope: query engines, media decoding, tensor collation, direct storage/
  RDMA, remote managed connectors, multi-parent merges, external source mutation,
  benchmarks, or a generic long-running workflow scheduler.
- SDK stream/file ingestion accepts data supplied by external source clients.
  Do not claim built-in S3/Iceberg catalog connectors based on this capability.

## Baseline facts and risks to resolve in code

- `DatasetStore` currently offers get/CAS/delete only. `ChunkKvDatasetStore` wraps
  point operations. Ordered APIs live under `crowdb-chunk-kv-client/src/compose`;
  adapt them with explicit item/byte/work limits, not a browser-local registry.
- `SnapshotPublication.manifest` embeds full bytes; Manifest partitions repeat
  Schema. `get_manifest`, projection and read-session planning can assemble full
  data. Paging only the response does not bound storage decode or allocation.
- `write_publication` and HTTP publication require parent == latest.
  `snapshot_reachable_from_head` is not a valid completed-operation receipt after
  branching. `advance_head` also conflates expected parent with expected head.
- Snapshot state Published can precede successful head advancement today. New
  catalogs must not expose such an uncommitted candidate as a committed branch.
- SDK is an independent Cargo workspace. `test-access-sdk` runs Iceberg, not
  Dataset. Use `test-dataset-sdk` and `test-dataset-sdk-python` below.

## Phase A — wire, state and compatibility boundary

- [x] **A1 Capture baseline and contracts**: inspect authority/publication/head,
  projection, upload ownership, leases, read sessions and HTTP/SDK callers. Run
  focused existing tests before mutation. Record public v1 behavior and exact
  persisted envelopes. Files: `lib/crowdb-access-dataset/src/{authority,record,
  publication,manifest,read_session}.rs`, `authority/`, `upload/`,
  `app/crowdb-access-server/src/dataset/`, `sdk/crates/crowdb-sdk-dataset/src/`.
  Exit: no assumptions about multi-key transactions or current CAS semantics.
- [x] **A2a Introduce v2 capability boundary**: add authenticated capability
  discovery, SDK version/capability checks and OpenAPI discovery schema. Preserve
  v1 control and binary framing. Advertise only implemented families; reject
  unsupported v2 routes without falling through to v1. Files: `dataset/api.rs`,
  `dataset/api/v2.rs`, `tests/dataset_v2_contract_test.rs`, SDK `capabilities.rs`,
  `tests/capabilities_test.rs`, OpenAPI v2. Exit: HTTP auth/admission and SDK
  old-server/version/feature-unavailable tests pass.
- [x] **A2b Define v2 operation contracts**: introduce a
  versioned `/api/dataset/v2` control surface for new identity/commit/composition
  semantics; preserve v1 routes and binary framing. New v2 models must reject
  unsupported required fields/versions and advertise required capabilities before
  SDK writes. Files: `dataset/api.rs`, new `dataset/api/v2.rs` and domain children,
  new `doc/design/access-server/dataset/openapi-crowdb-dataset-v2.yaml`, SDK models.
  Exit: valid/invalid request fixtures cover null vs omitted expected revisions,
  exact IDs/base64 and feature-unavailable behavior; older servers cannot silently
  ignore Split inputs. V2 is a planned addition, not an existing route.
  Sequencing refinement: finalize mutation DTOs with their authority operations,
  after A3 supplies validated identities/state and before E2/SDK writes. Discovery
  stays available now, with no unimplemented capability advertised.
- [x] **A3a Define immutable Split records**: implement validated opaque names,
  revision identity, whole-set records, head, membership pages, SHA-256 bindings
  and scoped keys. Files: `identity.rs`, `split.rs`, `split/{definition,record,
  codec,key}.rs`, `tests/split_record_test.rs`. Exit: exact UTF-8 byte bounds,
  no-revision versus empty-set revision, malformed/unsupported/trailing records,
  page ordering/count/scope/content integrity and recursion bounds pass.
- [x] **A3b Define operation state records**: add versioned current
  reference, draft revision, candidate, membership-page binding, operation status,
  commit attempt and successful receipt. Suggested symbols: `SplitRevisionId`,
  `SplitRevisionRecord`, `SplitHead`, `CompositionRecord`, `CommitAttempt`.
  Split head distinguishes no revision from a committed empty-set revision.
  Files: new `src/split.rs`, `split/{record,key}.rs`, `record.rs`, `key.rs`,
  new `composition.rs`, new crate `tests/split_record_test.rs`.
  Exit: legacy decoding is explicit; malformed/unsupported versions fail closed;
  identity is Dataset/Snapshot/revision/name, never numeric revision ordering.

## Phase B — bounded authority metadata and composition inputs

- [x] **B1 Add bounded metadata store operations**: adapt ordered range scans and
  key/value limits at `DatasetStore`; implement in production and test stores.
  Define `scan_page(prefix, after, item_budget, byte_budget)` or equivalent with
  explicit partial/end and cancellation/deadline. Check underlying transport
  value allocation bounds, not only decoded DTO length. Files: `store.rs`,
  `chunk_store.rs`, `tests/common/dataset_store.rs`, Chunk-KV adapter tests.
  Exit: old Dataset identity keys are discoverable across store restart without
  a new-only registry. Zero/invalid budgets, long keys and partial pages tested.
  Implementation note: the existing Chunk-KV byte limit was checked after
  decoding an RPC result, and the native scan admitted an oversized first entry.
  Added a dedicated bounded-scan RPC (3708) and strict tree/FFI path so old
  servers cannot silently ignore the limit. Strict scans check overflow lengths
  and external L0 cell sizes before payload materialization; empty truncated
  pages are explicit failures. Dataset adapters enforce item/byte/time budgets,
  cancellation, binary prefix cursors and exact-key reads without point fallback.
  Verified: protocol (7), server (14), client ordered (9), strict native scan
  (3), Dataset adapter (4), Dataset suite and inspection index (4) tests pass.
  Full Rust lint, focused final Clippy, tree lint and C++ test task exit 0.
- [x] **B2a Build new-write metadata projections**: persist small Snapshot summary,
  paged Schema, Sample IDs and per-field metadata/value bindings before publication.
  Keep inline bytes separate from inspection projections. Use immutable ordered
  keys/pages for Snapshot roots/children/inventory. Ready means the projection
  is complete; committed visibility is verified separately in B2b after C4.
  Files: new `src/inspection.rs`, `inspection/{index,pages,projection}.rs`,
  `authority/publication.rs`, `manifest.rs`. Exit: one metadata page does not decode
  the full publication Manifest; payload reader counters remain zero.
- [x] **B2b Bind index visibility to commit evidence**: after C4, all public
  enumeration verifies the small committed marker/receipt, never projection
  readiness or Published state alone. Add prepared/head-conflict/crash cases to
  the B4 API tests before advertising inspection capability.
- [x] **B3a Prepare legacy inspection metadata**: implement bounded resumable
  preparation per selected old Snapshot; scan old identities for discovery without
  preparing every Snapshot. Track revision/progress/error and idempotently publish
  new immutable projection bindings. Reject a legacy value beyond the configured
  storage read/decode cap before allocation if the store supports it; otherwise
  extend its bounded read contract, not a post-allocation check. Files: new
  `inspection/{legacy,preparation,rows,status}.rs`, `tests/inspection_legacy_test.rs`.
  HTTP routes are wired and verified in E2 with the complete v2 contract.
  Exit: interrupted/repeated preparation preserves old bytes, exposes explicit
  preparing/unsupported/limit state and never returns misleading empty data.
- [x] **B4 Add paged metadata authority**: Dataset list, root/child/inventory lists,
  Schema windows, Sample ID windows, exact-ID lookup and projected field matrix.
  New list defaults 20/max 100/1 MiB; UI fields request max 12 at a time, API max
  existing 256 Schema fields. Scan/decode/ancestry budgets must be independent of
  response limits. Bind cursors to scope/generation; immutable Snapshot cursors
  stay usable while unrelated latest changes. Files: `inspection/`, `authority/
  projection.rs`, new `tests/inspection_test.rs`. Exit: stale cursor conflicts,
  complete/non-complete results distinguished, no chunk reads or read-plan acks.
- [x] **B5 Implement effective-value selection**: extract a bounded evaluator
  over metadata projection, explicit inheritance and tombstones; use v2 semantics
  without changing old v1 persisted plans. Prefix/range/IDs use exact bytes;
  predicates resolve effective inline values; Not is relative to chosen universe.
  Files: `planner.rs`, new `selection.rs`, `authority/projection.rs`,
  `tests/selection_v2_test.rs`. Exit: inherited match, tombstone, missing, empty,
  chunk-backed and unknown-field cases have distinct asserted outcomes.

Sequencing refinement: execute C3/C4 before B4/B2b. Enumeration must rely on
small durable commit evidence, so publication helping and arbitrary-parent
preparation are prerequisites for exposing the index. B5 and C1/C2 then consume
the completed paged authority. No inspection/mutation capability is advertised
until its corresponding API and visibility tests pass.

## Phase C — durable drafts and data publication prerequisites

- [x] **C1a Persist and evaluate server composition**: immutable begin binding pins
  the source once; revision-CAS edits use immutable edit-ID pages and helpable
  receipts. Source, patch and validation windows persist redo records before
  applying Sample rows, then CAS progress. Bound pages to 20 Samples, 1 MiB
  metadata records and 4096 edit pages; copy effective locators, never chunk bytes.
  Tests cover lost edit/evaluation replies, historical retry, binary pagination,
  required fields, filtered reintroduction, cancellation and preview invalidation.
  Data-only prepare verifies count/digest before freezing a candidate, with an
  explicit 8 MiB assembled metadata limit. Initial Split prepare stays C1b/D4.
  Files: `composition/{begin,edit,state,evaluation,evaluate_step,prepare}.rs`,
  `inspection/materialize.rs`, `tests/composition{,_recovery}_test.rs`.
- [x] **C1b Complete composition publication and initial Splits**: durable revision-CAS draft binds
  parent and optional Split source revision, with bounded edit pages. Expose begin,
  apply-edit, inspect, evaluate-next-page, prepare and abandon operations; each
  editing request carries expected draft revision and idempotency identity.
  Include All/Selection/Split/intersection source and local mapping-generated edits.
  Files: new `composition/{record,edit,evaluate,prepare}.rs`, HTTP v2 composition.
  Exit: complete child membership assembled without browser/full-SDK scans,
  unchanged required values remain represented through server-owned bindings,
  no invented raw chunk locators or duplicate payload uploads for inheritance.
- [x] **C2 Bound draft lifecycle**: successful mutation invalidates preview;
  preview jobs bind draft revision and persist continuation. At most one complete
  result binding wins CAS. Caller-driven bounded steps avoid a new scheduler.
  Persist input refs/digests, not local file handles. Renew source protection while
  active; a stale draft must revalidate before prepare after lease expiry. Prepared
  candidates retain durable ownership until publish/abandon. Files: `composition/`,
  `lease.rs`, `upload/ownership.rs`, new `tests/composition_test.rs`.
  Exit: cancellation, duplicate edits, expiry, source reclaim and stale evaluation
  cannot publish incomplete membership or leak another operation's ownership.
- [x] **C3 Separate ancestry and data-head precondition**: expose opaque head
  generation, accept valid historical/parentless source, and freeze candidate
  content independently of commit attempts. Each attempt binds candidate,
  expected head generation and attempt ID. A conflict permits explicit new attempt
  after review with unchanged parent/content; divergent candidate edits require
  new preparation. Files: `authority/publication.rs`, `authority/head.rs`,
  `authority/publication/opaque.rs`, HTTP publication, SDK publication helpers.
  Exit: branching and multiple roots work; missing/foreign/reclaimed parents fail;
  stale generations do not overwrite latest/stable or automatically reparent.
  Authority complete; HTTP and SDK exposure remains E2/E3.
- [x] **C4 Prove publication success across crashes and branches**: replace
  ancestry-as-receipt logic with durable commit evidence. Implement a helpable
  single-head-CAS protocol: head CAS records commit identity; before replacing
  that head, every mutator first durably finalizes its successful receipt. A retry
  recovers from the head or receipt; it never re-publishes a prior success. Stable
  updates and reclaim fences participate in the same helping rule. Index visibility
  consults committed evidence, not Published state alone. Files: new
  `authority/publication/commit.rs`, `authority/head.rs`, retention/reclaim callers.
  Exit: injected crashes before/after CAS/receipt and divergent branch heads prove
  no lost success, no ghost committed child, no rewind, no application-wide lock.

## Phase D — Split set revision lifecycle

- [x] **D1 Evaluate definitions and partition policy**: validate names, IDs,
  predicates, overlap/coverage independently; implement exact backlog SHA-256
  algorithm with checked weights/128-bit arithmetic and fixed vectors. Persist
  ordered definition inputs and evaluator version; distinguish inherited frozen
  membership from reevaluation. Files: new `split/{definition,evaluate,partition,
  policy}.rs`, `tests/split_selection_test.rs`. Exit: deterministic sample assignment
  across order/restart/SDK, no payload search, bounded overlap examples and explicit
  complete/partial counts. Empty named Splits valid, empty data Snapshot invalid.
- [x] **D2a Persist independent Split drafts and edit receipts**: begin/status,
  whole-set revision-CAS edit and abandon use bounded durable records. A successful
  edit marker is helped into a receipt before any later draft mutation. Exact
  historical retries return their original revision without overwriting newer
  edits. Owning Snapshot leases protect active drafts. Files: `split/{lifecycle,
  state}.rs`, `tests/split_draft_test.rs`. Exit: lost reply/restart, divergent retry,
  historical retry, stale revision, unchanged Dataset head and abandon/frozen
  transitions pass. Focused tests (2), crate all-target Clippy and formatting pass.
- [x] **D2b Prepare immutable revision pages**: draft -> evaluating -> prepared;
  store page integrity and complete binding before ready status. Validate member
  universe and cross-Split policy on one Snapshot. Define idempotent revision edits,
  rename/remove and cancellation/abandon. Files: new `split/{draft,pages,prepare}.rs`,
  `tests/split_revision_test.rs`. Exit: stale preview cannot prepare, restart resumes
  bounded work, an empty set revision is distinguishable from never-created.
- [x] **D2c Resolve frozen membership sources**: after D3 provides committed
  revision reads, evaluate inherited definitions by source revision membership
  intersection; never reevaluate their rule implicitly. Pin source owners and
  reject missing/foreign revision/name. Add indexed membership lookup and tests
  for frozen r2 while r3 changes. Complete before C1/C2 composition integration.
- [x] **D3 Commit Split head with durable receipts**: reuse C4's proven protocol
  for independent `SplitHead`; expected-current-revision CAS and exact attempt IDs.
  A new revision updates the complete set atomically, never Dataset head. Reconcile
  unknown outcomes and retain committed histories. Files: new `split/{publication,
  history}.rs`, `tests/split_publication_test.rs`. Exit: competing editors choose one
  revision; historical retry, stale attempt and deletion cannot move data latest.
- [x] **D4 Bind initial revision to data publication**: prepare initial Split
  pages/binding before C4's data commit. Data commit evidence gates all initial
  visibility; lazy/helped initialization of Split head must use that binding and
  never overwrite a later revision. Independent Split edits require committed
  owning Snapshot. Files: `composition/prepare.rs`, `authority/publication/`,
  `split/publication.rs`. Exit: every crash boundary exposes either no new Snapshot
  or complete data plus requested initial Splits; no false full-success response.
- [x] **D5a Protect branch ancestry and concurrent reclaim ownership**: inspect
  bounded child indexes using committed proof independent of temporary admission
  fences. Only an Aborted record plus durable retirement marker permits reclaiming
  its parent. Walk captured legacy roots within 64 records/16 MiB for unindexed
  descendants. Access v3 explicitly decodes v1/v2 and adds CAS reclaim ownership
  plus an irreversible retiring phase; a stale worker cannot reopen admission.
  Files: `authority/access{,/reclaim}.rs`, `authority/publication/legacy_reclaim.rs`,
  `inspection/reclaim.rs`, branch/legacy/reclaim-race tests.
- [x] **D5b Reclaim retired Snapshot metadata in bounded steps**: persist phase
  and continuation in `DS GMC`; remove Manifest partitions/binding, inspection
  projections/indexes and every owned Split revision/member page. Keep commit
  receipts, head fences and retirement evidence. Steps delete at most 20 keys
  with strict 16 MiB/2-second scans; legacy convenience is capped at 64 steps and
  30 seconds. Help the final status marker after a lost/error response.
  Files: `authority/access/cleanup.rs`, `tests/split_reclaim_test.rs`.
- [x] **D5c Complete abandoned draft and payload cleanup**: all committed revisions live with
  owning Snapshot; read/compose leases pin source. Revision publication must win
  a live-owner fence against reclaim. Cleanup batches have durable progress and
  preserve prepared/shared upload ownership. Files: `retention.rs`, `lease.rs`,
  authority reclaim functions, new `tests/split_reclaim_test.rs`.
  Exit: race between read/edit/child preparation and reclaim cannot resurrect dead
  Snapshot metadata or delete protected payload. Revision removal is not deletion.

## Phase E — reads, API and SDK delivery

- [x] **E1 Bind read sessions**: version session specs with concrete Snapshot and
  Split revision/name. Resolve aliases once and persist binding before evaluation;
  filter -> ordering/shuffle -> worker partition -> payload fetch. Reuse issued
  cursor/ack protocol. Files: `read_session.rs`, `read_session/{planning,pages,
  progress,split}.rs`, `tests/split_session{,_edges}_test.rs`. HTTP conversion
  and route admission are E2; the durable authority session contract is complete.
  Exit: old read plans decode with old semantics; new plans cannot resume under
  another revision, same-name Split, projection or source; empty set ends normally.
- [x] **E2a Expose bounded metadata endpoints**: catalog, roots/inventory,
  children, summaries, Schema, Samples, projected matrix, exact membership and
  explicit legacy preparation/status. Files: `dataset/api/v2/{inspection,
  inspection_models,query}.rs`, `dataset_inspection_contract_test.rs`, OpenAPI.
  Exit: canonical binary IDs/cursors, bounded strict query handling, commit
  visibility, auth, metadata provenance and pagination tests pass.
- [x] **E2b Expose Split lifecycle endpoints**: strict versioned definition,
  policy, provenance and revision DTOs; begin/edit/evaluate/prepare/publish/status/
  abandon plus history/head/member pages. Files: `dataset/api/v2/{predicate,
  split_models,split_requests,split_responses,splits}.rs`, Split HTTP contract and
  restart/conflict tests. Exit: explicit null versus omitted preconditions, stable
  operation retries, historical reads, exact names and no data-head mutation pass.
  Mutation capability advertisement waits for C2/D5c cleanup completion.
- [x] **E2c Expose revision-bound reads**: strict read specs, bounded preparation,
  pinned status, next/ACK/progress and cancellation using existing binary framing.
  Files: `dataset/api/v2/{read_models,reads}.rs`, `inspection/read.rs`, Split read
  HTTP tests. Exit: pending status, cancellation fence, no implicit ACK, unchanged
  v1 behavior and 16 KiB binary-ID locator projection pass. V2 delivery resolves
  indexed field locators with one 16 MiB/2-second metadata work budget.
- [x] **E2d Add exact matrices and field previews**: read-only explicit-ID
  projection (20 Samples x 12 fields) for Split/exact-ID browsing and bounded
  field prefixes. Files: `inspection/exact.rs`, `api/v2/preview.rs`, HTTP preview
  tests. Exit: requested ID order, unknown/duplicate rejection, metadata-only
  projection, multipart prefix bounds and missing/tombstone distinction pass.
- [x] **E2e Complete composition and auxiliary endpoints**: expose capabilities, head generation,
  discovery/metadata preparation, composition, revision drafts/commit/status/
  history and scoped member/read operations. Suggested v2 route families under
  `/namespaces/{ns}/datasets/{dataset}`: `/head`, `/snapshot-inventory`,
  `/snapshots/{id}/children`, `/snapshots/{id}/schema`, `/snapshots/{id}/samples`,
  `/compositions/{operation}`, `/snapshots/{id}/split-revisions`,
  `/snapshots/{id}/split-revisions/{revision}/splits/{name}/members`.
  Fix exact DTOs/methods in A2 OpenAPI before SDK/UI work. Mutation retry IDs,
  statuses, error classes and list bounds are required fields, not prose-only.
  Files: `dataset/api/v2/`, OpenAPI v2, `tests/dataset_split_contract_test.rs`.
  Exit: direct HTTP fixtures exercise every route and invalid auth/scope/bounds;
  status/preview cannot acknowledge application consumption.
- [x] **E3 Implement Rust SDK helpers**: typed IDs/models, capabilities, lazy page
  iterators, compose/edit/evaluate/prepare/commit, revision editing/history,
  conflict/unknown-outcome identities, Split plans and cancellation. Stream/file
  sources use bounded existing transfer code; do not import repository internals.
  Files: `sdk/crates/crowdb-sdk-dataset/src/{models,client,session,transfer}.rs`, new
  `split.rs`, `composition.rs`, `tests/split_contract_test.rs`, examples.
  Exit: network fixtures verify retries preserve bytes/IDs and no eager all-page
  collection; unsupported v2 capability fails before writes.
- [x] **E4 Implement Python parity and source example**: extend PyO3 wrappers,
  Python models/helpers and packaged wheel exports; reuse Rust state machine.
  Include local import and caller-owned remote-stream adapter examples with exact
  field mappings; require replayable source/reselection for retry, never silently
  reread changed source bytes. Files: `sdk/crates/crowdb-sdk-python/src/`,
  `sdk/crowdb_dataset/{client,__init__}.py`, new `sdk/tests/test_splits.py`,
  `sdk/tests/real_consumer.py`, SDK README/examples.
  Exit: installed wheel outside source tree exercises real split reads/publication,
  GIL/cancel/fork regressions pass and source objects remain untouched.

## Phase F — release acceptance and handoff

- [~] **F1 Register native owned cases**: extend `e2e-test/scenarios.py`,
  `access_dataset_fixture.py`, `tests/access_dataset.rs` and SDK consumer fixture.
  Proposed new cases: `access_dataset::split_revision_lifecycle`,
  `access_dataset::split_commit_restart`, `dataset::sdk_split_revision_round_trip`.
  Register in existing `dataset.http`/`dataset.sdk`; names are new until implemented.
  Exit: real multi-node native storage, restart and installed SDK produce evidence;
  fixture setup does not bypass HTTP/SDK mutation acceptance.
- [ ] **F2 Gates, permanent docs and R236 handoff**: complete scoped tests and
  formatting/lints separately; update Dataset design/spec/OpenAPI v1 compatibility,
  add v2 index entry and SDK usage docs. Write actual API signatures/capabilities,
  package versions, test commands/artifacts and limits into R236 plan handoff.
  Files: Dataset docs, `doc/doc_index.md`, SDK docs, Console plan, backlog index.
  Exit: R236 can consume real APIs without unfinished authority tasks. Browser
  acceptance stays in R236. Only then use requirement cleanup; update cross-links
  before deleting R237 and this temporary plan.

## Acceptance trace and test layers

- Pure unit cases under crate `tests/`: A3/D1 identity, version codecs, exact hash
  vectors, policies, predicate logic, opaque IDs and canonical definition equality.
- In-memory/faulting-store integration: B1-B5 metadata work counters and projection;
  C1-C4 inheritance/full-membership/commit receipts; D2-D5 restart, revision CAS,
  initial binding, cleanup fences; E1 exact plan/progress binding. Assert work and
  payload counters, not timing alone. Use existing common helpers; new hooks need
  `test-util` self dev-dependency if production test hooks become necessary.
- HTTP integration: E2 all endpoint/error/version paths, v1 fixtures, large limits,
  lost responses and old-server rejection. SDK integration: E3/E4 same golden
  vectors/results/errors, no second Python matcher, packaged install and streaming.
- Native E2E: F1 durable storage/restart/publication/SDK parity. Every R237 Acceptance
  bullet maps to these phases; the final browser bullet is explicitly transferred
  to R236 tasks U7-U9, not falsely marked verified here.
- Mandatory race fixtures: head advances onto another branch before old retry;
  stable update between data CAS and receipt; reclaim during Split CAS; r3 while
  composing from r2; initial revision crash at each visibility boundary; completion
  succeeds but response is lost. No real sleeps for synchronization.

## Consolidated file ownership

- Authority: `lib/crowdb-access-dataset/src/{store,chunk_store,key,record,manifest,
  planner,lease,retention,read_session}.rs`, `authority/`, `read_session/`,
  `upload/ownership.rs`, new `inspection/`, `composition/`, `split/` facades/children.
- API: `app/crowdb-access-server/src/dataset/`, relevant `config.rs` only for
  bounded-operation configuration, crate tests/common and new focused tests.
- SDK: independent `sdk/Cargo.toml`/lock only if necessary, Rust/PyO3 crates,
  Python package/tests/examples and versioned contract fixtures. No private
  authority dependency in public SDK.
- Native acceptance: `e2e-test/scenarios.py`, Dataset fixture, test client and
  Python SDK consumer. Documentation: Dataset design/spec/v2 API, SDK README,
  doc index and handoff plan. No Console component code in this task.

## Commands and evidence rules

Verified task names with `pixi task list`; no tests run during planning. Start
long builds/tests as background sessions and poll at <=60-second intervals, keeping
complete output. Prefix server-spawning checks with `pixi run clean-env &&` only
when this runtime is owned and no other worker's fixture is active. Do not clean
another model's environment. On failure print exact test/error; CI/debug runs set
`CROWDB_E2E_PRINT_SERVICE_ERRORS=1`. Do not suppress errors or inflate deadlines.

Focused non-server tests:

```sh
pixi run cargo test -p crowdb-access-dataset --tests
pixi run cargo test -p crowdb-access-server --test dataset_contract_test
pixi run test-dataset-sdk
pixi run test-dataset-sdk-python
pixi run check-dataset-sdk-boundary
```

After registration, native gates (use `--case` with the proposed names only once
registered; `--list` verifies the inventory without running a fixture):

```sh
pixi run test-container-e2e --scope dataset.http --list
pixi run clean-env && pixi run test-container-e2e --scope dataset.http --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 8192
pixi run clean-env && pixi run test-container-e2e --scope dataset.sdk --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 8192
```

Final relevant gates, each recorded independently:

```sh
pixi run test-access
pixi run test-container-e2e-fixture
pixi run rs-fmt-check
pixi run rs-lint
pixi run sdk-fmt-check
pixi run sdk-lint
```

Do not run unrelated Iceberg SDK/C++ gates unless their implementation changed.
Apply implement-requirement's five root-cause-driven attempts for a persistent
failure; record actual failures and add Blocked only when that workflow requires.

## Evidence

- Planning: inspected current point-store, head envelope/publication, selection,
  read-session, SDK and test dispatch. Confirmed no production changes made here.
- Implementation baseline, 2026-10-15: `pixi run cargo test -p
  crowdb-access-dataset --tests` passed all 48 integration tests. Existing records
  use bincode `VersionedRecord` u16 version 1; head version 2 wraps `HeadRecord`
  with random `OperationId` fence and still reads version 1. Access lease records
  have their own versioned fence. Store CAS compares exact bytes, not a multi-key
  transaction. Upload ownership uses durable per-location owners and cleanup
  progress; read-session specifications bind immutable Snapshot before ID paging.
  V1 JSON ignores optional unknown fields and uses canonical base64 Sample IDs.
  Its SDK independently constructs v1 paths; v2 cannot safely reuse those writes.
- Phase verification, native artifact paths, permanent docs and API handoff: pending.
- A2a: authenticated `GET /api/dataset/v2/capabilities`, independent v2 routing,
  Rust SDK discovery/required-capability checks and OpenAPI are implemented.
  Advertised capabilities remain empty until operation families are complete.
  `dataset_contract_test` (3), `dataset_v2_contract_test` (1), `test-dataset-sdk`
  (14), `rs-fmt-check`, `sdk-fmt-check`, `rs-lint`, and `sdk-lint` passed.
  Discovery tests cover unauthorized requests, old-server 404, unknown versions,
  missing required capabilities, and no fallback mutation. Native acceptance and
  all mutation DTOs remain pending; this is not a completed v2 implementation.
- A3a: Split record codecs use new versioned JSON with a 1 MiB encoded limit,
  a parser recursion guard and strict record fields; existing bincode records are
  unchanged. Member pages hold at most 1024 ordered unique IDs and bind their
  complete encoded scope/content with SHA-256. Predicate depth is at most 32;
  effective-field lookup and unknown-field/universe validation remain B5/D1.
  `cargo test -p crowdb-access-dataset --tests` passed 53 integration tests;
  focused tests passed again after strict codec checks, and crate all-target
  clippy passed. Initial fixture syntax error was fixed; a concurrent lint using
  the old dependency graph was rerun after the sha2 dependency settled.
- A3b in progress: fixed-size `CommitAttempt`, `CommitTarget`, `CommitGeneration`
  and `SuccessfulCommit` records reject unsupported versions, missing expected
  head, unknown fields and zero identities. Three focused record tests pass.
  `DatasetAuthority::inspect_head` observes names/generation from one stored head;
  the v2 `/head` route and SDK helper advertise/require `head-inspection`.
  HTTP v1/v2 (5 tests), SDK workspace (15 tests), root and SDK clippy pass.
  Attempt/receipt storage and helping CAS are still C4, not implemented merely
  because record types exist. Composition/Split draft state remains A3b work.
- A3b record verification: `CompositionRecord`, `SplitDraftRecord` and
  `DraftProgress` now bind input revision, exact source Snapshot/Split revision,
  evaluation position/binding, prepared target and published attempt. Edits
  invalidate previews and reject frozen content or stale expected revisions.
  Full Dataset crate tests pass (59 integration tests). These are validated
  records/state primitives; durable edit CAS, lease renewal and publication
  recovery remain C1-C4/D2-D5.
- B1 investigation: `MultiScanRequest.max_bytes` is applied after receiving and
  decoding a partition response. Current `ScanRequest` wire has an item limit
  but no caller byte budget; `execute_scan` uses a fixed server budget. Production
  bounded metadata reads must enforce the cap before materializing/transporting
  legacy values. Extend the protocol/storage boundary and its tests before using
  a post-decode slice as a supposed read bound. No B1 implementation is claimed.

## Bounded metadata and projection verification — 2026-10-16

- `pixi run cargo test -p crowdb-protocol --test chunk_kv_wire_test`: 7 passed.
- `pixi run cargo test -p crowdb-chunk-kv-client --test ordered_test`: 9 passed;
  strict cross-partition continuation and unsupported transport covered.
- `pixi run cargo test -p crowdb-chunk-kv-server --test server_test`: 14 passed;
  oversized first value, retry with larger budget, and split-writer boundary covered.
- `pixi run cargo test -p crowdb-tree-ffi --test bounded_scan_test`: 3 passed;
  resident, flushed, external-cell, binary bounds and legacy behavior covered.
- `pixi run cargo test -p crowdb-access-dataset --tests`: 66 passed, followed by
  `--test inspection_index_test`: 4 passed after adding the binary-ID case.
- `pixi run rs-lint`: passed; final touched Dataset/client `cargo clippy
  --all-targets -- -D warnings`: passed. Rust formatting and diff whitespace pass.
- `pixi run tree-lint`: exit 0. Complete log `/tmp/dataset-tree-lint-final.log`;
  existing diagnostics outside the changed scan remain. New nested conditional
  warning was removed before the final run.
- `pixi run test-cpp`: exit 0. Complete log `/tmp/dataset-cpp-tests.log`;
  measurements `.crowdb-runtime/artifacts/unit/cpp-453e53c9789a434dbf0737af515b8362/results.json`.
- Index tests prove interrupted readiness cannot advance latest, retry preserves
  immutable rows, exact binary IDs retain byte order, payload bytes are separate
  from small field metadata, and legacy over-limit IDs remain publishable with
  explicit unsupported inspection status. Public inspection visibility remains B2b.

## Legacy inspection verification — 2026-10-16

- `pixi run cargo test -p crowdb-access-dataset --tests`: passed (71 cases).
  New legacy tests cover bounded resumable windows across authority recreation,
  stable completed progress, untouched legacy bytes, cancellation, explicit size
  limits/unsupported opaque records, lease release, reclaim fences and corrupted
  durable positions. Index and store regressions remain green.
- Added typed `ScanLimitExceeded` wire response and `StoreError::Limit` so a
  budget failure is distinguishable from invalid input and empty completion.
  Protocol tests: 8 passed; affected strict server scan case passed.
- Full `pixi run rs-lint` passed after splitting the preparation orchestration;
  complete log `/tmp/dataset-inspection-lint-final.log`. Dataset suite log:
  `/tmp/dataset-legacy-tests.log`. Rust fmt and diff whitespace pass.
- Preparation validates source existence before creating a lease, processes at
  most 100 rows/1 MiB per step, caps old record decode at 16 MiB and persists a
  source digest plus validated position. Ready publication follows all row and
  inventory writes. Status reads never load a Manifest.

## Candidate publication verification — 2026-10-16

- Candidate content/parent now bind an immutable preparation operation separately
  from each head-CAS attempt. Historical parents and new roots are supported.
- V3 head records retain the successful attempt until every replacing mutator
  has durably written its receipt. Stable changes and reclaim head fences help
  pending commits; retries return the original success without moving latest.
- Modern candidates require commit evidence, including candidates whose Published
  record was written before a losing CAS. Legacy ancestry uses a separately saved
  migration anchor and visibility markers, never a fabricated modern receipt.
- Full Dataset suite passed (77 tests); focused publication tests cover lost CAS,
  lost reply, receipt failure, stale attempts, divergent IDs, old-head retry and
  legacy ancestry after a new root. Root `pixi run rs-lint` passed. Logs:
  `/tmp/dataset-candidate-tests.log`, `/tmp/dataset-candidate-lint.log`.
- Public inspection remains B2b/B4; Split atomic initialization remains D4, and
  branch-wide reclamation/fence lifecycle remains D5. No new capability advertised.

## Paged inspection verification — 2026-10-16

- B2b/B4 authority now discovers legacy Dataset identity keys and exposes paged
  roots/children/inventory, Schema, binary Sample IDs, exact membership and
  projected effective field metadata. Page size is capped at 100 and aggregate
  storage bytes at 1 MiB. Reads share a deadline and cancellation; no Manifest,
  payload, unbounded point read, mutation or consumer acknowledgement is needed.
- Visibility uses the durable Snapshot receipt, current successful head marker,
  or validated legacy migration marker. Published state alone is insufficient.
  Legacy preparation reconciles proven publication before indexing; oversized old
  records remain bounded and unproven candidates remain invisible.
- Opaque versioned cursors bind Dataset/Snapshot/query/projection. Immutable
  Snapshot continuations survive unrelated head changes; cross-query reuse fails.
  Inherited cells expose source Snapshot, tombstones and missing cells distinctly.
- `pixi run cargo test -p crowdb-access-dataset --tests`: 84 tests passed.
  Six new inspection cases assert no unbounded/Manifest/payload reads, ghost
  exclusion, pending receipt visibility, exact IDs, source metadata, cancellation,
  aggregate limits, old identity discovery and projection cursor binding.
  Full log `/tmp/dataset-inspection-pages-tests.log`. Public HTTP wiring stays E2.
- Full `pixi run rs-lint` passed after consolidating a duplicated test helper
  module import; log `/tmp/dataset-inspection-pages-lint.log`. Rust fmt and
  diff whitespace pass.

## Effective selection and partition evaluation — 2026-10-16

- B5 evaluates bounded member windows against effective inherited inline values,
  preserving binary ID order, exact empty equality, tombstones and missing values.
  Unknown explicit IDs and predicate fields fail; chunk-backed predicates do not
  fetch payloads. Not complements the selected Snapshot universe. Predicate-scoped
  continuations prevent resuming a different rule; v1 planning remains unchanged.
- D1 adds `SamplePartition` using the exact SHA-256 domain/seed/ID-length bytes,
  u128 multiply/shift and checked u32 weight sums. Four Python hashlib golden
  vectors cover binary IDs, maximal seeds and maximal total weight. Assignment
  order/restart and invalid interval tests pass. `SplitValidation` tracks assigned,
  overlapping and unassigned counts independently with bounded example counts.
- Focused selection, partition and inspection tests passed; full Dataset suite
  passed 89 tests and root Rust lint passed. Logs: `/tmp/dataset-selection-tests.log`
  and `/tmp/dataset-selection-lint.log`. Final partition tests and crate lint also
  pass after adding a 32 KiB-per-category diagnostic byte cap. Draft evaluation and
  persistence remain D2, and revision publication remains D3.
- Sequencing refinement: complete D2/D3 before C1/C2, because child composition
  consumes frozen parent Split revisions. C1/C2 then adds child data and inherited
  membership, followed by atomic initial Split visibility in D4.

## Split draft evaluation verification — 2026-10-16

- D2b persists twenty-Sample evaluation windows and immutable member pages before
  advancing draft progress. Candidate identity derives from operation + editing
  revision; page retries are idempotent. Complete progress binds SHA-256, page
  counts, Snapshot universe count and policy diagnostics. Prepare freezes the
  entire set only from that exact evaluation revision.
- Empty named sets prepare successfully under allow-unassigned. Overlap/coverage
  violations stop Prepare; cancellation and stale evaluation requests do not
  advance progress. Draft edits clear evaluation state. Frozen inherited sources
  explicitly remain unsupported until D2c supplies committed revision lookup.
- Focused draft/evaluation tests pass (4); full Dataset suite passes 93 tests.
  `pixi run rs-lint`, crate Clippy, Rust fmt and diff whitespace pass. Logs:
  `/tmp/dataset-split-evaluation-tests.log`, `/tmp/dataset-split-evaluation-lint.log`.

## Split publication and historical read verification — 2026-10-16

- D3 uses an independent helpable Split head CAS and shared attempt/receipt
  identities. Draft Committing state excludes concurrent abandon; status recovers
  committed evidence or releases a definitively stale attempt back to Prepared.
  Successful retries never rewind Split current revision or Dataset latest/stable.
- Snapshot reclaim now fences the Split head after closing owner admission, and
  helps any pending successful Split receipt before replacing its marker. Broader
  branch/source cleanup and retention acceptance remain D5.
- Small prepared revision indexes are filtered by receipt/current-head proof.
  Historical revision definitions and checksum-bound member pages are available
  through bounded reads. Cursors bind owner, revision, name, query and version;
  no data Manifest or chunk read is required.
- New publication tests (3) and history/member tests (2) pass, covering competing
  editors, stale attempt reset, lost CAS response, receipt failure, historical
  retry, no data-head mutation, invisible prepared revisions and cross-scope
  continuation rejection. Full Dataset suite: 98 passed. Root Rust lint passes
  after splitting a long test fixture. Logs: `/tmp/dataset-split-publication-tests.log`,
  `/tmp/dataset-split-publication-lint.log`. Rust fmt and diff whitespace pass.

## Frozen Split inheritance verification — 2026-10-16

- D2c resolves exact committed parent revision/name and intersects its frozen
  membership with child Sample IDs. It does not run inherited predicates against
  new child samples. Explicit reevaluation switches origin to Evaluated.
- Frozen source definitions are cached per revision within one bounded evaluation
  window; binary search over checksum-bound member pages avoids a full source
  membership scan. Source ownership must match the child's direct parent.
  Drafts retain source leases, including sources removed by subsequent edits, so
  publish/abandon can release every acquired source handle without losing retry
  ownership. Expired leases are revalidated before evaluation/preparation.
- Tests prove frozen r1 survives parent r2, newly matching child IDs stay unassigned,
  explicit reevaluation admits them, removed IDs disappear by intersection, and
  uncommitted/unknown source revisions or names fail. Full Dataset suite: 100
  tests passed; root Rust lint passed. Logs: `/tmp/dataset-split-inheritance-tests.log`,
  `/tmp/dataset-split-inheritance-lint.log`. Rust fmt and diff whitespace pass.

### Composition draft/evaluation verification

- `pixi run cargo test -p crowdb-access-dataset`: passed, including five new
  composition/recovery cases. Full output: `/tmp/dataset-composition-tests.log`.
- `pixi run rs-lint`: passed. Full output: `/tmp/dataset-composition-lint.log`.
- `pixi run rs-fmt-check` and `git diff --check`: passed.
- Preparation currently accepts data-only drafts; initial Split binding, publish
  recovery and durable cleanup remain active C1b/C2/D4/D5 work. No mutation
  capability is advertised to HTTP/SDK clients yet.

### Initial Split and composition publication verification

- Candidate binding now includes an immutable initial revision. Internal initial
  Split preparation uses the candidate projection in bounded windows; public
  readers and independent Split mutation still require a committed data owner.
- Data commit requires the complete initial Split draft to be Prepared. Data
  receipt gates initial revision visibility and provides a virtual initial head;
  the first independent revision CAS does not overwrite or reinitialize it.
- Composition Prepared -> Committing -> Published recovers lost responses through
  durable receipts, excludes abandonment, and retains the candidate after a stale
  expected-head conflict. Historical retries do not move either head backward.
- `pixi run cargo test -p crowdb-access-dataset`: passed, full output
  `/tmp/dataset-initial-split-tests.log`. Five focused initial/publication cases
  also passed after tightening the initial-prepared gate, including pinned
  parent revision changes, partial invisibility, inheritance and lost head reply.
- `pixi run rs-lint`, `pixi run rs-fmt-check`, `git diff --check`: passed.
  Lint output: `/tmp/dataset-initial-split-lint.log`.
- C2/D5 still owns candidate abandonment fencing, durable orphan cleanup and
  branch-aware ancestry protection; HTTP/SDK integration remains Phase E.

### Branch reclaim verification

- `pixi run cargo test -p crowdb-access-dataset`: passed. Full output:
  `/tmp/dataset-branch-reclaim-tests.log`.
- New tests cover historical live children, prepared non-children, temporary
  child fences, unindexed legacy descendants and a fixed concurrent interleaving
  where an old protected result must not undo a newer terminal reclaim.
- Existing candidate fault-store needed bounded scan delegation after reclaim
  began checking child indexes. Retirement marker reads now include key bytes
  in their explicit read budget. Both first-divergence failures are fixed.
- `pixi run rs-lint`, `pixi run rs-fmt-check`, `git diff --check`: passed.
  Full lint output: `/tmp/dataset-branch-reclaim-lint.log`.

### Snapshot metadata cleanup verification

- `pixi run cargo test -p crowdb-access-dataset`: passed. Full output:
  `/tmp/dataset-metadata-cleanup-tests.log`.
- Restarted cleanup removes both historical revision pages while preserving a
  different Snapshot's current revision and old commit receipts. Injected failure
  after durable completion progress is repaired by the next cleanup step.
- `pixi run rs-lint`, `pixi run rs-fmt-check`, `git diff --check`: passed.
  Lint output: `/tmp/dataset-metadata-cleanup-lint.log`.
- Abandoned composition/candidate fencing, scratch/obsolete evaluation cleanup,
  and upload ownership release remain C2/D5c, with boundary wiring in E2.

### Prepared composition abandonment verification

- Abandon now fences the raw data candidate and initial Split owner, then retires
  only an uncommitted candidate. The fence helps pending head receipts before
  deciding; a successful historical candidate repairs composition Published state
  and is never deleted by cancellation. Owned source leases release afterward.
- `pixi run cargo test -p crowdb-access-dataset` and `pixi run rs-lint`: passed.
  Logs: `/tmp/dataset-abandon-tests.log`, `/tmp/dataset-abandon-lint.log`.
- `pixi run rs-fmt-check` and `git diff --check`: passed.
- C2/D5c remains open for obsolete preparation/scratch cleanup and durable upload
  owner retirement; this checkpoint covers the current prepared candidate fence.

### Upload owner retirement verification

- Added `DatasetUploads::retire_owner` for terminal publication cleanup: atomically
  remove only that owner and persist revocation, including a not-yet-arrived claim.
  Other owners continue protecting shared data. Version 3 sessions prevent older
  writers from silently ignoring revocations; version 2 remains readable.
- Revoked owner IDs are sorted, unique and capped at 4096 per upload. Ordinary
  reversible release retains its existing behavior. Boundary cleanup will use
  retirement when E2 connects composition ownership and cancellation.
- `pixi run cargo test -p crowdb-access-dataset`, `pixi run rs-lint`,
  `pixi run rs-fmt-check`, `git diff --check`: passed.
  Logs: `/tmp/dataset-upload-retirement-tests.log`,
  `/tmp/dataset-upload-retirement-lint.log`.
- Permanent Dataset architecture now records branch protection, reclaim ownership,
  bounded metadata cleanup and persistence-version rules.

### Boundary sequencing

- Core composition/revision publication, cancellation fencing and upload owner
  revocation are available. Remaining C2/D5c work depends on the HTTP ingestion
  coordinator that resolves upload references and owns claims. Implement E1's
  revision-bound sessions next, then E2 together with the remaining lifecycle
  orchestration; do not advertise mutation capabilities until those gates pass.
- Obsolete preparation/scratch and abandoned Split metadata cleanup remains
  required; no C2/D5c completion is claimed by this sequencing change.

### Revision-bound read session verification

- Versioned specs persist concrete Snapshot and optional Split revision/name before
  bounded selection. Existing session records retain their exact v2 bincode layout;
  a separate version reservation prevents v1/v2 reuse of one operation ID.
- Selection and frozen membership intersection precede shuffle and worker/rank
  partitioning. Prepared ID pages have count/byte limits and checksum binding.
  Empty sets finish normally; missing names fail. Resume uses commit metadata,
  not the full Manifest, and preserves acknowledged delivery progress.
- Six focused cases cover alias changes, restart/replay, worker disjointness,
  spec collisions, cancellation fences, empty inline equality and no Manifest
  reads. Request cancellation releases the owned lease while preserving progress.
- Full `pixi run cargo test -p crowdb-access-dataset` passed; the additional
  cancellation case passed in `--test split_session_edges_test` afterward.
  Its initial fixture incorrectly used v1 publication for a new root; corrected
  it to the candidate/attempt API without changing v1 semantics.
- `pixi run rs-lint`, `pixi run rs-fmt-check`, `git diff --check`: passed.

### Bounded HTTP metadata verification

- V2 now advertises `bounded-inspection` and `inspection-preparation` alongside
  head inspection. Routes expose catalog, inventory/roots/children, summary,
  Schema, IDs, effective field metadata, exact membership and preparation/status.
  No composition or Split mutation capability is advertised yet.
- DTOs preserve canonical base64 IDs/cursors, hexadecimal identities/digests and
  decimal-string counts/lengths. Query parameters reject duplicates, unknown
  keys, malformed encoding and unsupported bounds. Matrix projections cap at
  twelve fields; storage reads retain their aggregate item/byte/deadline limits.
- Public preparation status checks bounded commit evidence. Explicit preparation
  may resolve legacy ancestry; GET status does not materialize a Manifest.
- `pixi run clean-env && pixi run cargo test -p crowdb-access-server --test
  dataset_inspection_contract_test --test dataset_v2_contract_test`: six passed.
  `cargo test -p crowdb-access-dataset --test inspection_legacy_test --test
  inspection_visibility_test`: six passed. An initial invocation used a nonexistent
  preparation test filename; corrected to the actual legacy test target.
- Root `rs-lint`, `rs-fmt-check` and `git diff --check` passed. OpenAPI routes and
  DTO schemas are documented; optional standalone YAML parsing was unavailable
  because neither PyYAML nor the Node yaml package is installed. Validate the
  complete document with the release documentation tooling before F completion.

### Split lifecycle HTTP verification

- Strict DTOs cover predicates, weighted definitions with exact u64 seeds, frozen
  provenance, policy, operation/draft revisions and publication preconditions.
  Missing nullable preconditions differ from explicit null; unknown fields fail.
- Routes expose begin/edit/status/abandon, bounded evaluate, prepare/publish,
  current head, committed history/revisions and exact-name member pagination.
  Prepared revisions remain invisible; stale publication retains the candidate;
  historical receipt retry never moves current or Dataset latest/stable.
- `pixi run clean-env && pixi run cargo test -p crowdb-access-server --test
  dataset_split_contract_test --test dataset_split_recovery_contract_test --test
  dataset_inspection_contract_test --test dataset_v2_contract_test`: nine passed.
  Restart test resumes a partial evaluation, publishes an empty whole-set revision,
  rejects a competing publisher and still reads the older complete revision.
- `cargo test -p crowdb-access-dataset --test commit_record_test`: three passed.
  `rs-lint`, `rs-fmt-check`, `git diff --check`: passed. OpenAPI records the new
  operations. Mutation advertisement remains deferred until abandoned/superseded
  metadata cleanup is integrated; these HTTP tests do not claim native E2E delivery.

### Revision-bound HTTP read verification

- Added strict v2 prepare/status/next/progress/acknowledge/cancel endpoints. Status
  exposes frozen Snapshot/Split scope and partial progress; it does not acknowledge
  delivery. All u64 control values are decimal strings. V1 framing is reused.
- V2 batch delivery resolves exact member/field locators from bounded indexes,
  preserving request order and inherited field semantics without full Manifest
  allocation. V1 keeps its original path and limits. Binary IDs up to 16 KiB are
  accepted by indexed delivery; unknown samples/fields and cancellation fail.
- `pixi run clean-env && pixi run cargo test -p crowdb-access-server --test
  dataset_split_session_contract_test --test dataset_contract_test`: five passed.
  Focused Dataset inspection-read and Split-session suites: seven passed.
- Root `rs-lint`, `rs-fmt-check`, `git diff --check`: passed. Remaining auxiliary
  work includes exact-ID metadata matrices, bounded field preview, v2 ingestion,
  composition publication ownership and lifecycle cleanup before advertisement.

### Exact inspection and preview verification

- Added read-only POST matrix projection for a bounded explicit Sample window,
  retaining request order without loading inline bytes or creating read sessions.
  GET preview returns at most 64 KiB, distinguishes missing/tombstone/empty values,
  and verifies uploaded parts across part boundaries under a five-second deadline.
  Only Dataset-scoped upload locators are accepted by this preview implementation.
- Shared owned read leases now release on preview completion, error or dropped
  request; lease expiry remains the storage-failure fallback. Payload bytes are
  returned as canonical base64 for text/hex presentation; truncated MD5 describes
  the full field, not its prefix. No application acknowledgement is performed.
- `pixi run clean-env && pixi run cargo test -p crowdb-access-server --test
  dataset_preview_contract_test --test dataset_split_session_contract_test` passed;
  preview tests passed again after adding scoped upload validation.
- Root `rs-lint`, `rs-fmt-check`, `git diff --check` passed. System Python has the
  YAML parser unavailable in the Pixi Python environment: `pixi run
  /usr/bin/python3 /tmp/validate_dataset_openapi.py` passed (26 paths, 32 unique
  operations, no duplicate YAML keys, all local schema references resolve).

### Composition upload ownership verification

- Managed compositions use version 2 roots: staging references are durably
  registered before upload claims; candidate references are registered in bounded
  locator windows before claims. Publication rejects incomplete ownership.
- Terminal cleanup retires staging owners and obsolete candidate owners, preserving
  committed candidates and other owners of shared payloads. Candidate fencing and
  owner retirement share a twenty-operation window with durable progress. Revoked
  owners cannot claim again after delayed work resumes. Historical edit receipts
  remain readable after staging retirement.
- `pixi run cargo test -p crowdb-access-dataset --test composition_upload_test
  --test composition_test --test composition_publication_test --test upload_test`
  passed fifteen cases, including four new ownership scenarios. Initial test
  compilation exposed fixture type errors (binary locator and verified part tuple);
  corrected those without production changes. Clippy's similar-name failure was
  resolved by naming the ownership collection explicitly and factoring enrollment.
- `pixi run rs-lint` passed. C2/D5c remains open: obsolete preparation/scratch and
  abandoned Split metadata cleanup, committed Snapshot payload retirement, and
  HTTP cleanup orchestration are not completed by this change.

### Obsolete composition evaluation cleanup

- Added resumable `cleanup_composition_scratch_step`: one obsolete evaluation
  window per call, with bounded metadata reads and a two-second deadline. Active
  input revisions are excluded; terminal roots retire all input revisions.
- Work pages already describe every possible row write. Cleanup replaces those
  rows with small permanent CAS fences before sealing their work page; sealing
  the first absent work page prevents an in-flight evaluator from creating an
  untracked next window. A durable cursor survives restart and partial progress.
- Three new tests cover obsolete/current revision isolation, delayed CAS rejection,
  early abandonment, cancellation/restart, and committed Manifest/receipt survival.
  Focused composition cleanup/recovery/abandon suites and root Clippy passed.
- This retires bulk evaluation rows, not edit receipts, candidate preparation,
  Split membership or committed payload ownership. Remaining C2/D5c work stays open.

### Split work intent and retirement verification

- Split evaluation durably registers membership page keys/digests before sending
  immutable writes. New strict draft roots carry a work-format marker; older roots
  still decode but cannot use scratch cleanup without the recorded intents.
- `cleanup_split_scratch_step` retires obsolete/abandoned membership and candidate
  records in twenty-reference windows. It preserves live committed revisions and
  accepts initial Split cleanup only after terminal data Snapshot retirement.
  Small permanent CAS fences also survive general Snapshot metadata cleanup.
- New tests cover abandoned prepared data, delayed immutable CAS rejection,
  historical/current revision isolation, restart and 45 overlapping Splits.
  Initial-Split abandonment now exercises scratch cleanup. Existing reclaim test
  now asserts retained small fences and failed resurrection instead of absent keys,
  matching the strengthened storage contract; unrelated owners remain readable.
- Focused cleanup/abandon/reclaim suites passed ten cases; evaluation tests passed.
  Root Clippy passed after correcting a serde callback signature and MSRV use.
  A misspelled test target and a temporary delimiter error were corrected.
- Remaining C2/D5c: failed/superseded data-candidate metadata fencing, orphan edit
  pages, committed Snapshot upload-owner retirement and HTTP orchestration.
  No mutation capability advertisement or complete lifecycle claim is made yet.
- Full `pixi run cargo test -p crowdb-access-dataset`, root `rs-lint`,
  `rs-fmt-check` and `git diff --check` passed after the final reclaim changes.

### Failed and superseded candidate cleanup

- `cleanup_composition_candidates_step` visits deterministic candidate operations
  for obsolete input revisions, including preparation attempts absent from the
  composition root. Missing operation/Snapshot reservations become permanent
  fences; existing candidates are fenced against publication before bulk cleanup.
- Candidate metadata cleanup enumerates all possible Manifest/index writes from
  the retained original record, sealing keys that an interrupted writer has not
  created yet. It persists partition/index cursors, retains valid small retired
  summaries, and seals absent initial Split reservations before late creation.
  Successful historical publications are preserved.
- Four tests cover absent preparation, missing final candidate binding/index rows,
  superseded partial initial Splits, restart, retained successful publication and
  rejection of delayed index writes after general Snapshot cleanup.
- Focused candidate/abandon/branch/Split reclaim tests passed eleven cases; candidate
  tests passed again with delayed-write regression. Root `rs-lint` passed. Reads
  cap the retained candidate record at 9 MiB and each metadata window at 9 MiB;
  the operation has a two-second deadline. These are explicit preparation-cleanup
  bounds, separate from the ordinary inspection budget.
- Remaining lifecycle work: orphan edit pages, committed Snapshot upload owner
  retirement and full HTTP ingestion/cleanup orchestration. C2/D5c remain open.

### Terminal upload ownership and edit payload cleanup

- Snapshot upload reclaim uses the retained bounded Manifest after terminal
  admission fencing, retiring at most twenty field positions per call. Durable
  progress survives retry/index cleanup; independent shared owners remain valid.
  Physical data cleanup remains in the upload registry and is not reported as
  completed merely because ownership retirement finished.
- Composition version 3 registers edit IDs before writing edit payload pages.
  Terminal cleanup seals registered pages, including failed edits whose root CAS
  did not apply, while historical edit receipts continue to return their original
  result. Registration is capped at 4096 attempted edit IDs and upgrades old roots.
- New tests prove latest protection, shared upload survival, permanent retired-owner
  rejection, post-index owner cleanup, failed edit admission/late-write fencing,
  historical edit receipts and published data survival. Focused suites passed.
- Lifecycle pieces are available for composition API integration. D5c still needs
  HTTP orchestration and whole-Snapshot cleanup of in-flight Split draft work;
  mutation capabilities remain unadvertised pending that acceptance.
- Full Dataset crate tests and root Clippy passed after the terminal ownership/edit
  changes. The full test output is `/tmp/dataset-lifecycle-cleanup-tests.log`.

### Whole-Snapshot retirement orchestration

- Added resumable Snapshot-scoped scanning of Split draft roots, one root/window
  at a time. Terminal owner proof is required before scanning; unrelated Snapshot
  drafts remain unchanged. Existing work-format-free roots can be upgraded only
  under terminal retirement; ordinary metadata cleanup seals their stored pages.
- Split retirement also preserves a valid small revision summary fence, preventing
  delayed preparation from restoring a removed summary. General metadata cleanup
  retains these summaries alongside inventory/child summaries and negative fences.
- `reclaim_snapshot_step` composes owner retirement, draft work cleanup, index
  fencing and ordinary metadata cleanup under a ten-second deadline. It reports
  the incomplete phase and explicitly defers physical upload cleanup.
- Snapshot-wide draft cleanup, composition upload/reclaim and Split scratch suites
  passed nine cases; root Clippy passed. Tests cover restart, missing delayed member
  pages, foreign Snapshot isolation and successful composed retirement.

### Composition HTTP lifecycle and configured capabilities

- Added strict v2 composition begin/edit/evaluate/prepare/publish/status/abandon/
  cleanup, Dataset-scoped multipart uploads, Dataset creation/metadata, stable,
  retention, commit receipt lookup, Split cleanup and composed Snapshot reclaim.
  Composition bodies cap at 1 MiB and edits at twenty Samples. Raw storage
  locators are rejected; server-resolved uploads enter durable ownership first.
- Capabilities now include dataset-management, split-revisions and split-reads.
  uploads, composition and snapshot-reclaim are advertised only with a configured
  upload service. Historical edit responses retain their original draft; GET
  returns current status. OpenAPI validates 43 paths and 53 unique operations.
- Five new HTTP cases cover uploaded initial Split publication, frozen Split child
  inheritance, cancellation/historical retry, restart/head conflicts, canonical
  request validation, Dataset upload isolation, retention and bounded retirement.
  All passed. Full `pixi run cargo test -p crowdb-access-server` passed, including
  v1 and Iceberg/S3 regression tests; root Clippy and format checks passed.
- Initial fixture failures were missing upload-service wiring; an older unsupported
  version test now uses v3 because v2 Dataset metadata is implemented. Disk-full
  compilation was resolved with Cargo cache cleanup (222 GiB became available);
  the broad clean reported permission errors in unrelated OCI cache, while Rust
  artifacts were successfully cleared. No source or persistent runtime data was
  removed. No native multi-node or browser acceptance is claimed by these tests.

### Typed Rust control SDK

- `DatasetClient::control()` shares the existing transport and provides typed v2
  lifecycle, inspection, upload, Split and read operations with pre-write capability
  checks. IDs, head generations, binary Sample IDs and decimal u64s validate exact
  wire representations. Each mutation preserves caller retry identities.
- Lazy typed page cursors fetch only on demand, retain continuation on failure and
  reject repeated/oversized continuation results. Bounded file and caller-reader
  uploads verify replayed parts, total digest and exact EOF without changing sources.
  The streaming decoder selects v2 ID/field limits only for v2 read delivery.
- Five v2 SDK tests cover precision/canonical input, missing capabilities, lazy
  pagination, identical publication retry bytes and long v2 IDs with unchanged v1
  bounds. Full Dataset SDK suite passed twelve tests. SDK Clippy and formatting,
  root Clippy and `git diff --check` passed.
- Standalone `split_control` connects the public SDK to the real HTTP listener.
  Local file and replayed stream uploads, initial publication, revision replacement,
  historical membership/read, explicit ACK, frozen branching, metadata/preview,
  stable/latest separation and cleanup passed. This still uses an in-memory service
  store; native multi-node acceptance remains F1.

### Installed Python control SDK

- Synchronous/asynchronous `.control()` wrappers dispatch the same typed Rust v2
  operations. Paging remains lazy, part and stream transfers use native admission,
  cancellation aborts native tasks, and ambiguous errors preserve their exact
  attempt/edit identity. Added SplitSource and exact-u64 decimal helpers.
- Installed-wheel tests passed ten cases including new capability gating, lazy
  paging and async cancellation plus existing GIL/admission/fork/Iterable checks.
  Initial paging failure was a nullable request dictionary in the Python identity
  helper; corrected and retested in a fresh external wheel install.
- `/tmp/crowdb-split-wheel` contains the externally built sdist, wheel and consumer.
  The installed Python consumer passed local file/import, bounded reader adapter,
  initial/independent Split publication, historical membership, frozen branching,
  async read/explicit ACK and pagination against the real HTTP listener.
  Command: `CROWDB_DATASET_PYTHON=/tmp/crowdb-split-wheel/consumer/bin/python
  pixi run cargo test -p crowdb-access-server --test dataset_sdk_python_split_test
  -- --ignored --nocapture` (preceded by clean-env).
- SDK Python Clippy, root Clippy and formatting passed. Native acceptance is next;
  runner rejected the stale release image before executing cases, so rebuild the
  shared OCI image with current server inputs before claiming multi-node evidence.
