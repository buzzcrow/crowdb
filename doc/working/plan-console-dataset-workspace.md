<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console Dataset Workspace Plan

Upstream: [R236](../backlog/R236-console-dataset-workspace.md),
[R237](../backlog/R237-dataset-split-lifecycle.md),
[backend plan](plan-dataset-split-lifecycle.md),
[Console UI](../design/console/design-crowdb-console-ui.md).
Goal: deliver Dataset before S3 with real Snapshot/Split inspection and a complete
local import, publication and independent Split revision editing flow.

## Handoff and scope

- Implementation authorized on 2026-10-15; waiting for R237. All tasks unchecked;
  no Console implementation or browser test has run. R237 must deliver backend/HTTP/SDK acceptance before this plan's
  integration work. Read its actual API handoff, not just these intended names.
- Own Console listener discovery/proxy/capability enforcement, Dataset navigation,
  single Sample/Schema matrix, local test/demo import, revision editing and E2E.
  Backend authority/index/selection/composition/publication belongs to R237.
  Do not synthesize these functions by downloading full Manifests in React.
- UI mutations use real services and survive documented failure/recovery paths.
  Test/demo positioning reduces supported sources, not data correctness.
- No external connector UI, training monitor, rich media decoder, SQL/ETL builder,
  physical locator decoder, automatic dataset generation or new metrics dashboard.
  No Dataset deletion; no in-place Snapshot editing; no independent Split pruning.
- Fixed defaults: multiple parentless roots allowed; initial view is all Samples;
  text/hex preview <=64 KiB; 20 rows x 12 fields per matrix window; 20/max100/1 MiB
  list pages; one shared revision for the complete Split set; 32 history visits.
- Preserve concurrent R235 edits. Inspect working tree before touching shell,
  deployment/config or shared tests. Implementation follows repository
  skills/gated requirement commit cadence.

## Dependency handoff checklist

R237's implementing model fills in these fields before R236 starts; they are
unverified today. A missing field is unfinished backend integration, not permission
to mock the final acceptance path.

- [ ] **H1 Wire capabilities**: record v2 OpenAPI path, server/SDK versions and
  availability endpoint. Expected support: bounded catalog/inspection, metadata
  preparation status, compositions, branch publication and Split revisions.
- [ ] **H2 Publication contract**: record exact candidate/attempt/expected-head
  fields, success receipt and retry/status operations. Record how an explicit
  new commit attempt preserves candidate/parent after head conflict.
- [ ] **H3 Composition and revision contract**: record draft expected-revision
  edits, preview progression, source pinning, invalidation, initial revision
  binding, independent revision CAS, abandon and recovery operations.
- [ ] **H4 Evidence and fixtures**: record passing dataset.http/dataset.sdk native
  artifact paths and source fixture creation helpers. Verify old identity discovery,
  bounded inspection and same-name Split revision identity against live API.

## Shared UI state contract

- `DatasetSelection` is a discriminated union (new): root, Dataset, Snapshot,
  Split revision/set, named Split, Sample row, Schema field header or Sample/Field
  cell. Carry full parent scope plus immutable Snapshot/revision IDs where needed.
  Header selection never borrows an unrelated Sample ID.
- Domain state owns expanded tree identities, each child-page cursor, matrix
  row/column windows, exact-ID query, selected resource and scroll. Navigation
  snapshots contain only bounded identities/query metadata, no field bytes,
  complete Manifest, upload bodies or credentials.
- Draft state is separate from navigation: authority operation ID, expected draft
  revision, source references, file fingerprints/upload IDs and current action.
  Store bounded non-secret recovery references locally; authority owns complete
  durable draft edits. Browser File objects are not persistence or server paths.
- A selected r2 remains r2 when current becomes r3. Refresh current-pointer badges
  separately; offer explicit Go to current. Reclaimed destinations preserve
  context and show unavailable, never fall forward silently.

## Phase U — dependency-ordered implementation tasks

- [ ] **U1 Measure shell baseline and register new acceptance owners**: inspect
  current native browser fixture/selection/credentials paths; run affected shell
  and navigation owner baselines before visible changes. Reserve Dataset `8x`
  specs (Iceberg 6x, S3 7x, cross-domain 9x); register new owned native cases in
  `CONSOLE_OPERATION_CASES`. Files: `e2e-test/scenarios.py`, native fixture/browser
  helpers, `clients/console-ui/e2e/flows/00-shell.spec.ts`,
  `91-navigation-state.spec.ts`, new `80-dataset-browse.spec.ts`,
  `81-dataset-import.spec.ts`, `82-dataset-splits.spec.ts`.
  Exit: every new mutation flow has one fixture owner; proposed case names listed
  below resolve in root runner, no host browser process starts service binaries.
- [ ] **U2 Wire configured Dataset service**: add Dataset origin/config discovery,
  server-held bearer use and capability reporting. Audit local/remote/managed
  Access launch, monitor credentials and default ports without changing other
  protocol identities. Add API-prefix-safe v2 proxy/streaming routes with explicit
  GET/status vs mutation enforcement. Files: `app/crowdb-web/src/access.rs`,
  `access/credentials.rs`, new `access/dataset.rs`, `services/access.rs`,
  `services/deployment/`, relevant `lib/crowdb-console-shared/src/config.rs` and
  `deployment/`, monitor credentials/launch only when needed.
  Exit: configured origin only, no browser bearer/arbitrary URL; missing/unready/
  unauthorized are distinct; readonly blocks direct API mutations; managed mode
  supports Dataset data operations without enabling topology mutations.
- [ ] **U3 Add domain and typed frontend adapter**: Dataset enum/module/route,
  header order, embedding whitelist/initialDomain, view mount and state provider.
  Define API DTO parsing from R237 and keep exact integer/opaque IDs as strings.
  Files: UI `types.ts`, `App.tsx`, `embed.ts`, `shell/domainTabs.ts`,
  `contexts/{DomainContext,SelectionContext}.tsx`, new `views/DatasetView.tsx`,
  `dataset/{api,types,state,navigation}.ts(x)`.
  Exit: seven actual domains in correct order; preexisting Capacity alias still
  works; no duplicate connection dialog or styles outside Console scope.
- [ ] **U4 Build lazy catalog and Snapshot forest**: root lists authoritative
  Datasets; expand Dataset roots then declared child Snapshots independently.
  Include a paged Splits group per Snapshot with selected revision and history.
  Root/Dataset summaries use supplied identity/head/count only. Direct links reveal
  bounded ancestor context; prepared candidates remain in operation progress,
  not readable tree branches. Files: new `dataset/{DatasetTree,CatalogContent,
  SnapshotSummary,RevisionPicker}.tsx`, typed query hooks.
  Exit: SDK-created old/new resources and off-latest branches discoverable; no
  descendant prefetch, fake totals or parent inference from ID sorting.
- [ ] **U5 Build combined matrix and properties**: Sample ID sticky column, field
  name/requiredness headers, paged rows/columns and metadata-only cells. Header,
  row and cell clicks produce distinct structured properties. Display inline/
  chunk/tombstone/missing/inherited states, exact length and declared digest.
  Snapshot shows all Samples; named Split uses scoped membership with same Schema.
  Files: new `dataset/{SampleMatrix,FieldProperties,SchemaProperties}.tsx`,
  `dataset/useSampleWindow.ts`, tests.
  Exit: zero-field and wide Schema, UTF-8/binary IDs, empty Split, removed Samples,
  inheritance/tombstones and API partial/limit states render accurately. Browsing
  transfers no payload bytes or whole Manifest; field checksum is labelled declared.
- [ ] **U6 Add navigation, limits and explicit preview**: capture bounded visit
  identities/windows/scroll, abort obsolete queries, suspend hidden observation,
  retain primary success when optional metadata fails. Add text/hex Preview action
  for selected cell: check complete known length <=64 KiB before transfer, consume
  verified read framing fully, then display. Unknown length must be resolved or
  rejected; never GET-and-truncate. Files: Dataset navigation/query hooks,
  shared contexts as necessary, new `dataset/FieldPreview.tsx`, proxy read stream.
  Exit: Back/Forward restores exact r2/cell, stale cursor disables Next, corrupt/
  cancelled transfer discards output, keyboard/resize do not reset selection.
- [ ] **U7 Implement local import and data publication wizard**: root Create
  Dataset, Dataset Import/New Snapshot, Snapshot Create child and Split Create
  child use one stateful flow. Stages: choose Dataset/source -> Schema/member edits
  -> map local data -> upload -> optional Split definitions -> validate/review ->
  Prepare -> Publish. Separate server draft revision from frozen candidate.
  Files: new `dataset/import/{ImportDialog,SourceStep,SchemaStep,MappingStep,
  UploadStep,ReviewStep,PublicationStep}.tsx`, hooks/reducer and ResourceActions
  integration. Exit: arbitrary historical/parentless source, All/Selection/
  S1-r2-Split source, additions/removals/tombstones and inherited required fields
  reach real authority with complete membership; never submit a browser delta as
  a complete Manifest. Samples/Splits freeze together for initial publication.
- [ ] **U7a Specify and validate local mapping format**: use version 1 JSON envelope
  with Schema and Sample/Field mappings aligned to SDK data model; field source is
  inline canonical base64 or local file reference. Sample IDs use canonical base64
  with explicit UTF-8 helper input in UI. A file reference matches a user-selected
  file token/path relative to selection, never grants arbitrary disk access.
  Define missing/duplicate/ambiguous file errors; reject unsupported versions,
  duplicate IDs and fields before uploads. Preserve user input on validation.
  Files: new `dataset/import/mapping.ts`, fixtures/tests and UI user docs.
  Exit: ordinary few-file form works without a mapping file; batch JSON is bounded
  at 8 MiB and produces paged server edits, not an unbounded local registry.
- [ ] **U7b Transfer and recover uploads**: bounded incremental file digesting and
  chunked upload, at most 4 MiB per part/10000 parts, default two concurrent part
  requests and four-part retained-buffer cap. Honor stricter server admission.
  Show bytes/part/completion states and exact errors. Reuse immutable receipts on
  identical retry; reselected files must match length/digest before continuing.
  Files: new `dataset/import/{upload,digest,recovery}.ts`, worker if needed,
  proxy streaming, unit tests. Exit: memory does not grow with file size; reload
  recovers server IDs, never assumes lost File handles or changed bytes are valid.
- [ ] **U7c Handle prepare/publish conflicts and cleanup**: persist bounded recovery
  identities; use operation status for Outcome unknown. Head conflict presents
  observed change and explicit reviewed new attempt on same candidate; never
  silently switches parent or repeats completed publication. Close does not mean
  abort; explicit abandon shows claimed-upload/candidate cleanup progress.
  Files: wizard reducer/recovery components and HTTP adapter.
  Exit: dropped responses, reload, stale head, edit-after-prepare and cancellation
  each have deterministic state, input preserved and no duplicate Snapshot.
- [ ] **U8 Implement independent Split revision editor**: set-level draft supports
  create/edit/rename/remove, explicit IDs/rules, seed/weight partition, independent
  overlap/coverage settings, inherited/frozen vs reevaluated membership. Preview
  uses revision-bound server work and invalidates after edit. Whole-set review ->
  prepare -> publish sends expected Split revision, not Dataset head.
  Files: new `dataset/splits/{SplitEditor,DefinitionEditor,PolicyFields,
  SplitReview,RevisionHistory}.tsx`, reducer/hooks/tests.
  Exit: r2 publishes on S1 without S2 or latest movement; concurrent r1 edit
  conflicts with recoverable input; deleting last Split creates empty r2 and r1
  remains readable. Reuse editor for initial Splits in U7 without duplicating rules.
- [ ] **U9 Add Snapshot lifecycle controls and capabilities**: Set stable,
  Retain/Release/Reclaim with precise targets, actual protection reason, pending
  status and reconciliation. Copy/reference actions keep complete identities;
  readonly/managed behavior enforced through both UI and API. Files:
  `dataset/ResourceActions.tsx`, properties/dialogs, proxy tests.
  Exit: no unsupported Dataset delete or Split-history purge; stale eligibility
  never overrides backend checks; optional failures preserve primary view.
- [ ] **U10 Run real acceptance and finalize docs**: complete test matrix below,
  record timing/artifacts, update Console architecture/UI contract/domain counts/
  8x test ownership and local mapping examples. Update Dataset integration docs
  only where UI configuration affects them. Files: Console design docs, API-prefix/
  embedding docs, `e2e-test/README.md`, backlog index. Exit: every R236 Acceptance
  bullet verified; then remove completed backlog/plan per requirement workflow.

## UI state transitions to assert

- Import: Editing -> Uploading -> Validating -> Ready to prepare -> Prepared ->
  Publishing -> Completed. Error retains stage-specific inputs; unknown outcome
  reconciles to authoritative Prepared/Completed/conflict. Editing frozen content
  creates a new operation, never reuses its token with divergent bytes.
- Split edit: Editing(base r1) -> Evaluating(draft revision) -> Prepared ->
  Publishing(expected r1) -> Current r2. Stale r1 stays a conflict; no last-writer-
  wins action or automatic rebasing. Explicit review opens a new edit/attempt.
- Create child from Split: pin S1/r2/name once -> draft S2 data -> review inherited
  memberships intersected with S2 -> publish S2 with initial revision. Source r3
  updates may show a notice but cannot change the draft's inputs.
- Resource load: loading/ready/empty/partial/stale/unavailable/limit are explicit.
  An old resource requiring metadata preparation has actionable progress, not an
  endless spinner or empty result. First-page refresh resets incompatible cursors.

## Tests, owners and acceptance trace

### Component and unit tests

- U3: `domainTabs`/Header, `App.test.tsx`, Domain/Selection context tests and
  `test/embedding-contract.test.tsx`: domain order, module exclusion, prefixes,
  independent state, readonly visibility and Capacity compatibility.
- U4-U6: new Dataset component tests: independent lazy branches, binary ID display,
  20x12 windows, Schema/header vs row/cell selection, stale responses, hidden polling,
  explicit preview integrity/size, revision-aware history and focus restoration.
- U7/U8: pure reducers/mapping/transfer tests for invalid inputs, bounded buffers,
  revision invalidation, unknown outcomes, identical retries and source pinning.
  Do not assert only implementation structure; assert user-visible state and
  forbidden network/payload operations. Mutation success requires native tests.

### Console/API integration

- New `app/crowdb-web/tests/dataset_proxy_test.rs`: fixed origin, capability/errors,
  server bearer not returned to browser, streaming/cancel, prefix and readonly
  enforcement. Extend `access_routes_test.rs`, deployment/config tests and
  `managed_mode_test.rs` only for Dataset-specific behavior.
- Verify route method policy against actual v2 draft/status methods; never treat
  every POST as identical (read requests may be POST) or let generic proxy bypass
  readonly writes. No request-selected host redirects or credential leakage.
- Cross-check API request counters for one list page and preview. UI paging must
  not trigger implicit full authority scans, full Manifest reads or read-plan acks.

### Native browser E2E

All names below are planned new owners, not runnable until U1 registration.
Use existing console.operations dispatch; register fixtures/scopes explicitly
if adding a narrower scope. Keep one worker per mutable runtime and installed
system browser only. Seed data with API where the UI action is not under test.

- `console::browser_dataset::browse` / `80-dataset-browse.spec.ts`: discover
  SDK-created Dataset; forest with siblings and stable/latest on different branches;
  lazily page children/Splits; inspect wide matrix, binary IDs, inheritance and
  tombstones; restore scope and scroll after switching to S3/Iceberg/Back.
  Covers U3-U6 and R236 navigation/inspection/bounded observation assertions.
- `console::browser_dataset::import` / `81-dataset-import.spec.ts`: UI creates
  Dataset; maps local inline/payload fields; uploads >one part; defines initial
  Splits; prepares/publishes; verifies actual SDK-read bytes. Then derives child
  from historical parent/Split and checks membership and latest. Covers U7/U8
  local flow, real durability, full membership and initial revision visibility.
- `console::browser_dataset::split_revisions` / `82-dataset-splits.spec.ts`:
  two editors of r1, publish r2, stale conflict, same data Snapshot/latest, r1 read
  remains valid; rename/remove/empty set, partition/policy validation, new child
  from frozen r2 while r3 appears. Covers revision isolation and provenance.
- `console::browser_dataset::recovery` / import/split specs: controlled native
  response-loss/failure fixture, reload after upload/Prepare, reselect file, resolve
  outcome by operation ID, explicit retry/abandon. Ensure only one candidate/new
  revision; retained bytes remain available and partial payload is never success.
- `console::browser_dataset::capabilities` / browse/lifecycle tests: readonly
  direct API rejection, managed data actions, listener outage, unsupported v2,
  reclaim protection and pending cleanup. Destructive checks last. Covers U2/U9.
- Reuse shell `00/01` and cross-function `91-navigation-state` regression owners
  rather than repeating all legacy dialogs. Record exact existing test baselines;
  new tests record first passing baseline with date and no invented previous time.

## Fixture and runtime requirements

- Use one native three-node storage fixture with Dataset listener enabled and
  existing real KV/Chunk-KV/ChunkDB/DiskIO backing. Dataset and S3/Iceberg protocols
  share configured cluster identity; data operations never provision a second
  hidden cluster. Container ports from inspected node IPs and fixed reservations.
- Dataset fixture builders produce 21+ Samples, 13+ fields, multiple roots/siblings,
  two Split revisions, empty Split, inline/tombstone/inherited/chunk values and
  non-UTF8 IDs. Use modest data sizes except explicit bounded-transfer cases.
- API seeding is allowed for browse tests; it does not replace UI upload, prepare,
  publish, revision-edit or child-source actions in their dedicated tests.
- Follow `stepTimer`, 3-second assertions/10-second election and 100 ms polling;
  no fixed sleeps, screenshot-only acceptance, blanket retries or larger default
  deadlines. Print failing case/error and retain full service logs/artifacts.

## Consolidated files

- Console backend: `app/crowdb-web/src/access*`, services Access/deployment/config,
  scoped console-shared/monitor credential wiring and crate tests.
- Frontend: `types.ts`, `App.tsx`, `embed.ts`, shared shell/contexts, new
  `views/DatasetView.tsx` and `dataset/{api,state,navigation,tree,matrix,import,splits}`
  responsibilities with facade/children as appropriate. Keep modules near repository
  size guidelines; do not add a monolithic Workbench or second generic state framework.
- Tests: component tests alongside TS components, Rust integration in crate tests,
  e2e owned `80/81/82` flows, shared 00/01/91 regressions, scenarios/native fixtures.
- Docs: Console architecture/UI, mapping-file usage and E2E ownership. R237 owns
  Dataset persisted semantics/OpenAPI/SDK docs; modify only when an actual agreed
  contract correction is needed, not to accommodate a frontend shortcut.

## Commands and release gates

Task names verified by `pixi task list`; no gate is claimed passed during planning.
Use 60-second shell observation windows/background polling for long builds. All
executables run through Pixi. Before server-spawning tests, use clean-env only for
the owned runtime; coordinate rather than interrupt another model's active test.

```sh
pixi run test-console-ui
pixi run cargo test -p crowdb-web --test access_routes_test
pixi run cargo test -p crowdb-web --test managed_mode_test
pixi run test-console
pixi run test-container-e2e-fixture
pixi run test-container-e2e --scope console.operations --list
```

After U1 adds the owner, run the exact changed flow (repeat for each affected
registered Dataset owner, not just browse):

```sh
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::browse --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::import --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::split_revisions --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::recovery --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::capabilities --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
```

Final relevant integration/regression and static gates:

```sh
pixi run clean-env && pixi run test-container-e2e --scope console.operations --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run rs-fmt-check
pixi run rs-lint
pixi run ts-lint
```

If shared API/SDK code changes during R236, rerun its affected R237 gates too;
otherwise consume recorded passing backend acceptance. Build native image from
the actual changed checkout before browser acceptance using existing image workflow;
record image identity and source revision. An old image passing is not evidence.

## Evidence and completion checklist

- Planning: current UI spec, Iceberg patterns, domain/history, proxy, Dataset model,
  SDK and runner reviewed. Backend not implemented; H1-H4 pending.
- Baseline timings and fixture/image identities: pending.
- Component/API/native case results and artifact locations: pending.
- All task exits and upstream acceptance cases verified, modes/accessibility/
  bounds/failure recovery included: pending.
- Permanent docs updated and no dangling R237 cleanup links: pending.
- Remove completed requirement/index entry/plan only after full acceptance, under
  implement-requirement cleanup. Persistent failure follows five root-cause attempts;
  record Blocked only when its stated conditions apply.
