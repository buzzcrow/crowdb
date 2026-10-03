<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Complete Console UI Plan

Upstream: [R203](../backlog/R203-console-complete-ui.md).
Goal: seven domains with explicit scope, real operations/diagnostics, persistent
standalone bootstrap, and the same UI in Container with topology writes disabled.

## Baseline and scope

- User approved the seven-domain design and authorized planning and implementation.
  The formal UI design §§18–21 defines the expanded scope and known gaps.
- Commit each tab's verified implementation separately before proceeding to the
  next tab. Shared adapters belong with the tab that introduces them; remaining
  acceptance gaps stay explicit in this plan.
- Preserve the user's running localhost cluster and its default directory.
  Browser inspection is read-only; tests must use isolated runtime roots/ports.
- Initial Iceberg scope is Namespace/Table metadata CRUD. Row DML and object-to-
  Chunk reverse lookup remain human decisions in R203; other work proceeds.
- Existing startup/persistence edits are uncommitted and need integration/lint
  verification; three initial startup tests passed before the last scope changes.

## Phase 1 — startup and shell

- [x] **Standalone recovery**: finish atomic local persistence and process recovery,
  validate Group 0 authority and multi-node/DiskDB behavior, retain configured
  process identity. Files: `app/crowdb-web/src/{main,state,standalone}.rs`,
  `src/standalone/recovery.rs`, `src/diskdb/lifecycle.rs`, `src/mgmt/cluster_init.rs`,
  `tests/standalone_startup_test.rs`. Acceptance: A1–A5.
- [x] **Five-domain shell**: typed domain identities, per-domain scope and status,
  health/empty/error distinction, header and pane reuse. Files: `ui/src/App.tsx`,
  `types/index.ts`, `contexts/`, `shell/`, `views/`. Acceptance: A1, A6, A10, A23.
- [x] **KV/Cluster boundaries**: Cluster layout stops at physical services; KV left
  tree and init/operator scope own logical resources; guard Group 0 writes and
  demo. Files: `App.tsx`, `topology/`, `panels/KvOperatorPanel.tsx`, Web KV routes.
  Acceptance: A7–A9, A22–A23.

## Phase 2 — Chunk diagnostics

- [x] **Typed Chunk queries**: connect routed ListChunks/detail to Web, explicit
  prefix/paging and placement enrichment. Files: chunkdb client, Web Chunk routes,
  console shared ops. Acceptance: A12, A14.
- [x] **Chunk view**: list/scope sidebar, actual types and stable Strip sequence,
  Mirror/EC fragment cards, bounded rendering and cross-domain links. Files:
  `views/ChunkView.tsx`, new Chunk domain components/types/API. Acceptance: A13–A15.

## Phase 3 — native Access operations

- [x] **Access adapter**: validated deployment endpoint, capability/role handling, typed
  Iceberg REST and signed S3 requests, bounded transfer and error semantics.
  Files: new Web access domain modules, shared access operations, UI API/types.
  Acceptance: A16–A20.
- [x] **Iceberg UI**: Namespace/Table tree, metadata forms, Schema/Snapshots/Files,
  truthful capability gaps, explicit scope and demo resources. Files: new Iceberg
  view/domain components and tests. Acceptance: A16–A17, A23.
- [x] **S3 UI**: Bucket/prefix tree, paged object CRUD, bounded preview and streamed
  upload/download, multipart/cancel state and demo. Files: new S3 domain view/
  components/tests. Acceptance: A18–A20, A23.

## Phase 4 — managed deployment and verification

- [x] **Container integration**: convert confirmed managed snapshots to shared UI
  models, server-enforced capability boundaries and real logical/data operations.
  Files: Web managed routes, UI mode loader, Container web acceptance.
  Acceptance: A11, A21.
- [x] **Focused gates and commits**: measure affected baseline E2E specs before
  changes; run isolated acceptance, Rust fmt/clippy, TS lint and affected E2E;
  commit coherent verified tasks. Preserve live persistent services.
- [ ] **Permanent design and cleanup**: update Console/UI architecture for delivered
  scope. Remove R203/index/plan only after its full accepted scope is delivered.

## Seven-domain implementation sequence

- [x] **Independent domains**: split Capacity and Chunk in the enum, header,
  embedding URL parser, Sidebar/selection dispatch, and placement links. Add
  Chunk-KV as its own workbench. `domain=Chunk` now means the Chunk explorer;
  existing capacity links and fixtures must use `domain=Capacity` explicitly.
  Files: UI `types`, `main`, `App`, `shell`, `views`, `topology`, `chunk`.
- [x] **Chunk-KV catalog**: bounded catalog-page endpoint pinned to head
  generation, exact identities, unavailable states; present range map and selected
  partition artifact/overlay. Resolve placement only from matching known service
  endpoints; unmatched placement remains explicit. Head/page limits: 1/2 MiB;
  at most 100 returned partitions; five-second observation deadline.
  Files: Web `chunk_kv.rs`, UI `chunk-kv/`, `chunk_kv_catalog_test.rs`, E2E 55.
- [ ] **Chunk-KV runtime observation**: add authoritative server placement and runtime
  observations, then bounded tree/journal inspection without reading all pages.
  Files: Web `chunk_kv/`, UI `chunk-kv/`, protocol/client observation adapters.
- [x] **Paxos overview**: default KV to Group/Replica management; retain Data
  subview with explicit scope and Group 0 protection. Files: `views/KvView`,
  KV panels and corresponding topology/data E2E specs.
- [x] **Typed services**: consolidate all six service lifecycle operations in
  Cluster, eliminate non-KV fallthrough, add validated deployment forms and
  current-cluster dependency inputs. Files: Web lifecycle domain, shared launch
  adapters, `useClusterMenus`, deploy dialogs, physical server projections.
  Local deployment/lifecycle and UI dispatch are implemented and tested;
  remote auxiliary deployment is explicitly unsupported.
- [ ] **Native deployment acceptance**: exercise Chunk-KV/Access deployment
  through the new routes and close launch/publication crash-recovery gaps.
- [ ] **Chunk sources**: establish actual Repo metadata routing and implement
  source/type filters, bounded per-source cursors and partial coverage. Files:
  Web `chunk`, ChunkDB/Chunk-KV metadata adapters, UI `chunk`.
- [~] **Capacity and Access**: distinguish unknown usage, suspend inactive
  polling, connect S3 and Iceberg to the same cluster's Access deployment;
  finish bounded native file inspection and real data acceptance.
- [ ] **Integration acceptance**: scope restoration, layout-to-disk/node
  navigation, split parent/child overlays, owner movement, stale cursors, large
  populations, unavailable backend, and capability restrictions. Verify every
  visible step against the affected browser specs; commit coherent gated work.

Baseline before domain separation: shell embedding 5 tests passed; Chunk layout
2 tests passed (0.888 s and 0.814 s). Browser command:
`pixi run bash -c 'cd app/crowdb-web/ui && npx playwright test --config=e2e/realBackend.config.ts e2e/flows/00-shell-embedding.spec.ts e2e/flows/54-chunk-layout.spec.ts'`.

## Verification commands and coverage

- Unit: scope transitions, prefix/type parsing, Strip layout identity, capability
  combination, safe error/value presentation, request bodies.
- Integration: `standalone_startup_test`, `managed_mode_test`, DiskDB lifecycle,
  Chunk query adapters, Access protocol/credential/streaming paths.
- E2E: shell/embedding, Cluster physical boundary, KV init/operator, Chunk scope/
  placement, Iceberg and S3 functions, Container read-only hardware with writable
  authorized data. Use existing installed system browser.
- Commands are recorded in R203; append actual outcomes here per phase. No full
  UI suite unless specifically needed/authorized; focused specs first.

## Results

- Iceberg file-inspection checkpoint: Avro manifest lists/entries and Parquet
  footer, row-group and column metadata are rendered as fields and layouts.
  Four Iceberg browser cases passed (0.576/0.952/0.321/0.521 s), including
  exact snapshot IDs, replacement pages, no right property panel, automatic
  reader entry and separate native write-token authorization/clearing.
- Native acceptance used the user's TPC loader in the isolated persistent
  preview: TPC-H SF 0.001, eight tables. All eight chains reached their actual
  Parquet footers through snapshots, manifest lists and deflate manifests.
  Lineitem contains 6,005 rows and 16 columns; its 218,725-byte file has a
  2,319-byte footer. Inspection reported zero logical data-page bytes.
  The SDK's location without a trailing slash exposed an identity-check bug;
  authoritative Table ID now determines membership, with a regression test.
- Loader acceptance used an isolated PyArrow FileIO adapter to check exact
  object existence with a read because its default missing-path fallback uses
  unsupported directory listing. Normal credentials, upload checksums and
  footer verification were retained. A failed preflight probe was retained as
  a cleanup candidate in the loader report; this is not a fully clean loader run.
- Managed native browser flow passed in 4.5 s: KV, Iceberg create/inspect/clean,
  S3 object preview, Chunk physical placement and object cleanup. The first run
  still selected the removed Chunk subtab; the test now uses the top-level
  domain and 3-second action deadlines. Its exact leftover S3 demo was removed.
- Iceberg Rust gates: 9 Parquet metadata tests, 10 schema/statistics tests and
  2 HTTP inspection tests passed; all-target Clippy and formatting passed.
  Parent-proof rescans and independent backend column pagination remain bounded
  limitations; the TPC run is functional acceptance, not large-population proof.

- Cluster typed-service checkpoint: local CDB, DiskIO, Chunk-KV and Access
  deployment forms, stable service identities, tree/canvas/properties and
  typed stop/restart/remove routes are implemented. Lifecycle integration:
  4 passed, including a real CDB deployment against Group 0; private launch
  environment/restart: 2 passed; Access routing: 8 passed. Web/shared/Monitor
  all-target Clippy and Rust formatting passed.
- Cluster browser acceptance: 5 passed, including existing real KV deployment
  and cascade flows and the four auxiliary forms (2.4 s). Frontend TypeScript
  passed. Native Chunk-KV/Access deployment via the new forms, crash recovery
  between launch and publication, and overall topology population bounds remain
  integration work. These results do not mark the full redesign complete.

- Focused standalone/managed/DiskDB startup verification: 10 passed, including
  three-member Group 0 recovery, DiskDB replay with preserved custom ports, and
  stale local Rack cache replacement from Group 0. Invalid test profile corrected
  from `fast-test` to existing `e2e`; built missing DiskDB binary before rerun.
- Shell baseline on installed `/usr/bin/microsoft-edge`: embedding 1.6s,
  domain transition 0.372s, Docker writes 0.377s; all 5 tests passed. An initial
  attempt pointed at absent Google Chrome; no browser installation was needed.

- Initial `pixi run cargo test -p crowdb-web --test standalone_startup_test --
  --nocapture`: 3 passed; later sealed-intent/DiskDB recovery edits still need
  focused verification.
- Running UI inspected: Cluster duplicates logical Store/Group/Replica under KV
  Server; KV already uses logical navigation; Chunk currently repeats capacity
  topology. Preserve existing successful operations while unifying these scopes.

## Delivery and verification — 2026-10-03

- Five shared domains implemented. Cluster stops at services, KV owns logical
  resources, Capacity owns disks and real Chunk/Strip placement, Iceberg uses
  native Catalog metadata operations, S3 uses signed native object operations.
- Access does not currently register public origins in Group 0. Container uses
  profile-provided public origins; standalone saves validated origins as local
  launch inputs. Native credentials remain browser-session inputs.
- Recovery now confirms live KV registrations before reporting readiness. The
  first implementation exposed an unseeded discovery client; initializing the
  shared operation context fixes that without caller retries or relaxed tests.
- Final focused Rust checks: standalone startup 4, managed mode 5, Access routes
  6 passed. Earlier KV/DiskDB and Monitor focused tests also passed.
- Frontend: TypeScript lint and 89 unit tests passed. Focused shell, Cluster CRUD,
  server lifecycle, cross-jump, KV CRUD/demo, Capacity, Chunk, Iceberg and S3
  browser checks: 29 passed. Final authorization/credential-scope changes:
  shell/Iceberg/S3 7 passed.
- Real native S3 against cached MinIO: signed bucket CRUD, special object key,
  9 MiB multipart round trip and cleanup passed (2.4 s).
- Real isolated CROWDB managed service chain: authenticated KV CRUD, Iceberg
  metadata demo, S3 demo, actual S3 Chunk/Strip/disk placement and direct
  hardware/Init/Reset rejection passed (4.6 s).
- Rust fmt and Web/Monitor all-target Clippy with warnings denied passed.
- Production release Web restarted at localhost:9090. Existing KV PIDs
  68190/68329/68442 and DiskDB PIDs 68226/68365/68479 were preserved.

## Remaining acceptance work

- Docker image acceptance remains unverified: `pixi run build-single-node-container`
  built/staged the runtime successfully, then Docker Hub base-image retrieval
  failed because the configured proxy 192.168.31.238:7897 refused connection.
  The isolated host-native managed chain validates shared UI/backend behavior,
  but does not replace image packaging acceptance.
- Existing Capacity test skips its totals comparison when DiskDB does not publish
  usage in its fixture. Scanner/maintenance controls pass; that skipped condition
  is not proof of totals correctness.
- Atomic config publication failure and sealed bootstrap-intent replay now pass
  focused regressions. Extend coverage to process interruption during Group 0
  publication; not every interruption boundary has been exercised.
- Extend Chunk acceptance with multi-owner partial recovery and layout-change
  races; current coverage combines validation, a Mirror/EC UI fixture and a real
  routed S3 Chunk from the managed chain.
- Multipart part inspection now uses bounded native marker pages. Extend
  cancellation/unknown-result fault coverage before closing A19 in full.
- Keep R203 and this plan until the remaining acceptance work and the backlog's
  product questions are resolved. Do not claim all A1–A23 complete.

- Added bounded 20-strip pages with stable selected sequence, multipart part
  marker navigation, credential-change scope clearing, and native disk type
  mapping including ZoneSSD/SMRHDD. Final focused checks recorded below.

- Final new browser regressions: Chunk windowing and S3 part marker/credential
  scope checks, 4 passed; native managed chain rechecked after final assets,
  1 passed. Isolated test supervisor stopped after verification.

- Final startup tests: 5 passed, including non-writable publication preserving
  previous bytes and restart consuming sealed intent to initialize Group 0.
- Live Capacity inspection: physical disk topology survives, but Group 0's
  DiskDB instance query returns `[]`. Existing DiskDB logs repeatedly report
  `owned disk-group has no bind dg_id=1`; no runtime/config data was changed to
  conceal this condition. Native managed acceptance has valid registration/binds.
  Investigate this existing standalone deployment state separately.

## Iceberg file inspector — approved design, 2026-10-03

- [ ] **Inspection service (partially implemented)**: add authenticated, generation-bound reference
  inspection to Access table routes; reuse ManifestListReader/ManifestReader and
  footer parsing with bounded work and responses. Extend footer diagnostic fields.
  Files: Access `iceberg/inspection/`, table routes, Iceberg `file/parquet/`.
- [ ] **Explorer UI**: retain Catalog operations and replace primary JSON views
  with the reference tree, structured metadata and Parquet layout/column detail.
  Use two columns for Iceberg: no right property panel; refresh/table actions and
  column details belong in the center.
  Files: Web `ui/src/iceberg/`, `views/IcebergView.tsx`, native JSON transport.
- [ ] **Focused acceptance**: parser/integration tests, Iceberg browser spec,
  isolated TPC loader data, fmt/Clippy/TypeScript. Baseline existing Iceberg
  browser case: 0.732 s. Preserve the persistent localhost deployment.
- [ ] **Document and commit**: update Console UI architecture and record results;
  keep the broader requirement/plan open for unrelated remaining acceptance.

Tests: Iceberg footer metadata tests; Access table inspection HTTP tests; Web
Access proxy tests; `e2e/flows/60-iceberg-catalog.spec.ts`; real TPC metadata chain.

- Iceberg two-column layout verified: the right panel is absent, table refresh
  and actions remain in the center. The browser spec passes both catalog and
  file-inspection cases (0.778 s / 0.969 s): exact 64-bit snapshot identity,
  100-entry replacement pages, signed continuation history, selected footer
  retained during tree paging, and credential-change clearing.
- Frontend inspection responses are capped at 4 MiB; four branch pages and
  32 previous cursors are retained. Structured collections and schema tables
  render bounded pages. Inspection admission reserves 64 MiB per parser through
  the existing atomic 128 MiB budget. Full native data-chain acceptance remains
  pending; the browser fixture is not evidence for native parser completeness.

- Fixed-cluster entry verified: opening Iceberg automatically loads Catalog and
  namespaces without endpoint/token fields or a Load action. Read credentials
  stay in Web process state, are injected only for GET/HEAD, and cannot be
  redirected by the configuration API. Native mutations retain authorization.
  Proxy tests: 7 passed. Browser cases: 3 passed (0.688/0.893/0.343 s).
  Web/Monitor all-target Clippy and frontend TypeScript passed.


## Seven-domain checkpoint verification

- Frontend TypeScript and 29 focused unit tests passed. Shell, Chunk layout, and
  Chunk-KV E2E: 9 passed. Existing Chunk cases took 0.870 s / 0.488 s, within
  baseline. New Chunk-KV cases took 0.676 s / 0.238 s.
- Capacity E2E 50–53: 14 passed. The existing datacenter totals test conditionally
  skipped its live totals comparison because its DiskDB fixture did not report
  usage; tree/Inspector navigation passed. This is not proof of live usage totals.
- Real Group 0 catalog integration passed: missing catalog, bounded 100+5
  windows, exact u64 fields, stale generation, invalid offset, corrupt checksum.
- Rust fmt and Web all-target Clippy passed after factoring the long test setup.
- Tree pages, stream extents/watermarks, multi-backend Chunk enumeration, expanded
  deployment lifecycle, Paxos overview, and same-cluster S3 setup remain pending.
- Release Web rebuilt and the existing 9090 Web process replaced; all six
  original KV/DiskDB PIDs survived. The earlier test Access binding was removed
  with a config backup. `/healthz` returns 200, and the real Chunk-KV catalog
  route returns an explicit 404 because this cluster has no initialized catalog.
  Browser inspection confirms seven visible top-level domains and that state.

## Paxos overview and fenced runtime observations

- KV now defaults to Overview; Data mounts on first use and inherits Store,
  Group and Replica selections. Ordinary Put requires an explicit group.
  Scans abort on scope/view changes; continuations cannot overwrite a later
  scope. All Groups is bounded to 10 groups and displayed rows to 1,000.
- Baseline KV topology/basic/advanced browser selection: 11 passed. Updated
  selection: 12 passed. Final shell/reconfiguration/basic/catalog selection:
  16 passed. New overview case: 0.609 s; bounded continuation/cancellation:
  6.5 s. The first bound test incorrectly exhausted its fixture after counting
  initial auto-scans; its unlimited source now tests the actual display cap.
- Chunk-KV owner observation verifies catalog generation, owner epoch,
  partition/tree/stream identity and returns independently sampled runtime
  counters. Web accepts only a matching configured Chunk-KV RPC/HTTP mapping,
  caps owner responses at 64 KiB and rechecks Group 0 generation. No new locks,
  key scans, data-page reads or journal reads are introduced.
- Service management tests: 3 passed, including durable/applied progression
  after a real journal mutation and Serving without a live authority grant.
  Web Group 0 integration passed missing endpoints, stale epoch, wrong owner,
  wrong stream and oversized owner responses. Updated Chunk-KV E2E: 2 passed
  (0.771 s / 0.208 s). TypeScript, focused selection/topology unit tests,
  Rust fmt and Web/Chunk-KV all-target Clippy passed.
- Runtime page/tree/extent inspection and discovery of management origins from
  managed deployments remain open. The catalog/runtime observation is not a
  claim of complete Chunk-KV diagnostics or a completed seven-domain rollout.
