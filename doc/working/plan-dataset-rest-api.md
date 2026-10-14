<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Dataset Public REST API Plan

Upstream: [R231](../backlog/R231-dataset-public-rest-api.md),
[Dataset specification](../design/access-server/dataset/spec-crowdb-dataset.md),
[SDK execution plan](plan-dataset-sdk.md).

Goal: provide the complete, bounded HTTP Dataset path consumed by the Rust SDK.

Status: implementation resumed at the user's request on 2026-10-14.
Public contract and control foundations are verified. Payload/read-session work
is pending. Paths below
include both existing drafts and proposed implementation locations. This temporary plan is
removed when R231 is verified and cleaned up; the SDK plan remains until R232
is complete. There are no benchmark drivers, load tests or performance gates.

## Starting state before resumption

These historical notes describe the inherited drafts. The verified implementation
section below supersedes their compilation, contract and route status.

- The user wants both the real Dataset service and its SDK to work together,
  with API design covering Dataset usage rather than mechanically wrapping the
  old listener. The latest instruction is to finish plans/constraints and let
  another model implement. Implementation has now resumed; the handoff notes
  below describe the starting state, not evidence of delivery.
- Preserve the working tree. Nothing had been committed at resumption.
  Existing drafts are implementation attempts, not accepted wire contracts.
  Reconcile them with phases 1–2 before extending them. Do not hard-reset,
  restore an older revision or discard the drafts without user authorization.
- Started: `Dataset = 7` in protocol and ChunkDB conversions; publication code
  extracted to `authority/publication.rs`; new prepare/status methods and one
  test; draft `upload.rs`, `upload/parts.rs`, `payload.rs` and dependencies.
- Not started: public OpenAPI, JSON REST routing, production upload/reader
  wiring, independent read-session API, full upload recovery/GC, container SDK
  acceptance. The new SDK draft currently has no matching server routes.
- Verified commands: `pixi run -- cargo test -p crowdb-protocol --test
  chunkdb_wrappers_test` (9 passed), `pixi run -- cargo test -p crowdb-protocol
  --test dataset_chunk_type_test` (1 passed), `pixi run -- cargo test -p
  crowdb-access-dataset --tests` (38 passed, including the new preparation
  case), and `pixi run -- cargo check -p crowdb-access-dataset` (passed after
  adding upload/payload modules). These are focused checks, not acceptance of
  all current changes. No fmt/clippy gate or new service E2E has run.
- Draft upload hazards requiring explicit resolution: no durable pre-allocation
  write intent, no expiry scanner, no Snapshot-scoped reference release after
  `Claimed`, and no verified physical reclamation completion. A lost metadata
  CAS response cannot justify deleting bytes merely because one later read
  does not yet observe them. Treat reclaim outcomes such as Deferred honestly.
- Draft performance choices are provisional: all upload parts currently use the
  shared-object writer, and complete rereads all parts for the whole digest.
  Integrate the established large-write path where appropriate and document
  integrity passes; do not present a simple part implementation as a proven
  large-payload strategy. No performance measurements are required now.
- The draft SDK proposes JSON batch metadata plus binary field-part retrieval,
  whereas the earlier plan mentions binary batch frames. Resolve this in the
  public contract before adding routes: preserve bounded batch transfer and
  avoid mandatory per-field/per-tiny-part round trips. Update R231/R232 and both
  plans coherently if the chosen wire representation changes.

## First milestone verification — 2026-10-14

- Removed the obsolete Dataset TODO; R220 and the two current requirements/plans
  track deferred and active work.
- Added public OpenAPI and frozen JSON/binary fixtures. Binary batches use one
  response with frame digests, field digests, explicit ends and terminal errors.
  SDK decoder tests cover every transport split and reject truncated/corrupt data.
- New authenticated JSON routes support Dataset create/open, inline
  prepare/publish, operation/Snapshot/Manifest inspection, stable and retention.
  Upload, batch delivery, read sessions and physical reclamation are not wired.
  Payload references return unsupported rather than claiming persistence.
- Legacy routes remain in `dataset/legacy.rs`; modern API accepts explicit
  Dataset identity within the default namespace. Keep-alive/header timeout and
  bounded shutdown are enabled. JSON body limits apply while receiving; response
  byte ownership retains admission through HTTP delivery. Lifecycle testing of
  streaming payload leases remains pending.
- Fixed `get_dataset` to read current head bindings. True prepare/status were
  extracted into publication modules; test covers interrupted candidate write,
  invisible preparation, recovery and the same published ID. Tests/helpers live
  in `tests/publication_test.rs` and `tests/common/`.
- The standalone Rust control consumer successfully created/prepared/published
  and retrieved an inline Manifest through production HTTP handlers. This
  integration fixture uses a test metadata store and is not the required
  real-Chunk-KV/chunk container acceptance.
- Verified: Dataset crate 39 tests, HTTP contract 2 tests, standalone SDK control
  consumer 1 test; root fmt/clippy, SDK boundary/CI reachability checks. No
  container payload acceptance or benchmark has run.
- Payload adapter investigation found the existing `SmallWriteIntent` and
  `finish_durable_with_intent` hook. Use it to persist exact locations before
  shared-object disk writes; do not implement a second physical journal. The
  earlier upload draft is still unverified and rewrites a growing part list;
  replace this with bounded immutable per-part records and explicit recovery.

## Current implementation — continued verification

- Real Dataset chunk adapters, durable per-part intents, paged part receipts and
  ownership recovery are implemented. JSON upload/publication, bounded binary
  delivery, scoped read plans and management routes compile and pass focused
  component tests. Real container acceptance is pending.
- Read selection IDs are immutable 1024-ID pages; cursor/acknowledgement is a
  separate small CAS record. HTTP streams retain scoped leases and byte credits,
  refresh activity, and cancel independently. Reclaim fences read/publication
  admission and retains aborted operation metadata for crash-safe cleanup.
- Rust file transfer uses bounded configurable concurrent parts and one batch
  stream for download. HTTP pooling, explicit safe retries, refreshable credentials,
  TLS roots, transfer deadlines and recoverable mutation identities are implemented.
- PyO3/maturin source and wheels build independently outside the repo. An installed
  Linux CPython 3.14t wheel passed five Python lifecycle/iteration checks without a
  Rust compiler in its consumer PATH. Real-service Python acceptance is pending.
- Dataset component tests: 43 passed; production HTTP/control/payload consumer
  tests: 4 passed. Root and SDK lint/fmt verification remains active after changes.
- The default rootless OCI builder is missing host newuidmap/newgidmap; sudo is
  unavailable. Use the same staged release Dockerfile through the available Docker
  builder, retaining source/runtime labels and the existing fixture source checks.
  This is a build prerequisite workaround, not a replacement Dataset service.

## Required use cases and ownership boundaries

- Dataset import: create/open -> upload bytes -> build validated Manifest ->
  prepare -> publish -> resolve latest. All steps work remotely over HTTP.
- Incremental import: declare parent -> add/replace/tombstone fields or samples
  using specification semantics -> publish child. Preserve inherited data and
  explicit ancestry; verify the core's current deletion semantics before
  exposing SDK convenience methods.
- Training: resolve one Snapshot -> select/project/shuffle -> partition by
  rank/worker -> deliver bounded batches -> explicitly acknowledge -> resume.
  New latest must not alter an active plan, and filtering must precede payload
  retrieval. Never claim exactly-once application processing from an SDK cursor.
- Random access: ordered get-sample/get-batch and large-field streaming with
  explicit integrity behavior. Missing fields, tombstones, missing samples and
  failed checksums must remain distinguishable.
- Checkpoint as remote Dataset data: upload declared components, then publish
  one complete Snapshot. Prepare/part completion is not remote checkpoint
  commitment. Framework serialization, coordinated rank assembly and local
  disk staging remain adapter/future work; do not add them to the first SDK.
- Lifecycle: inspect Snapshot/Manifest, pin stable, retain/release, close reads,
  resume after restart and reclaim only unowned Dataset data. External source
  references never transfer ownership of S3 or Iceberg data to Dataset.

## Non-negotiable implementation constraints

- One published Dataset contract for HTTP, Rust SDK and Python wrapper. Define
  operation identity, durability, retry and cancellation semantics before
  exposing convenience APIs. Do not change persisted formats accidentally.
- Real chunk-backed payloads and dedicated Dataset chunk identity; no in-memory
  storage substitution, fake success or S3/Iceberg chunk-type reuse in delivery.
- Reuse connections; bound requests, metadata, upload windows and response
  bytes before allocation. Carry credits/leases through actual delivery, not
  just response construction. No whole-file buffering on the large-data path.
- Do not force new TCP/TLS connections per operation, duplicate full-file
  conversions, serialize every request behind a global lock or fetch payloads
  that worker partitioning will discard. Correctness-required checksum work
  remains enabled; a low-copy claim must have explicit owner lifetimes.
- Hot paths stay lock-free. Before introducing a lock, raise contention,
  ordering and complexity trade-offs with the user. Keep unsafe confined to
  approved existing boundaries; raise any new exception before adding it.
- No benchmark implementation, high-load runs, hardware throughput targets or
  latency gates. Functional tests of bounds, reuse, cancellation and concurrency
  are in scope and must use deterministic coordination where possible.
- Do not silently expand into GPU, RDMA, fat clients, cache/staging, new namespace
  administration or a rewrite of unrelated storage infrastructure.

## Phase 1 — Freeze the public wire contract

- [x] **Operation mapping**: map every R231 operation to an authority method,
  route, request/response schema and error. Use `/api/dataset/v1` with explicit
  namespace/Dataset identity. Separate JSON control from binary payload bodies;
  preserve `/v1/dataset/*` fixtures. Files:
  `doc/design/access-server/dataset/openapi-crowdb-dataset-v1.yaml` (new),
  `app/crowdb-access-server/src/dataset.rs`.
- [x] **Wire boundary**: specify string IDs, sample-ID byte encoding, opaque
  cursor/upload references, digest algorithms, version rejection, pagination,
  path escaping and numeric bounds. Define binary batch frame version, header
  size, payload length, sample/field ordering, integrity scope and final-frame
  completion; reject truncation before reporting a successful value. Do not
  expose internal bincode layout or base64 large payloads. Files: OpenAPI,
  `lib/crowdb-access-dataset/src/wire.rs`, new
  `app/crowdb-access-server/src/dataset/api.rs` and `dataset/framing.rs`.
- [x] **State transition contract**: enumerate upload create/part/complete/abort,
  prepare/publish/status, and read fetch/acknowledge/resume/close outcomes.
  Require stable tokens for retried mutations. Define matching-content identity
  using canonical validated fields, not incidental JSON formatting. Specify
  expiry, stale progress, already-completed and ambiguous-response outcomes.
  Files: OpenAPI, `lib/crowdb-access-dataset/src/publication.rs`,
  `lib/crowdb-access-dataset/src/cursor.rs`.
- [x] **Protocol fixtures**: add bounded JSON and binary golden fixtures for
  success, malformed data, version mismatch and logical errors. Share these
  with SDK tests without a production path dependency on the server. Files:
  new `sdk/contract-fixtures/dataset-v1/`,
  `app/crowdb-access-server/tests/dataset_contract_test.rs`.

Exit: request/response shapes and transition outcomes are concrete enough for
SDK implementation; remaining discovery must update R231 before broadening it.

## Phase 2 — Chunk identity and durable payload storage

- [ ] **Dataset chunk tag**: add `Dataset = 7` after confirming that tag remains
  unused. Update FlatBuffers, Rust conversion macros, ChunkDB client/server
  adapters and all exhaustive matches. Keep existing tags unchanged and verify
  unsupported peers reject the new tag rather than substituting another type.
  Reuse normal generated-code tooling. Files:
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
  `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/wire.rs`, affected tests.
- [~] **Payload adapter**: introduce Dataset-owned streaming write/read/reclaim
  operations around `crowdb-chunk-client`. Reuse small-object durable completion
  and large streaming writers; use the chunk reader's `read_stream` or
  `read_range_stream` instead of collecting a file then slicing it. Locator
  encoding/decoding stays in this adapter. Audit existing ownership/reclamation
  APIs before adding wrappers. Files:
  `lib/crowdb-access-dataset/src/chunk_store.rs`, new `payload.rs` and
  `payload/chunk.rs`, `lib/crowdb-access-dataset/Cargo.toml`.
- [ ] **Upload records**: persist Dataset-scoped session, immutable part
  identities, accepted lengths/digests, completion and expiry state in
  `DatasetStore`. Recover part retries without new logical parts; record write
  intent before allocating bytes where crash recovery needs it. Fence concurrent
  complete/abort/expiry and define cleanup of allocated but unpublished bytes.
  Files: new `lib/crowdb-access-dataset/src/upload.rs` and `upload/`,
  existing `key.rs`, `record.rs`, `store.rs`.
- [ ] **Payload ownership**: bind completed references to Dataset identity and
  preserve them until publication or expiry. Validate reference ownership and
  integrity during preparation/publication; coordinate publication against
  cleanup using durable state. Shared and parent-reachable payloads must survive
  reclaim. Files: `upload/`, `authority.rs`, `retention.rs`, `payload/`.
- [ ] **Production wiring**: replace the default unavailable reader in the
  Dataset service startup with the real adapter and writer resources. Reuse
  configured discovery, placement and lifecycle facilities. Keep legacy inline
  support. Files: `app/crowdb-access-server/src/dataset.rs`,
  `app/crowdb-access-server/Cargo.toml`, relevant access configuration modules.

Exit: real Dataset-owned bytes survive restart, are readable by opaque reference
and cannot be reclaimed while owned; tests use actual Dataset chunk allocation.

## Phase 3 — Publication and HTTP write path

- [x] **Preparation separation**: factor the oversized `authority.rs` along
  publication responsibility while adding a true prepare operation and operation
  lookup. Prepared candidates stay invisible; publish reuses their Snapshot ID.
  Avoid unrelated authority refactoring. Files: `authority.rs`, new
  `authority/publication.rs`, existing `publication.rs`, `record.rs`, `key.rs`.
- [ ] **Publication recovery**: cover failure after each durable transition,
  lost responses, retry after restart and competing head updates. Define which
  operation outcome is returned if a published record exists but head advance
  did not complete. Preserve existing persisted records or introduce explicit
  decoding/migration; do not silently change bincode struct layouts. Files:
  `authority/publication.rs`, `lib/crowdb-access-dataset/tests/`.
- [ ] **HTTP upload and control**: implement authenticated create/open,
  upload/status/complete/abort, prepare/publish/status, snapshot inspection and
  name resolution using the same authority. Bound incoming bodies while reading;
  return stable structured errors. Keep route modules scoped by responsibility.
  Files: `dataset.rs`, new `dataset/upload.rs`, `dataset/snapshot.rs`,
  `dataset/error.rs`, `dataset/api.rs`.
- [ ] **First SDK round trip**: connect SDK plan phases 1–3 to the container
  fixture: create, stream a file larger than a transfer window, prepare,
  inspect unchanged latest, publish and retrieve identical bytes. Include one
  restart. This is functional E2E, not a throughput test. Files:
  `e2e-test/access_dataset_fixture.py`, `e2e-test/tests/access_dataset.rs`,
  `e2e-test/scenarios.py`, SDK client fixture from the SDK plan.

## Phase 4 — Read sessions, bounds and lifecycle

- [ ] **Independent read handles**: replace service-global user cancellation
  with read-scoped handles. Resolve mutable names once; bind Snapshot,
  selection/projection/order/shuffle and worker partition into plan identity.
  Preserve whole-service shutdown as an internal lifecycle action. Files:
  `lib/crowdb-access-dataset/src/transport.rs`, `planner.rs`, `lease.rs`,
  `adapter.rs`, new `read_session.rs`, server `dataset/read.rs`.
- [ ] **Acknowledgement and resume**: separate batch fetch from acknowledged
  cursor persistence. SaveProgress must not fetch another batch. Make repeated
  acknowledgement idempotent, reject stale/divergent progress, and define
  resume after expiry. Partition selection before payload fetch. Files:
  `cursor.rs`, `read_session.rs`, `authority.rs`, `transport.rs`, `key.rs`.
- [ ] **End-to-end streaming**: add a streaming response path instead of
  forcing large fields through `SampleView.values: Vec<u8>` and `Full<Bytes>`.
  Retain request/byte credits and active-read protection for response lifetime;
  release on completion, error, dropped response and cancellation. Whole-value
  convenience calls must reject oversize input or use explicit bounded limits.
  Files: `chunk_store.rs`, `transport.rs`, `delivery.rs`, server
  `dataset/read.rs`, `dataset/framing.rs`.
- [ ] **Connection lifetime**: enable keep-alive with bounded idle timeout and
  connection admission. Ensure idle connections leave work capacity available.
  Drain or cancel within a bounded shutdown deadline. Check keep-alive reuse
  with a deterministic connection-count test; do not impose speed thresholds.
  Files: `app/crowdb-access-server/src/dataset.rs`, relevant configuration/tests.
- [ ] **Complete management surface**: implement bounded snapshot/Manifest
  traversal, stable, retain/release and reclaim/status. Validate Dataset scope
  on every operation and preserve ownership across active reads and restarts.
  Files: `authority.rs`, `retention.rs`, server `dataset/snapshot.rs`.

## Phase 5 — Verification and requirement closure

- [ ] **Unit cases**: validate wire fixtures, type tags, part identity, digests,
  version rejection and cursor binding. Files:
  `lib/crowdb-protocol/tests/`, `lib/crowdb-access-dataset/tests/`.
- [ ] **Integration cases**: exercise publication/expiry races, lost responses,
  restart recovery, isolated cancellation, authentication, streaming bounds and
  connection reuse. Use gates/fault injection rather than performance timing.
  Files: `lib/crowdb-access-dataset/tests/`,
  `app/crowdb-access-server/tests/dataset_*_test.rs`.
- [ ] **E2E cases**: register real chunk payload, upload recovery, SDK round trip
  and active-read reclaim protection in the existing Dataset container domain.
  Print failing case/error and retain service logs. Files:
  `e2e-test/tests/access_dataset.rs`, `e2e-test/access_dataset_fixture.py`,
  `e2e-test/scenarios.py`.
- [ ] **Document and close**: update the normative spec/design and stale
  Dataset implementation claims only after acceptance is verified. The old
  `dataset-todo.md` was removed because its only remaining item is covered by
  R220/R231/R232; this plan and the SDK plan own remaining implementation work.
  Remove R231 and its index entry and this plan in requirement cleanup. Keep
  R232 pending until SDK-specific acceptance completes. Files:
  `doc/design/access-server/dataset/`, `doc/backlog/backlog.md`, working docs.

## Consolidated file scope

- Wire/protocol: `lib/crowdb-protocol/`, ChunkDB RPC adapters and tests.
- Authority/payload: `lib/crowdb-access-dataset/` and its tests; reuse
  `lib/crowdb-chunk-client/` interfaces without speculative storage rewrites.
- HTTP: `app/crowdb-access-server/src/dataset.rs`, proposed `dataset/` modules,
  configuration, dependency manifest and component tests.
- Contract/E2E: Dataset OpenAPI/spec/design, shared SDK fixtures,
  `e2e-test/access_dataset_fixture.py`, `tests/access_dataset.rs`, `scenarios.py`.

## Execution and gates

- Resume by inspecting the handoff draft and finishing phase 1. Work sequentially
  through phases; the linked SDK plan may scaffold after
  phase 1, but SDK release waits for all R231 acceptance cases. No delegation
  is required. Implementation is active.
- During implementation use `/implement-requirement` and `/coding`; commit
  coherent gated changes. Preserve existing work, keep one active checkbox,
  and record verified steps. New unsafe exceptions or locks follow AGENTS.md.
- Run commands through Pixi, default shell timeout 60 seconds. Start long
  builds/tests asynchronously and poll while preserving complete output.
- Relevant commands: `pixi run -- cargo test -p crowdb-protocol`,
  `pixi run -- cargo test -p crowdb-access-dataset`,
  `pixi run clean-env && pixi run test-access`,
  `pixi run clean-env && pixi run test-e2e-dataset`,
  `pixi run rs-fmt-check`, `pixi run rs-lint`.
- Add affected ChunkDB/client tests when changing enum conversions; run changed
  C++ format/tree-lint only if C++ changes prove necessary. Follow requirement
  retry rules; no benchmark or latency/throughput gates are introduced.
