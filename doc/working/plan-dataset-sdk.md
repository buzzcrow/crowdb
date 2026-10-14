<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Dataset SDK Plan

Upstream: [R232](../backlog/R232-sdk-dataset-http.md),
[R231 REST prerequisite](../backlog/R231-dataset-public-rest-api.md),
[REST execution plan](plan-dataset-rest-api.md).

Goal: deliver an independent Rust Dataset HTTP SDK and Python wrapper over the
same implementation, with efficient resource ownership and functional coverage.

Status: implementation resumed at the user's request on 2026-10-14.
Rust workspace, HTTP core and control consumer foundations are verified;
full payload/session/binding delivery remains pending. No benchmark work started.
Paths below include both draft code and proposed files.
This temporary plan is removed after R232 acceptance and requirement cleanup.

## Starting state before resumption

These historical notes describe the inherited drafts. The verified implementation
section below supersedes their compilation, contract and route status.

- Read the [REST plan starting state](plan-dataset-rest-api.md) and both upstream
  requirements before coding. The user requested a complete service plus SDK,
  then stopped implementation for a handoff; implementation has now resumed.
  Preserve all uncommitted work; no commit or publish action had occurred at resumption.
- Current draft: `sdk/Cargo.toml`, `crowdb-sdk-core` HTTP/error code,
  `crowdb-sdk-dataset` models/client/file-transfer code, and exclusion of `sdk`
  from the root workspace. The SDK has not been compiled or tested; there is
  no SDK lockfile, Python extension, wheel, public OpenAPI or matching new
  server route implementation yet. Existing Python iterable is unchanged.
- Do not mistake draft defaults for design decisions: hard-coded 60-second
  timeout, 8-MiB response limit, 1-MiB file parts and sequential transfers must
  be reconciled with configurable deadlines/budgets and concurrent APIs.
  There is no SDK-level admission budget, retry policy or async Python wrapper
  implemented yet. No claim of performance readiness is justified.
- The draft file uploader reads once to compute MD5 and again to transfer;
  draft server completion also rereads stored parts. Decide and document the
  checksum contract and necessary passes across both ends. Avoid accidental
  redundant hashing/copying; never drop required integrity checks to save work.
- Draft downloads write into the destination before whole-field MD5 completes.
  Specify partial-output/error behavior; a convenience download-to-path should
  use a temporary file and publish it only after successful verification, or
  explicitly require callers to discard partial output. Caller-owned streams
  cannot be rolled back, so their contract must say so.
- Complete phase 1 gates before expanding implementation; root Cargo fmt,
  clippy and test tasks do not cover this independent workspace. Use existing
  fixture deployment for real-server acceptance, never a mock-only substitute.

## First milestone verification — 2026-10-14

- Independent workspace has a lockfile and Rust 1.85-compatible dependency
  resolution (`resolver = "3"`); root gates do not discover it implicitly.
  MSRV resolution is checked; a build on the minimum compiler remains pending.
- `HttpConfig` exposes request/connect and idle timeouts, pool size, request and
  body windows. Client clones share pool and atomic admission. JSON encoding is
  bounded before sending; cancellation returns capacity. Credential refresh,
  separate overall retry deadlines and a retry policy are still pending.
- JSON control methods match server routes. Required nullable fields remain
  required. Binary decoder owns one shared header and transfers payload frame
  owners without concatenation; field/batch completion and terminal errors are
  explicit. The bounded `get_batch` codec is ready; server delivery and the
  large-field streaming facade remain pending.
- `transfer.rs` remains an uncompiled local draft. Do not restore its old
  per-field/per-part download facade as the final API: use the single batch
  stream, configurable admission and safe destination semantics.
- `examples/control.rs` is a functional consumer, not a benchmark. It passed
  against production HTTP route handling via the independent SDK workspace.
  Real chunk container acceptance is still required before delivery.
- Added Rust SDK test/fmt/lint and dependency-boundary tasks to Pixi and CI;
  the CI reachability checker requires them. SDK tests currently include five
  HTTP/resource cases and four control/framing cases. Python bindings, wheels,
  worker iteration changes and package installation have not been implemented.

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

## Required interface and performance constraints

- Rust is the only implementation of HTTP, authentication, retries, integrity,
  upload/read state and future cache/native transport. Python calls it through
  a native wrapper; do not add Python requests/httpx transport as a shortcut.
- Rust public operations are asynchronous and support concurrent independent
  calls through a reusable client. Python exposes batch/file interfaces and
  explicit close/cancel; blocking native work releases the GIL. Runtime and
  connection pools are process-owned, not created per operation.
- Data ownership, byte credits and cancellation must span buffers retained by
  the SDK. Define which allocations belong to the caller after delivery; do
  not claim an SDK can cap memory a caller deliberately copies and retains.
- Large-file upload/download remains bounded independently of total file size.
  Do not require whole-file JSON, Python byte lists, unnecessary full-payload
  copies, or one Python/native crossing per byte or tiny frame.
- Safe retry requires the same token and replayable input; ambiguous writes
  return recoverable identity/status. Token generation happens once per
  operation, not per request attempt. Authentication/integrity failures are
  not blanket-retried. Propagate the overall deadline through retry and body I/O.
- IDs, cursor binding, prepare/publish visibility, projection, worker partition,
  snapshot retention and error classes match the server contract. Batch fetch
  and prefetch do not imply application acknowledgement.
- Benchmark drivers and measurements are explicitly deferred. Design supports
  later Rust-SDK and Python-wrapper load generators using the normal API; no
  special fast path may bypass validation, durability or integrity semantics.
- Functional tests, package installation and real-service E2E remain required.
  The product is not complete until the installed Python wrapper and standalone
  Rust consumer both use the real service to write, publish and retrieve bytes.

## Scope and execution order

- Rust owns protocol, HTTP, retry, integrity, transfer and progress state.
  Python owns ergonomic types, exceptions, iteration and framework integration.
- Use reqwest/Tokio/rustls and PyO3/maturin as selected in R232. Pin compatible
  versions after checking SDK MSRV and wheel build support; do not copy root
  server patches or upgrade unrelated server dependencies.
- REST plan phase 1 precedes wire/client implementation. SDK scaffolding can
  start then; the first real round trip follows REST phase 3, complete release
  follows REST phase 5. Work is sequential unless separately authorized.
- No benchmark executable, performance baseline, load test or optimization
  campaign. Functional tests check resource bounds and concurrency semantics.
  Local disk cache/checkpoint staging, direct chunk access and RDMA remain
  R233/R220 work. Initial SDK publication waits for remote commitment.

## Phase 1 — Independent project and build boundary

- [x] **Rust workspace**: add `sdk/Cargo.toml` and `sdk/Cargo.lock`, exclude
  `sdk` from root workspace, and introduce `crowdb-sdk-core`,
  `crowdb-sdk-dataset` under `sdk/crates/`. Introduce the non-published Python
  extension during phase 5, when its binding implementation is added. Rust
  crates declare publishable versioned
  dependencies as well as local development paths. Replicate applicable lint
  rules, including unsafe deny. Files: root `Cargo.toml`, `sdk/Cargo.toml`,
  `sdk/crates/*/Cargo.toml`, minimal `src/lib.rs` entries.
- [x] **Version and dependency isolation**: keep SDK package versions independent
  of root VERSION. Verify SDK resolution does not inherit the server Hyper fork,
  storage FFI or unpublished server crates. Preserve the existing
  `crowdb-dataset` package/import identity. Files: `sdk/Cargo.lock`,
  `sdk/pyproject.toml`, `tools/ci-checks/check-version.py` only if needed.
- [x] **SDK test gates**: introduce dedicated Pixi tasks for Rust SDK tests,
  fmt/clippy and workspace boundary checks; add Python package gates in phase 5; do not repurpose `test-access-sdk`, which
  currently runs Iceberg acceptance. Register standalone workspace coverage
  explicitly in CI/task checks. Files: `pixi.toml`, `pixi.lock`,
  `tools/pixi-tasks/test.py`, `tools/ci-checks/check-ci-test-tasks.py`,
  `.github/workflows/ci.yml`.

## Phase 2 — Public models and HTTP core

- [ ] **Public Dataset models**: implement opaque Dataset/Snapshot/operation
  IDs, upload status, publication state, read plans and errors from R231.
  Keep reqwest, internal locators and authority types out of public signatures.
  Validate user parameters without duplicating storage authority decisions.
  Files: `sdk/crates/crowdb-sdk-dataset/src/identity.rs`, `snapshot.rs`,
  `upload.rs`, `read.rs`, `error.rs`, `wire.rs` (new).
- [ ] **Core configuration**: define builder/config for endpoint, credential
  provider, TLS, connect/request/overall deadlines and in-flight byte/request
  limits. Own one reusable async client; Rust uses the caller's Tokio runtime,
  while Python runtime ownership belongs in its binding crate. Files:
  `sdk/crates/crowdb-sdk-core/src/config.rs`, `credentials.rs`, `http.rs`.
- [ ] **Safe retry boundary**: preserve operation/session/part identity across
  attempts, honor bounded backoff and one overall deadline, and distinguish
  retryable reads from ambiguous mutations. Reopen seekable file ranges for
  retry; report non-replayable streams explicitly rather than silently buffering
  them or replaying different bytes. Files: core `retry.rs`, `error.rs`,
  Dataset `upload.rs`, `snapshot.rs`.
- [ ] **Streaming primitives**: wrap owned buffers and streams with explicit
  lengths/limits. Decode R231 binary frames incrementally, reject truncation
  and digest mismatch, and expose only the integrity guarantees actually
  established for a frame or completed stream. No success on partial bodies.
  Files: core `body.rs`, Dataset `wire.rs`, `read.rs`.
- [ ] **Protocol tests**: consume `sdk/contract-fixtures/dataset-v1/`; test
  optional/required fields, versions, escaped identities, structured errors,
  credential redaction and frame boundaries. Keep fixtures test-only. Files:
  `sdk/crates/crowdb-sdk-core/tests/`,
  `sdk/crates/crowdb-sdk-dataset/tests/`.

## Phase 3 — First complete Rust client path

- [ ] **Dataset client**: expose cloneable client and Dataset-bound handle for
  create/open. Add prepare, publish, operation status and immutable snapshot
  lookup. Generate a mutation token once per logical operation and allow callers
  to retain/reuse it after process failure. Files: Dataset `client.rs`,
  `dataset.rs`, `snapshot.rs`.
- [ ] **File upload**: implement session creation, streamed part upload,
  status/resume, complete and abort. Enforce request/byte budgets before reading
  more input; propagate cancellation and preserve pending status. Accept file
  paths/owned streams and return server-issued opaque references. Files:
  Dataset `upload.rs`, `upload/`, core `body.rs`.
- [ ] **Point and streamed reads**: expose get-sample/get-batch plus bounded
  large-field streaming. Return Snapshot identity and checked data; bounded
  convenience reads cannot allocate arbitrarily large files. Files:
  Dataset `read.rs`, `read/`, `wire.rs`.
- [ ] **Real client fixture**: build a small functional consumer in the SDK
  workspace and stage it through the existing container harness, rather than
  linking the SDK into the server workspace. No benchmark loop. Exercise the
  complete upload/prepare/publish/read path and lost-reply recovery. Files:
  new `sdk/tests/consumer/`, `e2e-test/client.py`,
  `e2e-test/tests/access_dataset.rs`, `e2e-test/scenarios.py`.

Exit: a standalone Rust consumer can round-trip real payload bytes using only
public HTTP operations; preparation does not advance latest.

## Phase 4 — Complete Rust operations and read lifecycle

- [ ] **Remaining control methods**: snapshot listing/pagination, Manifest,
  latest/stable resolution, stable update, retain/release and reclaim/status.
  Keep method naming consistent across Rust and Python. Files: Dataset
  `snapshot.rs`, `dataset.rs`, associated tests.
- [ ] **Read session**: start a plan with immutable Snapshot, selection,
  projection, ordering/shuffle and worker partition; expose next batch,
  explicit acknowledgement, saved progress, resume and close/cancel. Do not
  acknowledge prefetched or merely yielded data. Files: Dataset `read/session.rs`,
  `read/progress.rs`, `read/stream.rs`.
- [ ] **Bounded delivery**: retain byte credits for queued and outstanding
  SDK-owned buffers until consumption/drop, not merely until HTTP receipt.
  Prevent deadlock when a field exceeds one configured window by streaming it.
  Cancellation drops outstanding transport work and closes the scoped read
  best-effort; server TTL handles broken connections. Files: core `body.rs`,
  Dataset `read/`, `upload/`.
- [ ] **Compatibility migration**: inventory all callers of the existing
  `DatasetHttpClient` and preserve an internal legacy adapter where needed.
  Keep server-workspace tests independent of SDK production dependencies and
  migrate external acceptance via the standalone fixture. Files:
  `lib/crowdb-access-dataset/src/client.rs`, affected tests and E2E harness.

## Phase 5 — Python wrapper and installable package

- [ ] **Extension packaging**: add the non-published `crowdb-sdk-python` crate,
  register Python package gates in Pixi/CI, and migrate `sdk/pyproject.toml` to maturin and
  expose an internal native module while retaining `crowdb_dataset` imports.
  Define supported Python/platform targets in package metadata and CI; only
  claim tested wheels. Source packages include every Rust build input. Files:
  `sdk/pyproject.toml`, `sdk/crates/crowdb-sdk-python/Cargo.toml`,
  `sdk/crowdb_dataset/__init__.py`.
- [ ] **Binding lifecycle**: implement client/read/upload handles, Python error
  mapping and context-manager close. Sync calls release the GIL around Rust
  work; provide async methods bridged to Python cancellation without running a
  blocking wait on its event loop. Share runtime within a process, not per
  request; detect/reject inherited live handles after fork. Files:
  Python crate `src/runtime.rs`, `client.rs`, `read.rs`, `upload.rs`, `error.rs`.
- [ ] **Batch and file interfaces**: bind the same Rust upload and read APIs,
  prefer files and batches over per-byte callbacks, and define owned result
  lifetime across handle close. A bounded copy is acceptable; avoid repeated
  full-payload conversion to Python lists. Files: Python crate `src/buffer.rs`,
  `read.rs`, `upload.rs`, Python public module/type annotations.
- [ ] **Iterable adaptation**: replace client-side fetch-then-discard worker
  partitioning with the server plan partition contract; preserve public import
  compatibility and keep tensor decoding/collation above the SDK. Surface
  explicit acknowledgement/resume rather than advancing on prefetch. Files:
  `sdk/crowdb_dataset/iterable.py`, `sdk/test_iterable.py`, new Python tests.
- [ ] **Outside-repo consumer**: build Rust packages, source distribution and
  Python wheel, install in a temporary consumer without server paths or Rust
  compiler for wheel installation, and run examples against the Dataset
  fixture. Ensure CI imports the installed package rather than source shadowing.
  Files: SDK packaging task, `sdk/tests/consumer/`, `e2e-test/client.py`.

## Phase 6 — Functional verification and closure

- [ ] **Unit cases**: wire/model validation, retry classification, bounds and
  error mapping. Place Rust tests under each crate's `tests/`; add `test-util`
  self dev-dependencies only if test hooks are needed. Files: SDK crate tests.
- [ ] **Integration cases**: inject lost replies, throttling, malformed streams,
  cancellation, GIL contention and forked handles. Gate concurrent requests
  deterministically to prove progress and pool/runtime reuse; no performance
  thresholds or saturation sweeps. Files: SDK Rust/Python tests.
- [ ] **E2E cases**: run Rust and installed Python clients through real chunk
  publication/read, progress/resume, isolation, retention and restart recovery.
  Register all cases in the Dataset domain with failure logs. Files:
  `e2e-test/scenarios.py`, `tests/access_dataset.rs`, SDK consumer fixture.
- [ ] **Public examples and compatibility**: document installation, create,
  file upload, publication, point/batch/stream reads, acknowledge/resume and
  explicit close. State API version, Python/platform support, durability and
  size limits. Explain that local staging and direct access are not implemented.
  Files: new `sdk/README.md`, `sdk/examples/`, package release notes.
- [ ] **Requirement closure**: reconcile permanent Dataset docs, remove R232
  and its backlog entry, then remove this plan only
  after all acceptance is verified. Keep deferred R233/R220. Files:
  `doc/backlog/backlog.md`, Dataset docs. The obsolete Dataset TODO was removed
  on resumption; R220 tracks the deferred fat client.

## Consolidated file scope

- Build: root `Cargo.toml`, `sdk/Cargo.toml`, `sdk/Cargo.lock`,
  `sdk/crates/{crowdb-sdk-core,crowdb-sdk-dataset,crowdb-sdk-python}/`.
- Python: `sdk/pyproject.toml`, `sdk/crowdb_dataset/`, `sdk/test_*.py`.
- Contract/consumers: `sdk/contract-fixtures/`, `sdk/tests/consumer/`,
  `sdk/examples/`, `sdk/README.md`.
- Gates: `pixi.toml`, `pixi.lock`, `tools/pixi-tasks/test.py`, CI coverage
  checks and `.github/workflows/ci.yml`.
- Integration: `e2e-test/client.py`, `access_dataset_fixture.py`,
  `tests/access_dataset.rs`, `scenarios.py`; legacy client callers as needed.

## Execution and gates

- Implementation uses
  `/implement-requirement` and `/coding`, keep one active task, and commit
  coherent changes after relevant gates. Do not commit existing unrelated work.
- Candidate new task names: `test-dataset-sdk`, `test-dataset-sdk-python`,
  `test-dataset-sdk-package`, `sdk-fmt-check`, `sdk-lint`. Add them in phase 1
  before treating them as runnable; preserve existing task meanings.
- Rust gates: `pixi run -- cargo test --manifest-path sdk/Cargo.toml --workspace`,
  `pixi run -- cargo fmt --manifest-path sdk/Cargo.toml --all -- --check`,
  `pixi run -- cargo clippy --manifest-path sdk/Cargo.toml --workspace --all-targets -- -D warnings`.
- Python gate after extension build/install:
  `pixi run -- python -m unittest discover -s sdk -p 'test_*.py'`;
  package-isolation task must run outside the source tree separately.
- Server integration: `pixi run clean-env && pixi run test-e2e-dataset`.
  Root changes also run `pixi run rs-fmt-check`, `pixi run rs-lint` and
  `pixi run check-ci-test-tasks` as applicable.
- Run all build/test/lint executables through Pixi. Use 60-second shell
  timeouts by default, asynchronous execution/polling for long builds, and
  complete failure output. Apply requirement retry rules to implementation.
