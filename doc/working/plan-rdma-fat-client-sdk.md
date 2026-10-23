<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Trusted internal S3 fat client and shared RDMA runtime Plan

Upstream: [R242](../backlog/R242-rdma-fat-client-sdk.md),
[root RDMA plan](plan-rdma.md),
[registered buffers](../design/rpc/design-crowdb-rpc-rdma.md#4-registered-arenas-and-slot-ownership),
[service integration](../design/rpc/design-crowdb-rpc-rdma.md#7-service-interactions),
and subsequent [Dataset integration R220](../backlog/R220-dataset-fat-client-plan.md).

Goal: embed trusted internal storage access through a shared topology-aware
native runtime, implement S3 first, and let Dataset attach to the accepted API.

Status: mixed-size transport acceptance passes with repeatable CPU/request
benefit and unchanged per-size latency bounds. Continue this requirement under
`/implement-requirement`; the accepted prerequisite is recorded in the root
RDMA plan. Native SDK implementation and acceptance remain outstanding.

## Scope and current boundary

- The embedding data-processing or AI system is in the Access Server trust
  domain and can reach internal services. Native execution takes an explicit
  tenant/context; it does not construct or verify SigV4, obtain a user-secret
  database, or call HTTP for per-operation authorization.
- Reuse `crowdb-access-s3` metadata, conditions, `PreparedGet`,
  `VerifiedGetStream`, publication, and multipart components. Complete S3
  operations still live partly in `app/crowdb-access-server/src/s3/`.
- `BufferLeaseMetadata`, `BufferOwnership`, `WritableBuffer`, and completion
  callbacks already exist in `crowdb-rpc-ffi`. `standalone_buffer` and
  `receive_data_buffer` have copy/detach branches; audit rather than assume
  end-to-end ownership is already preserved.
- Add `lib/crowdb-sdk-native/` to the main workspace. Keep the independently
  published `sdk/` HTTP crates and Python wheel free of native storage/FFI
  dependencies. New Python/C++ business bindings are not R242 gates; the
  existing C ABI buffer lease boundary remains in scope.
- R242 accepts shared runtime/topology/data APIs and native S3. Dataset
  attaches afterward under R220; its measurements and Python work must not
  block S3 or create a parallel native runtime.

## Phase 1: shared runtime and native API

- [x] **Create native crate and baseline**: register `crowdb-sdk-native` with
  `runtime`, `topology`, `buffer`, and `s3` modules and tests under `tests/`.
  Run focused existing S3/RPC/access and standalone SDK baselines before
  moving operation code. Files: `Cargo.toml`, `lib/crowdb-sdk-native/`,
  `lib/crowdb-access-s3/tests/`, `app/crowdb-access-server/tests/`.
- [x] **Define runtime ownership**: implement `ClientRuntime::connect(config)`
  and shutdown, sharing discovery, compatible transport groups, producer
  allocation, and hard request/byte limits across handles. Preserve distinct
  S3/Dataset chunk types and writer policies; do not create a generic Repo
  chunk type or mix incompatible writer pools. Let `S3StorageClients` accept
  shared clients instead of reconnecting for each facade. Files:
  `lib/crowdb-sdk-native/src/runtime.rs`, `lib/crowdb-access-s3/src/storage.rs`,
  `lib/crowdb-chunk-client/src/client.rs`.
- [~] **Define business context**: expose `S3Client::new(runtime, config)` with
  tenant/write policy and typed per-operation request identity, deadline,
  cancellation, conditions, and range. Preserve S3 errors and uncertain
  outcomes without HTTP headers, XML, or `CredentialProvider`. Files:
  `lib/crowdb-sdk-native/src/s3/`, `lib/crowdb-access-s3/src/error.rs`,
  `lib/crowdb-access-s3/src/condition.rs`, `lib/crowdb-access-s3/src/range.rs`.
- [~] **Define common delivery**: reads return metadata plus a pull-based
  `next() -> Result<Option<BufferLease>, _>` body; `Ok(None)` means applicable
  length/integrity checks passed. Preserve terminal failure across subsequent
  polling; early drop is cancellation, not successful EOF. Define unique
  writable storage, freeze/submit ownership, and write finalization. Keep business
  results separate from common buffer types so later Dataset batches can own
  sample/field streams. Files: `lib/crowdb-sdk-native/src/buffer.rs`,
  `lib/crowdb-sdk-native/src/s3/`, `lib/crowdb-access-s3/src/retrieval.rs`.

## Phase 2: supported topology and scheduling surface

- [x] **Expose retained topology views**: provide inspection and explicit
  refresh of service endpoints, rack/node/disk membership, Chunk-KV range
  owners, transport capabilities, and source generation/age. Reuse existing
  catalog and route owners; independently observed generations must not
  become an invented global topology epoch. Files:
  `lib/crowdb-sdk-native/src/topology.rs`,
  `lib/crowdb-chunk-kv-client/src/catalog.rs`,
  `lib/crowdb-diskio-client/src/topology.rs`,
  `lib/crowdb-diskio-client/src/status.rs`,
  `lib/crowdb-diskio-client/src/semantic/native_routes.rs`.
- [x] **Expose object locality**: resolve a logical object generation/range to
  chunk placement and route observations for caller scheduling/prefetch.
  Preserve the metadata-selected identity during execution instead of
  re-resolving a mutable key to a different generation. Observation cannot
  publish placement. Reuse `ChunkIoClient::native_storage_routes` where
  suitable. Files: `lib/crowdb-sdk-native/src/s3/`,
  `lib/crowdb-sdk-native/src/topology.rs`,
  `lib/crowdb-access-s3/src/metadata/record.rs`,
  `lib/crowdb-chunk-client/src/client.rs`.
- [~] **Handle stale observations**: retain catalog invalidation, endpoint
  refresh, ownership epochs, and generation fencing. Old views stay safe to
  inspect but cannot force execution past validation. Return typed
  stale/fenced/deadline outcomes when refresh fails; never replay ambiguous
  writes. Files: `lib/crowdb-sdk-native/tests/`,
  `lib/crowdb-chunk-kv-client/src/client.rs`,
  `lib/crowdb-diskio-client/src/semantic/`.

## Phase 3: shared S3 execution and first native frontend

- [x] **Extract bucket and metadata operations**: move typed bucket, HEAD,
  list, and delete orchestration into the S3 library. Preserve bounded
  listing, continuation scope, conditions, and shared-range cleanup. HTTP
  parses/authenticates, calls shared execution, and encodes. Files:
  `lib/crowdb-access-s3/src/`,
  `app/crowdb-access-server/src/s3/operations.rs`,
  `app/crowdb-access-server/src/s3/operations/delete.rs`,
  `lib/crowdb-sdk-native/src/s3/`.
- [~] **Extract streaming GET**: adapt `PreparedGet`/`VerifiedGetStream` to
  typed metadata and a lazy native body with logical offsets. Preserve
  selected generation, range checks, storage checksums, and final checksum
  errors. Do not expose HTTP strings or RPC fragments as native object data.
  Files: `lib/crowdb-access-s3/src/retrieval.rs`,
  `app/crowdb-access-server/src/s3/operations.rs`,
  `lib/crowdb-sdk-native/src/s3/`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`.
- [x] **Extract PUT and copy**: separate HTTP decoding from bounded input,
  checksums, ChunkIO writes, publication, and definite-failure cleanup. Add
  ordinary-input and frozen native-buffer entry points. Preserve
  `PutOutcome::Timeout` uncertainty; final success requires durable data and
  object publication. Files: `lib/crowdb-access-s3/src/streaming.rs`,
  `lib/crowdb-access-s3/src/publication.rs`,
  `app/crowdb-access-server/src/s3/operations/copy.rs`,
  `app/crowdb-access-server/src/s3/operations/upload.rs`,
  `lib/crowdb-sdk-native/src/s3/`.
- [x] **Attach multipart**: reuse the repository and multipart engine for
  begin, upload/list parts, list uploads, complete, and abort. Preserve upload
  identity, validation, publication uncertainty, and deferred cleanup. Files:
  `lib/crowdb-access-s3/src/metadata/multipart_repository/`,
  `lib/crowdb-access-multipart/`,
  `app/crowdb-access-server/src/s3/operations/multipart.rs`,
  `lib/crowdb-sdk-native/src/s3/`.
- [ ] **Verify native TCP parity first**: compare supported operations through
  HTTP and native entry points, then run native access without an HTTP
  listener or S3 credential store. Compare bytes, metadata, conditions,
  errors, publication, and cancellation before enabling RDMA. Files:
  `lib/crowdb-sdk-native/tests/`, `lib/crowdb-access-s3/tests/`,
  `app/crowdb-access-server/tests/`.

## Phase 4: retained leases, RDMA, and bounded lifecycle

- [x] **Complete lease ABI**: reuse opaque `crowdb_rpc_buffer_t`,
  `BufferLeaseMetadata`, and actual buffer length for native `BufferLease`.
  Preserve request/stream identity, sequence, offset, capacity class, and
  single final release. Rust borrowing prevents access after release;
  document C ownership/misuse rather than promising repeated release of freed
  handles is safe. Files: `lib/crowdb-rpc/ffi/src/buffer.rs`,
  `lib/crowdb-rpc/ffi/src/sys.rs`, `lib/crowdb-rpc/include/crowdb-rpc/c_api.h`,
  `lib/crowdb-sdk-native/src/buffer.rs`.
- [x] **Preserve completion ownership**: audit C++ completion,
  `on_complete_cb`, `receive_data_buffer`, DiskIO conversion, ChunkIO views,
  and S3 adaptation. Retain eligible receive storage through caller release
  rather than a copy/detach shortcut that loses credits. Keep control
  decoding/assembly, alignment, and EC copies explicit; cover existing RPC
  callers before changing ownership. Files: `lib/crowdb-rpc/ffi/src/client.rs`,
  `lib/crowdb-diskio-client/src/client.rs`,
  `lib/crowdb-diskio-client/src/semantic.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-access-s3/src/retrieval.rs`.
- [~] **Wire producer buffers**: reuse `ProducerBufferAllocator` and
  `WritableBuffer`, expose only initialized frozen prefixes, and retain
  submitted owners through transport/disk completion. Ordinary input
  adapters stay bounded and account for copies. Files:
  `lib/crowdb-rpc/ffi/src/producer_buffer.rs`,
  `lib/crowdb-chunk-client/src/io.rs`, `lib/crowdb-sdk-native/src/buffer.rs`,
  `lib/crowdb-access-s3/src/streaming.rs`.
- [x] **Enforce held-capacity budgets**: charge actual allocation capacity
  once across shared views until final release. Keep request/producer/held
  limits explicit and lock-free; waits honor deadlines/cancellation without
  blocking RPC workers. Release backing storage before waking waiters. Files:
  `lib/crowdb-sdk-native/src/runtime.rs`, `lib/crowdb-sdk-native/src/buffer.rs`,
  `lib/crowdb-rpc/ffi/tests/`, `lib/crowdb-sdk-native/tests/`.
- [x] **Implement cancel and shutdown**: close admission, stop producers,
  resolve operations once, and preserve storage through disk/CQ/caller
  ownership. Release after shutdown retires slots without accessing destroyed
  transport. Files: `lib/crowdb-sdk-native/src/runtime.rs`,
  `lib/crowdb-sdk-native/src/buffer.rs`, `lib/crowdb-rpc/ffi/src/client.rs`,
  `lib/crowdb-sdk-native/tests/`.
- [ ] **Verify fallback and GPU-copy lifetime**: retain existing TCP/RDMA
  policy, request identity, and eligible pre-payload fallback; fail accepted
  or partial RDMA operations without TCP suffixes or ambiguous replay. Add an
  asynchronous host-to-GPU copy simulation holding a lease beyond stream
  close until copy completion. Files: `lib/crowdb-sdk-native/tests/`,
  `lib/crowdb-rpc/ffi/tests/`, `lib/crowdb-diskio-client/tests/rdma_failure_test.rs`.

## Phase 5: S3 acceptance and Dataset handoff

- [ ] **Run native and OCI profiles**: extend accepted launcher/runtime
  conventions with the S3 SDK corpus, no-device TCP and shared-HCA profiles.
  Record topology, actual path/classes, independent QPs, held-capacity high
  water, errors, and exact HTTP/native parity. Native-only execution must not
  require Access Server. Files: `tools/rdma/`, `e2e-test/tests/access_s3/`,
  `lib/crowdb-sdk-native/tests/`.
- [ ] **Verify distribution and document API**: run standalone SDK boundary
  and functional controls; document shared runtime, topology scheduling,
  stream termination, lease release, publication, and shutdown. Record R242
  evidence and the shared API consumed by R220. Files:
  `lib/crowdb-sdk-native/README.md`, `sdk/README.md`,
  `tools/ci-checks/check-dataset-sdk-boundary.py`,
  `doc/backlog/R220-dataset-fat-client-plan.md`.

R242 exit: accepted runtime/topology/lease APIs and native S3 semantics,
TCP/RDMA parity, bounded pressure, and shutdown-safe caller ownership. Existing
transport acceptance alone cannot establish SDK completion.

## Execution evidence

- Mixed-size optimization prerequisite is committed as `f3de1109`; repeated
  final HCA measurements and service parity pass under the root RDMA plan.
- Existing S3 library all-target tests, Access Server all-target tests, and
  standalone SDK boundary/functional controls pass before extraction.
- `crowdb-sdk-native` is registered in the main workspace. Initial tests cover
  actual Vec capacity, shared-slice accounting, frozen native producer owners,
  request cancellation/deadline, shutdown-safe held data, two tenant handles
  without an HTTP listener/credential store, and separate typed writer groups.
  Nine native cases pass, including asynchronous-copy and interrupted-mutation
  outcomes. The S3, ChunkIO and DiskIO all-target suites, affected HTTP tests
  and focused four-crate all-target clippy also pass.
- Runtime connection and attachment reuse control/catalog/chunk clients.
  Compatible writer policies share a bounded group; incompatible policies get
  separate small-writer pools sharing transports, reader and SDK budgets.
  Shutdown currently closes SDK admission and drains small writers. Completing
  native transport/completion teardown remains in Phase 4.
- Disk routes now have a retained inspection view with local generation and
  publication age. Native catalog/service/transport observations do not invent
  a global epoch. Attached catalog age is explicitly unknown until refreshed.
  Full stale-view/locality acceptance remains outstanding.
- Basic bucket and point object lookup/delete orchestration is extracted into
  `S3Executor`, and HTTP delegates to it. Streaming GET/PUT, copy, multipart,
  typed native results and real HTTP/native parity are not complete.
- FFI completions now retain their owned native handles, including logical
  control admission and receive recycling. Additive ABI queries expose the
  canonical allocation identity, actual capacity, view offset and initialized
  span; unknown external owners never substitute their view length for capacity.
  `OwnedPayload` preserves this metadata across slices and logical credit owners.
  DiskIO/ChunkIO propagation and SDK accounting remain outstanding.
- Explicit shared-HCA FFI acceptance with two posted receives retains both
  response payloads, observes the third response remain pending, releases one
  and completes the third. Held data stays readable after connection/server
  destruction. The complete FFI suite/clippy and native RPC suite pass (211
  native cases, three explicitly unavailable or separately gated cases).
- Shared GET now offers typed half-open ranges and `next_verified`, with sticky
  storage/length/integrity failures. Empty full reads also verify their checksum.
  The legacy HTTP `next_chunk` remains fused after its first terminal result.
  Three focused cases and the complete S3 library suite/clippy pass. Native
  object metadata/body adaptation is still pending.
- Before completing native GET delivery, carry retained allocation identity,
  actual capacity and source ownership from RPC through DiskIO/ChunkIO. Use an
  owner-backed payload type with slices preserving one allocation owner; do
  not rediscover receive ranges by scanning connection tables or adding a hot
  lock, and do not substitute a guessed capacity for the actual native class.
- DiskIO now exposes `read_payload`, and production ChunkIO mirror/direct
  reads, frame slices, logical stream-credit wrappers and S3 `next_payload`
  preserve `OwnedPayload`. Existing public `Bytes` adapters erase metadata only
  at their compatibility boundary. Required EC/physical coalescing uses fresh
  Vec owners with known capacity; metadata-less injected seams remain explicit.
  The 1,500-line reader is split by layout, location, frame views, verified
  frame reads and stream delivery. A focused framing test proves original
  address/capacity retention and logical credit lifetime through final Bytes
  release; all existing reader cases pass. Native SDK capacity admission and
  S3 body adaptation are the next dependent tasks.
- Runtime-wide held allocation accounting now uses the existing lock-free
  skip-list dependency and weak entries removed at final backing release.
  Per-view upstream logical read owners remain retained independently of the
  shared physical charge. Admission waits register before checking capacity
  and publication, honor request cancellation/deadline and wake on shutdown.
  Unknown Bytes owners reserve capacity before a bounded copy, counted in
  `ResourceStatus::compatibility_copy_bytes`. Five focused tests cover shared
  capacity, 64 concurrent publication/release rounds, bounded copy pressure,
  cancellation/shutdown and undersized views hiding oversized allocations.
- Native S3 now has typed configuration, entity-tag/date conditions, HEAD,
  lazy GET and half-open ranges. Shared condition evaluation serves HTTP and
  typed inputs. `ResolvedObject` captures the metadata revision in the same
  point read, retains its immutable record, and rejects another tenant or
  metadata authority; identical overwrites have distinct revisions. Native
  metadata exposes validated user values without HTTP headers.
- `ObjectBody::next` returns retained `BufferLease` views and verifies final
  length/integrity before successful EOF. Terminal errors remain sticky;
  cancellation/early drop return request admission independently of delivered
  buffers. A cancelled pending pull retains its not-yet-delivered payload, so
  resuming after capacity release cannot skip it. Seven native read cases
  cover original pointer/capacity, range offsets, overwrite/authority binding,
  conditions, empty/corrupt checksum, capacity deadline, cancellation/shutdown
  and pending-pull cancellation. The native suite now has 22 passing cases;
  six allocation cases separately verify retention of each upstream logical
  owner while sharing one physical charge. Shared S3 tests and affected HTTP
  service/inspection tests, all-target clippy and Rust formatting pass.
- Still outstanding: native S3 PUT/copy/multipart/listing completion and HTTP
  extraction, object locality/stale-view
  acceptance, owned transport shutdown, and the complete native/OCI S3 corpus.
  Current read fixtures use real storage reader/framing with injected metadata
  and disk seams; they are not the required real-service TCP/RDMA acceptance.
- Runtime data transport now accepts an explicit `RdmaConfig`, forwarded
  through shared ChunkIO discovery before DiskIO connections open. Legacy
  constructors keep environment defaults. Connection construction is split
  out of the nearly 1,000-line ChunkIO client.
- Native GET now also traverses real TCP and shared-HCA RC RPC through the
  production semantic DiskIO adapter, ChunkIO framing and S3 verification.
  The RDMA case charges an 8-byte object to its actual 64 KiB receive class,
  blocks the next delivery until final release and keeps the delivered lease
  readable after all client/server owners are destroyed. Both profiles report
  zero SDK compatibility-copy bytes and no read retries. `ServerRequest::reply`
  retains the original connection/server via a safe owned-handler registration;
  new native test code adds no unsafe exception. FFI/ChunkIO/native all-target
  tests, clippy and formatting pass. These focused RPC tests still inject
  metadata/layout and disk contents; the final corpus must use real services.
- Retained topology now follows newer catalog generations installed by normal
  operations, with unknown original observation age rather than a fabricated
  timestamp. Explicit catalog-only refresh reports typed invalid/unavailable
  failures and refuses generation regression. Endpoint, disk and catalog
  sources remain independent. Typed transport observations expose readiness,
  wire classes, TCP fallback policy and configured queue/registration bounds;
  configured values do not pretend to be provider connection limits.
- `object_locality` inspects one tenant/authority-bound `ResolvedObject`,
  range and bounded 1..128 extent page without fetching payload. Each chunk
  observation retains its own modification revision and layout validity;
  mirror/EC roles, unavailable fragments and observed disk/rack/node owners
  remain explicit. Shared chunk references query once per page. Pagination
  stays on the selected generation after name overwrite. Old views remain
  inspectable, report typed stale outcomes at expiry and cannot force a deleted
  layout into execution. General injected disk writers can expose actual
  native route/transport observations; real TCP/RDMA cases verify these owners.
  Three new catalog/locality cases pass; the native suite now has 27 cases.
  Native/S3/ChunkIO all-target suites and clippy pass. Full service restart,
  stale-catalog/endpoint fault acceptance remains part of the final corpus.
- HTTP and native listing/batch deletion now use shared `S3Executor` execution.
  Native listing returns bounded typed entries and scoped continuation; handles
  share a runtime-local random signer key unless configured for stable cursors.
  This key is not a user credential and requires no credential database.
  Lazy native batch deletion validates the entire selection, returns each
  independent ordered result, keeps duplicate semantics and stops its tail on
  drop/cancellation. Shared typed deletion preserves transport/deadline reply
  loss as uncertainty; HTTP renders the same stable ServiceUnavailable code.
  Four focused native cases cover paging/scope, lazy ordered partial progress,
  cancellation and an applied mutation with a deliberately lost reply. The
  native suite now has 31 cases. Native/S3 all-target tests, affected HTTP
  service/inspection/multipart tests, three-crate clippy and formatting pass.

## Immediate dependent implementation

- Owned shutdown now has one lock-free coordinator and retained terminal result.
  Closing admission starts cleanup independently of any caller's wait future;
  owned small pools drain before owned ChunkIO/ChunkDB/Chunk-KV RPC workers and
  reapers join on a blocking worker. Attached `from_clients` groups remain
  borrowed; `adopt_clients` explicitly transfers shutdown responsibility, and
  independently created writer policies own only their new small pools.
  DiskIO closes admission and wakes native-route refresh workers. Held native
  receive storage remains readable after actual worker shutdown; new DiskIO
  operations are rejected. Two new cases cover borrowed ownership and cancelled
  close waiters with a paused owned close. Explicit TCP/HCA receive tests pass
  with the stronger worker-stop assertion. Native and four affected storage
  client all-target suites (48 native cases), clippy and formatting pass.

- HTTP and native multipart now share session creation/expiry, part writer
  preparation, copied-part execution, exact generation publication, completion
  freeze and abort. Retained native upload identities are tenant/authority-bound;
  admission refreshes durable phase/expiry instead of trusting an old handle.
  Native part input shares PUT's bounded ordinary/framed buffers and cancellation.
  Five new cases cover 5 MiB plus framed-tail composition, paged parts/uploads,
  replacement/captured-source copy, abort/reconnect, confirmed lost replies and
  an applied object publication whose confirmation reads also fail. That failure
  remains uncertain; the same frozen selection later confirms without data
  replay. Native/S3/Access Server all-target suites pass (46 native cases), as
  do three-crate clippy and formatting. Access Server required more than the
  default 60-second outer compilation limit; continuing its cached build and
  complete test run passes without changing any test timeout.

- HTTP object copy and upload-part copy now use shared verified copy execution.
  Native copy accepts ordinary source names or a tenant/authority-bound retained
  selection; overwrites cannot replace that selection. Metadata COPY/REPLACE,
  self-copy validation, conditions, source integrity, durable completion and
  uncertain publication remain shared. Three native cases cover these contracts
  and lost applied replies. Native/S3 all-target suites (41 native cases),
  affected HTTP tests, focused clippy and formatting pass. Full service parity
  and native multipart attachment remain pending.

- Native `begin_put`/`put_vec` now use the shared durable PUT session and return
  the publication's exact revision. Input calls accept at most 1 MiB and retain
  actual allocation capacity; foreign-runtime owners are rejected. Registered
  producers initialize payload slots directly with reserved frame space and
  freeze into `FrozenObjectBuffer`; all physical/payload views retain their
  producer credit, including initializer failure/panic and shutdown.
  Large framed streams require aligned interior frames; one small framed owner
  must hold the complete declared object. Native writer waits are cancellable;
  dropping a pending input terminates that writer and returns request admission.
  Seven new cases cover mixed sizes through multiple chunks, original payload
  addresses, length/checksum/cancellation, pending-input drop and an applied
  publication whose reply is lost. Native/S3 all-target suites pass (38 native
  cases), with focused three-crate clippy and formatting. Native RPC write/HCA
  acceptance, copy and multipart remain outstanding.

- Shared writer selection and `PreparedPut` now serve HTTP PUT. Durable
  completion precedes one versioned publication, retaining the same metadata
  revision and uncertain timeout classification without a subsequent HEAD.
  Native physical views require finalization and reject later header/footer
  rewrites, including a changed chunk destination. The new ownership test,
  full S3 library suite, affected HTTP tests, three-crate clippy and formatting
  pass. Explicit HCA native producer framing still sends 1 MiB directly with
  zero staging-copy bytes. Native PUT attachment is the next task.

- Accepted RDMA read transport, deadline and malformed-response failures now
  retain `AcceptedReadInterrupted` through routed DiskIO, mirror/EC readers,
  physical/frame windows and native `ReadFailureKind::Interrupted`. No route,
  replica, recovery-candidate or layout retry can replay that accepted read.
  Ordinary disk/CRC recovery controls still pass. Four new CPU cases cover
  direct mirror/EC, interrupted recovery and framed-stream propagation.
  Explicit shared-HCA GET accepts a read, stops the peer with TCP fallback
  enabled and three retries configured, and reports the same sticky terminal
  error without any additional semantic read, connect, reconnect or retry.
  A prior caller-held receive remains readable through owned runtime shutdown.
  The three DiskIO HCA failure cases also pass. Native (49 cases), DiskIO and
  ChunkIO all-target suites, focused three-crate clippy and formatting pass.

- Next: native RPC write/registered producer acceptance and the complete
  real-service native/OCI S3 corpus, including frontend parity and restart.

## Subsequent Dataset integration under R220

- [ ] **Attach Dataset after S3**: add a facade using the same runtime,
  topology, and buffers. Reuse `DatasetAuthority`, `DatasetSessions`, upload,
  composition, and Split logic; extract remaining common HTTP v1/v2 delivery
  where needed. Files: `lib/crowdb-sdk-native/src/dataset/`,
  `lib/crowdb-access-dataset/`, `app/crowdb-access-server/src/dataset/api/`.
- [ ] **Preserve Dataset completion**: retain Snapshot/Split binding,
  selection/partition, active-read protection, explicit acknowledgement,
  resume, and cleanup. Legacy `DatasetDirectClient::scan` progress behavior
  is not the v2 contract. Buffer release and download completion never
  acknowledge batches. Files: `lib/crowdb-access-dataset/src/read_session/`,
  `lib/crowdb-sdk-native/tests/`, `app/crowdb-access-server/tests/`.
- [ ] **Complete Dataset follow-up**: perform R220's HTTP/direct workload
  comparison and native Python integration using this runtime, preserving
  the independent HTTP SDK distribution. Files:
  `doc/backlog/R220-dataset-fat-client-plan.md`, `sdk/`,
  `e2e-test/tests/access_dataset/`.

These tasks belong to R220, follow S3 delivery, and are not prerequisites for
R242 acceptance or a second implementation of its runtime.

## Tests

Unit tests:

- typed context/range/conditions and retained topology ownership;
- opaque ABI layout, fixed-width fields, initialized-length and null/error rules;
- unique write/freeze ownership, shared accounting, and single final release;
- nonblocking completion, single terminal result, and asynchronous copy ownership.

Integration tests:

- shared runtime handles with isolated operation cancellation;
- topology/locality inspection, stale owner/endpoint refresh, and fencing;
- S3 bucket/object/copy/multipart parity, conditions, and uncertain publication;
- native execution without HTTP listener, SigV4 verifier, or secret database;
- final integrity failure, lease order/offsets, and eligible no-copy payloads;
- held-capacity pressure, deadlines, cancel, shutdown, and release afterward;
- TCP-only results, pre-payload fallback, terminal accepted/partial RDMA failures;
- standalone Dataset HTTP SDK packaging and functional regression controls.

E2E tests:

- native S3 against metadata/ChunkIO/DiskIO with explicit topology inspection;
- no-device TCP OCI and shared-HCA local-loop multi-container profiles;
- held-lease pressure, multiple streams, and simulated asynchronous GPU copy;
- HTTP/native parity separate from the native-only execution fixture.

## Consolidated files

- `Cargo.toml`, new `lib/crowdb-sdk-native/`, its `tests/` and README.
- `lib/crowdb-access-s3/`, `lib/crowdb-access-multipart/`, S3 HTTP operations.
- `lib/crowdb-chunk-kv-client/`, `lib/crowdb-chunk-client/`, `lib/crowdb-diskio-client/`.
- `lib/crowdb-rpc/ffi/`, `lib/crowdb-rpc/include/crowdb-rpc/c_api.h`, affected C++ ownership paths.
- Access Server tests, `e2e-test/tests/access_s3/`, `tools/rdma/`.
- `sdk/README.md` and standalone boundary controls; no new native dependency in `sdk/`.
- R220 follow-up: Dataset library/HTTP adapters, native Dataset facade and bindings.

## Verification gates

Run after registering the native crate; use existing-package gates for the
baseline. Root Cargo gates do not cover the independent SDK workspace.

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-sdk-native --all-targets
pixi run -- cargo test -p crowdb-access-s3 --all-targets
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
pixi run check-dataset-sdk-boundary
pixi run test-dataset-sdk
pixi run sdk-fmt-check
pixi run sdk-lint
pixi run test-e2e-s3
```

Enable `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` in CI or while diagnosing failures.
Capture actual native/OCI profile and lease evidence; passing TCP gates do
not replace the real-HCA profile.
