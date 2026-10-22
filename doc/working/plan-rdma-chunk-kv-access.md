<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA Chunk-KV access Plan

Upstream: [R241](../backlog/R241-rdma-chunk-kv-access.md),
[root RDMA plan](plan-rdma.md), [registered buffers](../design/rpc/design-crowdb-rpc-rdma.md#4-registered-arenas-and-slot-ownership),
and [RDMA RPC transport](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: use the common RDMA stream for Access Server to Chunk-KV operations while
preserving existing aggregation, request numbering, generation checks, and TCP
semantic parity.

## Execution checkpoint and implementation order

The datapath is accepted at `ab1ffbfb`; its cleanup is `b38d3d78`. Preserve its
1 MiB direct payload path and the ordinary contiguous RPC benchmark. The
focused Chunk-KV baseline passes all 22 client/CLI cases before these changes.
All 278 affected Access/ChunkIO/DiskIO cases and full native/FFI gates passed
at the preceding service checkpoint.

Observed contract:

- `ChunkKvRpcTransport` encodes point/seek/scan/multi-get/batch requests as
  FlatBuffer control buffers and calls the existing `RpcClient::call`. Its
  connection pool and completion IDs remain the correlation owner. The
  server's `ChunkKvRpcService` decodes one complete control, checks the same
  RPC ID and submits one encoded response. Large values and aggregates are
  inside these control bytes, not the RPC data field.
- The routing/composition layer retains logical mutation identities during
  retries and retries only unresolved group operations. Seek/scan retain
  partition generation and bounded-page semantics. Do not add a separate
  Chunk-KV request table or retry scheduler.
- Native RDMA currently rejects control beyond 1010 bytes. Control/data gives
  independently aligned data with exact 64 KiB/256 KiB/1 MiB classes; use this
  existing layout for encoded-control fragments. Keep the current direct
  path for ordinary small control plus real datapath data.
- Metadata point calls use 64 KiB; grouped/seek/scan calls use 256 KiB. An
  explicit class config may select 1 MiB. Each class owns a fixed native
  connection group and common RPC completion engine, with no producer thread
  per loader. Server default is 256 KiB. QP data capacity is the minimum of
  the two advertised capacities and is used for actual RX allocation/posting
  as well as SEND admission; expose that effective value in immutable status.

- [x] **Build bounded common control-stream primitives**: add portable byte
  admission, fragment metadata validation and one-message assembly, tested
  without verbs. Reserve full logical length, validate request/stream ID,
  sequence, exact offset, total, class, generation and final flag. Default
  maximum encoded control is 32 MiB, covering existing 16 MiB response limits;
  independent send/receive budgets default to 64 MiB each. Reject invalid
  arithmetic/limits before allocation. Retain completed reservation until
  final caller release. Preserve a failed/blocked fragment's existing owner;
  blocked admission must not carve replacement storage. Files: private RPC
  control-stream modules, `framing.h` control owner, focused portable tests.
- [x] **Negotiate and configure common streams**: add a small transport-owned
  fragment FlatBuffer in `common_msg.fbs`, preserving the common leading
  correlation fields, with reserved message type 4. Bump RDMA protocol to
  version four; the existing final 4 handshake bytes advertise maximum logical
  control length and participate in the CM capability digest. Extend the CM
  offer from 48 to 52 bytes to carry that limit explicitly: the listener does
  not retain the client's TCP preflight capability, so its digest alone cannot
  provide the client's receive limit for response admission. Validate the
  explicit limit against the capability when it is available. Legacy decode
  remains for explicit pre-payload TCP fallback. Extend native/env/status and
  a new versioned C ABI config entry without changing the version-one struct
  or existing configure entries. Rust typed config carries max message,
  per-direction budget and partial-assembly timeout (default 30 seconds).
  The Chunk-KV typed class factory derives send arena bytes from selected SQ
  depth and data capacity, as receive defaults already do. Preserve native
  `RdmaConfig::for_layout`'s existing 64 MiB send arena default for the accepted
  1 MiB datapath and ordinary RPC. Files: capability/offer/config/status, FFI.
- [x] **Integrate CQ-owned fragmentation and assembly**: enqueue one original
  logical frame under byte/count admission. The CQ owner walks it in order,
  builds only the next bounded fragment, and holds immutable source owners
  through all WR completions. Do not materialize an unbounded fragment list.
  Receive validates metadata before copying into one logical owner and returns
  each consumed RQ credit immediately; result size may exceed the entire RQ
  window. On exhausted assembly admission retain at most the existing RQ
  frames. Final lease release wakes the owning polling/event worker through
  the existing retained operation gate, including safe release after shutdown.
  Active partial assemblies expire; consumer-blocked first fragments remain
  backpressured without an arbitrary consumer hold deadline. Dispatch only a
  complete original Frame. Move quota-owned control storage through C ABI
  without another full-message malloc/copy. Files: RDMA send/receive/CQ/CM
  pools, common framing/C ABI conversion and owner-lifetime tests.

  Implementation order within this task:
  - Add private fragment codec and CQ-only sender cursor. Charge the original
    control using a Buffer slice whose retained owner is the byte lease;
    rejected submit restores the caller's original buffer. WR records retain
    the single original cursor until every posted SEND has completed.
  - Set effective data capacity and peer generation/maximum from CM. Active
    connections create/prime receive storage after the validated CM reply and
    before application publication, so asymmetric classes allocate/post the
    actual minimum rather than a larger local default.
  - Extend the existing CQ deferred receive vector for byte admission. A
    per-QP count preserves FIFO, and a per-pass epoch prevents later fragments
    overtaking an admission-blocked first fragment. Existing RQ owners stay
    intact; no replacement buffers are carved.
  - Check active assembly deadlines under CQ ownership and bound the event
    wait to the nearest active deadline. Shared receive budget release wakes
    CQ workers through the retained transport operation gate. Keep storage
    destruction before returning byte admission.
  - Record logical-message, fragment, copied-control, directional admission
    and timeout counters separately from direct application payload counters.
- [x] **Wire Chunk-KV classes and typed settings**: add service defaults and
  explicit RDMA configuration for metadata/aggregate connection groups,
  preserve TCP defaults and current serializers/errors/retries, and wire the
  Chunk-KV server plus Access storage construction. Cover point, mutation,
  seek, bounded scan, multi-get and batch mutation, including duplicate result
  order and stable logical mutation IDs. Files: client transport/config,
  server startup/RPC boundary, Access storage config and tests.

  Implementation order within this task:
  - Serialize the existing Rust `RdmaConfig` for cold service configuration;
    preserve its native defaults. Add an explicit
    `with_control_data_capacity` helper that derives wire capacity and SQ/RQ
    arena bytes from the selected data class and configured depths.
  - Add `ChunkKvRpcTransportConfig` with optional `metadata_rdma` and
    `aggregate_rdma` overrides. Missing overrides preserve environment policy
    and select native 64 KiB/256 KiB service defaults. Explicit enabled
    overrides require control/data and one of 64 KiB/256 KiB/1 MiB. Each
    group retains its own existing connection index and one fixed transport;
    the transport shares one `RpcClient`, reaper and request-ID generator.
  - Split the current client transport runtime and endpoint parsing into
    private child modules before extending the 337-line facade. Add read-only
    class/endpoint status for actual path, QP and provider capacities.
  - Configure the Chunk-KV server with a 256 KiB control/data service default
    and optional explicit `rpc_rdma`. Server construction owns listener
    policy; client groups are always non-listeners. Wire Access common
    `chunk_kv_rpc` settings through S3, Iceberg and Dataset storage construction,
    preserving existing convenience constructors with default configuration.
  - Pin the native connection in Frames transferred to asynchronous FFI
    handlers. A completed logical control has no RQ data lease to retain that
    connection; the existing raw handler pointer otherwise outlives CM teardown.
    Add safe `ServerRequest::close_connection` backed by that retained owner.
    Close and report rejected Chunk-KV response submissions instead of silently
    dropping a response. Cover handler hold/peer close/final release.
  - Preserve the current serializers, stable logical mutation IDs, exact pool
    generation invalidation and retry/error behavior. Cover class selection,
    serialized settings, invalid bounds and existing client/server RPC cases
    before the real service corpus.
- [~] **Verify real service and clean up**: test malformed sequence/offset,
  duplicate/end/generation/class, byte exhaustion, held final leases,
  cancellation/close/expiry, and TCP setup fallback. Run the actual Chunk-KV
  service on the HCA with metadata and results larger than 256 KiB, then two
  non-root host-network containers and a no-device TCP profile. Compare bytes,
  typed errors and identity using the same corpus. Rerun the HTTP/DiskIO
  acceptance after common transport changes. Finish native/no-verbs/FFI,
  workspace format/lint and affected service gates. Rebuild matching release
  CLI/fb-server and repeat the 1 s warmup + 10 s peak controls before cleanup.
  Record all settings, actual classes/QPs, memory and error categories. Delete
  this completed child plan/detail/index entry after permanent docs/evidence
  are updated. R242 remains outside the requested scope.

  Implementation order within this task:
  - Run production `ChunkKvRpcService`, `CrowdbPartitionTree` and partition WAL
    logic in a separate process. Inject only catalog/serving authority and the
    memory ChunkStream backing; label that scope explicitly. Use the same
    point/conditional/seek/scan/multi-get/batch corpus and stable mutation IDs
    for strict RDMA, explicit TCP and pre-payload setup fallback.
  - Reuse the canonical benchmark `Runtime` for native and two non-root OCI
    processes. Record exact binaries, source, image, UID, actual connections,
    classes, queues, copied-control bytes, directional admission and errors.
  - Add a `test-util`-only native fragment gate. After the normal real-service
    corpus, pause the response after exactly one posted control fragment and
    close the server. Verify one fragment arrived, no logical result was
    published, the caller fails promptly and TCP fallback remains zero.
  - Repeat HTTP/DiskIO semantic profiles and matching 10-second CLI throughput
    controls after the common RPC changes, then update permanent evidence.

## Phase 1: access contract mapping

- [x] **Map request/response types**: identify metadata, point, batch, range,
  and aggregate operations; record idempotency, request ID, generation, error,
  and retry behavior. Files: `lib/crowdb-chunk-kv-client/`, Chunk-KV server
  modules, and Access Server access-core modules.
- [x] **Map aggregation owner**: identify where Chunk-KV combines responses and
  where it decides flush/end boundaries. RDMA must receive the aggregate after
  that decision, never create another aggregation layer.
- [x] **Define class selection**: metadata defaults to 64 KiB, aggregated
  Chunk-KV results default to 256 KiB, and 1 MiB remains a configured option for
  larger value/range results. Transport creation receives defaults from config.

## Phase 2: common client/server wiring

- [ ] **Add RDMA transport choice**: make the Chunk-KV client use the R239
  logical connection interface and preserve TCP as fallback. Do not duplicate
  request correlation in the Chunk-KV layer.
- [ ] **Advertise selected class**: include class and capacity in stream setup
  or request metadata, validate the peer's negotiated class, and reject a
  payload larger than that class before publishing it.
- [ ] **Preserve request identity**: carry request ID, stream ID, sequence,
  logical offset, generation, and operation type through RDMA completion and
  response assembly.
- [ ] **Preserve access errors**: map transport, timeout, cancellation,
  generation, integrity, and Chunk-KV errors identically for TCP and RDMA.

## Phase 3: multi-message response assembly

- [ ] **Split aggregate responses**: when an existing aggregate exceeds 64 KiB
  or 256 KiB, emit ordered messages without changing its logical result.
- [ ] **Reassemble on client**: validate sequence, offset, total/end flags, and
  duplicate request identity; reject gaps, overlaps, stale generations, and
  wrong classes.
- [ ] **Bound assembly memory**: use R238 classes and caller/consumer release
  to keep aggregate response memory within the configured budget.
- [ ] **Retry safely**: use shared request IDs and only retry operations whose
  current semantics provide idempotency or duplicate suppression.

## Phase 4: fallback and compatibility

- [ ] **Pre-payload fallback**: when RDMA setup fails, retry the same eligible
  request on TCP with the same request ID and generation.
- [ ] **Partial response failure**: fail the whole logical response after an
  uncertain RDMA message; never append a TCP suffix or publish incomplete data.
- [ ] **Config compatibility**: preserve TCP defaults and allow RDMA classes,
  endpoint selection, and enablement to be configured when creating the
  transport.

## Tests

Unit tests:

- class selection for metadata, aggregate, and large range operations;
- split and reassembly of responses at 64 KiB and 256 KiB boundaries;
- sequence gaps, overlap, duplicate, stale generation, and wrong-class errors;
- request identity and idempotency/retry decisions;
- TCP/RDMA error mapping parity.

Integration tests:

- metadata point operation over RDMA matches TCP;
- aggregate result larger than 256 KiB is reconstructed exactly;
- aggregate producer remains the only flush/aggregation owner;
- pre-payload RDMA failure retries eligible requests on TCP;
- partial RDMA response fails without accepting a TCP suffix;
- bounded consumer release applies backpressure to Chunk-KV response production.

E2E tests:

- single-node HCA local-loop OCI Access Server to Chunk-KV operations;
- TCP-only OCI profile with no RDMA attempts;
- single-node local-loop multi-container profile with multiple Chunk-KV connections;
- mixed metadata, point, batch, and range corpus compared byte-for-byte across
  TCP and RDMA.

## Consolidated files

- `lib/crowdb-chunk-kv-client/src/`
- `lib/crowdb-chunk-kv-client/tests/`
- Chunk-KV server RPC/service modules
- Access Server access-core and configuration modules
- `lib/crowdb-rpc/` shared frame and transport modules
- OCI E2E test configuration

## Verification gates

Portable stream checkpoint:

- All 11 focused native control-stream/C ABI cases passed, including exact
  original allocation transfer through cloned FFI handles and held admission.
- `pixi run test-cpp` passed: 543 tree, 177 RPC, 71 DiskIO and 35 common native
  cases, plus both FFI suites. Full logs:
  `.crowdb-runtime/artifacts/unit/cpp-4548393f6da54a0e9d3d3ba47a5cc115/`.
- TCP-only/no-verbs build and all 155 CTest cases passed (hardware case skipped).
  Changed C++ format and scoped `tree-lint` passed; style warnings remain.
- The first full tree run failed the unchanged
  `DoubleBuffer.ConcurrentReadersDuringFrequentFreezeAndDrainNoCorruption`
  assertion `bad.load() == false`. The isolated 13-case DoubleBuffer suite
  and subsequent complete 543-case tree run passed without source changes.
  Preserve the initial failure at
  `.crowdb-runtime/artifacts/unit/cpp-f0f7acd09fb541f59d048f81e3923cc2/00-tree.log`;
  this checkpoint does not claim to repair that intermittent tree failure.

Negotiation checkpoint:

- Protocol v4 carries maximum logical control bytes in the 160-byte TCP
  capability and explicit 52-byte CM offer. Digest/generation/layout/receive
  admission validation binds both ends; versions 1/2/3 remain decodable for
  pre-payload fallback. Native/env/status and version-three C ABI configuration
  expose independent logical send/receive budgets and partial-assembly timeout.
  Version-one config layout and previous configure entries remain valid.
- Full RPC binary passed (168 portable tests; 13 hardware cases skipped),
  no-verbs build passed all 157 CTest cases with its hardware case skipped,
  and protocol/FFI Rust suites passed. Three actual HCA cases passed: direct
  payload/lease round trip, layout mismatch and explicit TCP fallback policy.
  Native XML is under `.crowdb-runtime/rdma-check/artifacts/r241-control/`.
- CLI's exhaustive typed-config literal now inherits the new stream defaults;
  all four coroutine/timed RPC benchmark integration cases passed. Focused
  all-feature Rust clippy, workspace `rs-lint`, format and scoped C++ lint
  passed. The existing request-count worker fixture requires separate client
  and server roles; its role-less invocation failed setup, not a payload test.

CQ integration checkpoint:

- The CQ walks one retained source control at a time. Each fragment fits the
  actual negotiated 64 KiB/256 KiB/1 MiB data capacity; active CM setup primes
  receive storage only after the reply establishes that minimum. Source bytes
  stay charged through the final WR, and rejected queue publication restores
  the caller's original buffer/reference.
- Six actual HCA cases passed: asymmetric classes and exact round trips larger
  than the entire two-slot RQ window; fixed polling/event worker FIFO pressure
  with a held completed result and release wakeup; idle partial expiry;
  source-owner/queue rejection; peer logical limit; invalid generation and
  mismatched encoded-control identity. The 1 MiB case reconstructs 2,097,165
  bytes per direction. A completed control remains readable after transport
  destruction, then safely returns admission on final release.
- `pixi run test-cpp` passed at
  `.crowdb-runtime/artifacts/unit/cpp-6d344d3e3eaf45ba8d8e1d20a0cd1437/`:
  543 tree, 189 RPC, 71 DiskIO and 35 common CTest cases, plus both FFI suites.
  Optional HCA cases are skipped by that default gate and exercised separately.
  No-verbs build passed all 159 CTest cases (one hardware skip). Format and
  scoped C++ lint passed. Final RPC/XML and six HCA case results are under
  `.crowdb-runtime/rdma-check/artifacts/r241-control/`.

Chunk-KV class wiring checkpoint:

- All 1,244 client/server and Access/S3/Iceberg/Dataset target cases passed
  (three optional cases ignored), including
  serialized class settings, point/scan pool separation, one global RPC ID
  sequence and existing routing/retry/duplicate-result semantics. Logs:
  `/tmp/r241-class-rust-tests.log`.
- Full C++/FFI gate passed at
  `.crowdb-runtime/artifacts/unit/cpp-c2122fc1d6a14e7e9beb7959fe2ad245/`:
  543 tree, 190 RPC, 71 DiskIO and 35 common native cases plus both FFI suites.
  No-verbs build passed 160 CTest cases (one hardware skip). The added C ABI
  handler lifetime case proves a control-only Frame pins its connection until
  final release; all eight Rust transport config cases passed.
- Workspace all-feature clippy passed. The first `rs-lint` invocation expired
  while waiting for another Cargo build lock; rerun it without competing Cargo
  work passed. Format and scoped C++ lint passed (existing style warnings).
  All six real HCA control cases passed again at `r241-control/class-wiring.xml`.
- The first complete native gate failed the unchanged
  `SqFullBackpressureTest.UringEngineTinySqManyWrites`: five callbacks returned
  `-ENOMEM` at `sq_full_test.cpp:240`. Existing `DiskIOUring::submit_lockfree`
  returns that error when its 1000 yield attempts cannot obtain an SQ slot.
  The exact case, its 14-case suite and subsequent full native gate passed
  without source changes. Preserve initial evidence at
  `.crowdb-runtime/artifacts/unit/cpp-bdbe5707db274b8aa625247ce0eb00eb/`;
  this task does not claim to repair that existing scheduling sensitivity.

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

Do not hand off to R242 until Chunk-KV response assembly and TCP/RDMA semantic
parity pass for metadata and multi-message aggregate responses.
