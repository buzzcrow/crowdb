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
- [~] **Negotiate and configure common streams**: add a small transport-owned
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
- [ ] **Integrate CQ-owned fragmentation and assembly**: enqueue one original
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
- [ ] **Wire Chunk-KV classes and typed settings**: add service defaults and
  explicit RDMA configuration for metadata/aggregate connection groups,
  preserve TCP defaults and current serializers/errors/retries, and wire the
  Chunk-KV server plus Access storage construction. Cover point, mutation,
  seek, bounded scan, multi-get and batch mutation, including duplicate result
  order and stable logical mutation IDs. Files: client transport/config,
  server startup/RPC boundary, Access storage config and tests.
- [ ] **Verify real service and clean up**: test malformed sequence/offset,
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
