<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB RDMA implementation Plan

Upstream: [R5](../backlog/R5-rdma-alloc.md),
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md), and child
backlogs [R238](../backlog/R238-rdma-registered-buffer-foundation.md),
[R239](../backlog/R239-rdma-rpc-transport.md),
[R240](../backlog/R240-rdma-datapath-stream.md),
[R241](../backlog/R241-rdma-chunk-kv-access.md), and
[R242](../backlog/R242-rdma-fat-client-sdk.md).

Goal: implement the host-memory RDMA SEND/RECV path end to end, with RDMA as
the preferred connection path, TCP as the one-way fallback, shared aggregation
semantics, bounded buffer leases, and automated OCI verification using a
single-node HCA local-loop with multiple containers.

## Current implementation status

R238's foundation implementation is committed. The common RPC buffer layer and verbs-backed per-connection
arenas share the four capacity classes, bounded admission, explicit owner
transitions, safe release ordering, quarantine handling, shutdown quarantine,
and C/Rust lease metadata accessors. R239's transport path is implemented and
passes the prepared HCA
local-loop plus the real `RpcServer`/`RpcClient` worker round trip. Its
requirement is still open because negotiated capability preflight,
pre-payload fallback, aggregation/CLI benchmarking and
multi-container acceptance have not all passed. Live HCA tests now verify
held registered/receive leases after pool, transport and CM-ID destruction;
the final release deregisters MR before PD release. Application reply allocation
is separate from receive admission, and every CQ has a fixed polling/event
owner. The transport plan records the corresponding gates and diagnostics.

The latest transport handoff closes the completion ownership gap: SEND and
RECV work requests now carry stable connection/request/stream metadata, receive
leases remain caller-owned until their final C++/FFI reference is released
before repost, and CQ send failures close the uncertain RDMA stream after quarantining
its buffers. The worker test checks a non-empty response payload as well as
request correlation. The current local-loop runtime is prepared. The single-worker baseline with
1024 receive WRs and small wire slots passes. The staged 1/2/4/8/16/32
producer/QP profiles now each run for ten measured seconds after warmup,
with zero errors after fixing CM event ownership and CQ continuation races.
The 1024 and 4096 RQ profiles, full C++ gate, Rust RPC FFI suite, formatting,
clippy, and tree-lint pass. The transport plan records exact counts, latency,
and the fixed-I/O-worker control; protocol and OCI acceptance remain open.

This is the root plan. The five child plans contain implementation details;
only this plan controls phase order, cross-backlog decisions, and final
acceptance. The agreed joint CQ/benchmark/aggregation work is detailed in
[the transport plan](plan-rdma-rpc-transport.md#agreed-implementation-sequence).
Update the formal design with each implemented architecture change.

## Fixed design decisions

- Use one common RPC abstraction for TCP and RDMA.
- Use RC SEND/RECV, one contiguous wire slot and one SGE per message.
- Use service discovery for coarse RDMA capability and TCP handshake for the
  current RDMA endpoint, capability generation, and data classes.
- Select RDMA before the first payload; fallback is RDMA to TCP only.
- Preserve existing ChunkIO and Chunk-KV aggregation. Additionally permit
  bounded transport coalescing of complete RPC frames without a batching timer;
  every SEND fits both the local send slot and peer receive slot, including
  headers, and retains each RPC's identity and completion ownership.
- Assign each QP to one CQ and each CQ to one fixed polling/event worker.
  Implement this together with `crowdb-cli bench rdma` and the existing RPC
  regression runner; report QP count, slot size, aggregation and errors.
- Use 64 KiB metadata, 256 KiB aggregated Chunk-KV, and 1 MiB datapath
  capacities, configured as transport defaults.
- Use posted receives as implicit data credit; do not add an extra credit
  protocol.
- Expose host buffer leases first. GPU delivery initially means an explicit
  host-to-GPU copy. RDMA READ/WRITE is outside this implementation.
- Use `setup-rdma.sh local` as the standard host setup. A single node runs
  client and server through the same HCA port and local IP; multiple containers
  on that node use the same local-loop model and open independent QPs.
- Treat `run-local-loop.sh local` as the smoke test for every local hardware
  setup. The current acceptance scope is one physical node with multiple
  containers sharing the configured HCA and opening independent QPs.
- Use host networking only for the multi-container profile. Assign each
  container a `CROWDB_PORT_OFFSET`; apply it uniformly to all default
  TCP/RPC and RDMA CM service ports, while keeping the HCA physical port
  unchanged.
- Physical multi-node testing is deferred to [the separate follow-up plan](plan-rdma-multi-node.md)
  and is not part of the current backlog scope.

## Dependency graph

```text
R238 registered buffers
        |
        v
R239 common RDMA RPC transport
       / \
      v   v
R240   R241
datapath Chunk-KV/access
       \ /
        v
R242 fat client SDK and FFI lease
```

## Immediate implementation order

User continuation on 2026-10-22: complete R238 and R239, then continue through
R240 and R241 in this same task. Finish transport/benchmark/OCI acceptance
before advancing; implement R240, then R241, using their existing
[datapath stream plan](plan-rdma-datapath-stream.md) and
[Chunk-KV/access plan](plan-rdma-chunk-kv-access.md), with their own gates.
Do not create duplicate plan files. R242 remains outside this requested continuation.

1. Finish the R238 foundation contract and its gates. Do not add new R239
   transport behavior until the common and verbs-backed pools agree on class,
   ownership, quarantine, and shutdown semantics.
2. Close R239 acceptance gaps: injectable CM/CQ failure coverage, stable
   `wr_id` ownership mapping, pre-payload retry on TCP, partial-transfer
   isolation, and the single-node multi-container OCI profile.
3. Run the R238/R239 handoff gates, complete R240, then complete R241.
   R242 remains downstream of R241 and the stable FFI lease surface.

R240 and R241 start after R239 passes its transport acceptance.
R242 starts only after R241 establishes the access semantics and after the FFI
lease contract from R238 is stable.

## Phase 0: baseline and decision lock

- [x] **Lock hardware test model**: use `setup-rdma.sh local` and
  `run-local-loop.sh local` for single-node validation; do not use namespace
  setup in product or acceptance tests. Multiple containers on one node share
  the local HCA loop and open independent QPs.
- [ ] **Record baseline**: run the existing TCP RPC, Access Server, DiskIO, and
  Chunk-KV gates before modifying production code. Root files: affected crate
  manifests and CI configuration.
- [ ] **Confirm host capabilities**: record `ibv_devices`, `rdma link`, the
  selected HCA port, GID/IP mapping, and whether the local HCA loop is usable.
  Record the container-to-QP mapping for the single-node shared-HCA run.
  Do not treat an unavailable host device as an implementation failure.
- [ ] **Lock configuration names**: agree on RDMA enablement, device/port/local
  address, control/data class defaults, registered bytes, receive depth, and
  fallback policy. Root files: configuration schema and formal design.
- [ ] **Prepare OCI image**: include `libibverbs`, `librdmacm`, and provider
  libraries; keep a TCP-only image invocation that exposes no RDMA device.
  Root files: Dockerfile/OCI test scripts and CI documentation.

Exit condition: TCP baseline passes, the host topology is recorded, and the
configuration contract is stable. If the host has no RDMA, continue with the
TCP-only profile and build/mocked tests.

## Phase 1: R238 registered buffers

- [x] **Close R238 pool contract**: align `RegisteredBufferPool` and the
  verbs-backed `RdmaBufferPool` on the four classes, lease metadata, owner
  transitions, quarantine accounting, and bounded shutdown. Keep registration
  optional and do not introduce a new lock on the allocation/recycle path.
  Files: `lib/crowdb-rpc/include/crowdb-rpc/buffer.h`,
  `lib/crowdb-rpc/src/buffer.cpp`, and
  `lib/crowdb-rpc/src/transport/rdma/rdma_transport.*`.
- [x] **Run R238 gates**: unit and integration tests pass with RDMA disabled
  and enabled build configurations, including registration failure and
  uncertain completion cases.
- [x] **Review handoff**: verify that R239 can obtain control/data buffers and
  that every asynchronous owner has a release transition.
- [x] **Close portable pool parity**: share the verified arena lifetime and
  tagged-index freelist machinery with the no-verbs registered pool, then
  verify concurrent bounds, held leases, quarantine and detach without an HCA.
  The detailed transport plan owns this continuation before aggregation.
- [x] **Bound retired registration admission**: keep registered bytes charged
  to the existing device owner while old caller leases retain their MRs, reject
  failed MR setup before publishing a connection, and verify the current/high
  water budget and rejection counters. The transport plan owns this final
  resource admission continuation.

Exit condition: no buffer can be recycled while transport, disk, or caller
ownership remains unresolved.

## Phase 2: R239 common RDMA RPC

- [ ] **Close R239 acceptance**: add mock/integration coverage for CQ
  success/error/flush, CM disconnect/QP error, receive exhaustion, stable
  `wr_id` ownership, pre-payload fallback, and no mixed TCP/RDMA stream.
- [x] **Local-loop verification**: run `setup-rdma.sh local` followed by
  `run-local-loop.sh local`; both CM roles, framed SEND/RECV, receive repost,
  and exact payload lengths have passed on the prepared HCA.
- [ ] **Container local-loop verification**: start multiple containers on one
  node after the local setup; verify each container opens independent QPs over
  the same HCA port and that request correlation and bounded buffers remain
  correct.

Exit condition: R239 can carry a framed stream, report completion to the common
  RPC layer, and leave TCP fully usable when RDMA is absent or fails.

## Phase 3: R240 and R241 service integration

- [ ] **Execute R240 child plan**: connect ChunkIO-produced 1 MiB buffers to
  Access Server/DiskIO, retain DiskIO buffers through disk completion, and test
  large multi-message streams.
- [ ] **Execute R241 child plan**: connect Access Server/Chunk-KV with 64 KiB
  metadata and 256 KiB aggregate classes, preserving existing request IDs,
  ordering, and retry semantics.
- [ ] **Run semantic parity**: for the same test corpus, compare TCP and RDMA
  results byte-for-byte and compare errors, sequence, generation, and
  cancellation behavior.

Exit condition: S3/DiskIO and Chunk-KV paths use RDMA without a second
aggregation layer, without an intermediate CPU payload copy, and with TCP
fallback before payload submission.

## Phase 4: R242 fat client SDK

- [ ] **Execute R242 child plan**: split access core from HTTP frontend, expose
  FFI-safe host leases, and connect SDK operations to R241/R240 transports.
- [ ] **Host-to-GPU simulation**: hold each lease until the explicit host-to-GPU
  copy completes, then release it; no GPU pointer is exposed by the base API.
- [ ] **SDK pressure tests**: hold all returned leases, verify bounded admission,
  cancellation, shutdown, and recovery after release.

Exit condition: native SDK and HTTP frontend return equivalent access results,
and the caller-controlled lease lifetime is visible and testable through FFI.

## Phase 5: OCI E2E matrix

- [ ] **TCP-only profile**: run OCI containers without RDMA devices and assert
  service discovery skips RDMA entirely.
- [ ] **Single-node multi-container profile**: run `setup-rdma.sh local`, start
  multiple containers on one physical host, share the configured HCA, and
  assert independent QPs, request correlation, bounded buffers, and TCP
  fallback.
- [ ] **Automated local topology selection**: make the test runner execute the
  TCP-only and single-node local-loop profiles when their prerequisites are
  present.

Exit condition: every profile reports its topology, RDMA capability, selected
device, selected path, fallback count, buffer pressure, and final result.

## Cross-phase invariants

- [ ] **No mixed stream**: never append TCP bytes to a partially completed RDMA
  stream.
- [ ] **Bounded aggregation**: application message boundaries remain intact;
  transport coalescing preserves complete frames, order and per-RPC identity
  and stays within both peers' slot limits.
- [ ] **No premature reuse**: completion, disk completion, and caller release
  all precede buffer reuse for their respective ownership path.
- [ ] **Bounded memory**: registered bytes, posted receives, send depth, disk
  queue bytes, and SDK-held bytes remain within configuration.
- [ ] **Capability optionality**: a host or OCI container without RDMA remains
  a supported TCP deployment.
- [ ] **NIC ownership**: each single-node container uses the explicitly
  configured shared HCA; no test depends on moving a link or entering a network
  namespace.
- [ ] **Shared identity**: request ID, generation, stream sequence, and logical
  offset retain their meaning across TCP and RDMA.

## Final verification

Unit and integration gates:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc --all-targets
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo test -p crowdb-diskio-client --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

OCI E2E verification must run the TCP-only and single-node HCA local-loop
multi-container profiles when their prerequisites are available. Physical
multi-node verification is outside this plan and is covered by the separate
follow-up plan. Namespace profiles are not part of acceptance.

## Test and setup files

- `tools/rdma/setup-rdma.sh`
- `tools/rdma/run-local-loop.sh`
- OCI image and multi-container runner configuration

## Handoff and blocking rules

Each child plan is marked `[x]` only after its own acceptance cases and gates
pass. If a child discovers a protocol ambiguity, stop that child, record the
question here, and resolve it before changing the formal design. If the host
cannot provide RDMA, do not block R238/R239 mock work or TCP fallback tests;
mark only hardware E2E as pending.
