<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB RDMA implementation Plan

Upstream: [R5](../backlog/R5-rdma-alloc.md),
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md), and the remaining
child backlog [R242](../backlog/R242-rdma-fat-client-sdk.md).

Goal: implement the host-memory RDMA SEND/RECV path end to end, with RDMA as
the preferred connection path, TCP as the one-way fallback, shared aggregation
semantics, bounded buffer leases, and automated OCI verification using a
single-node HCA local-loop with multiple containers.

This root plan persists until the umbrella goal is complete. Completed child
plans are deleted; remaining child plans retain their implementation detail.

## Current implementation status

Registered host buffers and the common RC SEND/RECV transport are accepted.
The shared arena has bounded lock-free admission, retained MR/PD/device
ownership, explicit leases and quarantine. TCP preflight binds CM setup to the
current endpoint and receive admission. QPs have one fixed polling/event owner
per CQ; bounded aggregation preserves complete frames. Strict RDMA CLI tests
share the coroutine workload and canonical results with TCP.

The final transport source `944274af` passes 543 tree, 192 RPC, 71 DiskIO and
35 common native cases plus both FFI suites, 160 no-verbs CTest cases, fourteen
real-HCA cases, formatting and scoped C++ lint. The unchanged Rust integration
passes 1,244 affected cases, workspace all-feature clippy and `rs-lint` at
`018da2fc`. Native and two non-root containers on one shared HCA pass all 26
final 1 s warmup + 10 s measurement trials. Median RDMA throughput is 3.027M
TPS raw-server and 3.237M OCI, with zero errors/fallback. Matching TCP medians
are 1.203M/2.168M raw-server and 1.095M/2.113M OCI with Nagle off/on. Exact
settings, identities, latency tails and the old TCP mislabeling are retained
in [RPC flow analysis](../design/rpc/rpc-flow-analysis.md) and both benchmark
wrapper scripts.

The HTTP/ChunkIO/DiskIO datapath is accepted: immutable registered HTTP owners,
aligned receive-to-disk handoff, read reuse through SEND completion, caller-held
receive credits, retained asynchronous service owners and terminal accepted-RDMA
failures. A 3,158,073-byte HTTP corpus crosses two chunks and matches
TCP/native/OCI bytes and geometry. All five functional profiles also pass the
common-control regression at `018da2fc`; peer-close fails a partial download
without a TCP suffix. The fixture injects placement/catalog state and records
its actual io_uring/OCI BlockingEngine selection.

Chunk-KV/access integration is accepted with distinct 64 KiB metadata and
256 KiB aggregate connection groups sharing request correlation. Protocol v4
supports bounded logical controls beyond one RQ window, independent byte
budgets, negotiated capacity and final-caller admission ownership. The real
Chunk-KV service/tree/WAL corpus verifies eight values totalling 3,409,975 bytes,
typed errors, mutation-ID replay and exact TCP/RDMA equality in seven native/OCI
profiles. Partial-fragment peer close publishes no incomplete result or TCP
suffix. Catalog/serving authority/memory ChunkStream backing are injected;
Access construction is covered by the affected Rust suites. Formal service
contracts and complete acceptance evidence are linked from the RDMA design
and RPC flow analysis.

The requested foundation, transport, datapath and Chunk-KV continuation is
complete. Only the fat-client SDK child remains. R242 and physical multi-node
validation are outside the completed continuation; no SDK implementation is
started by this plan update.

## Fixed design decisions

- Use one common RPC abstraction for TCP and RDMA.
- Use RC SEND/RECV. The accepted contiguous RPC layout uses one wire slot
  and one SGE per message. The accepted datapath adds a negotiated control/data layout with
  separate SGEs for immutable producer payload and per-target control; its
  total wire admission includes both regions.
- Use service discovery for coarse RDMA capability and TCP handshake for the
  current RDMA endpoint, capability generation, and data classes.
- Select RDMA before the first payload; fallback is RDMA to TCP only.
- Preserve existing ChunkIO and Chunk-KV aggregation. Additionally permit
  bounded transport coalescing of complete RPC frames without a batching timer;
  every SEND fits both the local send slot and peer receive slot, including
  headers, and retains each RPC's identity and completion ownership.
- Assign each QP to one CQ and each CQ to one fixed polling/event worker.
  Use `crowdb-cli bench rdma` and the shared RPC regression runner to report
  QP count, slot size, aggregation and errors.
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

## Remaining dependency order

```text
Accepted registered host buffers and common RPC
                  |
Accepted HTTP/ChunkIO/DiskIO and Chunk-KV/access
                  |
                  v
       R242 fat client SDK and FFI lease
```

The remaining child uses the existing [fat-client SDK plan](plan-rdma-fat-client-sdk.md)
when the user includes that scope. Do not create a duplicate plan or repeat the
completed service integration. The shared benchmark/functional `Runtime`
already launches native, no-device TCP OCI and shared-HCA OCI profiles.

Before changing the SDK frontend, run its focused baseline corpus, retain the
accepted transport/service settings and compare exact result bytes and errors.
Transport echo acceptance alone does not prove SDK lease semantics.

## Phase 4: R242 fat client SDK

- [ ] **Execute R242 child plan**: split access core from HTTP frontend, expose
  FFI-safe host leases, and reuse the accepted Chunk-KV/ChunkIO transports.
- [ ] **Host-to-GPU simulation**: hold each lease until the explicit host-to-GPU
  copy completes, then release it; no GPU pointer is exposed by the base API.
- [ ] **SDK pressure tests**: hold all returned leases, verify bounded admission,
  cancellation, shutdown, and recovery after release.

Exit condition: native SDK and HTTP frontend return equivalent access results,
and the caller-controlled lease lifetime is visible and testable through FFI.

## Phase 5: SDK OCI E2E matrix

- [ ] **SDK TCP-only profile**: extend the shared launcher with the SDK corpus,
  omit RDMA devices and require zero RDMA attempts with exact frontend parity.
- [ ] **SDK shared-HCA profile**: run the SDK corpus in two non-root host-network
  containers, requiring independent QPs, retained caller leases, bounded
  pressure and setup-only TCP fallback.
- [ ] **SDK topology selection**: reuse existing runtime/readiness/report
  contracts for the SDK fixture when each profile's prerequisites are present.

Exit condition: SDK profiles report topology, actual path/classes/QPs, fallback,
held lease pressure, typed errors and exact frontend result equality. Existing
HTTP/DiskIO and Chunk-KV native/OCI corpora remain regression controls.

## Invariants to retain in SDK work

The accepted transport/service tests already cover these properties. Reapply
them to the new SDK frontend and its caller-controlled leases:

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
pixi run clean-env && pixi run test-cpp
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
cannot provide RDMA, continue portable SDK/TCP checks and mark only hardware
E2E as pending.
