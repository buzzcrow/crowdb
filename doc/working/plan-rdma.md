<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB RDMA implementation Plan

Upstream: [R5](../backlog/R5-rdma-alloc.md),
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md), and child
backlogs [R240](../backlog/R240-rdma-datapath-stream.md),
[R241](../backlog/R241-rdma-chunk-kv-access.md), and
[R242](../backlog/R242-rdma-fat-client-sdk.md).

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

The final transport source `55ffee38` passes full C++/FFI gates, 137 no-verbs
cases, CLI/runner contract tests, real HCA lifetime/budget/fallback/disconnect
cases, formatting and workspace lint. Native and two non-root containers on
one shared HCA pass repeated 1 s warmup + 10 s measurement profiles. Median
RDMA throughput is 2.903M TPS native and 2.954M TPS in OCI, with zero errors or
fallback. The old 1.187M value labeled RDMA was TCP; matching current TCP
measures 1.196M TPS. Exact identities, settings and artifact names are retained
in [RPC flow analysis](../design/rpc/rpc-flow-analysis.md).

Current work is R240, then R241, using their existing plans. R242 and physical
multi-node validation remain outside the user's requested continuation.

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

1. Complete R240 using its existing datapath stream plan and service gates.
2. Complete R241 using its existing Chunk-KV/access plan and parity gates.
3. Keep R242 downstream until the user includes that scope.

Before modifying each service path, run its focused baseline corpus and record
it in that child's plan. Transport baseline, hardware topology, queue names and
OCI runtime preparation are already verified. Service acceptance must prove
actual RDMA, ownership through disk/caller completion, and bounded pressure;
transport echo acceptance alone does not prove those properties.

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
