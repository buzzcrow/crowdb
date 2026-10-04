<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R5: RDMA — Registered host buffers and scheduled client writes

## Status

Design proposal, not implemented. The original low-priority allocator seam
remains part of this requirement; the tree-specific RDMA `BlockPageStore`
medium is still not started. Its absence does not prevent a separate host-memory
transfer prototype. No acceptance result or production throughput is claimed.

## Problem

GPU compute needs large logical object ranges from storage. Without usable
GPUDirect RDMA, data must land in client host memory before a Host-to-Device
CUDA copy. Ordinary TCP RPC can add CPU protocol work and intermediate copies;
RDMA permits the RNIC to transfer payload directly between registered buffers.
It does not remove disk I/O, checksum work, network traffic, or the CUDA copy.

The original `buffer::allocate` seam in
`lib/crowdb-tree/include/crowdb-tree/buffer.h` is intended to permit an allocator
other than `std::malloc`. Registered-memory allocation additionally needs an
owner, registration per RDMA device/protection domain, byte limits, and a
lifetime extending beyond every asynchronous operation. It cannot be modeled
as merely returning a different pointer from malloc.

The deployment has two useful paths:

- **AccessServer relay:** DiskIO transfers to AccessServer host memory;
  AccessServer writes to the client's host buffer. RC connection counts are
  controlled, and the gateway can remain the mandatory network boundary.
- **DiskIO direct write:** AccessServer forwards an authorized destination
  descriptor and read plan; DiskIO reads, validates, and writes directly to the
  client. This removes one payload transfer and the gateway memory bounce.
  RC is sufficient when connections are bounded; DC is an optional scaling
  mechanism, not a prerequisite for direct transfer.

The core transport contract is [RPC RDMA](../design/rpc/design-crowdb-rpc-rdma.md).
This design is independent of cuObject: registered allocation, span scheduling,
relay/direct topology, and completion ownership do not require its protocol or
libraries. A native client library may use RC or DC directly.

**cuObject is one optional adapter**, described by
[R170](R170-s3-cuobject-rdma.md) and
[Access Server accelerated transfer](../design/access-server/s3/design-crowdb-access-s3-rdma.md).
Those documents constrain only the cuObject option; their no-gateway-payload
invariant does not prohibit the generic relay path defined here. No transport
adapter may silently switch paths after a partially executed operation.

## Solution

### 1. Registered allocation and ownership

1. Preserve the tree allocator seam. Implement transport buffers through the
   native `lib/crowdb-rpc` / `crowdb-rpc-ffi` ownership boundary; DiskIO and
   AccessServer borrow owners rather than passing naked pointers across FFI.
   Tree-medium integration remains conditional on that medium being designed.
2. Maintain bounded, reusable registered host-buffer pools in participating
   processes. Distinguish CUDA pinned host memory from RDMA memory registration:
   they serve different devices and neither automatically establishes the other.
   Client buffers used for asynchronous CUDA copy need both. AccessServer and
   DiskIO staging buffers need RDMA registration but not CUDA registration.
3. Register a relay buffer separately for each participating RDMA device/PD.
   A buffer may use the same virtual allocation for backend receive and frontend
   send, with distinct local registrations. Do not forward local handles/lkeys
   to another process or assume keys are interchangeable across devices.
4. Track pool byte usage, in-flight transfers, queue depth, and CUDA-copy users.
   Reuse pools to amortize registration; apply admission/backpressure before
   accepting work that exceeds configured limits. No new hot-path lock is
   selected by this design.

### 2. Control request and RC relay

The client prepares its destination and sends an ordinary authenticated request
with object/range, operation ID, capacity, and destination descriptor. The
request is distinct from an RDMA Read: a Read needs a remote address/rkey and
reads already prepared remote memory; it does not trigger application disk I/O.

```text
Compute client          AccessServer                 DiskIO
     | request + destination |                         |
     |---------------------->| authenticate/plan        |
     |                       |--- prepare range ------->|
     |                       |                         | disk read/checksum
     |                       |<-- ready source span ----|
     |                       |=== backend RDMA Read ===>| registered source
     |                       |<====== payload ==========|
     |<== frontend Write ====| registered relay buffer |
     | completion + result   |                         |
     | CUDA copy -> kernel   |                         |
```

5. `crowdb-access-server` authenticates and resolves a stable logical generation
   using existing S3/chunk-reader semantics. `crowdb-diskio` reads into a local
   registered buffer and validates the relevant storage integrity metadata
   before declaring the source ready. Merely computing a new checksum without
   comparing trusted expected integrity information does not validate old data.
6. AccessServer waits for successful backend Read completion before submitting
   frontend Write. DiskIO retains the source until backend transfer completion
   is confirmed through the control protocol. AccessServer retains its relay
   buffer until the frontend send completion. A separate outbound copy is not
   needed if that allocation is registered for the sending RNIC.
7. Use a defined notification mechanism. With Write with Immediate, the client
   posts Receive WQEs before issuing the request; the Write WR selects the
   destination address, while the Receive WQE provides notification capacity.
   With ordinary Write, use an explicit correctly ordered completion protocol.
   Storage submits the Write WR, not the receiving client.

At verbs level, an outbound Write names a local SGE (`addr`, `length`, `lkey`)
and remote destination (`remote_addr`, `rkey`). RC can submit it through
`ibv_post_send()` with `IBV_WR_RDMA_WRITE` or
`IBV_WR_RDMA_WRITE_WITH_IMM`. Submission success means queued, not completed.
[Public API documentation](https://man7.org/linux/man-pages/man3/ibv_post_send.3.html)

### 3. Optional direct write and scheduler

```text
Compute client          AccessServer                 DiskIO A/B
     | request + destination |                         |
     |---------------------->| authenticate/plan        |
     |                       |-- authorized spans ---->|
     |<====================== direct RC/DC Write ======|
     |                       |<-- span completions ----|
     |<-- aggregate result --|                         |
```

8. The coordinator groups source spans by DiskIO node and sends bounded batches
   rather than one control RPC per fragment. Each task includes operation,
   tenant, generation, logical source, destination offset, exact length, expiry,
   and authority for that span. Storage physical layout remains internal.
   Mirrored reads choose a healthy replica; EC reads reconstruct logical bytes
   before writing object offsets. Metadata does not expose parity as object data.
9. Each DiskIO reads and validates its spans, then submits multiple Writes to
   one destination allocation. For a 4 MiB destination, node A may write two
   1 MiB spans at offsets 0 and 1 MiB, and node B at offsets 2 and 3 MiB.
   Every WR specifies its own source and `destination_base + offset`. Check
   arithmetic overflow, MR bounds, operation bounds, and non-overlap before
   submission. A shared rkey may cover all these offsets; application span
   authority is still narrower than the MR's hardware permissions.
10. Reuse long-lived RC QPs when endpoint counts fit configured resource limits.
    DC descriptors contain endpoint/path information as well as memory access
    information; local DCI/channel handles stay local. On the sending DiskIO,
    batching by destination client/DCT can reduce target changes. Grouping by
    source node alone reduces control work, not necessarily DCI target changes.
    Use bounded fairness so a busy destination cannot monopolize resources.
11. Direct writes require an allowed, reachable RDMA path from DiskIO to client.
    DC does not bypass routing, authentication, or network isolation. If policy
    requires all payload to cross AccessServer, select the relay path explicitly.
    Keep the scheduler and buffer ownership independent of adapters. A native
    RC/DC adapter uses its own defined destination descriptor; if cuObject is
    selected, follow R170's opaque-descriptor and distributed validation
    contracts. Do not assume these descriptors or protocols are interchangeable.

DC is publicly exposed through rdma-core's mlx5 extensions and UCX, but depends
on supported hardware/firmware. It is not a portable replacement for RC on
arbitrary RNICs. DCI availability, target changes, handshake configuration,
and completion draining can all affect cost; no fixed microsecond overhead is
assumed. [DC API](https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/man/mlx5dv_wr_post.3.md),
[UCX scheduling and handshake configuration](https://networking-docs.nvidia.com/hpcxum/2.50/unified-communication-x-framework-library).

### 4. Completion, failure, and lifetime

12. Track every span independently; writes from different nodes may complete in
    any order. Return full-operation success only after all required spans and
    transfer notifications succeed. The client starts CUDA copy only after the
    selected interval is known complete, and starts its kernel after the CUDA
    dependency. Release/reuse client host storage only after the copy completes.
13. Preserve source/MR/channel ownership until local DMA access has ended.
    A timeout invalidates the request but does not prove that remote Writes have
    stopped. Cancellation, connection teardown/drain, or access revocation must
    establish quiescence before destination reuse. An operation ID in a late
    notification alone cannot prevent a stale RNIC write into reused memory.
14. Bind delegation to tenant, generation, direction, interval, nonce, and
    expiry. Never treat address/rkey as object authorization or log capabilities.
    Rejection before submission may choose a separately negotiated fallback;
    after partial transfer fail the operation and keep the destination invalid
    until quiescence. Do not splice fallback bytes into partial RDMA results.

### 5. Network cost and production verification

The production scenario discussed here is **40 GB/s payload throughput**, not
40 Gb/s and not the development machine's rate. With 1 MiB transfers this is
about 38,147 transfers/s and 26.2 microseconds of ideal payload transmission
per transfer. These are arithmetic bounds, not DC measurements; line rate must
also accommodate headers and other overhead. Multiple in-flight operations may
hide some setup latency, so serialized per-request timing is not a throughput
prediction.

15. Measure relay and direct paths under the same production workload. Relay
    needs approximately 40 GB/s payload on each leg and adds roughly 40 GB/s
    relay-memory writes plus 40 GB/s reads, excluding checksum and other work.
    Separate physical backend/frontend fabrics isolate link consumption but
    still converge at AccessServer NICs, PCIe, and memory. On one shared fabric,
    the extra internal leg consumes shared resources; exact contention depends
    on paths and topology, not an automatic halving of every link's bandwidth.
16. Pipeline bounded chunks so backend receive, frontend send, and client CUDA
    processing overlap. Measure storage throughput, checksum CPU cost, NIC and
    PCIe utilization, memory bandwidth, registered bytes, CQ/queue pressure,
    throughput and p99 completion latency. Compare fixed-target RC, fixed-target
    DC, and DC changing target per 1 MiB at representative concurrency. Set an
    environment-specific SLO before claiming the 40 GB/s goal is met.

As of 2026-10-04, the development host reports Huawei OEM ConnectX-5,
board ID `HUA0000000004`, firmware `16.21.3002`, MLNX_OFED
`24.10-5.1.6.1`, kernel `6.8.0-142-generic`, and UCX `1.18.0`.
Its two active Ethernet ports report 25 Gb/s. UCX exposes RC/UD but no DC
transport; a minimal temporary DCI creation probe returned `EOPNOTSUPP` on both
RDMA devices. This is a limited probe, not proof of OEM disablement or a firmware
root cause. Recheck creation parameters/capabilities and the OEM PSID support
matrix before choosing a matching firmware update. Production hardware remains
separate. The local UCX build reports `--without-cuda`; host staging experiments
must therefore manage the CUDA copy separately rather than assume UCX GPU support.

## Dependencies

- Existing `lib/crowdb-rpc`, `crowdb-rpc-ffi`, `crowdb-diskio`,
  `crowdb-access-server`, and chunk-reader ownership/integrity contracts.
- [R4 bounded mempool](R4-bounded-mempool.md): coordinate budgets without making
  RDMA registrations an unbounded side pool.
- cuObject and R170 are optional, not prerequisites for this requirement. Only
  selecting that adapter introduces its libraries, support matrix, and base S3
  milestone dependencies. Native RC/DC development can proceed independently.
- RDMA-capable devices and compatible drivers on participating hosts; CUDA
  pinned-host support on compute clients. GPUDirect is a future optional path,
  not a prerequisite. Unsupported builds retain the ordinary TCP service.
- Tree-specific RDMA `BlockPageStore` remains future work; preserve the allocator
  seam without claiming that transport registration implements that medium.

## Acceptance

- Given bounded registered pools, when allocations reach their limits or a
  device registration fails, assert admission fails cleanly and all completed
  owners return resources without altering the tree allocator seam. Invariant:
  bounded allocation and exception-safe ownership. Integration test.
- Given a relay with two RDMA devices, when DiskIO prepares valid bytes and
  AccessServer transfers them, assert separate registrations work and frontend
  submission follows backend completion without an extra outbound memcpy.
  Invariant: device-specific registration and ordered relay. Integration test.
- Given corrupted disk bytes and trusted checksum metadata, when DiskIO prepares
  a read, assert failure before publishing ready or submitting client Write.
  Invariant: validated logical bytes. Integration test.
- Given prepared notification capacity, when storage uses Write with Immediate
  or the chosen ordinary-Write completion protocol, assert exact destination
  bytes and completion before client consumption. Invariant: submission is not
  completion. E2E test.
- Given four 1 MiB spans on two DiskIO nodes, when each executes a grouped task
  with multiple Writes, assert exact disjoint offsets, one coherent generation,
  mirror/EC logical semantics, and success only after all spans complete in any
  order. Invariant: complete logical range assembly. E2E test.
- Given malformed, overlapping, overflowing, expired, replayed, wrong-tenant,
  or wrong-generation spans, when delegation is checked, assert rejection before
  submission and no capability leakage. Invariant: bounded authority. Integration test.
- Given network policy forbidding DiskIO-client access, when a request selects
  its transport, assert explicit generic relay or rejection, with no hidden
  cuObject relay. Invariant: policy and transport-contract preservation. E2E test.
- Given an in-flight timeout, late Write, partial failure, shutdown, or pending
  CUDA copy, when cleanup runs, assert no buffer/MR reuse before quiescence,
  no partial fallback mixing, and no kernel consumption of incomplete data.
  Invariant: lifetime covers every device access. E2E test.
- Given supported RC/DC production equipment, when running fixed and rotating
  targets with 1 MiB transfers, assert configured pool/queue bounds and record
  throughput, p99, and per-leg resource usage against the agreed SLO. Invariant:
  performance claims are measured, not inferred from API availability. E2E test.

## Open Questions

- Which deployments require gateway payload isolation, and which permit
  authenticated DiskIO-client direct writes? This selects relay versus direct.
- What is the first implementation boundary: a generic internal RC prototype
  or an external client library? Implement the shared buffer/scheduler core
  independently; native RC/DC and cuObject are alternative adapters, and
  cuObject is selected only when its client compatibility is needed.
- Which integrity algorithm, client notification protocol, and quiescence
  mechanism are selected? These must fit existing storage and transport semantics.
- Which production NICs, topology, concurrent clients, DCI budget if enabled,
  and p99 target define the 40 GB/s acceptance environment?

Required implementation gates (not run for this documentation-only change):

```bash
pixi run test-rpc-ffi
pixi run test-diskio-ct
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo fmt --all -- --check
pixi run rs-lint
pixi run tree-lint
```
