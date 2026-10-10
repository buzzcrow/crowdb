<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R220: Dataset — Fat-client direct access plan

Status: Separate Dataset SDK project maintained by this team. Deferred until R219
is implemented and the CPU HTTP path has measured latency, throughput, memory,
and digest costs.

Problem: The SDK fat client can avoid HTTP routing and payload relay by connecting
directly to internal Chunk-KV and DiskIO, but it also takes responsibility for
routing freshness, retries, buffer lifetime, backpressure, and worker
partitioning. Implementing it before the HTTP baseline would make optimization
claims hard to measure and could duplicate authority logic.

Solution:

- Document the SDK direct-client protocol over the same Dataset authority and Read
  Plan contract used by HTTP.
- Compare direct Chunk-KV metadata reads and DiskIO/chunk range reads with the
  HTTP baseline, including routing refresh, stale topology, failure retry, and
  60-second active-read TTL behavior.
- Define owner-backed buffer lifetime, bounded prefetch, cache admission,
  cancellation, and checksum/MD5 verification without adding client-side global
  locks or unbounded queues.
- Analyze which work belongs in the client versus Chunk-KV, DiskIO, and Dataset
  library. The client cannot publish placement or bypass Dataset authority.
- Reserve GPU/direct-memory delivery for a later design after CPU direct access
  is measured and correct.

Dependencies: R219 HTTP baseline, R217 CPU E2E/container acceptance, and the
measured Chunk-KV/DiskIO read flows. Unlanded direct-access work remains a design
artifact; no native client is required for R213–R219 acceptance.

Acceptance:

1. Given HTTP and direct-client prototypes over the same snapshot, produce a
   comparison of p50/p99 latency, throughput, CPU, memory, and digest cost.
   Measurement contract. Integration test.
2. Given stale routing or a disconnected DiskIO endpoint, assert the fat client
   refreshes or retries without changing Dataset selection or snapshot identity.
   Non-authoritative topology invariant. Integration test.
3. Given a slow consumer, assert direct prefetch and in-flight buffers remain
   bounded and cancellation releases client buffers. Bounded delivery invariant.
   Integration test.

Resolved decisions:

- The core remains Rust. The separate SDK exposes a narrow C ABI over opaque
  handles first, then builds Python/PyTorch bindings over the same contract.
  C++ and RDMA wrappers remain later data-plane work.
- Client metadata is cached by immutable Snapshot ID and explicitly refreshed
  when resolving `latest`; no Dataset-specific global watch stream is required
  for the first version.

Open Issues:

- Define the C ABI ownership, error, cancellation, and buffer lifetime contract
  before implementing the fat client. Keep the ABI in the separate SDK project.

Verification commands:

- `pixi run rs-fmt-check`
- `pixi run rs-lint`
- `pixi run test-access-server`
- `pixi run test-unit`
