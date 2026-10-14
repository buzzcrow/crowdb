<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R220: Dataset — Fat-client direct access plan

Status: Deferred until R231/R232 deliver the public HTTP SDK and measured CPU
HTTP latency, throughput, memory, and digest costs. The SDK project lives under
`sdk/` in this repository with independent packages, as defined by R232.

Problem: The SDK fat client can avoid HTTP routing and payload relay by connecting
directly to internal Chunk-KV and DiskIO, but it also takes responsibility for
routing freshness, retries, buffer lifetime, backpressure, and worker
partitioning. Implementing it before the HTTP baseline would make optimization
claims hard to measure and could duplicate authority logic.
The root contract is [Dataset design](../design/access-server/dataset/design-crowdb-access-dataset.md).

Solution:

- Implement direct access in the Rust SDK core and expose it through the same
  Python wrapper established by R232. Routing, buffers, retries and optional
  RDMA stay in Rust; Python does not implement another data plane. Additional
  C/C++ bindings can be added later without making a C ABI a prerequisite.
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
- Use explicit server capabilities and authorization for direct Chunk-KV/chunk
  access. Direct access never grants unrestricted internal metadata mutation.
  Keep HTTP available for unsupported capabilities; choose transport before
  starting an operation and never replay an ambiguous mutation on a new path.

Dependencies: R231 public REST and R232 packaged HTTP SDK baseline, existing CPU
E2E/container acceptance, and measured Chunk-KV/DiskIO read flows. R233 caching
is optional. Unlanded direct access does not block the first HTTP SDK release.

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
4. Given unsupported direct capability or denied direct credentials, select
   transport and attempt access; assert HTTP fallback where authorized, no
   authority bypass and no cross-transport replay of ambiguous writes.
   Transport authority invariant. Integration test.
5. Given the same direct read through Rust and the Python wrapper, cancel and
   close the read while retaining returned data; assert equivalent logical
   results, valid buffer lifetimes and isolated cleanup. Binding invariant.
   Integration test.

Verification commands:

- `pixi run rs-fmt-check`
- `pixi run rs-lint`
- `pixi run test-access-server`
- `pixi run test-unit`
