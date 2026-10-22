<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA Chunk-KV access Plan

Upstream: [R241](../backlog/R241-rdma-chunk-kv-access.md),
[root RDMA plan](plan-rdma.md), [registered buffers](../design/rpc/design-crowdb-rpc-rdma.md#4-registered-arenas-and-slot-ownership),
and [RDMA RPC transport](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: use the common RDMA stream for Access Server to Chunk-KV operations while
preserving existing aggregation, request numbering, generation checks, and TCP
semantic parity.

## Phase 1: access contract mapping

- [ ] **Map request/response types**: identify metadata, point, batch, range,
  and aggregate operations; record idempotency, request ID, generation, error,
  and retry behavior. Files: `lib/crowdb-chunk-kv-client/`, Chunk-KV server
  modules, and Access Server access-core modules.
- [ ] **Map aggregation owner**: identify where Chunk-KV combines responses and
  where it decides flush/end boundaries. RDMA must receive the aggregate after
  that decision, never create another aggregation layer.
- [ ] **Define class selection**: metadata defaults to 64 KiB, aggregated
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
