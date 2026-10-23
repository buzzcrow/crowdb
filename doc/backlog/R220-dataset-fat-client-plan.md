<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R220: Dataset — Fat-client direct access plan

## Problem

Trusted internal data-processing and AI systems need Dataset semantics over
direct Chunk-KV and ChunkIO/DiskIO access, with exposed storage topology for
scheduling. Routing freshness, retries, buffer lifetime, backpressure, and
worker partitioning must remain correct without duplicating authority logic.
Performance claims need an HTTP workload baseline.

The root contract is [Dataset design](../design/access-server/dataset/design-crowdb-access-dataset.md).
Reuse the [native runtime contract](../design/access-server/design-crowdb-native-sdk.md)
and [SDK API guide](../../lib/crowdb-sdk-native/README.md).

## Solution

1. Attach Dataset to `ClientRuntime` in the main workspace. Reuse
   `TopologyView`, `OperationContext`, `BufferLease`, frozen producer storage
   and resource admission; do not introduce
   another runtime or make the independent HTTP SDK depend on storage FFI.
   The fat client shares Access Server's internal trust domain, takes explicit
   Dataset identity/context, and needs neither SigV4 verification, the S3
   user-secret database, nor external-user capability issuance.
2. Reuse `crowdb-access-dataset` authority, sessions, composition, Split, and
   upload logic. Preserve Snapshot/Read Plan binding, selection, worker
   partitioning, integrity, cleanup, and deletion admission. Topology is
   a supported scheduling surface, not placement authority.
3. Preserve explicit acknowledgement and resume: fetching data, releasing a
   buffer, and acknowledging processing are separate events. Native delivery
   uses the shared owner-backed streams and bounded prefetch. Cancellation
   stops producers; caller-held leases remain valid and charged until release.
4. Compare native Chunk-KV and chunk-range reads with the HTTP baseline,
   including topology refresh, stale owners, failures, digest costs, and the
   snapshot deletion contract: reject new reads after a durable deletion mark,
   preserve a five-minute cleanup grace period, and allow overdue reads to fail.
   Ordinary reads must not register, renew or release persistent reader leases. Keep placement, fencing,
   durability, and publication rules in their existing components.
5. Expose native Dataset through an opt-in main-workspace Python extension
   (`crowdb_dataset_native`) to the same Rust data plane. The existing
   `crowdb_dataset` HTTP package remains independently installable.
   Preserve the portable HTTP distribution without adding mandatory native
   dependencies. Additional C/C++ business bindings are later work; reuse the
   existing lease ABI rather than making a new business ABI a prerequisite.
6. Use the runtime's TCP/RDMA policy, with eligible pre-payload fallback only.
   Native access does not automatically select HTTP on transport failure or
   replay ambiguous mutations on another path. GPU delivery reuses the runtime's
   host-copy lifetime contract; direct GPU memory remains later work.

## Dependencies

- Existing `ClientRuntime`, topology/locality observations, owner-backed
  delivery and first native S3 frontend. Dataset uses the common transport
  and budget contracts while retaining its own selection and acknowledgement.
  Successful stream EOF, durable publication, caller release and owned runtime
  shutdown remain distinct events. The HCA acceptance is same-host functional
  evidence; it does not replace Dataset workload or cross-host measurements.
- Existing public REST API, packaged HTTP SDK, Dataset CPU container
  acceptance, and measured Chunk-KV/DiskIO flows. Keep HTTP available as an
  explicitly selected client while native work is unlanded.
- R233 caching is optional. Dataset integration does not block the existing
  HTTP SDK distribution or require a second transport/buffer implementation.

## Acceptance

- Given HTTP and native reads of the same Snapshot/Read Plan/Split, when each
  executes, bytes, projection, worker partition, integrity, and cleanup agree.
  Invariant: Dataset surface equivalence. Integration test.
- Given HTTP/native workloads over the same snapshot, when measured under
  documented settings, report p50/p99 latency, throughput, CPU, memory, and
  digest cost. Invariant: comparable measurement contract. Integration test.
- Given stale routing or a disconnected DiskIO endpoint, when a native read
  executes, it refreshes, retries where eligible, or fails without changing
  selection/snapshot or publishing placement. Invariant: non-authoritative
  topology. Integration test.
- Given concurrent reads of one snapshot, when requests start, stream and end,
  no snapshot activity mutations occur. Given a deletion mark, new requests fail
  while admitted streams may finish; cleanup waits for the persisted grace
  deadline across retries/restarts. Invariant: read independence and delayed GC.
  Integration test.
- Given caller-held buffers and a slow consumer, when prefetch or cancellation
  occurs, memory stays bounded and held data stays valid until final release.
  Invariant: bounded owner-backed delivery. Integration test.
- Given a fetched but unacknowledged batch, when its buffers are released and
  the read resumes, progress has not advanced; explicit acknowledgement alone
  advances it. Invariant: processing acknowledgement. Integration test.
- Given a native Dataset facade sharing a runtime with S3, when either facade
  executes or closes, resource sharing preserves Dataset authority and the
  other's operations. Invariant: shared runtime, separate semantics.
  Integration test.
- Given disabled RDMA, eligible setup failure, or accepted partial failure,
  when direct access executes, TCP fallback obeys existing rules, with no
  implicit HTTP retry or ambiguous mutation replay. Invariant: transport
  failure boundary. Integration test.
- Given the same native read through Rust and Python, when cancellation and
  close occur with retained data, logical results, buffer lifetime, isolated
  cleanup, and explicit acknowledgement agree. Invariant: binding parity.
  Integration test.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-sdk-native --all-targets
pixi run -- cargo test -p crowdb-access-dataset --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run check-dataset-sdk-boundary
pixi run test-dataset-sdk
pixi run test-dataset-sdk-python
pixi run test-e2e-dataset
```
