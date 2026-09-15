<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R166: access server / S3 — Compatibility, correctness, and performance E2E suite

## Problem

Unit tests cannot prove that AWS-compatible clients, Hyper streaming, native
buffers, Chunk-KV routing, chunks, retries, and reclamation compose correctly.
A throughput number alone can also hide extra copies, unbounded memory, weak
failure behavior, or a single metadata bottleneck.

The tested invariants are
`doc/design/accessserver/design-crowdb-access-server.md` §10 and
`doc/design/accessserver/design-crowdb-access-server-s3.md` §7.

## Solution

1. Build a reproducible E2E harness using the official Python `boto3` SDK as
   its one SDK client, plus raw HTTP cases for every basic bucket/object
   operation, conditional, range, continuation, unsupported feature, and
   error contract. Install `boto3` only in a dedicated Pixi `s3-e2e`
   environment; it is not a Rust dependency and is absent from release builds.
   The required test owns its environment: build and start KV group 0/data
   groups, diskdb, diskio, chunkdb, Chunk-KV, and access-server, wait for each
   readiness boundary, pass the resulting loopback endpoint to boto3, and tear
   every process down. An externally supplied endpoint remains a developer
   override, not a reason for the required test to skip.
   The lightweight required topology uses one diskdb process, one diskio
   process, one disk group, one disk, and one zone. ChunkDB runs the explicit
   `unsafe_colocated` placement strategy so the 2+1 EC object fragments may
   occupy that one physical failure domain. The test therefore validates
   composition and protocol correctness, not node- or disk-failure survival.
2. Exercise empty, tiny shared, block-boundary, chunk-boundary, EC-boundary,
   and large dedicated objects with randomized body fragmentation and binary
   keys. Compare exact bytes and persisted integrity.
3. Inject process restarts, lost replies, routing changes, chunk failures,
   slow clients, exhausted pools, and overwrite/delete/read races at every
   durable transition. Verify eventual cleanup separately from visibility.
4. Benchmark direct chunk-client versus loopback and remote S3 PUT, GET, and
   range GET. Record CPU, memory traffic, allocations, copy counters,
   time-to-first-byte, throughput, p50/p95/p99 latency, and network bytes over
   object size and concurrency.
5. Add access-server and Chunk-KV owners independently and measure scale-out.
   Keep environment, datasets, thresholds, and raw result artifacts explicit;
   do not encode hardware-specific throughput as a universal contract.

## Dependencies

- Depends on R152–R165 for the complete initial contract.
- Uses existing chunk/chunk-KV E2E fixtures and fault injection where possible.
- SigV4 cases may remain skipped with an explicit R162 reason while that
  requirement is deferred; unauthenticated trusted mode must be visible.
- Uses path-style local endpoints in the `s3-e2e` Pixi environment. The suite
  switches to explicit static SigV4 test credentials when R162 lands.

## Acceptance

- Given supported SDK and raw HTTP matrices, when all basic operations and
  excluded features run, assert wire responses and final namespace match the
  documented compatibility surface. Invariant: the advertised S3 subset works
  end to end. E2E test.
- Given randomized fragmentation and every storage boundary, when objects are
  round-tripped, assert bytes, ranges, checksums, ETags, and memory bounds match
  the reference model. Invariant: transport fragmentation does not alter data.
  E2E test.
- Given each injected crash, lost response, ownership move, slow peer, and
  race, when recovery settles, assert visibility is atomic, retries are
  idempotent, and cleanup targets only unreachable generations. Invariant:
  failures preserve object consistency. E2E test.
- Given the benchmark matrix and increasing frontend/metadata owners, when
  results are recorded, assert every copy/fallback is counted and no hidden
  gateway-local state is required for correctness. Invariant: performance
  claims are evidence-backed and scope-specific. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
