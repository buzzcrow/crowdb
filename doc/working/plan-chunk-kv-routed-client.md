<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Routed Client Plan

Source: [`design-chunk-kv-routed-client.md`](design-chunk-kv-routed-client.md)
and
[`../backlog/R145-chunk-kv-routed-client.md`](../backlog/R145-chunk-kv-routed-client.md).

Goal: implement a bounded catalog-aware client that preserves R143/R142
identity, ordering, fencing, and partial-success semantics.

## Phase 1: Catalog and Identity

- [x] Add the workspace client crate and injected catalog/transport boundaries.
- [x] Validate and atomically publish complete catalog generations with binary
  point and interval routing.
- [x] Generate OS-random 128-bit handle identity and allocate one nonzero atomic
  sequence per logical mutation, including explicit persisted-ID resubmission.

## Phase 2: Direct Operations and Retry

- [x] Route point and conditional operations directly to owners.
- [x] Preserve typed results, journal positions, minimum-position reads, and
  identity across bounded refresh/transport retries under one deadline.
- [x] Wrap seek and single-partition directional scan without semantic emulation.
- [x] Add production group-0 catalog loading and bounded direct-owner
  crowdb-rpc transport for point, seek, and scan operations.

## Phase 3: Bounded Composition

- [x] Implement duplicate-preserving, input-ordered, partial-success multi-get.
- [x] Implement per-operation-ID, partition-ordered, non-transactional batch.
- [x] Implement globally bounded forward/reverse multi-partition scan and exact
  topology-safe continuation/replanning.

## Phase 4: Gates and Documentation

- [x] Add unit and injected integration coverage for the available boundaries.
- [x] Add real-process client coverage after the server RPC process exists.
- [x] Run formatting, workspace lint, client/server tests, and aggregate server
  gates through `pixi run`.
- [x] Fold stable behavior into a permanent client design and index entry.

## Gate Results

- `cargo fmt --all -- --check`: passed.
- `rs-lint`: passed for the full workspace.
- `crowdb-chunk-kv-client` all-target tests: 14 passed.
- `crowdb-chunk-kv-server` all-target tests: 45 passed.
- Clean aggregate `test-server`: passed KV server, diskdb, diskdb-client,
  chunkdb, chunk-client, and diskio-client stages.
- `bench-chunk-kv-regression.sh`: passed through the release CLI and three real
  chunk-KV servers. It completed 30,000 routed 512-byte writes at 6,227 ops/s
  with zero errors and 13.388 ms p99, converged to 13 partitions at 5/4/4,
  recorded 715,874 us maximum split-fence duration, recovered five assigned
  partitions in 1,050 ms, read back an acknowledged value, and increased
  aggregate server RSS by 2,112 KiB. Evidence:
  `bench-log/chunk-kv-regression-20260913-081906/results.tsv`.
- Production defaults remain four partitions per owner and a 1 GiB size
  threshold. The ratio converged directly; the same size-trigger algorithm was
  exercised at a scaled 5 MiB threshold so both policies fired within a bounded
  local run. The measurements justify no lower production size threshold.

## Open Issues

- Review decision: `crowdb-diskio-client` currently exposes transport-shaped
  calls and leaves topology plus connection ownership to each caller. R150 now
  owns the semantic client redesign. R143/R145 keep only a bounded interim fix
  that reuses unchanged ChunkDB DiskIO connections so the production regression
  can finish without expanding these requirements into that redesign.
- Review decision: the owner connection pool follows existing CROWDB RPC
  transports and uses a bounded `DashMap` keyed by endpoint. This introduces a
  sharded lock on connection lookup, outside the storage data path, in exchange
  for preventing duplicate connection storms and enforcing the owner cap.
