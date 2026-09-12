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
- [ ] Add real-process client coverage after the server RPC process exists.
- [x] Run formatting, workspace lint, client/server tests, and aggregate server
  gates through `pixi run`.
- [x] Fold stable behavior into a permanent client design and index entry.

## Gate Results

- `cargo fmt --all -- --check`: passed.
- `rs-lint`: passed for the full workspace.
- `crowdb-chunk-kv-client` all-target tests: 12 passed.
- `crowdb-chunk-kv-server` all-target tests: 24 passed.
- Clean aggregate `test-server`: passed KV server, diskdb, diskdb-client,
  chunkdb, chunk-client, and diskio-client stages.

## Open Issues

- Review decision: the owner connection pool follows existing CROWDB RPC
  transports and uses a bounded `DashMap` keyed by endpoint. This introduces a
  sharded lock on connection lookup, outside the storage data path, in exchange
  for preventing duplicate connection storms and enforcing the owner cap.
- R143 internal group RPC handlers and production transport for multi-get and
  batch remain.
- R144 merge-specific continuation and retained-result cases stay skipped.
