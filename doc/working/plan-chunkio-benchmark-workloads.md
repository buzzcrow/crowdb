<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Benchmark Workloads Plan

## Requirement

Extend [R135](../backlog/R135-chunkio-end-to-end-performance.md) from its
landed large-write baseline to small writes and small, large, and mixed reads.
Benchmark metadata uses memory-backed KV/WAL and payload IO uses NullDisk.

## Work

- [x] Add library-owned small-write runner and queue/pipeline metrics.
- [x] Add bounded real-write preparation plus small, large, and mixed readers.
- [x] Add CLI verbs and validation while preserving `chunkio write`.
- [x] Add CLI command-surface and library workload tests.
- [x] Extend regression scripts for 1 KiB/8 KiB writes, small/large/mixed
  reads, and maximum-load cases using mem-block metadata plus NullDisk.
- [x] Run affected Rust format, lint, tests, and script syntax checks.
- [x] Run the full repository gate and classify unrelated failures.
- [x] Update permanent design documentation.
- [ ] Remove R135 backlog and working documents in the requirement cleanup
  commit.

## Acceptance evidence

- Small-write results account for every admitted object and expose batch,
  queue, active-pipeline, scale-out, and scale-in metrics.
- Read preparation creates real writer-produced locations and remains outside
  the timed window; reads validate returned lengths through the full client,
  ChunkDB, and DiskIO route.
- Mixed request selection is deterministic and reports exact small/large
  request and byte counts.
- Regression deployment explicitly selects `mem-block` for both KV and WAL;
  no benchmark data or metadata is written to a real disk backend.
- The full repository gate reaches the existing crowdb-tree failures
  `Gc.CompactSparseBlocksRespectsByteBudget` and
  `Gc.CompactSparseBlocksMaintainsDataIntegrity`; affected Rust tests and
  lint pass.
