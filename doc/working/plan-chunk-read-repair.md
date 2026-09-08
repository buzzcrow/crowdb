<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Read Repair Plan

Upstream: [R111](../backlog/R111-chunkdb-read-io-error-handling.md) and
[working design](design-chunk-read-repair.md).

Goal: make read failures explicit, bounded, durably tracked, and repairable.

## Phase 1: Client contract

- [x] **Failure observations**: retain failed segment identities from fallback. Files: chunk client strip reader.
- [x] **Partial results**: add exact success/failure ranges to range and stream APIs. Files: chunk reader, errors, client.
- [x] **Client tests**: recoverable, unrecoverable, degraded, streaming, memory bounds. Files: reader tests.

## Phase 2: Durable admission

- [x] **Wire contract**: add repair task kind and reuse fenced strip replacement. Files: protocol types.
- [x] **Lifecycle mark**: idempotently persist unavailable identities through exact replacement. Files: chunk client and chunkdb lifecycle.
- [x] **RPC path**: reuse the existing allocator replacement seam. Files: chunk client traits.
- [x] **Admission tests**: duplicate report, terminal revival, and crash-gap recovery. Files: full-stack tests.

## Phase 3: Repair task

- [x] **Coordinator**: deterministic payload/admission and unavailable scan. Files: `app/crowdb-chunkdb/src/repair.rs`.
- [x] **Handler**: bounded full mirror/EC rebuild and fenced publication. Files: repair and DiskIO modules.
- [x] **Runtime**: register the handler and config/metrics. Files: chunkdb main/config/metrics.
- [x] **Crash safety**: restart before task admission; verify fsync-before-publication and reuse lifecycle replacement/reconciliation coverage. Files: chunkdb/client E2E.

## Phase 4: Completion

- [x] **Affected gates**: protocol, chunkdb, reader, and writer tests plus fmt/clippy.
- [x] **Review**: correctness and hot-path review.
- [ ] **Design cleanup**: fold read designs into permanent docs; remove R107/R111 and working docs.
- [ ] **Full gate**: workspace fmt, `rs-lint`, and `test-suite` with baseline failures reported.
