<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: crowdb-tree Storage Engine (Overview)

Depends on: [Paxos KV](../kv/design-crowdb-paxos-kv.md), [state machine](../kv/design-crowdb-paxos-kv-state-machine.md)
Satisfies: [Paxos KV](../kv/design-crowdb-paxos-kv.md) §8.3

crowdb-tree is the ordered, slot-aware storage engine used by CROWDB learners.
The C++ engine owns publication, memory, page I/O and recovery. Rust supplies
consensus, external WAL policy and the host lifecycle through an opaque C ABI.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
- [2. Architecture](#2-architecture)
- [3. Engine Abstraction](#3-engine-abstraction)
- [4. FFI Boundary](#4-ffi-boundary)
- [5. Sub-Design Document Map](#5-sub-design-document-map)
- [6. Correctness Invariants](#6-correctness-invariants)

## 1. Goals and Non-Goals

- Expose the latest value or tombstone for each key, using the highest eligible
  consensus slot. Retained versions support incomplete prefixes and captured
  views; the public API does not provide arbitrary historical queries.
- Keep L0 ingestion concurrent and L1 preparation parallel. Bound memory,
  queue slots, CPU work and I/O before accepting work that must complete.
- Hand cell ownership from sources to immutable page overlays without copying
  value payloads or pinning the whole source index.
- Support Local file/block storage and immutable mirrored Chunk storage.
  Logical publication, reloadable writeback and durable checkpoint are
  separate transitions.
- Preserve captured views and owned results across later writes, generation
  replacement and Tree close. Last-owner release determines physical reuse.
- Compose with the consensus WAL. Tree persistence does not create a second
  operation log and does not independently decide when WAL data is safe to drop.

## 2. Architecture

A host owns a `TreeRuntime`: a bounded CPU pool, resource partitions and
retirement services. Multiple Trees bind independent accounts and execution
lanes to that host. A standalone Tree without an injected runtime uses a
shared process runtime; servers inject their explicitly configured host.

A `Crowdbtree` facade owns a `TreeCore` whose live state consists of:

- `SourceAdmission`, with active and captured MemTables, writer grants and
  the contiguous apply frontier;
- `PublishedTreeState`, an immutable root containing the mapping, source
  catalog/floor, generation, coverage, commit sequence and logical statistics;
- a `PublicationCoordinator`, which serializes bounded prepared transactions
  while range jobs, readers, encoding and I/O execute independently;
- a shared page cache and typed backing references, with physical ownership
  separate from logical page identity and cache residency;
- an optional checkpoint backend, whose successful root commit establishes
  durable coverage.

L0 is an epoch-protected skip list. L1 is a persistent COW mapping of immutable
`PageState` and inner descriptors. Leaves compose base references and sorted
`OverlayBatch` runs. Split and merge preserve range ownership; page layout is
materialized only when required by bounded maintenance or persistence.

The CPU pool does not wait synchronously for nested jobs. Continuations return
their lane while awaiting child work or I/O. Backend callbacks retain operation
state until terminal completion, including after cancellation or facade close.

## 3. Engine Abstraction

`CrowdbTreeEngine` implements `KVEngine` with owned async apply/read/scan,
immutable views, typed maintenance reports and durable snapshot operations.
Proposal admission returns a native grant before consensus accepts a batch.
Follower and WAL replay use the accepted-work path and the reserved gap lane.
A failed preparation publishes no key and does not consume an automatic slot.

`EngineView` retains a captured immutable view. Reads, iteration and comparison
use that view's source catalog and mapping, including demand-loading cold
frames through their captured backend capabilities. Range rebuild and split
handoff publish through the same coordinator as ordinary writes.

### 3.1 Out-of-order apply, snapshots, and the two GCs

Apply can arrive out of slot order. Versions retain the greatest value needed
below the captured prefix and values above it; a future write must not hide a
value needed by an earlier contiguous flush. NoOp slots advance the frontier
explicitly and cannot be inferred from MemTable contents.

A flush captures a finite contiguous frontier and source catalog, closes
source admission, waits for accepted writers and hands bounded ranges into L1.
Coverage advances only for published work. Failure may leave an installed
prefix: its report identifies that prefix, and retry preserves uncovered
sources. Flush does not itself advance the durable replay point.

Writeback creates reloadable backing for selected dirty pages and performs
identity-checked representation rebases. It does not publish a durable root
or allow the host to advance its WAL checkpoint. A checkpoint captures one
view, completes its page/dependency writes and commits the backend root before
reporting durable coverage. Recovery starts from that coverage and replays
the external WAL suffix.

Two distinct reclamation decisions apply:

- Logical retention uses the externally supplied safe/snapshot floor to
  discard obsolete values and tombstones without changing retained views.
- Physical reclamation waits for real reader, source, snapshot, cache,
  candidate and backend owners to release. Logical removal alone does not
  release a byte charge or authorize pack/block reuse.

## 4. FFI Boundary

The public ABI exposes opaque Tree, runtime, operation and result handles.
Private sender types remain within C++. Versioned structures validate their
size and version before reading or writing caller memory.

Async work completes through an owned native future and a notification fd.
Rust checks completion around readiness registration and owns its registered
fd independently, so dropping a previous reactor cannot deregister a reused
native descriptor. `PinnedValue` and packed scan owners retain their actual
allocation or immutable frame owner across threads.

Known external `Bytes` backing is identified at the full allocation boundary.
Slices share payload capacity once within a host; slice/control holders have
separate charges. Unknown capacity or incompatible host ownership uses a
bounded owned copy. No C++ exception crosses the C boundary.

A Tree permits one destructive work-window collector. It captures one immutable
sample and fans it out to logs and diagnostics. Ordinary gauges do not scan
the full mapping. Allocation counters follow actual allocation and release;
capturing a view does not require a separate statistics-registration pass.

## 5. Sub-Design Document Map

- [Engine](design-crowdb-tree-engine.md): source versions, immutable pages,
  publication, read ownership, resource accounting and async FFI.
- [Flush](design-crowdb-tree-engine-flush-flow.md): capture, bounded range
  handoff, maintenance join, retry and reports.
- [Durable storage](design-crowdb-tree-storage.md): frames, cache residency,
  Local backends, checkpoint, recovery and physical reclamation.
- [Checkpoint flow](design-crowdb-tree-engine-snapshot-flow.md): capture,
  encoding/reuse, write completion, root commit and rebase.
- [Chunk storage](design-crowdb-tree-chunk-storage.md): fixed immutable packs,
  process fencing, catalog pins, physical collection and explicit migration.

## 6. Correctness Invariants

- **I1 — Single published state:** mapping, source floor, coverage and logical
  counters change in one coordinator transaction. Preparation may be parallel;
  stale expected identities cannot overwrite a newer publication.
- **I2 — Accepted work is prepaid:** an admitted writer owns its complete
  descriptor/payload allowance. Flush owns its handoff/routing/reclaim/I/O
  windows before closing sources, including the capacity needed to free work.
- **I3 — Capture is finite:** new writes use the successor; they cannot extend
  an already captured flush or checkpoint indefinitely.
- **I4 — Durability is explicit:** reloadable backing and successful handoff
  are insufficient to advance durable coverage. Backend root commit is required.
- **I5 — Ownership is physical:** shared payload/backing bytes are charged once
  to their canonical owner. Counters fall
  only when the real owner releases or the physical backend confirms deletion.
- **I6 — Close preserves accepted lifetimes:** stop new admission, finish or
  cancel accepted operations with exactly one terminal callback, drain backend
  and retirement work, then destroy the host resources they require.
