<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R183: access server / Iceberg — Reachability and bounded reclamation

Status: physical GC remains deferred. The capacity behavior confirmed on
2026-09-24 uses the existing disk provisioning/allocation flow. Current disks
are file-backed simulations with configured capacity limits; they are not
unbounded growable files. When available managed capacity cannot satisfy an
allocation, a new chunk cannot be created. No separate Iceberg capacity quota or
pre-full write-stop threshold is required for the current functional checkpoint.

## Problem

Catalog clear, table purge, snapshot expiration, failed commits, staged uploads,
multipart aborts, and projection replacement all create unreachable state. Deleting
on request latency or using per-file reference counts would race retained snapshots,
branches, tags, metadata logs, readers, and crash recovery. Scanning a full table or
catalog into memory would fail at Iceberg scale.

R177 selects generation-indexed candidates plus reachability traversal, mandatory
retention and pins, and no racing reference counts. This requirement implements the
durable background proof and deletion workflow.

Before GC is implemented, unreachable storage remains allocated and can exhaust
the provisioned capacity. DiskDB/ChunkDB allocation failure is the capacity
boundary, not an Iceberg-layer free-space policy. This requirement owns later
reclamation and full-capacity recovery acceptance. Allocation can fail when
eligible placement capacity is insufficient, not only when every physical disk
contains zero free bytes.

## Solution

- **GC-I1 — Invisibility first:** physical deletion is considered only after the
  owning catalog, table generation, operation, or upload state is unreachable.
- **GC-I2 — Positive proof:** a file or record is removed only after a proof against
  retained metadata roots, snapshots, refs, metadata logs, operations, credentials,
  leases, readers, and operator pins.
- **GC-I3 — Bounded traversal:** discovery, graph traversal, sorting, retry, and
  deletion use durable continuations and independent limits.
- **GC-I4 — Restart safety:** duplicate, reordered, or resumed work can leak but
  cannot erase reachable state or restore visibility.
- **GC-I5 — Foreground isolation:** cleanup has separate CPU, memory, KV, chunk I/O,
  bandwidth, and concurrency admission from catalog and FileIO requests.
- **GC-I6 — Capacity exhaustion preserves authority:** failed allocation cannot
  publish incomplete bytes, replace a committed head, or authorize unsafe deletion.

1. Add `gc/candidate.rs`, `reachability.rs`, `task.rs`, `repository.rs`,
   `worker.rs`, and `pins.rs`. Store tasks and generation-indexed candidate pages
   under their CatalogId/TableId; do not create one key per file in Group 0.
2. Emit candidates for failed/abandoned metadata generations, expired staged table
   creates, multipart sessions and parts, orphan projections, expired snapshots,
   purge-requested dropped tables, and retired catalog ranges. Candidate creation
   never performs physical deletion.
3. Traverse standard metadata JSON, metadata logs, retained snapshots and refs,
   manifest lists, manifests, data/delete files, deletion vectors, and statistics
   files according to the owning format version. Spill bounded sorted mark pages to
   durable task state instead of retaining the graph in memory.
4. Compare candidate pages with the retained mark set under a captured table or
   catalog fence. Revalidate the fence, retention deadline, active operations,
   delegated credentials, reader leases, and operator pins immediately before
   scheduling deletion.
5. For catalog clear, wait for R178's maintenance publication, lease-plus-grace
   completion, minimum retention, and pins; scan the retired CatalogId half-open
   range with restartable continuations. Never scan the active range by name.
6. Delete file records and chunk roots idempotently only after proof. Delete derived
   projections before or with their owning unreachable generation. A partial chunk
   failure leaves durable retry state and never reconstructs a removed authority.
7. Expose pause, resume, inspect, pin, unpin, rate, progress, stalled reason, and
   retry controls. Validate every configured item, byte, time, and concurrency cap;
   use bounded exponential backoff and terminal quarantine for repeated corruption.
8. Reuse provisioned disk capacity and authoritative DiskDB/ChunkDB allocation
   outcomes. For the current file-backed simulated disks, use their configured
   capacity limits, not all remaining space on the host filesystem; exhausted
   capacity must not silently expand the emulated disk. Future physical disks
   follow the same allocation boundary. Do not introduce an independent Iceberg
   quota, reserved-space ratio
   or pre-full write ban as a prerequisite. Requests needing new chunks fail
   through the existing bounded storage-error path when allocation is impossible;
   operations that need no new allocation are not globally disabled solely by
   such a failure. Preserve durable intent and any uncertain publication outcome.
9. Verify full-capacity recovery: lack of space may also prevent writing GC mark
   pages or progress records. Retain resumable state and report the resource
   failure rather than spinning, dropping proof data or bypassing reachability.
   Resume after capacity is added through the normal storage flow or safe
   reclamation makes allocation possible. Do not promise GC can make progress at
   absolute exhaustion without verifying its own durable-work requirements.

## Dependencies

- Depends on R177, R178, R180, R181, and R182 for all roots, locations, pins,
  candidates, lifecycle fences, and v1/v2/v3 reachability semantics.
- Snapshot-expiration commits remain R182 mutations; this requirement performs only
  the resulting physical cleanup.
- R184 exposes only authenticated operator status/control, not a public object
  delete endpoint.
- R185 cache entries and invalidation never constitute reachability. R183 waits for
  the durable lease boundary defined in R177, not for physical cache eviction.

## Acceptance

- Given retained v1, v2, and v3 snapshots, branches, tags, metadata logs, data and
  delete files, deletion vectors, and statistics, when reachability runs, assert all
  referenced files are marked and no task memory or KV value grows with the graph.
  Invariants: GC-I2 and GC-I3. Integration test.
- Given a failed commit candidate, expired stage, aborted multipart upload, and
  orphan projection, when cleanup runs after deadlines, assert only unreachable
  state is removed and repeated execution is idempotent. Invariants: GC-I1 and
  GC-I4. Integration test.
- Given a drop with purge and concurrent reader, credential, commit operation, and
  operator pin, when each fence expires or releases in every order, assert deletion
  starts only after the last valid fence and never affects the reader's bytes.
  Invariant: GC-I2. E2E test.
- Given clear of a catalog containing billions of simulated keys across partitions,
  when workers crash and resume, assert foreground clear does not scan children,
  continuation makes progress, every batch stays bounded, and the new CatalogId is
  untouched. Invariants: GC-I3 and GC-I4. Integration test.
- Given cleanup saturation and simultaneous namespace, commit, and FileIO load,
  when resource limits are reached, assert cleanup throttles or pauses while
  foreground admission retains its configured budget. Invariant: GC-I5. Integration test.
- Given a corrupt manifest, digest mismatch, missing candidate page, and repeated
  chunk-delete error, when workers process them, assert they fail closed into
  inspectable retry or quarantine state without guessing reachability. Invariants:
  GC-I2 and GC-I4. Integration test.
- Given file-backed simulated disks at their configured allocation limit, even
  with host filesystem space remaining, or physical disks with insufficient
  eligible capacity for another chunk,
  when PUT, multipart completion or candidate metadata writing needs allocation,
  assert bounded failure, no partial file/head publication, intact committed
  authority and recoverable uncertain intent. No separate Iceberg quota is needed
  to trigger this boundary. Invariant: GC-I6. E2E test.
- Given full storage and a GC task needing durable workspace, when that allocation
  fails and capacity is later added or safely reclaimed, assert the task retains
  its proof/continuation and resumes without unsafe deletion, duplicate publication
  or an unbounded retry loop. Invariants: GC-I2, GC-I4 and GC-I6. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
