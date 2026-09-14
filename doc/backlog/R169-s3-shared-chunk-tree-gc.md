<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R169: access server / S3 — B+tree and shared-chunk garbage collection

## Status

**Deferred until R147, R168, and the basic S3 workload provide measured garbage
and fragmentation data.** It is unblocked when qualified dead ranges and
metadata tombstones can be enumerated safely and compaction benefit is known.

## Problem

Qualified range deletion can mark shared object bytes dead, but mixed-live
chunks may remain fragmented and S3 metadata trees accumulate obsolete object
generations, tombstones, completed upload intents, and cleanup records. Simple
age-based deletion can race snapshots/readers or discard the only recovery
evidence for an ambiguous operation.

The lifecycle scope is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §5; generic tree
strip reclamation is R147.

## Solution

1. Measure dead/live ratios and plan bounded compaction of shared object chunks.
   Copy only still-reachable ranges into new shared chunks, verify contents,
   atomically replace generation references, then retire old layouts after the
   reader-validity fence.
2. Feed unreachable storage strips and chunks into the qualified generic
   in-chunk/whole-chunk lifecycle rather than freeing DiskDB ranges directly
   from S3.
3. Add S3 metadata retention rules for obsolete generations, tombstones,
   completed/aborted upload intents, and completed cleanup records. Prove they
   are no longer visibility, retry, reader, or recovery authority before
   deleting their Chunk-KV keys.
4. Integrate tree logical-GC output with R147's manifest-fenced strip
   reclamation. Compaction and metadata GC are restart-safe, idempotent,
   throttled, and lower priority than foreground traffic.
5. Expose reclaimable, copied, freed, pinned, quarantined, and amplification
   metrics. Disable compaction when its projected benefit does not exceed the
   configured threshold.

## Dependencies

- Depends on R147 and R168; R92/R95 provide qualified in-chunk/range lifecycle
  mechanisms where applicable.
- Uses R153 generation metadata, R154 retry identities, R159 tombstones, and
  R161 background admission.
- It is not a prerequisite for basic S3 visibility or large-object deletion.

## Acceptance

- Given a shared chunk with interleaved live/dead ranges and pinned old readers,
  when compaction runs, assert live bytes remain exact, metadata switches
  atomically, and the old layout is reclaimed only after pins expire.
  Invariant: compaction never changes visible object bytes. E2E test.
- Given obsolete and still-authoritative generations, intents, tombstones, and
  cleanup records, when metadata GC evaluates them, assert only records proven
  irrelevant to visibility/retry/recovery are removed. Invariant: GC cannot
  erase recovery authority. Integration test.
- Given a crash after copy, metadata switch, or old-layout retirement, when GC
  restarts, assert it resumes to one valid layout without leaks outside the
  recorded candidate set. Invariant: every compaction phase is idempotent.
  E2E test.
- Given low dead ratio or foreground pressure, when the planner evaluates work,
  assert it skips/throttles compaction and respects reserved resources.
  Invariant: GC benefit and load gates protect foreground service. Integration
  test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run test-tree-ct`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
