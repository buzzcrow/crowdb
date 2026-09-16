<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R173** — Bump this line in the same commit when adding a new item.

### Next Milestone — Chunk-backed range KV

R144 is a deferred merge follow-up to the completed chunk-KV server and routed
client split/transfer baseline. R147 is the remaining deferred chunk lifecycle
follow-up. R148 follows the now-measured mirror-only production baseline and
keeps stream metadata scale-out and sealed-chunk EC disabled until that
requirement is implemented.
- **[R144](R144-chunk-kv-partition-merge.md)** — adjacent partition merge —
  Area: crowdb-tree / KV / server / group 0 — Deferred follow-up that composes
  two adjacent chunk-backed trees, fences both owners, reconciles their WAL
  sequences, and atomically replaces both parent ranges with one destination.
- **[R147](R147-tree-chunk-gc.md)** — reclaim B+tree chunk strips — Area:
  crowdb-tree / chunkdb / diskdb — Turn tree logical-GC results into durable,
  manifest-fenced reclaim candidates. Repack mixed live strips, then use an
  idempotent generic in-chunk operation to release whole unreachable strips or
  chunks without racing retained manifests, snapshot pins, or layout readers.
- **[R148](R148-chunk-stream-scale-out.md)** — partition metadata scale-out and
  sealed-chunk EC — Area: chunk-stream / chunk-kv / KV / chunkdb — Move the
  stream namespace and tree root catalog as one fenced binding generation,
  optionally shard their indexes, and convert sealed mirror chunks to EC.

### Planned — S3 data access service

R152–R164 delivered the limited basic S3 service. R166 remains active for the
single-service restart acceptance baseline; R172 carries the expanded fault and
scale-out E2E matrix. R167–R169 defer multipart upload and
shared-storage GC without blocking basic large-object deletion. R170 separately
adds optional cuObject/RDMA acceleration after the TCP baseline is correct and
measured.
- **[R166](R166-s3-e2e-suite.md)** — basic end-to-end acceptance — Area: access
  server / S3 / testing — **Active.** Verify the real-service basic CRUD suite,
  including every individual service restart, before closing the milestone.
- **[R167](R167-s3-multipart-upload.md)** — multipart upload — Area: access
  server / S3 — **Deferred.** Add durable part state, atomic completion, cleanup,
  and multipart integrity after the basic milestone stabilizes.
- **[R168](R168-s3-shared-object-reclamation.md)** — shared small-object
  reclamation — Area: access server / S3 / chunkdb — **Deferred on R95.** Turn
  exact pending shared ranges into qualified, restart-safe physical deletion.
- **[R169](R169-s3-shared-chunk-tree-gc.md)** — B+tree and shared-chunk garbage
  collection — Area: access server / S3 / crowdb-tree / chunkdb — **Deferred on
  measurement, R147, and R168.** Compact fragmented shared chunks and retire
  obsolete S3 metadata without erasing reader or recovery authority.
- **[R170](R170-s3-cuobject-rdma.md)** — optional cuObject RDMA data plane —
  Area: access server / S3 / DiskIO / RDMA — **Deferred until basic TCP S3 is
  stable.** Keep acceleration in a separate optional library and requirement;
  AccessServer coordinates while DiskIO-owned cuObjServer endpoints transfer
  parallel logical spans directly to or from client registered memory.
- **[R172](R172-s3-fault-scaleout-e2e.md)** — fault and scale-out E2E matrix —
  Area: access server / S3 / testing — Validate the basic CRUD service across
  process recovery, routing changes, concurrency races, and multiple owners;
  performance measurement is explicitly deferred.

### High Priority

- **[R103](R103-chunkdb-range-migration.md)** — chunkdb range ownership
  migration — Area: chunkdb / kv — Implement the full
  `Copying`/`Cutover`/`Complete` migration flow for transferring chunkdb
  instance range ownership. Dual-serve reads during cutover, new-owner-only
  writes, background metadata verification, graceful client redirect.
  Distinct from R102: R103 transfers which chunkdb instance serves a hash
  range; R102 rebinds which paxos group stores a disk-group's data. Both
  reuse the common `BindingStrategy` framework
  (`doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5).
- **[R102](R102-diskdb-dynamic-binding-migration.md)** — diskdb dynamic
  disk-group binding migration — Area: diskdb / kv — Reuse the common
  `BindingStrategy` framework
  (`doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5) to
  dynamically rebind diskdb disk-groups to paxos groups, replacing the
  operator-manual `BindMapValue` write with automatic monitoring +
  rebinding. Monitor detects instance join/leave, rebalances disk-group
  assignments, migrates data during rebinding.
- **[R80](R80-diskdb-rebalance.md)** — diskdb space rebalance across
  disks — Area: diskdb — Complete the existing passive allocator and durable
  relocation baseline with sustained-skew admission, projected-improvement and
  target-headroom gates, bounded repeated moves, truthful stalled/balanced
  status, and a real DiskIO/ChunkDB convergence test for newly added or
  recovered empty disks.
- **[R82](R82-kv-watch-notify-coalescing.md)** — watch/notify
  coalescing (debounce) — Area: kv / diskdb — the watch/notify
  extension ships without coalescing: one notify per changed key per
  matching prefix. Burst writes to a watched prefix (e.g. diskdb
  `batch_write` touching 10 disks) generate 10 separate notifies,
  amplifying subscriber wakeups + re-read load. Add a per-prefix
  debounce coalescer with timer-task flush between the apply-path hook
  and `WatchRegistry::emit`. The original coalescer was removed because
  the timer task captured no registry/coalescer refs (buffered keys
  were silently dropped); R82 must wire the `Weak` refs properly. Load
  optimization, not correctness — the safety-net poller covers missed
  notifies.

### Data Path (diskio + chunk object writers + read flow)

Chunk reads, read repair, mirror-to-EC conversion, write error handling, and
end-to-end Chunk IO performance workloads are landed. The RPC migration items
(R115, R116, R117) are in a separate area (see RPC Migration section below).

### Medium Priority

- **[R171](R171-chunkdb-ad-hoc-ec-read-recovery.md)** — ad-hoc EC read
  recovery — Area: chunk-client / chunkdb / diskdb — Preserve slice-only ISA-L
  reconstruction for small reads, while routing eligible full-fragment
  recoveries to a bounded ChunkDB in-memory coalescer that reuses rebuilt bytes
  and hands one target to the existing durable repair publication sequence.
- **[R83](R83-chunkdb-complete-recovery-flow.md)** — chunkdb
  complete recovery flow (real data recovery + speed control) —
  Area: chunkdb / diskdb / diskio — diskdb's recovery is disk-layer
  only: the R76 `RecoveryScanTask` lists impacted busy blocks +
  `owner_chunk` but the repair step is a placeholder
  (`RecoveryAction::LogOnly`, no data rebuild). There is no chunkdb
  yet (only a reserved proto surface), so when a disk goes `Bad` the
  impacted blocks are handed to a "future recovery/relocation path"
  (§8) that does not exist — no surviving replica/parity is read, no
  rebuilt data is written, no strip is updated. Full data recovery
  needs chunkdb (the chunk→strip→segment owner) to rebuild lost
  mirror replicas / EC data+parity from surviving strips via the
  `diskio` service, `UpdateChunkStrip` to new segments, and free the
  old `Bad`-disk segments. Recovery speed must be throttled at the
  chunkdb layer (configurable bandwidth/IOps/concurrency) so
  foreground traffic is not starved. Blocked on the chunkdb server
  component + the `diskio` service (both unlanded; must be filed as
  their own backlog items first). Replaces R76's `LogOnly` with
  `Relocate` / `RebuildFromEc`.
- **[R84](R84-chunkdb-post-disk-move-placement-scanner.md)** —
  chunkdb post-disk-move placement scanner — Area: chunkdb / diskdb —
  R81 Part 2 adds disk move with a stable `DiskId` (record copy
  during Maintenance, no full scan). The move is placement-only and
  the data is intact, but there is no verification that chunk
  placement is still consistent after a move: chunks reference blocks
  via `Segment { disk_id, ... }` (in `MirrorStrip` / `EcStrip`), and
  every chunk with a segment on the moved disk must still reach that
  segment via the disk's new placement. Add a placement-integrity
  scanner (chunkdb-side, following diskdb's `ScannerTask` /
  `BgRunner` pattern, §10) that walks chunk→strip→segment after a
  move (and periodically), resolves each segment's `DiskId` to its
  current group-0 placement, and reports unreachable / orphaned
  segments — handing `Bad`/`Missing`-disk segments to R83 for
  rebuild. Triggered on move via watch/notify (R78) with a periodic
  safety net. Blocked on the chunkdb server component (unlanded) and
  R81 Part 2.
### RPC Migration (legacy → crowdb-rpc)

Historical migration order: R115 → R116 (unary); R117 (streaming) followed
R114 plus the original R32 consensus migration. R115 first validated the
migration pattern (schema, server, client, and error mapping) before the
streaming services. All four migrations follow the
zero-copy wrapper convention (`design-crowdb-rpc.md` §6): `FB`-prefixed
flatbuffer types, wrapper classes in `crowdb-protocol`, no owned
intermediate structs, no per-field copy. The four transport migrations (R115
diskdb, the original R32 KV consensus scope, R117 KV client-facing, and R116
chunkdb) are DONE. The post-migration KV server/library review is also complete.

- **[R33](R33-crowdb-tree-rename.md)** — Extract crowdb-tree to separate repo and rename — Area:
  workspace — Move `crowdbtree/` into its own git repository (preserving
  history), wire `crowdb-kv` to depend on `crowdb-tree-ffi` as an external
  dependency, and rename the crate/namespace/macros from `crowdbtree` to
  `crowdb-tree` / `crow::tree` / `CROWDB_TREE_*`. Establishes the `crowdb-kv` →
  `crowdb-tree` dependency boundary analogous to `crowdb-kv` → `crowdb-common`.
  Most naturally done after R12.
- **[R50](R50-epoch-protected-memtable.md)** — Epoch-protected
  lock-free MemTable — Area: scan / get / crowdb-tree engine —
  **Done.** `MemTable::snapshot()` deep-copied every live L0 entry
  (key + full cell payload) on every scan regardless of range or
  `limit`, and an L0 `get` hit copied twice. Root cause: L0 was the
  only reader-visible structure outside the engine's EBR scheme.
  Replaced the `absl::btree_map` under `mu_` with a
  `ConcurrentSkipList` (inline keys, versioned cell pointers,
  epoch-deferred reclamation). Readers now traverse L0 lock-free
  under their existing epoch guard with zero copy; the cursor seeks
  directly (no `upper_bound` skip pass); `get_view` borrows the
  cell directly off the node. Closes the known gap at
  `crowdb-tree.h:81`. All 383 `test-tree-ct` tests pass.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowdbtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.
- **[R139](R139-group0-service-config.md)** — Group-0 distributed service
  configuration — Area: config / control plane — Publish versioned,
  scoped config through group 0; each service validates revisions, applies
  dynamic fields atomically, and reports fields that require restart.

**Complexity — Medium:**
- **[R4](R4-bounded-mempool.md)** — Bounded memory pool — Area: crowdbtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.
- **[R60](R60-tree-scan-sibling-leaf-readahead.md)** — Sibling-leaf
  readahead on cold scans — **Deferred pending cold file/block-backed
  measurement.** Area: scan / crowdb-tree engine — the scan
  path demand-loads each L1 leaf inline (sync) or one pending page per
  reactor round trip (async), so a cold multi-leaf range pays one
  stall/round-trip per leaf, serialized with merge work on prior
  leaves. The scan knows `right_sibling` before finishing the current
  leaf — issue a readahead for the next
  leaf to overlap I/O with merging. Sync path: prefetch the
  right-sibling page id via a page-cache async-resolve seam. Async
  path: use a fixed one-leaf lookahead to overlap the next leaf read with
  merge and packing of the current resident leaf. Readahead is scan-only and
  conditional on the range, remaining limits, deadline, residency, and async
  disk backend. Win is zero on mem-mode (leaves resident); implement only if
  a cold benchmark with eviction shows a material latency or throughput
  improvement. Medium complexity.
---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.agents/skills/implement-requirement/SKILL.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
