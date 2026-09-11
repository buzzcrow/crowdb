<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R148** — Bump this line in the same commit when adding a new item.

### Next Milestone — Chunk-backed range KV

Dependency order: R142 depends on R141 and the completed chunk-backed tree
storage; R143 depends on R142; R145 depends on R143. R144 is a deferred
follow-up after R145 and after split and transfer are proven. R146 and R147 are
deferred tree-chunk lifecycle follow-ups. The milestone deliberately separates
the embeddable KV library, server process, routed client, and physical chunk
maintenance.
- **[R141](R141-chunk-stream.md)** — chunk-stream mirrored logical byte stream — Area:
  chunk IO / WAL — Compose finite chunks into a logically unbounded,
  offset-addressed stream for WAL users. Group 0 registers which metadata KV
  group owns each stream map; ordered logical/physical offset arrays in that
  group map reads across three-way mirror chunks. The first version defers
  metadata scale-out and reclaims complete strips below a durable logical
  watermark.
- **[R142](R142-chunk-kv-library.md)** — `crowdb-chunk-kv` range-partitioned KV
  library — Area: KV / crowdb-tree / chunk-stream — Build an embeddable KV
  component whose partition count is independent of node count, whose tree
  pages live in chunk storage, and whose WAL uses chunk-stream. A node may host
  multiple key ranges; split and ownership transfer publish new manifests and
  epochs without migrating existing chunks.
- **[R143](R143-chunk-kv-server.md)** — `crowdb-chunk-kv-server` service —
  Area: KV / server / group 0 — Add the standalone process, protocol/server
  surface, range router, lifecycle, owner leases, failover/balance, and
  service-requested group-0 monitor supervision around `crowdb-chunk-kv`. It
  supports object-name to object-metadata workloads while keeping the
  underlying KV API generic and the KV server free of service-library
  dependencies.
- **[R144](R144-chunk-kv-partition-merge.md)** — adjacent partition merge —
  Area: crowdb-tree / KV / server / group 0 — Deferred follow-up that composes
  two adjacent chunk-backed trees, fences both owners, reconciles their WAL
  sequences, and atomically replaces both parent ranges with one destination.
- **[R145](R145-chunk-kv-routed-client.md)** — routed multi-partition
  `crowdb-chunk-kv-client` — Area: KV / client / RPC / group 0 — Wrap the R143
  RPC surface with catalog-aware point routing, durable client request
  identities, multi-get, non-transactional batch mutation, multi-partition
  forward/reverse scan, bounded retry, and client-facing E2E coverage.
- **[R146](R146-chunk-orphan-sealing.md)** — seal abandoned chunks across all chunk users
  chunks — Area: crowdb-tree / chunkdb — Renew durable writer leases while a
  tree owns its active chunk, allocate a fresh chunk after process restart, and
  extend chunkdb's restart-safe expired-writer sweep to seal abandoned B+tree
  chunks at their acknowledged cursors.
- **[R147](R147-tree-chunk-gc.md)** — reclaim B+tree chunk strips — Area:
  crowdb-tree / chunkdb / diskdb — Turn tree logical-GC results into durable,
  manifest-fenced reclaim candidates. Repack mixed live strips, then use an
  idempotent generic in-chunk operation to release whole unreachable strips or
  chunks without racing retained manifests, snapshot pins, or layout readers.

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
- **[R101](R101-kv-put-cas.md)** — KV compare-and-set on Put — Area: kv —
  deferred pending an ordered-application design. Leader-side
  read-before-propose is not atomic with concurrent proposals, while
  replica-local apply-time predicates can diverge under out-of-order apply.
- **[R79](R79-diskdb-free-batch.md)** — diskdb free batch
  (size-threshold, no timer) — Area: diskdb — Group frees into a
  batch and flush via one `batch_write` when the batch reaches a
  configurable size (default 256). No timer — the flush is
  synchronous on the free path, not a background loop. v1 ships with
  immediate free (R72); this is a follow-up for high-free-throughput
  workloads.
- **[R80](R80-diskdb-rebalance.md)** — diskdb space rebalance across
  disks + disk-groups — Area: diskdb — New/recovered disks enter
  `allocating_disks` empty while peers stay near-full; the round-robin
  allocator is load-unaware so imbalance persists. Add imbalance
  gauges (per-disk-group `used_pct` spread), load-aware allocation
  skewing (weight new allocates by free space — passive convergence,
  no data move), and a per-disk-group rebalance planner that emits
  `RebalancePlanValue` (source busy blocks + `owner_chunk` + target
  disk) with placeholder relocation (`LogOnly`, no `diskio` — same
  envelope as the disk failure recovery scan). Disk-group-level
  rebalance is a caller concern
  (§3.2 — caller picks `disk_group_id`); diskdb contributes a
  `GetRebalanceHint` RPC + keepalive summary, not cross-instance
  moves. Real data relocation deferred to a future `diskio` service.
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

- **[R66](R66-kv-wal-io-uring.md)** — WAL io_uring backend — eliminate
  `spawn_blocking` on the durability path. The WAL's production I/O
  backend (`File` / `BlockDevice`) routes `fdatasync` and file writes
  through `tokio::fs` / `std::fs`, both of which use `spawn_blocking`
  internally (thread hop + blocking pool saturation under burst load).
  Add `IoBackend::Uring` variant that reuses `DiskIOUring` in
  `crowdb-common` (already proven for B-tree page I/O) for WAL segment
  I/O via `io_uring` SQE/CQE. Expose `DiskIOUring`'s submit API
  (`submit_read`/`submit_write`/`submit_fsync`) via FFI as Rust async
  functions. `WalFileInner::Uring` implements all `WalFile` operations
  via `DiskIOUring` SQEs — no `spawn_blocking`, no thread hop.
  Fallback to `File` on non-Linux / no-liburing. `O_DIRECT` aligned
  writes. No `pipeline_writer` or `segment` API changes (drop-in async
  fn replacement). Linux + liburing only; tests skip on other platforms.

### Data Path (diskio + chunk object writers + read flow)

Chunk reads, read repair, mirror-to-EC conversion, write error handling, and
end-to-end Chunk IO performance workloads are landed. The RPC migration items
(R115, R116, R117) are in a separate area (see RPC Migration section below);
R32 depends on R115.

### Medium Priority

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
- **[R32](R32-kv-custom-rust-rpc.md)** — KV consensus hot path →
  `crowdb-rpc` — Area: kv / RPC — Migrate the internal replica-to-replica
  Paxos path from the legacy tonic/h2 stack to the `crowdb-rpc` flatbuffer RPC library.
  Recovers the ~17% h2-lock throughput loss at 2T:1C
  (measured in `kv-read-flow-analysis.md`). Protocol semantics
  preserved (same request/response shapes, `NotLeaderHint`, error
  codes); only the transport changes. Depends on R104 (finished) +
  R114 (finished — bidirectional request-response for LearnerStream +
  StreamSnapshot). Management
  API stays on Axum/HTTP. Open Question resolved: full `.fbs`
  conversion (no prost bridge — the rejected approach), consistent with R105/diskio.

### RPC Migration (legacy → crowdb-rpc)

Dependency order: R115 → R116 (unary); R117 (streaming) depends on
R114 (finished) + R32. R115 lands first to validate the
migration pattern (schema, server, client, error mapping, mixed
rollout) before the streaming services. All four items follow the
zero-copy wrapper convention (`design-crowdb-rpc.md` §6): `FB`-prefixed
flatbuffer types, wrapper classes in `crowdb-protocol`, no owned
intermediate structs, no per-field copy. All four items (R115 diskdb,
R32 KV consensus, R117 KV client-facing, R116 chunkdb) are DONE.

- **[R68](R68-kv-write-largeval-bench.md)** — Large-value write
  benchmark — Area: cluster / maintenance / bench — R67 fixed the 16 KiB
  scan error spike by wrapping the maintenance loop's `flush` /
  `persist_snapshot` / `collect_garbage` in `spawn_blocking`, but
  verified it only on the scan path. The maintenance loop runs
  identically under write load, yet the write regression sentinel
  (`bench-kv-write-regression.sh`) only exercises 512 B values — there is
  no large-value write config. Add a `largeval_16k` write config
  (`--value-size 16384`, 100k keys, 10s mem mode) and verify 0 write
  errors across 3 consecutive runs on Linux. If errors appear, RCA into
  whether the R67 fix has a write-path gap and file a follow-up
  requirement. Low complexity; verifies R67's coverage extends to
  writes.
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
- **[R52](R52-reverse-scan.md)** — Reverse scan — Area: scan / crowdb-tree
  engine — `scan` is forward-only today (ascending key order). Reverse
  scan (descending order, `start_before` instead of `start_after`) is a
  distinct cost shape: the B+tree descent targets the leaf containing
  `start_before`, the merge loop walks cursors backward, and the
  `LeafChainCursor` needs a reverse seek/advance. The skip-list L0
  cursor (R50) is forward-only — a reverse cursor would need
  `prev()` links or a separate reverse traversal path. Client API:
  `KvScanRequest` gains a `direction` field; the S3-style pagination
  uses the first key of each page as the next `start_before`. Needs
  its own scan perf baseline (reverse scans have different cache
  behavior — backward leaf traversal touches pages in reverse
  allocation order).
- **[R54](R54-kv-scan-engine-profiling.md)** — Scan engine profiling —
  Area: scan / crowdb-tree engine — both read modes saturate near ~38k
  scans/s at 32T:32C; the bottleneck moved to the C++ crowdb-tree merge
  loop (L0 skip-list + L1 B+tree cursor) but the specific hot spot is
  unknown. Add `tools/profile-scan.sh` (mirroring
  `tools/profile-write.sh`), profile the 32T:32C scan bench, and
  document the top hot stacks. Investigation only — no scan-path code
  changes. If a clear optimization target emerges, file a follow-up
  requirement with the profiling evidence. Low complexity.
- **[R60](R60-tree-scan-sibling-leaf-readahead.md)** — Sibling-leaf
  readahead on cold scans — Area: scan / crowdb-tree engine — the scan
  path demand-loads each L1 leaf inline (sync) or one pending page per
  reactor round trip (async), so a cold multi-leaf range pays one
  stall/round-trip per leaf, serialized with merge work on prior
  leaves. The scan knows `right_sibling` (`crowdb-tree.cpp:1822/2074`)
  before finishing the current leaf — issue a readahead for the next
  leaf to overlap I/O with merging. Sync path: prefetch the
  right-sibling page id via a page-cache async-resolve seam. Async
  path: batch the right-sibling read with the current leaf's read in
  the reactor submission (small readahead window, default 1). Win is
  zero on mem-mode (leaves resident); needs a cold/disk bench config to
  validate. Medium complexity.
- **[R68](R68-kv-write-largeval-bench.md)** — Large-value write
  benchmark — Area: cluster / maintenance / bench — R67 fixed the 16 KiB
  scan error spike by wrapping the maintenance loop's `flush` /
  `persist_snapshot` / `collect_garbage` in `spawn_blocking`, but
  verified it only on the scan path. The maintenance loop runs
  identically under write load, yet the write regression sentinel
  (`bench-kv-write-regression.sh`) only exercises 512 B values — there is
  no large-value write config. Add a `largeval_16k` write config
  (`--value-size 16384`, 100k keys, 10s mem mode) and verify 0 write
  errors across 3 consecutive runs on Linux. If errors appear, RCA into
  whether the R67 fix has a write-path gap and file a follow-up
  requirement. Low complexity; verifies R67's coverage extends to
  writes.
---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.agents/skills/implement-requirement/SKILL.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
