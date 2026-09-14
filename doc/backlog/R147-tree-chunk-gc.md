<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R147: crowdb-tree / chunkdb — Reclaim B+tree chunk strips

**Status: Deferred.** Implement after R140 establishes immutable page packs,
manifest retention, and durable reclaim candidates. R146 must make abandoned
active chunks sealable before they can enter physical reclamation.

## Problem

The existing crowdb-tree GC removes unreachable page IDs from the mapping table
and eventually releases local page-store extents. For the R140 backend, that is
only logical reclamation: page packs reside in chunk strips whose DiskDB blocks
remain allocated until chunkdb releases them. Deleting a mapping entry without
this second step leaks chunk capacity; releasing a strip too early can destroy
pages still referenced by another manifest, split child, or pinned snapshot.

A strip can also contain both dead and live page packs. Chunkdb's proposed
in-chunk GC in `doc/backlog/R92-chunkdb-in-chunk-gc.md` is the correct ownership
boundary for releasing strip storage, but crowdb-tree must first remove or
relocate every live reference and publish that fact durably. The release flow
must survive tree, KV-server, and chunkdb restart and must tolerate an ambiguous
release response.

The root storage contracts are
`doc/design/tree/design-crowdb-tree-storage.md` and
`doc/design/chunkdb/design-crowdb-chunkdb.md`.

## Solution

Use a two-phase, manifest-fenced flow: crowdb-tree proves a strip unreachable,
then chunkdb idempotently reclaims it through a durable cleanup intent.

1. Extend R140 logical GC to emit a durable reclaim candidate containing tree
   identity, source manifest generation, chunk ID and revision, strip sequence
   and byte range, and the page-pack ordinals formerly stored there. Emit it
   only after a newer manifest that removes those references is durable.
2. Advance a candidate to releasable only after every retained manifest,
   split-child manifest, and in-memory snapshot pin that can name any listed
   page pack is past the reclamation watermark. Recompute reachability from the
   current manifest before dispatch; a reachable ordinal cancels the candidate
   as stale.
3. Add or complete the generic R92 `ReclaimStrip` chunkdb operation for both
   `BtreePage` and `PageIndex` chunks. Its request carries a deterministic
   reclaim ID, chunk ID, expected chunk revision, and exact
   strip identities; under the existing lifecycle guard it verifies that the
   chunk is Sealed and the identities still match, removes the strips from the
   published layout, and persists the reclaim ID plus qualified DiskDB cleanup
   intents. Retired segments are freed only after the layout-validity window.
   A retry with the same reclaim ID returns the persisted result.
4. Delete an entire sealed chunk when all of its strips are releasable. For a
   partially dead strip, do not punch arbitrary byte holes in the first
   release. Copy its live page packs to fresh R140 B+tree chunks, verify the
   copies, atomically publish a new manifest with replacement references, wait
   for the old generation and reader-layout window to expire, and then reclaim
   the whole old strip. This gives a workable compaction flow without changing
   stable chunk offsets or requiring R92 `CollapseStrip` and `MergeStrips`.
5. Persist GC state as `Prepared`, `Releasing`, or `Complete` in the injected
   root catalog before issuing chunkdb mutations. On startup, the current tree
   owner resumes non-complete records; chunkdb independently resumes its
   segment cleanup intents. An ambiguous RPC result is resolved by querying
   the chunk revision and strip identities before retrying.
6. Bound candidates, bytes copied, concurrent chunk mutations, and work per
   maintenance pass. GC and repacking run outside page lookup, apply, and
   mapping-resolution hot paths and add no lock to them. Foreground checkpoint
   publication wins the existing manifest gate; stale GC output becomes
   orphan work and is retried from the newer generation.
7. Expose logical bytes retired, physical bytes released, pending and oldest
   candidate age, live bytes copied, reclaim conflicts, ambiguous outcomes,
   cleanup retries, and bytes retained by manifest or snapshot pins.

## Dependencies

- Depends on R140 for generation-addressed page packs, immutable manifests,
  reference retention, logical GC, and the injected root catalog.
- Depends on R146 so crashed writers eventually produce Sealed chunks. GC skips
  Active chunks and remains safe while sealing is unavailable.
- Uses R92's generic in-chunk `ReclaimStrip` boundary. If the broader collapse
  and merge work is still unlanded, R147 implements only the idempotent
  whole-strip operation needed here and leaves those compaction operations
  deferred.
- R142 supplies the group-backed production root catalog and schedules tree
  maintenance. Tests may use R140's in-memory epoch-fenced catalog.
- DiskDB qualified frees and chunkdb cleanup-intent reconciliation remain the
  physical block-release authority.

## Acceptance

- Given tree GC removes a page from the current mapping but an older manifest
  or pinned snapshot still references its pack, when GC evaluates the strip,
  assert no chunkdb reclaim is issued and retained-byte metrics identify the
  pin. Invariant: logical death does not imply physical unreachability.
  Integration test.
- Given all packs in one strip are unreachable past the watermark, when the
  persisted candidate is dispatched, assert chunkdb removes that exact strip,
  waits out layout validity, frees its qualified segments, and marks the record
  Complete. Invariant: physical release follows durable reference removal.
  Integration test.
- Given every strip of a sealed chunk is releasable, when GC runs, assert it
  uses whole-chunk deletion and all cleanup intents complete idempotently.
  Invariant: an empty sealed chunk retains no allocated strips. Integration
  test.
- Given a strip mixes live and dead packs, when GC compacts it, assert live
  packs are copied and verified, the replacement manifest is published before
  the old strip is retired, and reads remain correct across the cutover.
  Invariant: partial-strip reclamation never creates a page-reference hole.
  E2E test.
- Given a checkpoint publishes generation `g + 1` while GC works from `g`, when
  GC reaches publication, assert it cannot replace `g + 1`, its unreferenced
  copied packs become orphan candidates, and work restarts from the current
  manifest. Invariant: GC never rolls back acknowledged tree state.
  Integration test.
- Given the KV-server restarts in each of `Prepared` and `Releasing`, when the
  partition reopens, assert it resumes the same candidate without an early or
  duplicate free. Invariant: tree-side GC progress is restart-safe. E2E test.
- Given chunkdb restarts after layout retirement but before DiskDB cleanup,
  when reconciliation resumes, assert the retired segments are freed after the
  validity window and the cleanup intent clears. Invariant: physical release
  survives chunkdb restart. E2E test.
- Given a reclaim RPC response is lost, when the tree owner queries and retries
  the same reclaim ID, assert the persisted result identifies the exact retired
  strips; a different ID or replacement identity conflicts. Invariant:
  ambiguous completion cannot reclaim a replacement strip. Integration test.
- Given bounded GC budgets and sustained foreground traffic, when many strips
  become reclaimable, assert each pass stays within its byte, candidate, and
  concurrency limits while candidates eventually complete. Invariant: physical
  GC is bounded and makes progress without entering tree hot paths. Integration
  test.

Required gates:

- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
