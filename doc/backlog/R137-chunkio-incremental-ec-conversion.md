<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R137: chunkio — Incremental small-write EC conversion

**Status:** Backlog. Extracted from `doc/working/todo_swrite.md` Phase 3 after
the byte-write, DiskIO alignment, and attached-strip prefetch foundations were
implemented.

## Problem

The shared small-write path mirrors each open strip and keeps one 1 MiB shadow
for repair. On strip close, the current foreground converter freezes the image,
retains eight complete data images, and incrementally updates four parity
buffers. Only after the eighth mirror closes does it allocate an EC replacement
and write all eight data plus four parity shards.

Parity calculation is incremental, but data-image retirement and EC DiskIO are
not. A complete 8+4 group remains resident and the eighth strip pays a burst of
12 writes plus metadata preparation and publication. The flow also copies data
that already exists in three mirror replicas.

Chunks are not required to consume eight strips. An early-sealed chunk or final
tail with only one to seven used mirror strips remains valid and must reclaim
all unused group resources.

## Solution

Use one special eight-strip allocation/prefetch group containing eight
three-replica mirror sets and four parity blocks. ChunkDB places all 28 blocks
in one operation. The placement result must contain a choice of one survivor
from each mirror set which, together with the four parity blocks, is an optimal
8+4 EC placement under the current placement policy.

The expected path consumes all eight strips and converts without rewriting data.
A chunk is still allowed to seal after consuming only one to seven strips. Early
seal keeps those used strips mirrored and releases unused attached strips, all
four parity blocks, and the incomplete group plan. This spends uncommon
preallocation work to keep placement and the common conversion path simple.

For the shared writer, the existing 1 MiB buffer view enters parity computation
only after its mirror DiskIO, including replacement, succeeds. The same view
updates four parity accumulators and is then released. The eight survivors
already hold the EC data, so conversion writes only the four parity blocks.
After input eight, it writes parity, establishes durability for all 12 blocks,
and atomically replaces eight MirrorStrips with one EcStrip.

Computing parity before mirror IO is deliberately rejected for shared chunks.
A terminal mirror failure would otherwise require undoing or poisoning a
speculative contribution. Mirror-success-first gives one clear failure boundary
without adding conversion to the foreground response barrier.

### Placement

- ChunkDB allocates all 28 blocks as one special conversion group: eight sets
  of three mirror candidates plus four parity blocks. The group selector solves
  the mirror and final EC constraints together rather than issuing a 12-block
  EC allocation followed by sixteen independent replica allocations.
- The selector records one candidate survivor per mirror set whose 8+4 result
  has the best placement-policy score for the allocation topology snapshot.
  Each three-block set is still an independently valid MirrorStrip attached to
  the Active chunk. Parity ownership stays in a hidden conversion plan and is
  not readable chunk layout.
- The prefetcher may hold multiple bounded groups. Refill and allocation remain
  on the metadata line rather than the object response path.
- Immediately before EC publication and retirement of redundant mirrors,
  ChunkDB reruns the EC selector over the healthy candidate in each mirror set
  and the four parity blocks against current topology. Publication may choose
  different survivors from those preferred at allocation, but the retained
  8+4 set must still have the current optimal placement-policy score.
- If no candidate selection is currently optimal, ChunkDB keeps all mirrors
  authoritative, frees none of their replicas, and admits or retains a
  deterministic `MirrorToEc` task in retry state. The task tracks relocation or
  replacement until an optimal 8+4 set can be published; it is not an
  untracked best-effort cleanup failure.
- If remaining configured chunk capacity cannot hold eight strips, the allocator
  may attach a smaller mirror-only tail without creating a parity plan.

### Conversion and metadata state

The durable group state is:

```text
Allocated -> Collecting -> ParityReady -> Publishing -> Published
    |            |             |             |
    +------------+-------------+-------------+-> Aborted
```

- `Allocated` owns eight attached MirrorStrips and four hidden parity blocks.
- `Collecting` records closed, mirror-durable inputs. The contribution bitmap
  may remain volatile because takeover can rebuild it from authoritative mirrors.
- `ParityReady` has all eight contributions and durable parity output.
- `Publishing` first validates the selected survivors against the current EC
  placement optimum, then uses the existing fenced range replacement and is
  idempotent under ambiguous completion.
- `Published` exposes one EcStrip and records sixteen redundant replicas in the
  layout-validity cleanup intent.
- `Aborted` releases parity and unused-strip allocations while leaving every
  used MirrorStrip readable.

`advance_chunk_write` tracks acknowledged mirror bytes only. It neither waits
for parity nor represents conversion progress. Publication waits until the
persisted cursor covers the complete source range and all eight MirrorStrips are
closed. Returned logical locations remain stable across the range replacement.

### Memory and response contract

- Keep exactly one mutable 1 MiB shadow for the currently open mirror strip.
- Pass a zero-copy `Bytes` view to parity after mirror DiskIO succeeds.
- Release that 1 MiB image after mirror DiskIO and its four parity contributions
  finish. Do not retain eight data images or allocate another staging buffer.
- Keep four bounded 1 MiB parity accumulators. If their budget is unavailable,
  skip foreground conversion and leave mirrors for the background scanner.
- Object acknowledgement waits only for required mirror DiskIO. Cursor advance,
  parity, EC publication, and redundant-replica cleanup remain off the normal
  response path.

### Failure, seal, and cleanup

- A parity or conversion failure never revokes an acknowledged mirror location.
- An unrecoverable foreground mirror failure contributes nothing to parity,
  retires the pipeline, seals at the acknowledged cursor or deletes an empty
  chunk, and returns the write error. Later traffic rotates to a new chunk.
- The sixteen redundant replicas cannot be freed merely because their buffers
  were consumed. They remain referenced until all survivors and parity blocks
  are durable, the retained 8+4 set passes the current optimal-placement check,
  and fenced publication records their cleanup intent.
- A failed optimal-placement check leaves the mirror layout and all replicas
  intact and leaves a persistent conversion task that can relocate blocks and
  retry. It never degrades into an untracked cleanup omission.
- After publication, reclamation follows the existing layout-validity grace
  period so stale readers cannot target recycled blocks.
- Seal does not wait for optional conversion. For an early-sealed group, it keeps
  the used one-to-seven MirrorStrips, removes unused attached strips, cancels the
  parity plan, and releases every unused strip and parity block. A complete
  durable plan is either taken over by the background converter or aborted.

## Dependencies

- Implemented small-write byte path and single-shadow lifetime rules.
- R113 attached-strip batch prefetch, extended to allocate/refill in aligned
  groups of eight and clean unused group resources at seal.
- Existing chunkdb mirror-to-EC prepare/complete protocol and cleanup intent.
- Existing background conversion as the correctness fallback and takeover path.
- A permanent-design update for survivor metadata, group placement, durability
  barriers, early-seal cleanup, and conversion-plan recovery.

R136 reserved-strip confirm is not required. R137 works with R113's attached
strip flow and must remain compatible if R136 later lands.

## Acceptance

- Allocate one shared conversion group in one placement operation: eight
  attached MirrorStrips each contain three candidate survivors and one hidden
  plan owns four parity blocks; the selected eight candidates plus parity have
  the allocator's optimal 8+4 score. Integration test.
- Write only one of the eight strips, then seal. The sealed chunk retains exactly
  one valid MirrorStrip and cleanup releases seven unused strips, four parity
  blocks, and the group plan. Repeat for retained tails of two through seven.
- Feed each exact post-DiskIO 1 MiB buffer view into parity and release it after
  its contribution; peak retained input images remain below two strips per
  group, excluding the active shadow and four parity accumulators.
- Publish EC without rewriting the eight data shards. DiskIO accounting shows
  conversion writes only four parity blocks, and reconstructed data matches all
  eight original images including zero-padded tails.
- A delayed parity operation does not delay object acknowledgement or the next
  mirror write when mirror capacity is available.
- Failure at every collection/publication boundary leaves mirrors readable.
  Restart or lease takeover reconstructs volatile parity from authoritative
  mirrors and never exposes a partial EC layout.
- Cursor advancement never waits for conversion. Publication waits for the
  persisted cursor and exact closed range, then atomically swaps the layout.
- Redundant mirror replicas remain allocated before publication and are freed
  only through the fenced cleanup intent after the grace period.
- Change topology or availability so the allocation-time survivor choice is no
  longer optimal, then complete the group: publication reselects an equally
  optimal candidate set when one exists; otherwise no replica is freed and one
  deterministic retryable `MirrorToEc` task tracks relocation through restart.
  Integration test.
- Placement failure falls back to mirrors/background relocation without failing
  the user write.
- Add unit, placement, real ChunkDB+DiskIO E2E, failure-injection, memory-bound,
  payload-accounting, and latency-regression coverage. Ensure all affected gates
  pass through `pixi run`.

## Open Questions

- Encode the allocation-time survivor preference and candidate sets directly in
  `MirrorStrip`, or in a group plan that old readers ignore.
- When one 28-block group allocation partially fails, retry atomically, reduce
  the mirror-only batch, or rotate to a new chunk. The foreground write must not
  synchronously wait behind repeated placement retries.
- Define the exact durability barrier for the eight survivor writes before EC
  publication: existing mirror completion, per-segment fsync, or a group barrier.
- Decide whether complete durable plans always finish in background after seal
  or may be aborted while the original mirrors remain authoritative.
