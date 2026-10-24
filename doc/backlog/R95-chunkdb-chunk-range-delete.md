<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R95: chunkdb — Qualified chunk range deletion and orphan scan

## Problem

Shared chunks contain ranges owned by different objects or multipart parts.
Deleting a whole chunk for one unreachable range can erase live neighbors.
Uploads may also write a chunk and fail before their part or final object
reference is recorded. A cleanup queue populated by each MPU mutation would
add metadata writes to the upload path and still miss those pre-record crashes.
The [ChunkDB design](../design/chunkdb/design-crowdb-chunkdb.md) separates
logical range retirement from physical strip release. A successful notification
does not supply independent orphan discovery or publication fencing.

Current user-data frames contain length and write time but no final resource
identity. Dataset upload expiry discovers ranges through upload/part records;
Iceberg discovers pre-publication ranges through block intents. Removing these
records before an independent discovery path exists would leak writes that
crash before publishing their result. A second failure window is publication
admission before expiry followed by an indefinitely delayed final write after
GC has reclaimed its physical range. A local clock check and a long grace
period do not fence that delayed write.

## Solution

1. Complete `DeleteChunkRange(chunk_id, offset, size)` in the chunkdb protocol,
   client and server. Access submits one notification without a preliminary
   QueryChunk or whole-chunk deletion. An optional observed chunk revision
   adds a strict layout precondition; otherwise ChunkDB obtains and atomically
   conditions on its current layout. Only sealed user-data chunks accept exact
   byte ranges; a dedicated chunk additionally requires its complete sealed
   object extent. Stale explicit layouts and active writers are rejected or
   deferred. Persist merged dead intervals and the sealed physical
   identity as resource metadata, atomically fenced by the chunk revision.
   This background resource state is approved; per-upload process records are
   not. Successful retirement completes the caller's notification responsibility;
   Access must not wait for physical reclamation or poll the returned physical
   status. ChunkDB owns subsequent release and restart recovery independently.
   Never round a dead frame outward to the usage bitmap's coarser granularity.
2. Use a bounded, chunk-centered scanner to find old chunks and ranges whose
   bytes have no live owner. Do not create per-MPU cleanup intents, candidate
   records or a durable cleanup queue. Start with a configurable age threshold
   of one day; a candidate must be older than the threshold after its last
   write. Age alone never authorizes deletion. Enumerate complete, CRC-validated
   frames only within a sealed chunk's acknowledged extent. A malformed frame,
   incomplete boundary or uncertain authority defers its range; never scan
   past corruption by guessing the next object boundary.
   New frame formats must carry enough resource identity to resolve the exact
   final publication authority and its fixed eligibility deadline. Encode this
   in the existing data write, without a separate foreground intent/index RPC.
   A digest of a variable key is insufficient unless it directly addresses the
   final authority. Header extensions change per-frame payload capacity: the
   location format and native reader/writer arithmetic must select the layout
   unambiguously. Continue reading old frames and retain their existing minimum
   discovery facts until their ranges can be independently qualified.
3. Compare each candidate against published S3 objects, Iceberg file
   descriptors, committed Dataset/Split manifests and still-eligible completed
   parts. Unpublished parts have an explicit commit deadline. Final publication
   must be fenced against expiry reclamation, including a request admitted
   before expiry whose publication arrives later. A lost part-publication
   response or missing intermediate MPU record is not proof that a chunk is
   unused. Refuse deletion when references or the publication fence cannot be confirmed.
   Dataset's immutable part authority permits a direct boundary: an expired
   missing part and its delayed final creation compete on the same part key.
   GC installs a small permanent retirement value before retiring the range;
   the delayed create-if-absent must then fail. Do not delete this value after
   the range notification, which would reopen the key to an arbitrarily late
   creation. If a final part exists with different exact locations, a losing
   physical write cannot replace that immutable result. Existing owners and
   complete final references protect the matching locations beyond the temporary
   deadline. This eliminates process progress, not final resource facts.
4. Keep S3 MPU session, current-part and immutable part-generation keys under
   a common upload prefix in Chunk-KV so the scanner can enumerate one upload's
   references with a bounded prefix scan. The S3 multipart authority owns that
   key layout and Abort/expiry semantics. Only completed data resources and
   published references are required by this contract; it does not authorize
   durable upload workflows. Old part generations remain available until the
   scanner proves their bytes are unreachable. An aborted or expired upload becomes a candidate only after
   the age gate and reference checks.
5. Recheck chunk identity, layout generation and exact range against current
   authority immediately before reclaim. Treat lost delete replies
   idempotently and keep an in-memory scan cursor and bounded work budget.
   Restart may rescan old chunks. Report examined,
   eligible, retired, deferred and physically reclaimed bytes without per-upload
   metric labels. Snapshot deletion denies new requests and waits its persisted
   grace period; readers are not registered or renewed. Layout retirement uses
   its bounded validity interval; overdue readers may fail explicitly.
6. Establish overwrite publication authority before removing S3/Iceberg
   discovery records. S3 ordinary objects are directly overwritten at their
   tenant/bucket/object key and have no globally addressed immutable file
   authority. Current Iceberg files publish at a canonical table path and file
   name; the catalog/file-ID lookup is a legacy path, not the authority for
   every new file. Neither can safely use a permanent tombstone at a mutable
   name alone: a legitimate replacement must remain possible, while an old
   generation must never return. A separate generation index alone also leaves
   a cross-key race between checking eligibility and publishing the final name.
   Select an authoritative conditional mutation or an immutable payload
   generation plus an atomic reference boundary before implementing this part.
   Specify the additional resource fields and foreground RPC cost for review;
   do not add a request ledger, journal or response receipt by default.
7. Keep complete-result expiry discoverable. Dataset Snapshot candidate and
   Split revision roots need fixed creation/eligibility facts in their existing
   resource writes; retries must not extend their original deadline. Complete
   unreferenced results remain readable by ID until retirement. Discover and
   fence both partial and complete expired resources, preserving latest/stable,
   retention, live children and inherited/shared data. An owner acquired before
   the final root completes must not pin an abandoned payload forever. These
   authority facts are a dependency of orphan qualification, not a reason to
   restore editing/evaluation workflows.

## Dependencies

- R92 supplies in-chunk strip reclamation after R95 qualifies dead ranges.
- The S3 multipart authority supplies grouped MPU keys and completed part
  and final object references. The scanner also recognizes Iceberg's frozen
  MPU selection.
- Published references, part deadlines and deletion/layout grace must be
  verifiable before physical deletion is enabled; R168 may use the qualified range-delete
  interface for ordinary S3 object deletion.
- Access retains the existing minimum physical-range discovery facts until
  the frame scanner and final publication fences are verified. Access cleanup
  does not implement background physical scanning or wait for physical free.
- The overwrite authority choice below and complete-resource expiry must land
  before removing their only orphan-discovery path. Without them, defer unsafe
  ranges and retain the existing facts; do not report the scanner as complete.

## Acceptance

- Given a shared chunk with live and unreachable ranges, when the scanner
  evaluates a candidate older than one day, assert only the exact unreachable
  range is passed to `DeleteChunkRange`. Invariant: a live neighbor is never
  reclaimed. E2E test.
- Given an MPU part that was replaced, aborted or expired, when its grouped KV
  prefix is scanned, assert old locations remain protected by any active or
  frozen selection and become eligible only after the age and reference
  checks. Invariant: no extra MPU cleanup record is required. Integration test.
- Given a write that crashed before part metadata publication, when the chunk
  scan runs, assert it defers the chunk until the age gate and in-flight writer
  proof settle, then discovers the orphan without a part record. Invariant:
  missing upload metadata alone cannot reclaim data. Integration test.
- Given an unexpired deletion/layout grace, changed layout generation or uncertain authority,
  when range deletion is attempted, assert no bytes are reused. After the
  grace expires and identity is confirmed, repeat the same delete through a
  lost reply and assert one idempotent result. Invariant: reclamation is fenced
  by current chunk and publication authority, without reader sessions. Integration test.
- Given a large backlog and foreground load, when scanning runs, assert it
  respects item, byte and time budgets and reports deferred/reclaimed progress.
  Invariant: cleanup cannot monopolize the data path. Integration test.

- Given adjacent, overlapping and unaligned byte ranges, when the server records
  their retirement and restarts, assert exact union, unchanged live gaps and
  idempotent replay. A new stale-layout range fails without mutation; an already
  retired range can be queried again. Invariant: monotonic exact retirement.
  Integration test.
- Given retirement success with blocks still allocated, when the client receives
  that response, assert Accepted without further polling. Invariant: notification
  completion is independent of physical capacity recovery. Unit test.
- Given an expired Dataset part whose final creation is paused in transit, when
  GC seals the same resource key and notifies range retirement, then resume the
  write and retry it after worker restart, assert both creations fail and the
  permanent resource fence remains. Invariant: retirement cannot resurrect.
  Integration test.
- Given a complete part shared by live Snapshot owners and an abandoned root
  that acquired an owner before completing, when expiry scans run after restart,
  assert only the abandoned owner's reference is retired, matching live bytes
  survive, and a complete resource's original deadline is unchanged by retry.
  Invariant: final reference facts determine liveness. Integration test.
- Given an old S3 PUT or Iceberg file publication paused after its eligibility
  check, when its generation expires and a new generation is published, resume
  the old mutation and assert it cannot replace the new value or reference the
  retired bytes. Repeat with a lost mutation reply. Invariant: overwrite
  publication and generation retirement share an authoritative boundary.
  Integration test.
- Given old frames and new frames with extended resource headers, when native
  and HTTP readers request first, interior and final frame ranges before and
  after restart, assert exact logical bytes without collecting a copy or using
  the old fixed-header offset arithmetic for the new layout. Corrupt or
  incomplete resource headers defer reclamation. Invariant: format-compatible
  physical discovery preserves readable data. Integration test.

## Open Questions

- Which authoritative overwrite boundary should S3 and current Iceberg file
  publication use? An in-authority conditional mutation must fence the exact
  retired generation without an extra eligibility-read race. An immutable
  payload-generation resource adds at least one resource write per generation
  and still requires an atomic final reference boundary. Describe the concrete
  failure flow and RPC cost before selecting either; an independent index or a
  local expiry check is insufficient.

Required gates:

- `pixi run clean-env && pixi run cargo test -p crowdb-chunkdb --all-targets`
- `pixi run cargo test -p crowdb-chunk-client --all-targets`
- `pixi run cargo test -p crowdb-access-s3 --all-targets`
- `pixi run cargo test -p crowdb-access-dataset --all-targets`
- `pixi run cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run cargo fmt --all -- --check`
- `pixi run rs-lint`
