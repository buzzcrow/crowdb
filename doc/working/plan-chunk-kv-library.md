<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Library Plan

Source: [`design-chunk-kv-library.md`](design-chunk-kv-library.md) and
[`../backlog/R142-chunk-kv-library.md`](../backlog/R142-chunk-kv-library.md).

Goal: implement an embeddable, epoch-fenced, range-partitioned KV state machine
whose ordered journal is R141 and whose durable tree is R140.

## Phase 1: Types, Framing, and Injected Contracts

- [ ] Add `crowdb-chunk-kv` to the workspace with partition/range/epoch,
  request identity, mutation/condition/result, journal position, lifecycle,
  checkpoint, split plan/artifact/proof, and typed error models.
- [ ] Implement canonical operation digests and checksummed bounded WAL frame
  encode/decode with corruption and incomplete-tail handling.
- [ ] Add private journal and tree contracts plus in-memory `test-util`
  implementations; do not expose a raw R141 stream from the partition API.

## Phase 2: Partition Sequencer and Reads

- [ ] Implement lock-free bounded request/frame admission and one ordered MPSC
  sequencer with staged-overlay conditional evaluation.
- [ ] Journal resolved success/failure, apply only after durability, retain
  request results/digests, and publish durable/applied frontiers.
- [ ] Implement range-checked point reads, min-position waits, bounded scans,
  and forward seek; add real C++ reverse cursor support for reverse operations.
- [ ] Cover multi-partition independence, retries/conflicts/expiry, concurrent
  conditions, pending-read visibility, stalls, and apply uncertainty.

## Phase 3: Checkpoint, Replay, and Transfer

- [ ] Publish checkpoint tuples and enforce frontier/pin/trim invariants.
- [ ] Decode and replay the durable suffix without reevaluating conditions;
  reject sequence/epoch/digest conflicts and recover one partition only.
- [ ] Fence/drain a lower epoch and reopen the same tree and stream identities
  at a higher epoch without data copy.

## Phase 4: Online Split

- [ ] Validate and persist idempotent split preparation state and pin base `b`.
- [ ] Rebuild exact children from one R140 manifest and replay serving deltas
  with matching mutation/no-op sequence advancement.
- [ ] Enforce lag limits, fence/drain the parent, checkpoint both children at
  cutover `c`, and return one immutable prepared artifact.
- [ ] Resolve exact commit/abort proofs fail-closed and schedule bounded
  post-commit materialization/repack.

## Phase 5: Gates and Documentation

- [ ] Run C++ format/lint/tree tests, Rust format/lint, stream/chunk-KV tests,
  and the server gate through `pixi run`.
- [ ] Fold the stable design into `doc/design/kv/` and update the document
  index while preserving unresolved production items in the final section.
- [ ] Remove completed backlog files only in a separate cleanup commit after
  every required production acceptance is satisfied.

## Open Issues

- Production R140 page-store and R141 registry/metadata/chunk adapters are not
  yet simultaneously constructible from this library.
- Reverse C++ cursors, durable catalog proofs, checkpoint retention, orphan
  reporting, and post-split physical separation remain implementation work.
- Hardware evidence is required before finalizing queue, replay, fence, and
  memory defaults.
