<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Library Plan

Source: [`../backlog/R142-chunk-kv-library.md`](../backlog/R142-chunk-kv-library.md).

Goal: implement an embeddable, epoch-fenced, range-partitioned KV state machine
whose ordered journal is R141 and whose durable tree is R140.

## Phase 1: Types, Framing, and Injected Contracts

- [x] Add `crowdb-chunk-kv` to the workspace with partition/range/epoch,
  request identity, mutation/condition/result, journal position, lifecycle,
  checkpoint, split plan/artifact/proof, and typed error models.
- [x] Implement canonical operation digests and checksummed bounded WAL frame
  encode/decode with corruption and incomplete-tail handling.
- [x] Extend WAL framing with the physical `chunk_id` trailer: submit
  header+body+CRC through R141 chunk-bound append, validate the trailer against
  read provenance, and keep the durable cursor as the recovery upper bound.
  Files: `lib/crowdb-chunk-kv/src/partition/`,
  `lib/crowdb-chunk-kv/tests/`.
- [x] Add private journal and tree contracts plus in-memory `test-util`
  implementations; do not expose a raw R141 stream from the partition API.
- [~] Add production adapters for R140 native tree construction and the R141
  stream registry, metadata store, and chunk store. Checkpoints and catalog
  artifacts carry an explicit tree ID. Export lifetime-safe ChunkDB/DiskIO RPC
  routes, assemble the native chunk transport, and persist the root catalog
  through generation CAS. Files: `lib/crowdb-chunk-kv/src/`,
  `lib/crowdb-tree/ffi/`, `lib/crowdb-chunk-client/`,
  `lib/crowdb-chunkdb-client/`.

## Phase 2: Partition Sequencer and Reads

- [x] Implement lock-free bounded request/frame admission and one ordered MPSC
  sequencer with staged-overlay conditional evaluation.
- [x] Journal resolved success/failure, apply only after durability, retain
  request results/digests, and publish durable/applied frontiers.
- [x] Implement range-checked point reads, min-position waits, bounded forward
  and reverse scans, and native ceiling/higher/floor/lower seek across L0 and
  L1 without materializing and sorting.
- [x] Cover multi-partition independence, retries/conflicts/expiry, concurrent
  conditions, pending-read visibility, stalls, and apply uncertainty.
- [x] Expose per-partition lock-free counters for mutation outcomes, rejects,
  admission, stalls, recovery, checkpoints, ordered seeks/scans, and split
  control, maintenance reclamation, native pin state, and detailed split work.

## Phase 3: Checkpoint, Replay, and Transfer

- [x] Publish checkpoint tuples and enforce frontier/result-retention/trim
  invariants for the injected journal boundary.
- [x] Decode and replay the durable suffix without reevaluating conditions;
  reject sequence/epoch/digest conflicts and recover one partition only.
- [x] Fence/drain a lower epoch and reopen the same tree and stream identities
  at a higher epoch without data copy.
- [x] Wire checkpoint retention, orphan reporting, and prefix reclamation to
  durable R141 metadata watermarks. Files: `lib/crowdb-chunk-kv/src/`,
  `lib/crowdb-chunk-stream/src/`.

## Phase 4: Online Split

- [x] Validate idempotent split preparation state and exact typed catalog
  proofs. R143 remains responsible for durable plan publication.
- [x] Rebuild exact children from one R140 manifest and replay serving deltas
  with matching mutation/no-op sequence advancement.
- [x] Enforce lag limits, fence/drain the parent, checkpoint both children at
  cutover `c`, and return one immutable prepared artifact.
- [x] Open assigned children as non-serving `Prepared` handles and activate
  them only after exact catalog proof. Files: `lib/crowdb-chunk-kv/src/`,
  `lib/crowdb-chunk-kv/tests/`.
- [~] Resolve exact commit/abort proofs fail-closed; bounded post-commit
  materialization/repack remains production adapter work.

## Phase 5: Gates and Documentation

- [~] Run C++ format/lint/tree tests, Rust format/lint, stream/chunk-KV tests,
  and the server gate through `pixi run`. C++ build/format, affected C++ tests,
  Rust format/clippy, and the complete chunk-KV target suite pass. The user
  already completed the full suite and tree-lint run; rerun the final focused
  gates after the durable catalog and benchmark land.
- [ ] Measure queue, replay, split-fence, latency, and memory behavior on target
  hardware and commit evidence-backed defaults. Files:
  `lib/crowdb-chunk-kv/`, `tools/`.
- [x] Fold the stable design into `doc/design/chunkds/`, update the document
  index, and keep unresolved implementation gaps in the backlog's final
  `Open Issues` section.
- [ ] Remove completed backlog files only in a separate cleanup commit after
  every required production acceptance is satisfied.
