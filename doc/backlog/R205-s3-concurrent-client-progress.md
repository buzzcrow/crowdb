<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R205: access-s3 — Concurrent client admission and storage progress

#### Problem

AWS CLI 2.36.47 default concurrent multipart failed on the real-storage fixture
with SlowDown. An unchanged run first logged KV coalescer stuck batches, then
DiskIO fsync deadlines and chunk-stream metadata conflicts, followed by
UploadPart ServiceUnavailable. No crash report was produced; group0 remained
alive until teardown. The fixture has a 1 MiB native receive budget. This
evidence does not identify a specific MemTable or protocol parsing root cause.

The full default-client suite also failed during the existing serial 1,000-key
batch deletion. First divergence was persist_snapshot Corruption at
19:09:57 UTC; journal cursor recovery failed at 19:09:58 with expected=382427,
new=382634, durable=382013. DeleteObjects then returned ServiceUnavailable after
120.620 seconds and SDK retries. Logs and fixture state are retained privately
under .crowdb-runtime/persistent/s3-client-failures/default-suite-journal.
This broadens the reproducer beyond concurrency; neither a successful isolated
case nor a later full-suite pass resolves the observed storage corruption.
An unchanged full-suite reproduction fails the same case after 114.277 seconds:
the cursor first regresses (expected=301076, new=301283, durable=300662), then
snapshot Corruption appears. Both event orders are retained; a specific
snapshot/handoff cause is not yet established. The repeat fixture is under
.crowdb-runtime/persistent/s3-client-failures/default-suite-journal-repeat.
The same thousand-key test passes on a fresh isolated fixture. Accumulated
state/load matters; the full-suite failure remains unresolved.

Resumed diagnosis confirms an independent TextPageStore defect: distinct
segment-directory addresses alias the same segdir.crb file. The fixed backend
uses address-specific filenames; a retained-directory reopen regression fails
before the fix and passes afterward. Rebuilt full-stack verification no longer
reports directory corruption, but still reads a journal cursor 414 bytes behind
its acknowledged position. This separates directory preservation from the
remaining metadata visibility failure; neither is evidence that the deferred
handoff implementation may be applied without review.

The tree-backed KV NoOp path also incorrectly forces its contiguous frontier
past earlier pending apply calls. A delayed write is then rejected below the
flush durable floor, although Paxos already chose it. A deterministic
out-of-order NoOp test reproduces the premature frontier; recording an empty
batch marks only the NoOp slot and retains earlier gaps. Engine, group and
store regressions pass with this fix; full-stack verification is in progress.

The [S3 data path](../design/access-server/s3/design-crowdb-access-s3.md)
requires bounded admission and continued progress under concurrent peers.
Single-concurrency recipes pass but do not certify defaults or resolve stalls.

#### Solution

1. Reproduce with test-aws-cli-concurrent and correlate the first divergence
   across native owner credits, writer admission, KV coalescing and DiskIO.
   Preserve redacted logs/timings and distinguish finite resource rejection
   from a deadlock or lost progress.
2. Fix the confirmed earliest defect in its owning component, preserving
   lock-free hot paths. Increasing budgets, retries or deadlines is not proof
   of fixing progress.
   A NoOp records exactly one applied slot without asserting earlier writes
   complete. Snapshot directory storage keeps distinct durable addresses
   independent, preserving retained anchors.
3. Retain the low-memory recipe and separately gate default concurrency under
   documented sufficient budgets. Rejected requests cannot publish partial
   objects, and admitted work/retries must recover.

#### Dependencies

- R200 retains the concurrent reproducer and verified single-concurrency recipe.
- Deferred MemTable handoff work is related only if evidence confirms it.
- R201 remains user-deferred. Do not apply its stashed implementation or assign
  this snapshot failure to its handoff race without first-divergence evidence.

#### Acceptance

- Given a pending write before an out-of-order NoOp, flush and snapshot, then
  finish the delayed write; assert the frontier does not cross the gap early
  and restart retains the delayed value. Integration test.
- Given two snapshot directories at distinct addresses, write and reopen the
  text backend; assert both exact directory images remain readable. Unit test.
- Given the bounded fixture and concurrent multipart, run the reproducer;
  assert finite admission errors, no progress stall and no partial publication.
  E2E test.
- Given sufficient resources, run default CLI concurrency and overlapping slow
  uploads; assert bytes and eventual release of credits without weakening
  ownership assertions. E2E test.
- Given rejected/interrupted uploads, retry and restart; assert committed data
  remains readable and incomplete candidates invisible. E2E test.

Run pixi run -e s3-e2e test-aws-cli-concurrent,
pixi run clean-env && pixi run -e s3-e2e test-boto3-e2e,
pixi run rs-fmt-check, and pixi run rs-lint, plus the diagnosed component tests.
