<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Client Progress Plan

Upstream: [R205](../backlog/R205-s3-concurrent-client-progress.md).

Verify concurrent client progress after the storage repairs, preserving bounded
admission, exact data and truthful failure evidence.

## Tasks

- [x] **Unchanged concurrent reproduction**: run the canonical concurrency-10
  AWS CLI task on fresh real storage. Correlate the first failure across native
  credits, ChunkDB, KV and DiskIO before selecting a fix.
- [x] **Admission and recovery acceptance**: distinguish finite low-budget
  rejection from lost progress; verify no partial publication and retry/restart
  recovery. Verify sufficient-budget concurrent transfers independently.
- [x] **Owning-component repair**: if reproduced, fix the earliest confirmed
  cause without new hot-path locks, inflated deadlines or caller retries. Add
  focused regression coverage and rerun affected tests and gates.
- [ ] **Completion**: record verified behavior and close the requirement, index
  entry and this plan after acceptance.

## Files

- `tools/pixi-tasks/test-s3-client.sh`, `test-s3-e2e.sh`
- `app/crowdb-access-server/tests/s3_full_stack_test.rs`, `s3_e2e/clients.py`
- Diagnosed owning-component code/tests if a failure is reproduced.

## Verification

- Canonical concurrent CLI fails UploadPart with SlowDown after 6.466 seconds.
  Diagnostic repeat fails after 5.516 seconds: session CAS conflict at revision
  3 leaves part 1 generation 1; two client retries collide with that immutable
  record. No old cursor regression/fsync/coalescer stall is observed. Private
  fixtures: `post-handoff-cli-slowdown`, `post-handoff-cli-diagnostic` under the
  persistent failure directory. Removed all temporary instrumentation.
- Repair helps/re-evaluates after durable session progress and selects immutable
  candidates without orphan poisoning. Generation validation allows strictly
  increasing gaps. All access-S3 tests pass, including ten concurrent part
  publications and identical/different-content orphan regression cases.
- Unchanged concurrency-10 CLI passes after repair on the original 1 MiB
  native-budget fixture. A complete accumulated suite also passes all 32 cases
  with concurrency 10, including zero retained native bytes after CLI cleanup,
  slow overlapping uploads, interrupted-upload recovery, lost replies and six
  service restarts. No budget, caller retries or deadlines were increased.
- Multipart regression suite passes 13/13 including exact frozen selection
  after a generation gap. Workspace fmt/clippy and feature-enabled full-stack
  harness clippy pass. Container verification is running; optional language
  clients remain last in the parent client plan.

- Three accumulated default S3 runs pass after the MemTable repair; latest
  canonical run is 32/32 with zero ignored on 2026-10-03 at `78649976`.
- `pixi run -e s3-e2e test-aws-cli-concurrent`
- Focused failed/interrupted-upload and restart cases through `pixi run`.
- `pixi run rs-fmt-check`, `pixi run rs-lint`; affected component tests and
  changed C++ format/tree-lint if implementation changes are needed.
