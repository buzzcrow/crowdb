<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Fault and Scale-out E2E Plan

Upstream: R172; R152–R166. Goal: turn the current CRUD/restart harness into a
named recovery and horizontal-routing matrix without adding performance work.

## Baseline and existing coverage

- [~] **Audit current matrix**: map
  current boto3 scripts and restart phases to R172 acceptance cases. Files:
  `app/crowdb-access-server/tests/s3_full_stack_test.rs`,
  `app/crowdb-access-server/tests/s3_e2e/`.
- [~] **Record completed baseline**: mark CRUD, range, list, ETag, SigV4,
  lost reply, and access/ChunkDB/DiskDB/DiskIO/Chunk-KV restart cases as
  covered in R172 without duplicating implementation detail. Files:
  `doc/backlog/R172-s3-fault-scaleout-e2e.md`.

## Fault cases

- [ ] **Concurrent CRUD race**: add a deterministic boto3 overwrite/delete/
  GET race with final-generation and namespace assertions. Files:
  `app/crowdb-access-server/tests/s3_e2e/`, `s3_full_stack_test.rs`.
- [ ] **Interrupted PUT retry**: use the existing request identity/lost-reply
  boundary to prove one logical PUT persists exactly once. Files: S3 E2E
  scripts and access-server test.
- [ ] **Pressure cases**: add bounded slow-reader and admission/pool pressure
  assertions only where the existing harness exposes a deterministic control.
  Files: access-server S3 E2E scripts and test configuration.

## Scale-out

- [ ] **Two frontend portability**: route each phase through both listeners,
  then validate concurrent mutations through distinct listeners. Files: S3 E2E
  scripts and full-stack test.
- [ ] **Multiple Chunk-KV owners**: add a second owner only after confirming
  the bootstrap ownership/binding fixture supports an independent partition;
  preserve request identities across a handoff. Files: Chunk-KV harness and
  S3 full-stack test.

## Verification and cleanup

- [ ] **Acceptance gates**: run focused S3 E2E, access-s3 tests, fmt, and
  strict clippy. Files: none.
- [ ] **Requirement cleanup**: delete R172, its index entry, and this plan
  only once every unskipped acceptance case passes. Files: backlog and plan.

## File list

- `app/crowdb-access-server/tests/s3_full_stack_test.rs`
- `app/crowdb-access-server/tests/s3_e2e/`
- `lib/crowdb-test-harness/src/chunk_kv.rs`
- `doc/backlog/R172-s3-fault-scaleout-e2e.md`
- `doc/working/plan-s3-fault-scaleout-e2e.md`

## Tests

- E2E: `pixi run clean-env && CROWDB_S3_E2E_PYTHON=... pixi run -- cargo test -p crowdb-access-server --features s3-e2e --test s3_full_stack_test -- --nocapture`
- Integration: `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- Quality: `pixi run -- cargo fmt --all -- --check`; strict focused clippy.
