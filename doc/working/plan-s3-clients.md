<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Clients Plan

Upstream: [real-client compatibility](../backlog/R200-s3-client-compatibility.md).

Goal: accept reproducible client workflows with verified integrity and truthful prerequisite/API gaps.

## Tasks

- [x] **Default integrity**: pin clients, share bounded UploadBody between protocols, carry the authenticated streaming seed, preserve native ownership and MD5 error codes. Eight encoding tests and fmt/clippy pass.
- [x] **SDK focused acceptance**: default ordinary/multipart CRC32, malformed MD5, presigned GET/PUT expiry/tamper, corrupted/truncated/suffixed trailers and fragmented bodies pass. Fresh trailer gate also verifies forbidden streaming bucket creation leaves absence. Existing cancellation tests remain in the full regression.
- [x] **CLI investigation and AWS recipe**: retain AWS CLI/rclone discovery, transfer, listing, copy, sync and cleanup scenarios. Both configured recipes pass. Files: tools/pixi-tasks/test-s3-client.sh, tests/s3_e2e/clients.py.
- [x] **Positive rclone acceptance**: bounded user metadata persists across PUT, COPY/REPLACE and multipart publication. Canonical rclone task, new-format session restart and full container recovery/restart gates pass. Old-version data compatibility is not required.
- [x] **Container and recipes**: default SDK and configured CLI ordinary/multipart copy pass, with exact bytes after all service crash/hang recovery and persisted-volume restart. Full pixi run test-single-node-container passes. README documents pinned versions/configurations and unsupported workflows.
- [x] **Applicable quality gates**: library/server tests, shared encoding/auth tests, official embedded-copy error test, fmt/clippy, Python compilation, shell syntax and release policy checks pass.
- [ ] **Remaining acceptance — deferred by user**: after the MemTable handoff implementation, all three focused reproductions and two complete accumulated suites pass. The slow-upload case now executes at both Python and Rust entry points. The user asks to stop after critical verification to switch tasks; leave the third complete run, default-concurrency CLI and optional SDK reruns pending, with SDK work last.
- [ ] **Completion cleanup**: retain requirement/index/plan until all positive client and full-stack gates pass, then remove them in the cleanup commit.

## Verification

- Post-handoff verification on 2026-10-03 uses implementation `139149b7`.
  The Python slow-upload skip was removed by that implementation, but both Rust
  entry points still skipped it. Remove those guards and report zero ignored
  cases in the complete suite. Focused default MPU/checksums, thousand-key
  deletion/retry and concurrent slow signed uploads all pass. Library/server
  tests and official boto3 embedded-copy error recognition also pass.
- Accumulated runs 1 and 2 each pass all 32 cases with zero ignored, including the old
  thousand-key failure, CLI/rclone, lost replies, six service restarts and both
  benchmark paths. The third run, default-concurrency CLI and SDK reruns remain
  pending at the user's explicit stop request. Critical reproductions now pass;
  this is not a claim that all R201/R205 acceptance is complete.
  After the canonical task builds services and passes its prerequisite gates,
  repeated runs invoke the same full-stack Cargo target with the pinned Python
  path, preceded by `pixi run clean-env`; no focus/SDK/external-endpoint selector
  is set. Successful fixtures are automatically removed by the harness.

- Unit/integration: pixi run test-access-s3; pixi run test-access-server; affected encoding/auth tests.
- E2E: pixi run clean-env && pixi run -e s3-e2e test-boto3-e2e; registered CLI tasks; pixi run test-single-node-container.
- Gates: pixi run rs-fmt-check; pixi run rs-lint.

## Evidence

- Diagnostic full run confirms R201 relocation visibility: slot 853/modify_ts 60/cursor 24040 is applied and acknowledged, then a flush at frontier 852 unlinks it from table 18 before insertion into 19. Point reads in the gap return L1 slot 843/modify_ts 58/cursor 23039; CAS correctly rejects the next update and chunk-stream reports cursor regression. Relocation restores slot 853 milliseconds later. Full run fails in default boto3 MPU after 66.080 seconds, before the historical thousand-key case. Preserve fixture and diagnostic patch in default-suite-relocation-gap; remove all temporary source instrumentation. R205 records the causal timeline and limits of attribution. User authorizes deferring this acceptance and proceeding to optional SDK work.
- Resumed storage diagnosis finds a deterministic TextPageStore defect: every segment-directory address maps to segdir.crb. The retained repeat fixture has five distinct directory addresses aliased to that file; tree diagnostics identify committed segment directory unreadable rather than a generic snapshot failure. New distinct-address/reopen regression fails before the fix and passes afterward. Use segdir-<address>.crb for new writes; existing manifest filenames still decode. All 589 C++ tree tests pass. Rebuild real-stack service binaries before validating accumulated S3 behavior; journal regression is not yet attributed to this defect or to deferred handoff work.
- Rebuilt full stack still fails thousand-key deletion after 164.424 seconds: expected=341456, new=341663, durable=341042. No segment-directory corruption occurs in this run, separating the two defects. Preserve fixture under .crowdb-runtime/persistent/s3-client-failures/default-suite-addressed-directories. Its binary WAL contains accepted chunk advances to 341249 and 341456 at slots 8434 and 8441. These are durable-record evidence, not by themselves proof of the exact visibility race. C++ gate 589/589, FFI 49/49, tree-lint exit 0 with existing warnings, fmt/clippy pass.
- Temporary apply completion visibility diagnostic does not observe an unpublished nonempty table; the instrumented full run still fails thousand-key deletion after 179.705 seconds. Retain fixture under .crowdb-runtime/persistent/s3-client-failures/default-suite-visibility-diagnostic; remove instrumentation. Audit finds CrowdbTreeEngine::noop calls force_advance_slot, falsely covering earlier pending slots. Deterministic apply(1), flush, noop(3), flush regression reports frontier 3 instead of 1 before delayed apply(2). Change NoOp to empty batch admission so contiguous tracking retains the gap; recovery initialization keeps its explicit force operation. No deferred handoff implementation is applied.
- Ordered NoOp fix passes 232 engine/group/store tests, including delayed-write persistence/reopen, plus fmt/clippy. Rebuilt S3 stack still fails thousand-key deletion after 89.461 seconds with expected=118611, new=118818, durable=118197; no directory corruption. Preserve default-suite-ordered-noop fixture. This confirms the NoOp bug independently but does not establish that it explains the remaining cursor regression. Continue first-divergence diagnosis across ChunkDB cached state and KV CAS/read revisions.
- Copy and batch deletion are complete before beginning this requirement.
- Existing Iceberg upload encoding already validates bounded AWS chunks, five checksums, signed chunk chains and trailers; general S3 currently validates only MD5 and payload SHA256. Reuse the decoder while retaining protocol-specific authority/admission.
- Recipes operate within the current shared listener namespace. Per-user permission expansion is outside the requested scope.
- User removed filesystem mounting from scope. Remove its client, CI, Pixi dependency/tasks and acceptance requirements. Retain valid trailing-slash bucket routing independently of mount clients.
- Scope cleanup passes Python syntax and discovery (22 basic/client cases), shell syntax, full-stack Rust harness compilation, fmt and clippy. Pixi regenerates the lock without the mount client or libfuse3; the separate Python S3 library in the Iceberg environment remains unchanged. This cleanup does not revalidate the outstanding accumulated storage failure.
- Default boto3 ordinary/CRC32/multipart case and eight shared encoding tests pass. AWS CLI discovery exposed missing CreationDate; bucket records lack that field, so the wire now reports a documented stable epoch placeholder.
- The next exact AWS CLI run failed during concurrent multipart UploadPart: KV coalescer watchdog reported stuck batches first, then DiskIO fsync deadlines and chunk-stream metadata conflicts. No crash report was produced; group0 remained alive until harness teardown. Full service logs preserved under .crowdb-runtime/persistent/s3-client-failures/aws-cli-concurrent. Reproducing unchanged before choosing a fix; no timeout or assertion was relaxed.
- Unchanged concurrent reproduction then returned SlowDown promptly. R205 records both observations without assigning an unproven root cause. Explicit concurrency=1 AWS discovery/transfer/prefix/copy/sync/cleanup passes.
- Two real rclone runs rejected x-amz-meta-mtime for PUT and multipart initiation. Bounded user-metadata persistence now resolves that required field without dropping it.
- User explicitly excludes old-version data compatibility. Metadata persistence needs no legacy decoder, migration or upload-draining decision; new-version restart recovery remains required.
- First full default-client run passes copy and default CRC32/multipart, then finds expired presigned URLs accepted because clock skew extends expiry. Remove expiry extension; preserve future-clock tolerance. Add skew-enabled unit regression and retain real HTTP expiry test.
- Release policy checks pass; publication credentials remain isolated from the verification job. Container acceptance includes default boto3 and the accepted AWS CLI/rclone recipes across recovery/restart.
- Full suite then passes all new SDK cases, rclone fail-closed and exact-key batch deletion, but the existing thousand-key case fails after 120.620 seconds. Snapshot Corruption precedes the journal cursor mismatch (expected=382427, new=382634, durable=382013). Preserve fixture under .crowdb-runtime/persistent/s3-client-failures/default-suite-journal; rerun unchanged. R205 includes this serial failure; no unproven R201 attribution or deferred stash application.
- Unchanged full rerun fails the same existing case after 114.277 seconds with cursor regression preceding snapshot errors. Isolated thousand-key test passes on a fresh stack. This identifies accumulated fixture state/load as relevant but does not prove the root cause. Full-suite acceptance remains blocked; no skip or timeout change was introduced. Final fmt/clippy pass.
- Full canonical container gate passes: image/runtime linkage, anonymous/bootstrap boots, browser acceptance, default SDK and CLI writes, ordinary/multipart CLI copy, seven service crash and hang recovery paths, persisted restart, exact SDK/CLI reads, secret-free lifecycle logs, invalid identity/manifest/profile rejection and monitor lifecycle. No image was published.
- Fresh trailer-only real-stack gate passes final streaming route restrictions and old-object preservation. Existing thousand-key isolated gate also passes. These focused successes do not replace the failed accumulated regression.
- Default SDK focused gate now runs assert_native_write_metrics with copy_baseline=0: native receive bytes, framed owners/views, small writes and bounded read metrics are positive, while large payload-copy operations remain zero. Exact ordinary/multipart bytes and CRC32/MD5 failures pass. Final fmt/clippy pass after this focused ownership assertion.
- Verified independent implementation and blocked state committed as d2103fe2. R200 remains open; no final requirement cleanup or push was performed. Both original stashes are preserved.
- User metadata implementation committed as f2790225 after library/server tests, official SDK embedded-error recognition, metadata/copy/MPU cases, six-service metadata/session restart, canonical rclone, fmt/clippy and full container acceptance pass. Container checks verify rclone exact bytes and mtime after all seven crash/hang recoveries and persisted restart. Completed metadata requirement is removed; R205 remains unresolved.
- Optional Java 2.x/JavaScript v3/Go v2 tasks and their sequential aggregate pass normal operations and injected MPU-failure cleanup. Manual-only workflow and [SDK recipes](../../app/crowdb-access-server/tests/common/s3_sdks/README.md) are available outside regular CI/container/release. JavaScript exposed two independent protocol gaps, now fixed: informational SDK user-agent rejection in copy/batch delete, and unverified hoisted presigned upload checksums. These focused successes do not close the accumulated storage regression.

## Files

- Integrity: lib/crowdb-access-s3/src/auth*, app/crowdb-access-server/src/upload_flow/body_encoding*, s3/dispatcher.rs, s3/operations*, iceberg/file_http*, encoding/auth tests.
- Wire: lib/crowdb-access-s3/src/route*, wire.rs and integration tests; server listing selectors.
- Clients: pixi.toml/lock, tools/pixi-tasks/test-s3-client.sh, tests/s3_e2e/{clients,default_client,basic}.py, s3_full_stack_test.rs.
- Container: tests/{s3-client,s3-cli-client}.py, container-e2e.sh, release-policy.sh, README.md.
- Contracts: S3 design, backlog R200/R205 and index.
