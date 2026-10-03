<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Clients Plan

Upstream: [real-client compatibility](../backlog/R200-s3-client-compatibility.md).

Goal: accept reproducible client workflows with verified integrity and truthful prerequisite/API gaps.

## Tasks

- [x] **Default integrity**: pin clients, share bounded UploadBody between protocols, carry the authenticated streaming seed, preserve native ownership and MD5 error codes. Eight encoding tests and fmt/clippy pass.
- [x] **SDK focused acceptance**: default ordinary/multipart CRC32, malformed MD5, presigned GET/PUT expiry/tamper, corrupted/truncated/suffixed trailers and fragmented bodies pass. Fresh trailer gate also verifies forbidden streaming bucket creation leaves absence. Existing cancellation tests remain in the full regression.
- [x] **CLI investigation and AWS recipe**: retain AWS CLI/rclone discovery, transfer, listing, copy, sync and cleanup scenarios. The configured AWS recipe passes; rclone metadata rejection is traced and positive acceptance stays pending R204. Files: tools/pixi-tasks/test-s3-client.sh, tests/s3_e2e/clients.py.
- [x] **Mounted investigation**: actual s3fs mount succeeds, mkdir fails on x-amz-meta-atime. Fix single trailing-slash bucket routing, avoid blocking FUSE stat for mount detection, isolate filesystem calls in a bounded worker, retain required manual CI gate. Files: tests/s3_e2e/fuse_client.py, .github/workflows/s3-fuse.yml.
- [ ] **Positive rclone/FUSE acceptance**: resume mandatory metadata, mounted operations and remount gates after R204 decisions and implementation.
- [x] **Container and recipes**: default SDK and configured CLI ordinary/multipart copy pass, with exact bytes after all service crash/hang recovery and persisted-volume restart. Full pixi run test-single-node-container passes. README documents pinned versions/configurations and unsupported workflows.
- [x] **Applicable quality gates**: library/server tests, shared encoding/auth tests, official embedded-copy error test, fmt/clippy, Python compilation, shell syntax and release policy checks pass.
- [~] **Accumulated full-stack gate**: diagnose the first storage divergence, fix confirmed upstream defects and rerun without skipping tests. Addressed TextPageStore directory preservation is verified. Out-of-order NoOp currently forces the tree frontier past pending writes; a deterministic delayed-write regression fails before replacing forced advancement with an empty batch. Verify engine/Paxos tests and rebuilt full S3 stack.
- [ ] **Completion cleanup**: retain requirement/index/plan until all positive client and full-stack gates pass, then remove them in the cleanup commit.

## Verification

- Unit/integration: pixi run test-access-s3; pixi run test-access-server; affected encoding/auth tests.
- E2E: pixi run clean-env && pixi run -e s3-e2e test-boto3-e2e; registered CLI/FUSE tasks; pixi run test-single-node-container.
- Gates: pixi run rs-fmt-check; pixi run rs-lint.

## Evidence

- Resumed storage diagnosis finds a deterministic TextPageStore defect: every segment-directory address maps to segdir.crb. The retained repeat fixture has five distinct directory addresses aliased to that file; tree diagnostics identify committed segment directory unreadable rather than a generic snapshot failure. New distinct-address/reopen regression fails before the fix and passes afterward. Use segdir-<address>.crb for new writes; existing manifest filenames still decode. All 589 C++ tree tests pass. Rebuild real-stack service binaries before validating accumulated S3 behavior; journal regression is not yet attributed to this defect or to deferred handoff work.
- Rebuilt full stack still fails thousand-key deletion after 164.424 seconds: expected=341456, new=341663, durable=341042. No segment-directory corruption occurs in this run, separating the two defects. Preserve fixture under .crowdb-runtime/persistent/s3-client-failures/default-suite-addressed-directories. Its binary WAL contains accepted chunk advances to 341249 and 341456 at slots 8434 and 8441. These are durable-record evidence, not by themselves proof of the exact visibility race. C++ gate 589/589, FFI 49/49, tree-lint exit 0 with existing warnings, fmt/clippy pass.
- Temporary apply completion visibility diagnostic does not observe an unpublished nonempty table; the instrumented full run still fails thousand-key deletion after 179.705 seconds. Retain fixture under .crowdb-runtime/persistent/s3-client-failures/default-suite-visibility-diagnostic; remove instrumentation. Audit finds CrowdbTreeEngine::noop calls force_advance_slot, falsely covering earlier pending slots. Deterministic apply(1), flush, noop(3), flush regression reports frontier 3 instead of 1 before delayed apply(2). Change NoOp to empty batch admission so contiguous tracking retains the gap; recovery initialization keeps its explicit force operation. No deferred handoff implementation is applied.
- Ordered NoOp fix passes 232 engine/group/store tests, including delayed-write persistence/reopen, plus fmt/clippy. Rebuilt S3 stack still fails thousand-key deletion after 89.461 seconds with expected=118611, new=118818, durable=118197; no directory corruption. Preserve default-suite-ordered-noop fixture. This confirms the NoOp bug independently but does not establish that it explains the remaining cursor regression. Continue first-divergence diagnosis across ChunkDB cached state and KV CAS/read revisions.
- Copy and batch deletion are complete before beginning this requirement.
- Existing Iceberg upload encoding already validates bounded AWS chunks, five checksums, signed chunk chains and trailers; general S3 currently validates only MD5 and payload SHA256. Reuse the decoder while retaining protocol-specific authority/admission.
- R206 records the separate principal/grant authority decision; recipes operate within the current listener realm.
- Default boto3 ordinary/CRC32/multipart case and eight shared encoding tests pass. AWS CLI discovery exposed missing CreationDate; bucket records lack that field, so the wire now reports a documented stable epoch placeholder.
- The next exact AWS CLI run failed during concurrent multipart UploadPart: KV coalescer watchdog reported stuck batches first, then DiskIO fsync deadlines and chunk-stream metadata conflicts. No crash report was produced; group0 remained alive until harness teardown. Full service logs preserved under .crowdb-runtime/persistent/s3-client-failures/aws-cli-concurrent. Reproducing unchanged before choosing a fix; no timeout or assertion was relaxed.
- Unchanged concurrent reproduction then returned SlowDown promptly. R205 records both observations without assigning an unproven root cause. Explicit concurrency=1 AWS discovery/transfer/prefix/copy/sync/cleanup passes.
- Two real rclone runs reject x-amz-meta-mtime for PUT and multipart initiation. No supported configuration removes this mandatory field; R204 owns persistence instead of dropping it.
- Actual FUSE is available. The initial root request loop selected GetObject for /bucket/?list-type=2; corrected bucket trailing-slash routing. Bounded rerun mounts and fails mkdir in 0.618 seconds, with server diagnostic x-amz-meta-atime. This is not a prerequisite skip.
- First full default-client run passes copy and default CRC32/multipart, then finds expired presigned URLs accepted because clock skew extends expiry. Remove expiry extension; preserve future-clock tolerance. Add skew-enabled unit regression and retain real HTTP expiry test.
- Release policy checks pass; publication credentials remain isolated from the verification job. Container acceptance includes default boto3 and the accepted AWS CLI recipe across recovery/restart. Rclone remains excluded until its positive gate passes.
- Full suite then passes all new SDK cases, rclone fail-closed and exact-key batch deletion, but the existing thousand-key case fails after 120.620 seconds. Snapshot Corruption precedes the journal cursor mismatch (expected=382427, new=382634, durable=382013). Preserve fixture under .crowdb-runtime/persistent/s3-client-failures/default-suite-journal; rerun unchanged. R205 includes this serial failure; no unproven R201 attribution or deferred stash application.
- Unchanged full rerun fails the same existing case after 114.277 seconds with cursor regression preceding snapshot errors. Isolated thousand-key test passes on a fresh stack. This identifies accumulated fixture state/load as relevant but does not prove the root cause. Full-suite acceptance remains blocked; no skip or timeout change was introduced. Final fmt/clippy pass.
- Full canonical container gate passes: image/runtime linkage, anonymous/bootstrap boots, browser acceptance, default SDK and CLI writes, ordinary/multipart CLI copy, seven service crash and hang recovery paths, persisted restart, exact SDK/CLI reads, secret-free lifecycle logs, invalid identity/manifest/profile rejection and monitor lifecycle. No image was published.
- Fresh trailer-only real-stack gate passes final streaming route restrictions and old-object preservation. Existing thousand-key isolated gate also passes. These focused successes do not replace the failed accumulated regression.
- Default SDK focused gate now runs assert_native_write_metrics with copy_baseline=0: native receive bytes, framed owners/views, small writes and bounded read metrics are positive, while large payload-copy operations remain zero. Exact ordinary/multipart bytes and CRC32/MD5 failures pass. Final fmt/clippy pass after this focused ownership assertion.
- Final real FUSE rerun mounts, rejects mkdir on x-amz-meta-atime in 0.511 seconds, unmounts and exits without leftover daemon/worker. R204 remains a known failing positive gate; prerequisites are available.
- Verified independent implementation and blocked state committed as d2103fe2. R200 remains open; no final requirement cleanup or push was performed. Both original stashes are preserved.

## Files

- Integrity: lib/crowdb-access-s3/src/auth*, app/crowdb-access-server/src/upload_flow/body_encoding*, s3/dispatcher.rs, s3/operations*, iceberg/file_http*, encoding/auth tests.
- Wire: lib/crowdb-access-s3/src/route*, wire.rs and integration tests; server listing selectors.
- Clients: pixi.toml/lock, tools/pixi-tasks/test-s3-client.sh, tests/s3_e2e/{clients,default_client,fuse_client,basic}.py, s3_full_stack_test.rs, .github/workflows/s3-fuse.yml.
- Container: tests/{s3-client,s3-cli-client}.py, container-e2e.sh, release-policy.sh, README.md.
- Contracts: S3 design, backlog R200/R204/R205 and index.

## Blocked

- Positive rclone/s3fs acceptance requires the durable multipart metadata migration and authority/ACL choices listed in R204/R206. Alternatives are recorded there; rejecting required headers preserves the current contract. User explicitly authorizes backlog issues and continued independent work, so SDK/AWS/container gates continue.
- R200 detail/index/plan must remain until positive client acceptance passes. Final cleanup is deferred; partial implementation is not completion.
