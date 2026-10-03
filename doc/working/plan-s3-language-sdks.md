<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Language SDK Verification Plan

Upstream: [manual SDK verification](../backlog/R206-s3-manual-sdk-verification.md).

Goal: independently run pinned Java 2.x, JavaScript v3 and Go v2 clients locally and through manual CI.

## Tasks

- [x] **Manual fixture and Java**: add isolated Pixi environments and wrapper modes for preparation, existing endpoint and real-stack execution; reuse s3_full_stack_test setup without ordinary suite dependencies. Java 2.55.10 scenarios and injected MPU-failure cleanup pass. Files: pixi.toml/lock, tools/pixi-tasks/test-s3-sdk.sh, tests/s3_full_stack_test.rs, tests/common/s3_sdks/java/*.
- [x] **JavaScript**: Node/TypeScript with pinned npm lock passes normal and injected-failure cleanup. Accept informational SDK user-agent headers for copy and batch delete; normalize authenticated presigned query checksums before body verification. Files: tests/common/s3_sdks/js/*, S3 copy/delete/integrity and dispatcher.
- [x] **Go**: pinned Go SDK v2 passes normal and injected-failure cleanup. Use LoadDefaultConfig to obtain default integrity settings, observed CRC32, standard net/http and isolated pure-Go toolchain. Files: tests/common/s3_sdks/go/*.
- [x] **Manual workflow and recipes**: workflow_dispatch selection java/js/go/all, bounded sequential local aggregate, redacted diagnostics, shared external-endpoint verification/cleanup and documented transport/default checksum coverage. Trigger/environment/task exclusion inspected; remote GitHub dispatch has not been run. Files: .github/workflows/s3-sdk-verification.yml, component client recipe README.
- [~] **Verification and cleanup**: standalone entries pass through the sequential aggregate, including injected failure cleanup. Finish affected library/server and optional harness checks; commit implementation then remove completed requirement/index/plan.

## Verification

- Each client: unique owned bucket; default integrity and retries; ordinary and low-level multipart bytes; HEAD/range/metadata; COPY; paginated prefix listing; presigned PUT/GET; single/batch deletion; missing key, bad credentials and corrupt checksum; abort outstanding MPU and clean owned resources in finally/defer.
- Runtime: fresh per-task fixture namespace; external endpoint uses environment credentials; bounded execution with sanitized failure output and interrupt cleanup.
- Gates: independent Pixi SDK tasks and explicit aggregate; workflow dispatch/dependency inspection; pixi run rs-fmt-check and pixi run rs-lint.

## Evidence

- Prior full-suite diagnostics confirm R201 remove-before-publish relocation visibility gap. R205 retains the fixture and causal timeline. User requests proceeding here, with a minimal correctness repair only if this prevents SDK verification. No storage failure may be converted to a passing SDK assertion.
- Java uses URLConnection, default CRC32/retries, sequential low-level MPU and SDK presigning. Normal and injected-failure cleanup pass on a real fixture. Pagination may have an empty terminal page after a full page; assert exact unique keys and per-page bounds rather than a fixed page count.
- JavaScript 3.1146.0 initially fails CopyObject because the finite copy header validator rejects x-amz-user-agent. Accept that informational SDK header without changing copy selection or signed metadata requirements; continue the same scenario. Presigned JS PUT hoists CRC32 into the signed URL query, so explicitly verify corrupt-body rejection as well as successful upload.
- JavaScript reproduces a separate protocol defect: a valid presigned CRC32 URL accepts changed bytes with HTTP 200. Normalize supported signed-query upload checksums only after authentication, reject duplicates/header overlap/wrong operation, and feed the existing body verifier. Retain the actual SDK corrupt-presign assertion and exact original-object check; this issue is independent of the deferred storage race.
- Canonical pixi run test-s3-sdks passes Java, JavaScript and Go standalone tasks sequentially, with normal operations and uploaded-part failure cleanup in each. All three retain default checksums/retries. No temporary storage repair was needed. Fmt and standard clippy pass after extracting authenticated upload preparation from the dispatcher.
- Final gates pass: pixi run test-access-s3; isolated clean-env then test-access-server; rs-fmt-check; rs-lint; cargo clippy for the s3-e2e optional harness; Go vet/build; Java Maven build; TypeScript build; shell syntax. Existing unrelated ignored tests remain unchanged. Manual workflow dispatch is defined and inspected, but has not been executed on GitHub. Ready for implementation commit and completed-requirement cleanup.
