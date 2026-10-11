<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Optional S3 language SDK verification

- Run from the repository root through Pixi. Language clients live in separate
  `s3-java`, `s3-js` and `s3-go` projects, with their existing dependency locks.
- The unified runner starts a fresh protected, persistent release-container
  stack for each language; clients execute on the host. Fixed service ports
  belong to independent bridge networks, so fixture concurrency is supported.
- The `s3.sdk` scope also runs as part of the S3 container gate. Every recipe
  includes normal operations and injected MPU-failure cleanup.
- Java: AWS SDK 2.55.10, OpenJDK 21, Maven 3.9, URLConnection transport.
- JavaScript: AWS SDK v3 3.1146.0, Node.js 22, TypeScript, Node HTTP transport.
- Go: AWS SDK v2 core 1.47.1 / S3 1.114.0, Go 1.25, net/http transport,
  CGO disabled. Toolchains are resolved in pixi.lock; npm dependencies in
  package-lock.json, Go modules in go.mod/go.sum, Java dependencies in pom.xml.

```sh
pixi run test-s3-java-sdk
pixi run test-s3-js-sdk
pixi run test-s3-go-sdk
# Explicit sequential aggregate:
pixi run test-s3-sdks
```

## Existing test endpoint

Set CROWDB_S3_E2E_ENDPOINT, CROWDB_S3_E2E_ACCESS_KEY and
CROWDB_S3_E2E_SECRET_KEY in the environment, then run the same task. The access
and secret keys also accept AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
Use an endpoint whose credentials allow bucket creation and deletion.
Do not put credentials in commands or commit them to client configuration.

- The endpoint branch does not build or start CROWDB services.
- Every client creates a random, uniquely named bucket, deletes only its own
  objects and outstanding multipart uploads, deletes that bucket and verifies
  absence. It never clears an existing bucket.
- Normal and injected-failure cases each have a 300-second limit plus a
  30-second termination grace period. A failure after an uploaded part must
  clean its bucket and unfinished MPU before returning the expected exit 42.
  Unexpected errors, timeouts or cleanup errors fail the task.
- Interrupted clients attempt cleanup while the endpoint remains available.
  Forced termination or an unavailable endpoint can prevent cleanup; the task
  reports failure rather than claiming resources were removed.
- Diagnostics show scenario stages and error codes/types. They omit SDK
  exception messages, stack traces, credentials and presigned URLs. The runner deletes the private credential handoff file immediately and redacts
  client diagnostics before saving artifacts.

## Verified contract

Local verification on 2026-10-11 runs Java, Go and JavaScript sequentially on
fresh 14-node formal release-container fixtures. Each pinned recipe passes normal operations and
the injected uploaded-part failure with verified bucket/MPU cleanup. The
GitHub Actions workflow has not been dispatched by this local verification.

- Explicit custom endpoint, region us-east-1 and path-style addressing.
- Default SDK request checksum calculation, response validation and retries
  remain enabled. Each client observes CRC32 on its ordinary PUT. The endpoint
  does not promise every optional response checksum negotiation mode.
- Bucket discovery/lifecycle; exact ordinary bytes; ranged GET; HEAD and user
  metadata; COPY and REPLACE metadata; bounded prefix pages with no duplicate
  or omitted names; single and batch deletion.
- Sequential low-level multipart upload: one 5 MiB part plus a final small
  part, list/complete, exact download and metadata; separate upload/abort.
- SDK presigned PUT/GET. Java and Go construct presigned PUT without a body;
  JavaScript signs a known body with its default hoisted CRC32 and additionally
  verifies that changed bytes are rejected without overwriting the object.
- Missing-object and invalid-credential errors; explicit incorrect CRC32
  rejection and old-object preservation; cleanup after injected MPU failure.
- High-level transfer managers, concurrent multipart workloads, alternate Java
  HTTP transports and browser JavaScript are outside this recipe.

The accumulated general S3 gate covers storage visibility, interrupted uploads
and service restart recovery. AWS CLI's separate concurrency-10 gate covers its
configured classic transfer recipe. These fresh SDK scenarios cover the
sequential low-level operations above; they do not establish unrestricted
concurrent behavior across other client transports or transfer managers.

## Manual GitHub Actions

Dispatch `.github/workflows/s3-sdk-verification.yml` and select java, js, go or
all. It invokes the same tasks above, with all running sequentially. It has no
push, pull-request, schedule or reusable-workflow trigger. GitHub execution
requires a manual dispatch; local verification does not imply a remote CI run.
