<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R206: access-s3 — Manual Java, JavaScript and Go SDK verification

#### Problem

The current general S3 client acceptance covers boto3 and a configured AWS CLI
recipe, with rclone coverage under active implementation. These workflows do
not establish compatibility with independent language SDK request construction,
checksum defaults, streaming, retries and multipart transfer behavior. The
finite [S3 design](../design/access-server/s3/design-crowdb-access-s3.md)
requires explicit tested workflows rather than a blanket compatibility claim.

Additional language toolchains and real-storage stacks must remain optional.
The user requests standalone local execution and manually dispatched GitHub
Actions, following the existing Iceberg SDK verification pattern.

#### Solution

1. Add separate real-client programs for AWS SDK for Java 2.x, AWS SDK for
   JavaScript v3 (Node.js/TypeScript), and AWS SDK for Go v2. Pin SDK and toolchain
   versions through isolated environments and language lock/build files. Reuse
   fixture conventions from app/crowdb-access-server/tests/common/iceberg_java,
   the S3 full-stack harness and tools/pixi-tasks/test-iceberg-sdk.sh; exercise
   the general S3 endpoint through each actual SDK. Do not substitute boto3 or
   CLI calls for language SDK operations.
2. Define one shared finite scenario contract: bucket discovery and lifecycle;
   ordinary and multipart upload/download; ranged reads; prefix listing with
   continuation; HEAD and user metadata; object copy; single and batch deletion;
   presigned GET/PUT; and missing-object, invalid-credential and bad-integrity
   failures. Assert exact payloads, names, metadata and response outcomes.
   Retain SDK default checksum and retry behavior, and distinguish low-level
   MPU calls from any tested high-level transfer helpers. Document every
   required endpoint, path-style and concurrency setting. Record unsupported
   capabilities honestly; a disabled integrity check is not a passing gate.
3. Expose independent Pixi tasks test-s3-java-sdk, test-s3-js-sdk and
   test-s3-go-sdk, plus an explicit test-s3-sdks aggregate. Local runs start an
   isolated real-storage fixture by default and can target an explicitly
   supplied test endpoint using environment credentials. Use unique test-owned
   buckets and clean up only their own resources, including unfinished MPUs.
4. Add .github/workflows/s3-sdk-verification.yml with workflow_dispatch only,
   following .github/workflows/iceberg-rust-sdk.yml. An input selects java, js,
   go or all; selected jobs call the same Pixi tasks used locally. Retain bounded
   execution and redacted failure diagnostics. Do not add push, pull_request,
   schedule or workflow_call triggers, required status checks, normal test-task
   dependencies, container acceptance dependencies or release gates.
5. Document exact invocation, pinned versions, transport/transfer configurations
   and results in the component's client recipe documentation. Routine builds
   and tests must neither install these optional toolchains nor execute the
   SDK suites. Additional Java transport variants or browser JS testing are
   separate future scope unless an actual required workflow demonstrates need.

#### Dependencies

- R200 supplies the general S3 fixture and core client contract. R204 supplies
  user-metadata persistence; metadata scenarios remain visibly incomplete until
  it lands. R205 tracks known storage correctness and progress failures; retain
  failures when reproduced rather than weakening assertions or hiding skips.
- This manual verification is independent of R200 completion and normal release
  acceptance. Implementation follows the current metadata work sequentially.
- Use the current shared listener namespace. No additional permission model,
  filesystem mounting or old-version data migration is included.

#### Acceptance

- Given each pinned SDK and a clean real-storage fixture, run its independent
  task; assert the declared operations, exact data/metadata and expected error
  outcomes through that SDK. E2E test.
- Given a paginated namespace and a multipart-sized payload, execute listing
  and transfer through each SDK; assert complete nonduplicated names and exact
  payloads with the declared default integrity settings. E2E test.
- Given an explicit test endpoint and environment credentials, run a selected
  task and interrupt/fail a transfer; assert bounded termination, cleanup of
  owned test resources and no credential disclosure in diagnostics. E2E test.
- Given workflow_dispatch with each selection and all, execute the selected
  jobs; assert matching local task entry points and failure propagation.
  Integration test.
- Given repository workflow and task definitions, inspect their triggers and
  dependency graph; assert the SDK suites have no automatic or normal-test/
  release invocation and optional toolchains remain isolated. Integration test.

Run pixi run test-s3-java-sdk, pixi run test-s3-js-sdk,
pixi run test-s3-go-sdk and pixi run test-s3-sdks once those tasks are registered;
run pixi run rs-fmt-check and pixi run rs-lint for affected Rust harness changes.
