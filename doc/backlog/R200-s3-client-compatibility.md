<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R200: access-s3 — Real-client compatibility and default checksum coverage

#### Status

Partially implemented. Default SDK and configured AWS CLI acceptance proceed
independently. Positive rclone compatibility remains pending
[R204](R204-s3-client-metadata.md): the client requires persisted user metadata.
No compatibility claim substitutes for a failing positive gate. Default concurrent CLI
progress is tracked by [R205](R205-s3-concurrent-client-progress.md).
The accumulated full-stack regression also remains unaccepted because of the
reproduced journal cursor regression and snapshot corruption recorded there;
the isolated thousand-key case and new SDK cases pass independently.

#### Problem

Existing boto3 real-storage tests cover core operations, multipart, restart,
response loss, and streaming boundaries. The container boto3 smoke client
explicitly sets checksum calculation/validation to `when_required`. Neither
that recipe nor passing boto3 API tests establishes that AWS CLI or rclone
default workflows work. These clients may select additional APIs,
addressing modes, checksum/trailer framing, or listing behavior.

The [S3 design](../design/access-server/s3/design-crowdb-access-s3.md)
advertises a finite surface. Publish tested workflow compatibility instead of
inferring broad S3 compatibility from one SDK.

#### Solution

1. Extend `app/crowdb-access-server/tests/s3_e2e/basic.py` and the full-stack
   harness with a separate default-boto3 configuration. Retain existing focused
   tests. Cover ordinary and multipart transfers, integrity headers/trailers,
   presigned GET/PUT, bad checksums, expiry, and slow/fragmented bodies. Preserve
   the existing SigV4 and bounded upload ownership invariants.
2. Add reproducible AWS CLI and rclone scenarios against the general S3 endpoint:
   bucket discovery, upload/download, prefix listing, multipart-sized transfers,
   copy, sync, and recursive deletion. Trace request sequences and classify
   required API gaps. In `lib/crowdb-access-s3/src/route.rs`, `auth/sigv4.rs`,
   `integrity.rs`, and the server S3 dispatcher/upload layer, implement only
   extensions needed by the declared recipes; create separate requirements for
   substantial new semantics instead of bypassing authentication or checksums.
3. Wire client environments/tasks in `pixi.toml` and retained scripts under
   `tools/pixi-tasks/`, pin client versions, and retain diagnostics without
   credential-bearing headers or presigned query strings. All commands run
   through Pixi. Extend container acceptance with accepted AWS CLI/rclone
   workflows, alongside boto3, before the credentialed publish job.
4. Document each tested version, exact workflow, endpoint/addressing setup,
   required configuration, and known unsupported operations in
   `container/single-node-container/README.md`. Distinguish out-of-box defaults
   from explicitly configured path-style/compatibility recipes. Unsupported
   extensions return truthful S3 errors without mutation.

#### Dependencies

- Server-side copy and batch deletion are available where traces
  demonstrate those APIs are required. Research and existing-core tests can
  proceed first; copy/delete recipes cannot be declared accepted until their
  required capabilities land.
- Existing `test-boto3-e2e`, container S3 client, and container release verification
  remain regression baselines. R196 owns performance benchmarks, not this gate.
- R204 owns mandatory client metadata and multipart metadata persistence. The
  retained positive rclone gate resumes after it lands.
- Current recipes use the configured shared listener namespace.
- R205 retains default concurrent multipart progress diagnostics. A documented
  single-concurrency development recipe does not certify default concurrency.

#### Acceptance

- Given the pinned default boto3 client without `when_required` overrides,
  upload/download ordinary and multipart data; assert exact bytes and the
  documented default checksum modes. Default-client compatibility. E2E test.
- Given presigned GET/PUT URLs, valid and expired/tampered signatures, and bad
  checksums/trailers, execute transfers; assert successful authorized requests
  and rejection without partial publication. Integrity and authentication. E2E test.
- Given pinned AWS CLI and rclone clients, execute the declared discovery,
  transfer, prefix, copy, sync, and cleanup recipes; assert exact resulting
  namespaces and bytes and retain redacted API-gap evidence. Workflow fidelity.
  E2E test.
- Given interrupted transfers and persisted-container restart, rerun accepted
  clients; assert no partial objects and successful retrieval of committed
  data. Recovery safety. E2E test.
- Given a client-selected unsupported extension and failing checksum, run the
  recipe; assert a truthful error, no mutation, and no credentials in retained
  diagnostics. Fail closed and credential containment. E2E test.
- Given the publication workflow, run container acceptance; assert accepted
  boto3/AWS CLI/rclone recipes run before publication credentials are available,
  and any required recipe failure prevents publishing. Release gating. E2E test.

Run `pixi run test-access-s3`, `pixi run test-access-server`,
`pixi run -e s3-e2e test-boto3-e2e`, `pixi run test-single-node-container`,
`pixi run rs-fmt-check`, and `pixi run rs-lint`. The implementation must register
and document exact Pixi task commands for AWS CLI and rclone
before accepting their workflows.
