<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Scope and Compatibility Plan

Upstream: [R152](../backlog/R152-s3-scope-compatibility.md),
[Access Server S3 design](../design/accessserver/design-crowdb-access-server-s3.md),
and [Hyper fork guide](../dev/hyper_fork.md).

Goal: establish the independently configurable HTTP/1 S3 service boundary,
with a reviewed pinned Hyper fork and no implicit authentication or unsupported
operation behavior.

Persistent-plan exception: the basic S3 requirements share one metadata
record, HTTP listener, and E2E harness. This plan remains the execution index
through that series and is removed with the completed backlog set.

## Review

- [x] **Requirement and repository review**: confirmed that the workspace has
  neither `lib/crowdb-access-s3`, `app/crowdb-access-server`, nor the required
  `third-party/hyper` submodule. Files: `Cargo.toml`, `.gitmodules`,
  `doc/backlog/R152-s3-scope-compatibility.md`, `doc/dev/hyper_fork.md`.
- [x] **Compatibility ownership review**: confirmed that R163, not an existing
  module, owns the exact unsupported S3 error serialization. Files:
  `doc/backlog/R152-s3-scope-compatibility.md`,
  `doc/backlog/R163-s3-error-http-compatibility.md`.
- [x] **R153 bucket deletion review**: selected name-mapping tombstones and
  never-reused bucket IDs so deletion may be slow without adding a permit or
  final-publication synchronization step to PUT. Files:
  `doc/backlog/R153-s3-object-metadata-schema.md`,
  `doc/design/accessserver/design-crowdb-access-server-s3.md`.

## Requirement closure review

- [x] **R152 service boundary**: the finite HTTP surface, feature isolation,
  authentication ordering, concrete dispatcher, pinned Hyper source,
  configured master key, native body allocator, UUID buckets, range-delete
  boundary, and self-hosted E2E contract are complete.
- [x] **R153–R154 metadata and publication**: direct object records, bucket
  generations, and one final unconditional publication are complete; their
  backlog details have been removed.
- [~] **R155 streaming PUT**: bounded pull-to-writer flow, checksums,
  pipeline-local backpressure, direct publication, and the fork-native receive
  allocator are complete. Bounded RPC view chains and their copy accounting
  remain follow-up work.
- [x] **R156–R158 read and listing operations**: metadata-only HEAD,
  pull-based full/range GET, conditions, and signed stateless listing are
  connected to production storage clients.
- [x] **R159 deletion**: direct idempotent logical deletion and the
  non-mutating chunkdb range-delete RPC placeholder are complete; full
  reclamation remains R168/R95 work.
- [x] **R160–R161 routing and admission**: object routing includes tenant,
  immutable bucket ID, and key; small-object selection uses pipeline-local
  byte/object budgets and applies backpressure before another body poll.
- [x] **R162 authentication**: header and presigned SigV4, configured-master-key
  encryption, group-0 issuance, initial scan, periodic refresh, and the
  lock-free fail-closed credential cache are complete. Stable-user and token
  lifecycle management are explicitly deferred.
- [x] **R163–R164 compatibility and integrity core**: stable bounded errors,
  bodyless HEAD failures, correlation headers, conditional/range mapping,
  single-part ETag, MD5, signed payload SHA-256, and terminal full-read
  verification are implemented. Ambiguous mutations receive no safe-retry
  advice or compensating write.
- [~] **R165 observability**: fixed-cardinality request outcome/latency/byte,
  concurrency, bypass, backpressure, retained-byte, and dependency readiness
  primitives exist. Native copy/view counters and dependency status serving
  remain coupled to the corresponding R152 open implementations.
- [x] **R166 basic E2E**: the required boto3 path owns KV, diskdb, diskio,
  chunkdb, Chunk-KV, access-server, readiness, credentials, and teardown. Its
  minimum topology uses explicit `unsafe_colocated` placement with 2+1 EC on
  one disk group, disk, and zone. Fault injection and performance matrices
  remain the larger R166 follow-up scope.

## Fork and workspace

- [x] **Pin reviewed Hyper fork**: pinned `feature-crowdb` provenance commit
  `c6dca2078ce223050dc0832be7c9ab07baa6c4bf` as the
  `third-party/hyper` gitlink and added workspace path/patch integration.
  Files: `.gitmodules`, `third-party/hyper`, `Cargo.toml`, `Cargo.lock`.
- [x] **Create standard error baseline**: add the S3 library with the frozen
  `NotImplemented` HTTP 501 XML response, including safe XML escaping and
  request/host correlation fields. Files: `lib/crowdb-access-s3/Cargo.toml`,
  `lib/crowdb-access-s3/src/lib.rs`, `lib/crowdb-access-s3/src/error.rs`,
  `lib/crowdb-access-s3/tests/error_test.rs`.
- [x] **Create S3 library**: add `crowdb-access-s3` with its own configuration,
  raw-request authentication hook, route classifier, unsupported-operation
  outcome, metrics interface, and storage-boundary traits. Files:
  `lib/crowdb-access-s3/Cargo.toml`, `lib/crowdb-access-s3/src/lib.rs`,
  `lib/crowdb-access-s3/src/*.rs`.
- [x] **Create access-server process**: add a Tokio process which constructs
  only enabled protocol libraries, validates trusted-network authentication,
  binds the S3 HTTP/1 listener, and shuts it down independently. Files:
  `app/crowdb-access-server/Cargo.toml`,
  `app/crowdb-access-server/src/lib.rs`,
  `app/crowdb-access-server/src/main.rs`.

## Compatibility and lifecycle

- [x] **Classify finite surface**: map exactly the ten declared bucket/object
  operations from raw method, URI, query, and headers before metadata/body
  work; route all excluded operations to the approved stable response with no
  storage mutation. Files: `lib/crowdb-access-s3/src/route.rs`,
  `lib/crowdb-access-s3/src/error.rs`.
- [x] **Wire authenticated HTTP dispatcher**: pass raw method, URI, headers, and
  body mode to the hook before dispatch; fail startup unless trusted-network
  bypass is explicit, and emit the required warning and metric when it is;
  split listener, dispatcher, storage operations, and wire serialization so
  compatibility behavior has one owner.
  Files: `lib/crowdb-access-s3/src/auth.rs`,
  `app/crowdb-access-server/src/s3.rs`,
  `app/crowdb-access-server/src/s3/dispatcher.rs`,
  `app/crowdb-access-server/src/s3/operations.rs`,
  `lib/crowdb-access-s3/src/wire.rs`.
- [x] **Keep disabled service inert**: ensure a disabled or omitted S3 feature
  creates no listener, task, pool, route, or S3 dependency in another protocol
  path. Files: `app/crowdb-access-server/src/lib.rs`,
  `app/crowdb-access-server/tests/service_test.rs`.

## Object read, list, and delete

- [x] **Prepare metadata-only HEAD**: derive conditions and headers from one
  direct object record without chunk access. Files:
  `lib/crowdb-access-s3/src/condition.rs`,
  `lib/crowdb-access-s3/src/retrieval.rs`.
- [x] **Prepare bounded range GET**: retain one record and create a pull-based
  chunk range stream with no eager payload read. Files:
  `lib/crowdb-access-s3/src/retrieval.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`.
- [x] **Implement stateless listing core**: scan the encoded key-prefix range
  and sign a request-bound continuation cursor. Files:
  `lib/crowdb-access-s3/src/object.rs`,
  `lib/crowdb-access-s3/src/continuation.rs`.
- [x] **Keep logical delete direct**: use one unconditional object-key delete;
  physical cleanup remains asynchronous. Files:
  `lib/crowdb-access-s3/src/object.rs`.
- [x] **Wire bucket HTTP operations**: connect create, head, list, and
  empty-only delete to the concrete Chunk-KV metadata store and serialize
  boto3-compatible XML. Files:
  `app/crowdb-access-server/src/s3/operations.rs`,
  `lib/crowdb-access-s3/src/wire.rs`.
- [x] **Wire object HTTP operations**: connect PUT publication, metadata-only
  HEAD, pull-based full/range GET, stateless listing, and logical DELETE to the
  concrete metadata/chunk clients. Select a shared writer only for a trusted
  declared length within the configured small-object limit; otherwise use a
  prepared large writer. Files: `app/crowdb-access-server/src/s3/operations.rs`.
- [x] **Stream HTTP responses**: adapt `ChunkReadStream::next_chunk` to Hyper
  frames without collecting the object; propagate a late storage/integrity
  failure as a body error. Files: `app/crowdb-access-server/src/s3/operations.rs`.

## Authentication, errors, and operations

- [x] **Implement SigV4 verification core**: canonicalize raw header-signed and
  presigned requests, validate timestamp/expiry/scope/signed headers and
  supported payload modes, derive HMAC keys, and compare in constant time
  through an injected credential provider. Files:
  `lib/crowdb-access-s3/src/auth/sigv4.rs`.
- [x] **Persist credential authority**: the configured master key, encrypted
  group-0 record, one-time user-token issuance, initial scan, periodic rescan,
  lock-free immutable cache, maximum-staleness fail-close, and secret
  zeroization boundary are complete. Files:
  `lib/crowdb-access-s3/src/auth/snapshot.rs`, group-0 management, and
  access-server configuration.
- [x] **Require portable continuation authority**: accept no implicit/random
  token key at startup; require deployment configuration until the group-0
  rotation design is selected. Files: `app/crowdb-access-server/src/main.rs`,
  `doc/backlog/R152-s3-scope-compatibility.md`.
- [x] **Bound public errors and telemetry**: provide stable S3 status/XML,
  bounded client-derived fields, fixed-cardinality atomic metrics, and
  dependency readiness. Files: `lib/crowdb-access-s3/src/error.rs`,
  `lib/crowdb-access-s3/src/metrics.rs`.
- [x] **Validate basic integrity**: incrementally compute single-part MD5 ETag
  and validate optional `Content-MD5` plus supported `SigV4` payload SHA-256
  before publication. Files:
  `lib/crowdb-access-s3/src/integrity.rs`,
  `lib/crowdb-access-s3/src/streaming.rs`.

## Tests and gates

- [x] **Unit tests**: cover route classification, unsupported-operation
  non-mutation, raw authentication-hook ordering, and invalid authentication
  configuration. Files: `lib/crowdb-access-s3/tests/*_test.rs`.
- [x] **Integration tests**: cover enabled listener dispatch, authentication
  before operation execution, real-backend request mapping, trusted-network
  rejection/bypass warning metric, and disabled-service inertness. Files:
  `app/crowdb-access-server/tests/*_test.rs`.
- [x] **Basic E2E tests**: cover create/head/list/delete-empty bucket behavior,
  non-empty delete preservation, PUT/HEAD/GET/range/LIST/DELETE, and exact
  unsupported/error behavior against a self-hosted local stack. Files:
  `app/crowdb-access-server/tests/*_test.rs`.
- [x] **SDK compatibility client**: add the official Python `boto3` client to
  an isolated Pixi `s3-e2e` environment and exercise a path-style configured
  local endpoint. Keep it out of all Rust production dependencies. Files:
  `pixi.toml`, `app/crowdb-access-server/tests/s3_e2e/`.
- [x] **Required gates**: run `pixi run -- cargo test -p crowdb-access-s3
  --all-targets`, `pixi run -- cargo test -p crowdb-access-server
  --all-targets`, `pixi run -- cargo tree -d`, `pixi run rs-fmt-check`, and
  `pixi run rs-lint`. Files: workspace.

## Selected follow-up implementation

- [x] Load and validate the access-server master key from configuration; use
  it for encrypted group-0 user credentials and continuation signing records.
- [x] Add user-token issuance, initial scan, periodic refresh, and authenticated
  readiness. Stable-user and token lifecycle management are deferred.
- [x] Keep bucket mappings in their dedicated Chunk-KV prefix and use a fresh
  UUID for every bucket generation; add list/recreate compatibility coverage.
- [~] Add the CROWDB body allocator contract, native owner wrapper, Hyper
  HTTP/1 socket read integration, immutable frame handoff, and allocator-credit
  backpressure. Bounded RPC view chains and copy accounting remain.
- [x] Add `DeleteChunkRange(chunk_id, offset, size)` protocol/client/server
  plumbing whose pre-R95 handler returns not-implemented without mutation.
- [x] Replace the optional boto3 endpoint-only runner with a harness that owns
  KV groups, diskdb, diskio, chunkdb, Chunk-KV, access-server, readiness, and
  teardown. Retain external endpoint mode as an explicit override.

## Files

- `.gitmodules`
- `third-party/hyper`
- `Cargo.toml`
- `Cargo.lock`
- `lib/crowdb-access-s3/Cargo.toml`
- `lib/crowdb-access-s3/src/`
- `lib/crowdb-access-s3/tests/`
- `app/crowdb-access-server/Cargo.toml`
- `app/crowdb-access-server/src/`
- `app/crowdb-access-server/tests/`
