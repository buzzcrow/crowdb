<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R152: access server / S3 — Basic service scope and compatibility contract

## Problem

CROWDB has chunk storage and Chunk-KV clients but no external object service or
stable S3 compatibility boundary. Implementing handlers before fixing that
boundary would invite accidental support for storage classes, encryption,
versioning, or other features whose semantics are not designed. It would also
couple future Catalog and Dataset protocols to S3 implementation choices.

The architecture is `doc/design/accessserver/design-crowdb-access-server.md`
and `doc/design/accessserver/design-crowdb-access-server-s3.md`; fork
maintenance is defined in `doc/dev/hyper_fork.md`.

## Solution

1. Create `lib/crowdb-access-s3` and the minimal
   `app/crowdb-access-server` process. The S3 library owns S3 request types,
   routing, metadata rules, errors, and metrics; it does not implement a common
   object-store trait for Catalog or Dataset.
2. Add the `third-party/hyper` submodule from `buzzcrow/hyper`, pin one reviewed
   commit, and integrate it through the workspace path and crates.io patch
   rules in `Cargo.toml`. Enable only the fork features required by the HTTP/1
   server. Normal builds must compile the pinned source without network access.
3. Define the first compatibility surface as `CreateBucket`, `HeadBucket`,
   `ListBuckets`, empty-only `DeleteBucket`, `PutObject`, `HeadObject`,
   `GetObject`, one contiguous byte range, `ListObjectsV2`, and `DeleteObject`.
   Reject a non-empty bucket deletion without changing the namespace.
4. Explicitly exclude multipart upload, versioning, lifecycle, replication
   controls, server-side encryption, storage classes, object lock, tagging,
   website hosting, notifications, and S3 Select. Unsupported features return
   the stable error contract supplied by R163 and are never silently ignored.
5. Keep S3, Catalog, Dataset, and optional transfer extensions in independent
   libraries. The access-server loads only compiled and configured libraries;
   disabling S3 registers no listener, task, pool, or route. Basic S3 contains
   no cuObject dependency or RDMA implementation; R170 may consume its stable
   request/generation/completion boundaries through an optional build feature.
6. Establish HTTP/1.1 as the initial protocol. Preserve raw method, URI,
   headers, and streaming body for later SigV4 and checksum requirements. Add
   the authentication hook now, but until R162 lands permit bypass only through
   an explicit unauthenticated trusted-network mode with a startup warning and
   request metric.

## Dependencies

- Uses `crowdb-chunk-client` and `crowdb-chunk-kv-client` as the only shared
  storage boundaries above CROWDB RPC.
- R153–R166 implement the operations and production contracts declared here;
  R167–R169 are explicitly deferred follow-ups.
- The fork repository and submodule process follow `doc/dev/hyper_fork.md`.
- Catalog, Dataset, and R170 are peers and do not block basic S3.

## Acceptance

- Given the S3 feature is built and enabled, when the access-server starts,
  assert it binds the configured HTTP/1.1 listener and dispatches only the ten
  declared operations. Invariant: the initial public surface is
  explicit and finite. Integration test.
- Given any excluded S3 feature or operation, when a request selects it, assert
  the server returns a stable unsupported response and performs no metadata or
  chunk mutation. Invariant: unsupported behavior is never accepted silently.
  Integration test.
- Given the service is configured before R162, when trusted-network bypass is
  disabled or enabled, assert startup respectively rejects missing
  authentication or emits the explicit warning/metric and passes through the
  reserved hook. Invariant: unauthenticated operation is never implicit.
  Integration test.
- Given an empty and a non-empty bucket, when basic bucket operations run,
  assert create/head/list are consistent, empty deletion succeeds, and
  non-empty deletion preserves both bucket and objects. Invariant: bucket
  management cannot orphan live object metadata. E2E test.
- Given S3 is disabled or absent at build time, when the access-server starts,
  assert it creates no S3 listener, background task, or protocol pool.
  Invariant: a disabled protocol has no runtime data-path impact. Integration
  test.
- Given an initialized checkout without network access, when the workspace is
  built, assert Cargo uses the pinned `third-party/hyper` source and resolves
  one Hyper package in the access-server graph. Invariant: build input is
  reproducible and type-compatible. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo tree -d`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
