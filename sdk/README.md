<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowDB SDK

The SDK is an independent Rust workspace inside the CrowDB repository. The
HTTP client uses reqwest, Tokio and rustls. Applications supply their Tokio
runtime; client clones reuse the connection pool and admission budgets.

Implementation is in progress. The current service supports JSON Dataset
creation/open, inline Manifest preparation/publication, operation and Snapshot
inspection, Manifest retrieval, stable selection and retain/release. The new
routes start at `/api/dataset/v1/namespaces/dataset-ns/datasets/{name}`; names
are encoded as one path segment. The existing bincode routes remain available.

The complete public contract is in
[Dataset OpenAPI](../doc/design/access-server/dataset/openapi-crowdb-dataset-v1.yaml).
Not every route is implemented yet. Chunk-backed uploads and reclamation,
streaming batch delivery, independent read sessions, acknowledged progress,
safe retry policy, Python native bindings and installable wheels remain in the
[REST plan](../doc/working/plan-dataset-rest-api.md) and
[SDK plan](../doc/working/plan-dataset-sdk.md). File transfer integration with batch streams and resource admission remains
pending. The existing Python
iterable package is still the earlier adapter, not the native SDK wrapper.

The binary batch decoder consumes data incrementally. Frame digest checks
protect emitted bytes; whole-field integrity is established only at field end,
and batch success requires the final frame and HTTP EOF. Bounded convenience
reads transfer result ownership to callers, who budget any retained results.

`HttpConfig` controls connection/request deadlines, idle pooling, request and
response bounds, maximum active requests and active bytes. Admission uses
atomics and fails explicitly when full. There is no unbounded waiting queue.
JSON is serialized under an admitted byte limit. Dropping a request future
cancels its transport work and returns its capacity. Mutations currently use
one attempt; callers retain the supplied operation/upload token for recovery.
The credential is static at this stage; provider refresh and bounded retry
policy are pending. TLS verification uses rustls defaults.

Run from the repository root:

```sh
pixi run test-dataset-sdk
pixi run sdk-fmt-check
pixi run sdk-lint
```

The SDK has its own lockfile, package version and dependency resolver. It does
not inherit the server's Hyper fork, storage FFI, C++ toolchain or RDMA clients.
Its Python extension will use PyO3/maturin. Wheel/platform support will be
published only after package installation and actual Dataset service acceptance.
No benchmark driver or performance measurement is part of this implementation.

The control example creates/opens a Dataset, prepares and publishes an inline
Manifest, checks latest and reads it back. Against a configured service, set
`CROWDB_DATASET_ENDPOINT`, `CROWDB_DATASET_NAME` and the optional
`CROWDB_DATASET_READ_TOKEN`, then run:

```sh
pixi run -- cargo run --manifest-path sdk/Cargo.toml --example control
```

This is an inline control check. Chunk payload and installed Python acceptance
remain pending.
