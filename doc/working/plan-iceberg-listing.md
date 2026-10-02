<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Native Iceberg Listing Plan

Upstream: [native listing requirement](../backlog/R194-access-iceberg-object-listing.md).

Goal: support authorized native ListObjectsV2 while preserving existing exact-file operations and addresses.

## Tasks

- [x] **Client request evidence**: official PyIceberg/PyArrow probes distinguish exact file operations from intentional prefix selection; direct DuckDB and engine profiles remain under ecosystem acceptance. Files: app/crowdb-access-server/tests/common/iceberg_listing_client.py; tools/pixi-tasks/test-iceberg-listing-client.sh; pixi.toml.
- [x] **Authority decision**: retain catalog-shaped bucket and require explicit table prefix; user confirmed listing is required alongside existing exact-file support. Files: doc/backlog/R194-access-iceberg-object-listing.md.
- [x] **Supported contract**: implement bounded selected-file scans, table-bound list capability, authenticated live cursors, strict request parser and XML serialization; test publication/deletion, scope and official native clients. Files: lib/crowdb-access-iceberg/src/file/listing/; app/crowdb-access-server/src/iceberg/file_list_request.rs, file_list_response.rs, file_http/listing.rs; native tests and Pixi task.
- [x] **Permanent contract**: document visibility, live pagination, slash delimiter, URL encoding, fixed epoch timestamp and independent S3 authority. Files: doc/design/access-server/iceberge/design-crowdb-iceberg.md.
- [x] **Acceptance and cleanup**: focused client and native regression gates passed; commit verified work and remove requirement/index/plan.

## Files and verification

- Client probe: official PyIceberg/PyArrow installed in iceberg-e2e; capture method/path/query names only, never credentials.
- Native integration: file address and credential isolation tests; existing exact-file native suite.
- Gates: pixi run rs-fmt-check; pixi run rs-lint; pixi run cargo test -p crowdb-access-iceberg; pixi run cargo test -p crowdb-access-server.
- Container/engine evidence depends on the declared accepted client profile; pending ecosystem workflows remain unproven.

## Evidence

- Official-client probe passed all four cases with PyIceberg 0.11.1 and PyArrow 25.0.0; both versions are resolved in pixi.lock.
- Existing exact paths use HEAD/GET; missing exact paths trigger incidental listing; explicit FileSelector triggers intentional listing.
- Native listing tests passed, including official PyArrow create/read/discovery and signed pagination/isolation requests.
- Full Iceberg library and access-server suites, six listing cases, Rust formatting and clippy passed.
- Single-node container E2E passed, including writes, directory discovery, service recovery and persisted-volume reads.
- Native exact-file regression passed: 10 tests, including multipart restart/replay, ranges, slow sockets and concurrent uploads; 12 opt-in stress/crash/Java fixtures remain ignored in that invocation. The separate official native listing task passed both tests.
- Copy and batch-delete drafts are preserved in the named Git stash and are outside this active requirement.
