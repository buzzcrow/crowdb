<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Native Iceberg Listing Plan

Upstream: [native listing requirement](../backlog/R194-access-iceberg-object-listing.md).

Goal: decide native listing from reproducible client evidence before choosing its address and visibility contract.

## Tasks

- [~] **Client request evidence**: trace PyIceberg exact-file existence/open and Arrow prefix selection using installed clients; inspect accepted Java/Rust FileIO and direct DuckDB boundary. Files: app/crowdb-access-server/tests/common/iceberg_listing_client.py; tools/pixi-tasks/test-iceberg-listing-client.sh; pixi.toml.
- [ ] **Authority decision**: compare catalog, table and opaque routing scopes against existing table-bound credentials and two-catalog/two-table tests; record the selected scope only if listing is justified. Files: lib/crowdb-access-iceberg/src/file/credentials.rs; app/crowdb-access-server/tests/iceberg_file_request_test.rs.
- [ ] **Supported contract**: retain exact-object adapters if no accepted workflow needs discovery; otherwise define published-file visibility and a bounded consistent pagination contract before implementing. Files: doc/backlog/R194-access-iceberg-object-listing.md; doc/design/access-server/iceberge/design-crowdb-iceberg.md.
- [ ] **Acceptance and cleanup**: run focused client and native regression gates, commit verified work, remove requirement/index/plan only when its accepted scope is complete.

## Files and verification

- Client probe: official PyIceberg/PyArrow installed in iceberg-e2e; capture method/path/query names only, never credentials.
- Native integration: file address and credential isolation tests; existing exact-file native suite.
- Gates: pixi run rs-fmt-check; pixi run rs-lint; pixi run cargo test -p crowdb-access-iceberg; pixi run cargo test -p crowdb-access-server.
- Container/engine evidence depends on the declared accepted client profile; pending ecosystem workflows remain unproven.

## Evidence

- Official-client probe passed all four cases with PyIceberg 0.11.1 and PyArrow 25.0.0; both versions are resolved in pixi.lock.
- Existing exact paths use HEAD/GET; missing exact paths trigger incidental listing; explicit FileSelector triggers intentional listing.
- No running container was present during inspection; this probe does not claim container acceptance.
- Copy and batch-delete drafts are preserved in the named Git stash and are outside this active requirement.

## Blocked

- Decision: retain exact-file adapters and close the native listing item, or support intentional table-scoped prefix discovery.
- Evidence: exact existing files need no list operation; missing-file probes and default exact creation fail if directory fallback is rejected; explicit Arrow FileSelector intentionally lists. This does not establish a required Iceberg table workflow.
- Alternatives: exact adapters keep the current address/authority model and avoid pagination state; intentional listing adds a supported discovery surface and requires choosing address scope, published-file visibility, and stable bounded pagination.
- No accepted direct DuckDB/distributed-engine request trace establishes a clear product winner. The scope question was sent to the user; no address or visibility contract is chosen until answered.
- Passing checks: official-client probe (4 cases), shell syntax, CI task registration, Rust formatting, workspace clippy and diff whitespace.
