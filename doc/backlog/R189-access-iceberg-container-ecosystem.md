<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R189: access server / Iceberg — Container client and engine workflows

## Status

Deferred until R187 provides a publish-ready single-node image. This is a
separate client-ecosystem project, not a gate for publishing the non-production
Docker preview and is independent of the completed REST/official-SDK acceptance.

## Problem

The completed REST conformance work proves the declared REST protocol with official SDKs and a supported
subset of the Apache compatibility kit. R187 proves a packaged container with
PyIceberg and S3 client fixtures. Neither proves that a developer can connect a
notebook, dataframe library, SQL engine, or distributed compute engine to the
same image and obtain correct table rows across commits and restarts. A catalog
endpoint alone is insufficient: clients may require different credential
delegation, file-location, format-version, and delete-file behavior. Advertising
untested client compatibility would mislead evaluators of the preview.

The [access architecture](../design/access-server/design-crowdb-access-server.md)
keeps Iceberg table authority separate from general S3 buckets. This project
tests clients against that boundary rather than assuming an S3-shaped file URI
is a normal S3 object.

## Solution

1. Build an isolated, reproducible client harness around the R187 image and
   `container/single-node-container/tests/`. Pin each client and dependency in
   Pixi or a locked external image, record its exact version and operation
   profile, publish only S3/Iceberg/Web ports, and use generated scoped
   credentials. Tests must not modify the user's persistent volume or accept
   arbitrary external object locations as native Iceberg files.
2. Prove the Python notebook/dataframe path first: PyIceberg discovers the
   REST catalog, writes a small Arrow/Parquet-backed table through its supported
   FileIO, and reads selected rows and a prior snapshot into Arrow batches and
   pandas. Test Polars through PyIceberg's conversion separately. Include a
   notebook-style analysis and a batch-oriented ML/data-processing consumer;
   distinguish eagerly materialized dataframes from a bounded Arrow batch
   reader. Keep version and feature claims limited to pinned passing fixtures.
3. Test a local SQL path with DuckDB's Iceberg REST catalog integration, not
   only a static metadata-file scan or a PyIceberg-to-DuckDB in-memory copy.
   Verify catalog discovery, delegated file access, SELECT and a supported
   write if the pinned client and current CROWDB profile permit it. If its
   required FileIO or authentication contract is unsupported, record the exact
   first divergence and a separate implementation dependency; do not expose a
   misleading success recipe.
4. In the separately provisioned engine project, pin Spark, Flink and Trino
   profiles against the same container and run the capabilities each client
   actually supports: namespace/table lifecycle, append/read, schema or
   partition evolution, snapshots/time travel, row-level deletes, and restart.
   Compare one engine's committed rows with another engine and PyIceberg; do
   not claim an engine or format version compatible from catalog-only tests.
   Failures must identify REST, FileIO, format, credential or client behavior
   without weakening the server's authority and durability contracts.
   Exercise a cross-tool handoff where one client writes, another reads, and a
   third verifies the same selected snapshot after a container restart.
5. Evaluate streaming/ingest as a later scenario, starting with the official
   Iceberg Kafka Connect sink only after its pinned connector can use the
   supported REST and FileIO profile. Record its setup and first divergence
   separately; Kafka infrastructure is not required for the Python/SQL/engine
   acceptance above. Exercise a simple BI query through a tested SQL engine;
   do not claim direct BI-tool or catalog compatibility without its own fixture.
6. Add only passing, reproducible recipes to
   `doc/user-manual/docker-single-node-user-guide.md`. Maintain a client
   capability matrix with tested versions, read/write scope, known exclusions,
   and links to executable fixtures. Label the image and all examples as
   development/test, not production data storage or upgrade-stable service.
   A direct Parquet file read or generic S3 object operation does not establish
   Iceberg catalog, snapshot, or table-row compatibility.

## Dependencies

- R187 supplies the image, volume/port contract, monitor, credentials, and
  container test baseline. R184 supplies REST/SDK conformance and the declared
  capability profile. This requirement does not block either one's completion.
- R177's deferred cross-engine acceptance moves here. R186 ORC, R185 caching,
  production durability, and general S3 bucket semantics are not prerequisites;
  unsupported operations remain explicitly excluded from published recipes.
- The official [PyIceberg API](https://py.iceberg.apache.org/api/) documents
  Arrow batches and pandas conversion; the official
  [DuckDB catalog guide](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs)
  documents direct REST attachment. These describe client capabilities, not
  proven CROWDB compatibility. Pin versions before implementation.

## Acceptance

- Given a clean R187 image and isolated volume, when the client harness starts
  and exits, assert fixed versions, generated scoped credentials, bounded test
  data, no internal port publication, complete diagnostic artifacts on failure,
  and no change to another volume. Invariant: reproducible isolation. E2E test.
- Given a PyIceberg-written Parquet table with multiple snapshots, when Arrow
  batches, pandas and Polars read a filtered current and historical view before
  and after container restart, assert identical rows, types and snapshot
  selection; the batch path does not materialize the whole result at once.
  Invariant: Python dataframe correctness. E2E test.
- Given a notebook-style query and a batch-oriented downstream consumer, when
  each uses the PyIceberg catalog and Arrow batch reader, assert selected rows
  and types agree with the table snapshot and memory use is bounded by the
  chosen batch rather than the full table. Invariant: analysis and ML consume
  catalog-selected data. E2E test.
- Given a pinned DuckDB Iceberg REST profile, when it attaches CROWDB and
  queries a PyIceberg-created table, assert rows and catalog identity match; if
  direct attachment cannot satisfy the declared CROWDB FileIO contract, assert
  the failure is classified and no direct-DuckDB recipe is published.
  Invariant: direct SQL claims require proof. E2E test.
- Given pinned Spark, Flink and Trino profiles with only supported operations,
  when each writes or reads and another client verifies after restart, assert
  committed rows, schema, snapshots and supported deletes agree, with every
  unsupported operation recorded rather than counted as a pass. Invariant:
  cross-engine interoperability. E2E test.
- Given a table written by one client and changed by another, when a third
  client reads before and after container restart, assert the selected snapshot
  and visible rows agree across clients; reading the underlying Parquet file
  alone is not counted as a catalog pass. Invariant: cross-tool handoff follows
  Iceberg authority. E2E test.
- Given a candidate Iceberg Kafka Connect sink and an isolated event stream,
  when its REST/FileIO handshake and one append are attempted, assert either a
  verified end-to-end row result or a documented first unsupported boundary;
  neither outcome blocks the core client matrix. Invariant: honest ingest
  compatibility. E2E test.
- Given a verified SQL engine and a small dashboard-style aggregate query,
  when the query runs against a catalog table, assert its result matches the
  selected snapshot; do not claim an untested BI tool connects directly to
  CROWDB. Invariant: BI recipes use a proven SQL path. E2E test.
- Given the client recipes and matrix, when a user follows each published
  example against the pinned image, assert every advertised operation passes
  and the non-production, no-upgrade and format limits remain visible.
  Invariant: documentation follows evidence. E2E test.

Required gates:

- `pixi run test-single-node-container`
- `pixi run test-iceberg-container-ecosystem` (new task to add with the harness)
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
