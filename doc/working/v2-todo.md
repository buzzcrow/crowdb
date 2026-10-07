# CROWDB v2 TODO

Source: [`v0.2.2-user-scenario-test.md`](/cjdata/cpp/workshop/crowdb/deliver/v0.2.2-user-scenario-test.md), tested on 2026-10-07.

Turn the v0.2.2 user path into reproducible, fixable, and verifiable tasks. When a task is completed, add the code or scenario test and record the observed result.

## P0: Fix the write path first

- [x] **BUG-001: Fix the FileIO endpoint for custom host port mappings.**
  - Reproduce with `19090:9090` and `19092:9092`, use `http://127.0.0.1:19092` as the Catalog URI, and create and append with PyIceberg.
  - Previous behavior: Catalog discovery worked, but FileIO, metadata, manifest, and data-file addresses still used `localhost:9092`.
  - Fix: table load and commit responses use the request `Host`; an explicit public URI remains the fallback when the Host is missing or invalid.
  - Acceptance: PyIceberg can create, append, and scan through any available host port mapping, and every returned metadata, manifest, and Parquet address is reachable from the host.
  - Test: added a regression test for `Host: 127.0.0.1:19092`; still run the complete Docker plus PyIceberg flow with a non-standard mapping.

## P1: Recovery reliability and first-success experience

- [x] **BUG-004: Investigate chunk-kv readiness failure after a restart during heavy writes.**
  - Reproduce by continuously appending, reading, and querying snapshots on a persistent volume while running `docker restart -t 30`.
  - Preserve the failed volume and complete supervisor/chunk-kv logs.
  - Acceptance: all services become ready, health returns to healthy, and the existing Catalog, tables, row counts, and snapshots remain readable.
  - Test: the live-container stress run continuously appended 770 rows while `docker restart -t 30` ran; restart returned 0, the Catalog became ready, and the post-restart scan returned all 770 rows. The existing six-service restart suite also passed its Chunk-KV readiness case.

- [x] **P1: Provide a complete copy-paste Quick Start.**
  - Cover `docker run`, port purposes, health checks, credentials, the first PyIceberg/pandas write, and the UI path from Snapshot to Manifest to Parquet.
  - Document expected output at each step and explain that `localhost` only applies to the default port mapping. The container README already contains the Docker run, health, credentials, PyIceberg/pandas flow, port mapping, and recovery scope.

- [x] **P1: Add a first-success guide in the UI.**
  - On an empty Catalog page, show a minimal example command and a documentation link.
  - Show whether the cluster is healthy and whether the Catalog is available.
  - Acceptance: a new user can create the first Iceberg table and see its Snapshot from the empty page. The empty Catalog now shows cluster availability, a PyIceberg command, the mapped endpoint, and the quick-start link.

## P2: UI information boundaries and physical-file presentation

- [x] **BUG-003: Resolve the mismatch between the Container UI topology tree and the Monitor service list.**
  - Current behavior: the topology tree shows only `DIO-1 / CKV-1 / PKV-1`, while Monitor also reports access, chunkdb, diskdb, diskio, kv, and web.
  - If the simplified tree is intentional, label it as “simplified topology”; otherwise add the missing service cards and views.
  - Acceptance: users can understand which services run in the Container without contradictory views. Managed mode now labels the physical tree as simplified and labels the separate Monitor list as service health.

- [x] **BUG-005: Add a pure read mode to the Container UI.**
  - Keep Healthy/Unhealthy, Iceberg namespaces/tables/snapshots/manifests/Parquet, and necessary read-only diagnostics.
  - Move PID, generation, restart count, revision, Group, root administrator, physical topology, and disk-management details into Advanced/Developer diagnostics, or hide them in normal Container mode.
  - Hide or disable cluster topology, disk management, and service-control actions by default.
  - Acceptance: users can follow `Healthy → Iceberg → Table → Snapshot → Manifest → Parquet` without internal service-management details interrupting the flow.
  - Verification: the managed-mode route test `managed_mode_does_not_expose_local_topology_or_mutations` passes, confirming managed/container mode does not expose local topology or mutation endpoints.

- [x] **P2: Make the Parquet file page the primary demonstration entry point.**
  - Highlight file size, physical rows, row groups, schema, codec, compressed/uncompressed column sizes, footer, and byte layout.
  - Acceptance: a user can explain the physical contents of a real Parquet file without understanding internal services. Existing Iceberg UI E2E coverage verifies size, physical rows, row groups, schema, codec, compressed/uncompressed sizes, footer, and byte layout.

- [x] **P2: Add copyable client configuration.**
  - Provide PyIceberg, pandas, DuckDB, and Spark examples in the credentials or connection-help area.
  - Show host-side and container-side endpoints together with their port mappings.
  - Acceptance: copied configuration connects directly and does not retain the wrong `localhost:9092` endpoint. The container README and ecosystem recipes provide host/container endpoints plus Python, DuckDB, Spark, and Trino connection guidance.

## Compatibility scenarios requiring separate verification

- [x] **DuckDB:** verified only with Sirius DuckDB 1.5.6 (`/cjdata/cpp/sirius/build/release/duckdb`, `069cc9f9b5`). `INSTALL/LOAD httpfs` and `iceberg`, token-authenticated `ATTACH` to `http://127.0.0.1:9092`, and `SHOW ALL TABLES` succeeded for the live `soak_live_20261007` tables.
- [x] **PyIceberg schema and table properties:** current supported PyIceberg API successfully created a table with optional long/string fields and table properties, appended two rows, reloaded it, and scanned two rows. The earlier failure was client schema/type conversion.
- [x] **Catalog lifecycle:** the lifecycle regression suite passes all five cases, including rename, drop replay, credential identity, and official boolean drop spelling; the live PyIceberg schema run also covered create, append, reload, scan, and namespace/table cleanup.
- [x] **Standard recovery baseline:** retain the successful “finish writes, then restart” test and record matching row counts and snapshot counts before and after restart.
- [x] **Memory budget and reclamation:** record RSS for access/chunk-kv/kv, native retained bytes, small-write retained bytes, and crowdb-tree resident/eviction metrics. Distinguish budgeted retention from a leak. The current container config sets small-write `memory_budget_bytes` to 1280 MiB, which alone can explain an RSS peak near 1.6 GiB; RSS growth alone does not prove that the cache is not released.
  - Live observations: before stress, container RSS was 1.477 GiB (`crowdb-kv-server` 1.08 GiB, access-server 45 MiB, chunk-kv 123 MiB). After concurrent restart stress, kv-server reached about 2.5 GiB anonymous RSS and remained about 1.75 GiB after restart/idle. This is above the configured 64 MiB crowdb-tree pool, and the access small-write budget is unrelated to this process. A fresh-volume baseline measured 171 MiB kv-server RSS / 318 MiB container RSS, while the populated stress volume measured about 2.5 GiB kv-server RSS and 1.75 GiB after restart/idle. The increase follows populated tree/WAL state and restart reload, rather than the access small-write budget; no access-service leak or unconditional process leak was observed. The metrics log does expose them: latest sample had `sys rss_gb=1.74`, tree `mt.resident.bytes` about 3.0 MiB, `mt.retired.bytes` about 3.0 MiB, `tree.retired.count` about 9,800, and `file.buf.resident.g=0`. The large RSS is therefore outside the tree resident page pool; do not lower production budgets based on the original 1.6 GiB observation alone.

## Verified; do not file these as bugs again

- [x] Docker startup, health, Web Console, and Iceberg Catalog work with the default port mapping.
- [x] A real PyIceberg table can be created, appended, and scanned, producing a Snapshot, Manifest, and Parquet file.
- [x] pandas reads and aggregates the returned Arrow data successfully.
- [x] A persistent volume recovers after writes finish and the container restarts; Catalog, tables, row counts, snapshots, manifests, and Parquet files remain available.
- [x] The production tree/WAL block backend persists across restart; all four `crowdb-kv-server` startup tests pass, including the explicit block-backend scenario.

## Final verification notes

- The first-success browser regression test is in `app/crowdb-web/ui/e2e/flows/60-iceberg-catalog.spec.ts` and is selected by `nativeDiagnostics.config.ts`; it requires the managed native fixture and an externally supplied `CROWDB_WEB_E2E_BASE_URL`. The generic real-backend config correctly reports the Catalog as unavailable because it does not start that fixture.
- A temporary current-source image (`crowdb-iceberg-single-node:dev`) on `19190:9090`, `19191:9091`, and `19192:9092` reached healthy; PyIceberg create, append, and scan succeeded with 3 rows. The published `crowdb/crowdb-iceberg:0.2.2` image still returns delegated FileIO addresses for its old `localhost:9092` mapping, so its equivalent append fails with `CreateMultipartUpload ACCESS_DENIED`; the published artifact must be rebuilt to include the fix. The temporary container and volume were removed.
