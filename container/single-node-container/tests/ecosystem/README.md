<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Manual Iceberg ecosystem acceptance

- Development/test preview only. No production storage, upgrade stability,
  ORC selection, cache performance or general S3 bucket claims are established.
- Run the **Iceberg container ecosystem** GitHub Actions workflow manually.
  Its `all` profile is the complete verified matrix; `python` is a focused
  Python/DuckDB diagnostic and cannot certify distributed engines.
- Locally, run `pixi run build-single-node-container`, then
  `pixi run -e iceberg-ecosystem test-iceberg-container-ecosystem`.
  No default task or default CI job depends on this environment or test.
- Every run creates its own container, bind volume, generated credentials and
  public listener checks. Restart preserves that volume and credentials;
  EXIT cleanup removes only the owned resources. Port 9092 must be free for
  the image's advertised Iceberg FileIO URI. User volumes are never reused.
- `CROWDB_ECOSYSTEM_ARTIFACTS` receives redacted diagnostics, image identity
  and operation reports. Credentials and raw runtime/volume files are excluded.
  A failed infrastructure, dependency, authentication or unknown query stage
  fails the job; it is never counted as an unsupported client capability.

## Verified capability matrix

The complete `all` profile passed on 2026-10-05 against the actual release image,
including persisted-volume restart and cleanup. Spark and Flink run in standalone
JVMs with locked OpenJDK 17.0.18-internal, the full runtime classpath and Flink's Java 17
module-access configuration. Trino uses its digest-pinned image runtime.
Only the operations listed below are established; no engine-wide certification
or untested format/version is implied.

- **Python**: PyIceberg 0.11.1, Arrow 25.0.1 (Conda package 25.0.0), pandas
  3.0.6, Polars 1.35.2. Two snapshots with 8192 rows per append. Filtered current
  and historical Arrow batches, eager pandas/Polars, a notebook-style query and
  a scalar downstream consumer retaining only one batch of at most 8192 rows.
  Repeat after persisted-volume restart. Fixture: [Python](python_client.py).
- **DuckDB 1.4.3**: exact-version bundled extension channel; direct REST attach,
  catalog identity and selected table rows through delegated FileIO. Successful
  SELECT additionally proves a dashboard aggregate. Only an observed explicit
  missing remote-signing capability after discovery may be classified as
  unsupported; other errors fail. Fixture: [DuckDB](duckdb_client.py).
- **Spark 3.5.6 / Iceberg 1.11.0**: REST catalog + S3FileIO; read Python rows,
  append, schema and partition evolution, row deletion, rename/drop of a
  separate lifecycle table, read Flink's commit and selected snapshot, and BI
  aggregate before/after restart. Fixture: [Spark](spark/src/main/java/SparkAcceptance.java).
- **Flink 1.20.2 / Iceberg 1.11.0**: bounded batch SQL execution, REST catalog
  + S3FileIO, read Spark's evolved table and delete result, append, read the
  same final rows before/after restart. SQL row deletes and partition evolution
  are not advertised for this Flink fixture. Fixture: [Flink](flink/src/main/java/FlinkAcceptance.java).
- **Trino 483**, Linux amd64 image pinned to digest
  `sha256:fca43d1fdfdcd45f36b791f7117a47f8ce69c58232e5398bfdb8539cd28e778b`:
  direct REST discovery, delegated FileIO read and aggregate before/after
  restart. A first divergence is recorded with its exact stage and error;
  unsupported Trino FileIO is not silently converted into compatibility.
  Fixture: [Trino](trino_client.py).
- **Cross-client handoff**: Python writes, Spark evolves/appends/deletes,
  Flink reads/appends, Python records and checks the selected snapshot,
  Spark and Python verify it after restart; Flink, DuckDB and Trino verify the
  same final visible rows and delete result. Underlying Parquet scans or
  in-memory dataframe copies cannot satisfy these checks.
- **Kafka Connect** remains an optional later probe. No connector or
  streaming-ingest compatibility is claimed by this core matrix.

Client interfaces follow the [PyIceberg API](https://py.iceberg.apache.org/api/),
[DuckDB REST catalog guide](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs),
[Iceberg engine releases](https://iceberg.apache.org/releases/), and
[Trino REST catalog properties](https://trino.io/docs/current/object-storage/metastores.html).
Recipes derive from the passing fixtures, rather than client feature lists.

## Connection recipes

- Python and DuckDB take only the catalog URI and generated Iceberg token.
  DuckDB attaches the REST catalog itself and uses delegated FileIO; it never
  receives the generic S3 bucket credentials or an in-memory dataframe copy.
- Spark uses RESTCatalog and S3FileIO. Rename targets use the source namespace
  (`RENAME TO renamed`); a catalog-qualified target is not a namespace name.
- Flink uses bounded batch SQL and its pinned connector/Hadoop runtime libraries.
  [The JVM launcher](engine.sh) supplies the application classpath to JobMaster
  deserialization. It does not change CROWDB request or serving-lease budgets.
- Trino enables REST OAuth token authentication, vended credentials and native
  S3 FileIO. Set `s3.endpoint` to the CROWDB Iceberg/FileIO origin,
  `s3.path-style-access=true` and `s3.region=us-east-1`. Endpoint configuration
  is required separately from the REST catalog URI. Do not supply the generic
  S3 bucket credentials; table-scoped credentials come from REST delegation.
- The selected format is plaintext Parquet with Iceberg v2 for this matrix.
  Spark verifies schema/partition evolution and position deletes; Flink SQL
  delete/evolution and Trino writes are not advertised by these fixtures.
- The default workflow profile is `all`; the optional `python` profile proves
  only Python and DuckDB. Run both through their separate environment, never as
  a dependency of default tests. No workflow was dispatched by local acceptance.
