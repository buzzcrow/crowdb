<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R194: access-iceberg — Native object listing and S3-style address semantics

#### Status

Implementation in progress. Both exact-object FileIO and native `ListObjectsV2` are required. Preserve the current catalog-shaped bucket and authorize each listing through an explicit table prefix and a table-bound delegated credential.

Observed with PyIceberg 0.11.1 and PyArrow 25.0.0 using
`pixi run -e iceberg-e2e test-iceberg-listing-client`:

- Existing exact-file `exists`, length and open: HEAD, HEAD, HEAD, ranged GET;
  no `ListObjectsV2`.
- Missing exact-file `exists`: HEAD returns 404, then GET bucket with
  `list-type=2` and prefix `<exact-key>/`. This is incidental directory
  fallback, also reached by `create(overwrite=False)` before upload.
- Explicit Arrow `FileSelector` on a table data prefix: GET bucket with
  `list-type=2` and the requested prefix. This is intentional discovery.
- The probe uses an isolated HTTP fixture and tests official client request
  selection, not a passing native/container listing implementation.
- Existing Java 1.11.0 FileIO fixtures exercise exact PUT, multipart, HEAD,
  GET and seek. They do not accept a native prefix-discovery workflow.
- Direct DuckDB REST attachment and Spark/Flink/Trino workflows remain pending
  under the ecosystem requirement. No native-listing conclusion is inferred
  from their unrelated static-file or in-memory query checks.

The [base Iceberg FileIO interface](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/io/FileIO.java)
addresses exact input/output files. Prefix enumeration is a separate optional
[SupportsPrefixOperations interface](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/io/SupportsPrefixOperations.java).
This distinction does not certify an engine's maintenance workflow; its actual
request sequence still needs acceptance.

#### Problem

The native Iceberg file endpoint accepts signed operations on exact immutable objects, but rejects `ListObjectsV2`. [PyIceberg's PyArrow S3 FileIO](https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/) calls PyArrow `get_file_info` for `exists` and length, which can issue `ListObjectsV2` even when the caller has an exact object path. In a local SF 0.01 loader run, that call failed before upload with `InvalidRequest`. A CrowDB-specific FileIO can use exact-object HEAD-backed reads and complete this load, but clients that intentionally list prefixes still lack an answer. [Iceberg's FileIO guide](https://iceberg.apache.org/docs/latest/fileio/) names read, write, and seek as essential file operations and tracks data paths in table metadata; it does not make S3 prefix listing an essential FileIO operation. This requirement must establish which clients actually need S3-style listing before extending the native file endpoint.

The current S3-shaped file location uses an encoded catalog ID in the URI authority field and a `t/<table-id>/` key prefix. This is a routing convention, not a declared Iceberg bucket. The native service has no general S3 bucket authority or file DELETE, and the general S3 service has separate authority. See [native Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md) sections 3 and 6.

#### Solution

Research and record the exact API call sequences of PyIceberg, Arrow, DuckDB, and any engine accepted under R189. Distinguish incidental listing used for exact-object existence from intentional prefix discovery. Check the Iceberg FileIO and REST Catalog contracts separately; compatibility with an S3-shaped URI alone does not make S3 bucket/list semantics part of Iceberg.

Retain the catalog in the bucket field and the explicit `t/<table-id>/` prefix. A table-shaped bucket would unnecessarily change existing locations and credential vending; an opaque scope adds indirection without improving the existing table-bound grant. Require a complete table prefix on listing, reject bucket-wide discovery, and authorize catalog and table before scanning. General S3 records and credentials remain unrelated. No address migration is needed, including for future multiple-catalog deployments.

Implement the native listing contract:

1. Extend `app/crowdb-access-server/src/iceberg/file_request.rs` and the native route selection to parse and validate `ListObjectsV2` parameters, including prefix, delimiter, continuation token, encoding, and page limit. Reject unsupported or ambiguous requests before storage access.
2. Add authorized, bounded prefix scans over selected file-location records in `lib/crowdb-access-iceberg/src/file/repository.rs` and its catalog storage. Include every fully published immutable file in the table scope, even when not yet referenced by a committed snapshot. Exclude draft storage candidates without a selected location, incomplete uploads, losing CAS candidates and logically deleted files. This is object discovery, not current-snapshot enumeration. Return real lengths and ETags; native records lack wall-clock publication time, so expose the documented fixed epoch timestamp instead of inventing one.
3. Extend `lib/crowdb-access-iceberg/src/file/credentials.rs` and credential vending only if a distinct list permission is needed. Bind it to the chosen routing scope and table authorization, with no privilege inherited from the general S3 service.
4. Use bounded forward live cursors with strict lexical progress. An unchanged eligible set has no omissions or duplicates. Publication before the cursor is observed only by a new traversal; deletion before a later page removes that entry; publication ahead may appear. A returned CommonPrefix advances past its entire subtree so it cannot repeat. Bind authenticated tokens to caller, credential nonce, catalog epoch, table, prefix, delimiter and encoding, with an expiry no later than the original credential. Bound each scan to 256 records/4 MiB and XML to 2 MiB; reject malformed, expired, or cross-scope tokens before scanning. Support at most 1,000 requested keys, zero-key pages, slash delimiter and URL encoding; reject unsupported selectors.
5. Add protocol and official-client tests for clients shown by the research to require listing. Keep exact-object FileIO functional without listing and keep the separate general S3 authority unchanged.

#### Dependencies

- R189 client and engine acceptance supplies the observed call sequences and determines which listing cases have user value. Until this requirement is implemented, use an exact-object FileIO for clients that only need Iceberg table files.
- The native file, catalog, and credential contracts in the [Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md) define the present authority boundary. General S3 listing is not a fallback for native Iceberg files.
- Listing is required independently of the ecosystem acceptance. Keep exact-object FileIO functional and verify default PyArrow missing-file probes and explicit prefix discovery against the native endpoint.

#### Acceptance

- Given the chosen supported clients and a current container, trace an exact-file open and an intentional prefix query; record which client issues each request and which Iceberg or S3 interface it relies on. Assert the decision does not infer an Iceberg listing requirement from a PyArrow existence probe alone. Integration test.
- Given each proposed address model, resolve two catalogs and two tables with different principals; assert routing and authorization cannot expose another table's names or files. Select and document one model before implementing the endpoint. Integration test.
- Given an authorized table with committed, staged, abandoned, and retired file records, request a native listing prefix; assert only the chosen visible set appears, while an unauthorized principal sees none. Integration test.
- Given more matching files than one page, request successive pages with prefix and delimiter; assert bounded pages have no duplicate or omitted eligible keys and tokens cannot be replayed under another principal or scope. Integration test.
- Given concurrent file publication or reclamation during pagination, continue listing; assert the documented snapshot or cursor rule, with bounded work and no cross-table leakage. Integration test.
- Given malformed parameters, foreign catalog/table routes, and attempts to use general S3 credentials, issue a native list request; assert fail-closed responses before scanning. E2E test.
- Given exact-object PyIceberg reads and writes, run the existing native FileIO tests after any routing change; assert they still work without `ListObjectsV2`. E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-access-iceberg`, and `pixi run cargo test -p crowdb-access-server` for the implemented scope.
