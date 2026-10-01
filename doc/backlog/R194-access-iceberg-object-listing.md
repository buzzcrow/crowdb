<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R194: access-iceberg — Native object listing and S3-style address semantics

#### Status

Deferred until the client-use and address-model research below is complete. Exact-object FileIO already supports the small TPC-H and TPC-DS loader flow; listing is not a prerequisite for that flow.

#### Problem

The native Iceberg file endpoint accepts signed operations on exact immutable objects, but rejects `ListObjectsV2`. [PyIceberg's PyArrow S3 FileIO](https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/) calls PyArrow `get_file_info` for `exists` and length, which can issue `ListObjectsV2` even when the caller has an exact object path. In a local SF 0.01 loader run, that call failed before upload with `InvalidRequest`. A CrowDB-specific FileIO can use exact-object HEAD-backed reads and complete this load, but clients that intentionally list prefixes still lack an answer. [Iceberg's FileIO guide](https://iceberg.apache.org/docs/latest/fileio/) names read, write, and seek as essential file operations and tracks data paths in table metadata; it does not make S3 prefix listing an essential FileIO operation. This requirement must establish which clients actually need S3-style listing before extending the native file endpoint.

The current S3-shaped file location uses an encoded catalog ID in the URI authority field and a `t/<table-id>/` key prefix. This is a routing convention, not a declared Iceberg bucket. The native service has no general S3 bucket authority or file DELETE, and the general S3 service has separate authority. See [native Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md) sections 3 and 6.

#### Solution

Research and record the exact API call sequences of PyIceberg, Arrow, DuckDB, and any engine accepted under R189. Distinguish incidental listing used for exact-object existence from intentional prefix discovery. Check the Iceberg FileIO and REST Catalog contracts separately; compatibility with an S3-shaped URI alone does not make S3 bucket/list semantics part of Iceberg.

Choose an address model only after that research. The S3 request's bucket field could represent a catalog, a table, or an opaque native routing scope. Preserve catalog/table IDs as first-class Iceberg authorities and avoid creating general S3 bucket records or granting cross-table discovery by default. Document what each choice means for existing table locations, catalog isolation, credentials, pagination, and future multiple-catalog deployments. There is no historical-data compatibility requirement, but an address change must still be atomic for active tables and clients.

If intentional listing is needed, implement only the chosen native listing contract:

1. Extend `app/crowdb-access-server/src/iceberg/file_request.rs` and the native route selection to parse and validate `ListObjectsV2` parameters, including prefix, delimiter, continuation token, encoding, and page limit. Reject unsupported or ambiguous requests before storage access.
2. Add authorized, bounded prefix scans over published file records in `lib/crowdb-access-iceberg/src/file/repository.rs` and its catalog storage. Return only files visible to the caller's table scope; exclude drafts, uncommitted uploads, losing CAS candidates, and retired files according to a documented visibility rule.
3. Extend `lib/crowdb-access-iceberg/src/file/credentials.rs` and credential vending only if a distinct list permission is needed. Bind it to the chosen routing scope and table authorization, with no privilege inherited from the general S3 service.
4. Make pagination stable under concurrent publication and deletion. Bind continuation tokens to the caller, scope, prefix, delimiter, and listing generation or equivalent consistent cursor. Bound scan work and response size; reject malformed, expired, or cross-scope tokens.
5. Add protocol and official-client tests for clients shown by the research to require listing. Keep exact-object FileIO functional without listing and keep the separate general S3 authority unchanged.

#### Dependencies

- R189 client and engine acceptance supplies the observed call sequences and determines which listing cases have user value. Until this requirement is implemented, use an exact-object FileIO for clients that only need Iceberg table files.
- The native file, catalog, and credential contracts in the [Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md) define the present authority boundary. General S3 listing is not a fallback for native Iceberg files.
- If research finds no client that needs intentional prefix listing, close this requirement with the client evidence and retain only exact-object FileIO adapters.

#### Acceptance

- Given the chosen supported clients and a current container, trace an exact-file open and an intentional prefix query; record which client issues each request and which Iceberg or S3 interface it relies on. Assert the decision does not infer an Iceberg listing requirement from a PyArrow existence probe alone. Integration test.
- Given each proposed address model, resolve two catalogs and two tables with different principals; assert routing and authorization cannot expose another table's names or files. Select and document one model before implementing the endpoint. Integration test.
- Given an authorized table with committed, staged, abandoned, and retired file records, request a native listing prefix; assert only the chosen visible set appears, while an unauthorized principal sees none. Integration test.
- Given more matching files than one page, request successive pages with prefix and delimiter; assert bounded pages have no duplicate or omitted eligible keys and tokens cannot be replayed under another principal or scope. Integration test.
- Given concurrent file publication or reclamation during pagination, continue listing; assert the documented snapshot or cursor rule, with bounded work and no cross-table leakage. Integration test.
- Given malformed parameters, foreign catalog/table routes, and attempts to use general S3 credentials, issue a native list request; assert fail-closed responses before scanning. E2E test.
- Given exact-object PyIceberg reads and writes, run the existing native FileIO tests after any routing change; assert they still work without `ListObjectsV2`. E2E test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run cargo test -p crowdb-access-iceberg`, and `pixi run cargo test -p crowdb-access-server` for the implemented scope.

#### Open Questions

- Which supported client operations require intentional prefix listing rather than an exact-object existence or length check? Is the behavior required by the Iceberg FileIO or REST Catalog specification, or by a particular S3 client implementation?
- Should the S3 bucket field map to a catalog, a table, or an opaque native routing scope? A catalog keeps existing locations compact but makes per-table isolation rely on key prefixes; a table makes isolation explicit but affects location and credential vending; an opaque scope allows routing evolution but is less readable to clients.
- Should listing include only files reachable from current table snapshots, all retained snapshots, or every published immutable file awaiting reclamation? How should that choice interact with namespace/table deletion and concurrent commits?
- Is a native `ListObjectsV2` endpoint worth maintaining if supported clients can instead use exact-object FileIO and Iceberg metadata enumeration?
