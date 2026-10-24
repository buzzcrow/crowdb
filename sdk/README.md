<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowDB Dataset SDK

The independent `sdk/` Rust workspace provides a Dataset HTTP client and the
`crowdb-dataset` Python package. Rust owns HTTP, retries, integrity, transfers and
read progress. Python uses the same implementation through PyO3; applications
can use synchronous or asynchronous methods. Rust and installed Python clients
pass real storage/container acceptance, including service restart, payload
integrity, worker partition and explicit progress replay.

- Rust packages: `crowdb-sdk-core`, `crowdb-sdk-dataset`, version 0.1.0.
- Python: `crowdb_dataset.DatasetClient`, `AsyncDatasetClient`, `DatasetIterable`.
- HTTP: reqwest/Tokio/rustls, pooled HTTP/1.1, API v1. Rust callers supply Tokio.
  Python owns one two-worker Rust runtime per client and releases the GIL while
  waiting. Create clients inside workers; inherited live handles are rejected.
- The server uses real Dataset chunks (`Dataset = 7`) through Chunk-KV and normal
  chunk placement/durability. SDK packages have no server/storage-FFI dependency.
- Public [OpenAPI](../doc/design/access-server/dataset/openapi-crowdb-dataset-v1.yaml)
  defines JSON and binary framing. Legacy bincode routes remain internal
  compatibility interfaces. S3/Iceberg standard access uses ecosystem SDKs.

Trusted internal S3 embedding uses the main-workspace
[native SDK](../lib/crowdb-sdk-native/README.md), with shared topology and host
buffer leases. This HTTP distribution has no dependency on that native stack.

## Python

Build a wheel with the interpreter it will be installed into:

```sh
pixi run -- maturin build --release --manifest-path sdk/crates/crowdb-sdk-python/Cargo.toml --interpreter python --out sdk/target/dist
pixi run -- python -m pip install sdk/target/dist/<matching-wheel>.whl
```

Wheels require no Rust compiler at installation. A source build requires Rust
and the selected interpreter. Current installation verification covers Linux
x86_64, CPython 3.14 with and without the GIL; wheels are interpreter-specific.
Other platforms/interpreters need their own build and acceptance. Dependencies
resolve for Rust 1.85; the current build environment uses Rust 1.98.

```python
from crowdb_dataset import DatasetClient, new_id, sample_id

with DatasetClient("http://localhost:9093", "training", "token") as client:
    # Create once; open an existing Dataset on later runs.
    info = client.create()
    upload_id = new_id()  # Keep this ID to resume the same file after interruption.
    upload = client.upload_file("weights.bin", upload_id, concurrency=4)
    publication = {
        "operation": new_id(),  # Keep the operation and identical manifest for retries.
        "parent": info["latest"],
        "manifest": {
            "schema": [{"name": "weights", "required": True}],
            "samples": [{"id": sample_id(b"checkpoint"), "fields": {
                "weights": {"kind": "payload", "upload": upload["id"]}
            }}],
        },
    }
    prepared = client.prepare(publication)  # Latest is unchanged.
    snapshot = client.publish(publication)  # Remote publication completes here.
    client.download_to_path(snapshot["id"], sample_id(b"checkpoint"),
                            "weights", "restored.bin")
```

Async methods use `AsyncDatasetClient` with the same arguments. Metadata/file
methods are awaited; `stream_batch` and `next_batch` return async context managers
and iterators. Cancellation aborts the Rust operation. Closing a stream waits
for producer cancellation and releases its admission; completed Python bytes
remain valid. `refresh_bearer` updates the credential used by subsequent attempts.

For training, send a ReadPlan with `snapshot` (`latest`, `stable`, or an
immutable ID), selection, projected fields, batch size, partition and nullable
shuffle. Selection supports explicit IDs, prefix, range, keyword, field match
and composition. Predicates evaluate inline manifest metadata; payloads are
fetched only after server selection/shuffle/partition.

```python
from crowdb_dataset import DatasetIterable, WorkerPartition

plan = {
    "snapshot": "latest",
    "selection": {"kind": "prefix", "value": sample_id(b"sample-")},
    "fields": ["label"], "batch_size": 64, "shuffle": None,
}
with DatasetClient("http://localhost:9093", "training", "token") as client:
    for batch in DatasetIterable(client, plan, partition=WorkerPartition(worker=0, workers=2)):
        consume(batch.header, batch.fields)
        batch.acknowledge()
```

The iterable acknowledges locally and makes no progress RPC. Continuing without
acknowledgement fails. Save the cursor returned by `batch.acknowledge()` after
processing succeeds; pass it as `checkpoint` to a new iterable to resume.
The iterable skips empty selection or worker windows and advances their local
continuation. Nonempty samples with zero-length fields are still delivered and
require acknowledgement. The lower-level `next_batch` retains empty windows.
`bind_read(plan)` pins immutable versions and returns the initial cursor.
`next_batch(cursor)` replays that cursor on any server. Drop or cancel stops only
the current stream; there is no read ID, session TTL or remote close. Use streams
for fields exceeding the convenience batch limit.

## Rust

`DatasetClient` is cloneable; clones share the pool and atomic admission. It
exposes create/open, upload parts/status/complete/abort, file transfer,
prepare/publish/status, snapshot/manifest/listing, stable, retain/release,
reclaim/status, batch streams and stateless read continuations.
`UploadOptions` controls part size and concurrency. File convenience defaults
to 1-MiB parts and four transfers, bounded by the configured request capacity.
Use 4-MiB parts for uploads up to the v1 limit of 10000 parts (39.0625 GiB).

```sh
pixi run -- cargo run --manifest-path sdk/Cargo.toml --example control
```

Set `CROWDB_DATASET_ENDPOINT`, `CROWDB_DATASET_NAME` and optional
`CROWDB_DATASET_READ_TOKEN`. `examples/payload.rs` is a functional multi-part
consumer with write/verify phases; the Dataset container fixture runs it.
It contains no benchmark driver or timing threshold.

## Bounds, completion and recovery

- `HttpConfig` configures connect/request/overall/transfer deadlines, idle pool,
  request/response windows, active requests/bytes, retry attempts/backoff and TLS
  roots. Python exposes the same configuration names with durations in seconds.
  The Dataset response window admits the v1 8-MiB header. Capacity exhaustion is
  explicit; there is no unbounded request waiting queue.
- File upload makes a bounded digest pass and bounded transfer pass. Completion
  independently verifies durable parts and the whole digest. The completion
  request uses the transfer deadline (default 30 minutes), matching the server's
  longer integrity deadline. Retry an interrupted file with its original ID,
  part configuration and bytes. Accepted parts are reused.
- A lost storage reply is recovered only after confirmed durable chunk cursor
  coverage, followed by integrity validation. Readable unconfirmed bytes are not
  accepted. A fenced abandoned range is reclaimed before replacing its physical
  allocation; the logical part identity stays unchanged.
- Binary batches use one response for all selected fields. Frames carry MD5;
  whole-field verification happens at field end. Success requires batch end and
  HTTP EOF with no trailing bytes. Caller-owned writers may contain a verified
  prefix on failure; discard it. `download_to_path` publishes a temporary file
  only after verification and preserves the old destination on transfer error.
- `get_batch`/`BatchStream.batch()` deliberately bound materialization (8 MiB
  by default). Use streams/files for larger values. Python copies each bounded
  native frame once. Retained Python bytes and completed Rust results belong to
  callers; budget caller-retained data separately from SDK producer windows.
- Only documented idempotent operations retry; bodies/tokens remain unchanged
  within one overall deadline. Authentication, integrity and semantic conflicts
  are not blanket-retried. Rust ambiguous errors and Python exceptions retain
  resource identity. After response loss, retry the identical publication condition;
  a status query is optional and does not prove that the head advanced. A successful
  publish confirms the current effect. If a later publication replaced it, the old
  condition returns a conflict and its observed head for caller review.
- Manifests declare the complete current sample membership. Omitted samples
  disappear; surviving samples may inherit omitted optional fields through
  parents. Explicit tombstones hide inherited fields, including required fields
  that are explicitly removed. Required field names remain in the schema.
- JSON/manifest metadata is bounded to 8 MiB; inline values to 64 KiB. Stateless
  reads keep no selected-ID pages or progress on the server. A shuffle group has
  at most 100000 IDs and 8 MiB of ID bytes; the cursor is bounded to 256 KiB before
  encoding. Small batches within a shuffle group reconstruct that bounded group.
- Prepared candidates persist until publication or explicit reclaim. Reclaim
  preserves head/stable/retained/ancestor data and a deletion grace period. Deferred cleanup reports
  pending; shared ranges do not count as reclaimed whole chunks.

Local disk cache/checkpoint staging, direct chunk access, RDMA, storage-side
Parquet pushdown and benchmark implementation/measurement remain future work.

## Verification

```sh
pixi run test-dataset-sdk
pixi run test-dataset-sdk-python
pixi run sdk-fmt-check
pixi run sdk-lint
pixi run check-dataset-sdk-boundary
pixi run clean-env && pixi run test-e2e-dataset
```

The packaging gate builds an sdist outside the repository and installs its wheel
in a compiler-free consumer PATH. Root Cargo gates do not cover this workspace;
SDK and wheel tasks are registered explicitly in CI.

## Dataset control v2

`DatasetClient::control()` returns the typed Rust `v2::ControlClient`, sharing
HTTP pools, credentials and admission budgets. Every operation checks its server
capability before sending a request; unsupported mutations never fall back to v1.
Existing `DatasetClient` methods retain their v1 contracts.

Successful capability discovery supplies a bounded set of hints shared by client
clones and control wrappers. Warm calls send the business request directly;
authentication, deletion admission and conditional updates still run on the server.
Missing capabilities and failed discoveries are not remembered as denials.
`capabilities()` always queries the server and replaces the hints on a valid reply;
an unsupported API version clears them. Refresh discovery or create a new client
after changing server features. Concurrent cold calls may repeat discovery within
the existing HTTP admission budget. The hint has no TTL and is not a cached read
permission or promise that a redeployed endpoint still supports the operation.

- `OpaqueId` preserves operation/Snapshot/revision IDs; `Generation` is an opaque
  head precondition. `Decimal` encodes exact u64 values as JSON strings, and
  `SampleId` preserves binary IDs through canonical base64.
- `prepare_snapshot(snapshot, request)` submits complete parent/source/schema/
  Sample patches/Split definitions to an immutable resource PUT. Retain the Snapshot
  ID and full input for retries. It requires `composition-preparation`; body/input/
  final Manifest are bounded to 8 MiB and computation to 30 seconds. Parent and
  schema are explicitly nullable. Upsert patches specified fields and keeps other
  effective parent fields; each Sample ID appears once. Frozen Split sources and
  inherited definitions name exact revisions, so retries never resolve currentSplit.
  Rust uses `PrepareSnapshot`, `SnapshotChange`, `SnapshotField` and `SnapshotValue`.
  Upload values include the completed result's `id`, `length` and `md5`; conversion
  adds no descriptor query. Synchronous and async Python expose the same operation.
  Complete Snapshots and their initial Splits are readable before head publication.
  Incomplete work is recomputed; matching final content is reused, while conflicting
  content or initial Split binding is rejected. No workflow progress is saved.
  Capture the intended Dataset head base before computation and publish separately.
- Composition workflow methods and standalone Split workflow methods are removed.
  Their old begin/edit/evaluate/prepare/publish HTTP routes return 501; SDKs never
  fall back to them. Legacy draft observation/abandonment/cleanup remain HTTP
  maintenance operations for existing records. `commit_status` reads only legacy
  receipts; new pointer updates create no request ledger or historical replay.
- `split_members`, `split_history`, `inventory`, `roots`, `children`, `schema` and
  `samples` return lazy `PageCursor`s. Each `next_page` fetches at most 100 items;
  it never eagerly collects the Dataset. Persist `continuation()` for scoped
  resume. Dropping the cursor stops further requests.
- Complete Split revisions can be read or used as frozen sources before updating
  currentSplit, after a head conflict, or after abandoning their draft. Each new
  read still checks Snapshot availability; incomplete revisions remain unavailable.
- `prepare_split_revision(snapshot, revision, request)` submits the complete policy
  and definitions to an immutable resource PUT. Keep the revision ID and exact
  input for retries. It requires `split-preparation`; different input at a complete
  ID conflicts. There is no draft or evaluation cursor, and incomplete work is
  recomputed. Input and final bindings are bounded to 1 MiB; computation has a
  30-second deadline. The complete result is readable before head publication.
  Capture the intended head base before preparation. Rust uses
  `PrepareSplitRevision`; synchronous and async Python expose the same operation.
  Standalone Split workflow SDK methods are removed; their old HTTP write routes
  return 501, with no fallback. Initial Splits are prepared with `prepare_snapshot`.
- `update_split_head(snapshot, request)` updates a complete revision directly with
  `revision`, `expected_revision` and `expected_head`; both expected values must be
  supplied, including null. It requires `split-head-update`, takes no attempt ID
  and does not read a draft. Rust's `SplitHead::from_conflict` and Python's
  `ConflictError.observed_head` return the failed condition's observation. Review
  that base before retrying; the SDK does not rebase automatically.
- `update_snapshot_head(request)` selects an existing complete Snapshot with
  `snapshot` and explicit nullable `expected_head`; it requires
  `snapshot-head-update`, preserves stable and reads no draft or attempt ID.
  Rust uses `UpdateSnapshotHead` and `Head::from_conflict`; synchronous and async
  Python clients expose `ConflictError.observed_head`. A conflict supplies the
  observed latest/stable/generation without an extra head GET. The caller reviews
  the new base and may need to rebuild content from it; the SDK never rebases or
  falls back to a legacy publication route.
- `bind_read` pins Snapshot and optional Split revision without writes;
  `next_batch(cursor)` uses verified streaming framing. Save the next cursor after
  successful processing. The required capability is `stateless-reads`; old session
  routes are retired. No remote acknowledgement or cleanup call is necessary.
- `upload_file` performs bounded digest and transfer passes. `upload_reader` accepts
  caller-owned local/remote readers plus exact length and MD5, checking existing
  parts against replayed bytes. Reopen the same source from the beginning on retry.
  Changed data fails integrity checks; source files and remote objects are untouched.
  This is a reader adapter, not a built-in remote storage/catalog connector.
- Complete Snapshot and Split preparation use caller-owned input; legacy draft cleanup
  actions return 501. Background workers retire old draft resources without client
  cleanup calls. Snapshot deletion marks durable retirement and reports completion
  after background cleanup; latest/stable/retention and deletion grace still apply.

The `split_control` example exercises local file and caller-owned stream ingestion,
initial Split publication, independent revision publication, historical Split reads,
frozen child inheritance, paged inspection and continuation replay. Run it
against a configured Dataset listener with `CROWDB_DATASET_ENDPOINT`,
`CROWDB_DATASET_NAME` and `CROWDB_DATASET_READ_TOKEN` set.

Python `DatasetClient.control()` and `AsyncDatasetClient.control()` expose the same
v2 operations through Rust. `SplitSource` identifies a frozen revision/name and
`decimal(n)` rejects floats, booleans and out-of-range integers. Request dictionaries
use the OpenAPI v2 field names; omitted required nullable preconditions fail.
`pages(kind, ...)` is lazy in both synchronous and asynchronous clients. Use
`next_batch` as a context manager, then save the returned continuation after processing.
Cancellation aborts the Rust operation; clients inherited across fork remain invalid.

`tests/split_consumer.py` is a complete installed-wheel consumer and includes a
bounded caller-owned stream adapter. Supply a stable source, known length/checksum
and the same upload ID for retries. File/remote source objects are never mutated.
The adapter uses the native part API for idempotence and integrity; it does not add
another HTTP transport. `upload_file(path, id)` runs the bounded Rust file flow.
