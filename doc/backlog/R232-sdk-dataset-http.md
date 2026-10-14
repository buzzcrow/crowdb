<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R232: SDK — Project boundary and Dataset HTTP client

Problem:

- CrowDB needs a public SDK for its custom Dataset domain, with room for
  optional S3 RDMA and Iceberg storage-side Parquet predicate pushdown later.
  The [Dataset design](../design/access-server/dataset/design-crowdb-access-dataset.md)
  requires HTTP/native equivalence; the existing Rust client is embedded in
  `crowdb-access-dataset`, while `sdk/crowdb_dataset` only wraps a supplied scan
  client. Neither is an independently usable complete HTTP SDK.
- Dataset authors need remote publication; training users need bounded reads,
  explicit progress and Python integration without building server storage.
- Future Dataset performance benchmarks will drive the public SDK used by
  applications. The first release must support that use without architectural
  rework; benchmark drivers, performance baselines and load testing are outside
  this requirement.

Solution:

1. Develop the SDK in the existing `sdk/` directory in this repository, with
   independent package versions, release notes and compatibility declarations.
   Keep protocol/server/client changes and real-server tests in one review.
   Use a standalone Rust workspace under `sdk/`, excluded from the server root
   workspace, with its own lockfile and build/test entry points. Packages must
   build from their distribution without unpublished parent-directory paths,
   server binaries, the server Hyper fork, C++ storage, RDMA, or Python tooling
   unless the selected optional component actually needs them.
2. The initial Rust packages are `crowdb-sdk-core` for endpoint, credentials,
   deadlines, request errors and HTTP plumbing, and `crowdb-sdk-dataset` for
   Dataset wire models and typed operations. Wire schemas belong to R231;
   authority, Manifest persistence and topology remain server responsibilities.
   Adapt public models explicitly instead of importing the authority crate.
   Add transport extension points only around concrete Dataset operations;
   do not invent a common S3/Iceberg/Dataset storage model.
   Use async reqwest on Tokio with rustls for the initial HTTP backend; keep
   reqwest types behind the SDK boundary. Use PyO3 and maturin for the Python
   extension and wheels. The reference REST implementation in
   [Apache Iceberg Rust](https://github.com/apache/iceberg-rust/blob/main/crates/catalog/rest/src/client.rs)
   uses reqwest; its [Python bindings](https://github.com/apache/iceberg-rust/tree/main/bindings/python)
   use PyO3/maturin. These are implementation precedents, not Dataset semantics.
3. Implement the SDK core in Rust and expose it through a Python native wrapper
   extending `sdk/crowdb_dataset` in this requirement. Rust owns wire codecs,
   HTTP connections, retries, deadlines, upload/read state, progress, integrity
   checks and resource limits. Python owns Python-facing types, exception
   mapping, context managers, iteration and framework integration; it does not
   implement a second HTTP client or duplicate Dataset state machines. Preserve
   existing iterable imports. Deliver both the Rust API and installable Python
   wheels; a C ABI for other languages is not a first-release prerequisite. Move
   old in-server client consumers to the public client or a temporary clearly
   internal compatibility adapter without changing persisted storage records.
4. Cover all R231 public operations, including streamed local-file upload,
   status/recovery, prepare/publish, point/batch reads and bounded iteration.
   Resolve latest once when opening a read session; expose immutable identity.
   The caller explicitly acknowledges completed consumption. Iterator prefetch
   must not advance acknowledged progress; worker/rank partitioning preserves
   disjoint ownership and deterministic resume without fetching discarded
   payloads for every worker. Tensor decoding/collation stays above the client.
5. Configure endpoint, bearer credential provider, TLS verification, connection
   pooling, connect/request/overall deadlines, bounded in-flight bytes and
   requests. Cancellation stops only the caller's work. Retry within one
   deadline with bounded backoff only for documented retryable/idempotent
   operations, preserving upload/publication identity. Report ambiguous mutation
   outcomes with their operation token; do not automatically issue a new write.
   Never include credentials or payload contents in errors/logging.
6. Register SDK unit, packaging, compatibility and E2E gates in `pixi.toml` and
   CI explicitly, since root Cargo gates cannot see the standalone workspace.
   Build/install distributions in an external temporary consumer directory and
   run against the existing Dataset container fixture. Check frozen previous
   wire fixtures alongside current server parity; pin supported API versions
   and add released-server compatibility once a release exists.
   Python wheels include the Rust extension for declared supported platforms;
   installing a wheel requires no Rust compiler. Building a source distribution
   may require Rust, but must not require the server workspace.
7. Keep S3 standard access on existing ecosystem SDKs and Iceberg standard
   access on its existing catalog/FileIO integrations. Future `s3` RDMA and
   `iceberg` predicate-pushdown adapters use explicit capability negotiation and
   remain optional; HTTP packages do not load RDMA or storage FFI dependencies.
   The Python Rust extension is part of the baseline package. A future
   fat client shares Dataset semantics but owns routing refresh and buffers,
   never publication or placement authority. R220 covers that next stage.
8. Define wrapper ownership and runtime lifetime explicitly: handles retain
   Rust-owned resources until explicit close or safe final release; returned
   data remains valid after the originating read handle closes. Blocking Rust
   I/O releases the Python GIL, and Python cancellation propagates to the same
   Rust operation. Create runtime/client handles inside each worker process;
   never reuse inherited live connections or runtimes after fork. Do not require
   unsafe SDK code merely to obtain zero-copy delivery; a bounded copy is valid
   for the first release. Any new unsafe exception follows repository rules.
9. Preserve efficient public APIs for future SDK-driven benchmarks. Reuse
   clients, connection pools and runtimes across operations; allow concurrent
   async calls without serializing all requests behind a global lock. Expose
   bounded concurrency, bytes in flight and part/batch sizes. Stream large
   payloads with backpressure; memory must scale with configured windows rather
   than total file size. Python accepts batches and files/buffers so per-byte
   Python calls are not required. Avoid repeated whole-payload copies and
   conversions while preserving explicit ownership and required integrity
   checks. Keep hot paths lock-free under repository rules.
   Preserve distinct operation completion, retry/error and publication outcomes
   so a later driver can measure the real SDK path without special bypasses.
   This is a design and functional-correctness constraint: do not implement a
   benchmark driver, establish baselines, run load tests or impose throughput
   and latency gates as part of this requirement.

Repository alternatives and decision:

- Same repository, isolated packages: recommended now because the public API
  and server are still evolving together and the existing E2E harness is here.
- Separate repository: appropriate when SDK release ownership or access differs;
  requires cross-repository API changes and a pinned server compatibility matrix.
- Git submodule: retains separate history and a parent commit pointer, so it
  still requires coordinated updates. See the primary
  [Git submodule documentation](https://git-scm.com/docs/gitsubmodules).
  It adds little for jointly owned first-party code at this stage. Do not add
  one now; independent packaging keeps a later repository extraction practical.

Dependencies:

- R231 supplies the stable public API and full remote payload path. SDK
  scaffolding can proceed against its schemas; release waits for real E2E.
- R233 adds local disk staging/cache; first release streams to the server and
  promises remote durability only after successful publication.
- R220 consumes the same Rust core and Python wrapper. Its older
  separate-repository/C-ABI-first assumption is superseded by this project
  boundary; direct access extends the Rust implementation behind the wrapper.
- R196 owns S3/Iceberg controlled upload regression and does not block this
  requirement. Future Dataset benchmark work should use the public Rust SDK
  and Python wrapper and may reuse existing deployment/artifact conventions.
  Its driver, measurements and baselines are deferred and are not SDK release
  acceptance conditions. R233/R220 require measurements when pursued later.

Acceptance:

1. Given SDK source distributions outside the repository -> build the Rust
   packages, build the Python distribution and install its wheel in a consumer
   without Rust -> assert no server/storage-FFI/private-path dependencies and
   runnable examples. Package independence invariant. Integration test.
2. Given API fixtures and injected Rust failures -> call the Rust API and Python
   wrapper -> assert equivalent types, IDs, errors and version behavior through
   the same Rust codec and operation implementation. Binding parity invariant. Integration test.
3. Given a clean server and local multi-part file -> use each installed SDK to
   create, upload, prepare, publish, inspect, read and exercise retention/status ->
   assert complete remote round trip and unchanged latest after prepare.
   HTTP completeness invariant. E2E test.
4. Given a slow consumer, multiple workers, a new latest and an interrupted
   batch -> iterate, acknowledge and resume -> assert bounded bytes, disjoint
   worker delivery, fixed Snapshot and no unacknowledged skip. Progress invariant. Integration test.
5. Given response loss, throttling, TLS/auth failure, timeout and cancellation ->
   execute reads and writes -> assert deadline/retry limits, stable operation
   identity, isolated cancellation and credential-free diagnostics. Retry invariant. Integration test.
6. Given packaged SDKs and current/frozen API fixtures -> run compatibility and
   real container tests through the CI tasks -> assert coverage includes SDK
   packages despite root workspace exclusion. Compatibility invariant. E2E test.
7. Given a default HTTP installation -> inspect dependencies and run all Dataset
   operations -> assert no optional native transport is loaded or required.
   Optional transport invariant. Integration test.
8. Given retained result buffers, blocked I/O, cancellation and multiple Python
   worker processes -> close handles, run another Python thread, cancel a read
   and create worker clients -> assert valid result lifetimes, GIL progress,
   isolated cancellation, released resources and rejection of inherited live
   handles. Binding lifecycle invariant. Integration test.
9. Given a controlled transport with gated responses and a bounded stream ->
   make concurrent public SDK calls and consume a Python batch -> assert client
   reuse, independently progressing requests, bounded producer admission and
   explicit completion/error results. Use deterministic synchronization without
   throughput or latency thresholds. Resource contract invariant. Integration test.

Verification commands (the SDK workspace is introduced by this requirement):

- `pixi run -- cargo test --manifest-path sdk/Cargo.toml --workspace`
- `pixi run -- python -m unittest discover -s sdk -p 'test_*.py'`
- `pixi run clean-env && pixi run test-e2e-dataset`
- `pixi run -- cargo fmt --manifest-path sdk/Cargo.toml --all -- --check`
- `pixi run -- cargo clippy --manifest-path sdk/Cargo.toml --workspace --all-targets -- -D warnings`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
