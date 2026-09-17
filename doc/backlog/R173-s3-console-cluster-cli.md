<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R173: console / access server / S3 — Cluster deployment, CLI operations, and benchmark

## Problem

The basic S3 service delivered by R152–R166 can serve authenticated bucket and
object traffic, but an operator must manually assemble its KV, DiskDB, ChunkDB,
DiskIO, access-server, and web dependencies and export every access-server
environment variable.  The CLI has no S3 domain: it cannot deploy or inspect an
S3 service, create/list buckets, operate on objects, or run a repeatable S3
benchmark.  The only benchmark is a Python E2E helper, which is not a CLI
workload and does not provide a common result contract.

This makes a working S3 cluster difficult to reproduce and hides its endpoint,
capacity plan, protection scheme, and credentials from the normal console
control path.  It also risks separate CLI and future web implementations.  The
shared-console boundary in
`doc/design/console/design-crowdb-console.md` §2 requires both frontends to
call `crowdb-console-shared::ops`; the S3 namespace and storage invariants are
defined by `doc/design/accessserver/design-crowdb-access-server-s3.md`.

Concrete operator scenarios are: provision a usable default S3 cluster on an
empty registered topology; request a usable capacity with replication or EC
protection and learn whether the current free storage and failure domains can
satisfy it; script bucket/object CRUD without creating a tenant; and compare
write, read, range-read, list, and weighted mixed traffic against the same
endpoint.

## Solution

The CLI gains an `s3` domain.  It is a control and data-plane client for the
existing S3 HTTP service, not a second S3 implementation.  All validation,
planning, process lifecycle, S3 request construction, result types, and
benchmark execution live in `crowdb-console-shared`; `crowdb-cli` only parses
arguments and renders the shared result.  `crowdb-web` is deployed with the
cluster and may consume these shared operations later, but this requirement
adds no S3-specific web route, navigation, or SPA screen.

The resulting control flow is:

```text
crowdb-cli s3 cluster deploy
          |
          v
crowdb-console-shared::ops::s3
   | plan + persist launch records
   v
KV + DiskDB + DiskIO + ChunkDB ----> access-server endpoints
          |                                  |
          +---------------------> crowdb-web + console config
```

1. Add `ops::s3` to `crowdb-console-shared` with typed deployment input,
   validated `S3ClusterPlan`, deployment/status result, S3 endpoint client,
   credential reference, bucket/object results, mix parser, and S3 benchmark
   runner.  Extend the persisted console configuration and local lifecycle
   records with distinct `AccessServer` and `Web` service kinds, launch
   specification, PID, readiness endpoint, S3 endpoint, and non-secret cluster
   identity.  The master key and issued secret are never persisted in TOML,
   logs, CLI JSON, or a process argument; deployment receives them through a
   protected environment source and prints a newly issued credential exactly
   once to the invoking terminal.
2. Add `crowdb-cli s3 cluster {plan,deploy,status,stop,delete}`.  `plan` is
   read-only and prints requested usable capacity, raw capacity, selected
   nodes/failure domains, storage layout, access endpoints, and validation
   warnings.  `deploy` commits only after a valid plan, starts dependencies in
   KV → DiskDB → DiskIO → ChunkDB → access-server → web order, waits for each
   required health/registry condition, persists enough lifecycle state for
   `status`, `stop`, and `delete`, and returns the S3 endpoint(s), web URL, and
   one tenant credential.  `stop` preserves data and records; `delete` stops
   only services created by this S3 cluster record and removes that record.  A
   failed deployment stops only processes started by that invocation, preserves
   prior services and data, and reports the completed and rolled-back steps.
3. Define the initial planner contract.  `--capacity` is requested usable
   bytes and defaults to `1TiB`.  Without `--ec`, `--protection <copies>`
   defaults to `3`, requires that many distinct failure domains, and reserves
   `capacity * copies` raw bytes.  `--ec <data>+<parity>` selects EC for large
   objects, requires `data + parity` distinct failure domains, and reserves
   `ceil(capacity * (data + parity) / data)` raw bytes; it is mutually exclusive
   with a non-default `--protection`.  The planner queries current allocatable
   free bytes from DiskDB rather than registered nominal disk capacity, excludes
   unhealthy or duplicate failure domains, and fails before mutation if capacity
   or placement cannot satisfy the request.  Initial default topology selects
   three healthy failure domains and starts one KV, DiskDB, DiskIO, ChunkDB, and
   access-server instance per selected node plus one web instance; explicit
   `--nodes` constrains this selection.  Normal S3 deployment places all
   storage state on file-backed disk blocks, including S3 object data and
   metadata; the requested capacity is backed by that storage, not by a
   synthetic disk.  The access-server receives the planned management seeds,
   fixed default tenant, master-key source, listener address,
   and selected large-object EC scheme.  Multiple access endpoints are returned
   as equivalent S3 entry points; load balancing and gateway failover remain
   outside this requirement.
4. Add `crowdb-cli s3 bucket {add,remove,list,inspect}` and
   `crowdb-cli s3 object {put,get,delete,list,inspect}`.  These use the
   deployed endpoint or an explicit `--endpoint`, authenticate with an explicit
   credential source, and translate the existing S3 HTTP compatibility surface
   directly: bucket add/remove/list/head; object put/get/head/delete; and
   ordered list with `--prefix`, `--limit`, and opaque continuation token.
   `object get --range <start>-<end>` requests exactly one inclusive contiguous
   S3 byte range.  `put` streams a file or standard input, while `get` streams
   to a file or standard output without materialising the object in the CLI.
   These ordinary bucket/object commands run against the normal file-backed
   S3 deployment and provide the byte-exact, durable correctness check; they
   are separate from the synthetic-storage benchmark commands.
   There is exactly one default tenant established by deployment; no tenant
   create/list/delete command is exposed.  Existing S3 errors, including empty
   bucket deletion, unsupported multi-range reads, missing bucket/key, and an
   invalid continuation token, retain their S3 meaning and perform no CLI-side
   fallback mutation.
5. Add `crowdb-cli bench s3 {write,read,range-read,list,mix}` and a matching
   shared S3 benchmark target.  Each workload accepts endpoint, credential,
   bucket, object-size/dataset, concurrency, duration or operation limit, seed,
   and output location.  Preparation creates the benchmark bucket and
   deterministic objects when needed; teardown removes only benchmark-owned
   keys and then the empty benchmark bucket.  Read and range-read consume the
   complete response without checking object bytes; range-read chooses one
   valid contiguous range per request.  List checks ordered pagination and
   continuation-token progress so metadata failures cannot pass as throughput.
   `mix --ratio w20R70RR5L5` is case-insensitive, accepts positive integer
   weights whose total need not equal 100, supports `W`, `R`, `RR`, and `L`,
   rejects unknown, duplicate, zero-total, malformed, or overflowing terms, and
   samples the normalized weights deterministically from the seed.  Results
   retain the common benchmark JSON envelope and add separate write, read,
   range-read, and list operation counts, errors, throughput, and latency
   percentiles; metadata, protocol, and transport failures are reported
   separately.
   Benchmark deployment has an explicit storage profile recorded in every
   result.  Its purpose is to find obvious performance bugs along the S3,
   chunk-stream, chunk-KV, and service path without physical I/O dominating
   the measurement.  KV servers and other control-plane state use mem disk.
   S3 object data and chunk-stream chunks may use null disk for both write and
   read-path measurements.  Chunk-KV may use null disk only while its entire
   required value/page working set remains in memory; when it can spill or read
   back from disk, its backing must be mem disk so metadata remains correct and
   physical metadata I/O does not mask path performance.  Read, range-read,
   list, and mixed workloads require successful metadata lookup, S3 response
   status, expected response length or range, and list pagination, but make no
   object-byte correctness claim.  A null-disk response that fails an internal
   chunk or S3 integrity check counts as an error, not a successful read.
   The runner reports the selected backing by component, memory footprint and
   peak, and that object bytes were not verified.
   Preparation and results distinguish warm-up from measured operations.
   Mem-disk capacity and process memory are bounded by a configurable
   benchmark budget; an exhausted budget fails the run explicitly rather than
   silently changing backing or counting failed metadata operations as valid.
6. Keep frontend boundaries strict.  The CLI clap definitions live under its
   `commands::s3` and `commands::bench::s3` adapters only.  `crowdb-web` may
   expose generic lifecycle/status information needed to run the web console,
   but it neither owns S3 orchestration nor reimplements bucket/object/benchmark
   behavior.  A later S3 UI calls the same `ops::s3` contracts and receives the
   same validation, planning, lifecycle, and error semantics.

## Dependencies

- Depends on R152–R166 basic TCP S3 behavior, especially its atomic publication,
  ordered list, contiguous range, SigV4, credential, and error contracts.
- Uses the existing `crowdb-console-shared` lifecycle, service discovery,
  DiskDB capacity query, cluster deployer, and persisted console configuration;
  it must not duplicate their process/PID management in `crowdb-cli` or
  `crowdb-web`.
- Requires runnable `crowdb-kv-server`, `crowdb-diskdb`, `crowdb-diskio`,
  `crowdb-chunkdb`, `crowdb-access-server`, and `crowdb-web` artifacts.  When a
  required binary or configured master-key source is unavailable, planning or
  deployment fails before it starts a dependent process.
- R167 multipart upload, R168 shared-object reclamation, R169 tree/shared-chunk
  GC, and R170 cuObject/RDMA remain independent.  The CLI exposes only the
  basic S3 surface and TCP data path until those requirements land.
- A local three-node mem-disk chunk-KV run with 30,000 puts, 4 KiB values, and
  concurrency 32 entered `SplitPreparing` during the load.  Its first failure
  was range rebuild rejecting an oversized source leaf; after that was fixed,
  the same workload still ended with 320 transport failures while split was
  active.  Logs also showed expired serving grants and a full chunk RPC
  completion slab.  This is an observed storage-path regression, not evidence
  that mem disk itself exhausted host memory.  S3 benchmark results that cross
  this split path cannot be accepted until the underlying failure is resolved.

## Acceptance

- Given healthy DiskDB instances across three failure domains with at least
  `3TiB` allocatable free space, when `s3 cluster plan` uses defaults, assert it
  returns `1TiB` usable capacity, three-copy protection, `3TiB` raw capacity,
  three selected domains, and one instance of every required storage and access
  service per selected node plus web.  Invariant: the default plan is explicit,
  reproducible, and capacity is usable rather than raw. Integration test.
- Given insufficient allocatable free bytes, repeated failure domains, unhealthy
  disks, invalid capacity, invalid copy count, or malformed/unsatisfiable EC,
  when `s3 cluster plan` or `deploy` runs, assert it names the constraint and
  starts no process or mutation.  Invariant: a plan never promises capacity or
  fault tolerance that the live topology cannot provide. Unit test.
- Given `--capacity 12TiB --ec 4+2` over six healthy failure domains, when the
  planner runs, assert it selects EC, requires six domains, and reserves `18TiB`
  raw capacity; given a non-default `--protection` too, assert validation fails.
  Invariant: EC overhead and protection input cannot be interpreted
  ambiguously. Unit test.
- Given a valid plan and all local service binaries, when `s3 cluster deploy`
  runs, assert dependency startup order, health/registry waits, persisted
  lifecycle records, access endpoint(s), web URL, and a one-time credential;
  then `status`, `stop`, and `delete` report and affect only that deployment.
  Invariant: deployment is recoverable and never manages unrelated services.
  E2E test.
- Given an access-server or web startup failure after storage starts, when
  deployment handles the failure, assert it stops only invocation-owned
  processes, preserves data and earlier services, redacts secret material, and
  returns completed and rolled-back steps.  Invariant: partial deployment is
  safe to diagnose and retry. Integration test.
- Given the deployed default tenant and valid credentials, when bucket add,
  inspect, list, empty remove and object put/get/inspect/delete/list run,
  against file-backed disk blocks, assert their HTTP requests and outputs
  preserve existing S3 metadata, ordered paging, byte-exact streaming, and S3
  errors.  Invariant: the CLI is a thin S3 client and its ordinary commands
  provide the disk-backed correctness check. E2E test.
- Given an object and `object get --range 3-9`, when it runs, assert exactly the
  inclusive seven-byte interval is written; given multiple ranges, a missing
  key, a non-empty bucket remove, or an invalid continuation token, assert the
  matching S3 failure and no fallback mutation.  Invariant: CLI range and list
  behavior preserve the S3 service contract. Integration test.
- Given prepared deterministic data, when each `bench s3 write`, `read`,
  `range-read`, and `list` workload runs, assert its JSON result has its own
  operation count, errors, throughput, and latency percentiles, metadata and
  protocol failures are separate, and benchmark cleanup touches only its owned
  bucket.
  Invariant: benchmark measurements remain attributable and non-destructive.
  Integration test.
- Given the benchmark profile, when S3 object data and chunk-stream chunks use
  null disk and KV/control-plane state uses mem disk, assert the result records
  each backing and reports write and read-path throughput without claiming
  object-byte verification.  Invariant: the I/O-free result has an honest
  correctness scope. Integration test.
- Given a chunk-KV working set that exceeds its in-memory residency, when a
  benchmark writes metadata and then reads or lists it, assert chunk-KV uses
  mem disk and returns the exact written values; configuring null disk for that
  spill path fails validation.  Invariant: metadata cannot be lost behind a
  synthetic disk. Integration test.
- Given read, range-read, list, and mixed benchmark profiles on null disk, when
  prepared objects are requested after setup, assert successful S3 status,
  expected response lengths and ranges, ordered keys and token progress, with
  no byte-comparison requirement; count internal integrity or metadata failures
  as errors.  Invariant: reported throughput reflects completed request paths
  with usable metadata. Integration test.
- Given a mem-disk benchmark dataset near and above the configured memory
  budget, when the workload runs, assert it records peak memory and either
  completes within the budget or fails with an explicit resource error before
  reporting throughput as valid.  Invariant: eliminating physical I/O cannot
  hide memory exhaustion or corrupt results. E2E test.
- Given enough chunk-KV metadata to trigger partition split during a benchmark,
  when concurrent writes continue through split preparation and publication,
  assert the split converges, requests complete without transport errors, and
  metadata remains readable.  Invariant: split cannot silently invalidate an
  S3 benchmark result. E2E test.
- Given a normal S3 deployment, when an object is written, the services restart,
  and the object is read, assert object bytes and metadata survive on file-backed
  disk blocks.  Invariant: benchmark-only synthetic storage cannot leak into
  the production deployment. E2E test.
- Given `bench s3 mix --ratio w20R70RR5L5` and a fixed seed, when it runs twice,
  assert both normalized weighted schedules match and account for all four
  operation types; given malformed, duplicate, unknown, zero-total, or
  overflowing ratios, assert rejection before workload setup.  Invariant: mix
  selection is deterministic and unambiguous even when weights do not sum to
  100. Unit test.
- Given a future web handler for any S3 operation, when it invokes the shared
  operation with the same input as the CLI adapter, assert it receives the same
  plan, result, and typed error without a second implementation.  Invariant:
  shared operations are the single S3 console behavior. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-console-shared --all-targets`
- `pixi run -- cargo test -p crowdb-cli --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -e s3-e2e test-boto3-e2e`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy -p crowdb-console-shared -p crowdb-cli -p crowdb-access-server --all-targets -- -D warnings`

## Open Questions

- What measured resident-memory limit and dataset sizes can the target
  benchmark hosts sustain with mem disk? A smaller dataset protects the host
  but may miss spill and scale-related performance bugs; a larger one tests
  those paths but risks memory exhaustion. Establish this with a bounded
  experiment before setting the default benchmark budget.
