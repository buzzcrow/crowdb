<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R173: console / access server / S3 — Persistent CLI and memory benchmark

## Problem

The S3 service can serve bucket and object traffic, but an operator otherwise
has to assemble KV, DiskDB, DiskIO, ChunkDB, chunk-KV, and access-server by
hand. A manually assembled stack is difficult to restart with the same storage
and identities, and the CLI has no repeatable S3 performance workload. The
console architecture defines shared operations as the owner of CLI behavior;
`design-crowdb-console.md` section 7.8 defines the persistent mini-cluster
contract.

Two deliberately separate scenarios are required. Ordinary bucket and object
commands use a file-backed cluster and prove restart durability. Benchmarks use
an explicitly memory-backed cluster so physical disk throughput does not hide
performance in the S3, chunk-stream, and chunk-KV paths. Benchmark success must
still mean a complete valid request path, not a synthetic successful response.

## Solution

`crowdb-console-shared::ops::s3` owns cluster lifecycle, thin S3 HTTP
operations, and benchmark execution. `crowdb-cli` only parses commands, streams
input or output, and renders shared results.

1. `s3 cluster start --data-dir <path>` initializes a missing or empty
   directory and otherwise restarts a recognized cluster. The durable marker
   and console configuration retain stable service identities, endpoints,
   launch specifications, storage paths, and PIDs. A foreign non-empty
   directory fails without mutation.
2. Persistent startup order is KV, DiskDB, DiskIO, ChunkDB, chunk-KV, then
   access-server. Each dependency becomes ready before the next starts. DiskIO
   uses sparse files below the selected directory. `status` is read-only and
   `stop` is idempotent, terminates only recorded processes, clears persisted
   PIDs, and preserves configuration and data.
3. `s3 bucket {add,remove,list,inspect}` and
   `s3 object {put,get,delete,list,inspect}` discover the endpoint from the
   selected directory and map directly to the existing S3 HTTP surface.
   Object get supports one inclusive byte range. List preserves prefix, limit,
   ordering, and opaque continuation tokens. Non-success responses return the
   S3 failure without fallback mutation.
4. `bench s3 {write,read,range-read,list,mix}` deploys or attaches to an
   explicitly memory-backed local stack. KV and control metadata use memory
   storage. Object data and chunk streams may use null storage only where reads
   retain a complete valid metadata and length path; chunk-KV uses memory
   storage whenever values or pages can be read after eviction. The result
   records each component backing and never claims byte verification for null
   object data.
5. Every benchmark accepts bucket, object size or prepared dataset, concurrency,
   duration or operation limit, seed, warm-up, and output. Read consumes full
   responses, range-read selects one valid contiguous interval, and list checks
   ordering and continuation progress. `mix` accepts case-insensitive positive
   `W`, `R`, `RR`, and `L` weights, rejects malformed, duplicate, unknown,
   zero-total, or overflowing terms, and samples deterministically from seed.
6. Benchmark results use the common JSON envelope and report per-operation
   attempts, successes, failures, throughput, and latency percentiles. Metadata,
   protocol, transport, and resource failures are separate. A bounded memory
   budget fails explicitly instead of silently changing backing or reporting
   invalid throughput. Cleanup removes only benchmark-owned objects and bucket.
7. A failed cluster start stops only processes created by that invocation. It
   retains diagnostic logs but does not publish a complete marker until the
   endpoint is ready. Secrets are absent from marker data and rendered output.

## Dependencies

- Depends on existing S3 bucket, object, ordered-list, range, and trusted
  loopback authentication contracts.
- Uses existing console lifecycle, benchmark result types, and local storage
  deployer rather than duplicating them in the CLI.
- Automatic chunk-KV split and owner balance remain enabled.

## Acceptance

- Given a missing, empty, recognized, or foreign non-empty directory, when
  `cluster start` runs, assert initialization/restart is correct and the foreign
  directory is unchanged. Invariant: the directory is the cluster identity and
  recovery boundary. Integration test.
- Given a running persistent cluster, when bucket and object CRUD/list run,
  assert byte-exact output, encoded keys, pagination, and S3 error passthrough;
  after full stop and restart, assert the same bytes remain readable.
  Invariant: ordinary CLI operations prove disk-backed durability. E2E test.
- Given `object get --range 3-9`, assert exactly seven bytes are written; given
  invalid range, token, key, or non-empty bucket deletion, assert the S3 failure
  and no fallback mutation. Invariant: HTTP semantics are preserved.
  Integration test.
- Given each S3 benchmark verb, assert a memory-backed cluster is used, warm-up
  is excluded, the requested bound terminates the run, and results contain
  backing, memory, operation, failure, throughput, and latency fields.
  Invariant: results are bounded and attributable. Integration test.
- Given prepared data, assert read consumes expected lengths, range-read consumes
  the selected length, list validates ordering/token progress, and internal
  metadata or integrity failures count as errors. Invariant: memory backing
  cannot bypass the real request path. Integration test.
- Given `w20R70RR5L5` and a fixed seed, assert repeated schedules match and all
  four operations are represented; reject malformed, duplicate, unknown,
  zero-total, and overflowing ratios before setup. Invariant: mixed selection
  is deterministic and unambiguous. Unit test.
- Given a memory budget at and below dataset demand, assert the first run
  records peak use and the second fails explicitly without valid throughput.
  Invariant: memory mode cannot hide exhaustion. E2E test.
- Given startup failure, assert invocation-owned processes stop, the marker is
  not published complete, secrets are not rendered, and retry remains possible.
  Invariant: partial startup is safe and diagnosable. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-console-shared --all-targets`
- `pixi run -- cargo test -p crowdb-cli --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -e s3-e2e test-boto3-e2e`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy -p crowdb-console-shared -p crowdb-cli -p crowdb-access-server --all-targets -- -D warnings`

## Open Questions

None.
