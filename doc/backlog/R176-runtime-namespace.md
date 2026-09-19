<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R176: test harness / console — Unified runtime namespace

## Problem

Multi-process tests and local clusters allocate network ports and filesystem
paths through separate mechanisms. Port allocation records every selected port
in a claim file, while process harnesses independently create data, config, and
log paths using process IDs and counters. A process restart can therefore be
mistaken for a new logical instance and receive a different endpoint. Stale
service-registry records then remain selectable even though the replacement
process listens elsewhere. Separately started E2E binaries can race between a
bind probe and child-process bind, and unrelated runs leave intermixed files
under shared `test-data` and `test-logs` directories.

Runtime files are also spread across `test-data`, `test-logs`, `runtime-data`,
per-crate `log`, `waldata`, `ctdata`, `.test-tmp`, and hard-coded system-temp
paths. `.gitignore`, `clean-env`, and `clean` duplicate this inventory and can
either miss new artifacts or delete a broad directory without knowing whether
it contains disposable test state or a persistent local cluster. CROWDB-owned
runtime files need one workspace-local hierarchy with explicit cleanup class;
the system temporary directory is not part of that hierarchy.

This is visible in complete-stack scenarios such as S3 compatibility testing:
one environment contains KV, DiskDB, DiskIO, ChunkDB, chunk-KV, access-server,
durable data, generated configs, logs, restart phases, and benchmark artifacts.
The environment needs one identity and one ownership boundary. The test
strategy in `design-crowdb-kv-test.md` defines these tests as independently
diagnosable layers but does not yet define their runtime-resource isolation.

## Solution

`crowdb-test-harness` and console deployment use a shared runtime-namespace
model. A namespace owns both its filesystem tree and its logical-to-physical
port assignments. A service process consumes an assignment; it does not
allocate resources by itself.

1. Add a versioned namespace manifest containing a stable namespace ID, mode,
   owner, root, and a map from `(ServicePort, logical instance)` to port. Port
   assignment is atomic under the existing cross-process claim-file `flock`,
   with a bind probe before publication.
2. Use one workspace-global claim registry instead of per-process claim files.
   Each claim records its namespace owner. Ephemeral owners include PID plus a
   process-start token so dead owners can be reclaimed without confusing PID
   reuse. Persistent owners remain claimed until their cluster is deleted.
3. Use the repository-local `.crowdb-runtime/` as the only default root for
   CROWDB-created runtime files. It contains `ephemeral/`, `persistent/`,
   `artifacts/`, and `ports/` lifecycle classes. No CROWDB test, benchmark,
   sanitizer, helper script, or default local deployment writes to the system
   temporary directory.
4. Give every namespace `data/`, `config/`, `log/`, and `artifacts/` children.
   Every server receives paths below
   `<namespace>/services/<service>/<logical-instance>/`; unrelated runs never
   share mutable files or log names. C++ and Rust test helpers resolve the same
   root through an explicit environment variable with the repository root as
   the fallback.
5. Ephemeral test namespaces live for the complete test environment. Normal
   completion releases their claims and removes their tree; panic or failed
   setup releases live resources but preserves the tree for diagnosis.
   Benchmark artifacts may be explicitly exported before cleanup.
6. Persistent CLI namespaces store the same assignments in their cluster
   manifest. Stop preserves assignments and data; restart validates and reuses
   them; cluster deletion releases the claims. A port occupied by a different
   owner is an explicit conflict and never causes silent renumbering.
7. Define lifecycle operations precisely: `start_new` creates a new logical
   identity and assignment, `restart` reuses identity, paths, and ports, and
   `replace` creates a new identity and assignment while explicitly retiring
   the old registry record. A restart never calls the allocator.
8. Migrate `KvCluster`, DiskDB, DiskIO, ChunkDB, chunk-KV, access-server, web,
   CLI, and their E2E helpers to accept a namespace plus logical identity.
   Remove local PID/counter path generation and direct `alloc_test_port` use
   from process-spawning tests.
9. Keep kernel-selected port zero only for in-process listeners that retain the
   bound socket and never hand the address to a child process. Subprocess
   handoff, advertised endpoints, restart tests, and persistent clusters must
   use namespace assignments.
10. Make complete-stack runners create the environment before reporting test
   cases. Each functional scenario is a separately named case; environment
   creation and cleanup are fixture work, not a synthetic test.
11. Reduce `.gitignore` runtime entries to the unified root after migration.
    `clean-env` terminates processes recorded by ephemeral namespace manifests,
    releases stale claims, and removes only ephemeral state. `clean` may also
    remove build products and disposable artifacts but preserves
    `.crowdb-runtime/persistent/`. Persistent data is removed only by an
    explicit cluster-delete or dedicated destructive cleanup command naming
    the exact namespace.
12. Remove legacy system-temp cleanup and broad source-tree searches after all
    producers migrate. Cleanup operates on known namespace roots and manifests,
    not filename patterns, recursive `find`, or global `/tmp` globs.

The following invariants apply:

- **RN1 — Single ownership:** every subprocess port and mutable path belongs to
  exactly one live or persistent namespace.
- **RN2 — Stable restart:** one logical service identity has identical ports
  and paths before and after restart.
- **RN3 — Explicit replacement:** a new identity never masquerades as a
  restart, and the old discovery record is retired or allowed to expire only
  through an explicit replacement flow.
- **RN4 — Cross-process exclusion:** concurrent E2E binaries cannot receive the
  same port assignment.
- **RN5 — Diagnostic isolation:** a failed environment preserves one complete
  namespace tree without files from another run.
- **RN6 — No silent renumbering:** recovery either reacquires the recorded
  assignment or reports its conflicting owner.
- **RN7 — Workspace locality:** every default CROWDB runtime path is below
  `.crowdb-runtime/`; the system temporary directory is never an implicit
  storage or coordination dependency.
- **RN8 — Cleanup safety:** ordinary clean operations remove only ephemeral or
  rebuildable namespace classes and never persistent cluster data.

## Dependencies

- Uses the existing `ServicePort` ranges and `fs2`-based claim-file locking.
- Extends `crowdb-test-harness::test_dirs` and the console cluster manifest;
  it does not change production RPC discovery semantics.
- Existing full-stack restart fixes that reuse DiskDB and chunk-KV configs and
  ports are the migration baseline.
- Persistent namespace deletion composes with the existing CLI cluster-delete
  lifecycle; until deletion exists, an explicit namespace-release operation is
  required and stop remains non-destructive.

## Acceptance

- Given two concurrently started ephemeral namespaces, when both request the
  same service and logical instance, assert distinct ports and disjoint root,
  data, config, log, and artifact paths. Invariant: RN1 and RN4. Integration
  test.
- Given an ephemeral owner killed without cleanup, when a later namespace
  allocates resources, assert dead claims are reclaimed using PID plus process
  start identity while live claims remain unavailable. Invariant: RN4. E2E
  test.
- Given a running service, when `restart` is invoked, assert identity, config,
  data path, advertised endpoint, and all listener ports are unchanged.
  Invariant: RN2. Integration test.
- Given a running service, when `replace` is invoked, assert a new identity and
  assignment are created and the old discovery endpoint is not selected as the
  replacement. Invariant: RN3. Integration test.
- Given a persistent mini-cluster stopped and started from the same directory,
  assert all assignments and stored data are reused; if one recorded port is
  held by another owner, assert startup fails with that owner and port rather
  than renumbering. Invariant: RN2 and RN6. E2E test.
- Given a successful ephemeral test, assert its namespace tree and claims are
  removed; given a panic or failed setup, assert its namespaced data and logs
  remain and no subprocess remains live. Invariant: RN1 and RN5. Integration
  test.
- Given Rust tests, C++ tests, sanitizer scripts, benchmarks, and local cluster
  defaults, assert all generated data, config, logs, coordination files, and
  artifacts resolve below `.crowdb-runtime/` and no `/tmp` or platform temp API
  is used. Invariant: RN7. Integration test.
- Given `pixi run clean-env`, assert recorded ephemeral processes, namespaces,
  and stale claims are removed while persistent namespaces survive; given
  `pixi run clean`, assert the same plus build artifacts, without recursive
  runtime-name searches or system-temp globs. Invariant: RN8. Integration test.
- Given the migrated runtime hierarchy, assert `.gitignore` ignores the unified
  root and no longer carries obsolete entries for generated test data, logs,
  port claims, or per-crate runtime directories. Invariant: RN7 and RN8. Unit
  test.
- Given all process-spawning Rust E2E tests, assert no direct per-process
  `alloc_test_port`, PID/counter config path, shared mutable test-log filename,
  or child-process port-zero handoff remains outside the namespace module.
  Invariant: RN1 and RN4. Integration test.
- Given the S3 complete-stack runner, assert environment startup is outside the
  test count and each boto3, restart, and benchmark capability has its own
  visible result; all restart cases reuse their original assignments.
  Invariant: RN2 and RN5. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-protocol --all-targets`
- `pixi run -- cargo test -p crowdb-test-harness --all-targets`
- `pixi run -- cargo test -p crowdb-console-shared --all-targets`
- `pixi run -- cargo test -p crowdb-cli --all-targets`
- `pixi run -- cargo test -p crowdb-web --all-targets`
- `pixi run -e s3-e2e test-boto3-e2e`
- `pixi run clean-env`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`

## Open Questions

None.
