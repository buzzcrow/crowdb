<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R184: access server / Iceberg — REST integration and core conformance

Status: engine acceptance (Spark/Flink/Trino) is deferred by the user's
2026-09-24 decision to a separate testing project they will establish later.
Track it in the functional catalog plan's Next section; do not run it during the
current implementation phase or claim it has passed. The acceptance contract
below remains outstanding rather than being removed.

## Problem

Component repositories can be locally correct while the public catalog remains
incompatible: `/v1/config` may advertise unimplemented routes, identifiers may be
decoded differently between handlers, error types may not match the OpenAPI,
authentication may disclose renamed resources, and a real Spark, Flink, Trino, or
Iceberg client may exercise a different sequence from unit tests.

R178 through R183 define the native authority and operations. This requirement
owns the single public REST composition, capability discovery, common protocol
behavior, and conformance evidence for the first usable milestone.

## Solution

- **REST-I1 — Honest discovery:** `/v1/config` advertises exactly the enabled and
  verified endpoint and format capability set.
- **REST-I2 — One protocol boundary:** all handlers share bounded decoding,
  authentication, authorization, request identity, error serialization, admission,
  deadlines, cancellation, and metrics.
- **REST-I3 — Standard semantics:** declared behavior matches the backed-up OpenAPI
  and v1/v2/v3 table spec rather than one client implementation's quirks.
- **REST-I4 — Failure isolation:** invalid, unauthorized, oversized, timed-out, or
  cancelled requests do not leave an ambiguous mutation.
- **REST-I5 — Interoperability:** official clients can create, evolve, write, commit,
  load, time-travel, read, rename, expire, and drop core tables through CROWDB.

1. Complete `app/crowdb-access-server/src/iceberg/` routing and
   `lib/crowdb-access-iceberg/src/rest/`. Use one generated-or-verified wire schema
   model tied to the backed-up OpenAPI; domain repositories never parse raw HTTP.
2. Advertise config, namespace CRUD/properties/exists, table list/create/load/update/
   drop/exists/rename, credentials, and metrics only when their requirements and
   runtime dependencies are enabled. Do not advertise register-table, views,
   transactions, or scan planning.
3. Extend R178's common identity/authentication boundary and R179's namespace
   decoding and pagination contracts to the complete surface. Implement decoding
   for prefix, multipart namespace, table identifier,
   pagination, idempotency key, data-access, snapshot-loading-mode, ETag, warehouse,
   and purge parameters. Enforce header, URI, query, JSON, and response bounds
   before allocating domain work.
4. Apply configured bearer/OAuth authentication before namespace or table lookup and
   authorize each catalog, namespace, table, management, credential, and file
   action separately. Only advertise the token endpoint if token issuance is
   configured and implemented. Error details never disclose a destination rename,
   location, credential, or existence to an unauthorized principal.
5. Map domain outcomes to the exact standard status and Iceberg error type. Preserve
   conflict categories needed for client retry; never turn unknown updates,
   unsupported operations, corruption, or expired authority into success.
6. Add a conformance harness that runs the Apache REST Compatibility Kit, official
   Java and Rust clients, and supported Spark, Flink, and Trino smoke profiles
   against one and multiple Access Servers with fault injection. Treat the backed-up
   specs as authority when test oracles disagree.
7. Publish an executable v1/v2/v3 capability matrix. Cover create/read/write and
   v1-to-v2/v2-to-v3 upgrades with version-specific fixtures, including row-level
   deletes, row lineage, deletion vectors, defaults, types, statistics, and format
   encodings required by the declared profile.
8. Add protocol metrics for endpoint, outcome class, latency, admitted bytes,
   response bytes, retry/conflict class, and selected format version without logging
   credentials, payloads, or unbounded identifiers.

## Dependencies

- Depends on R177 through R183. R184 is the integration gate for the core
  correctness milestone.
- R177 also permits an earlier foreground functional checkpoint before R183.
  Build on completed namespace acceptance and run REST/client integration with
  R180 through R182; retain reclamation-dependent gates as pending and do not
  close R184 at that checkpoint.
- Reuses the Access Server HTTP runtime and authentication infrastructure but keeps
  an independent listener, routes, admission budgets, metrics, and shutdown drain.
- R185 is deliberately not a dependency. Conformance must pass with caches disabled.
- Client/version combinations selected for release must be pinned in the test
  environment; oracle updates do not silently change the specification contract.

## Acceptance

- Given every enabled and disabled endpoint combination, when `/v1/config` is
  queried and each route is called, assert discovery lists exactly callable routes
  and unadvertised routes return unsupported without mutation. Invariant: REST-I1.
  E2E test.
- Given malformed identifiers, separators, tokens, headers, JSON unions, oversized
  bodies, deadlines, and cancellation at mutation crash points, when requests run,
  assert common errors are stable and durable operations are absent or recoverable.
  Invariants: REST-I2 and REST-I4. E2E test.
- Given requests without page tokens, empty tokens, UUIDv7 retry keys, terminal
  conflicts, response loss, and catalog clear, when official clients list and retry,
  assert complete unpaginated success, bounded resource errors, advertised key
  retention, and no replay of retired resources. Invariants: REST-I2 and REST-I3.
  E2E test.
- Given principals with catalog, namespace, table, file, management, and no access,
  when all route classes and rename hints are exercised, assert only authorized
  information and credentials are returned. Invariant: REST-I2. E2E test.
- Given the Apache compatibility kit and official Java and Rust clients, when the
  declared endpoint matrix runs against multiple servers with response loss, assert
  standard successes, conflicts, retries, pagination, and errors pass. Invariants:
  REST-I3 and REST-I5. E2E test.
- Given supported Spark, Flink, and Trino profiles, when each creates, evolves,
  writes, commits, loads, time-travels, reads row-level deletes, renames, expires,
  and drops tables, assert results agree across engines and remain valid after an
  Access Server restart. Invariant: REST-I5. E2E test.
- Given v1, v2, and v3 fixture matrices and valid upgrades, when differential tests
  run against reference implementations, assert metadata and visible rows agree;
  any oracle disagreement is resolved against the backed-up spec and recorded in
  the fixture. Invariant: REST-I3. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
