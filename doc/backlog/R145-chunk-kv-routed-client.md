<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R145: chunk-kv-client — Routed multi-partition KV client

## Problem

R143 exposes owner-fenced RPC operations on one chunk-KV partition, but direct
RPC calls are not an acceptable application interface. Every caller would need
to read the group-0 range map, maintain owner connections, preserve request
identity across retries, handle `NotMyRange`, and replan around split or
transfer. Implementing those rules independently would create inconsistent
retry and routing behavior.

The required application surface also includes multi-partition scan, multi-get,
and non-transactional batch mutation. No server owns all ranges, so these are
client-composed operations. Their result ordering, partial-success behavior,
continuation token, topology-change handling, and concurrency bounds must be
defined rather than hidden behind a method named `batch`.

Request identity cannot use an RPC port, socket, or connection ID: each is
ephemeral and may be reused. A stable namespace is also required before R144
can combine retained request results from two partition histories.

The root design links are R142's ordered journal and request-result semantics,
R143's range catalog, owner lease, RPC, and error contract, and
`doc/design/kv/design-crowdb-kv-group0.md` for group-0 discovery.

## Solution

Create `crowdb-chunk-kv-client` as the only supported routed client for the
R143 service. It composes partitions explicitly and provides no cross-partition
atomicity claim.

1. Write a permanent client design under `doc/design/kv/` defining range-map
   caching, owner connection management, request identity, point routing,
   retries, multi-get, batch mutation, multi-partition scan, continuation, and
   consistency limits.
2. Add `lib/crowdb-chunk-kv-client` on `crowdb-rpc`, `crowdb-protocol`, and the
   generic group-0 client. Load the latest immutable R143 catalog generation,
   validate complete non-overlapping coverage of the binary keyspace, and
   publish the decoded ordered map through an immutable cache. Subscribe to the
   catalog head and owner endpoints, with bounded periodic refresh as a missed-
   notification safety net. A cold client cannot route without group 0; a client
   with a valid cached map may continue attempting current owners while it
   refreshes, subject to the owners' serving leases.
3. Maintain a bounded connection pool by owner instance and endpoint. Point and
   conditional calls binary-search the cached range map and send directly to
   the owner; they never use a chunk-KV server as a forwarding proxy. On
   `NotMyRange`, stale epoch, or connection loss, invalidate the affected route,
   refresh the catalog, and retry within one total deadline.
4. Give each client handle a random 128-bit `client_instance_id` from the OS
   random source and an atomic nonzero `u64 client_sequence` starting at 1.
   Allocate the sequence exactly once when constructing a logical mutation and
   preserve the pair across routing and transport retries. Reconnection does not
   change the client ID. A new process or new default handle gets a new random
   ID, so its sequence may restart at 1. Applications that must retry an
   unresolved request after their own process restart persist and resubmit the
   complete request identity and operation. Before sequence overflow, stop new
   mutations, drain outstanding requests, and rotate to a new random client ID.
5. Wrap every R143 single-partition operation: get, put, delete, put-if-absent,
   compare-exchange, conditional delete, ceiling/higher/floor/lower, and bounded
   forward/reverse scan. Preserve typed errors, applied revisions, journal
   positions, condition results, and `min_journal_position`; do not reduce them
   to a boolean or generic transport failure.
6. Add an internal per-partition multi-get RPC and implement `multi_get(keys)`
   by retaining each input index, grouping keys by current partition and owner,
   issuing bounded parallel partition RPCs, and restoring results to input
   order. The server rejects a group containing any out-of-range key before
   reading it. Return one result per input key so one unavailable partition does
   not erase successful results from other partitions. Multi-get is not a
   global snapshot; each group observes its partition's applied view.
7. Add an internal per-partition batch-mutation RPC and implement the public
   non-transactional batch by assigning one durable request identity per input
   operation, grouping operations by partition, preserving their relative order
   within each partition request, and dispatching groups with bounded
   parallelism. The server rejects a cross-range group before WAL admission.
   Return per-operation results in input order. Successful groups remain
   committed if another group fails; retry only transient failed or unknown
   operations with their original identities. Conditions in one partition
   follow its R142 sequencer order, while no order or atomic commit is promised
   across partitions.
8. Implement multi-partition forward and reverse scan by intersecting the
   requested binary interval with one pinned catalog generation and building an
   ordered query plan. Consume partitions in key order, optionally prefetching a
   bounded number, and enforce one global item/byte limit. The continuation
   token carries direction, original bounds, last emitted key, and observed
   catalog generation. If split, transfer, or `NotMyRange` invalidates the plan,
   refresh and replan only the remaining interval strictly after the last key
   for forward scan or strictly before it for reverse scan. This prevents
   duplicates and gaps caused by topology replacement, but it is not a global
   snapshot: concurrent mutations on already consumed keys need not appear.
9. Treat empty multi-get/batch/scan as a successful empty result. Preserve
   duplicate multi-get keys and their input positions. Reject malformed bounds,
   oversized batches, and a reused request identity with a different operation
   before retry. Prefix listing is expressed as a binary bounded scan; TTL,
   watches, range delete, and atomic multi-key transactions remain out of scope.
10. Apply one total operation deadline across discovery, connection, refresh,
    RPC, and backoff. Retry only typed transient outcomes and cap route refresh,
    in-flight partition groups, buffered scan pages, response bytes, and owner
    connections. Cancellation before server admission creates no record;
    cancellation after admission leaves the request identity available for
    explicit result recovery.
11. Put all real-process E2E coverage for the first R140-R143 release in this
    requirement: point and ordered operations, response loss, server restart,
    owner death, automatic balance transfer, split, stale catalog, group-0
    interruption, multi-get, batch partial success, and forward/reverse
    multi-partition scan. R143 retains unit and integration coverage for server
    and control-plane components; deferred R144 owns its later merge-specific
    E2E cases.

## Dependencies

- Depends on R143 for the group-0 catalog, owner endpoints/epochs, serving
  leases, single-partition RPC protocol, typed errors, scan tokens, and
  `EnsureDomainMonitor` startup behavior.
- Depends on R142 for request digest/result retention, journal positions,
  conditional ordering, range enforcement, split result transfer, and
  read-after-write semantics.
- Uses `crowdb-rpc`, `crowdb-protocol`, and the existing generic group-0 client;
  it does not depend on `crowdb-kv`, R140's C++ backend, or raw R141 streams.
- R144 consumes the request-identity contract when combining two parent result
  windows, but R145 does not depend on merge.

## Acceptance

- Given a valid complete binary range map, when the client loads it, assert
  every binary key maps to exactly one partition; given a hole, overlap, or
  invalid order, assert the generation is rejected. Invariant: client routing
  never operates on an ambiguous or incomplete keyspace. Unit test.
- Given owner endpoints and repeated point, conditional, seek, and bounded scan
  calls, when the client routes them, assert each goes directly to the current
  owner and preserves every typed R143 result field. Invariant: the wrapper does
  not weaken the server operation contract. Integration test.
- Given a connection rebuild, owner redirect, and transient retry, when one
  logical mutation completes, assert its 128-bit client ID and sequence stay
  unchanged; given a new default client process, assert a new random client ID
  allows its sequence to restart at 1. Invariant: request identity is independent
  of RPC ports and connections. Unit test.
- Given an application persists an unresolved request identity and operation,
  when a new process resubmits them after the original response was lost, assert
  it receives the recorded result; change the operation under that identity and
  assert `RequestConflict`. Invariant: explicit cross-process retry is durable
  and cannot alias another mutation. E2E test.
- Given duplicate and cross-partition keys in `multi_get`, when one partition is
  unavailable, assert every input position has its matching value/not-found or
  typed error and successful partitions remain present. Invariant: multi-get
  preserves input cardinality and exposes partial failure. Integration test.
- Given a malformed internal multi-get or batch RPC contains a key outside its
  declared partition, when the owner validates it, assert the whole group is
  rejected before read execution or WAL admission. Invariant: client grouping
  cannot bypass the server's range fence. Integration test.
- Given a batch spans partitions and one group has an ambiguous response, when
  the client returns and retries, assert successful operations remain committed,
  results retain input order, and only unresolved operations retry with their
  original identities. Invariant: non-transactional batch exposes partial
  success without duplicating mutations. E2E test.
- Given conditional operations for one partition occur in a batch, when the
  server executes them, assert their relative input order is preserved; given
  groups on different partitions, assert the API makes no cross-group ordering
  or atomicity claim. Invariant: client batching does not invent distributed
  transaction semantics. Integration test.
- Given a forward scan spans several partitions, when the client applies a
  global limit, assert results are globally bytewise ordered and the token
  resumes strictly after the last emitted key. Repeat in reverse and assert
  strict descending order and resume-before behavior. Invariant: composed scan
  order and continuation are directionally exact. E2E test.
- Given a split or owner transfer invalidates a scan plan after some results are
  emitted, when the client refreshes and replans the remaining interval, assert
  no already emitted key repeats and every stable unconsumed key is returned.
  Invariant: topology change does not create pagination duplicates or stable-key
  gaps. E2E test.
- Given group 0 is unavailable, when a cold client and a client with a validated
  cached map issue operations, assert the cold client fails discovery and the
  warm client may attempt only cached owners until its deadline; neither invents
  an owner. Invariant: loss of catalog access cannot manufacture routing
  authority. Integration test.
- Given empty inputs, duplicate keys, malformed bounds, oversized requests,
  client-sequence exhaustion, and cancellation before or after admission, when
  the client handles them, assert each follows the declared result, rotation,
  and recovery behavior with bounded memory. Invariant: edge inputs cannot
  bypass admission or request identity. Unit test.
- Given multiple owners under high fan-out, when multi-get, batch, and scan run,
  assert configured connection, in-flight group, buffered page, byte, and total
  deadline limits hold. Invariant: client-side composition is asynchronously
  bounded. Integration test.
- Given three chunk-KV servers, sufficient data, and the default ratio four,
  when automatic splitting and balancing converge, assert the client observes
  12 non-empty full-keyspace partitions, owner counts differ by at most one, and
  all point and scan operations remain routable. Invariant: the configured
  partition-to-owner ratio is realized through the public client path. E2E test.
- Given an owner process dies, when its heartbeat is absent for 10 seconds,
  assert recovery action starts, the client observes only transient typed
  failures, no replacement serves before the old lease fence, and operations
  resume on a recovered owner without data migration. Invariant: automatic
  owner healing is client-visible and never creates overlapping authority.
  E2E test.
- Given group 0 becomes unavailable while data owners continue running, when
  serving grants reach their conservative deadlines, assert the client cannot
  obtain an acknowledged mutation from an expired owner; after group 0 returns,
  assert monitor and routing recovery resume. Invariant: control-plane loss
  fails closed through the complete client/server path. E2E test.
- Given one server gracefully drains and later restarts with assigned
  partitions, when the client maintains traffic, assert requests redirect during
  drain, the restarted partitions remain `Prepared` through replay, and routing
  resumes only after new serving grants. Invariant: process lifecycle is hidden
  by bounded client retry without bypassing readiness. E2E test.
- Given an object name and metadata value, when the routed client puts, gets,
  conditionally updates, lists by binary prefix, survives server restart and
  owner death, and continues across automatic split/balance, assert every
  acknowledged result remains recoverable without page or WAL data migration.
  Invariant: the complete object-metadata client path preserves durability,
  routing, and fencing. E2E test.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-server --all-targets`
- `pixi run clean-env && pixi run test-server`
