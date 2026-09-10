<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R143: chunk-kv-server — Range-partitioned KV service

## Problem

R142 is an embeddable library. It does not provide a process that clients can
discover, a network protocol, cross-partition routing, group-0 ownership
records, owner leases/epochs, startup recovery, health reporting, or operator
lifecycle. Folding those concerns into the library would make it harder to
reuse and would repeat the coupling between `crowdb-kv` and
`crowdb-kv-server`.

The service must support an object-store metadata scenario: a client writes
metadata keyed by object name and later reads it back. It must also scale by
assigning many independent key ranges to one node, moving authority without
moving chunk data, and splitting hot or large ranges. A stale routing cache or
old owner must produce a redirect/fence outcome rather than lost or divergent
writes.

The current binding-monitor wiring is not a sufficient precedent as-is.
`crowdb-kv-server` explicitly starts a chunkdb-specific monitor, while chunkdb
only registers a heartbeat. Diskdb monitoring is planned as another explicit
wiring path. This makes the generic KV server choose which higher-level
services exist, does not supervise a lost monitor task, and has no persisted
request from a service domain saying that its health and balance must be
managed. R143 needs an on-demand group-0 monitor supervisor before partition
owner death can be repaired reliably. The current generic monitor also converts
an existing-binding read failure to an empty table before computing placement;
a control-plane read error must instead abort the tick so it cannot erase
transition context or manufacture a new initial assignment.

The initial RPC list also omits several semantics already required from R142:
conditional mutation, ordered seek, reverse scan, request deduplication,
read-after-write positions, overload, and retry after an ambiguous response.
Without an end-to-end operation contract, the server could preserve simple
put/get while losing the library's ordering or exactly-once result behavior at
the network boundary.

The control-plane precedent is
`doc/design/kv/design-crowdb-kv-group0.md`; server lifecycle precedent is
`doc/design/kv/design-crowdb-kv-server.md`; shared service configuration is
`doc/design/config/design-crowdb-config.md`.

## Solution

Create `crowdb-chunk-kv-server` as the standalone serving process, router, and
network surface for `crowdb-chunk-kv` partitions. Group 0 is the durable
control-plane authority; the server owns partition lifecycle tasks but never a
raw tree or stream handle.

1. Write a permanent server design defining group-0 schema, range-map revision,
   non-overlap validation, assignment policy, ownership epoch/lease, split and
   transfer state machines, request routing, stale-cache redirects, bootstrap,
   recovery, and failure handling. The range map always covers the complete
   binary keyspace using crowdb-tree's unsigned bytewise ordering; holes and
   `UnassignedRange` are not supported. Its first inclusive bound is the empty
   byte string, adjacent bounds are identical, and its final end is unbounded.
2. Add `app/crowdb-chunk-kv-server` with shared config, structured logging,
   graceful shutdown, metrics, health, and management endpoints following
   existing server conventions. One process hosts zero or more partitions.
3. Add `crowdb-rpc` protocol types in `crowdb-protocol` plus server handlers for
   get, put, delete, put-if-absent, compare-exchange, conditional delete,
   ceiling/higher/floor/lower, and bounded forward/reverse scan. Carry a durable
   request identity `(client_instance_id, client_sequence)`, range-map revision,
   partition identity, ownership epoch, and optional `min_journal_position`;
   return the applied revision and R141 journal position where R142 defines
   them. Encode `client_instance_id` as two `u64` values generated randomly per
   client handle and `client_sequence` as a nonzero monotonically increasing
   `u64`. RPC ports, socket addresses, and connection IDs never participate in
   identity. Keep multi-key transactions out of scope; R145 owns the routed
   client and composed operations.
4. Store the authoritative catalog in group 0 as immutable ordered range-map
   pages plus one small generation head. Pages contain each partition's
   identity, exact bounds, current owner, owner epoch, lifecycle state, artifact
   references, and transition ID; a new generation reuses unchanged pages. The
   single group-0 monitor sequencer writes and validates all changed pages
   before publishing its head. It never retries an ambiguous old head write
   blindly: it rereads the head and either proves that transition committed or
   derives a later generation. This supplies atomic split/transfer visibility
   despite group 0 offering blind puts rather than a multi-key transaction,
   without rewriting the whole map for one owner change. Always reject overlaps,
   holes, invalid bounds, stale revisions, and epoch regression, and validate
   complete binary-keyspace coverage. Retain catalog generations while a
   transition or declared client-token grace period pins them, then reclaim only
   pages unreachable from every retained head.
5. Register each R141 `stream_name` with its bound metadata KV group. Stream
   registration is discovery/configuration metadata; active extent maps and
   tails never live in group 0. Partition activation authorizes use of its
   referenced stream, so split does not require atomically updating independent
   stream registry entries with the range-map head.
6. Generalize the existing group-0 binding-monitor wiring into a supervised,
   leader-gated domain-monitor runtime. Add an idempotent
   `EnsureDomainMonitor` control-plane request and a persisted monitor descriptor
   containing the service-registry name, strategy/capability version, health
   timeouts, and balance policy reference. `crowdb-chunk-kv-server`,
   `crowdb-chunkdb`, and `crowdb-diskdb` call it after establishing their
   group-0 client and before declaring ready. Every group-0 replica observes the
   descriptors and prepares a monitor task; only the current group-0 leader may
   publish catalog or binding changes. A supervisor restarts a failed task, and
   a new group-0 leader reconstructs it from the descriptor. Repeated identical
   requests are no-ops; a conflicting strategy version fails closed for
   operator resolution. Descriptors select a compiled, versioned monitor driver;
   they never deliver executable code. A group-0 server that does not support
   the requested driver returns `UnsupportedMonitorDomain`, and the requesting
   service remains unready. Any service-registry, catalog, binding, or transition
   read failure aborts that monitor tick; no failure is converted to an empty
   control-plane snapshot.
7. Keep domain policy and schemas in `crowdb-protocol` and
   `crowdb-kv-client`, behind a common monitor-driver boundary. The
   `crowdb-kv-server` executable hosts the generic supervisor and group-0 leader
   fence but does not depend on `crowdb-chunk-kv`, chunkdb, or diskdb libraries.
   A domain descriptor declares whether dead-owner reassignment and balancing
   are automatic shared-storage operations or warning/operator-only operations;
   this preserves diskdb's stricter migration policy.
8. Register and heartbeat chunk-KV instances in group 0 with endpoint, capacity,
   load, hosted partition IDs/epochs, recovery state, and health. The monitor
   classifies an owner as healthy, suspect, or dead using bounded heartbeat
   expiry rather than one missed tick. Extend `ServiceRegistryClient` with a raw
   observation scan that retains expired entries and timestamps for the monitor;
   ordinary discovery continues to return only TTL-live instances. Keep leases
   separate from the range-map catalog: issue one aggregated serving grant per
   instance, with a monotonic lease sequence, expiry, catalog generation, and
   digest of the exact
   `(partition_id, owner_epoch)` assignments it covers. Lease renewal therefore
   scales with owner count rather than partition count and does not publish a
   new range map. Monitor-issued serving grants, not instance-written
   heartbeats, authorize requests. An owner self-fences affected partitions
   before expiry if it cannot obtain a newer matching grant. A new owner is not
   activated until the prior grant has expired or an explicit old-owner fence
   is proven, and its R142 partition reports `Prepared` at the catalog's epoch
   and durable tail. Default to a 2-second heartbeat/renew interval, `Suspect`
   after 6 seconds, `Dead` with recovery action starting after 10 seconds, a
   12-second serving lease, a 1-second maximum clock-skew budget, and a 1-second
   self-fence margin. Expose every value as configuration. The owner uses the
   earlier of the grant's wall-clock deadline minus both margins and a local
   monotonic deadline calculated from that safe remaining duration when the
   grant is received. The monitor never activates a replacement before the old
   grant's deadline plus the skew budget unless explicit fencing is proven.
9. On owner death, persist one idempotent transfer transition, choose a healthy
   target, advance the partition epoch, wait for the old grant fence, recover
   the existing manifest and stream on the target, and publish a new range-map
   generation only after readiness proof. On a graceful or balance transfer,
   fence and drain the old owner first so cutover need not wait for lease expiry.
   Chunk-KV servers subscribe to their domain's catalog and transition prefixes,
   with periodic refresh as a safety net: the selected target observes the
   persisted plan, invokes idempotent R142 prepare, and reports proof; the old
   owner observes the same plan and fences. The group-0 monitor does not load a
   service library or depend on an in-memory callback. Never bulk-copy page or
   WAL chunks between servers. If no target is healthy, leave the range
   unavailable and its durable artifacts pinned rather than assigning an unsafe
   owner.
10. Enable automatic chunk-KV balancing. Configure
    `target_partitions_per_owner`, default 4, so three healthy owners target 12
    partitions once enough data exists to form useful children. The desired
    count is at least `live_owner_count * target_partitions_per_owner` and may be
    higher when a partition exceeds `target_partition_bytes`. Split the largest
    eligible partition and choose the split key near its live-byte median, not
    the lexical midpoint; do not create empty children merely to reach the
    count. Placement first minimizes partition-count difference, keeping it at
    most one where feasible, then uses durable bytes as the secondary weight.
    Request rate and resource headroom are safety constraints, not initial score
    terms. Preserve the current owner unless a move fixes a count imbalance or
    improves weighted load by at least 25%; default to a 10-minute per-partition
    cooldown and at most one concurrent transfer per source or target. All
    values are configurable. Automatic moves use the persisted transfer state
    machine and never edit an owner record directly.
11. Route each point or conditional request to exactly one current partition.
    R145 sends data RPCs directly to the mapped owner; a contacted server never
    proxies them to another owner. A stale or fenced server returns a structured
    `NotMyRange` containing the latest known map revision and owner hint without
    appending WAL. The protocol preserves `request_id` across an explicit
    reroute. `Overloaded`, `WriteStalled`, `Recovering`, `LeaseExpired`,
    `RequestExpired`, and `RequestConflict` remain distinct outcomes rather than
    becoming generic transport errors.
12. For scans, validate and clip the interval before execution. A
    single-partition continuation token binds direction, last key, partition
    identity, owner epoch, and map revision; a split or transfer invalidates it
    with a refresh-required result rather than silently skipping or duplicating
    keys. R145 composes these server primitives into required multi-partition
    scans.
13. On startup, ensure the domain monitor, register the instance, load and
    validate the current catalog head, open assigned partitions as `Prepared`,
    replay each through its durable tail, report readiness, and serve only after
    receiving a current lease. On shutdown, stop admission, drain bounded
    in-flight work, checkpoint where possible, report draining, and explicitly
    relinquish grants; a crash follows the expiry and reassignment path.
14. Provide server-side integration fixtures for object names as keys and
    chunk-object locations/attributes as values. R145 owns real-process E2E
    coverage for the complete client/server system. Keep S3 HTTP semantics,
    authentication, and object payload I/O outside these requirements; this
    server is the metadata KV layer.

Operation outcomes are closed as follows:

- A get linearizes on R142's applied prefix. It may return the old value while
  a concurrent mutation is journal-pending; a returned journal position can be
  supplied as `min_journal_position` for read-after-write.
- Put, delete, and successful conditional mutations are acknowledged only after
  WAL durability and tree apply. A lost response is retried with the same
  `request_id` and returns the recorded result.
- A condition failure is itself journaled and advances the applied frontier as
  a no-op, so retry or recovery never re-evaluates it against a newer value.
- Ordered seek and scan execute inside one R142 partition view. The server does
  not emulate ceiling/floor or reverse order by issuing multiple point calls.
- Transport loss, stale routing, and owner redirection consume one bounded
  client retry budget. A retry never changes the original request ID or
  ownership epoch silently.
- Cancellation or deadline expiry before sequencer admission returns without a
  WAL record. After admission, the server may drop the response but completes
  the operation to one durable result retrievable by the same `request_id`; it
  never attempts to cancel a possibly durable append.
- Journal uncertainty stalls only writes for that partition; healthy applied
  reads continue. Tree corruption recovers only that partition. Group-0 loss
  permits an already leased owner to serve only until its local safety deadline
  and never permits a new assignment.

## Dependencies

- Depends on R142 for partition storage, recovery, split preparation, and epoch
  enforcement. R142 owns each active R141 stream handle; R143 owns only the
  group-0 assignment, lease, and epoch that authorize R142 to open it.
- Depends on R141's group-0 registry schema. R143 selects the metadata KV group
  and writes/reconciles bindings, but mutable stream manifests and extent pages
  are stored in that group through R142/R141 rather than by the server.
- Depends on group-0 sysdata, service registry, and KV client APIs. R139 is not
  required for the first version; file-backed config is the fallback until
  distributed service configuration lands.
- Extends the current `crowdb-kv-client::BindingMonitor` and
  `app/crowdb-kv-server/src/binding_monitor_wiring.rs` into the persisted
  on-demand supervisor. R102 and R103 retain their domain-specific migration
  policies, but diskdb and chunkdb startup adopt the same
  `EnsureDomainMonitor` registration contract.
- Depends on `crowdb-rpc` and the CROWDB protocol key/type conventions.
- Object payload writers/readers consume this metadata service but are not a
  prerequisite. Full S3-compatible API behavior is a separate future backlog.
- R145 consumes the server RPC and catalog contracts for routed and composed
  operations and owns real-process E2E coverage; it is an outgoing requirement,
  not an R143 prerequisite.

## Acceptance

- Given three servers and more than three partitions, when assignments load
  from group 0, assert at least one server hosts multiple ranges and every
  published range has exactly one current owner. Invariant: partition count is
  decoupled from node count. Integration test.
- Given a complete binary range map and maps containing a hole, overlap,
  reversed bound, or duplicate range, when a server loads each revision, assert
  only the complete map activates and every invalid revision leaves the last
  valid map in place. Invariant: routing always covers each binary key exactly
  once. Unit test.
- Given one owner transfer changes one catalog page, when the group-0 monitor
  publishes it, assert unchanged pages are reused and the head changes only
  after every referenced page is readable and valid. Inject an ambiguous head
  response and assert the monitor rereads it rather than replaying an older head
  put. Invariant: readers observe one complete monotonic catalog generation.
  Integration test.
- Given a stream is registered to metadata group 7, when the server activates
  its partition, assert group 0 contains only the binding and ownership record,
  group 7 contains stream manifests/extents, and append traffic creates no
  group-0 writes. Invariant: group 0 discovers stream metadata without carrying
  its hot state. Integration test.
- Given an object name and metadata value, when a direct RPC harness puts then
  gets through one owner and restarts that server, assert the exact metadata is
  recovered. Invariant: the server primitive makes acknowledged object metadata
  durable and addressable by name. Integration test.
- Given keys inside one partition, when a direct RPC harness performs put, get,
  delete, put-if-absent, compare-exchange, conditional delete,
  ceiling/higher/floor/lower, and bounded forward/reverse scan, assert the
  results, revisions, and order match the R142 view. Invariant: the network
  surface preserves every first-release library operation. Integration test.
- Given a mutation is journal-pending, when an ordinary get and a get carrying
  its eventual journal position execute, assert the ordinary get may return the
  prior applied value while the constrained get waits and returns the applied
  value. Invariant: the RPC layer preserves R142 visibility and read-after-write
  semantics. Integration test.
- Given a successful or condition-failed response is dropped, when a direct RPC
  harness resubmits the same request identity after an intervening mutation,
  assert the server returns the original journaled result without evaluating
  the condition again. Invariant: repeated RPC delivery cannot change a durable
  logical outcome. Integration test.
- Given a direct RPC harness reuses a request ID with a different operation
  digest, when the server handles it, assert `RequestConflict` is returned
  unchanged without another WAL append. Invariant: the RPC layer cannot alias
  two mutations to one deduplication identity. Integration test.
- Given an RPC deadline expires before sequencer admission, assert no WAL record
  exists; given cancellation occurs after admission, assert the server completes
  one durable outcome and a repeated call by request ID retrieves it.
  Invariant: cancellation cannot create an unknowable or partially aborted
  mutation. Integration test.
- Given a direct RPC call with a stale range-map revision contacts the former
  owner, assert `NotMyRange` supplies a usable revision/owner hint and one
  response causes no server proxy or old-owner WAL append. Invariant: stale
  routing cannot become a write on the wrong partition. Integration test.
- Given the old owner is isolated after an ownership epoch advances, when its
  lease expires and it receives a write, assert the request is rejected before
  WAL append. Invariant: at most one live owner may acknowledge writes for a
  partition epoch. Integration test.
- Given a chunk-KV owner misses one heartbeat but returns before the suspect
  deadline, when the domain monitor ticks, assert no epoch or owner changes;
  given its monitor-issued lease expires, assert it self-fences and a new owner
  activates only after the fence boundary. Invariant: failure detection avoids
  one-miss churn without permitting overlapping grants. Integration test.
- Given the default timing policy and a stopped owner heartbeat, when a fake
  clock advances through 6, 10, 11, 12, and 13 seconds, assert the monitor marks
  `Suspect` at 6, starts recovery at 10, the owner reaches its conservative
  self-fence deadline by 10, and replacement activation waits through the
  12-second grant plus 1-second skew budget unless explicit fencing is proven.
  Invariant: the 10-second detection target never overlaps serving authority.
  Unit test.
- Given one server owns many partitions, when its serving lease renews, assert
  group 0 writes one grant carrying the current catalog generation and exact
  assignment digest rather than one record per partition; change one partition
  epoch and assert the old grant no longer authorizes it. Invariant: lease
  renewal is bounded by owner count without weakening the per-partition fence.
  Integration test.
- Given the selected replacement cannot open the manifest or replay the durable
  stream tail, when failover preparation reports failure, assert the catalog
  retains no serving grant for that target, retries another healthy target if
  available, and otherwise leaves the partition unavailable with artifacts
  pinned. Invariant: availability pressure cannot publish an unrecovered owner.
  Integration test.
- Given a partition transfer, when the new owner enters serving state, assert it
  recovers all acknowledged keys from existing chunks and instrumentation shows
  no page- or WAL-chunk migration and no direct server-to-stream operation.
  Invariant: transfer moves authority while partition storage identities remain
  stable. Integration test.
- Given a partition split at `m` with concurrent client traffic, when the
  transition completes, assert every acknowledged key is returned from exactly
  one child and stale parent requests redirect. Invariant: range-map cutover
  exposes either the parent or validated children, never a gap or overlap.
  Integration test.
- Given the control-plane monitor restarts during transfer or split, when a new
  leader reads group 0, assert it resumes or safely aborts the persisted
  transition without issuing a lower epoch. Invariant: control-plane failover
  preserves monotonic ownership. Integration test.
- Given chunk-KV, chunkdb, and diskdb start against group 0, when each sends
  `EnsureDomainMonitor`, assert one persisted descriptor per domain exists,
  identical retries create no additional task, and every group-0 replica
  prepares the task while only the leader publishes changes. Invariant: service
  startup requests monitoring without hard-coded startup wiring in kv-server.
  Integration test.
- Given a domain monitor task exits unexpectedly, when its supervisor observes
  the exit, assert it restarts with backoff from persisted state; given a group-0
  leader change, assert the new leader resumes outstanding transitions without
  waiting for another service startup request. Invariant: monitor availability
  is independent of one task or requester lifetime. Integration test.
- Given two startup requests use the same domain name but conflicting strategy
  versions, when group 0 validates the second request, assert it fails closed
  and the existing monitor keeps its prior descriptor. Invariant: a rolling
  deployment cannot silently change ownership semantics. Integration test.
- Given reading the current catalog, binding table, or transition state fails,
  when a monitor tick runs, assert it publishes no assignment and retries the
  read with backoff; it never substitutes an empty collection. Invariant: an
  observation failure cannot be interpreted as an empty cluster or clean
  control plane. Integration test.
- Given three healthy owners, sufficient data, and
  `target_partitions_per_owner = 4`, when automatic split and placement
  converge, assert 12 non-empty partitions cover the full keyspace, split keys
  approximate live-byte medians, and owner counts differ by at most one.
  Invariant: partition count is primary and partition size is the secondary
  balancing signal. Integration test.
- Given a proposed transfer neither fixes a count imbalance nor improves
  weighted bytes by 25%, or the partition remains inside its 10-minute
  cooldown, when the monitor evaluates it, assert the current owner remains;
  given a qualifying move, assert it executes through the normal fenced
  transfer with per-owner concurrency one. Invariant: automatic balance avoids
  owner flipping and never bypasses ownership fencing. Integration test.
- Given group 0 is unavailable, when a lease approaches expiry, assert the
  server follows the designed drain/reject behavior and does not self-assign a
  partition. Invariant: control-plane loss cannot create ownership. Integration
  test.
- Given R142 returns `Overloaded`, `WriteStalled`, `Recovering`,
  `LeaseExpired`, `RequestExpired`, `RequestConflict`, or `NotMyRange`, when the
  server encodes the RPC response, assert the exact typed outcome and retry hint
  survive round-trip decoding. Invariant: server error translation cannot hide
  ownership, recovery, expiry, or identity state. Unit test.
- Given a scan continuation token and a later owner transfer or split, when a
  direct RPC call resumes the scan, assert the stale token returns
  refresh-required and never silently skips or duplicates a key. Invariant:
  topology change is explicit at the pagination boundary. Integration test.
- Given disjoint hosted ranges, when checkpoint workers dump them in parallel,
  assert configured concurrency and memory limits hold and all manifests
  recover. Invariant: parallel subtree dump is bounded and independent.
  Integration test.
- Given shutdown with in-flight requests, when graceful shutdown runs, assert
  admission closes, acknowledged operations remain recoverable, leases are
  relinquished or allowed to expire, and no partial transition is published.
  Invariant: process lifecycle preserves durable ownership state. Integration
  test.
- Given a server hosting healthy, recovering, and draining partitions, when its
  health, metrics, and group-0 heartbeat are read, assert they report the same
  partition identities, epochs, and lifecycle states. Invariant: every serving
  decision has consistent operator-visible state. Integration test.
- Given a server starts with assigned partitions, when it registers but has not
  finished manifest validation and journal replay, assert it remains
  `Prepared`; after readiness proof and a current serving lease, assert it
  accepts requests. Invariant: service discovery alone cannot activate an
  unrecovered owner. Integration test.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo test -p crowdb-kv-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-server --all-targets`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo test -p crowdb-diskdb --all-targets`
- `pixi run clean-env && pixi run test-server`
