<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Server

`crowdb-chunk-kv-server` is the standalone ownership, routing, and network
boundary for chunk-backed metadata partitions. It hosts zero or many R142
partition handles but never owns or bypasses a raw tree or stream. Group 0 is
the durable catalog and monitor authority; monitor-issued serving grants are
the only authority to admit data requests.

## 1. Catalog Publication

The catalog is one checksummed generation head over ordered immutable pages.
Each entry contains an exact half-open binary-key range, stable partition ID,
owner endpoint, monotonic owner epoch, lifecycle state, tree manifest, stream
name, applied sequence, and optional transition ID. A valid generation starts
at the empty byte string, has exact adjacent bounds, ends unbounded, and covers
each binary key once.

Publishers validate the complete successor, including retained-partition epoch
non-regression, before I/O. They write only new pages, reread and validate every
referenced page, and write the head last. An ambiguous head write succeeds only
when rereading proves the exact intended head. Readers retain their last valid
immutable snapshot when a new head or page is absent, corrupt, incomplete, or
regressing. Unchanged pages may be referenced by later heads through their
original page generation.

## 2. Domain Monitors

`EnsureDomainMonitor` carries a persisted descriptor with domain and registry
names, compiled driver and capability versions, failure policy, balance policy,
and all health/lease timing. Identical registration is idempotent. Any field
conflict fails closed, and an unsupported compiled driver returns
`UnsupportedMonitorDomain` without changing the prior descriptor.

Every group-0 replica prepares tasks from persisted descriptors. A staged tick
may observe, plan, and publish only under the current group-0 leader fence.
Registry, catalog, binding, or transition observation failure ends the tick
before planning; it is never represented as an empty cluster. A supervisor
restarts failed tasks from durable state and a new leader resumes outstanding
transitions.

Normal service discovery filters expired registrations. Monitor observation
uses the raw scan, preserving expired entries and heartbeat timestamps so
absence cannot be confused with owner death. The default heartbeat is two
seconds, `Suspect` begins at six seconds, and `Dead` recovery begins at ten.

## 3. Serving Authority

The monitor issues one aggregate grant per instance. It contains a monotonic
lease sequence, catalog generation, wall-clock expiry, sorted exact
`(partition_id, owner_epoch)` assignments, and their canonical digest. An
instance heartbeat advertises health but cannot authorize a request.

On receipt, an owner subtracts maximum clock skew and its self-fence margin
from the wall-clock remaining duration, then projects that duration onto its
local monotonic clock. Request admission reads an immutable grant snapshot and
checks its local deadline, catalog generation, partition identity, and epoch
without taking a lock. The default 12-second lease, one-second skew, and
one-second self-fence margin fence the old owner by ten seconds. A replacement
waits until the old wall-clock expiry plus skew unless an explicit source fence
is durably proven.

## 4. Request Contract

Every request carries a random 128-bit client instance ID, nonzero monotonic
client sequence, catalog revision, partition ID, owner epoch, optional minimum
journal position, and optional deadline. The logical request identity survives
transport retry and rerouting. Endpoint, socket, and connection identities do
not participate in deduplication.

The contacted server validates deadline, catalog route, owner, grant, and local
partition before R142 admission. It never proxies. A stale route returns
`NotMyRange` with the current revision and owner hint without WAL I/O. Point
operations preserve get, put, delete, put-if-absent, compare-exchange, and
conditional-delete conditions and results. Successful and failed conditions
return the R142 journal position. `Overloaded`, `WriteStalled`, `Recovering`,
`LeaseExpired`, `RequestExpired`, `RequestConflict`, `NotMyRange`, and
`RefreshRequired` remain distinct wire outcomes.

A deadline observed before sequencer admission creates no WAL record. Once the
sequencer accepts a mutation, dropping the transport response does not cancel
the partition worker; retrying the same identity retrieves the recorded result.
Reads may carry a mutation's returned journal position for explicit
read-after-write ordering.

## 5. Ordered Reads

Seek provides ceiling, higher, floor, and lower operations within one partition
view. Scan intervals are validated and clipped to the routed half-open range
before execution. A continuation binds direction, last key, partition ID,
owner epoch, and catalog revision. Any split, transfer, revision, epoch, or
direction mismatch returns `RefreshRequired`; the server never guesses a resume
position. Multi-partition composition belongs to the routed client.

## 6. Transfer and Balance

A transfer is a durable idempotent record containing source and target owners,
strictly advancing epoch, exact range and artifact, old grant deadline, phase,
authority-release proof, and target-readiness proof. Graceful transfer obtains
an explicit fence and durable tail. Dead-owner transfer waits through lease
expiry plus skew. Only then may the target reopen the same manifest and stream,
recover through the durable tail as `Prepared`, and report an exact proof. The
catalog cutover follows that proof. Failure retains the artifact and leaves the
partition unavailable; transfer never copies page or WAL chunks.

Balancing targets at least `live_owner_count * target_partitions_per_owner`,
defaulting to four partitions per owner. Split chooses the largest eligible
partition and a key near the cumulative live-byte median, never an empty child.
Placement minimizes partition-count difference first, then durable-byte spread.
A move must repair count imbalance or improve weighted spread by at least 25%.
Request rate and target headroom are safety filters. The default per-partition
cooldown is ten minutes and an owner participates in at most one transfer at a
time.

## 7. Lifecycle and Observability

Startup ensures the monitor, registers the instance, loads a complete catalog,
opens assignments as `Prepared`, validates manifests, replays durable tails,
reports readiness, and serves only after installing a matching grant. Shutdown
atomically stops new admission and clears authority; already admitted R142
operations retain handles and finish before bounded checkpoint/drain work.

Configuration exposes identity, group-0 seeds, dedicated 15xxx HTTP/RPC ports,
hosted-partition capacity, refresh/drain intervals, timing policy, and balance
policy. Validation rejects unsafe timing, zero bounds, bad addresses, and empty
discovery seeds. Lock-free counters distinguish successes, redirects, lease and
deadline rejections, overload, and internal errors. Health and heartbeat views
derive from the same sorted partition snapshots and catalog generation.

## Open Issues

- R142 needs production native tree construction, prepared-child activation,
  and directional cursor operations before real seek/scan and restart coverage.
- Group-0 adapters remain for atomic monitor descriptor/transition storage,
  catalog page/head writes, generation retention and reclamation, serving-grant
  publication, and stream-binding authorization.
- The generic kv-server supervisor still needs durable descriptor watching,
  leader-change fencing, restart backoff, and chunkdb/diskdb startup adoption.
- FlatBuffers schemas and crowdb-rpc server transport remain; current protocol
  and handler tests use the in-process typed boundary.
- Target recovery, online split/catch-up, catalog proof wiring, real-process
  object-metadata restart tests, and three-node failover/balance tests remain.
- Management HTTP endpoints, structured process logging, heartbeat publication,
  metrics export, and bounded checkpoint concurrency remain to be wired.
