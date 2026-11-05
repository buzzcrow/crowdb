<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: crowdb-tree In-Memory Engine

Depends on: [Tree overview](design-crowdb-tree.md), [state machine](../kv/design-crowdb-paxos-kv-state-machine.md)
Satisfies: [Tree overview](design-crowdb-tree.md) §2

This document defines source versions, immutable page publication, read
ownership, resource accounting and async completion. Frame formats and durable
root publication are specified by [durable storage](design-crowdb-tree-storage.md).

## Table of Contents

- [1. The Tree Engine](#1-the-tree-engine)
- [2. Memory and Buffer Management](#2-memory-and-buffer-management)
- [3. Async FFI Bridge](#3-async-ffi-bridge)
- [4. Rust-Side KVEngine Async Trait Shape](#4-rust-side-kvengine-async-trait-shape)

## 1. The Tree Engine

### 1.1 Logical Model and the Slot-Aware Cell

A key maps to the highest eligible `(slot, kind, value)` cell. Slot is carried
in the value cell rather than the ordered key. Tombstones participate in slot
selection and remain visible to retention-aware iteration until the external
GC floor permits their removal.

L0 contains concurrent MemTables with retained versions. L1 contains immutable
base references and overlay runs. A finite read/flush view fixes its source
catalog, floor and mapping; selection cannot mix sources from one publication
with pages from another.

**I1 — Prefix retention:** an out-of-order future value or tombstone must not
hide the greatest value needed by an incomplete earlier prefix. Retention
keeps that boundary value and versions above the retained prefix.

### 1.2 Immutable Pages and Overlay Runs

`PageState` holds a half-open range, base references, overlay runs, content
identity, route generation and sibling information. `BaseRef` separates the
logical source from its resident representation and typed backing capability.
A range proxy may share a base without encoding or copying its payloads.

`OverlayBatch` owns sorted `EntryRef` descriptors and bounded key arena blocks.
Each entry independently retains its `CellVersion`; keeping an overlay alive
does not require retaining the entire source skip list or version descriptor.
A leaf with only overlays is valid. Inner descriptors contain separators and
child page identities; cold inner backing remains independently reloadable.

### 1.3 Write Path: MemTable Ingest and Flush

A writer prepares the complete batch before publishing its first key. Its
credit covers keys, payload capacity, nodes, version descriptors, history and
reference/control storage. Failed preparation leaves keys and automatic slot
allocation unchanged. Duplicate keys in one API batch use the last operation.

`SourceAdmission` registers writers against a source close/count word. A writer
that loses admission to closure retries against the successor; an already
admitted writer retains the resources needed to finish. Ordinary proposals and
accepted gap repair use distinct resource capacity. Replay does not discard
accepted work because the ordinary proposal queue is full.

Prepared ordinary proposals use temporary admission status when their apply
or backend operation window is occupied. No consensus slot is chosen for that
attempt. Once armed, their lifetime tickets retain accepted work through its
last callback; a full pre-consensus window is distinct from allocation failure.

Flush planning can reversibly pause ordinary source admission to obtain a
stable bound for all admitted writers, including those that have not applied.
The pause precedes input or automatic-slot consumption; gap repair keeps its
separate allowance. A paid owner holds the pause until source closure or
rollback. Soft rotation checks the complete flush budget for the prospective
source set and resumes on released credit without requiring another write.

Flush captures a finite contiguous prefix, closes captured sources, publishes
the successor and asynchronously drains accepted writers. Parallel bounded
range jobs transfer independently owned cell references to L1. Installation
uses the current coordinator root and expected identities, with idempotent
install tokens. See [flush flow](design-crowdb-tree-engine-flush-flow.md).

### 1.4 Consolidation, Split, and Merge

Coalescing merges sorted overlay runs without materializing value bytes. Layout
analysis computes representable primary pages and bounded overflow dependencies.
Local split may retain shared base/range proxies; encoding is performed when a
representation actually requires it. Candidate pages and eligible sibling pairs
are indexed by unique identity in the immutable published state.

Maintenance uses finite visit, entry, metadata, page-output and path bounds.
Cold analysis loads one selected leaf and required routing descriptors, retains
original expected identities and publishes only if they still match. A stale
plan cannot replace newer logical contents. Repeated bounded invocations can
advance classification, coalescing, split and merge without rescanning the
entire tree from the beginning.

**I2 — Atomic accounting:** a committed structural transaction updates mapping,
root, logical contributions and candidate membership together. Retries and
unchanged classification do not duplicate counts or publish empty work.

### 1.5 Versioned Root (Consistent Snapshots)

`PublishedTreeState` contains the mapping, source catalog/floor, coverage,
generation, commit sequence, root identity and aggregate logical statistics.
Persistent radix COW copies only touched paths and segments. A retained state
continues resolving its old page identities after later publications.

`PublicationCoordinator` owns the bounded commit lane. Prepared transactions
validate expected pages and generation against the latest root, merge unrelated
updates and publish one new state. The final commit segment has no allocation,
cancellation or caller callbacks; completion notification follows publication.

`PublishedSnapshot` owns a source-free view of the last complete L1 mapping
and its coverage, independent of the facade lifetime. During partial handoff,
the publication root retains that completed view alongside the changing live
mapping; capture selects both mapping and coverage in one epoch-protected
root read. Capturing a snapshot does not advance the flush frontier. Native
and portable export, range rebuild and generation replacement retain their
captured ownership through the same publication boundary.

A split session fixes the complete L1 baseline paired with its captured source
floor. A synchronous host capture joins an in-flight flush without blocking a
runtime worker. A new session fixes its baseline only after capturing at least
the frontier observed when capture began; an older pending operation may finish
first. Later parent flushes do not change the fixed baseline.

An existing session may already back a child's installed overlay. If intervening
parent flushes leave a gap after that session's retained sources, beginning its
shared view returns the older, continuously retained frontier. It does not claim
the parent's newer frontier is available. Callers publish the fixed cutover and
replay later slots; an explicit overlay request beyond the retained prefix is
rejected before changing the child.

An empty, unwritten child can inherit the missing prefix by copying the captured
baseline through bounded portable work windows, reserving its complete output
shape first. The
replacement publishes independent L1 storage and retained L0 sources together
after rechecking the child root under its ingress fence. Preparation failure
leaves the child root and frontier unchanged. A nonempty child must already
cover the required L1 prefix; missing history is rejected rather than credited.

### 1.6 Epoch-Based Reclamation

MemTable cursors and writers use a source's epoch lifetime to safely acquire
raw immutable nodes/version descriptors. Cross-thread handoff obtains an
independent `CellVersion` owner while still protected; it never reconstructs
ownership from a raw pointer after leaving the guard.

Epoch participants register through CAS and cache the participant by manager
identity. Nested guards share the participant. A source retains participants
and pending retirements until its outstanding readers have left. Replaced
version descriptors use preallocated retirement tickets; per-key mutation does
not take a reclamation mutex.

Published roots, descriptors, arrays and frame owners have independent shared
ownership. Releasing the live root only starts retirement; retained snapshots,
values, cache entries and callbacks can keep allocations alive. Host retirement
services perform bounded disposal after the last relevant owner exits.

**I3 — Actual release:** neither logical removal nor queueing retirement reduces
physical allocation counters. The allocator/deleter or confirmed physical
backend delete is the release event.

### 1.7 Read Path

Point reads and scans capture a consistent mapping/source view, choose matching
half-open ranges, and merge eligible source/run/base cells by slot with stable
tie ordering. Tombstones suppress older values; keys-only scans avoid value
materialization. Forward and reverse cursors preserve inclusive/exclusive
bounds, prefix/end limits and resumable byte budgets.

Cold frames are loaded asynchronously through the captured typed backing.
Reads use bounded retained-source and result windows; a scan does not reserve
or load every leaf in the mapping. Overflow chains use the captured primary
capability and have independent traversal, byte and cycle-detection bounds.
Malformed frames fail before becoming trusted descriptors.

A fixed snapshot entry cursor has no L0 sources. Its admission bound uses the
captured mapping shape and known L1 value bytes; unknown summaries retain the
configured maximum. Later live writes cannot enlarge that fixed window, and
decoder validation limits remain unchanged.

Owned read results retain only their necessary payload/frame owners. Packed
scan output carries its allocation owner through C and Rust, so dropping the
Tree or advancing a generation cannot invalidate returned data.

The public result limit and byte budget apply across internal work windows;
an unlimited public limit does not stop at the first window. A strict byte
budget that cannot fit the first entry returns an empty, truncated result.
Packed output uses an independent Routing allocation account. Growth pays for
both buffers during replacement and returns an explicit resource error if the
account cannot cover the new capacity; retained output remains charged until
the caller frees it.

### 1.8 Concurrency Summary

Standalone C and Rust opens share the process runtime. Their per-Tree binding
still validates and enforces queue, lifecycle and resource limits; omitting an
explicit runtime does not discard that binding. Range rebuild uses the
destination binding under the same rule.

- The host shares bounded CPU workers and backend execution; each Tree has an
  independent lane/account. New nested work yields while children are pending.
- Source insertion uses CAS. Prepared writers can finish after source closure;
  finite flush capture prevents new writers from extending a batch indefinitely.
- Range preparation, reads, page encoding and I/O can overlap. The publication
  lane serializes only validated root transitions.
- Backend callbacks own their operation/buffer/permit state until one terminal.
  Stop requests fence further dispatch and drain already accepted callbacks.
- Tree close fences ingress and stops unaccepted soft-rotation waits before
  joining ingress participants. It separately drains accepted chosen/replay
  work and preserves its operation owners. Generation replacement follows the
  same ingress ordering; finishing replacement cannot reopen a closed Tree.
  Host shutdown drains Trees, backend and retirement work before destroying
  their shared runtime resources.

### 1.9 Epoch-Protected MemTable (L0)

Nodes contain immutable inline keys, atomic forward links and an atomic
`VersionSet*`. Level-zero CAS establishes membership; upper links accelerate
search. Published nodes remain linked until source retirement. Immutable version
descriptors own their current cell and retained history through shared owners.
A monotonic pruning bound prevents a losing older writer from restoring an
obsolete version set after a CAS conflict.

For a retained prefix of 100, versions 2, 80, 102 and 105 retain 80, 102 and 105.
Views at 100, 102 and 105 therefore choose the corresponding highest eligible
version. Gaps can retain many versions; a proposal count limit is not a bound
on the total slot span.

Source summaries and physical allocation counters have different
meanings. Logical history/frozen bytes describe source contents; physical
bytes include allocations retained by readers, history descriptors and
independent payload owners. Concurrent samples can be approximate.

## 2. Memory and Buffer Management

### 2.1 The buffer Abstraction

`buffer` is move-only and supports inline, owned and externally owned storage.
Owned allocation capacity is the accounting unit, including header reserve;
an explicit clone copies bytes. External slices retain a validated full backing
identity/capacity and final drop owner. Incompatible host identity or unknown
capacity uses an admitted owned copy.

Keys are copied into bounded node/arena storage. Payloads can remain shared
from the public API through source versions and L1 overlays. Building a durable
frame necessarily copies its selected bytes into that representation; this
encoding event is distinct from ownership transfer.

### 2.2 Write and Read Pipelines

`AllocationLease`, allocation reservations and typed credits associate real
storage with a Tree account and its host budget. Reserved credit converts into
allocated bytes without charging both simultaneously. Unused credit returns on
rollback or sealing; allocated bytes remain until their actual deallocation.

Handoff, Routing, Reclaim, admitted-writer and gap-repair partitions prevent
ordinary growth from consuming all capacity needed to complete accepted work.
A complete reclaim operation receives its inputs, output, scratch, publication
and callback allowance before starting. I/O operations use separate permits.

### 2.3 Rust FFI Ownership

Known Rust `Bytes` allocations cross the ABI with a stable backing identity,
capacity and drop callback. Same-host slices share the payload charge; external
control blocks, foreign owner/context and slice holders remain separately
charged allocations. Failure/cancellation drops each transferred owner once.

`PinnedValue::into_bytes` retains a native result owner through `Bytes::from_owner`.
Packed scan owners follow the same principle. A shared output may be dropped
on another thread or after the facade closes without retaining an unrelated
source index or changing the charged capacity to the slice length.

### 2.4 Allocation Accounting and Ownership

Allocation owners charge their actual capacity once and return it on final
release. Shared cell, frame and external-buffer owners preserve that charge
across source handoff, immutable views and Tree closure. Epoch guards protect
borrowed accesses until reclamation is safe.

Snapshot capture and split source installation retain existing immutable
owners. They do not enumerate allocations or create per-reference statistics
tokens. Source summaries describe current logical contents; allocation and
budget counters describe physical occupancy and admission.

### 2.5 Work and Runtime Metrics

Each Tree records fixed stage summaries, terminal outcomes, event counters and
bounded observations of unfinished work. Queue wait, execution and end-to-end
pipeline durations describe different intervals; durations of parallel children
must not be interpreted as elapsed time for their parent. When age observation
slots fill, the sample reports incomplete coverage without rejecting work.

Budget request waits are separated into Handoff, Routing, Reclaim, admitted
writer, gap-repair and I/O classes. A wait runs from its first submission to its
single grant, error or cancellation terminal; direct non-waiting reservations
are separate. Shared-cache latency counts requests joining a load already in
flight, ending after each waiter's own proof and pin validation. First loaders
and ready cache hits are excluded from that wait summary.

One collector owns destructive window sampling and shares an immutable result
with logs and diagnostics. Cumulative samples never drain. Every supported C
capture version advances the same window, including enabled distributions that
an older output structure cannot expose. Versioned structures validate all
headers before changing a window or caller output.

Histograms are explicitly enabled for one Tree and one stage. They allocate no
buckets by default; enabling charges the real fixed bucket storage to that
Tree's Routing account. Concurrent enable calls retain one instance, and failed
enable leaves business state unchanged. Terminal observations use atomic updates
without allocating. Enabled storage survives with the metrics owners and is
released with the last owner. Quantiles come from the distribution; they are not
estimated from the summary average or maximum.

The shared host runtime also exposes cumulative CPU and backend executor queue,
execution and rejection counters, alongside reserved/allocated resources and
budget waits. These samples cover all Trees using that runtime. They are relaxed
cross-field observations, require no backend I/O, and remain readable after its
Trees close while the runtime handle is retained.

Periodic resource windows integrate non-draining samples to report average
and sampled maximum occupancy. Tree samples include backend, ordinary apply
and replay operation slots independently of runnable CPU queues. They also
include the active source's ordinary/gap entry and key-byte allowances,
including admitted writers that have not materialized records. These source
counts reset on source rotation and do not measure retained older sources.
Operation capacity denials are cumulative; source contention counters are not
measured. Sampling age, gaps and errors accompany the occupancy windows.

## 3. Async FFI Bridge

Private sender operations compose bounded CPU jobs and asynchronous backend
callbacks. Public headers expose opaque handles, status/result structures and
callbacks, without leaking sender templates. The synchronous facade waits for
its operation completion; async callers use native futures directly.

A native future retains accepted state and uses a notification fd to wake Rust.
Rust checks readiness, registers an independently owned `AsyncFd`, and rechecks
completion to avoid lost wakeups. Closing a former reactor cannot deregister a
new native descriptor that reuses the same fd number.

The first poll may already find a completed operation; callers must also handle
pending CPU dispatch even for resident data. Tree work is not implemented by
creating a new OS thread per flush or by wrapping all calls in `spawn_blocking`.

**I4 — Terminal ownership:** cancellation is a request, not permission to free
accepted buffers. Exactly one success/error/stopped terminal releases the
operation's remaining credits after its children and backend callbacks drain.

## 4. Rust-Side KVEngine Async Trait Shape

`KVFuture<T>` represents either an immediate result or a pending boxed future,
allowing object-safe `KVEngine` calls without requiring a heap future for every
immediate completion. `CrowdbTreeEngine` adapts owned native operations to that
contract and retains one metrics collector for its lifetime.

Proposal admission produces a grant that survives the consensus decision and
subsequent apply. Accepted apply/replay and NoOps drive the contiguous frontier;
ordinary proposal backpressure cannot consume the gap-repair allowance.

Typed maintenance reports distinguish handoff, writeback, checkpoint and
retirement progress. The host's finite idle loop checks actual candidates and
backend availability, so stopped writes can still converge without advancing
WAL durability from representation-only work.
