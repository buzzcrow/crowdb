<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Tree Flush Flow

Depends on: [Tree engine](design-crowdb-tree-engine.md)
Satisfies: [Tree overview](design-crowdb-tree.md) §2

Flush transfers a finite applied source prefix into immutable L1 ownership.
This document describes the completion and resource contracts of that transfer.

## Table of Contents

- [1. Capture and Admission](#1-capture-and-admission)
- [2. Bounded Range Handoff](#2-bounded-range-handoff)
- [3. Maintenance and Completion](#3-maintenance-and-completion)
- [4. Progress and Observation](#4-progress-and-observation)

## 1. Capture and Admission

`FlushController` prepares a `FlushOperation` using the current published root,
source catalog, finite frontier and accepted writer bounds. Resource admission
reserves complete Handoff, Routing, Reclaim and I/O windows, plus operation,
publication and CPU continuation controls. Overflow in the bound or insufficient
credit rejects preparation before sources are closed; soft maintenance can
retain a valid plan and await temporary credit pressure asynchronously.

Ordinary source admission holds a short atomic binder guard while recording its
entry and key-byte footprint. A reversible `SourcePause` prevents new ordinary
binders and observes completion of existing binders asynchronously. This fixes
the ordinary footprint, including admitted writes that have not applied yet,
without waiting for those writers to finish. Gap repair remains independently
admissible, so the bound adds its complete configured allowance. Returning
unused ordinary credit can only reduce this bound.

The finite gap source allowance is reserved for the next contiguous missing
slot. Higher missing slots retain their prepared ordinary credit and wait for
frontier progress before using that allowance. Filling the gap window with
higher holes must not prevent the oldest hole from advancing the prefix.

**I4 — Stable preparation:** the paid pause owner remains alive from planning
through actual capture and source closure. The plan is valid only for its
expected source catalog; a stale catalog requires replanning before closure.
Abandonment, cancellation or failed reservation releases the pause. Once the
outstanding binders have left, ordinary admission resumes and progress waiters
are notified. A permanently closed source never reopens.

An open source without such a pause uses its full configured ordinary and gap
bounds. Soft rotation stores the stable bound before permanently closing its
source. That sealed bound covers outstanding admitted writers; once a closed
source is drained, later preparation can use its actual record/key footprint.
A source without a known stable or drained bound retains the configured bound.

Before publishing a soft-rotation successor, `SourceRotation` checks that a
complete future flush fits every Handoff, Routing, Reclaim and I/O partition.
The check includes the sealed current footprint and a successor's full ordinary
and gap allowance. Source-count limits also leave room for the capture
successor and a split child's active table. If another source would exceed
these limits, maintenance hands off the existing prefix instead of publishing
that successor.

`SourceAdmission` closes the captured sources and publishes a successor.
Already admitted writers can finish from their own credits. The operation
waits for their terminal state asynchronously; a CPU worker does not block while
those writers or child tasks are pending. The captured catalog and contiguous
frontier remain fixed. Source handoff does not infer coverage from the largest
key slot: NoOp and missing-slot progress come from the apply frontier.

## 2. Bounded Range Handoff

`FlushBatch` routes captured sources through immutable inner descriptors and
runs bounded range jobs. Each job uses independent lower/upper source cursors,
with a stable source tie order and the captured prefix/floor. It creates sorted
`OverlayBatch` entries with independent `CellVersion` owners and bounded key
arena blocks. Payload bytes remain shared; the output does not keep the whole
MemTable index alive.

Entry and key-byte limits bound each installation window. Exhausting a key
arena pauses before consuming the next candidate. The next window continues
from the first unconsumed key with fresh thread-local guards. Parallelism,
range queue slots and inline dispatch all have explicit limits.

The total range count is bounded by the captured input entry allowance and
the captured mapping's leaf count. Handoff prepays this complete metadata
bound; it does not impose a fixed ceiling on the Tree's leaf count. Routing
yields after a bounded number of leaves, and the dynamic join limits active
range jobs independently of the total admitted range count.

Prepared replacements include expected page identities and an install token.
The coordinator merges a valid transaction with the latest root, preserving
unrelated sibling updates. Commit publishes mapping, source floor, coverage
and aggregate logical statistics together. Its final publication segment does
not allocate, cancel or invoke a caller callback.

**I1 — Prefix safety:** a successful installation may precede a later error.
`FlushResult` reports actual published coverage; uncovered sources remain
available for retry and reads. Repeated tokens cannot duplicate installation.

## 3. Maintenance and Completion

A sealed dynamic join waits for every accepted range terminal. First error
stops further dispatch and drains already accepted children. Only after range
handoff has joined does the operation acknowledge detached source ownership
and execute its bounded maintenance phase.

Maintenance performs coalescing, layout analysis, local split and eligible
merge publication. Unique page/pair candidates are indexed in the published
state; retry does not turn queue length into a page count. Cold maintenance
loads one bounded leaf and the required cold routing descriptors from one
captured view, then validates their original identities at publication.
Overflow payloads remain referenced during this primary-page work.

Overlay coalescing can remove a tombstone at or below the captured GC floor
only when that leaf has no base references. A resident or cold base still
requires its masking tombstone; checkpoint encoding merges the complete leaf
before applying retention to its durable representation.

The maintenance budget includes actual captured base-array bounds, configured
work/metadata limits and callback lifetimes. A continuation cannot consume an
unreserved host remainder merely because handoff has finished. All read and
I/O reservations are sealed only after their last required callback.

**I2 — Distinct outcomes:** handoff success and maintenance success are reported
separately. A later maintenance error does not undo already installed coverage.
**I3 — No durability inference:** flush and representation-only maintenance do
not publish a backend root or advance the durable WAL replay point.

## 4. Progress and Observation

The host schedules finite maintenance/writeback windows even after writes stop.
Each window advances the oldest eligible candidate or reports a bounded reason
for no progress. Dirty pages, merge candidates and checkpoint-pending pages are
separate published quantities; checkpoint completion is not inferred from
writeback completion.

Soft source maintenance uses the existing single dispatcher and prepaid
continuation controls. It either joins an accepted flush or retains a stable
plan while requesting the four resource partitions. Budget release resumes the
waiting continuation without another write. A changed immutable plan releases
its stale preparation and replans through the dispatcher; workers do not spin
or synchronously wait for credit. A bound that exceeds a partition's capacity
remains a rejection.

A synchronous host apply may wait for an identified reversible source pause or
source-publication transition before consuming its input or claiming an
automatic slot. Synchronous host flush joins finite accepted work or waits for
an identified controller preparation. CPU and backend callbacks return typed
unavailability rather than waiting on their own executor. Ordinary writer
credit rejection retains its error; historical payload or old-view retention
does not create an unbounded host wait.

Close and generation replacement first stop new ingress and stop unaccepted
soft-maintenance waits, then join ingress. This releases host callers awaiting
a pause before close waits for those callers. Already accepted chosen applies
have separate lifetimes and retain their prepaid handoff path until terminal;
close drains them before closing source admission and the flush controller.
Replacement may restart soft maintenance only while the Tree remains open.

Stage counters measure capture, writer drain, queueing, execution, publication,
maintenance and terminal cleanup. Dispatch statistics record actual inline and
queued jobs and actual concurrency. Ages report outstanding work with an
explicit completeness flag when bounded tracking capacity is exhausted.
Metric saturation never rejects otherwise admissible storage work.
