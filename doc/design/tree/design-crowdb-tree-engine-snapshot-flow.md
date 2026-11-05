<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Tree Checkpoint Flow

Depends on: [durable storage](design-crowdb-tree-storage.md), [Tree engine](design-crowdb-tree-engine.md)
Satisfies: [Tree overview](design-crowdb-tree.md) §3.1

A checkpoint turns one captured immutable Tree view into a durable backend
root. Reloadable writeback and logical handoff use related preparation but have
different completion contracts.

## Table of Contents

- [1. Capture and Plan](#1-capture-and-plan)
- [2. Writeback and Root Commit](#2-writeback-and-root-commit)
- [3. Rebase, Failure and Recovery](#3-rebase-failure-and-recovery)
- [4. Observation](#4-observation)

## 1. Capture and Plan

A checkpoint establishes a finite applied prefix through flush, then captures
an immutable published view. The operation retains its mapping, sources and
backing capabilities while later writes continue. Checkpoint serialization
uses the existing operation token; CPU and backend waits release their worker.

`CapturedFrameStream` chooses valid durable-frame reuse or bounded page
materialization. Reuse requires matching logical content, route, retained
floor and primary layout proof. New overlays or an incompatible retention
floor invalidate the receipt. Advancing the floor remains compatible when an
exact leaf summary proves that every encoded entry is a live key: encoding
keeps one winner per key, and no tombstone remains to collect. Receipts with
tombstones or unknown live counts still require the original floor. Overflow
dependencies are retained through the
captured primary capability, not resolved against a mutable current Tree.

The layout plan counts primaries whose existing representation already uses
the exact durable receipt. These pages need no replacement publication, so
checkpoint admission excludes them from the rebase replacement budget.
Relocation keeps the full allowance because it may change dependency owners.

If a captured page cannot fit the durable layout, checkpoint planning releases
its current layout and phase reservations before admitting a bounded structural
split. Each split must publish progress. Planning then restarts against that
result while preserving the original generation and applied frontier; it does
not perform another handoff or include later L0 writes. A competing L1
publication invalidates the attempt instead of silently changing its input.
The fence uses a per-generation L1 sequence covering the immutable mapping,
root, PID authority, coverage, GC floor and handoff ownership. Publishing a
successor L0 catalog changes the general commit sequence but leaves this fence
unchanged, so later source writes cannot invalidate the fixed layout proof.

A native export returns real frames even when a checkpoint could reuse them
without reading. Portable export instead yields the captured logical entries.
Import validates the complete staged graph before publishing a replacement
generation; failure leaves the old generation usable.

## 2. Writeback and Root Commit

Primary-page and overflow encoding have explicit input, output, scratch and
I/O bounds. Backend tickets retain their buffers and permits until the actual
callback. Inline, delayed, duplicate and failing completions must produce one
operation terminal.

Each accepted Chunk write retains independent prepaid credit so it can finish
after the enclosing reservation seals. Unused credit and later frees return
to the enclosing window while it is open, allowing a long checkpoint to reuse
the same transient capacity across frames; otherwise they return to the host.

Writeback stores selected page representations and returns reloadable backing.
It may rebase a matching page while leaving checkpoint-pending membership and
durable coverage unchanged. Host idle maintenance uses finite candidate windows
and does not wait for new writes to trigger this work.

A checkpoint writes or reuses every captured dependency, completes the required
backend barriers, then commits its root:

- Local commits the directory and anchor through `LocalCheckpoint`.
- Chunk publishes the fenced immutable root through the catalog generation CAS.

**I1 — Durable coverage:** only a successful root commit establishes the replay
point. Failed or ambiguous publication cannot be treated as successful merely
because some pages were written; an ambiguous result must be resolved through
the backend authority contract.

## 3. Rebase, Failure and Recovery

Representation rebase compares the captured page identity against the current
mapping. It may replace a captured suffix while retaining newer overlays.
Changed route/base/run identities defer the rebase instead of overwriting newer
content. Deferral does not invalidate an already successful durable checkpoint.

An exact reused receipt needs no rebase: the captured representation already
owns that backing. This leaves the current mapping untouched, including any
later overlays, route changes or replacement pages.

`SnapshotBaseline` retains committed primary receipts and their backing owners
without retaining the source mapping or covered overlay payload. Publication
checks its generation, capture token, coverage, GC floor and backend identity
before installing it. Representation rebase uses the existing page identity
and primary receipt; it does not install a separate durable range topology.

Recovery validates root metadata, frame identities, ranges and dependencies
before exposing a writable generation. Readers retain the original backing
owner even if later checkpoints or collection replace the current backend root.
Physical retirement waits for those owners and the backend's actual deletion.

Local cleanup holds an exclusive free-range lease from the extent ledger
through physical deletion, preventing concurrent address reuse. Memory block
storage releases only complete, unprotected 64 KiB backing pages; anchors,
partly protected pages and partial tails remain allocated. Cleanup advances in
bounded background windows through the same accepted operation lifetime. A
memory cleanup continues across windows without issuing another checkpoint,
preserving the logical device size and the bytes retained by live owners.
Backing capacity released by the allocator is distinct from process RSS.

**I2 — Reader safety:** old captured views remain readable after Tree close or a
new root commit. **I3 — Failure isolation:** rejected import, failed write or
stale rebase cannot partially replace the live generation. **I4 — WAL safety:**
the host replays after the durable coverage and applies its own safe-retention
policy before truncating consensus WAL.

## 4. Observation

Checkpoint reports distinguish encoded/written work, durable coverage and
terminal status. Backing gauges distinguish dirty logical pages, reloadable
backing, checkpoint-pending membership and canonical physical capacity.

A collector that outlives the Tree retains metrics only, so it does not keep
page or backend resources alive. Capture retains the immutable mapping and
its actual owners without enumerating allocations for statistics.
