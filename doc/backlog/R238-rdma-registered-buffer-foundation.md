<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R238: RDMA — Registered host-buffer foundation

## Problem

R5 requires bounded registered host buffers for RDMA, but the current RPC and
access layers do not share a lifetime contract for buffers used by NIC,
DiskIO, SDK callers, and asynchronous completion. An unbounded or prematurely
reused buffer can corrupt a later stream. The foundation must be independent of
the selected RPC transport and must work when RDMA is unavailable.

## Solution

1. Define a process and device scoped registered-memory pool with a hard byte
   budget and separate control and data capacity classes: 64 KiB, 256 KiB, and
   1 MiB.
2. Define the ownership states `available`, `posted_receive`,
   `receive_in_flight`, `send_in_flight`, `disk_owned`, `caller_owned`, and
   `quarantined`. A buffer has one owner and one release transition.
3. Expose an opaque buffer lease that records capacity, actual length, request
   ID, stream sequence, logical offset, and release state. Release returns a
   caller-owned buffer to its class pool only after all device access ends.
4. Make exhaustion bounded: producers wait or return backpressure, and no
   replacement allocation bypasses the registered-memory budget. Unknown
   completion state quarantines the buffer until connection quiescence.
5. Keep registration optional. A build or host without RDMA retains the TCP
   pool and does not require an RDMA device at runtime.

## Dependencies

- R5 remains the umbrella registered-memory goal.
- R4 bounded memory pool supplies the budget and admission principles.
- The output is consumed by R239 RDMA RPC transport and R240/R241 data paths.

## Acceptance

- Given a configured byte budget, when all classes are allocated, the next
  allocation returns bounded backpressure and never exceeds the budget.
  Invariant: registered bytes are bounded. Unit test.
- Given a buffer in each ownership state, when the valid owner releases it,
  the next state is deterministic and no second owner can release it.
  Invariant: single ownership. Unit test.
- Given an uncertain completion, when the connection closes, the buffer is
  quarantined and is not returned to a new stream. Invariant: no reuse before
  quiescence. Integration test.
- Given RDMA is unavailable, when the pool is initialized, TCP allocation
  remains usable without an RDMA runtime. Invariant: optional capability.
  Integration test.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
```
