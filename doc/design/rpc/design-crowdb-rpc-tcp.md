<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: RPC TCP Transport

Depends on: [`design-crowdb-rpc.md`](design-crowdb-rpc.md) (root — `Transport` interface, `Connection`, `OutFrame`, `Buffer`, `FrameParser`)
Satisfies: `design-crowdb-rpc.md` §2 (transport interface + I/O loop decisions)

TCP is the v1 transport. epoll (Linux) and kqueue (macOS) are the two
kernel event interfaces; they differ in API but share the same
event-driven loop structure. A common `SocketTransport` base holds the
shared logic (send queue drain, parser feed, connection management); the
event-dispatch primitives are in `EpollEngine` and `KqueueEngine`
subclasses. The engine subclass tells the base *when* to read/write; the
base does the actual I/O and parsing via `on_readable` and `on_writable`.

## Table of Contents

- [1. Worker Loop](#1-worker-loop)
- [2. Scatter-Gather Send](#2-scatter-gather-send)
- [3. Zero-Copy Receive](#3-zero-copy-receive)
- [4. EpollEngine (Linux)](#4-epollengine-linux)
- [5. KqueueEngine (macOS)](#5-kqueueengine-macos)
- [6. Multi-Engine Scaling](#6-multi-engine-scaling)
- [7. Listener ownership and restart](#7-listener-ownership-and-restart)

---

## 1. Worker Loop

```
Per worker thread:
  engine = transport-owned SocketEngine (shared with siblings if M>1)
  register: eventfd/notify-fd, timerfd/kqueue-timer

  loop:
    events = engine.wait(timeout)
    for event in events:
      if event.fd == notify_fd:           // shutdown wake only
        no-op (no cross-thread submit queue)
      elif event.fd == timer_fd:
        run due scheduled tasks → reset timer to next deadline
      elif event is READABLE:
        on_readable(conn)
        if engine.oneshot(): arm_read(fd)  // re-arm after EV_ONESHOT
      elif event is WRITABLE:
        on_writable(conn)
        if queue non-empty and engine.oneshot(): arm_write(fd)
        if queue empty and not oneshot: disarm_write(fd)
      elif event is ERROR/HUP:
        conn->close() → remove_connection(fd) → trigger reconnect

    // send aggregation: batch writev pending responses
    for conn in pending_write_conns:
      on_writable(conn)
      if partial/EAGAIN: engine.arm_write(fd)
```

The transport creates N independent engines (`io_engines`), with
`io_workers` total workers (per-engine M = `io_workers / io_engines`).
Connections are partitioned round-robin across all workers; each
connection is owned by one worker, so no cross-worker locking. When
M=1, the single worker uses the engine with no
`EV_ONESHOT`/`EPOLLONESHOT` — level-triggered, no re-arm overhead
(the fast path). When M>1, the M workers sharing one engine's fd use
`EV_ONESHOT`/`EPOLLONESHOT` so only one worker wakes per event; each
worker re-arms read/write after processing.

WRITABLE is armed only when there's data to send — idle connections
don't wake the worker; when the send queue drains, disarm (level-
triggered) or let the one-shot auto-disarm. The caller-thread
writev (`SocketTransport::submit`) handles sends on the caller's thread
— no cross-thread submit queue or notify wake. The `Connection::io_engine`
back-pointer routes `arm_write` to the owning engine on EAGAIN. One
timer per worker (`timerfd` / `EVFILT_TIMER`) serves all scheduled tasks.

## 2. Scatter-Gather Send

`on_writable` drains up to `BATCH_MAX` frames from the send queue, builds
an `iovec` array (3 per frame: header, control, data), and calls
`writev` in one syscall. The kernel reads directly from the pool buffers
— zero-copy. On partial write, `advance_iov` skips the written bytes and
the remaining iovecs stay queued; the next `on_writable` continues.
Fully-sent frames' buffers are released (refcount decrement → pool
recycle). A dropped connection mid-send returns `EPIPE` / `ENOTCONN`;
`on_writable` calls `conn->close()` and reconnect triggers. A worker
thread crash is detected by the engine's main loop, which closes all its
connections, fails pending requests, and triggers reconnect.

## 3. Zero-Copy Receive

`on_readable` loops: gets the next read target from `FrameParser`,
calls `read()` directly into the pool-allocated `Buffer`, feeds the
bytes to `parser.advance()`. When a complete `Frame` is yielded,
dispatches via `conn->on_frame()`. On `EAGAIN`, the loop breaks;
level-triggered epoll wakes again when more data arrives. No scratch
buffer — the `Frame` handed to `on_frame` points at the same bytes the
kernel wrote.

## 4. EpollEngine (Linux)

Level-triggered (not edge-triggered) — simpler correctness model, the
worker re-arms write only when there's data. `EPOLLET` would require
draining the socket to EAGAIN every wake; level-triggered lets us arm
on-demand and disarm when idle. `arm_read` / `arm_write` / `disarm_write`
are `epoll_ctl(EPOLL_CTL_MOD, …)`; `wait` is `epoll_wait`.

## 5. KqueueEngine (macOS)

kqueue uses `EV_CLEAR` (edge-triggered) for write — the API is cleaner
this way on macOS, and `on_writable` already drains the queue fully per
wake. Read uses level-triggered to match epoll semantics. `arm_read` /
`arm_write` / `disarm_write` are `kevent` on `EVFILT_READ` /
`EVFILT_WRITE`; notify is `EVFILT_USER` (pipe fallback on older macOS);
timer is `EVFILT_TIMER`.

## 6. Multi-Engine Scaling

The transport supports a 2D configuration: `io_engines` × per-engine
workers (`io_workers / io_engines`). Each engine is an independent
epoll/kqueue fd with its own connection set (round-robin partitioned at
accept time). This separates two tuning axes that were previously
conflated:

- **Engines** — parallelize across independent kernel event queues.
  Each engine has its own fd, its own interest set, and its own
  `wait` call. No cross-engine locking or re-arm overhead.
- **Workers per engine** — share one engine's fd among M threads using
  `EV_ONESHOT`/`EPOLLONESHOT`. Only one worker wakes per event; the
  worker re-arms after processing. This is the legacy multi-worker
  mode, preserved for comparison but not the primary scaling path.

### Configuration Matrix

| Config | Engines | Workers/Engine | ONESHOT | Use case |
| --- | --- | --- | --- | --- |
| 1×1 | 1 | 1 | no | Fast path — single worker, level-triggered |
| N×1 | N | 1 | no | Multi-engine — N independent event loops |
| 1×M | 1 | M | yes | Legacy multi-worker — shared fd, ONESHOT re-arm |
| N×M | N | M | yes | Mixed — N engines, M workers each |

### Connection Engine Back-Pointer

`Connection::io_engine` (type-erased `void*`) is set once at
`Worker::add_connection` time, before the fd is registered with the
engine. `SocketTransport::submit` casts it back to `SocketEngine*` and
calls `arm_write(fd)` on EAGAIN. This routes the re-arm to the correct
engine — arming write on a different engine's fd would be a silent
no-op (the fd is not in that engine's interest set) and the partial
send would stall.

### Per-Platform Tuning

- **macOS (kqueue)**: N×1 is the preferred scaling mode. Each
  kqueue fd is independent; no `EV_ONESHOT` re-arm overhead. The
  number of engines should match the number of performance cores
  (e.g. 4–8 on Apple Silicon).
- **Linux (epoll)**: N×1 is also preferred for the same reason. On
  kernels with `io_uring` support (future), the engine abstraction
  allows swapping epoll for io_uring without changing the worker
  model.

## 7. Listener ownership and restart

- **L1 — Stable endpoint**: a logical service retains its configured listening
  address and port across stop/start and crash recovery. Container-local service
  ports use fixed four-digit assignments; independent network namespaces reuse
  the same assignments. Host-published test ports are assigned by the runtime.
- **L2 — Restart address reuse**: TCP listeners enable `SO_REUSEADDR` before
  `bind`, on the first start and every restart. Reuse permits rebinding after
  the old listener closes while old connections remain in `TIME_WAIT`. This
  applies to RPC and HTTP management/health listeners; wrappers and async
  frameworks must preserve equivalent socket behavior. Failure to configure
  the option or bind the endpoint is a startup error with address/port context.
- **L3 — One serving instance**: an endpoint in one network namespace has one
  live service owner. Restart waits for the previous process/listener to exit.
  `EADDRINUSE` identifies an unresolved ownership conflict; do not hide it by
  incrementing ports, probing an alternative range, or changing the advertised
  endpoint. `SO_REUSEPORT` is not a default restart option: multiple accepting
  listeners require an explicitly designed ownership/handoff protocol.
- **L4 — Bounded verification**: recovery acceptance retains the endpoint,
  establishes client connections before stopping the service, restarts within
  a bounded deadline and verifies exact persisted data. A second live service
  attempting the same endpoint must fail without disrupting the first one.

These are CrowDB listener contracts. The operating-system meanings of
`SO_REUSEADDR`, active listeners and `SO_REUSEPORT` follow the
[Linux socket manual](https://man7.org/linux/man-pages/man7/socket.7.html);
namespace isolation and logical service ownership are CrowDB responsibilities.
