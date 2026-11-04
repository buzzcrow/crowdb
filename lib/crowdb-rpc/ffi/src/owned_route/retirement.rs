// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Process-lifetime owner teardown, independent of Tokio runtime shutdown.
//!
//! Only the last clone of a route wrapper queues work. In-flight RPC lease
//! clones never use the channel. Teardown can synchronously complete other
//! RPCs, so neither producers nor the worker may wait for queue capacity.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::time::Instant;

use crowdb_common::metrics::{self, Counter, Gauge, LatencyHistogram};

use super::RouteOwners;

unsafe extern "C" {
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

static WORKER: OnceLock<Retirement> = OnceLock::new();

struct Task {
    owners: RouteOwners,
    queued_at: Instant,
}

struct State {
    live: AtomicUsize,
    pending: AtomicUsize,
    pending_metric: Arc<Gauge>,
    completed: Arc<Counter>,
    latency: Arc<LatencyHistogram>,
}

struct Retirement {
    sender: mpsc::Sender<Task>,
    state: Arc<State>,
}

fn worker() -> &'static Retirement {
    WORKER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<Task>();
        let state = Arc::new(State {
            live: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            pending_metric: metrics::global_gauge("rpc.route.retirement.pending"),
            completed: metrics::global_counter("rpc.route.retirement.completed"),
            latency: metrics::global_histogram("rpc.route.retirement.lh"),
        });
        let worker_state = Arc::clone(&state);
        // The static sender remains admitted through the last resolver/callback
        // release. No runtime shutdown or JoinHandle drop can close it early.
        std::thread::Builder::new()
            .name("rpc-route-retire".to_owned())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    for task in receiver {
                        drop(task.owners);
                        worker_state
                            .latency
                            .observe(task.queued_at.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
                        worker_state.completed.inc();
                        worker_state.pending_metric.dec();
                        worker_state.live.fetch_sub(1, Ordering::Release);
                        // Include native joins and cascading retirements in the
                        // drain condition, rather than using a FIFO marker.
                        worker_state.pending.fetch_sub(1, Ordering::Release);
                    }
                }));
                if outcome.is_err() {
                    fatal("route retirement worker panicked");
                }
            })
            .unwrap_or_else(|_| fatal("could not start route retirement worker"));
        // Native logging globals are constructed before these route owners.
        // Drain queued destruction before those globals are destroyed.
        // SAFETY: this C callback never unwinds and uses process-lifetime state.
        // It does not close admission for cascading owner releases.
        if unsafe { atexit(shutdown) } != 0 {
            fatal("could not register route retirement exit drain");
        }
        Retirement { sender, state }
    })
}

pub(super) fn register_owner() {
    worker().state.live.fetch_add(1, Ordering::AcqRel);
}

pub(super) fn retire(owners: RouteOwners) {
    let worker = worker();
    worker.state.pending.fetch_add(1, Ordering::AcqRel);
    worker.state.pending_metric.inc();
    if let Err(undelivered) = worker.sender.send(Task {
        owners,
        queued_at: Instant::now(),
    }) {
        // A dead receiver must never drop these owners on the enqueueing RPC
        // thread. Retain them until abort instead of recreating a self-join.
        std::mem::forget(undelivered);
        fatal("route retirement receiver unexpectedly closed");
    }
}

fn fatal(message: &str) -> ! {
    eprintln!("critical: {message}; next step: inspect RPC lifecycle worker failure");
    std::process::abort();
}

pub(super) fn pending() -> usize {
    WORKER
        .get()
        .map_or(0, |worker| worker.state.pending.load(Ordering::Acquire))
}

pub(super) fn drain(timeout: std::time::Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while pending() != 0 {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

pub(super) fn quiescent() -> bool {
    WORKER
        .get()
        .is_none_or(|worker| worker.state.live.load(Ordering::Acquire) == 0)
}

pub(crate) extern "C" fn shutdown() {
    // Explicit logging shutdown already requires quiescing callbacks and
    // dropping instances first. Normal exit has also released Rust stack and
    // runtime owners. Do not wait indefinitely for arbitrary live wrappers:
    // process::exit intentionally leaves stack variables alive. Fail explicitly
    // rather than run native static destructors under their active RPC workers.
    if !drain(std::time::Duration::from_secs(30)) {
        fatal("RPC route owner teardown did not drain before native shutdown");
    }
    if !quiescent() {
        fatal("live RPC route owners remain at native shutdown; quiesce and drop routes first");
    }
}
