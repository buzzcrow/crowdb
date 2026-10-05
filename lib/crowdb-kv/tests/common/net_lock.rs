// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unique port allocator and serialization lock for parallel integration tests.
//!
//! Rust's default test runner executes tests in parallel. Tests that use
//! hardcoded placeholder ports (e.g. `node_id + 10_000`) collide when two
//! tests happen to use the same node IDs. [`unique_port`] solves the port
//! collision. However, cluster tests with tight election timers (5 ms
//! heartbeat) are also sensitive to tokio runtime contention under parallel
//! load, so [`lock`] provides a mutex that tests can hold for their entire
//! duration to prevent timing-induced failures.

use std::sync::OnceLock;

use tokio::sync::Mutex;

static NET_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Reserve a port through the shared process namespace so different test
/// binaries cannot reuse each other's live or deliberately closed ports.
///
/// # Panics
/// Panics when the shared port allocator cannot reserve a port.
pub fn unique_port() -> u16 {
    crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::KvServerListen)
}

/// Acquire the global network test lock. Hold the guard for the duration of
/// the test (store it in the cluster struct) to prevent timing-sensitive
/// election tests from interfering with each other under parallel load.
pub async fn lock() -> tokio::sync::MutexGuard<'static, ()> {
    NET_LOCK.get_or_init(|| Mutex::new(())).lock().await
}
