// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use crowdb_rpc_ffi::{ConnectionPoolError, ConnectionPoolIndex, RpcError, RpcServer};

fn running_server() -> Arc<RpcServer> {
    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    server.start();
    server
}

#[test]
fn concurrent_cold_acquisition_installs_one_complete_generation() {
    let server = running_server();
    let pool = Arc::new(ConnectionPoolIndex::new(2, None));
    let barrier = Arc::new(Barrier::new(9));
    let connect_count = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();

    for _ in 0..8 {
        let server = Arc::clone(&server);
        let pool = Arc::clone(&pool);
        let barrier = Arc::clone(&barrier);
        let connect_count = Arc::clone(&connect_count);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            pool.get_or_try_install("server", || {
                connect_count.fetch_add(1, Ordering::Relaxed);
                server.connect("127.0.0.1", server.port())
            })
            .expect("connection acquisition failed")
            .generation()
        }));
    }
    barrier.wait();

    let generations: Vec<u64> = threads
        .into_iter()
        .map(|thread| thread.join().expect("acquisition thread panicked"))
        .collect();
    assert!(generations.iter().all(|generation| *generation == generations[0]));
    assert_eq!(pool.len(), 1);
    assert!(connect_count.load(Ordering::Relaxed) >= 2);
    server.stop();
}

#[test]
fn stale_generation_cannot_invalidate_replacement() {
    let server = running_server();
    let pool = ConnectionPoolIndex::new(1, None);
    let connect = || server.connect("127.0.0.1", server.port());

    let old = pool
        .get_or_try_install("server", connect)
        .expect("old acquisition failed");
    assert!(pool.invalidate("server", old.generation()));
    let new = pool
        .get_or_try_install("server", connect)
        .expect("new acquisition failed");

    assert_ne!(old.generation(), new.generation());
    assert!(!pool.invalidate("server", old.generation()));
    assert_eq!(
        pool.get("server").expect("replacement missing").generation(),
        new.generation()
    );
    server.stop();
}

#[test]
fn endpoint_bound_is_enforced_at_publication() {
    let server = running_server();
    let pool = ConnectionPoolIndex::new(1, Some(1));
    pool.get_or_try_install("one", || server.connect("127.0.0.1", server.port()))
        .expect("first endpoint failed");

    let result = pool.get_or_try_install("two", || server.connect("127.0.0.1", server.port()));
    assert!(matches!(
        result,
        Err(ConnectionPoolError::<RpcError>::Capacity { max_endpoints: 1 })
    ));
    server.stop();
}
