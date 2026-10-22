// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use common::co_bench::TestCoHandler;
use crowdb_rpc_ffi::co_bench::co_spawn;
use crowdb_rpc_ffi::{RpcClient, RpcServer};

#[test]
fn coroutine_echo_validates_borrowed_bytes_and_native_totals() {
    let server = RpcServer::with_engines(None, 1, 4);
    server.listen("127.0.0.1", 0).unwrap();
    server.register_echo_handler(100);
    server.start();
    let connections: Vec<_> = (0..4)
        .map(|_| server.connect("127.0.0.1", server.port()).unwrap())
        .collect();
    let client = RpcClient::new();
    client.set_completion_pool_size(64);
    for connection in &connections {
        client.attach(connection);
    }
    client.start_reaper(5_000_000_000, 1_000_000);
    let handler = Arc::new(TestCoHandler::new(64, 32, false));
    let stats = co_spawn(&client, &server, &connections, 64, 100, handler.clone());
    client.stop_reaper();
    server.stop();
    assert_eq!(handler.errors.load(Ordering::Relaxed), 0);
    assert_eq!(handler.responses.load(Ordering::Relaxed), 2048);
    assert_eq!(stats.total_ops, 2048);
    assert_eq!(stats.total_errors, 0);
    assert!(stats.max_latency_ns >= stats.min_latency_ns);
}

#[test]
fn request_timeout_completes_coroutines_before_context_is_released() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    server.register_handler(101, |_request| {});
    server.start();
    let connections = [server.connect("127.0.0.1", server.port()).unwrap()];
    let client = RpcClient::new();
    client.set_completion_pool_size(8);
    client.attach(&connections[0]);
    client.start_reaper(2_000_000, 1_000_000);
    let handler = Arc::new(TestCoHandler::new(8, 1, true));
    let stats = co_spawn(&client, &server, &connections, 8, 101, handler.clone());
    client.stop_reaper();
    server.stop();
    assert_eq!(handler.errors.load(Ordering::Relaxed), 0);
    assert_eq!(handler.responses.load(Ordering::Relaxed), 8);
    assert_eq!(stats.total_ops, 8);
    assert_eq!(stats.total_errors, 8);
}

#[test]
fn closed_connection_rejection_is_counted_without_retrying_forever() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let connections = [server.connect("127.0.0.1", server.port()).unwrap()];
    let client = RpcClient::new();
    client.set_completion_pool_size(1);
    client.attach(&connections[0]);
    connections[0].close();
    let handler = Arc::new(TestCoHandler::new(1, u64::MAX, false));
    let stats = co_spawn(&client, &server, &connections, 1, 100, handler.clone());
    server.stop();
    assert_eq!(handler.responses.load(Ordering::Relaxed), 0);
    assert_eq!(stats.total_ops, 0);
    assert_eq!(stats.total_errors, 1);
}
