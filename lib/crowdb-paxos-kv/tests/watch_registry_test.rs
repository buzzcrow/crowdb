// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_paxos_kv::cluster::watch_registry::{CrowdbRpcPushTarget, WatchRegistry};
use crowdb_rpc_ffi::{RpcClient, RpcServer};

#[test]
fn fast_path_flag_tracks_watcher_mutations() {
    let server = Arc::new(RpcServer::with_engines(None, 1, 1));
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let connection = server.connect("127.0.0.1", server.port()).unwrap();
    let target = Arc::new(CrowdbRpcPushTarget::new(
        connection,
        Arc::new(RpcClient::new()),
        Arc::clone(&server),
    ));

    let registry = WatchRegistry::new();
    assert!(!registry.has_watchers());
    let first = registry.subscribe_crowdb_rpc(b"a", Arc::clone(&target));
    assert!(registry.has_watchers());
    let second = registry.subscribe_crowdb_rpc(b"ab", target);
    registry.unsubscribe(b"a", first);
    assert!(registry.has_watchers());
    registry.remove_all(&[second]);
    assert!(!registry.has_watchers());

    let replacement = Arc::new(RpcClient::new());
    let replacement_target = Arc::new(CrowdbRpcPushTarget::new(
        server.connect("127.0.0.1", server.port()).unwrap(),
        replacement,
        Arc::clone(&server),
    ));
    let _ = registry.subscribe_crowdb_rpc(b"", replacement_target);
    registry.clear();
    assert!(!registry.has_watchers());
    server.stop();
}
