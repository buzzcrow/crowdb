// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, TestDiskRoute};
use crowdb_rpc_ffi::RpcServer;

#[tokio::test]
async fn native_route_reconnects_after_server_restart_without_invalidating_old_lease() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let port = server.port();
    let client = Arc::new(
        DiskioClient::connect_for_tests(
            vec![TestDiskRoute {
                disk_id: DiskId::new(1, 2),
                rack_id: 1,
                node_id: 1,
                disk_group_id: 1,
                instance_id: 1,
                endpoint: format!("127.0.0.1:{port}"),
            }],
            DiskioClientConfig::default(),
        )
        .unwrap(),
    );
    let resolver = client.native_route_resolver(Duration::from_secs(30)).unwrap();
    let old = resolver.resolve(1, 2).unwrap();
    assert!(old.is_open());
    server.stop();
    drop(server);
    let restarted = RpcServer::new(None);
    restarted.listen("127.0.0.1", port).unwrap();
    restarted.start();
    let fresh = tokio::time::timeout(Duration::from_secs(3), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tick.tick().await;
            if !old.is_open() {
                if let Some(fresh) = resolver.resolve(1, 2) {
                    break fresh;
                }
            }
        }
    })
    .await
    .expect("closed native routes must trigger asynchronous reconnection");
    assert!(fresh.is_open());
    assert_ne!(old.raw_handles().2, fresh.raw_handles().2);
    assert!(!old.is_open());
    assert!(resolver.resolve(9, 9).is_none());
    drop((resolver, fresh, old, client));
    restarted.stop();
}
