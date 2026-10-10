// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use crowdb_monitor::{serve_node_management, DiscoveryConfig, NodeIdentity};
use crowdb_protocol::mgmt::node::{CandidateSnapshot, NodeHandshake};
use tokio::sync::oneshot;
use uuid::Uuid;

#[tokio::test]
async fn management_exposes_persistent_identity_and_requires_physical_host() {
    let root = std::env::temp_dir().join(format!("crowdb-node-management-{}", Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    let identity = NodeIdentity::load_or_create(&root).unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let config = DiscoveryConfig {
        interfaces: vec!["lo".into()],
        addresses: vec![bind.ip()],
        monitor_port: port,
        cluster_id: None,
    };
    assert!(
        serve_node_management(&root, bind, &config, String::new(), async {})
            .await
            .is_err()
    );
    drop(reservation);
    let (stop, stopped) = oneshot::channel();
    let server_root = root.clone();
    let task = tokio::spawn(async move {
        serve_node_management(&server_root, bind, &config, "test-host".into(), async {
            let _ = stopped.await;
        })
        .await
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let base = format!("http://{bind}");
    let handshake = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            assert!(!task.is_finished(), "management server exited before readiness");
            if let Ok(reply) = client.get(format!("{base}/node")).send().await {
                break reply
                    .error_for_status()
                    .unwrap()
                    .json::<NodeHandshake>()
                    .await
                    .unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(handshake.advertisement.discovery_id, identity.uuid().to_string());
    assert_eq!(handshake.physical_host_id, "test-host");
    assert_eq!(handshake.advertisement.cluster_id, None);
    let snapshot = client
        .get(format!("{base}/candidates"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<CandidateSnapshot>()
        .await
        .unwrap();
    assert!(snapshot.nodes.is_empty());
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(NodeIdentity::load_or_create(&root).unwrap(), identity);
    fs::remove_dir_all(root).unwrap();
}
