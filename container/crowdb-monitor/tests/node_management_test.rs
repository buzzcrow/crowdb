// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
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
        seeds: Vec::new(),
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

#[tokio::test]
async fn explicit_seed_and_live_binding_use_the_same_discovery_classification() {
    use crowdb_protocol::mgmt::{
        node::{CandidateState, NodeBinding},
        SystemBootstrapIdentity,
    };
    let root = std::env::temp_dir().join(format!("crowdb-seed-management-{}", Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    let (peer_id, seed, seed_server, seed_stop) = test_seed().await;
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = reservation.local_addr().unwrap();
    drop(reservation);
    let config = DiscoveryConfig {
        seeds: vec![seed],
        interfaces: vec!["lo".into()],
        addresses: vec![bind.ip()],
        monitor_port: bind.port(),
        cluster_id: None,
    };
    let (stop, stopped) = oneshot::channel();
    let server_root = root.clone();
    let task = tokio::spawn(async move {
        serve_node_management(&server_root, bind, &config, "seed-client-host".into(), async {
            let _ = stopped.await;
        })
        .await
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let origin = format!("http://{bind}");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(reply) = client.get(format!("{origin}/candidates")).send().await {
                let snapshot: CandidateSnapshot = reply.json().await.unwrap();
                if let Some(peer) = snapshot
                    .nodes
                    .iter()
                    .find(|peer| peer.advertisement.discovery_id == peer_id)
                {
                    assert_eq!(peer.state, CandidateState::Unbound);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let cluster_id = Uuid::new_v4().to_string();
    let binding = NodeBinding {
        node_id: 1,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: cluster_id.clone(),
            operation_id: Uuid::new_v4().to_string(),
            configuration_digest: "a".repeat(64),
        },
        management_seeds: vec!["http://127.0.0.1:10000".into()],
    };
    let temporary = root.join("binding.tmp");
    fs::write(&temporary, serde_json::to_vec(&binding).unwrap()).unwrap();
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).unwrap();
    fs::rename(temporary, root.join("node-binding.json")).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let peer: NodeHandshake = client
                .get(format!("{origin}/node"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if peer.advertisement.cluster_id.as_deref() == Some(cluster_id.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    seed_stop.send(()).unwrap();
    seed_server.await.unwrap();
    wait_seed_expiry(&client, &origin, &peer_id).await;
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    fs::remove_dir_all(root).unwrap();
}

async fn test_seed() -> (String, String, tokio::task::JoinHandle<()>, oneshot::Sender<()>) {
    use axum::{routing::get, Json, Router};
    use crowdb_protocol::mgmt::node::{NodeAdvertisement, NODE_PROTOCOL_VERSION};
    let peer_id = Uuid::new_v4().to_string();
    let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed = format!("http://{}", seed_listener.local_addr().unwrap());
    let peer = NodeHandshake {
        advertisement: NodeAdvertisement {
            discovery_id: peer_id.clone(),
            protocol_version: NODE_PROTOCOL_VERSION,
            monitor_endpoints: vec![seed.clone()],
            cluster_id: None,
        },
        physical_host_id: "seed-only-host".into(),
        rack_hint: None,
        hardware: crowdb_protocol::mgmt::node::NodeHardware::default(),
    };
    let app = Router::new().route(
        "/node",
        get(move || {
            let peer = peer.clone();
            async { Json(peer) }
        }),
    );
    let (seed_stop, seed_stopped) = oneshot::channel();
    let seed_server = tokio::spawn(async {
        axum::serve(seed_listener, app)
            .with_graceful_shutdown(async {
                let _ = seed_stopped.await;
            })
            .await
            .unwrap();
    });
    (peer_id, seed, seed_server, seed_stop)
}

async fn wait_seed_expiry(client: &reqwest::Client, origin: &str, peer_id: &str) {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let snapshot: CandidateSnapshot = client
                .get(format!("{origin}/candidates"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if !snapshot
                .nodes
                .iter()
                .any(|peer| peer.advertisement.discovery_id == peer_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
