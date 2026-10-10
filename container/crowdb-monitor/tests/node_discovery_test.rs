// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use crowdb_monitor::{DiscoveryConfig, NodeDiscovery, NodeIdentity};
use crowdb_protocol::mgmt::node::CandidateState;
use uuid::Uuid;

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("crowdb-mdns-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn config(port: u16) -> DiscoveryConfig {
    DiscoveryConfig {
        seeds: Vec::new(),
        interfaces: vec!["lo".into()],
        addresses: vec![IpAddr::from([127, 0, 0, 1])],
        monitor_port: port,
        cluster_id: None,
    }
}

#[tokio::test]
async fn multicast_discovers_peers_detects_clones_and_processes_goodbye() {
    let first = TestRoot::new();
    let second = TestRoot::new();
    let identity = NodeIdentity::load_or_create(&first.0).unwrap();
    let peer_identity = NodeIdentity::load_or_create(&second.0).unwrap();
    let mut observer = NodeDiscovery::start(identity, &config(19093)).unwrap();
    let peer = NodeDiscovery::start(peer_identity, &config(19094)).unwrap();
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            poll.tick().await;
            if observer.refresh().iter().any(|node| {
                node.advertisement.discovery_id == peer_identity.uuid().to_string()
                    && node.state == CandidateState::Unbound
            }) {
                break;
            }
        }
    })
    .await
    .expect("actual mDNS peer discovery");
    let clone = NodeDiscovery::start(peer_identity, &config(19095)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            poll.tick().await;
            if observer
                .refresh()
                .iter()
                .filter(|node| {
                    node.advertisement.discovery_id == peer_identity.uuid().to_string()
                        && node.state == CandidateState::IdentityConflict
                })
                .count()
                == 2
            {
                break;
            }
        }
    })
    .await
    .expect("cloned UUID is visible as a conflict");
    clone.shutdown().await.unwrap();
    peer.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            poll.tick().await;
            if observer
                .refresh()
                .iter()
                .all(|node| node.advertisement.discovery_id != peer_identity.uuid().to_string())
            {
                break;
            }
        }
    })
    .await
    .expect("goodbye removes candidate observations");
    observer.shutdown().await.unwrap();
}
