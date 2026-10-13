// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client orchestration is local: fake children retain PID-reaping and timing proofs.
//! Packaged three-voter lifecycle and authority have their container E2E owners.

#[path = "common/deployer_peer.rs"]
mod deployer_peer;
use crowdb_console_shared::cluster_deployer::{simple, CrowdbClusterDeployer};
use std::time::{Duration, Instant};

#[tokio::test]
async fn deployer_start_stop_reset_cycle() {
    let peer = deployer_peer::TestPeer::start().await;
    let mut deployer = CrowdbClusterDeployer::new(&peer.origin).unwrap();
    deployer.start(&simple()).await.unwrap();
    let info = deployer.info().unwrap();
    assert_eq!(info.nodes.len(), 3);
    assert_eq!(info.stores.len(), 1);
    assert_eq!(info.stores[0].groups.len(), 1);
    assert_eq!(info.stores[0].groups[0].leader_node_id, Some(1));
    assert_eq!(
        info.stores[0].groups[0].leader_endpoint.as_deref(),
        Some("127.0.0.1:7001")
    );
    assert_eq!(peer.snapshot().await["alive"], 3);
    let stop = Instant::now();
    deployer.stop().await;
    assert!(stop.elapsed() < Duration::from_secs(5));
    assert!(deployer.info().is_none());
    let stopped = peer.snapshot().await;
    assert_eq!(
        stopped["alive"], 0,
        "HTTP acknowledgement alone must not leave children alive"
    );
    assert_eq!(
        stopped["journal"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v.as_str().unwrap().ends_with("/server/stop"))
            .count(),
        3
    );
    let reset = Instant::now();
    deployer.reset().await.unwrap();
    assert!(reset.elapsed() < Duration::from_secs(1));
    assert_eq!(peer.snapshot().await["stores"], serde_json::json!([]));
}

#[tokio::test]
async fn deployer_repeated_cycles_no_state_leakage() {
    let peer = deployer_peer::TestPeer::start().await;
    let mut deployer = CrowdbClusterDeployer::new(&peer.origin).unwrap();
    for _ in 0..3 {
        deployer.start(&simple()).await.unwrap();
        assert_eq!(deployer.info().unwrap().nodes.len(), 3);
        assert_eq!(peer.snapshot().await["alive"], 3);
        deployer.teardown().await.unwrap();
        assert!(deployer.info().is_none());
        let state = peer.snapshot().await;
        assert_eq!(state["alive"], 0);
        assert_eq!(state["stores"], serde_json::json!([]));
    }
    let journal = peer.snapshot().await["journal"].as_array().unwrap().clone();
    assert_eq!(
        journal
            .iter()
            .filter(|v| v.as_str().unwrap().ends_with("/server/deploy"))
            .count(),
        9
    );
    assert_eq!(
        journal
            .iter()
            .filter(|v| v.as_str().unwrap() == "POST /api/cluster/init")
            .count(),
        3
    );
}
