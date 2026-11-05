// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/runtime_shutdown.rs"]
mod common;
use common::{TestRuntimeShutdown, DEADLINE};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv::paxos::roles::{PxBallot, PxPrepareReply};
use crowdb_paxos_kv::rpc::ReplicaDestination;
use crowdb_paxos_kv_server::shutdown::{shutdown_stores, shutdown_stores_once};
use reqwest::{Method, StatusCode};
use std::sync::Arc;

async fn removal_preserves_accepted_work(method: Method, path: &str) {
    let fixture = TestRuntimeShutdown::start().await;
    let (status, body) = fixture.request(method.clone(), path).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body.contains("shutdown incomplete"), "{body}");
    fixture.assert_retained();
    fixture.repair().await;
    let (status, body) = fixture.request(method, path).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        fixture.group.local_replica().learner.engine().resume_from_slot(),
        2
    );
    assert!(!fixture.engine_marker.exists());
    assert!(!fixture.wal_marker.exists());
    if fixture.registry.get_store(1).is_some() {
        assert!(fixture.store.shutdown(DEADLINE).await.is_clean());
    }
}
#[tokio::test(flavor = "current_thread")]
async fn store_removal_retains_registry_and_wal_until_accepted_gap_drains() {
    removal_preserves_accepted_work(Method::DELETE, "/stores/1").await;
}
#[tokio::test(flavor = "current_thread")]
async fn group_removal_retains_registry_and_wal_until_accepted_gap_drains() {
    removal_preserves_accepted_work(Method::DELETE, "/stores/1/groups/1").await;
}
#[tokio::test(flavor = "current_thread")]
async fn wipe_retains_registry_and_wal_until_accepted_gap_drains() {
    removal_preserves_accepted_work(Method::POST, "/stores/1/groups/1/wipe-user-data").await;
}
#[tokio::test(flavor = "current_thread")]
async fn host_timeout_retains_all_peer_servers_and_waits_for_real_progress() {
    let fixture = TestRuntimeShutdown::start().await;
    let peer = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    peer.add_group_without_election(PxGroup::new(
        2,
        PxLocalReplica::new(2, PxLocalReplicaRole::Follower),
    ));
    peer.start().await.unwrap();
    fixture.registry.add_store(2, &peer);
    let report = shutdown_stores_once(&fixture.registry).await;
    assert!(!report.is_clean());
    fixture.assert_retained();
    let shutdown = tokio::spawn({
        let registry = fixture.registry.clone();
        async move { shutdown_stores(&registry).await }
    });
    let endpoint = peer.listen_addr().unwrap().to_string();
    let reply = tokio::time::timeout(
        DEADLINE,
        fixture.store.rpc_transport().unwrap().send_prepare(
            ReplicaDestination {
                endpoint: &endpoint,
                replica_id: 2,
            },
            2,
            PxBallot::new(1, 1),
            0,
            2,
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(reply, PxPrepareReply::Promised { .. }));
    assert!(!shutdown.is_finished());
    fixture.repair().await;
    let report = tokio::time::timeout(DEADLINE, shutdown).await.unwrap().unwrap();
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(
        fixture.group.local_replica().learner.engine().resume_from_slot(),
        2
    );
}

#[tokio::test(flavor = "current_thread")]
async fn member_route_shutdown_releases_its_registry_before_returning() {
    let registry = Arc::new(
        crowdb_paxos_kv_server::store_registry::PaxosKvStoreRegistry::with_config(
            crowdb_paxos_kv::common::config::PaxosKvConfig::for_tests(),
        ),
    );
    let weak = Arc::downgrade(&registry);
    let worker = crowdb_paxos_kv_server::recovery::member_routes::start(registry.clone());
    drop(registry);
    worker.stop().await;
    assert!(weak.upgrade().is_none());
}
