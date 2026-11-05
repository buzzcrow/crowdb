// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use bytes::Bytes;
use crowdb_paxos_kv::{
    cluster::{
        group::PxGroup,
        replica::{HeartbeatRequestPayload, ReplicaHandler},
        PaxosKvServer, PxKvStore, PxLocalReplicaRole,
    },
    common::config::PaxosKvConfig,
    paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry},
};
use crowdb_paxos_kv_server::{
    mgmt::{self, operation_registry::AppState},
    recovery::startup::{create_group_with_wal_on_runtime, store_crowdb_tree_path, store_wal_root},
    store_registry::PaxosKvStoreRegistry,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
pub const DEADLINE: Duration = Duration::from_secs(3);
pub struct TestRuntimeShutdown {
    _root: crowdb_test_harness::test_dirs::TestDir,
    pub registry: Arc<PaxosKvStoreRegistry>,
    pub store: Arc<PxKvStore>,
    pub group: Arc<PxGroup>,
    pub engine_marker: PathBuf,
    pub wal_marker: PathBuf,
    origin: String,
    http: tokio::task::JoinHandle<()>,
}
impl TestRuntimeShutdown {
    pub async fn start() -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("runtime-host-shutdown");
        let mut config = PaxosKvConfig {
            wal_backend: "file".into(),
            crowtree_backend: "mem-block".into(),
            wal_root: root.path().join("wal"),
            data_root: root.path().join("data"),
            config_root: root.path().join("config"),
            ..PaxosKvConfig::for_tests()
        };
        config.server.shutdown_timeout_ms = 20;
        config.tree_runtime.host.workers = Some(1);
        let registry = Arc::new(PaxosKvStoreRegistry::try_with_config(config).unwrap());
        let group = create_group_with_wal_on_runtime(
            1,
            1,
            1,
            PxLocalReplicaRole::Follower,
            &registry.config,
            registry.wal_backend.clone(),
            registry.crowtree_backend,
            registry.tree_runtime.clone(),
        )
        .await
        .unwrap();
        let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
        store.add_group_without_election(group);
        store.start().await.unwrap();
        registry.add_store(1, &store);
        let group = store.get_group(1).unwrap();
        accept(&group, 2).await;
        group
            .local_replica()
            .on_heartbeat(
                HeartbeatRequestPayload {
                    term: 0,
                    leader_id: 2,
                    prev_log_slot: 0,
                    prev_log_term: 0,
                    committed_safe_slot: 2,
                    lease_grant_until_ms_mono: 0,
                    t_send_ms_mono: 0,
                    gc_before_slot: 0,
                    gc_membership_epoch: 0,
                },
                1,
            )
            .await
            .unwrap();
        let engine_dir = store_crowdb_tree_path(&registry.config.data_root, 1, 1);
        let wal_dir = store_wal_root(&registry.config.wal_root, 1).join("group1");
        std::fs::create_dir_all(&engine_dir).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let engine_marker = engine_dir.join("retained-marker");
        let wal_marker = wal_dir.join("retained-marker");
        std::fs::write(&engine_marker, b"engine retained").unwrap();
        std::fs::write(&wal_marker, b"WAL retained").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let router = mgmt::router(AppState::new(registry.clone()));
        let http = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            _root: root,
            registry,
            store,
            group,
            engine_marker,
            wal_marker,
            origin,
            http,
        }
    }
    pub async fn request(&self, method: reqwest::Method, path: &str) -> (reqwest::StatusCode, String) {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(DEADLINE)
            .build()
            .unwrap();
        let result = client
            .request(method, format!("{}{path}", self.origin))
            .send()
            .await
            .unwrap();
        (result.status(), result.text().await.unwrap())
    }
    pub fn assert_retained(&self) {
        assert!(self.registry.get_store(1).is_some());
        assert!(self.store.get_group(1).is_some());
        assert!(self.engine_marker.exists());
        assert!(self.wal_marker.exists());
        assert_eq!(self.group.local_replica().contiguous_applied(), 0);
        assert_eq!(self.group.local_replica().learner.engine().resume_from_slot(), 0);
    }
    pub async fn repair(&self) {
        accept(&self.group, 1).await;
        tokio::time::timeout(DEADLINE, self.group.local_replica().await_apply_fence(2))
            .await
            .unwrap();
    }
}
impl Drop for TestRuntimeShutdown {
    fn drop(&mut self) {
        self.http.abort();
        self.store.stop();
    }
}
async fn accept(group: &PxGroup, slot: u64) {
    let mut payload = vec![1, 0, 0];
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&slot.to_le_bytes());
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&slot.to_le_bytes());
    let entry = PxLogEntry {
        slot,
        term: 0,
        ballot: PxBallot::new(1, 1),
        payload: Bytes::from(payload),
    };
    assert!(matches!(
        group.local_replica().on_accept(&entry).await,
        PxAcceptReply::Accepted { .. }
    ));
}
