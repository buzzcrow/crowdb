// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pinned metadata for internal native root authority and generation proofs.

use std::sync::Arc;

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::paxos_kv_server::PaxosKvServer;
use crowdb_paxos_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine};
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::IoBackend;
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions};

pub struct TestMetadata {
    store: Arc<PxKvStore>,
    pub client: Arc<PaxosKvClient>,
}

impl TestMetadata {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        // Parallel fixtures are independent hosts. Their two real Tree engines
        // share one bounded runtime, without competing with unrelated fixtures.
        let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
        let replay = replay_group(&Arc::new(IoBackend::File), &[], 1).await.unwrap();
        for id in [0, 1] {
            let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig {
                runtime: Some(runtime.clone()),
                ..Default::default()
            })
            .unwrap();
            let replica = PxLocalReplica::restore_from_replay_with_engine(
                1,
                PxLocalReplicaRole::Leader,
                &replay,
                Box::new(engine),
            )
            .await
            .unwrap();
            store.add_group(PxGroup::new(id, replica));
        }
        store.start().await.expect("start pinned memory metadata");
        let endpoint = store.listen_addr().unwrap().to_string();
        let client = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![])));
        for id in [0, 1] {
            client.seed_leader(0, id, endpoint.clone());
        }
        Self { store, client }
    }
}

impl Drop for TestMetadata {
    fn drop(&mut self) {
        self.store.stop();
    }
}
