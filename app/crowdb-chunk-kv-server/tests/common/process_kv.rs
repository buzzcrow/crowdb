// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use std::sync::Arc;

pub struct TestProcessKv {
    pub store: Arc<PxKvStore>,
    pub kv: Arc<CrowdbKvClient>,
}

impl TestProcessKv {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![])));
        kv.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        Self { store, kv }
    }

    pub fn policy() -> DomainMonitorDescriptor {
        DomainMonitorDescriptor {
            domain: "chunk-kv".into(),
            service_registry_name: "chunk-kv".into(),
            driver_version: 1,
            capability_version: 1,
            heartbeat_interval_ms: 20,
            suspect_after_ms: 60,
            dead_after_ms: 100,
            lease_duration_ms: 1_000,
            max_clock_skew_ms: 1,
            self_fence_margin_ms: 1,
            failure_policy: DomainFailurePolicy::OperatorOnly,
            balance_policy: "count-first-v1".into(),
            chunk_kv_range_balance: None,
        }
    }
}

impl Drop for TestProcessKv {
    fn drop(&mut self) {
        self.store.stop();
    }
}
