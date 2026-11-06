// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::memory_kv::TestKvCluster;
use crowdb_console_shared::{
    config::{NodeEntry, RackEntry, ServerEntry},
    deployment::{PreparedBootstrap, CLUSTER_KEY},
    ConsoleConfig,
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use crowdb_protocol::mgmt::node::NodeBinding;
use crowdb_web::AppState;
use std::sync::Arc;

pub struct TestClusterTarget {
    pub state: AppState,
    pub prepared: PreparedBootstrap,
    pub binding: NodeBinding,
    pub kv: Arc<PaxosKvClient>,
    pub root: crowdb_test_harness::test_dirs::TestDir,
    _cluster: TestKvCluster,
}

impl TestClusterTarget {
    pub async fn start() -> Self {
        let cluster = TestKvCluster::start().await;
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
            cluster.mgmt_endpoints.clone(),
        )));
        kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cluster-target-api");
        let mut server = ServerEntry::new("kv-2", &cluster.mgmt_endpoints[0]);
        server.node_id = Some(2);
        server.rpc_url = Some(cluster.group0_leader_endpoint.clone());
        let config = ConsoleConfig {
            servers: vec![server],
            racks: vec![RackEntry {
                id: 1,
                name: "local".into(),
            }],
            nodes: vec![NodeEntry {
                id: 2,
                rack_id: 1,
                host: "127.0.0.1".into(),
                ssh_port: 22,
                ssh_user: String::new(),
                ssh_credential_ref: None,
                ssh_key: None,
                ssh_password: None,
            }],
            ..Default::default()
        };
        let prepared = PreparedBootstrap::open(&root.path().join("prepared.json"), &config, &[2]).unwrap();
        kv.put(0, 0, CLUSTER_KEY, &serde_json::to_vec(&prepared).unwrap(), None)
            .await
            .unwrap();
        let binding = NodeBinding {
            node_id: 2,
            bootstrap: prepared.identity.clone(),
            management_seeds: cluster.mgmt_endpoints.clone(),
        };
        std::fs::write(
            root.path().join("node-binding.json"),
            serde_json::to_vec(&binding).unwrap(),
        )
        .unwrap();
        std::fs::create_dir(root.path().join("console")).unwrap();
        let mut state = AppState::with_runtime_root(config, root.path().join("console"))
            .with_node_monitor("http://127.0.0.1:1")
            .unwrap();
        state.readonly = true;
        *state.kv_client.write().await = Some(Arc::clone(&kv));
        Self {
            state,
            prepared,
            binding,
            kv,
            root,
            _cluster: cluster,
        }
    }
}
