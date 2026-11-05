// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    config::{NodeEntry, RackEntry, ServerEntry},
    deployment::{PreparedBootstrap, CLUSTER_KEY},
    ops::OpContext,
    ConsoleConfig,
};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use std::sync::Arc;

pub struct TestPreparationAuthority {
    pub ctx: OpContext,
    pub bootstrap: PreparedBootstrap,
    pub store: Arc<PxKvStore>,
    _root: crowdb_test_harness::test_dirs::TestDir,
}

impl TestPreparationAuthority {
    pub async fn start(members: &[u64]) -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let endpoint = store.listen_addr().unwrap().to_string();
        let mut config = ConsoleConfig {
            racks: vec![RackEntry {
                id: 4,
                name: "local".into(),
            }],
            ..Default::default()
        };
        for member in members {
            config.nodes.push(NodeEntry {
                id: *member,
                rack_id: 4,
                host: format!("node-{member}"),
                ssh_port: 2222,
                ssh_user: "crowdb".into(),
                ssh_credential_ref: None,
                ssh_key: None,
                ssh_password: None,
            });
            let mut server = ServerEntry::new(format!("kv-{member}"), format!("http://node-{member}:10000"));
            server.node_id = Some(*member);
            server.rpc_url = Some(format!("node-{member}:10100"));
            config.servers.push(server);
        }
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("default-preparation");
        let bootstrap =
            PreparedBootstrap::open(&root.path().join("prepared.json"), &config, members).unwrap();
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![endpoint.clone()])));
        kv.seed_leader(0, 0, endpoint);
        kv.put(0, 0, CLUSTER_KEY, &serde_json::to_vec(&bootstrap).unwrap(), None)
            .await
            .unwrap();
        let ctx = OpContext::with_shared_client_preserving_hint(kv, &[], config);
        Self {
            ctx,
            bootstrap,
            store,
            _root: root,
        }
    }
}

impl Drop for TestPreparationAuthority {
    fn drop(&mut self) {
        self.store.stop();
    }
}
