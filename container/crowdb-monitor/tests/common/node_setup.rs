// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, routing::get, Json, Router};
use crowdb_console_shared::deployment::registry::{NodeRecord, NodeRegistry, NODES_KEY};
use crowdb_console_shared::{
    config::{NodeEntry, RackEntry, ServerEntry},
    deployment::{preparation, PreparedBootstrap},
    ops::OpContext,
    ConsoleConfig,
};
use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::mgmt::node::{setup::NodeSetupRequest, NodeBinding};
use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc};

pub struct TestAuthority {
    store: Arc<PxKvStore>,
    task: tokio::task::JoinHandle<()>,
    pub kv: CrowdbKvClient,
    pub binding: NodeBinding,
    pub registry: NodeRegistry,
}

impl TestAuthority {
    pub async fn start(root: &Path, request: &NodeSetupRequest) -> Self {
        async fn topology(State(store): State<Arc<PxKvStore>>) -> Json<serde_json::Value> {
            Json(serde_json::json!({"stores":[store.status()]}))
        }
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/topology", get(topology))
            .with_state(store.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let kv = CrowdbKvClient::new(ClientConfig::new(vec![seed.clone()]));
        kv.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        let bootstrap = bootstrap(root, &kv, &seed, &store.listen_addr().unwrap().to_string()).await;
        let binding = NodeBinding {
            node_id: 2,
            management_seeds: vec![seed],
            bootstrap: bootstrap.identity,
        };
        let registry = NodeRegistry {
            cluster_id: binding.bootstrap.cluster_id.clone(),
            nodes: vec![NodeRecord {
                web_port: 9090,
                discovery_id: request.discovery_id.clone(),
                operation_id: request.operation_id.clone(),
                node_id: 2,
                rack_id: 1,
                physical_host_id: "owned-host".into(),
                host: "127.0.0.1".into(),
                ssh_port: 22,
                ssh_user: "crowdb".into(),
                confirmed: true,
                host_key: None,
                cancelled: false,
            }],
        };
        kv.put(0, 0, NODES_KEY, &serde_json::to_vec(&registry).unwrap(), None)
            .await
            .unwrap();
        let path = root.join("node-binding.json");
        std::fs::write(&path, serde_json::to_vec(&binding).unwrap()).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        Self {
            store,
            task,
            kv,
            binding,
            registry,
        }
    }
}

async fn bootstrap(root: &Path, kv: &CrowdbKvClient, management: &str, rpc: &str) -> PreparedBootstrap {
    let mut server = ServerEntry::new("kv-2", management);
    server.node_id = Some(2);
    server.rpc_url = Some(rpc.into());
    let config = ConsoleConfig {
        racks: vec![RackEntry {
            id: 1,
            name: "owned-rack".into(),
        }],
        nodes: vec![NodeEntry {
            id: 2,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: "crowdb".into(),
            ssh_credential_ref: None,
            ssh_key: None,
            ssh_password: None,
        }],
        servers: vec![server],
        ..Default::default()
    };
    let prepared = PreparedBootstrap::open(&root.join("fixture-bootstrap.json"), &config, &[2]).unwrap();
    kv.put(
        0,
        0,
        b"/deployment/cluster",
        &serde_json::to_vec(&prepared).unwrap(),
        None,
    )
    .await
    .unwrap();
    let ctx = OpContext::new(rpc.into(), vec![management.into()], config);
    preparation::reserve(&ctx, &prepared.identity).await.unwrap();
    prepared
}

impl Drop for TestAuthority {
    fn drop(&mut self) {
        self.task.abort();
        self.store.stop();
    }
}
