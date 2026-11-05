// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::now_ms;
use crowdb_access_iceberg::catalog::RoutedCatalogStore;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use std::sync::Arc;

pub(super) struct TestNativeStorage {
    pub seeds: Vec<String>,
}

impl TestNativeStorage {
    pub fn new() -> Self {
        let ips = crate::common::management::addresses(14);
        Self {
            seeds: ips[..3]
                .iter()
                .map(|ip| crate::common::management::origin(ip))
                .collect(),
        }
    }

    pub async fn restart_catalog(&self) {
        crate::common::control::node("kill", 11).await;
        crate::common::control::node("start", 11).await;
    }

    pub async fn store(&self) -> Arc<RoutedCatalogStore> {
        let config = ClientConfig::default();
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::new(
            crowdb_paxos_kv_client::PaxosKvClient::new(crowdb_paxos_kv_client::ClientConfig::new(
                self.seeds.clone(),
            )),
        )));
        let transport = Arc::new(ChunkKvRpcTransport::new(config.max_owner_connections, 1, 2));
        let client = Arc::new(ChunkKvClient::new(config, source, transport).unwrap());
        client.refresh_catalog().await.unwrap();
        Arc::new(RoutedCatalogStore::new(client))
    }
}

pub async fn activate(repository: &crowdb_access_iceberg::catalog::CatalogRepository) {
    use crowdb_access_iceberg::{
        catalog::{Capabilities, ManagementPrivilege},
        key::OperationId,
        operation::{ManagementAction, ManagementRequest, RequestIdentity},
    };
    let (root, authority) = repository.status().await.unwrap();
    let now = now_ms();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now,
                },
                principal: "manager".into(),
                action: ManagementAction::Activate,
                expected_epoch: root.context.activation_epoch,
                display_name: authority.display_name,
                confirmation: None,
                capabilities: Some(Capabilities::from_bits(0x3fff).unwrap()),
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
}
